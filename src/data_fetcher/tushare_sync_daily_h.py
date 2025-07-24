"""
文件: tushare_sync_daily_h.py
功能:
  1. 读取 DuckDB 中 H 股代码 (stock_basic_h)
  2. 按股票增量同步 hk_daily / hk_daily_adj
  3. 用 sync_date 表记录每只股票在各数据表的“最后同步日期”，
     今日已同步过则直接跳过，提高效率
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd
import tushare as ts

# ─────────────────────────────────────────────────────────────── 日志 ──
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ─────────────────────────────────────────────────────────────── 配置 ──
from data_fetcher.settings import settings  # noqa: E402

TS_TOKEN: str = settings.tushare_api_token.get_secret_value()
START_DATE: str = getattr(settings, "start_date", "20120101")

DUCKDB_PATH = Path("data/data.duckdb")
LIMIT = 6000
MAX_RETRY = 3
SLEEP = 0.01
SLEEP_ON_FAIL = SLEEP * 2
BATCH_SIZE = 100  # 每 100 只股票提交一次

DATA_CONFIG: Dict[str, Dict[str, Any]] = {
    "daily": {
        "api_name": "hk_daily",
        "fields": [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        ],
        "table": "daily_h",
    },
    "adj": {
        "api_name": "hk_daily_adj",
        "fields": ["ts_code", "trade_date", "adj_factor"],
        "table": "adj_factor_h",
    },
}

# ───────────────────────────────────────────────────── DuckDB 工具 ──


def _connect() -> duckdb.DuckDBPyConnection:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    # 创建同步记录表
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_date (
            table_name       VARCHAR,
            ts_code          VARCHAR,
            last_update_date VARCHAR,
            PRIMARY KEY (table_name, ts_code)
        )
        """
    )
    return con


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return table in {row[0] for row in con.execute("SHOW TABLES").fetchall()}


def _get_latest_date(
    con: duckdb.DuckDBPyConnection, table: str, ts_code: str
) -> str | None:
    if not _table_exists(con, table):
        return None
    res = con.execute(
        f"SELECT MAX(trade_date) FROM {table} WHERE ts_code = ?", [ts_code]
    ).fetchone()[0]
    return res


# -------------------- sync_date 辅助 --------------------


def _get_last_sync(
    con: duckdb.DuckDBPyConnection, tbl: str, ts_code: str
) -> str | None:
    row = con.execute(
        """
        SELECT last_update_date FROM sync_date
        WHERE table_name = ? AND ts_code = ?
        """,
        [tbl, ts_code],
    ).fetchone()
    return None if row is None else row[0]


def _set_last_sync(
    con: duckdb.DuckDBPyConnection, tbl: str, ts_code: str, date_str: str
) -> None:
    con.execute(
        """
        INSERT INTO sync_date (table_name, ts_code, last_update_date)
        VALUES (?, ?, ?)
        ON CONFLICT (table_name, ts_code)
        DO UPDATE SET last_update_date = EXCLUDED.last_update_date
        """,
        [tbl, ts_code, date_str],
    )


# -------------------- 通用 UPSERT --------------------


def _upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        return
    if not _table_exists(con, table):
        con.register("df_view", df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_view")
        con.execute(f"CREATE UNIQUE INDEX {table}_uq ON {table} (ts_code, trade_date)")
        logging.info("新建表 %-14s ➜ 写入 %5d 行", table, len(df))
    else:
        con.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_uq "
            f"ON {table} (ts_code, trade_date)"
        )
        con.register("new_rows", df)
        con.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM new_rows")
        logging.info("表 %-14s ➜ 追加 %5d 行", table, len(df))


# ─────────────────────────────────────────────── Fetch 工具 ──


def _fetch_api_with_paging_once(
    pro: ts.pro_api,
    api_name: str,
    params: Dict[str, Any],
    fields: List[str],
) -> pd.DataFrame:
    """不带重试的分页抓取—最小单元。"""
    offset, chunks = 0, []
    while True:
        df_chunk = getattr(pro, api_name)(
            **params,
            offset=offset,
            limit=LIMIT,
            fields=",".join(fields),
        )
        chunks.append(df_chunk)
        if len(df_chunk) < LIMIT:
            break
        offset += LIMIT
        time.sleep(SLEEP)
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _fetch_api_with_paging(
    pro: ts.pro_api,
    api_name: str,
    params: Dict[str, Any],
    fields: List[str],
) -> pd.DataFrame:
    """带重试 + pandas 2.2 is_unique 补丁的分页抓取。"""
    # —— 第一层：正常尝试 & 重试 ——
    for attempt in range(1, MAX_RETRY + 1):
        try:
            return _fetch_api_with_paging_once(pro, api_name, params, fields)
        except AttributeError as exc:
            # 捕获 pandas 2.2 触发的 "built‑in function ... has no attribute is_unique"
            if "is_unique" in str(exc):
                logging.warning(
                    "触发 pandas is_unique bug，进入日颗粒兜底 ts_code=%s",
                    params.get("ts_code"),
                )
                break  # 跳出 retry 循环，改用日颗粒
            logging.warning(
                "%s 调用失败 ts_code=%s offset=%s attempt=%s/%s: %s",
                api_name,
                params.get("ts_code"),
                params.get("offset", 0),
                attempt,
                MAX_RETRY,
                exc,
            )
            time.sleep(SLEEP_ON_FAIL)
        except Exception as exc:
            logging.warning(
                "%s 调用失败 ts_code=%s attempt=%s/%s: %s",
                api_name,
                params.get("ts_code"),
                attempt,
                MAX_RETRY,
                exc,
            )
            time.sleep(SLEEP_ON_FAIL)
    else:
        # 三次正常重试仍失败
        raise RuntimeError(f"{api_name} 连续 {MAX_RETRY} 次失败，终止。")

    # —— 第二层：pandas bug 兜底（日颗粒拉取） ——
    start_str = params.get("start_date") or params.get("trade_date") or START_DATE
    end_str = (
        params.get("end_date")
        or params.get("trade_date")
        or datetime.now().strftime("%Y%m%d")
    )
    start_dt = pd.to_datetime(start_str)
    end_dt = pd.to_datetime(end_str)

    daily_chunks: List[pd.DataFrame] = []
    for date in pd.date_range(start_dt, end_dt):
        daily_params = params.copy()
        daily_params.pop("start_date", None)
        daily_params.pop("end_date", None)
        daily_params["trade_date"] = date.strftime("%Y%m%d")
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_day = _fetch_api_with_paging_once(
                    pro, api_name, daily_params, fields
                )
                if not df_day.empty:
                    daily_chunks.append(df_day)
                break
            except Exception as exc:
                if attempt == MAX_RETRY:
                    logging.warning(
                        "单日拉取失败终止 trade_date=%s ts_code=%s: %s",
                        daily_params["trade_date"],
                        params.get("ts_code"),
                        exc,
                    )
                time.sleep(SLEEP_ON_FAIL)
    return (
        pd.concat(daily_chunks, ignore_index=True) if daily_chunks else pd.DataFrame()
    )


# ─────────────────────────────────────────────────────── 主流程 ──


def data_sync() -> None:
    pro = ts.pro_api(TS_TOKEN)
    today = datetime.now().strftime("%Y%m%d")

    with _connect() as con:
        if not _table_exists(con, "stock_basic_h"):
            raise RuntimeError("缺少 stock_basic_h，请先运行 tushare_sync_basic.py")

        stock_df = con.execute("SELECT ts_code, name FROM stock_basic_h").fetchdf()
        total = len(stock_df)
        logging.info("共检测到 %s 只 H 股待同步", total)

        con.execute("BEGIN")  # 显式事务
        for idx, row in enumerate(stock_df.itertuples(index=False), 1):
            ts_code: str = row.ts_code
            stock_name: str = row.name

            for cfg in DATA_CONFIG.values():
                tbl = cfg["table"]

                # —— STEP 0: 当天是否同步过 ——
                last_sync = _get_last_sync(con, tbl, ts_code)
                if last_sync == today:
                    continue

                # —— STEP 1: 计算起始日期 ——
                latest = _get_latest_date(con, tbl, ts_code)
                start_date = (
                    (pd.to_datetime(latest) + timedelta(days=1)).strftime("%Y%m%d")
                    if latest
                    else START_DATE
                )
                if start_date > today:
                    _set_last_sync(con, tbl, ts_code, today)
                    continue

                params = {
                    "ts_code": ts_code,
                    "start_date": start_date,
                    "end_date": today,
                }
                df = _fetch_api_with_paging(pro, cfg["api_name"], params, cfg["fields"])
                if not df.empty:
                    df.sort_values("trade_date", inplace=True)
                    _upsert(con, df, tbl)

                _set_last_sync(con, tbl, ts_code, today)

            logging.info(
                "已完成 %-6s/%-6s | %-10s (%s)",
                idx,
                total,
                stock_name,
                ts_code,
            )
            time.sleep(SLEEP)

            if idx % BATCH_SIZE == 0:
                con.execute("COMMIT")
                con.execute("BEGIN")

        con.execute("COMMIT")  # 收尾


if __name__ == "__main__":
    data_sync()
