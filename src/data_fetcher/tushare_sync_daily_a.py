"""
文件: tushare_sync_daily_a.py
功能:
  1. 读取 DuckDB 中 A 股代码 (stock_basic_a)
  2. 按股票增量同步 daily / adj_factor
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

# ────────────────────────────────────────────────────────────── 日志 ──
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ────────────────────────────────────────────────────────────── 配置 ──
from data_fetcher.settings import settings  # noqa: E402

TS_TOKEN: str = settings.tushare_api_token.get_secret_value()
START_DATE: str = getattr(settings, "start_date", "20120101")

DUCKDB_PATH = Path("data/data.duckdb")
LIMIT = 6000
MAX_RETRY = 3
SLEEP = 0.01
SLEEP_ON_FAIL = SLEEP * 2
BATCH_SIZE = 100  # 每 100 只股票提交一次数据库

DATA_CONFIG: Dict[str, Dict[str, Any]] = {
    "daily": {
        "api_name": "daily",
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
        "table": "daily_a",
    },
    "adj": {
        "api_name": "adj_factor",
        "fields": ["ts_code", "trade_date", "adj_factor"],
        "table": "adj_factor_a",
    },
    "bak": {
        "api_name": "bak_daily",
        "fields": [
            "ts_code",
            "trade_date",
            "name",
            "pct_change",
            "vol_ratio",
            "turn_over",
            "swing",
            "vol",
            "selling",
            "buying",
            "total_share",
            "float_share",
            "pe",
            "industry",
            "area",
            "float_mv",
            "total_mv",
            "avg_price",
            "strength",
            "activity",
            "avg_turnover",
            "attack",
            "interval_3",
            "interval_6",
        ],
        "table": "bak_daily_a",
    },
}


# ───────────────────────────────────────────────────── DuckDB 工具 ──
def _connect() -> duckdb.DuckDBPyConnection:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))

    # --- 新增: 创建同步记录表 ------------------------------  ### ← 新增/修改
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


# --- 新增: 查询 / 更新 sync_date ---------------------------  ### ← 新增/修改
def _get_last_sync(
    con: duckdb.DuckDBPyConnection, tbl: str, ts_code: str
) -> str | None:
    row = con.execute(
        """
        SELECT last_update_date
        FROM sync_date
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
        con.execute(
            f"""
            INSERT OR IGNORE INTO {table}
            SELECT * FROM new_rows
            """
        )
        logging.info("表 %-14s ➜ 追加 %5d 行", table, len(df))


# ──────────────────────────────────────────────── Tushare 拉取工具 ──
def _fetch_api_with_paging(
    pro: ts.pro_api,
    api_name: str,
    params: Dict[str, Any],
    fields: List[str],
) -> pd.DataFrame:
    offset, chunks = 0, []
    while True:
        df_chunk = None
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_chunk = getattr(pro, api_name)(
                    **params,
                    offset=offset,
                    limit=LIMIT,
                    fields=",".join(fields),
                )
                break
            except Exception as exc:
                logging.warning(
                    "%s 调用失败 ts_code=%s offset=%s attempt=%s/%s: %s",
                    api_name,
                    params.get("ts_code"),
                    offset,
                    attempt,
                    MAX_RETRY,
                    exc,
                )
                time.sleep(SLEEP_ON_FAIL)

        if df_chunk is None:
            raise RuntimeError(f"{api_name} 连续 {MAX_RETRY} 次失败，终止。")

        chunks.append(df_chunk)
        if len(df_chunk) < LIMIT:
            break
        offset += LIMIT
        time.sleep(SLEEP)

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


# ──────────────────────────────────────────────────────── 主流程 ──
def data_sync() -> None:
    pro = ts.pro_api(TS_TOKEN)
    today = datetime.now().strftime("%Y%m%d")

    with _connect() as con:
        if not _table_exists(con, "stock_basic_a"):
            raise RuntimeError("缺少 stock_basic_a，请先运行 tushare_sync_basic.py")

        stock_df = con.execute("SELECT ts_code, name FROM stock_basic_a").fetchdf()
        total = len(stock_df)
        logging.info("共检测到 %s 只 A 股待同步", total)

        con.execute("BEGIN")  # ← 显式开启事务
        for idx, row in enumerate(stock_df.itertuples(index=False), 1):
            ts_code: str = row.ts_code
            stock_name: str = row.name

            for cfg in DATA_CONFIG.values():
                tbl = cfg["table"]

                # ---- STEP 0 : 看 sync_date 是否已是今天 ----------------  ### ← 新增/修改
                last_sync = _get_last_sync(con, tbl, ts_code)
                if last_sync == today:
                    logging.debug(
                        "[跳过] %s %s (%s) 今日已同步过", tbl, stock_name, ts_code
                    )
                    continue

                # ---- STEP 1 : 计算起始日期 ----
                latest = _get_latest_date(con, tbl, ts_code)
                start_date = (
                    (pd.to_datetime(latest) + timedelta(days=1)).strftime("%Y%m%d")
                    if latest
                    else START_DATE
                )
                if start_date > today:
                    # 本地已最新，但 sync_date 不是今天 → 只更新记录表
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

                # ---- STEP 2 : 写入 / 更新 sync_date ----  ### ← 新增/修改
                _set_last_sync(con, tbl, ts_code, today)

            logging.info(
                "已完成 %-6s/%-6s | %-10s (%s)",
                idx,
                total,
                stock_name,
                ts_code,
            )
            time.sleep(SLEEP)  # 控制节奏

            if idx % BATCH_SIZE == 0:
                con.execute("COMMIT")
                con.execute("BEGIN")  # 继续下一批

        con.execute("COMMIT")  # ← 收尾提交


# ───────────────────────────────────────────────────── CLI 入口 ──
if __name__ == "__main__":
    data_sync()
