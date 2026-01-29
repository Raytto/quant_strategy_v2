"""
文件: tushare_sync_daily.py
功能:
  增量同步多张日频数据表到 SQLite (逐表独立增量, 不互相影响):
    - daily_a        (A 股日线)
    - adj_factor_a   (A 股复权因子)
    - bak_daily_a    (A 股特色扩展行情)
    - daily_h        (港股日线)
    - adj_factor_h   (港股复权因子)
    - fx_daily       (外汇日线: 代码来源 fx_basic 全量，可 CLI 过滤)
    - index_daily    (国内指数日线: 目前仅 000300.SH 沪深300)
    - index_global   (国际/全球指数日线: 目前仅 HSI 恒生指数, IXIC 纳斯达克综合)
  使用统一 sync_date (table_name, ts_code) 控制去重。每个表、每个 ts_code 独立判断是否需要抓取，互不干扰。

重要增量策略:
  * 仅当某表对某 ts_code 真正写入了“新交易日”数据才把 sync_date 标记为今天；
  * 若今天尚未产出(接口返回空)则不标记，使得当日后续再次运行仍会尝试补抓，避免日线已出但复权因子尚未出的漏采问题。
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Iterable, Optional

import pandas as pd  # type: ignore
import tushare as ts  # type: ignore

from data_fetcher.settings import get_start_date, get_tushare_token  # noqa: E402
from qs.sqlite_utils import connect_sqlite, ensure_unique_index, insert_df_ignore, table_exists

# ───────────────────────────────────────────── 配置常量 ──
START_DATE: str = get_start_date("20120101")
SQLITE_PATH = Path("data/data.sqlite")
# 全局默认 (可被每个表在 TABLE_CONFIG 中以 limit / sleep / sleep_on_fail 覆盖)
DEFAULT_LIMIT = 3000
DEFAULT_SLEEP = 0.01
DEFAULT_SLEEP_ON_FAIL = 2  # 或者 DEFAULT_SLEEP * 2
MAX_RETRY = 3
BATCH_SIZE = 100  # 每 100 只股票提交一次事务

# ───────────────────────────────────────────── 目标表配置 (平铺) ──
# key = 目标表名 (亦作 CLI 指定名)
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "fx_daily": {  # 外汇 (动态代码池: fx_basic)
        "api_name": "fx_daily",
        "fields": [
            "ts_code",
            "trade_date",
            "bid_open",
            "bid_close",
            "bid_high",
            "bid_low",
            "ask_open",
            "ask_close",
            "ask_high",
            "ask_low",
            "tick_qty",
        ],
        "stock_table": "fx_basic",  # 取自基础表(由 tushare_sync_basic 写入)
        "limit": 3000,
    },
    "index_daily": {  # 国内指数 (使用显式 ts_codes 列表)
        "api_name": "index_daily",
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
        "ts_codes": ["000300.SH"],  # 沪深300
        "limit": 6000,
    },
    "index_global": {  # 国际指数 (当前示例: 恒生 HSI, 纳指 IXIC)
        "api_name": "index_global",
        "fields": [
            "ts_code",
            "trade_date",
            "open",
            "close",  # 注意该接口字段顺序与国内 daily 略有差别
            "high",
            "low",
            "pre_close",
            "change",
            "pct_chg",
            "swing",
            "vol",
        ],
        "ts_codes": ["HSI", "IXIC"],
        "limit": 6000,
    },
    "daily_a": {  # A 股日线
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
        "stock_table": "stock_basic_a",
        "limit": 6000,
    },
    "adj_factor_a": {  # A 股复权因子
        "api_name": "adj_factor",
        "fields": ["ts_code", "trade_date", "adj_factor"],
        "stock_table": "stock_basic_a",
        "limit": 6000,
    },
    "bak_daily_a": {  # A 股拓展
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
        "stock_table": "stock_basic_a",
        "limit": 6000,
    },
    "daily_h": {  # 港股日线
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
        "stock_table": "stock_basic_h",
        "limit": 6000,
    },
    "adj_factor_h": {  # 港股复权因子
        "api_name": "hk_daily_adj",
        "fields": ["ts_code", "trade_date", "adj_factor"],
        "stock_table": "stock_basic_h",
        "limit": 6000,
    },
}

# 去除旧别名兼容逻辑, CLI 直接使用上述表名; 外汇可通过 --fx-codes 覆盖 (若保留)
RUNTIME_FX_CODES: List[str] | None = None  # 若 CLI 提供则过滤，否则使用 fx_basic 全量

# ───────────────────────────────────────────── 日志配置 ──
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
logger = logging.getLogger(__name__)

# ───────────────────────────────────────────── SQLite 工具 ──


def _connect() -> sqlite3.Connection:
    con = connect_sqlite(SQLITE_PATH)
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


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    return table_exists(con, table)


def _get_latest_date(
    con: sqlite3.Connection, table: str, ts_code: str
) -> Optional[str]:
    if not _table_exists(con, table):
        return None
    row = con.execute(
        f"SELECT MAX(trade_date) FROM {table} WHERE ts_code=?", [ts_code]
    ).fetchone()
    return row[0] if row else None


def _get_last_sync(
    con: sqlite3.Connection, table: str, ts_code: str
) -> Optional[str]:
    row = con.execute(
        "SELECT last_update_date FROM sync_date WHERE table_name=? AND ts_code=?",
        [table, ts_code],
    ).fetchone()
    return row[0] if row else None


def _set_last_sync(
    con: sqlite3.Connection, table: str, ts_code: str, date_str: str
) -> None:
    con.execute(
        """
        INSERT INTO sync_date (table_name, ts_code, last_update_date)
        VALUES (?, ?, ?)
        ON CONFLICT (table_name, ts_code)
        DO UPDATE SET last_update_date = EXCLUDED.last_update_date
        """,
        [table, ts_code, date_str],
    )


def _upsert(
    con: sqlite3.Connection,
    df: pd.DataFrame,
    table: str,
    context_ts: str | None = None,
    context_name: str | None = None,
) -> None:
    """将单只股票(或若干)的增量数据写入.
    context_ts/context_name 仅用于日志增强, 不参与逻辑.
    """
    if df.empty:
        logger.debug(
            "[跳过] %s %s %s 空数据", table, context_ts or "", context_name or ""
        )
        return
    tag = f"{context_ts or ''} {context_name or ''}".strip()
    if not _table_exists(con, table):
        df.head(0).to_sql(table, con, if_exists="fail", index=False)
        if "ts_code" in df.columns and "trade_date" in df.columns:
            ensure_unique_index(
                con,
                table=table,
                columns=["ts_code", "trade_date"],
                index_name=f"{table}_uq",
            )
        inserted = insert_df_ignore(
            con,
            df=df,
            table=table,
            unique_by=["ts_code", "trade_date"]
            if "ts_code" in df.columns and "trade_date" in df.columns
            else None,
        )
        logger.info("[新建] %-14s %-22s 写入 %6d 行", table, tag, inserted)
    else:
        if "ts_code" in df.columns and "trade_date" in df.columns:
            ensure_unique_index(
                con,
                table=table,
                columns=["ts_code", "trade_date"],
                index_name=f"{table}_uq",
            )
        inserted = insert_df_ignore(
            con,
            df=df,
            table=table,
            unique_by=["ts_code", "trade_date"]
            if "ts_code" in df.columns and "trade_date" in df.columns
            else None,
        )
        logger.info(
            "[追加] %-14s %-22s 新增 %6d 行 (请求 %d)",
            table,
            tag,
            inserted,
            len(df),
        )


# ───────────────────────────────────────────── Tushare 抓取 ──


def _fetch_page(
    pro: ts.pro_api, api_name: str, params: Dict[str, Any], fields: List[str]
) -> pd.DataFrame:
    return getattr(pro, api_name)(**params, fields=",".join(fields))


def _fetch_api_with_paging(
    pro: ts.pro_api,
    api_name: str,
    params: Dict[str, Any],
    fields: List[str],
    *,
    limit: int,
    sleep: float,
    sleep_on_fail: float,
) -> pd.DataFrame:
    """分页 + 重试 + pandas is_unique 兜底(日颗粒)。支持 per-table limit/sleep。"""
    offset, chunks = 0, []
    while True:
        df_chunk = None
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_chunk = getattr(pro, api_name)(
                    **params, offset=offset, limit=limit, fields=",".join(fields)
                )
                break
            except AttributeError as exc:
                if "is_unique" in str(exc):
                    logger.warning(
                        "%s 触发 pandas is_unique 异常, 改用日颗粒兜底 ts_code=%s",
                        api_name,
                        params.get("ts_code"),
                    )
                    df_chunk = None
                    chunks = []
                    offset = 0
                    break
                logger.warning(
                    "%s 调用失败 offset=%s attempt=%s/%s: %s",
                    api_name,
                    offset,
                    attempt,
                    MAX_RETRY,
                    exc,
                )
                time.sleep(sleep_on_fail)
            except Exception as exc:
                logger.warning(
                    "%s 调用失败 offset=%s attempt=%s/%s: %s",
                    api_name,
                    offset,
                    attempt,
                    MAX_RETRY,
                    exc,
                )
                time.sleep(sleep_on_fail)
        if df_chunk is None and chunks == [] and offset == 0:
            break
        if df_chunk is None:
            raise RuntimeError(f"{api_name} 连续 {MAX_RETRY} 次失败")
        chunks.append(df_chunk)
        if len(df_chunk) < limit:
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        offset += limit
        time.sleep(sleep)

    # 第二阶段: 日颗粒兜底
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
        day_params = params.copy()
        day_params.pop("start_date", None)
        day_params.pop("end_date", None)
        day_params["trade_date"] = date.strftime("%Y%m%d")
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_day = getattr(pro, api_name)(
                    **day_params, offset=0, limit=limit, fields=",".join(fields)
                )
                if not df_day.empty:
                    daily_chunks.append(df_day)
                break
            except Exception as exc:
                if attempt == MAX_RETRY:
                    logger.warning(
                        "单日失败 %s trade_date=%s ts_code=%s: %s",
                        api_name,
                        day_params["trade_date"],
                        params.get("ts_code"),
                        exc,
                    )
                time.sleep(sleep_on_fail)
        time.sleep(sleep)
    return (
        pd.concat(daily_chunks, ignore_index=True) if daily_chunks else pd.DataFrame()
    )


# ───────────────────────────────────────────── 核心同步逻辑 ──


def _iter_tables(selected: Optional[Iterable[str]]) -> Iterable[str]:
    if not selected:
        for k in TABLE_CONFIG.keys():
            yield k
        return
    for name in selected:
        if name not in TABLE_CONFIG:
            raise ValueError(f"未知表: {name}")
        yield name


def _sync_one_table(
    pro: ts.pro_api,
    con: sqlite3.Connection,
    table_key: str,
    today: str,
) -> None:
    cfg = TABLE_CONFIG[table_key]
    target_table = table_key
    # 读取该表特定 limit/sleep
    limit = int(cfg.get("limit", DEFAULT_LIMIT))
    sleep = float(cfg.get("sleep", DEFAULT_SLEEP))
    sleep_on_fail = float(cfg.get("sleep_on_fail", cfg.get("sleep", DEFAULT_SLEEP) * 2))
    # -------- 通用获取代码集合逻辑 --------
    explicit_codes: Optional[List[str]] = None
    if "ts_codes" in cfg and cfg["ts_codes"]:
        if table_key == "fx_daily" and RUNTIME_FX_CODES is not None:
            explicit_codes = RUNTIME_FX_CODES
        else:
            explicit_codes = list(cfg["ts_codes"])
    if explicit_codes is not None:
        stock_df = pd.DataFrame({"ts_code": explicit_codes, "name": explicit_codes})
    else:
        stock_table = cfg.get("stock_table")
        if not stock_table:
            raise RuntimeError(f"{table_key} 缺少 ts_codes 或 stock_table 配置")
        if not _table_exists(con, stock_table):
            raise RuntimeError(f"缺少 {stock_table}, 请先运行 tushare_sync_basic.py")
        stock_df = con.execute(f"SELECT ts_code, name FROM {stock_table}").fetchdf()
        if table_key == "fx_daily" and RUNTIME_FX_CODES is not None:
            stock_df = stock_df[stock_df.ts_code.isin(RUNTIME_FX_CODES)]
    total = len(stock_df)
    logger.info("[%s] 股票数=%d limit=%d sleep=%.4f", target_table, total, limit, sleep)
    processed = 0
    con.execute("BEGIN")
    for row in stock_df.itertuples(index=False):
        ts_code: str = row.ts_code
        last_sync = _get_last_sync(con, target_table, ts_code)
        if last_sync == today:
            continue
        latest = _get_latest_date(con, target_table, ts_code)
        start_date = (
            (pd.to_datetime(latest) + timedelta(days=1)).strftime("%Y%m%d")
            if latest
            else START_DATE
        )
        if start_date > today:
            _set_last_sync(con, target_table, ts_code, today)
            continue
        params = {"ts_code": ts_code, "start_date": start_date, "end_date": today}
        df = _fetch_api_with_paging(
            pro,
            cfg["api_name"],
            params,
            cfg["fields"],
            limit=limit,
            sleep=sleep,
            sleep_on_fail=sleep_on_fail,
        )
        wrote_new = False
        if not df.empty:
            df.sort_values("trade_date", inplace=True)
            before_latest = latest
            _upsert(
                con,
                df,
                target_table,
                context_ts=ts_code,
                context_name=getattr(row, "name", None),
            )
            new_latest = _get_latest_date(con, target_table, ts_code)
            wrote_new = new_latest != before_latest
        else:
            logger.debug(
                "[%s] %s 无数据返回 start=%s", target_table, ts_code, start_date
            )
        if wrote_new or start_date < today:
            _set_last_sync(con, target_table, ts_code, today)
        processed += 1
        if processed % 10 == 0 or processed == total:
            logger.info(
                "[%s] 进度 %d/%d (%.1f%%)",
                target_table,
                processed,
                total,
                processed / total * 100,
            )
        time.sleep(sleep)
        if processed % BATCH_SIZE == 0:
            con.execute("COMMIT")
            con.execute("BEGIN")
    con.execute("COMMIT")


# ───────────────────────────────────────────── 顶层入口 ──


def sync(tables: Optional[Iterable[str]] = None) -> None:
    pro = ts.pro_api(get_tushare_token())
    today = datetime.now().strftime("%Y%m%d")
    with _connect() as con:
        for table_key in _iter_tables(tables):
            _sync_one_table(pro, con, table_key, today)
    logger.info("全部完成")


# ───────────────────────────────────────────── CLI ──


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="按数据表增量同步日频数据 (A/H/FX)")
    p.add_argument(
        "-t",
        "--table",
        dest="tables",
        nargs="*",
        help="指定目标表 (可多选)。表: daily_a adj_factor_a bak_daily_a daily_h adj_factor_h fx_daily index_daily index_global (缺省=全部)",
    )
    p.add_argument(
        "--fx-codes",
        dest="fx_codes",
        nargs="*",
        help="过滤外汇代码 (源自 fx_basic)。使用空或 all=保留全部；指定 none=不拉 fx_daily",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    global RUNTIME_FX_CODES
    if args.fx_codes:
        if len(args.fx_codes) == 1 and args.fx_codes[0].lower() == "all":
            RUNTIME_FX_CODES = None  # 使用 fx_basic 全量
        elif len(args.fx_codes) == 1 and args.fx_codes[0].lower() == "none":
            RUNTIME_FX_CODES = []
        else:
            RUNTIME_FX_CODES = args.fx_codes
    sync(args.tables)


if __name__ == "__main__":  # pragma: no cover
    main()
