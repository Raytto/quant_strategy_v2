"""
文件: tushare_sync_daily.py
功能:
  增量同步多张日频数据表到 SQLite (逐表独立增量, 不互相影响):
    - daily_a        (A 股日线)
    - adj_factor_a   (A 股复权因子)
    - bak_daily_a    (A 股特色扩展行情)
    - daily_h        (港股日线)
    - adj_factor_h   (港股复权因子)
    - etf_daily      (ETF 日线行情: fund_daily)
    - adj_factor_etf (ETF 复权因子: fund_adj)
    - index_daily_etf(ETF 对应指数日线: index_daily, 代码来源 etf_basic.index_code)
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
# Tushare Pro `limit` is effectively capped (commonly 2000). When our configured
# limit exceeds the API cap, the first page may return <limit rows even though
# more pages exist; pagination must use the effective page size.
TS_API_MAX_PAGE_SIZE = 2000

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
    "etf_daily": {  # ETF 日线行情 (代码池: etf_basic)
        "api_name": "fund_daily",
        "fields": [
            "ts_code",
            "trade_date",
            "pre_close",
            "open",
            "high",
            "low",
            "close",
            "change",
            "pct_chg",
            "vol",
            "amount",
        ],
        "stock_table": "etf_basic",
        "name_column": "csname",
        # etf_basic often contains duplicated `.OF` rows for the same 6-digit code.
        # fund_daily/fund_adj data we use are exchange-listed; prefer those.
        "ts_code_suffix_whitelist": [".SZ", ".SH", ".BJ"],
        # Skip pending/delisted products to avoid empty backfills.
        "require_list_status": ["L"],
        "limit": 6000,
    },
    "adj_factor_etf": {  # ETF 复权因子 (代码池: etf_basic)
        "api_name": "fund_adj",
        "fields": ["ts_code", "trade_date", "adj_factor", "discount_rate"],
        "stock_table": "etf_basic",
        "name_column": "csname",
        "ts_code_suffix_whitelist": [".SZ", ".SH", ".BJ"],
        "require_list_status": ["L"],
        "limit": 6000,
        "write_mode": "upsert",
        "update_columns": ["adj_factor", "discount_rate"],
        # 新增字段回填：当表结构缺少这些列时，按“已有最早日期~today”回拉一次以补齐列
        "backfill_if_missing_columns": ["discount_rate"],
    },
    "index_daily_etf": {  # ETF 对应指数日线 (代码池: etf_basic.index_code)
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
        "stock_table": "etf_basic",
        "code_column": "index_code",
        "name_column": "index_name",
        "limit": 6000,
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


def _get_table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    rows = con.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [r[1] for r in rows]  # (cid, name, type, notnull, dflt_value, pk)


def _sqlite_type_for_series(s: "pd.Series") -> str:
    if pd.api.types.is_bool_dtype(s):
        return "INTEGER"
    if pd.api.types.is_integer_dtype(s):
        return "INTEGER"
    if pd.api.types.is_float_dtype(s):
        return "REAL"
    return "TEXT"


def _ensure_table_has_columns(con: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    if df.empty or not _table_exists(con, table):
        return
    existing = set(_get_table_columns(con, table))
    for col in df.columns:
        if col in existing:
            continue
        col_type = _sqlite_type_for_series(df[col])
        con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {col_type}')
        existing.add(col)


def _get_latest_date(
    con: sqlite3.Connection, table: str, ts_code: str
) -> Optional[str]:
    if not _table_exists(con, table):
        return None
    row = con.execute(
        f"SELECT MAX(trade_date) FROM {table} WHERE ts_code=?", [ts_code]
    ).fetchone()
    return row[0] if row else None


def _get_earliest_date(
    con: sqlite3.Connection, table: str, ts_code: str
) -> Optional[str]:
    if not _table_exists(con, table):
        return None
    row = con.execute(
        f"SELECT MIN(trade_date) FROM {table} WHERE ts_code=?", [ts_code]
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
    *,
    write_mode: str = "ignore",
    update_columns: Optional[List[str]] = None,
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

    if write_mode not in {"ignore", "upsert"}:
        raise ValueError(f"未知 write_mode: {write_mode}")

    def _do_upsert() -> int:
        if df.empty:
            return 0
        _ensure_table_has_columns(con, table, df)
        cols = list(df.columns)
        quoted_cols = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["?"] * len(cols))
        conflict_cols = ["ts_code", "trade_date"]
        if not update_columns:
            update_cols = [c for c in cols if c not in conflict_cols]
        else:
            update_cols = [c for c in update_columns if c in cols]
        set_sql = ", ".join([f'"{c}"=excluded."{c}"' for c in update_cols])
        sql = (
            f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders}) '
            f'ON CONFLICT("ts_code","trade_date") DO UPDATE SET {set_sql}'
        )
        work = df.copy()
        work = work.drop_duplicates(subset=conflict_cols)
        before = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        records = (
            work.where(pd.notnull(work), None).to_records(index=False).tolist()
        )
        con.executemany(sql, records)
        after = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        return int(after - before)

    if not _table_exists(con, table):
        df.head(0).to_sql(table, con, if_exists="fail", index=False)
        if "ts_code" in df.columns and "trade_date" in df.columns:
            ensure_unique_index(
                con,
                table=table,
                columns=["ts_code", "trade_date"],
                index_name=f"{table}_uq",
            )
        inserted = (
            _do_upsert()
            if write_mode == "upsert"
            else insert_df_ignore(
                con,
                df=df,
                table=table,
                unique_by=["ts_code", "trade_date"]
                if "ts_code" in df.columns and "trade_date" in df.columns
                else None,
            )
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
        inserted = (
            _do_upsert()
            if write_mode == "upsert"
            else insert_df_ignore(
                con,
                df=df,
                table=table,
                unique_by=["ts_code", "trade_date"]
                if "ts_code" in df.columns and "trade_date" in df.columns
                else None,
            )
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
    page_limit = min(int(limit), TS_API_MAX_PAGE_SIZE)
    offset, chunks = 0, []
    while True:
        df_chunk = None
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_chunk = getattr(pro, api_name)(
                    **params,
                    offset=offset,
                    limit=page_limit,
                    fields=",".join(fields),
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
        if len(df_chunk) < page_limit:
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        offset += page_limit
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
                    **day_params,
                    offset=0,
                    limit=page_limit,
                    fields=",".join(fields),
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


def _normalize_yyyymmdd(value: object | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) != 8 or not s.isdigit():
        return None
    return s


def _max_yyyymmdd(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if a >= b else b


def _sync_one_table(
    pro: ts.pro_api,
    con: sqlite3.Connection,
    table_key: str,
    today: str,
    *,
    ts_codes_filter: Optional[set[str]] = None,
    rebuild: bool = False,
    backfill_history: bool = False,
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
        code_column = str(cfg.get("code_column") or "ts_code").strip()
        preferred_name_col = str(cfg.get("name_column") or "").strip() or None
        cols = set(_get_table_columns(con, stock_table))
        has_list_date = "list_date" in cols
        has_setup_date = "setup_date" in cols
        has_list_status = "list_status" in cols
        if code_column not in cols:
            raise RuntimeError(
                f"{table_key} 需要代码列 {stock_table}.{code_column}, "
                "但未找到。请先更新/重建基础表并重新同步。"
            )
        name_col = None
        if preferred_name_col and preferred_name_col in cols:
            name_col = preferred_name_col
        else:
            for c in ("name", "csname", "cname", "fullname", "enname"):
                if c in cols:
                    name_col = c
                    break
        date_select_parts: list[str] = []
        if has_list_date:
            date_select_parts.append('MIN("list_date") AS list_date')
        if has_setup_date:
            date_select_parts.append('MIN("setup_date") AS setup_date')
        if has_list_status:
            date_select_parts.append('MIN("list_status") AS list_status')
        date_select = (", " + ", ".join(date_select_parts)) if date_select_parts else ""
        if code_column == "ts_code":
            if name_col:
                stock_df = pd.read_sql_query(
                    f"""
                    SELECT ts_code, MIN("{name_col}") AS name{date_select}
                    FROM "{stock_table}"
                    WHERE ts_code IS NOT NULL AND TRIM(ts_code) <> ''
                    GROUP BY ts_code
                    """,
                    con,
                )
            else:
                stock_df = pd.read_sql_query(
                    f"""
                    SELECT ts_code, MIN(ts_code) AS name{date_select}
                    FROM "{stock_table}"
                    WHERE ts_code IS NOT NULL AND TRIM(ts_code) <> ''
                    GROUP BY ts_code
                    """,
                    con,
                )
        else:
            if name_col:
                stock_df = pd.read_sql_query(
                    f"""
                    SELECT "{code_column}" AS ts_code, MIN("{name_col}") AS name{date_select}
                    FROM "{stock_table}"
                    WHERE "{code_column}" IS NOT NULL AND TRIM("{code_column}") <> ''
                    GROUP BY "{code_column}"
                    """,
                    con,
                )
            else:
                stock_df = pd.read_sql_query(
                    f"""
                    SELECT "{code_column}" AS ts_code, MIN("{code_column}") AS name{date_select}
                    FROM "{stock_table}"
                    WHERE "{code_column}" IS NOT NULL AND TRIM("{code_column}") <> ''
                    GROUP BY "{code_column}"
                    """,
                    con,
                )
        stock_df = stock_df.dropna(subset=["ts_code"])
        stock_df = stock_df[stock_df.ts_code.astype(str).str.strip() != ""]
        if table_key == "fx_daily" and RUNTIME_FX_CODES is not None:
            stock_df = stock_df[stock_df.ts_code.isin(RUNTIME_FX_CODES)]

    suffix_whitelist = cfg.get("ts_code_suffix_whitelist")
    if suffix_whitelist:
        suffixes = tuple(str(s) for s in suffix_whitelist)
        stock_df = stock_df[stock_df.ts_code.astype(str).str.endswith(suffixes)]

    require_list_status = cfg.get("require_list_status")
    if require_list_status and "list_status" in stock_df.columns:
        allowed = {str(s) for s in require_list_status}
        stock_df = stock_df[stock_df["list_status"].astype(str).isin(allowed)]
    if ts_codes_filter:
        stock_df = stock_df[stock_df.ts_code.isin(ts_codes_filter)]
    total = len(stock_df)
    logger.info("[%s] 股票数=%d limit=%d sleep=%.4f", target_table, total, limit, sleep)
    processed = 0
    backfill_cols = list(cfg.get("backfill_if_missing_columns") or [])
    force_backfill = False
    if backfill_cols and _table_exists(con, target_table):
        existing_cols = set(_get_table_columns(con, target_table))
        force_backfill = any(c not in existing_cols for c in backfill_cols)
    con.execute("BEGIN")
    for row in stock_df.itertuples(index=False):
        ts_code: str = row.ts_code
        try:
            row_list_date = _normalize_yyyymmdd(getattr(row, "list_date", None))
            row_setup_date = _normalize_yyyymmdd(getattr(row, "setup_date", None))
            row_min_date = row_list_date or row_setup_date
            # Prefer per-symbol listing date (avoid wasting calls before listing).
            desired_min_date = _max_yyyymmdd(START_DATE, row_min_date)
            if rebuild:
                con.execute(f'DELETE FROM "{target_table}" WHERE ts_code=?', [ts_code])
                con.execute(
                    "DELETE FROM sync_date WHERE table_name=? AND ts_code=?",
                    [target_table, ts_code],
                )
            last_sync = _get_last_sync(con, target_table, ts_code)
            earliest = _get_earliest_date(con, target_table, ts_code)
            want_backfill = (
                backfill_history
                and not rebuild
                and desired_min_date is not None
                and earliest is not None
                and desired_min_date < str(earliest)
            )
            if (
                last_sync == today
                and not force_backfill
                and not rebuild
                and not want_backfill
            ):
                continue
            latest = _get_latest_date(con, target_table, ts_code)
            start_date = (
                (pd.to_datetime(latest) + timedelta(days=1)).strftime("%Y%m%d")
                if latest
                else (desired_min_date or START_DATE)
            )
            # 新增字段回填：如果目标表缺列，回拉“已有最早日期~today”补齐
            if force_backfill:
                if earliest:
                    start_date = earliest
                else:
                    start_date = desired_min_date or START_DATE
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
                    write_mode=str(cfg.get("write_mode") or "ignore"),
                    update_columns=list(cfg.get("update_columns") or []) or None,
                )
                new_latest = _get_latest_date(con, target_table, ts_code)
                wrote_new = new_latest != before_latest
            else:
                logger.debug(
                    "[%s] %s 无数据返回 start=%s", target_table, ts_code, start_date
                )

            # 历史补齐：若本地最早 trade_date 晚于 desired_min_date，则回拉缺失区间
            if want_backfill and desired_min_date is not None:
                earliest_now = _get_earliest_date(con, target_table, ts_code)
                if earliest_now is not None and desired_min_date < str(earliest_now):
                    backfill_end = (
                        pd.to_datetime(str(earliest_now)) - timedelta(days=1)
                    ).strftime("%Y%m%d")
                    if desired_min_date <= backfill_end:
                        logger.info(
                            "[%s] %s backfill %s~%s",
                            target_table,
                            ts_code,
                            desired_min_date,
                            backfill_end,
                        )
                        backfill_params = {
                            "ts_code": ts_code,
                            "start_date": desired_min_date,
                            "end_date": backfill_end,
                        }
                        df_old = _fetch_api_with_paging(
                            pro,
                            cfg["api_name"],
                            backfill_params,
                            cfg["fields"],
                            limit=limit,
                            sleep=sleep,
                            sleep_on_fail=sleep_on_fail,
                        )
                        if not df_old.empty:
                            df_old.sort_values("trade_date", inplace=True)
                            _upsert(
                                con,
                                df_old,
                                target_table,
                                context_ts=ts_code,
                                context_name=getattr(row, "name", None),
                                write_mode=str(cfg.get("write_mode") or "ignore"),
                                update_columns=list(cfg.get("update_columns") or []) or None,
                            )
            if wrote_new or start_date < today:
                _set_last_sync(con, target_table, ts_code, today)
        except Exception as exc:
            logger.warning("[%s] %s 同步失败: %s", target_table, ts_code, exc, exc_info=True)
            con.execute("ROLLBACK")
            con.execute("BEGIN")
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


def sync(
    tables: Optional[Iterable[str]] = None,
    *,
    ts_codes: Optional[Iterable[str]] = None,
    rebuild: bool = False,
    backfill_history: bool = False,
) -> None:
    pro = ts.pro_api(get_tushare_token())
    today = datetime.now().strftime("%Y%m%d")
    ts_filter = set(ts_codes) if ts_codes else None
    if rebuild and not ts_filter:
        raise ValueError("rebuild=True requires ts_codes")
    with _connect() as con:
        for table_key in _iter_tables(tables):
            _sync_one_table(
                pro,
                con,
                table_key,
                today,
                ts_codes_filter=ts_filter,
                rebuild=bool(rebuild),
                backfill_history=bool(backfill_history),
            )
    logger.info("全部完成")


# ───────────────────────────────────────────── CLI ──


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="按数据表增量同步日频数据 (A/H/FX)")
    p.add_argument(
        "-t",
        "--table",
        dest="tables",
        nargs="*",
        help="指定目标表 (可多选)。表: daily_a adj_factor_a bak_daily_a daily_h adj_factor_h etf_daily adj_factor_etf index_daily_etf fx_daily index_daily index_global (缺省=全部)",
    )
    p.add_argument(
        "--fx-codes",
        dest="fx_codes",
        nargs="*",
        help="过滤外汇代码 (源自 fx_basic)。使用空或 all=保留全部；指定 none=不拉 fx_daily",
    )
    p.add_argument(
        "--ts-codes",
        dest="ts_codes",
        nargs="*",
        help="过滤 ts_code（对 etf_daily/adj_factor_etf 等有效）。不指定=使用基础表全量。",
    )
    p.add_argument(
        "--rebuild",
        dest="rebuild",
        action="store_true",
        help="重建指定 ts_codes 的该表数据：先 DELETE 再全量回拉（需配合 --ts-codes）。",
    )
    p.add_argument(
        "--backfill",
        dest="backfill",
        action="store_true",
        help="补齐历史数据：若本地最早 trade_date 晚于 list_date/START_DATE，则回拉缺失的更早区间。",
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
    if args.rebuild and not args.ts_codes:
        raise SystemExit("--rebuild requires --ts-codes")
    sync(
        args.tables,
        ts_codes=args.ts_codes,
        rebuild=bool(args.rebuild),
        backfill_history=bool(args.backfill),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
