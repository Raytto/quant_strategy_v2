"""
文件: tushare_sync_basic.py
功能: 可配置地同步 A / H 股、ETF（及 FX 外汇基础）列表至 SQLite，并在导入时保证 INFO 日志可见。
新增: fx_basic (TuShare 接口 fx_obasic, doc_id=178) 动态外汇代码池供 fx_daily 使用。
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import tushare as ts

from qs.sqlite_utils import connect_sqlite, ensure_unique_index, insert_df_ignore, table_exists

# ----------------------------------------------------------------------
# 日志：若根 logger 尚未配置，立即设置成 INFO，确保被 import 调用时也能打印
# ----------------------------------------------------------------------
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ----------------------------------------------------------------------
# 读取配置
# ----------------------------------------------------------------------
from data_fetcher.settings import get_tushare_token

# ----------------------------------------------------------------------
# 通用常量
# ----------------------------------------------------------------------
SQLITE_PATH = Path("data/data.sqlite")
LIMIT = 3000
MAX_RETRY = 3
SLEEP = 0.6  # 等价于每分钟 ~100 次调用

# ----------------------------------------------------------------------
# 市场配置
# ----------------------------------------------------------------------
MARKET_CONFIG: Dict[str, Dict[str, Any]] = {
    "A": {
        "api_name": "stock_basic",
        "fields": [
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "cnspell",
            "market",
            "list_date",
            "act_name",
            "act_ent_type",
            "fullname",
            "enname",
            "exchange",
            "curr_type",
            "list_status",
            "delist_date",
            "is_hs",
        ],
        "params": {"ts_code": "", "list_status": ""},
        "table": "stock_basic_a",
    },
    "H": {
        "api_name": "hk_basic",
        "fields": [
            "ts_code",
            "name",
            "fullname",
            "enname",
            "cn_spell",
            "market",
            "list_status",
            "list_date",
            "delist_date",
            "trade_unit",
            "isin",
            "curr_type",
        ],
        "params": {"ts_code": "", "list_status": ""},
        "table": "stock_basic_h",
    },
    "FX": {  # 新增外汇基础 (doc_id=178)
        "api_name": "fx_obasic",
        "fields": [
            "ts_code",  # 货币对代码 e.g. USDCNH.FXCM
            "name",  # 简称
            "classify",  # 分类 (直盘/交叉 等)
            "exchange",  # 交易渠道
            "min_unit",  # 最小交易单位 (示例字段, 官方: min_unit)
            "max_unit",  # 最大交易单位
            "pip",  # 点
            "pip_cost",  # 点值
            "traget_spread",  # 官方字段拼写(文档示例可能有拼写, 保留)
            "min_stop_distance",
            "trading_hours",
            "break_time",
        ],
        "params": {"exchange": "", "classify": "", "ts_code": ""},
        "table": "fx_basic",
    },
    "ETF": {
        "api_name": "etf_basic",
        "fields": [
            "ts_code",
            "csname",
            "extname",
            "cname",
            "index_code",
            "index_name",
            "setup_date",
            "list_date",
            "list_status",
            "exchange",
            "mgr_name",
            "custod_name",
            "mgt_fee",
            "etf_type",
        ],
        "params": {
            "ts_code": "",
            "index_code": "",
            "list_date": "",
            "list_status": "",
            "exchange": "",
            "mgr": "",
        },
        "table": "etf_basic",
    },
}

# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------


def _fetch_table(
    pro: ts.pro_api,
    api_name: str,
    params: Dict[str, Any],
    fields: List[str],
) -> pd.DataFrame:
    """分页拉取指定表并拼接返回。"""

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
                    "%s 调用失败 offset=%s attempt=%s/%s: %s",
                    api_name,
                    offset,
                    attempt,
                    MAX_RETRY,
                    exc,
                )
                time.sleep(SLEEP * 2)

        if df_chunk is None:
            raise RuntimeError(f"连续 {MAX_RETRY} 次失败，终止。offset={offset}")

        chunks.append(df_chunk)
        logging.info("[%s] 拉取 %s 行 offset=%s", api_name, len(df_chunk), offset)

        if len(df_chunk) < LIMIT:
            all_df = pd.concat(chunks, ignore_index=True)
            if "ts_code" in all_df.columns:
                all_df = all_df.drop_duplicates(subset=["ts_code"])
            return all_df

        offset += LIMIT
        time.sleep(SLEEP)


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    return table_exists(con, table)


def _upsert(df: pd.DataFrame, table: str) -> int:
    con = connect_sqlite(SQLITE_PATH)
    try:
        if not table_exists(con, table):
            df.head(0).to_sql(table, con, if_exists="fail", index=False)
            if "ts_code" in df.columns:
                ensure_unique_index(
                    con, table=table, columns=["ts_code"], index_name=f"{table}_uq"
                )
        inserted = insert_df_ignore(
            con,
            df=df,
            table=table,
            unique_by=["ts_code"] if "ts_code" in df.columns else None,
        )
        if inserted == 0:
            logging.info("%s 已最新 无新增", table)
        else:
            logging.info("%s 插入 %s 行", table, inserted)
        cnt = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        con.commit()
    finally:
        con.close()
    return cnt


# ----------------------------------------------------------------------
# 顶层同步入口
# ----------------------------------------------------------------------


def data_sync() -> None:
    pro = ts.pro_api(get_tushare_token())
    for name, cfg in MARKET_CONFIG.items():
        df = _fetch_table(pro, cfg["api_name"], cfg["params"], cfg["fields"])
        cnt = _upsert(df, cfg["table"])
        logging.info("[%s] 同步完成 当前行数=%s", name, cnt)


# 若希望直接作为脚本运行，可保留以下守护；被 import 时不会影响
if __name__ == "__main__":
    data_sync()
