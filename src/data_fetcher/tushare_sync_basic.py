"""
文件: tushare_sync_basic.py
功能: 可配置地同步 A / H 股（及扩展市场）列表至 DuckDB，并在导入时就保证 INFO 日志可见。
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd
import tushare as ts

# ----------------------------------------------------------------------
# 日志：若根 logger 尚未配置，立即设置成 INFO，确保被 import 调用时也能打印
# ----------------------------------------------------------------------
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ----------------------------------------------------------------------
# 读取加密配置（settings.tushare_api_token: SecretStr）
# ----------------------------------------------------------------------
from data_fetcher.settings import settings

TS_TOKEN = settings.tushare_api_token.get_secret_value()

# ----------------------------------------------------------------------
# 通用常量
# ----------------------------------------------------------------------
DUCKDB_PATH = Path("data/data.duckdb")
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
            return pd.concat(chunks, ignore_index=True)

        offset += LIMIT
        time.sleep(SLEEP)


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return table in {row[0] for row in con.execute("SHOW TABLES").fetchall()}


def _upsert(df: pd.DataFrame, table: str) -> int:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))

    if not _table_exists(con, table):
        con.register("df_view", df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_view")
        logging.info("新建表 %s，写入 %s 行", table, len(df))
    else:
        existing = (
            con.execute(f"SELECT ts_code FROM {table}").fetchdf().ts_code.unique()
        )
        new_rows = df[~df.ts_code.isin(existing)]
        if new_rows.empty:
            logging.info("%s 已是最新，无新增", table)
        else:
            con.register("new_rows", new_rows)
            con.execute(f"INSERT INTO {table} SELECT * FROM new_rows")
            logging.info("%s 插入 %s 行", table, len(new_rows))

    row_cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    return row_cnt


# ----------------------------------------------------------------------
# 顶层同步入口
# ----------------------------------------------------------------------


def data_sync() -> None:
    pro = ts.pro_api(TS_TOKEN)
    for name, cfg in MARKET_CONFIG.items():
        df = _fetch_table(pro, cfg["api_name"], cfg["params"], cfg["fields"])
        cnt = _upsert(df, cfg["table"])
        logging.info("[%s] 同步完成，当前行数=%s", name, cnt)


# 若希望直接作为脚本运行，可保留以下守护；被 import 时不会影响
if __name__ == "__main__":
    data_sync()
