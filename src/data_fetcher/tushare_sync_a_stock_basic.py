"""
文件: tushare_sync_stock_basic.py
功能: 拉取 A 股列表到 DuckDB，自动增量更新
"""

import logging
import time
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
import tushare as ts

# ----------------------------------------------------------------------
# 读取加密配置（你的 settings.py 里应有 tushare_api_token: SecretStr）
# ----------------------------------------------------------------------
from data_fetcher.settings import settings  # 如果是 data_fecher，请按实际路径改

TS_TOKEN = settings.tushare_api_token.get_secret_value()

# ----------------------------------------------------------------------
# 常量与日志
# ----------------------------------------------------------------------
FIELDS: List[str] = [
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
]
DUCKDB_PATH = Path("data/data.duckdb")
TABLE_NAME = "stock_basic"
LIMIT_PER_CALL = 3000
MAX_RETRY = 3
MAX_CALLS_PER_MIN = 70  # 按你的账号权限自行调
SLEEP_BETWEEN_CALLS = 60 / MAX_CALLS_PER_MIN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# 核心函数
# ----------------------------------------------------------------------
def fetch_stock_basic_batches(token: str, limit: int = 3000) -> pd.DataFrame:
    """分页拉取所有 A 股列表，返回拼接后的 DataFrame。"""
    pro = ts.pro_api(token)
    offset = 0
    chunks: List[pd.DataFrame] = []

    while True:
        df_chunk: Optional[pd.DataFrame] = None
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_chunk = pro.stock_basic(
                    ts_code="",
                    offset=offset,
                    limit=limit,
                    fields=",".join(FIELDS),
                )
                break
            except Exception as e:
                logger.warning(
                    "Tushare 调用失败 (offset=%s, attempt=%s/%s): %s",
                    offset,
                    attempt,
                    MAX_RETRY,
                    e,
                )
                time.sleep(SLEEP_BETWEEN_CALLS * 2)

        if df_chunk is None:
            raise RuntimeError(f"连续 {MAX_RETRY} 次失败，终止。offset={offset}")

        row_cnt = len(df_chunk)
        logger.info("拉取 %s 行 (offset=%s)", row_cnt, offset)
        chunks.append(df_chunk)

        if row_cnt < limit:  # 最后一批
            break

        offset += limit
        time.sleep(SLEEP_BETWEEN_CALLS)

    return pd.concat(chunks, ignore_index=True)


def upsert_to_duckdb(df: pd.DataFrame, db_path: Path, table: str) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    # --- 首次建表 ---
    if table not in {row[0] for row in con.execute("SHOW TABLES").fetchall()}:
        logger.info("首次创建表 %s，写入全部 %s 行。", table, len(df))
        con.register("df_view", df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_view")
    else:
        # --- 增量插入 ---
        existing_codes = (
            con.execute(f"SELECT ts_code FROM {table}").fetchdf().ts_code.tolist()
        )
        new_rows = df[~df["ts_code"].isin(existing_codes)]
        if new_rows.empty:
            logger.info("无新增股票，数据库已是最新。")
        else:
            logger.info("检测到 %s 条新增股票，执行插入。", len(new_rows))
            con.register("new_rows_view", new_rows)
            con.execute(f"INSERT INTO {table} SELECT * FROM new_rows_view")

    # --- 无论走哪条路径，最后都统计行数并返回 ---
    row_cnt: int = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    return row_cnt


def data_sync() -> None:
    df_all = fetch_stock_basic_batches(TS_TOKEN, limit=LIMIT_PER_CALL)
    row_cnt = upsert_to_duckdb(df_all, DUCKDB_PATH, TABLE_NAME)
    logger.info("同步完成！当前数据库行数: %s", row_cnt)


if __name__ == "__main__":
    data_sync()
