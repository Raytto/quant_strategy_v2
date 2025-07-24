"""
文件: tushare_sync_h_stock_basic.py
功能: 拉取 H 股列表到 DuckDB，自动增量更新
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
from data_fetcher.settings import settings  # 如果是 data_fetcher，请按实际路径改

TS_TOKEN = settings.tushare_api_token.get_secret_value()

# ----------------------------------------------------------------------
# 常量与日志
# ----------------------------------------------------------------------
FIELDS: List[str] = [
    "ts_code",  # 股票唯一代码，形如 00001.HK
    "name",  # 简称
    "fullname",  # 中文全称
    "enname",  # 英文全称
    "cn_spell",  # 中文拼音缩写
    "market",  # 上市板（主板 / 创业板等）
    "list_status",  # 上市状态 L=上市, D=退市, P=暂停上市
    "list_date",  # 上市日期
    "delist_date",  # 退市日期
    "trade_unit",  # 交易单位
    "isin",  # ISIN 代码
    "curr_type",  # 计价货币
]

DUCKDB_PATH = Path("data/data.duckdb")
TABLE_NAME = "h_stock_basic"
LIMIT_PER_CALL = 3000
MAX_RETRY = 3
MAX_CALLS_PER_MIN = 100  # 按账号权限自行调整
SLEEP_BETWEEN_CALLS = 60 / MAX_CALLS_PER_MIN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# 核心函数
# ----------------------------------------------------------------------


def fetch_hk_basic_batches(token: str, limit: int = 3000) -> pd.DataFrame:
    """分页拉取所有 H 股列表，返回拼接后的 DataFrame。"""
    pro = ts.pro_api(token)
    offset = 0
    chunks: List[pd.DataFrame] = []

    while True:
        df_chunk: Optional[pd.DataFrame] = None
        for attempt in range(1, MAX_RETRY + 1):
            try:
                df_chunk = pro.hk_basic(
                    ts_code="",  # 不限制代码，拉全量
                    list_status="",  # 不限制状态
                    offset=offset,
                    limit=limit,
                    fields=",".join(FIELDS),
                )
                break
            except Exception as e:
                logger.warning(
                    "Tushare hk_basic 调用失败 (offset=%s, attempt=%s/%s): %s",
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
    """向 DuckDB 中 upsert 数据，按 ts_code 去重。返回最终行数。"""
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
            logger.info("无新增 H 股，数据库已是最新。")
        else:
            logger.info("检测到 %s 条新增 H 股，执行插入。", len(new_rows))
            con.register("new_rows_view", new_rows)
            con.execute(f"INSERT INTO {table} SELECT * FROM new_rows_view")

    # --- 统计行数并返回 ---
    row_cnt: int = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    return row_cnt


def data_sync() -> None:
    """主入口：拉取并 upsert 数据。"""
    df_all = fetch_hk_basic_batches(TS_TOKEN, limit=LIMIT_PER_CALL)
    row_cnt = upsert_to_duckdb(df_all, DUCKDB_PATH, TABLE_NAME)
    logger.info("同步完成！当前 H 股记录行数: %s", row_cnt)


if __name__ == "__main__":
    data_sync()
