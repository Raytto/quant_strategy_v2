from __future__ import annotations

"""ah_premium.py

功能:
  计算所有 A/H 双重上市股票(依据 data/ah_codes.csv) 历史每日 A/H 溢价:
    - 使用 A 股收盘价 (RMB)
    - 使用 H 股收盘价 (HKD) 并通过 USD/CNH 与 USD/HKD 两条汇率链折算为 RMB
    - 溢价 = A 股收盘价 / (H 股收盘价 * HKD→CNY) - 1
  数据来源:
    - 源行情库: data/data.duckdb  (表: daily_a, daily_h, fx_daily)
    - 对照映射: data/ah_codes.csv (列: name, cn_code, hk_code)
    - 汇率: fx_daily 中 ts_code IN ('USDCNH.FXCM','USDHKD.FXCM') 取 (bid_close+ask_close)/2 作为当日美元中间价
  输出:
    - 目标库: data/data_processed.duckdb
    - 表: ah_premium (全量重建, 可用 --append 选择增量追加未来日期)

表结构 (字段含义):
  trade_date    TEXT  YYYYMMDD
  name          TEXT  公司简称(来自映射)
  cn_code       TEXT  A 股 ts_code
  hk_code       TEXT  H 股 ts_code
  close_a       DOUBLE A 股收盘价 (未复权, RMB)
  close_h_hkd   DOUBLE H 股收盘价 (HKD)
  usd_cnh_mid   DOUBLE USD/CNH 中间价 (≈1 USD 兑多少 CNH)
  usd_hkd_mid   DOUBLE USD/HKD 中间价 (≈1 USD 兑多少 HKD)
  hk_to_cny     DOUBLE 1 HKD 兑多少 CNH = usd_cnh_mid / usd_hkd_mid
  close_h_cny   DOUBLE H 股折算人民币价格 = close_h_hkd * hk_to_cny
  premium_ratio DOUBLE A/H 价比 = close_a / close_h_cny
  premium_pct   DOUBLE 溢价百分比 = (premium_ratio - 1) * 100

缺失处理:
  任一所需价格/汇率缺失的交易日行被跳过 (不插入记录, 不做填补)。

用法:
  python -m data_processor.ah_premium            # 全量重建
  python -m data_processor.ah_premium --append   # 在已存在表基础上仅为缺失日期(新日期)追加
  可选参数见 --help。

注意:
  - 默认全量重建更简单可靠; 如需高频更新可用 --append, 但若源数据回溯修订需执行 --rebuild。
  - 可扩展加入复权价/复权因子、或使用直接 HKD/CNY 汇率(若后续引入)。
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import duckdb  # type: ignore

# 默认路径
SRC_DB_PATH = Path("data/data.duckdb")
OUT_DB_PATH = Path("data/data_processed.duckdb")
AH_CODES_CSV = Path("data/ah_codes.csv")

USDCNH_CODE = "USDCNH.FXCM"
USDHKD_CODE = "USDHKD.FXCM"

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )


def build_sql(existing_max_date: Optional[str] = None) -> str:
    """生成计算 SQL。若 existing_max_date 提供且使用追加模式, 仅计算 existing_max_date 之后日期。"""
    date_filter = ""
    if existing_max_date:
        # 只处理大于已存在最大日期的新日期
        date_filter = f"WHERE a.trade_date > '{existing_max_date}'"
    return f"""
WITH mapping AS (
  SELECT * FROM read_csv_auto('{AH_CODES_CSV.as_posix()}')
),
-- A 股收盘
A AS (
  SELECT ts_code, trade_date, close FROM daily_a
),
-- H 股收盘
H AS (
  SELECT ts_code, trade_date, close FROM daily_h
),
-- USD/CNH 中间价
R_CNH AS (
  SELECT trade_date, (bid_close + ask_close)/2.0 AS usd_cnh_mid
  FROM fx_daily WHERE ts_code = '{USDCNH_CODE}'
),
-- USD/HKD 中间价
R_HKD AS (
  SELECT trade_date, (bid_close + ask_close)/2.0 AS usd_hkd_mid
  FROM fx_daily WHERE ts_code = '{USDHKD_CODE}'
),
JOINED AS (
  SELECT
    a.trade_date,
    m.name,
    m.cn_code,
    m.hk_code,
    a.close  AS close_a,
    h.close  AS close_h_hkd,
    r1.usd_cnh_mid,
    r2.usd_hkd_mid,
    r1.usd_cnh_mid / r2.usd_hkd_mid AS hk_to_cny,
    h.close * (r1.usd_cnh_mid / r2.usd_hkd_mid) AS close_h_cny,
    a.close / NULLIF(h.close * (r1.usd_cnh_mid / r2.usd_hkd_mid), 0) AS premium_ratio,
    (a.close / NULLIF(h.close * (r1.usd_cnh_mid / r2.usd_hkd_mid), 0) - 1) * 100 AS premium_pct
  FROM mapping m
  JOIN A a ON a.ts_code = m.cn_code
  JOIN H h ON h.ts_code = m.hk_code AND h.trade_date = a.trade_date
  JOIN R_CNH r1 ON r1.trade_date = a.trade_date
  JOIN R_HKD r2 ON r2.trade_date = a.trade_date
  {date_filter}
)
SELECT * FROM JOINED
ORDER BY trade_date, cn_code
"""


def ensure_source_objects(con: duckdb.DuckDBPyConnection) -> None:
    required = ["daily_a", "daily_h", "fx_daily"]
    missing: list[str] = []
    for t in required:
        try:
            con.execute(f"SELECT 1 FROM src.main.{t} LIMIT 1")
        except Exception:
            missing.append(t)
    if missing:
        raise RuntimeError(
            f"源库缺少必需表: {missing}. 请先运行 tushare_sync_daily.py 同步相关表。"
        )


def rebuild(output_db: Path, source_db: Path) -> None:
    logger.info("全量重建 ah_premium (源=%s 输出=%s)", source_db, output_db)
    output_db.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(output_db)) as out_con:
        out_con.execute(f"ATTACH DATABASE '{source_db.as_posix()}' AS src (READ_ONLY)")
        ensure_source_objects(out_con)
        # 创建引用视图 (引用附加库 main schema 中的表)
        out_con.execute(
            "CREATE OR REPLACE VIEW daily_a AS SELECT * FROM src.main.daily_a"
        )
        out_con.execute(
            "CREATE OR REPLACE VIEW daily_h AS SELECT * FROM src.main.daily_h"
        )
        out_con.execute(
            "CREATE OR REPLACE VIEW fx_daily AS SELECT * FROM src.main.fx_daily"
        )
        sql = build_sql()
        out_con.execute("DROP TABLE IF EXISTS ah_premium")
        logger.info("执行计算 SQL ...")
        out_con.execute(f"CREATE TABLE ah_premium AS {sql}")
        out_con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ah_premium_uq ON ah_premium(cn_code, trade_date)"
        )
        cnt = out_con.execute("SELECT COUNT(*) FROM ah_premium").fetchone()[0]
        logger.info("完成: 写入 %d 行", cnt)


def append_new(output_db: Path, source_db: Path) -> None:
    if not output_db.exists():
        logger.warning("输出库不存在, 自动切换为全量重建")
        rebuild(output_db, source_db)
        return
    logger.info("追加模式: 仅计算新日期 (源=%s 输出=%s)", source_db, output_db)
    with duckdb.connect(str(output_db)) as out_con:
        out_con.execute(f"ATTACH DATABASE '{source_db.as_posix()}' AS src (READ_ONLY)")
        ensure_source_objects(out_con)
        out_con.execute(
            "CREATE OR REPLACE VIEW daily_a AS SELECT * FROM src.main.daily_a"
        )
        out_con.execute(
            "CREATE OR REPLACE VIEW daily_h AS SELECT * FROM src.main.daily_h"
        )
        out_con.execute(
            "CREATE OR REPLACE VIEW fx_daily AS SELECT * FROM src.main.fx_daily"
        )
        if "ah_premium" not in {
            r[0] for r in out_con.execute("SHOW TABLES").fetchall()
        }:
            logger.info("目标表不存在, 切换为全量重建")
            rebuild(output_db, source_db)
            return
        max_date = out_con.execute("SELECT MAX(trade_date) FROM ah_premium").fetchone()[
            0
        ]
        if max_date is None:
            logger.info("目标表为空, 切换为全量重建")
            rebuild(output_db, source_db)
            return
        sql = build_sql(existing_max_date=max_date)
        logger.info("新日期计算 SQL ... ( > %s )", max_date)
        out_con.execute("CREATE OR REPLACE TEMP VIEW new_rows AS " + sql)
        new_cnt = out_con.execute("SELECT COUNT(*) FROM new_rows").fetchone()[0]
        if new_cnt == 0:
            logger.info("无新增日期, 结束")
            return
        out_con.execute("INSERT INTO ah_premium SELECT * FROM new_rows")
        out_con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ah_premium_uq ON ah_premium(cn_code, trade_date)"
        )
        logger.info("追加完成: 新增 %d 行", new_cnt)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="计算 A/H 溢价 (HKD→CNY 通过 USD 交叉汇率)")
    p.add_argument(
        "--source-db",
        default=str(SRC_DB_PATH),
        help="源行情 DuckDB 路径 (含 daily_a/daily_h/fx_daily)",
    )
    p.add_argument(
        "--output-db", default=str(OUT_DB_PATH), help="输出处理后 DuckDB 路径"
    )
    p.add_argument("--ah-csv", default=str(AH_CODES_CSV), help="A/H 映射 CSV 路径")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--append", action="store_true", help="增量追加新日期")
    mode.add_argument(
        "--rebuild", action="store_true", help="强制全量重建 (默认亦为全量重建)"
    )
    return p.parse_args()


def main() -> None:  # pragma: no cover
    args = parse_args()
    global SRC_DB_PATH, OUT_DB_PATH, AH_CODES_CSV
    SRC_DB_PATH = Path(args.source_db)
    OUT_DB_PATH = Path(args.output_db)
    AH_CODES_CSV = Path(args.ah_csv)
    if args.append:
        append_new(OUT_DB_PATH, SRC_DB_PATH)
    else:
        rebuild(OUT_DB_PATH, SRC_DB_PATH)


if __name__ == "__main__":  # pragma: no cover
    main()
