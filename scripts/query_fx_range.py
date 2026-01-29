from pathlib import Path

from qs.sqlite_utils import connect_sqlite

DB_PATH = Path("data/data.sqlite")
if not DB_PATH.exists():
    print("数据库文件不存在:", DB_PATH)
    raise SystemExit(1)

con = connect_sqlite(DB_PATH, read_only=True)
try:
    row = con.execute(
        "SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM fx_daily WHERE ts_code='USDCNH.FXCM'"
    ).fetchone()
    if row is None:
        print("查询无结果")
    else:
        mn, mx, cnt = row
        print(f"USDCNH.FXCM 最早日期={mn} 最晚日期={mx} 行数={cnt}")
except Exception as e:
    print("查询失败:", e)
finally:
    con.close()
