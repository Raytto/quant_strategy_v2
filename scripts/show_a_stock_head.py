from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = Path("data/data.duckdb")  # ← 确认路径
TABLE = "stock_basic_a"  # ← 确认表名


def main() -> None:
    print(f"Connecting to {DB_PATH.resolve()}  (exists={DB_PATH.exists()})")

    con = duckdb.connect(str(DB_PATH))
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    print(f"Current tables in DB: {tables}")

    if TABLE not in tables:
        print(f"❌ 表 {TABLE} 不存在！")
        return

    df = con.execute(f"SELECT * FROM {TABLE} LIMIT 5").fetchdf()
    con.close()

    print("\n--- df.head() ---")
    print(df)
    print(f"\nShape: {df.shape}")


if __name__ == "__main__":
    main()
