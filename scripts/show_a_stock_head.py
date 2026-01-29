from pathlib import Path

from qs.sqlite_utils import connect_sqlite, read_sql_df

DB_PATH = Path("data/data.sqlite")  # ← 确认路径
TABLE = "stock_basic_a"  # ← 确认表名


def main() -> None:
    print(f"Connecting to {DB_PATH.resolve()}  (exists={DB_PATH.exists()})")

    con = connect_sqlite(DB_PATH, read_only=True)
    tables = [
        row[0]
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    print(f"Current tables in DB: {tables}")

    if TABLE not in tables:
        print(f"❌ 表 {TABLE} 不存在！")
        return

    df = read_sql_df(con, f'SELECT * FROM "{TABLE}" LIMIT 5')
    con.close()

    print("\n--- df.head() ---")
    print(df)
    print(f"\nShape: {df.shape}")


if __name__ == "__main__":
    main()
