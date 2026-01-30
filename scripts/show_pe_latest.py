import argparse
import csv
from pathlib import Path
from typing import Iterable, Sequence

import _bootstrap  # noqa: F401

from qs.sqlite_utils import connect_sqlite


DEFAULT_DB_PATH = Path("data/data.sqlite")
DEFAULT_TABLE = "bak_daily_a"  # A 股特色扩展行情, 含 pe 字段


def _iter_rows(
    con, sql: str, params: Sequence[object] | None = None
) -> tuple[list[str], Iterable[tuple[object, ...]]]:
    cur = con.execute(sql, list(params) if params else [])
    cols = [d[0] for d in cur.description] if cur.description else []
    return cols, cur.fetchall()


def _print_table(cols: list[str], rows: Iterable[tuple[object, ...]]) -> None:
    rows_list = list(rows)
    if not cols:
        print("(no columns)")
        return
    if not rows_list:
        print("(no rows)")
        return

    # simple fixed-width formatting (no external deps)
    str_rows = [[("" if v is None else str(v)) for v in r] for r in rows_list]
    widths = [len(c) for c in cols]
    for r in str_rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(v))

    def fmt_row(r: list[str]) -> str:
        return "  ".join(v.ljust(widths[i]) for i, v in enumerate(r))

    print(fmt_row(cols))
    print("  ".join("-" * w for w in widths))
    for r in str_rows:
        print(fmt_row(r))


def _summary(con) -> None:
    cols, rows = _iter_rows(
        con,
        f"""
        SELECT
          COUNT(*) AS rows,
          COUNT(DISTINCT ts_code) AS stocks,
          MIN(trade_date) AS min_trade_date,
          MAX(trade_date) AS max_trade_date,
          MIN(pe) AS min_pe,
          MAX(pe) AS max_pe
        FROM "{DEFAULT_TABLE}"
        """,
    )
    _print_table(cols, rows)


def _latest_one(con, ts_code: str, *, n: int) -> None:
    cols, rows = _iter_rows(
        con,
        f"""
        SELECT
          d.ts_code,
          s.name AS stock_name,
          d.trade_date,
          d.pe,
          d.total_mv,
          d.float_mv
        FROM "{DEFAULT_TABLE}" d
        LEFT JOIN "stock_basic_a" s
          ON s.ts_code = d.ts_code
        WHERE d.ts_code = ?
        ORDER BY d.trade_date DESC
        LIMIT ?
        """,
        [ts_code, n],
    )
    rows_list = list(rows)
    if not rows_list:
        print(f"❌ 未找到 ts_code={ts_code} 的记录 (表={DEFAULT_TABLE})")
        return
    _print_table(cols, rows_list)


def _export_all_latest(con, out_path: Path) -> None:
    cols, rows = _iter_rows(
        con,
        f"""
        WITH latest AS (
          SELECT ts_code, MAX(trade_date) AS trade_date
          FROM "{DEFAULT_TABLE}"
          GROUP BY ts_code
        )
        SELECT
          d.ts_code,
          s.name AS stock_name,
          d.trade_date,
          d.pe,
          d.total_mv,
          d.float_mv
        FROM "{DEFAULT_TABLE}" d
        JOIN latest l
          ON d.ts_code = l.ts_code AND d.trade_date = l.trade_date
        LEFT JOIN "stock_basic_a" s
          ON s.ts_code = d.ts_code
        ORDER BY d.ts_code
        """,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows_list)
    print(f"✅ 导出完成: {out_path} (rows={len(rows_list)})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="检查 SQLite 中 A 股市盈率(PE)数据：来自 bak_daily_a.pe"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--ts-code", type=str, default=None)
    parser.add_argument(
        "--n", type=int, default=1, help="当指定 --ts-code 时, 输出最近 N 条记录"
    )
    parser.add_argument(
        "--export-latest-csv",
        type=Path,
        default=None,
        help="导出每只股票的最新一条 PE 到 CSV (可能需要几十秒)",
    )
    args = parser.parse_args()

    print(f"Connecting to {args.db.resolve()}  (exists={args.db.exists()})")
    con = connect_sqlite(args.db, read_only=True)
    try:
        tables = [
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        if DEFAULT_TABLE not in tables:
            print(f"❌ 表 {DEFAULT_TABLE} 不存在。当前 tables={tables}")
            return

        if args.ts_code:
            _latest_one(con, args.ts_code, n=max(1, args.n))
            return

        _summary(con)
        if args.export_latest_csv:
            _export_all_latest(con, args.export_latest_csv)
    finally:
        con.close()


if __name__ == "__main__":
    main()
