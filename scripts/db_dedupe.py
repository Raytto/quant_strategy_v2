#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

import _bootstrap  # noqa: F401

from qs.sqlite_utils import connect_sqlite, dedupe_table, ensure_unique_index_with_dedupe, table_exists


UNIQUE_KEYS: dict[str, list[str]] = {
    "stock_basic_a": ["ts_code"],
    "stock_basic_h": ["ts_code"],
    "fx_basic": ["ts_code"],
    "daily_a": ["ts_code", "trade_date"],
    "adj_factor_a": ["ts_code", "trade_date"],
    "bak_daily_a": ["ts_code", "trade_date"],
    "daily_h": ["ts_code", "trade_date"],
    "adj_factor_h": ["ts_code", "trade_date"],
    "fx_daily": ["ts_code", "trade_date"],
    "index_daily": ["ts_code", "trade_date"],
    "index_global": ["ts_code", "trade_date"],
}


def _has_invalid_key_rows(con, table: str, key_cols: list[str]) -> bool:
    where = " OR ".join([f'"{c}" IS NULL OR "{c}" = \'\'' for c in key_cols])
    row = con.execute(f'SELECT 1 FROM "{table}" WHERE {where} LIMIT 1').fetchone()
    return row is not None


def repair(db_path: Path, *, force_scan: bool) -> None:
    con = connect_sqlite(db_path)
    try:
        for table, key_cols in UNIQUE_KEYS.items():
            if not table_exists(con, table):
                continue

            deleted = 0
            if force_scan:
                deleted = dedupe_table(con, table=table, key_columns=key_cols, delete_null_keys=True)

            ensure_unique_index_with_dedupe(
                con, table=table, columns=key_cols, index_name=f"{table}_uq", delete_null_keys=True
            )
            con.commit()

            invalid = _has_invalid_key_rows(con, table, key_cols)
            if deleted:
                print(f"{table}: deleted={deleted} (force_scan)")
            elif invalid:
                print(f"{table}: has null/empty key rows (run with --force-scan to delete)")
            else:
                print(f"{table}: ok")
    finally:
        con.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Check/repair SQLite duplicates and unique indexes")
    p.add_argument("--db", type=Path, default=Path("data/data.sqlite"))
    p.add_argument(
        "--force-scan",
        action="store_true",
        help="Scan tables and delete duplicates by key (can be slow on large DBs).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    repair(args.db, force_scan=bool(args.force_scan))


if __name__ == "__main__":
    main()
