from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence
import sqlite3

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd


def connect_sqlite(db_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a SQLite connection.

    - read_only=True uses SQLite URI mode=ro (fails if DB file is missing).
    - Applies a few pragmatic PRAGMA defaults.
    """
    path = Path(db_path)
    if read_only:
        uri = f"file:{path.as_posix()}?mode=ro"
        con = sqlite3.connect(uri, uri=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(path.as_posix())
    con.execute("PRAGMA foreign_keys=ON")
    if not read_only:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
    return con


def table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", [table]
    ).fetchone()
    return row is not None


def ensure_unique_index(
    con: sqlite3.Connection, *, table: str, columns: Sequence[str], index_name: str
) -> None:
    cols = ", ".join([f'"{c}"' for c in columns])
    con.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" ON "{table}"({cols})')


def dedupe_table(
    con: sqlite3.Connection,
    *,
    table: str,
    key_columns: Sequence[str],
    delete_null_keys: bool = False,
) -> int:
    """Delete duplicate rows by key, keeping the smallest rowid for each key."""
    if not key_columns:
        return 0

    before = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    if delete_null_keys:
        where = " OR ".join([f'"{c}" IS NULL OR "{c}" = \'\'' for c in key_columns])
        con.execute(f'DELETE FROM "{table}" WHERE {where}')

    cols = ", ".join([f'"{c}"' for c in key_columns])
    con.execute(
        f"""
        DELETE FROM "{table}"
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM "{table}" GROUP BY {cols}
        )
        """
    )
    after = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    return int(before - after)


def ensure_unique_index_with_dedupe(
    con: sqlite3.Connection,
    *,
    table: str,
    columns: Sequence[str],
    index_name: str,
    delete_null_keys: bool = False,
) -> None:
    """Ensure a UNIQUE index exists; if creation fails, dedupe then retry."""
    if delete_null_keys and columns:
        where = " OR ".join([f'"{c}" IS NULL OR "{c}" = \'\'' for c in columns])
        con.execute(f'DELETE FROM "{table}" WHERE {where}')
    try:
        ensure_unique_index(con, table=table, columns=columns, index_name=index_name)
    except sqlite3.IntegrityError:
        dedupe_table(con, table=table, key_columns=columns, delete_null_keys=delete_null_keys)
        ensure_unique_index(con, table=table, columns=columns, index_name=index_name)


def read_sql_df(
    con: sqlite3.Connection, sql: str, params: Sequence[Any] | None = None
) -> "pd.DataFrame":
    import pandas as pd

    return pd.read_sql_query(sql, con, params=list(params) if params else None)


def insert_df_ignore(
    con: sqlite3.Connection,
    *,
    df: "pd.DataFrame",
    table: str,
    unique_by: Sequence[str] | None = None,
) -> int:
    """Insert rows from df into table, ignoring duplicates (SQLite INSERT OR IGNORE).

    Returns inserted row count estimate (changes in total row count).
    """
    if df.empty:
        return 0

    import pandas as pd

    work = df.copy()
    if unique_by and all(c in work.columns for c in unique_by):
        work = work.drop_duplicates(subset=list(unique_by))

    if not table_exists(con, table):
        work.head(0).to_sql(table, con, if_exists="fail", index=False)

    cols = list(work.columns)
    quoted_cols = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    sql = f'INSERT OR IGNORE INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'

    before = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    records = work.where(pd.notnull(work), None).to_records(index=False).tolist()
    con.executemany(sql, records)
    after = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    return int(after - before)


__all__ = [
    "connect_sqlite",
    "table_exists",
    "ensure_unique_index",
    "dedupe_table",
    "ensure_unique_index_with_dedupe",
    "read_sql_df",
    "insert_df_ignore",
]
