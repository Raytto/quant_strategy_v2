from __future__ import annotations

import sqlite3

from qs.sqlite_utils import ensure_unique_index_with_dedupe


def test_ensure_unique_index_with_dedupe_removes_duplicates(tmp_path):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute('CREATE TABLE t (ts_code TEXT, trade_date TEXT, v INTEGER)')
        con.execute('INSERT INTO t VALUES ("000001.SZ", "20200102", 1)')
        con.execute('INSERT INTO t VALUES ("000001.SZ", "20200102", 1)')
        con.commit()

        ensure_unique_index_with_dedupe(
            con, table="t", columns=["ts_code", "trade_date"], index_name="t_uq"
        )
        con.commit()

        n = con.execute(
            'SELECT COUNT(*) FROM t WHERE ts_code="000001.SZ" AND trade_date="20200102"'
        ).fetchone()[0]
        assert n == 1
    finally:
        con.close()


def test_ensure_unique_index_with_dedupe_deletes_null_keys(tmp_path):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute('CREATE TABLE t (ts_code TEXT, trade_date TEXT, v INTEGER)')
        con.execute('INSERT INTO t VALUES (NULL, "20200102", 1)')
        con.execute('INSERT INTO t VALUES ("000001.SZ", "20200102", 1)')
        con.commit()

        ensure_unique_index_with_dedupe(
            con,
            table="t",
            columns=["ts_code", "trade_date"],
            index_name="t_uq",
            delete_null_keys=True,
        )
        con.commit()

        n_null = con.execute("SELECT COUNT(*) FROM t WHERE ts_code IS NULL").fetchone()[0]
        assert n_null == 0
    finally:
        con.close()

