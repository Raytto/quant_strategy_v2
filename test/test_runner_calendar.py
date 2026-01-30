from __future__ import annotations

import sqlite3

from qs.backtester.runner import load_calendar_bars_from_sqlite


def test_load_calendar_bars_from_sqlite_union_sorted(tmp_path):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute(
            "CREATE TABLE daily_a (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL)"
        )
        con.execute(
            "CREATE TABLE daily_h (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL)"
        )

        con.execute(
            'INSERT INTO daily_a VALUES ("000001.SZ","20200102",1,1,1,1)'
        )
        con.execute(
            'INSERT INTO daily_h VALUES ("00005.HK","20200103",2,2,2,2)'
        )
        con.execute(
            'INSERT INTO daily_a VALUES ("000001.SZ","20200103",1,1,1,1)'
        )
        con.commit()
    finally:
        con.close()

    bars = load_calendar_bars_from_sqlite(db_path=db, start_date="20200101")
    assert [b.trade_date for b in bars] == ["20200102", "20200103"]

