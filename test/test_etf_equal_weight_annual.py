from __future__ import annotations

import sqlite3

from qs.backtester.runner import run_backtest, load_calendar_bars_for_symbols_from_sqlite
from qs.strategy.etf_equal_weight_annual import ETFEqualWeightAnnualStrategy


def _init_db(db_path):
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            "CREATE TABLE etf_daily (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL)"
        )
        con.execute(
            "CREATE TABLE adj_factor_etf (ts_code TEXT, trade_date TEXT, adj_factor REAL)"
        )
        con.commit()
    finally:
        con.close()


def test_etf_adjusted_prices_use_latest_adj_factor_base(tmp_path):
    db = tmp_path / "t.sqlite"
    _init_db(db)
    con = sqlite3.connect(db)
    try:
        con.execute('INSERT INTO etf_daily VALUES ("510500.SH","20200102",10,10,10,11)')
        con.execute('INSERT INTO etf_daily VALUES ("510500.SH","20200103",20,20,20,22)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("510500.SH","20200102",2.0)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("510500.SH","20200103",4.0)')  # latest base
        con.commit()
    finally:
        con.close()

    strat = ETFEqualWeightAnnualStrategy(
        db_path_raw=str(db),
        symbols=["510500.SH"],
        start_date="20200101",
        use_adjusted=True,
    )
    opens = strat._load_opens("20200102", ["510500.SH"])  # noqa: SLF001
    assert abs(opens["510500.SH"] - 5.0) < 1e-9  # 10 * 2 / 4

    marks = strat._load_marks_close("20200102", ["510500.SH"])  # noqa: SLF001
    assert abs(marks["510500.SH"] - 5.5) < 1e-9  # 11 * 2 / 4


def test_etf_strategy_rebalances_once_per_year(tmp_path):
    db = tmp_path / "t.sqlite"
    _init_db(db)
    con = sqlite3.connect(db)
    try:
        for sym in ("510500.SH", "518660.SH"):
            con.execute(f'INSERT INTO etf_daily VALUES ("{sym}","20200102",10,10,10,10)')
            con.execute(f'INSERT INTO etf_daily VALUES ("{sym}","20210104",10,10,10,10)')
            con.execute(f'INSERT INTO adj_factor_etf VALUES ("{sym}","20200102",1.0)')
            con.execute(f'INSERT INTO adj_factor_etf VALUES ("{sym}","20210104",1.0)')
        con.commit()
    finally:
        con.close()

    bars = load_calendar_bars_for_symbols_from_sqlite(
        db_path=db,
        table="etf_daily",
        symbols=["510500.SH", "518660.SH"],
        start_date="20200101",
    )
    assert [b.trade_date for b in bars] == ["20200102", "20210104"]

    strat = ETFEqualWeightAnnualStrategy(
        db_path_raw=str(db),
        symbols=["510500.SH", "518660.SH"],
        start_date="20200101",
        use_adjusted=True,
        rebalance_year_interval=1,
    )
    res = run_backtest(bars=bars, strategy=strat, initial_cash=1_000_000.0, symbol="")
    assert len(strat.rebalance_history) == 2
    assert set(res.broker.positions.keys()) == {"510500.SH", "518660.SH"}
    assert all(p.size >= 0 for p in res.broker.positions.values())

