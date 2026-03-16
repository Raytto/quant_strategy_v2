from __future__ import annotations

import sqlite3
import pytest

from qs.backtester.market import PriceRequest, SqliteMarketData
from qs.backtester.runner import (
    load_calendar_bars_for_symbols_from_sqlite,
    load_calendar_bars_from_sqlite,
    run_backtest,
)
from qs.strategy.ah_premium_quarterly import AHPremiumQuarterlyStrategy
from qs.strategy.etf_equal_weight_annual import ETFEqualWeightAnnualStrategy
from qs.strategy.etf_min_premium_weekly import ETFMinPremiumWeeklyStrategy
from qs.strategy.ignored_stock_strategy import IgnoredStockStrategy
from qs.strategy.low_pe_quarterly import LowPEQuarterlyStrategy
from qs.strategy.simple_strategy_2 import SimpleStrategy2


def _init_etf_db(db_path):
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            "CREATE TABLE etf_daily (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL)"
        )
        con.execute(
            "CREATE TABLE adj_factor_etf (ts_code TEXT, trade_date TEXT, adj_factor REAL, discount_rate REAL)"
        )
        con.commit()
    finally:
        con.close()


def test_market_data_adjusted_prices_use_latest_adj_factor_base(tmp_path):
    db = tmp_path / "t.sqlite"
    _init_etf_db(db)
    con = sqlite3.connect(db)
    try:
        con.execute('INSERT INTO etf_daily VALUES ("510500.SH","20200102",10,10,10,11)')
        con.execute('INSERT INTO etf_daily VALUES ("510500.SH","20200103",20,20,20,22)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("510500.SH","20200102",2.0,NULL)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("510500.SH","20200103",4.0,NULL)')
        con.commit()
    finally:
        con.close()

    market_data = SqliteMarketData(db)
    try:
        open_map = market_data.get_price_map(
            request=PriceRequest(
                table="etf_daily",
                field="open",
                adjusted=True,
                adjustment_table="adj_factor_etf",
                exact=True,
            ),
            symbols=["510500.SH"],
            trade_date="20200102",
        )
        assert abs(open_map["510500.SH"] - 5.0) < 1e-9

        close_map = market_data.get_price_map(
            request=PriceRequest(
                table="etf_daily",
                field="close",
                adjusted=True,
                adjustment_table="adj_factor_etf",
                exact=False,
            ),
            symbols=["510500.SH"],
            trade_date="20200102",
        )
        assert abs(close_map["510500.SH"] - 5.5) < 1e-9
    finally:
        market_data.close()


def test_context_history_rejects_future_queries(tmp_path):
    db = tmp_path / "t.sqlite"
    _init_etf_db(db)
    market_data = SqliteMarketData(db)
    try:
        history = market_data.history("20200102")
        try:
            history.get_dataset_values(
                table="adj_factor_etf",
                symbols=["510500.SH"],
                fields=["discount_rate"],
                trade_date="20200103",
                exact=True,
            )
        except ValueError as exc:
            assert "historical cutoff" in str(exc)
        else:
            raise AssertionError("expected future query to be rejected")
    finally:
        market_data.close()


def test_market_data_supports_snapshot_rows_and_full_reference_reads(tmp_path):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute(
            "CREATE TABLE bak_daily_a (ts_code TEXT, trade_date TEXT, pe REAL)"
        )
        con.execute(
            "CREATE TABLE stock_basic_a (ts_code TEXT, name TEXT, list_date TEXT, delist_date TEXT)"
        )
        con.execute('INSERT INTO bak_daily_a VALUES ("600001.SH","20200102",5.0)')
        con.execute('INSERT INTO bak_daily_a VALUES ("600001.SH","20200103",6.0)')
        con.execute('INSERT INTO bak_daily_a VALUES ("600002.SH","20200102",9.0)')
        con.execute('INSERT INTO stock_basic_a VALUES ("600001.SH","Demo A","20100101","")')
        con.execute('INSERT INTO stock_basic_a VALUES ("600002.SH","Demo B","20110101","")')
        con.commit()
    finally:
        con.close()

    market_data = SqliteMarketData(db)
    try:
        history = market_data.history("20200103")
        rows = history.get_snapshot_rows(
            table="bak_daily_a",
            fields=["pe"],
            trade_date="20200103",
            exact=False,
        )
        assert {row["ts_code"] for row in rows} == {"600001.SH", "600002.SH"}
        assert history.get_latest_trade_date(
            table="bak_daily_a",
            on_or_before="20200103",
        ) == "20200103"

        ref = market_data.reference().get_values(
            table="stock_basic_a",
            symbols=None,
            fields=["name", "list_date"],
        )
        assert ref["600001.SH"]["name"] == "Demo A"
        assert ref["600002.SH"]["list_date"] == "20110101"
    finally:
        market_data.close()


def test_etf_strategy_rebalances_once_per_year_via_market_data_context(tmp_path):
    db = tmp_path / "t.sqlite"
    _init_etf_db(db)
    con = sqlite3.connect(db)
    try:
        for sym in ("510500.SH", "518660.SH"):
            con.execute(f'INSERT INTO etf_daily VALUES ("{sym}","20200102",10,10,10,10)')
            con.execute(f'INSERT INTO etf_daily VALUES ("{sym}","20210104",10,10,10,10)')
            con.execute(f'INSERT INTO adj_factor_etf VALUES ("{sym}","20200102",1.0,NULL)')
            con.execute(f'INSERT INTO adj_factor_etf VALUES ("{sym}","20210104",1.0,NULL)')
        con.commit()
    finally:
        con.close()

    bars = load_calendar_bars_for_symbols_from_sqlite(
        db_path=db,
        table="etf_daily",
        symbols=["510500.SH", "518660.SH"],
        start_date="20200101",
    )

    strat = ETFEqualWeightAnnualStrategy(
        db_path_raw=str(db),
        symbols=["510500.SH", "518660.SH"],
        start_date="20200101",
        use_adjusted=True,
        rebalance_year_interval=1,
    )
    market_data = SqliteMarketData(db)
    try:
        res = run_backtest(
            bars=bars,
            strategy=strat,
            initial_cash=1_000_000.0,
            symbol="",
            market_data=market_data,
        )
    finally:
        market_data.close()

    assert len(strat.rebalance_history) == 2
    assert set(res.broker.positions.keys()) == {"510500.SH", "518660.SH"}
    assert all(p.size >= 0 for p in res.broker.positions.values())


def test_weekly_min_premium_uses_previous_day_signal(tmp_path):
    db = tmp_path / "t.sqlite"
    _init_etf_db(db)
    con = sqlite3.connect(db)
    try:
        for sym in ("AAA.SH", "BBB.SH"):
            con.execute(f'INSERT INTO etf_daily VALUES ("{sym}","20200103",10,10,10,10)')
            con.execute(f'INSERT INTO etf_daily VALUES ("{sym}","20200106",10,10,10,10)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("AAA.SH","20200103",1.0,1.0)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("BBB.SH","20200103",1.0,3.0)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("AAA.SH","20200106",1.0,5.0)')
        con.execute('INSERT INTO adj_factor_etf VALUES ("BBB.SH","20200106",1.0,0.5)')
        con.commit()
    finally:
        con.close()

    bars = load_calendar_bars_for_symbols_from_sqlite(
        db_path=db,
        table="etf_daily",
        symbols=["AAA.SH", "BBB.SH"],
        start_date="20200101",
    )
    strat = ETFMinPremiumWeeklyStrategy(
        db_path_raw=str(db),
        symbols=["AAA.SH", "BBB.SH"],
        start_date="20200101",
        use_adjusted=True,
        monday_only=True,
        min_improvement=0.0,
    )
    market_data = SqliteMarketData(db)
    try:
        res = run_backtest(
            bars=bars,
            strategy=strat,
            initial_cash=100_000.0,
            symbol="",
            market_data=market_data,
        )
    finally:
        market_data.close()

    held = {sym: pos.size for sym, pos in res.broker.positions.items() if pos.size > 0}
    assert set(held) == {"AAA.SH"}
    assert strat.check_history[-1].signal_date == "20200103"
    assert strat.check_history[-1].best_symbol == "AAA.SH"


def test_ah_premium_strategy_uses_framework_market_data(tmp_path):
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
            "CREATE TABLE adj_factor_a (ts_code TEXT, trade_date TEXT, adj_factor REAL)"
        )
        con.execute(
            "CREATE TABLE adj_factor_h (ts_code TEXT, trade_date TEXT, adj_factor REAL)"
        )
        con.execute(
            "CREATE TABLE fx_daily (ts_code TEXT, trade_date TEXT, bid_close REAL, ask_close REAL)"
        )
        con.execute(
            "CREATE TABLE stock_basic_a (ts_code TEXT, delist_date TEXT)"
        )
        con.execute(
            "CREATE TABLE stock_basic_h (ts_code TEXT, delist_date TEXT)"
        )
        con.execute('INSERT INTO daily_a VALUES ("600001.SH","20200102",10,10,10,11)')
        con.execute('INSERT INTO daily_a VALUES ("600001.SH","20200103",10,10,10,10)')
        con.execute('INSERT INTO daily_h VALUES ("00001.HK","20200102",8,8,8,8)')
        con.execute('INSERT INTO daily_h VALUES ("00001.HK","20200103",8,8,8,8)')
        con.execute('INSERT INTO adj_factor_a VALUES ("600001.SH","20200102",1.0)')
        con.execute('INSERT INTO adj_factor_a VALUES ("600001.SH","20200103",1.0)')
        con.execute('INSERT INTO adj_factor_h VALUES ("00001.HK","20200102",1.0)')
        con.execute('INSERT INTO adj_factor_h VALUES ("00001.HK","20200103",1.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDCNH.FXCM","20200102",7.0,7.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDHKD.FXCM","20200102",7.75,7.75)')
        con.execute('INSERT INTO fx_daily VALUES ("USDCNH.FXCM","20200103",7.0,7.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDHKD.FXCM","20200103",7.75,7.75)')
        con.execute('INSERT INTO stock_basic_a VALUES ("600001.SH","")')
        con.execute('INSERT INTO stock_basic_h VALUES ("00001.HK","")')
        con.commit()
    finally:
        con.close()

    pairs = tmp_path / "ah_codes.csv"
    pairs.write_text("name,cn_code,hk_code\nDemo Pair,600001.SH,00001.HK\n", encoding="utf-8")

    bars = load_calendar_bars_from_sqlite(db_path=db, start_date="20200101")
    strat = AHPremiumQuarterlyStrategy(
        db_path_raw=str(db),
        pairs_csv_path=str(pairs),
        top_k=1,
        bottom_k=1,
        start_date="20200101",
        capital_split=0.5,
        use_adjusted=True,
        premium_use_adjusted=False,
        rebalance_month_interval=3,
    )
    market_data = SqliteMarketData(db)
    try:
        res = run_backtest(
            bars=bars,
            strategy=strat,
            initial_cash=100_000.0,
            symbol="",
            market_data=market_data,
        )
    finally:
        market_data.close()

    assert len(strat.rebalance_history) == 1
    assert set(sym for sym, pos in res.broker.positions.items() if pos.size > 0) == {
        "600001.SH",
        "00001.HK",
    }


def test_simple_strategy_2_uses_framework_market_data(tmp_path):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute(
            "CREATE TABLE daily_a (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, pct_chg REAL)"
        )
        con.execute(
            "CREATE TABLE daily_h (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, pct_chg REAL)"
        )
        con.execute(
            "CREATE TABLE fx_daily (ts_code TEXT, trade_date TEXT, bid_close REAL, ask_close REAL)"
        )
        con.execute('INSERT INTO daily_a VALUES ("601628.SH","20200102",10,10,10,10,0.0)')
        con.execute('INSERT INTO daily_a VALUES ("601628.SH","20200103",10,10,10,10,0.0)')
        con.execute('INSERT INTO daily_h VALUES ("02628.HK","20200102",8,8,8,8,2.0)')
        con.execute('INSERT INTO daily_h VALUES ("02628.HK","20200103",8,8,8,8,0.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDCNH.FXCM","20200102",7.0,7.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDHKD.FXCM","20200102",7.75,7.75)')
        con.execute('INSERT INTO fx_daily VALUES ("USDCNH.FXCM","20200103",7.0,7.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDHKD.FXCM","20200103",7.75,7.75)')
        con.commit()
    finally:
        con.close()

    bars = load_calendar_bars_from_sqlite(db_path=db, start_date="20200101")
    strat = SimpleStrategy2("601628.SH", "02628.HK")
    market_data = SqliteMarketData(db)
    try:
        res = run_backtest(
            bars=bars,
            strategy=strat,
            initial_cash=100_000.0,
            symbol="",
            market_data=market_data,
        )
    finally:
        market_data.close()

    held = {sym for sym, pos in res.broker.positions.items() if pos.size > 0}
    assert held == {"601628.SH"}


def test_low_pe_strategy_uses_framework_market_data(tmp_path):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute(
            "CREATE TABLE daily_a (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, pct_chg REAL)"
        )
        con.execute(
            "CREATE TABLE daily_h (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, pct_chg REAL)"
        )
        con.execute(
            "CREATE TABLE bak_daily_a (ts_code TEXT, trade_date TEXT, pe REAL)"
        )
        con.execute(
            "CREATE TABLE adj_factor_a (ts_code TEXT, trade_date TEXT, adj_factor REAL)"
        )
        con.execute(
            "CREATE TABLE adj_factor_h (ts_code TEXT, trade_date TEXT, adj_factor REAL)"
        )
        con.execute(
            "CREATE TABLE fx_daily (ts_code TEXT, trade_date TEXT, bid_close REAL, ask_close REAL)"
        )
        con.execute(
            "CREATE TABLE stock_basic_a (ts_code TEXT, name TEXT, list_date TEXT, delist_date TEXT)"
        )
        con.execute(
            "CREATE TABLE stock_basic_h (ts_code TEXT, delist_date TEXT)"
        )

        con.execute('INSERT INTO daily_a VALUES ("600001.SH","20200102",10,10,10,10,0.0)')
        con.execute('INSERT INTO daily_a VALUES ("600001.SH","20200103",10,10,10,10,0.0)')
        con.execute('INSERT INTO daily_a VALUES ("600002.SH","20200102",20,20,20,20,0.0)')
        con.execute('INSERT INTO daily_a VALUES ("600002.SH","20200103",20,20,20,20,0.0)')
        con.execute('INSERT INTO daily_h VALUES ("00001.HK","20200102",8,8,8,8,0.0)')
        con.execute('INSERT INTO daily_h VALUES ("00001.HK","20200103",8,8,8,8,0.0)')

        con.execute('INSERT INTO bak_daily_a VALUES ("600001.SH","20200102",5.0)')
        con.execute('INSERT INTO bak_daily_a VALUES ("600002.SH","20200102",8.0)')

        con.execute('INSERT INTO adj_factor_a VALUES ("600001.SH","20200102",1.0)')
        con.execute('INSERT INTO adj_factor_a VALUES ("600001.SH","20200103",1.0)')
        con.execute('INSERT INTO adj_factor_a VALUES ("600002.SH","20200102",1.0)')
        con.execute('INSERT INTO adj_factor_a VALUES ("600002.SH","20200103",1.0)')
        con.execute('INSERT INTO adj_factor_h VALUES ("00001.HK","20200102",1.0)')
        con.execute('INSERT INTO adj_factor_h VALUES ("00001.HK","20200103",1.0)')

        con.execute('INSERT INTO fx_daily VALUES ("USDCNH.FXCM","20200102",7.0,7.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDHKD.FXCM","20200102",7.75,7.75)')
        con.execute('INSERT INTO fx_daily VALUES ("USDCNH.FXCM","20200103",7.0,7.0)')
        con.execute('INSERT INTO fx_daily VALUES ("USDHKD.FXCM","20200103",7.75,7.75)')

        con.execute('INSERT INTO stock_basic_a VALUES ("600001.SH","A One","20100101","")')
        con.execute('INSERT INTO stock_basic_a VALUES ("600002.SH","A Two","20100101","")')
        con.execute('INSERT INTO stock_basic_h VALUES ("00001.HK","")')
        con.commit()
    finally:
        con.close()

    pairs = tmp_path / "ah_codes.csv"
    pairs.write_text("name,cn_code,hk_code\nDemo Pair,600001.SH,00001.HK\n", encoding="utf-8")

    bars = load_calendar_bars_from_sqlite(db_path=db, start_date="20200101")
    strat = LowPEQuarterlyStrategy(
        db_path_raw=str(db),
        pairs_csv_path=str(pairs),
        a_k=1,
        h_k=1,
        start_date="20200101",
        rebalance_month_interval=3,
        use_adjusted=True,
    )
    market_data = SqliteMarketData(db)
    try:
        res = run_backtest(
            bars=bars,
            strategy=strat,
            initial_cash=100_000.0,
            symbol="",
            market_data=market_data,
        )
    finally:
        market_data.close()

    assert len(strat.rebalance_history) == 1
    assert set(sym for sym, pos in res.broker.positions.items() if pos.size > 0) == {
        "600001.SH",
        "00001.HK",
    }


def test_ignored_stock_strategy_is_explicitly_legacy():
    with pytest.raises(RuntimeError, match="legacy research code"):
        IgnoredStockStrategy()
