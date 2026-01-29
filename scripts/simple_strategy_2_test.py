from __future__ import annotations

import _bootstrap  # noqa: F401

from pathlib import Path
from qs.backtester.data import Bar, DataFeed
from qs.backtester.broker import Broker
from qs.backtester.engine import BacktestEngine
from qs.backtester.stats import (
    compute_annual_returns,
    compute_max_drawdown,
    compute_risk_metrics,
)
from qs.strategy.simple_strategy_2 import SimpleStrategy2, PairContext

START_DATE = "20200101"
A_CODE = "601628.SH"
H_CODE = "02628.HK"
INITIAL_CASH = 1_000_000.0
from qs.sqlite_utils import connect_sqlite, read_sql_df

SRC_DB = Path("data/data.sqlite")

con = connect_sqlite(SRC_DB, read_only=True)
a_df = read_sql_df(
    con,
    f"""
SELECT trade_date, open, high, low, close, pct_chg
FROM daily_a
WHERE ts_code='{A_CODE}' AND trade_date >= '{START_DATE}'
ORDER BY trade_date
"""
)

h_df = read_sql_df(
    con,
    f"""
SELECT trade_date, open, close, pct_chg
FROM daily_h
WHERE ts_code='{H_CODE}' AND trade_date >= '{START_DATE}'
ORDER BY trade_date
"""
)
con.close()

h_open = {r.trade_date: r.open for r in h_df.itertuples(index=False)}
h_pct = {r.trade_date: r.pct_chg for r in h_df.itertuples(index=False)}
h_close = {r.trade_date: r.close for r in h_df.itertuples(index=False)}
ctx = PairContext(h_open=h_open, h_pct=h_pct, h_close=h_close)

bars = [
    Bar(r.trade_date, r.open, r.high, r.low, r.close, r.pct_chg)
    for r in a_df.itertuples(index=False)
]
feed = DataFeed(bars)

broker = Broker(INITIAL_CASH, enable_trade_log=True)
# 不设置默认 symbol，使用多标的 API
strategy = SimpleStrategy2(A_CODE, H_CODE, ctx)
engine = BacktestEngine(feed, broker, strategy)
curve = engine.run()

final_equity = curve[-1].equity if curve else INITIAL_CASH
print(
    f"Bars: {len(bars)}  Final Equity: {final_equity:.2f}  Return: {(final_equity/INITIAL_CASH-1)*100:.2f}%  Total Fees: {broker.total_fees:.2f}"
)

annual_returns = compute_annual_returns(curve)
if annual_returns:
    print("Annual Returns:")
    for y, r in annual_returns.items():
        print(f"  {y}: {r*100:.2f}%")

max_dd, dd_peak, dd_trough = compute_max_drawdown(curve)
if dd_peak and dd_trough:
    print(f"Max Drawdown: {max_dd*100:.2f}%  Period: {dd_peak} -> {dd_trough}")

risk = compute_risk_metrics(curve, INITIAL_CASH)
if risk:
    print(
        f"CAGR: {risk['CAGR']*100:.2f}%  AnnReturn: {risk['AnnReturn']*100:.2f}%  AnnVol: {risk['AnnVol']*100:.2f}%  Sharpe: {risk['Sharpe']:.2f}  WinRate: {risk['WinRate']*100:.2f}%"
    )

# 输出额外：当前持仓列表
for sym, pos in broker.positions.items():
    if pos.size:
        print(f"POSITION {sym} size={pos.size:.0f} avg={pos.avg_price:.4f}")
