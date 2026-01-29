from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse
from pathlib import Path
from qs.backtester.data import Bar, DataFeed
from qs.backtester.broker import Broker
from qs.backtester.engine import BacktestEngine
from qs.strategy.simple_strategy import SimpleStrategy
from qs.backtester.stats import (
    compute_annual_returns,
    compute_max_drawdown,
    compute_risk_metrics,
)

START_DATE = "20200101"
TS_CODE = "601628.SH"  # 中国人寿
INITIAL_CASH = 1_000_000.0

from qs.sqlite_utils import connect_sqlite, read_sql_df

SRC_DB = Path("data/data.sqlite")


def parse_args():
    p = argparse.ArgumentParser(description="Run simple strategy backtest")
    p.add_argument("--log", action="store_true", help="输出交易日志")
    return p.parse_args()


args = parse_args()

# 读取数据
con = connect_sqlite(SRC_DB, read_only=True)
df = read_sql_df(
    con,
    f"""
SELECT trade_date, open, high, low, close, pct_chg
FROM daily_a
WHERE ts_code='{TS_CODE}' AND trade_date >= '{START_DATE}'
ORDER BY trade_date
"""
)
con.close()

bars = [
    Bar(r.trade_date, r.open, r.high, r.low, r.close, r.pct_chg)
    for r in df.itertuples(index=False)
]
feed = DataFeed(bars)
broker = Broker(INITIAL_CASH, enable_trade_log=True, symbol=TS_CODE)
strategy = SimpleStrategy(TS_CODE)
engine = BacktestEngine(feed, broker, strategy)
curve = engine.run()

# 输出结果概要
final_equity = curve[-1].equity if curve else INITIAL_CASH
print(
    f"Bars: {len(bars)}  Final Equity: {final_equity:.2f}  Return: {(final_equity/INITIAL_CASH-1)*100:.2f}%  Total Fees: {broker.total_fees:.2f}"
)

# 年度收益
annual_returns = compute_annual_returns(curve)
if annual_returns:
    print("Annual Returns:")
    for y, r in annual_returns.items():
        print(f"  {y}: {r*100:.2f}%")

# 最大回撤
max_dd, dd_peak, dd_trough = compute_max_drawdown(curve)
if dd_peak and dd_trough:
    print(f"Max Drawdown: {max_dd*100:.2f}%  Period: {dd_peak} -> {dd_trough}")

# 风险指标
risk = compute_risk_metrics(curve, INITIAL_CASH)
if risk:
    print(
        f"CAGR: {risk['CAGR']*100:.2f}%  AnnReturn: {risk['AnnReturn']*100:.2f}%  AnnVol: {risk['AnnVol']*100:.2f}%  Sharpe: {risk['Sharpe']:.2f}  WinRate: {risk['WinRate']*100:.2f}%"
    )

# 保存 equity 曲线 CSV
import csv

out_path = Path("data/equity_simple_strategy.csv")
with out_path.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["trade_date", "equity"])
    for p in curve:
        w.writerow([p.trade_date, f"{p.equity:.2f}"])
print(f"Equity curve saved to {out_path}")

# 导出交易明细 CSV
trades_out = Path("data/trades_simple_strategy.csv")
from qs.backtester.broker import TradeRecord  # type: ignore

with trades_out.open("w", newline="", encoding="utf-8") as f:
    import csv as _csv

    w = _csv.writer(f)
    w.writerow(
        [
            "trade_date",
            "action",
            "symbol",
            "price",
            "exec_price",
            "size",
            "gross_amount",
            "fees",
            "cash_after",
            "position_after",
            "equity_after",
        ]
    )
    for tr in broker.trades:
        w.writerow(
            [
                tr.trade_date,
                tr.action,
                tr.symbol,
                f"{tr.price:.4f}",
                f"{tr.exec_price:.4f}",
                int(tr.size),
                f"{tr.gross_amount:.2f}",
                f"{tr.fees:.2f}",
                f"{tr.cash_after:.2f}",
                int(tr.position_after),
                f"{tr.equity_after:.2f}",
            ]
        )
print(f"Trade details saved to {trades_out}")
