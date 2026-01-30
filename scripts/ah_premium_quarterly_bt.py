from __future__ import annotations

"""Run quarterly A/H premium strategy backtest from 2018-01-01 to latest.

Steps:
 1. Build a calendar feed (use A-share benchmark index 000300.SH or simply distinct trade dates from daily_a).
 2. Instantiate broker & strategy.
 3. Run engine, compute stats.

Assumptions:
  - Database: data/data.sqlite (raw, contains daily_a/daily_h/fx_daily and optionally adj_factor_a/adj_factor_h).

CLI:
  python scripts/ah_premium_quarterly_bt.py --start 20180101 --top 5 --bottom 5 --cash 1000000
"""
import _bootstrap  # noqa: F401

import argparse
from qs.sqlite_utils import connect_sqlite
from qs.backtester.data import Bar, DataFeed
from qs.backtester.broker import Broker
from qs.backtester.engine import BacktestEngine
from qs.backtester.stats import (
    compute_annual_returns,
    compute_max_drawdown,
    compute_risk_metrics,
)
from qs.strategy.ah_premium_quarterly import AHPremiumQuarterlyStrategy


def load_calendar(start_date: str, db_path: str = "data/data.sqlite"):
    con = connect_sqlite(db_path, read_only=True)
    # Use union of A and H trading days to approximate combined calendar
    q = f"""
    SELECT trade_date,
           MIN(open) AS open,
           MIN(high) AS high,
           MIN(low) AS low,
           MIN(close) AS close,
           NULL AS pct_chg
    FROM (
      SELECT trade_date, open, high, low, close FROM daily_a
      UNION ALL
      SELECT trade_date, open, high, low, close FROM daily_h
    )
    WHERE trade_date >= '{start_date}'
    GROUP BY 1
    ORDER BY 1
    """
    rows = con.execute(q).fetchall()
    con.close()
    bars = [Bar(*r) for r in rows]
    return DataFeed(bars)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="20180101")
    parser.add_argument(
        "--top", type=int, default=5, help="Number of top premium pairs (H leg)"
    )
    parser.add_argument(
        "--bottom", type=int, default=5, help="Number of bottom premium pairs (A leg)"
    )
    parser.add_argument("--cash", type=float, default=1_000_000)
    parser.add_argument(
        "--capital-split",
        type=float,
        default=0.5,
        help="Fraction of capital to allocate to H leg cohort (0-1)",
    )
    args = parser.parse_args()

    feed = load_calendar(args.start)
    broker = Broker(cash=args.cash, enable_trade_log=False)
    strat = AHPremiumQuarterlyStrategy(
        top_k=args.top,
        bottom_k=args.bottom,
        start_date=args.start,
        capital_split=args.capital_split,
    )
    engine = BacktestEngine(feed, broker, strat)
    curve = engine.run()

    ann = compute_annual_returns(curve)
    max_dd, dd_peak, dd_trough = compute_max_drawdown(curve)
    risk = compute_risk_metrics(curve, args.cash)

    print("Annual Returns:")
    for y, r in ann.items():
        print(f"  {y}: {r:.2%}")
    print(f"Max Drawdown: {max_dd:.2%} from {dd_peak} to {dd_trough}")
    print("Risk Metrics:")
    for k, v in risk.items():
        if k.endswith("Rate") or k in ("CAGR", "AnnReturn", "AnnVol", "Sharpe"):
            print(f"  {k}: {v:.4f}")
        else:
            print(f"  {k}: {v}")
    print(f"Final Equity: {curve[-1].equity:.2f}")


if __name__ == "__main__":
    main()
