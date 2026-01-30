from __future__ import annotations

import argparse
from pathlib import Path

from .runner import (
    build_strategy,
    load_calendar_bars_from_sqlite,
    load_bars_from_sqlite,
    parse_json_kwargs,
    run_backtest,
    write_equity_curve_csv,
    write_trades_csv,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="qs backtest runner (SQLite daily tables)")
    p.add_argument(
        "--feed",
        default="single",
        choices=["single", "calendar_ah"],
        help="Feed type: single-symbol table, or A/H combined trading calendar",
    )
    p.add_argument(
        "--strategy",
        required=True,
        help="Strategy constructor spec like 'qs.strategy.simple_strategy:SimpleStrategy'",
    )
    p.add_argument(
        "--strategy-kwargs",
        default="",
        help='JSON object for strategy kwargs, e.g. \'{\"ts_code\":\"601628.SH\"}\'',
    )
    p.add_argument("--db", type=Path, default=Path("data/data.sqlite"))
    p.add_argument("--table", default="daily_a", help="SQLite table (e.g. daily_a)")
    p.add_argument("--symbol", default="", help="ts_code for single-symbol feeds")
    p.add_argument("--start", default="20200101")
    p.add_argument("--end", default="")
    p.add_argument("--cash", type=float, default=1_000_000.0)
    p.add_argument("--log-trades", action="store_true")
    p.add_argument(
        "--mark-error-policy",
        default="warn",
        choices=["raise", "warn", "ignore"],
        help="How to handle mark_prices() errors",
    )
    p.add_argument("--out-dir", type=Path, default=Path("data/backtests"))
    p.add_argument("--tag", default="", help="Optional tag for output filenames")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    strategy = build_strategy(args.strategy, parse_json_kwargs(args.strategy_kwargs))

    if args.feed == "single":
        if not args.symbol:
            raise SystemExit("--symbol is required for --feed single")
        bars = load_bars_from_sqlite(
            db_path=args.db,
            table=args.table,
            ts_code=args.symbol,
            start_date=args.start,
            end_date=args.end or None,
        )
        default_tag = args.symbol.replace(".", "_")
        default_symbol = args.symbol
    else:
        bars = load_calendar_bars_from_sqlite(
            db_path=args.db,
            start_date=args.start,
            end_date=args.end or None,
        )
        default_tag = args.strategy.split(":")[-1].split(".")[-1]
        default_symbol = ""

    res = run_backtest(
        bars=bars,
        strategy=strategy,
        initial_cash=args.cash,
        symbol=default_symbol,
        enable_trade_log=bool(args.log_trades),
        mark_error_policy=args.mark_error_policy,
    )

    tag = args.tag or default_tag
    out_dir = args.out_dir
    write_equity_curve_csv(res.equity_curve, out_dir / f"equity_{tag}.csv")
    write_trades_csv(res.broker, out_dir / f"trades_{tag}.csv")

    print(
        f"Bars: {len(bars)}  Final Equity: {res.final_equity:.2f}  Return: {(res.final_equity/res.initial_cash-1)*100:.2f}%  Total Fees: {res.broker.total_fees:.2f}"
    )
    if res.annual_returns:
        print("Annual Returns:")
        for y, r in res.annual_returns.items():
            print(f"  {y}: {r*100:.2f}%")
    if res.dd_peak and res.dd_trough:
        print(f"Max Drawdown: {res.max_drawdown*100:.2f}%  Period: {res.dd_peak} -> {res.dd_trough}")
    if res.risk:
        print(
            f"CAGR: {res.risk['CAGR']*100:.2f}%  AnnReturn: {res.risk['AnnReturn']*100:.2f}%  AnnVol: {res.risk['AnnVol']*100:.2f}%  Sharpe: {res.risk['Sharpe']:.2f}  WinRate: {res.risk['WinRate']*100:.2f}%"
        )
    print(f"Saved: {out_dir}/equity_{tag}.csv and {out_dir}/trades_{tag}.csv")


if __name__ == "__main__":
    main()
