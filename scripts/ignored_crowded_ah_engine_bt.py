from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

import _bootstrap  # noqa: F401
from qs.backtester.broker import Broker
from qs.backtester.data import DataFeed
from qs.backtester.engine import BacktestEngine
from qs.backtester.stats import compute_annual_returns, compute_max_drawdown, compute_risk_metrics
from qs.strategy.ignored_crowded_ah_monthly import (
    FINAL_CONFIG,
    IgnoredCrowdedAHMonthlyStrategy,
    build_monthly_bars_from_panel,
    load_trade_panel,
)


DB_PATH = Path("data/data.sqlite")
PANEL_PATH = Path("data/backtests/ignored_buzz_ah/cache/trade_panel.pkl")
OUT_DIR = Path("data/backtests/ignored_buzz_ah_engine")


@dataclass(frozen=True)
class CurvePoint:
    trade_date: str
    equity: float


def to_curve(df: pd.DataFrame) -> list[CurvePoint]:
    return [CurvePoint(str(r.trade_date), float(r.equity)) for r in df.itertuples(index=False)]


def load_benchmark_monthly(con: sqlite3.Connection, start_date: str, end_date: str) -> pd.DataFrame:
    hs300 = pd.read_sql_query(
        f"""
        SELECT trade_date, ts_code, close
        FROM index_daily
        WHERE ts_code='000300.SH' AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """,
        con,
    )
    ixic = pd.read_sql_query(
        f"""
        SELECT trade_date, ts_code, close
        FROM index_global
        WHERE ts_code='IXIC' AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """,
        con,
    )
    raw = pd.concat([hs300, ixic], ignore_index=True)
    raw["trade_date"] = pd.to_datetime(raw["trade_date"], format="%Y%m%d")
    raw["ym"] = raw["trade_date"].dt.strftime("%Y%m")
    month_end = (
        raw.sort_values("trade_date")
        .groupby(["ts_code", "ym"], as_index=False)
        .tail(1)
        .sort_values(["ts_code", "trade_date"])
        .reset_index(drop=True)
    )
    month_end["nav"] = month_end.groupby("ts_code")["close"].transform(lambda s: s / s.iloc[0])
    month_end["trade_date"] = month_end["trade_date"].dt.strftime("%Y%m%d")
    return month_end[["ts_code", "trade_date", "nav"]]


def plot_curves(curve_df: pd.DataFrame, benchmarks: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.plot(curve_df["trade_date"], curve_df["equity_norm"], label="Strategy", linewidth=2.6, color="#b22222")
    for ts_code, label, color in [
        ("000300.SH", "CSI 300", "#1f77b4"),
        ("IXIC", "NASDAQ", "#2ca02c"),
    ]:
        bench = benchmarks.loc[benchmarks["ts_code"] == ts_code]
        if bench.empty:
            continue
        ax.plot(bench["trade_date"], bench["nav"], label=label, linewidth=1.8, color=color)
    ax.set_title("Ignored/Crowded A-H Monthly Strategy (Engine)")
    ax.set_ylabel("Normalized NAV")
    ax.set_xlabel("Month")
    ax.grid(alpha=0.25)
    ax.legend()
    tick_idx = pd.Series(range(len(curve_df))).astype(int)
    if len(tick_idx) > 0:
        sample = tick_idx.iloc[:: max(len(curve_df) // 10, 1)]
        ax.set_xticks(curve_df["trade_date"].iloc[sample])
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ignored/crowded A-H strategy with BacktestEngine.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--panel", type=Path, default=PANEL_PATH)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--cash", type=float, default=1_000_000.0)
    parser.add_argument("--start-date", default="20170731")
    parser.add_argument("--log-trades", action="store_true")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    panel = load_trade_panel(args.panel)
    bars = build_monthly_bars_from_panel(panel)
    feed = DataFeed(bars)
    strategy = IgnoredCrowdedAHMonthlyStrategy(
        panel_path=args.panel,
        start_date=args.start_date,
        config=FINAL_CONFIG,
    )
    broker = Broker(cash=args.cash, enable_trade_log=args.log_trades)
    engine = BacktestEngine(feed, broker, strategy, mark_error_policy="raise")
    curve = engine.run()

    curve_df = pd.DataFrame(
        [{"trade_date": p.trade_date, "equity": float(p.equity)} for p in curve]
    )
    state_df = pd.DataFrame(strategy.state_history)
    merged = curve_df.merge(state_df, on="trade_date", how="left")
    merged["position_count"] = merged["position_count"].fillna(0).astype(int)
    merged["holdings"] = merged["holdings"].fillna("")

    active = merged.loc[merged["position_count"] > 0].copy()
    metric_df = active if not active.empty else merged.copy()
    metric_curve = to_curve(metric_df[["trade_date", "equity"]])

    risk = compute_risk_metrics(metric_curve, initial_equity=args.cash, ann_factor=12)
    annual = compute_annual_returns(metric_curve)
    max_dd, dd_peak, dd_trough = compute_max_drawdown(metric_curve)

    merged["equity_norm"] = merged["equity"] / args.cash
    merged.to_csv(args.out_dir / "engine_equity_curve.csv", index=False)

    trades_df = pd.DataFrame(
        [
            {
                "trade_date": tr.trade_date,
                "action": tr.action,
                "symbol": tr.symbol,
                "price": tr.price,
                "exec_price": tr.exec_price,
                "size": tr.size,
                "gross_amount": tr.gross_amount,
                "fees": tr.fees,
                "cash_after": tr.cash_after,
                "position_after": tr.position_after,
                "equity_after": tr.equity_after,
            }
            for tr in broker.trades
        ]
    )
    trades_df.to_csv(args.out_dir / "engine_trades.csv", index=False)
    pd.DataFrame(strategy.rebalance_history).to_csv(
        args.out_dir / "engine_rebalance_history.csv",
        index=False,
    )

    con = sqlite3.connect(args.db)
    bench = load_benchmark_monthly(
        con,
        str(metric_df["trade_date"].iloc[0]),
        str(metric_df["trade_date"].iloc[-1]),
    )
    con.close()
    bench.to_csv(args.out_dir / "engine_benchmarks_monthly.csv", index=False)
    plot_curves(merged.loc[merged["trade_date"] >= str(metric_df["trade_date"].iloc[0])], bench, args.out_dir / "engine_strategy_vs_benchmarks.png")

    final_holdings = []
    for sym, pos in broker.positions.items():
        if pos.size <= 0:
            continue
        final_holdings.append(
            {
                "symbol": sym,
                "size": float(pos.size),
                "avg_price": float(pos.avg_price),
                "market_value": float(pos.size * broker.last_prices.get(sym, pos.avg_price)),
                "weight_in_strategy": float((pos.size * broker.last_prices.get(sym, pos.avg_price)) / broker.total_equity()) if broker.total_equity() > 0 else 0.0,
            }
        )

    summary: dict[str, Any] = {
        "framework": "qs.backtester.BacktestEngine + Broker",
        "config": asdict(FINAL_CONFIG),
        "initial_cash": args.cash,
        "final_equity": float(curve[-1].equity),
        "total_return": float(curve[-1].equity / args.cash - 1.0),
        "active_start": str(metric_df["trade_date"].iloc[0]),
        "active_end": str(metric_df["trade_date"].iloc[-1]),
        "active_months": int(len(metric_df)),
        "calendar_months": int(len(merged)),
        "annual_returns": dict(annual),
        "risk": risk,
        "max_drawdown": float(max_dd),
        "drawdown_peak": dd_peak,
        "drawdown_trough": dd_trough,
        "trade_count": int(len(trades_df)),
        "rebalance_count": int(len(strategy.rebalance_history)),
        "avg_positions": float(metric_df["position_count"].mean()),
        "final_holdings": final_holdings,
    }
    (args.out_dir / "engine_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
