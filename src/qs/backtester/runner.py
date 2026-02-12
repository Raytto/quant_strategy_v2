from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from qs.sqlite_utils import connect_sqlite

from .broker import Broker
from .data import Bar, DataFeed
from .defaults import DEFAULT_INITIAL_CASH
from .engine import BacktestEngine, Strategy
from .stats import compute_annual_returns, compute_max_drawdown, compute_risk_metrics


@dataclass(frozen=True)
class BacktestResult:
    initial_cash: float
    final_equity: float
    equity_curve: list[Any]
    broker: Broker
    annual_returns: Mapping[str, float]
    max_drawdown: float
    dd_peak: str | None
    dd_trough: str | None
    risk: Mapping[str, float]


def load_bars_from_sqlite(
    *,
    db_path: str | Path,
    table: str,
    ts_code: str,
    start_date: str,
    end_date: str | None = None,
) -> list[Bar]:
    where = ["ts_code = ?", "trade_date >= ?"]
    params: list[Any] = [ts_code, start_date]
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)
    sql = f"""
    SELECT trade_date, open, high, low, close, pct_chg
    FROM "{table}"
    WHERE {" AND ".join(where)}
    ORDER BY trade_date
    """
    con = connect_sqlite(db_path, read_only=True)
    try:
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()
    return [Bar(*r) for r in rows]


def load_calendar_bars_from_sqlite(
    *,
    db_path: str | Path,
    start_date: str,
    end_date: str | None = None,
    a_table: str = "daily_a",
    h_table: str = "daily_h",
) -> list[Bar]:
    """Load a combined trading calendar from A+H daily tables.

    Returns "calendar bars" where OHLC are placeholders aggregated from the
    underlying tables. Strategies can treat trade_date as the primary signal.
    """
    where = ["trade_date >= ?"]
    params: list[Any] = [start_date]
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)

    sql = f"""
    SELECT trade_date,
           MIN(open)  AS open,
           MIN(high)  AS high,
           MIN(low)   AS low,
           MIN(close) AS close,
           NULL       AS pct_chg
    FROM (
      SELECT trade_date, open, high, low, close FROM "{a_table}"
      UNION ALL
      SELECT trade_date, open, high, low, close FROM "{h_table}"
    )
    WHERE {" AND ".join(where)}
    GROUP BY 1
    ORDER BY 1
    """
    con = connect_sqlite(db_path, read_only=True)
    try:
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()
    return [Bar(*r) for r in rows]


def load_calendar_bars_for_symbols_from_sqlite(
    *,
    db_path: str | Path,
    table: str,
    symbols: Sequence[str],
    start_date: str,
    end_date: str | None = None,
) -> list[Bar]:
    """Load a multi-symbol trading calendar from a single OHLC table.

    This is commonly used for multi-asset strategies where the bar's `trade_date`
    is the primary driver, and marks/execution prices are loaded separately
    (e.g. via the strategy's own DB lookups).
    """
    syms = [str(s).strip() for s in symbols if str(s).strip()]
    if not syms:
        raise ValueError("symbols must not be empty")

    where = ["trade_date >= ?", f"ts_code IN ({','.join([repr(s) for s in syms])})"]
    params: list[Any] = [start_date]
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)

    sql = f"""
    SELECT trade_date,
           MIN(open)  AS open,
           MIN(high)  AS high,
           MIN(low)   AS low,
           MIN(close) AS close,
           NULL       AS pct_chg
    FROM "{table}"
    WHERE {" AND ".join(where)}
    GROUP BY 1
    ORDER BY 1
    """

    con = connect_sqlite(db_path, read_only=True)
    try:
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()
    return [Bar(*r) for r in rows]


def run_backtest(
    *,
    bars: list[Bar],
    strategy: Strategy,
    initial_cash: float = DEFAULT_INITIAL_CASH,
    symbol: str = "",
    enable_trade_log: bool = False,
    mark_error_policy: str = "warn",
) -> BacktestResult:
    feed = DataFeed(bars)
    broker = Broker(initial_cash, enable_trade_log=enable_trade_log, symbol=symbol)
    engine = BacktestEngine(
        feed, broker, strategy, mark_error_policy=mark_error_policy  # type: ignore[arg-type]
    )
    curve = engine.run()
    final_equity = curve[-1].equity if curve else initial_cash
    annual = compute_annual_returns(curve)
    max_dd, dd_peak, dd_trough = compute_max_drawdown(curve)
    risk = compute_risk_metrics(curve, initial_cash)
    return BacktestResult(
        initial_cash=initial_cash,
        final_equity=final_equity,
        equity_curve=curve,
        broker=broker,
        annual_returns=annual,
        max_drawdown=max_dd,
        dd_peak=dd_peak,
        dd_trough=dd_trough,
        risk=risk,
    )


def write_equity_curve_csv(curve: list[Any], out_path: str | Path) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["trade_date", "equity"])
        for p in curve:
            w.writerow([p.trade_date, f"{p.equity:.2f}"])


def write_trades_csv(broker: Broker, out_path: str | Path) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
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


def parse_json_kwargs(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("--strategy-kwargs must be a JSON object")
    return obj


def import_object(spec: str) -> Any:
    """Import an object from 'module:attr' or 'module.attr'."""
    if ":" in spec:
        mod, attr = spec.split(":", 1)
    elif "." in spec:
        mod, attr = spec.rsplit(".", 1)
    else:
        raise ValueError("strategy spec must be like 'qs.strategy.x:MyStrategy'")
    import importlib

    m = importlib.import_module(mod)
    return getattr(m, attr)


def build_strategy(strategy_spec: str, kwargs: Optional[dict[str, Any]] = None) -> Strategy:
    obj = import_object(strategy_spec)
    if callable(obj):
        return obj(**(kwargs or {}))  # type: ignore[return-value]
    raise TypeError(f"{strategy_spec} is not callable")
