from __future__ import annotations

"""Weekly min-premium ETF rotation strategy (single-holding, multi-symbol).

Universe
  - A fixed list of ETF ts_code symbols (e.g. 4 Nasdaq-100 ETFs).

Signal
  - Check once per week (default: Monday trading days only).
  - Select the ETF with the minimum `discount_rate` on the check date.
    (Tushare `fund_adj.discount_rate`, stored in SQLite `adj_factor_etf.discount_rate`.)

Trade rule
  - If the min-premium ETF differs from the current holding, and its discount_rate is
    lower than the current holding by at least `min_improvement` (unit: pct-pt, i.e. 1.0 == 1%),
    then switch the entire portfolio into the new ETF at today's open.

Pricing / adj_factor
  - Execution uses the current bar trade_date's ETF open price from `etf_daily`.
  - Valuation marks use ETF close prices from `etf_daily` (latest <= trade_date).
  - If use_adjusted=True, both open/close are adjusted by:
        adj_price = raw_price * adj_factor(trade_date) / base_adj_factor(symbol)
    where base_adj_factor is the latest available adj_factor for that symbol.

DB tables
  - etf_daily(ts_code, trade_date, open, close, ...)
  - adj_factor_etf(ts_code, trade_date, adj_factor, discount_rate, ...)
"""

from dataclasses import dataclass
import datetime as dt
from typing import Dict, List, Optional, Sequence

from ..backtester.market import PriceRequest, StrategyContext


@dataclass(frozen=True)
class WeeklyPremiumCheck:
    trade_date: str
    signal_date: str
    week_key: str
    held_symbol_before: str | None
    best_symbol: str
    held_discount_rate: float | None
    best_discount_rate: float
    improvement: float | None  # held - best
    switched: bool
    open_prices: Dict[str, float]
    discount_rates: Dict[str, float]


class ETFMinPremiumWeeklyStrategy:
    def __init__(
        self,
        *,
        db_path_raw: str = "data/data.sqlite",
        symbols: Sequence[str],
        start_date: str = "20100101",
        end_date: str | None = None,
        use_adjusted: bool = True,
        monday_only: bool = True,
        min_improvement: float = 1.0,
    ):
        self.symbols = [str(s).strip() for s in symbols if str(s).strip()]
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        self.start_date = str(start_date)
        self.end_date = str(end_date) if end_date else None

        self.use_adjusted = bool(use_adjusted)
        self.monday_only = bool(monday_only)
        self.min_improvement = float(min_improvement)

        self._last_checked_week: str | None = None

        self.check_history: List[WeeklyPremiumCheck] = []
        self._open_request = PriceRequest(
            table="etf_daily",
            field="open",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_etf" if self.use_adjusted else None,
            exact=True,
        )
        self._close_request = PriceRequest(
            table="etf_daily",
            field="close",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_etf" if self.use_adjusted else None,
            exact=False,
        )

    def get_check_history(self) -> list[WeeklyPremiumCheck]:
        return list(self.check_history)

    # --- date helpers --------------------------------------------------
    @staticmethod
    def _to_date(date_str: str) -> dt.date:
        return dt.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))

    def _week_key(self, date_str: str) -> str:
        d = self._to_date(date_str)
        y, w, _ = d.isocalendar()
        return f"{y}-W{int(w):02d}"

    def _is_monday(self, date_str: str) -> bool:
        return self._to_date(date_str).weekday() == 0

    # --- engine hooks --------------------------------------------------
    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        ctx.set_mark_request(self._close_request)
        if ctx.trade_date < self.start_date:
            return
        if self.end_date and ctx.trade_date > self.end_date:
            return

        week = self._week_key(ctx.trade_date)
        if self._last_checked_week == week:
            return

        if self.monday_only and not self._is_monday(ctx.trade_date):
            return

        if ctx.signal_date is None:
            return
        signal_date = ctx.signal_date

        # Use previous trading day's discount_rate as signal to avoid lookahead,
        # while executing the switch at today's open.
        raw_map = ctx.history.get_dataset_values(
            table="adj_factor_etf",
            symbols=self.symbols,
            fields=["discount_rate"],
            trade_date=signal_date,
            exact=True,
        )
        dr_map = {
            sym: float(values["discount_rate"])
            for sym, values in raw_map.items()
            if values.get("discount_rate") is not None
        }
        if any(s not in dr_map for s in self.symbols):
            return

        best_sym = min(self.symbols, key=lambda s: dr_map[s])
        held_sym = ctx.portfolio.largest_holding_symbol()

        held_dr = dr_map.get(held_sym) if held_sym else None
        best_dr = dr_map[best_sym]
        improvement = (held_dr - best_dr) if held_dr is not None else None

        switched = False
        if held_sym is None:
            ctx.rebalance_to_weights({best_sym: 1.0}, execution_request=self._open_request)
            switched = True
        elif held_sym != best_sym:
            if improvement is not None and improvement >= self.min_improvement:
                ctx.rebalance_to_weights(
                    {best_sym: 1.0},
                    execution_request=self._open_request,
                )
                switched = True

        self._last_checked_week = week
        self.check_history.append(
            WeeklyPremiumCheck(
                trade_date=ctx.trade_date,
                signal_date=signal_date,
                week_key=week,
                held_symbol_before=held_sym,
                best_symbol=best_sym,
                held_discount_rate=held_dr,
                best_discount_rate=best_dr,
                improvement=improvement,
                switched=switched,
                open_prices={},
                discount_rates=dr_map,
            )
        )


__all__ = ["ETFMinPremiumWeeklyStrategy", "WeeklyPremiumCheck"]
