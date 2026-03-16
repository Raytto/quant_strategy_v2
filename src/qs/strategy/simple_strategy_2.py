from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from qs.backtester.market import PriceRequest, StrategyContext


@dataclass
class PairContext:
    """Deprecated compatibility shim for older scripts.

    Historical and current prices are now provided by `StrategyContext`.
    """

    h_open: Dict[str, float]
    h_pct: Dict[str, float]
    h_close: Dict[str, float]


class SimpleStrategy2:
    """A/H pair rotation example built on framework-managed market data."""

    def __init__(self, a_symbol: str, h_symbol: str, ctx: PairContext | None = None):
        self.a_symbol = a_symbol
        self.h_symbol = h_symbol
        self.compat_ctx = ctx
        self._a_open_request = PriceRequest(table="daily_a", field="open", exact=True)
        self._a_mark_request = PriceRequest(table="daily_a", field="close", exact=False)
        self._h_open_request = PriceRequest(table="daily_h", field="open", exact=True)
        self._h_mark_request = PriceRequest(table="daily_h", field="close", exact=False)

    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        mark_map = ctx.current_price_map(
            request=self._a_mark_request,
            symbols=[self.a_symbol],
        )
        h_rate = ctx.current_hk_to_cny_rate()
        if h_rate is not None:
            h_marks = ctx.current_price_map(
                request=self._h_mark_request,
                symbols=[self.h_symbol],
            )
            if self.h_symbol in h_marks:
                mark_map[self.h_symbol] = float(h_marks[self.h_symbol]) * h_rate
        if mark_map:
            ctx.set_mark_request(prices=mark_map)

        if ctx.signal_date is None:
            return

        a_row = ctx.history.get_dataset_values(
            table="daily_a",
            symbols=[self.a_symbol],
            fields=["pct_chg"],
            trade_date=ctx.signal_date,
            exact=True,
        ).get(self.a_symbol)
        h_row = ctx.history.get_dataset_values(
            table="daily_h",
            symbols=[self.h_symbol],
            fields=["pct_chg"],
            trade_date=ctx.signal_date,
            exact=True,
        ).get(self.h_symbol)
        if not a_row or not h_row:
            return
        a_chg = a_row.get("pct_chg")
        h_chg = h_row.get("pct_chg")
        if a_chg is None or h_chg is None or float(a_chg) == float(h_chg):
            return

        execution_prices = ctx.current_price_map(
            request=self._a_open_request,
            symbols=[self.a_symbol],
        )
        h_opens = ctx.current_price_map(
            request=self._h_open_request,
            symbols=[self.h_symbol],
        )
        if h_rate is None or self.h_symbol not in h_opens or self.a_symbol not in execution_prices:
            return
        execution_prices[self.h_symbol] = float(h_opens[self.h_symbol]) * h_rate

        want_a = float(h_chg) > float(a_chg)
        target = {self.a_symbol: 1.0} if want_a else {self.h_symbol: 1.0}
        ctx.rebalance_to_weights(target, execution_prices=execution_prices)
