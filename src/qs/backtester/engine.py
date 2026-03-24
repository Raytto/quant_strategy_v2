from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Protocol, Literal

from .data import DataFeed, Bar
from .broker import Broker
from .market import PortfolioView, SqliteMarketData, StrategyContext


class Strategy(Protocol):
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None: ...


@dataclass
class EquityPoint:
    trade_date: str
    equity: float


class BacktestEngine:
    def __init__(
        self,
        feed: DataFeed,
        broker: Broker,
        strategy: Strategy,
        *,
        mark_error_policy: Literal["raise", "warn", "ignore"] = "warn",
        require_marks_when_positions: bool = True,
        strict_missing_execution_prices: bool = True,
        market_data: SqliteMarketData | None = None,
    ):
        self.feed = feed
        self.broker = broker
        self.strategy = strategy
        self.mark_error_policy = mark_error_policy
        self.require_marks_when_positions = require_marks_when_positions
        self.strict_missing_execution_prices = strict_missing_execution_prices
        self.market_data = market_data
        self.equity_curve: List[EquityPoint] = []

    def _collect_marks(self, bar: Bar, ctx: StrategyContext | None = None) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        if ctx is not None and ctx.mark_prices is not None:
            marks.update({str(k): float(v) for k, v in ctx.mark_prices.items()})
        if (
            ctx is not None
            and not marks
            and ctx.mark_request is not None
            and self.market_data is not None
        ):
            held_symbols = [
                sym for sym, pos in self.broker.positions.items() if pos.size and pos.size > 0
            ]
            marks.update(
                self.market_data.get_price_map(
                    request=ctx.mark_request,
                    symbols=held_symbols,
                    trade_date=bar.trade_date,
                )
            )
        # Optional multi-symbol hook: provide symbol->price marks for equity
        mp = getattr(self.strategy, "mark_prices", None)
        if not marks and callable(mp):  # user supplied multi-symbol marks
            try:
                marks.update(mp(bar, self.feed, self.broker))
            except Exception as e:
                if self.mark_error_policy == "raise":
                    raise
                if self.mark_error_policy == "warn":
                    print(
                        f"[BacktestEngine] mark_prices error on {bar.trade_date}: {type(e).__name__}: {e}"
                    )
        # Ensure default symbol mark if single-symbol mode
        if self.broker.symbol and self.broker.symbol not in marks:
            marks[self.broker.symbol] = bar.close
        return marks

    def _build_context(self, bar: Bar) -> StrategyContext:
        history = (
            self.market_data.history(self.feed.prev.trade_date)  # type: ignore[union-attr]
            if self.market_data is not None and self.feed.prev is not None
            else self.market_data.history(None) if self.market_data is not None else None
        )
        if history is None:
            raise RuntimeError("strategy context requires market_data")
        return StrategyContext(
            trade_date=bar.trade_date,
            signal_date=self.feed.prev.trade_date if self.feed.prev is not None else None,
            history=history,
            reference=self.market_data.reference(),
            portfolio=PortfolioView(self.broker),
            market_data=self.market_data,
        )

    def _run_strategy(self, bar: Bar) -> StrategyContext | None:
        on_bar_ctx = getattr(self.strategy, "on_bar_ctx", None)
        if callable(on_bar_ctx):
            if self.market_data is None:
                raise RuntimeError("market_data is required for on_bar_ctx strategies")
            ctx = self._build_context(bar)
            on_bar_ctx(ctx)
            self._execute_context_orders(ctx)
            return ctx
        self.strategy.on_bar(bar, self.feed, self.broker)
        return None

    def _execute_context_orders(self, ctx: StrategyContext) -> None:
        for symbol, reason in ctx.write_offs:
            self.broker.force_write_off(ctx.trade_date, symbol, reason=reason)
        if ctx.target_weights is None:
            return
        price_map = dict(ctx.execution_prices or {})
        if not price_map:
            request = ctx.execution_request
            if request is None or self.market_data is None:
                raise RuntimeError("execution_request and market_data are required")
            symbols = sorted(set(ctx.target_weights.keys()) | set(self.broker.positions.keys()))
            price_map = self.market_data.get_price_map(
                request=request,
                symbols=symbols,
                trade_date=ctx.trade_date,
            )
        self.broker.rebalance_target_percents(
            ctx.trade_date,
            price_map,
            dict(ctx.target_weights),
            strict_missing_prices=self.strict_missing_execution_prices,
        )

    def run(self):
        self.feed.reset()
        self.equity_curve = []
        on_start = getattr(self.strategy, "on_start", None)
        on_end = getattr(self.strategy, "on_end", None)
        if callable(on_start):
            on_start(self.feed, self.broker)
        try:
            if len(self.feed) == 0:
                return self.equity_curve
            while True:
                bar = self.feed.current
                ctx = self._run_strategy(bar)
                # update marks (close based)
                marks = self._collect_marks(bar, ctx)
                if not marks:
                    if self.broker.symbol:
                        marks = {self.broker.symbol: bar.close}
                    elif self.require_marks_when_positions and any(
                        p.size for p in self.broker.positions.values()
                    ):
                        msg = (
                            f"[BacktestEngine] empty marks on {bar.trade_date} with open positions; "
                            "implement mark_prices() for multi-symbol strategies."
                        )
                        if self.mark_error_policy == "raise":
                            raise RuntimeError(msg)
                        if self.mark_error_policy == "warn":
                            print(msg)
                self.broker.update_marks(marks)
                self.equity_curve.append(
                    EquityPoint(bar.trade_date, self.broker.total_equity())
                )
                if not self.feed.step():
                    break
            return self.equity_curve
        finally:
            if callable(on_end):
                on_end(self.feed, self.broker)
