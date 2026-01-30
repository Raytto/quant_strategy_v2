from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Protocol, Mapping, Literal

from .data import DataFeed, Bar
from .broker import Broker


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
    ):
        self.feed = feed
        self.broker = broker
        self.strategy = strategy
        self.mark_error_policy = mark_error_policy
        self.require_marks_when_positions = require_marks_when_positions
        self.equity_curve: List[EquityPoint] = []

    def _collect_marks(self, bar: Bar) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        # Optional multi-symbol hook: provide symbol->price marks for equity
        mp = getattr(self.strategy, "mark_prices", None)
        if callable(mp):  # user supplied multi-symbol marks
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
                self.strategy.on_bar(bar, self.feed, self.broker)
                # update marks (close based)
                marks = self._collect_marks(bar)
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
