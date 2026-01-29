from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Protocol, Mapping, Optional

from .data import DataFeed, Bar
from .broker import Broker


class Strategy(Protocol):
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None: ...

    # Optional multi-symbol hook: provide symbol->price marks for equity
    def mark_prices(self, bar: Bar, feed: DataFeed, broker: Broker) -> Mapping[str, float]: ...  # type: ignore[override]


@dataclass
class EquityPoint:
    trade_date: str
    equity: float


class BacktestEngine:
    def __init__(self, feed: DataFeed, broker: Broker, strategy: Strategy):
        self.feed = feed
        self.broker = broker
        self.strategy = strategy
        self.equity_curve: List[EquityPoint] = []

    def _collect_marks(self, bar: Bar) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        # Try strategy.mark_prices if exists
        mp = getattr(self.strategy, "mark_prices", None)
        if callable(mp):  # user supplied multi-symbol marks
            try:
                marks.update(mp(bar, self.feed, self.broker))
            except Exception:
                pass
        # Ensure default symbol mark if single-symbol mode
        if self.broker.symbol and self.broker.symbol not in marks:
            marks[self.broker.symbol] = bar.close
        return marks

    def run(self):
        self.feed.reset()
        self.equity_curve = []
        while True:
            bar = self.feed.current
            self.strategy.on_bar(bar, self.feed, self.broker)
            # update marks (close based)
            marks = self._collect_marks(bar)
            if not marks and self.broker.symbol:
                marks = {self.broker.symbol: bar.close}
            self.broker.update_marks(marks)
            self.equity_curve.append(
                EquityPoint(bar.trade_date, self.broker.total_equity())
            )
            if not self.feed.step():
                break
        return self.equity_curve
