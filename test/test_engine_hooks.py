from __future__ import annotations

from qs.backtester.broker import Broker
from qs.backtester.data import Bar, DataFeed
from qs.backtester.engine import BacktestEngine


class _HookStrategy:
    def __init__(self) -> None:
        self.started = 0
        self.ended = 0

    def on_start(self, feed: DataFeed, broker: Broker) -> None:
        self.started += 1

    def on_end(self, feed: DataFeed, broker: Broker) -> None:
        self.ended += 1

    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None:
        return


def test_engine_calls_on_start_and_on_end():
    bars = [
        Bar("20200102", 1.0, 1.0, 1.0, 1.0, None),
        Bar("20200103", 1.0, 1.0, 1.0, 1.0, None),
    ]
    strat = _HookStrategy()
    engine = BacktestEngine(DataFeed(bars), Broker(1_000_000.0, symbol="TEST.SYM"), strat)
    engine.run()
    assert strat.started == 1
    assert strat.ended == 1

