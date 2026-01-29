from __future__ import annotations

from qs.backtester.broker import Broker
from qs.backtester.data import Bar, DataFeed
from qs.backtester.engine import BacktestEngine


class _NoOpStrategy:
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None:
        return


def test_engine_curve_len_matches_bars():
    bars = [
        Bar("20200102", 1.0, 1.0, 1.0, 1.0, None),
        Bar("20200103", 1.0, 1.0, 1.0, 1.0, None),
    ]
    feed = DataFeed(bars)
    broker = Broker(1_000_000.0, symbol="TEST.SYM")
    engine = BacktestEngine(feed, broker, _NoOpStrategy())

    curve = engine.run()

    assert len(curve) == len(bars)
    assert [p.trade_date for p in curve] == [b.trade_date for b in bars]

