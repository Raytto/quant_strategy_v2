from __future__ import annotations

from qs.backtester.data import Bar
from qs.backtester.defaults import DEFAULT_INITIAL_CASH
from qs.backtester.runner import run_backtest


class _NoOpStrategy:
    def on_bar(self, bar: Bar, feed, broker) -> None:  # noqa: ANN001
        return


def test_run_backtest_default_initial_cash_is_1m():
    bars = [Bar("20200102", 1.0, 1.0, 1.0, 1.0, None)]
    res = run_backtest(bars=bars, strategy=_NoOpStrategy())

    assert res.initial_cash == DEFAULT_INITIAL_CASH
    assert res.broker.cash == DEFAULT_INITIAL_CASH

