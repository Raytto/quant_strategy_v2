from __future__ import annotations

from qs.backtester.broker import Broker


def test_min_commission_applies_on_buy():
    broker = Broker(1_000.0, slippage=0.0, symbol="TEST.SYM")
    filled = broker.buy("20200102", price=10.0, size=1)

    assert filled == 1
    assert broker.trades[-1].action == "BUY"
    assert broker.trades[-1].fees == 5.0


def test_min_commission_applies_on_sell_commission_component():
    broker = Broker(1_000.0, slippage=0.0, symbol="TEST.SYM")
    broker.buy("20200102", price=10.0, size=1)
    broker.sell("20200103", price=10.0, size=1)

    last = broker.trades[-1]
    assert last.action == "SELL"
    # commission is min 5, plus tax (0.05% of gross)
    assert last.fees > 5.0
    assert abs(last.fees - 5.005) < 1e-9


def test_etf_sell_has_no_stamp_tax():
    broker = Broker(1_000.0, slippage=0.0, symbol="512050.SH")
    broker.buy("20200102", price=10.0, size=1)
    broker.sell("20200103", price=10.0, size=1)

    last = broker.trades[-1]
    assert last.action == "SELL"
    # ETF: no stamp duty; only commission (min 5)
    assert abs(last.fees - 5.0) < 1e-9


def test_rebalance_raises_when_execution_price_is_missing():
    broker = Broker(1_000.0, slippage=0.0)
    broker.buy_sym("20200102", "AAA.SZ", price=10.0, size=10)

    try:
        broker.rebalance_target_percents(
            "20200103",
            {"BBB.SZ": 10.0},
            {"BBB.SZ": 1.0},
        )
    except ValueError as exc:
        assert "AAA.SZ" in str(exc)
    else:
        raise AssertionError("expected missing execution price to raise ValueError")
