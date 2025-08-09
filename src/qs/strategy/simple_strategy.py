from __future__ import annotations
from qs.backtester.data import DataFeed, Bar
from qs.backtester.broker import Broker


class SimpleStrategy:
    """针对中国人寿 A 股 (601628.SH) 的简单策略:
    若上一个交易日涨幅 >= +1%, 当日开盘全仓买入 (若未持仓)
    若上一个交易日跌幅 <= -1%, 当日开盘全仓卖出 (若持仓)
    其余保持现状。
    使用新的类 backtrader 风格 API: order_target_percent / close
    """

    def __init__(self, ts_code: str = "601628.SH"):
        self.ts_code = ts_code

    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None:
        prev = feed.prev
        if prev and prev.pct_chg is not None:
            if prev.pct_chg >= 1.0:
                # 目标 100% 仓位
                broker.order_target_percent(bar.trade_date, bar.open, 1.0)
            elif prev.pct_chg <= -1.0:
                broker.close(bar.trade_date, bar.open)
        # 其余不操作
