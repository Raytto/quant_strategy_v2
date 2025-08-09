from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, Mapping
from qs.backtester.data import DataFeed, Bar
from qs.backtester.broker import Broker


@dataclass
class PairContext:
    h_open: Dict[str, float]
    h_pct: Dict[str, float]
    h_close: Dict[str, float]  # H 股收盘


class SimpleStrategy2:
    """A/H 轮动简单策略 (均值回归) 多标的雏形适配."""

    def __init__(self, a_symbol: str, h_symbol: str, ctx: PairContext):
        self.a_symbol = a_symbol
        self.h_symbol = h_symbol
        self.ctx = ctx

    # 提供估值价格: 返回当前两个标的的收盘价(或可用价)
    def mark_prices(self, bar: Bar, feed: DataFeed, broker: Broker) -> Mapping[str, float]:  # type: ignore[override]
        prices: Dict[str, float] = {self.a_symbol: bar.close}
        h_close = self.ctx.h_close.get(bar.trade_date)
        if h_close is not None:
            prices[self.h_symbol] = h_close
        return prices

    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None:
        prev: Optional[Bar] = feed.prev
        if prev is None:
            return
        d_prev = prev.trade_date
        if d_prev not in self.ctx.h_pct:
            return
        a_chg = prev.pct_chg
        h_chg = self.ctx.h_pct[d_prev]
        if a_chg is None or h_chg is None:
            return
        # 判定方向
        if h_chg == a_chg:
            return
        want_a = h_chg > a_chg  # H 更强 -> 做 A
        h_open = self.ctx.h_open.get(bar.trade_date)
        if h_open is None:
            return
        # 当前持仓情况
        a_pos = broker.positions.get(self.a_symbol, broker._get_position(self.a_symbol))
        h_pos = broker.positions.get(self.h_symbol, broker._get_position(self.h_symbol))
        # 我们采用互斥满仓：先平另一侧，再建立目标侧
        if want_a:
            if h_pos.size > 0:
                broker.sell_all_sym(bar.trade_date, self.h_symbol, h_open)
            if a_pos.size == 0:
                broker.buy_all_sym(bar.trade_date, self.a_symbol, bar.open)
        else:
            if a_pos.size > 0:
                broker.sell_all_sym(bar.trade_date, self.a_symbol, bar.open)
            if h_pos.size == 0:
                broker.buy_all_sym(bar.trade_date, self.h_symbol, h_open)
