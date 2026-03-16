from __future__ import annotations

from qs.backtester.market import PriceRequest, StrategyContext


class SimpleStrategy:
    """针对中国人寿 A 股 (601628.SH) 的简单策略:
    若上一个交易日涨幅 >= +1%, 当日开盘全仓买入 (若未持仓)
    若上一个交易日跌幅 <= -1%, 当日开盘全仓卖出 (若持仓)
    其余保持现状。
    使用新的类 backtrader 风格 API: order_target_percent / close
    """

    def __init__(self, ts_code: str = "601628.SH"):
        self.ts_code = ts_code
        self._open_request = PriceRequest(
            table="daily_a",
            field="open",
            exact=True,
        )
        self._close_request = PriceRequest(
            table="daily_a",
            field="close",
            exact=False,
        )

    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        ctx.set_mark_request(self._close_request)
        if ctx.signal_date is None:
            return
        row = ctx.history.get_dataset_values(
            table="daily_a",
            symbols=[self.ts_code],
            fields=["pct_chg"],
            trade_date=ctx.signal_date,
            exact=True,
        ).get(self.ts_code)
        if not row or row.get("pct_chg") is None:
            return
        pct_chg = float(row["pct_chg"])
        if pct_chg >= 1.0:
            ctx.rebalance_to_weights({self.ts_code: 1.0}, execution_request=self._open_request)
        elif pct_chg <= -1.0:
            ctx.rebalance_to_weights({}, execution_request=self._open_request)
