from __future__ import annotations

"""Annual equal-weight ETF allocation strategy (multi-symbol).

Strategy
  - Universe: a fixed list of ETF ts_code symbols (default: 5 ETFs).
  - Target: equal weights (20% each by default).
  - Rebalance: once per year (first trading day of each year in the feed calendar).

Pricing / adj_factor
  - Execution uses the current bar trade_date's ETF open price from `etf_daily`.
  - Valuation marks use ETF close prices from `etf_daily` (latest <= trade_date).
  - If use_adjusted=True, both open/close are adjusted by:
        adj_price = raw_price * adj_factor(trade_date) / base_adj_factor(symbol)
    where base_adj_factor is the latest available adj_factor for that symbol
    (i.e. "total return" style, normalized to the latest trade_date).

DB tables
  - etf_daily(ts_code, trade_date, open, close, ...)
  - adj_factor_etf(ts_code, trade_date, adj_factor, ...)
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from ..backtester.market import PriceRequest, StrategyContext


DEFAULT_ETFS: list[str] = [
    # Long-history proxies (aim: >= 8y backtest window).
    # Notes:
    # - Some newer ETFs have only ~1y data in the local DB, which shortens the
    #   overlap window when doing multi-asset backtests.
    # - These are similar exposures but with earlier listing dates / longer coverage.
    "159001.SZ",  # 现金/货币类: 易方达保证金货币A
    "159922.SZ",  # A股: 中证500ETF
    "159934.SZ",  # 商品: 黄金ETF
    "159941.SZ",  # 海外: 纳指100ETF(QDII)
    "159905.SZ",  # 因子: 红利ETF
]


@dataclass(frozen=True)
class RebalanceRecord:
    rebalance_date: str
    targets: Dict[str, float]
    open_prices: Dict[str, float]


class ETFEqualWeightAnnualStrategy:
    def __init__(
        self,
        *,
        db_path_raw: str = "data/data.sqlite",
        symbols: Sequence[str] = tuple(DEFAULT_ETFS),
        start_date: str = "20100101",
        use_adjusted: bool = True,
        rebalance_year_interval: int = 1,
    ):
        self.dbr = db_path_raw
        self.symbols = list(symbols)
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        self.start_date = start_date
        self.use_adjusted = bool(use_adjusted)
        self.rebalance_year_interval = max(1, int(rebalance_year_interval))
        self._last_rebalance_period: Optional[str] = None

        self.rebalance_history: List[RebalanceRecord] = []
        self._open_request = PriceRequest(
            table="etf_daily",
            field="open",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_etf" if self.use_adjusted else None,
            exact=True,
        )
        self._close_request = PriceRequest(
            table="etf_daily",
            field="close",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_etf" if self.use_adjusted else None,
            exact=False,
        )

    def get_rebalance_history(self) -> list[RebalanceRecord]:
        return list(self.rebalance_history)

    # --- helpers -------------------------------------------------------
    def _period_key(self, date_str: str) -> str:
        y = int(date_str[:4])
        # bucket years by interval: e.g. interval=2 => 2020,2021 share same bucket
        bucket = y // self.rebalance_year_interval
        return f"Y{bucket}"

    def _is_rebalance_day(self, trade_date: str) -> bool:
        if trade_date < self.start_date:
            return False
        pk = self._period_key(trade_date)
        return pk != self._last_rebalance_period

    # --- engine hooks --------------------------------------------------
    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        ctx.set_mark_request(self._close_request)
        if not self._is_rebalance_day(ctx.trade_date):
            return
        targets = {s: 1.0 / len(self.symbols) for s in self.symbols}
        ctx.rebalance_to_weights(targets, execution_request=self._open_request)
        self._last_rebalance_period = self._period_key(ctx.trade_date)
        self.rebalance_history.append(
            RebalanceRecord(
                rebalance_date=ctx.trade_date,
                targets=targets,
                open_prices={},
            )
        )


__all__ = ["ETFEqualWeightAnnualStrategy", "DEFAULT_ETFS", "RebalanceRecord"]
