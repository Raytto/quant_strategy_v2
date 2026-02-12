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
from typing import Any, Dict, List, Optional, Sequence
import sqlite3

from ..sqlite_utils import connect_sqlite
from ..backtester.data import Bar, DataFeed
from ..backtester.broker import Broker


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

        self._con_raw: sqlite3.Connection | None = None
        self._last_rebalance_period: Optional[str] = None
        self._open_cache: Dict[str, Dict[str, float]] = {}

        self._base_adj: Dict[str, float] = {}
        if self.use_adjusted:
            con = connect_sqlite(self.dbr, read_only=True)
            try:
                self._base_adj = self._load_latest_adj_factors(
                    con, "adj_factor_etf", self.symbols
                )
            finally:
                con.close()

        self.rebalance_history: List[RebalanceRecord] = []

    # --- lifecycle -----------------------------------------------------
    def _raw_con(self) -> sqlite3.Connection:
        if self._con_raw is None:
            self._con_raw = connect_sqlite(self.dbr, read_only=True)
        return self._con_raw

    def on_start(self, feed: DataFeed, broker: Broker) -> None:
        self._raw_con()

    def on_end(self, feed: DataFeed, broker: Broker) -> None:
        if self._con_raw is not None:
            try:
                self._con_raw.close()
            finally:
                self._con_raw = None

    def get_rebalance_history(self) -> list[RebalanceRecord]:
        return list(self.rebalance_history)

    # --- helpers -------------------------------------------------------
    def _period_key(self, date_str: str) -> str:
        y = int(date_str[:4])
        # bucket years by interval: e.g. interval=2 => 2020,2021 share same bucket
        bucket = y // self.rebalance_year_interval
        return f"Y{bucket}"

    def _is_rebalance_day(self, bar: Bar) -> bool:
        if bar.trade_date < self.start_date:
            return False
        pk = self._period_key(bar.trade_date)
        return pk != self._last_rebalance_period

    @staticmethod
    def _load_latest_adj_factors(
        con: sqlite3.Connection, table: str, symbols: Sequence[str]
    ) -> Dict[str, float]:
        if not symbols:
            return {}
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", [table]
        ).fetchone()
        if row is None:
            return {}
        in_list = ",".join([repr(s) for s in symbols])
        q = f"""
        SELECT a.ts_code, a.adj_factor
        FROM {table} a
        JOIN (SELECT ts_code, MAX(trade_date) AS last_date FROM {table} GROUP BY ts_code) t
          ON a.ts_code=t.ts_code AND a.trade_date=t.last_date
        WHERE a.ts_code IN ({in_list})
        """
        return {ts: float(adj) for ts, adj in con.execute(q).fetchall() if adj is not None}

    def _adjust_px(self, ts_code: str, px: float, adj_factor: float) -> float:
        if not self.use_adjusted:
            return px
        base = self._base_adj.get(ts_code) or 1.0
        if base == 0:
            base = 1.0
        return float(px) * float(adj_factor) / float(base)

    def _load_opens(self, trade_date: str, symbols: Sequence[str]) -> Dict[str, float]:
        if trade_date in self._open_cache:
            cache = self._open_cache[trade_date]
            if all(s in cache for s in symbols):
                return {s: cache[s] for s in symbols}

        con = self._raw_con()
        in_list = ",".join([repr(s) for s in symbols])
        rows = con.execute(
            f"""
            SELECT d.ts_code, d.open, COALESCE(af.adj_factor, 1.0) AS adj_factor
            FROM etf_daily d
            LEFT JOIN adj_factor_etf af USING(ts_code, trade_date)
            WHERE d.trade_date=? AND d.ts_code IN ({in_list})
            """,
            [trade_date],
        ).fetchall()

        out: Dict[str, float] = {}
        for ts, open_, adj in rows:
            if open_ is None:
                continue
            px = self._adjust_px(str(ts), float(open_), float(adj))
            if px > 0:
                out[str(ts)] = px
        self._open_cache.setdefault(trade_date, {}).update(out)
        return out

    def _load_marks_close(self, trade_date: str, symbols: Sequence[str]) -> Dict[str, float]:
        if not symbols:
            return {}
        con = self._raw_con()
        in_list = ",".join([repr(s) for s in symbols])
        rows = con.execute(
            f"""
            WITH last AS (
              SELECT ts_code, MAX(trade_date) AS trade_date
              FROM etf_daily
              WHERE ts_code IN ({in_list}) AND trade_date <= ?
              GROUP BY ts_code
            )
            SELECT d.ts_code, d.close, COALESCE(af.adj_factor, 1.0) AS adj_factor
            FROM last l
            JOIN etf_daily d
              ON d.ts_code=l.ts_code AND d.trade_date=l.trade_date
            LEFT JOIN adj_factor_etf af
              ON af.ts_code=d.ts_code AND af.trade_date=d.trade_date
            """,
            [trade_date],
        ).fetchall()

        out: Dict[str, float] = {}
        for ts, close_, adj in rows:
            if close_ is None:
                continue
            px = self._adjust_px(str(ts), float(close_), float(adj))
            if px > 0:
                out[str(ts)] = px
        return out

    # --- engine hooks --------------------------------------------------
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None:
        if not self._is_rebalance_day(bar):
            return
        targets = {s: 1.0 / len(self.symbols) for s in self.symbols}
        open_map = self._load_opens(bar.trade_date, self.symbols)
        if any(s not in open_map for s in self.symbols):
            # Retry on next bar within the same year bucket if data is missing.
            return
        broker.rebalance_target_percents(bar.trade_date, open_map, targets)
        self._last_rebalance_period = self._period_key(bar.trade_date)
        self.rebalance_history.append(
            RebalanceRecord(
                rebalance_date=bar.trade_date,
                targets=targets,
                open_prices=open_map,
            )
        )

    def mark_prices(self, bar: Bar, feed: DataFeed, broker: Broker) -> Dict[str, float]:  # type: ignore[override]
        symbols = list(broker.positions.keys())
        return self._load_marks_close(bar.trade_date, symbols)


__all__ = ["ETFEqualWeightAnnualStrategy", "DEFAULT_ETFS", "RebalanceRecord"]
