from __future__ import annotations

"""Quarterly A/H premium mean-reversion allocation strategy.

Idea:
  - Universe: A/H dual-listed pairs defined in data/ah_codes.csv (already used in ah_premium processing)
  - For each quarter start trading day (first trading day whose month in {1,4,7,10} and previous bar month != current or prev date < start date),
      * Look at latest available premium snapshot (table ah_premium in data_processed.duckdb) for previous trading day.
      * Rank by premium_pct (A over H premium). Higher premium means A >> H relative (A expensive). We buy H (expect convergence) on high premium side.
      * Lowest premium (A cheap vs H) -> buy A.
      * Capital split: 50% allocated to top_k H-leg symbols equally; 50% to bottom_k A-leg symbols equally.
      * Close positions not in new selection via rebalance (weights not provided -> 0).
  - Hold until next quarter rebalance.

Simplifications:
  - Use previous trading day's premium snapshot to avoid lookahead (assuming premium table built after close with same-day prices + FX of that day).
  - Execution price uses current bar open price for selected side (A or H). We need those open prices available. Strategy relies on a price_loader callback to supply current open of each symbol.
  - Accepts parameters: top_k, bottom_k (default same k), start_date, min_price optional filters.
  - 货币换算: H 股价格以 HKD 报价, 回测资金以 CNY 计价。这里按 fx_daily 中 USD/CNH 与 USD/HKD 交叉得到 HKD→CNY 汇率, 对 H 股 open/close 做换算; 若当日缺失, 取最近不晚于 trade_date 的可用汇率。否则跳过再平衡以避免混合币种。

Dependencies:
  - DuckDB file data/data_processed.duckdb (table ah_premium)
  - Price provider capable of returning today's open for arbitrary symbol (A or H) for `price_map`.
  - fx_daily: ts_code IN ('USDCNH.FXCM','USDHKD.FXCM').

Usage in script:
  feed = DataFeed(bars_for_calendar_only) # can be a simple index daily bars to iterate trading days
  broker = Broker(cash=1_000_000, enable_trade_log=False)
  strat = AHPremiumQuarterlyStrategy(db_path_processed='data/data_processed.duckdb',
                                     db_path_raw='data/data.duckdb',
                                     top_k=5, bottom_k=5)
  engine = BacktestEngine(feed, broker, strat)
  curve = engine.run()

Note: feed bars drive the calendar; actual traded symbols are updated via mark_prices using last close (approx) or open. Here we set marks only for held symbols after each bar using provided price provider.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Sequence
import duckdb
from ..backtester.data import Bar, DataFeed
from ..backtester.broker import Broker
import time


@dataclass
class PremiumRecord:
    trade_date: str
    name: str
    cn_code: str
    hk_code: str
    premium_pct: float


def quarter_key(date_str: str) -> str:
    y = date_str[:4]
    m = int(date_str[4:6])
    q = (m - 1) // 3 + 1
    return f"{y}Q{q}"


class AHPremiumQuarterlyStrategy:
    def __init__(
        self,
        db_path_processed: str = "data/data_processed.duckdb",
        db_path_raw: str = "data/data.duckdb",
        top_k: int = 5,
        bottom_k: int = 5,
        start_date: str = "20180101",
        capital_split: float = 0.5,
        price_cache_days: int = 30,
        use_adjusted: bool = True,
    ):
        t0 = time.perf_counter()
        self.dbp = db_path_processed
        self.dbr = db_path_raw
        self.top_k = top_k
        self.bottom_k = bottom_k
        self.start_date = start_date
        self.capital_split = capital_split
        self.price_cache_days = price_cache_days
        self.use_adjusted = use_adjusted
        self._last_rebalance_quarter: Optional[str] = None
        self._latest_premium_date: Optional[str] = None
        self._open_cache: Dict[str, Dict[str, float]] = {}
        self._fx_cache: Dict[str, float] = {}
        self._max_adj_a: Dict[str, float] = {}
        self._max_adj_h: Dict[str, float] = {}
        if self.use_adjusted:
            con = duckdb.connect(self.dbr, read_only=True)
            for row in con.execute(
                "SELECT ts_code, MAX(adj_factor) FROM adj_factor_a GROUP BY ts_code"
            ).fetchall():
                self._max_adj_a[row[0]] = float(row[1])
            for row in con.execute(
                "SELECT ts_code, MAX(adj_factor) FROM adj_factor_h GROUP BY ts_code"
            ).fetchall():
                self._max_adj_h[row[0]] = float(row[1])
            con.close()
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] __init__ preload max_adj_factor: {t1-t0:.3f}s"
        )

    # --- FX helpers ---------------------------------------------------
    def _load_hk_to_cny_rate(self, trade_date: str) -> Optional[float]:
        if trade_date in self._fx_cache:
            return self._fx_cache[trade_date]
        con = duckdb.connect(self.dbr, read_only=True)
        # find latest date <= trade_date having both rates
        q = f"""
        WITH d AS (
          SELECT trade_date FROM fx_daily
          WHERE ts_code IN ('USDCNH.FXCM','USDHKD.FXCM') AND trade_date <= '{trade_date}'
          GROUP BY trade_date
          HAVING COUNT(DISTINCT ts_code)=2
          ORDER BY trade_date DESC
          LIMIT 1
        )
        SELECT d.trade_date,
               MAX(CASE WHEN f.ts_code='USDCNH.FXCM' THEN (f.bid_close+f.ask_close)/2 END) AS usd_cnh_mid,
               MAX(CASE WHEN f.ts_code='USDHKD.FXCM' THEN (f.bid_close+f.ask_close)/2 END) AS usd_hkd_mid
        FROM d JOIN fx_daily f ON f.trade_date=d.trade_date AND f.ts_code IN ('USDCNH.FXCM','USDHKD.FXCM')
        GROUP BY d.trade_date
        """
        row = con.execute(q).fetchone()
        con.close()
        if not row:
            return None
        _, usd_cnh, usd_hkd = row
        if usd_cnh is None or usd_hkd is None or usd_hkd == 0:
            return None
        rate = float(usd_cnh) / float(usd_hkd)  # 1 HKD -> CNY via USD cross
        self._fx_cache[trade_date] = rate
        return rate

    # --- data helpers -------------------------------------------------
    def _load_premium_for_date(self, trade_date: str) -> List[PremiumRecord]:
        t0 = time.perf_counter()
        hk_col = "hk_code"
        try:
            with open("data/ah_codes.csv", "r", encoding="utf-8") as f:
                header = f.readline().strip().split(",")
            if "hk_code" not in header and "c" in header:
                hk_col = "c"
        except Exception:
            pass
        con = duckdb.connect(self.dbr, read_only=True)
        mapping = con.execute(
            f"SELECT name, cn_code, {hk_col} AS hk_code FROM read_csv_auto('data/ah_codes.csv')"
        ).fetchall()
        a_codes = [row[1] for row in mapping]
        h_codes = [row[2] for row in mapping]
        a_raw = {
            row[0]: (float(row[1]), float(row[2]))
            for row in con.execute(
                f"SELECT ts_code, close, adj_factor FROM daily_a JOIN adj_factor_a USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in a_codes])})"
            ).fetchall()
        }
        h_raw = {
            row[0]: (float(row[1]), float(row[2]))
            for row in con.execute(
                f"SELECT ts_code, close, adj_factor FROM daily_h JOIN adj_factor_h USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in h_codes])})"
            ).fetchall()
        }
        fx_row = con.execute(
            f"SELECT (bid_close+ask_close)/2 FROM fx_daily WHERE ts_code='USDCNH.FXCM' AND trade_date='{trade_date}'"
        ).fetchone()
        usd_cnh_mid = float(fx_row[0]) if fx_row and fx_row[0] is not None else None
        fx_row = con.execute(
            f"SELECT (bid_close+ask_close)/2 FROM fx_daily WHERE ts_code='USDHKD.FXCM' AND trade_date='{trade_date}'"
        ).fetchone()
        usd_hkd_mid = float(fx_row[0]) if fx_row and fx_row[0] is not None else None
        con.close()
        if usd_cnh_mid is None or usd_hkd_mid is None:
            t1 = time.perf_counter()
            print(
                f"[AHPremiumQuarterlyStrategy] _load_premium_for_date({trade_date}) missing FX: {t1-t0:.3f}s"
            )
            return []
        hk_to_cny = usd_cnh_mid / usd_hkd_mid if usd_hkd_mid else None
        recs = []
        for name, cn_code, hk_code in mapping:
            if cn_code not in a_raw or hk_code not in h_raw:
                continue
            close_a, adj_a = a_raw[cn_code]
            close_h, adj_h = h_raw[hk_code]
            max_af_a = self._max_adj_a.get(cn_code, 1.0)
            max_af_h = self._max_adj_h.get(hk_code, 1.0)
            close_a_fq = close_a * adj_a / max_af_a
            close_h_hkd_fq = close_h * adj_h / max_af_h
            close_h_cny_fq = close_h_hkd_fq * hk_to_cny if hk_to_cny else None
            if close_h_cny_fq and close_h_cny_fq != 0:
                premium_pct = (close_a_fq / close_h_cny_fq - 1) * 100
                recs.append(
                    PremiumRecord(trade_date, name, cn_code, hk_code, premium_pct)
                )
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] _load_premium_for_date({trade_date}): {t1-t0:.3f}s, {len(recs)} records"
        )
        if recs:
            self._latest_premium_date = recs[0].trade_date
        return recs

    def _is_quarter_rebalance_day(self, bar: Bar, feed: DataFeed) -> bool:
        if bar.trade_date < self.start_date:
            return False
        qk = quarter_key(bar.trade_date)
        if qk != self._last_rebalance_quarter:
            # ensure first day of that quarter in the feed timeline
            # previous bar not same quarter
            if feed.prev is None or quarter_key(feed.prev.trade_date) != qk:
                return True
        return False

    # load open prices for target symbols for the current bar.trade_date, converting HKD->CNY and adjusting if needed
    def _load_opens(self, trade_date: str, symbols: Sequence[str]) -> Dict[str, float]:
        t0 = time.perf_counter()
        if trade_date in self._open_cache:
            cache = self._open_cache[trade_date]
            if all(s in cache for s in symbols):
                return {s: cache[s] for s in symbols}
        con = duckdb.connect(self.dbr, read_only=True)
        res: Dict[str, float] = {}
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        if a_syms:
            rows = con.execute(
                f"SELECT ts_code, open, adj_factor FROM daily_a JOIN adj_factor_a USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in a_syms])})"
            ).fetchall()
            for ts, open_, adj in rows:
                max_af = self._max_adj_a.get(ts, 1.0)
                res[ts] = float(open_) * float(adj) / max_af
        if h_syms:
            rows = con.execute(
                f"SELECT ts_code, open, adj_factor FROM daily_h JOIN adj_factor_h USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in h_syms])})"
            ).fetchall()
            rate = self._load_hk_to_cny_rate(trade_date)
            if rate is None:
                con.close()
                t1 = time.perf_counter()
                print(
                    f"[AHPremiumQuarterlyStrategy] _load_opens({trade_date}) missing FX: {t1-t0:.3f}s"
                )
                return {}
            for ts, open_, adj in rows:
                max_af = self._max_adj_h.get(ts, 1.0)
                res[ts] = float(open_) * float(adj) / max_af * rate
        con.close()
        self._open_cache.setdefault(trade_date, {}).update(res)
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] _load_opens({trade_date}): {t1-t0:.3f}s, {len(res)} symbols"
        )
        return res

    # mark held symbols to compute equity after close (use close price for valuation, converted)
    def mark_prices(self, bar: Bar, feed: DataFeed, broker: Broker):  # type: ignore[override]
        symbols = list(broker.positions.keys())
        if not symbols:
            return {}
        con = duckdb.connect(self.dbr, read_only=True)
        res: Dict[str, float] = {}
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        if a_syms:
            rows = con.execute(
                f"SELECT ts_code, close, adj_factor FROM daily_a JOIN adj_factor_a USING(ts_code,trade_date) WHERE trade_date='{bar.trade_date}' AND ts_code IN ({','.join([repr(x) for x in a_syms])})"
            ).fetchall()
            for ts, close_, adj in rows:
                max_af = self._max_adj_a.get(ts, 1.0)
                res[ts] = float(close_) * float(adj) / max_af
        if h_syms:
            rows = con.execute(
                f"SELECT ts_code, close, adj_factor FROM daily_h JOIN adj_factor_h USING(ts_code,trade_date) WHERE trade_date='{bar.trade_date}' AND ts_code IN ({','.join([repr(x) for x in h_syms])})"
            ).fetchall()
            rate = self._load_hk_to_cny_rate(bar.trade_date)
            if rate is None:
                con.close()
                return {}
            for ts, close_, adj in rows:
                max_af = self._max_adj_h.get(ts, 1.0)
                res[ts] = float(close_) * float(adj) / max_af * rate
        con.close()
        return res

    # --- core event ----------------------------------------------------
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker):
        t0 = time.perf_counter()
        if not self._is_quarter_rebalance_day(bar, feed):
            return
        prev_bar = feed.prev
        if prev_bar is None:
            return
        premium_recs = self._load_premium_for_date(prev_bar.trade_date)
        if not premium_recs:
            return
        sorted_recs = sorted(premium_recs, key=lambda r: r.premium_pct)
        bottom = sorted_recs[: self.bottom_k]
        top = sorted_recs[-self.top_k :]
        a_symbols = [r.cn_code for r in bottom]
        h_symbols = [r.hk_code for r in top]
        if not a_symbols or not h_symbols:
            return
        w_each_a = (1 - self.capital_split) / len(a_symbols) if a_symbols else 0
        w_each_h = self.capital_split / len(h_symbols) if h_symbols else 0
        targets = {
            **{s: w_each_a for s in a_symbols},
            **{s: w_each_h for s in h_symbols},
        }
        price_symbols = sorted(
            set(list(targets.keys()) + list(broker.positions.keys()))
        )
        price_map = self._load_opens(bar.trade_date, price_symbols)
        if not price_map:
            return
        broker.rebalance_target_percents(bar.trade_date, price_map, targets)
        self._last_rebalance_quarter = quarter_key(bar.trade_date)
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] on_bar({bar.trade_date}) rebalance: {t1-t0:.3f}s"
        )
