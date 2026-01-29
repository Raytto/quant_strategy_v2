from __future__ import annotations

"""Quarterly (configurable) A/H premium mean-reversion allocation strategy.

Idea:
  - Universe: A/H dual-listed pairs defined in data/ah_codes.csv (already used in ah_premium processing)
  - For each quarter start trading day (first trading day whose month in {1,4,7,10} and previous bar month != current or prev date < start date),
      * Look at latest available premium snapshot (table ah_premium in data_processed.sqlite) for previous trading day.
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
  - SQLite file data/data_processed.sqlite (table ah_premium)
  - Price provider capable of returning today's open for arbitrary symbol (A or H) for `price_map`.
  - fx_daily: ts_code IN ('USDCNH.FXCM','USDHKD.FXCM').

Usage in script:
  feed = DataFeed(bars_for_calendar_only) # can be a simple index daily bars to iterate trading days
  broker = Broker(cash=1_000_000, enable_trade_log=False)
  strat = AHPremiumQuarterlyStrategy(db_path_processed='data/data_processed.sqlite',
                                     db_path_raw='data/data.sqlite',
                                     top_k=5, bottom_k=5)
  engine = BacktestEngine(feed, broker, strat)
  curve = engine.run()

Note: feed bars drive the calendar; actual traded symbols are updated via mark_prices using last close (approx) or open. Here we set marks only for held symbols after each bar using provided price provider.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Sequence, Any
import csv

from ..sqlite_utils import connect_sqlite
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
    a_close_raw: float  # A股原始收盘价 (CNY)
    h_close_raw_cny: float  # H股原始收盘价(已换算CNY)
    a_close_adj: float  # A股复权价(以统一基准因子归一)
    h_close_adj_cny: float  # H股复权价(换算CNY, 以统一基准因子归一)


def quarter_key(date_str: str) -> str:
    y = date_str[:4]
    m = int(date_str[4:6])
    q = (m - 1) // 3 + 1
    return f"{y}Q{q}"


class AHPremiumQuarterlyStrategy:
    def __init__(
        self,
        db_path_processed: str = "data/data_processed.sqlite",
        db_path_raw: str = "data/data.sqlite",
        top_k: int = 5,
        bottom_k: int = 5,
        start_date: str = "20180101",
        capital_split: float = 0.5,
        price_cache_days: int = 30,
        use_adjusted: bool = True,
        premium_use_adjusted: bool = False,  # 溢价计算是否使用复权价
        rebalance_month_interval: int = 3,
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
        self.premium_use_adjusted = premium_use_adjusted
        self.rebalance_month_interval = (
            rebalance_month_interval if rebalance_month_interval > 0 else 3
        )
        self._last_rebalance_quarter: Optional[str] = None
        self._last_rebalance_period: Optional[str] = None
        self._latest_premium_date: Optional[str] = None
        self._open_cache: Dict[str, Dict[str, float]] = {}
        self._fx_cache: Dict[str, float] = {}
        self._max_adj_a: Dict[str, float] = {}
        self._max_adj_h: Dict[str, float] = {}
        # per-symbol last adj_factor 基准
        self._base_adj_a: Dict[str, float] = {}
        self._base_adj_h: Dict[str, float] = {}
        self.rebalance_history: List[Dict[str, Any]] = []
        if self.use_adjusted or self.premium_use_adjusted:
            con = connect_sqlite(self.dbr, read_only=True)
            for row in con.execute(
                "SELECT ts_code, MAX(adj_factor) FROM adj_factor_a GROUP BY ts_code"
            ).fetchall():
                self._max_adj_a[row[0]] = float(row[1])
            for row in con.execute(
                "SELECT ts_code, MAX(adj_factor) FROM adj_factor_h GROUP BY ts_code"
            ).fetchall():
                self._max_adj_h[row[0]] = float(row[1])
            # 由每个 symbol 自身最后一个交易日的 adj_factor 作为 base
            for row in con.execute(
                """
                SELECT a.ts_code, a.adj_factor
                FROM adj_factor_a a
                JOIN (SELECT ts_code, MAX(trade_date) AS last_date FROM adj_factor_a GROUP BY ts_code) t
                  ON a.ts_code=t.ts_code AND a.trade_date=t.last_date
                """
            ).fetchall():
                self._base_adj_a[row[0]] = float(row[1])
            for row in con.execute(
                """
                SELECT h.ts_code, h.adj_factor
                FROM adj_factor_h h
                JOIN (SELECT ts_code, MAX(trade_date) AS last_date FROM adj_factor_h GROUP BY ts_code) t
                  ON h.ts_code=t.ts_code AND h.trade_date=t.last_date
                """
            ).fetchall():
                self._base_adj_h[row[0]] = float(row[1])
            con.close()
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] __init__ preload factors: {t1-t0:.3f}s (premium_use_adjusted={self.premium_use_adjusted}, interval={self.rebalance_month_interval}m, base=per-symbol-last)"
        )

    # 提供给 notebook: 全量再平衡历史
    def get_rebalance_history(self) -> List[Dict[str, Any]]:
        return self.rebalance_history

    # --- FX helpers ---------------------------------------------------
    def _load_hk_to_cny_rate(self, trade_date: str) -> Optional[float]:
        if trade_date in self._fx_cache:
            return self._fx_cache[trade_date]
        con = connect_sqlite(self.dbr, read_only=True)
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
        mapping: list[tuple[str, str, str]] = []
        with open("data/ah_codes.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return []
            if "hk_code" in reader.fieldnames:
                hk_col = "hk_code"
            elif "c" in reader.fieldnames:
                hk_col = "c"
            else:
                raise RuntimeError(
                    "ah_codes.csv missing hk_code column (expected hk_code or c)"
                )
            for row in reader:
                cn_code = (row.get("cn_code") or "").strip()
                hk_code = (row.get(hk_col) or "").strip()
                if not cn_code or not hk_code:
                    continue
                mapping.append(((row.get("name") or "").strip(), cn_code, hk_code))

        con = connect_sqlite(self.dbr, read_only=True)
        a_codes = [row[1] for row in mapping]
        h_codes = [row[2] for row in mapping]
        a_rows = con.execute(
            f"SELECT ts_code, close, adj_factor FROM daily_a JOIN adj_factor_a USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in a_codes])})"
        ).fetchall()
        h_rows = con.execute(
            f"SELECT ts_code, close, adj_factor FROM daily_h JOIN adj_factor_h USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in h_codes])})"
        ).fetchall()
        a_raw_map = {r[0]: (float(r[1]), float(r[2])) for r in a_rows}
        h_raw_map = {r[0]: (float(r[1]), float(r[2])) for r in h_rows}
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
        recs: List[PremiumRecord] = []
        for name, cn_code, hk_code in mapping:
            if cn_code not in a_raw_map or hk_code not in h_raw_map:
                continue
            close_a_raw, adj_a = a_raw_map[cn_code]
            close_h_raw_hkd, adj_h = h_raw_map[hk_code]
            # 原始价格(换算CNY后)
            h_close_raw_cny = close_h_raw_hkd * hk_to_cny if hk_to_cny else None
            if h_close_raw_cny is None or h_close_raw_cny == 0:
                continue
            # 复权（统一基准：latest date 的 adj_factor）
            base_a = self._base_adj_a.get(cn_code) or self._max_adj_a.get(cn_code, 1.0)
            base_h = self._base_adj_h.get(hk_code) or self._max_adj_h.get(hk_code, 1.0)
            if not base_a:
                base_a = 1.0
            if not base_h:
                base_h = 1.0
            a_close_adj = close_a_raw * adj_a / base_a
            h_close_adj_cny = (
                (close_h_raw_hkd * adj_h / base_h) * hk_to_cny if hk_to_cny else None
            )
            if h_close_adj_cny is None or h_close_adj_cny == 0:
                continue
            # 溢价：根据用户参数决定使用 raw 还是 adj
            if self.premium_use_adjusted:
                premium_pct = (a_close_adj / h_close_adj_cny - 1) * 100
            else:
                premium_pct = (close_a_raw / h_close_raw_cny - 1) * 100
            recs.append(
                PremiumRecord(
                    trade_date,
                    name,
                    cn_code,
                    hk_code,
                    premium_pct,
                    close_a_raw,
                    h_close_raw_cny,
                    a_close_adj,
                    h_close_adj_cny,
                )
            )
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] _load_premium_for_date({trade_date}): {t1-t0:.3f}s, {len(recs)} records (premium_use_adjusted={self.premium_use_adjusted})"
        )
        if recs:
            self._latest_premium_date = recs[0].trade_date
        return recs

    def _period_key(self, date_str: str) -> str:
        y = date_str[:4]
        m = int(date_str[4:6])
        p = (m - 1) // self.rebalance_month_interval
        return f"{y}P{p}"

    def _is_quarter_rebalance_day(
        self, bar: Bar, feed: DataFeed
    ) -> bool:  # 保留旧名以避免其他引用出错
        if bar.trade_date < self.start_date:
            return False
        pk = self._period_key(bar.trade_date)
        if pk != self._last_rebalance_period:
            if feed.prev is None or self._period_key(feed.prev.trade_date) != pk:
                return True
        return False

    # load open prices for target symbols for the current bar.trade_date, converting HKD->CNY and adjusting if needed
    def _load_opens(self, trade_date: str, symbols: Sequence[str]) -> Dict[str, float]:
        t0 = time.perf_counter()
        if trade_date in self._open_cache:
            cache = self._open_cache[trade_date]
            if all(s in cache for s in symbols):
                return {s: cache[s] for s in symbols}
        con = connect_sqlite(self.dbr, read_only=True)
        res: Dict[str, float] = {}
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        if a_syms:
            rows = con.execute(
                f"SELECT ts_code, open, adj_factor FROM daily_a JOIN adj_factor_a USING(ts_code,trade_date) WHERE trade_date='{trade_date}' AND ts_code IN ({','.join([repr(x) for x in a_syms])})"
            ).fetchall()
            for ts, open_, adj in rows:
                if self.use_adjusted:
                    base = self._base_adj_a.get(ts) or self._max_adj_a.get(ts, 1.0)
                    if base == 0:
                        base = 1.0
                    res[ts] = float(open_) * float(adj) / base
                else:
                    res[ts] = float(open_)
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
                if self.use_adjusted:
                    base = self._base_adj_h.get(ts) or self._max_adj_h.get(ts, 1.0)
                    if base == 0:
                        base = 1.0
                    local = float(open_) * float(adj) / base
                else:
                    local = float(open_)
                res[ts] = local * rate
        con.close()
        self._open_cache.setdefault(trade_date, {}).update(res)
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] _load_opens({trade_date}): {t1-t0:.3f}s, {len(res)} symbols (use_adjusted={self.use_adjusted})"
        )
        return res

    # mark held symbols to compute equity after close (use close price for valuation, converted)
    def mark_prices(self, bar: Bar, feed: DataFeed, broker: Broker):  # type: ignore[override]
        symbols = list(broker.positions.keys())
        if not symbols:
            return {}
        con = connect_sqlite(self.dbr, read_only=True)
        res: Dict[str, float] = {}
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        if a_syms:
            rows = con.execute(
                f"SELECT ts_code, close, adj_factor FROM daily_a JOIN adj_factor_a USING(ts_code,trade_date) WHERE trade_date='{bar.trade_date}' AND ts_code IN ({','.join([repr(x) for x in a_syms])})"
            ).fetchall()
            for ts, close_, adj in rows:
                if self.use_adjusted:
                    base = self._base_adj_a.get(ts) or self._max_adj_a.get(ts, 1.0)
                    if base == 0:
                        base = 1.0
                    res[ts] = float(close_) * float(adj) / base
                else:
                    res[ts] = float(close_)
        if h_syms:
            rows = con.execute(
                f"SELECT ts_code, close, adj_factor FROM daily_h JOIN adj_factor_h USING(ts_code,trade_date) WHERE trade_date='{bar.trade_date}' AND ts_code IN ({','.join([repr(x) for x in h_syms])})"
            ).fetchall()
            rate = self._load_hk_to_cny_rate(bar.trade_date)
            if rate is None:
                con.close()
                return {}
            for ts, close_, adj in rows:
                if self.use_adjusted:
                    base = self._base_adj_h.get(ts) or self._max_adj_h.get(ts, 1.0)
                    if base == 0:
                        base = 1.0
                    local = float(close_) * float(adj) / base
                else:
                    local = float(close_)
                res[ts] = local * rate
        con.close()
        return res

    # --- core event ----------------------------------------------------
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker):
        t0 = time.perf_counter()
        # 先检查已持仓标的是否在今日之后退市(或今日即退市)，若退市则按 0 价值核销
        if broker.positions:
            try:
                con = connect_sqlite(self.dbr, read_only=True)
                # 查询 A / H 基础表的 delist_date
                held_syms = list(broker.positions.keys())
                # 拆分 A/H
                a_syms = [
                    s for s in held_syms if s.endswith(".SH") or s.endswith(".SZ")
                ]
                h_syms = [s for s in held_syms if s.endswith(".HK")]
                rows: list[tuple[str, Optional[str]]] = []
                if a_syms:
                    q_a = f"SELECT ts_code, delist_date FROM stock_basic_a WHERE ts_code IN ({','.join([repr(x) for x in a_syms])})"
                    rows += con.execute(q_a).fetchall()
                if h_syms:
                    q_h = f"SELECT ts_code, delist_date FROM stock_basic_h WHERE ts_code IN ({','.join([repr(x) for x in h_syms])})"
                    rows += con.execute(q_h).fetchall()
                con.close()
                for ts_code, delist_date in rows:
                    if (
                        delist_date
                        and delist_date != ""
                        and delist_date <= bar.trade_date
                    ):
                        # 退市：强制清零
                        broker.force_write_off(
                            bar.trade_date, ts_code, reason=f"delist@{delist_date}"
                        )
            except Exception as e:
                print(f"[AHPremiumQuarterlyStrategy] delist check error: {e}")
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
        # --- 记录与输出决策 (包含原始价/复权价) ---
        decisions: List[Dict[str, Any]] = []
        for rec in bottom:
            decisions.append(
                {
                    "symbol": rec.cn_code,
                    "leg": "A",
                    "pair_name": rec.name,
                    "cn_code": rec.cn_code,
                    "hk_code": rec.hk_code,
                    "premium_pct": rec.premium_pct,
                    "target_weight": w_each_a,
                    "a_close_raw": rec.a_close_raw,
                    "h_close_raw_cny": rec.h_close_raw_cny,
                    "a_close_adj": rec.a_close_adj,
                    "h_close_adj_cny": rec.h_close_adj_cny,
                }
            )
        for rec in top:
            decisions.append(
                {
                    "symbol": rec.hk_code,
                    "leg": "H",
                    "pair_name": rec.name,
                    "cn_code": rec.cn_code,
                    "hk_code": rec.hk_code,
                    "premium_pct": rec.premium_pct,
                    "target_weight": w_each_h,
                    "a_close_raw": rec.a_close_raw,
                    "h_close_raw_cny": rec.h_close_raw_cny,
                    "a_close_adj": rec.a_close_adj,
                    "h_close_adj_cny": rec.h_close_adj_cny,
                }
            )
        self.rebalance_history.append(
            {
                "rebalance_date": bar.trade_date,
                "premium_date": prev_bar.trade_date,
                "decisions": decisions,
            }
        )
        print(
            f"[AHPremiumQuarterlyStrategy] rebalance {bar.trade_date} premium_date={prev_bar.trade_date} A_count={len(a_symbols)} H_count={len(h_symbols)}"
        )
        for d in decisions:
            print(
                "  symbol={symbol} leg={leg} prem={premium_pct:.2f}% wt={target_weight:.4f} "
                "A_raw={a_close_raw:.4f} H_raw={h_close_raw_cny:.4f} A_adj={a_close_adj:.4f} H_adj={h_close_adj_cny:.4f} pair={pair_name}".format(
                    **d
                )
            )
        broker.rebalance_target_percents(bar.trade_date, price_map, targets)
        self._last_rebalance_period = self._period_key(bar.trade_date)
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] on_bar({bar.trade_date}) rebalance done: {t1-t0:.3f}s"
        )
