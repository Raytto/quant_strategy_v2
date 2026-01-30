from __future__ import annotations

"""Quarterly (configurable) Low-PE allocation strategy for A + H shares.

What it does
  - A-leg: pick the lowest-PE A-shares (from `bak_daily_a.pe`) top `a_k`
  - H-leg: TuShare hk_daily doesn't provide PE in this DB. For dual-listed A/H pairs
           (from `data/ah_codes.csv`), we *imply* H-share PE from the A-share PE:

        A_PE = A_price(CNY) / EPS(CNY)
        H_PE = H_price(HKD)*HKD→CNY / EPS(CNY)
             = A_PE * (H_price(HKD)*HKD→CNY / A_price(CNY))

    FX: HKD→CNY is derived via USD-cross from `fx_daily`:
        HKD→CNY = (USD/CNH) / (USD/HKD)

Rebalance
  - Rebalance every `rebalance_month_interval` months (default: 3).
  - Signal uses *previous* trading day's close data (T-1) to avoid lookahead.
  - Execute at current bar's open price (T open). H open is converted HKD→CNY.

Notes / limitations
  - H-leg universe is limited to dual-listed pairs (ah_codes.csv) with all required
    data available: A PE, A close, H close, and FX.
  - Strategy currency is CNY. Benchmarks in notebooks are normalized only.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import csv
import sqlite3
import time

from ..backtester.broker import Broker
from ..backtester.data import Bar, DataFeed
from ..sqlite_utils import connect_sqlite


@dataclass(frozen=True)
class LowPERecord:
    trade_date: str
    leg: str  # "A" | "H"
    symbol: str
    stock_name: str
    pe: float
    # diagnostics (for H implied PE)
    a_symbol: str | None = None
    h_symbol: str | None = None
    a_pe: float | None = None
    a_close_cny: float | None = None
    h_close_hkd: float | None = None
    hk_to_cny: float | None = None


class LowPEQuarterlyStrategy:
    def __init__(
        self,
        *,
        db_path_raw: str = "data/data.sqlite",
        pairs_csv_path: str | Path = "data/ah_codes.csv",
        a_k: int = 5,
        h_k: int = 5,
        start_date: str = "20180101",
        rebalance_month_interval: int = 3,
        pe_min: float = 0.0,
        candidate_limit: int = 300,
        use_adjusted: bool = True,
    ):
        self.dbr = db_path_raw
        self.pairs_csv_path = Path(pairs_csv_path)
        self.a_k = max(0, int(a_k))
        self.h_k = max(0, int(h_k))
        self.start_date = start_date
        self.rebalance_month_interval = (
            int(rebalance_month_interval) if rebalance_month_interval > 0 else 3
        )
        self.pe_min = float(pe_min)
        self.candidate_limit = max(50, int(candidate_limit))
        self.use_adjusted = bool(use_adjusted)

        self._con_raw: sqlite3.Connection | None = None
        self._last_rebalance_period: Optional[str] = None

        self._pairs: list[tuple[str, str, str]] = self._load_pairs(self.pairs_csv_path)
        self._fx_cache: Dict[str, float] = {}
        self._open_cache: Dict[str, Dict[str, float]] = {}

        # Lazy-loaded "per-symbol last adj_factor" bases (for total-return style)
        self._base_adj_a: Dict[str, float] = {}
        self._base_adj_h: Dict[str, float] = {}

        self.rebalance_history: List[Dict[str, Any]] = []
        print(
            f"[LowPEQuarterlyStrategy] init pairs={len(self._pairs)} "
            f"(a_k={self.a_k}, h_k={self.h_k}, interval={self.rebalance_month_interval}m, use_adjusted={self.use_adjusted})"
        )

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

    # --- helpers -------------------------------------------------------
    @staticmethod
    def _load_pairs(csv_path: Path) -> list[tuple[str, str, str]]:
        resolved = csv_path
        if not resolved.exists() and not resolved.is_absolute():
            # Notebook kernels often run with cwd=notebooks/, so resolve relative paths
            # against repo root (…/src/qs/strategy/ -> repo root is parents[3]).
            try:
                repo_root = Path(__file__).resolve().parents[3]
                candidate = repo_root / resolved
                if candidate.exists():
                    resolved = candidate
            except Exception:
                pass
        if not resolved.exists():
            print(f"[LowPEQuarterlyStrategy] missing pairs csv: {csv_path}")
            return []
        with resolved.open("r", encoding="utf-8-sig", newline="") as f:
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
            out: list[tuple[str, str, str]] = []
            for row in reader:
                name = (row.get("name") or "").strip()
                a_code = (row.get("cn_code") or "").strip()
                h_code = (row.get(hk_col) or "").strip()
                if not a_code or not h_code:
                    continue
                out.append((name, a_code, h_code))
            return out

    def _period_key(self, date_str: str) -> str:
        y = date_str[:4]
        m = int(date_str[4:6])
        p = (m - 1) // self.rebalance_month_interval
        return f"{y}P{p}"

    def _is_rebalance_day(self, bar: Bar, feed: DataFeed) -> bool:
        if bar.trade_date < self.start_date:
            return False
        pk = self._period_key(bar.trade_date)
        # Retry policy: if a rebalance attempt fails (e.g. H market closed / missing FX),
        # keep trying on subsequent bars within the same period until we succeed.
        return pk != self._last_rebalance_period

    # --- FX (USD-cross) ------------------------------------------------
    def _load_hk_to_cny_rate(self, trade_date: str) -> Optional[float]:
        if trade_date in self._fx_cache:
            return self._fx_cache[trade_date]
        con = self._raw_con()
        q = f"""
        WITH d AS (
          SELECT trade_date
          FROM fx_daily
          WHERE ts_code IN ('USDCNH.FXCM','USDHKD.FXCM') AND trade_date <= '{trade_date}'
          GROUP BY trade_date
          HAVING COUNT(DISTINCT ts_code)=2
          ORDER BY trade_date DESC
          LIMIT 1
        )
        SELECT d.trade_date,
               MAX(CASE WHEN f.ts_code='USDCNH.FXCM' THEN (f.bid_close+f.ask_close)/2 END) AS usd_cnh_mid,
               MAX(CASE WHEN f.ts_code='USDHKD.FXCM' THEN (f.bid_close+f.ask_close)/2 END) AS usd_hkd_mid
        FROM d
        JOIN fx_daily f
          ON f.trade_date=d.trade_date AND f.ts_code IN ('USDCNH.FXCM','USDHKD.FXCM')
        GROUP BY d.trade_date
        """
        row = con.execute(q).fetchone()
        if not row:
            return None
        _, usd_cnh, usd_hkd = row
        if usd_cnh is None or usd_hkd is None or float(usd_hkd) == 0.0:
            return None
        rate = float(usd_cnh) / float(usd_hkd)  # 1 HKD -> CNY
        self._fx_cache[trade_date] = rate
        return rate

    def _latest_trade_date(self, table: str, on_or_before: str) -> Optional[str]:
        con = self._raw_con()
        row = con.execute(
            f"SELECT MAX(trade_date) FROM {table} WHERE trade_date <= ?", [on_or_before]
        ).fetchone()
        if not row or not row[0]:
            return None
        return str(row[0])

    # --- adj_factor base loading (lazy) --------------------------------
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
        return {
            ts: float(adj)
            for ts, adj in con.execute(q).fetchall()
            if adj is not None
        }

    def _ensure_base_adj_a(self, symbols: Sequence[str]) -> None:
        if not self.use_adjusted or not symbols:
            return
        need = [s for s in symbols if s not in self._base_adj_a]
        if not need:
            return
        con = self._raw_con()
        self._base_adj_a.update(self._load_latest_adj_factors(con, "adj_factor_a", need))

    def _ensure_base_adj_h(self, symbols: Sequence[str]) -> None:
        if not self.use_adjusted or not symbols:
            return
        need = [s for s in symbols if s not in self._base_adj_h]
        if not need:
            return
        con = self._raw_con()
        self._base_adj_h.update(self._load_latest_adj_factors(con, "adj_factor_h", need))

    # --- price loaders -------------------------------------------------
    def _load_opens(self, trade_date: str, symbols: Sequence[str]) -> Dict[str, float]:
        t0 = time.perf_counter()
        if trade_date in self._open_cache:
            cache = self._open_cache[trade_date]
            if all(s in cache for s in symbols):
                return {s: cache[s] for s in symbols}

        con = self._raw_con()
        res: Dict[str, float] = {}
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]

        if a_syms:
            self._ensure_base_adj_a(a_syms)
            rows = con.execute(
                f"""
                SELECT a.ts_code, a.open, COALESCE(af.adj_factor, 1.0) AS adj_factor
                FROM daily_a a
                LEFT JOIN adj_factor_a af USING(ts_code, trade_date)
                WHERE a.trade_date='{trade_date}' AND a.ts_code IN ({','.join([repr(x) for x in a_syms])})
                """
            ).fetchall()
            for ts, open_, adj in rows:
                if open_ is None:
                    continue
                if self.use_adjusted:
                    base = self._base_adj_a.get(ts) or 1.0
                    if base == 0:
                        base = 1.0
                    res[ts] = float(open_) * float(adj) / base
                else:
                    res[ts] = float(open_)

        if h_syms:
            self._ensure_base_adj_h(h_syms)
            rows = con.execute(
                f"""
                SELECT h.ts_code, h.open, COALESCE(af.adj_factor, 1.0) AS adj_factor
                FROM daily_h h
                LEFT JOIN adj_factor_h af USING(ts_code, trade_date)
                WHERE h.trade_date='{trade_date}' AND h.ts_code IN ({','.join([repr(x) for x in h_syms])})
                """
            ).fetchall()
            rate = self._load_hk_to_cny_rate(trade_date)
            if rate is None:
                t1 = time.perf_counter()
                print(
                    f"[LowPEQuarterlyStrategy] _load_opens({trade_date}) missing FX: {t1-t0:.3f}s"
                )
                return {}
            for ts, open_, adj in rows:
                if open_ is None:
                    continue
                if self.use_adjusted:
                    base = self._base_adj_h.get(ts) or 1.0
                    if base == 0:
                        base = 1.0
                    local = float(open_) * float(adj) / base
                else:
                    local = float(open_)
                res[ts] = local * rate

        self._open_cache.setdefault(trade_date, {}).update(res)
        t1 = time.perf_counter()
        print(
            f"[LowPEQuarterlyStrategy] _load_opens({trade_date}): {t1-t0:.3f}s, {len(res)} symbols (use_adjusted={self.use_adjusted})"
        )
        return res

    def mark_prices(self, bar: Bar, feed: DataFeed, broker: Broker):  # type: ignore[override]
        symbols = list(broker.positions.keys())
        if not symbols:
            return {}
        con = self._raw_con()
        res: Dict[str, float] = {}
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]

        if a_syms:
            self._ensure_base_adj_a(a_syms)
            rows = con.execute(
                f"""
                SELECT a.ts_code, a.close, COALESCE(af.adj_factor, 1.0) AS adj_factor
                FROM daily_a a
                LEFT JOIN adj_factor_a af USING(ts_code, trade_date)
                WHERE a.trade_date='{bar.trade_date}' AND a.ts_code IN ({','.join([repr(x) for x in a_syms])})
                """
            ).fetchall()
            for ts, close_, adj in rows:
                if close_ is None:
                    continue
                if self.use_adjusted:
                    base = self._base_adj_a.get(ts) or 1.0
                    if base == 0:
                        base = 1.0
                    res[ts] = float(close_) * float(adj) / base
                else:
                    res[ts] = float(close_)

        if h_syms:
            self._ensure_base_adj_h(h_syms)
            rows = con.execute(
                f"""
                SELECT h.ts_code, h.close, COALESCE(af.adj_factor, 1.0) AS adj_factor
                FROM daily_h h
                LEFT JOIN adj_factor_h af USING(ts_code, trade_date)
                WHERE h.trade_date='{bar.trade_date}' AND h.ts_code IN ({','.join([repr(x) for x in h_syms])})
                """
            ).fetchall()
            rate = self._load_hk_to_cny_rate(bar.trade_date)
            if rate is None:
                return res
            for ts, close_, adj in rows:
                if close_ is None:
                    continue
                if self.use_adjusted:
                    base = self._base_adj_h.get(ts) or 1.0
                    if base == 0:
                        base = 1.0
                    local = float(close_) * float(adj) / base
                else:
                    local = float(close_)
                res[ts] = local * rate
        # If some markets are closed on this bar date (calendar is A∪H),
        # keep last known marks to avoid "empty marks with open positions".
        for sym in symbols:
            if sym in res:
                continue
            last = broker.last_prices.get(sym)
            if last is not None:
                res[sym] = float(last)
            else:
                pos = broker.positions.get(sym)
                if pos and pos.avg_price > 0:
                    res[sym] = float(pos.avg_price)
        return res

    # --- selection logic ----------------------------------------------
    def _pick_low_pe_a(
        self, *, signal_date: str, execute_date: str, k: int
    ) -> list[LowPERecord]:
        if k <= 0:
            return []
        sd = self._latest_trade_date("daily_a", signal_date)
        if sd is None:
            return []
        con = self._raw_con()

        limit = self.candidate_limit
        chosen: list[LowPERecord] = []
        while True:
            rows = con.execute(
                """
                SELECT b.ts_code, s.name, b.pe
                FROM bak_daily_a b
                JOIN stock_basic_a s ON s.ts_code=b.ts_code
                WHERE b.trade_date=?
                  AND b.pe IS NOT NULL
                  AND b.pe > ?
                  AND (s.list_date IS NULL OR s.list_date='' OR s.list_date <= ?)
                  AND (s.delist_date IS NULL OR s.delist_date='' OR s.delist_date > ?)
                ORDER BY b.pe ASC
                LIMIT ?
                """,
                [sd, self.pe_min, sd, execute_date, limit],
            ).fetchall()
            if not rows:
                return []
            cand_syms = [r[0] for r in rows]
            tradeable = {
                ts
                for (ts,) in con.execute(
                    f"""
                    SELECT ts_code
                    FROM daily_a
                    WHERE trade_date=?
                      AND ts_code IN ({",".join([repr(x) for x in cand_syms])})
                      AND open IS NOT NULL
                      AND open > 0
                    """,
                    [execute_date],
                ).fetchall()
            }
            chosen = []
            for ts_code, name, pe in rows:
                if ts_code in tradeable:
                    chosen.append(
                        LowPERecord(
                            trade_date=sd,
                            leg="A",
                            symbol=str(ts_code),
                            stock_name=str(name or ""),
                            pe=float(pe),
                        )
                    )
                    if len(chosen) >= k:
                        return chosen

            if len(chosen) >= k:
                return chosen
            if limit >= 3000:
                return chosen
            limit *= 2

    def _pick_low_pe_h_implied(
        self, *, signal_date: str, execute_date: str, k: int
    ) -> list[LowPERecord]:
        if k <= 0 or not self._pairs:
            return []
        con = self._raw_con()

        sd = self._latest_trade_date("daily_h", signal_date)
        if sd is None:
            return []
        hk_to_cny = self._load_hk_to_cny_rate(sd)
        if hk_to_cny is None:
            return []

        a_codes = [a for _, a, _ in self._pairs]
        h_codes = [h for _, _, h in self._pairs]
        in_a = ",".join([repr(x) for x in a_codes]) if a_codes else "''"
        in_h = ",".join([repr(x) for x in h_codes]) if h_codes else "''"

        # A: PE + close; H: close. Keep raw close for consistent PE conversion.
        a_pe_rows = con.execute(
            f"""
            SELECT ts_code, pe
            FROM bak_daily_a
            WHERE trade_date='{sd}' AND ts_code IN ({in_a}) AND pe IS NOT NULL AND pe > {self.pe_min}
            """
        ).fetchall()
        a_close_rows = con.execute(
            f"""
            SELECT ts_code, close
            FROM daily_a
            WHERE trade_date='{sd}' AND ts_code IN ({in_a}) AND close IS NOT NULL AND close > 0
            """
        ).fetchall()
        h_close_rows = con.execute(
            f"""
            SELECT ts_code, close
            FROM daily_h
            WHERE trade_date='{sd}' AND ts_code IN ({in_h}) AND close IS NOT NULL AND close > 0
            """
        ).fetchall()

        a_pe_map = {ts: float(pe) for ts, pe in a_pe_rows if pe is not None}
        a_close_map = {ts: float(c) for ts, c in a_close_rows if c is not None}
        h_close_map = {ts: float(c) for ts, c in h_close_rows if c is not None}

        # delist filter for execute_date
        h_delist = {
            ts: (d or "")
            for ts, d in con.execute(
                f"""
                SELECT ts_code, delist_date
                FROM stock_basic_h
                WHERE ts_code IN ({in_h})
                """
            ).fetchall()
        }

        # tradeable on execute_date
        h_tradeable = {
            ts
            for (ts,) in con.execute(
                f"""
                SELECT ts_code
                FROM daily_h
                WHERE trade_date=?
                  AND ts_code IN ({in_h})
                  AND open IS NOT NULL
                  AND open > 0
                """,
                [execute_date],
            ).fetchall()
        }

        implied: list[LowPERecord] = []
        for name, a_code, h_code in self._pairs:
            if h_code not in h_tradeable:
                continue
            delist_date = h_delist.get(h_code, "")
            if delist_date and delist_date <= execute_date:
                continue
            a_pe = a_pe_map.get(a_code)
            a_close = a_close_map.get(a_code)
            h_close = h_close_map.get(h_code)
            if a_pe is None or a_close is None or h_close is None:
                continue
            if a_close <= 0:
                continue
            pe_h = float(a_pe) * (float(h_close) * float(hk_to_cny) / float(a_close))
            if pe_h <= self.pe_min:
                continue
            implied.append(
                LowPERecord(
                    trade_date=sd,
                    leg="H",
                    symbol=h_code,
                    stock_name=name,
                    pe=pe_h,
                    a_symbol=a_code,
                    h_symbol=h_code,
                    a_pe=a_pe,
                    a_close_cny=a_close,
                    h_close_hkd=h_close,
                    hk_to_cny=hk_to_cny,
                )
            )

        implied.sort(key=lambda r: r.pe)
        return implied[:k]

    def compute_targets(
        self, *, signal_date: str, execute_date: str
    ) -> tuple[dict[str, float], list[LowPERecord]]:
        a_recs = self._pick_low_pe_a(signal_date=signal_date, execute_date=execute_date, k=self.a_k)
        h_recs = self._pick_low_pe_h_implied(
            signal_date=signal_date, execute_date=execute_date, k=self.h_k
        )
        # If user asked for both legs, but one leg is unavailable on this date,
        # return empty and let the engine try again on the next bar (retry policy).
        if self.a_k > 0 and not a_recs:
            return {}, []
        if self.h_k > 0 and not h_recs:
            return {}, []
        recs = [*a_recs, *h_recs]
        if not recs:
            return {}, []
        w = 1.0 / float(len(recs))
        targets = {r.symbol: w for r in recs}
        return targets, recs

    # --- core event ----------------------------------------------------
    def on_bar(self, bar: Bar, feed: DataFeed, broker: Broker) -> None:
        # delist -> write off (same idea as AHPremiumQuarterlyStrategy)
        if broker.positions:
            try:
                con = self._raw_con()
                held_syms = list(broker.positions.keys())
                a_syms = [s for s in held_syms if s.endswith(".SH") or s.endswith(".SZ")]
                h_syms = [s for s in held_syms if s.endswith(".HK")]
                rows: list[tuple[str, Optional[str]]] = []
                if a_syms:
                    rows += con.execute(
                        f"SELECT ts_code, delist_date FROM stock_basic_a WHERE ts_code IN ({','.join([repr(x) for x in a_syms])})"
                    ).fetchall()
                if h_syms:
                    rows += con.execute(
                        f"SELECT ts_code, delist_date FROM stock_basic_h WHERE ts_code IN ({','.join([repr(x) for x in h_syms])})"
                    ).fetchall()
                for ts_code, delist_date in rows:
                    if delist_date and delist_date != "" and delist_date <= bar.trade_date:
                        broker.force_write_off(
                            bar.trade_date, ts_code, reason=f"delist@{delist_date}"
                        )
            except Exception as e:
                print(f"[LowPEQuarterlyStrategy] delist check error: {e}")

        if not self._is_rebalance_day(bar, feed):
            return
        prev_bar = feed.prev
        if prev_bar is None:
            return

        t0 = time.perf_counter()
        targets, recs = self.compute_targets(
            signal_date=prev_bar.trade_date, execute_date=bar.trade_date
        )
        if not targets:
            return

        price_symbols = sorted(set(list(targets.keys()) + list(broker.positions.keys())))
        price_map = self._load_opens(bar.trade_date, price_symbols)
        if not price_map:
            return

        self.rebalance_history.append(
            {
                "rebalance_date": bar.trade_date,
                "signal_date": prev_bar.trade_date,
                "records": [r.__dict__ for r in recs],
                "targets": targets,
            }
        )

        print(
            f"[LowPEQuarterlyStrategy] rebalance {bar.trade_date} signal_date={prev_bar.trade_date} "
            f"A={len([r for r in recs if r.leg=='A'])} H={len([r for r in recs if r.leg=='H'])}"
        )
        for r in recs:
            if r.leg == "A":
                print(f"  A {r.symbol} pe={r.pe:.2f} name={r.stock_name}")
            else:
                print(
                    f"  H {r.symbol} pe={r.pe:.2f} name={r.stock_name} "
                    f"(A={r.a_symbol} A_pe={r.a_pe:.2f} A_close={r.a_close_cny:.3f} "
                    f"H_close={r.h_close_hkd:.3f} fx(HKD/CNY)={r.hk_to_cny:.4f})"
                )

        broker.rebalance_target_percents(bar.trade_date, price_map, targets)
        self._last_rebalance_period = self._period_key(bar.trade_date)
        t1 = time.perf_counter()
        print(f"[LowPEQuarterlyStrategy] on_bar({bar.trade_date}) done: {t1-t0:.3f}s")


__all__ = ["LowPEQuarterlyStrategy", "LowPERecord"]
