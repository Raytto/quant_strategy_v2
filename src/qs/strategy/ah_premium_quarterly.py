from __future__ import annotations

"""Quarterly A/H premium mean-reversion allocation strategy."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
import csv
import time

from ..backtester.market import PriceRequest, StrategyContext


@dataclass(frozen=True)
class PremiumRecord:
    trade_date: str
    name: str
    cn_code: str
    hk_code: str
    premium_pct: float
    a_close_raw: float
    h_close_raw_cny: float
    a_close_adj: float
    h_close_adj_cny: float


def quarter_key(date_str: str) -> str:
    y = date_str[:4]
    m = int(date_str[4:6])
    q = (m - 1) // 3 + 1
    return f"{y}Q{q}"


class AHPremiumQuarterlyStrategy:
    def __init__(
        self,
        db_path_raw: str = "data/data.sqlite",
        pairs_csv_path: str | Path = "data/ah_codes.csv",
        top_k: int = 5,
        bottom_k: int = 5,
        start_date: str = "20180101",
        capital_split: float = 0.5,
        price_cache_days: int = 30,
        use_adjusted: bool = True,
        premium_use_adjusted: bool = False,
        rebalance_month_interval: int = 3,
    ):
        self.dbr = db_path_raw
        self.top_k = top_k
        self.bottom_k = bottom_k
        self.start_date = start_date
        self.capital_split = capital_split
        self.price_cache_days = price_cache_days
        self.use_adjusted = bool(use_adjusted)
        self.premium_use_adjusted = bool(premium_use_adjusted)
        self.rebalance_month_interval = (
            rebalance_month_interval if rebalance_month_interval > 0 else 3
        )
        self._last_rebalance_period: Optional[str] = None
        self._pairs: list[tuple[str, str, str]] = list(
            self._load_pairs_csv(self._resolve_data_path(pairs_csv_path))
        )
        self.rebalance_history: List[Dict[str, Any]] = []

        self._a_open_request = PriceRequest(
            table="daily_a",
            field="open",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_a" if self.use_adjusted else None,
            exact=True,
        )
        self._h_open_request = PriceRequest(
            table="daily_h",
            field="open",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_h" if self.use_adjusted else None,
            exact=True,
        )
        self._a_mark_request = PriceRequest(
            table="daily_a",
            field="close",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_a" if self.use_adjusted else None,
            exact=False,
        )
        self._h_mark_request = PriceRequest(
            table="daily_h",
            field="close",
            adjusted=self.use_adjusted,
            adjustment_table="adj_factor_h" if self.use_adjusted else None,
            exact=False,
        )
        self._a_raw_close_request = PriceRequest(
            table="daily_a",
            field="close",
            adjusted=False,
            exact=True,
        )
        self._h_raw_close_request = PriceRequest(
            table="daily_h",
            field="close",
            adjusted=False,
            exact=True,
        )
        self._a_adj_close_request = PriceRequest(
            table="daily_a",
            field="close",
            adjusted=True,
            adjustment_table="adj_factor_a",
            exact=True,
        )
        self._h_adj_close_request = PriceRequest(
            table="daily_h",
            field="close",
            adjusted=True,
            adjustment_table="adj_factor_h",
            exact=True,
        )

    @staticmethod
    def _resolve_data_path(path: str | Path) -> Path:
        p = Path(path)
        if p.is_absolute():
            return p
        if p.exists():
            return p
        repo_root = Path(__file__).resolve().parents[3]
        return repo_root / p

    @staticmethod
    def _load_pairs_csv(path: str | Path) -> Iterable[tuple[str, str, str]]:
        p = Path(path)
        with p.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return []
            if "hk_code" in reader.fieldnames:
                hk_col = "hk_code"
            elif "c" in reader.fieldnames:
                hk_col = "c"
            else:
                raise RuntimeError("ah_codes.csv missing hk_code column (expected hk_code or c)")

            pairs: list[tuple[str, str, str]] = []
            for row in reader:
                cn_code = (row.get("cn_code") or "").strip()
                hk_code = (row.get(hk_col) or "").strip()
                if not cn_code or not hk_code:
                    continue
                pairs.append(((row.get("name") or "").strip(), cn_code, hk_code))
            return pairs

    def get_rebalance_history(self) -> List[Dict[str, Any]]:
        return self.rebalance_history

    def _period_key(self, date_str: str) -> str:
        y = date_str[:4]
        m = int(date_str[4:6])
        p = (m - 1) // self.rebalance_month_interval
        return f"{y}P{p}"

    def _is_rebalance_day(self, trade_date: str, signal_date: str | None) -> bool:
        if signal_date is None or trade_date < self.start_date:
            return False
        pk = self._period_key(trade_date)
        return pk != self._last_rebalance_period

    def _load_premium_for_date(self, ctx: StrategyContext, trade_date: str) -> List[PremiumRecord]:
        t0 = time.perf_counter()
        a_codes = [cn for _, cn, _ in self._pairs]
        h_codes = [hk for _, _, hk in self._pairs]
        a_raw = ctx.history.get_price_map(
            request=self._a_raw_close_request,
            symbols=a_codes,
            trade_date=trade_date,
        )
        h_raw = ctx.history.get_price_map(
            request=self._h_raw_close_request,
            symbols=h_codes,
            trade_date=trade_date,
        )
        a_adj = ctx.history.get_price_map(
            request=self._a_adj_close_request,
            symbols=a_codes,
            trade_date=trade_date,
        )
        h_adj = ctx.history.get_price_map(
            request=self._h_adj_close_request,
            symbols=h_codes,
            trade_date=trade_date,
        )
        hk_to_cny = ctx.history.get_hk_to_cny_rate(trade_date)
        if hk_to_cny is None:
            return []

        recs: List[PremiumRecord] = []
        for name, cn_code, hk_code in self._pairs:
            close_a_raw = a_raw.get(cn_code)
            close_h_raw_hkd = h_raw.get(hk_code)
            a_close_adj = a_adj.get(cn_code)
            h_close_adj_local = h_adj.get(hk_code)
            if (
                close_a_raw is None
                or close_h_raw_hkd is None
                or a_close_adj is None
                or h_close_adj_local is None
            ):
                continue
            h_close_raw_cny = float(close_h_raw_hkd) * hk_to_cny
            h_close_adj_cny = float(h_close_adj_local) * hk_to_cny
            if h_close_raw_cny <= 0 or h_close_adj_cny <= 0:
                continue
            premium_pct = (
                (float(a_close_adj) / h_close_adj_cny - 1) * 100
                if self.premium_use_adjusted
                else (float(close_a_raw) / h_close_raw_cny - 1) * 100
            )
            recs.append(
                PremiumRecord(
                    trade_date=trade_date,
                    name=name,
                    cn_code=cn_code,
                    hk_code=hk_code,
                    premium_pct=float(premium_pct),
                    a_close_raw=float(close_a_raw),
                    h_close_raw_cny=float(h_close_raw_cny),
                    a_close_adj=float(a_close_adj),
                    h_close_adj_cny=float(h_close_adj_cny),
                )
            )
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] premium({trade_date}) {len(recs)} records in {t1-t0:.3f}s"
        )
        return recs

    def _current_open_prices(self, ctx: StrategyContext, symbols: Sequence[str]) -> Dict[str, float]:
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        out: Dict[str, float] = {}
        if a_syms:
            out.update(ctx.current_price_map(request=self._a_open_request, symbols=a_syms))
        if h_syms:
            rate = ctx.current_hk_to_cny_rate()
            if rate is None:
                return {}
            h_map = ctx.current_price_map(request=self._h_open_request, symbols=h_syms)
            out.update({sym: float(px) * rate for sym, px in h_map.items()})
        return out

    def _current_mark_prices(self, ctx: StrategyContext, symbols: Sequence[str]) -> Dict[str, float]:
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        out: Dict[str, float] = {}
        if a_syms:
            out.update(ctx.current_price_map(request=self._a_mark_request, symbols=a_syms))
        if h_syms:
            rate = ctx.current_hk_to_cny_rate()
            if rate is None:
                return out
            h_map = ctx.current_price_map(request=self._h_mark_request, symbols=h_syms)
            out.update({sym: float(px) * rate for sym, px in h_map.items()})
        return out

    def _request_write_offs(self, ctx: StrategyContext) -> None:
        symbols = list(ctx.portfolio.positions.keys())
        if not symbols:
            return
        a_syms = [s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")]
        h_syms = [s for s in symbols if s.endswith(".HK")]
        meta_a = ctx.reference.get_values(
            table="stock_basic_a",
            symbols=a_syms,
            fields=["delist_date"],
        )
        meta_h = ctx.reference.get_values(
            table="stock_basic_h",
            symbols=h_syms,
            fields=["delist_date"],
        )
        for sym in symbols:
            row = meta_a.get(sym) or meta_h.get(sym) or {}
            delist_date = str(row.get("delist_date") or "")
            if delist_date and delist_date <= ctx.trade_date:
                ctx.request_write_off(sym, reason=f"delist@{delist_date}")

    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        t0 = time.perf_counter()
        self._request_write_offs(ctx)

        current_symbols = sorted(ctx.portfolio.positions.keys())
        base_marks = self._current_mark_prices(ctx, current_symbols)
        if base_marks:
            ctx.set_mark_request(prices=base_marks)

        if not self._is_rebalance_day(ctx.trade_date, ctx.signal_date):
            return
        signal_date = ctx.signal_date
        if signal_date is None:
            return

        premium_recs = self._load_premium_for_date(ctx, signal_date)
        if not premium_recs:
            return
        sorted_recs = sorted(premium_recs, key=lambda r: r.premium_pct)
        bottom = sorted_recs[: self.bottom_k]
        top = sorted_recs[-self.top_k :]
        a_symbols = [r.cn_code for r in bottom]
        h_symbols = [r.hk_code for r in top]
        if not a_symbols or not h_symbols:
            return

        w_each_a = (1 - self.capital_split) / len(a_symbols)
        w_each_h = self.capital_split / len(h_symbols)
        targets = {
            **{s: w_each_a for s in a_symbols},
            **{s: w_each_h for s in h_symbols},
        }
        trade_symbols = sorted(set(targets.keys()) | set(current_symbols))
        price_map = self._current_open_prices(ctx, trade_symbols)
        if len(price_map) != len(trade_symbols):
            return

        mark_map = self._current_mark_prices(ctx, trade_symbols)
        if mark_map:
            ctx.set_mark_request(prices=mark_map)
        ctx.rebalance_to_weights(targets, execution_prices=price_map)

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
                "rebalance_date": ctx.trade_date,
                "premium_date": signal_date,
                "decisions": decisions,
            }
        )
        self._last_rebalance_period = self._period_key(ctx.trade_date)
        t1 = time.perf_counter()
        print(
            f"[AHPremiumQuarterlyStrategy] rebalance {ctx.trade_date} premium_date={signal_date} "
            f"A_count={len(a_symbols)} H_count={len(h_symbols)} in {t1-t0:.3f}s"
        )


__all__ = ["AHPremiumQuarterlyStrategy", "PremiumRecord", "quarter_key"]
