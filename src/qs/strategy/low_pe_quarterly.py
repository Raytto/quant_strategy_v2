from __future__ import annotations

"""Quarterly (configurable) Low-PE allocation strategy for A + H shares."""

import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from ..backtester.market import PriceRequest, StrategyContext


@dataclass(frozen=True)
class LowPERecord:
    trade_date: str
    leg: str  # "A" | "H"
    symbol: str
    stock_name: str
    pe: float
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
        self.start_date = str(start_date)
        self.rebalance_month_interval = (
            int(rebalance_month_interval) if rebalance_month_interval > 0 else 3
        )
        self.pe_min = float(pe_min)
        self.candidate_limit = max(50, int(candidate_limit))
        self.use_adjusted = bool(use_adjusted)

        self._pairs: list[tuple[str, str, str]] = self._load_pairs(self.pairs_csv_path)
        self._last_rebalance_period: Optional[str] = None
        self._a_reference_cache: Dict[str, Dict[str, Any]] | None = None
        self._h_reference_cache: Dict[str, Dict[str, Any]] | None = None
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

        print(
            f"[LowPEQuarterlyStrategy] init pairs={len(self._pairs)} "
            f"(a_k={self.a_k}, h_k={self.h_k}, interval={self.rebalance_month_interval}m, use_adjusted={self.use_adjusted})"
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

    @classmethod
    def _load_pairs(cls, csv_path: str | Path) -> list[tuple[str, str, str]]:
        resolved = cls._resolve_data_path(csv_path)
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

    def _is_rebalance_day(self, trade_date: str, signal_date: str | None) -> bool:
        if signal_date is None or trade_date < self.start_date:
            return False
        return self._period_key(trade_date) != self._last_rebalance_period

    def _a_reference(self, ctx: StrategyContext) -> Dict[str, Dict[str, Any]]:
        if self._a_reference_cache is None:
            self._a_reference_cache = ctx.reference.get_values(
                table="stock_basic_a",
                symbols=None,
                fields=["name", "list_date", "delist_date"],
            )
        return self._a_reference_cache

    def _h_reference(self, ctx: StrategyContext) -> Dict[str, Dict[str, Any]]:
        if self._h_reference_cache is None:
            self._h_reference_cache = ctx.reference.get_values(
                table="stock_basic_h",
                symbols=None,
                fields=["delist_date"],
            )
        return self._h_reference_cache

    def _current_open_prices(
        self, ctx: StrategyContext, symbols: Sequence[str]
    ) -> Dict[str, float]:
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

    def _current_mark_prices(
        self, ctx: StrategyContext, symbols: Sequence[str]
    ) -> Dict[str, float]:
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
        meta_a = ctx.reference.get_values(
            table="stock_basic_a",
            symbols=[s for s in symbols if s.endswith(".SH") or s.endswith(".SZ")],
            fields=["delist_date"],
        )
        meta_h = ctx.reference.get_values(
            table="stock_basic_h",
            symbols=[s for s in symbols if s.endswith(".HK")],
            fields=["delist_date"],
        )
        for sym in symbols:
            row = meta_a.get(sym) or meta_h.get(sym) or {}
            delist_date = str(row.get("delist_date") or "")
            if delist_date and delist_date <= ctx.trade_date:
                ctx.request_write_off(sym, reason=f"delist@{delist_date}")

    def _pick_low_pe_a(
        self, ctx: StrategyContext, *, signal_date: str, execute_date: str, k: int
    ) -> list[LowPERecord]:
        if k <= 0:
            return []
        sd = ctx.history.get_latest_trade_date(table="bak_daily_a", on_or_before=signal_date)
        if sd is None:
            return []
        rows = ctx.history.get_snapshot_rows(
            table="bak_daily_a",
            fields=["pe"],
            trade_date=sd,
            exact=True,
        )
        meta = self._a_reference(ctx)
        candidates: list[tuple[str, str, float]] = []
        for row in rows:
            ts_code = row["ts_code"]
            pe = row.get("pe")
            ref = meta.get(ts_code)
            if ref is None or pe is None:
                continue
            pe_value = float(pe)
            if pe_value <= self.pe_min:
                continue
            list_date = str(ref.get("list_date") or "")
            delist_date = str(ref.get("delist_date") or "")
            if list_date and list_date > sd:
                continue
            if delist_date and delist_date <= execute_date:
                continue
            candidates.append((ts_code, str(ref.get("name") or ""), pe_value))
        candidates.sort(key=lambda item: item[2])
        if not candidates:
            return []

        chosen: list[LowPERecord] = []
        limit = min(self.candidate_limit, len(candidates))
        while True:
            batch = candidates[:limit]
            tradeable = ctx.current_price_map(
                request=self._a_open_request,
                symbols=[ts_code for ts_code, _, _ in batch],
            )
            chosen = [
                LowPERecord(
                    trade_date=sd,
                    leg="A",
                    symbol=ts_code,
                    stock_name=name,
                    pe=pe_value,
                )
                for ts_code, name, pe_value in batch
                if ts_code in tradeable
            ][:k]
            if len(chosen) >= k or limit >= len(candidates):
                return chosen
            limit = min(limit * 2, len(candidates))

    def _pick_low_pe_h_implied(
        self, ctx: StrategyContext, *, signal_date: str, execute_date: str, k: int
    ) -> list[LowPERecord]:
        if k <= 0 or not self._pairs:
            return []
        sd = ctx.history.get_latest_trade_date(table="daily_h", on_or_before=signal_date)
        if sd is None:
            return []
        hk_to_cny = ctx.history.get_hk_to_cny_rate(sd)
        if hk_to_cny is None:
            return []

        a_codes = [a for _, a, _ in self._pairs]
        h_codes = [h for _, _, h in self._pairs]
        a_closes = ctx.history.get_price_map(
            request=self._a_raw_close_request,
            symbols=a_codes,
            trade_date=sd,
        )
        h_closes = ctx.history.get_price_map(
            request=self._h_raw_close_request,
            symbols=h_codes,
            trade_date=sd,
        )
        a_pe_rows = ctx.history.get_snapshot_rows(
            table="bak_daily_a",
            fields=["pe"],
            trade_date=sd,
            exact=True,
            symbols=a_codes,
        )
        a_pe_map = {
            row["ts_code"]: float(row["pe"])
            for row in a_pe_rows
            if row.get("pe") is not None and float(row["pe"]) > self.pe_min
        }
        h_meta = self._h_reference(ctx)
        h_tradeable = ctx.current_price_map(request=self._h_open_request, symbols=h_codes)

        implied: list[LowPERecord] = []
        for name, a_code, h_code in self._pairs:
            if h_code not in h_tradeable:
                continue
            delist_date = str((h_meta.get(h_code) or {}).get("delist_date") or "")
            if delist_date and delist_date <= execute_date:
                continue
            a_pe = a_pe_map.get(a_code)
            a_close = a_closes.get(a_code)
            h_close = h_closes.get(h_code)
            if a_pe is None or a_close is None or h_close is None or a_close <= 0:
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
        implied.sort(key=lambda rec: rec.pe)
        return implied[:k]

    def compute_targets(
        self, ctx: StrategyContext, *, signal_date: str, execute_date: str
    ) -> tuple[dict[str, float], list[LowPERecord]]:
        a_recs = self._pick_low_pe_a(
            ctx,
            signal_date=signal_date,
            execute_date=execute_date,
            k=self.a_k,
        )
        h_recs = self._pick_low_pe_h_implied(
            ctx,
            signal_date=signal_date,
            execute_date=execute_date,
            k=self.h_k,
        )
        if self.a_k > 0 and not a_recs:
            return {}, []
        if self.h_k > 0 and not h_recs:
            return {}, []
        recs = [*a_recs, *h_recs]
        if not recs:
            return {}, []
        weight = 1.0 / float(len(recs))
        return ({rec.symbol: weight for rec in recs}, recs)

    def on_bar_ctx(self, ctx: StrategyContext) -> None:
        t0 = time.perf_counter()
        self._request_write_offs(ctx)

        current_symbols = sorted(ctx.portfolio.positions.keys())
        base_marks = self._current_mark_prices(ctx, current_symbols)
        if base_marks:
            ctx.set_mark_request(prices=base_marks)

        if not self._is_rebalance_day(ctx.trade_date, ctx.signal_date):
            return
        if ctx.signal_date is None:
            return

        targets, recs = self.compute_targets(
            ctx,
            signal_date=ctx.signal_date,
            execute_date=ctx.trade_date,
        )
        if not targets:
            return

        trade_symbols = sorted(set(targets.keys()) | set(current_symbols))
        price_map = self._current_open_prices(ctx, trade_symbols)
        if len(price_map) != len(trade_symbols):
            return

        mark_map = self._current_mark_prices(ctx, trade_symbols)
        if mark_map:
            ctx.set_mark_request(prices=mark_map)
        ctx.rebalance_to_weights(targets, execution_prices=price_map)

        self.rebalance_history.append(
            {
                "rebalance_date": ctx.trade_date,
                "signal_date": ctx.signal_date,
                "records": [record.__dict__ for record in recs],
                "targets": targets,
            }
        )
        self._last_rebalance_period = self._period_key(ctx.trade_date)
        t1 = time.perf_counter()
        print(
            f"[LowPEQuarterlyStrategy] rebalance {ctx.trade_date} signal_date={ctx.signal_date} "
            f"A={len([r for r in recs if r.leg == 'A'])} H={len([r for r in recs if r.leg == 'H'])} "
            f"in {t1-t0:.3f}s"
        )


__all__ = ["LowPEQuarterlyStrategy", "LowPERecord"]
