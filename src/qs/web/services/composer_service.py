from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np

from qs.backtester.market import SqliteMarketData
from qs.backtester.stats import compute_max_drawdown, compute_risk_metrics

from ..models.dto import (
    BenchmarkPoint,
    ComboComponentWeight,
    ComboResult,
    EquityPoint,
    HoldingSnapshot,
    StandardSnapshot,
)
from .benchmark_service import BenchmarkService
from .kelly_service import KellyService


class ComposerService:
    def __init__(
        self,
        benchmark_service: BenchmarkService | None = None,
        *,
        market_db_path: str | Path | None = None,
    ):
        self.benchmark_service = benchmark_service
        self.market_db_path = Path(market_db_path) if market_db_path is not None else None

    def evaluate(
        self,
        snapshots: list[StandardSnapshot],
        *,
        optimizer: dict[str, Any] | None = None,
        benchmarks: list[str] | None = None,
    ) -> ComboResult:
        if not snapshots:
            raise ValueError("at least one snapshot is required")
        optimizer = optimizer or {}
        max_weight = float(optimizer.get("max_strategy_weight", 1.0))
        allow_cash = bool(optimizer.get("allow_cash", True))
        scale = float(optimizer.get("kelly_scale", 0.5))

        aligned_dates, returns_matrix = self._aligned_returns(snapshots)
        if len(aligned_dates) < 2:
            raise ValueError("not enough overlapping history between strategies")

        mu = np.mean(returns_matrix, axis=0)
        sigma = np.cov(returns_matrix, rowvar=False, ddof=0)
        if sigma.ndim == 0:
            sigma = np.array([[float(sigma)]])

        raw_weights = self._solve_weights(mu, sigma, max_weight=max_weight, allow_cash=allow_cash)
        kelly_weights = np.clip(raw_weights * scale, 0.0, max_weight)
        if not allow_cash and np.sum(kelly_weights) > 0:
            kelly_weights = kelly_weights / np.sum(kelly_weights)

        nav_curve = self._combine_nav(aligned_dates, returns_matrix, kelly_weights)
        metrics = self._build_metrics(nav_curve)
        benchmark_points = self._build_benchmarks(nav_curve, benchmarks or [])
        component_weights = self._build_component_weights(
            snapshots,
            raw_weights.tolist(),
            kelly_weights.tolist(),
        )
        holdings = self._aggregate_holdings(snapshots, component_weights)

        return ComboResult(
            combo_run_id=f"combo-{uuid4().hex[:12]}",
            selected_strategies=[s.strategy_key for s in snapshots],
            optimizer_config=dict(optimizer),
            metrics=metrics,
            equity_curve=nav_curve,
            benchmarks=benchmark_points,
            component_weights=component_weights,
            holdings=holdings,
        )

    def _build_benchmarks(
        self,
        equity_curve: list[EquityPoint],
        benchmarks: list[str],
    ) -> list[BenchmarkPoint]:
        if self.benchmark_service is not None:
            return self.benchmark_service.build_for_curve(equity_curve, benchmarks)
        if self.market_db_path is None:
            return []
        market_data = SqliteMarketData(self.market_db_path)
        try:
            return BenchmarkService(market_data).build_for_curve(equity_curve, benchmarks)
        finally:
            market_data.close()

    def _aligned_returns(
        self, snapshots: list[StandardSnapshot]
    ) -> tuple[list[str], np.ndarray]:
        date_sets = []
        nav_maps = []
        for snapshot in snapshots:
            nav_map = {p.trade_date: p.nav for p in snapshot.equity_curve}
            nav_maps.append(nav_map)
            date_sets.append(set(nav_map))
        common_dates = sorted(set.intersection(*date_sets))
        if len(common_dates) < 2:
            return common_dates, np.zeros((0, len(snapshots)))
        rows = []
        for idx in range(1, len(common_dates)):
            prev_date = common_dates[idx - 1]
            date = common_dates[idx]
            row = []
            for nav_map in nav_maps:
                prev_nav = nav_map[prev_date]
                curr_nav = nav_map[date]
                row.append(curr_nav / prev_nav - 1.0 if prev_nav else 0.0)
            rows.append(row)
        return common_dates, np.array(rows, dtype=float)

    def _solve_weights(
        self,
        mu: np.ndarray,
        sigma: np.ndarray,
        *,
        max_weight: float,
        allow_cash: bool,
    ) -> np.ndarray:
        n = len(mu)
        if n == 1:
            return np.array([min(1.0, max_weight)])
        w = np.full(n, min(1.0 / n, max_weight), dtype=float)
        lr = 0.05
        for _ in range(400):
            grad = mu - sigma @ w
            w = w + lr * grad
            w = np.clip(w, 0.0, max_weight)
            total = float(np.sum(w))
            if total > 1.0:
                w = w / total
                w = np.clip(w, 0.0, max_weight)
                total = float(np.sum(w))
                if total > 1.0:
                    w *= 1.0 / total
            if not allow_cash and total > 0:
                w = w / total
                w = np.clip(w, 0.0, max_weight)
        total = float(np.sum(w))
        if not allow_cash and total > 0:
            w = w / total
        return w

    def _combine_nav(
        self, dates: list[str], returns_matrix: np.ndarray, weights: np.ndarray
    ) -> list[EquityPoint]:
        nav = 1.0
        out = [EquityPoint(trade_date=dates[0], nav=1.0)] if dates else []
        for idx, date in enumerate(dates[1:], start=0):
            step_return = float(np.dot(returns_matrix[idx], weights))
            nav *= 1.0 + step_return
            out.append(EquityPoint(trade_date=date, nav=nav))
        return out

    def _build_metrics(self, equity_curve: list[EquityPoint]) -> dict[str, Any]:
        if not equity_curve:
            return {}
        curve = [_CurvePoint(p.trade_date, p.nav) for p in equity_curve]
        risk = compute_risk_metrics(curve, 1.0)
        max_dd, peak, trough = compute_max_drawdown(curve)
        return {
            "final_equity": equity_curve[-1].nav,
            "cagr": risk.get("CAGR", 0.0),
            "ann_return": risk.get("AnnReturn", 0.0),
            "ann_vol": risk.get("AnnVol", 0.0),
            "sharpe": risk.get("Sharpe", 0.0),
            "max_drawdown": max_dd,
            "drawdown_peak": peak,
            "drawdown_trough": trough,
        }

    def _build_component_weights(
        self,
        snapshots: list[StandardSnapshot],
        raw_weights: list[float],
        kelly_weights: list[float],
    ) -> list[ComboComponentWeight]:
        out: list[ComboComponentWeight] = []
        for snapshot, raw, kelly in zip(snapshots, raw_weights, kelly_weights):
            out.append(
                ComboComponentWeight(
                    strategy_key=snapshot.strategy_key,
                    display_name=snapshot.strategy_key,
                    raw_weight=float(raw),
                    kelly_weight=float(kelly),
                    deploy_ratio=float(kelly / raw) if raw > 0 else 0.0,
                    cagr=_metric_float(snapshot.metrics, "cagr"),
                    sharpe=_metric_float(snapshot.metrics, "sharpe"),
                    max_drawdown=_metric_float(snapshot.metrics, "max_drawdown"),
                )
            )
        return out

    def _aggregate_holdings(
        self,
        snapshots: list[StandardSnapshot],
        component_weights: list[ComboComponentWeight],
    ) -> list[HoldingSnapshot]:
        bucket: dict[str, HoldingSnapshot] = {}
        for snapshot, comp in zip(snapshots, component_weights):
            for holding in snapshot.holdings:
                scaled_raw = comp.kelly_weight * holding.raw_weight
                existing = bucket.get(holding.symbol)
                if existing is None:
                    bucket[holding.symbol] = replace(
                        holding,
                        raw_weight=scaled_raw,
                        kelly_weight=scaled_raw,
                        source_strategy_weight=comp.kelly_weight,
                    )
                    continue
                total_mv = existing.market_value + holding.market_value
                total_qty = existing.quantity + holding.quantity
                bucket[holding.symbol] = HoldingSnapshot(
                    symbol=holding.symbol,
                    symbol_name=holding.symbol_name,
                    market=holding.market,
                    price_cny=holding.price_cny or existing.price_cny,
                    quantity=total_qty,
                    market_value=total_mv,
                    raw_weight=existing.raw_weight + scaled_raw,
                    kelly_weight=existing.kelly_weight + scaled_raw,
                    source_strategy_weight=existing.source_strategy_weight + comp.kelly_weight,
                )
        return sorted(bucket.values(), key=lambda item: item.kelly_weight, reverse=True)


def _metric_float(metrics: dict[str, Any], key: str) -> float | None:
    value = metrics.get(key)
    return float(value) if value is not None else None


class _CurvePoint:
    def __init__(self, trade_date: str, nav: float):
        self.trade_date = trade_date
        self.equity = nav
