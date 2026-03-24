from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qs.backtester.stats import compute_daily_returns

from ..models.dto import EquityPoint, HoldingSnapshot


@dataclass(frozen=True)
class KellyResult:
    mu: float
    sigma2: float
    full_kelly: float
    deploy_ratio: float
    cash_weight: float
    observations: int
    scaled_holdings: list[HoldingSnapshot]


class KellyService:
    def __init__(
        self,
        *,
        scale: float = 0.5,
        max_gross_exposure: float = 1.0,
        min_observations: int = 20,
    ):
        self.scale = float(scale)
        self.max_gross_exposure = float(max_gross_exposure)
        self.min_observations = int(min_observations)

    def evaluate(
        self,
        equity_curve: list[EquityPoint],
        holdings: list[HoldingSnapshot],
    ) -> KellyResult:
        proxy_curve = [_CurvePoint(p.trade_date, p.nav) for p in equity_curve]
        returns = compute_daily_returns(proxy_curve)
        obs = len(returns)
        if obs < self.min_observations:
            return self._empty_result(holdings, obs)
        mu = sum(returns) / obs
        sigma2 = sum((r - mu) ** 2 for r in returns) / obs if obs else 0.0
        if sigma2 <= 0:
            return self._empty_result(holdings, obs, mu=mu, sigma2=sigma2)
        full_kelly = mu / sigma2
        deploy_ratio = max(0.0, min(self.scale * full_kelly, self.max_gross_exposure))
        scaled = [
            HoldingSnapshot(
                symbol=h.symbol,
                symbol_name=h.symbol_name,
                market=h.market,
                price_cny=h.price_cny,
                quantity=h.quantity,
                market_value=h.market_value,
                raw_weight=h.raw_weight,
                kelly_weight=h.raw_weight * deploy_ratio,
                source_strategy_weight=h.source_strategy_weight,
            )
            for h in holdings
        ]
        return KellyResult(
            mu=mu,
            sigma2=sigma2,
            full_kelly=full_kelly,
            deploy_ratio=deploy_ratio,
            cash_weight=max(0.0, 1.0 - deploy_ratio),
            observations=obs,
            scaled_holdings=scaled,
        )

    def _empty_result(
        self,
        holdings: list[HoldingSnapshot],
        observations: int,
        *,
        mu: float = 0.0,
        sigma2: float = 0.0,
    ) -> KellyResult:
        scaled = [
            HoldingSnapshot(
                symbol=h.symbol,
                symbol_name=h.symbol_name,
                market=h.market,
                price_cny=h.price_cny,
                quantity=h.quantity,
                market_value=h.market_value,
                raw_weight=h.raw_weight,
                kelly_weight=0.0,
                source_strategy_weight=h.source_strategy_weight,
            )
            for h in holdings
        ]
        return KellyResult(
            mu=mu,
            sigma2=sigma2,
            full_kelly=0.0,
            deploy_ratio=0.0,
            cash_weight=1.0,
            observations=observations,
            scaled_holdings=scaled,
        )


class _CurvePoint:
    def __init__(self, trade_date: str, nav: float):
        self.trade_date = trade_date
        self.equity = nav
