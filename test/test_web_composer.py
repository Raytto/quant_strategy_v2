from __future__ import annotations

from qs.web.models.dto import EquityPoint, HoldingSnapshot, StandardSnapshot
from qs.web.services.composer_service import ComposerService


class _BenchmarkService:
    def build_for_curve(self, equity_curve, benchmark_codes):
        return []


def _snapshot(key: str, values: list[float]) -> StandardSnapshot:
    return StandardSnapshot(
        strategy_key=key,
        run_id=f"{key}-run",
        run_tag="tag",
        as_of_date="20240103",
        start_date="20240101",
        end_date="20240103",
        initial_cash=1.0,
        params={},
        metrics={"cagr": 0.1, "sharpe": 1.0, "max_drawdown": -0.1},
        equity_curve=[
            EquityPoint("20240101", values[0]),
            EquityPoint("20240102", values[1]),
            EquityPoint("20240103", values[2]),
        ],
        benchmarks=[],
        holdings=[
            HoldingSnapshot(
                symbol=f"{key}.SH",
                symbol_name=key,
                market="SH",
                price_cny=10.0,
                quantity=100.0,
                market_value=1000.0,
                raw_weight=1.0,
                kelly_weight=1.0,
                source_strategy_weight=1.0,
            )
        ],
        rebalance_history=[],
        output_dir="/tmp",
    )


def test_composer_service_returns_combo_curve():
    service = ComposerService(_BenchmarkService())
    result = service.evaluate(
        [_snapshot("a", [1.0, 1.1, 1.2]), _snapshot("b", [1.0, 1.0, 1.05])],
        optimizer={"max_strategy_weight": 0.7, "allow_cash": True, "kelly_scale": 0.5},
        benchmarks=[],
    )

    assert result.equity_curve
    assert result.component_weights
    assert result.metrics["final_equity"] > 0
