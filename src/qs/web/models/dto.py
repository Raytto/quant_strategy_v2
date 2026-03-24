from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_key: str
    display_name: str
    description: str
    category: str
    module_path: str | None
    class_name: str | None
    feed_type: str
    default_params: dict[str, Any]
    param_schema: dict[str, Any]
    default_benchmarks: list[str]
    supports_composer: bool = True
    status: str = "active"
    source_type: str = "framework"

    def to_record(self) -> dict[str, Any]:
        rec = asdict(self)
        rec["default_params_json"] = rec.pop("default_params")
        rec["param_schema_json"] = rec.pop("param_schema")
        rec["default_benchmarks_json"] = rec.pop("default_benchmarks")
        return rec


@dataclass(frozen=True)
class EquityPoint:
    trade_date: str
    nav: float


@dataclass(frozen=True)
class BenchmarkPoint:
    benchmark_code: str
    trade_date: str
    nav: float


@dataclass(frozen=True)
class HoldingSnapshot:
    symbol: str
    symbol_name: str
    market: str
    price_cny: float
    quantity: float
    market_value: float
    raw_weight: float
    kelly_weight: float
    source_strategy_weight: float


@dataclass(frozen=True)
class StandardSnapshot:
    strategy_key: str
    run_id: str
    run_tag: str
    as_of_date: str
    start_date: str
    end_date: str
    initial_cash: float
    params: dict[str, Any]
    metrics: dict[str, Any]
    equity_curve: list[EquityPoint]
    benchmarks: list[BenchmarkPoint]
    holdings: list[HoldingSnapshot]
    rebalance_history: list[dict[str, Any]]
    output_dir: str
    source_type: str = "framework"

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "strategy_key": self.strategy_key,
            "run_id": self.run_id,
            "run_tag": self.run_tag,
            "as_of_date": self.as_of_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "initial_cash": self.initial_cash,
            "params": self.params,
            "metrics": self.metrics,
            "source_type": self.source_type,
        }


@dataclass(frozen=True)
class StrategyLatestRecord:
    strategy_key: str
    display_name: str
    description: str
    category: str
    status: str
    supports_composer: bool
    source_type: str
    default_params: dict[str, Any]
    default_benchmarks: list[str]
    latest_run_id: str | None = None
    latest_run_tag: str | None = None
    latest_completed_at: str | None = None
    metrics: dict[str, float] = field(default_factory=dict)
    holding_count: int = 0


@dataclass(frozen=True)
class ComboComponentWeight:
    strategy_key: str
    display_name: str
    raw_weight: float
    kelly_weight: float
    deploy_ratio: float
    cagr: float | None = None
    sharpe: float | None = None
    max_drawdown: float | None = None


@dataclass(frozen=True)
class ComboResult:
    combo_run_id: str
    selected_strategies: list[str]
    optimizer_config: dict[str, Any]
    metrics: dict[str, Any]
    equity_curve: list[EquityPoint]
    benchmarks: list[BenchmarkPoint]
    component_weights: list[ComboComponentWeight]
    holdings: list[HoldingSnapshot]
