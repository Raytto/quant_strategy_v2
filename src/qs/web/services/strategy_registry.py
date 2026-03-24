from __future__ import annotations

from qs.strategy.etf_equal_weight_annual import DEFAULT_ETFS

from ..models.dto import StrategyDefinition


def get_builtin_definitions() -> list[StrategyDefinition]:
    return [
        StrategyDefinition(
            strategy_key="ah_premium_quarterly",
            display_name="AH 溢价季度策略",
            description="按 A/H 溢价做季度轮动，分别做多高折价 A 与 H 标的。",
            category="AH",
            module_path="qs.strategy.ah_premium_quarterly",
            class_name="AHPremiumQuarterlyStrategy",
            feed_type="calendar_ah",
            default_params={
                "db_path_raw": "data/data.sqlite",
                "pairs_csv_path": "data/ah_codes.csv",
                "top_k": 5,
                "bottom_k": 5,
                "start_date": "20180101",
                "capital_split": 0.5,
                "use_adjusted": True,
                "premium_use_adjusted": False,
                "rebalance_month_interval": 3,
            },
            param_schema={
                "top_k": {"type": "integer", "minimum": 1},
                "bottom_k": {"type": "integer", "minimum": 1},
                "start_date": {"type": "string"},
                "capital_split": {"type": "number", "minimum": 0, "maximum": 1},
            },
            default_benchmarks=["000300.SH", "HSI", "IXIC"],
        ),
        StrategyDefinition(
            strategy_key="low_pe_quarterly",
            display_name="低 PE 季度策略",
            description="分别在 A/H 市场筛选低估值标的并按季度调仓。",
            category="Value",
            module_path="qs.strategy.low_pe_quarterly",
            class_name="LowPEQuarterlyStrategy",
            feed_type="calendar_ah",
            default_params={
                "db_path_raw": "data/data.sqlite",
                "pairs_csv_path": "data/ah_codes.csv",
                "a_k": 5,
                "h_k": 5,
                "start_date": "20180101",
                "rebalance_month_interval": 3,
                "pe_min": 0.0,
                "candidate_limit": 300,
                "use_adjusted": True,
            },
            param_schema={
                "a_k": {"type": "integer", "minimum": 0},
                "h_k": {"type": "integer", "minimum": 0},
                "start_date": {"type": "string"},
                "rebalance_month_interval": {"type": "integer", "minimum": 1},
            },
            default_benchmarks=["000300.SH", "HSI", "IXIC"],
        ),
        StrategyDefinition(
            strategy_key="etf_equal_weight_annual",
            display_name="ETF 年度等权策略",
            description="固定 ETF 池年度再平衡，维持等权暴露。",
            category="ETF",
            module_path="qs.strategy.etf_equal_weight_annual",
            class_name="ETFEqualWeightAnnualStrategy",
            feed_type="calendar_etf",
            default_params={
                "db_path_raw": "data/data.sqlite",
                "symbols": list(DEFAULT_ETFS),
                "start_date": "20100101",
                "use_adjusted": True,
                "rebalance_year_interval": 1,
            },
            param_schema={
                "symbols": {"type": "array"},
                "start_date": {"type": "string"},
                "rebalance_year_interval": {"type": "integer", "minimum": 1},
            },
            default_benchmarks=["000300.SH", "IXIC"],
        ),
        StrategyDefinition(
            strategy_key="etf_min_premium_weekly",
            display_name="ETF 最低溢价周频轮动",
            description="在 ETF 池中每周选择折价率最优的单一标的。",
            category="ETF",
            module_path="qs.strategy.etf_min_premium_weekly",
            class_name="ETFMinPremiumWeeklyStrategy",
            feed_type="calendar_etf",
            default_params={
                "db_path_raw": "data/data.sqlite",
                "symbols": ["513100.SH", "513500.SH", "159941.SZ", "513130.SH"],
                "start_date": "20180101",
                "use_adjusted": True,
                "monday_only": True,
                "min_improvement": 1.0,
            },
            param_schema={
                "symbols": {"type": "array"},
                "start_date": {"type": "string"},
                "min_improvement": {"type": "number", "minimum": 0},
            },
            default_benchmarks=["000300.SH", "IXIC"],
        ),
        StrategyDefinition(
            strategy_key="ignored_crowded_ah_monthly",
            display_name="冷门拥挤度月频策略",
            description="基于预计算面板按月筛选被忽视且不拥挤的 A/H 标的。",
            category="AH",
            module_path="qs.strategy.ignored_crowded_ah_monthly",
            class_name="IgnoredCrowdedAHMonthlyStrategy",
            feed_type="monthly_panel",
            default_params={
                "panel_path": "data/backtests/ignored_buzz_ah/cache/trade_panel.pkl",
                "start_date": "20170731",
            },
            param_schema={
                "panel_path": {"type": "string"},
                "start_date": {"type": "string"},
            },
            default_benchmarks=["000300.SH", "HSI"],
        ),
        StrategyDefinition(
            strategy_key="ignored_buzz_ah_research",
            display_name="冷门 AH 研究产物",
            description="直接读取现有研究脚本导出的最终回测产物并统一到标准快照。",
            category="Research",
            module_path=None,
            class_name=None,
            feed_type="artifact",
            default_params={
                "artifact_dir": "data/backtests/ignored_buzz_ah",
            },
            param_schema={"artifact_dir": {"type": "string"}},
            default_benchmarks=["000300.SH", "HSI", "IXIC"],
            source_type="artifact",
        ),
    ]


class StrategyRegistry:
    def __init__(self, definitions: list[StrategyDefinition] | None = None):
        self._definitions = {
            d.strategy_key: d for d in (definitions or get_builtin_definitions())
        }

    def list_definitions(self) -> list[StrategyDefinition]:
        return list(self._definitions.values())

    def get(self, strategy_key: str) -> StrategyDefinition:
        try:
            return self._definitions[strategy_key]
        except KeyError as exc:
            raise KeyError(f"unknown strategy_key: {strategy_key}") from exc
