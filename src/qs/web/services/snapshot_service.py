from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from qs.backtester.market import SqliteMarketData
from qs.backtester.data import Bar
from qs.backtester.runner import (
    build_strategy,
    load_bars_from_sqlite,
    load_calendar_bars_from_sqlite,
    run_backtest,
)
from qs.sqlite_utils import connect_sqlite

from ..config import WebConfig
from ..models.dto import BenchmarkPoint, EquityPoint, HoldingSnapshot, StandardSnapshot
from ..repo.web_db import WebDB
from .benchmark_service import BenchmarkService
from .kelly_service import KellyService
from .strategy_registry import StrategyRegistry


class SnapshotService:
    def __init__(
        self,
        *,
        config: WebConfig,
        registry: StrategyRegistry,
        repo: WebDB,
    ):
        self.config = config
        self.registry = registry
        self.repo = repo

    def refresh_strategy(
        self,
        strategy_key: str,
        *,
        params_override: dict[str, Any] | None = None,
        as_of_date: str | None = None,
    ) -> StandardSnapshot:
        definition = self.registry.get(strategy_key)
        if definition.source_type == "artifact":
            snapshot = self._load_artifact_snapshot(definition, params_override, as_of_date)
        else:
            snapshot = self._run_framework_snapshot(definition, params_override, as_of_date)
        self._write_snapshot_files(snapshot)
        self.repo.save_snapshot(snapshot)
        stale_dirs = self.repo.cleanup_old_runs(
            strategy_key=strategy_key,
            retention_days=self.config.retention_days,
        )
        for stale_dir in stale_dirs:
            path = Path(stale_dir)
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)
        return snapshot

    def refresh_all(self) -> list[StandardSnapshot]:
        snapshots: list[StandardSnapshot] = []
        for definition in self.registry.list_definitions():
            snapshots.append(self.refresh_strategy(definition.strategy_key))
        return snapshots

    def _run_framework_snapshot(
        self,
        definition,
        params_override: dict[str, Any] | None,
        as_of_date: str | None,
    ) -> StandardSnapshot:
        params = dict(definition.default_params)
        if params_override:
            params.update(params_override)
        market_data = SqliteMarketData(self.config.market_db_path)
        try:
            end_date = as_of_date or self._resolve_end_date(definition.feed_type, params, market_data)
            params = self._prepare_params(definition.feed_type, params, end_date)
            bars = self._load_feed(definition.feed_type, params, end_date)
            strategy_spec = f"{definition.module_path}:{definition.class_name}"
            strategy = build_strategy(strategy_spec, params)
            result = run_backtest(
                bars=bars,
                strategy=strategy,
                initial_cash=float(params.get("initial_cash", self.config.initial_cash)),
                enable_trade_log=False,
                strict_missing_execution_prices=False,
                market_data=market_data,
            )
            equity_curve = [
                EquityPoint(trade_date=point.trade_date, nav=point.equity / result.initial_cash)
                for point in result.equity_curve
            ]
            benchmark_service = BenchmarkService(market_data)
            benchmarks = benchmark_service.build_for_curve(
                equity_curve, definition.default_benchmarks
            )
            holdings = self._extract_holdings(
                market_data=market_data,
                broker=result.broker,
                as_of_date=equity_curve[-1].trade_date if equity_curve else end_date,
                base_equity=result.final_equity,
            )
            kelly = KellyService(
                scale=self.config.default_kelly_scale,
                max_gross_exposure=self.config.max_gross_exposure,
                min_observations=self.config.min_kelly_observations,
            ).evaluate(equity_curve, holdings)
            metrics = {
                "final_equity": result.final_equity,
                "cagr": result.risk.get("CAGR", 0.0),
                "ann_return": result.risk.get("AnnReturn", 0.0),
                "ann_vol": result.risk.get("AnnVol", 0.0),
                "sharpe": result.risk.get("Sharpe", 0.0),
                "max_drawdown": result.max_drawdown,
                "drawdown_peak": result.dd_peak,
                "drawdown_trough": result.dd_trough,
                "trade_count": len(result.broker.trades),
                "rebalance_count": len(self._standardize_rebalance_history(strategy)),
                "kelly_deploy_ratio": kelly.deploy_ratio,
                "cash_weight": kelly.cash_weight,
            }
            run_id, run_tag, output_dir = self._new_run_paths(definition.strategy_key)
            return StandardSnapshot(
                strategy_key=definition.strategy_key,
                run_id=run_id,
                run_tag=run_tag,
                as_of_date=end_date,
                start_date=equity_curve[0].trade_date if equity_curve else end_date,
                end_date=equity_curve[-1].trade_date if equity_curve else end_date,
                initial_cash=result.initial_cash,
                params=params,
                metrics=metrics,
                equity_curve=equity_curve,
                benchmarks=benchmarks,
                holdings=kelly.scaled_holdings,
                rebalance_history=self._standardize_rebalance_history(strategy),
                output_dir=str(output_dir),
            )
        finally:
            market_data.close()

    def _load_artifact_snapshot(
        self,
        definition,
        params_override: dict[str, Any] | None,
        as_of_date: str | None,
    ) -> StandardSnapshot:
        params = dict(definition.default_params)
        if params_override:
            params.update(params_override)
        artifact_dir = Path(params["artifact_dir"])
        summary_payload = json.loads((artifact_dir / "final_summary.json").read_text(encoding="utf-8"))
        summary = summary_payload.get("summary", {})
        equity_curve = self._load_artifact_equity(artifact_dir / "final_equity_curve.csv")
        market_data = SqliteMarketData(self.config.market_db_path)
        try:
            benchmarks = self._load_artifact_benchmarks(
                artifact_dir / "final_benchmarks_monthly.csv"
            )
            if not benchmarks:
                benchmarks = BenchmarkService(market_data).build_for_curve(
                    equity_curve, definition.default_benchmarks
                )
            holdings = self._artifact_holdings(
                market_data=market_data,
                as_of_date=as_of_date or summary.get("EndDate") or equity_curve[-1].trade_date,
                summary_payload=summary_payload,
                initial_cash=self.config.initial_cash,
            )
            kelly = KellyService(
                scale=self.config.default_kelly_scale,
                max_gross_exposure=self.config.max_gross_exposure,
                min_observations=self.config.min_kelly_observations,
            ).evaluate(equity_curve, holdings)
            run_id, run_tag, output_dir = self._new_run_paths(definition.strategy_key)
            return StandardSnapshot(
                strategy_key=definition.strategy_key,
                run_id=run_id,
                run_tag=run_tag,
                as_of_date=as_of_date or summary.get("EndDate") or equity_curve[-1].trade_date,
                start_date=summary.get("StartDate") or equity_curve[0].trade_date,
                end_date=summary.get("EndDate") or equity_curve[-1].trade_date,
                initial_cash=self.config.initial_cash,
                params=params,
                metrics={
                    "final_equity": float(summary.get("FinalEquity", equity_curve[-1].nav)),
                    "cagr": float(summary.get("CAGR", 0.0)),
                    "ann_return": float(summary.get("AnnReturn", 0.0)),
                    "ann_vol": float(summary.get("AnnVol", 0.0)),
                    "sharpe": float(summary.get("Sharpe", 0.0)),
                    "max_drawdown": float(summary.get("MaxDrawdown", 0.0)),
                    "drawdown_peak": summary.get("DrawdownPeak"),
                    "drawdown_trough": summary.get("DrawdownTrough"),
                    "trade_count": int(summary.get("Trades", 0)),
                    "rebalance_count": len(summary_payload.get("last_trades", [])),
                    "kelly_deploy_ratio": kelly.deploy_ratio,
                    "cash_weight": kelly.cash_weight,
                },
                equity_curve=equity_curve,
                benchmarks=benchmarks,
                holdings=kelly.scaled_holdings,
                rebalance_history=self._standardize_artifact_history(summary_payload.get("last_trades", [])),
                output_dir=str(output_dir),
                source_type="artifact",
            )
        finally:
            market_data.close()

    def _resolve_end_date(self, feed_type: str, params: dict[str, Any], market_data: SqliteMarketData) -> str:
        if feed_type == "calendar_etf":
            table = "etf_daily"
        elif feed_type == "single":
            table = "daily_a"
        else:
            table = "daily_a"
        end_date = params.get("end_date")
        if end_date:
            return str(end_date)
        latest = market_data.get_latest_trade_date(table=table, on_or_before="29991231")
        if latest is None:
            raise RuntimeError(f"no latest trade date for table={table}")
        return latest

    def _load_feed(self, feed_type: str, params: dict[str, Any], end_date: str):
        start_date = str(params.get("start_date", "20100101"))
        if feed_type == "calendar_ah":
            return load_calendar_bars_from_sqlite(
                db_path=self.config.market_db_path,
                start_date=start_date,
                end_date=end_date,
            )
        if feed_type == "calendar_etf":
            return self._load_etf_intersection_bars(
                symbols=params["symbols"],
                start_date=start_date,
                end_date=end_date,
            )
        if feed_type == "monthly_panel":
            from qs.strategy.ignored_crowded_ah_monthly import (
                build_monthly_bars_from_panel,
                load_trade_panel,
            )

            panel = load_trade_panel(params["panel_path"])
            return build_monthly_bars_from_panel(panel, start_date=start_date, end_date=end_date)
        if feed_type == "single":
            return load_bars_from_sqlite(
                db_path=self.config.market_db_path,
                table=params.get("table", "daily_a"),
                ts_code=params["symbol"],
                start_date=start_date,
                end_date=end_date,
            )
        raise ValueError(f"unsupported feed_type={feed_type}")

    def _prepare_params(
        self, feed_type: str, params: dict[str, Any], end_date: str
    ) -> dict[str, Any]:
        prepared = dict(params)
        if feed_type != "calendar_etf":
            return prepared
        symbols = [str(sym).strip() for sym in prepared.get("symbols", []) if str(sym).strip()]
        start_date = str(prepared.get("start_date", "20100101"))
        if not symbols:
            return prepared
        con = connect_sqlite(self.config.market_db_path, read_only=True)
        try:
            in_list = ",".join([repr(sym) for sym in symbols])
            rows = con.execute(
                f"""
                SELECT ts_code, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
                FROM etf_daily
                WHERE ts_code IN ({in_list})
                GROUP BY ts_code
                """
            ).fetchall()
        finally:
            con.close()
        valid = [
            str(ts_code)
            for ts_code, first_date, last_date in rows
            if str(first_date or "") <= start_date and str(last_date or "") >= end_date
        ]
        if valid:
            prepared["symbols"] = valid
        return prepared

    def _load_etf_intersection_bars(
        self,
        *,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> list[Bar]:
        syms = [str(sym).strip() for sym in symbols if str(sym).strip()]
        if not syms:
            raise ValueError("symbols must not be empty")
        in_list = ",".join([repr(sym) for sym in syms])
        con = connect_sqlite(self.config.market_db_path, read_only=True)
        try:
            rows = con.execute(
                f"""
                SELECT
                    trade_date,
                    MIN(open) AS open,
                    MIN(high) AS high,
                    MIN(low) AS low,
                    MIN(close) AS close
                FROM etf_daily
                WHERE ts_code IN ({in_list})
                  AND trade_date >= ?
                  AND trade_date <= ?
                GROUP BY trade_date
                HAVING COUNT(DISTINCT ts_code) = ?
                ORDER BY trade_date
                """,
                (start_date, end_date, len(syms)),
            ).fetchall()
        finally:
            con.close()
        return [Bar(str(d), float(o), float(h), float(l), float(c), None) for d, o, h, l, c in rows]

    def _extract_holdings(
        self,
        *,
        market_data: SqliteMarketData,
        broker,
        as_of_date: str,
        base_equity: float,
    ) -> list[HoldingSnapshot]:
        symbols = [sym for sym, pos in broker.positions.items() if pos.size > 0]
        if not symbols or base_equity <= 0:
            return []
        names = self._resolve_names(market_data, symbols)
        holdings: list[HoldingSnapshot] = []
        for symbol in symbols:
            pos = broker.positions[symbol]
            px = float(broker.last_prices.get(symbol) or pos.avg_price or 0.0)
            mv = float(pos.size) * px
            raw_weight = mv / base_equity if base_equity > 0 else 0.0
            meta = names.get(symbol, {"symbol_name": symbol, "market": self._infer_market(symbol)})
            holdings.append(
                HoldingSnapshot(
                    symbol=symbol,
                    symbol_name=str(meta["symbol_name"]),
                    market=str(meta["market"]),
                    price_cny=px,
                    quantity=float(pos.size),
                    market_value=mv,
                    raw_weight=raw_weight,
                    kelly_weight=raw_weight,
                    source_strategy_weight=raw_weight,
                )
            )
        return sorted(holdings, key=lambda item: item.raw_weight, reverse=True)

    def _artifact_holdings(
        self,
        *,
        market_data: SqliteMarketData,
        as_of_date: str,
        summary_payload: dict[str, Any],
        initial_cash: float,
    ) -> list[HoldingSnapshot]:
        trades = summary_payload.get("last_trades", [])
        target_str = str(trades[-1].get("target", "")) if trades else ""
        symbols = [sym.strip() for sym in target_str.split(",") if sym.strip()]
        if not symbols:
            return []
        names = self._resolve_names(market_data, symbols)
        weight = 1.0 / len(symbols)
        holdings: list[HoldingSnapshot] = []
        for symbol in symbols:
            table = "daily_h" if symbol.endswith(".HK") else "daily_a"
            price = market_data.get_price_map(
                request=_price_request(table), symbols=[symbol], trade_date=as_of_date
            ).get(symbol, 0.0)
            if symbol.endswith(".HK"):
                fx = market_data.get_hk_to_cny_rate(as_of_date)
                if fx:
                    price *= fx
            market_value = initial_cash * weight
            quantity = market_value / price if price else 0.0
            meta = names.get(symbol, {"symbol_name": symbol, "market": self._infer_market(symbol)})
            holdings.append(
                HoldingSnapshot(
                    symbol=symbol,
                    symbol_name=str(meta["symbol_name"]),
                    market=str(meta["market"]),
                    price_cny=float(price),
                    quantity=float(quantity),
                    market_value=float(market_value),
                    raw_weight=weight,
                    kelly_weight=weight,
                    source_strategy_weight=weight,
                )
            )
        return holdings

    def _standardize_rebalance_history(self, strategy: Any) -> list[dict[str, Any]]:
        raw = []
        if hasattr(strategy, "get_rebalance_history"):
            raw = list(strategy.get_rebalance_history())
        elif hasattr(strategy, "get_check_history"):
            raw = list(strategy.get_check_history())
        standardized = []
        for item in raw:
            if not isinstance(item, dict):
                if hasattr(item, "__dict__"):
                    item = dict(item.__dict__)
                else:
                    item = json.loads(json.dumps(item, default=str))
            targets = item.get("targets") or item.get("target") or []
            if isinstance(targets, str):
                targets = [sym for sym in targets.split(",") if sym]
            elif isinstance(targets, dict):
                targets = list(targets.keys())
            standardized.append(
                {
                    "rebalance_date": item.get("rebalance_date")
                    or item.get("trade_date")
                    or item.get("hold_month")
                    or item.get("week_key")
                    or "",
                    "signal_date": item.get("signal_date") or item.get("signal_month"),
                    "target_count": item.get("position_count") or len(targets),
                    "targets_json": targets,
                    "decision_items_json": item,
                }
            )
        return standardized

    def _standardize_artifact_history(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "rebalance_date": item.get("hold_month", ""),
                "signal_date": item.get("signal_month"),
                "target_count": item.get("position_count", 0),
                "targets_json": [
                    sym.strip()
                    for sym in str(item.get("target", "")).split(",")
                    if sym.strip()
                ],
                "decision_items_json": item,
            }
            for item in items
        ]

    def _resolve_names(self, market_data: SqliteMarketData, symbols: list[str]) -> dict[str, dict[str, str]]:
        etf_values = market_data.get_reference_values(
            table="etf_basic",
            symbols=symbols,
            fields=["csname", "exchange"],
        )
        hk_symbols = [sym for sym in symbols if sym.endswith(".HK")]
        a_symbols = [sym for sym in symbols if sym.endswith(".SH") or sym.endswith(".SZ")]
        stock_a = market_data.get_reference_values(
            table="stock_basic_a",
            symbols=a_symbols,
            fields=["name", "market"],
        )
        stock_h = market_data.get_reference_values(
            table="stock_basic_h",
            symbols=hk_symbols,
            fields=["name", "market"],
        )
        out: dict[str, dict[str, str]] = {}
        for symbol in symbols:
            if symbol in etf_values:
                row = etf_values[symbol]
                out[symbol] = {
                    "symbol_name": str(row.get("csname") or symbol),
                    "market": str(row.get("exchange") or "ETF"),
                }
                continue
            row = stock_h.get(symbol) or stock_a.get(symbol) or {}
            out[symbol] = {
                "symbol_name": str(row.get("name") or symbol),
                "market": str(row.get("market") or self._infer_market(symbol)),
            }
        return out

    def _write_snapshot_files(self, snapshot: StandardSnapshot) -> None:
        out_dir = Path(snapshot.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "strategy_key": snapshot.strategy_key,
                    "run_id": snapshot.run_id,
                    "run_tag": snapshot.run_tag,
                    "as_of_date": snapshot.as_of_date,
                    "source_type": snapshot.source_type,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (out_dir / "summary.json").write_text(
            json.dumps(snapshot.to_summary_dict(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        with (out_dir / "equity_curve.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["trade_date", "nav"])
            for point in snapshot.equity_curve:
                writer.writerow([point.trade_date, point.nav])
        with (out_dir / "benchmarks.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["benchmark_code", "trade_date", "nav"])
            for point in snapshot.benchmarks:
                writer.writerow([point.benchmark_code, point.trade_date, point.nav])
        with (out_dir / "holdings.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "symbol",
                    "symbol_name",
                    "market",
                    "price_cny",
                    "quantity",
                    "market_value",
                    "raw_weight",
                    "kelly_weight",
                    "source_strategy_weight",
                ]
            )
            for holding in snapshot.holdings:
                writer.writerow(
                    [
                        holding.symbol,
                        holding.symbol_name,
                        holding.market,
                        holding.price_cny,
                        holding.quantity,
                        holding.market_value,
                        holding.raw_weight,
                        holding.kelly_weight,
                        holding.source_strategy_weight,
                    ]
                )
        (out_dir / "rebalance_history.json").write_text(
            json.dumps(snapshot.rebalance_history, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def _load_artifact_equity(self, path: Path) -> list[EquityPoint]:
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [
                EquityPoint(trade_date=str(row["trade_date"]), nav=float(row["equity"]))
                for row in reader
            ]

    def _load_artifact_benchmarks(self, path: Path) -> list[BenchmarkPoint]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [
                BenchmarkPoint(
                    benchmark_code=str(row["ts_code"]),
                    trade_date=str(row["trade_date"]),
                    nav=float(row["nav"]),
                )
                for row in reader
            ]

    def _new_run_paths(self, strategy_key: str) -> tuple[str, str, Path]:
        now = datetime.now(timezone.utc)
        run_id = f"{strategy_key}-{uuid4().hex[:12]}"
        run_tag = now.strftime("%Y%m%dT%H%M%SZ")
        output_dir = self.config.snapshot_root / strategy_key / run_tag
        return run_id, run_tag, output_dir

    @staticmethod
    def _infer_market(symbol: str) -> str:
        if symbol.endswith(".HK"):
            return "HK"
        if symbol.endswith(".SH"):
            return "SH"
        if symbol.endswith(".SZ"):
            return "SZ"
        return "UNKNOWN"


def _price_request(table: str):
    from qs.backtester.market import PriceRequest

    return PriceRequest(table=table, field="close", adjusted=False, exact=False)
