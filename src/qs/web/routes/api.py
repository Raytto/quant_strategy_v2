from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from qs.backtester.stats import compute_max_drawdown, compute_risk_metrics

from ..models.dto import (
    BenchmarkPoint,
    ComboComponentWeight,
    ComboResult,
    EquityPoint,
    HoldingSnapshot,
    StandardSnapshot,
)


router = APIRouter(prefix="/api")


class ComposerRequest(BaseModel):
    strategies: list[dict[str, str]]
    optimizer: dict[str, Any] = Field(default_factory=dict)
    benchmarks: list[str] = Field(default_factory=list)


def _repo(request: Request):
    return request.app.state.web_repo


def _snapshot_service(request: Request):
    return request.app.state.snapshot_service


def _composer_service(request: Request):
    return request.app.state.composer_service


def _require_admin(request: Request) -> None:
    auth_service = request.app.state.auth_service
    current_user = auth_service.get_current_user(request)
    if current_user is None:
        raise HTTPException(status_code=401, detail="login required")
    if not auth_service.is_admin(current_user):
        raise HTTPException(status_code=403, detail="admin required")


@router.get("/strategies")
def list_strategies(request: Request):
    rows = _repo(request).list_strategies()
    return [
        {
            "strategy_key": row.strategy_key,
            "display_name": row.display_name,
            "description": row.description,
            "category": row.category,
            "status": row.status,
            "supports_composer": row.supports_composer,
            "source_type": row.source_type,
            "default_params": row.default_params,
            "default_benchmarks": row.default_benchmarks,
            "latest_run_id": row.latest_run_id,
            "latest_run_tag": row.latest_run_tag,
            "latest_completed_at": row.latest_completed_at,
            "metrics": row.metrics,
            "holding_count": row.holding_count,
        }
        for row in rows
    ]


@router.get("/strategies/{strategy_key}")
def get_strategy(request: Request, strategy_key: str):
    repo = _repo(request)
    definition = repo.get_strategy_definition(strategy_key)
    latest = repo.get_strategy_latest(strategy_key)
    if definition is None:
        raise HTTPException(status_code=404, detail="strategy not found")
    return {"definition": definition, "latest": latest.__dict__ if latest else None}


@router.get("/strategies/{strategy_key}/latest")
def get_strategy_latest(request: Request, strategy_key: str):
    repo = _repo(request)
    run_id = repo.get_latest_run_id(strategy_key)
    if run_id is None:
        raise HTTPException(status_code=404, detail="latest run not found")
    summary = repo.get_run_summary(run_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="run not found")
    summary["equity_points"] = len(repo.get_run_equity(run_id))
    summary["holding_count"] = len(repo.get_run_holdings(run_id))
    return summary


@router.get("/runs/{run_id}/equity")
def get_run_equity(request: Request, run_id: str):
    return _repo(request).get_run_equity(run_id)


@router.get("/runs/{run_id}/benchmarks")
def get_run_benchmarks(request: Request, run_id: str):
    return _repo(request).get_run_benchmarks(run_id)


@router.get("/runs/{run_id}/metrics-compare")
def get_run_metrics_compare(request: Request, run_id: str):
    repo = _repo(request)
    summary = repo.get_run_summary(run_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="run not found")

    definition = repo.get_strategy_definition(summary["strategy_key"])
    benchmark_rows = repo.get_run_benchmarks(run_id)
    benchmark_groups: dict[str, list[dict[str, Any]]] = {}
    for row in benchmark_rows:
        benchmark_groups.setdefault(row["benchmark_code"], []).append(row)

    ordered_codes: list[str] = []
    if definition is not None:
        ordered_codes.extend(definition.get("default_benchmarks", []))
    ordered_codes.extend(code for code in benchmark_groups if code not in ordered_codes)

    columns = [
        {
            "key": "strategy",
            "label": "Strategy",
            "metrics": {
                "cagr": summary["metrics"].get("cagr"),
                "ann_return": summary["metrics"].get("ann_return"),
                "sharpe": summary["metrics"].get("sharpe"),
                "max_drawdown": summary["metrics"].get("max_drawdown"),
                "kelly_deploy_ratio": summary["metrics"].get("kelly_deploy_ratio"),
            },
        }
    ]
    for code in ordered_codes:
        rows = benchmark_groups.get(code, [])
        columns.append(
            {
                "key": code,
                "label": code,
                "metrics": _compute_nav_metrics(rows),
            }
        )

    return {
        "run_id": run_id,
        "columns": columns,
    }


@router.get("/runs/{run_id}/holdings")
def get_run_holdings(request: Request, run_id: str):
    return _repo(request).get_run_holdings(run_id)


@router.get("/runs/{run_id}/rebalances")
def get_run_rebalances(request: Request, run_id: str):
    return _repo(request).get_run_rebalances(run_id)


@router.post("/strategies/{strategy_key}/refresh")
def refresh_strategy(request: Request, strategy_key: str, payload: dict[str, Any] | None = None):
    _require_admin(request)
    payload = payload or {}
    try:
        snapshot = _snapshot_service(request).refresh_strategy(
            strategy_key,
            params_override=payload.get("params"),
            as_of_date=payload.get("as_of_date"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{type(exc).__name__}: {exc}",
        ) from exc
    return snapshot.to_summary_dict()


@router.post("/composer/evaluate")
def evaluate_composer(request: Request, body: ComposerRequest):
    repo = _repo(request)
    snapshots = []
    for item in body.strategies:
        strategy_key = item["strategy_key"]
        run_id = repo.get_latest_run_id(strategy_key)
        if run_id is None:
            raise HTTPException(status_code=400, detail=f"strategy {strategy_key} has no latest run")
        summary = repo.get_run_summary(run_id)
        if summary is None:
            raise HTTPException(status_code=404, detail=f"run {run_id} not found")
        snapshots.append(_snapshot_from_repo(repo, summary))
    result = _composer_service(request).evaluate(
        snapshots,
        optimizer=body.optimizer,
        benchmarks=body.benchmarks,
    )
    return _combo_to_dict(result)


@router.post("/composer/save")
def save_composer(request: Request, body: ComposerRequest):
    repo = _repo(request)
    snapshots = []
    for item in body.strategies:
        strategy_key = item["strategy_key"]
        run_id = repo.get_latest_run_id(strategy_key)
        if run_id is None:
            raise HTTPException(status_code=400, detail=f"strategy {strategy_key} has no latest run")
        summary = repo.get_run_summary(run_id)
        if summary is None:
            raise HTTPException(status_code=404, detail=f"run {run_id} not found")
        snapshots.append(_snapshot_from_repo(repo, summary))
    result = _composer_service(request).evaluate(
        snapshots,
        optimizer=body.optimizer,
        benchmarks=body.benchmarks,
    )
    repo.save_combo_result(result)
    return _combo_to_dict(result)


def _snapshot_from_repo(repo, summary: dict[str, Any]) -> StandardSnapshot:
    run_id = summary["run_id"]
    return StandardSnapshot(
        strategy_key=summary["strategy_key"],
        run_id=run_id,
        run_tag=summary["run_tag"],
        as_of_date=summary["as_of_date"],
        start_date=summary["start_date"],
        end_date=summary["end_date"],
        initial_cash=float(summary["initial_cash"]),
        params=summary["params"],
        metrics=summary["metrics"],
        equity_curve=[EquityPoint(**row) for row in repo.get_run_equity(run_id)],
        benchmarks=[BenchmarkPoint(**row) for row in repo.get_run_benchmarks(run_id)],
        holdings=[HoldingSnapshot(**row) for row in repo.get_run_holdings(run_id)],
        rebalance_history=repo.get_run_rebalances(run_id),
        output_dir=summary["output_dir"],
        source_type=summary.get("source_type", "framework"),
    )


def _combo_to_dict(result: ComboResult) -> dict[str, Any]:
    return {
        "combo_run_id": result.combo_run_id,
        "selected_strategies": result.selected_strategies,
        "optimizer_config": result.optimizer_config,
        "metrics": result.metrics,
        "equity_curve": [point.__dict__ for point in result.equity_curve],
        "benchmarks": [point.__dict__ for point in result.benchmarks],
        "component_weights": [item.__dict__ for item in result.component_weights],
        "holdings": [item.__dict__ for item in result.holdings],
    }


def _compute_nav_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda item: str(item["trade_date"]))
    curve = [
        SimpleNamespace(trade_date=str(row["trade_date"]), equity=float(row["nav"]))
        for row in ordered
    ]
    if len(curve) < 2:
        return {
            "cagr": None,
            "ann_return": None,
            "sharpe": None,
            "max_drawdown": None,
            "kelly_deploy_ratio": None,
        }
    risk = compute_risk_metrics(curve, initial_equity=curve[0].equity)
    max_dd, _, _ = compute_max_drawdown(curve)
    return {
        "cagr": risk.get("CAGR"),
        "ann_return": risk.get("AnnReturn"),
        "sharpe": risk.get("Sharpe"),
        "max_drawdown": max_dd,
        "kelly_deploy_ratio": None,
    }
