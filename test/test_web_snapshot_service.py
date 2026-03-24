from __future__ import annotations

from pathlib import Path

from qs.web.config import WebConfig
from qs.web.repo.web_db import WebDB
from qs.web.services.snapshot_service import SnapshotService
from qs.web.services.strategy_registry import StrategyRegistry


def test_artifact_snapshot_refresh_persists_snapshot(tmp_path: Path):
    config = WebConfig(
        market_db_path=Path("data/data.sqlite"),
        web_db_path=tmp_path / "web.sqlite",
        snapshot_root=tmp_path / "snapshots",
    )
    registry = StrategyRegistry()
    repo = WebDB(config.web_db_path)
    repo.init_db()
    repo.upsert_strategy_definitions(registry.list_definitions())
    service = SnapshotService(config=config, registry=registry, repo=repo)

    snapshot = service.refresh_strategy(
        "ignored_buzz_ah_research",
        params_override={"artifact_dir": str(Path("data/backtests/ignored_buzz_ah").resolve())},
    )

    assert snapshot.strategy_key == "ignored_buzz_ah_research"
    assert snapshot.equity_curve
    assert Path(snapshot.output_dir, "summary.json").exists()
    latest_run_id = repo.get_latest_run_id("ignored_buzz_ah_research")
    assert latest_run_id == snapshot.run_id
