from __future__ import annotations

import _bootstrap  # noqa: F401

import argparse

from qs.web.config import DEFAULT_CONFIG
from qs.web.repo.web_db import WebDB
from qs.web.services.snapshot_service import SnapshotService
from qs.web.services.strategy_registry import StrategyRegistry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh QS web snapshots")
    parser.add_argument("--strategy", default="", help="Optional strategy_key to refresh")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    registry = StrategyRegistry()
    repo = WebDB(DEFAULT_CONFIG.web_db_path)
    repo.init_db()
    repo.upsert_strategy_definitions(registry.list_definitions())
    service = SnapshotService(config=DEFAULT_CONFIG, registry=registry, repo=repo)
    if args.strategy:
        snapshot = service.refresh_strategy(args.strategy)
        print(f"refreshed {snapshot.strategy_key} -> {snapshot.run_id}")
        return
    snapshots = service.refresh_all()
    print(f"refreshed {len(snapshots)} snapshots")


if __name__ == "__main__":
    main()
