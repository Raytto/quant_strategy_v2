from __future__ import annotations

from pathlib import Path

from qs.web.repo.web_db import WebDB
from qs.web.services.auth_service import AuthService, DEFAULT_ADMIN_PASSWORD_HASH
from qs.web.services.strategy_registry import StrategyRegistry


def test_web_repo_init_and_seed(tmp_path: Path):
    db_path = tmp_path / "web.sqlite"
    repo = WebDB(db_path)
    registry = StrategyRegistry()
    auth_service = AuthService(repo)

    repo.init_db()
    repo.upsert_strategy_definitions(registry.list_definitions())
    auth_service.ensure_default_admin()

    strategies = repo.list_strategies()
    keys = {item.strategy_key for item in strategies}
    assert "ah_premium_quarterly" in keys
    assert "ignored_buzz_ah_research" in keys

    admin_user = repo.get_user_by_username("pp")
    assert admin_user is not None
    assert admin_user["role"] == "admin"
    assert admin_user["password_hash"] == DEFAULT_ADMIN_PASSWORD_HASH
    assert admin_user["password_hash"] != "1994188"
