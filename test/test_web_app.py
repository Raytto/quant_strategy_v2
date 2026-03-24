from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from qs.web.app import create_app
from qs.web.config import WebConfig
from qs.web.services.auth_service import hash_password


def test_web_app_pages_and_api(tmp_path: Path):
    config = WebConfig(
        market_db_path=Path("data/data.sqlite"),
        web_db_path=tmp_path / "web.sqlite",
        snapshot_root=tmp_path / "snapshots",
    )
    app = create_app(config)

    with TestClient(app) as client:
        login_page = client.get("/login")
        assert login_page.status_code == 200
        assert "管理员登录" in login_page.text
        assert 'name="username"' in login_page.text
        assert 'name="password"' in login_page.text

        refresh = client.post(
            "/api/strategies/ignored_buzz_ah_research/refresh",
            json={"params": {"artifact_dir": str(Path("data/backtests/ignored_buzz_ah").resolve())}},
        )
        assert refresh.status_code == 401

        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "策略快照总览" in dashboard.text

        detail = client.get("/strategies/ignored_buzz_ah_research")
        assert detail.status_code == 200
        assert "冷门 AH 研究产物" in detail.text
        assert "历史年度收益" in detail.text
        assert 'data-strategy="ignored_buzz_ah_research"' not in detail.text

        strategies_page = client.get("/strategies")
        assert strategies_page.status_code == 200
        assert "data-strategy=" not in strategies_page.text

        composer_page = client.get("/composer")
        assert composer_page.status_code == 200
        assert "历史年度收益" in composer_page.text

        bad_login = client.post(
            "/login",
            data={"username": "pp", "password": "wrong-password"},
        )
        assert bad_login.status_code == 401
        assert "用户名或密码错误" in bad_login.text

        login = client.post(
            "/login",
            data={"username": "pp", "password": "1994188"},
            follow_redirects=False,
        )
        assert login.status_code == 303

        detail = client.get("/strategies/ignored_buzz_ah_research")
        assert detail.status_code == 200
        assert 'data-strategy="ignored_buzz_ah_research"' in detail.text

        refresh = client.post(
            "/api/strategies/ignored_buzz_ah_research/refresh",
            json={"params": {"artifact_dir": str(Path("data/backtests/ignored_buzz_ah").resolve())}},
        )
        assert refresh.status_code == 200

        latest = client.get("/api/strategies/ignored_buzz_ah_research/latest")
        assert latest.status_code == 200
        payload = latest.json()
        assert payload["strategy_key"] == "ignored_buzz_ah_research"

        metrics_compare = client.get(
            f"/api/runs/{payload['run_id']}/metrics-compare"
        )
        assert metrics_compare.status_code == 200
        compare_payload = metrics_compare.json()
        assert compare_payload["columns"][0]["label"] == "Strategy"
        assert len(compare_payload["columns"]) >= 1

        combo = client.post(
            "/api/composer/evaluate",
            json={
                "strategies": [{"strategy_key": "ignored_buzz_ah_research"}],
                "optimizer": {"kelly_scale": 0.5, "max_strategy_weight": 1.0, "allow_cash": True},
                "benchmarks": ["000300.SH"],
            },
        )
        assert combo.status_code == 200
        assert combo.json()["equity_curve"]

        logout = client.post("/logout", follow_redirects=False)
        assert logout.status_code == 303
        assert (
            'data-strategy="ignored_buzz_ah_research"'
            not in client.get("/strategies/ignored_buzz_ah_research").text
        )


def test_non_admin_user_cannot_refresh(tmp_path: Path):
    config = WebConfig(
        market_db_path=Path("data/data.sqlite"),
        web_db_path=tmp_path / "web.sqlite",
        snapshot_root=tmp_path / "snapshots",
    )
    app = create_app(config)

    with TestClient(app) as client:
        client.app.state.web_repo.upsert_user(
            username="viewer",
            password_hash=hash_password("viewer-pass"),
            role="viewer",
            display_name="viewer",
            is_active=True,
        )

        login = client.post(
            "/login",
            data={"username": "viewer", "password": "viewer-pass"},
            follow_redirects=False,
        )
        assert login.status_code == 303

        detail = client.get("/strategies/ignored_buzz_ah_research")
        assert detail.status_code == 200
        assert 'data-strategy="ignored_buzz_ah_research"' not in detail.text

        refresh = client.post(
            "/api/strategies/ignored_buzz_ah_research/refresh",
            json={"params": {"artifact_dir": str(Path("data/backtests/ignored_buzz_ah").resolve())}},
        )
        assert refresh.status_code == 403
