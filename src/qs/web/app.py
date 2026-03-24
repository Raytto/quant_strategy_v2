from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from qs.backtester.market import SqliteMarketData

from .config import DEFAULT_CONFIG, WebConfig
from .repo.web_db import WebDB
from .routes import api, pages
from .services.auth_service import AuthService
from .services.composer_service import ComposerService
from .services.snapshot_service import SnapshotService
from .services.strategy_registry import StrategyRegistry


@asynccontextmanager
async def _lifespan(app: FastAPI):
    config: WebConfig = app.state.web_config
    registry = StrategyRegistry()
    repo = WebDB(config.web_db_path)
    market_data = SqliteMarketData(config.market_db_path)
    repo.init_db()
    repo.upsert_strategy_definitions(registry.list_definitions())
    auth_service = AuthService(repo)
    auth_service.ensure_default_admin()
    app.state.registry = registry
    app.state.web_repo = repo
    app.state.auth_service = auth_service
    app.state.snapshot_service = SnapshotService(config=config, registry=registry, repo=repo)
    app.state.market_data = market_data
    app.state.composer_service = ComposerService(market_db_path=config.market_db_path)
    yield
    market_data.close()


def create_app(config: WebConfig | None = None) -> FastAPI:
    config = config or DEFAULT_CONFIG
    app = FastAPI(
        title="qs web",
        root_path=config.root_path,
        lifespan=_lifespan,
    )
    base_dir = Path(__file__).resolve().parent
    app.state.web_config = config
    app.state.templates = Jinja2Templates(directory=str(base_dir / "templates"))
    app.state.templates.env.globals["site_root"] = config.root_path
    app.add_middleware(
        SessionMiddleware,
        secret_key=config.session_secret,
        session_cookie=config.session_cookie_name,
    )

    app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")
    app.include_router(pages.router)
    app.include_router(api.router)
    return app
