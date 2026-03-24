from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WebConfig:
    root_path: str = "/qs"
    market_db_path: Path = Path("data/data.sqlite")
    web_db_path: Path = Path("data/web.sqlite")
    snapshot_root: Path = Path("data/backtests/web_snapshots")
    retention_days: int = 7
    initial_cash: float = 1_000_000.0
    max_gross_exposure: float = 1.0
    default_kelly_scale: float = 0.5
    min_kelly_observations: int = 20
    session_secret: str = "qs-web-session-secret"
    session_cookie_name: str = "qs_session"


DEFAULT_CONFIG = WebConfig()
