from __future__ import annotations

import os
from pathlib import Path

_DOTENV_LOADED = False


def _load_env_file(path: Path) -> None:
    """Load KEY=VALUE pairs from a local .env file into os.environ (no override).

    This intentionally supports only a minimal subset of dotenv syntax and does
    not execute arbitrary code (unlike `source .env`).
    """

    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if (
            len(value) >= 2
            and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'"))
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _ensure_dotenv_loaded() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True

    repo_root = Path(__file__).resolve().parents[2]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        _load_env_file(dotenv_path)


def _get_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def get_tushare_token() -> str:
    _ensure_dotenv_loaded()
    token = _get_env("tushare_api_token") or _get_env("TUSHARE_API_TOKEN")
    if not token:
        raise RuntimeError(
            "Missing `tushare_api_token`. Create `.env` from `.env.example` and set it, "
            "or export it in the environment."
        )
    return token


def get_start_date(default: str = "20120101") -> str:
    _ensure_dotenv_loaded()
    return _get_env("start_date") or _get_env("START_DATE") or default
