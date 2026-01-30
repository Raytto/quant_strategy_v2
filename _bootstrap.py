from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
RAW_DB_PATH = DATA_DIR / "data.sqlite"
PROCESSED_DB_PATH = DATA_DIR / "data_processed.sqlite"


def add_src_to_sys_path() -> Path:
    """Ensure the repo's `src/` is importable when running from repo root.

    This is useful for Jupyter notebooks or ad-hoc `python -c ...` usage where
    the project isn't installed as a package but we still want imports like
    `import qs` / `import data_fetcher` to work.
    """

    src_dir = REPO_ROOT / "src"
    if src_dir.is_dir():
        src_str = str(src_dir)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)
    return src_dir


add_src_to_sys_path()
