from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
RAW_DB_PATH = DATA_DIR / "data.sqlite"
PROCESSED_DB_PATH = DATA_DIR / "data_processed.sqlite"


def add_src_to_sys_path() -> Path:
    """Ensure the repo's `src/` is importable when running notebooks.

    In Jupyter, the kernel's working directory is usually the notebook folder,
    so we add `<repo>/src` to `sys.path` to make imports like
    `import qs` / `import data_fetcher` work without installing the package.
    """

    src_dir = REPO_ROOT / "src"
    if src_dir.is_dir():
        src_str = str(src_dir)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)
    return src_dir


add_src_to_sys_path()
