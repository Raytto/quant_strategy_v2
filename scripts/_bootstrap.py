from __future__ import annotations

import sys
from pathlib import Path


def add_src_to_sys_path() -> Path:
    """Ensure the repo's `src/` is importable when running scripts directly.

    Running `python scripts/foo.py` puts `scripts/` on `sys.path`, not the repo root.
    This project uses a `src/` layout, so we add `<repo>/src` to `sys.path` to make
    imports like `import qs` / `import data_fetcher` work without installing.
    """

    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if src_dir.is_dir():
        src_str = str(src_dir)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)
    return src_dir


add_src_to_sys_path()

