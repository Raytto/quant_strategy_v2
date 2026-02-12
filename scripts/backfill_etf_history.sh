#!/usr/bin/env bash
set -euo pipefail

# Backfill ETF history into SQLite by pulling missing older ranges.
#
# This uses `tushare_sync_daily.py --backfill` which:
# - starts each ETF from its `list_date` (when available), and
# - for existing partial data, backfills the missing earlier window.
#
# Notes:
# - This can take a long time and consume TuShare API quota/rate limits.
# - You can optionally filter a few symbols via `--ts-codes`.

ENV_NAME="${1:-myqs}"
shift || true

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

CONDA_BIN=""
if command -v conda >/dev/null 2>&1; then
  CONDA_BIN="conda"
elif [[ -n "${CONDA_EXE:-}" && -x "${CONDA_EXE}" ]]; then
  CONDA_BIN="${CONDA_EXE}"
fi

if [[ -z "${CONDA_BIN}" ]]; then
  echo "ERROR: conda not found in PATH; please run in the correct environment." >&2
  exit 1
fi

exec env PYTHONPATH="${REPO_ROOT}/src" "${CONDA_BIN}" run --no-capture-output -n "${ENV_NAME}" python -m data_fetcher.tushare_sync_daily \
  --table etf_daily adj_factor_etf index_daily_etf \
  --backfill \
  "$@"
