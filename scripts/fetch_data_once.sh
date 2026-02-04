#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ENV_NAME="${1:-myqs}"
MODE="${2:-etf}"

CONDA_BIN=""
if command -v conda >/dev/null 2>&1; then
  CONDA_BIN="conda"
elif [[ -n "${CONDA_EXE:-}" && -x "${CONDA_EXE}" ]]; then
  CONDA_BIN="${CONDA_EXE}"
fi

cd "${REPO_ROOT}"

if [[ ! -f ".env" ]]; then
  echo "WARN: .env not found; copy .env.example to .env and set tushare_api_token." >&2
fi

if [[ -n "${CONDA_BIN}" ]]; then
  if [[ "${MODE}" == "all" ]]; then
    "${CONDA_BIN}" run -n "${ENV_NAME}" python scripts/fetch_data.py
  else
    "${CONDA_BIN}" run -n "${ENV_NAME}" python -c "import _bootstrap; from data_fetcher.tushare_sync_basic import data_sync; data_sync()"
    "${CONDA_BIN}" run -n "${ENV_NAME}" python -c "import _bootstrap; from data_fetcher.tushare_sync_daily import sync; sync(['etf_daily','adj_factor_etf','index_daily_etf'])"
  fi
else
  echo "WARN: conda not found in PATH; running with current python (ignoring env name: ${ENV_NAME})." >&2
  if [[ "${MODE}" == "all" ]]; then
    python scripts/fetch_data.py
  else
    python -c "import _bootstrap; from data_fetcher.tushare_sync_basic import data_sync; data_sync()"
    python -c "import _bootstrap; from data_fetcher.tushare_sync_daily import sync; sync(['etf_daily','adj_factor_etf','index_daily_etf'])"
  fi
fi
