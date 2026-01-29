#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ENV_NAME="${1:-myqs}"

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
  "${CONDA_BIN}" run -n "${ENV_NAME}" python scripts/fetch_data.py
else
  echo "WARN: conda not found in PATH; running with current python (ignoring env name: ${ENV_NAME})." >&2
  python scripts/fetch_data.py
fi
