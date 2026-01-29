#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ENV_NAME="${1:-myqs}"

if ! command -v conda >/dev/null 2>&1; then
  echo "ERROR: conda not found in PATH. Please install Anaconda/Miniconda and retry." >&2
  exit 127
fi

# Initialize conda for non-interactive shells.
CONDA_BASE="$(conda info --base 2>/dev/null)"
if [[ -z "${CONDA_BASE}" || ! -f "${CONDA_BASE}/etc/profile.d/conda.sh" ]]; then
  echo "ERROR: unable to locate conda base. Try running this from an initialized conda shell." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${CONDA_BASE}/etc/profile.d/conda.sh"
conda activate "${ENV_NAME}"

cd "${REPO_ROOT}"

if [[ ! -f ".env" ]]; then
  echo "WARN: .env not found; copy .env.example to .env and set tushare_api_token." >&2
fi

python scripts/fetch_data.py
