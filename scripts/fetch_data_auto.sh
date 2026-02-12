#!/usr/bin/env bash
set -euo pipefail

SESSION_NAME="qs_fetch_data"
TZ_NAME="${QS_TZ:-Asia/Shanghai}"
RUN_HOUR="${QS_RUN_HOUR:-7}"
RUN_MIN="${QS_RUN_MIN:-0}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
ONCE_SCRIPT="${SCRIPT_DIR}/fetch_data_once.sh"
LOG_FILE_DEFAULT="${REPO_ROOT}/logs/fetch_data_auto.log"
LOG_FILE="${QS_LOG_FILE:-${LOG_FILE_DEFAULT}}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/fetch_data_auto.sh start [ENV_NAME] [MODE]
  bash scripts/fetch_data_auto.sh stop
  bash scripts/fetch_data_auto.sh status
  bash scripts/fetch_data_auto.sh attach

Behavior:
  - Creates/uses a screen session named: qs_fetch_data
  - Inside the session, runs scripts/fetch_data_once.sh once per day at 07:00 Beijing time (Asia/Shanghai)
  - MODE is passed through to fetch_data_once.sh (e.g. "all", "etf", "etf_backfill")
    - etf_backfill 会补齐 ETF 历史缺口（耗时/耗额度，建议首次跑或怀疑缺历史时使用；日常用 etf/all）

Environment overrides:
  - QS_TZ=Asia/Shanghai   Timezone used for scheduling
  - QS_RUN_HOUR=7         Hour for daily run (0-23)
  - QS_RUN_MIN=0          Minute for daily run (0-59)
  - QS_LOG_FILE=...       Log file path (default: logs/fetch_data_auto.log)
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: '${cmd}' not found in PATH." >&2
    exit 1
  fi
}

screen_session_exists() {
  screen -ls 2>/dev/null | grep -qE "[[:space:]]+[0-9]+\\.${SESSION_NAME}[[:space:]]"
}

log_line() {
  local msg="$1"
  local ts
  ts="$(date -Is)"
  mkdir -p -- "$(dirname -- "${LOG_FILE}")"
  printf '[%s] %s\n' "${ts}" "${msg}" | tee -a "${LOG_FILE}"
}

format_epoch_gnu_date() {
  local epoch="$1"
  TZ="${TZ_NAME}" date -d "@${epoch}" '+%F %T %z %Z'
}

format_epoch_python() {
  python - <<PY
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception as e:
    raise SystemExit("zoneinfo unavailable; please install tzdata or use GNU date") from e

tz = ZoneInfo("${TZ_NAME}")
print(datetime.fromtimestamp(int("${1}"), tz=tz).strftime("%Y-%m-%d %H:%M:%S %z %Z"))
PY
}

format_epoch() {
  local epoch="$1"
  if TZ="${TZ_NAME}" date -d "@${epoch}" '+%F %T %z %Z' >/dev/null 2>&1; then
    format_epoch_gnu_date "${epoch}"
  else
    format_epoch_python "${epoch}"
  fi
}

next_run_epoch_gnu_date() {
  local run_time now next
  run_time="$(printf '%02d:%02d:00' "${RUN_HOUR}" "${RUN_MIN}")"
  now="$(TZ="${TZ_NAME}" date +%s)"
  next="$(TZ="${TZ_NAME}" date -d "today ${run_time}" +%s)"
  if [[ "${now}" -ge "${next}" ]]; then
    next="$(TZ="${TZ_NAME}" date -d "tomorrow ${run_time}" +%s)"
  fi
  printf '%s\n' "${next}"
}

next_run_epoch_python() {
  python - <<PY
from datetime import datetime, time, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception as e:
    raise SystemExit("zoneinfo unavailable; please install tzdata or use GNU date") from e

tz = ZoneInfo("${TZ_NAME}")
now = datetime.now(tz=tz)
target = datetime.combine(now.date(), time(${RUN_HOUR}, ${RUN_MIN}), tzinfo=tz)
if now >= target:
    target = target + timedelta(days=1)
print(int(target.timestamp()))
PY
}

next_run_epoch() {
  if TZ="${TZ_NAME}" date -d "today 00:00:00" +%s >/dev/null 2>&1; then
    next_run_epoch_gnu_date
  else
    next_run_epoch_python
  fi
}

worker_loop() {
  local env_name="${1:-myqs}"
  local mode="${2:-all}"

  if [[ ! -f "${ONCE_SCRIPT}" ]]; then
    echo "ERROR: ${ONCE_SCRIPT} not found." >&2
    exit 1
  fi

  log_line "任务启动：env=${env_name}，mode=${mode}，时区=${TZ_NAME}，每日执行时间=$(printf '%02d:%02d' "${RUN_HOUR}" "${RUN_MIN}")"

  while true; do
    local now next sleep_s run_time next_str
    run_time="$(printf '%02d:%02d:00' "${RUN_HOUR}" "${RUN_MIN}")"
    now="$(TZ="${TZ_NAME}" date +%s)"
    next="$(next_run_epoch)"
    sleep_s="$((next - now))"
    if (( sleep_s < 0 )); then
      sleep_s=0
    fi

    next_str="$(format_epoch "${next}")"
    log_line "正在等待到 ${next_str}，还有 ${sleep_s} 秒（时区：${TZ_NAME}）"
    sleep "${sleep_s}"

    log_line "开始拉取数据：北京时间 $(TZ="${TZ_NAME}" date '+%F %T %z %Z')（env=${env_name}，mode=${mode}）"
    (
      cd -- "${REPO_ROOT}"
      bash "${ONCE_SCRIPT}" "${env_name}" "${mode}" 2>&1 | tee -a "${LOG_FILE}"
      exit "${PIPESTATUS[0]}"
    ) || {
      local ec=$?
      log_line "拉取失败（exit=${ec}）"
    }
    log_line "拉取完成：北京时间 $(TZ="${TZ_NAME}" date '+%F %T %z %Z')"
  done
}

cmd="${1:-start}"
shift || true

case "${cmd}" in
  start)
    require_cmd screen
    env_name="${1:-myqs}"
    mode="${2:-all}"

    if screen_session_exists; then
      echo "INFO: screen session already exists: ${SESSION_NAME}"
      exit 0
    fi

    # Start detached and run the worker loop inside.
    screen -S "${SESSION_NAME}" -dm bash -lc "$(printf '%q ' bash "${SCRIPT_DIR}/fetch_data_auto.sh" --worker "${env_name}" "${mode}")"
    next="$(next_run_epoch)"
    echo "OK: started screen session: ${SESSION_NAME}"
    echo "INFO: next run at $(format_epoch "${next}") (tz=${TZ_NAME})"
    ;;
  stop)
    require_cmd screen
    if screen_session_exists; then
      screen -S "${SESSION_NAME}" -X quit
      echo "OK: stopped screen session: ${SESSION_NAME}"
    else
      echo "INFO: screen session not found: ${SESSION_NAME}"
    fi
    ;;
  status)
    require_cmd screen
    screen -ls || true
    ;;
  attach)
    require_cmd screen
    exec screen -r "${SESSION_NAME}"
    ;;
  --worker)
    require_cmd screen
    worker_loop "${1:-myqs}" "${2:-all}"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "ERROR: unknown command: ${cmd}" >&2
    usage >&2
    exit 2
    ;;
esac
