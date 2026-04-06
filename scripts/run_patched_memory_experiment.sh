#!/bin/zsh
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <report_path> <server_log_path> <ingester_log_path>" >&2
  exit 1
fi

REPORT_PATH="$1"
SERVER_LOG_PATH="$2"
INGESTER_LOG_PATH="$3"

: "${DS_ROOT:?set DS_ROOT}"
: "${PORT:?set PORT}"
: "${DURABLE_STREAMS_R2_ACCOUNT_ID:?set DURABLE_STREAMS_R2_ACCOUNT_ID}"
: "${DURABLE_STREAMS_R2_BUCKET:?set DURABLE_STREAMS_R2_BUCKET}"
: "${DURABLE_STREAMS_R2_ACCESS_KEY_ID:?set DURABLE_STREAMS_R2_ACCESS_KEY_ID}"
: "${DURABLE_STREAMS_R2_SECRET_ACCESS_KEY:?set DURABLE_STREAMS_R2_SECRET_ACCESS_KEY}"
: "${DS_LOCAL_BACKLOG_MAX_BYTES:?set DS_LOCAL_BACKLOG_MAX_BYTES}"
: "${DS_SEGMENT_CACHE_MAX_BYTES:?set DS_SEGMENT_CACHE_MAX_BYTES}"
: "${DS_INDEX_RUN_CACHE_MAX_BYTES:?set DS_INDEX_RUN_CACHE_MAX_BYTES}"

BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT}}"
STREAM_PREFIX="${STREAM_PREFIX:-gharchive-memtest}"
STREAM_NAME="${STREAM_PREFIX}-all"
AUTO_TUNE_MB="${AUTO_TUNE_MB:-4096}"
INGEST_SAMPLES="${INGEST_SAMPLES:-6}"
RECOVERY_SAMPLES="${RECOVERY_SAMPLES:-6}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-300}"

COLLECT_SCRIPT="$(cd "$(dirname "$0")" && pwd)/collect-memory-investigation-sample.sh"

server_pid=""
ingester_pid=""

cleanup() {
  if [[ -n "${ingester_pid}" ]] && kill -0 "${ingester_pid}" 2>/dev/null; then
    kill -TERM "${ingester_pid}" 2>/dev/null || true
    wait "${ingester_pid}" 2>/dev/null || true
  fi
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
    kill -TERM "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

append_note() {
  local title="$1"
  shift
  {
    echo
    echo "### $(date '+%Y-%m-%d %H:%M:%S %Z') | ${title}"
    echo
    echo '```text'
    printf '%s\n' "$@"
    echo '```'
  } >> "${REPORT_PATH}"
}

mkdir -p "$(dirname "${SERVER_LOG_PATH}")" "$(dirname "${INGESTER_LOG_PATH}")"

append_note "patched-phase-start" \
  "server_log=${SERVER_LOG_PATH}" \
  "ingester_log=${INGESTER_LOG_PATH}" \
  "base_url=${BASE_URL}" \
  "stream=${STREAM_NAME}" \
  "sample_interval_seconds=${SAMPLE_INTERVAL_SECONDS}" \
  "ingest_samples=${INGEST_SAMPLES}" \
  "recovery_samples=${RECOVERY_SAMPLES}"

(
  export MIMALLOC_SHOW_STATS="${MIMALLOC_SHOW_STATS:-1}"
  bun run src/server.ts --object-store r2 --auto-tune="${AUTO_TUNE_MB}"
) >"${SERVER_LOG_PATH}" 2>&1 &
server_pid=$!

append_note "patched-server-started" "pid=${server_pid}"

for _ in {1..120}; do
  if curl -fsS --connect-timeout 1 --max-time 5 "${BASE_URL}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS --connect-timeout 1 --max-time 5 "${BASE_URL}/health" >/dev/null 2>&1; then
  append_note "patched-server-start-failed" "pid=${server_pid}" "base_url=${BASE_URL}"
  exit 1
fi

"${COLLECT_SCRIPT}" "${REPORT_PATH}" "patched-server-before-ingest" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}"

(
  bun run demo:gharchive all \
    --url "${BASE_URL}" \
    --stream-prefix "${STREAM_PREFIX}" \
    --debug-progress \
    --debug-progress-interval-ms 5000
) >"${INGESTER_LOG_PATH}" 2>&1 &
ingester_pid=$!

append_note "patched-ingester-started" "pid=${ingester_pid}" "stream=${STREAM_NAME}"

for ((i = 1; i <= INGEST_SAMPLES; i++)); do
  sleep "${SAMPLE_INTERVAL_SECONDS}"
  "${COLLECT_SCRIPT}" "${REPORT_PATH}" "patched-server-during-all-ingest" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}"
done

if kill -0 "${ingester_pid}" 2>/dev/null; then
  kill -TERM "${ingester_pid}" 2>/dev/null || true
  wait "${ingester_pid}" 2>/dev/null || true
  append_note "patched-ingester-stopped" "pid=${ingester_pid}" "signal=TERM"
else
  wait "${ingester_pid}" 2>/dev/null || true
  append_note "patched-ingester-finished-before-stop" "pid=${ingester_pid}"
fi
ingester_pid=""

"${COLLECT_SCRIPT}" "${REPORT_PATH}" "patched-server-after-ingest-stop" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}"

for ((i = 1; i <= RECOVERY_SAMPLES; i++)); do
  sleep "${SAMPLE_INTERVAL_SECONDS}"
  "${COLLECT_SCRIPT}" "${REPORT_PATH}" "patched-server-recovery-after-ingest-stop" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}"
done

if kill -0 "${server_pid}" 2>/dev/null; then
  kill -TERM "${server_pid}" 2>/dev/null || true
  wait "${server_pid}" 2>/dev/null || true
  append_note "patched-server-stopped" "pid=${server_pid}" "signal=TERM"
fi
server_pid=""
