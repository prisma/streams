#!/bin/zsh
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 <report_path> <server_log_path> <ingester_log_path> <sampler_path>" >&2
  exit 1
fi

REPORT_PATH="$1"
SERVER_LOG_PATH="$2"
INGESTER_LOG_PATH="$3"
SAMPLER_PATH="$4"

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
STREAM_PREFIX="${STREAM_PREFIX:-gharchive-memsampler}"
STREAM_NAME="${STREAM_PREFIX}-all"
AUTO_TUNE_MB="${AUTO_TUNE_MB:-4096}"
INGEST_SAMPLES="${INGEST_SAMPLES:-6}"
RECOVERY_SAMPLES="${RECOVERY_SAMPLES:-6}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-300}"
SAMPLER_INTERVAL_MS="${SAMPLER_INTERVAL_MS:-1000}"
DB_PATH="${DS_DB_PATH:-${DS_ROOT}/wal.sqlite}"
GHARCHIVE_NOINDEX="${GHARCHIVE_NOINDEX:-0}"
GHARCHIVE_ONLYINDEX="${GHARCHIVE_ONLYINDEX:-}"
DEBUG_PROGRESS_INTERVAL_MS="${DEBUG_PROGRESS_INTERVAL_MS:-5000}"
TARGET_UPLOADED_SEGMENTS="${TARGET_UPLOADED_SEGMENTS:-0}"
TARGET_LOGICAL_SIZE_BYTES="${TARGET_LOGICAL_SIZE_BYTES:-0}"
MAX_INGEST_SECONDS="${MAX_INGEST_SECONDS:-0}"

COLLECT_SCRIPT="$(cd "$(dirname "$0")" && pwd)/collect-memory-investigation-sample.sh"
SUMMARY_SCRIPT="$(cd "$(dirname "$0")" && pwd)/summarize_memory_sampler.ts"
BUN_BIN="${BUN_BIN:-$(command -v bun || true)}"

if [[ -z "${BUN_BIN}" ]]; then
  echo "bun binary not found in PATH" >&2
  exit 1
fi

export BUN_BIN

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

mkdir -p "$(dirname "${REPORT_PATH}")" "$(dirname "${SERVER_LOG_PATH}")" "$(dirname "${INGESTER_LOG_PATH}")" "$(dirname "${SAMPLER_PATH}")"
sampler_stale_files=("${SAMPLER_PATH%.jsonl}"*.jsonl(N))
if (( ${#sampler_stale_files[@]} > 0 )); then
  rm -f -- "${sampler_stale_files[@]}"
fi

append_note "memory-sampler-experiment-start" \
  "server_log=${SERVER_LOG_PATH}" \
  "ingester_log=${INGESTER_LOG_PATH}" \
  "sampler_path=${SAMPLER_PATH}" \
  "stream=${STREAM_NAME}" \
  "base_url=${BASE_URL}" \
  "db_path=${DB_PATH}" \
  "sample_interval_seconds=${SAMPLE_INTERVAL_SECONDS}" \
  "ingest_samples=${INGEST_SAMPLES}" \
  "recovery_samples=${RECOVERY_SAMPLES}" \
  "sampler_interval_ms=${SAMPLER_INTERVAL_MS}" \
  "gharchive_noindex=${GHARCHIVE_NOINDEX}" \
  "gharchive_onlyindex=${GHARCHIVE_ONLYINDEX:-none}" \
  "target_uploaded_segments=${TARGET_UPLOADED_SEGMENTS}" \
  "target_logical_size_bytes=${TARGET_LOGICAL_SIZE_BYTES}" \
  "max_ingest_seconds=${MAX_INGEST_SECONDS}"

(
  export MIMALLOC_SHOW_STATS="${MIMALLOC_SHOW_STATS:-1}"
  export DS_MEMORY_SAMPLER_PATH="${SAMPLER_PATH}"
  export DS_MEMORY_SAMPLER_INTERVAL_MS="${SAMPLER_INTERVAL_MS}"
  "${BUN_BIN}" run src/server.ts --object-store r2 --auto-tune="${AUTO_TUNE_MB}"
) >"${SERVER_LOG_PATH}" 2>&1 &
server_pid=$!

append_note "memory-sampler-server-started" "pid=${server_pid}"

for _ in {1..120}; do
  if curl -fsS --connect-timeout 1 --max-time 5 "${BASE_URL}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS --connect-timeout 1 --max-time 5 "${BASE_URL}/health" >/dev/null 2>&1; then
  append_note "memory-sampler-server-start-failed" "pid=${server_pid}" "base_url=${BASE_URL}"
  exit 1
fi

"${COLLECT_SCRIPT}" "${REPORT_PATH}" "server-before-ingest" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}" "${DB_PATH}"

ingest_args=(all --url "${BASE_URL}" --stream-prefix "${STREAM_PREFIX}" --debug-progress --debug-progress-interval-ms "${DEBUG_PROGRESS_INTERVAL_MS}")
if [[ "${GHARCHIVE_NOINDEX}" == "1" ]]; then
  ingest_args+=(--noindex)
fi
if [[ -n "${GHARCHIVE_ONLYINDEX}" ]]; then
  ingest_args+=(--onlyindex "${GHARCHIVE_ONLYINDEX}")
fi

("${BUN_BIN}" run demo:gharchive "${ingest_args[@]}") >"${INGESTER_LOG_PATH}" 2>&1 &
ingester_pid=$!

append_note "memory-sampler-ingester-started" "pid=${ingester_pid}" "stream=${STREAM_NAME}" "args=${(j: :)ingest_args}"

current_uploaded_segments() {
  sqlite3 -noheader "${DB_PATH}" "select coalesce(uploaded_segment_count, 0) from streams where stream='${STREAM_NAME}';" 2>/dev/null | tr -d '[:space:]'
}

current_logical_size_bytes() {
  sqlite3 -noheader "${DB_PATH}" "select coalesce(logical_size_bytes, 0) from streams where stream='${STREAM_NAME}';" 2>/dev/null | tr -d '[:space:]'
}

samples_taken=0
ingest_elapsed_seconds=0
while (( INGEST_SAMPLES == 0 || samples_taken < INGEST_SAMPLES )); do
  sleep "${SAMPLE_INTERVAL_SECONDS}"
  samples_taken=$((samples_taken + 1))
  ingest_elapsed_seconds=$((samples_taken * SAMPLE_INTERVAL_SECONDS))
  "${COLLECT_SCRIPT}" "${REPORT_PATH}" "server-during-ingest" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}" "${DB_PATH}"

  if ! kill -0 "${ingester_pid}" 2>/dev/null; then
    wait "${ingester_pid}" 2>/dev/null || true
    append_note "memory-sampler-ingester-finished-before-stop" "pid=${ingester_pid}"
    ingester_pid=""
    break
  fi

  uploaded_segments="$(current_uploaded_segments)"
  logical_size_bytes="$(current_logical_size_bytes)"
  if [[ "${TARGET_UPLOADED_SEGMENTS}" =~ ^[0-9]+$ && "${uploaded_segments}" =~ ^[0-9]+$ ]] && (( TARGET_UPLOADED_SEGMENTS > 0 )) && (( uploaded_segments >= TARGET_UPLOADED_SEGMENTS )); then
    append_note "memory-sampler-stop-condition-met" \
      "reason=uploaded_segment_target" \
      "uploaded_segments=${uploaded_segments}" \
      "target_uploaded_segments=${TARGET_UPLOADED_SEGMENTS}" \
      "elapsed_seconds=${ingest_elapsed_seconds}"
    break
  fi
  if [[ "${TARGET_LOGICAL_SIZE_BYTES}" =~ ^[0-9]+$ && "${logical_size_bytes}" =~ ^[0-9]+$ ]] && (( TARGET_LOGICAL_SIZE_BYTES > 0 )) && (( logical_size_bytes >= TARGET_LOGICAL_SIZE_BYTES )); then
    append_note "memory-sampler-stop-condition-met" \
      "reason=logical_size_target" \
      "logical_size_bytes=${logical_size_bytes}" \
      "target_logical_size_bytes=${TARGET_LOGICAL_SIZE_BYTES}" \
      "uploaded_segments=${uploaded_segments:-unknown}" \
      "elapsed_seconds=${ingest_elapsed_seconds}"
    break
  fi
  if [[ "${MAX_INGEST_SECONDS}" =~ ^[0-9]+$ ]] && (( MAX_INGEST_SECONDS > 0 )) && (( ingest_elapsed_seconds >= MAX_INGEST_SECONDS )); then
    append_note "memory-sampler-stop-condition-met" \
      "reason=max_ingest_seconds" \
      "logical_size_bytes=${logical_size_bytes:-unknown}" \
      "uploaded_segments=${uploaded_segments:-unknown}" \
      "elapsed_seconds=${ingest_elapsed_seconds}" \
      "max_ingest_seconds=${MAX_INGEST_SECONDS}"
    break
  fi
done

if [[ -n "${ingester_pid}" ]] && kill -0 "${ingester_pid}" 2>/dev/null; then
  kill -TERM "${ingester_pid}" 2>/dev/null || true
  wait "${ingester_pid}" 2>/dev/null || true
  append_note "memory-sampler-ingester-stopped" "pid=${ingester_pid}" "signal=TERM"
elif [[ -n "${ingester_pid}" ]]; then
  wait "${ingester_pid}" 2>/dev/null || true
  append_note "memory-sampler-ingester-finished-before-stop" "pid=${ingester_pid}"
fi
ingester_pid=""

"${COLLECT_SCRIPT}" "${REPORT_PATH}" "server-after-ingest-stop" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}" "${DB_PATH}"

for ((i = 1; i <= RECOVERY_SAMPLES; i++)); do
  sleep "${SAMPLE_INTERVAL_SECONDS}"
  "${COLLECT_SCRIPT}" "${REPORT_PATH}" "server-recovery-after-ingest-stop" "${server_pid}" "${STREAM_NAME}" "${BASE_URL}" "${DB_PATH}"
done

if kill -0 "${server_pid}" 2>/dev/null; then
  kill -TERM "${server_pid}" 2>/dev/null || true
  wait "${server_pid}" 2>/dev/null || true
  append_note "memory-sampler-server-stopped" "pid=${server_pid}" "signal=TERM"
fi
server_pid=""

sampler_files=("${SAMPLER_PATH%.jsonl}"*.jsonl(N))
if (( ${#sampler_files[@]} > 0 )); then
  summary_json="$("${BUN_BIN}" run "${SUMMARY_SCRIPT}" "${sampler_files[@]}" 2>&1 || true)"
  append_note "memory-sampler-summary" "${summary_json}"
fi
