#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19562}"
STREAMS_PORT="${STREAMS_PORT:-18162}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="history-envelope-migration"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""

cleanup() {
  if [[ -n "${STREAMS_PID}" ]]; then
    kill "${STREAMS_PID}" 2>/dev/null || true
    wait "${STREAMS_PID}" 2>/dev/null || true
  fi
  if [[ -n "${S3_PID}" ]]; then
    kill "${S3_PID}" 2>/dev/null || true
    wait "${S3_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

start_streams() {
  local format="$1"
  local log="${TMP_DIR}/streams-v${format}.log"
  : >"${log}"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" \
    --s3-endpoint "${S3_URL}" --bucket migration --region auto \
    --access-key-id test --secret-access-key test --path-prefix history-envelope \
    --initial-shards 2 --auth-token "${AUTH_TOKEN}" \
    --absorb-bytes 1 --absorb-age-secs 1 \
    --history-block-write-format "${format}" >"${log}" 2>&1 &
  STREAMS_PID=$!
  local attempts=0
  until curl --fail --silent --max-time 1 "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 200 )); then
      echo "history envelope writer ${format} did not become ready" >&2
      tail -100 "${log}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

stop_streams() {
  kill "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

wait_absorbed() {
  local format="$1"
  local attempts=0
  until grep -q 'absorbed .* records into streams/' "${TMP_DIR}/streams-v${format}.log"; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      echo "history writer ${format} did not absorb" >&2
      tail -100 "${TMP_DIR}/streams-v${format}.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
  sleep 0.5
}

read_body() {
  curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/migrated" \
    -H "authorization: Bearer ${AUTH_TOKEN}" \
    -H "stream-encryption-key: ${KEY}"
}

append() {
  local producer_seq="$1"
  local body="$2"
  curl --fail --silent --show-error -X POST \
    "${STREAMS_URL}/v1/stream/migrated" \
    -H "authorization: Bearer ${AUTH_TOKEN}" \
    -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
    -H "producer-id: envelope-migration" -H "producer-epoch: 0" \
    -H "producer-seq: ${producer_seq}" -d "${body}" >/dev/null
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
KEY="$("${TARGET_DIR}/streams-keys" generate)"

# Read-first wave: dual reader, legacy writer.
start_streams 1
curl --fail --silent --show-error -X PUT \
  "${STREAMS_URL}/v1/stream/migrated" \
  -H "authorization: Bearer ${AUTH_TOKEN}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -d '[{"envelope":1}]' >/dev/null
wait_absorbed 1
[[ "$(read_body)" == '[{"envelope":1}]' ]]
stop_streams

# Canary flip: bound-v2 writer reads legacy, then creates a mixed corpus.
start_streams 2
[[ "$(read_body)" == '[{"envelope":1}]' ]]
append 0 '[{"envelope":2}]'
wait_absorbed 2
[[ "$(read_body)" == '[{"envelope":1},{"envelope":2}]' ]]
stop_streams

# One-release rollback uses the dual reader with writer 1. It must read v2 and
# may emit legacy while the incident is active.
start_streams 1
[[ "$(read_body)" == '[{"envelope":1},{"envelope":2}]' ]]
append 1 '[{"envelope":1,"rollback":true}]'
wait_absorbed 1
[[ "$(read_body)" == '[{"envelope":1},{"envelope":2},{"envelope":1,"rollback":true}]' ]]
stop_streams

# Finalize back on v2 and prove the entire mixed history remains readable.
start_streams 2
[[ "$(read_body)" == '[{"envelope":1},{"envelope":2},{"envelope":1,"rollback":true}]' ]]

echo "history envelope read-first, v2 flip, and dual-reader rollback passed"
