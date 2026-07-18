#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19500}"
STREAMS_PORT="${STREAMS_PORT:-18090}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-token"
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

wait_ready() {
  local attempts=0
  until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 100 )); then
      echo "streams server did not become ready" >&2
      tail -100 "${TMP_DIR}/streams.log" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

start_streams() {
  "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix ci-quality --initial-shards 4 --auth-token "${AUTH_TOKEN}" \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

auth=(-H "authorization: Bearer ${AUTH_TOKEN}")

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 2 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$(${TARGET_DIR}/streams-keys generate)"
start_streams

assert_request_id() {
  grep -Eiq '^x-prisma-request-id: [0-9a-f]{32}\r?$' "$1"
  ! grep -qi '^x-prisma-request-id: caller-controlled' "$1"
}

# Create-with-body must be a durable idempotent transaction across a hard
# process loss. The exact retry cannot duplicate; a different body conflicts.
curl --fail --silent --show-error -D "${TMP_DIR}/create.headers" \
  -X PUT "${STREAMS_URL}/v1/stream/restart" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" \
  -H 'x-prisma-request-id: caller-controlled' \
  -H "content-type: application/json" -d '[{"n":1},{"n":2}]' >/dev/null
assert_request_id "${TMP_DIR}/create.headers"
kill -9 "${STREAMS_PID}"
wait "${STREAMS_PID}" 2>/dev/null || true
STREAMS_PID=""
start_streams
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/restart" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" \
  -H "content-type: application/json" -d '[{"n":1},{"n":2}]' >/dev/null
body="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/restart" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"n":1},{"n":2}]' ]]
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X PUT "${STREAMS_URL}/v1/stream/restart" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -d '[{"n":99}]')"
[[ "${status}" == "409" ]]

# Producer dedupe is durable and runs before body validation on a retry.
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/restart" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" \
  -H "content-type: application/json" -H "producer-id: ci-producer" \
  -H "producer-epoch: 0" -H "producer-seq: 0" -d '{"n":3}' >/dev/null
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/restart" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -H "producer-id: ci-producer" -H "producer-epoch: 0" -H "producer-seq: 0" \
  -d 'not-json')"
[[ "${status}" == "204" ]]
body="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/restart" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"n":1},{"n":2},{"n":3}]' ]]

# Operator diagnostics and tenant data fail closed without credentials.
status="$(curl --silent --output /dev/null -D "${TMP_DIR}/unauthorized.headers" \
  --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/debug/sleep?ms=1")"
[[ "${status}" == "401" ]]
assert_request_id "${TMP_DIR}/unauthorized.headers"
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/stream/restart" -H "stream-encryption-key: ${KEY}")"
[[ "${status}" == "401" ]]

echo "quality smoke passed"
