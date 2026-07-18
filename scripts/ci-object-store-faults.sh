#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19502}"
STREAMS_PORT="${STREAMS_PORT:-18092}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-fault-token"
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
    if (( attempts > 200 )); then
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
    --s3-request-timeout-ms 100 \
    --path-prefix ci-faults --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

stop_streams() {
  kill "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

inject() {
  curl --fail --silent --show-error -X POST "${S3_URL}/_s3lite/fault" \
    -H 'content-type: application/json' -d "$1" >/dev/null
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
start_streams

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}")
stream=(-H "stream-encryption-key: ${KEY}" -H 'content-type: application/json')

curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/faults" \
  "${auth[@]}" "${stream[@]}" -d '[]' >/dev/null

# A pre-commit provider 503 is retryable. A 2xx may be returned only after a
# real remote-durable WAL object exists.
inject '{"operation":"put","key_contains":"/wal/","remaining":1,"status":503}'
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/faults" \
  "${auth[@]}" "${stream[@]}" -H 'producer-id: before-503' \
  -H 'producer-epoch: 0' -H 'producer-seq: 0' -d '{"phase":"before"}' >/dev/null

# Provider throttling is retryable and remains scoped to the affected write.
inject '{"operation":"put","key_contains":"/wal/","remaining":1,"status":429}'
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/faults" \
  "${auth[@]}" "${stream[@]}" -H 'producer-id: rate-429' \
  -H 'producer-epoch: 0' -H 'producer-seq: 0' -d '{"phase":"rate"}' >/dev/null

# Delay beyond the configured request deadline exercises the actual transport
# timeout path rather than merely returning an HTTP timeout code.
inject '{"operation":"put","key_contains":"/wal/","remaining":1,"status":500,"delay_ms":250}'
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/faults" \
  "${auth[@]}" "${stream[@]}" -H 'producer-id: timeout' \
  -H 'producer-epoch: 0' -H 'producer-seq: 0' -d '{"phase":"timeout"}' >/dev/null

# The provider commits the registry CAS but loses its response. The create
# path must read the winner and expose exactly one incarnation.
inject '{"operation":"put","key_contains":"registry/","remaining":1,"status":412,"after_commit":true}'
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/cas-loss" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" >/dev/null
curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/cas-loss" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" >/dev/null

# A lost success response for a WAL PUT must not create a duplicate logical
# append. Durable producer identity makes the client retry unambiguous even if
# the first HTTP request cannot determine its outcome.
inject '{"operation":"put","key_contains":"/wal/","remaining":1,"status":500,"after_commit":true}'
status="$(curl --silent --show-error --max-time 20 --output /dev/null --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/faults" "${auth[@]}" "${stream[@]}" \
  -H 'producer-id: after-500' -H 'producer-epoch: 0' -H 'producer-seq: 0' \
  -d '{"phase":"after"}' || true)"
if [[ "${status}" != "204" ]]; then
  # A writer whose remote outcome is ambiguous fences itself. A production
  # retry is routed to a fresh owner; restart models that ownership transfer.
  stop_streams
  start_streams
  curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/faults" \
    "${auth[@]}" "${stream[@]}" -H 'producer-id: after-500' \
    -H 'producer-epoch: 0' -H 'producer-seq: 0' -d '{"phase":"after"}' >/dev/null
fi

body="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/faults" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"phase":"before"},{"phase":"rate"},{"phase":"timeout"},{"phase":"after"}]' ]]
faults="$(curl --fail --silent "${S3_URL}/_s3lite/stats" | \
  sed -E 's/.*"faults":([0-9]+).*/\1/')"
(( faults == 5 ))

echo "object-store fault smoke passed"
