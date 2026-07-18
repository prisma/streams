#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19515}"
STREAMS_PORT="${STREAMS_PORT:-18107}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
PREFIX="ci-stream-fairness"
AUTH_TOKEN="stream-fairness-token"
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
    if (( attempts > 250 )); then
      tail -150 "${TMP_DIR}/streams.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

start_service() {
  "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" --instance-name stream-fair-a \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix "${PREFIX}" --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

assert_stream_throttle() {
  local status="$1"
  local dimension="$2"
  local body="$3"
  [[ "${status}" == "429" ]]
  grep -q '"code":"throttled"' "${body}"
  grep -q '"scope":"stream"' "${body}"
  grep -q "\"dimension\":\"${dimension}\"" "${body}"
}

append() {
  local stream="$1"
  local producer="$2"
  local sequence="$3"
  local payload="$4"
  curl --fail --silent -X POST "${STREAMS_URL}/v1/stream/${stream}" "${auth[@]}" \
    -H 'content-type: application/octet-stream' \
    -H "producer-id: ${producer}" -H 'producer-epoch: 0' \
    -H "producer-seq: ${sequence}" -d "${payload}" >/dev/null
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
for _ in $(seq 1 100); do
  if curl --fail --silent -X PUT "${S3_URL}/streams" >/dev/null; then
    break
  fi
  sleep 0.02
done

start_service
KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
hot_limits=(
  -H 'stream-append-requests-per-second: 1'
  -H 'stream-append-request-burst: 2'
  -H 'stream-write-bytes-per-second: 0'
  -H 'stream-write-burst-bytes: 1'
  -H 'stream-commit-weight: 2'
)

curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/hot" "${auth[@]}" \
  "${hot_limits[@]}" >/dev/null
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/sibling" "${auth[@]}" >/dev/null

append hot hot-writer 0 a
append hot hot-writer 1 b
status="$(curl --silent --output "${TMP_DIR}/request-throttle.body" --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/hot" "${auth[@]}" \
  -H 'content-type: application/octet-stream' \
  -H 'producer-id: hot-writer' -H 'producer-epoch: 0' -H 'producer-seq: 2' -d c)"
assert_stream_throttle "${status}" append_burst_requests "${TMP_DIR}/request-throttle.body"

# A stream sharing the same customer and shard retains independent tokens.
append sibling sibling-writer 0 sibling-ok

# Provisioned config is immutable on idempotent create, including weight.
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/hot" "${auth[@]}" \
  "${hot_limits[@]}" >/dev/null
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X PUT "${STREAMS_URL}/v1/stream/hot" "${auth[@]}" \
  -H 'stream-append-requests-per-second: 1' \
  -H 'stream-append-request-burst: 3' \
  -H 'stream-write-bytes-per-second: 0' \
  -H 'stream-write-burst-bytes: 1' \
  -H 'stream-commit-weight: 2')"
[[ "${status}" == "409" ]]

status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X PUT "${STREAMS_URL}/v1/stream/invalid" "${auth[@]}" \
  -H 'stream-commit-weight: 101')"
[[ "${status}" == "400" ]]

byte_limits=(
  -H 'stream-append-requests-per-second: 0'
  -H 'stream-append-request-burst: 1'
  -H 'stream-write-bytes-per-second: 1'
  -H 'stream-write-burst-bytes: 4'
)
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/bytes" "${auth[@]}" \
  "${byte_limits[@]}" >/dev/null
append bytes byte-writer 0 four
status="$(curl --silent --output "${TMP_DIR}/byte-throttle.body" --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/bytes" "${auth[@]}" \
  -H 'content-type: application/octet-stream' \
  -H 'producer-id: byte-writer' -H 'producer-epoch: 0' -H 'producer-seq: 1' -d x)"
assert_stream_throttle "${status}" write_burst_bytes "${TMP_DIR}/byte-throttle.body"

# Restart resets only the in-memory bucket, not the provisioned descriptor.
kill "${STREAMS_PID}"
wait "${STREAMS_PID}" 2>/dev/null || true
STREAMS_PID=""
start_service
append hot hot-writer 2 c
append hot hot-writer 3 d
status="$(curl --silent --output "${TMP_DIR}/restart-throttle.body" --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/hot" "${auth[@]}" \
  -H 'content-type: application/octet-stream' \
  -H 'producer-id: hot-writer' -H 'producer-epoch: 0' -H 'producer-seq: 4' -d e)"
assert_stream_throttle "${status}" append_burst_requests "${TMP_DIR}/restart-throttle.body"

body="$(curl --fail --silent "${STREAMS_URL}/v1/stream/hot" "${auth[@]}")"
[[ "${body}" == "abcd" ]]

echo "per-stream admission, persistence, and sibling isolation passed"
