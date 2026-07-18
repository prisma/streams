#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19504}"
STREAMS_PORT="${STREAMS_PORT:-18094}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-token"
PREFIX="ci-shard-split"
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
    --path-prefix "${PREFIX}" --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

stop_streams() {
  kill -9 "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
start_streams

# SHA-256("b") begins with bit 0 and SHA-256("a") with bit 1 in the legacy
# single-tenant namespace, so the two streams exercise opposite children.
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H "content-type: text/plain" -d 'left-1' >/dev/null
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/a" \
  "${auth[@]}" -H "content-type: text/plain" -d 'right-1' >/dev/null
stop_streams

"${TARGET_DIR}/streams-shard-admin" \
  --parent root --confirm-serving-quiesced \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}"

start_streams
left="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/b" "${auth[@]}")"
right="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/a" "${auth[@]}")"
[[ "${left}" == 'left-1' ]]
[[ "${right}" == 'right-1' ]]

curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H "content-type: text/plain" -d 'left-2' >/dev/null
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/a" \
  "${auth[@]}" -H "content-type: text/plain" -d 'right-2' >/dev/null
left="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/b" "${auth[@]}")"
right="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/a" "${auth[@]}")"
[[ "${left}" == 'left-1left-2' ]]
[[ "${right}" == 'right-1right-2' ]]

echo "offline projected shard split passed"
