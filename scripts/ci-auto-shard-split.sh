#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19506}"
STREAMS_PORT="${STREAMS_PORT:-18096}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-token"
PREFIX="ci-auto-shard-split"
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

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test \
  --path-prefix "${PREFIX}" --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  --single-shard-write-ceiling-bytes-per-sec 100 \
  --auto-split-sustain-secs 1 \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!

attempts=0
until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    tail -100 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
payload="$(printf '%0256d' 0 | tr '0' 'x')"
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/auto" \
  "${auth[@]}" -H "content-type: text/plain" -d "${payload}" >/dev/null

seq=0
while (( seq < 400 )); do
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    -X POST "${STREAMS_URL}/v1/stream/auto" "${auth[@]}" \
    -H "content-type: text/plain" -H 'producer-id: auto-producer' \
    -H 'producer-epoch: 0' -H "producer-seq: ${seq}" -d "${payload}")"
  if [[ "${status}" == "200" || "${status}" == "204" ]]; then
    seq=$((seq + 1))
  elif [[ "${status}" == "503" || "${status}" == "429" || "${status}" == "408" ]]; then
    sleep 0.01
  else
    echo "unexpected append status ${status} at seq ${seq}" >&2
    exit 1
  fi
  sleep 0.01
done

attempts=0
until curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json" |
  grep -q '"version":[2-9]'; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done

actual_bytes="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/auto" "${auth[@]}" | wc -c | tr -d ' ')"
expected_bytes=$(( 401 * 256 ))
[[ "${actual_bytes}" == "${expected_bytes}" ]]

echo "automatic sustained-load shard split passed"
