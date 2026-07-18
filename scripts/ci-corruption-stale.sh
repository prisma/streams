#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19508}"
STREAMS_PORT="${STREAMS_PORT:-18099}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-corruption-token"
PREFIX="ci-corruption-stale"
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

start_streams() {
  : >"${TMP_DIR}/streams.log"
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

inject() {
  curl --fail --silent --show-error -X POST "${S3_URL}/_s3lite/fault" \
    -H 'content-type: application/json' -d "$1" >/dev/null
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

start_streams
KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/integrity" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'base|' >/dev/null
expected='base|'
for seq in $(seq 0 19); do
  body="$(printf 'R%02d|' "${seq}")"
  expected+="${body}"
  curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/integrity" \
    "${auth[@]}" -H 'content-type: text/plain' -d "${body}" >/dev/null
done

# The five-second recovery flush must produce a real immutable SST; testing
# corruption against a WAL-only dataset would not exercise block checksums.
attempts=0
until curl --fail --silent \
  "${S3_URL}/streams?list-type=2&prefix=${PREFIX}%2Fshards%2Froot%2Fcompacted%2F" |
  grep -q '\.sst'; do
  attempts=$((attempts + 1))
  if (( attempts > 120 )); then
    echo "compacted SST did not appear" >&2
    exit 1
  fi
  sleep 0.1
done

# Publish topology v2, which leaves v1 in s3lite's bounded previous-version
# slot. A stale GET must be rejected as a regression; the in-memory topology
# and data routing stay on v2.
curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}" >/dev/null
inject "{\"operation\":\"get\",\"key_contains\":\"${PREFIX}/topology.json\",\"remaining\":1,\"status\":200,\"stale_body\":true}"
attempts=0
until grep -q 'topology version regressed' "${TMP_DIR}/streams.log"; do
  attempts=$((attempts + 1))
  if (( attempts > 80 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
[[ "$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/integrity" "${auth[@]}")" == "${expected}" ]]

# Reopen from object storage with every compacted-object GET corrupted in
# transit (same ETag and length, one flipped bit). No plaintext data may be
# returned. Clear the response fault and prove the authoritative object and
# service recover without mutation.
stop_streams
start_streams
inject '{"operation":"get","key_contains":"/compacted/","remaining":100,"status":200,"corrupt_body":true}'
status="$(curl --silent --show-error --output "${TMP_DIR}/corrupt-body" \
  --write-out '%{http_code}' "${STREAMS_URL}/v1/stream/integrity" "${auth[@]}" || true)"
if [[ "${status}" =~ ^2 ]]; then
  echo "corrupt SST read returned success" >&2
  exit 1
fi
if grep -q 'base|' "${TMP_DIR}/corrupt-body"; then
  echo "corrupt SST read leaked partial plaintext" >&2
  exit 1
fi
curl --fail --silent -X DELETE "${S3_URL}/_s3lite/fault" >/dev/null

attempts=0
while true; do
  status="$(curl --silent --show-error --output "${TMP_DIR}/recovered-body" \
    --write-out '%{http_code}' "${STREAMS_URL}/v1/stream/integrity" "${auth[@]}" || true)"
  if [[ "${status}" == "200" ]]; then
    break
  fi
  [[ "${status}" == "503" || "${status}" == "500" ]]
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
[[ "$(cat "${TMP_DIR}/recovered-body")" == "${expected}" ]]

echo "stale-topology and corrupt-SST fail-closed drill passed"
