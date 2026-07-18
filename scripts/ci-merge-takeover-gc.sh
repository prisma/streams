#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19521}"
STREAMS_PORT="${STREAMS_PORT:-18112}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="merge-takeover-token"
PREFIX="ci-merge-takeover-gc"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
MERGE_PID=""
SERVICE_INSTANCE="merge-takeover-a"

cleanup() {
  for pid in "${MERGE_PID}" "${STREAMS_PID}" "${S3_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

wait_ready() {
  local attempts=0
  until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      tail -180 "${TMP_DIR}/streams.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

start_streams() {
  "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" --instance-name "${SERVICE_INSTANCE}" \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id test --secret-access-key test --path-prefix "${PREFIX}" \
    --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
    --split-gc-retention-secs 0 --split-gc-interval-secs 1 \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 2 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
for _ in $(seq 1 100); do
  if curl --fail --silent -X PUT "${S3_URL}/streams" >/dev/null; then
    break
  fi
  sleep 0.02
done

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
start_streams
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'base|' >/dev/null
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/a" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'other|' >/dev/null
curl --fail --silent -X POST "${STREAMS_URL}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}" >/dev/null
for seq in $(seq 0 9); do
  curl --fail --silent -X POST "${STREAMS_URL}/v1/stream/b" \
    "${auth[@]}" -H 'content-type: text/plain' \
    -H 'producer-id: merge-takeover-writer' -H 'producer-epoch: 0' \
    -H "producer-seq: ${seq}" -d "$(printf 'T%02d|' "${seq}")" >/dev/null
done

# Return the first target PUT only after it has committed and remained in
# flight long enough to kill the claimant. The successor must never reuse
# this path because the old request may still be completing remotely.
curl --fail --silent -X POST "${S3_URL}/_s3lite/fault" \
  -H 'content-type: application/json' \
  -d '{"operation":"put","key_contains":"shards/merges/","remaining":1,"status":500,"delay_ms":500,"after_commit":true}' \
  >/dev/null
curl --silent --max-time 20 --output "${TMP_DIR}/merge.out" \
  -X POST "${STREAMS_URL}/v1/admin/shards/root/merge" \
  -H "authorization: Bearer ${AUTH_TOKEN}" &
MERGE_PID=$!

attempts=0
until intent="$(curl --fail --silent \
  "${S3_URL}/streams/${PREFIX}/merge-intents/root.json" 2>/dev/null)"; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    echo 'merge intent did not become visible' >&2
    exit 1
  fi
  sleep 0.01
done
old_operation="$(printf '%s' "${intent}" | sed -E 's/^\{"version":[^,]+,"status":"[^"]+","operation_id":"([0-9a-f]+)".*/\1/')"
[[ "${old_operation}" =~ ^[0-9a-f]{32}$ ]]

attempts=0
while true; do
  old_objects="$(curl --fail --silent \
    "${S3_URL}/streams?list-type=2&prefix=${PREFIX}%2Fshards%2Fmerges%2F${old_operation}%2F")"
  if [[ "${old_objects}" == *'<Key>'* ]]; then
    break
  fi
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    echo 'abandoned merge generation never received an object' >&2
    exit 1
  fi
  sleep 0.01
done

kill -9 "${STREAMS_PID}"
wait "${STREAMS_PID}" 2>/dev/null || true
STREAMS_PID=""
wait "${MERGE_PID}" 2>/dev/null || true
MERGE_PID=""

SERVICE_INSTANCE="merge-takeover-b"
recovery_started="$(date +%s)"
start_streams
attempts=0
while true; do
  topology="$(curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json")"
  if [[ "${topology}" == *'"shards":[""]'* ]]; then
    break
  fi
  attempts=$((attempts + 1))
  if (( attempts > 360 )); then
    tail -180 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done
recovery_elapsed=$(( $(date +%s) - recovery_started ))
if (( recovery_elapsed > 18 )); then
  echo "merge takeover exceeded recovery budget: ${recovery_elapsed}s" >&2
  exit 1
fi

expected='base|'
for seq in $(seq 0 9); do
  expected+="$(printf 'T%02d|' "${seq}")"
done
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/b" "${auth[@]}")" == "${expected}" ]]
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/a" "${auth[@]}")" == 'other|' ]]

attempts=0
while true; do
  old_objects="$(curl --fail --silent \
    "${S3_URL}/streams?list-type=2&prefix=${PREFIX}%2Fshards%2Fmerges%2F${old_operation}%2F")"
  marker_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "${S3_URL}/streams/${PREFIX}/merge-gc-candidates/${old_operation}.json")"
  if [[ "${old_objects}" != *'<Key>'* && "${marker_status}" == '404' ]]; then
    break
  fi
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    echo 'abandoned merge generation was not garbage-collected' >&2
    exit 1
  fi
  sleep 0.05
done

intent="$(curl --fail --silent \
  "${S3_URL}/streams/${PREFIX}/merge-intents/root.json")"
[[ "${intent}" == *'"status":"released"'* ]]
current_operation="$(printf '%s' "${intent}" | sed -E 's/^\{"version":[^,]+,"status":"[^"]+","operation_id":"([0-9a-f]+)".*/\1/')"
[[ "${current_operation}" =~ ^[0-9a-f]{32}$ ]]
[[ "${current_operation}" != "${old_operation}" ]]
[[ "${intent}" == *"\"abandoned_generations\":[{\"operation_id\":\"${old_operation}\""* ]]

echo "merge takeover rotated and garbage-collected an abandoned generation"
