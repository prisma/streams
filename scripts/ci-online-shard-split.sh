#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19505}"
STREAMS_PORT="${STREAMS_PORT:-18095}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-token"
PREFIX="ci-online-shard-split"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
SERVICE_INSTANCE="split-owner-a"

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
    if (( attempts > 300 )); then
      tail -150 "${TMP_DIR}/streams.log" >&2 || true
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
    --instance-name "${SERVICE_INSTANCE}" \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

stop_streams() {
  kill -9 "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

append_series() {
  local stream="$1"
  local producer="$2"
  local marker="$3"
  local seq=0
  while (( seq < 30 )); do
    local body
    body="$(printf '%s%02d|' "${marker}" "${seq}")"
    local status
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      -X POST "${STREAMS_URL}/v1/stream/${stream}" "${auth[@]}" \
      -H "content-type: text/plain" -H "producer-id: ${producer}" \
      -H "producer-epoch: 0" -H "producer-seq: ${seq}" -d "${body}")"
    if [[ "${status}" == "200" || "${status}" == "204" ]]; then
      seq=$((seq + 1))
    elif [[ "${status}" == "503" || "${status}" == "429" || "${status}" == "408" ]]; then
      sleep 0.02
    else
      echo "unexpected append status ${status} for ${stream} seq ${seq}" >&2
      return 1
    fi
  done
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 3 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
start_streams

curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H "content-type: text/plain" -d 'left|' >/dev/null
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/a" \
  "${auth[@]}" -H "content-type: text/plain" -d 'right|' >/dev/null

append_series b producer-left L &
left_pid=$!
append_series a producer-right R &
right_pid=$!

split_body="$(curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${split_body}" == *'"shards":["0","1"]'* ]]
[[ "${split_body}" == *'"shard_paths"'* ]]

wait "${left_pid}"
wait "${right_pid}"

expected_left='left|'
expected_right='right|'
for seq in $(seq 0 29); do
  expected_left+="$(printf 'L%02d|' "${seq}")"
  expected_right+="$(printf 'R%02d|' "${seq}")"
done
left="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/b" "${auth[@]}")"
right="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/a" "${auth[@]}")"
[[ "${left}" == "${expected_left}" ]]
[[ "${right}" == "${expected_right}" ]]

stop_streams
start_streams
left="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/b" "${auth[@]}")"
right="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/a" "${auth[@]}")"
[[ "${left}" == "${expected_left}" ]]
[[ "${right}" == "${expected_right}" ]]

# Crash immediately after the next durable intent becomes visible. On restart
# the parent must remain unavailable until the same intent is reclaimed,
# quiesced, cloned, and published; acknowledged data must remain exact.
curl --fail --silent --show-error -X POST "${S3_URL}/_s3lite/fault" \
  -H 'content-type: application/json' \
  -d '{"operation":"put","key_contains":"shards/splits/","remaining":1,"status":500,"delay_ms":500}' \
  >/dev/null
curl --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/0/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}" \
  >"${TMP_DIR}/crash-split.out" 2>&1 &
split_pid=$!
attempts=0
until curl --fail --silent \
  "${S3_URL}/streams?list-type=2&prefix=${PREFIX}%2Fsplit-intents%2F0.json" |
  grep -q 'split-intents/0.json'; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    echo "split intent did not become visible" >&2
    return 1
  fi
  sleep 0.01
done
stop_streams
wait "${split_pid}" 2>/dev/null || true
# Recover under a different process identity. The prior 12 s lease must
# expire, rotate to a fresh clone generation, and finish inside the 15 s
# service crash-RTO budget (one second of shell/test scheduling allowance).
SERVICE_INSTANCE="split-owner-b"
recovery_started="$(date +%s)"
start_streams
attempts=0
while true; do
  status="$(curl --silent --output "${TMP_DIR}/after-crash-body" \
    --write-out '%{http_code}' "${STREAMS_URL}/v1/stream/b" "${auth[@]}")"
  if [[ "${status}" == "200" ]]; then
    break
  fi
  [[ "${status}" == "503" ]]
  attempts=$((attempts + 1))
  if (( attempts > 340 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    return 1
  fi
  sleep 0.05
done
recovery_elapsed=$(( $(date +%s) - recovery_started ))
if (( recovery_elapsed > 16 )); then
  echo "split takeover exceeded recovery budget: ${recovery_elapsed}s" >&2
  return 1
fi
left="$(cat "${TMP_DIR}/after-crash-body")"
right="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/a" "${auth[@]}")"
[[ "${left}" == "${expected_left}" ]]
[[ "${right}" == "${expected_right}" ]]

echo "online shard split with concurrent durable producers passed"
