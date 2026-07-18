#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19518}"
STREAMS_PORT="${STREAMS_PORT:-18108}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="merge-ci-token"
PREFIX="ci-online-shard-merge"
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
    if (( attempts > 300 )); then
      tail -200 "${TMP_DIR}/streams.log" >&2 || true
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
    --instance-name merge-owner \
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
  while (( seq < 20 )); do
    local status
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      -X POST "${STREAMS_URL}/v1/stream/${stream}" "${auth[@]}" \
      -H 'content-type: text/plain' -H "producer-id: ${producer}" \
      -H 'producer-epoch: 0' -H "producer-seq: ${seq}" \
      -d "$(printf '%s%02d|' "${marker}" "${seq}")")"
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

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 2 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
start_streams

curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'left|' >/dev/null
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/a" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'right|' >/dev/null

split="$(curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${split}" == *'"shards":["0","1"]'* ]]

append_series b merge-left L &
left_pid=$!
append_series a merge-right R &
right_pid=$!
wait "${left_pid}"
wait "${right_pid}"

merged="$(curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/merge" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${merged}" == *'"shards":[""]'* ]]
[[ "${merged}" == *'shards/merges/'* ]]

expected_left='left|'
expected_right='right|'
for seq in $(seq 0 19); do
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

curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H 'content-type: text/plain' -H 'producer-id: merge-left' \
  -H 'producer-epoch: 0' -H 'producer-seq: 20' -d 'L20|' >/dev/null
[[ "$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/b" "${auth[@]}")" == "${expected_left}L20|" ]]

# Reuse the same logical prefixes. Released intent/fence tombstones must be
# replaced by CAS, never deleted by a delayed actor, and must not block a
# later split -> merge generation.
split_again="$(curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${split_again}" == *'"shards":["0","1"]'* ]]
merged_again="$(curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/merge" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${merged_again}" == *'"shards":[""]'* ]]
[[ "$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/b" "${auth[@]}")" == "${expected_left}L20|" ]]
[[ "$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/a" "${auth[@]}")" == "${expected_right}" ]]

echo "online sibling merge with exact union data passed"
