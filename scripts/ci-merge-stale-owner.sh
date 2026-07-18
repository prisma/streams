#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19520}"
PORT_A="${PORT_A:-18110}"
PORT_B="${PORT_B:-18111}"
S3_URL="http://127.0.0.1:${S3_PORT}"
URL_A="http://127.0.0.1:${PORT_A}"
URL_B="http://127.0.0.1:${PORT_B}"
AUTH_TOKEN="merge-stale-token"
PREFIX="ci-merge-stale-owner"
TMP_DIR="$(mktemp -d)"
S3_PID=""
PID_A=""
PID_B=""

cleanup() {
  for pid in "${PID_A}" "${PID_B}" "${S3_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

wait_ready() {
  local url="$1"
  local log="$2"
  local attempts=0
  until curl --fail --silent "${url}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 250 )); then
      tail -180 "${log}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 3 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

common=(--s3-endpoint "${S3_URL}" --bucket streams --region auto
  --ops-bucket ops --shard-bucket shards --data-bucket data
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}"
  --initial-shards 1 --auth-token "${AUTH_TOKEN}")
"${TARGET_DIR}/streams-slate" --listen "127.0.0.1:${PORT_A}" \
  --instance-name merge-stale-a "${common[@]}" >"${TMP_DIR}/a.log" 2>&1 &
PID_A=$!
wait_ready "${URL_A}" "${TMP_DIR}/a.log"

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
curl --fail --silent -X PUT "${URL_A}/v1/stream/b" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'base|' >/dev/null
curl --fail --silent -X PUT "${URL_A}/v1/stream/a" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'other|' >/dev/null
curl --fail --silent -X POST "${URL_A}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}" >/dev/null

"${TARGET_DIR}/streams-slate" --listen "127.0.0.1:${PORT_B}" \
  --instance-name merge-stale-b "${common[@]}" >"${TMP_DIR}/b.log" 2>&1 &
PID_B=$!
wait_ready "${URL_B}" "${TMP_DIR}/b.log"
# Open both child projections on B, fencing A while A still believes it is
# the coordinator. The merge actor must recover them behind durable fences.
curl --fail --silent "${URL_B}/v1/stream/b" "${auth[@]}" >/dev/null
curl --fail --silent "${URL_B}/v1/stream/a" "${auth[@]}" >/dev/null

(
  seq=0
  while (( seq < 100 )); do
    body="$(printf 'W%03d|' "${seq}")"
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      -X POST "${URL_B}/v1/stream/b" "${auth[@]}" \
      -H 'content-type: text/plain' -H 'producer-id: merge-stale-writer' \
      -H 'producer-epoch: 0' -H "producer-seq: ${seq}" -d "${body}")"
    if [[ "${status}" == "200" || "${status}" == "204" ]]; then
      seq=$((seq + 1))
    elif [[ "${status}" == "503" || "${status}" == "408" ]]; then
      printf '%s' "${seq}" >"${TMP_DIR}/next-seq"
      exit 0
    else
      echo "unexpected stale merge-writer status ${status}" >&2
      exit 1
    fi
  done
  printf '%s' "${seq}" >"${TMP_DIR}/next-seq"
) &
writer_pid=$!

curl --fail --silent -X POST "${S3_URL}/_s3lite/fault" \
  -H 'content-type: application/json' \
  -d '{"operation":"put","key_contains":"shards/merges/","remaining":1,"status":500,"delay_ms":500}' \
  >/dev/null
curl --fail --silent -X POST "${URL_A}/v1/admin/shards/root/merge" \
  -H "authorization: Bearer ${AUTH_TOKEN}" >/dev/null
wait "${writer_pid}"

next="$(cat "${TMP_DIR}/next-seq")"
seq="${next}"
while (( seq < 100 )); do
  body="$(printf 'W%03d|' "${seq}")"
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    -X POST "${URL_A}/v1/stream/b" "${auth[@]}" \
    -H 'content-type: text/plain' -H 'producer-id: merge-stale-writer' \
    -H 'producer-epoch: 0' -H "producer-seq: ${seq}" -d "${body}")"
  if [[ "${status}" == "200" || "${status}" == "204" ]]; then
    seq=$((seq + 1))
  elif [[ "${status}" == "503" || "${status}" == "408" ]]; then
    sleep 0.02
  else
    echo "unexpected recovery writer status ${status}" >&2
    exit 1
  fi
done

expected='base|'
for seq in $(seq 0 99); do
  expected+="$(printf 'W%03d|' "${seq}")"
done
actual="$(curl --fail --silent "${URL_A}/v1/stream/b" "${auth[@]}")"
[[ "${actual}" == "${expected}" ]]
[[ "$(curl --fail --silent "${URL_A}/v1/stream/a" "${auth[@]}")" == 'other|' ]]
grep -q 'withholding durable acknowledgements behind reconfiguration fence' "${TMP_DIR}/b.log"

echo "stale-owner merge acknowledgement fence passed"
