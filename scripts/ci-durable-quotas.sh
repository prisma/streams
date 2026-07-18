#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19510}"
PORT_A="${PORT_A:-18101}"
PORT_B="${PORT_B:-18102}"
S3_URL="http://127.0.0.1:${S3_PORT}"
URL_A="http://127.0.0.1:${PORT_A}"
URL_B="http://127.0.0.1:${PORT_B}"
AUTH_TOKEN="quota-token"
PREFIX="ci-durable-quotas"
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
      tail -150 "${log}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

start_service() {
  local port="$1"
  local instance="$2"
  local log="$3"
  "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${port}" --instance-name "${instance}" \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix "${PREFIX}" --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
    >"${log}" 2>&1 &
  SERVICE_PID=$!
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

# Legacy auth maps to a real, fixed customer ID. Customer object paths use
# the service's 128-bit SHA-256 prefix rather than leaking that ID.
customer_hash="$(printf '%s' '__legacy__' | openssl dgst -sha256 -binary |
  xxd -p -c 256 | cut -c1-32)"
limits_url="${S3_URL}/streams/${PREFIX}/customers/${customer_hash}/limits.json"
printf '%s' '{"version":1,"max_inflight":8,"write_bytes_per_second":1,"write_burst_bytes":4,"streams_count":2}' \
  >"${TMP_DIR}/limits.json"
curl --fail --silent -X PUT "${limits_url}" \
  --data-binary "@${TMP_DIR}/limits.json" >/dev/null

start_service "${PORT_A}" quota-a "${TMP_DIR}/a.log"
PID_A="${SERVICE_PID}"
start_service "${PORT_B}" quota-b "${TMP_DIR}/b.log"
PID_B="${SERVICE_PID}"
wait_ready "${URL_A}" "${TMP_DIR}/a.log"
wait_ready "${URL_B}" "${TMP_DIR}/b.log"

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")

# Race distinct names through two independent processes. HTTP outcomes can
# be retryable because the deliberately unfenced shard owners fight, but the
# durable descriptor set must never oversubscribe the account limit.
pids=()
for seq in $(seq 0 7); do
  if (( seq % 2 == 0 )); then url="${URL_A}"; else url="${URL_B}"; fi
  curl --silent --output /dev/null --write-out '%{http_code}' \
    -X PUT "${url}/v1/stream/q${seq}" "${auth[@]}" \
    >"${TMP_DIR}/create-${seq}.status" &
  pids+=("$!")
done
for pid in "${pids[@]}"; do wait "${pid}"; done

list="$(curl --fail --silent "${URL_A}/v1/streams" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
live_count="$(printf '%s' "${list}" | grep -o '"name"' | wc -l | tr -d ' ')"
[[ "${live_count}" == "2" ]]

status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X PUT "${URL_A}/v1/stream/overflow" "${auth[@]}")"
[[ "${status}" == "429" ]]

# A durable tombstone drops out of the authoritative recount, so a new name
# can claim the released slot without editing a best-effort memory counter.
first="$(printf '%s' "${list}" | grep -o '"name":"[^"]*"' | head -1 |
  sed 's/"name":"\([^"]*\)"/\1/')"
curl --fail --silent -X DELETE "${URL_A}/v1/stream/${first}" "${auth[@]}" >/dev/null
curl --fail --silent -X PUT "${URL_A}/v1/stream/replacement" "${auth[@]}" >/dev/null
list="$(curl --fail --silent "${URL_A}/v1/streams" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
live_count="$(printf '%s' "${list}" | grep -o '"name"' | wc -l | tr -d ' ')"
[[ "${live_count}" == "2" && "${list}" == *'replacement'* ]]

# The same durable document overrides the process defaults for the request
# byte bucket. Four bytes consume the burst; the immediate next byte is 429
# before it reaches the committer.
curl --fail --silent -X POST "${URL_A}/v1/stream/replacement" "${auth[@]}" \
  -H 'content-type: application/octet-stream' -H 'producer-id: quota-writer' \
  -H 'producer-epoch: 0' -H 'producer-seq: 0' -d 'four' >/dev/null
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X POST "${URL_A}/v1/stream/replacement" "${auth[@]}" \
  -H 'content-type: application/octet-stream' -H 'producer-id: quota-writer' \
  -H 'producer-epoch: 0' -H 'producer-seq: 1' -d 'x')"
[[ "${status}" == "429" ]]

# A malformed durable limit document fails closed after a clean restart;
# requests do not silently fall back to permissive process defaults.
kill "${PID_A}" "${PID_B}"
wait "${PID_A}" 2>/dev/null || true
wait "${PID_B}" 2>/dev/null || true
PID_A=""
PID_B=""
printf '%s' 'not-json' >"${TMP_DIR}/limits.json"
curl --fail --silent -X PUT "${limits_url}" \
  --data-binary "@${TMP_DIR}/limits.json" >/dev/null
start_service "${PORT_A}" quota-c "${TMP_DIR}/c.log"
PID_A="${SERVICE_PID}"
wait_ready "${URL_A}" "${TMP_DIR}/c.log"
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${URL_A}/v1/streams" -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${status}" == "503" ]]

echo "durable per-customer stream-count and write-rate quotas passed"
