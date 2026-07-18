#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19514}"
STREAMS_PORT="${STREAMS_PORT:-18106}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
PREFIX="ci-tenant-admission"
AUTH_TOKEN="tenant-admission-token"
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
    --listen "127.0.0.1:${STREAMS_PORT}" --instance-name admission-a \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix "${PREFIX}" --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

assert_throttled() {
  local status="$1"
  local dimension="$2"
  local body="$3"
  [[ "${status}" == "429" ]]
  grep -q '"code":"throttled"' "${body}"
  grep -q "\"dimension\":\"${dimension}\"" "${body}"
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

customer_hash="$(printf '%s' '__legacy__' | openssl dgst -sha256 -binary |
  xxd -p -c 256 | cut -c1-32)"
limits_url="${S3_URL}/streams/${PREFIX}/customers/${customer_hash}/limits.json"
printf '%s' '{"version":1,"max_inflight":8,"max_live_connections":1,"write_bytes_per_second":0,"write_burst_bytes":1,"append_requests_per_second":1,"append_request_burst":2,"read_requests_per_second":1,"read_request_burst":2,"read_bytes_per_second":104857600,"read_burst_bytes":104857600,"queue_receives_per_second":1,"queue_receive_burst":2,"streams_count":10}' \
  >"${TMP_DIR}/limits.json"
curl --fail --silent -X PUT "${limits_url}" \
  --data-binary "@${TMP_DIR}/limits.json" >/dev/null

start_service
KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")

curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}" \
  -H 'content-type: application/octet-stream' >/dev/null
head -c 4096 /dev/zero | tr '\0' 'a' >"${TMP_DIR}/payload.bin"
curl --fail --silent -X POST "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}" \
  -H 'content-type: application/octet-stream' \
  -H 'producer-id: admission-writer' -H 'producer-epoch: 0' -H 'producer-seq: 0' \
  --data-binary "@${TMP_DIR}/payload.bin" >/dev/null
curl --fail --silent -X POST "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}" \
  -H 'content-type: application/octet-stream' \
  -H 'producer-id: admission-writer' -H 'producer-epoch: 0' -H 'producer-seq: 1' \
  -d x >/dev/null
status="$(curl --silent --output "${TMP_DIR}/append-throttle.body" --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}" \
  -H 'content-type: application/octet-stream' \
  -H 'producer-id: admission-writer' -H 'producer-epoch: 0' -H 'producer-seq: 2' \
  -d x)"
assert_throttled "${status}" append_burst_requests "${TMP_DIR}/append-throttle.body"

# Read request count is rejected before another storage read is attempted.
curl --fail --silent "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}" >/dev/null
curl --fail --silent "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}" >/dev/null
status="$(curl --silent --output "${TMP_DIR}/read-throttle.body" --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}")"
assert_throttled "${status}" read_burst_requests "${TMP_DIR}/read-throttle.body"

# Queue receives use their own dimension; ack/extend do not consume it.
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/work" "${auth[@]}" \
  -H 'content-type: application/json' -H 'stream-profile: queue' >/dev/null
for _ in 1 2; do
  curl --fail --silent -X POST \
    "${STREAMS_URL}/v1/stream/work/queue/worker/receive" "${auth[@]}" >/dev/null
done
status="$(curl --silent --output "${TMP_DIR}/queue-throttle.body" --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/stream/work/queue/worker/receive" "${auth[@]}")"
assert_throttled "${status}" queue_receive_burst_requests "${TMP_DIR}/queue-throttle.body"

# Refill the independent read bucket, then prove an SSE response retains its
# live-connection slot until the client actually disconnects.
sleep 2.2
curl --silent --no-buffer --max-time 20 \
  "${STREAMS_URL}/v1/stream/admitted?offset=now&live=sse" "${auth[@]}" \
  >"${TMP_DIR}/sse.out" &
SSE_PID=$!
for _ in $(seq 1 100); do
  grep -q 'event: control' "${TMP_DIR}/sse.out" 2>/dev/null && break
  sleep 0.05
done
grep -q 'event: control' "${TMP_DIR}/sse.out"
status="$(curl --silent --output "${TMP_DIR}/live-throttle.body" --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/stream/admitted?offset=now&live=sse" "${auth[@]}")"
assert_throttled "${status}" live_connections "${TMP_DIR}/live-throttle.body"
kill "${SSE_PID}" 2>/dev/null || true
wait "${SSE_PID}" 2>/dev/null || true
sleep 0.2
status="$(curl --silent --max-time 1 --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/stream/admitted?offset=now&live=sse" "${auth[@]}" || true)"
[[ "${status}" == "200" ]]

# A clean restart loads a stricter durable egress rate. The already-admitted
# finite response is paced instead of being reset after its 200 headers.
kill "${STREAMS_PID}"
wait "${STREAMS_PID}" 2>/dev/null || true
STREAMS_PID=""
printf '%s' '{"version":1,"max_inflight":8,"max_live_connections":1,"write_bytes_per_second":0,"write_burst_bytes":1,"append_requests_per_second":0,"append_request_burst":1,"read_requests_per_second":0,"read_request_burst":1,"read_bytes_per_second":1024,"read_burst_bytes":1,"queue_receives_per_second":0,"queue_receive_burst":1,"streams_count":10}' \
  >"${TMP_DIR}/limits.json"
curl --fail --silent -X PUT "${limits_url}" \
  --data-binary "@${TMP_DIR}/limits.json" >/dev/null
start_service
result="$(curl --silent --output "${TMP_DIR}/paced.body" \
  --write-out '%{http_code} %{time_total} %{size_download}' \
  "${STREAMS_URL}/v1/stream/admitted" "${auth[@]}")"
read -r status elapsed downloaded <<<"${result}"
[[ "${status}" == "200" ]]
[[ "${downloaded}" == "4097" ]]
awk -v elapsed="${elapsed}" 'BEGIN { exit !(elapsed >= 3.5 && elapsed < 10.0) }'

echo "tenant request, egress, queue, and streaming-lifetime admission passed"
