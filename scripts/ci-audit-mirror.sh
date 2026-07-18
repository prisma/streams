#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
PRIMARY_PORT="${PRIMARY_PORT:-19564}"
MIRROR_PORT="${MIRROR_PORT:-19565}"
STREAMS_PORT="${STREAMS_PORT:-18164}"
PRIMARY_URL="http://127.0.0.1:${PRIMARY_PORT}"
MIRROR_URL="http://127.0.0.1:${MIRROR_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="audit-mirror-ci"
TMP_DIR="$(mktemp -d)"
PRIMARY_PID=""
MIRROR_PID=""
STREAMS_PID=""

cleanup() {
  if [[ -n "${STREAMS_PID}" ]]; then
    kill "${STREAMS_PID}" 2>/dev/null || true
    wait "${STREAMS_PID}" 2>/dev/null || true
  fi
  for pid in "${PRIMARY_PID}" "${MIRROR_PID}"; do
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
  until curl --fail --silent --max-time 1 "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      tail -160 "${TMP_DIR}/streams.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

object_count() {
  local endpoint="$1"
  local bucket="$2"
  local prefix="$3"
  curl --fail --silent \
    "${endpoint}/${bucket}?list-type=2&prefix=${prefix//\//%2F}" |
    grep -o '<Key>' | wc -l | tr -d ' '
}

wait_count() {
  local endpoint="$1"
  local bucket="$2"
  local prefix="$3"
  local expected="$4"
  local attempts=0
  until [[ "$(object_count "${endpoint}" "${bucket}" "${prefix}")" == "${expected}" ]]; do
    attempts=$((attempts + 1))
    if (( attempts > 150 )); then
      echo "audit object count did not reach ${expected}: ${endpoint}/${bucket}/${prefix}" >&2
      exit 1
    fi
    sleep 0.1
  done
}

inject_mirror() {
  curl --fail --silent -X POST "${MIRROR_URL}/_s3lite/fault" \
    -H 'content-type: application/json' -d "$1" >/dev/null
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${PRIMARY_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/primary.log" 2>&1 &
PRIMARY_PID=$!
"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${MIRROR_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/mirror.log" 2>&1 &
MIRROR_PID=$!
"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${PRIMARY_URL}" --bucket primary --region auto \
  --access-key-id primary-test --secret-access-key primary-test \
  --s3-request-timeout-ms 500 \
  --path-prefix audit-primary --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  --audit-mirror-s3-endpoint "${MIRROR_URL}" --audit-mirror-s3-bucket mirror \
  --audit-mirror-s3-region auto --audit-mirror-s3-access-key-id mirror-test \
  --audit-mirror-s3-secret-access-key mirror-test --audit-mirror-s3-allow-http \
  --audit-mirror-path-prefix audit-secondary --require-audit-mirror \
  --audit-sample-denominator 1 >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
wait_ready

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}")
stream=(-H "stream-encryption-key: ${KEY}" -H 'content-type: application/json')
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/audited" \
  "${auth[@]}" "${stream[@]}" -d '[]' >/dev/null
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/audited" \
  "${auth[@]}" "${stream[@]}" -d '{"audit":1}' >/dev/null

wait_count "${PRIMARY_URL}" primary audit-primary/audit/control/ 1
wait_count "${MIRROR_URL}" mirror audit-secondary/audit/control/ 1
wait_count "${PRIMARY_URL}" primary audit-primary/audit/batches/ 1
wait_count "${MIRROR_URL}" mirror audit-secondary/audit/batches/ 1

# A control operation is not reported successful when its independent audit
# write fails. Retrying the idempotent create records a mirrored event and
# restores readiness.
inject_mirror '{"operation":"put","key_contains":"audit/control/","remaining":100,"status":503}'
status="$(curl --silent --show-error --output "${TMP_DIR}/control-failure.json" \
  --write-out '%{http_code}' -X PUT "${STREAMS_URL}/v1/stream/audit-failure" \
  "${auth[@]}" "${stream[@]}" -d '[]')"
[[ "${status}" == "503" ]]
grep -q 'audit_unavailable' "${TMP_DIR}/control-failure.json"
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/health/ready")" == "503" ]]
curl --fail --silent -X DELETE "${MIRROR_URL}/_s3lite/fault" >/dev/null
curl --fail --silent --show-error -X PUT \
  "${STREAMS_URL}/v1/stream/audit-failure" "${auth[@]}" "${stream[@]}" -d '[]' >/dev/null
wait_ready
wait_count "${MIRROR_URL}" mirror audit-secondary/audit/control/ 2

# Sampled batches retain one stable primary object while the mirror retries;
# a mirror outage cannot turn into duplicate primary billing/audit batches.
inject_mirror '{"operation":"put","key_contains":"audit/batches/","remaining":100,"status":503}'
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/audited" \
  "${auth[@]}" "${stream[@]}" -d '{"audit":2}' >/dev/null
attempts=0
until [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "${STREAMS_URL}/health/ready")" == "503" ]]; do
  attempts=$((attempts + 1))
  if (( attempts > 50 )); then
    echo "failed audit batch did not invalidate readiness" >&2
    exit 1
  fi
  sleep 0.1
done
wait_count "${PRIMARY_URL}" primary audit-primary/audit/batches/ 2
[[ "$(object_count "${MIRROR_URL}" mirror audit-secondary/audit/batches/)" == "1" ]]
curl --fail --silent -X DELETE "${MIRROR_URL}/_s3lite/fault" >/dev/null
wait_ready
wait_count "${MIRROR_URL}" mirror audit-secondary/audit/batches/ 2
[[ "$(object_count "${PRIMARY_URL}" primary audit-primary/audit/batches/)" == "2" ]]

curl --fail --silent "${STREAMS_URL}/v1/debug/metrics" "${auth[@]}" |
  grep -q '^streams_audit_mirror_configured 1$'

echo "independent audit dual-write, retry, and readiness drill passed"
