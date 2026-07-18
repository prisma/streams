#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19532}"
STREAMS_PORT="${STREAMS_PORT:-18132}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-absorber-recovery"
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
      echo "streams server did not become ready" >&2
      tail -100 "${TMP_DIR}/streams-${1}.log" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

start_streams() {
  local generation="$1"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix ci-absorber-recovery --initial-shards 1 \
    --auth-token "${AUTH_TOKEN}" --absorb-bytes 1073741824 \
    --absorb-age-secs 3600 \
    >"${TMP_DIR}/streams-${generation}.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready "${generation}"
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 2 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}")

# The first owner durably acknowledges a record while both live thresholds are
# deliberately unreachable, then disappears before history absorption.
start_streams before
curl --fail --silent --show-error -X PUT \
  "${STREAMS_URL}/v1/stream/idle-debt" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -d '[{"idle":true}]' >/dev/null
if grep -q 'absorbed .* records into streams/' "${TMP_DIR}/streams-before.log"; then
  echo "pre-crash owner unexpectedly absorbed below its thresholds" >&2
  exit 1
fi
kill -9 "${STREAMS_PID}"
wait "${STREAMS_PID}" 2>/dev/null || true
STREAMS_PID=""

# A replacement has an empty key cache. It must nevertheless recover and
# expose the durable debt before any new stream request.
start_streams after
metrics="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/debug/metrics" "${auth[@]}")"
pending="$(awk '$1 == "streams_absorber_pending_bytes" {print $2}' <<<"${metrics}")"
[[ -n "${pending}" && "${pending}" -gt 0 ]]

# A read supplies the customer key. No append or maintenance signal follows;
# the recovered forced work item must retry and drain on its own.
body="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/idle-debt" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"idle":true}]' ]]

attempts=0
while :; do
  metrics="$(curl --fail --silent --show-error \
    "${STREAMS_URL}/v1/debug/metrics" "${auth[@]}")"
  pending="$(awk '$1 == "streams_absorber_pending_bytes" {print $2}' <<<"${metrics}")"
  if [[ "${pending}" == "0" ]]; then
    break
  fi
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "recovered absorber metric did not drain" >&2
    exit 1
  fi
  sleep 0.1
done

# A third owner must find no debt marker and must serve the record through the
# durable history frontier. This distinguishes real completion from a merely
# decremented process-local gauge.
kill -9 "${STREAMS_PID}"
wait "${STREAMS_PID}" 2>/dev/null || true
STREAMS_PID=""
start_streams verified
metrics="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/debug/metrics" "${auth[@]}")"
pending="$(awk '$1 == "streams_absorber_pending_bytes" {print $2}' <<<"${metrics}")"
[[ "${pending}" == "0" ]]
body="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/idle-debt" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"idle":true}]' ]]

echo "durable absorber debt recovery passed without a post-restart append"
