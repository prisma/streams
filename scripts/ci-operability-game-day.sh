#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19563}"
STREAMS_PORT="${STREAMS_PORT:-18163}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
PREFIX="operability-game-day"
AUTH_TOKEN="operator-game-day"
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
  until curl --fail --silent --max-time 1 "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      tail -150 "${TMP_DIR}/streams.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

metrics() {
  curl --fail --silent --show-error "${STREAMS_URL}/v1/debug/metrics" \
    -H "authorization: Bearer ${AUTH_TOKEN}"
}

python3 scripts/ci-alert-rules.py ops/prometheus-alerts.json

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}" \
  --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
wait_ready

unauthorized="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/debug/metrics")"
[[ "${unauthorized}" == "401" ]]

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}")
stream=(-H "stream-encryption-key: ${KEY}" -H 'content-type: application/json')
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/orders" \
  "${auth[@]}" "${stream[@]}" -d '[]' >/dev/null
curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/orders?offset=now&live=long-poll&timeout=5s" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" \
  >"${TMP_DIR}/tail.json" &
TAIL_PID=$!
sleep 0.2
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/orders" \
  "${auth[@]}" "${stream[@]}" -d '{"event":1}' >/dev/null
wait "${TAIL_PID}"
grep -q '"event":1' "${TMP_DIR}/tail.json"

attempts=0
until curl --fail --silent --show-error -D "${TMP_DIR}/metrics.headers" \
    "${STREAMS_URL}/v1/debug/metrics" "${auth[@]}" >"${TMP_DIR}/healthy.metrics" \
    && awk '$1 == "streams_tail_freshness_seconds_count" && $2 > 0 { found=1 } END { exit !found }' \
      "${TMP_DIR}/healthy.metrics" \
    && awk '$1 == "streams_absorber_pending_bytes" && $2 > 0 { found=1 } END { exit !found }' \
      "${TMP_DIR}/healthy.metrics"; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
grep -qi '^content-type: application/openmetrics-text; version=1.0.0' \
  "${TMP_DIR}/metrics.headers"
grep -q 'streams_http_requests_total{operation="append",status_class="2xx"} 1' \
  "${TMP_DIR}/healthy.metrics"
grep -q 'streams_http_request_duration_seconds_count{operation="append"} 1' \
  "${TMP_DIR}/healthy.metrics"
grep -q 'streams_component_ready{component="topology"} 1' \
  "${TMP_DIR}/healthy.metrics"
grep -q 'streams_component_ready{component="absorber"} 1' \
  "${TMP_DIR}/healthy.metrics"
grep -q 'streams_shard_appended_records_total{shard="root"} 1' \
  "${TMP_DIR}/healthy.metrics"
grep -q '^streams_backup_recovery_point_age_seconds +Inf$' \
  "${TMP_DIR}/healthy.metrics"
grep -q '^streams_backup_rpo_budget_seconds 0$' "${TMP_DIR}/healthy.metrics"
grep -q '^streams_fence_events_total{kind="writer"} 0$' \
  "${TMP_DIR}/healthy.metrics"
grep -q '^# EOF$' "${TMP_DIR}/healthy.metrics"
if grep -q 'orders' "${TMP_DIR}/healthy.metrics"; then
  echo "tenant-controlled stream name leaked into bounded scrape labels" >&2
  exit 1
fi
(( $(wc -l <"${TMP_DIR}/healthy.metrics") < 512 ))

# Advance the stored topology through the production online-split path, then
# replay the valid v1 body. The watcher must keep the last-known-good trie,
# fail readiness, and expose the exact component signal consumed by
# StreamsComponentUnready.
TOPOLOGY_URL="${S3_URL}/streams/${PREFIX}/topology.json"
curl --fail --silent "${TOPOLOGY_URL}" >"${TMP_DIR}/topology-v1.json"
curl --fail --silent --show-error -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/split" "${auth[@]}" \
  >"${TMP_DIR}/topology-v2.json"
grep -q '"version":2' "${TMP_DIR}/topology-v2.json"
wait_ready
curl --fail --silent -X PUT --data-binary @"${TMP_DIR}/topology-v1.json" \
  "${TOPOLOGY_URL}" >/dev/null
attempts=0
until metrics | grep -q 'streams_component_ready{component="topology"} 0'; do
  attempts=$((attempts + 1))
  if (( attempts > 80 )); then
    tail -120 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/health/ready")" == "503" ]]
grep -q 'streams_component_ready == 0' ops/prometheus-alerts.json

curl --fail --silent -X PUT --data-binary @"${TMP_DIR}/topology-v2.json" \
  "${TOPOLOGY_URL}" >/dev/null
wait_ready
metrics | grep -q 'streams_component_ready{component="topology"} 1'

echo "bounded telemetry, actionable alert policy, and degraded-topology game day passed"
