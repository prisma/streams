#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19522}"
STREAMS_PORT="${STREAMS_PORT:-18113}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="split-merge-race-token"
PREFIX="ci-split-merge-race"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
SPLIT_PID=""

cleanup() {
  for pid in "${SPLIT_PID}" "${STREAMS_PID}" "${S3_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 2 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" --instance-name race-owner \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}" \
  --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  --split-gc-retention-secs 0 --split-gc-interval-secs 1 \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
for _ in $(seq 1 300); do
  if curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; then
    break
  fi
  sleep 0.05
done

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/b" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'left|' >/dev/null
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/a" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'right|' >/dev/null
curl --fail --silent -X POST "${STREAMS_URL}/v1/admin/shards/root/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}" >/dev/null

# Hold the first child-0 split lock PUT after its topology validation but
# before commit. Merge can then fence both siblings and publish root. When the
# delayed split retries against the released tombstone it must durably abort,
# not strand a non-live child intent or make readiness fail forever.
curl --fail --silent -X POST "${S3_URL}/_s3lite/fault" \
  -H 'content-type: application/json' \
  -d '{"operation":"put","key_contains":"split-intents/0.json","remaining":1,"status":500,"delay_ms":1500}' \
  >/dev/null
curl --silent --max-time 20 --output "${TMP_DIR}/split.out" --write-out '%{http_code}' \
  -X POST "${STREAMS_URL}/v1/admin/shards/0/split" \
  -H "authorization: Bearer ${AUTH_TOKEN}" >"${TMP_DIR}/split.status" &
SPLIT_PID=$!
sleep 0.1
merged="$(curl --fail --silent -X POST \
  "${STREAMS_URL}/v1/admin/shards/root/merge" \
  -H "authorization: Bearer ${AUTH_TOKEN}")"
[[ "${merged}" == *'"shards":[""]'* ]]
wait "${SPLIT_PID}"
SPLIT_PID=""
[[ "$(cat "${TMP_DIR}/split.status")" == '503' ]]
grep -q 'split aborted because its parent topology changed' "${TMP_DIR}/split.out"

attempts=0
until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    tail -180 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done
child_lock="$(curl --fail --silent \
  "${S3_URL}/streams/${PREFIX}/split-intents/0.json")"
[[ "${child_lock}" == *'"status":"released"'* ]]
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/b" "${auth[@]}")" == 'left|' ]]
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/a" "${auth[@]}")" == 'right|' ]]

echo "split-vs-merge topology race aborted safely"
