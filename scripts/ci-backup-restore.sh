#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19501}"
STREAMS_PORT="${STREAMS_PORT:-18091}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-backup-token"
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
      tail -100 "${TMP_DIR}/streams.log" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

stop_streams() {
  kill "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

start_streams() {
  local bucket="$1"
  local prefix="$2"
  shift 2
  "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" \
    --s3-endpoint "${S3_URL}" --bucket "${bucket}" --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix "${prefix}" --initial-shards 2 --auth-token "${AUTH_TOKEN}" \
    "$@" >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
  wait_ready
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}")

# Seed a primary keyspace without backup, then stop all writers. Restart with
# backup required: readiness cannot turn green until the marker-last snapshot
# that includes the durable stream has completed.
start_streams primary ci-primary
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/recovery" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" \
  -H "content-type: application/json" -d '[{"restore":1},{"restore":2}]' >/dev/null
stop_streams

start_streams primary ci-primary \
  --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket backup \
  --backup-s3-access-key-id test --backup-s3-secret-access-key test \
  --backup-path-prefix ci-backup --backup-interval-secs 60 --require-backup
first_report="$(curl --fail --silent "${S3_URL}/backup/ci-backup/latest.json")"
first_snapshot="$(sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p' <<<"${first_report}")"
[[ -n "${first_snapshot}" ]]

# Advance the primary after the first complete recovery point, then restart the
# backup actor. The second immediate snapshot must reuse immutable blobs while
# capturing the later durable append as a distinct PITR point.
curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/recovery" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}" \
  -H "content-type: application/json" -H "stream-producer: backup-test" \
  -H "stream-seq: 0" -d '[{"after":3}]' >/dev/null
stop_streams

start_streams primary ci-primary \
  --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket backup \
  --backup-s3-access-key-id test --backup-s3-secret-access-key test \
  --backup-path-prefix ci-backup --backup-interval-secs 60 --require-backup
second_report="$(curl --fail --silent "${S3_URL}/backup/ci-backup/latest.json")"
second_snapshot="$(sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p' <<<"${second_report}")"
[[ -n "${second_snapshot}" && "${second_snapshot}" != "${first_snapshot}" ]]
grep -Eq '"reused_objects":[1-9][0-9]*' <<<"${second_report}"
stop_streams

"${TARGET_DIR}/streams-restore" \
  --snapshot-id "${first_snapshot}" \
  --backup-endpoint "${S3_URL}" --backup-bucket backup --backup-region auto \
  --backup-access-key-id test --backup-secret-access-key test \
  --backup-prefix ci-backup \
  --target-endpoint "${S3_URL}" --target-bucket restored --target-region auto \
  --target-access-key-id test --target-secret-access-key test \
  --target-prefix ci-restored --confirm-offline-empty-targets \
  >"${TMP_DIR}/restore.json"

start_streams restored ci-restored
body="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/recovery" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"restore":1},{"restore":2}]' ]]
stop_streams

"${TARGET_DIR}/streams-restore" \
  --snapshot-id "${second_snapshot}" \
  --backup-endpoint "${S3_URL}" --backup-bucket backup --backup-region auto \
  --backup-access-key-id test --backup-secret-access-key test \
  --backup-prefix ci-backup \
  --target-endpoint "${S3_URL}" --target-bucket restored-latest --target-region auto \
  --target-access-key-id test --target-secret-access-key test \
  --target-prefix ci-restored-latest --confirm-offline-empty-targets \
  >"${TMP_DIR}/restore-latest.json"

start_streams restored-latest ci-restored-latest
body="$(curl --fail --silent --show-error "${STREAMS_URL}/v1/stream/recovery" \
  "${auth[@]}" -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"restore":1},{"restore":2},{"after":3}]' ]]
stop_streams

# Corrupt one referenced content blob without changing its size. Incremental
# indexing will still reuse it, but the independent rolling scrub must keep
# readiness red; a later snapshot success must not erase scrub failure state.
blob_key="$(curl --fail --silent \
  "${S3_URL}/backup?list-type=2&prefix=ci-backup%2Fblobs%2Fsha256%2F" |
  grep -o '<Key>[^<]*' | sed -n '1s/<Key>//p')"
[[ -n "${blob_key}" ]]
blob_size="$(curl --fail --silent --head "${S3_URL}/backup/${blob_key}" |
  awk 'tolower($1) == "content-length:" {gsub("\r", "", $2); print $2}')"
[[ "${blob_size}" =~ ^[1-9][0-9]*$ ]]
head -c "${blob_size}" /dev/zero | tr '\0' X |
  curl --fail --silent -X PUT --data-binary @- "${S3_URL}/backup/${blob_key}" >/dev/null

"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket primary --region auto \
  --access-key-id test --secret-access-key test \
  --path-prefix ci-primary --initial-shards 2 --auth-token "${AUTH_TOKEN}" \
  --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket backup \
  --backup-s3-access-key-id test --backup-s3-secret-access-key test \
  --backup-path-prefix ci-backup --backup-interval-secs 60 \
  --backup-scrub-interval-secs 10 --require-backup \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
attempts=0
until grep -q 'backup content scrub failed' "${TMP_DIR}/streams.log"; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "backup scrub did not detect same-size corruption" >&2
    tail -100 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
ready_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/health/ready")"
[[ "${ready_status}" == "503" ]]

echo "checkpoint-pinned incremental PITR/restore smoke passed"
