#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19521}"
FIRST_PORT="${FIRST_PORT:-18121}"
SECOND_PORT="${SECOND_PORT:-18122}"
S3_URL="http://127.0.0.1:${S3_PORT}"
AUTH_TOKEN="ci-backup-coordinator"
TMP_DIR="$(mktemp -d)"
S3_PID=""
FIRST_PID=""
SECOND_PID=""

cleanup() {
  for pid in "${FIRST_PID}" "${SECOND_PID}" "${S3_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

wait_ready() {
  local port="$1"
  local log="$2"
  local attempts=0
  until curl --fail --silent "http://127.0.0.1:${port}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 200 )); then
      echo "backup member on ${port} did not become ready" >&2
      tail -100 "${log}" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

start_member() {
  local instance="$1"
  local port="$2"
  local log="$3"
  shift 3
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${port}" \
    --s3-endpoint "${S3_URL}" --bucket primary --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix coord-primary --initial-shards 1 \
    --instance-name "${instance}" --auth-token "${AUTH_TOKEN}" \
    --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket backup \
    --backup-s3-access-key-id test --backup-s3-secret-access-key test \
    --backup-path-prefix coord-backup --backup-interval-secs 60 \
    --backup-scrub-interval-secs 10 --require-backup \
    "$@" \
    >"${log}" 2>&1 &
  MEMBER_PID=$!
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$("${TARGET_DIR}/streams-keys" generate)"
RUST_LOG=info "${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${FIRST_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket primary --region auto \
  --access-key-id test --secret-access-key test \
  --path-prefix coord-primary --initial-shards 1 \
  --instance-name seed --auth-token "${AUTH_TOKEN}" \
  >"${TMP_DIR}/seed.log" 2>&1 &
FIRST_PID=$!
wait_ready "${FIRST_PORT}" "${TMP_DIR}/seed.log"
curl --fail --silent --show-error -X PUT \
  "http://127.0.0.1:${FIRST_PORT}/v1/stream/coordinator" \
  -H "authorization: Bearer ${AUTH_TOKEN}" \
  -H "stream-encryption-key: ${KEY}" \
  -H "content-type: application/json" -d '[{"durable":1}]' >/dev/null
kill "${FIRST_PID}"
wait "${FIRST_PID}" 2>/dev/null || true
FIRST_PID=""

start_member streams-1 "${FIRST_PORT}" "${TMP_DIR}/first.log"
FIRST_PID="${MEMBER_PID}"
start_member streams-2 "${SECOND_PORT}" "${TMP_DIR}/second.log"
SECOND_PID="${MEMBER_PID}"
wait_ready "${FIRST_PORT}" "${TMP_DIR}/first.log"
wait_ready "${SECOND_PORT}" "${TMP_DIR}/second.log"

first_report="$(curl --fail --silent "${S3_URL}/backup/coord-backup/latest.json")"
first_snapshot="$(sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p' <<<"${first_report}")"
first_epoch="$(sed -n 's/.*"coordinator_epoch":\([0-9]*\).*/\1/p' <<<"${first_report}")"
[[ -n "${first_snapshot}" && "${first_epoch}" =~ ^[1-9][0-9]*$ ]]
lease="$(curl --fail --silent \
  "${S3_URL}/primary/coord-primary/backup/coordinator-lease.json")"
grep -q '"format_version":2' <<<"${lease}"
grep -Eq '"renewal_sequence":[1-9][0-9]*' <<<"${lease}"
if grep -q 'lease_until_ms' <<<"${lease}"; then
  echo "clock-dependent coordinator lease was published" >&2
  exit 1
fi
owner="$(sed -n 's/.*"owner":"\([^"]*\)".*/\1/p' <<<"${lease}")"
case "${owner}" in
  streams-1)
    kill "${FIRST_PID}"
    wait "${FIRST_PID}" 2>/dev/null || true
    FIRST_PID=""
    survivor_port="${SECOND_PORT}"
    survivor_log="${TMP_DIR}/second.log"
    ;;
  streams-2)
    kill "${SECOND_PID}"
    wait "${SECOND_PID}" 2>/dev/null || true
    SECOND_PID=""
    survivor_port="${FIRST_PORT}"
    survivor_log="${TMP_DIR}/first.log"
    ;;
  *)
    echo "unexpected backup coordinator owner: ${owner}" >&2
    exit 1
    ;;
esac

attempts=0
second_snapshot="${first_snapshot}"
second_epoch="${first_epoch}"
until [[ "${second_snapshot}" != "${first_snapshot}" ]] \
  && (( second_epoch > first_epoch )); do
  attempts=$((attempts + 1))
  if (( attempts > 150 )); then
    echo "backup coordinator failover did not publish a fenced point" >&2
    tail -100 "${survivor_log}" >&2 || true
    exit 1
  fi
  sleep 0.1
  second_report="$(curl --fail --silent "${S3_URL}/backup/coord-backup/latest.json")"
  second_snapshot="$(sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p' <<<"${second_report}")"
  second_epoch="$(sed -n 's/.*"coordinator_epoch":\([0-9]*\).*/\1/p' <<<"${second_report}")"
done
wait_ready "${survivor_port}" "${survivor_log}"

marker_count="$(curl --fail --silent \
  "${S3_URL}/backup?list-type=2&prefix=coord-backup%2Fsnapshots%2F" |
  grep -o '<Key>[^<]*_complete.json' | wc -l | tr -d ' ')"
[[ "${marker_count}" == "2" ]]

# Exercise the one-version rollback contract after format 3 has published.
# The rollback writer must claim a higher epoch, ignore the format-3 reference
# root for its format-2 content layout, and publish a restorable format-2 point.
if [[ -n "${FIRST_PID}" ]]; then
  kill "${FIRST_PID}"
  wait "${FIRST_PID}" 2>/dev/null || true
  FIRST_PID=""
else
  kill "${SECOND_PID}"
  wait "${SECOND_PID}" 2>/dev/null || true
  SECOND_PID=""
fi
start_member streams-rollback "${FIRST_PORT}" "${TMP_DIR}/rollback.log" \
  --backup-write-format 2
FIRST_PID="${MEMBER_PID}"

attempts=0
rollback_snapshot="${second_snapshot}"
rollback_epoch="${second_epoch}"
until [[ "${rollback_snapshot}" != "${second_snapshot}" ]] \
  && (( rollback_epoch > second_epoch )); do
  attempts=$((attempts + 1))
  if (( attempts > 150 )); then
    echo "format-2 rollback did not publish a higher-epoch point" >&2
    tail -100 "${TMP_DIR}/rollback.log" >&2 || true
    exit 1
  fi
  sleep 0.1
  rollback_report="$(curl --fail --silent \
    "${S3_URL}/backup/coord-backup/latest.json")"
  rollback_snapshot="$(sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p' \
    <<<"${rollback_report}")"
  rollback_epoch="$(sed -n 's/.*"coordinator_epoch":\([0-9]*\).*/\1/p' \
    <<<"${rollback_report}")"
done
grep -q '"format_version":2' <<<"${rollback_report}"
grep -Eq '"reused_objects":[1-9][0-9]*' <<<"${rollback_report}"
wait_ready "${FIRST_PORT}" "${TMP_DIR}/rollback.log"

format3_refs="$(curl --fail --silent \
  "${S3_URL}/backup?list-type=2&prefix=coord-backup%2Fformats%2F3%2Fblob-refs%2F" |
  grep -o '<Key>[^<]*' | wc -l | tr -d ' ')"
format2_refs="$(curl --fail --silent \
  "${S3_URL}/backup?list-type=2&prefix=coord-backup%2Fblob-refs%2F" |
  grep -o '<Key>[^<]*' | wc -l | tr -d ' ')"
[[ "${format3_refs}" =~ ^[1-9][0-9]*$ && "${format2_refs}" =~ ^[1-9][0-9]*$ ]]

marker_count="$(curl --fail --silent \
  "${S3_URL}/backup?list-type=2&prefix=coord-backup%2Fsnapshots%2F" |
  grep -o '<Key>[^<]*_complete.json' | wc -l | tr -d ' ')"
[[ "${marker_count}" == "3" ]]

echo "fenced backup coordinator failover and format rollback smoke passed"
