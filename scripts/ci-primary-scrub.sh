#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19522}"
STREAMS_PORT="${STREAMS_PORT:-18123}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-primary-scrub"
PREFIX="ci-primary-scrub"
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
      echo "primary scrub service did not become ready" >&2
      tail -150 "${TMP_DIR}/streams.log" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

start_streams() {
  : >"${TMP_DIR}/streams.log"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" \
    --s3-endpoint "${S3_URL}" --bucket primary --region auto \
    --access-key-id test --secret-access-key test \
    --path-prefix "${PREFIX}" --initial-shards 1 \
    --instance-name primary-scrub-1 --auth-token "${AUTH_TOKEN}" \
    "$@" >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!
}

start_protected() {
  start_streams \
    --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket backup \
    --backup-s3-access-key-id test --backup-s3-secret-access-key test \
    --backup-path-prefix "${PREFIX}" --backup-interval-secs 60 \
    --backup-scrub-interval-secs 10 \
    --primary-scrub-interval-secs 10 \
    --primary-scrub-objects-per-interval 100 --require-backup
}

stop_streams() {
  kill "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

start_streams --absorb-bytes 1 --absorb-age-secs 1
wait_ready
KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
curl --fail --silent --show-error -X PUT \
  "${STREAMS_URL}/v1/stream/primary-integrity" \
  "${auth[@]}" -H 'content-type: text/plain' -d 'checksum-protected' >/dev/null

list_url="${S3_URL}/primary?list-type=2&prefix=${PREFIX}%2Fshards%2Froot%2Fcompacted%2F"
attempts=0
sst_key=""
until [[ -n "${sst_key}" ]]; do
  sst_key="$(curl --fail --silent "${list_url}" |
    grep -o '<Key>[^<]*\.sst' | sed -n '1s/<Key>//p' || true)"
  attempts=$((attempts + 1))
  if (( attempts > 150 )); then
    echo "seed did not flush a compacted SST" >&2
    exit 1
  fi
  sleep 0.1
done
attempts=0
until grep -q 'absorbed .* records into streams/' "${TMP_DIR}/streams.log"; do
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    echo "seed did not create encrypted history" >&2
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
history_list_url="${S3_URL}/primary?list-type=2&prefix=${PREFIX}%2Fstreams%2F"
history_key="$(curl --fail --silent "${history_list_url}" |
  grep -o '<Key>[^<]*/compacted/[^<]*\.sst' | sed -n '1s/<Key>//p')"
[[ -n "${history_key}" ]]
# Absorption advances the shard manifest. Select the newest physical SST only
# after that transition so the corruption target is still referenced.
sst_key="$(curl --fail --silent "${list_url}" |
  grep -o '<Key>[^<]*\.sst' | sed 's/<Key>//' | tail -1)"
[[ -n "${sst_key}" ]]
stop_streams

# A complete logical sweep, recovery scrub, and marker-last point are all
# required before readiness can turn green.
start_protected
wait_ready
grep -q 'completed_sweep=true' "${TMP_DIR}/streams.log"
cursor="$(curl --fail --silent \
  "${S3_URL}/primary/${PREFIX}/integrity/primary-scrub.json")"
grep -Eq '"coordinator_epoch":[1-9][0-9]*' <<<"${cursor}"
good_snapshot="$(curl --fail --silent \
  "${S3_URL}/backup/${PREFIX}/latest.json" |
  sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p')"
[[ -n "${good_snapshot}" ]]
stop_streams

# Replace a currently referenced SST with same-length garbage. Metadata-only
# checking would accept it; SlateDB's block decoder and checksum path must not.
curl --fail --silent "${S3_URL}/primary/${sst_key}" >"${TMP_DIR}/good.sst"
sst_size="$(wc -c <"${TMP_DIR}/good.sst" | tr -d ' ')"
[[ "${sst_size}" =~ ^[1-9][0-9]*$ ]]
head -c "${sst_size}" /dev/zero | tr '\0' X |
  curl --fail --silent -X PUT --data-binary @- \
    "${S3_URL}/primary/${sst_key}" >/dev/null

start_protected
attempts=0
until grep -q 'primary SlateDB integrity scrub failed' "${TMP_DIR}/streams.log"; do
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    echo "same-size primary corruption was not detected" >&2
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/health/ready")"
[[ "${status}" == "503" ]]
health="$(curl --fail --silent \
  "${S3_URL}/primary/${PREFIX}/backup/health.json")"
grep -q '"primary_scrub_healthy":false' <<<"${health}"
red_snapshot="$(curl --fail --silent \
  "${S3_URL}/backup/${PREFIX}/latest.json" |
  sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p')"
[[ -n "${red_snapshot}" ]]

# Repairing primary authority is insufficient by itself: primary recovery
# forces a fresh post-repair snapshot before readiness is restored.
curl --fail --silent -X PUT --data-binary @"${TMP_DIR}/good.sst" \
  "${S3_URL}/primary/${sst_key}" >/dev/null
wait_ready
repaired_snapshot="$(curl --fail --silent \
  "${S3_URL}/backup/${PREFIX}/latest.json" |
  sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p')"
[[ -n "${repaired_snapshot}" && "${repaired_snapshot}" != "${red_snapshot}" ]]
[[ "$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/primary-integrity" "${auth[@]}")" == "checksum-protected" ]]

# Customer-key encrypted history cannot be decoded by a background actor
# without retaining tenant keys. Its ciphertext must instead match the
# create-only digest that the absorber wrote after keyed logical validation.
stop_streams
curl --fail --silent "${S3_URL}/primary/${history_key}" >"${TMP_DIR}/good-history.sst"
history_size="$(wc -c <"${TMP_DIR}/good-history.sst" | tr -d ' ')"
[[ "${history_size}" =~ ^[1-9][0-9]*$ ]]
head -c "${history_size}" /dev/zero | tr '\0' H |
  curl --fail --silent -X PUT --data-binary @- \
    "${S3_URL}/primary/${history_key}" >/dev/null

start_protected
attempts=0
until grep -q 'writer-verified baseline' "${TMP_DIR}/streams.log"; do
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    echo "encrypted history corruption was not detected" >&2
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/health/ready")"
[[ "${status}" == "503" ]]
history_red_snapshot="$(curl --fail --silent \
  "${S3_URL}/backup/${PREFIX}/latest.json" |
  sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p')"
curl --fail --silent -X PUT --data-binary @"${TMP_DIR}/good-history.sst" \
  "${S3_URL}/primary/${history_key}" >/dev/null
wait_ready
history_repaired_snapshot="$(curl --fail --silent \
  "${S3_URL}/backup/${PREFIX}/latest.json" |
  sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p')"
[[ -n "${history_repaired_snapshot}" \
  && "${history_repaired_snapshot}" != "${history_red_snapshot}" ]]

echo "primary manifest/SST/WAL scrub and readiness recovery drill passed"
