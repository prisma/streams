#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19529}"
PORT_A="${PORT_A:-18119}"
PORT_B="${PORT_B:-18120}"
S3_URL="http://127.0.0.1:${S3_PORT}"
URL_A="http://127.0.0.1:${PORT_A}"
URL_B="http://127.0.0.1:${PORT_B}"
AUTH_TOKEN="auto-merge-fleet-token"
PREFIX="ci-auto-merge-remote"
FLEET_PREFIX="${PREFIX}-fleet"
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

start_b() {
  RUST_LOG=info "${TARGET_DIR}/streams-slate" --listen "127.0.0.1:${PORT_B}" \
    --instance-name streams-2 "${common[@]}" >"${TMP_DIR}/b.log" 2>&1 &
  PID_B=$!
  wait_ready "${URL_B}" "${TMP_DIR}/b.log"
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

# Hold a two-instance active ring throughout the drill. With the versioned
# rendezvous function, root belongs to streams-1 while 0 and 1 both belong
# to streams-2, so every activity signal consumed by the coordinator is remote.
attempts=0
until curl --fail --silent -X PUT \
  "${S3_URL}/streams/${FLEET_PREFIX}/fleet/desired.json" \
  -H 'content-type: application/json' \
  -d '{"count":2,"reason":"ci fixed ring","epoch":1,"computed_at_ms":1}' >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    tail -100 "${TMP_DIR}/s3lite.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done

common=(--s3-endpoint "${S3_URL}" --bucket streams --region auto-merge-fleet
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}"
  --fleet-prefix "${FLEET_PREFIX}" --initial-shards 2 --auth-token "${AUTH_TOKEN}"
  --single-shard-write-ceiling-bytes-per-sec 1000
  --auto-split-sustain-secs 60 --auto-merge-cold-fraction-pct 10
  --auto-merge-sustain-secs 1 --scale-in-secs 600 --fleet-max 2)

RUST_LOG=info "${TARGET_DIR}/streams-slate" --listen "127.0.0.1:${PORT_A}" \
  --instance-name streams-1 "${common[@]}" >"${TMP_DIR}/a.log" 2>&1 &
PID_A=$!
start_b
wait_ready "${URL_A}" "${TMP_DIR}/a.log"

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
payload="$(printf '%0256d' 0 | tr '0' 'r')"
curl --fail --silent --show-error -X PUT "${URL_B}/v1/stream/remote-merge" \
  "${auth[@]}" -H 'content-type: text/plain' -d "${payload}" >/dev/null

seq=0
while (( seq < 80 )); do
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    -X POST "${URL_B}/v1/stream/remote-merge" "${auth[@]}" \
    -H 'content-type: text/plain' -H 'producer-id: remote-merge-producer' \
    -H 'producer-epoch: 0' -H "producer-seq: ${seq}" -d "${payload}")"
  if [[ "${status}" == "200" || "${status}" == "204" ]]; then
    seq=$((seq + 1))
  elif [[ "${status}" == "503" || "${status}" == "409" || "${status}" == "408" ]]; then
    sleep 0.03
  else
    echo "unexpected remote-owner append status ${status}" >&2
    exit 1
  fi
  sleep 0.03
done

heartbeat="$(curl --fail --silent \
  "${S3_URL}/streams/${FLEET_PREFIX}/fleet/streams-2.json")"
grep -q '"shard_activity"' <<<"${heartbeat}"
grep -q '"shard":"0"' <<<"${heartbeat}"
grep -q '"shard":"1"' <<<"${heartbeat}"

# Stop the only current child owner. Re-reading its last heartbeat must not
# advance the cold clock, even though the configured cold window is one second.
kill "${PID_B}"
wait "${PID_B}" || true
PID_B=""
sleep 5
topology="$(curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json")"
if ! grep -q '"shards":\["0","1"\]' <<<"${topology}"; then
  echo "siblings merged without a fresh current-owner report: ${topology}" >&2
  tail -180 "${TMP_DIR}/a.log" >&2 || true
  exit 1
fi

start_b
attempts=0
until curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json" |
  grep -q '"shards":\[""\]'; do
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    tail -220 "${TMP_DIR}/a.log" >&2 || true
    tail -120 "${TMP_DIR}/b.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
grep -q 'automatic sustained-cold sibling merge triggered' "${TMP_DIR}/a.log"

actual_bytes="$(curl --fail --silent --show-error \
  "${URL_A}/v1/stream/remote-merge" "${auth[@]}" | wc -c | tr -d ' ')"
[[ "${actual_bytes}" == "$(( 81 * 256 ))" ]]

echo "automatic remote-owner cold merge passed"
