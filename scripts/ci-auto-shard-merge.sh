#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19519}"
STREAMS_PORT="${STREAMS_PORT:-18109}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-token"
PREFIX="ci-auto-shard-merge"
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

start_streams() {
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto-merge \
    --access-key-id test --secret-access-key test \
    --path-prefix "${PREFIX}" --initial-shards 2 --auth-token "${AUTH_TOKEN}" \
    --single-shard-write-ceiling-bytes-per-sec 1000 \
    --auto-split-sustain-secs 60 \
    --auto-merge-cold-fraction-pct 10 \
    --auto-merge-sustain-secs 1 \
    >"${TMP_DIR}/streams.log" 2>&1 &
  STREAMS_PID=$!

  local attempts=0
  until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 200 )); then
      tail -150 "${TMP_DIR}/streams.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
start_streams

KEY="$(${TARGET_DIR}/streams-keys generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${KEY}")
payload="$(printf '%0256d' 0 | tr '0' 'm')"
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/auto-merge" \
  "${auth[@]}" -H "content-type: text/plain" -d "${payload}" >/dev/null

# Keep one sibling well above the 100 B/s combined cold threshold across
# multiple sampler windows. A false-cold decision would collapse the root.
seq=0
while (( seq < 80 )); do
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    -X POST "${STREAMS_URL}/v1/stream/auto-merge" "${auth[@]}" \
    -H "content-type: text/plain" -H 'producer-id: auto-merge-producer' \
    -H 'producer-epoch: 0' -H "producer-seq: ${seq}" -d "${payload}")"
  if [[ "${status}" == "200" || "${status}" == "204" ]]; then
    seq=$((seq + 1))
  elif [[ "${status}" == "503" || "${status}" == "429" || "${status}" == "408" ]]; then
    sleep 0.02
  else
    echo "unexpected append status ${status} at seq ${seq}" >&2
    exit 1
  fi
  sleep 0.03
done

topology="$(curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json")"
if ! grep -q '"shards":\["0","1"\]' <<<"${topology}"; then
  echo "hot siblings merged or split unexpectedly: ${topology}" >&2
  tail -150 "${TMP_DIR}/streams.log" >&2 || true
  exit 1
fi

# Once writes stop, two monotonic zero-delta samples plus the sustained cold
# window must drive the same crash-safe merge protocol as the operator API.
attempts=0
until curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json" |
  grep -q '"shards":\[""\]'; do
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    tail -200 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
grep -q 'automatic sustained-cold sibling merge triggered' "${TMP_DIR}/streams.log"

actual_bytes="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/auto-merge" "${auth[@]}" | wc -c | tr -d ' ')"
expected_bytes=$(( 81 * 256 ))
[[ "${actual_bytes}" == "${expected_bytes}" ]]

kill "${STREAMS_PID}"
wait "${STREAMS_PID}" || true
STREAMS_PID=""
start_streams

actual_bytes="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/auto-merge" "${auth[@]}" | wc -c | tr -d ' ')"
[[ "${actual_bytes}" == "${expected_bytes}" ]]

curl --fail --silent --show-error -X POST "${STREAMS_URL}/v1/stream/auto-merge" \
  "${auth[@]}" -H "content-type: text/plain" \
  -H 'producer-id: auto-merge-producer' -H 'producer-epoch: 0' \
  -H "producer-seq: ${seq}" -d "${payload}" >/dev/null
actual_bytes="$(curl --fail --silent --show-error \
  "${STREAMS_URL}/v1/stream/auto-merge" "${auth[@]}" | wc -c | tr -d ' ')"
[[ "${actual_bytes}" == "$(( expected_bytes + 256 ))" ]]

echo "automatic sustained-cold sibling merge passed"
