#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19519}"
STREAMS_PORT="${STREAMS_PORT:-18109}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="merge-crash-token"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
PREFIX=""
SERVICE_INSTANCE="merge-matrix"

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
  local log="$1"
  local attempts=0
  until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 250 )); then
      tail -180 "${log}" >&2 || true
      exit 1
    fi
    sleep 0.05
  done
}

start_streams() {
  local phase="$1"
  local log="$2"
  if [[ -n "${phase}" ]]; then
    STREAMS_TEST_MERGE_CRASH_AFTER="${phase}" \
      "${TARGET_DIR}/streams-slate" \
      --listen "127.0.0.1:${STREAMS_PORT}" --instance-name "${SERVICE_INSTANCE}" \
      --s3-endpoint "${S3_URL}" --bucket streams --region auto \
      --access-key-id test --secret-access-key test --path-prefix "${PREFIX}" \
      --initial-shards 1 --auth-token "${AUTH_TOKEN}" >"${log}" 2>&1 &
  else
    "${TARGET_DIR}/streams-slate" \
      --listen "127.0.0.1:${STREAMS_PORT}" --instance-name "${SERVICE_INSTANCE}" \
      --s3-endpoint "${S3_URL}" --bucket streams --region auto \
      --access-key-id test --secret-access-key test --path-prefix "${PREFIX}" \
      --initial-shards 1 --auth-token "${AUTH_TOKEN}" >"${log}" 2>&1 &
  fi
  STREAMS_PID=$!
  wait_ready "${log}"
}

stop_streams() {
  if [[ -n "${STREAMS_PID}" ]]; then
    kill "${STREAMS_PID}" 2>/dev/null || true
    wait "${STREAMS_PID}" 2>/dev/null || true
    STREAMS_PID=""
  fi
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

phases=(
  intent_created
  fences_ready
  zero_quiesced
  one_quiesced
  target_ready
  topology_published
  intent_deleted
)
if [[ -n "${MERGE_CRASH_PHASE:-}" ]]; then
  phases=("${MERGE_CRASH_PHASE}")
fi

for phase in "${phases[@]}"; do
  PREFIX="ci-merge-crash-${phase}"
  SERVICE_INSTANCE="merge-matrix"
  crash_log="${TMP_DIR}/${phase}-crash.log"
  recovery_log="${TMP_DIR}/${phase}-recovery.log"
  start_streams "${phase}" "${crash_log}"

  key="$("${TARGET_DIR}/streams-keys" generate)"
  auth=(-H "authorization: Bearer ${AUTH_TOKEN}" -H "stream-encryption-key: ${key}")
  curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/b" \
    "${auth[@]}" -H 'content-type: text/plain' -d 'base|' >/dev/null
  curl --fail --silent -X POST "${STREAMS_URL}/v1/admin/shards/root/split" \
    -H "authorization: Bearer ${AUTH_TOKEN}" >/dev/null

  expected='base|'
  for sequence in $(seq 0 9); do
    payload="$(printf 'M%02d|' "${sequence}")"
    expected+="${payload}"
    curl --fail --silent -X POST "${STREAMS_URL}/v1/stream/b" \
      "${auth[@]}" -H 'content-type: text/plain' \
      -H 'producer-id: merge-matrix-writer' -H 'producer-epoch: 0' \
      -H "producer-seq: ${sequence}" -d "${payload}" >/dev/null
  done

  curl --silent --max-time 20 --output /dev/null \
    -X POST "${STREAMS_URL}/v1/admin/shards/root/merge" \
    -H "authorization: Bearer ${AUTH_TOKEN}" || true
  for _ in $(seq 1 100); do
    if ! kill -0 "${STREAMS_PID}" 2>/dev/null; then
      break
    fi
    sleep 0.02
  done
  if kill -0 "${STREAMS_PID}" 2>/dev/null; then
    echo "merge failpoint did not crash at ${phase}" >&2
    exit 1
  fi
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
  grep -q 'test crash after durable merge transition' "${crash_log}"
  grep -q "phase=\"${phase}\"" "${crash_log}"

  # A different identity after publication must retain, not abandon, the
  # already-visible union generation when it takes over the expired lease.
  if [[ "${phase}" == "topology_published" ]]; then
    SERVICE_INSTANCE="merge-matrix-takeover"
  fi
  start_streams "" "${recovery_log}"
  attempts=0
  while true; do
    topology="$(curl --fail --silent "${S3_URL}/streams/${PREFIX}/topology.json")"
    if [[ "${topology}" == *'"shards":[""]'* ]]; then
      break
    fi
    attempts=$((attempts + 1))
    if (( attempts > 340 )); then
      tail -180 "${recovery_log}" >&2 || true
      exit 1
    fi
    sleep 0.05
  done
  attempts=0
  while true; do
    status="$(curl --silent --output "${TMP_DIR}/${phase}-body" \
      --write-out '%{http_code}' "${STREAMS_URL}/v1/stream/b" "${auth[@]}")"
    if [[ "${status}" == "200" ]]; then
      break
    fi
    [[ "${status}" == "503" ]]
    attempts=$((attempts + 1))
    if (( attempts > 340 )); then
      tail -180 "${recovery_log}" >&2 || true
      exit 1
    fi
    sleep 0.05
  done
  [[ "$(cat "${TMP_DIR}/${phase}-body")" == "${expected}" ]]

  [[ "${topology}" == *'shards/merges/'* ]]
  intent="$(curl --fail --silent \
    "${S3_URL}/streams/${PREFIX}/merge-intents/root.json")"
  [[ "${intent}" == *'"status":"released"'* ]]
  zero_fence="$(curl --fail --silent \
    "${S3_URL}/streams/${PREFIX}/split-intents/0.json")"
  one_fence="$(curl --fail --silent \
    "${S3_URL}/streams/${PREFIX}/split-intents/1.json")"
  [[ "${zero_fence}" == *'"kind":"released"'* ]]
  [[ "${one_fence}" == *'"kind":"released"'* ]]
  candidates="$(curl --fail --silent \
    "${S3_URL}/streams?list-type=2&prefix=${PREFIX}%2Fmerge-gc-candidates%2F")"
  [[ "${candidates}" != *'<Key>'* ]]

  curl --fail --silent -X POST "${STREAMS_URL}/v1/stream/b" \
    "${auth[@]}" -H 'content-type: text/plain' \
    -H 'producer-id: merge-matrix-writer' -H 'producer-epoch: 0' \
    -H 'producer-seq: 10' -d 'after|' >/dev/null
  expected+='after|'
  [[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/b" "${auth[@]}")" == "${expected}" ]]
  stop_streams
done

echo "merge crash matrix passed all ${#phases[@]} durable transitions"
