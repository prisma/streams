#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19569}"
STREAMS_PORT="${STREAMS_PORT:-18169}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="mixed-version-semantic-ci"
EXPECTED_RELEASE_ID="${EXPECTED_RELEASE_ID:?set EXPECTED_RELEASE_ID to the binary build ID}"
FLEET_PREFIX="mixed-version-semantic-fleet"
PATH_PREFIX="mixed-version-semantic"
RUN_ID="ci-mixed-version-semantic"
STREAM_NAME="mixed-version-semantic"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
CURRENT_LOG=""

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
  local phase="$1"
  local history_writer="$2"
  CURRENT_LOG="${TMP_DIR}/${phase}.log"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${STREAMS_PORT}" --instance-name streams-1 \
    --s3-endpoint "${S3_URL}" --bucket migration --region auto \
    --access-key-id test --secret-access-key test --path-prefix "${PATH_PREFIX}" \
    --fleet-prefix "${FLEET_PREFIX}" --fleet-max 1 --initial-shards 2 \
    --auth-token "${AUTH_TOKEN}" --absorb-bytes 1 --absorb-age-secs 1 \
    --history-block-write-format "${history_writer}" \
    >"${CURRENT_LOG}" 2>&1 &
  STREAMS_PID=$!
  local attempts=0
  until curl --fail --silent --max-time 1 "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      echo "semantic canary phase ${phase} did not become ready" >&2
      tail -160 "${CURRENT_LOG}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

stop_streams() {
  kill "${STREAMS_PID}"
  wait "${STREAMS_PID}" 2>/dev/null || true
  STREAMS_PID=""
}

wait_absorbed() {
  local attempts=0
  until grep -q 'absorbed .* records into streams/' "${CURRENT_LOG}"; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      echo "semantic canary marker did not enter encrypted history" >&2
      tail -160 "${CURRENT_LOG}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

run_phase() {
  local phase="$1"
  local history_writer="$2"
  start_streams "${phase}" "${history_writer}"

  local snapshot="${TMP_DIR}/${phase}-capabilities.json"
  local capability_evidence="${TMP_DIR}/${phase}-capability-evidence.json"
  local attempts=0
  until curl --fail --silent --show-error \
      "${STREAMS_URL}/v1/debug/capabilities" \
      -H "authorization: Bearer ${AUTH_TOKEN}" >"${snapshot}" \
    && python3 scripts/judge-mixed-version-canary.py \
      --phase "${phase}" --run-id "${RUN_ID}" --snapshot "${snapshot}" \
      --expected-instance streams-1 --expected-release "${EXPECTED_RELEASE_ID}" \
      --expected-history-writer "${history_writer}" --expected-backup-writer 3 \
      --output "${capability_evidence}" \
      >"${TMP_DIR}/${phase}-judge.out" 2>"${TMP_DIR}/${phase}-judge.err"; do
    rm -f "${capability_evidence}"
    attempts=$((attempts + 1))
    if (( attempts > 100 )); then
      echo "semantic canary capability view did not converge for ${phase}" >&2
      cat "${snapshot}" >&2 || true
      cat "${TMP_DIR}/${phase}-judge.err" >&2 || true
      tail -160 "${CURRENT_LOG}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done

  local prior_state="${TMP_DIR}/${phase}-state-before.json"
  local had_prior_state=false
  if [[ -f "${TMP_DIR}/semantic-state.json" ]]; then
    cp "${TMP_DIR}/semantic-state.json" "${prior_state}"
    had_prior_state=true
  fi
  python3 scripts/mixed-version-semantic-canary.py \
    --phase "${phase}" --run-id "${RUN_ID}" --stream "${STREAM_NAME}" \
    --url "${STREAMS_URL}" --auth-token-file "${TMP_DIR}/token" \
    --stream-key-file "${TMP_DIR}/key" \
    --capability-evidence "${capability_evidence}" \
    --expected-history-writer "${history_writer}" --expected-backup-writer 3 \
    --state "${TMP_DIR}/semantic-state.json" \
    --evidence "${TMP_DIR}/${phase}-semantic-evidence.json" \
    --allow-http-loopback --allow-opaque-token

  # Simulate a crash after immutable evidence creation but before the state
  # replace. Recovery must recognize the already-durable marker and advance
  # the exact prior chain without appending another event.
  if [[ "${had_prior_state}" == true ]]; then
    cp "${prior_state}" "${TMP_DIR}/semantic-state.json"
  else
    rm -f "${TMP_DIR}/semantic-state.json"
  fi
  python3 scripts/mixed-version-semantic-canary.py \
    --phase "${phase}" --run-id "${RUN_ID}" --stream "${STREAM_NAME}" \
    --url "${STREAMS_URL}" --auth-token-file "${TMP_DIR}/token" \
    --stream-key-file "${TMP_DIR}/key" \
    --capability-evidence "${capability_evidence}" \
    --expected-history-writer "${history_writer}" --expected-backup-writer 3 \
    --state "${TMP_DIR}/semantic-state.json" \
    --evidence "${TMP_DIR}/${phase}-semantic-evidence.json" \
    --allow-http-loopback --allow-opaque-token
  # A later exact operator retry verifies the committed state/evidence digest.
  python3 scripts/mixed-version-semantic-canary.py \
    --phase "${phase}" --run-id "${RUN_ID}" --stream "${STREAM_NAME}" \
    --url "${STREAMS_URL}" --auth-token-file "${TMP_DIR}/token" \
    --stream-key-file "${TMP_DIR}/key" \
    --capability-evidence "${capability_evidence}" \
    --expected-history-writer "${history_writer}" --expected-backup-writer 3 \
    --state "${TMP_DIR}/semantic-state.json" \
    --evidence "${TMP_DIR}/${phase}-semantic-evidence.json" \
    --allow-http-loopback --allow-opaque-token
  wait_absorbed
  stop_streams
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

attempts=0
until curl --fail --silent -X PUT \
  "${S3_URL}/migration/${FLEET_PREFIX}/fleet/desired.json" \
  -H 'content-type: application/json' \
  -d '{"count":1,"reason":"semantic canary","epoch":1,"computed_at_ms":1}' \
  >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    tail -100 "${TMP_DIR}/s3lite.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done

"${TARGET_DIR}/streams-keys" generate >"${TMP_DIR}/key"
printf '%s\n' "${AUTH_TOKEN}" >"${TMP_DIR}/token"
chmod 600 "${TMP_DIR}/key" "${TMP_DIR}/token"

run_phase read-first 1
run_phase canary-flip 2
run_phase rollback 1
run_phase finalize 2

grep -q '"complete": true' "${TMP_DIR}/finalize-semantic-evidence.json"
grep -q '"phase": "finalize"' "${TMP_DIR}/semantic-state.json"
echo "restart-safe mixed-version semantic canary and rollback passed"
