#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
PRIMARY_PORT="${PRIMARY_PORT:-19551}"
RECOVERY_PORT="${RECOVERY_PORT:-19552}"
DRILL_PORT="${DRILL_PORT:-18151}"
TMP_DIR="$(mktemp -d)"
PRIMARY_PID=""
RECOVERY_PID=""

cleanup() {
  for pid in "${PRIMARY_PID}" "${RECOVERY_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${PRIMARY_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/primary-provider.log" 2>&1 &
PRIMARY_PID=$!
"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${RECOVERY_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/recovery-provider.log" 2>&1 &
RECOVERY_PID=$!

for url in "http://127.0.0.1:${PRIMARY_PORT}" "http://127.0.0.1:${RECOVERY_PORT}"; do
  attempts=0
  until curl --silent --max-time 1 "${url}/probe?list-type=2" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 100 )); then
      echo "provider emulator failed to start: ${url}" >&2
      exit 1
    fi
    sleep 0.1
  done
done

export TARGET_DIR DRILL_PORT
export PRIMARY_PROVIDER_ID="ci-primary-process"
export PRIMARY_S3_ENDPOINT="http://127.0.0.1:${PRIMARY_PORT}"
export PRIMARY_S3_BUCKET="primary"
export PRIMARY_S3_REGION="auto"
export PRIMARY_S3_ACCESS_KEY_ID="test"
export PRIMARY_S3_SECRET_ACCESS_KEY="test"
export PRIMARY_S3_ALLOW_HTTP="true"
export PRIMARY_PATH_PREFIX="provider-drill-primary"
export RECOVERY_PROVIDER_ID="ci-recovery-process"
export RECOVERY_S3_ENDPOINT="http://127.0.0.1:${RECOVERY_PORT}"
export RECOVERY_S3_BUCKET="recovery"
export RECOVERY_S3_REGION="auto"
export RECOVERY_S3_ACCESS_KEY_ID="test"
export RECOVERY_S3_SECRET_ACCESS_KEY="test"
export RECOVERY_S3_ALLOW_HTTP="true"
export RECOVERY_PATH_PREFIX="provider-drill-backup"
export FAILOVER_S3_ENDPOINT="${RECOVERY_S3_ENDPOINT}"
export FAILOVER_S3_BUCKET="activated"
export FAILOVER_S3_REGION="auto"
export FAILOVER_S3_ACCESS_KEY_ID="test"
export FAILOVER_S3_SECRET_ACCESS_KEY="test"
export FAILOVER_PATH_PREFIX="provider-drill-activated"
export DRILL_PRIMARY_PROVIDER_PID="${PRIMARY_PID}"
export DRILL_ALLOW_SHARED_TEST_CREDENTIALS="true"
export DRILL_RPO_BUDGET_MS="30000"
export DRILL_RTO_BUDGET_MS="30000"
export DRILL_RELEASE_ID="ci-provider-failover"
export DRILL_EVIDENCE_PATH="${TMP_DIR}/provider-failover.json"

"$(dirname "$0")/provider-failover-drill.sh" | tee "${TMP_DIR}/stdout.json"
grep -q '"status":"pass"' "${DRILL_EVIDENCE_PATH}"
grep -q '"release_id":"ci-provider-failover"' "${DRILL_EVIDENCE_PATH}"
grep -q '"post_failover_write_verified":true' "${DRILL_EVIDENCE_PATH}"
echo "independent-process provider failover RPO/RTO drill passed"
