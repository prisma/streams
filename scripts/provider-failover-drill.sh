#!/usr/bin/env bash
set -euo pipefail

# End-to-end provider failover gate. The caller provisions an empty failover
# namespace on the recovery provider and supplies a hook (or a hermetic-process
# PID) that makes the primary provider unavailable before restore begins.

TARGET_DIR="${TARGET_DIR:-target/release}"
DRILL_PORT="${DRILL_PORT:-18151}"
DRILL_URL="http://127.0.0.1:${DRILL_PORT}"
DRILL_AUTH_TOKEN="${DRILL_AUTH_TOKEN:-provider-failover-drill}"
DRILL_STREAM="${DRILL_STREAM:-provider-failover}"
DRILL_RPO_BUDGET_MS="${DRILL_RPO_BUDGET_MS:-300000}"
DRILL_RTO_BUDGET_MS="${DRILL_RTO_BUDGET_MS:-1800000}"
DRILL_ALLOW_SHARED_TEST_CREDENTIALS="${DRILL_ALLOW_SHARED_TEST_CREDENTIALS:-false}"

required=(
  PRIMARY_PROVIDER_ID PRIMARY_S3_ENDPOINT PRIMARY_S3_BUCKET PRIMARY_S3_REGION
  PRIMARY_S3_ACCESS_KEY_ID PRIMARY_S3_SECRET_ACCESS_KEY PRIMARY_PATH_PREFIX
  RECOVERY_PROVIDER_ID RECOVERY_S3_ENDPOINT RECOVERY_S3_BUCKET RECOVERY_S3_REGION
  RECOVERY_S3_ACCESS_KEY_ID RECOVERY_S3_SECRET_ACCESS_KEY RECOVERY_PATH_PREFIX
  FAILOVER_S3_ENDPOINT FAILOVER_S3_BUCKET FAILOVER_S3_REGION
  FAILOVER_S3_ACCESS_KEY_ID FAILOVER_S3_SECRET_ACCESS_KEY FAILOVER_PATH_PREFIX
  DRILL_EVIDENCE_PATH
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "missing required drill variable: ${name}" >&2
    exit 2
  fi
done
[[ "${DRILL_RPO_BUDGET_MS}" =~ ^[1-9][0-9]*$ ]]
[[ "${DRILL_RTO_BUDGET_MS}" =~ ^[1-9][0-9]*$ ]]
if [[ "${PRIMARY_PROVIDER_ID}" == "${RECOVERY_PROVIDER_ID}" ]]; then
  echo "primary and recovery provider IDs must differ" >&2
  exit 2
fi
primary_endpoint="${PRIMARY_S3_ENDPOINT%/}"
recovery_endpoint="${RECOVERY_S3_ENDPOINT%/}"
failover_endpoint="${FAILOVER_S3_ENDPOINT%/}"
if [[ "${primary_endpoint}" == "${recovery_endpoint}" ]]; then
  echo "primary and recovery endpoints must be different authorities" >&2
  exit 2
fi
if [[ "${failover_endpoint}" != "${recovery_endpoint}" ]]; then
  echo "failover target must be hosted by the recovery provider endpoint" >&2
  exit 2
fi
if [[ "${DRILL_ALLOW_SHARED_TEST_CREDENTIALS}" != "true" ]] \
  && [[ "${PRIMARY_S3_ACCESS_KEY_ID}" == "${RECOVERY_S3_ACCESS_KEY_ID}" ]]; then
  echo "real-provider drill requires independently identified credentials" >&2
  exit 2
fi
if [[ -z "${DRILL_PRIMARY_CUTOVER_HOOK:-}" && -z "${DRILL_PRIMARY_PROVIDER_PID:-}" ]]; then
  echo "set DRILL_PRIMARY_CUTOVER_HOOK or hermetic DRILL_PRIMARY_PROVIDER_PID" >&2
  exit 2
fi
if [[ -n "${DRILL_PRIMARY_CUTOVER_HOOK:-}" && ! -x "${DRILL_PRIMARY_CUTOVER_HOOK}" ]]; then
  echo "primary cutover hook is not executable" >&2
  exit 2
fi
if [[ -n "${DRILL_PRIMARY_RECOVER_HOOK:-}" && ! -x "${DRILL_PRIMARY_RECOVER_HOOK}" ]]; then
  echo "primary recovery hook is not executable" >&2
  exit 2
fi

TMP_DIR="$(mktemp -d)"
SERVICE_PID=""
PRIMARY_CUT="false"

cleanup() {
  if [[ -n "${SERVICE_PID}" ]]; then
    kill "${SERVICE_PID}" 2>/dev/null || true
    wait "${SERVICE_PID}" 2>/dev/null || true
  fi
  if [[ "${PRIMARY_CUT}" == "true" && -n "${DRILL_PRIMARY_RECOVER_HOOK:-}" ]]; then
    "${DRILL_PRIMARY_RECOVER_HOOK}" || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

monotonic_ms() {
  perl -MTime::HiRes=clock_gettime,CLOCK_MONOTONIC \
    -e 'printf "%d\n", clock_gettime(CLOCK_MONOTONIC) * 1000'
}

wait_ready() {
  local log="$1"
  local attempts=0
  until curl --fail --silent --max-time 1 "${DRILL_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 400 )); then
      echo "service did not become ready during provider failover drill" >&2
      tail -150 "${log}" >&2 || true
      return 1
    fi
    if ! kill -0 "${SERVICE_PID}" 2>/dev/null; then
      echo "service exited before readiness" >&2
      tail -150 "${log}" >&2 || true
      return 1
    fi
    sleep 0.1
  done
}

stop_service() {
  if [[ -n "${SERVICE_PID}" ]]; then
    kill "${SERVICE_PID}"
    wait "${SERVICE_PID}" 2>/dev/null || true
    SERVICE_PID=""
  fi
}

start_primary() {
  local log="$1"
  shift
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${DRILL_PORT}" \
    --s3-endpoint "${PRIMARY_S3_ENDPOINT}" --bucket "${PRIMARY_S3_BUCKET}" \
    --region "${PRIMARY_S3_REGION}" \
    --access-key-id "${PRIMARY_S3_ACCESS_KEY_ID}" \
    --secret-access-key "${PRIMARY_S3_SECRET_ACCESS_KEY}" \
    --path-prefix "${PRIMARY_PATH_PREFIX}" --initial-shards 2 \
    --instance-name provider-drill-primary --auth-token "${DRILL_AUTH_TOKEN}" \
    "$@" >"${log}" 2>&1 &
  SERVICE_PID=$!
  wait_ready "${log}"
}

start_failover() {
  local log="$1"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${DRILL_PORT}" \
    --s3-endpoint "${FAILOVER_S3_ENDPOINT}" --bucket "${FAILOVER_S3_BUCKET}" \
    --region "${FAILOVER_S3_REGION}" \
    --access-key-id "${FAILOVER_S3_ACCESS_KEY_ID}" \
    --secret-access-key "${FAILOVER_S3_SECRET_ACCESS_KEY}" \
    --path-prefix "${FAILOVER_PATH_PREFIX}" --initial-shards 2 \
    --instance-name provider-drill-failover --auth-token "${DRILL_AUTH_TOKEN}" \
    >"${log}" 2>&1 &
  SERVICE_PID=$!
  wait_ready "${log}"
}

provider_check() {
  local provider_id="$1"
  local endpoint="$2"
  local bucket="$3"
  local region="$4"
  local access_key="$5"
  local secret_key="$6"
  local allow_http="$7"
  local output="$8"
  local args=(
    --provider-id "${provider_id}" --endpoint "${endpoint}"
    --bucket "${bucket}" --region "${region}"
    --access-key-id "${access_key}" --secret-access-key "${secret_key}"
    --prefix "provider-conformance/${PRIMARY_PATH_PREFIX}"
  )
  if [[ "${allow_http}" == "true" ]]; then
    args+=(--allow-http)
  fi
  "${TARGET_DIR}/streams-provider-check" "${args[@]}" >"${output}"
}

provider_check "${PRIMARY_PROVIDER_ID}" "${PRIMARY_S3_ENDPOINT}" \
  "${PRIMARY_S3_BUCKET}" "${PRIMARY_S3_REGION}" \
  "${PRIMARY_S3_ACCESS_KEY_ID}" "${PRIMARY_S3_SECRET_ACCESS_KEY}" \
  "${PRIMARY_S3_ALLOW_HTTP:-false}" "${TMP_DIR}/primary-provider.json"
provider_check "${RECOVERY_PROVIDER_ID}" "${RECOVERY_S3_ENDPOINT}" \
  "${RECOVERY_S3_BUCKET}" "${RECOVERY_S3_REGION}" \
  "${RECOVERY_S3_ACCESS_KEY_ID}" "${RECOVERY_S3_SECRET_ACCESS_KEY}" \
  "${RECOVERY_S3_ALLOW_HTTP:-false}" "${TMP_DIR}/recovery-provider.json"

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${DRILL_AUTH_TOKEN}")

# Establish a durable baseline before backup is enabled.
start_primary "${TMP_DIR}/primary-seed.log"
curl --fail --silent --show-error -X PUT \
  "${DRILL_URL}/v1/stream/${DRILL_STREAM}" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -d '[{"dr_seq":0}]' >/dev/null
ack0_ms="$(monotonic_ms)"
stop_service

# The first green state proves an independent marker-last recovery point.
backup_args=(
  --backup-s3-endpoint "${RECOVERY_S3_ENDPOINT}"
  --backup-s3-bucket "${RECOVERY_S3_BUCKET}"
  --backup-s3-region "${RECOVERY_S3_REGION}"
  --backup-s3-access-key-id "${RECOVERY_S3_ACCESS_KEY_ID}"
  --backup-s3-secret-access-key "${RECOVERY_S3_SECRET_ACCESS_KEY}"
  --backup-path-prefix "${RECOVERY_PATH_PREFIX}"
  --backup-interval-secs 60 --backup-scrub-interval-secs 10 --require-backup
)
start_primary "${TMP_DIR}/primary-backup-1.log" "${backup_args[@]}"
curl --fail --silent --show-error -X POST \
  "${DRILL_URL}/v1/stream/${DRILL_STREAM}" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -H "producer-id: provider-drill" -H "producer-epoch: 0" -H "producer-seq: 0" \
  -d '[{"dr_seq":1}]' >/dev/null
ack1_ms="$(monotonic_ms)"
stop_service

# Restart forces lease takeover and an immediate post-seq-1 recovery point.
start_primary "${TMP_DIR}/primary-backup-2.log" "${backup_args[@]}"
curl --fail --silent --show-error -X POST \
  "${DRILL_URL}/v1/stream/${DRILL_STREAM}" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -H "producer-id: provider-drill" -H "producer-epoch: 0" -H "producer-seq: 1" \
  -d '[{"dr_seq":2}]' >/dev/null
ack2_ms="$(monotonic_ms)"

# Seq 2 is intentionally acknowledged after the last proven point. The drill
# therefore measures, rather than assumes, the data-loss window.
stop_service
# The disaster clock starts at the actual provider cut, after the serving
# process is stopped. This also makes every acknowledged-write timestamp
# causally precede the RPO boundary.
failure_ms="$(monotonic_ms)"
if [[ -n "${DRILL_PRIMARY_CUTOVER_HOOK:-}" ]]; then
  "${DRILL_PRIMARY_CUTOVER_HOOK}"
else
  kill "${DRILL_PRIMARY_PROVIDER_PID}"
  wait "${DRILL_PRIMARY_PROVIDER_PID}" 2>/dev/null || true
fi
PRIMARY_CUT="true"

"${TARGET_DIR}/streams-restore" \
  --snapshot-id latest \
  --backup-endpoint "${RECOVERY_S3_ENDPOINT}" \
  --backup-bucket "${RECOVERY_S3_BUCKET}" \
  --backup-region "${RECOVERY_S3_REGION}" \
  --backup-access-key-id "${RECOVERY_S3_ACCESS_KEY_ID}" \
  --backup-secret-access-key "${RECOVERY_S3_SECRET_ACCESS_KEY}" \
  --backup-prefix "${RECOVERY_PATH_PREFIX}" \
  --target-endpoint "${FAILOVER_S3_ENDPOINT}" \
  --target-bucket "${FAILOVER_S3_BUCKET}" \
  --target-region "${FAILOVER_S3_REGION}" \
  --target-access-key-id "${FAILOVER_S3_ACCESS_KEY_ID}" \
  --target-secret-access-key "${FAILOVER_S3_SECRET_ACCESS_KEY}" \
  --target-prefix "${FAILOVER_PATH_PREFIX}" \
  --confirm-offline-empty-targets >"${TMP_DIR}/restore.json"

start_failover "${TMP_DIR}/failover.log"
attempts=0
while true; do
  recovered_status="$(curl --silent --show-error --max-time 2 \
    --output "${TMP_DIR}/recovered-body.json" --write-out '%{http_code}' \
    "${DRILL_URL}/v1/stream/${DRILL_STREAM}" "${auth[@]}" \
    -H "stream-encryption-key: ${KEY}" || true)"
  if [[ "${recovered_status}" == "200" ]]; then
    break
  fi
  if [[ "${recovered_status}" != "409" && "${recovered_status}" != "503" \
    && "${recovered_status}" != "000" ]]; then
    echo "unexpected recovery read status: ${recovered_status}" >&2
    tail -100 "${TMP_DIR}/failover.log" >&2 || true
    exit 1
  fi
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    echo "recovered stream did not become readable" >&2
    tail -100 "${TMP_DIR}/failover.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
recovered_body="$(<"${TMP_DIR}/recovered-body.json")"
recovered_ms="$(monotonic_ms)"
recovered_seq="$(python3 -c \
  'import json,sys; rows=json.loads(sys.argv[1]); print(max(row.get("dr_seq", -1) for row in rows))' \
  "${recovered_body}")"
[[ "${recovered_seq}" =~ ^[0-2]$ ]]
if (( recovered_seq < 1 )); then
  echo "failover lost the last recovery-point-proven append (recovered seq ${recovered_seq})" >&2
  sed -n '1,3p' "${TMP_DIR}/restore.json" >&2 || true
  grep -E 'incremental backup snapshot complete|primary SlateDB integrity scrub' \
    "${TMP_DIR}/primary-backup-2.log" >&2 || true
  tail -100 "${TMP_DIR}/failover.log" >&2 || true
  exit 1
fi
case "${recovered_seq}" in
  0) last_recovered_ack_ms="${ack0_ms}" ;;
  1) last_recovered_ack_ms="${ack1_ms}" ;;
  2) last_recovered_ack_ms="${ack2_ms}" ;;
esac
rpo_ms=$((failure_ms - last_recovered_ack_ms))
rto_ms=$((recovered_ms - failure_ms))
if (( rpo_ms < 0 || rpo_ms > DRILL_RPO_BUDGET_MS )); then
  echo "provider failover RPO ${rpo_ms}ms exceeded ${DRILL_RPO_BUDGET_MS}ms" >&2
  exit 1
fi
if (( rto_ms < 0 || rto_ms > DRILL_RTO_BUDGET_MS )); then
  echo "provider failover RTO ${rto_ms}ms exceeded ${DRILL_RTO_BUDGET_MS}ms" >&2
  exit 1
fi

# Prove the activated recovery-provider keyspace is writable and fenced, not a
# read-only export that happened to satisfy one GET.
attempts=0
while true; do
  post_status="$(curl --silent --show-error --max-time 2 -X POST \
    --output "${TMP_DIR}/post-body.json" --write-out '%{http_code}' \
    "${DRILL_URL}/v1/stream/${DRILL_STREAM}" "${auth[@]}" \
    -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
    -H "producer-id: post-failover" -H "producer-epoch: 0" -H "producer-seq: 0" \
    -d '[{"post_failover":true}]' || true)"
  if [[ "${post_status}" =~ ^2[0-9][0-9]$ ]]; then
    break
  fi
  if [[ "${post_status}" != "409" && "${post_status}" != "503" \
    && "${post_status}" != "000" ]]; then
    echo "unexpected post-failover append status: ${post_status}" >&2
    tail -100 "${TMP_DIR}/failover.log" >&2 || true
    exit 1
  fi
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "activated recovery-provider stream did not become writable" >&2
    sed -n '1,5p' "${TMP_DIR}/post-body.json" >&2 || true
    tail -100 "${TMP_DIR}/failover.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
post_body="$(curl --fail --silent --show-error \
  "${DRILL_URL}/v1/stream/${DRILL_STREAM}" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
python3 -c \
  'import json,sys; assert any(row.get("post_failover") is True for row in json.loads(sys.argv[1]))' \
  "${post_body}"

mkdir -p "$(dirname "${DRILL_EVIDENCE_PATH}")"
python3 - "${TMP_DIR}/primary-provider.json" \
  "${TMP_DIR}/recovery-provider.json" "${TMP_DIR}/restore.json" \
  "${DRILL_EVIDENCE_PATH}" "${PRIMARY_PROVIDER_ID}" "${RECOVERY_PROVIDER_ID}" \
  "${failure_ms}" "${recovered_ms}" "${rpo_ms}" "${rto_ms}" \
  "${DRILL_RPO_BUDGET_MS}" "${DRILL_RTO_BUDGET_MS}" "${recovered_seq}" <<'PY'
import json
import pathlib
import sys

primary_path, recovery_path, restore_path, output_path = sys.argv[1:5]
evidence = {
    "format_version": 1,
    "status": "pass",
    "primary_provider_id": sys.argv[5],
    "recovery_provider_id": sys.argv[6],
    "failure_monotonic_ms": int(sys.argv[7]),
    "recovered_monotonic_ms": int(sys.argv[8]),
    "measured_rpo_ms": int(sys.argv[9]),
    "measured_rto_ms": int(sys.argv[10]),
    "rpo_budget_ms": int(sys.argv[11]),
    "rto_budget_ms": int(sys.argv[12]),
    "last_recovered_sequence": int(sys.argv[13]),
    "post_failover_write_verified": True,
    "primary_provider_conformance": json.loads(pathlib.Path(primary_path).read_text()),
    "recovery_provider_conformance": json.loads(pathlib.Path(recovery_path).read_text()),
    "restore": json.loads(pathlib.Path(restore_path).read_text()),
}
encoded = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
pathlib.Path(output_path).write_text(encoded + "\n")
print(encoded)
PY
