#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19530}"
PORT_1="${PORT_1:-18121}"
PORT_2="${PORT_2:-18122}"
PORT_3="${PORT_3:-18123}"
S3_URL="http://127.0.0.1:${S3_PORT}"
AUTH_TOKEN="fleet-aggregation-token"
EXPECTED_RELEASE_ID="${EXPECTED_RELEASE_ID:-0.1.0}"
PREFIX="ci-fleet-aggregation"
FLEET_PREFIX="${PREFIX}-fleet"
TMP_DIR="$(mktemp -d)"
S3_PID=""
PID_1=""
PID_2=""
PID_3=""

cleanup() {
  for pid in "${PID_1}" "${PID_2}" "${PID_3}" "${S3_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

pid_for() {
  case "$1" in
    streams-1) printf '%s' "${PID_1}" ;;
    streams-2) printf '%s' "${PID_2}" ;;
    streams-3) printf '%s' "${PID_3}" ;;
    *) return 1 ;;
  esac
}

clear_pid() {
  case "$1" in
    streams-1) PID_1="" ;;
    streams-2) PID_2="" ;;
    streams-3) PID_3="" ;;
    *) return 1 ;;
  esac
}

url_for() {
  case "$1" in
    streams-1) printf 'http://127.0.0.1:%s' "${PORT_1}" ;;
    streams-2) printf 'http://127.0.0.1:%s' "${PORT_2}" ;;
    streams-3) printf 'http://127.0.0.1:%s' "${PORT_3}" ;;
    *) return 1 ;;
  esac
}

port_for() {
  case "$1" in
    streams-1) printf '%s' "${PORT_1}" ;;
    streams-2) printf '%s' "${PORT_2}" ;;
    streams-3) printf '%s' "${PORT_3}" ;;
    *) return 1 ;;
  esac
}

wait_ready() {
  local instance="$1"
  local url
  url="$(url_for "${instance}")"
  local attempts=0
  until curl --fail --silent "${url}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      tail -180 "${TMP_DIR}/${instance}.log" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

start_instance() {
  local instance="$1"
  local port
  port="$(port_for "${instance}")"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${port}" --instance-name "${instance}" \
    "${common[@]}" >"${TMP_DIR}/${instance}.log" 2>&1 &
  local pid=$!
  case "${instance}" in
    streams-1) PID_1="${pid}" ;;
    streams-2) PID_2="${pid}" ;;
    streams-3) PID_3="${pid}" ;;
  esac
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

attempts=0
until curl --fail --silent -X PUT \
  "${S3_URL}/streams/${FLEET_PREFIX}/fleet/desired.json" \
  -H 'content-type: application/json' \
  -d '{"count":3,"reason":"ci fixed ring","epoch":1,"computed_at_ms":1}' >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    tail -100 "${TMP_DIR}/s3lite.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done

common=(--s3-endpoint "${S3_URL}" --bucket streams --region fleet-aggregation
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}"
  --fleet-prefix "${FLEET_PREFIX}" --initial-shards 4 --auth-token "${AUTH_TOKEN}"
  --fleet-max 3 --scale-in-secs 600)

start_instance streams-1
start_instance streams-2
start_instance streams-3
wait_ready streams-1
wait_ready streams-2
wait_ready streams-3

# Every direct instance view must converge on the same bounded capability
# envelope. The gate also proves that this operator surface is not public.
judge_args=(--phase ci-read-first --run-id ci-fleet-capabilities
  --expected-history-writer 2 --expected-backup-writer 3
  --expected-release "${EXPECTED_RELEASE_ID}"
  --output "${TMP_DIR}/capability-evidence.json")
for instance in streams-1 streams-2 streams-3; do
  url="$(url_for "${instance}")"
  [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "${url}/v1/debug/capabilities")" == "401" ]]
  snapshot_path="${TMP_DIR}/${instance}-capabilities.json"
  curl --fail --silent --show-error "${url}/v1/debug/capabilities" \
    -H "authorization: Bearer ${AUTH_TOKEN}" >"${snapshot_path}"
  judge_args+=(--snapshot "${snapshot_path}" --expected-instance "${instance}")
done
python3 scripts/judge-mixed-version-canary.py "${judge_args[@]}"
grep -q '"passed": true' "${TMP_DIR}/capability-evidence.json"

# Exercise the judge's mixed-release and pairwise-reader logic with copies of
# the real converged views. These are judge fixtures, not deployment evidence.
SYNTHETIC_RELEASE_ID="ci-synthetic-next"
[[ "${SYNTHETIC_RELEASE_ID}" != "${EXPECTED_RELEASE_ID}" ]]
python3 - "${TMP_DIR}" "${SYNTHETIC_RELEASE_ID}" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
next_release = sys.argv[2]
for instance in ("streams-1", "streams-2", "streams-3"):
    source = root / f"{instance}-capabilities.json"
    document = json.loads(source.read_text())
    nodes = [document["local"], *document["fleet"]]
    for node in nodes:
        if node["instance"] == "streams-3":
            node["capabilities"]["release_id"] = next_release
    (root / f"{instance}-mixed-capabilities.json").write_text(json.dumps(document))

    incompatible = json.loads(json.dumps(document))
    for node in [incompatible["local"], *incompatible["fleet"]]:
        if node["instance"] in ("streams-1", "streams-2"):
            node["capabilities"]["history_writer"] = 1
        else:
            node["capabilities"]["history_reader_min"] = 2
            node["capabilities"]["history_reader_max"] = 2
            node["capabilities"]["history_writer"] = 2
    (root / f"{instance}-incompatible-capabilities.json").write_text(
        json.dumps(incompatible)
    )
PY

mixed_args=(--phase ci-mixed --run-id ci-fleet-mixed
  --expected-history-writer 2 --expected-backup-writer 3
  --expected-release "${EXPECTED_RELEASE_ID}"
  --expected-release "${SYNTHETIC_RELEASE_ID}"
  --output "${TMP_DIR}/mixed-capability-evidence.json")
incompatible_args=(--phase ci-incompatible --run-id ci-fleet-incompatible
  --expected-history-writer 2 --expected-backup-writer 3
  --expected-release "${EXPECTED_RELEASE_ID}"
  --expected-release "${SYNTHETIC_RELEASE_ID}"
  --output "${TMP_DIR}/incompatible-capability-evidence.json")
for instance in streams-1 streams-2 streams-3; do
  mixed_args+=(--snapshot "${TMP_DIR}/${instance}-mixed-capabilities.json"
    --expected-instance "${instance}")
  incompatible_args+=(--snapshot "${TMP_DIR}/${instance}-incompatible-capabilities.json"
    --expected-instance "${instance}")
done
python3 scripts/judge-mixed-version-canary.py "${mixed_args[@]}"
if python3 scripts/judge-mixed-version-canary.py "${incompatible_args[@]}" \
  >"${TMP_DIR}/incompatible.out" 2>"${TMP_DIR}/incompatible.err"; then
  echo "capability judge accepted an incompatible mixed fleet" >&2
  exit 1
fi
grep -q 'history reader cannot read every writer' "${TMP_DIR}/incompatible.err"

FLEET_URL="${S3_URL}/streams/${FLEET_PREFIX}/fleet.json"
LEASE_URL="${S3_URL}/streams/${FLEET_PREFIX}/fleet/aggregate-lease.json"
snapshot="$(curl --fail --silent "${FLEET_URL}")"
for instance in streams-1 streams-2 streams-3; do
  grep -q "\"instance\":\"${instance}\"" <<<"${snapshot}"
done
lease="$(curl --fail --silent "${LEASE_URL}")"
old_owner="$(sed -n 's/.*"owner":"\([^"]*\)".*/\1/p' <<<"${lease}")"
old_epoch="$(sed -n 's/.*"epoch":\([0-9][0-9]*\).*/\1/p' <<<"${lease}")"
[[ -n "${old_owner}" && -n "${old_epoch}" ]]

old_pid="$(pid_for "${old_owner}")"
kill "${old_pid}"
wait "${old_pid}" || true
clear_pid "${old_owner}"

# Lease expiry is 6 s and actors poll every 2 s. A different process must
# take over with a strictly larger epoch; stale old-epoch snapshots are then
# rejected by both lease verification and fleet.json's conditional CAS.
attempts=0
new_owner=""
new_epoch=""
until [[ -n "${new_owner}" && "${new_owner}" != "${old_owner}" && \
  -n "${new_epoch}" && "${new_epoch}" -gt "${old_epoch}" ]]; do
  lease="$(curl --fail --silent "${LEASE_URL}")"
  new_owner="$(sed -n 's/.*"owner":"\([^"]*\)".*/\1/p' <<<"${lease}")"
  new_epoch="$(sed -n 's/.*"epoch":\([0-9][0-9]*\).*/\1/p' <<<"${lease}")"
  attempts=$((attempts + 1))
  if (( attempts > 180 )); then
    tail -220 "${TMP_DIR}/streams-1.log" >&2 || true
    tail -220 "${TMP_DIR}/streams-2.log" >&2 || true
    tail -220 "${TMP_DIR}/streams-3.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done

attempts=0
until snapshot="$(curl --fail --silent "${FLEET_URL}")" && \
  grep -q "\"lease_epoch\":${new_epoch}" <<<"${snapshot}" && \
  ! grep -q "\"instance\":\"${old_owner}\"" <<<"${snapshot}"; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    echo "dead heartbeat did not expire from aggregate: ${snapshot:-missing}" >&2
    exit 1
  fi
  sleep 0.1
done

for instance in streams-1 streams-2 streams-3; do
  if [[ "${instance}" != "${old_owner}" ]]; then
    wait_ready "${instance}"
  fi
done

start_instance "${old_owner}"
wait_ready "${old_owner}"
attempts=0
until snapshot="$(curl --fail --silent "${FLEET_URL}")" && \
  grep -q "\"instance\":\"${old_owner}\"" <<<"${snapshot}"; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "restarted instance did not rejoin aggregate: ${snapshot:-missing}" >&2
    exit 1
  fi
  sleep 0.1
done

# Corrupt aggregate state must not be silently overwritten or served as
# healthy. Every instance exposes the control-plane failure in readiness;
# restoring the last verified snapshot lets the current lease holder resume
# at a higher sequence.
good_snapshot="${snapshot}"
curl --fail --silent -X PUT "${FLEET_URL}" \
  -H 'content-type: application/json' -d '{"version":1}' >/dev/null
attempts=0
until [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "$(url_for streams-1)/health/ready")" != "200" \
  && "$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "$(url_for streams-2)/health/ready")" != "200" \
  && "$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "$(url_for streams-3)/health/ready")" != "200" ]]; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "corrupt fleet snapshot did not fail readiness" >&2
    exit 1
  fi
  sleep 0.1
done
curl --fail --silent -X PUT "${FLEET_URL}" \
  -H 'content-type: application/json' -d "${good_snapshot}" >/dev/null
wait_ready streams-1
wait_ready streams-2
wait_ready streams-3

echo "lease-fenced fleet aggregation and failover passed"
