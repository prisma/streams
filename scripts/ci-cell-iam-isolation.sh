#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
IAM_PORT="${IAM_PORT:-19631}"
TMP_DIR="$(mktemp -d)"
S3_PID=""

cleanup() {
  if [[ -n "${S3_PID}" ]]; then
    kill "${S3_PID}" 2>/dev/null || true
    wait "${S3_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

write_policy() {
  local path="$1"
  local overgrant="$2"
  python3 - "${path}" "${overgrant}" <<'PY'
import json
import sys

path, overgrant = sys.argv[1:]
grants = [
    {"access_key_id": "registry-key", "bucket": "managed", "prefix": "global-registry"},
    {"access_key_id": "cell-a-key", "bucket": "managed", "prefix": "cells/cell-a"},
    {"access_key_id": "cell-b-key", "bucket": "managed", "prefix": "cells/cell-b"},
]
if overgrant == "true":
    grants.append(
        {"access_key_id": "cell-a-key", "bucket": "managed", "prefix": "cells/cell-b"}
    )
with open(path, "w", encoding="utf-8") as output:
    json.dump({"format_version": 1, "grants": grants}, output)
PY
}

wait_provider() {
  local attempts=0
  until curl --silent --max-time 1 \
    "http://127.0.0.1:${IAM_PORT}/_s3lite/stats" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 100 )); then
      echo "IAM provider emulator failed to start" >&2
      return 1
    fi
    sleep 0.1
  done
}

start_provider() {
  local policy="$1"
  local log="$2"
  "${TARGET_DIR}/s3lite" --listen "127.0.0.1:${IAM_PORT}" --latency-ms 1 \
    --iam-policy "${policy}" >"${log}" 2>&1 &
  S3_PID=$!
  wait_provider
}

stop_provider() {
  kill "${S3_PID}"
  wait "${S3_PID}" 2>/dev/null || true
  S3_PID=""
}

check_args=(
  --release-id ci-cell-iam
  --provider-id ci-scoped-iam
  --endpoint "http://127.0.0.1:${IAM_PORT}"
  --region auto --allow-http
  --registry-bucket managed --registry-prefix global-registry
  --registry-access-key-id registry-key --registry-secret-access-key registry-secret
  --cell-a-id cell-a --cell-a-bucket managed
  --cell-a-access-key-id cell-a-key --cell-a-secret-access-key cell-a-secret
  --cell-b-id cell-b --cell-b-bucket managed
  --cell-b-access-key-id cell-b-key --cell-b-secret-access-key cell-b-secret
)

write_policy "${TMP_DIR}/scoped-policy.json" false
start_provider "${TMP_DIR}/scoped-policy.json" "${TMP_DIR}/scoped-provider.log"
"${TARGET_DIR}/streams-cell-iam-check" "${check_args[@]}" \
  >"${TMP_DIR}/evidence.json"
python3 - "${TMP_DIR}/evidence.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    evidence = json.load(source)
assert evidence["format_version"] == 1
assert evidence["status"] == "pass"
assert evidence["release_id"] == "ci-cell-iam"
assert evidence["positive_checks"] == 21
assert evidence["permission_denials"] == 42
assert len(evidence["boundaries"]) == 6
assert all(boundary["permission_denials"] == 7 for boundary in evidence["boundaries"])
assert evidence["probes_cleaned"] is True
assert "access_key" not in json.dumps(evidence).lower()
PY
stop_provider

# Negative control: one accidental cross-cell grant must make the checker fail.
write_policy "${TMP_DIR}/overgrant-policy.json" true
start_provider "${TMP_DIR}/overgrant-policy.json" "${TMP_DIR}/overgrant-provider.log"
if "${TARGET_DIR}/streams-cell-iam-check" "${check_args[@]}" \
  >"${TMP_DIR}/overgrant.json" 2>"${TMP_DIR}/overgrant.err"; then
  echo "IAM checker accepted a deliberate cross-cell overgrant" >&2
  exit 1
fi
grep -q 'cell-a -> cell-b get was not denied' "${TMP_DIR}/overgrant.err"

echo "cell IAM boundary conformance and overgrant negative control passed"
