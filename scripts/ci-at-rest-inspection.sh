#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19561}"
STREAMS_PORT="${STREAMS_PORT:-18161}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="at-rest-inspection"
SENTINEL="PRISMA_STREAMS_FORBIDDEN_PAYLOAD_7f86e9114ad04769"
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
  until curl --fail --silent --max-time 1 "${STREAMS_URL}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 300 )); then
      echo "service did not become ready for at-rest inspection" >&2
      tail -120 "${TMP_DIR}/streams.log" >&2 || true
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

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!

KEY="$("${TARGET_DIR}/streams-keys" generate)"
auth=(-H "authorization: Bearer ${AUTH_TOKEN}")

RUST_LOG=info "${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket primary --region auto \
  --access-key-id test --secret-access-key test --path-prefix at-rest-primary \
  --initial-shards 2 --auth-token "${AUTH_TOKEN}" \
  --absorb-bytes 1 --absorb-age-secs 1 >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
wait_ready

curl --fail --silent --show-error -X PUT \
  "${STREAMS_URL}/v1/stream/inspected" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H "content-type: application/json" \
  -d "[{\"secret\":\"${SENTINEL}\"},{\"secret\":\"${SENTINEL}-history\"}]" \
  >/dev/null
attempts=0
until grep -q 'absorbed .* records into streams/' "${TMP_DIR}/streams.log"; do
  attempts=$((attempts + 1))
  if (( attempts > 300 )); then
    echo "history tier was not materialized for at-rest inspection" >&2
    tail -120 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
sleep 0.5
stop_streams

# Restart over the stable primary and require a complete recovery point that
# includes both the hot shard and encrypted history corpus.
RUST_LOG=info "${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket primary --region auto \
  --access-key-id test --secret-access-key test --path-prefix at-rest-primary \
  --initial-shards 2 --auth-token "${AUTH_TOKEN}" \
  --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket recovery \
  --backup-s3-access-key-id test --backup-s3-secret-access-key test \
  --backup-path-prefix at-rest-recovery --backup-interval-secs 60 \
  --require-backup >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!
wait_ready
stop_streams

# The spec contains the exact payload sentinel, its printable request key, and
# the decoded 32-byte root key. The file is private and deleted on exit; the
# checker reports only labels and aggregate corpus evidence.
umask 077
python3 - "${TMP_DIR}/forbidden.json" "${SENTINEL}" "${KEY}" <<'PY'
import base64
import json
import pathlib
import sys

path, sentinel, printable_key = sys.argv[1:]
padding = "=" * (-len(printable_key) % 4)
raw_key = base64.urlsafe_b64decode(printable_key + padding)
patterns = [
    ("payload_sentinel", sentinel.encode()),
    ("printable_root_key", printable_key.encode()),
    ("raw_root_key", raw_key),
]
spec = {"forbidden": [
    {"label": label, "base64": base64.b64encode(value).decode()}
    for label, value in patterns
]}
pathlib.Path(path).write_text(json.dumps(spec, separators=(",", ":")) + "\n")
PY

check_args=(
  --release-id ci-at-rest
  --endpoint "${S3_URL}" --region auto --access-key-id test
  --secret-access-key test --allow-http
  --forbidden-file "${TMP_DIR}/forbidden.json"
)
"${TARGET_DIR}/streams-at-rest-check" "${check_args[@]}" \
  --provider-id ci-primary --bucket primary --prefix at-rest-primary \
  >"${TMP_DIR}/primary-evidence.json"
"${TARGET_DIR}/streams-at-rest-check" "${check_args[@]}" \
  --provider-id ci-recovery --bucket recovery --prefix at-rest-recovery \
  >"${TMP_DIR}/recovery-evidence.json"
grep -q '"status":"pass"' "${TMP_DIR}/primary-evidence.json"
grep -q '"release_id":"ci-at-rest"' "${TMP_DIR}/primary-evidence.json"
grep -q '"stable_inventory_verified":true' "${TMP_DIR}/primary-evidence.json"
grep -q '"status":"pass"' "${TMP_DIR}/recovery-evidence.json"
grep -q '"stable_inventory_verified":true' "${TMP_DIR}/recovery-evidence.json"

# Prove the scanner itself fails closed on an exact leak.
curl --fail --silent -X PUT --data-binary "${SENTINEL}" \
  "${S3_URL}/leak/corpus/object" >/dev/null
if "${TARGET_DIR}/streams-at-rest-check" "${check_args[@]}" \
  --provider-id ci-deliberate-leak --bucket leak --prefix corpus \
  >"${TMP_DIR}/leak.out" 2>"${TMP_DIR}/leak.err"; then
  echo "at-rest checker accepted a deliberate plaintext leak" >&2
  exit 1
fi
grep -q "forbidden pattern 'payload_sentinel' found" "${TMP_DIR}/leak.err"

echo "stable primary and recovery ciphertext-at-rest inspection passed"
