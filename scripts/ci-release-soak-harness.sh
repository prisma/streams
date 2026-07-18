#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19564}"
STREAMS_PORT="${STREAMS_PORT:-18164}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
AUTH_TOKEN="ci-release-soak"
PREFIX="release-soak-ci"
ISSUER="https://issuer.invalid/release-soak"
AUDIENCE="prisma-streams"
TMP_DIR="$(mktemp -d)"
S3_PID=""
STREAMS_PID=""
ROTATE_PID=""

cleanup() {
  if [[ -n "${STREAMS_PID}" ]]; then
    kill "${STREAMS_PID}" 2>/dev/null || true
    wait "${STREAMS_PID}" 2>/dev/null || true
  fi
  if [[ -n "${S3_PID}" ]]; then
    kill "${S3_PID}" 2>/dev/null || true
    wait "${S3_PID}" 2>/dev/null || true
  fi
  if [[ -n "${ROTATE_PID}" ]]; then
    wait "${ROTATE_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

b64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

sign_token() {
  local subject="$1"
  local token_id="$2"
  local now expires header payload signature
  now="$(date +%s)"
  expires=$(( now + 600 ))
  header="$(printf '%s' '{"alg":"RS256","kid":"ci-rsa","typ":"JWT"}' | b64url)"
  payload="$(printf '{"sub":"%s","exp":%s,"iat":%s,"iss":"%s","aud":"%s","jti":"%s","stream_prefixes":[""],"verbs":["create","append","read","list"]}' \
    "${subject}" "${expires}" "${now}" "${ISSUER}" "${AUDIENCE}" "${token_id}" | b64url)"
  signature="$(printf '%s.%s' "${header}" "${payload}" |
    openssl dgst -sha256 -sign "${TMP_DIR}/jwt-key.pem" | b64url)"
  printf '%s.%s.%s' "${header}" "${payload}" "${signature}"
}

customer_hash() {
  printf '%s' "$1" | openssl dgst -sha256 -binary |
    xxd -p -c 256 | cut -c1-32
}

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
for _ in $(seq 1 100); do
  if curl --fail --silent -X PUT "${S3_URL}/auth" >/dev/null; then
    break
  fi
  sleep 0.02
done

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "${TMP_DIR}/jwt-key.pem" >/dev/null 2>&1
modulus_hex="$(openssl rsa -in "${TMP_DIR}/jwt-key.pem" -modulus -noout |
  sed 's/^Modulus=//')"
modulus="$(printf '%s' "${modulus_hex}" | xxd -r -p | b64url)"
printf '{"keys":[{"kty":"RSA","kid":"ci-rsa","use":"sig","alg":"RS256","n":"%s","e":"AQAB"}]}' \
  "${modulus}" >"${TMP_DIR}/jwks.json"
printf '%s' '{"version":1,"revoked_token_ids":[]}' >"${TMP_DIR}/revocations.json"
curl --fail --silent -X PUT "${S3_URL}/auth/jwks.json" \
  --data-binary "@${TMP_DIR}/jwks.json" >/dev/null
curl --fail --silent -X PUT "${S3_URL}/auth/revocations.json" \
  --data-binary "@${TMP_DIR}/revocations.json" >/dev/null

victim_hash="$(customer_hash 'soak-victim')"
attacker_hash="$(customer_hash 'soak-attacker')"
printf '%s' '{"version":1,"max_inflight":16,"write_bytes_per_second":1048576,"write_burst_bytes":1048576,"streams_count":10}' \
  >"${TMP_DIR}/victim-limits.json"
printf '%s' '{"version":1,"max_inflight":2,"write_bytes_per_second":1024,"write_burst_bytes":1024,"streams_count":10}' \
  >"${TMP_DIR}/attacker-limits.json"
curl --fail --silent -X PUT \
  "${S3_URL}/streams/${PREFIX}/customers/${victim_hash}/limits.json" \
  --data-binary "@${TMP_DIR}/victim-limits.json" >/dev/null
curl --fail --silent -X PUT \
  "${S3_URL}/streams/${PREFIX}/customers/${attacker_hash}/limits.json" \
  --data-binary "@${TMP_DIR}/attacker-limits.json" >/dev/null

"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --path-prefix "${PREFIX}" \
  --initial-shards 1 --auth-token "${AUTH_TOKEN}" \
  --auth-jwks-url "${S3_URL}/auth/jwks.json" \
  --auth-revocation-url "${S3_URL}/auth/revocations.json" \
  --auth-issuer "${ISSUER}" --auth-audience "${AUDIENCE}" \
  --auth-jwks-refresh-secs 1 --auth-jwks-max-stale-secs 10 \
  --auth-revocation-refresh-secs 1 --auth-revocation-max-stale-secs 10 \
  --absorb-bytes 1 --absorb-age-secs 1 \
  >"${TMP_DIR}/streams.log" 2>&1 &
STREAMS_PID=$!

attempts=0
until curl --fail --silent "${STREAMS_URL}/health/ready" >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 200 )); then
    tail -150 "${TMP_DIR}/streams.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done

export SOAK_STREAM_KEY
SOAK_STREAM_KEY="$(${TARGET_DIR}/streams-keys generate)"
export SOAK_ATTACKER_STREAM_KEY
SOAK_ATTACKER_STREAM_KEY="$(${TARGET_DIR}/streams-keys generate)"
export SOAK_OPERATOR_TOKEN="${AUTH_TOKEN}"
unset SOAK_AUTH_TOKEN
TOKEN_A="$(sign_token 'soak-victim' 'soak-victim-a')"
TOKEN_B="$(sign_token 'soak-victim' 'soak-victim-b')"
ATTACKER_TOKEN_A="$(sign_token 'soak-attacker' 'soak-attacker-a')"
ATTACKER_TOKEN_B="$(sign_token 'soak-attacker' 'soak-attacker-b')"
export TOKEN_A TOKEN_B ATTACKER_TOKEN_A ATTACKER_TOKEN_B
printf '%s\n' "${TOKEN_A}" >"${TMP_DIR}/workload-token"
chmod 600 "${TMP_DIR}/workload-token"
export SOAK_AUTH_TOKEN_FILE="${TMP_DIR}/workload-token"
printf '%s\n' "${ATTACKER_TOKEN_A}" >"${TMP_DIR}/attacker-token"
chmod 600 "${TMP_DIR}/attacker-token"
export SOAK_ATTACKER_AUTH_TOKEN_FILE="${TMP_DIR}/attacker-token"
(
  sleep 1.5
  printf '%s\n' "${TOKEN_B}" >"${TMP_DIR}/workload-token.next"
  chmod 600 "${TMP_DIR}/workload-token.next"
  mv "${TMP_DIR}/workload-token.next" "${TMP_DIR}/workload-token"
  printf '%s\n' "${ATTACKER_TOKEN_B}" >"${TMP_DIR}/attacker-token.next"
  chmod 600 "${TMP_DIR}/attacker-token.next"
  mv "${TMP_DIR}/attacker-token.next" "${TMP_DIR}/attacker-token"
) &
ROTATE_PID=$!
python3 scripts/release-soak.py \
  --url "${STREAMS_URL}" --metrics-url "${STREAMS_URL}" \
  --metrics-url "http://localhost:${STREAMS_PORT}" \
  --bench-bin "${TARGET_DIR}/bench" --evidence "${TMP_DIR}/evidence.json" \
  --release-id ci-short --target-label hermetic-s3lite \
  --instance-class local-process --storage-provider s3lite \
  --duration-secs 3 --warmup-secs 1 --monitor-secs 1 --drain-secs 8 \
  --concurrency 4 --streams 2 --payload-bytes 128 --allow-short \
  --require-token-rotation --auth-token-refresh-secs 1 \
  --require-noisy-neighbor --min-attacker-attempts 10 \
  --max-p99-ms 2000 --max-p999-ms 5000 \
  >"${TMP_DIR}/soak.stdout"

python3 - "${TMP_DIR}/evidence.json" <<'PY'
import json
import os
import pathlib
import sys

evidence = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert evidence["format_version"] == 1
assert evidence["status"] == "pass"
assert evidence["workload"]["short_run"] is True
assert evidence["workload"]["token_rotation_required"] is True
assert evidence["workload"]["noisy_neighbor_required"] is True
assert evidence["target"]["metrics_targets"] == 2
assert evidence["monitor"]["samples"] >= 6
assert evidence["bench"]["auth"]["source"] == "file"
assert evidence["bench"]["auth"]["subject_pinned"] is True
assert evidence["bench"]["auth"]["token_changes"] >= 1
assert evidence["bench"]["auth"]["refresh_failures"] == 0
assert evidence["noisy_neighbor"]["attempts"] >= 10
assert evidence["noisy_neighbor"]["non_429"] == 0
assert evidence["noisy_neighbor"]["auth"]["subject_pinned"] is True
assert evidence["noisy_neighbor"]["auth"]["token_changes"] >= 1
assert evidence["noisy_neighbor"]["auth"]["refresh_failures"] == 0
assert all(item["passed"] for item in evidence["checks"].values())
raw = pathlib.Path(sys.argv[1]).read_text()
assert os.environ["SOAK_STREAM_KEY"] not in raw
assert os.environ["SOAK_ATTACKER_STREAM_KEY"] not in raw
assert os.environ["SOAK_OPERATOR_TOKEN"] not in raw
assert os.environ["TOKEN_A"] not in raw
assert os.environ["TOKEN_B"] not in raw
assert os.environ["ATTACKER_TOKEN_A"] not in raw
assert os.environ["ATTACKER_TOKEN_B"] not in raw
PY

if python3 scripts/release-soak.py \
  --url "${STREAMS_URL}" --metrics-url "${STREAMS_URL}" \
  --bench-bin "${TARGET_DIR}/bench" --evidence "${TMP_DIR}/rejected.json" \
  --release-id ci-rejected-budget --target-label hermetic-s3lite \
  --instance-class local-process --storage-provider s3lite \
  --duration-secs 1 --warmup-secs 0 --monitor-secs 1 --drain-secs 0 \
  --concurrency 1 --streams 1 --payload-bytes 64 --allow-short \
  --min-req-per-sec 1000000000000 --max-p99-ms 5000 --max-p999-ms 5000 \
  >"${TMP_DIR}/rejected.stdout"; then
  echo "release soak accepted an impossible throughput budget" >&2
  exit 1
fi
python3 - "${TMP_DIR}/rejected.json" <<'PY'
import json
import pathlib
import sys

evidence = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert evidence["status"] == "fail"
assert evidence["checks"]["throughput"]["passed"] is False
PY

echo "target-hardware release-soak harness smoke passed"
