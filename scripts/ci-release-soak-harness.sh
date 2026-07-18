#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19564}"
STREAMS_PORT="${STREAMS_PORT:-18164}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
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
  local operator="${3:-false}"
  local lifetime_secs="${4:-600}"
  local operator_claim=""
  local now expires header payload signature
  if [[ "${operator}" == true ]]; then
    operator_claim=',"operator":true'
  fi
  now="$(date +%s)"
  expires=$(( now + lifetime_secs ))
  header="$(printf '%s' '{"alg":"RS256","kid":"ci-rsa","typ":"JWT"}' | b64url)"
  payload="$(printf '{"sub":"%s","exp":%s,"iat":%s,"iss":"%s","aud":"%s","jti":"%s"%s,"stream_prefixes":[""],"verbs":["create","append","read","list"]}' \
    "${subject}" "${expires}" "${now}" "${ISSUER}" "${AUDIENCE}" "${token_id}" \
    "${operator_claim}" | b64url)"
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

if "${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:$((STREAMS_PORT + 1))" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test --path-prefix reject-static-operator \
  --initial-shards 1 --auth-token stale-static-operator \
  --auth-jwks-url "${S3_URL}/auth/jwks.json" \
  --auth-revocation-url "${S3_URL}/auth/revocations.json" \
  --auth-issuer "${ISSUER}" --auth-audience "${AUDIENCE}" \
  >"${TMP_DIR}/static-operator.out" 2>"${TMP_DIR}/static-operator.err"; then
  echo "JWKS mode accepted the pilot static operator token" >&2
  exit 1
fi
grep -q 'AUTH_TOKEN is pilot-only' "${TMP_DIR}/static-operator.err"

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
  --initial-shards 1 \
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

"${TARGET_DIR}/streams-keys" generate >"${TMP_DIR}/victim-key"
"${TARGET_DIR}/streams-keys" generate >"${TMP_DIR}/attacker-key"
chmod 600 "${TMP_DIR}/victim-key" "${TMP_DIR}/attacker-key"
export SOAK_STREAM_KEY_FILE="${TMP_DIR}/victim-key"
export SOAK_ATTACKER_STREAM_KEY_FILE="${TMP_DIR}/attacker-key"
unset SOAK_STREAM_KEY SOAK_ATTACKER_STREAM_KEY SOAK_OPERATOR_TOKEN SOAK_AUTH_TOKEN
TOKEN_A="$(sign_token 'soak-victim' 'soak-victim-a')"
TOKEN_B="$(sign_token 'soak-victim' 'soak-victim-b')"
ATTACKER_TOKEN_A="$(sign_token 'soak-attacker' 'soak-attacker-a')"
ATTACKER_TOKEN_B="$(sign_token 'soak-attacker' 'soak-attacker-b')"
OPERATOR_TOKEN_A="$(sign_token 'soak-operator' 'soak-operator-a' true)"
OPERATOR_TOKEN_B="$(sign_token 'soak-operator' 'soak-operator-b' true)"
APPROVER_TOKEN_A="$(sign_token 'soak-approver' 'soak-approver-a' true)"
LONG_OPERATOR_TOKEN="$(sign_token 'soak-long-operator' 'soak-long-operator-a' true 3600)"
export TOKEN_A TOKEN_B ATTACKER_TOKEN_A ATTACKER_TOKEN_B OPERATOR_TOKEN_A OPERATOR_TOKEN_B
export APPROVER_TOKEN_A LONG_OPERATOR_TOKEN
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/debug/metrics" \
  -H "authorization: Bearer ${TOKEN_A}")" == "401" ]]
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/debug/metrics" \
  -H "authorization: Bearer ${OPERATOR_TOKEN_A}")" == "200" ]]
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${STREAMS_URL}/v1/debug/metrics" \
  -H "authorization: Bearer ${LONG_OPERATOR_TOKEN}")" == "401" ]]

# Production admin mutation requires two independently authenticated people;
# missing approval, a tenant token, or a second token for the same subject all
# fail before the split runs. The immutable result carries both identities.
admin_url="${STREAMS_URL}/v1/admin/shards/root/split"
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -X POST \
  "${admin_url}" -H "authorization: Bearer ${OPERATOR_TOKEN_A}")" == "403" ]]
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -X POST \
  "${admin_url}" -H "authorization: Bearer ${OPERATOR_TOKEN_A}" \
  -H "x-prisma-operator-approval: Bearer ${TOKEN_A}")" == "403" ]]
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -X POST \
  "${admin_url}" -H "authorization: Bearer ${OPERATOR_TOKEN_A}" \
  -H "x-prisma-operator-approval: Bearer ${OPERATOR_TOKEN_B}")" == "403" ]]
curl --fail --silent --show-error -X POST "${admin_url}" \
  -D "${TMP_DIR}/admin.headers" \
  -H "authorization: Bearer ${OPERATOR_TOKEN_A}" \
  -H "x-prisma-operator-approval: Bearer ${APPROVER_TOKEN_A}" >/dev/null
python3 - "${S3_URL}" "${PREFIX}" "${TMP_DIR}/admin.headers" <<'PY'
import json
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

endpoint, prefix, headers_path = sys.argv[1:]
admin_request_id = None
for line in open(headers_path, encoding="utf-8"):
    name, _, value = line.partition(":")
    if name.lower() == "x-prisma-request-id":
        admin_request_id = value.strip()
assert admin_request_id is not None
assert len(admin_request_id) == 32
int(admin_request_id, 16)
audit_prefix = f"{prefix}/audit/control/"
query = urllib.parse.urlencode({"list-type": "2", "prefix": audit_prefix})
with urllib.request.urlopen(f"{endpoint}/streams?{query}", timeout=2) as response:
    root = ET.fromstring(response.read())
keys = [node.text for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "Key"]
assert len(keys) == 4, f"expected four production admin audit events, got {len(keys)}"
events = []
for key in keys:
    url = f"{endpoint}/streams/{urllib.parse.quote(key, safe='/')}"
    with urllib.request.urlopen(url, timeout=2) as response:
        events.append(json.load(response))
assert sorted(event["status"] for event in events) == [200, 403, 403, 403]
assert all(event["format_version"] == 1 for event in events)
assert all(len(event["request_id"]) == 32 for event in events)
assert all(int(event["request_id"], 16) >= 0 for event in events)
assert len({event["request_id"] for event in events}) == 4
assert all(event["customer_id"] == "soak-operator" for event in events)
assert all(event["token_id"] == "soak-operator-a" for event in events)
assert all(event["stream"] == "/v1/admin/shards/root/split" for event in events)
assert all(event["method"] == "POST" for event in events)
event = next(event for event in events if event["status"] == 200)
assert event["request_id"] == admin_request_id
assert event["customer_id"] == "soak-operator"
assert event["token_id"] == "soak-operator-a"
assert event["approval_customer_id"] == "soak-approver"
assert event["approval_token_id"] == "soak-approver-a"
denied_approvers = {
    (event.get("approval_customer_id"), event.get("approval_token_id"))
    for event in events if event["status"] == 403
}
assert denied_approvers == {
    (None, None),
    ("soak-victim", "soak-victim-a"),
    ("soak-operator", "soak-operator-b"),
}
PY
printf '%s\n' "${TOKEN_A}" >"${TMP_DIR}/workload-token"
chmod 600 "${TMP_DIR}/workload-token"
export SOAK_AUTH_TOKEN_FILE="${TMP_DIR}/workload-token"
printf '%s\n' "${ATTACKER_TOKEN_A}" >"${TMP_DIR}/attacker-token"
chmod 600 "${TMP_DIR}/attacker-token"
export SOAK_ATTACKER_AUTH_TOKEN_FILE="${TMP_DIR}/attacker-token"
printf '%s\n' "${OPERATOR_TOKEN_A}" >"${TMP_DIR}/operator-token"
chmod 600 "${TMP_DIR}/operator-token"
export SOAK_OPERATOR_TOKEN_FILE="${TMP_DIR}/operator-token"
(
  sleep 1.5
  printf '%s\n' "${TOKEN_B}" >"${TMP_DIR}/workload-token.next"
  chmod 600 "${TMP_DIR}/workload-token.next"
  mv "${TMP_DIR}/workload-token.next" "${TMP_DIR}/workload-token"
  printf '%s\n' "${ATTACKER_TOKEN_B}" >"${TMP_DIR}/attacker-token.next"
  chmod 600 "${TMP_DIR}/attacker-token.next"
  mv "${TMP_DIR}/attacker-token.next" "${TMP_DIR}/attacker-token"
  printf '%s\n' "${OPERATOR_TOKEN_B}" >"${TMP_DIR}/operator-token.next"
  chmod 600 "${TMP_DIR}/operator-token.next"
  mv "${TMP_DIR}/operator-token.next" "${TMP_DIR}/operator-token"
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
assert evidence["monitor"]["auth"]["source"] == "file"
assert evidence["monitor"]["auth"]["subject_pinned"] is True
assert evidence["monitor"]["auth"]["token_changes"] >= 1
assert evidence["monitor"]["auth"]["refresh_failures"] == 0
assert all(item["passed"] for item in evidence["checks"].values())
raw = pathlib.Path(sys.argv[1]).read_text()
assert pathlib.Path(os.environ["SOAK_STREAM_KEY_FILE"]).read_text().strip() not in raw
assert pathlib.Path(os.environ["SOAK_ATTACKER_STREAM_KEY_FILE"]).read_text().strip() not in raw
assert pathlib.Path(os.environ["SOAK_OPERATOR_TOKEN_FILE"]).read_text().strip() not in raw
assert os.environ["TOKEN_A"] not in raw
assert os.environ["TOKEN_B"] not in raw
assert os.environ["ATTACKER_TOKEN_A"] not in raw
assert os.environ["ATTACKER_TOKEN_B"] not in raw
assert os.environ["OPERATOR_TOKEN_A"] not in raw
assert os.environ["OPERATOR_TOKEN_B"] not in raw
assert os.environ["APPROVER_TOKEN_A"] not in raw
assert os.environ["LONG_OPERATOR_TOKEN"] not in raw
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

if SOAK_STREAM_KEY=forbidden-raw-secret python3 scripts/release-soak.py \
  --url "${STREAMS_URL}" --metrics-url "${STREAMS_URL}" \
  --bench-bin "${TARGET_DIR}/bench" --evidence "${TMP_DIR}/raw-secret.json" \
  --release-id ci-raw-secret --target-label hermetic-s3lite \
  --instance-class local-process --storage-provider s3lite \
  --duration-secs 1 --warmup-secs 0 --monitor-secs 1 --drain-secs 0 \
  --concurrency 1 --streams 1 --payload-bytes 64 --allow-short \
  >"${TMP_DIR}/raw-secret.out" 2>"${TMP_DIR}/raw-secret.err"; then
  echo "release soak accepted a raw secret environment variable" >&2
  exit 1
fi
grep -q 'raw secret environment variables are forbidden' "${TMP_DIR}/raw-secret.err"

echo "target-hardware release-soak harness smoke passed"
