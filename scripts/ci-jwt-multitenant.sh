#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19509}"
STREAMS_PORT="${STREAMS_PORT:-18100}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
PREFIX="ci-jwt-multitenant"
ISSUER="https://issuer.invalid/ci"
AUDIENCE="prisma-streams"
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

b64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

sign_token() {
  local subject="$1"
  local token_id="$2"
  local prefixes="$3"
  local verbs="$4"
  local now expires header payload signature
  now="$(date +%s)"
  expires=$(( now + 600 ))
  header="$(printf '%s' '{"alg":"RS256","kid":"ci-rsa","typ":"JWT"}' | b64url)"
  payload="$(printf '{"sub":"%s","exp":%s,"iat":%s,"iss":"%s","aud":"%s","jti":"%s","stream_prefixes":%s,"verbs":%s}' \
    "${subject}" "${expires}" "${now}" "${ISSUER}" "${AUDIENCE}" \
    "${token_id}" "${prefixes}" "${verbs}" | b64url)"
  signature="$(printf '%s.%s' "${header}" "${payload}" |
    openssl dgst -sha256 -sign "${TMP_DIR}/jwt-key.pem" | b64url)"
  printf '%s.%s.%s' "${header}" "${payload}" "${signature}"
}

expect_status() {
  local expected="$1"
  shift
  local actual
  actual="$(curl --silent --output /dev/null --write-out '%{http_code}' "$@")"
  [[ "${actual}" == "${expected}" ]]
}

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "${TMP_DIR}/jwt-key.pem" >/dev/null 2>&1
modulus_hex="$(openssl rsa -in "${TMP_DIR}/jwt-key.pem" -modulus -noout |
  sed 's/^Modulus=//')"
modulus="$(printf '%s' "${modulus_hex}" | xxd -r -p | b64url)"
printf '{"keys":[{"kty":"RSA","kid":"ci-rsa","use":"sig","alg":"RS256","n":"%s","e":"AQAB"}]}' \
  "${modulus}" >"${TMP_DIR}/jwks.json"
printf '%s' '{"version":1,"revoked_token_ids":[]}' >"${TMP_DIR}/revocations.json"

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
for _ in $(seq 1 100); do
  if curl --fail --silent -X PUT "${S3_URL}/auth" >/dev/null; then
    break
  fi
  sleep 0.02
done
curl --fail --silent -X PUT "${S3_URL}/auth/jwks.json" \
  --data-binary "@${TMP_DIR}/jwks.json" >/dev/null
curl --fail --silent -X PUT "${S3_URL}/auth/revocations.json" \
  --data-binary "@${TMP_DIR}/revocations.json" >/dev/null

"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test \
  --path-prefix "${PREFIX}" --initial-shards 1 \
  --auth-jwks-url "${S3_URL}/auth/jwks.json" \
  --auth-revocation-url "${S3_URL}/auth/revocations.json" \
  --auth-issuer "${ISSUER}" --auth-audience "${AUDIENCE}" \
  --auth-jwks-refresh-secs 1 --auth-jwks-max-stale-secs 10 \
  --auth-revocation-refresh-secs 1 --auth-revocation-max-stale-secs 10 \
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

all_verbs='["create","append","read","delete","queue","touch","list"]'
TOKEN_A="$(sign_token 'tenant-a' 'tenant-a-token' '[""]' "${all_verbs}")"
TOKEN_B="$(sign_token 'tenant-b' 'tenant-b-token' '[""]' "${all_verbs}")"
TOKEN_RESTRICTED="$(sign_token 'tenant-a' 'tenant-a-read-limited' '["allowed/"]' '["read"]')"
KEY_A="$(${TARGET_DIR}/streams-keys generate)"
KEY_B="$(${TARGET_DIR}/streams-keys generate)"
auth_a=(-H "authorization: Bearer ${TOKEN_A}" -H "stream-encryption-key: ${KEY_A}")
auth_b=(-H "authorization: Bearer ${TOKEN_B}" -H "stream-encryption-key: ${KEY_B}")

# Identical logical names are distinct registry, routing, storage, and key
# identities. Each tenant sees only its own bytes.
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/shared" \
  "${auth_a[@]}" -H 'content-type: text/plain' -d 'tenant-a' >/dev/null
curl --fail --silent --show-error -X PUT "${STREAMS_URL}/v1/stream/shared" \
  "${auth_b[@]}" -H 'content-type: text/plain' -d 'tenant-b' >/dev/null
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/shared" "${auth_a[@]}")" == 'tenant-a' ]]
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/shared" "${auth_b[@]}")" == 'tenant-b' ]]

curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/only-a" \
  "${auth_a[@]}" -H 'content-type: text/plain' -d 'private-a' >/dev/null
expect_status 404 "${STREAMS_URL}/v1/stream/only-a" "${auth_b[@]}"
list_a="$(curl --fail --silent "${STREAMS_URL}/v1/streams" -H "authorization: Bearer ${TOKEN_A}")"
list_b="$(curl --fail --silent "${STREAMS_URL}/v1/streams" -H "authorization: Bearer ${TOKEN_B}")"
[[ "${list_a}" == *'only-a'* && "${list_b}" != *'only-a'* ]]

# A valid token for the same customer still cannot escape its verb/name
# capability. Missing authentication also fails before any existence lookup.
expect_status 403 "${STREAMS_URL}/v1/stream/shared" \
  -H "authorization: Bearer ${TOKEN_RESTRICTED}" -H "stream-encryption-key: ${KEY_A}"
expect_status 403 -X PUT "${STREAMS_URL}/v1/stream/allowed/new" \
  -H "authorization: Bearer ${TOKEN_RESTRICTED}" -H "stream-encryption-key: ${KEY_A}"
expect_status 401 "${STREAMS_URL}/v1/stream/shared" -H "stream-encryption-key: ${KEY_A}"

# Revocation is observed by the live background poller. A lower document
# version cannot un-revoke the token.
printf '%s' '{"version":2,"revoked_token_ids":["tenant-a-token"]}' \
  >"${TMP_DIR}/revocations.json"
curl --fail --silent -X PUT "${S3_URL}/auth/revocations.json" \
  --data-binary "@${TMP_DIR}/revocations.json" >/dev/null
attempts=0
until expect_status 401 "${STREAMS_URL}/v1/stream/shared" "${auth_a[@]}"; do
  attempts=$((attempts + 1))
  if (( attempts > 50 )); then
    exit 1
  fi
  sleep 0.1
done
[[ "$(curl --fail --silent "${STREAMS_URL}/v1/stream/shared" "${auth_b[@]}")" == 'tenant-b' ]]

printf '%s' '{"version":1,"revoked_token_ids":[]}' >"${TMP_DIR}/revocations.json"
curl --fail --silent -X PUT "${S3_URL}/auth/revocations.json" \
  --data-binary "@${TMP_DIR}/revocations.json" >/dev/null
sleep 1.2
expect_status 401 "${STREAMS_URL}/v1/stream/shared" "${auth_a[@]}"
grep -q 'revocation document rollback rejected' "${TMP_DIR}/streams.log"

echo "production JWT multi-tenant isolation and revocation passed"
