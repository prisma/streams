#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19533}"
PORT_A="${PORT_A:-18133}"
PORT_B="${PORT_B:-18134}"
S3_URL="http://127.0.0.1:${S3_PORT}"
URL_A="http://127.0.0.1:${PORT_A}"
URL_B="http://127.0.0.1:${PORT_B}"
ISSUER="https://issuer.invalid/multicell-ci"
AUDIENCE="prisma-streams"
TMP_DIR="$(mktemp -d)"
S3_PID=""
PID_A=""
PID_B=""

cleanup() {
  local status=$?
  if (( status != 0 )); then
    echo "multi-cell drill failed; cell logs follow" >&2
    for log in "${TMP_DIR}/cell-a.log" "${TMP_DIR}/cell-b.log"; do
      if [[ -f "${log}" ]]; then
        echo "${log}" >&2
        tail -120 "${log}" >&2 || true
      fi
    done
  fi
  for pid in "${PID_A}" "${PID_B}" "${S3_PID}"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  rm -rf "${TMP_DIR}"
  return "${status}"
}
trap cleanup EXIT

fail() {
  echo "$1" >&2
  exit 1
}

b64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

sign_token() {
  local now expires header payload signature
  now="$(date +%s)"
  expires=$(( now + 600 ))
  header="$(printf '%s' '{"alg":"RS256","kid":"ci-rsa","typ":"JWT"}' | b64url)"
  payload="$(printf '{"sub":"tenant-a","exp":%s,"iat":%s,"iss":"%s","aud":"%s","jti":"multicell-token","stream_prefixes":[""],"verbs":["create","append","read","delete","queue","touch","list"]}' \
    "${expires}" "${now}" "${ISSUER}" "${AUDIENCE}" | b64url)"
  signature="$(printf '%s.%s' "${header}" "${payload}" |
    openssl dgst -sha256 -sign "${TMP_DIR}/jwt-key.pem" | b64url)"
  printf '%s.%s.%s' "${header}" "${payload}" "${signature}"
}

directory_v1='{"version":1,"generation":1,"cells":[{"cell_id":"c-a","region":"test-a","ops_prefix":"cells/c-a","weight":1,"state":"active"},{"cell_id":"c-b","region":"test-b","ops_prefix":"cells/c-b","weight":1,"state":"active"}]}'
directory_v2='{"version":1,"generation":2,"cells":[{"cell_id":"c-a","region":"test-a","ops_prefix":"cells/c-a","weight":1,"state":"active"},{"cell_id":"c-b","region":"test-b","ops_prefix":"cells/c-b","weight":1,"state":"active"}]}'
migration_directory='{"version":1,"generation":1,"cells":[{"cell_id":"c-a","region":"test-a","ops_prefix":"cells/c-a","weight":1,"state":"active"}]}'

put_directory() {
  curl --fail --silent --show-error -X PUT \
    "${S3_URL}/streams/global-registry/cells.json" \
    -H 'content-type: application/json' --data-binary "$1" >/dev/null
}

wait_ready() {
  local url="$1"
  local log="$2"
  local attempts=0
  until curl --fail --silent "${url}/health/ready" >/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 250 )); then
      echo "cell did not become ready" >&2
      tail -120 "${log}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

start_cell() {
  local cell="$1"
  local port="$2"
  local log="$3"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${port}" --instance-name "${cell}-1" \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id "${cell}-data" --secret-access-key "${cell}-data-secret" \
    --path-prefix "cells/${cell}" --cell-id "${cell}" \
    --registry-s3-endpoint "${S3_URL}" --registry-s3-bucket streams \
    --registry-s3-region auto --registry-s3-access-key-id registry-control \
    --registry-s3-secret-access-key registry-control-secret \
    --registry-s3-allow-http --registry-path-prefix global-registry \
    --cell-directory-refresh-secs 5 --initial-shards 1 \
    --auth-jwks-url "${S3_URL}/auth/jwks.json" \
    --auth-revocation-url "${S3_URL}/auth/revocations.json" \
    --auth-issuer "${ISSUER}" --auth-audience "${AUDIENCE}" \
    --auth-jwks-refresh-secs 1 --auth-jwks-max-stale-secs 10 \
    --auth-revocation-refresh-secs 1 --auth-revocation-max-stale-secs 10 \
    >"${log}" 2>&1 &
  printf '%s' "$!"
}

replay_cell() {
  awk 'BEGIN {IGNORECASE=1} /^streams-replay-to-cell:/ {gsub("\r", "", $2); print $2}' "$1"
}

url_for_cell() {
  case "$1" in
    c-a) printf '%s' "${URL_A}" ;;
    c-b) printf '%s' "${URL_B}" ;;
    *) return 1 ;;
  esac
}

other_cell() {
  case "$1" in
    c-a) printf 'c-b' ;;
    c-b) printf 'c-a' ;;
    *) return 1 ;;
  esac
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
attempts=0
until curl --silent --fail -I "${S3_URL}/streams" >/dev/null; do
  attempts=$((attempts + 1))
  if (( attempts > 100 )); then
    echo "s3lite did not become ready" >&2
    exit 1
  fi
  sleep 0.05
done
curl --fail --silent --show-error -X PUT "${S3_URL}/auth" >/dev/null
curl --fail --silent --show-error -X PUT "${S3_URL}/auth/jwks.json" \
  --data-binary "@${TMP_DIR}/jwks.json" >/dev/null
curl --fail --silent --show-error -X PUT "${S3_URL}/auth/revocations.json" \
  --data-binary "@${TMP_DIR}/revocations.json" >/dev/null

# Prove the explicit one-cell cutover before admitting a second cell. This
# deleted descriptor needs no data corpus but exercises affinity, index-first,
# descriptor CAS, idempotence, and the mandatory post-audit.
put_directory "${migration_directory}"
migration_customer_hash="$(printf 'tenant-migrate' | shasum -a 256 | awk '{print substr($1,1,32)}')"
migration_name_hash="$(printf 'migrated' | shasum -a 256 | awk '{print substr($1,1,32)}')"
migration_descriptor="${S3_URL}/streams/global-registry/registry/by-customer/${migration_customer_hash}/by-name/${migration_name_hash:0:2}/${migration_name_hash}.json"
curl --fail --silent --show-error -X PUT "${migration_descriptor}" \
  -H 'content-type: application/json' \
  --data-binary '{"customer_id":"tenant-migrate","name":"migrated","stream_epoch":"00000000000000000000000000000000","key_fingerprint":"migration","created_ms":1,"deleted":true}' >/dev/null
cell_admin=("${TARGET_DIR}/streams-cell-admin" --cell-id c-a \
  --s3-endpoint "${S3_URL}" --s3-bucket streams --s3-region auto \
  --s3-access-key-id registry-control \
  --s3-secret-access-key registry-control-secret --s3-allow-http \
  --path-prefix global-registry --max-descriptors 10)
audit="$("${cell_admin[@]}")"
[[ "${audit}" == *'"pending_placements": 1'* ]] || fail "migration audit missed unassigned descriptor"
applied="$("${cell_admin[@]}" --apply --confirm-serving-quiesced)"
[[ "${applied}" == *'"pending_placements": 0'* ]] || fail "migration post-audit was incomplete"
migrated="$(curl --fail --silent --show-error "${migration_descriptor}")"
[[ "${migrated}" == *'"cell":"c-a"'* ]] || fail "migration did not pin the descriptor"

put_directory "${directory_v1}"

PID_A="$(start_cell c-a "${PORT_A}" "${TMP_DIR}/cell-a.log")"
PID_B="$(start_cell c-b "${PORT_B}" "${TMP_DIR}/cell-b.log")"
wait_ready "${URL_A}" "${TMP_DIR}/cell-a.log"
wait_ready "${URL_B}" "${TMP_DIR}/cell-b.log"

KEY="$("${TARGET_DIR}/streams-keys" generate)"
TOKEN="$(sign_token)"
auth=(-H "authorization: Bearer ${TOKEN}")

# Send the create to an arbitrary cell. The global affinity CAS and directory
# either place it there or return the cell-level replay correction.
status="$(curl --silent --show-error -D "${TMP_DIR}/create.headers" \
  -o "${TMP_DIR}/create.body" --write-out '%{http_code}' -X PUT \
  "${URL_A}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H 'content-type: application/json' \
  -d '[{"cell":true}]')"
if [[ "${status}" == "409" ]]; then
  selected="$(replay_cell "${TMP_DIR}/create.headers")"
  [[ -n "${selected}" ]] || fail "create replay omitted its target cell"
  selected_url="$(url_for_cell "${selected}")"
  status="$(curl --silent --show-error -o "${TMP_DIR}/create.body" \
    --write-out '%{http_code}' -X PUT \
    "${selected_url}/v1/stream/cell-pinned" "${auth[@]}" \
    -H "stream-encryption-key: ${KEY}" -H 'content-type: application/json' \
    -d '[{"cell":true}]')"
else
  selected='c-a'
  selected_url="${URL_A}"
fi
[[ "${status}" == "201" ]] || fail "selected cell did not create the stream: ${status}"
other="$(other_cell "${selected}")"
other_url="$(url_for_cell "${other}")"

# The wrong cell must replay before opening a shard or consulting a key.
status="$(curl --silent --show-error -D "${TMP_DIR}/wrong.headers" \
  -o "${TMP_DIR}/wrong.body" --write-out '%{http_code}' \
  "${other_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${status}" == "409" ]] || fail "wrong cell did not replay the stream: ${status}"
[[ "$(replay_cell "${TMP_DIR}/wrong.headers")" == "${selected}" ]] || fail "wrong-cell replay target was incorrect"
listing="$(curl --fail --silent --show-error --get "${S3_URL}/streams" \
  --data-urlencode 'list-type=2' --data-urlencode "prefix=cells/${other}/shards/")"
[[ "${listing}" != *"<Key>cells/${other}/shards/"* ]] || fail "wrong cell opened a shard"

body="$(curl --fail --silent --show-error \
  "${selected_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"cell":true}]' ]] || fail "owning cell returned the wrong body"

# A second stream for the customer cannot escape its durable <=4-cell
# affinity even if its independent rendezvous score would differ.
status="$(curl --silent --show-error -D "${TMP_DIR}/second.headers" \
  -o /dev/null --write-out '%{http_code}' -X PUT \
  "${other_url}/v1/stream/cell-pinned-2" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${status}" == "409" ]] || fail "customer affinity allowed a second cell: ${status}"
[[ "$(replay_cell "${TMP_DIR}/second.headers")" == "${selected}" ]] || fail "affinity replay target was incorrect"

customer_hash="$(printf 'tenant-a' | shasum -a 256 | awk '{print substr($1,1,32)}')"
affinity="$(curl --fail --silent --show-error \
  "${S3_URL}/streams/global-registry/customers/${customer_hash}/cell-affinity.json")"
[[ "${affinity}" == *"\"cells\":[\"${selected}\"]"* ]] || fail "durable customer affinity was incorrect"
name_hash="$(printf 'cell-pinned' | shasum -a 256 | awk '{print substr($1,1,32)}')"
descriptor="$(curl --fail --silent --show-error \
  "${S3_URL}/streams/global-registry/registry/by-customer/${customer_hash}/by-name/${name_hash:0:2}/${name_hash}.json")"
[[ "${descriptor}" == *"\"cell\":\"${selected}\""* ]] || fail "global descriptor placement was incorrect"
cell_index="$(curl --fail --silent --show-error \
  "${S3_URL}/streams/global-registry/registry/by-cell/${selected}/by-customer/${customer_hash}/by-name/${name_hash:0:2}/${name_hash}.json")"
[[ "${cell_index}" == *'"customer_id":"tenant-a"'* ]] || fail "cell index owner was incorrect"
[[ "${cell_index}" == *"\"cell\":\"${selected}\""* ]] || fail "cell index placement was incorrect"

# Same-generation mutation is poison. Both cells retain the last-known-good
# directory for existing streams, but readiness and all new placement fail.
poisoned="${directory_v1/test-b/test-poisoned}"
put_directory "${poisoned}"
attempts=0
until [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  "${selected_url}/health/ready")" == "503" ]]; do
  attempts=$((attempts + 1))
  if (( attempts > 180 )); then
    echo "poisoned cells.json did not fail readiness" >&2
    exit 1
  fi
  sleep 0.1
done
body="$(curl --fail --silent --show-error \
  "${selected_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"cell":true}]' ]] || fail "last-known-good routing did not preserve existing reads"
status="$(curl --silent --show-error -o /dev/null --write-out '%{http_code}' -X PUT \
  "${selected_url}/v1/stream/placement-blocked" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${status}" == "503" ]] || fail "poisoned directory did not block new placement: ${status}"

put_directory "${directory_v2}"
wait_ready "${URL_A}" "${TMP_DIR}/cell-a.log"
wait_ready "${URL_B}" "${TMP_DIR}/cell-b.log"

echo "multi-cell descriptor pinning, bounded affinity, replay, and LKG drill passed"
