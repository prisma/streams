#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
S3_PORT="${S3_PORT:-19511}"
STREAMS_PORT="${STREAMS_PORT:-18103}"
S3_URL="http://127.0.0.1:${S3_PORT}"
STREAMS_URL="http://127.0.0.1:${STREAMS_PORT}"
PREFIX="ci-noisy-neighbor"
ISSUER="https://issuer.invalid/noisy-neighbor"
AUDIENCE="prisma-streams"
BASELINE_WRITES=12
NOISY_WRITES=40
NOISE_WORKERS=8
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
  local now expires header payload signature
  now="$(date +%s)"
  expires=$(( now + 600 ))
  header="$(printf '%s' '{"alg":"RS256","kid":"ci-rsa","typ":"JWT"}' | b64url)"
  payload="$(printf '{"sub":"%s","exp":%s,"iat":%s,"iss":"%s","aud":"%s","jti":"%s","stream_prefixes":[""],"verbs":["create","append","read","list"]}' \
    "${subject}" "${expires}" "${now}" "${ISSUER}" "${AUDIENCE}" \
    "${token_id}" | b64url)"
  signature="$(printf '%s.%s' "${header}" "${payload}" |
    openssl dgst -sha256 -sign "${TMP_DIR}/jwt-key.pem" | b64url)"
  printf '%s.%s.%s' "${header}" "${payload}" "${signature}"
}

customer_hash() {
  printf '%s' "$1" | openssl dgst -sha256 -binary |
    xxd -p -c 256 | cut -c1-32
}

percentile() {
  local file="$1"
  local percentile="$2"
  local count rank
  count="$(wc -l <"${file}" | tr -d ' ')"
  rank=$(( (count * percentile + 99) / 100 ))
  sort -n "${file}" | sed -n "${rank}p"
}

append_b() {
  local sequence="$1"
  local output_file="$2"
  local result status elapsed
  result="$(curl --silent --show-error --max-time 5 --output /dev/null \
    --write-out '%{http_code} %{time_total}' \
    -X POST "${STREAMS_URL}/v1/stream/victim" "${auth_b[@]}" \
    -H 'content-type: text/plain' -H 'producer-id: victim-writer' \
    -H 'producer-epoch: 0' -H "producer-seq: ${sequence}" -d 'b')"
  status="${result%% *}"
  elapsed="${result#* }"
  [[ "${status}" == "204" ]]
  printf '%s\n' "${elapsed}" >>"${output_file}"
}

noise_worker() {
  local worker="$1"
  local status_file="${TMP_DIR}/noise-${worker}.status"
  : >"${status_file}"
  for _ in $(seq 1 1000); do
    [[ -e "${TMP_DIR}/stop-noise" ]] && break
    curl --silent --max-time 5 --output /dev/null --write-out '%{http_code}\n' \
      -X POST "${STREAMS_URL}/v1/stream/noisy" "${auth_a[@]}" \
      -H 'content-type: application/octet-stream' \
      --data-binary "@${TMP_DIR}/noise.bin" >>"${status_file}" ||
      printf '%s\n' 'curl-error' >>"${status_file}"
  done
}

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "${TMP_DIR}/jwt-key.pem" >/dev/null 2>&1
modulus_hex="$(openssl rsa -in "${TMP_DIR}/jwt-key.pem" -modulus -noout |
  sed 's/^Modulus=//')"
modulus="$(printf '%s' "${modulus_hex}" | xxd -r -p | b64url)"
printf '{"keys":[{"kty":"RSA","kid":"ci-rsa","use":"sig","alg":"RS256","n":"%s","e":"AQAB"}]}' \
  "${modulus}" >"${TMP_DIR}/jwks.json"
printf '%s' '{"version":1,"revoked_token_ids":[]}' >"${TMP_DIR}/revocations.json"
head -c 16384 /dev/zero | tr '\0' 'a' >"${TMP_DIR}/noise.bin"

"${TARGET_DIR}/s3lite" --listen "127.0.0.1:${S3_PORT}" --latency-ms 1 \
  >"${TMP_DIR}/s3lite.log" 2>&1 &
S3_PID=$!
for _ in $(seq 1 100); do
  if curl --fail --silent -X PUT "${S3_URL}/auth" >/dev/null; then
    break
  fi
  sleep 0.02
done
curl --fail --silent -X PUT "${S3_URL}/streams" >/dev/null
curl --fail --silent -X PUT "${S3_URL}/auth/jwks.json" \
  --data-binary "@${TMP_DIR}/jwks.json" >/dev/null
curl --fail --silent -X PUT "${S3_URL}/auth/revocations.json" \
  --data-binary "@${TMP_DIR}/revocations.json" >/dev/null

hash_a="$(customer_hash 'tenant-a')"
hash_b="$(customer_hash 'tenant-b')"
printf '%s' '{"version":1,"max_inflight":2,"write_bytes_per_second":1024,"write_burst_bytes":1024,"streams_count":10}' \
  >"${TMP_DIR}/limits-a.json"
printf '%s' '{"version":1,"max_inflight":16,"write_bytes_per_second":1048576,"write_burst_bytes":1048576,"streams_count":10}' \
  >"${TMP_DIR}/limits-b.json"
curl --fail --silent -X PUT \
  "${S3_URL}/streams/${PREFIX}/customers/${hash_a}/limits.json" \
  --data-binary "@${TMP_DIR}/limits-a.json" >/dev/null
curl --fail --silent -X PUT \
  "${S3_URL}/streams/${PREFIX}/customers/${hash_b}/limits.json" \
  --data-binary "@${TMP_DIR}/limits-b.json" >/dev/null

"${TARGET_DIR}/streams-slate" \
  --listen "127.0.0.1:${STREAMS_PORT}" \
  --s3-endpoint "${S3_URL}" --bucket streams --region auto \
  --access-key-id test --secret-access-key test \
  --path-prefix "${PREFIX}" --initial-shards 1 --admit-max-inflight 64 \
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

TOKEN_A="$(sign_token 'tenant-a' 'noisy-token')"
TOKEN_B="$(sign_token 'tenant-b' 'victim-token')"
KEY_A="$(${TARGET_DIR}/streams-keys generate)"
KEY_B="$(${TARGET_DIR}/streams-keys generate)"
auth_a=(-H "authorization: Bearer ${TOKEN_A}" -H "stream-encryption-key: ${KEY_A}")
auth_b=(-H "authorization: Bearer ${TOKEN_B}" -H "stream-encryption-key: ${KEY_B}")

curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/noisy" \
  "${auth_a[@]}" -H 'content-type: application/octet-stream' >/dev/null
curl --fail --silent -X PUT "${STREAMS_URL}/v1/stream/victim" \
  "${auth_b[@]}" -H 'content-type: text/plain' >/dev/null

: >"${TMP_DIR}/baseline.latency"
for sequence in $(seq 0 $(( BASELINE_WRITES - 1 ))); do
  append_b "${sequence}" "${TMP_DIR}/baseline.latency"
done

noise_pids=()
for worker in $(seq 1 "${NOISE_WORKERS}"); do
  noise_worker "${worker}" &
  noise_pids+=("$!")
done

: >"${TMP_DIR}/noisy.latency"
for sequence in $(seq "${BASELINE_WRITES}" \
  $(( BASELINE_WRITES + NOISY_WRITES - 1 ))); do
  append_b "${sequence}" "${TMP_DIR}/noisy.latency"
done
touch "${TMP_DIR}/stop-noise"
for pid in "${noise_pids[@]}"; do
  wait "${pid}"
done

cat "${TMP_DIR}"/noise-*.status >"${TMP_DIR}/noise.status"
noise_attempts="$(wc -l <"${TMP_DIR}/noise.status" | tr -d ' ')"
noise_non_429="$(grep -Evc '^429$' "${TMP_DIR}/noise.status" || true)"
[[ "${noise_attempts}" -ge "${NOISE_WORKERS}" ]]
[[ "${noise_non_429}" == "0" ]]

baseline_p99="$(percentile "${TMP_DIR}/baseline.latency" 99)"
noisy_p50="$(percentile "${TMP_DIR}/noisy.latency" 50)"
noisy_p99="$(percentile "${TMP_DIR}/noisy.latency" 99)"
latency_limit="$(awk -v baseline="${baseline_p99}" \
  'BEGIN { limit = baseline * 10 + 0.25; if (limit > 2) limit = 2; printf "%.6f", limit }')"
awk -v observed="${noisy_p99}" -v limit="${latency_limit}" \
  'BEGIN { if (observed > limit) exit 1 }'

body="$(curl --fail --silent "${STREAMS_URL}/v1/stream/victim" "${auth_b[@]}")"
expected=""
for _ in $(seq 1 $(( BASELINE_WRITES + NOISY_WRITES ))); do
  expected+="b"
done
[[ "${body}" == "${expected}" ]]

printf 'noisy-neighbor isolation passed: attacker_429=%s victim_ok=%s baseline_p99_s=%s noisy_p50_s=%s noisy_p99_s=%s limit_s=%s\n' \
  "${noise_attempts}" "$(( BASELINE_WRITES + NOISY_WRITES ))" \
  "${baseline_p99}" "${noisy_p50}" "${noisy_p99}" "${latency_limit}"
