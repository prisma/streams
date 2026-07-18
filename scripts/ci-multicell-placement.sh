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
    --listen "127.0.0.1:${port}" --instance-name streams-1 \
    --s3-endpoint "${S3_URL}" --bucket streams --region auto \
    --access-key-id "${cell}-data" --secret-access-key "${cell}-data-secret" \
    --path-prefix "cells/${cell}" --cell-id "${cell}" \
    --fleet-prefix "cells/${cell}/fleet-coordination" --fleet-max 1 \
    --registry-s3-endpoint "${S3_URL}" --registry-s3-bucket streams \
    --registry-s3-region auto --registry-s3-access-key-id registry-control \
    --registry-s3-secret-access-key registry-control-secret \
    --registry-s3-allow-http --registry-path-prefix global-registry \
    --cell-directory-refresh-secs 5 --initial-shards 1 \
    --absorb-bytes 1 --absorb-age-secs 1 \
    --auth-jwks-url "${S3_URL}/auth/jwks.json" \
    --auth-revocation-url "${S3_URL}/auth/revocations.json" \
    --auth-issuer "${ISSUER}" --auth-audience "${AUDIENCE}" \
    --auth-jwks-refresh-secs 1 --auth-jwks-max-stale-secs 10 \
    --auth-revocation-refresh-secs 1 --auth-revocation-max-stale-secs 10 \
    --backup-s3-endpoint "${S3_URL}" --backup-s3-bucket backup \
    --backup-s3-region auto --backup-s3-access-key-id backup-control \
    --backup-s3-secret-access-key backup-control-secret \
    --backup-path-prefix "recovery/${cell}" --backup-interval-secs 60 \
    --backup-scrub-interval-secs 10 --require-backup \
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

latest_snapshot() {
  curl --fail --silent --show-error \
    "${S3_URL}/backup/recovery/$1/latest.json" |
    sed -n 's/.*"snapshot_id":"\([^"]*\)".*/\1/p'
}

restart_selected_cell() {
  local cell="$1"
  local prior="$2"
  local port log new_pid attempts snapshot
  case "${cell}" in
    c-a)
      kill "${PID_A}" 2>/dev/null || true
      wait "${PID_A}" 2>/dev/null || true
      PID_A=""
      port="${PORT_A}"
      log="${TMP_DIR}/cell-a.log"
      ;;
    c-b)
      kill "${PID_B}" 2>/dev/null || true
      wait "${PID_B}" 2>/dev/null || true
      PID_B=""
      port="${PORT_B}"
      log="${TMP_DIR}/cell-b.log"
      ;;
  esac
  new_pid="$(start_cell "${cell}" "${port}" "${log}")"
  if [[ "${cell}" == "c-a" ]]; then PID_A="${new_pid}"; else PID_B="${new_pid}"; fi
  wait_ready "$(url_for_cell "${cell}")" "${log}"
  attempts=0
  snapshot="$(latest_snapshot "${cell}")"
  while [[ -z "${snapshot}" || "${snapshot}" == "${prior}" ]]; do
    attempts=$((attempts + 1))
    (( attempts <= 200 )) || fail "replacement cell did not publish a new recovery point"
    sleep 0.1
    snapshot="$(latest_snapshot "${cell}")"
  done
}

start_recovered_cell() {
  local cell="$1"
  local port="$2"
  local log="$3"
  RUST_LOG=info "${TARGET_DIR}/streams-slate" \
    --listen "127.0.0.1:${port}" --instance-name "${cell}-recovered" \
    --s3-endpoint "${S3_URL}" --bucket restored-primary --region auto \
    --access-key-id "${cell}-restored" --secret-access-key "${cell}-restored-secret" \
    --path-prefix "cells/${cell}" --cell-id "${cell}" \
    --registry-s3-endpoint "${S3_URL}" --registry-s3-bucket restored-registry \
    --registry-s3-region auto --registry-s3-access-key-id registry-restored \
    --registry-s3-secret-access-key registry-restored-secret \
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
curl --fail --silent --show-error -X PUT "${S3_URL}/backup" >/dev/null
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

# Force the keyed absorber to publish encrypted history before moving. The
# mover must copy that physical database and its writer-verified baselines
# without receiving the customer key.
selected_log="${TMP_DIR}/cell-${selected#c-}.log"
attempts=0
until grep -q 'absorbed .* records into streams/' "${selected_log}"; do
  attempts=$((attempts + 1))
  (( attempts <= 300 )) || fail "selected cell did not create encrypted history before move"
  sleep 0.1
done
sleep 0.5

# Force coordinator takeover so a fresh point necessarily captures the stream
# and its exact cell-local registry closure after the create.
initial_snapshot="$(latest_snapshot "${selected}")"
restart_selected_cell "${selected}" "${initial_snapshot}"
body="$(curl --fail --silent --show-error \
  "${selected_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"cell":true}]' ]] || fail "replacement owner lost the pinned stream"

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

# Move the live stream to the other physical cell. The tool has no stream key:
# it copies the exact raw shard range and checkpointed encrypted history, then
# changes global placement only after a durable source-shard fence exists.
move_source="${selected}"
move_source_url="${selected_url}"
move_target="${other}"
move_target_url="${other_url}"
move_operation='1234567890abcdef1234567890abcdef'
"${TARGET_DIR}/streams-cell-move" \
  --customer-id tenant-a --stream cell-pinned \
  --source-cell "${move_source}" --target-cell "${move_target}" \
  --operation-id "${move_operation}" --allow-http \
  --confirm-target-stream-replaceable \
  --registry-endpoint "${S3_URL}" --registry-bucket streams \
  --registry-region auto --registry-access-key-id registry-control \
  --registry-secret-access-key registry-control-secret \
  --registry-prefix global-registry \
  --source-endpoint "${S3_URL}" --source-bucket streams \
  --source-region auto --source-access-key-id "${move_source}-move" \
  --source-secret-access-key "${move_source}-move-secret" \
  --source-prefix "cells/${move_source}" \
  --source-fleet-prefix "cells/${move_source}/fleet-coordination" \
  --target-endpoint "${S3_URL}" --target-bucket streams \
  --target-region auto --target-access-key-id "${move_target}-move" \
  --target-secret-access-key "${move_target}-move-secret" \
  --target-prefix "cells/${move_target}" \
  --target-fleet-prefix "cells/${move_target}/fleet-coordination" \
  >"${TMP_DIR}/cell-move.json"

# A stale positive descriptor at the former cell may briefly avoid the global
# replay response, but the local shard fence must still make a write impossible.
status="$(curl --silent --show-error -o "${TMP_DIR}/stale-source.body" \
  --write-out '%{http_code}' -X POST \
  "${move_source_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H 'content-type: application/json' \
  -d '[{"must":"not-commit"}]')"
[[ "${status}" != "204" && "${status}" != "200" ]] || fail "former cell accepted a post-fence append"
status="$(curl --silent --show-error -o "${TMP_DIR}/stale-source-delete.body" \
  --write-out '%{http_code}' -X DELETE \
  "${move_source_url}/v1/stream/cell-pinned" "${auth[@]}")"
[[ "${status}" != "204" && "${status}" != "200" ]] || fail "former cell deleted the moved descriptor"

attempts=0
until body="$(curl --fail --silent --show-error \
  "${move_target_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" 2>/dev/null)"; do
  attempts=$((attempts + 1))
  (( attempts <= 120 )) || fail "target cell did not serve the completed move"
  sleep 0.1
done
[[ "${body}" == '[{"cell":true}]' ]] || fail "cell move lost encrypted history"
curl --fail --silent --show-error -X POST \
  "${move_target_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}" -H 'content-type: application/json' \
  -d '[{"after":"move"}]' >/dev/null
body="$(curl --fail --silent --show-error \
  "${move_target_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"cell":true},{"after":"move"}]' ]] || fail "target did not continue the moved offset sequence"

descriptor="$(curl --fail --silent --show-error \
  "${S3_URL}/streams/global-registry/registry/by-customer/${customer_hash}/by-name/${name_hash:0:2}/${name_hash}.json")"
[[ "${descriptor}" == *"\"cell\":\"${move_target}\""* ]] || fail "move did not publish target placement"
[[ "${descriptor}" == *'"state":"completed"'* ]] || fail "move completion was not retained idempotently"
affinity="$(curl --fail --silent --show-error \
  "${S3_URL}/streams/global-registry/customers/${customer_hash}/cell-affinity.json")"
[[ "${affinity}" == *'"c-a"'* && "${affinity}" == *'"c-b"'* ]] || fail "move did not durably expand customer affinity"

# The lost-response retry must resolve the retained completion without
# clearing or recopying the now-authoritative target.
retry_report="$("${TARGET_DIR}/streams-cell-move" \
  --customer-id tenant-a --stream cell-pinned \
  --source-cell "${move_source}" --target-cell "${move_target}" \
  --operation-id "${move_operation}" --allow-http \
  --confirm-target-stream-replaceable \
  --registry-endpoint "${S3_URL}" --registry-bucket streams \
  --registry-region auto --registry-access-key-id registry-control \
  --registry-secret-access-key registry-control-secret \
  --registry-prefix global-registry \
  --source-endpoint "${S3_URL}" --source-bucket streams \
  --source-region auto --source-access-key-id "${move_source}-move" \
  --source-secret-access-key "${move_source}-move-secret" \
  --source-prefix "cells/${move_source}" \
  --source-fleet-prefix "cells/${move_source}/fleet-coordination" \
  --target-endpoint "${S3_URL}" --target-bucket streams \
  --target-region auto --target-access-key-id "${move_target}-move" \
  --target-secret-access-key "${move_target}-move-secret" \
  --target-prefix "cells/${move_target}" \
  --target-fleet-prefix "cells/${move_target}/fleet-coordination")"
[[ "${retry_report}" == *'"already_completed": true'* ]] || fail "move retry did not resolve completion"

selected="${move_target}"
selected_url="${move_target_url}"
other="${move_source}"
other_url="${move_source_url}"
initial_snapshot="$(latest_snapshot "${selected}")"
restart_selected_cell "${selected}" "${initial_snapshot}"
body="$(curl --fail --silent --show-error \
  "${selected_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"cell":true},{"after":"move"}]' ]] || fail "moved target restart lost data"

# Destroy both serving cells and the live global registry from the serving
# path, restore the selected point into empty primary + registry targets, and
# prove the first decrypted read. The recovery point must not need a global
# descriptor scan or another cell's data.
kill "${PID_A}" "${PID_B}" 2>/dev/null || true
wait "${PID_A}" 2>/dev/null || true
wait "${PID_B}" 2>/dev/null || true
PID_A=""
PID_B=""
curl --fail --silent --show-error -X PUT "${S3_URL}/restored-primary" >/dev/null
curl --fail --silent --show-error -X PUT "${S3_URL}/restored-registry" >/dev/null
"${TARGET_DIR}/streams-registry-restore" \
  --snapshot-id latest --backup-endpoint "${S3_URL}" --backup-bucket backup \
  --backup-region auto --backup-access-key-id backup-control \
  --backup-secret-access-key backup-control-secret \
  --backup-prefix "recovery/${selected}" \
  --target-endpoint "${S3_URL}" --target-bucket restored-registry \
  --target-region auto --target-access-key-id registry-restored \
  --target-secret-access-key registry-restored-secret \
  --target-prefix global-registry --allow-http \
  --confirm-registry-offline >"${TMP_DIR}/registry-restore.json"
"${TARGET_DIR}/streams-restore" \
  --snapshot-id latest --backup-endpoint "${S3_URL}" --backup-bucket backup \
  --backup-region auto --backup-access-key-id backup-control \
  --backup-secret-access-key backup-control-secret \
  --backup-prefix "recovery/${selected}" \
  --target-endpoint "${S3_URL}" --target-bucket restored-primary \
  --target-region auto --target-access-key-id restored-data \
  --target-secret-access-key restored-data-secret \
  --target-prefix "cells/${selected}" \
  --skip-registry \
  --confirm-offline-empty-targets >"${TMP_DIR}/restore.json"
recovered_log="${TMP_DIR}/cell-recovered.log"
recovered_pid="$(start_recovered_cell "${selected}" "${PORT_A}" "${recovered_log}")"
PID_A="${recovered_pid}"
selected_url="${URL_A}"
wait_ready "${selected_url}" "${recovered_log}"
body="$(curl --fail --silent --show-error \
  "${selected_url}/v1/stream/cell-pinned" "${auth[@]}" \
  -H "stream-encryption-key: ${KEY}")"
[[ "${body}" == '[{"cell":true},{"after":"move"}]' ]] || fail "managed registry recovery lost the moved stream"

echo "multi-cell placement, fenced move, registry recovery, replay, and LKG drill passed"
