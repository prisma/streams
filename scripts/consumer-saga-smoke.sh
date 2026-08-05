#!/bin/bash
# Consumer-deletion saga wire smoke (round 17): the versioned-DELETE
# contract against a LIVE server. Usage:
#   STREAMS_TOKEN=<bearer> scripts/consumer-saga-smoke.sh <base-url>
# Exercises: version required; delete; tombstone-idempotent retry;
# recreation mints a new incarnation; a STALE retry is 204 and leaves
# the replacement untouched.
set -euo pipefail
BASE=${1:?base url}
TOKEN=${STREAMS_TOKEN:?bearer token}
KEY=${STREAMS_KEY:-BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=}
S="saga17-$(date +%s)-$$"
H=(-H "authorization: Bearer $TOKEN" -H "prisma-encryption-key: $KEY")
JH=("${H[@]}" -H 'content-type: application/json')
say() { echo "[saga-smoke] $*"; }
fail() { echo "[saga-smoke] FAIL: $*" >&2; exit 1; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

st=$(code -X PUT "${JH[@]}" -d '{"format":{"kind":"json"}}' "$BASE/v1/streams/$S")
[ "$st" = 201 ] || fail "create stream: $st"
for i in 0 1; do
  st=$(code -X POST "${JH[@]}" -H "prisma-routing-key: k$i" -d "{\"n\":$i}" "$BASE/v1/streams/$S/records")
  [ "$st" = 200 ] || fail "append $i: $st"
done
V1=$(curl -s -D- -o /dev/null -X PUT "${JH[@]}" -d '{"visibilityTimeoutMs":30000}' \
  "$BASE/v1/streams/$S/consumers/c1" | tr -d '\r' | awk -F': ' 'tolower($1)=="prisma-consumer-version"{print $2}')
[ -n "$V1" ] || fail "create consumer returned no version"
st=$(code -X POST "${JH[@]}" -d '{"max":2}' "$BASE/v1/streams/$S/consumers/c1:pull")
[ "$st" = 200 ] || fail "pull: $st"

st=$(code -X DELETE "${H[@]}" "$BASE/v1/streams/$S/consumers/c1")
[ "$st" = 400 ] || fail "unversioned DELETE must 400, got $st"
st=$(code -X DELETE "${H[@]}" -H "prisma-consumer-version: $V1" "$BASE/v1/streams/$S/consumers/c1")
[ "$st" = 204 ] || fail "versioned DELETE: $st"
st=$(code "${H[@]}" "$BASE/v1/streams/$S/consumers/c1")
[ "$st" = 404 ] || fail "GET after delete: $st"
st=$(code -X DELETE "${H[@]}" -H "prisma-consumer-version: $V1" "$BASE/v1/streams/$S/consumers/c1")
[ "$st" = 204 ] || fail "tombstone retry: $st"

V2=$(curl -s -D- -o /dev/null -X PUT "${JH[@]}" -d '{"visibilityTimeoutMs":30000}' \
  "$BASE/v1/streams/$S/consumers/c1" | tr -d '\r' | awk -F': ' 'tolower($1)=="prisma-consumer-version"{print $2}')
[ -n "$V2" ] || fail "recreate returned no version"
[ "$V1" != "$V2" ] || fail "recreation kept the old incarnation token"
st=$(code -X POST "${JH[@]}" -d '{"max":2}' "$BASE/v1/streams/$S/consumers/c1:pull")
[ "$st" = 200 ] || fail "recreated pull: $st"

# The ABA probe: a stale retry with V1 is idempotent success and the
# replacement stays Active with its own incarnation.
st=$(code -X DELETE "${H[@]}" -H "prisma-consumer-version: $V1" "$BASE/v1/streams/$S/consumers/c1")
[ "$st" = 204 ] || fail "stale retry: $st"
NOW=$(curl -s -D- -o /dev/null "${H[@]}" "$BASE/v1/streams/$S/consumers/c1" | tr -d '\r' | awk -F': ' 'tolower($1)=="prisma-consumer-version"{print $2}')
[ "$NOW" = "$V2" ] || fail "stale retry touched the replacement (version $NOW != $V2)"

st=$(code -X DELETE "${H[@]}" -H "prisma-consumer-version: $V2" "$BASE/v1/streams/$S/consumers/c1")
[ "$st" = 204 ] || fail "final delete: $st"
st=$(code -X DELETE "${H[@]}" "$BASE/v1/streams/$S")
case "$st" in 200|202|204) ;; *) fail "stream cleanup: $st";; esac
say "PASS ($S)"
