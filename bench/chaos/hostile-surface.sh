#!/bin/bash
# Hostile-surface gate: every request below is one a hostile or merely
# confused client can send. The gate asserts the EXACT status class we
# promise for each, because the failure modes here are silent ones —
# a 5xx leaks an internal error, and a 200 where we meant to refuse
# means the server acted on input it did not understand.
#
# Grew out of the 2026-08-09 chaos campaign, which found CHAOS-4 this
# way: `maxBytes=-5` answered 200 and returned the 8 MiB default while
# `deliver=bogus` next to it answered 400.
#
#   hostile-surface.sh <base-url> <bearer> <key-b64>
#
# Exits non-zero on the first mismatch; prints one line per check.
set -uo pipefail
U=${1:?base url}
TOK=${2:?bearer}
KEY=${3:?stream key (base64)}
A="Authorization: Bearer $TOK"
K="Prisma-Encryption-Key: $KEY"
K2='Prisma-Encryption-Key: AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIA='
S=chaos-hostile-$$
PASS=0; FAIL=0

# want: an exact code (404) or a class (4xx, 2xx). Never accepts 5xx.
chk() {
  local want="$1" label="$2"; shift 2
  local code
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$@" 2>/dev/null)
  local rc=$?
  if [ $rc -ne 0 ]; then
    printf 'FAIL  %-34s curl error %d (hang or reset)\n' "$label" "$rc"; FAIL=$((FAIL+1)); return
  fi
  local ok=0
  case "$want" in
    [0-9][0-9][0-9]) [ "$code" = "$want" ] && ok=1 ;;
    4xx) case "$code" in 4??) ok=1 ;; esac ;;
    2xx) case "$code" in 2??) ok=1 ;; esac ;;
  esac
  if [ $ok = 1 ]; then
    printf 'ok    %-34s %s\n' "$label" "$code"; PASS=$((PASS+1))
  else
    printf 'FAIL  %-34s got %s want %s\n' "$label" "$code" "$want"; FAIL=$((FAIL+1))
  fi
}

# Setup is a PRECONDITION, not a check. Against a server under load it
# can legitimately answer 429, and treating that as a failure is how one
# transient shed turned into 19 bogus "failures" on the first Singapore
# run — every later check hit 404 on a stream that was never created.
# Retry the retryable codes, then refuse to report anything at all.
setup() {
  local label="$1" want="$2"; shift 2
  local code i
  for i in 1 2 3 4 5 6 7 8 9 10; do
    code=$(curl -s --max-time 30 -o /dev/null -w '%{http_code}' "$@" 2>/dev/null)
    case "$code" in
      "$want") printf 'ok    %-34s %s\n' "$label" "$code"; return 0 ;;
      429|503|502|504) sleep $(( i < 5 ? i : 5 )) ;;
      *) break ;;
    esac
  done
  printf 'ABORT %-34s got %s want %s after retries\n' "$label" "$code" "$want"
  echo
  echo "hostile-surface: SETUP FAILED — no checks were run. The target must" >&2
  echo "be reachable and admitting writes before the battery means anything." >&2
  exit 2
}

setup "setup: create" 201 -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
    -d '{"format":{"kind":"bytes"}}' "$U/v1/streams/$S"
setup "setup: append" 200 -X POST -H "$A" -H "$K" --data-binary 'r0' "$U/v1/streams/$S/records"

echo "-- auth: a rejected credential must never reach the data plane"
chk 401 "no bearer"          -X POST -H "$K" --data-binary 'x' "$U/v1/streams/$S/records"
chk 401 "wrong bearer"       -X POST -H 'Authorization: Bearer nope' -H "$K" --data-binary 'x' "$U/v1/streams/$S/records"
chk 401 "bearer without scheme" -X POST -H "Authorization: $TOK" -H "$K" --data-binary 'x' "$U/v1/streams/$S/records"
chk 403 "wrong stream key"   -X POST -H "$A" -H "$K2" --data-binary 'x' "$U/v1/streams/$S/records"
chk 400 "absent stream key"  -X POST -H "$A" --data-binary 'x' "$U/v1/streams/$S/records"
chk 403 "key not base64"     -X POST -H "$A" -H 'Prisma-Encryption-Key: !!!notbase64!!!' --data-binary 'x' "$U/v1/streams/$S/records"
chk 403 "key too short"      -X POST -H "$A" -H 'Prisma-Encryption-Key: AAAA' --data-binary 'x' "$U/v1/streams/$S/records"
chk 403 "read with wrong key" -H "$A" -H "$K2" "$U/v1/streams/$S/records"

echo "-- route grammar: nothing here may reach a handler as a real name"
chk 4xx "parent traversal"   -H "$A" -H "$K" "$U/v1/streams/../../etc/passwd/records"
chk 400 "encoded traversal"  -H "$A" -H "$K" "$U/v1/streams/%2e%2e%2f%2e%2e%2fetc/records"
chk 400 "NUL in name"        -H "$A" -H "$K" "$U/v1/streams/vic%00tim/records"
chk 400 "empty name"         -H "$A" -H "$K" "$U/v1/streams//records"
chk 400 "newline in name"    -H "$A" -H "$K" "$U/v1/streams/vic%0atim/records"
chk 400 "4000-byte name"     -H "$A" -H "$K" "$U/v1/streams/$(printf 'a%.0s' $(seq 1 4000))/records"
chk 404 "unknown subresource" -H "$A" -H "$K" "$U/v1/streams/$S/wat"
chk 404 "extra path segments" -H "$A" -H "$K" "$U/v1/streams/a/b/c/d/records"

echo "-- reserved namespace: system ledgers are not customer streams"
for r in _usage _ops_metrics; do
  chk 403 "create $r"  -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
      -d '{"format":{"kind":"bytes"}}' "$U/v1/streams/$r"
  chk 403 "append $r"  -X POST -H "$A" -H "$K" --data-binary 'x' "$U/v1/streams/$r/records"
  chk 403 "read $r"    -H "$A" -H "$K" "$U/v1/streams/$r/records"
  chk 403 "delete $r"  -X DELETE -H "$A" -H "$K" "$U/v1/streams/$r"
done

echo "-- query values: understood or refused, never silently defaulted"
chk 400 "deliver=bogus"      -H "$A" -H "$K" "$U/v1/streams/$S/records?deliver=bogus"
chk 400 "maxBytes=abc"       -H "$A" -H "$K" "$U/v1/streams/$S/records?maxBytes=abc"
chk 400 "maxBytes=-5"        -H "$A" -H "$K" "$U/v1/streams/$S/records?maxBytes=-5"
chk 400 "waitMs=abc"         -H "$A" -H "$K" "$U/v1/streams/$S/records?waitMs=abc"
chk 400 "waitMs=-1"          -H "$A" -H "$K" "$U/v1/streams/$S/records?waitMs=-1"
chk 400 "routingKey > 1KiB"  -H "$A" -H "$K" "$U/v1/streams/$S/records?routingKey=$(printf 'k%.0s' $(seq 1 1025))"
chk 200 "maxBytes below floor clamps up" -H "$A" -H "$K" "$U/v1/streams/$S/records?maxBytes=1"
chk 400 "cursor: garbage"    -H "$A" -H "$K" "$U/v1/streams/$S/records?cursor=AAAAAAAAAAAAAAAA"
chk 400 "cursor: not base64" -H "$A" -H "$K" "$U/v1/streams/$S/records?cursor=%%%%"

echo "-- R23-4: the SAME bug class on every other public route"
# The first fix only covered the records read handler; scan, watch and
# catalog kept collapsing a malformed value into the route default.
chk 400 "scan maxBytes=abc"     -H "$A" -H "$K" "$U/v1/streams/$S:scan?maxBytes=abc"
chk 400 "scan maxBytes=-5"      -H "$A" -H "$K" "$U/v1/streams/$S:scan?maxBytes=-5"
chk 400 "catalog limit=abc"     -H "$A" "$U/v1/streams?limit=abc"
chk 400 "catalog limit=-1"      -H "$A" "$U/v1/streams?limit=-1"
chk 400 "catalog unknown key"   -H "$A" "$U/v1/streams?nosuchparam=1"
chk 400 "catalog duplicate key" -H "$A" "$U/v1/streams?limit=10&limit=20"
chk 2xx "catalog limit=10"      -H "$A" "$U/v1/streams?limit=10"

echo "-- R23-4: an oversized body is REFUSED, not reset"
# A body far over the ceiling used to draw a connection reset, so a
# client could not tell refusal from a broken network and retried
# forever. curl exit 0 with a 413 is the whole point of this check.
chk 413 "declared oversized body" -X POST -H "$A" -H "$K" \
    -H "content-length: 67108864" --data-binary "@/dev/null" \
    "$U/v1/streams/$S/records"

echo "-- bodies and methods"
chk 405 "TRACE"              -X TRACE -H "$A" "$U/v1/streams/$S/records"
chk 405 "PATCH records"      -X PATCH -H "$A" -H "$K" --data-binary 'x' "$U/v1/streams/$S/records"
chk 400 "create: bad JSON"   -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
    -d '{"format":' "$U/v1/streams/$S-bad"
chk 400 "create: unknown field" -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
    -d '{"format":{"kind":"bytes"},"evil":1}' "$U/v1/streams/$S-bad2"
chk 400 "create: no format"  -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
    -d '{}' "$U/v1/streams/$S-bad3"

echo "-- liveness after the whole battery"
chk 200 "health"             "$U/health"
chk 200 "read the victim"    -H "$A" -H "$K" "$U/v1/streams/$S/records"

curl -s --max-time 20 -o /dev/null -X DELETE -H "$A" -H "$K" "$U/v1/streams/$S" 2>/dev/null

echo
echo "hostile-surface: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
