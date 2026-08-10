#!/bin/bash
# R23-1 gate: the hard backlog bound must convert unbounded growth into
# bounded, retryable refusal — WITHOUT making the overload unrecoverable.
#
# Four properties, in the order they matter:
#   1. appends are refused with a RETRYABLE 503 once the bound is passed
#   2. the CONTROL PLANE stays admitted (create/delete/seal) — otherwise
#      an operator cannot repair the very condition that is shedding
#   3. READS and CONSUMERS stay admitted — otherwise the drain stops and
#      the backlog can never fall
#   4. admission REOPENS once the backlog drains (hysteresis, no flap)
#
#   backpressure-gate.sh <base-url> <bearer> <key-b64>
set -uo pipefail
U=${1:?base url}
TOK=${2:?bearer}
KEY=${3:?stream key}
A="Authorization: Bearer $TOK"
K="Prisma-Encryption-Key: $KEY"
S=bp-$$
PASS=0; FAIL=0

code() { curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$@" 2>/dev/null; }
say()  { if [ "$1" = ok ]; then PASS=$((PASS+1)); printf 'ok    %s\n' "$2";
         else FAIL=$((FAIL+1)); printf 'FAIL  %s\n' "$2"; fi }

bp() { curl -s --max-time 15 -H "$A" "$U/v1/debug/load" 2>/dev/null \
  | python3 -c 'import json,sys
try: d=json.load(sys.stdin).get("maintenance_backpressure",{})
except Exception: d={}
print("%s %s %s" % (d.get("engaged"), d.get("appends_shed",0), d.get("unabsorbed_bytes",0)))' 2>/dev/null || echo "? 0 0"; }

echo "baseline: engaged=$(bp)"
c=$(code -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
      -d '{"format":{"kind":"bytes"}}' "$U/v1/streams/$S")
[ "$c" = 201 ] || { echo "ABORT: create -> $c"; exit 2; }

# Build backlog with the absorber paused: ingest keeps counting while
# absorbed does not move, which is exactly the CHAOS-5 shape.
curl -s --max-time 15 -o /dev/null -X POST -H "$A" "$U/v1/debug/absorb-pause?on=1"
echo "absorber paused; driving appends until the bound engages"
PAYLOAD=$(printf 'x%.0s' $(seq 1 65536))
engaged=no
for i in $(seq 1 400); do
  c=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -X POST -H "$A" -H "$K" \
        --data-binary "$PAYLOAD" "$U/v1/streams/$S/records")
  if [ "$c" = 503 ]; then engaged=yes; break; fi
  [ $((i % 25)) -eq 0 ] && echo "  ...$i appends, bp=$(bp)"
done

if [ "$engaged" = yes ]; then
  say ok "appends refused once the backlog bound was passed (503)"
else
  say fail "never engaged after 400 appends (bp=$(bp))"
fi

# The refusal must be RETRYABLE and name itself.
body=$(curl -s --max-time 20 -X POST -H "$A" -H "$K" --data-binary 'x' \
        "$U/v1/streams/$S/records" 2>/dev/null)
case "$body" in
  *maintenance_backpressure*) say ok "refusal is typed maintenance_backpressure" ;;
  *) say fail "refusal body was: ${body:0:120}" ;;
esac
case "$body" in
  *'"retryable":true'*) say ok "refusal is marked retryable" ;;
  *) say fail "refusal not marked retryable: ${body:0:120}" ;;
esac

# THE property that keeps an overload recoverable.
c=$(code -H "$A" -H "$K" "$U/v1/streams/$S/records"); [ "${c:0:1}" = 2 ] \
  && say ok "reads still admitted while shedding ($c)" \
  || say fail "reads refused while shedding ($c)"
c=$(code -X PUT -H "$A" -H "$K" -H 'content-type: application/json' \
      -d '{"format":{"kind":"bytes"}}' "$U/v1/streams/$S-cp")
case "$c" in 200|201) say ok "control plane: create still admitted ($c)" ;;
  *) say fail "control plane: create refused ($c)" ;; esac
c=$(code -X DELETE -H "$A" -H "$K" "$U/v1/streams/$S-cp")
case "$c" in 200|202|204) say ok "control plane: delete still admitted ($c)" ;;
  *) say fail "control plane: delete refused ($c)" ;; esac
c=$(code -X POST -H "$A" -H "$K" "$U/v1/streams/$S:seal")
case "$c" in 200|409) say ok "control plane: seal still admitted ($c)" ;;
  *) say fail "control plane: seal refused ($c)" ;; esac

# Release: resume absorption and wait for the low watermark.
echo "resuming absorber; waiting for release"
curl -s --max-time 15 -o /dev/null -X POST -H "$A" "$U/v1/debug/absorb-pause?on=0"
released=no
for i in $(seq 1 60); do
  set -- $(bp)
  if [ "$1" = "False" ] || [ "$1" = "false" ]; then released=yes; break; fi
  sleep 5
done
[ "$released" = yes ] \
  && say ok "admission reopened after the backlog drained" \
  || say fail "still engaged after 5 min of draining (bp=$(bp))"

curl -s --max-time 20 -o /dev/null -X DELETE -H "$A" -H "$K" "$U/v1/streams/$S" 2>/dev/null
echo
echo "backpressure-gate: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
