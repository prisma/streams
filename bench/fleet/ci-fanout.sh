#!/bin/bash
# BOUNDED cross-owner CI gate (round-19 next-arc item 1). The fan-out
# proof previously lived only in a hand-driven campaign; this runs the
# same two-process rig unattended in a few minutes and fails loudly.
#
#   bench/fleet/ci-fanout.sh [out-dir]
#
# Covers, in one pass:
#   cross-owner keyed read / scan / pull / settle / consumer deletion
#   internal-API authorization (customer bearer refused, no-auth refused,
#     fleet token refused on the product surface)
#   incarnation binding (a stale target is refused, not rebound)
#   hierarchical + percent-encoded stream names through the router
#   a dead upstream yields retryable 503s, never semantic 404s
set -euo pipefail
OUT=${1:-/tmp/ci-fanout}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
AUTH=localsoak
FLEET_TOKEN=local-fleet-internal-token-0001
KEY=BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=
LB=http://127.0.0.1:8090
S1=http://127.0.0.1:8091
S2=http://127.0.0.1:8092
fail() { echo "[ci-fanout] FAIL: $*" >&2; exit 1; }
pass() { echo "[ci-fanout] ok: $*"; }

"$HERE/local-fanout.sh" "$OUT" hold > "$OUT.boot.log" 2>&1 &
RIG=$!
trap 'kill $RIG 2>/dev/null || true; pkill -f "target/release/streams-slate --listen 127.0.0.1:809" 2>/dev/null || true; pkill -f "target/release/pilot" 2>/dev/null || true; pkill -f "target/release/s3lite" 2>/dev/null || true' EXIT
for _ in $(seq 1 180); do grep -q holding "$OUT.boot.log" 2>/dev/null && break; sleep 1; done
grep -q holding "$OUT.boot.log" || fail "rig never came up (see $OUT.boot.log)"

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

# ---- security boundary -------------------------------------------------
[ "$(code -H "authorization: Bearer $AUTH" "$S1/v1/internal/queue-cursor/x")" = 401 ] \
  || fail "customer bearer reached an internal route"
[ "$(code -X POST "$S1/v1/internal/sweep-segment/x")" = 401 ] \
  || fail "unauthenticated caller reached an internal route"
[ "$(code -H "authorization: Bearer $FLEET_TOKEN" "$S1/v1/streams")" = 401 ] \
  || fail "the fleet-internal token authorized a product operation"
[ "$(code -X POST "$S1/v1/debug/absorb-pause?on=1")" = 401 ] \
  || fail "a mutating debug route answered unauthenticated"
pass "internal API is a separate, closed trust boundary"

# ---- origin marker -----------------------------------------------------
curl -s -D- -o /dev/null "$S1/health" | grep -qi "prisma-streams-origin" \
  || fail "responses are missing the origin marker"
pass "every response carries Prisma-Streams-Origin"

# ---- functional cross-owner battery ------------------------------------
AUTH_TOKEN=$AUTH STREAM_KEY=$KEY LB=$LB A=$S1 B=$S2 \
  python3 "$HERE/fanout-probe.py" || fail "cross-owner battery"
pass "cross-owner reads/scan/pull/settle/deletion"

# ---- hierarchical + encoded names --------------------------------------
H=(-H "authorization: Bearer $AUTH" -H "prisma-encryption-key: $KEY" -H "content-type: application/json")
NAME='customers/acme%20corp/orders'
[ "$(code -X PUT "${H[@]}" -d '{"format":{"kind":"json"}}' "$LB/v1/streams/$NAME")" = 201 ] \
  || fail "hierarchical/encoded name create"
[ "$(code -X POST "${H[@]}" -H 'prisma-routing-key: k' -d '{"n":1}' "$LB/v1/streams/$NAME/records")" = 200 ] \
  || fail "hierarchical/encoded name append"
[ "$(code "${H[@]}" "$LB/v1/streams/$NAME/records?routingKey=k")" = 200 ] \
  || fail "hierarchical/encoded name read"
pass "hierarchical + percent-encoded names route intact"

# ---- dead upstream: retryable 503, never semantic 404 ------------------
for i in 0 1 2 3 4 5 6 7; do
  curl -s -o /dev/null -X PUT "${H[@]}" -d '{"format":{"kind":"json"}}' "$LB/v1/streams/dead/s$i"
done
pkill -f "target/release/streams-slate --listen 127.0.0.1:8092" || true
sleep 1
BAD=0; RETRYABLE=0
for i in 0 1 2 3 4 5 6 7; do
  for _ in 1 2 3; do
    C=$(code -X POST "${H[@]}" -H 'prisma-routing-key: k' -d '{"n":1}' "$LB/v1/streams/dead/s$i/records")
    case "$C" in
      404|502) BAD=$((BAD+1));;
      503) RETRYABLE=$((RETRYABLE+1));;
    esac
  done
done
[ "$BAD" = 0 ] || fail "$BAD non-retryable responses from an infrastructure failure"
pass "dead upstream produced only retryable responses ($RETRYABLE x 503)"

echo "[ci-fanout] ALL CHECKS PASSED"
