#!/bin/bash
# OOM acceptance-campaign driver (one driver, seven legs — the release
# gate before preview.7-class posture is restored; see
# docs/OBSERVABILITY-BILLING-STATUS.md "Acceptance gate").
#
#   oom-acceptance.sh verify <server-url> <auth-token-file>
#   oom-acceptance.sh leg    <name> <server-url> <gen-url> <auth-file> \
#                            <target_ok> <max_minutes> [stall_ms]
#
# `verify` MUST pass before any leg starts load: it reads the live
# budget introspection (GET /v1/debug/absorb) and FAILS unless the
# server's effective posture matches deploy/profiles/compute-1g.env —
# a leg run against the wrong posture proves nothing and wastes hours.
#
# `leg` samples every 5 s until the accepted target, death, or the time
# cap, applying the per-run acceptance checks from the STATUS doc:
# process alive the whole way (health + origin marker), no monotonic
# RSS growth after warm-up, absorber reservation invariant
# (reserved <= capacity ALWAYS), and — for stall legs — flush-wait
# metrics actually reflecting the injected delay. Samples are kept for
# every leg; a FAILED leg additionally leaves the namespace intact for
# forensics (teardown is the operator's explicit step).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROFILE="$ROOT/deploy/profiles/compute-1g.env"

pval() { awk -F= -v k="$1" '$1==k {print $2}' "$PROFILE"; }

case "${1:-}" in
verify)
  URL=$2; AUTH=$(cat "$3")
  J=$(curl -sf --max-time 15 -H "Authorization: Bearer $AUTH" "$URL/v1/debug/absorb")
  python3 - "$J" "$(pval ABSORB_GLOBAL_BUDGET_BYTES)" "$(pval ABSORB_GLOBAL_GATHERS)" \
      "$(pval TELEMETRY_CACHE_BYTES)" "$(pval ADMIT_RSS_SHED_MB)" <<'PY'
import json, sys
j = json.loads(sys.argv[1])
want_budget, want_slots, want_tcache, want_shed = (int(x) for x in sys.argv[2:6])
b = j["budget"]
errs = []
# The binary may FLOOR the configured budget upward; verify takes the
# floored truth from the server and requires it >= the profile value.
if b["capacityBytes"] < want_budget:
    errs.append(f"budget capacity {b['capacityBytes']} < profile {want_budget}")
if b["gatherSlots"] != want_slots:
    errs.append(f"gather slots {b['gatherSlots']} != profile {want_slots}")
if j["telemetry"]["cacheCapacityBytes"] != want_tcache:
    errs.append(f"telemetry cache {j['telemetry']['cacheCapacityBytes']} != {want_tcache}")
if b["shedLineMb"] != want_shed:
    errs.append(f"shed line {b['shedLineMb']} != profile {want_shed}")
if errs:
    print("VERIFY_FAIL: " + "; ".join(errs)); sys.exit(1)
print(f"VERIFY_OK budget={b['capacityBytes']} slots={b['gatherSlots']} "
      f"effective={b['effectiveGatherConcurrency']} shed={b['shedLineMb']}MB")
PY
  ;;
leg)
  NAME=$2; URL=$3; GENURL=$4; AUTH=$(cat "$5")
  TARGET=${6:?target_ok}; MAXMIN=${7:?max_minutes}; STALL=${8:-0}
  OUT="${OOM_RESULTS_DIR:-/tmp}/oom-leg-$NAME.jsonl"; : > "$OUT"
  "$0" verify "$URL" "$5"
  if [ "$STALL" != "0" ]; then
    curl -sf --max-time 10 -X POST -H "Authorization: Bearer $AUTH" \
      "$URL/v1/debug/history-stall?ms=$STALL" >/dev/null
    echo "stalled-history-flush leg: injected ${STALL}ms per flush"
  fi
  DEAD=0; FAILS=""
  for i in $(seq 1 $(( MAXMIN * 12 ))); do
    ts=$(date +%s)
    g=$(curl -s --max-time 10 "$GENURL/" || echo '{}')
    ab=$(curl -s --max-time 10 -H "Authorization: Bearer $AUTH" "$URL/v1/debug/absorb" || echo '{}')
    ld=$(curl -s --max-time 10 -H "Authorization: Bearer $AUTH" "$URL/v1/debug/load" || echo '{}')
    hc=$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' "$URL/health" || echo 0)
    origin=$(curl -s --max-time 10 -D- -o /dev/null "$URL/health" | grep -ci 'prisma-streams-origin' || true)
    printf '{"ts":%s,"i":%s,"health":%s,"origin":%s,"gen":%s,"absorb":%s,"load":%s}\n' \
      "$ts" "$i" "$hc" "$origin" "$g" "$ab" "$ld" >> "$OUT"
    VERDICT=$(python3 - "$OUT" "$TARGET" "$STALL" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
target, stall = int(sys.argv[2]), int(sys.argv[3])
r = rows[-1]
ok = (r.get("gen") or {}).get("ok") or 0
# Invariant: declared reservation never exceeds capacity.
b = (r.get("absorb") or {}).get("budget") or {}
res = ((r.get("absorb") or {}).get("absorber") or {}).get("reservedBytes")
if b and res is not None and res > b.get("capacityBytes", 1 << 62):
    print("FAIL_INVARIANT reserved>capacity"); sys.exit()
# Stall legs: the injected delay must be visible in flush timing.
if stall and len(rows) > 24:
    fl = [((x.get("absorb") or {}).get("absorber") or {}).get("lastFlushMs") or 0 for x in rows[-24:]]
    if max(fl) < stall:
        print("FAIL_METRIC stall invisible in lastFlushMs"); sys.exit()
dead = sum(1 for x in rows[-6:] if x["health"] != 200 and not x.get("origin"))
if dead >= 6:
    print(f"FAIL_DEAD ok={ok}"); sys.exit()
if ok >= target:
    # No-monotonic-RSS check over the settled second half.
    rss = [x.get("load", {}).get("rss_mb") for x in rows if x.get("load", {}).get("rss_mb")]
    if len(rss) > 40:
        half = rss[len(rss)//2:]
        q = len(half)//4
        if q and (sum(half[-q:])/q) > (sum(half[:q])/q) * 1.25:
            print(f"FAIL_RSS_TREND ok={ok}"); sys.exit()
    print(f"PASS ok={ok}"); sys.exit()
print("CONTINUE")
PY
)
    case "$VERDICT" in
      CONTINUE) ;;
      PASS*) echo "LEG_PASS $NAME: $VERDICT (samples: $OUT)"; exit 0 ;;
      *) echo "LEG_FAIL $NAME: $VERDICT — namespace and samples PRESERVED ($OUT)"; exit 1 ;;
    esac
    sleep 5
  done
  echo "LEG_FAIL $NAME: TIMECAP before target — samples preserved ($OUT)"; exit 1
  ;;
*)
  echo "usage: $0 verify <url> <auth-file> | leg <name> <url> <gen-url> <auth-file> <target> <max-min> [stall_ms]" >&2
  exit 1
  ;;
esac
