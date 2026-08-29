#!/bin/bash
# Campaign 2 (round 12, restructured per review): TWO INDEPENDENT AXES.
# The first ladder conflated them — the all-solo rungs received ~6x the
# write rate of the fanout rungs (appendRate = delivery_target/SUBS_PER),
# so "breadth is the ceiling" was confounded with "breadth got more
# writes".
#
#   AXIS=residency (default)
#     Fixed write workload at EVERY geometry: 100 subscribed writes/s
#     + 200 background writes/s = 300 writes/s total, appends confined
#     to 100 streams. Delivery rate then varies with fanout — at the
#     product geometry 100x100 this IS the product-throughput receipt
#     (100 w/s x 100 subscribers = 10,000 deliveries/s + 300 w/s).
#     Use THIS axis for feed/connection memory capacity claims.
#
#   AXIS=delivery
#     Fixed delivery load at the product geometry (100x100), ladder
#     1k -> 2.5k -> 5k -> 10k deliveries/s (SUBSCRIBED_WPS = target/100).
#     Use for delivery-throughput ceilings ONLY, never feed-memory.
#
#   RESULTS=<dir> [AXIS=residency|delivery] run-capacity.sh [rungs...]
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
RESULTS=${RESULTS:?set RESULTS dir}
mkdir -p "$RESULTS"
AXIS=${AXIS:-residency}
RUNGS=("$@")
export WARMUP_SECS=30 IDLE_SECS=600 SPARSE_SECS=60 FANOUT_SECS=180 MIXED_SECS=180 \
       SLOW_SECS=60 TEARDOWN_SECS=300 WRITE_BREADTH=100 MIXED_BG_RATE=200

gate() { # manifest -> PASS/FAIL per the capacity gates
  python3 - "$1" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
ph = {p["name"]: p for p in m.get("phases", [])}
fails = []
if m.get("verdict") != "PASS":
    fails.append("reconciliation")
mx = ph.get("mixed", {}).get("client", {})
# Review blocker 4: the denominator is SCHEDULED intent; errors AND
# concurrency drops count against it (a drop is offered load the
# client could not even launch).
sched = mx.get("appends_scheduled") or mx.get("appends") or 0
bad = (mx.get("append_errors") or 0) + (mx.get("append_conc_drops") or 0)
if sched and bad / sched > 0.001:
    fails.append(f"append_shed {bad}/{sched}")
p99 = mx.get("delivery_latency_ms", {}).get("p99")
if p99 is not None and p99 > 250:
    fails.append(f"delivery_p99 {p99}ms")
peak = m.get("proc_peak_rss_kb", 0) / 1024
if peak > 450:  # 500MB shed line minus headroom
    fails.append(f"peak_rss {peak:.0f}MB")
srv = ph.get("mixed", {}).get("server_after") or {}
if (srv.get("admit_shed") or 0) > 0:
    fails.append(f"admit_shed {srv['admit_shed']}")
print("CAPACITY_RUNG_" + ("FAIL " + "; ".join(fails) if fails else "PASS"))
sys.exit(1 if fails else 0)
PY
}

if [ "$AXIS" = delivery ]; then
  # ---- delivery-throughput ladder at a fanout geometry -------------
  # DELIVERY_GEO (default 100x100, the product shape) picks the
  # geometry; at 10k connections the SHIPPED budgets are RSS-bound
  # before CPU — an RSS-light 25x100 run isolates the delivery-CPU
  # ceiling. Never use this axis for feed-memory claims.
  GEO=${DELIVERY_GEO:-100x100}
  GF=${GEO%x*}; GS=${GEO#*x}
  [ ${#RUNGS[@]} -gt 0 ] || RUNGS=(1000 2500 5000 10000)
  export SSE_MAX_CONNECTIONS=$((GF * GS + 200))
  for D in "${RUNGS[@]}"; do
    OUT="$RESULTS/del-${D}ps-$GEO"
    export SUBSCRIBED_WPS=$((D / GS))
    echo "== delivery rung ${D}/s at $GEO (SUBSCRIBED_WPS=$SUBSCRIBED_WPS)"
    [ -s "$OUT/manifest.json" ] || bash "$HERE/run-one.sh" c "$GF" "$GS" "$OUT" "delivery-${D}ps-$GEO" || true
    if ! gate "$OUT/manifest.json"; then
      echo "== delivery ladder stops at ${D}/s"
      break
    fi
    sleep 20
  done
  echo "DELIVERY_LADDER_DONE"
  exit 0
fi

# ---- residency/capacity ladder: FIXED 300 w/s at every geometry ----
[ ${#RUNGS[@]} -gt 0 ] || RUNGS=(1000 2500 5000 7500 10000)
export SUBSCRIBED_WPS=100
HIGHEST=""
for N in "${RUNGS[@]}"; do
  OUT="$RESULTS/rung-${N}x1"
  export SSE_MAX_CONNECTIONS=$((N + 200))
  echo "== capacity rung ${N}x1 (SSE_MAX_CONNECTIONS=$SSE_MAX_CONNECTIONS, 300 w/s fixed)"
  if [ ! -s "$OUT/manifest.json" ]; then
    bash "$HERE/run-one.sh" c "$N" 1 "$OUT" "capacity-${N}x1" || true
  fi
  if gate "$OUT/manifest.json"; then
    HIGHEST=$N
  else
    echo "== ladder stops at ${N}x1"
    break
  fi
  sleep 20
done
[ -n "$HIGHEST" ] || { echo "CAPACITY_LADDER_FAIL: no rung passed"; exit 1; }
echo "== highest passing rung: $HIGHEST — attribution geometries (same 300 w/s)"
export SSE_MAX_CONNECTIONS=$((HIGHEST + 200))
for GEO in "$((HIGHEST / 2))x2" "$((HIGHEST / 100))x100"; do
  F=${GEO%x*}; S=${GEO#*x}
  OUT="$RESULTS/attr-$GEO"
  [ -s "$OUT/manifest.json" ] || bash "$HERE/run-one.sh" c "$F" "$S" "$OUT" "capacity-attr-$GEO" || true
  gate "$OUT/manifest.json" || true
  sleep 20
done
echo "CAPACITY_LADDER_DONE highest=${HIGHEST}"
