#!/bin/bash
# Campaign 2: the per-instance capacity ladder (round 12) on arm C.
#
# Worst-case geometry first (N streams x 1 subscriber, max feed count);
# per-rung gates per the charter: append-shed < 0.1%, delivery p99
# < 250 ms, zero missing, no RSS-shed, peak RSS below the 500 MB
# admission line WITH headroom. The ladder stops at the first failing
# rung; the highest passing rung then reruns at N/2 x 2 and N/100 x 100
# to attribute the ceiling (feed state vs connection state vs fanout).
#
#   RESULTS=<dir> run-capacity.sh [rungs...]
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
RESULTS=${RESULTS:?set RESULTS dir}
mkdir -p "$RESULTS"
RUNGS=("$@")
[ ${#RUNGS[@]} -gt 0 ] || RUNGS=(1000 2500 5000 7500 10000)
# Constant write workload while subscribers scale: appends confined
# to 100 streams; the density campaign showed write breadth (absorber
# state per stream) drives RSS, and it must not ride the rungs.
export WARMUP_SECS=30 IDLE_SECS=600 SPARSE_SECS=60 FANOUT_SECS=180 MIXED_SECS=180 \
       SLOW_SECS=60 TEARDOWN_SECS=300 FANOUT_DELIVERY_RATE=${FANOUT_DELIVERY_RATE:-1000} \
       MIXED_BG_RATE=200 WRITE_BREADTH=100

gate() { # manifest -> PASS/FAIL per the capacity gates
  python3 - "$1" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
ph = {p["name"]: p for p in m.get("phases", [])}
fails = []
if m.get("verdict") != "PASS":
    fails.append("reconciliation")
mx = ph.get("mixed", {}).get("client", {})
if mx.get("appends"):
    if mx["append_errors"] / mx["appends"] > 0.001:
        fails.append(f"append_shed {mx['append_errors']}/{mx['appends']}")
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

HIGHEST=""
for N in "${RUNGS[@]}"; do
  OUT="$RESULTS/rung-${N}x1"
  export SSE_MAX_CONNECTIONS=$((N + 200))
  echo "== capacity rung ${N}x1 (SSE_MAX_CONNECTIONS=$SSE_MAX_CONNECTIONS)"
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
echo "== highest passing rung: $HIGHEST — attribution geometries"
export SSE_MAX_CONNECTIONS=$((HIGHEST + 200))
for GEO in "$((HIGHEST / 2))x2" "$((HIGHEST / 100))x100"; do
  F=${GEO%x*}; S=${GEO#*x}
  OUT="$RESULTS/attr-$GEO"
  [ -s "$OUT/manifest.json" ] || bash "$HERE/run-one.sh" c "$F" "$S" "$OUT" "capacity-attr-$GEO" || true
  gate "$OUT/manifest.json" || true
  sleep 20
done
echo "CAPACITY_LADDER_DONE highest=${HIGHEST}"
