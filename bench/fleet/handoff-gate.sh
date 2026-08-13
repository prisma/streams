#!/bin/bash
# R27-5: fleet handoff at PEAK maintenance backlog.
#
# The durable-gauge property under its worst case: an owner dies (real
# SIGABRT via /v1/debug/abort — no WAL flush, no fencing handoff, no
# absorber drain) while holding hundreds of MB of maintenance backlog,
# and the successors must (a) restore the per-shard gauges from the
# durable rows — never zero, never rebuilt-empty — and (b) drain the
# debt, with (c) exactly-once accounting across the whole episode via
# the R26-8 op ledger.
#
# Sequence: local awsbench -> fleet LB (incompressible, 32 streams);
# pick the instance carrying the most ingest; PAUSE its absorber so its
# ledger climbs to PEAK_MB; snapshot per-shard gauges; abort it; poll
# survivors until the dead instance's shards reappear with restored
# gauges; wait for drain; /stop the generator; exact reconcile through
# the LB (pseudo-region: url-server/url-gen files written here).
#
# Needs: fleet deployed (deploy-fleet.sh servers+urls+lb) with
# STREAMS_DEBUG_EXIT=1, awsbench built (debug), SOAK_HOME secrets.
set -euo pipefail
S=${SOAK_HOME:?set SOAK_HOME}
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
RUN=${HANDOFF_RUN_ID:-fh$(date -u +%H%M%S)}
PEAK_MB=${HANDOFF_PEAK_MB:-250}
BUILD_TIMEOUT=${HANDOFF_BUILD_TIMEOUT_SECS:-1500}
OUT="$S/results/handoff-$RUN"
mkdir -p "$OUT"
AUTH=$(cat "$S/auth.txt"); KEY=$(cat "$S/skey.txt")
LB=$(cat "$S/url-fleet-lb.txt")
GEN_PORT=8085

hdr() { echo "== $* =="; }
jload() { # instance url -> debug/load JSON (empty on failure)
  curl -s -m 15 -H "Authorization: Bearer $AUTH" "$1/v1/debug/load" 2>/dev/null || true
}

hdr "handoff-$RUN  LB=$LB  peak target=${PEAK_MB}MB"

# 0) readiness: a cold fleet 503s creates while instances claim shard
# ownership under a fresh data prefix; the generator's create retries
# are finite, so gate on a probe create succeeding first.
for n in $(seq 1 60); do
  RC=$(curl -s -m 15 -X PUT "$LB/v1/stream/soak-$RUN-ready" \
        -H "Authorization: Bearer $AUTH" -H "Stream-Encryption-Key: $KEY" \
        -o /dev/null -w "%{http_code}" || echo 000)
  case "$RC" in 200|201|409) echo "  fleet ready (probe $RC after ${n}0s)"; break;; esac
  [ "$n" = 60 ] && { echo "fleet never became ready (last $RC)"; exit 1; }
  sleep 10
done

# 1) generator: local awsbench through the LB, held for scrape.
BENCH_SYSTEM=prisma BENCH_SHAPE=cap BENCH_TARGET="$LB" \
BENCH_CONC=${HANDOFF_CONC:-32} BENCH_BATCH=10 BENCH_RECORD_BYTES=1024 \
BENCH_SECS=7200 BENCH_HOLD=1 BENCH_STREAMS_N=32 BENCH_INCOMPRESSIBLE=1 \
BENCH_OUT="$OUT/gen.jsonl" AUTH_TOKEN="$AUTH" STREAM_KEY="$KEY" \
BENCH_STREAM="soak-$RUN" PORT=$GEN_PORT \
"$REPO/bench/awsbench/target/debug/awsbench" > "$OUT/gen.log" 2>&1 &
GEN_PID=$!
trap 'kill $GEN_PID 2>/dev/null || true' EXIT
sleep 90

# 2) pick the owner carrying the most ingest.
TARGET=""; TARGET_URL=""; BEST=0
for i in 1 2 3 4; do
  U=$(cat "$S/url-fleet-s$i.txt")
  ING=$(jload "$U" | python3 -c "import json,sys
try: print(json.loads(sys.stdin.read()).get('maintenance_shards',{}).get('ingest_frame_bytes_total',0))
except Exception: print(0)")
  echo "  s$i ingest=$ING"
  if [ "${ING:-0}" -gt "$BEST" ]; then BEST=$ING; TARGET=$i; TARGET_URL=$U; fi
done
[ -n "$TARGET" ] || { echo "no owner found"; exit 1; }
hdr "target owner: s$TARGET ($TARGET_URL, ingest=$BEST)"

# 2b) steady-state fleet ledger baseline (pre-pause): the drain
# criterion under continued load is RETURN TO THIS BAND — near-zero is
# structurally unreachable while the generator offers load (the
# absorber cycle keeps tens of MB of healthy in-flight backlog; run 3
# oscillated 32-66MB forever against a 10MB bar).
BASE=0
for _ in 1 2 3; do
  T=0
  for i in 1 2 3 4; do
    U=$(cat "$S/url-fleet-s$i.txt")
    L=$(jload "$U" | python3 -c "import json,sys
try:
    m=json.loads(sys.stdin.read()).get('maintenance_shards',{})
    print(sum(x.get('unabsorbed_frame_bytes',0) for x in m.get('shards',[])))
except Exception: print(0)")
    T=$((T + L))
  done
  [ "$T" -gt "$BASE" ] && BASE=$T
  sleep 5
done
echo "  steady fleet-ledger baseline: $((BASE>>20))MB"
BAND=$((BASE * 2 + 33554432))   # 2x baseline + 32MB slack

# 3) pause EVERY instance's absorber: the whole fleet's backlog
# builds (4x faster than pausing one), and — critical for step 5 —
# survivor gauges are FROZEN through the restore verification, so the
# comparison is monotone: restored >= pre-kill exactly, with only
# post-kill ingest on top, never absorption underneath (R29).
for i in 1 2 3 4; do
  U=$(cat "$S/url-fleet-s$i.txt")
  curl -s -m 15 -X POST -H "Authorization: Bearer $AUTH" \
    "$U/v1/debug/absorb-pause?on=1" -o /dev/null -w "  s$i absorb-pause: %{http_code}\n"
done
T0=$(date +%s)
while :; do
  sleep 15
  J=$(jload "$TARGET_URL")
  LED=$(echo "$J" | python3 -c "import json,sys
try:
    m=json.loads(sys.stdin.read()).get('maintenance_shards',{})
    print(sum(x.get('unabsorbed_frame_bytes',0) for x in m.get('shards',[]))>>20)
except Exception: print(-1)")
  echo "  +$(( $(date +%s) - T0 ))s target ledger ${LED}MB"
  [ "$LED" -ge "$PEAK_MB" ] && break
  if [ $(( $(date +%s) - T0 )) -gt "$BUILD_TIMEOUT" ]; then
    echo "HANDOFF FAILED: backlog build timed out at ${LED}MB (< ${PEAK_MB}MB required)"
    exit 1
  fi
done

# 4) snapshot per-shard gauges, then abort the owner.
jload "$TARGET_URL" | python3 -c "import json,sys
m=json.loads(sys.stdin.read()).get('maintenance_shards',{})
rows={x['prefix']: x['unabsorbed_frame_bytes'] for x in m.get('shards',[]) if x.get('unabsorbed_frame_bytes',0)>0}
json.dump(rows, open('$OUT/prekill-shards.json','w'), indent=1)
print('pre-kill shards:', {k: v>>20 for k,v in rows.items()}, 'MB')"
PRE_SUM=$(python3 -c "import json; print(sum(json.load(open('$OUT/prekill-shards.json')).values()))")
hdr "aborting s$TARGET with $((PRE_SUM>>20))MB durable backlog"
date +%s > "$OUT/kill.ts"
curl -s -m 15 -X POST -H "Authorization: Bearer $AUTH" "$TARGET_URL/v1/debug/abort" | head -c 60; echo

# 5) poll survivors for gauge restoration.
python3 - "$S" "$OUT" "$TARGET" "$AUTH" <<'PY'
import json, sys, time, urllib.request
S, OUT, dead, auth = sys.argv[1:5]
pre = json.load(open(f"{OUT}/prekill-shards.json"))
pre_sum = sum(pre.values())
survivors = [open(f"{S}/url-fleet-s{i}.txt").read().strip()
             for i in range(1, 5) if str(i) != dead]
FAILS = {}
def load(u):
    # R29: a survivor that stops answering is a FAILURE, not zero
    # backlog — three consecutive misses fail the gate.
    try:
        req = urllib.request.Request(f"{u}/v1/debug/load",
            headers={"Authorization": f"Bearer {auth}"})
        with urllib.request.urlopen(req, timeout=15) as r:
            FAILS[u] = 0
            return json.load(r)
    except Exception:
        FAILS[u] = FAILS.get(u, 0) + 1
        if FAILS[u] >= 3:
            sys.exit(f"HANDOFF FAILED: survivor telemetry lost: {u}")
        return {}
t0 = time.time()
best = {}
while time.time() - t0 < 600:
    time.sleep(5)
    seen = {}
    for u in survivors:
        m = load(u).get("maintenance_shards", {})
        for x in m.get("shards", []):
            if x["prefix"] in pre:
                seen[x["prefix"]] = x["unabsorbed_frame_bytes"]
    for k, v in seen.items():
        best[k] = max(best.get(k, 0), v)
    missing = [k for k in pre if k not in seen]
    print(f"  +{int(time.time()-t0)}s restored {len(seen)}/{len(pre)} shards "
          f"sum_now={sum(seen.values())>>20}MB first_seen_sum={sum(best.values())>>20}MB "
          f"(pre {pre_sum>>20}MB) missing={missing[:3]}", flush=True)
    if not missing:
        break
else:
    sys.exit(f"HANDOFF FAILED: shards never restored: "
             f"{[k for k in pre if k not in best]}")
verdict = {
    "prekill_sum_bytes": pre_sum,
    "restored_first_seen_sum_bytes": sum(best.values()),
    "restored_over_prekill": round(sum(best.values()) / pre_sum, 4) if pre_sum else None,
    "per_shard": {k: {"pre": pre[k], "restored_first_seen": best.get(k, 0)} for k in pre},
}
json.dump(verdict, open(f"{OUT}/restore-verdict.json", "w"), indent=1)
# The gauge is exact and durable at every commit; absorption on the
# successor may legitimately retire debt between polls, so require the
# max-observed restored sum to reach 70% of pre-kill. ZERO on any shard
# that had debt is the failure the gate exists to catch.
# R29: survivor absorbers are PAUSED through this window, so each
# restored gauge is monotone — pre-kill value plus any post-kill
# ingest. Anything below 100% per shard means durable state was lost.
low = {k: (best.get(k, 0), pre[k]) for k in pre if best.get(k, 0) < pre[k]}
if low:
    sys.exit(f"HANDOFF FAILED: restored below pre-kill on {low}")
print("GAUGE RESTORE: OK (every shard >= 100% of pre-kill)",
      verdict["restored_over_prekill"])
PY

# 6) restore verified: unpause the SURVIVORS so the debt drains
# under the continuing generator load.
for i in 1 2 3 4; do
  [ "$i" = "$TARGET" ] && continue
  U=$(cat "$S/url-fleet-s$i.txt")
  curl -s -m 15 -X POST -H "Authorization: Bearer $AUTH" \
    "$U/v1/debug/absorb-pause?on=0" -o /dev/null -w "  s$i absorb-resume: %{http_code}\n"
done
hdr "waiting for drain (generator still offering load)"
T0=$(date +%s)
while :; do
  sleep 15
  TOT=0
  for i in 1 2 3 4; do
    [ "$i" = "$TARGET" ] && continue
    U=$(cat "$S/url-fleet-s$i.txt")
    L=$(jload "$U" | python3 -c "import json,sys
try:
    m=json.loads(sys.stdin.read()).get('maintenance_shards',{})
    print(sum(x.get('unabsorbed_frame_bytes',0) for x in m.get('shards',[])))
except Exception: print(-1)")
    if [ "$L" = "-1" ]; then
      MISS="s$i-$(date +%s)"
      echo "  survivor s$i telemetry MISS ($MISS)"
      MISSES=$((${MISSES:-0} + 1))
      [ "$MISSES" -ge 5 ] && { echo "DRAIN FAILED: survivor telemetry lost"; exit 1; }
      L=0
    fi
    TOT=$((TOT + L))
  done
  echo "  +$(( $(date +%s) - T0 ))s fleet ledger $((TOT>>20))MB (band $((BAND>>20))MB)"
  if [ "$TOT" -le "$BAND" ]; then
    INBAND=$((${INBAND:-0} + 1))
    [ "$INBAND" -ge 3 ] && break
  else
    INBAND=0
  fi
  [ $(( $(date +%s) - T0 )) -gt 1800 ] && { echo "DRAIN TIMEOUT (under load)"; exit 1; }
done
echo "$(( $(date +%s) - T0 ))" > "$OUT/drain-secs.txt"
hdr "catch-up under load complete (back to steady band); stopping load for absolute drain"

# 7a) stop the generator (clean join -> final line + ledger).
curl -s -m 10 "http://127.0.0.1:$GEN_PORT/stop" >/dev/null || true
for _ in $(seq 1 30); do
  grep -q "BENCH_DONE" "$OUT/gen.log" 2>/dev/null && break
  sleep 2
done

# 7b) absolute drain with load stopped: the R27-4 recovery model.
T0=$(date +%s)
while :; do
  sleep 10
  TOT=0
  for i in 1 2 3 4; do
    [ "$i" = "$TARGET" ] && continue
    U=$(cat "$S/url-fleet-s$i.txt")
    L=$(jload "$U" | python3 -c "import json,sys
try:
    m=json.loads(sys.stdin.read()).get('maintenance_shards',{})
    print(sum(x.get('unabsorbed_frame_bytes',0) for x in m.get('shards',[])))
except Exception: print(0)")
    TOT=$((TOT + L))
  done
  echo "  +$(( $(date +%s) - T0 ))s post-load ledger $((TOT>>20))MB"
  [ "$TOT" -lt 10485760 ] && break
  [ $(( $(date +%s) - T0 )) -gt 600 ] && { echo "ABSOLUTE DRAIN TIMEOUT"; exit 1; }
done
echo "$(( $(date +%s) - T0 ))" > "$OUT/absolute-drain-secs.txt" 
hdr "reconciling through the LB"
printf '%s' "$LB" > "$S/url-server-$RUN.txt"
printf 'http://127.0.0.1:%s' "$GEN_PORT" > "$S/url-gen-$RUN.txt"
# R29: preview-domain DNS is transient; retry the whole reconcile up
# to 4 times, 90s apart, before declaring failure. The generator stays
# alive (its op ledger IS the acceptance input) until a verdict lands.
RECON_OK=0
for attempt in 1 2 3 4; do
  if SOAK_HOME="$S" SOAK_RUN_ID="handoff-$RUN" BENCH_BATCH=10 \
    python3 "$REPO/bench/soak/reconcile.py" "$RUN"; then
    RECON_OK=1; break
  fi
  echo "reconcile attempt $attempt failed; retrying in 90s"
  sleep 90
done
kill $GEN_PID 2>/dev/null || true
[ "$RECON_OK" = "1" ] || { echo "HANDOFF FAILED: reconcile never passed"; exit 1; }
hdr "HANDOFF GATE COMPLETE — artifacts in $OUT"
