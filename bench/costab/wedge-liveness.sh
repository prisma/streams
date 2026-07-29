#!/bin/bash
# Saturation-wedge LIVENESS GATE (cost review round 2, Gate 0):
# drive a single stream past the absorber envelope at the FIELD memory
# posture until the service wedges (sustained 429s / stalled ok-count),
# then REMOVE the load and require recovery: fresh appends must succeed
# again within RECOVERY_SECS, without a restart.
#
#   wedge-liveness.sh <server-binary> <out-dir>
#
# Exit 0: PASS (wedged, then recovered)   — the release gate
# Exit 1: FAIL (wedged and stayed wedged) — today's known state
# Exit 2: INCONCLUSIVE (never wedged within the load window)
set -uo pipefail
SERVER_BIN=${1:?server binary}; OUT=${2:?out dir}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"
KEY=$(cat "$HERE/streamkey.txt")
AUTH=localsoak
LOAD_SECS=${WEDGE_LOAD_SECS:-720}
RECOVERY_SECS=${WEDGE_RECOVERY_SECS:-300}

cleanup() {
  kill "$GEN_PID" 2>/dev/null || true
  kill "$SRV_PID" 2>/dev/null || true
  kill "$S3_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

"$HERE/bin/s3lite-ab" --listen 127.0.0.1:9500 --latency-ms 25 > "$OUT/s3lite.log" 2>&1 &
S3_PID=$!
sleep 1

# FIELD posture on purpose, including the 600 MB shed line: the gate is
# about how the field configuration behaves after overload, not about
# giving the rig headroom.
env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=soakab \
  SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
  AUTH_TOKEN=$AUTH PATH_PREFIX=wedge INSTANCE_NAME=streams-1 INITIAL_SHARDS=4 \
  WAL_GROUP_COMMIT=1 WAL_FLUSH_GAP_MS=10 FLUSH_INTERVAL_MS=25 \
  WAL_POST_ACK_GATHER_MS=6 TAIL_RING_BYTES=33554432 \
  FRAME_COMPRESS=1 L0_SST_SIZE_BYTES=16777216 MAX_UNFLUSHED_BYTES=33554432 \
  L0_MAX_SSTS=64 MANIFEST_POLL_MS=1000 COMPACTOR_POLL_MS=500 \
  COMPACTOR_MAX_CONCURRENT=2 SHARED_CACHE_BYTES=67108864 SLATEDB_RT_THREADS=2 \
  ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 ADMIT_RSS_SHED_MB=600 \
  ABSORB_BYTES=4194304 ABSORB_AGE_SECS=60 ABSORB_PASS_BYTES=67108864 \
  TRIM_PER_OP=65536 RUST_LOG=warn \
  "$SERVER_BIN" --listen 127.0.0.1:8090 > "$OUT/server.log" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  curl -sf -o /dev/null -H "authorization: Bearer $AUTH" \
    http://127.0.0.1:8090/v1/debug/store && break
  sleep 1
  kill -0 "$SRV_PID" 2>/dev/null || { echo "VERDICT=ERROR server died at boot"; exit 2; }
done

# Load: one hot stream at conc24 x batch10 — past the absorber envelope.
env BENCH_SYSTEM=prisma BENCH_SHAPE=tiers BENCH_TARGET=http://127.0.0.1:8090 \
  BENCH_TIERS=24 BENCH_SECS="$LOAD_SECS" BENCH_BATCH=10 BENCH_RECORD_BYTES=1024 \
  BENCH_CONSUME=false BENCH_STREAM=wedge-1 AUTH_TOKEN=$AUTH STREAM_KEY="$KEY" \
  BENCH_OUT="$OUT/gen.jsonl" \
  "$HERE/bin/awsbench-ab" > "$OUT/gen.log" 2>&1 &
GEN_PID=$!

# Overload detector. The property this gate protects is: after
# sustained memory shedding, removing the load restores service. Two
# stress signatures count (the recovery probe runs after either):
#  - collapse: window ok-delta under 1% of the best window, twice in a
#    row, with throttles rising (the historical ratchet wedge), or
#  - sustained shedding: heavy 429 volume across two consecutive
#    windows while goodput is degraded. With the honest footprint
#    gauge + over-line purge, the instance self-regulates AT the line
#    (throttled equilibrium, goodput never collapses) — that is
#    overload too, and recovery afterwards is what must be proven.
wedged=0; stressed=0; prev_ok=-1; prev_thr=0; frozen=0; shed_windows=0; best_delta=1
end=$((SECONDS + LOAD_SECS))
while [ $SECONDS -lt $end ]; do
  sleep 30
  kill -0 "$GEN_PID" 2>/dev/null || break
  line=$(grep '"label"' "$OUT/gen.log" | tail -1)
  [ -n "$line" ] || continue
  ok=$(echo "$line" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['ok'])" 2>/dev/null || echo -1)
  thr=$(echo "$line" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['throttled'])" 2>/dev/null || echo 0)
  ps -o rss= -p "$SRV_PID" | awk '{print "'"$SECONDS"'", $1}' >> "$OUT/rss.log"
  if [ "$prev_ok" -ge 0 ]; then
    delta=$((ok - prev_ok))
    thr_delta=$((thr - prev_thr))
    [ "$delta" -gt "$best_delta" ] && best_delta=$delta
    if [ $((delta * 100)) -lt "$best_delta" ] && [ "$thr_delta" -gt 0 ]; then
      frozen=$((frozen+1))
    else
      frozen=0
    fi
    # Sustained shedding: many rejects while goodput runs under half
    # of the best window seen.
    if [ "$thr_delta" -gt 10000 ] && [ $((delta * 2)) -lt "$best_delta" ]; then
      shed_windows=$((shed_windows+1))
    else
      shed_windows=0
    fi
  fi
  prev_ok=$ok; prev_thr=$thr
  if [ "$frozen" -ge 2 ]; then wedged=1; break; fi
  if [ "$shed_windows" -ge 2 ]; then stressed=1; break; fi
done
if [ "$wedged" != 1 ] && [ "$stressed" != 1 ]; then
  echo "VERDICT=INCONCLUSIVE no overload within ${LOAD_SECS}s (ok=$prev_ok thr=$prev_thr)"
  exit 2
fi
echo "OVERLOADED at t=${SECONDS}s (wedged=$wedged stressed=$stressed ok=$prev_ok throttled=$prev_thr); removing load"
kill "$GEN_PID" 2>/dev/null || true
GEN_PID=""

# Recovery probe: a FRESH stream must accept appends again. Five
# consecutive append successes = recovered. Stream creation is itself
# a write (the shed 429s it while still engaged), so it is retried
# inside the loop rather than attempted once — a swallowed creation
# failure turned every probe into a 404 and produced a phantom FAIL.
consec=0; recovered=0; created=0
probe_end=$((SECONDS + RECOVERY_SECS))
while [ $SECONDS -lt $probe_end ]; do
  sleep 5
  if [ "$created" != 1 ]; then
    ccode=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
      -H "authorization: Bearer $AUTH" -H "stream-encryption-key: $KEY" \
      -H "content-type: application/json" http://127.0.0.1:8090/v1/stream/wedge-probe)
    case "$ccode" in 2*) created=1;; esac
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "authorization: Bearer $AUTH" -H "stream-encryption-key: $KEY" \
    -H "content-type: application/json" \
    -d '[{"probe":1}]' http://127.0.0.1:8090/v1/stream/wedge-probe)
  gauge=$(curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/load \
    | python3 -c "import json,sys; print(round(json.loads(sys.stdin.read())['rss_mb']))" 2>/dev/null || echo -)
  echo "$SECONDS $(ps -o rss= -p "$SRV_PID" | tr -d ' ') $code fp=${gauge}MB" >> "$OUT/rss.log"
  case "$code" in
    2*) consec=$((consec+1));;
    *) consec=0;;
  esac
  if [ "$consec" -ge 5 ]; then recovered=1; break; fi
done
curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/store > "$OUT/final-store.json" || true
curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/usage > "$OUT/final-usage.json" || true
if [ "$recovered" = 1 ]; then
  echo "VERDICT=PASS recovered $((probe_end - SECONDS))s before deadline"
  exit 0
fi
echo "VERDICT=FAIL still rejecting appends ${RECOVERY_SECS}s after load removal"
exit 1
