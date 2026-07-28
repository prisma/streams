#!/bin/bash
# One full 30-minute soak (field tier schedule) against a local rig:
#   s3lite (25 ms latency) <- streams-slate (soak7 env shape) <- awsbench
# Usage: run-soak.sh <label> <server-binary> <out-dir>
set -euo pipefail
LABEL=${1:?label}; SERVER_BIN=${2:?server binary}; OUT=${3:?out dir}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT/snaps"
KEY=$(cat "$HERE/streamkey.txt")
AUTH=localsoak

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

env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=soakab \
  SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
  AUTH_TOKEN=$AUTH PATH_PREFIX="$LABEL" INSTANCE_NAME=streams-1 INITIAL_SHARDS=4 \
  WAL_GROUP_COMMIT=1 WAL_FLUSH_GAP_MS=10 FLUSH_INTERVAL_MS=25 \
  WAL_POST_ACK_GATHER_MS=6 TAIL_RING_BYTES=33554432 STREAMS_DEBUG_TIMING=0 \
  FRAME_COMPRESS=1 L0_SST_SIZE_BYTES=16777216 MAX_UNFLUSHED_BYTES=33554432 \
  L0_MAX_SSTS=64 MANIFEST_POLL_MS=1000 COMPACTOR_POLL_MS=500 \
  COMPACTOR_MAX_CONCURRENT=2 SHARED_CACHE_BYTES=67108864 SLATEDB_RT_THREADS=2 \
  ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 ADMIT_RSS_SHED_MB=600 \
  ABSORB_BYTES=4194304 ABSORB_AGE_SECS=60 ABSORB_PASS_BYTES=67108864 \
  TRIM_PER_OP=65536 RUST_LOG=warn \
  "$SERVER_BIN" --listen 127.0.0.1:8090 > "$OUT/server.log" 2>&1 &
SRV_PID=$!

# Wait for the server to answer.
for i in $(seq 1 60); do
  if curl -sf -o /dev/null -H "authorization: Bearer $AUTH" \
      http://127.0.0.1:8090/v1/debug/store; then break; fi
  sleep 1
  if ! kill -0 "$SRV_PID" 2>/dev/null; then echo "server died at boot"; exit 1; fi
done

snap() {
  local tag=$1
  curl -s http://127.0.0.1:9500/_s3lite/stats  > "$OUT/snaps/$tag-stats.json" || true
  curl -s http://127.0.0.1:9500/_s3lite/stats2 > "$OUT/snaps/$tag-stats2.json" || true
  curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/store   > "$OUT/snaps/$tag-store.json" || true
  curl -s -H "authorization: Bearer $AUTH" http://127.0.0.1:8090/v1/debug/timings > "$OUT/snaps/$tag-timings.json" || true
  ps -o rss= -p "$SRV_PID" 2>/dev/null | awk -v t="$tag" '{print t, $1}' >> "$OUT/rss.log" || true
}
snap t0

# Timeline poller (60 s cadence) until the generator finishes.
( i=0; while true; do sleep 60; i=$((i+1)); snap "t$i"; done ) &
POLL_PID=$!

env BENCH_SYSTEM=prisma BENCH_SHAPE=tiers BENCH_TARGET=http://127.0.0.1:8090 \
  BENCH_TIERS="${SOAK_TIERS:-1,2,4,8,12,16,24,32,48,64}" BENCH_SECS="${SOAK_SECS:-180}" BENCH_BATCH=10 \
  BENCH_RECORD_BYTES=1024 BENCH_CONSUME=true BENCH_STREAM="soak-$LABEL" \
  AUTH_TOKEN=$AUTH STREAM_KEY="$KEY" BENCH_OUT="$OUT/awsbench.jsonl" \
  "$HERE/bin/awsbench-ab" > "$OUT/gen.log" 2>&1 &
GEN_PID=$!
wait "$GEN_PID" || true
GEN_PID=""

kill "$POLL_PID" 2>/dev/null || true
# Post-run settle: let in-flight absorption/compaction quiesce briefly so
# final counters include the tail of the run's work.
sleep 20
snap final
kill "$SRV_PID" 2>/dev/null || true
SRV_PID=""
sleep 2
curl -s http://127.0.0.1:9500/_s3lite/stats2 > "$OUT/snaps/post-stats2.json" || true
kill "$S3_PID" 2>/dev/null || true
S3_PID=""
echo "SOAK $LABEL DONE"
