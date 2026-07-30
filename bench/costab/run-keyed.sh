#!/bin/bash
# One keyed-campaign arm (ROUTING-V3 spec §15): boot the rig, run
# keyed-driver.py against one server build, tear down.
#   run-keyed.sh <label> <server-binary> <out-dir>
# Env: KEYED_KEYS/BATCH/ROUNDS/ACTIVE/WINDOWS (driver knobs).
set -euo pipefail
LABEL=${1:?label}; SERVER_BIN=${2:?server binary}; OUT=${3:?out dir}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"
KEY=$(cat "$HERE/streamkey.txt")
AUTH=localsoak

cleanup() {
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
  ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 \
  ADMIT_RSS_SHED_MB=1400 \
  ABSORB_BYTES=4194304 ABSORB_AGE_SECS=15 ABSORB_PASS_BYTES=67108864 \
  ABSORB_MIN_BYTES_FOR_AGE=0 \
  TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET=65536 \
  LIMIT_REQS_PER_SEC=100000 LIMIT_RECS_PER_SEC=500000 LIMIT_BYTES_PER_SEC=500000000 \
  RUST_LOG=warn \
  "$SERVER_BIN" --listen 127.0.0.1:8090 > "$OUT/server.log" 2>&1 &
SRV_PID=$!

for i in $(seq 1 60); do
  if curl -sf -o /dev/null -H "authorization: Bearer $AUTH" \
      http://127.0.0.1:8090/v1/debug/store; then break; fi
  sleep 1
  if ! kill -0 "$SRV_PID" 2>/dev/null; then echo "server died at boot"; exit 1; fi
done

KEYED_LABEL="$LABEL" KEYED_OUT="$OUT" AUTH_TOKEN="$AUTH" STREAM_KEY="$KEY" \
  SRV_PID="$SRV_PID" \
  python3 "$HERE/keyed-driver.py"
RC=$?
kill "$SRV_PID" 2>/dev/null || true; SRV_PID=""
sleep 1
kill "$S3_PID" 2>/dev/null || true; S3_PID=""
echo "KEYED $LABEL DONE rc=$RC"
exit $RC
