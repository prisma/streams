#!/bin/bash
# Mature-stream second-absorption stress (review round 4, P0 gate).
#
# Scenario: N streams each build a DEEP absorbed-but-untrimmed-eligible
# prefix (wave 1), then every stream receives ONE new record and absorbs
# again (wave 2). Before the global trim budget, wave 2 expanded into
# ONE shard WriteBatch of N x depth record deletes (67M at the fleet
# posture: 1,024 x TRIM_PER_OP=65536 — a multi-GiB batch). The gates:
#
#   G1  trim.deletes_max_batch <= TRIM_GLOBAL_BUDGET at all times
#   G2  trim debt OBSERVED after wave 2 (boundary/trim decoupling engaged)
#   G3  trim.deletes_total == N x DEPTH at convergence (every owed offset
#       trimmed exactly once), debt drains to zero via the 5 s ticker
#   G4  server RSS gauge stays bounded (no batch-sized spike)
#   G5  integrity: sampled streams read back DEPTH+1 records end-to-end
#
# Runs the WIDE fleet posture (TRIM_PER_OP=65536) — the configuration
# the review flagged as most dangerous. Depth is scaled (2,048/stream ~
# 2.1M owed deletes) so the run fits a laptop; the bound under test is
# depth-independent (the budget caps per-commit work regardless of how
# much is owed), and the DST twin proves the per-stream-cap interplay.
#
#   run-mature.sh <label> <server-binary> <out-dir>
set -euo pipefail
LABEL=${1:?label}; SERVER_BIN=${2:?server binary}; OUT=${3:?out dir}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT/snaps"
# Key lives OUTSIDE the tree (RUNBOOK §12); override with COSTAB_KEY_FILE.
KEY=$(cat "${COSTAB_KEY_FILE:-${SOAK_HOME:-$HOME/.streams-soak}/costab-streamkey.txt}")
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
  L0_MAX_SSTS=64 \
  COMPACTOR_MAX_CONCURRENT=2 SHARED_CACHE_BYTES=67108864 SLATEDB_RT_THREADS=2 \
  ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 \
  ADMIT_RSS_SHED_MB="${MATURE_SHED_MB:-1400}" \
  ABSORB_BYTES=4194304 ABSORB_AGE_SECS=30 ABSORB_PASS_BYTES=67108864 \
  ABSORB_MIN_BYTES_FOR_AGE=262144 \
  TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET="${MATURE_TRIM_BUDGET:-65536}" \
  RUST_LOG=warn \
  "$SERVER_BIN" --listen 127.0.0.1:8090 > "$OUT/server.log" 2>&1 &
SRV_PID=$!

for i in $(seq 1 60); do
  if curl -sf -o /dev/null -H "authorization: Bearer $AUTH" \
      http://127.0.0.1:8090/v1/debug/store; then break; fi
  sleep 1
  if ! kill -0 "$SRV_PID" 2>/dev/null; then echo "server died at boot"; exit 1; fi
done

MATURE_LABEL="$LABEL" MATURE_OUT="$OUT" AUTH_TOKEN="$AUTH" STREAM_KEY="$KEY" \
  SRV_PID="$SRV_PID" \
  python3 "$HERE/mature-driver.py"
RC=$?

kill "$SRV_PID" 2>/dev/null || true; SRV_PID=""
sleep 1
kill "$S3_PID" 2>/dev/null || true; S3_PID=""
echo "MATURE $LABEL DONE rc=$RC"
exit $RC
