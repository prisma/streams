#!/bin/bash
# Cross-owner segment fan-out rig (#131): two fleet instances sharing one
# s3lite bucket behind a pilot LB. Splits give child segments their own
# shard routes, so with 8 shards over 2 instances a lineage crosses
# owners about half the time — the exact topology the v0.2.0-preview.4
# launch posture left unproven. fanout-probe.py drives splits and issues
# the verdicts.
#
#   bench/fleet/local-fanout.sh [out-dir]
#
# Requires target/release/{streams-slate,pilot,s3lite} (built here if
# missing). Servers get the field-gate split knobs; desired.json is
# seeded to 2 so both ordinals are active from the first request.
set -euo pipefail
OUT=${1:-/tmp/fanout-local}
mkdir -p "$OUT"
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
AUTH=localsoak
# The standard test key (same literal consumer-saga-smoke.sh defaults to).
KEY=BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=
# Distinct from AUTH: /v1/internal/* is a separate trust boundary and the
# server refuses to start fleet mode without its own credential.
FLEET_TOKEN=local-fleet-internal-token-0001

cargo build --release --bin streams-slate --bin pilot --bin s3lite \
  --manifest-path "$ROOT/Cargo.toml" 2>&1 | grep -E "^error" && exit 1 || true

cleanup() {
  kill ${LB_PID:-} ${S1_PID:-} ${S2_PID:-} ${S3_PID:-} 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

"$ROOT/target/release/s3lite" --listen 127.0.0.1:9500 --latency-ms 5 \
  > "$OUT/s3lite.log" 2>&1 &
S3_PID=$!
sleep 1

# Seed desired=2 before boot: bootstrap would publish 1 and the LB would
# route everything to streams-1, making every segment single-owner.
# SCALE_IN_SECS below keeps the fleet from shrinking it back.
python3 - <<'PY'
import boto3
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9500",
    aws_access_key_id="test", aws_secret_access_key="test", region_name="local")
s3.put_object(Bucket="fanout", Key="fleetops/fleet/desired.json",
    Body=b'{"count":2,"reason":"seeded by local-fanout.sh","epoch":1,"computed_at_ms":0}')
PY

server() { # ordinal, port
  env SLATE_S3_ENDPOINT=http://127.0.0.1:9500 SLATE_S3_BUCKET=fanout \
    SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
    AUTH_TOKEN=$AUTH PATH_PREFIX=fand INSTANCE_NAME="streams-$1" \
    SELF_URL="http://127.0.0.1:$2" \
    FLEET_INTERNAL_TOKEN="$FLEET_TOKEN" USAGE_STREAM_KEY="$KEY" TELEMETRY_DRAIN_SECS=1 ROLLUP=$( [ "$2" = 8091 ] && echo 1 || echo 0 ) FLEET_ALLOW_HTTP_PEERS=1 \
    FLEET_PREFIX=fleetops FLEET_MAX=2 SCALE_IN_SECS=999999 \
    WAL_GROUP_COMMIT=1 WAL_FLUSH_GAP_MS=10 FLUSH_INTERVAL_MS=25 \
    WAL_POST_ACK_GATHER_MS=6 FRAME_COMPRESS=1 \
    L0_SST_SIZE_BYTES=16777216 MAX_UNFLUSHED_BYTES=33554432 L0_MAX_SSTS=64 \
    COMPACTOR_MAX_CONCURRENT=2 SHARED_CACHE_BYTES=67108864 SLATEDB_RT_THREADS=2 \
    ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 ADMIT_RSS_SHED_MB=1400 \
    ABSORB_BYTES=4194304 ABSORB_AGE_SECS=15 ABSORB_PASS_BYTES=67108864 \
    TRIM_PER_OP=65536 \
    SCALE_EVAL_SECS=2 SCALE_RATE_WINDOW_SECS=10 SCALE_HOT_PCT=1 \
    SCALE_HOT_EVALS=1 SCALE_COOLDOWN_SECS=5 \
    RUST_LOG=warn \
    "$ROOT/target/release/streams-slate" --listen "127.0.0.1:$2" \
    > "$OUT/server-$1.log" 2>&1 &
}
server 1 8091; S1_PID=$!
server 2 8092; S2_PID=$!

for port in 8091 8092; do
  for i in $(seq 1 60); do
    curl -sf -o /dev/null -H "authorization: Bearer $AUTH" \
      "http://127.0.0.1:$port/health" && break
    sleep 1
  done
done

env MODE=lb UPSTREAMS="http://127.0.0.1:8091,http://127.0.0.1:8092" \
  S3_ENDPOINT=http://127.0.0.1:9500 S3_BUCKET=fanout S3_REGION=local \
  S3_ACCESS_KEY_ID=test S3_SECRET_ACCESS_KEY=test \
  FLEET_PREFIX=fleetops DATA_PREFIX=fand ROUTER_NAME=router-local \
  PORT=8090 \
  "$ROOT/target/release/pilot" > "$OUT/lb.log" 2>&1 &
LB_PID=$!
for i in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:8090/stats" && break
  sleep 1
done

echo "rig up: LB :8090 -> streams-1 :8091, streams-2 :8092 (logs in $OUT)"
if [ "${2:-}" = hold ]; then
  # Boot-only: keep the rig alive for manual driving; ^C tears down.
  echo "holding (kill this script to tear down)"
  wait
  exit 0
fi
AUTH_TOKEN=$AUTH STREAM_KEY=$KEY LB=http://127.0.0.1:8090 \
  A=http://127.0.0.1:8091 B=http://127.0.0.1:8092 \
  python3 "$HERE/fanout-probe.py"
RC=$?
echo "fanout probe rc=$RC"
exit $RC
