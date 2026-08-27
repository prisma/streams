#!/bin/bash
# One controlled run of one arm at one shape (round-12 campaign 1/2).
#
#   run-one.sh <arm: a|b|c> <feeds> <subs_per> <outdir> [tag]
#
# Topology (docker, all linux/amd64 under Rosetta):
#   perf-store  — s3lite, no caps, --latency-ms 2 (recorded)
#   perf-server — the arm binary; THE MEASURED CGROUP:
#                 --cpus 1 --memory 1g --ulimit nofile=32768
#   perf-gen    — node loadgen, uncapped (its cost must not ride the
#                 measured quota)
# The server container runs sampler.sh against the server pid; the
# manifest merges loadgen output, proc series, config and binary shas.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ARM=${1:?arm a|b|c}
FEEDS=${2:?feeds}
SUBS=${3:?subs-per}
OUTDIR=${4:?outdir}
TAG=${5:-run}
mkdir -p "$OUTDIR"
NET="perfnet-$$"
STORE="perf-store-$$"
SERVER="perf-server-$$"
BUCKET="perf-$(date +%s)-$$"
AUTH="perf-token-0123456789abcdef"
KEY_B64=$(node -e 'console.log(Buffer.from(Array(32).fill(7)).toString("base64"))')
STORE_LAT=${STORE_LAT_MS:-2}

case "$ARM" in
  a) BIN="$HERE/arms/streams-dual-1834b726"; ENGINE_ENV="STREAMS_SSE_ENGINE=legacy";;
  b) BIN="$HERE/arms/streams-dual-1834b726"; ENGINE_ENV="STREAMS_SSE_ENGINE=livefeed";;
  c) BIN="$HERE/arms/streams-rc-3a8016e6";  ENGINE_ENV="";;
  *) echo "arm must be a|b|c"; exit 2;;
esac
BIN_SHA=$(shasum -a 256 "$BIN" | cut -d' ' -f1)

cleanup() {
  docker rm -f "$SERVER" "$STORE" > /dev/null 2>&1 || true
  docker network rm "$NET" > /dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create "$NET" > /dev/null

docker run -d --name "$STORE" --network "$NET" --platform linux/amd64 \
  -v "$HERE/arms/s3lite:/bin-art/s3lite:ro" \
  perf-base:1 /bin-art/s3lite --listen 0.0.0.0:9500 --latency-ms "$STORE_LAT" > /dev/null

# The measured server. Entry script: start binary, run sampler.
docker run -d --name "$SERVER" --network "$NET" --platform linux/amd64 \
  --cpus 1 --memory 1g --ulimit nofile=32768:32768 \
  -v "$BIN:/bin-art/streams-slate:ro" \
  -v "$HERE/sampler.sh:/sampler.sh:ro" \
  -v "$OUTDIR:/out" \
  -e AUTH_TOKEN="$AUTH" \
  ${ENGINE_ENV:+-e "$ENGINE_ENV"} \
  -e MAX_RECORD_PAYLOAD_BYTES=131072 \
  -e SSE_FEED_RING_BYTES=1048576 \
  -e SSE_FEED_TOTAL_BYTES=16777216 \
  -e SSE_FEED_PROJECT_BYTES=4194304 \
  -e SSE_MAX_CONNECTIONS="${SSE_MAX_CONNECTIONS:-0}" \
  -e ABSORB_GATHER_MAX_BYTES=8388608 \
  -e ABSORB_GLOBAL_BUDGET_BYTES=100859904 \
  -e ABSORB_GLOBAL_GATHERS=1 \
  -e SLATEDB_RT_THREADS=4 \
  -e TELEMETRY_CACHE_BYTES=16777216 \
  -e SHARED_CACHE_BYTES=134217728 \
  -e HISTORY_CACHE_BYTES=33554432 \
  -e POSTINGS_CACHE_BYTES=67108864 \
  -e MAX_UNFLUSHED_BYTES=16777216 \
  -e L0_SST_SIZE_BYTES=8388608 \
  -e L0_MAX_SSTS=32 \
  -e ADMIT_RSS_SHED_MB=500 \
  -e STORE_BULK_INFLIGHT_MAX_BYTES=33554432 \
  -e COMPACTOR_MAX_CONCURRENT=1 \
  -e COMPACT_MAX_SUBCOMPACTIONS=1 \
  -e COMPACT_MAX_FETCH_TASKS=1 \
  -e COMPACT_BYTES_TO_FETCH=1048576 \
  -e COMPACT_MAX_SST_SIZE_BYTES=33554432 \
  perf-base:1 sh -c '
    /bin-art/streams-slate --listen 0.0.0.0:8080 \
      --s3-endpoint http://'"$STORE"':9500 --bucket '"$BUCKET"' \
      --flush-interval-ms 1 --wal-flush-gap-ms 2 > /out/server.log 2>&1 &
    SP=$!
    sh /sampler.sh $SP /out/proc.jsonl &
    wait $SP' > /dev/null

# Gen container (blocks until the run finishes).
set +e
docker run --rm --network "$NET" --platform linux/amd64 \
  -v "$HERE/loadgen.mjs:/loadgen.mjs:ro" \
  -v "$OUTDIR:/out" \
  -e TARGET="http://$SERVER:8080" -e AUTH="$AUTH" -e KEY_B64="$KEY_B64" -e OUT=/out \
  -e FEEDS="$FEEDS" -e SUBS_PER="$SUBS" \
  -e WARMUP_SECS="${WARMUP_SECS:-30}" -e IDLE_SECS="${IDLE_SECS:-600}" \
  -e SPARSE_SECS="${SPARSE_SECS:-120}" -e FANOUT_SECS="${FANOUT_SECS:-180}" \
  -e MIXED_SECS="${MIXED_SECS:-180}" -e SLOW_SECS="${SLOW_SECS:-120}" \
  -e TEARDOWN_SECS="${TEARDOWN_SECS:-600}" \
  -e FANOUT_DELIVERY_RATE="${FANOUT_DELIVERY_RATE:-1000}" \
  -e MIXED_BG_RATE="${MIXED_BG_RATE:-200}" \
  -e PAYLOAD_BYTES="${PAYLOAD_BYTES:-1024}" \
  perf-base:1 node /loadgen.mjs > "$OUTDIR/loadgen.log" 2>&1
GEN_RC=$?
set -e

python3 - "$OUTDIR" "$ARM" "$BIN_SHA" "$FEEDS" "$SUBS" "$TAG" "$GEN_RC" "$STORE_LAT" <<'PY'
import json, sys, os
out, arm, sha, feeds, subs, tag, rc, lat = sys.argv[1:9]
run = {}
p = os.path.join(out, "run.json")
if os.path.exists(p):
    run = json.load(open(p))
proc = []
pp = os.path.join(out, "proc.jsonl")
if os.path.exists(pp):
    for line in open(pp):
        line = line.strip()
        if line:
            try: proc.append(json.loads(line))
            except Exception: pass
manifest = {
    "arm": arm,
    "engine": {"a": "legacy", "b": "livefeed", "c": "final"}[arm],
    "commit": {"a": "1834b726", "b": "1834b726", "c": "3a8016e6"}[arm],
    "binary_sha256": sha,
    "store_latency_ms": int(lat),
    "cgroup": {"cpus": 1, "memory_bytes": 1 << 30, "nofile": 32768},
    "tag": tag,
    "gen_exit": int(rc),
    **run,
    "proc_series_points": len(proc),
    "proc_first": proc[0] if proc else None,
    "proc_last": proc[-1] if proc else None,
    "proc_peak_rss_kb": max((s.get("rss_kb", 0) for s in proc), default=0),
}
json.dump(manifest, open(os.path.join(out, "manifest.json"), "w"), indent=1)
print("manifest ->", os.path.join(out, "manifest.json"), "verdict", run.get("verdict"), "gen_rc", rc)
PY
exit "$GEN_RC"
