#!/bin/zsh
# HISTORICAL (2026-07 sinmax campaign) — DO NOT DEPLOY FROM THIS FILE.
# It predates the compute-1g memory profile (deploy/profiles/), carries
# the pre-OOM-review posture (SLATEDB_RT_THREADS=2, shed 550, old
# L0/cache values) and hard-codes a dead scratchpad path. Kept as the
# campaign record only. Active Compute deploys: bench/soak/,
# bench/fleet/, scripts/bench-fra-ab.sh — all of which source the
# canonical memory profile.
if [ "${UNSAFE_LEGACY_MEMORY_PROFILE:-0}" != "1" ]; then
  echo "REFUSING: historical script with the pre-OOM-review memory posture." >&2
  echo "Use the active deploy scripts, or set UNSAFE_LEGACY_MEMORY_PROFILE=1 deliberately." >&2
  exit 1
fi
# 4-instance Compute cluster for SCALING.md's on-Compute validation.
# One service per ordinal (the ring's ordinal set is streams-1..N).
# usage: cluster-deploy.sh [up|status]
set -e
S=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/92de44c8-b33b-41e2-90d2-cd3f47beaa72/scratchpad
export PRISMA_API_TOKEN=$(cat $S/platform-token.txt)
P=$(cat $S/sinmax/proj.txt)
R=ap-southeast-1
AUTH=$(cat $S/sinmax/auth.txt)
BINEP=https://t3.storage.dev
BINBKT=prisma-streams-slatedb-sin
BINID=$(grep -oE '\bS3_ACCESS_KEY_ID=tid_[A-Za-z0-9_-]+' $S/deploy-obs.sh | head -1 | cut -d= -f2)
BINSEC=$(grep -oE "S3_SECRET_ACCESS_KEY='[^']+'" $S/deploy-obs.sh | head -1 | sed "s/S3_SECRET_ACCESS_KEY='//; s/'\$//")
j() { python3 -c "import json;print(json.load(open('$S/sinmax/bkey.json'))['data']['$1'])"; }

for i in 1 2 3 4; do
  SVCFILE=$S/scale-docker/cluster-svc-$i.txt
  SVCARG=()
  [ -f "$SVCFILE" ] && SVCARG=(--service $(cat $SVCFILE))
  cd $S/app-cbench-server
  OUT=$(bunx --bun @prisma/compute-cli deploy --project $P ${SVCARG[@]} --region $R --path . --http-port 8080 \
    --service-name "scale-cluster-$i" \
    --env SERVER_BINARY_S3_KEY=bin/streams-cluster-x64 \
    --env BIN_S3_ENDPOINT=$BINEP --env BIN_S3_BUCKET=$BINBKT --env BIN_S3_REGION=auto \
    --env BIN_S3_ACCESS_KEY_ID=$BINID --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env SLATE_S3_ENDPOINT=https://t3.storage.dev --env SLATE_S3_BUCKET=$(j bucketName) --env SLATE_S3_REGION=auto \
    --env SLATE_S3_ACCESS_KEY_ID=$(j accessKeyId) --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
    --env AUTH_TOKEN="$AUTH" \
    --env PATH_PREFIX=cluster1 --env FLEET_PREFIX=cluster1-fleet \
    --env INSTANCE_NAME=streams-$i \
    --env FLEET_MIN=4 --env FLEET_MAX=4 \
    --env INITIAL_SHARDS=8 \
    --env WAL_GROUP_COMMIT=1 --env WAL_FLUSH_GAP_MS=10 --env FLUSH_INTERVAL_MS=25 \
    --env L0_SST_SIZE_BYTES=16777216 --env MAX_UNFLUSHED_BYTES=33554432 \
    --env L0_MAX_SSTS=64 \
    --env COMPACTOR_MAX_CONCURRENT=2 --env SHARED_CACHE_BYTES=67108864 \
    --env ADMIT_MAX_INFLIGHT=512 --env ADMIT_MAX_INFLIGHT_PER_STREAM=256 --env ADMIT_RSS_SHED_MB=550 \
    --env ABSORB_BYTES=4194304 --env ABSORB_AGE_SECS=60 --env ABSORB_PASS_BYTES=67108864 --env TRIM_PER_OP=65536 \
    --env SCALE_RATE_WINDOW_SECS=60 --env SCALE_COOLDOWN_SECS=60 --env SCALE_COLD_EVALS=12 \
    --env REBALANCE_RETURN_SECS=120 \
    --env POOL_IDLE_SECS=4 --env KEEP_AWAKE=1 --env FRAME_COMPRESS=1 \
    2>&1)
  echo "$OUT" | grep -E 'New version|service|error' | sed "s/^/cluster-$i: /"
  SVC=$(echo "$OUT" | grep -oE 'cps_[a-z0-9]+' | head -1)
  [ -n "$SVC" ] && echo "$SVC" > $SVCFILE
done
# Emit the instance-name -> URL map the driver/checker route with.
python3 - "$S" <<'PY'
import json, sys, os, re
S = sys.argv[1]
m = {}
for i in (1, 2, 3, 4):
    f = f"{S}/scale-docker/cluster-svc-{i}.txt"
    if os.path.exists(f):
        sid = open(f).read().strip().replace("cps_", "")
        m[f"streams-{i}"] = f"https://{sid}.sin.prisma.build"
open(f"{S}/scale-docker/cluster-urls.json", "w").write(json.dumps(m, indent=1))
print("CLUSTER_URLS:", json.dumps(m))
PY
