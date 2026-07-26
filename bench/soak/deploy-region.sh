#!/bin/bash
# Deploy one region's soak pair: streams server, then a co-located generator.
#
#   ./deploy-region.sh <region> server
#   ./deploy-region.sh <region> gen
#
# The generator sits IN the region under test. That is the whole point of
# the topology: a generator run from the operator's laptop measures the
# operator's distance to the region, not what Streams costs its callers.
#
# Everything secret lives in $SOAK_HOME, which MUST be outside the repo
# (RUNBOOK section 12). Expected layout:
#
#   $SOAK_HOME/platform-token.txt   Prisma platform API token
#   $SOAK_HOME/auth.txt             AUTH_TOKEN for the streams servers
#   $SOAK_HOME/skey.txt             stream encryption key
#   $SOAK_HOME/binid.txt            artifact-bucket access key id
#   $SOAK_HOME/binsec.txt           artifact-bucket secret
#   $SOAK_HOME/proj-<region>.txt    Compute project id
#   $SOAK_HOME/bkey-<region>.json   Prisma Bucket key response (management API)
#   $SOAK_HOME/app-server-<region>/ copy of deploy/app-server (bun install'd)
#   $SOAK_HOME/app-gen-<region>/    copy of deploy/app-gen    (bun install'd)
#
# Run regions SEQUENTIALLY. Parallel `bunx` calls race on the shared
# package cache and fail with EEXIST (deploy/README.md).
set -euo pipefail

R=${1:?region}; ROLE=${2:?role: server|gen}
S=${SOAK_HOME:?set SOAK_HOME to a scratch dir outside the repo}
BENCH_TIERS=${BENCH_TIERS:-1,2,4,8,12,16,24,32,48,64}
BENCH_SECS=${BENCH_SECS:-180}
BIN_TAG=${BIN_TAG:-soak}
BINEP=${ARTIFACT_ENDPOINT:-https://t3.storage.dev}
BINBKT=${ARTIFACT_BUCKET:-prisma-streams-slatedb-sin}

export PRISMA_API_TOKEN=$(cat "$S/platform-token.txt")
P=$(cat "$S/proj-$R.txt")
AUTH=$(cat "$S/auth.txt"); KEY=$(cat "$S/skey.txt")
BINID=$(cat "$S/binid.txt"); BINSEC=$(cat "$S/binsec.txt")
j() { python3 -c "import json;print(json.load(open('$S/bkey-$R.json'))['data']['$1'])"; }

# Reuse the service across redeploys when we already know its id. Note the
# id must come from `services list` or a previous run of this script --
# `deploy` prints a VERSION id (cpv_), not a service id (cps_).
SVCFILE=$S/svc-$ROLE-$R.txt
SVCARG=(); [ -f "$SVCFILE" ] && SVCARG=(--service "$(cat "$SVCFILE")")

if [ "$ROLE" = server ]; then
  cd "$S/app-server-$R"
  OUT=$(bunx --bun @prisma/compute-cli deploy --project "$P" "${SVCARG[@]}" \
    --region "$R" --path . --http-port 8080 --service-name "soak-server-$R" \
    --env SERVER_BINARY_S3_KEY="bin/streams-$BIN_TAG-x64" \
    --env BIN_S3_ENDPOINT=$BINEP --env BIN_S3_BUCKET=$BINBKT --env BIN_S3_REGION=auto \
    --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env SLATE_S3_ENDPOINT="$(j endpoint)" --env SLATE_S3_BUCKET="$(j bucketName)" \
    --env SLATE_S3_REGION=auto \
    --env SLATE_S3_ACCESS_KEY_ID="$(j accessKeyId)" \
    --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
    --env AUTH_TOKEN="$AUTH" \
    --env PATH_PREFIX=soak --env INSTANCE_NAME=streams-1 \
    --env INITIAL_SHARDS=4 \
    --env WAL_GROUP_COMMIT=1 --env WAL_FLUSH_GAP_MS=10 --env FLUSH_INTERVAL_MS=25 \
    --env FRAME_COMPRESS=1 \
    --env L0_SST_SIZE_BYTES=16777216 --env MAX_UNFLUSHED_BYTES=33554432 \
    --env L0_MAX_SSTS=64 --env MANIFEST_POLL_MS=1000 \
    --env COMPACTOR_POLL_MS=500 --env COMPACTOR_MAX_CONCURRENT=2 \
    --env SHARED_CACHE_BYTES=67108864 --env SLATEDB_RT_THREADS=2 \
    --env ADMIT_MAX_INFLIGHT=512 --env ADMIT_MAX_INFLIGHT_PER_STREAM=256 \
    --env ADMIT_RSS_SHED_MB=600 \
    --env ABSORB_BYTES=4194304 --env ABSORB_AGE_SECS=60 \
    --env ABSORB_PASS_BYTES=67108864 --env TRIM_PER_OP=65536 \
    --env POOL_IDLE_SECS=4 --env KEEP_AWAKE=1 \
    2>&1 | grep -viE 'resolving|resolved|saved')
else
  TARGET=$(cat "$S/url-server-$R.txt")
  cd "$S/app-gen-$R"
  OUT=$(bunx --bun @prisma/compute-cli deploy --project "$P" "${SVCARG[@]}" \
    --region "$R" --path . --http-port 8080 --service-name "soak-gen-$R" \
    --env AWSBENCH_S3_KEY="bin/awsbench-$BIN_TAG-x64" \
    --env S3_ENDPOINT=$BINEP --env S3_BUCKET=$BINBKT --env S3_REGION=auto \
    --env S3_ACCESS_KEY_ID="$BINID" --env S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env BENCH_SYSTEM=prisma --env BENCH_SHAPE=tiers --env BENCH_TARGET="$TARGET" \
    --env AUTH_TOKEN="$AUTH" --env STREAM_KEY="$KEY" \
    --env BENCH_STREAM="soak-$R" \
    --env BENCH_TIERS="$BENCH_TIERS" --env BENCH_SECS="$BENCH_SECS" \
    --env BENCH_BATCH=10 --env BENCH_RECORD_BYTES=1024 --env BENCH_CONSUME=true \
    --env BENCH_HOLD=1 --env KEEP_AWAKE=1 \
    2>&1 | grep -viE 'resolving|resolved|saved')
fi

echo "$OUT" | grep -E 'New version|error' | sed "s/^/$ROLE-$R: /"
SVC=$(echo "$OUT" | grep -oE 'cps_[a-z0-9]+' | head -1)
[ -n "$SVC" ] && echo "$SVC" > "$SVCFILE"

# Preview domains belong to a VERSION, not a service: a redeploy retires the
# old domain (it then answers 503, which reads like a boot failure). Always
# re-resolve from the running version instead of caching the deploy output.
"$(dirname "$0")/resolve-urls.sh" "$R" "$ROLE"
