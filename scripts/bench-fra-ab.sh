#!/bin/zsh
# Single-instance saturation benchmark on Prisma Compute (the release gate
# of AWS-readyness.md §5). This is the EXACT harness that exposed the
# slate-codex OOM crash loop on 2026-07-21.
#
# Topology: one server instance, one generator instance pointed DIRECTLY at
# it (no LB, no router), same region, fresh data bucket per run provisioned
# via the management API.
#
# All identifiers/credentials come from the environment — nothing secret
# lives in this script:
#   PRISMA_API_TOKEN   management API token (also used to create buckets)
#   BENCH_PROJECT      project id (create one per region: `compute projects create`)
#   BENCH_REGION       e.g. eu-central-1
#   BENCH_SVC_SERVER   compute service id for the server
#   BENCH_SVC_GEN      compute service id for the generator
#   BENCH_URL_SERVER   https://<hash>.<region-code>.prisma.build for the server
#   BENCH_BIN_KEY      bin/<server binary object> in the hosting bucket
#   BIN_S3_ENDPOINT/BIN_S3_BUCKET/BIN_S3_ACCESS_KEY_ID/BIN_S3_SECRET_ACCESS_KEY
#                      binary-hosting bucket (distinct var names from data
#                      creds — project-scope env merge, AWS-readyness §6)
#   GEN_BIN_KEY        bin/<pilot binary object> (MODE=gen)
#   AUTH_TOKEN_FILE STREAM_KEY_FILE  files holding the service auth token and
#                      the benchmark stream key
#   BENCH_RUN          run label; the data bucket is named streams-bench-$BENCH_RUN
#
# Usage:
#   bench-fra-ab.sh provision   # create fresh bucket + key -> bench/bucket-$BENCH_RUN.json
#   bench-fra-ab.sh server      # deploy the server against that bucket
#   bench-fra-ab.sh gen         # deploy the generator at the server (starts load)
#   bench-fra-ab.sh sample <minutes> <out.jsonl>   # 20 s sampler
#   bench-fra-ab.sh stop        # stop load (gen scales to zero)
set -e
HERE=$(dirname "$0")
# Memory-survival profile (OOM review): every deploy sources the ONE
# checked-in profile so no script can silently restore the unsafe
# posture. Flags are built as a REAL ARRAY — scalar expansion is
# shell-dependent (zsh does not word-split unquoted scalars) and once
# passed the whole profile as a single argument. Opt out only with
# UNSAFE_LEGACY_MEMORY_PROFILE=1.
MEMPROFILE="$(cd "$HERE/.." && pwd)/deploy/profiles/compute-1g.env"
MEMFLAGS=()
if [ "${UNSAFE_LEGACY_MEMORY_PROFILE:-0}" = "1" ]; then
  echo "WARNING: UNSAFE_LEGACY_MEMORY_PROFILE=1 — deploying WITHOUT the compute-1g memory profile" >&2
else
  while IFS='=' read -r key value; do
    case "$key" in ''|\#*) continue ;; esac
    MEMFLAGS+=(--env "$key=$value")
  done < "$MEMPROFILE"
fi

BUCKET_JSON=$HERE/../bench/bucket-$BENCH_RUN.json

case $1 in
provision)
  curl -sf -X POST -H "Authorization: Bearer $PRISMA_API_TOKEN" -H "Content-Type: application/json" \
    -d "{\"projectId\":\"$BENCH_PROJECT\",\"name\":\"streams-bench-$BENCH_RUN\"}" \
    https://api.prisma.io/v1/buckets > /tmp/bkt.json
  BKT=$(python3 -c "import json;print(json.load(open('/tmp/bkt.json'))['data']['id'])")
  curl -sf -X POST -H "Authorization: Bearer $PRISMA_API_TOKEN" -H "Content-Type: application/json" \
    -d '{"role":"read_write","name":"bench"}' \
    "https://api.prisma.io/v1/buckets/$BKT/keys" > $BUCKET_JSON
  echo "bucket $(python3 -c "import json;print(json.load(open('$BUCKET_JSON'))['data']['bucketName'])") -> $BUCKET_JSON"
  ;;
server)
  j() { python3 -c "import json;print(json.load(open('$BUCKET_JSON'))['data']['$1'])" }
  # SURVIVAL POSTURE (OOM review): 4 shards (the 16-shard topology
  # multiplied absorber exposure 16x and killed preview.7), 8 MiB
  # per-gather cap under a 64 MiB PROCESS-WIDE budget with 2 concurrent
  # gathers, 4 SlateDB runtime threads (telemetry flushers must not
  # starve the history compactor), shed at 500 MB (600 was too close to
  # the ~740 MB platform kill line for inter-sample SST spikes), and
  # EVERY cache bound explicit. NOTE: a fresh namespace is REQUIRED for
  # a shard-count change — existing topology.json wins over
  # INITIAL_SHARDS.
  bunx --bun @prisma/compute-cli deploy --project $BENCH_PROJECT --service $BENCH_SVC_SERVER \
    --region $BENCH_REGION --path $HERE/../deploy/app-server --http-port 8080 \
    --env SERVER_BINARY_S3_KEY=$BENCH_BIN_KEY \
    --env BIN_S3_ENDPOINT=$BIN_S3_ENDPOINT --env BIN_S3_BUCKET=$BIN_S3_BUCKET --env BIN_S3_REGION=auto \
    --env BIN_S3_ACCESS_KEY_ID=$BIN_S3_ACCESS_KEY_ID --env BIN_S3_SECRET_ACCESS_KEY="$BIN_S3_SECRET_ACCESS_KEY" \
    --env SLATE_S3_ENDPOINT=$(j endpoint) --env SLATE_S3_BUCKET=$(j bucketName) --env SLATE_S3_REGION=auto \
    --env SLATE_S3_ACCESS_KEY_ID=$(j accessKeyId) --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
    --env AUTH_TOKEN="$(cat $AUTH_TOKEN_FILE)" \
    --env PATH_PREFIX=bench --env INSTANCE_NAME=bench-server \
    --env FLUSH_INTERVAL_MS=50 --env L0_MAX_SSTS_PER_KEY=0 --env MANIFEST_POLL_MS=1000 \
    --env INITIAL_SHARDS=4 --env ADMIT_MAX_INFLIGHT=256 \
    --env ABSORB_BYTES=4194304 --env ABSORB_AGE_SECS=300 --env ABSORB_PASS_BYTES=33554432 --env TRIM_PER_OP=8192 \
    --env KEEP_AWAKE=1 \
    ${MEMFLAGS[@]+"${MEMFLAGS[@]}"} \
    2>&1 | grep -E 'New version|error'
  ;;
gen)
  bunx --bun @prisma/compute-cli deploy --project $BENCH_PROJECT --service $BENCH_SVC_GEN \
    --region $BENCH_REGION --path $HERE/../deploy/app-gen --http-port 8080 \
    --env GEN_BINARY_S3_KEY=$GEN_BIN_KEY --env MODE=gen \
    --env S3_ENDPOINT=$BIN_S3_ENDPOINT --env S3_BUCKET=$BIN_S3_BUCKET --env S3_REGION=auto \
    --env S3_ACCESS_KEY_ID=$BIN_S3_ACCESS_KEY_ID --env S3_SECRET_ACCESS_KEY="$BIN_S3_SECRET_ACCESS_KEY" \
    --env AUTH_TOKEN="$(cat $AUTH_TOKEN_FILE)" --env STREAM_KEY="$(cat $STREAM_KEY_FILE)" \
    --env LB_URL=$BENCH_URL_SERVER --env STREAM_PREFIX=bench-$BENCH_RUN \
    --env STREAMS=32 --env CONC_START=128 --env CONC_MAX=128 --env RAMP_SECS=60 --env BATCH=16 \
    --env KEEP_AWAKE=1 \
    --unset-env BENCH_VERB --unset-env TARGET --unset-env BENCH_STREAM \
    2>&1 | grep -E 'New version|error'
  ;;
sample)
  MIN=${2:-16}; OUT=${3:-bench/samples-$BENCH_RUN.jsonl}
  AUTH=$(cat $AUTH_TOKEN_FILE)
  GENURL=$(echo $BENCH_SVC_GEN | sed "s/^cps_//"); GENURL="https://$GENURL.${BENCH_URL_SERVER#*.}"
  for i in $(seq 1 $(( MIN * 3 ))); do
    ts=$(date +%s)
    g=$(curl -s --max-time 15 $GENURL/ || echo '{}')
    t=$(curl -s --max-time 15 -H "Authorization: Bearer $AUTH" "$BENCH_URL_SERVER/v1/debug/timings?window=20" || echo '{}')
    st=$(curl -s --max-time 15 -H "Authorization: Bearer $AUTH" "$BENCH_URL_SERVER/v1/debug/store?window=20" || echo '{}')
    ld=$(curl -s --max-time 15 -H "Authorization: Bearer $AUTH" "$BENCH_URL_SERVER/v1/debug/load" || echo '{}')
    echo "{\"ts\":$ts,\"i\":$i,\"gen\":$g,\"timings\":$t,\"store\":$st,\"load\":$ld}" >> $OUT
    sleep 20
  done
  echo SAMPLING_DONE
  ;;
stop)
  bunx --bun @prisma/compute-cli deploy --project $BENCH_PROJECT --service $BENCH_SVC_GEN \
    --region $BENCH_REGION --path $HERE/../deploy/app-gen --http-port 8080 \
    --env MODE=gen --env STREAMS=0 --env CONC_START=0 --env CONC_MAX=0 \
    --unset-env KEEP_AWAKE 2>&1 | grep -E 'New version|error'
  ;;
*) echo "usage: $0 provision|server|gen|sample|stop"; exit 1 ;;
esac
