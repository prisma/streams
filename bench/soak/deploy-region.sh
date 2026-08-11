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
# Absolute script dir: the deploy cd's into the app directory, which
# breaks any later $(dirname "$0") relative lookup.
HERE=$(cd "$(dirname "$0")" && pwd)
# Memory-survival profile (OOM review): every deploy sources the ONE
# checked-in profile so no script can silently restore the unsafe
# posture. Flags are built as a REAL ARRAY — scalar expansion is
# shell-dependent (zsh does not word-split unquoted scalars) and once
# passed the whole profile as a single argument. Opt out only with
# UNSAFE_LEGACY_MEMORY_PROFILE=1.
MEMPROFILE="$(cd "$HERE/../.." && pwd)/deploy/profiles/compute-1g.env"
MEMFLAGS=()
if [ "${UNSAFE_LEGACY_MEMORY_PROFILE:-0}" = "1" ]; then
  echo "WARNING: UNSAFE_LEGACY_MEMORY_PROFILE=1 — deploying WITHOUT the compute-1g memory profile" >&2
else
  while IFS='=' read -r key value; do
    case "$key" in ''|\#*) continue ;; esac
    MEMFLAGS+=(--env "$key=$value")
  done < "$MEMPROFILE"
fi

S=${SOAK_HOME:?set SOAK_HOME to a scratch dir outside the repo}
BENCH_TIERS=${BENCH_TIERS:-1,2,4,8,12,16,24,32,48,64}
BENCH_SECS=${BENCH_SECS:-180}
# R26-9 campaign shape:
#  SOAK_STREAMS_N          streams per region (default 32 — enough route
#                          hashes to cover every physical shard; 1 stream
#                          exercised exactly one segment of one shard)
#  SOAK_LIMIT_RECS_PER_SEC per-stream record limiter for the campaign
#                          (default 100000 — RAISED so the ordinary
#                          limiter does not mask the maintenance bound;
#                          the 2026-08-11 plateau was the 5k default,
#                          not backpressure). Recorded in the report.
#  SOAK_GATED              generators start parked and wait for the
#                          campaign's synchronized POST /start (default
#                          true; set false for ad-hoc single deploys)
BIN_TAG=${BIN_TAG:-soak}
# Keyspace prefix: override per run (soak2r1, soak2r2, ...) so repeated
# runs start from a fresh keyspace without re-provisioning buckets.
SOAK_PREFIX=${SOAK_PREFIX:-soak}
# Optional DNS override written by the wrapper before exec (soak3 finding:
# the platform forwarder mis-steers Tigris's geo-DNS). Literal \n escapes.
RESOLV_OVERRIDE=${RESOLV_OVERRIDE:-}
# Artifact bucket/endpoint: prefer the campaign's provisioned values in
# $SOAK_HOME (written by provisioning) over env, over the last-resort
# constant. A stale constant here cost an hour on 2026-08-03: the
# wrapper stat'ed the right key in the WRONG (dead) bucket and the
# resulting AccessDenied read like an egress/DNS problem.
BINEP=${ARTIFACT_ENDPOINT:-$( [ -s "$S/artifact-endpoint.txt" ] && cat "$S/artifact-endpoint.txt" || echo https://fly.storage.tigris.dev )}
BINBKT=${ARTIFACT_BUCKET:-$( [ -s "$S/artifact-bucket.txt" ] && cat "$S/artifact-bucket.txt" || echo prisma-streams-slatedb-sin )}

# Prefer the CLI's own stored login (it auto-refreshes). A static token
# file is only a fallback — a copied OAuth access token expires in about
# an hour, which silently killed every deploy of a soak run mid-campaign
# (2026-07-27: five WARN "failures" that were all a 401 in disguise).
if [ -s "$S/platform-token.txt" ]; then
  export PRISMA_API_TOKEN=$(cat "$S/platform-token.txt")
fi
P=$(cat "$S/proj-$R.txt")
# Campaign-scope guard (the destroyed-specimen lesson): every deploy
# stamps the project file with this campaign's RUN_ID. teardown.sh
# refuses projects whose stamp does not match, and refuses anything
# listed in $SOAK_HOME/preserve.txt regardless.
RUN_ID=${SOAK_RUN_ID:?set SOAK_RUN_ID (campaign.sh generates one)}
echo "$RUN_ID" > "$S/proj-$R.txt.campaign"
AUTH=$(cat "$S/auth.txt"); KEY=$(cat "$S/skey.txt")
BINID=$(cat "$S/binid.txt"); BINSEC=$(cat "$S/binsec.txt")
j() { python3 -c "import json;print(json.load(open('$S/bkey-$R.json'))['data']['$1'])"; }

# Reuse the service across redeploys when we already know its id. Note the
# id must come from `services list` or a previous run of this script --
# `deploy` prints a VERSION id (cpv_), not a service id (cps_).
# R25-G: service caches are PROJECT-scoped. The flat cache matched a
# service BY NAME across projects, so a superseded campaign's cps_ id
# was fed to a new project's deploy — "App cps_... belongs to project
# proj_OLD" — which permanently blocked us-east-1 and eu-central-1 on
# the 2026-08-11 run.
mkdir -p "$S/projects/$P"
SVCFILE=$S/projects/$P/svc-$ROLE-$R.txt
# Resolve the service id BEFORE deploying when we don't have it cached:
# `deploy` prints a VERSION id (cpv_), never a service id, and deploying
# by --service-name a second time fails with "already exists". `services
# list` is the only source of truth (deploy/README.md footgun).
if [ ! -f "$SVCFILE" ]; then
  EXISTING=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null \
             | awk -v n="soak-$ROLE-$R" '$2==n {print $1; exit}') || true
  [ -n "$EXISTING" ] && echo "$EXISTING" > "$SVCFILE"
fi
# ${arr[@]+...} form: macOS bash 3.2 treats an empty array as unbound
# under `set -u`, so a plain expansion aborts the whole deploy.
# Revalidate: a cached id must appear in THIS project's list with the
# expected name, else drop it and re-resolve.
if [ -f "$SVCFILE" ]; then
  CACHED=$(cat "$SVCFILE")
  LISTED=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null \
           | awk -v id="$CACHED" -v n="soak-$ROLE-$R" '$1==id && $2==n {print $1}')
  if [ -z "$LISTED" ]; then
    echo "stale service cache for $ROLE-$R (id $CACHED not in $P); re-resolving" >&2
    rm -f "$SVCFILE"
  fi
fi
SVCARG=(); [ -f "$SVCFILE" ] && SVCARG=(--service "$(cat "$SVCFILE")")
# Empty --env values are rejected by the CLI; only pass when set.
RESOLVARG=(); [ -n "$RESOLV_OVERRIDE" ] && RESOLVARG=(--env "RESOLV_OVERRIDE=$RESOLV_OVERRIDE")
# Field-gate split knobs: the 20-check corpus drives a REAL scaler split,
# which production thresholds would take hours to trigger. Opt-in via
# SCALE_KNOBS=1 (campaign deploys), absent otherwise.
SCALEARG=()
if [ "${SCALE_KNOBS:-0}" = "1" ]; then
  SCALEARG=(--env SCALE_EVAL_SECS=5 --env SCALE_RATE_WINDOW_SECS=10 \
            --env SCALE_HOT_PCT=1 --env SCALE_HOT_EVALS=1 --env SCALE_COOLDOWN_SECS=5)
fi

if [ "$ROLE" = server ]; then
  cd "$S/app-server-$R"
  # MANIFEST_POLL_MS / COMPACTOR_POLL_MS are deliberately NOT set: the
  # binary defaults (2000/2500) ARE the field idle-cost posture
  # (docs/TIGRIS-404-COST.md); overriding them here re-arms the idle
  # 404-probe tax the stretch removed.
  OUT=$(bunx --bun @prisma/compute-cli@0.39.0 deploy --project "$P" ${SVCARG[@]+"${SVCARG[@]}"} \
    --region "$R" --path . --http-port 8080 --service-name "soak-server-$R" \
    --env SERVER_BINARY_S3_KEY="bin/streams-$BIN_TAG-x64" \
    --env BIN_S3_ENDPOINT=$BINEP --env BIN_S3_BUCKET=$BINBKT --env BIN_S3_REGION=auto \
    --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env SLATE_S3_ENDPOINT="$(j endpoint)" --env SLATE_S3_BUCKET="$(j bucketName)" \
    --env SLATE_S3_REGION=auto \
    --env SLATE_S3_ACCESS_KEY_ID="$(j accessKeyId)" \
    --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
    --env AUTH_TOKEN="$AUTH" \
    --env PATH_PREFIX="$SOAK_PREFIX" --env INSTANCE_NAME=streams-1 \
    --env INITIAL_SHARDS=4 \
    --env WAL_GROUP_COMMIT=1 --env WAL_FLUSH_GAP_MS=10 --env FLUSH_INTERVAL_MS=25 \
    --env WAL_POST_ACK_GATHER_MS="${WAL_POST_ACK_GATHER_MS:-6}" \
    --env TAIL_RING_BYTES="${TAIL_RING_BYTES:-0}" \
    --env STREAMS_DEBUG_TIMING="${STREAMS_DEBUG_TIMING:-0}" \
    --env FRAME_COMPRESS=1 \
    --env COMPACTOR_MAX_CONCURRENT=2 \
    --env ADMIT_MAX_INFLIGHT=512 --env ADMIT_MAX_INFLIGHT_PER_STREAM=256 \
    --env LIMIT_RECS_PER_SEC="${SOAK_LIMIT_RECS_PER_SEC:-100000}" \
    --env ABSORB_BYTES=4194304 --env ABSORB_AGE_SECS=60 \
    --env ABSORB_PASS_BYTES=67108864 --env TRIM_PER_OP=65536 \
    --env POOL_IDLE_SECS=4 --env KEEP_AWAKE=1 \
    ${MEMFLAGS[@]+"${MEMFLAGS[@]}"} \
    ${SCALEARG[@]+"${SCALEARG[@]}"} \
    ${RESOLVARG[@]+"${RESOLVARG[@]}"} \
    2>&1 | grep -viE 'resolving|resolved|saved')
else
  TARGET=$(cat "$S/url-server-$R.txt")
  # An empty BENCH_TIERS env still selects the (empty) tier ramp in
  # awsbench and runs nothing — only pass the var when it has tiers.
  TIERARG=(); [ -n "$BENCH_TIERS" ] && TIERARG=(--env "BENCH_TIERS=$BENCH_TIERS")
  cd "$S/app-gen-$R"
  OUT=$(bunx --bun @prisma/compute-cli@0.39.0 deploy --project "$P" ${SVCARG[@]+"${SVCARG[@]}"} \
    --region "$R" --path . --http-port 8080 --service-name "soak-gen-$R" \
    --env AWSBENCH_S3_KEY="bin/awsbench-$BIN_TAG-x64" \
    --env S3_ENDPOINT=$BINEP --env S3_BUCKET=$BINBKT --env S3_REGION=auto \
    --env S3_ACCESS_KEY_ID="$BINID" --env S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env BENCH_SYSTEM=prisma --env BENCH_SHAPE="${SOAK_BENCH_SHAPE:-tiers}" --env BENCH_TARGET="$TARGET" \
    --env BENCH_CONC="${SOAK_BENCH_CONC:-1}" \
    --env AUTH_TOKEN="$AUTH" --env STREAM_KEY="$KEY" \
    --env BENCH_STREAM="soak-$R" \
    --env BENCH_STREAMS_N="${SOAK_STREAMS_N:-32}" \
    --env BENCH_START_GATED="${SOAK_GATED:-true}" \
    ${TIERARG[@]+"${TIERARG[@]}"} --env BENCH_SECS="$BENCH_SECS" \
    --env BENCH_BATCH=10 --env BENCH_RECORD_BYTES=1024 --env BENCH_CONSUME=true \
    --env BENCH_HOLD=1 --env KEEP_AWAKE=1 \
    ${RESOLVARG[@]+"${RESOLVARG[@]}"} \
    2>&1 | grep -viE 'resolving|resolved|saved')
fi

echo "$OUT" | grep -E 'New version|error' | sed "s/^/$ROLE-$R: /" || true
# `deploy` prints a VERSION id (cpv_), not a service id — the id we need
# comes from `services list`, matched by name (deploy/README.md footgun).
if [ ! -f "$SVCFILE" ]; then
  SVC=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null         | awk -v n="soak-$ROLE-$R" '$2==n {print $1; exit}')
  [ -n "$SVC" ] && echo "$SVC" > "$SVCFILE"
fi

# Preview domains belong to a VERSION, not a service: a redeploy retires the
# old domain (it then answers 503, which reads like a boot failure). Always
# re-resolve from the running version instead of caching the deploy output.
"$HERE/resolve-urls.sh" "$R" "$ROLE"
