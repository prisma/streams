#!/bin/bash
# Workload-cert field ladder (bench/WORKLOAD-CERT-PLAN.md P3): one fra
# cell, 10k-project feeds, awsbench cert shape. Stages via env:
#   WC_SUBS_N=0      L1 writes-only (default)
#   WC_SUBS_N=5000.. L2/L3 subscriber rungs (SUBS gen holds the swarm)
#   WC_LIVE_HUB=0|1  default 1 (binary default since review round 2).
#                    Passed EXPLICITLY: platform env vars persist across
#                    deploys, so omitting it kept an old =0 on the service
#                    (L2i ran hub-off because of exactly that).
#   WC_GEN_REGION    deploy the generator out-of-region (see below)
#   WC_SECS=1800     steady window
# The server RESTARTS fresh per run (P0 discipline) on a fresh
# PATH_PREFIX keyspace with this run's 10k-project feeds.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
S=${SOAK_HOME:?set SOAK_HOME}
R=${WC_REGION:-eu-central-1}
# Review round 3: the GENERATOR may live in another region. The
# in-region hairpin path to cv-* buffers streaming responses even with
# X-Accel-Buffering: no (edge-repro addendum), so subscriber ladders
# run their gen OUT-of-region (WC_GEN_REGION) until the platform fixes
# it. Server stays in R.
GR=${WC_GEN_REGION:-$R}
# K generators on distinct source IPs (the edge caps concurrent
# connections PER SOURCE IP at ~1.2-2k): gen k parks subscribers
# [k*slice, (k+1)*slice) and only gen 0 writes. Regions round-robin
# from WC_GEN_REGIONS so the gens really are distinct hosts/IPs.
GENS=${WC_GENS:-1}
GEN_REGIONS=(${WC_GEN_REGIONS:-$GR})
export SOAK_RUN_ID=${SOAK_RUN_ID:-"wc-$(date -u +%Y%m%dT%H%M%SZ)"}
export BIN_TAG=$SOAK_RUN_ID
OUT="$S/results/$SOAK_RUN_ID"; mkdir -p "$OUT"
TENANTS=${WC_TENANTS:-10000}
SUB_TENANTS=${WC_SUB_TENANTS:-1000}
SUBS_N=${WC_SUBS_N:-0}
ACTIVE=${WC_ACTIVE:-100}
FANOUT=${WC_FANOUT:-10}
WPS=${WC_WPS:-1000}
SECS=${WC_SECS:-1800}
STAGE=${WC_STAGE:-L1}
CELL=mt-cell-1
echo "== wc-ladder $SOAK_RUN_ID stage=$STAGE tenants=$TENANTS subs=$SUBS_N wps=$WPS secs=$SECS"

if [ -s "$S/platform-token.txt" ]; then
  export PRISMA_API_TOKEN=$(cat "$S/platform-token.txt")
fi
P=$(cat "$S/proj-$R.txt")
echo "$SOAK_RUN_ID" > "$S/proj-$R.txt.campaign"
AUTH=$(cat "$S/auth.txt"); KEY=$(cat "$S/skey.txt")
BINID=$(cat "$S/binid.txt"); BINSEC=$(cat "$S/binsec.txt")
BINEP=$(cat "$S/artifact-endpoint.txt"); BINBKT=$(cat "$S/artifact-bucket.txt")
j() { python3 -c "import json;print(json.load(open('$S/bkey-$R.json'))['data']['$1'])"; }
MEMFLAGS=()
while IFS='=' read -r key value; do
  case "$key" in ''|\#*) continue ;; esac
  MEMFLAGS+=(--env "$key=$value")
done < "$ROOT/deploy/profiles/compute-1g.env"

# WC_DIET=1: the 10k-resident-stream memory diet (L1 finding: 456MB
# steady RSS vs the 600MB shed line left no headroom for absorber
# reservations — 36% bursty admit_shed). Later --env wins, so these
# override the profile. TAIL_RING off for writes-only stages; L3
# revisits (subscribers want the ring).
DIETFLAGS=()
if [ "${WC_DIET:-0}" = "1" ]; then
  DIETFLAGS=(
    --env SHARED_CACHE_BYTES=67108864
    --env POSTINGS_CACHE_BYTES=16777216
    --env HISTORY_CACHE_BYTES=16777216
    --env TAIL_RING_BYTES=0
    --env HANDLE_MAX_RESIDENT=6000
  )
fi

# Rung-level overrides that must BEAT the compute-1g profile: later
# --env wins, so anything the profile also pins (round-11.5 added
# SSE_MAX_CONNECTIONS=1200 and the feed budgets to it) goes here, at
# the very end of the deploy args. Feed budgets default to the
# PROFILE (round-12 E4: 64/32 MiB); pass WC_FEED_* only to experiment.
RUNGFLAGS=(--env SSE_MAX_CONNECTIONS=${WC_SSE_MAX:-2000})
[ -n "${WC_FEED_RING:-}" ]    && RUNGFLAGS+=(--env SSE_FEED_RING_BYTES=$WC_FEED_RING)
[ -n "${WC_FEED_TOTAL:-}" ]   && RUNGFLAGS+=(--env SSE_FEED_TOTAL_BYTES=$WC_FEED_TOTAL)
[ -n "${WC_FEED_PROJECT:-}" ] && RUNGFLAGS+=(--env SSE_FEED_PROJECT_BYTES=$WC_FEED_PROJECT)
# Cache-posture experiments (round-12 F4/F5: the L1 writes-only diet's
# full cache trim put WAN store reads on the append path — 137 ms ->
# 7.3 s p50; tune per rung instead of all-or-nothing).
[ -n "${WC_SHARED_CACHE:-}" ]   && RUNGFLAGS+=(--env SHARED_CACHE_BYTES=$WC_SHARED_CACHE)
[ -n "${WC_POSTINGS_CACHE:-}" ] && RUNGFLAGS+=(--env POSTINGS_CACHE_BYTES=$WC_POSTINGS_CACHE)
[ -n "${WC_HISTORY_CACHE:-}" ]  && RUNGFLAGS+=(--env HISTORY_CACHE_BYTES=$WC_HISTORY_CACHE)

"$HERE/build-upload.sh"

MTDIR="$S/wc-$SOAK_RUN_ID"
# The noisy-project class (leg 5) owns the LAST project outright, so
# it needs one project beyond the tenant population.
MTN=$TENANTS
[ "${WC_NOISY_FEEDS:-0}" -gt 0 ] && MTN=$((TENANTS + 1))
node "$HERE/mtgen.mjs" --projects "$MTN" --cell "$CELL" --out "$MTDIR"
python3 - "$MTDIR/feeds-bundle.json" "wc/$SOAK_RUN_ID/feeds.json" \
          "$MTDIR/tokens.json"       "wc/$SOAK_RUN_ID/tokens.json" <<'PY'
import boto3, os, sys
S = os.environ["SOAK_HOME"]
r = lambda f: open(os.path.join(S, f)).read().strip()
c = boto3.client("s3", endpoint_url=r("artifact-endpoint.txt"),
    aws_access_key_id=r("binid.txt"), aws_secret_access_key=r("binsec.txt"),
    region_name="auto")
bucket = r("artifact-bucket.txt")
for path, key in [(sys.argv[1], sys.argv[2]), (sys.argv[3], sys.argv[4])]:
    data = open(path, "rb").read()
    c.put_object(Bucket=bucket, Key=key, Body=data)
    assert len(c.get_object(Bucket=bucket, Key=key, Range="bytes=0-15")["Body"].read()) == 16
    print(f"uploaded {key} ({len(data)} bytes)")
PY

role_region() {
  case "$1" in
    gen)   echo "${GEN_REGIONS[0]}";;
    gen-*) local k=${1#gen-}; echo "${GEN_REGIONS[$((k % ${#GEN_REGIONS[@]}))]}";;
    *)     echo "$R";;
  esac
}
role_project() { cat "$S/proj-$(role_region "$1").txt"; }
svc_id() {
  local ROLE=$1 DR=$(role_region "$1") DP=$(role_project "$1")
  local SVCFILE="$S/projects/$DP/svc-$ROLE-$DR.txt"
  mkdir -p "$S/projects/$DP"
  if [ -f "$SVCFILE" ]; then
    local CACHED=$(cat "$SVCFILE")
    local LISTED=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$DP" 2>/dev/null \
                   | awk -v id="$CACHED" -v n="soak-$ROLE-$DR" '$1==id && $2==n {print $1}')
    [ -z "$LISTED" ] && rm -f "$SVCFILE"
  fi
  if [ ! -f "$SVCFILE" ]; then
    local EX=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$DP" 2>/dev/null \
               | awk -v n="soak-$ROLE-$DR" '$2==n {print $1; exit}') || true
    [ -n "$EX" ] && echo "$EX" > "$SVCFILE"
  fi
  [ -f "$SVCFILE" ] && cat "$SVCFILE" || true
}
deploy() {
  local ROLE=$1; shift
  local DIR="$S/app-${ROLE%%-*}-$R"   # gen-k roles share the gen app dir
  local DR=$(role_region "$ROLE") DP=$(role_project "$ROLE")
  local SVC=$(svc_id "$ROLE"); local SVCARG=()
  [ -n "$SVC" ] && SVCARG=(--service "$SVC")
  ( cd "$DIR"
    local DEPLOYED=0 OUT_CLI=""
    for ATT in $(seq 1 12); do
      if OUT_CLI=$(bunx --bun @prisma/compute-cli@0.39.0 deploy --project "$DP" ${SVCARG[@]+"${SVCARG[@]}"} \
        --region "$DR" --path . --http-port 8080 --service-name "soak-$ROLE-$DR" "$@" \
        2>&1 | grep -viE 'resolving|resolved|saved'); then DEPLOYED=1; break; fi
      echo "deploy attempt $ATT failed for $ROLE:" >&2; echo "$OUT_CLI" | tail -4 >&2; sleep 30
    done
    [ "$DEPLOYED" = 1 ] || { echo "deploy failed for $ROLE" >&2; exit 1; }
  )
  local SVCFILE="$S/projects/$DP/svc-$ROLE-$DR.txt"
  if [ ! -f "$SVCFILE" ]; then
    local NEWID=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$DP" 2>/dev/null \
                  | awk -v n="soak-$ROLE-$DR" '$2==n {print $1; exit}')
    [ -n "$NEWID" ] && echo "$NEWID" > "$SVCFILE"
  fi
  "$HERE/resolve-urls.sh" "$DR" "$ROLE" > /dev/null
}
verify_server_live() {
  local URL=$(cat "$S/url-server-$R.txt")
  for i in $(seq 1 30); do
    local CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$URL/livez" || true)
    [ "$CODE" = 200 ] && { echo "server live ($URL)"; return 0; }
    sleep 5
  done
  echo "SERVER NOT LIVE:" >&2; curl -s --max-time 10 "$URL/livez" | head -c 600 >&2 || true
  exit 1
}

echo "== server: fresh process, $TENANTS-project feeds, prefix $SOAK_RUN_ID"
deploy server \
  --env SERVER_BINARY_S3_KEY="bin/streams-$BIN_TAG-x64" \
  --env BIN_S3_ENDPOINT=$BINEP --env BIN_S3_BUCKET=$BINBKT --env BIN_S3_REGION=auto \
  --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
  --env SLATE_S3_ENDPOINT="$(j endpoint)" --env SLATE_S3_BUCKET="$(j bucketName)" \
  --env SLATE_S3_REGION=auto \
  --env SLATE_S3_ACCESS_KEY_ID="$(j accessKeyId)" \
  --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
  --env AUTH_TOKEN="$AUTH" \
  --env PATH_PREFIX="$SOAK_RUN_ID" --env INSTANCE_NAME=streams-1 \
  --env INITIAL_SHARDS=${WC_SHARDS:-4} \
  --env WAL_GROUP_COMMIT=1 --env WAL_FLUSH_GAP_MS=10 --env FLUSH_INTERVAL_MS=25 \
  --env WAL_POST_ACK_GATHER_MS=6 --env FRAME_COMPRESS=1 \
  --env ADMIT_MAX_INFLIGHT=${WC_ADMIT:-512} --env ADMIT_MAX_INFLIGHT_PER_STREAM=256 \
  --env LIMIT_RECS_PER_SEC=100000 \
  --env ABSORB_BYTES=${WC_ABSORB_BYTES:-4194304} --env ABSORB_AGE_SECS=${WC_ABSORB_AGE:-60} \
  --env ABSORB_PASS_BYTES=${WC_ABSORB_PASS:-67108864} --env TRIM_PER_OP=65536 \
  --env ABSORB_PACE_WINDOW_MS=${WC_PACE_WINDOW:-50} --env ABSORB_PACE_MS=${WC_PACE_MS:-0} \
  --env ABSORB_READ_PAR=${WC_READ_PAR:-8} \
  --env ADMIT_RSS_SHED_MB=${WC_RSS_SHED:-600} \
  --env MAX_RECORD_PAYLOAD_BYTES=${WC_RECORD_CEILING:-131072} \
  --env POOL_IDLE_SECS=4 --env KEEP_AWAKE=1 --env CELL_ID="$CELL" \
  --env PROJECT_ID=proj-mt-deploy \
  --env SLATEDB_RT_THREADS=${WC_SLATE_RT:-2} \
  --env STREAMS_AUTH_MODE=enforce \
  --env STREAMS_AUTH_ISSUER=https://auth.prisma.io \
  --env STREAMS_AUTH_KEYS_FILE=/tmp/feeds/keys.json \
  --env STREAMS_AUTH_POLICY_FILE=/tmp/feeds/policies.json \
  --env STREAMS_AUTH_GRANTS_FILE=/tmp/feeds/grants.json \
  --env STREAMS_AUTH_REFRESH_SECS=60 \
  --env FEEDS_S3_KEY="wc/$SOAK_RUN_ID/feeds.json" \
  ${MEMFLAGS[@]+"${MEMFLAGS[@]}"} \
  ${DIETFLAGS[@]+"${DIETFLAGS[@]}"} \
  ${RUNGFLAGS[@]+"${RUNGFLAGS[@]}"}
verify_server_live
curl -sf --max-time 20 -H "authorization: Bearer $AUTH" \
  "$(cat "$S/url-server-$R.txt")/v1/debug/load" > "$OUT/server-before-$STAGE.json" || true

echo "== gen: cert stage $STAGE (gens=$GENS regions=${GEN_REGIONS[*]})"
SLICE=$(( (SUBS_N + GENS - 1) / GENS ))
for k in $(seq 0 $((GENS - 1))); do
  ROLE=gen; [ "$k" -gt 0 ] && ROLE="gen-$k"
  OFFSET=$(( k * SLICE )); N=$SLICE
  [ $(( OFFSET + N )) -gt "$SUBS_N" ] && N=$(( SUBS_N - OFFSET ))
  [ "$N" -lt 0 ] && N=0
  W=1; [ "$k" -gt 0 ] && W=0
  echo "   gen $k ($ROLE @ $(role_region "$ROLE")): subs $N from $OFFSET, writers=$W"
  deploy "$ROLE" \
    --env AWSBENCH_S3_KEY="bin/awsbench-$BIN_TAG-x64" \
    --env S3_ENDPOINT=$BINEP --env S3_BUCKET=$BINBKT --env S3_REGION=auto \
    --env S3_ACCESS_KEY_ID="$BINID" --env S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env BENCH_SYSTEM=prisma --env BENCH_SHAPE=cert \
    --env BENCH_TARGET="$(cat "$S/url-server-$R.txt")" \
    --env TOKENS_S3_KEY="wc/$SOAK_RUN_ID/tokens.json" \
    --env BENCH_CERT_TENANTS="$TENANTS" --env BENCH_CERT_SUB_TENANTS="$SUB_TENANTS" \
    --env BENCH_CERT_SUBS_N="$N" --env BENCH_CERT_SUB_OFFSET="$OFFSET" --env BENCH_CERT_WRITERS="$W" \
    --env BENCH_CERT_ACTIVE="$ACTIVE" \
    --env BENCH_CERT_FANOUT_ACTIVE="$FANOUT" --env BENCH_CERT_WINDOW_MS=5000 \
    --env BENCH_CERT_CONNECT_CONC=${WC_CONNECT_CONC:-48} \
    --env BENCH_CERT_WPS="$WPS" --env BENCH_CERT_SECS="$SECS" \
    --env BENCH_CERT_SETTLE_SECS="${WC_SETTLE:-60}" \
    --env BENCH_CERT_FEEDS_PER_PROJECT="${WC_FPP:-1}" \
    --env BENCH_CERT_NOISY_FEEDS="${WC_NOISY_FEEDS:-0}" \
    --env BENCH_CERT_NOISY_SUBS_PER="${WC_NOISY_SUBS:-2}" \
    --env BENCH_CERT_NOISY_WPS="${WC_NOISY_WPS:-100}" \
    --env BENCH_CERT_NOISY_RECORD_BYTES="${WC_NOISY_BYTES:-8192}" \
    --env BENCH_WIDE_SETUP_CONC=64 --env BENCH_RECORD_BYTES="${WC_RECORD_BYTES:-1024}" --env BENCH_BATCH=1 \
    --env BENCH_HOLD=1 --env BENCH_START_GATED=true \
    --env AUTH_TOKEN="$AUTH" --env STREAM_KEY="$KEY" \
    --env BENCH_STREAM="wc$STAGE-" --env KEEP_AWAKE=1
done

gen_url() { # gen index -> stats URL
  if [ "$1" = 0 ]; then cat "$S/url-gen-$(role_region gen).txt"
  else cat "$S/url-gen-$1-$(role_region "gen-$1").txt"; fi
}

# Barrier: every gen parks its slice ungated, then waits for POST
# /start. Release ALL gens in one pass only when each reports its
# slice parked (subsOpen in the "parking" status line) — the release
# starts every gen's measurement clock, so deadline+settle line up.
if [ "$SUBS_N" -gt 0 ]; then
  echo "== barrier: waiting for $SUBS_N subscriptions across $GENS gens"
  PARK_DEADLINE=$(( $(date +%s) + 2700 ))
  while :; do
    TOTAL=0 ALLREADY=1
    for k in $(seq 0 $((GENS - 1))); do
      WANT=$SLICE
      [ $(( k * SLICE + SLICE )) -gt "$SUBS_N" ] && WANT=$(( SUBS_N - k * SLICE ))
      [ "$WANT" -le 0 ] && continue
      GOT=$(curl -sf --max-time 10 "$(gen_url "$k")/" 2>/dev/null \
            | grep -o '"subsOpen":[0-9]*' | tail -1 | cut -d: -f2) || GOT=0
      GOT=${GOT:-0}
      TOTAL=$(( TOTAL + GOT ))
      [ "$GOT" -lt $(( WANT * 98 / 100 )) ] && ALLREADY=0
    done
    echo "   parked $TOTAL / $SUBS_N"
    [ "$ALLREADY" = 1 ] && break
    if [ "$(date +%s)" -gt "$PARK_DEADLINE" ]; then
      echo "PARK BARRIER TIMED OUT at $TOTAL/$SUBS_N" >&2
      exit 1
    fi
    sleep 20
  done
fi
echo "== releasing all gens (subscriber gens first, writer last)"
for k in $(seq $((GENS - 1)) -1 0); do
  curl -sf --max-time 10 -X POST "$(gen_url "$k")/start" > /dev/null || \
    { echo "release failed for gen $k" >&2; exit 1; }
done

GURL=$(gen_url 0)   # the writer gen anchors cert_done
SURL=$(cat "$S/url-server-$R.txt")
DEADLINE=$(( $(date +%s) + SECS + 1800 ))
# RSS timeline: correlate memory peaks with shed windows.
( while :; do
    TS=$(date +%s)
    LOAD=$(curl -sf --max-time 8 -H "authorization: Bearer $AUTH" "$SURL/v1/debug/load" 2>/dev/null || echo "{}")
    echo "{\"ts\":$TS,\"load\":$LOAD}" >> "$OUT/rss-timeline-$STAGE.jsonl"
    sleep 10
  done ) &
RSSPID=$!
trap 'kill $RSSPID 2>/dev/null' EXIT
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  sleep 30
  # EVERY gen must reach cert_done (each carries its slice's exact
  # reconciliation); collect as they land, finish when all have.
  ALLDONE=1
  for k in $(seq 0 $((GENS - 1))); do
    F="$OUT/stage-$STAGE-gen$k.json"
    [ -s "$F" ] && grep -q '"cert_done"' "$F" && continue
    BODY=$(curl -sf --max-time 15 "$(gen_url "$k")/" || true)
    if [ -n "$BODY" ] && echo "$BODY" | grep -q '"cert_done"'; then
      echo "$BODY" > "$F"
    else
      ALLDONE=0
    fi
  done
  if [ "$ALLDONE" = 1 ]; then
    cp "$OUT/stage-$STAGE-gen0.json" "$OUT/stage-$STAGE.json"
    curl -sf --max-time 20 -H "authorization: Bearer $AUTH" \
      "$(cat "$S/url-server-$R.txt")/v1/debug/load" > "$OUT/server-after-$STAGE.json" || true
    echo "WC_STAGE_DONE $STAGE"
    exit 0
  fi
done
echo "stage $STAGE TIMED OUT" >&2
curl -sf --max-time 15 "$GURL/" > "$OUT/stage-$STAGE.partial.json" || true
exit 1
