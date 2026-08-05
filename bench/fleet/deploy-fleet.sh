#!/bin/bash
# #113 fleet campaign, step 2: deploy. Four ordinal stream instances
# (streams-1..4; the LB's wake fallback constructs exactly these names),
# one pilot MODE=lb rendezvous router (the only client entry point), and
# — separately, when you are ready to observe — one pilot MODE=gen
# closed-loop generator whose concurrency doubles every RAMP_SECS.
#
#   bench/fleet/deploy-fleet.sh servers   # s1..s4, resolves URLs
#   bench/fleet/deploy-fleet.sh lb        # needs the 4 URLs resolved
#   bench/fleet/deploy-fleet.sh gen       # starts the ramp IMMEDIATELY
set -euo pipefail
S=${SOAK_HOME:?set SOAK_HOME}
STEP=${1:?servers|lb|gen}
P=$(cat "$S/proj-fleet.txt")
[ -s "$S/platform-token.txt" ] && export PRISMA_API_TOKEN=$(cat "$S/platform-token.txt")
BIN_TAG=${BIN_TAG:-freeze4}
PILOT_TAG=${PILOT_TAG:-fleet1}
REGION=eu-central-1
BINEP=$(cat "$S/artifact-endpoint.txt"); BINBKT=$(cat "$S/artifact-bucket.txt")
BINID=$(cat "$S/binid.txt"); BINSEC=$(cat "$S/binsec.txt")
AUTH=$(cat "$S/auth.txt"); KEY=$(cat "$S/skey.txt")
j() { python3 -c "import json;print(json.load(open('$S/bkey-fleet.json'))['data']['$1'])"; }
RESOLV=$'nameserver 108.61.10.10\nnameserver 8.8.8.8'

svc_arg() { # existing service id for a name, if any
  local f="$S/svc-$1.txt"
  if [ -s "$f" ]; then echo "--service $(cat "$f")"; fi
}
record_svc() { # name -> id after first deploy
  bunx --bun @prisma/compute-cli services list --project "$P" 2>/dev/null \
    | awk -v n="$1" '$0 ~ n {print $1; exit}' > "$S/svc-$1.txt" || true
}
resolve_url() { # service name -> running preview URL
  local id; id=$(cat "$S/svc-$1.txt")
  bunx --bun @prisma/compute-cli versions list --project "$P" --service "$id" 2>/dev/null \
    | awk '$2=="running"{print "https://"$3; exit}' > "$S/url-$1.txt"
  cat "$S/url-$1.txt"
}

if [ "$STEP" = servers ]; then
  cd "$S/fleet-app-server"
  for i in 1 2 3 4; do
    KA=(); [ "$i" = 1 ] && KA=(--env KEEP_AWAKE=1)
    echo "== deploying fleet-s$i =="
    bunx --bun @prisma/compute-cli deploy --project "$P" $(svc_arg "fleet-s$i") \
      --region "$REGION" --path . --http-port 8080 --service-name "fleet-s$i" \
      --env SERVER_BINARY_S3_KEY="bin/streams-$BIN_TAG-x64" \
      --env BIN_S3_ENDPOINT="$BINEP" --env BIN_S3_BUCKET="$BINBKT" --env BIN_S3_REGION=auto \
      --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
      --env SLATE_S3_ENDPOINT="$(j endpoint)" --env SLATE_S3_BUCKET="$(j bucketName)" \
      --env SLATE_S3_REGION=auto \
      --env SLATE_S3_ACCESS_KEY_ID="$(j accessKeyId)" \
      --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
      --env AUTH_TOKEN="$AUTH" \
      --env PATH_PREFIX=fleetd --env INSTANCE_NAME="streams-$i" \
      --env FLEET_PREFIX=fleetops --env FLEET_MAX=4 \
      --env SCALE_OUT_CPU_PCT=30 --env SCALE_CPU_SUSTAIN_SECS=10 \
      --env SCALE_IN_CPU_PCT=5 --env SCALE_IN_SECS=900 \
      --env INITIAL_SHARDS=4 \
      --env WAL_GROUP_COMMIT=1 --env WAL_FLUSH_GAP_MS=10 --env FLUSH_INTERVAL_MS=25 \
      --env WAL_POST_ACK_GATHER_MS=6 --env FRAME_COMPRESS=1 \
      --env L0_SST_SIZE_BYTES=16777216 --env MAX_UNFLUSHED_BYTES=33554432 \
      --env L0_MAX_SSTS=64 --env COMPACTOR_MAX_CONCURRENT=2 \
      --env SHARED_CACHE_BYTES=67108864 --env SLATEDB_RT_THREADS=2 \
      --env ADMIT_MAX_INFLIGHT=512 --env ADMIT_MAX_INFLIGHT_PER_STREAM=256 \
      --env ADMIT_RSS_SHED_MB=600 \
      --env ABSORB_BYTES=4194304 --env ABSORB_AGE_SECS=60 \
      --env ABSORB_PASS_BYTES=67108864 --env TRIM_PER_OP=65536 \
      --env POOL_IDLE_SECS=4 \
      ${KA[@]+"${KA[@]}"} \
      --env RESOLV_OVERRIDE="$RESOLV" 2>&1 | grep -viE 'resolving|resolved|saved' | tail -2
    record_svc "fleet-s$i"
    echo "fleet-s$i -> $(resolve_url "fleet-s$i")"
  done
elif [ "$STEP" = lb ]; then
  UP="$(cat "$S/url-fleet-s1.txt"),$(cat "$S/url-fleet-s2.txt"),$(cat "$S/url-fleet-s3.txt"),$(cat "$S/url-fleet-s4.txt")"
  echo "UPSTREAMS=$UP"
  cd "$S/fleet-app-lb"
  bunx --bun @prisma/compute-cli deploy --project "$P" $(svc_arg fleet-lb) \
    --region "$REGION" --path . --http-port 8080 --service-name "fleet-lb" \
    --env LB_BINARY_S3_KEY="bin/pilot-$PILOT_TAG-x64" \
    --env BIN_S3_ENDPOINT="$BINEP" --env BIN_S3_BUCKET="$BINBKT" --env BIN_S3_REGION=auto \
    --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env S3_ENDPOINT="$(j endpoint)" --env S3_BUCKET="$(j bucketName)" --env S3_REGION=auto \
    --env S3_ACCESS_KEY_ID="$(j accessKeyId)" --env S3_SECRET_ACCESS_KEY="$(j secretAccessKey)" \
    --env FLEET_PREFIX=fleetops --env DATA_PREFIX=fleetd \
    --env ROUTER_NAME=router-1 --env UPSTREAMS="$UP" \
    --env KEEP_AWAKE=1 \
    --env RESOLV_OVERRIDE="$RESOLV" 2>&1 | grep -viE 'resolving|resolved|saved' | tail -2
  record_svc fleet-lb
  echo "fleet-lb -> $(resolve_url fleet-lb)"
elif [ "$STEP" = gen ]; then
  LBURL=$(cat "$S/url-fleet-lb.txt")
  cd "$S/fleet-app-lb"   # same pilot wrapper, MODE=gen
  bunx --bun @prisma/compute-cli deploy --project "$P" $(svc_arg fleet-gen) \
    --region "$REGION" --path . --http-port 8080 --service-name "fleet-gen" \
    --env LB_BINARY_S3_KEY="bin/pilot-$PILOT_TAG-x64" \
    --env BIN_S3_ENDPOINT="$BINEP" --env BIN_S3_BUCKET="$BINBKT" --env BIN_S3_REGION=auto \
    --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC" \
    --env PILOT_MODE=gen \
    --env LB_URL="$LBURL" --env AUTH_TOKEN="$AUTH" --env STREAM_KEY="$KEY" \
    --env STREAMS="${STREAMS:-32}" --env STREAM_PREFIX="${STREAM_PREFIX:-fleet1}" \
    --env CONC_START="${CONC_START:-4}" --env CONC_MAX="${CONC_MAX:-512}" \
    --env RAMP_SECS="${RAMP_SECS:-120}" \
    --env KEEP_AWAKE=1 \
    --env RESOLV_OVERRIDE="$RESOLV" 2>&1 | grep -viE 'resolving|resolved|saved' | tail -2
  record_svc fleet-gen
  echo "fleet-gen -> $(resolve_url fleet-gen)"
fi
