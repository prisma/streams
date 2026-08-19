#!/bin/bash
# MT-TENANTS campaign: how does throughput respond to ACTIVE-TENANT
# cardinality on one cell? One streams-slate on Prisma Compute against
# a real bucket; the SAME physical workload (1000 streams, all active,
# ~6k offered rps at 1 KiB) is re-partitioned across N projects.
#
#   stage 0             auth OFF, legacy raw surface  (regression control)
#   stages 1/10/100/1000 enforce + product surface, N active projects
#
# Each stage: fresh stream prefix (streams belong to projects — stage
# N's partitioning must not collide with stage M's ownership), 300s
# steady window, generator co-located in-region, JSONL scraped over
# HTTP. Server stays up across MT stages; only the generator redeploys.
#
#   SOAK_HOME=~/.streams-soak ./bench/soak/mt-tenants.sh
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
S=${SOAK_HOME:?set SOAK_HOME}
R=${MT_REGION:-eu-central-1}
export SOAK_RUN_ID=${SOAK_RUN_ID:-"mtten-$(date -u +%Y%m%dT%H%M%SZ)"}
export BIN_TAG=$SOAK_RUN_ID
export SOAK_PREFIX=$SOAK_RUN_ID
OUT="$S/results/$SOAK_RUN_ID"
mkdir -p "$OUT"
STAGES=${MT_STAGES:-"0 1 10 100 1000"}
STEADY_SECS=${MT_STEADY_SECS:-300}
INTERVAL_MS=${MT_INTERVAL_MS:-167}
STREAMS_N=${MT_STREAMS_N:-1000}
RECORD_BYTES=${MT_RECORD_BYTES:-1024}
CELL=mt-cell-1
echo "== mt-tenants $SOAK_RUN_ID region=$R stages=[$STAGES] steady=${STEADY_SECS}s"

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

# ---- 0. provision (project + bucket + key, region-pinned receipt) ---------
# The previous campaign's teardown removes its project; every run
# provisions its own (provision.py refuses cross-run receipt reuse).
python3 "$HERE/provision.py" --run-id "$SOAK_RUN_ID" "$R"
rm -f "$S/projects/$(cat "$S/proj-$R.txt")"/svc-*-"$R".txt 2>/dev/null || true
P=$(cat "$S/proj-$R.txt")

# ---- 1. binaries ----------------------------------------------------------
"$HERE/build-upload.sh"

# ---- 2. feeds + tokens ----------------------------------------------------
MTDIR="$S/mt-$SOAK_RUN_ID"
node "$HERE/mtgen.mjs" --projects "$STREAMS_N" --cell "$CELL" --out "$MTDIR"
python3 - "$MTDIR/feeds-bundle.json" "mt/$SOAK_RUN_ID/feeds.json" \
          "$MTDIR/tokens.json"       "mt/$SOAK_RUN_ID/tokens.json" <<'PY'
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
    got = c.get_object(Bucket=bucket, Key=key, Range="bytes=0-15")["Body"].read()
    assert len(got) == 16, key
    print(f"uploaded {key} ({len(data)} bytes, ranged-GET verified)")
PY

# ---- deploy helpers (deploy-region.sh's traps, campaign-scoped) ------------
svc_id() { # role -> cached+REVALIDATED service id or empty
  local ROLE=$1 SVCFILE="$S/projects/$P/svc-$ROLE-$R.txt"
  mkdir -p "$S/projects/$P"
  # Revalidate exactly like deploy-region.sh: a cached id must appear
  # in THIS project's list under the expected name — a torn-down
  # campaign leaves stale cps_ ids that deploy as "Resource Not Found"
  # (this exact failure, 2026-08-19).
  if [ -f "$SVCFILE" ]; then
    local CACHED=$(cat "$SVCFILE")
    local LISTED=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null \
                   | awk -v id="$CACHED" -v n="soak-$ROLE-$R" '$1==id && $2==n {print $1}')
    if [ -z "$LISTED" ]; then
      echo "stale service cache for $ROLE-$R (id $CACHED); dropping" >&2
      rm -f "$SVCFILE"
    fi
  fi
  if [ ! -f "$SVCFILE" ]; then
    local EX=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null \
               | awk -v n="soak-$ROLE-$R" '$2==n {print $1; exit}') || true
    [ -n "$EX" ] && echo "$EX" > "$SVCFILE"
  fi
  [ -f "$SVCFILE" ] && cat "$SVCFILE" || true
}
deploy() { # role, then --env args...
  local ROLE=$1; shift
  local DIR="$S/app-$ROLE-$R"
  local SVC=$(svc_id "$ROLE"); local SVCARG=()
  [ -n "$SVC" ] && SVCARG=(--service "$SVC")
  ( cd "$DIR"
    local DEPLOYED=0 OUT_CLI=""
    for ATT in 1 2 3 4 5 6; do
      if OUT_CLI=$(bunx --bun @prisma/compute-cli@0.39.0 deploy --project "$P" ${SVCARG[@]+"${SVCARG[@]}"} \
        --region "$R" --path . --http-port 8080 --service-name "soak-$ROLE-$R" "$@" \
        2>&1 | grep -viE 'resolving|resolved|saved'); then DEPLOYED=1; break; fi
      echo "deploy attempt $ATT failed for $ROLE:" >&2; echo "$OUT_CLI" | tail -4 >&2; sleep 30
    done
    [ "$DEPLOYED" = 1 ] || { echo "deploy failed for $ROLE" >&2; exit 1; }
  )
  # First deploy of a role CREATES the service by name; cache its id so
  # every later redeploy targets the same service (deploying by
  # --service-name twice fails with "already exists").
  local SVCFILE="$S/projects/$P/svc-$ROLE-$R.txt"
  if [ ! -f "$SVCFILE" ]; then
    local NEWID=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null \
                  | awk -v n="soak-$ROLE-$R" '$2==n {print $1; exit}')
    [ -n "$NEWID" ] && echo "$NEWID" > "$SVCFILE"
  fi
  "$HERE/resolve-urls.sh" "$R" "$ROLE" > /dev/null
}
SERVER_COMMON=(
  --env SERVER_BINARY_S3_KEY="bin/streams-$BIN_TAG-x64"
  --env BIN_S3_ENDPOINT=$BINEP --env BIN_S3_BUCKET=$BINBKT --env BIN_S3_REGION=auto
  --env BIN_S3_ACCESS_KEY_ID="$BINID" --env BIN_S3_SECRET_ACCESS_KEY="$BINSEC"
  --env SLATE_S3_ENDPOINT="$(j endpoint)" --env SLATE_S3_BUCKET="$(j bucketName)"
  --env SLATE_S3_REGION=auto
  --env SLATE_S3_ACCESS_KEY_ID="$(j accessKeyId)"
  --env SLATE_S3_SECRET_ACCESS_KEY="$(j secretAccessKey)"
  --env AUTH_TOKEN="$AUTH"
  --env PATH_PREFIX="$SOAK_PREFIX" --env INSTANCE_NAME=streams-1
  --env INITIAL_SHARDS=4
  --env WAL_GROUP_COMMIT=1 --env WAL_FLUSH_GAP_MS=10 --env FLUSH_INTERVAL_MS=25
  --env WAL_POST_ACK_GATHER_MS=6 --env FRAME_COMPRESS=1
  --env ADMIT_MAX_INFLIGHT=512 --env ADMIT_MAX_INFLIGHT_PER_STREAM=256
  --env LIMIT_RECS_PER_SEC=100000
  --env ABSORB_BYTES=4194304 --env ABSORB_AGE_SECS=60
  --env ABSORB_PASS_BYTES=67108864 --env TRIM_PER_OP=65536
  --env POOL_IDLE_SECS=4 --env KEEP_AWAKE=1
  --env CELL_ID="$CELL"
  # Enforce refuses to boot with a default deployment project id; the
  # 2026-08-19 stage-1 crash-loop was exactly this refusal (correct
  # behavior — billing must never land on a placeholder project).
  --env PROJECT_ID=proj-mt-deploy
)
verify_server_live() { # retry /livez until 200 or fail loudly
  local URL=$(cat "$S/url-server-$R.txt")
  for i in $(seq 1 24); do
    local CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$URL/livez" || true)
    [ "$CODE" = 200 ] && { echo "server live ($URL)"; return 0; }
    sleep 5
  done
  echo "SERVER NOT LIVE after deploy — diagnostic:" >&2
  curl -s --max-time 10 "$URL/livez" | head -c 600 >&2 || true
  exit 1
}
gen_env_common() { # stage prefix
  local PFX=$1
  GEN_COMMON=(
    --env AWSBENCH_S3_KEY="bin/awsbench-$BIN_TAG-x64"
    --env S3_ENDPOINT=$BINEP --env S3_BUCKET=$BINBKT --env S3_REGION=auto
    --env S3_ACCESS_KEY_ID="$BINID" --env S3_SECRET_ACCESS_KEY="$BINSEC"
    --env BENCH_SYSTEM=prisma --env BENCH_SHAPE=wide
    --env BENCH_TARGET="$(cat "$S/url-server-$R.txt")"
    --env BENCH_WIDE_STREAMS="$STREAMS_N" --env BENCH_WIDE_ACTIVE="$STREAMS_N"
    --env BENCH_WIDE_SECS="$STEADY_SECS" --env BENCH_WIDE_APPEND_INTERVAL_MS="$INTERVAL_MS"
    --env BENCH_WIDE_SCAN_RPS=0 --env BENCH_WIDE_SETUP_CONC=64
    --env BENCH_RECORD_BYTES="$RECORD_BYTES" --env BENCH_BATCH=1
    --env BENCH_HOLD=1 --env BENCH_START_GATED=false
    --env AUTH_TOKEN="$AUTH" --env STREAM_KEY="$KEY"
    --env BENCH_STREAM="$PFX" --env KEEP_AWAKE=1
  )
}
scrape_stage() { # stage-label; waits for wide_done, saves scrape
  local LABEL=$1
  local URL=$(cat "$S/url-gen-$R.txt")
  local DEADLINE=$(( $(date +%s) + 1200 ))
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    sleep 20
    local BODY=$(curl -sf --max-time 15 "$URL/" || true)
    if [ -n "$BODY" ] && echo "$BODY" | grep -q '"wide_done"'; then
      echo "$BODY" > "$OUT/stage-$LABEL.json"
      echo "-- stage $LABEL done: $(echo "$BODY" | python3 -c "
import json,sys
lines=json.load(sys.stdin)
d=[l for l in lines if l.get('phase')=='wide_done'][-1]
print(f\"apOk={d['apOk']} thr={d['apThr']} err={d['apErr']} rps={d['apOk']/max(1,d['steadySecs']):.0f}\")")"
      return 0
    fi
  done
  echo "stage $LABEL TIMED OUT waiting for wide_done" >&2
  curl -sf --max-time 15 "$URL/" > "$OUT/stage-$LABEL.partial.json" || true
  return 1
}
snap_server() { # label
  curl -sf --max-time 20 -H "authorization: Bearer $AUTH" \
    "$(cat "$S/url-server-$R.txt")/v1/debug/load" > "$OUT/server-$1.json" || true
}

# ---- 3. stages -------------------------------------------------------------
for N in $STAGES; do
  if [ "$N" = 0 ]; then
    echo "== stage 0: server AUTH OFF (raw-surface regression control)"
    deploy server "${SERVER_COMMON[@]}" --env STREAMS_AUTH_MODE=off ${MEMFLAGS[@]+"${MEMFLAGS[@]}"}
    verify_server_live
    gen_env_common "mt0x-"
    deploy gen "${GEN_COMMON[@]}"
  else
    if [ ! -f "$OUT/.enforce-deployed" ]; then
      echo "== switching server to ENFORCE (1000-project feeds, cell $CELL)"
      deploy server "${SERVER_COMMON[@]}" \
        --env STREAMS_AUTH_MODE=enforce \
        --env STREAMS_AUTH_ISSUER=https://auth.prisma.io \
        --env STREAMS_AUTH_KEYS_FILE=/tmp/feeds/keys.json \
        --env STREAMS_AUTH_POLICY_FILE=/tmp/feeds/policies.json \
        --env STREAMS_AUTH_GRANTS_FILE=/tmp/feeds/grants.json \
        --env STREAMS_AUTH_REFRESH_SECS=60 \
        --env FEEDS_S3_KEY="mt/$SOAK_RUN_ID/feeds.json" \
        ${MEMFLAGS[@]+"${MEMFLAGS[@]}"}
      verify_server_live
      touch "$OUT/.enforce-deployed"
    fi
    echo "== stage $N: $N active project(s), product surface"
    gen_env_common "mt${N}x-"
    deploy gen "${GEN_COMMON[@]}" \
      --env BENCH_MT=1 --env BENCH_PROJECTS_ACTIVE="$N" \
      --env TOKENS_S3_KEY="mt/$SOAK_RUN_ID/tokens.json"
  fi
  snap_server "before-$N"
  scrape_stage "$N"
  snap_server "after-$N"
done

echo "== all stages scraped; results in $OUT"
