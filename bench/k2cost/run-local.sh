#!/bin/bash
# K2 cost experiment: one local measurement point in the release posture
# (target/k2-design.md §6, §8, §9.1).
#
#   bench/k2cost/run-local.sh bench/k2cost/points/L1-tiny.env
#
# Per point: the platform emulator (keys, policies, grants, the workload
# JWT and a data-plane credential), a FRESH s3lite with the point's
# latency and a fresh bucket, SERVERS cells of target/release/streams-slate
# (enforce + workload fleet auth + STREAMS_RELEASE_POSTURE=1 + required
# billing, ROLLUP=1 on streams-1 only), a pilot LB when SERVERS=2, the
# scrape loop, the point's PHASES, optional quiescence, the usage read
# and price.py. Everything it starts is killed on exit.
#
# Results: $K2_HOME/results/<point>-<UTC stamp>/ (K2_HOME defaults to
# ~/.streams-k2). Secrets (the deployment bearer, the credential secret,
# the stream key, the headers file) live in a private directory under
# $K2_HOME/secrets/ that is removed at exit; nothing secret is written to
# the results directory.
#
# Point file (shell, sourced): S3LITE_LATENCY_MS, SERVERS (1|2),
# SERVER_ENV_EXTRA ("KEY=VALUE ..."; "-KEY" removes KEY from the pinned
# posture), PHASES (one k2gen invocation per line; the rig appends
# --base/--headers-file/--out/--ledger), QUIESCE (1 = wait for §8
# quiescence after the last phase), IDLE_SECS. Optional: POINT_NAME,
# QUIESCE_MIN_SECS (1800), QUIESCE_MAX_SECS (10800), QUIESCE_WINDOW_SECS
# (300; three such windows of equal heavy-PUT counts end the tail),
# USAGE_WAIT_SECS (330), SCRAPE_SECS (10). Every one of them, and anything
# a point reads as ${VAR:-default}, can come from the caller's environment.
# K2_BASELINE (a priced run directory, e.g. an L6 idle point) is passed to
# price.py --baseline: its per-cell idle floor rates. K2_ALLOW_COMMIT_MISMATCH=1
# runs although the servers' git_commit differs from the worktree HEAD
# (price.py voids such a point all the same).
#
# With QUIESCE=1 the deferred window ends at the `quiesced` snapshot; the
# IDLE_SECS that follow are an idle tail (quiesced -> end) that price.py
# uses as the point's floor baseline when no K2_BASELINE is given.
#
# Phase lines the RIG executes itself (not k2gen):
#   idle SECS   scrape only, no client traffic (baselines)
#   restart     stop every server gracefully and start it again on the
#               same bucket (cold reads); the scrape stitches the boots
#   quiesce     wait for §8 quiescence now (as QUIESCE=1 does after the
#               last phase): the earlier phases' deferred work (absorption,
#               compaction, GC) lands here and is priced as produce, not in
#               the phases that follow
# "$RUN" in a phase line (written \$RUN in the point's PHASES) is the
# results directory, e.g. walk ... --expect \$RUN/phase-3.ledger.json.
# A k2gen line ending in "&" runs in the background: the next line starts
# at once and the group ends when its last foreground line ends (all of
# the group's background phases are waited for).
#
# Ports 9580-9584 only. One point at a time per machine.
set -euo pipefail

POINT=${1:?usage: run-local.sh <point-file>}
[ -f "$POINT" ] || { echo "run-local: no such point file: $POINT" >&2; exit 2; }
POINT=$(cd "$(dirname "$POINT")" && pwd)/$(basename "$POINT")
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
# shellcheck source=/dev/null
. "$ROOT/scripts/lib/python.sh"

K2_HOME=${K2_HOME:-$HOME/.streams-k2}
EMU_PORT=9580
S3_PORT=9581
SRV_BASE_PORT=9582 # streams-1 = 9582, streams-2 = 9583
LB_PORT=9584
EMU=http://127.0.0.1:$EMU_PORT
S3=http://127.0.0.1:$S3_PORT
CUSTOMER_PROJECT=proj-k2
CUSTOMER_WORKSPACE=ws-k2
CELL=cell-k2

# ---- point --------------------------------------------------------------
# Defaults; a value from the caller's environment survives into the point
# file, so ${VAR:-default} in a point can be overridden per run.
S3LITE_LATENCY_MS=${S3LITE_LATENCY_MS:-20}
SERVERS=${SERVERS:-1}
SERVER_ENV_EXTRA=${SERVER_ENV_EXTRA:-}
PHASES=${PHASES:-}
QUIESCE=${QUIESCE:-0}
IDLE_SECS=${IDLE_SECS:-0}
QUIESCE_MIN_SECS=${QUIESCE_MIN_SECS:-1800}
QUIESCE_MAX_SECS=${QUIESCE_MAX_SECS:-10800}
QUIESCE_WINDOW_SECS=${QUIESCE_WINDOW_SECS:-300}
USAGE_WAIT_SECS=${USAGE_WAIT_SECS:-330}
SCRAPE_SECS=${SCRAPE_SECS:-10}
POINT_NAME=${POINT_NAME:-}
# shellcheck source=/dev/null
. "$POINT"
[ -n "$POINT_NAME" ] || POINT_NAME=$(basename "$POINT" .env)
case "$SERVERS" in 1|2) ;; *) echo "run-local: SERVERS must be 1 or 2" >&2; exit 2 ;; esac

for bin in streams-slate s3lite pilot; do
  [ -x "$ROOT/target/release/$bin" ] || {
    echo "run-local: target/release/$bin missing; build it with" \
      "nice -n 19 cargo build --locked --release --bin streams-slate --bin s3lite --bin pilot" >&2
    exit 2
  }
done
for port in $EMU_PORT $S3_PORT $SRV_BASE_PORT $((SRV_BASE_PORT + 1)) $LB_PORT; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "run-local: port $port is in use (another point running?)" >&2
    exit 2
  fi
done

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN=$K2_HOME/results/$POINT_NAME-$STAMP
SECRETS=$K2_HOME/secrets/$POINT_NAME-$STAMP
mkdir -p "$RUN" "$SECRETS/feeds"
chmod 700 "$K2_HOME/secrets" "$SECRETS"
cp "$POINT" "$RUN/point.env"
BUCKET=k2-$(echo "$STAMP" | tr 'A-Z' 'a-z')
MARKS=$RUN/marks.jsonl
: > "$MARKS"
log() { echo "[k2 $(date -u +%H:%M:%S)] $*"; }
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
mark() { # name [extra JSON members]
  printf '{"t":%s,"mark":"%s"%s}\n' "$(now_ms)" "$1" "${2:+,$2}" >> "$MARKS"
}

# ---- teardown -----------------------------------------------------------
PIDS_EMU='' PID_S3='' PID_LB='' PID_SCRAPE='' PID_REFRESH=
SRV_PIDS=()
PHASE_PIDS=()
stop_pid() { # pid seconds
  local pid=$1 secs=$2 i=0
  [ -n "$pid" ] || return 0
  kill "$pid" 2>/dev/null || return 0
  while kill -0 "$pid" 2>/dev/null && [ $i -lt $((secs * 5)) ]; do
    sleep 0.2
    i=$((i + 1))
  done
  kill -9 "$pid" 2>/dev/null || true
}
cleanup() {
  local rc=$?
  set +e
  for pid in ${PHASE_PIDS[@]+"${PHASE_PIDS[@]}"}; do stop_pid "$pid" 5; done
  stop_pid "$PID_REFRESH" 2
  stop_pid "$PID_SCRAPE" 5
  stop_pid "$PID_LB" 5
  for pid in ${SRV_PIDS[@]+"${SRV_PIDS[@]}"}; do stop_pid "$pid" 30; done
  stop_pid "$PID_S3" 5
  stop_pid "$PIDS_EMU" 5
  wait 2>/dev/null
  rm -rf "$SECRETS"
  log "torn down (exit $rc); results in $RUN"
  exit $rc
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---- secrets (files only; never on a command line) ----------------------
rand_b64() { python3 -c 'import os, base64; print(base64.b64encode(os.urandom(32)).decode())'; }
umask 077
rand_b64 > "$SECRETS/stream_key"
rand_b64 > "$SECRETS/usage_key"
python3 -c 'import secrets; print(secrets.token_hex(24))' > "$SECRETS/auth_token"
printf 'authorization: Bearer %s\n' "$(cat "$SECRETS/auth_token")" > "$SECRETS/debug.hdr"
umask 022

# ---- platform emulator --------------------------------------------------
log "point $POINT_NAME -> $RUN"
node platform-demo/src/emulator.mjs --port $EMU_PORT \
  --cells "$CELL=$SECRETS/feeds" \
  --fixture "$CUSTOMER_PROJECT:$CUSTOMER_WORKSPACE:$CELL" \
  > "$RUN/emulator.log" 2>&1 < /dev/null &
PIDS_EMU=$!

# ---- fresh s3lite -------------------------------------------------------
"$ROOT/target/release/s3lite" --listen 127.0.0.1:$S3_PORT \
  --latency-ms "$S3LITE_LATENCY_MS" > "$RUN/s3lite.log" 2>&1 < /dev/null &
PID_S3=$!
for i in $(seq 1 50); do
  curl -sf -o /dev/null "$EMU/admin/placement" && curl -sf -o /dev/null "$S3/_s3lite/stats" && break
  sleep 0.2
done
curl -sf -o /dev/null "$EMU/admin/placement" || { echo "run-local: emulator did not start" >&2; exit 1; }
curl -sf -o /dev/null "$S3/_s3lite/stats" || { echo "run-local: s3lite did not start" >&2; exit 1; }

# The data-plane credential exists BEFORE the cells boot, so their boot
# snapshot carries the grant. Every scope the product defines: the
# generator creates, appends, reads, pulls/settles consumer groups,
# deletes streams and reads project usage.
SCOPES='["streams.metadata.read","streams.records.read","streams.records.append","streams.create","streams.lifecycle.manage","streams.consumers.pull","streams.consumers.settle","streams.consumers.configure","streams.forks.create","streams.dlq.configure","streams.watches.manage","streams.catalog.read","streams.usage.read"]'
curl -sf -X POST -H 'content-type: application/json' \
  -d "{\"displayName\":\"k2gen $POINT_NAME\",\"scopes\":$SCOPES}" \
  "$EMU/v1/projects/$CUSTOMER_PROJECT/streams/credentials" > "$SECRETS/credential.json"
umask 077
jq -r '"authorization: StreamsCredential " + .secret' "$SECRETS/credential.json" > "$SECRETS/exchange.hdr"
umask 022
jq '{credential: {id: .credential.id, scopes: .credential.scopes, projectId: .credential.projectId}}' \
  "$SECRETS/credential.json" > "$RUN/credential.json"
rm -f "$SECRETS/credential.json"
HDR=$SECRETS/headers
mint_headers() { # exchange the credential, then replace the headers file atomically
  local tok
  tok=$(curl -sf -X POST -H @"$SECRETS/exchange.hdr" "$EMU/v1/token/streams" | jq -r .accessToken) || return 1
  [ -n "$tok" ] && [ "$tok" != null ] || return 1
  (
    umask 077
    printf 'authorization: Bearer %s\nprisma-encryption-key: %s\n' "$tok" "$(cat "$SECRETS/stream_key")" \
      > "$HDR.tmp"
  )
  mv -f "$HDR.tmp" "$HDR"
}
mint_headers || { echo "run-local: token exchange failed" >&2; exit 1; }
# Tokens live 600 s (the emulator's exp); the generator re-reads the file.
(
  while sleep 240; do
    mint_headers || echo "[k2] token refresh failed" >&2
  done
) < /dev/null &
PID_REFRESH=$!

# ---- server posture -----------------------------------------------------
# Order: profile, pinned engine posture (§6), topology, release posture,
# then the point's SERVER_ENV_EXTRA ("-KEY" removes KEY).
PROFILE_ENV=$ROOT/deploy/profiles/compute-1g.env
ENGINE_ENV="WAL_GROUP_COMMIT=1 FLUSH_INTERVAL_MS=25 WAL_POST_ACK_GATHER_MS=6 FRAME_COMPRESS=1
ABSORB_BYTES=4194304 ABSORB_AGE_SECS=60 TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET=65536
ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256
LIMIT_BYTES_PER_SEC=5000000 LIMIT_REQS_PER_SEC=1000 LIMIT_RECS_PER_SEC=5000 INITIAL_SHARDS=4"
server_env_file() { # ordinal -> writes $SECRETS/server-N.env (KEY='value' lines)
  local n=$1 port=$((SRV_BASE_PORT + $1 - 1)) out=$SECRETS/server-$1.env
  local rollup=0
  [ "$n" = 1 ] && rollup=1
  {
    grep -E '^[A-Z][A-Z0-9_]*=' "$PROFILE_ENV"
    for kv in $ENGINE_ENV; do echo "$kv"; done
    echo "SLATE_S3_ENDPOINT=$S3"
    echo "SLATE_S3_BUCKET=$BUCKET"
    echo "SLATE_S3_REGION=local"
    echo "SLATE_S3_ACCESS_KEY_ID=test"
    echo "SLATE_S3_SECRET_ACCESS_KEY=test"
    echo "PATH_PREFIX=k2data"
    echo "FLEET_PREFIX=k2fleet"
    echo "INSTANCE_NAME=streams-$n"
    if [ "$SERVERS" = 1 ]; then
      echo "FLEET_MAX=1"
    else
      echo "FLEET_MAX=2"
      echo "FLEET_MIN=2"
      echo "SELF_URL=http://127.0.0.1:$port"
      echo "FLEET_ALLOW_HTTP_PEERS=1"
    fi
    echo "STREAMS_AUTH_MODE=enforce"
    echo "STREAMS_AUTH_ISSUER=https://auth.prisma.io"
    echo "STREAMS_AUTH_KEYS_FILE=$SECRETS/feeds/keys.json"
    echo "STREAMS_AUTH_POLICY_FILE=$SECRETS/feeds/policies.json"
    echo "STREAMS_AUTH_GRANTS_FILE=$SECRETS/feeds/grants.json"
    echo "FLEET_AUTH_MODE=workload"
    echo "WORKLOAD_TOKEN_FILE=$SECRETS/feeds/workload.jwt"
    echo "STREAMS_RELEASE_POSTURE=1"
    echo "BILLING_MODE=required"
    echo "USAGE_STREAM_KEY=$(cat "$SECRETS/usage_key")"
    echo "ACCOUNT_ID=acct-k2cost"
    echo "PROJECT_ID=proj-k2cost-deploy"
    echo "CELL_ID=$CELL"
    echo "ROLLUP=$rollup"
    echo "AUTH_TOKEN=$(cat "$SECRETS/auth_token")"
    for kv in $SERVER_ENV_EXTRA; do echo "$kv"; done
  } | python3 "$HERE/scrape.py" envfile > "$out"
  chmod 600 "$out"
}
start_server() { # ordinal
  local n=$1 port=$((SRV_BASE_PORT + $1 - 1))
  # env -i: the server sees exactly the posture file, nothing from this
  # shell; the values never appear on a command line.
  env -i PATH=/usr/bin:/bin HOME="$HOME" /bin/bash -c \
    'set -a; . "$1"; set +a; exec "$2" --listen "$3"' _ \
    "$SECRETS/server-$n.env" "$ROOT/target/release/streams-slate" "127.0.0.1:$port" \
    >> "$RUN/server-$n.log" 2>&1 < /dev/null &
  SRV_PIDS[$n]=$!
}
wait_ready() { # ordinal
  local n=$1 port=$((SRV_BASE_PORT + $1 - 1)) i
  for i in $(seq 1 300); do
    curl -sf -o /dev/null "http://127.0.0.1:$port/readyz" && return 0
    kill -0 "${SRV_PIDS[$n]}" 2>/dev/null || { echo "run-local: streams-$n exited; tail of its log:" >&2; tail -20 "$RUN/server-$n.log" >&2; return 1; }
    sleep 0.5
  done
  echo "run-local: streams-$n not ready after 150 s" >&2
  return 1
}

if [ "$SERVERS" = 2 ]; then
  # Seed desired=2 before boot so both ordinals serve from the first
  # request (bench/fleet/local-fanout.sh). One PUT, before the ledger window.
  curl -sf -X PUT --data-binary '{"count":2,"reason":"seeded by k2cost run-local.sh","epoch":1,"computed_at_ms":0}' \
    "$S3/$BUCKET/k2fleet/fleet/desired.json" -o /dev/null
fi
SERVER_SPECS=
for n in $(seq 1 "$SERVERS"); do
  server_env_file "$n"
  start_server "$n"
  SERVER_SPECS="${SERVER_SPECS:+$SERVER_SPECS,}streams-$n=http://127.0.0.1:$((SRV_BASE_PORT + n - 1))"
done
for n in $(seq 1 "$SERVERS"); do wait_ready "$n"; done
BASE=http://127.0.0.1:$SRV_BASE_PORT
if [ "$SERVERS" = 2 ]; then
  env -i PATH=/usr/bin:/bin HOME="$HOME" MODE=lb \
    UPSTREAMS="http://127.0.0.1:$SRV_BASE_PORT,http://127.0.0.1:$((SRV_BASE_PORT + 1))" \
    S3_ENDPOINT="$S3" S3_BUCKET="$BUCKET" S3_REGION=local \
    S3_ACCESS_KEY_ID=test S3_SECRET_ACCESS_KEY=test \
    FLEET_PREFIX=k2fleet DATA_PREFIX=k2data ROUTER_NAME=router-k2 PORT=$LB_PORT \
    "$ROOT/target/release/pilot" > "$RUN/lb.log" 2>&1 < /dev/null &
  PID_LB=$!
  for i in $(seq 1 50); do
    curl -sf -o /dev/null "http://127.0.0.1:$LB_PORT/stats" && break
    sleep 0.2
  done
  BASE=http://127.0.0.1:$LB_PORT
fi
log "cell up: $SERVERS server(s), base $BASE, s3lite latency ${S3LITE_LATENCY_MS} ms, bucket $BUCKET"

SCRAPE_COMMON=(--run "$RUN" --servers "$SERVER_SPECS" --s3lite "$S3" --auth-file "$SECRETS/debug.hdr")
prc=0
python3 "$HERE/scrape.py" posture "${SCRAPE_COMMON[@]}" --root "$ROOT" \
  --env-dir "$SECRETS" --point "$POINT_NAME" --base "$BASE" \
  --latency-ms "$S3LITE_LATENCY_MS" --bucket "$BUCKET" --project "$CUSTOMER_PROJECT" || prc=$?
if [ $prc != 0 ] && [ "${K2_ALLOW_COMMIT_MISMATCH:-0}" != 1 ]; then
  echo "run-local: refusing the point (posture.json; set K2_ALLOW_COMMIT_MISMATCH=1 to run it anyway)" >&2
  exit 2
fi
python3 "$HERE/scrape.py" loop "${SCRAPE_COMMON[@]}" --interval "$SCRAPE_SECS" \
  --quiesce-min-secs "$QUIESCE_MIN_SECS" --quiet-secs "$QUIESCE_WINDOW_SECS" \
  > "$RUN/scrape.log" 2>&1 < /dev/null &
PID_SCRAPE=$!
sleep 1

# ---- phases -------------------------------------------------------------
pid_spec() { # the live servers' and router's pids, for CPU time (D1 compute)
  local n spec=
  for n in $(seq 1 "$SERVERS"); do spec="${spec:+$spec,}streams-$n=${SRV_PIDS[$n]}"; done
  [ -z "$PID_LB" ] || spec="$spec,router=$PID_LB"
  echo "$spec"
}
snapshot() { python3 "$HERE/scrape.py" snapshot "${SCRAPE_COMMON[@]}" --pids "$(pid_spec)" --label "$1"; }
boundary() { python3 "$HERE/scrape.py" snapshot "${SCRAPE_COMMON[@]}" --pids "$(pid_spec)" --boundary --label "$1" || true; }
restart_servers() {
  local n
  boundary "restart-stop" # the old boots' totals, as late as they can be read
  for n in $(seq 1 "$SERVERS"); do stop_pid "${SRV_PIDS[$n]}" 30; done
  for n in $(seq 1 "$SERVERS"); do start_server "$n"; done
  for n in $(seq 1 "$SERVERS"); do wait_ready "$n"; done
}
json_str() { python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"; }
# quiesce_wait ID: ask the scrape loop for §8 quiescence (counted from now)
# and wait for its answer; 0 when quiesced, 1 on timeout or a dead loop.
quiesce_wait() {
  local id=$1 waited=0
  [ "$id" = load_end ] || mark quiesce_request "\"id\":\"$id\""
  log "waiting for quiescence ($id; min ${QUIESCE_MIN_SECS} s, max ${QUIESCE_MAX_SECS} s)"
  while ! grep -qF "\"mark\":\"quiesced\",\"id\":\"$id\"" "$MARKS"; do
    if [ $waited -ge "$QUIESCE_MAX_SECS" ]; then
      mark quiesce_timeout "\"id\":\"$id\""
      log "quiescence ($id) not reached in ${QUIESCE_MAX_SECS} s: the point is void"
      return 1
    fi
    if ! kill -0 "$PID_SCRAPE" 2>/dev/null; then
      mark quiesce_timeout "\"id\":\"$id\",\"reason\":\"scrape loop died\""
      log "scrape loop died; see $RUN/scrape.log"
      return 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
  log "quiesced ($id) after ${waited} s"
}

snapshot start
mark load_start
set -f # phase lines are split into words, never globbed
PHASE_FAIL=0
i=0
GROUP_BG=()
while IFS= read -r line; do
  line=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  [ -n "$line" ] || continue
  case "$line" in \#*) continue ;; esac
  i=$((i + 1))
  bg=0
  case "$line" in *'&') bg=1; line=$(echo "${line%&}" | sed -e 's/[[:space:]]*$//') ;; esac
  set -- $line
  mode=$1
  boundary "phase-$i-start"
  mark phase_start "\"i\":$i,\"mode\":\"$mode\",\"bg\":$bg,\"args\":$(json_str "$line")"
  log "phase $i: $line$( [ $bg = 1 ] && echo ' (background)')"
  rc=0
  case "$mode" in
    idle) sleep "${2:?idle needs SECS}" ;;
    restart) restart_servers || rc=$? ;;
    quiesce) quiesce_wait "phase-$i" || true ;; # a timeout voids the point in price.py
    *)
      eval "set -- $line"
      if [ $bg = 1 ]; then
        bun "$HERE/k2gen.ts" "$@" --base "$BASE" --headers-file "$HDR" \
          --out "$RUN/phase-$i.jsonl" --ledger "$RUN/phase-$i.ledger.json" \
          > "$RUN/phase-$i.log" 2>&1 < /dev/null &
        PHASE_PIDS+=($!)
        GROUP_BG+=("$i:$!")
        continue
      fi
      bun "$HERE/k2gen.ts" "$@" --base "$BASE" --headers-file "$HDR" \
        --out "$RUN/phase-$i.jsonl" --ledger "$RUN/phase-$i.ledger.json" \
        > "$RUN/phase-$i.log" 2>&1 < /dev/null || rc=$?
      ;;
  esac
  # The group's background phases end with its foreground line.
  for entry in ${GROUP_BG[@]+"${GROUP_BG[@]}"}; do
    bi=${entry%%:*}
    bpid=${entry#*:}
    brc=0
    wait "$bpid" || brc=$?
    mark phase_end "\"i\":$bi,\"rc\":$brc"
    [ $brc = 0 ] || { log "phase $bi failed (rc $brc); see $RUN/phase-$bi.log"; PHASE_FAIL=1; }
  done
  boundary "phase-$i-end"
  mark phase_end "\"i\":$i,\"mode\":\"$mode\",\"rc\":$rc"
  GROUP_BG=()
  if [ $rc != 0 ]; then
    log "phase $i failed (rc $rc); see $RUN/phase-$i.log; skipping the remaining phases"
    PHASE_FAIL=1
  fi
  [ $PHASE_FAIL = 0 ] || break
done <<< "$PHASES"
for entry in ${GROUP_BG[@]+"${GROUP_BG[@]}"}; do
  wait "${entry#*:}" || PHASE_FAIL=1
  mark phase_end "\"i\":${entry%%:*}"
done
set +f
snapshot loadend
mark load_end "\"quiesce\":$([ "$QUIESCE" = 1 ] && echo 1 || echo 0)"
log "load window closed"

# ---- deferred tail ------------------------------------------------------
# The deferred window ends at the `quiesced` snapshot (§4.4); the idle tail
# after it is priced as floor and serves as the point's baseline.
if [ "$QUIESCE" = 1 ] && quiesce_wait load_end; then
  snapshot quiesced
fi
if [ "${IDLE_SECS:-0}" -gt 0 ]; then
  log "idle tail ${IDLE_SECS} s"
  sleep "$IDLE_SECS"
fi
snapshot end
mark end

# ---- meters -------------------------------------------------------------
python3 "$HERE/scrape.py" usage "${SCRAPE_COMMON[@]}" --rollup "http://127.0.0.1:$SRV_BASE_PORT" \
  --headers-file "$HDR" --project "$CUSTOMER_PROJECT" --max-wait-secs "$USAGE_WAIT_SECS" \
  2>&1 | tee -a "$RUN/scrape.log" || true
stop_pid "$PID_SCRAPE" 5
PID_SCRAPE=
if [ -n "${K2_BASELINE:-}" ]; then
  python3 "$HERE/price.py" "$RUN" --baseline "$K2_BASELINE" || log "price.py failed"
else
  python3 "$HERE/price.py" "$RUN" || log "price.py failed"
fi
[ $PHASE_FAIL = 0 ] || { log "one or more phases failed: the point is void"; exit 3; }
