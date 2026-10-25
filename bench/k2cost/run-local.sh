#!/bin/bash
# K2 cost experiment: one local measurement point in the release posture
# (target/k2-design.md §6, §8, §9.1).
#
#   bench/k2cost/run-local.sh bench/k2cost/points/L1-tiny.env [--dry-run]
#
# Per point: the platform emulator (keys, policies, grants, the workload
# JWT and a data-plane credential), a FRESH s3lite with the point's
# latency and a fresh bucket, SERVERS cells of streams-slate (enforce +
# STREAMS_RELEASE_POSTURE=1 + required billing, ROLLUP=1 on streams-1
# only), a pilot LB when SERVERS=2, the scrape loop, the point's PHASES,
# optional quiescence, the usage read, price.py and counts.py. Everything
# it starts is killed on exit. --dry-run prints the ports, the binaries,
# the commit the servers are held to and each server's environment
# (secrets redacted), and starts nothing.
#
# Results: $K2_HOME/results/<point>[-<K2_RUN_TAG>]-<UTC stamp>/ (K2_HOME
# defaults to ~/.streams-k2). Secrets (the deployment bearer, the
# credential secret, the stream key, the headers file) live in a private
# directory under $K2_HOME/secrets/ that is removed at exit; nothing
# secret is written to the results directory.
#
# Point file (shell, sourced): S3LITE_LATENCY_MS, SERVERS (1|2), POSTURE
# (k2 | launch, below), SERVER_ENV_EXTRA ("KEY=VALUE ..."; "-KEY" removes
# KEY from the posture), PHASES (one k2gen invocation per line; the rig
# appends --base/--headers-file/--out/--ledger), QUIESCE (1 = wait for §8
# quiescence after the last phase), IDLE_SECS. Optional: POINT_NAME,
# QUIESCE_MIN_SECS (1800), QUIESCE_MAX_SECS (10800), QUIESCE_WINDOW_SECS
# (300; three such windows of equal heavy-PUT counts end the tail),
# USAGE_WAIT_SECS (330), SCRAPE_SECS (10). Every one of them, and anything
# a point reads as ${VAR:-default}, can come from the caller's environment.
#
# POSTURE=k2 (the default) is §6's: compute-1g.env, the pinned engine knobs
# and INITIAL_SHARDS=4, the fleet prefix with workload fleet auth.
# POSTURE=launch is the launch cell's: deploy/profiles/compute-1g.env then
# shared-cell.env, layered as deploy/composer/streams/env.ts does (one
# shard, fleet off, ring off, the shared cell's caches; KEEP_AWAKE, the
# wrapper's, dropped), with the rig's store, feeds, keys and identity; one
# server only.
#
# The caller's environment (never the point file):
#   K2_PORT_BASE (9580)  every port: emulator BASE, s3lite BASE+1,
#       streams-1 BASE+2, streams-2 BASE+3, pilot LB BASE+4. Points on
#       distinct bases run at once (the rig refuses a port in use).
#   K2_SERVER_BIN, K2_S3LITE_BIN, K2_PILOT_BIN  the binaries (default
#       target/release/{streams-slate,s3lite,pilot}; pilot only when
#       SERVERS=2), recorded with their sha256 in posture.json.
#   K2_SERVER_COMMIT  the commit the servers must report (/v1/debug/load
#       git_commit, which build.rs embeds; 7-40 hex digits, a prefix of
#       it), for a binary built from another commit than the worktree
#       HEAD (the default). The rig refuses a server that reports another
#       commit, and price.py voids the point.
#   K2_ALLOW_COMMIT_MISMATCH=1  run although the servers' git_commit is
#       not the expected one (price.py voids such a point all the same).
#   K2_RUN_TAG  a word appended to the results directory's point name
#       (two runs of one point started in the same second, e.g. a
#       screening pair: ref and cand).
#   K2_BASELINE  a priced run directory (e.g. an L6 idle point), passed to
#       price.py --baseline: its per-cell idle floor rates.
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
#   tenants [--time-scale F]   (tenants mode, below) the manifest's run:
#               tenants.ts run under the projects' tokens, with a boundary
#               snapshot and a period_start/period_end mark at each of the
#               manifest's period edges (counts.py's "period NAME" windows)
#
# Tenants mode (the packing measurement, README "Packing"): a point that
# sets TENANTS_MANIFEST (a tenants manifest, tenant-plan.ts; relative to the
# checkout's root) runs on POSTURE=launch with no platform emulator. Its
# projects are admitted as a shared cell admits them: cell-admin's cell on
# the two launch profiles (tenants-admit.ts admit: every project with its
# class's quota set, the cell's ceilings otherwise, and the canary), 12 h
# tokens (tenants-admit.ts tokens) and the bundle's three feed files, which
# the server reads (tenants-admit.ts feeds). Its PHASES take idle, restart,
# quiesce and tenants lines only (its traffic is the manifest's); the scrape
# also reads /operator/data.json (the parked waits). After the end
# snapshot: tenants.ts usage and audit, counts.py and packing.py price,
# not price.py (which prices one project's k2gen ledgers). A point must not
# move the profiles' sizing keys in SERVER_ENV_EXTRA: cell-admin derives
# the quotas it publishes from the profiles alone.
# "$RUN" in a phase line (written \$RUN in the point's PHASES) is the
# results directory, e.g. walk ... --expect \$RUN/phase-3.ledger.json.
# A k2gen line ending in "&" runs in the background: the next line starts
# at once and the group ends when its last foreground line ends (all of
# the group's background phases are waited for).
set -euo pipefail

die() { local rc=$1; shift; echo "run-local: $*" >&2; exit "$rc"; }
POINT=${1:?usage: run-local.sh <point-file> [--dry-run]}
DRY_RUN=0
case "${2:-}" in
  '') ;;
  --dry-run) DRY_RUN=1 ;;
  *) die 2 "unknown argument $2 (usage: run-local.sh <point-file> [--dry-run])" ;;
esac
[ -f "$POINT" ] || die 2 "no such point file: $POINT"
POINT=$(cd "$(dirname "$POINT")" && pwd)/$(basename "$POINT")
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
# shellcheck source=/dev/null
. "$ROOT/scripts/lib/python.sh"

K2_HOME=${K2_HOME:-$HOME/.streams-k2}
K2_PORT_BASE=${K2_PORT_BASE:-9580}
case "$K2_PORT_BASE" in *[!0-9]*) die 2 "K2_PORT_BASE must be a number, not $K2_PORT_BASE" ;; esac
[ "$K2_PORT_BASE" -ge 1024 ] && [ "$K2_PORT_BASE" -le 65531 ] \
  || die 2 "K2_PORT_BASE $K2_PORT_BASE: the rig takes BASE..BASE+4, within 1024..65535"
EMU_PORT=$K2_PORT_BASE
S3_PORT=$((K2_PORT_BASE + 1))
SRV_BASE_PORT=$((K2_PORT_BASE + 2)) # streams-1 = BASE+2, streams-2 = BASE+3
LB_PORT=$((K2_PORT_BASE + 4))
EMU=http://127.0.0.1:$EMU_PORT
S3=http://127.0.0.1:$S3_PORT
CUSTOMER_PROJECT=proj-k2
CUSTOMER_WORKSPACE=ws-k2
CELL=cell-k2
DEPLOY_PROJECT=proj-k2cost-deploy
ACCOUNT=acct-k2cost

# ---- point --------------------------------------------------------------
# Defaults; a value from the caller's environment survives into the point
# file, so ${VAR:-default} in a point can be overridden per run.
S3LITE_LATENCY_MS=${S3LITE_LATENCY_MS:-20}
SERVERS=${SERVERS:-1}
POSTURE=${POSTURE:-k2}
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
TENANTS_MANIFEST=${TENANTS_MANIFEST:-}
# shellcheck source=/dev/null
. "$POINT"
[ -n "$POINT_NAME" ] || POINT_NAME=$(basename "$POINT" .env)
case "$SERVERS" in 1|2) ;; *) die 2 "SERVERS must be 1 or 2" ;; esac
case "$POSTURE" in k2|launch) ;; *) die 2 "POSTURE must be k2 or launch, not $POSTURE" ;; esac
[ "$POSTURE" = k2 ] || [ "$SERVERS" = 1 ] \
  || die 2 "POSTURE=launch is one server (the launch cell runs no fleet), not SERVERS=$SERVERS"
TENANTS=0
if [ -n "$TENANTS_MANIFEST" ]; then
  TENANTS=1
  [ "$POSTURE" = launch ] \
    || die 2 "TENANTS_MANIFEST runs on POSTURE=launch: cell-admin admits its projects onto a shared cell"
  case "$TENANTS_MANIFEST" in /*) ;; *) TENANTS_MANIFEST=$ROOT/$TENANTS_MANIFEST ;; esac
  [ -f "$TENANTS_MANIFEST" ] || die 2 "no tenants manifest $TENANTS_MANIFEST"
  CUSTOMER_PROJECT=tenants
fi
# A tenants point's traffic is its manifest's: the rig's own lines only. A
# tenants line runs in the foreground (its period snapshots are timed from it).
while IFS= read -r line; do
  line=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  case "$line" in ''|\#*) continue ;; esac
  mode=${line%%[[:space:]&]*}
  case "$mode" in
    idle|restart|quiesce) ;;
    tenants)
      [ "$TENANTS" = 1 ] || die 2 "a tenants phase needs TENANTS_MANIFEST (the manifest whose projects the rig admits)"
      case "$line" in *'&') die 2 "a tenants phase runs in the foreground, not \"$line\"" ;; esac
      ;;
    *) [ "$TENANTS" = 0 ] \
         || die 2 "phase \"$line\": a tenants point runs idle, restart, quiesce and tenants lines only (its traffic is the manifest's)" ;;
  esac
done <<< "$PHASES"
K2_RUN_TAG=${K2_RUN_TAG:-}
case "$K2_RUN_TAG" in *[!A-Za-z0-9._-]*) die 2 "K2_RUN_TAG must be letters, digits, '.', '_' or '-': $K2_RUN_TAG" ;; esac

# ---- binaries and the commit the servers are held to --------------------
# bin_path VAR NAME: $VAR's file, absolute, or target/release/NAME.
bin_path() {
  local var=$1 name=$2 path
  path=${!var:-}
  if [ -z "$path" ]; then
    [ -x "$ROOT/target/release/$name" ] || die 2 "target/release/$name missing; build it with" \
      "nice -n 19 cargo build --locked --release --bin streams-slate --bin s3lite --bin pilot, or set $var"
    echo "$ROOT/target/release/$name"
    return
  fi
  [ -f "$path" ] && [ -x "$path" ] || die 2 "$var=$path is not an executable file"
  echo "$(cd "$(dirname "$path")" && pwd)/$(basename "$path")"
}
SERVER_BIN=$(bin_path K2_SERVER_BIN streams-slate) || exit 2
S3LITE_BIN=$(bin_path K2_S3LITE_BIN s3lite) || exit 2
PILOT_BIN=
[ "$SERVERS" = 1 ] || PILOT_BIN=$(bin_path K2_PILOT_BIN pilot) || exit 2
HEAD_COMMIT=$(git -C "$ROOT" rev-parse HEAD)
if [ -n "${K2_SERVER_COMMIT:-}" ]; then
  [[ "$K2_SERVER_COMMIT" =~ ^[0-9a-f]{7,40}$ ]] \
    || die 2 "K2_SERVER_COMMIT must be 7-40 lowercase hex digits (a commit the binary reports), not $K2_SERVER_COMMIT"
  EXPECT_COMMIT=$K2_SERVER_COMMIT
  EXPECT_SOURCE=K2_SERVER_COMMIT
else
  EXPECT_COMMIT=$HEAD_COMMIT
  EXPECT_SOURCE="worktree HEAD"
fi
sha256_of() { python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"; }

if [ "$DRY_RUN" = 0 ]; then
  for port in $EMU_PORT $S3_PORT $SRV_BASE_PORT $((SRV_BASE_PORT + 1)) $LB_PORT; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      die 2 "port $port is in use (another point running? K2_PORT_BASE moves every port)"
    fi
  done
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_NAME=$POINT_NAME${K2_RUN_TAG:+-$K2_RUN_TAG}-$STAMP
RUN=$K2_HOME/results/$RUN_NAME
SECRETS=$K2_HOME/secrets/$RUN_NAME
mkdir -p "$K2_HOME/results" "$K2_HOME/secrets"
chmod 700 "$K2_HOME/secrets"
# mkdir, not -p: a second run of the same name in the same second fails
# here, before the trap that would remove the first one's secrets.
mkdir -m 700 "$SECRETS" 2>/dev/null \
  || die 2 "$SECRETS exists: another run of $POINT_NAME started this second (set K2_RUN_TAG)"
mkdir "$SECRETS/feeds"
BUCKET=k2-$(echo "$STAMP" | tr 'A-Z' 'a-z')
MARKS=$RUN/marks.jsonl
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
  [ "$DRY_RUN" = 1 ] || log "torn down (exit $rc); results in $RUN"
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

# ---- server posture -----------------------------------------------------
# POSTURE=k2, in order: compute-1g.env, the pinned engine posture (§6), the
# store, topology, the release posture. POSTURE=launch: compute-1g.env then
# shared-cell.env as deploy/composer/streams/env.ts layers them (KEEP_AWAKE
# is the wrapper's, never the binary's), the store, the feeds and the
# cell's identity, where a deploy's cells file and secrets put them. Then
# the point's SERVER_ENV_EXTRA ("-KEY" removes KEY).
PROFILE_ENV=$ROOT/deploy/profiles/compute-1g.env
SHARED_CELL_ENV=$ROOT/deploy/profiles/shared-cell.env
ENGINE_ENV="WAL_GROUP_COMMIT=1 FLUSH_INTERVAL_MS=25 WAL_POST_ACK_GATHER_MS=6 FRAME_COMPRESS=1
ABSORB_BYTES=4194304 ABSORB_AGE_SECS=60 TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET=65536
ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256
LIMIT_BYTES_PER_SEC=5000000 LIMIT_REQS_PER_SEC=1000 LIMIT_RECS_PER_SEC=5000 INITIAL_SHARDS=4"
store_env() {
  echo "SLATE_S3_ENDPOINT=$S3"
  echo "SLATE_S3_BUCKET=$BUCKET"
  echo "SLATE_S3_REGION=local"
  echo "SLATE_S3_ACCESS_KEY_ID=test"
  echo "SLATE_S3_SECRET_ACCESS_KEY=test"
  echo "PATH_PREFIX=k2data"
}
feeds_env() { # the emulator's feed files
  echo "STREAMS_AUTH_ISSUER=https://auth.prisma.io"
  echo "STREAMS_AUTH_KEYS_FILE=$SECRETS/feeds/keys.json"
  echo "STREAMS_AUTH_POLICY_FILE=$SECRETS/feeds/policies.json"
  echo "STREAMS_AUTH_GRANTS_FILE=$SECRETS/feeds/grants.json"
}
identity_env() { # the billing identity (cell-admin's init takes the same ids in tenants mode)
  echo "USAGE_STREAM_KEY=$(cat "$SECRETS/usage_key")"
  echo "ACCOUNT_ID=$ACCOUNT"
  echo "PROJECT_ID=$DEPLOY_PROJECT"
  echo "CELL_ID=$CELL"
}
k2_env() { # ordinal
  local n=$1 port=$((SRV_BASE_PORT + $1 - 1))
  local rollup=0
  [ "$n" = 1 ] && rollup=1
  grep -E '^[A-Z][A-Z0-9_]*=' "$PROFILE_ENV"
  for kv in $ENGINE_ENV; do echo "$kv"; done
  store_env
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
  feeds_env
  echo "FLEET_AUTH_MODE=workload"
  echo "WORKLOAD_TOKEN_FILE=$SECRETS/feeds/workload.jwt"
  echo "STREAMS_RELEASE_POSTURE=1"
  echo "BILLING_MODE=required"
  identity_env
  echo "ROLLUP=$rollup"
  echo "AUTH_TOKEN=$(cat "$SECRETS/auth_token")"
}
launch_env() { # the one server of a launch cell
  grep -h -E '^[A-Z][A-Z0-9_]*=' "$PROFILE_ENV" "$SHARED_CELL_ENV"
  echo "-KEEP_AWAKE"
  store_env
  echo "REGION=local"
  feeds_env
  identity_env
  echo "AUTH_TOKEN=$(cat "$SECRETS/auth_token")"
}
server_env_file() { # ordinal -> writes $SECRETS/server-N.env (KEY='value' lines)
  local n=$1 out=$SECRETS/server-$1.env
  {
    if [ "$POSTURE" = launch ]; then launch_env; else k2_env "$n"; fi
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
    "$SECRETS/server-$n.env" "$SERVER_BIN" "127.0.0.1:$port" \
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

# ---- dry run: what the point would run, nothing started ---------------------
if [ "$DRY_RUN" = 1 ]; then
  echo "DRY_RUN point: $POINT_NAME ($POINT)"
  echo "DRY_RUN posture: $POSTURE ($SERVERS server$([ "$SERVERS" = 1 ] || echo s))"
  echo "DRY_RUN results: $RUN"
  echo "DRY_RUN ports: emulator $EMU_PORT s3lite $S3_PORT streams-1 $SRV_BASE_PORT" \
    "streams-2 $((SRV_BASE_PORT + 1)) lb $LB_PORT"
  echo "DRY_RUN server binary: $SERVER_BIN sha256 $(sha256_of "$SERVER_BIN")"
  echo "DRY_RUN s3lite binary: $S3LITE_BIN sha256 $(sha256_of "$S3LITE_BIN")"
  echo "DRY_RUN pilot binary: ${PILOT_BIN:-unused (1 server)}"
  echo "DRY_RUN server commit: $EXPECT_COMMIT ($EXPECT_SOURCE)"
  if [ "$TENANTS" = 1 ]; then
    echo "DRY_RUN tenants: $TENANTS_MANIFEST ($(python3 -c 'import json, sys
m = json.load(open(sys.argv[1]))
print("manifest %s, %d projects, periods %s" % (m["name"], sum(c["count"] for c in m["classes"]),
                                               json.dumps(m.get("periods") or {}, sort_keys=True)))' \
      "$TENANTS_MANIFEST")); cell $CELL, deployment project $DEPLOY_PROJECT, account $ACCOUNT"
  fi
  for n in $(seq 1 "$SERVERS"); do
    server_env_file "$n"
    echo "DRY_RUN server-$n.env:"
    python3 "$HERE/scrape.py" showenv "$SECRETS/server-$n.env" --secrets-dir "$SECRETS"
  done
  exit 0
fi
mkdir "$RUN" || die 2 "$RUN exists"
cp "$POINT" "$RUN/point.env"
: > "$MARKS"

log "point $POINT_NAME -> $RUN ($POSTURE posture, ports $EMU_PORT-$LB_PORT, server $SERVER_BIN," \
  "expected commit $EXPECT_COMMIT from $EXPECT_SOURCE)"

# ---- fresh s3lite -------------------------------------------------------
"$S3LITE_BIN" --listen 127.0.0.1:$S3_PORT \
  --latency-ms "$S3LITE_LATENCY_MS" > "$RUN/s3lite.log" 2>&1 < /dev/null &
PID_S3=$!

if [ "$TENANTS" = 1 ]; then
  # ---- the tenants' cell: cell-admin's admission, tokens and feeds ------
  # The cell's signing key (ed25519, as cell-admin init expects), made by
  # bun: macOS's /usr/bin/openssl (LibreSSL) has no ed25519.
  K2_CELL_KEY=$SECRETS/cell.pem bun -e 'require("node:fs").writeFileSync(process.env.K2_CELL_KEY,
    require("node:crypto").generateKeyPairSync("ed25519").privateKey.export({ type: "pkcs8", format: "pem" }),
    { mode: 0o600 })' || die 1 "could not make the cell's signing key"
  TOKENS=$SECRETS/tokens.json
  cp "$TENANTS_MANIFEST" "$RUN/tenants-manifest.json"
  log "admitting $(basename "$TENANTS_MANIFEST")'s projects into $CELL (cell-admin, the launch profiles)"
  bun "$HERE/tenants-admit.ts" admit --manifest "$TENANTS_MANIFEST" --state "$SECRETS/cell" \
    --key-file "$SECRETS/cell.pem" --cell-id "$CELL" --deployment-project "$DEPLOY_PROJECT" --account "$ACCOUNT" \
    --profile "$PROFILE_ENV,$SHARED_CELL_ENV" > "$RUN/admit.log" 2>&1 < /dev/null \
    || die 1 "admission failed; see $RUN/admit.log"
  bun "$HERE/tenants-admit.ts" tokens --manifest "$TENANTS_MANIFEST" --state "$SECRETS/cell" \
    --key-file "$SECRETS/cell.pem" --out "$TOKENS" >> "$RUN/admit.log" 2>&1 < /dev/null \
    || die 1 "token minting failed; see $RUN/admit.log"
  bun "$HERE/tenants-admit.ts" feeds --state "$SECRETS/cell" --out "$SECRETS/feeds" >> "$RUN/admit.log" 2>&1 \
    < /dev/null || die 1 "the feed files failed; see $RUN/admit.log"
  log "$(grep -E '^admit:' "$RUN/admit.log" | tail -1)"
  for i in $(seq 1 50); do curl -sf -o /dev/null "$S3/_s3lite/stats" && break; sleep 0.2; done
  curl -sf -o /dev/null "$S3/_s3lite/stats" || { echo "run-local: s3lite did not start" >&2; exit 1; }
else
  # ---- platform emulator ------------------------------------------------
  node platform-demo/src/emulator.mjs --port $EMU_PORT \
    --cells "$CELL=$SECRETS/feeds" \
    --fixture "$CUSTOMER_PROJECT:$CUSTOMER_WORKSPACE:$CELL" \
    > "$RUN/emulator.log" 2>&1 < /dev/null &
  PIDS_EMU=$!
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
      printf 'authorization: Bearer %s\nstreams-encryption-key: %s\n' "$tok" "$(cat "$SECRETS/stream_key")" \
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
fi

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
    "$PILOT_BIN" > "$RUN/lb.log" 2>&1 < /dev/null &
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
ALLOW_MISMATCH=()
[ "${K2_ALLOW_COMMIT_MISMATCH:-0}" != 1 ] || ALLOW_MISMATCH=(--allow-mismatch)
python3 "$HERE/scrape.py" posture "${SCRAPE_COMMON[@]}" --root "$ROOT" \
  --env-dir "$SECRETS" --point "$POINT_NAME" --base "$BASE" \
  --latency-ms "$S3LITE_LATENCY_MS" --bucket "$BUCKET" --project "$CUSTOMER_PROJECT" \
  --posture "$POSTURE" --port-base "$K2_PORT_BASE" --server-bin "$SERVER_BIN" --s3lite-bin "$S3LITE_BIN" \
  --pilot-bin "$PILOT_BIN" --expect-commit "$EXPECT_COMMIT" --expect-source "$EXPECT_SOURCE" \
  ${ALLOW_MISMATCH[@]+"${ALLOW_MISMATCH[@]}"} || prc=$?
if [ $prc != 0 ] && [ "${K2_ALLOW_COMMIT_MISMATCH:-0}" != 1 ]; then
  echo "run-local: refusing the point (posture.json; set K2_SERVER_COMMIT to the binary's commit," \
    "or K2_ALLOW_COMMIT_MISMATCH=1 to run it void)" >&2
  exit 2
fi
# A tenants point also reads /operator/data.json: the parked waits, which
# the cell's live pool (SSE_MAX_CONNECTIONS) bounds.
OPERATOR=()
[ "$TENANTS" = 0 ] || OPERATOR=(--operator)
python3 "$HERE/scrape.py" loop "${SCRAPE_COMMON[@]}" --interval "$SCRAPE_SECS" \
  --quiesce-min-secs "$QUIESCE_MIN_SECS" --quiet-secs "$QUIESCE_WINDOW_SECS" ${OPERATOR[@]+"${OPERATOR[@]}"} \
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

# period_edges SCALE: "SECS start|end NAME" per edge of the manifest's
# periods, in time order, at most its duration (the run ends there).
period_edges() {
  python3 - "$TENANTS_MANIFEST" "$1" <<'PY'
import json, re, sys
m = json.load(open(sys.argv[1]))
scale, end = float(sys.argv[2]), float(m["duration"]) * float(sys.argv[2])
edges = []
for name, (a, b) in sorted((m.get("periods") or {}).items()):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        print(f"run-local: period {name!r} is not one word: not snapshotted", file=sys.stderr)
        continue
    edges += [(t * scale, i, kind, name) for i, (t, kind) in enumerate(((a, "start"), (b, "end"))) if t * scale <= end]
for t, _, kind, name in sorted(edges):
    print(f"{t:.3f} {kind} {name}")
PY
}
# mark_periods SCALE T0_MS: a boundary snapshot `period-NAME-start|end`
# and a period_start|period_end mark at each edge, timed from T0_MS (taken
# just before the tenants run starts its clock, so an edge comes at most
# about a second early).
mark_periods() {
  local t kind name wait_s
  while read -r t kind name; do
    wait_s=$(python3 -c 'import sys, time; print(max(0.0, int(sys.argv[1]) / 1000 + float(sys.argv[2]) - time.time()))' "$2" "$t")
    sleep "$wait_s"
    boundary "period-$name-$kind" < /dev/null
    mark "period_$kind" "\"name\":\"$name\""
  done < <(period_edges "$1")
}
# run_tenants [--time-scale F]: the manifest's run under the projects'
# tokens, its periods snapshotted.
run_tenants() {
  local scale=1 t0 pid_marks rc=0
  case "${1:-}" in
    '') ;;
    --time-scale) scale=${2:?tenants --time-scale needs a factor} ;;
    *) log "tenants: unknown argument $1 (tenants [--time-scale F])"; return 2 ;;
  esac
  t0=$(now_ms)
  mark_periods "$scale" "$t0" &
  pid_marks=$!
  PHASE_PIDS+=("$pid_marks")
  bun "$HERE/tenants.ts" run --manifest "$TENANTS_MANIFEST" --base "$BASE" --token-map "$TOKENS" \
    --out "$RUN/tenants" --time-scale "$scale" > "$RUN/tenants-run.log" 2>&1 < /dev/null || rc=$?
  # Every edge is at most the run's duration, so a run that ended has
  # passed them all; one that failed early leaves the rest unsnapshotted.
  if [ $rc = 0 ]; then wait "$pid_marks" || true; else stop_pid "$pid_marks" 2; fi
  log "tenants run: $(tail -1 "$RUN/tenants-run.log")"
  return $rc
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
    tenants) run_tenants "${@:2}" || rc=$? ;;
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
if [ "$TENANTS" = 1 ]; then
  # Every project's meters against its ledger, then every produced stream
  # read back (the oracle's leaks, lost acks): after the end snapshot, so
  # neither is counted in a window.
  TARGS=(--manifest "$TENANTS_MANIFEST" --base "$BASE" --token-map "$TOKENS" --out "$RUN/tenants")
  bun "$HERE/tenants.ts" usage "${TARGS[@]}" --max-wait-secs "$USAGE_WAIT_SECS" > "$RUN/tenants-usage.log" 2>&1 \
    < /dev/null || log "tenants usage failed; see $RUN/tenants-usage.log"
  log "tenants usage: $(tail -1 "$RUN/tenants-usage.log")"
  bun "$HERE/tenants.ts" audit "${TARGS[@]}" > "$RUN/tenants-audit.log" 2>&1 < /dev/null \
    || log "tenants audit failed; see $RUN/tenants-audit.log"
  log "tenants audit: $(tail -1 "$RUN/tenants-audit.log")"
  stop_pid "$PID_SCRAPE" 5
  PID_SCRAPE=
  python3 "$HERE/counts.py" "$RUN" > "$RUN/counts.log" 2>&1 || log "counts.py failed; see $RUN/counts.log"
  # price.py prices one project's k2gen ledgers; packing.py prices the
  # instance and its tenants (RUN/packing.json).
  python3 "$HERE/packing.py" price "$RUN" || log "packing.py failed"
else
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
  # The screening counts (requests by cell, PUT bytes, pump flushes) per
  # window: RUN/counts.json; counts.py --compare REF CAND sets two side by side.
  python3 "$HERE/counts.py" "$RUN" || log "counts.py failed"
fi
[ $PHASE_FAIL = 0 ] || { log "one or more phases failed: the point is void"; exit 3; }
