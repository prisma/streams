#!/bin/bash
# The Layer B rig (deploy-q5 design §3.1-3.3; triage B7 and D6): one
# shared cell from a release directory, in a linux/amd64 container at
# 1 vCPU and 1 GiB, over s3lite at 20 ms on the host, loaded by a tenants
# manifest of 1,000 projects, judged by shared_verdict.py.
#
#   bench/k2cost/run-shared.sh --release DIR [--manifest M] [--time-scale F]
#       [--binary PATH] [--bun-baseline-sha256 HEX] [--s3lite PATH] [--dry-run]
#   bench/k2cost/run-shared.sh --release DIR --canary
#
# The container is bench/docker/Dockerfile.rig over a staged copy of DIR
# (deploy/, cells/, release.json, the release's own streams-slate), and runs
# `bun deploy/composer/streams/entry.ts` with PORT=8080, CELL_STAGE=scratch
# and the cell's seven platform secret names (an env file, never argv), so
# Layer B runs the real entry, env builder, wrapper and feed poller. The rig
# writes cells/scratch.env into the staged copy, over any the release
# carries: the per-cell keys, the data bucket and the feeds bucket on the
# host's s3lite (host.docker.internal). Triage B2's env builder admits
# CELL_STAGE scratch, cert or production, equal to its cells file's name;
# the rig is scratch (it sets no KEEP_AWAKE_UNTIL_MS, the one key only
# scratch admits). If B2's entry ever checks a cells file against
# release.json, the rig needs its own way past that check.
#
# Certification takes the release's binary only: its sha256 must be
# release.json's server_sha256 and it must be an x86_64 ELF. --binary PATH
# is the layout-5 rehearsal: any x86_64 binary, staged with release.json's
# server_sha256 rewritten to it, and recorded as a rehearsal, which
# shared_verdict.py can never pass.
#
# One run:
#   1. tenants-admit.ts: a fresh cell-admin state on the release's
#      profiles (cell_rig), the manifest's projects admitted, the token
#      map (12 h tokens, re-minted every 8 h in the background), the bundle
#      published to s3lite's feeds bucket.
#   2. the image built, the container started, /health awaited; the
#      identity read from the wrapper's `binary sha256` line and
#      /v1/debug/load; scrape.py loop on /v1/debug/{store,load,usage} and
#      /operator/data.json every 10 s.
#   3. marks: idle_start, RIG_IDLE_SECS (900) idle with every project
#      admitted (the Q0 baseline), load_start, tenants.ts run, load_end,
#      tenants.ts usage (every project's meters against its ledger), audit,
#      q0_start, RIG_Q0_IDLE_SECS (900) idle (the Q0 bar reads 600 s in),
#      end.
#   4. shared_verdict.py layer-b on the run: RUN/layer-b-manifest.json.
#
# Results: $K2_HOME/results/layer-b-<UTC stamp>/ (K2_HOME defaults to
# ~/.streams-k2); secrets in $K2_HOME/secrets/layer-b-<stamp>/ (0700),
# removed at exit. Ports 9591 (s3lite) and 9592 (the cell). One rig per
# machine, never during a gate, the suite or a mutation leg (AGENTS.md §1).
#
# The host: s3lite listens on S3_BIND (127.0.0.1; Docker Desktop reaches a
# host's loopback as host.docker.internal; on Linux set S3_BIND to the
# docker bridge address). On an arm64 host bun runs under Rosetta, which
# lacks AVX: give --bun-baseline-sha256 (the sha256 of
# bun-linux-x64-baseline.zip, Bun 1.3.13, from its release page) or run on
# an x86_64 host.
#
# --canary runs the solo LiveFeed canary (triage B9) on the release's
# binaries in a node linux/amd64 container over this worktree, once
# bench/canary/livefeed-canary.mjs has its CANARY_SHAPE=solo; until then it
# is refused.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"

RIG_STAGE=scratch
CELL_ID=cell_rig
DEPLOY_PROJECT=proj_rig_deploy
ACCOUNT=acct_rig_sink
S3_PORT=9591
CELL_PORT=9592
S3LITE_LATENCY_MS=20
S3_BIND=${S3_BIND:-127.0.0.1}
IDLE_SECS=${RIG_IDLE_SECS:-900}
Q0_IDLE_SECS=${RIG_Q0_IDLE_SECS:-900}
USAGE_WAIT_SECS=${USAGE_WAIT_SECS:-600}
SECRET_NAMES="STREAMS_OPERATOR_TOKEN STREAMS_USAGE_STREAM_KEY STREAMS_CURSOR_KEY STREAMS_DATA_S3_ACCESS_KEY_ID STREAMS_DATA_S3_SECRET_ACCESS_KEY STREAMS_FEEDS_S3_ACCESS_KEY_ID STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"
ENTRY=deploy/composer/streams/entry.ts
BIN_IN_RELEASE=deploy/composer/streams/streams-slate

die() { local rc=$1; shift; echo "run-shared: $*" >&2; exit "$rc"; }
log() { echo "[rig $(date -u +%H:%M:%S)] $*"; }

RELEASE='' BINARY='' MANIFEST=$HERE/tenants/layer-b.json TIME_SCALE=1 DRY_RUN=0 CANARY=0
BUN_BASELINE_SHA256=${BUN_BASELINE_SHA256:-} S3LITE=''
while [ $# -gt 0 ]; do
  case $1 in
    --release) RELEASE=${2:?}; shift 2 ;;
    --binary) BINARY=${2:?}; shift 2 ;;
    --manifest) MANIFEST=${2:?}; shift 2 ;;
    --time-scale) TIME_SCALE=${2:?}; shift 2 ;;
    --bun-baseline-sha256) BUN_BASELINE_SHA256=${2:?}; shift 2 ;;
    --s3lite) S3LITE=${2:?}; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --canary) CANARY=1; shift ;;
    *) die 2 "unknown argument $1 (see the header of $0)" ;;
  esac
done
[ -n "$RELEASE" ] || die 2 "--release DIR is required (a directory cut by scripts/cut-release.sh)"
RELEASE=$(cd "$RELEASE" 2>/dev/null && pwd) || die 2 "no release directory $RELEASE"
[ -f "$RELEASE/release.json" ] || die 2 "$RELEASE has no release.json"
[ -f "$RELEASE/$ENTRY" ] || die 2 "$RELEASE has no $ENTRY (the cell's Composer entry, triage B2)"
for p in compute-1g.env shared-cell.env; do
  [ -f "$RELEASE/deploy/profiles/$p" ] || die 2 "$RELEASE has no deploy/profiles/$p"
done

# file -> "sha256 e_machine"
bin_facts() {
  python3 - "$1" <<'PY'
import hashlib, sys
data = open(sys.argv[1], "rb").read()
print(hashlib.sha256(data).hexdigest(), hex(data[18]) if data[:4] == b"\x7fELF" and len(data) > 18 else "none")
PY
}
json_get() { python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get(sys.argv[2]) or "")' "$1" "$2"; }

RELEASE_SHA=$(json_get "$RELEASE/release.json" server_sha256)
REHEARSAL=false
if [ -n "$BINARY" ]; then
  [ -f "$BINARY" ] || die 2 "no binary $BINARY"
  REHEARSAL=true
  SHIP=$BINARY
else
  SHIP=$RELEASE/$BIN_IN_RELEASE
  [ -f "$SHIP" ] || die 2 "$RELEASE has no $BIN_IN_RELEASE"
fi
read -r SHIP_SHA SHIP_MACHINE < <(bin_facts "$SHIP")
[ "$SHIP_MACHINE" = 0x3e ] || die 2 "$SHIP is not an x86_64 ELF (e_machine $SHIP_MACHINE): the rig runs linux/amd64"
if [ "$REHEARSAL" = false ] && [ "$SHIP_SHA" != "${RELEASE_SHA#sha256:}" ]; then
  die 2 "$SHIP has sha256 $SHIP_SHA, release.json names ${RELEASE_SHA#sha256:}: certification runs the release's binary only"
fi

HOST_ARCH=$(uname -m)
if [ "$HOST_ARCH" = arm64 ] || [ "$HOST_ARCH" = aarch64 ]; then
  [ -n "$BUN_BASELINE_SHA256" ] || die 2 "this $HOST_ARCH host runs linux/amd64 under emulation without AVX, which bun's x64 build needs: give --bun-baseline-sha256 (bun-linux-x64-baseline.zip, Bun 1.3.13) or run on an x86_64 host"
fi
if [ -n "$BUN_BASELINE_SHA256" ] && ! [[ "$BUN_BASELINE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  die 2 "--bun-baseline-sha256 must be 64 lowercase hex digits"
fi

if [ "$CANARY" = 1 ]; then
  grep -q CANARY_SHAPE bench/canary/livefeed-canary.mjs \
    || die 2 "--canary needs the solo LiveFeed canary (CANARY_SHAPE=solo, triage B9), which this tree does not have"
  S3LITE_REL=$RELEASE/s3lite
  [ -f "$S3LITE_REL" ] || die 2 "$RELEASE has no s3lite for the canary"
  exec docker run --rm --platform linux/amd64 -v "$ROOT:/src:ro" -v "$RELEASE:/release:ro" -w /src \
    -e CANARY_SHAPE=solo -e CANARY_SERVER_BIN="/release/$BIN_IN_RELEASE" -e CANARY_S3LITE_BIN=/release/s3lite \
    node:22-bookworm-slim node bench/canary/livefeed-canary.mjs
fi

if [ -z "$S3LITE" ]; then
  S3LITE=$ROOT/target/release/s3lite
  [ -x "$RELEASE/s3lite" ] && [ "$HOST_ARCH" = x86_64 ] && [ "$(uname -s)" = Linux ] && S3LITE=$RELEASE/s3lite
fi
[ -x "$S3LITE" ] || die 2 "no s3lite at $S3LITE (cargo build --locked --release --bin s3lite, or --s3lite PATH)"
[ -f "$MANIFEST" ] || die 2 "no manifest $MANIFEST"

K2_HOME=${K2_HOME:-$HOME/.streams-k2}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN=$K2_HOME/results/layer-b-$STAMP
SECRETS=$K2_HOME/secrets/layer-b-$STAMP
NAME=streams-rig-$(echo "$STAMP" | tr 'A-Z' 'a-z')
IMAGE=$NAME:rig
mkdir -p "$RUN" "$SECRETS"
chmod 700 "$K2_HOME/secrets" "$SECRETS"
CONTEXT=$RUN/rig-context
DATA_BUCKET=rig-data-$(echo "$STAMP" | tr 'A-Z' 'a-z')
FEEDS_BUCKET=rig-feeds-$(echo "$STAMP" | tr 'A-Z' 'a-z')
FEEDS_KEY=cells/$CELL_ID/feeds-bundle.json
# The harness that runs is this checkout's (tenants.ts, tenants-admit.ts
# with deploy/cell-admin, scrape.py, shared_verdict.py, the image's
# Dockerfile): record its trees and any uncommitted path under them;
# shared_verdict.py's harness leg requires the server commit's trees, clean.
CHECKOUT_TREES="bench/k2cost deploy/cell-admin bench/docker"
# shellcheck disable=SC2086 # the tree list splits on purpose
python3 - "$RUN/checkout.json" "$ROOT" $CHECKOUT_TREES <<'PY'
import json, subprocess, sys
out, root, *paths = sys.argv[1:]
def git(*a):
    return subprocess.run(["git", "-C", root, *a], capture_output=True, text=True).stdout.strip()
json.dump({"head": git("rev-parse", "HEAD") or None,
           "trees": {p: git("rev-parse", "--verify", "-q", f"HEAD:{p}") or None for p in paths},
           "dirty": [l for l in git("status", "--porcelain", "--untracked-files=all", "--", *paths).splitlines() if l]},
          open(out, "w"), indent=1)
PY

PID_S3='' PID_SCRAPE='' PID_TOKENS='' STARTED=0
stop_pid() { [ -n "$1" ] || return 0; kill "$1" 2>/dev/null || return 0; wait "$1" 2>/dev/null || true; }
cleanup() {
  local rc=$?
  set +e
  if [ "$STARTED" = 1 ]; then
    docker logs "$NAME" > "$RUN/container.log" 2>&1
    docker rm -f "$NAME" > /dev/null 2>&1
    docker rmi "$IMAGE" > /dev/null 2>&1
  fi
  stop_pid "$PID_SCRAPE"
  stop_pid "$PID_TOKENS"
  stop_pid "$PID_S3"
  rm -rf "$SECRETS"
  [ "$DRY_RUN" = 1 ] || log "torn down (exit $rc); results in $RUN"
  exit $rc
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---- the staged release and the cell's files ---------------------------------
mkdir -p "$CONTEXT"
(cd "$RELEASE" && tar cf - .) | (cd "$CONTEXT" && tar xf -)
if [ "$REHEARSAL" = true ]; then
  cp "$BINARY" "$CONTEXT/$BIN_IN_RELEASE"
  python3 - "$CONTEXT/release.json" "$SHIP_SHA" <<'PY'
import json, sys
path, sha = sys.argv[1:3]
doc = json.load(open(path))
doc["server_sha256"] = ("sha256:" if str(doc.get("server_sha256", "")).startswith("sha256:") else "") + sha
doc["rehearsal"] = True
json.dump(doc, open(path, "w"), indent=1)
PY
fi
mkdir -p "$CONTEXT/cells"
cat > "$CONTEXT/cells/$RIG_STAGE.env" <<EOF
CELL_STAGE=$RIG_STAGE
CELL_ID=$CELL_ID
PROJECT_ID=$DEPLOY_PROJECT
ACCOUNT_ID=$ACCOUNT
STREAMS_AUTH_ISSUER=https://auth.prisma.io
PATH_PREFIX=rig
REGION=local
SLATE_S3_ENDPOINT=http://host.docker.internal:$S3_PORT
SLATE_S3_BUCKET=$DATA_BUCKET
SLATE_S3_REGION=us-east-1
FEEDS_S3_ENDPOINT=http://host.docker.internal:$S3_PORT
FEEDS_S3_BUCKET=$FEEDS_BUCKET
FEEDS_S3_REGION=us-east-1
FEEDS_S3_KEY=$FEEDS_KEY
CPU_LOG_SECS=60
EOF
umask 077
rand() { python3 -c 'import os, base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("="))'; }
OPERATOR=$(rand)
{
  echo "STREAMS_OPERATOR_TOKEN=$OPERATOR"
  echo "STREAMS_USAGE_STREAM_KEY=$(rand)"
  echo "STREAMS_CURSOR_KEY=$(rand)"
  echo "STREAMS_DATA_S3_ACCESS_KEY_ID=rig"
  echo "STREAMS_DATA_S3_SECRET_ACCESS_KEY=$(rand)"
  echo "STREAMS_FEEDS_S3_ACCESS_KEY_ID=rig"
  echo "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY=$(rand)"
} > "$SECRETS/container.env"
printf 'authorization: Bearer %s\n' "$OPERATOR" > "$SECRETS/debug.hdr"
printf '{"accessKeyId": "rig", "secretAccessKey": "%s"}\n' "$(rand)" > "$SECRETS/s3.json"
umask 022
BUILD=(docker build --platform linux/amd64 -f "$ROOT/bench/docker/Dockerfile.rig" -t "$IMAGE")
[ -z "$BUN_BASELINE_SHA256" ] || BUILD+=(--build-arg "BUN_BASELINE_SHA256=$BUN_BASELINE_SHA256")
BUILD+=("$CONTEXT")
RUNC=(docker run -d --name "$NAME" --platform linux/amd64 --cpus 1 --memory 1g --memory-swap 1g
  -p "127.0.0.1:$CELL_PORT:8080" --add-host=host.docker.internal:host-gateway
  --env-file "$SECRETS/container.env" -e PORT=8080 -e "CELL_STAGE=$RIG_STAGE" "$IMAGE")

if [ "$DRY_RUN" = 1 ]; then
  echo "DRY_RUN staged: $CONTEXT (rehearsal: $REHEARSAL, binary sha256 $SHIP_SHA)"
  echo "DRY_RUN cells/$RIG_STAGE.env:"
  sed 's/^/  /' "$CONTEXT/cells/$RIG_STAGE.env"
  echo "DRY_RUN container env names: $(cut -d= -f1 "$SECRETS/container.env" | tr '\n' ' ')"
  echo "DRY_RUN ${BUILD[*]}"
  echo "DRY_RUN ${RUNC[*]}"
  echo "DRY_RUN s3lite: $S3LITE --listen $S3_BIND:$S3_PORT --latency-ms $S3LITE_LATENCY_MS"
  echo "DRY_RUN checkout: $RUN/checkout.json"
  exit 0
fi

for port in $S3_PORT $CELL_PORT; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then die 2 "port $port is in use (another rig running?)"; fi
done
MARKS=$RUN/marks.jsonl
: > "$MARKS"
mark() { python3 -c 'import json, sys, time; print(json.dumps({"t": int(time.time() * 1000), "mark": sys.argv[1]}))' "$1" >> "$MARKS"; }
TENANTS=(bun "$HERE/tenants.ts")
ADMIT=(bun "$HERE/tenants-admit.ts")

# ---- 1. admission, tokens, s3lite, the bundle ---------------------------------
openssl genpkey -algorithm ed25519 -out "$SECRETS/cell.pem" 2>/dev/null
chmod 600 "$SECRETS/cell.pem"
log "admitting $(basename "$MANIFEST")'s projects into $CELL_ID"
"${ADMIT[@]}" admit --manifest "$MANIFEST" --state "$SECRETS/cell" --key-file "$SECRETS/cell.pem" \
  --cell-id "$CELL_ID" --deployment-project "$DEPLOY_PROJECT" --account "$ACCOUNT" \
  --profile "$CONTEXT/deploy/profiles/compute-1g.env,$CONTEXT/deploy/profiles/shared-cell.env" | tail -1
"${ADMIT[@]}" tokens --manifest "$MANIFEST" --state "$SECRETS/cell" --key-file "$SECRETS/cell.pem" \
  --out "$SECRETS/tokens.json" --every 28800 > "$RUN/tokens.log" 2>&1 < /dev/null &
PID_TOKENS=$!
"$S3LITE" --listen "$S3_BIND:$S3_PORT" --latency-ms "$S3LITE_LATENCY_MS" > "$RUN/s3lite.log" 2>&1 < /dev/null &
PID_S3=$!
for _ in $(seq 1 100); do
  [ -s "$SECRETS/tokens.json" ] && curl -sf -o /dev/null "http://127.0.0.1:$S3_PORT/_s3lite/stats" && break
  sleep 0.2
done
[ -s "$SECRETS/tokens.json" ] || die 1 "no token map (see $RUN/tokens.log)"
"${ADMIT[@]}" publish --state "$SECRETS/cell" --upload "s3://$FEEDS_BUCKET/$FEEDS_KEY" \
  --s3-endpoint "http://127.0.0.1:$S3_PORT" --s3-creds "$SECRETS/s3.json"

# ---- 2. the cell -------------------------------------------------------------
log "building the rig image from $RELEASE"
"${BUILD[@]}" > "$RUN/docker-build.log" 2>&1 || die 1 "docker build failed (see $RUN/docker-build.log)"
STARTED=1
"${RUNC[@]}" > /dev/null
for _ in $(seq 1 300); do curl -sf -o /dev/null "http://127.0.0.1:$CELL_PORT/health" && break; sleep 1; done
curl -sf -o /dev/null "http://127.0.0.1:$CELL_PORT/health" || die 1 "the cell never answered /health (see docker logs $NAME)"
WRAPPER_SHA=$(docker logs "$NAME" 2>&1 | grep -oE 'binary sha256 [0-9a-f]{64}' | tail -1 | awk '{print $3}' || true)
curl -sf -H @"$SECRETS/debug.hdr" "http://127.0.0.1:$CELL_PORT/v1/debug/load" > "$RUN/debug-load-boot.json"
python3 - "$RUN/rig.json" "$RUN/debug-load-boot.json" "$RELEASE" "$CONTEXT" "$SHIP" "$SHIP_SHA" "$WRAPPER_SHA" "$REHEARSAL" "$RIG_STAGE" "$RUN/checkout.json" <<'PY'
import hashlib, json, os, sys, time
out, load_file, release, context, ship, ship_sha, wrapper, rehearsal, stage, checkout = sys.argv[1:11]
load = json.load(open(load_file))
sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()
prof = {n: {"path": os.path.join(context, "deploy", "profiles", n), "sha256": sha(os.path.join(context, "deploy", "profiles", n))}
        for n in ("compute-1g.env", "shared-cell.env")}
cell_env = os.path.join(context, "cells", f"{stage}.env")
rel = json.load(open(os.path.join(release, "release.json")))
json.dump({
    "arm": "layer-b", "rehearsal": rehearsal == "true", "release_dir": release, "release": rel,
    "commit": load.get("git_commit"), "region": "local",
    "binary": {"path": ship, "sha256": ship_sha}, "wrapper_sha256": wrapper or None,
    "debug_load_sha256": load.get("binary_sha256"), "boot_id": load.get("boot_id"),
    "profiles": prof, "cell_env": {"path": cell_env, "sha256": sha(cell_env)},
    "checkout": json.load(open(checkout)),
    "started_unix": int(time.time()), "finished_unix": None,
}, open(out, "w"), indent=1)
PY
python3 "$HERE/scrape.py" loop --run "$RUN" --servers "streams-1=http://127.0.0.1:$CELL_PORT" \
  --s3lite "http://127.0.0.1:$S3_PORT" --auth-file "$SECRETS/debug.hdr" --operator \
  > "$RUN/scrape.log" 2>&1 < /dev/null &
PID_SCRAPE=$!

# ---- 3. idle, load, drain, usage, audit, idle ------------------------------------
mark idle_start
log "idle ${IDLE_SECS} s with every project admitted (the Q0 baseline)"
sleep "$IDLE_SECS"
mark load_start
log "tenants run: $(basename "$MANIFEST") at time scale $TIME_SCALE"
TARGS=(--manifest "$MANIFEST" --base "http://127.0.0.1:$CELL_PORT" --token-map "$SECRETS/tokens.json" --out "$RUN/tenants")
"${TENANTS[@]}" run "${TARGS[@]}" --time-scale "$TIME_SCALE" | tail -1
mark load_end
"${TENANTS[@]}" usage "${TARGS[@]}" --max-wait-secs "$USAGE_WAIT_SECS" | tail -1
"${TENANTS[@]}" audit "${TARGS[@]}" | tail -1
mark q0_start
log "idle ${Q0_IDLE_SECS} s after the load (the Q0 bar)"
sleep "$Q0_IDLE_SECS"
mark end
python3 - "$RUN/rig.json" <<'PY'
import json, sys, time
doc = json.load(open(sys.argv[1]))
doc["finished_unix"] = int(time.time())
json.dump(doc, open(sys.argv[1], "w"), indent=1)
PY

# ---- 4. the verdict --------------------------------------------------------------
python3 "$HERE/shared_verdict.py" layer-b "$RUN"
