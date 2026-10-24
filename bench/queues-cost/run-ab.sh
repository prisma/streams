#!/bin/bash
# The queue cost benchmark on the local costab rig
# (docs/QUEUES-PRICING-BENCHMARK.md §6, docs/COST-METHODOLOGY.md):
#   s3lite (25 ms/op, /_s3lite/stats2 ledger) <- streams-slate (soak7 env)
#     <- awsbench wide appenders (the baseline co-tenant)
#     <- replay.mjs (the queue scenario)
#
# Usage: run-ab.sh <mode> <label> <server-binary> <out-dir>
#   ab          arm A: baseline only; arm B: baseline + queue replay
#               -> costs.json and cf_bill.py's verdict
#   idle        arm A: baseline only; arm B: baseline + consumers, no sends
#               -> idle.json (object requests over the baseline)
#   standalone  arm A: an empty cell; arm B: the replay alone
#               -> standalone.json (feed it to an ab run: STANDALONE_DIR)
#
# Each arm runs on a fresh s3lite and a fresh PATH_PREFIX (methodology
# invariant 2), with the same binaries; the arms run one after the other.
# Per arm: boot, mark t0, baseline setup, WARMUP_S, mark start, [BURST_WINDOW_S,
# mark burst_end], the rest of DURATION_S, mark end, the replay's drain
# (DRAIN_S at most), mark final, teardown. Only start..end is priced.
#
# Environment (defaults in brackets):
#   DURATION_S [3600]  WARMUP_S [300]  BURST_WINDOW_S [0]  DRAIN_S [900]
#   SCALE [1]  COMPRESS [1]  PROFILE [day]  START_HOUR [0]  REPLAY_ARGS []
#   BASELINE [1; 0 in standalone]  BASELINE_STREAMS [20]
#   BASELINE_INTERVAL_MS [500]  BASELINE_BATCH [10]  BASELINE_RECORD_BYTES [1024]
#   BASELINE_PREFIX [base-] the baseline streams' names: the same in every
#     arm and run, so they hash to the same shards (a shard's WAL flushes
#     depend on which appenders it holds; arms are isolated by PATH_PREFIX
#     and a fresh s3lite, not by stream names)
#   S3LITE_BIN [target/release/s3lite, else bench/costab/bin/s3lite-ab]
#   AWSBENCH_BIN [bench/costab/bin/awsbench-ab]  S3LITE_LATENCY_MS [25]
#   S3_PORT [9500]  SRV_PORT [8090]  SERVER_ENV_EXTRA ["K=V ..."]
#   COSTAB_KEY_FILE [~/.streams-soak/costab-streamkey.txt]
#   STANDALONE_DIR [] a standalone run's out dir, priced into costs.json
#   CELL_VCPUS [1]  CELL_MEM_GIB [1]  UTILIZATION [0.6]  IDLE_TOLERANCE [0]
#   ARMS ["A B"]
# One run per machine at a time; no builds or other load during a run.
set -euo pipefail

MODE=${1:?mode: ab|idle|standalone}
LABEL=${2:?label}
SERVER_BIN=${3:?server binary}
OUT=${4:?out dir}
case "$MODE" in ab|idle|standalone) ;; *) echo "run-ab: mode is ab, idle or standalone" >&2; exit 2 ;; esac
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
[ -x "$SERVER_BIN" ] || { echo "run-ab: no server binary at $SERVER_BIN" >&2; exit 2; }

DURATION_S=${DURATION_S:-3600}
WARMUP_S=${WARMUP_S:-300}
BURST_WINDOW_S=${BURST_WINDOW_S:-0}
DRAIN_S=${DRAIN_S:-900}
SCALE=${SCALE:-1}
COMPRESS=${COMPRESS:-1}
PROFILE=${PROFILE:-day}
START_HOUR=${START_HOUR:-0}
REPLAY_ARGS=${REPLAY_ARGS:-}
if [ "$MODE" = standalone ]; then BASELINE=${BASELINE:-0}; else BASELINE=${BASELINE:-1}; fi
BASELINE_STREAMS=${BASELINE_STREAMS:-20}
BASELINE_INTERVAL_MS=${BASELINE_INTERVAL_MS:-500}
BASELINE_BATCH=${BASELINE_BATCH:-10}
BASELINE_RECORD_BYTES=${BASELINE_RECORD_BYTES:-1024}
BASELINE_PREFIX=${BASELINE_PREFIX:-base-}
if [ -z "${S3LITE_BIN:-}" ]; then
  if [ -x "$ROOT/target/release/s3lite" ]; then S3LITE_BIN=$ROOT/target/release/s3lite
  else S3LITE_BIN=$ROOT/bench/costab/bin/s3lite-ab; fi
fi
AWSBENCH_BIN=${AWSBENCH_BIN:-$ROOT/bench/costab/bin/awsbench-ab}
S3LITE_LATENCY_MS=${S3LITE_LATENCY_MS:-25}
S3_PORT=${S3_PORT:-9500}
SRV_PORT=${SRV_PORT:-8090}
SERVER_ENV_EXTRA=${SERVER_ENV_EXTRA:-}
KEY_FILE=${COSTAB_KEY_FILE:-${SOAK_HOME:-$HOME/.streams-soak}/costab-streamkey.txt}
STANDALONE_DIR=${STANDALONE_DIR:-}
CELL_VCPUS=${CELL_VCPUS:-1}
CELL_MEM_GIB=${CELL_MEM_GIB:-1}
UTILIZATION=${UTILIZATION:-0.6}
IDLE_TOLERANCE=${IDLE_TOLERANCE:-0}
ARMS=${ARMS:-A B}
S3=http://127.0.0.1:$S3_PORT
SRV=http://127.0.0.1:$SRV_PORT

[ -x "$S3LITE_BIN" ] || { echo "run-ab: no s3lite at $S3LITE_BIN" >&2; exit 2; }
[ -f "$KEY_FILE" ] || { echo "run-ab: no stream key file at $KEY_FILE (RUNBOOK §12)" >&2; exit 2; }
if [ "$BASELINE" = 1 ]; then
  [ -x "$AWSBENCH_BIN" ] || { echo "run-ab: no awsbench at $AWSBENCH_BIN" >&2; exit 2; }
fi
if [ "$DURATION_S" -le "$BURST_WINDOW_S" ]; then
  echo "run-ab: BURST_WINDOW_S must be shorter than DURATION_S" >&2; exit 2
fi
# A compressed replay carries 1/COMPRESS of the month-start burst, which a
# burst window would then count once: only the uncompressed run has one.
if [ "$BURST_WINDOW_S" -gt 0 ] && [ "$COMPRESS" != 1 ]; then
  echo "run-ab: a burst window needs COMPRESS=1" >&2; exit 2
fi
for port in "$S3_PORT" "$SRV_PORT"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "run-ab: port $port is in use (another run?)" >&2; exit 2
  fi
done
read -r -a EXTRA_ENV <<< "$SERVER_ENV_EXTRA"
read -r -a EXTRA_REPLAY <<< "$REPLAY_ARGS"

mkdir -p "$OUT"
log() { echo "[qcost $(date -u +%H:%M:%S)] $*"; }

# ---- secrets: files only, removed at exit -------------------------------
SECRETS=$(mktemp -d "${TMPDIR:-/tmp}/qcost.XXXXXX")
chmod 700 "$SECRETS"
umask 077
od -An -tx1 -N24 /dev/urandom | tr -d ' \n' > "$SECRETS/auth"
printf 'authorization: Bearer %s\n' "$(cat "$SECRETS/auth")" > "$SECRETS/auth.hdr"
umask 022

# ---- teardown -----------------------------------------------------------
PID_S3='' PID_SRV='' PID_GEN='' PID_SAMPLER='' PID_REPLAY=''
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
stop_arm() {
  stop_pid "$PID_REPLAY" 10; PID_REPLAY=''
  stop_pid "$PID_GEN" 5; PID_GEN=''
  stop_pid "$PID_SAMPLER" 5; PID_SAMPLER=''
  stop_pid "$PID_SRV" 30; PID_SRV=''
  stop_pid "$PID_S3" 5; PID_S3=''
}
cleanup() {
  local rc=$?
  set +e
  stop_arm
  wait 2>/dev/null
  rm -rf "$SECRETS"
  log "torn down (exit $rc); results in $OUT"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---- one arm --------------------------------------------------------------
mark() { # dir tag
  node "$HERE/collect.mjs" mark --dir "$1" --tag "$2" --pid "$PID_SRV" --s3 "$S3" \
    --server "$SRV" --auth-file "$SECRETS/auth" >> "$1/marks.log" 2>&1 || log "mark $2 failed (see $1/marks.log)"
}

run_arm() { # arm baseline(0|1) load(none|replay|idle)
  local arm=$1 baseline=$2 load=$3
  local dir=$OUT/$arm
  rm -rf "$dir"
  mkdir -p "$dir"
  log "arm $arm: baseline=$baseline load=$load"
  printf '{"arm":"%s","mode":"%s","baseline":%s,"load":"%s","label":"%s","server_bin":"%s","s3lite_bin":"%s"}\n' \
    "$arm" "$MODE" "$baseline" "$load" "$LABEL" "$SERVER_BIN" "$S3LITE_BIN" > "$dir/arm.json"

  "$S3LITE_BIN" --listen "127.0.0.1:$S3_PORT" --latency-ms "$S3LITE_LATENCY_MS" \
    > "$dir/s3lite.log" 2>&1 < /dev/null &
  PID_S3=$!
  for _ in $(seq 1 50); do curl -sf -o /dev/null "$S3/_s3lite/stats" && break; sleep 0.2; done
  curl -sf -o /dev/null "$S3/_s3lite/stats" || { echo "run-ab: s3lite did not start" >&2; exit 1; }

  # The costab posture (soak7 env, docs/COST-METHODOLOGY.md "The rig").
  ( AUTH_TOKEN=$(cat "$SECRETS/auth"); export AUTH_TOKEN
    exec env SLATE_S3_ENDPOINT="$S3" SLATE_S3_BUCKET=qcost \
      SLATE_S3_REGION=local SLATE_S3_ACCESS_KEY_ID=test SLATE_S3_SECRET_ACCESS_KEY=test \
      PATH_PREFIX="$LABEL-$arm" INSTANCE_NAME=streams-1 INITIAL_SHARDS=4 \
      WAL_GROUP_COMMIT=1 WAL_FLUSH_GAP_MS=100 FLUSH_INTERVAL_MS=25 \
      WAL_POST_ACK_GATHER_MS=6 TAIL_RING_BYTES=33554432 STREAMS_DEBUG_TIMING=0 \
      FRAME_COMPRESS=1 L0_SST_SIZE_BYTES=16777216 MAX_UNFLUSHED_BYTES=33554432 \
      L0_MAX_SSTS=64 \
      COMPACTOR_MAX_CONCURRENT=2 SHARED_CACHE_BYTES=67108864 SLATEDB_RT_THREADS=2 \
      ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 ADMIT_RSS_SHED_MB=600 \
      ABSORB_BYTES=4194304 ABSORB_AGE_SECS=60 \
      TRIM_PER_OP=65536 RUST_LOG=warn \
      ${EXTRA_ENV[@]+"${EXTRA_ENV[@]}"} \
      "$SERVER_BIN" --listen "127.0.0.1:$SRV_PORT" ) > "$dir/server.log" 2>&1 < /dev/null &
  PID_SRV=$!
  for _ in $(seq 1 60); do
    curl -sf -o /dev/null -H @"$SECRETS/auth.hdr" "$SRV/v1/debug/store" && break
    kill -0 "$PID_SRV" 2>/dev/null || { echo "run-ab: server died at boot (see $dir/server.log)" >&2; exit 1; }
    sleep 1
  done
  curl -sf -o /dev/null -H @"$SECRETS/auth.hdr" "$SRV/v1/debug/store" || { echo "run-ab: server did not answer" >&2; exit 1; }

  mark "$dir" t0
  node "$HERE/collect.mjs" sample --dir "$dir" --pid "$PID_SRV" --s3 "$S3" > "$dir/sampler.log" 2>&1 &
  PID_SAMPLER=$!

  local total=$((WARMUP_S + DURATION_S))
  if [ "$baseline" = 1 ]; then
    ( STREAM_KEY=$(cat "$KEY_FILE"); export STREAM_KEY
      AUTH_TOKEN=$(cat "$SECRETS/auth"); export AUTH_TOKEN
      exec env BENCH_SYSTEM=prisma BENCH_SHAPE=wide BENCH_TARGET="$SRV" \
        BENCH_WIDE_STREAMS="$BASELINE_STREAMS" BENCH_WIDE_ACTIVE="$BASELINE_STREAMS" \
        BENCH_WIDE_SECS=$((total + DRAIN_S + 120)) BENCH_WIDE_APPEND_INTERVAL_MS="$BASELINE_INTERVAL_MS" \
        BENCH_WIDE_SCAN_RPS=0 BENCH_WIDE_SETUP_CONC=8 \
        BENCH_BATCH="$BASELINE_BATCH" BENCH_RECORD_BYTES="$BASELINE_RECORD_BYTES" \
        BENCH_STREAM="$BASELINE_PREFIX" BENCH_OUT="$dir/baseline.jsonl" \
        "$AWSBENCH_BIN" ) > "$dir/baseline.log" 2>&1 < /dev/null &
    PID_GEN=$!
    for _ in $(seq 1 120); do
      grep -q SETUP_DONE "$dir/baseline.log" 2>/dev/null && break
      kill -0 "$PID_GEN" 2>/dev/null || { echo "run-ab: baseline generator died (see $dir/baseline.log)" >&2; exit 1; }
      sleep 1
    done
    grep -q SETUP_DONE "$dir/baseline.log" || { echo "run-ab: baseline setup did not finish" >&2; exit 1; }
  fi

  if [ "$load" != none ]; then
    # The month-start invoice burst is replayed only into a burst window,
    # where it is counted once; without one a rate-scaled window would
    # count it every hour of the month.
    local extra=()
    if [ "$load" = idle ]; then extra=(--idle)
    elif [ "$BURST_WINDOW_S" -eq 0 ]; then extra=(--no-invoice-burst); fi
    node "$HERE/replay.mjs" --base "$SRV" --auth-file "$SECRETS/auth" --key-file "$KEY_FILE" \
      --scale "$SCALE" --compress "$COMPRESS" --profile "$PROFILE" --start-hour "$START_HOUR" \
      --burst-at-s "$WARMUP_S" --duration-s "$total" --drain-s "$DRAIN_S" \
      --out "$dir/replay-summary.json" --timeline "$dir/replay-timeline.jsonl" \
      ${extra[@]+"${extra[@]}"} ${EXTRA_REPLAY[@]+"${EXTRA_REPLAY[@]}"} \
      > "$dir/replay.log" 2>&1 < /dev/null &
    PID_REPLAY=$!
  fi

  sleep "$WARMUP_S"
  mark "$dir" start
  if [ "$BURST_WINDOW_S" -gt 0 ]; then
    sleep "$BURST_WINDOW_S"
    mark "$dir" burst_end
    sleep $((DURATION_S - BURST_WINDOW_S))
  else
    sleep "$DURATION_S"
  fi
  mark "$dir" end

  local replay_rc=0
  if [ -n "$PID_REPLAY" ]; then
    wait "$PID_REPLAY" || replay_rc=$?
    PID_REPLAY=''
    log "arm $arm: replay exited $replay_rc ($(tail -1 "$dir/replay.log"))"
  fi
  mark "$dir" final
  stop_arm
  return 0
}

for arm in $ARMS; do
  case "$MODE:$arm" in
    ab:A|idle:A) run_arm A "$BASELINE" none ;;
    ab:B) run_arm B "$BASELINE" replay ;;
    idle:B) run_arm B "$BASELINE" idle ;;
    standalone:A) run_arm A 0 none ;;
    standalone:B) run_arm B 0 replay ;;
    *) echo "run-ab: unknown arm $arm" >&2; exit 2 ;;
  esac
done

# ---- price and verdict --------------------------------------------------
CELL_ARGS=(--cell-vcpus "$CELL_VCPUS" --cell-mem-gib "$CELL_MEM_GIB" --utilization "$UTILIZATION")
RC=0
case "$MODE" in
  ab)
    SA=()
    [ -n "$STANDALONE_DIR" ] && SA=(--sa "$STANDALONE_DIR/A" --sb "$STANDALONE_DIR/B")
    node "$HERE/collect.mjs" price --a "$OUT/A" --b "$OUT/B" ${SA[@]+"${SA[@]}"} \
      --scale "$SCALE" "${CELL_ARGS[@]}" --out "$OUT/costs.json" | tee "$OUT/costs.txt"
    python3 "$HERE/cf_bill.py" --scale "$SCALE" --measured "$OUT/costs.json" | tee "$OUT/verdict.txt"
    grep -q 'goal MET' "$OUT/verdict.txt" || RC=1
    ;;
  standalone)
    node "$HERE/collect.mjs" price --a "$OUT/A" --b "$OUT/B" --sa "$OUT/A" --sb "$OUT/B" \
      --scale "$SCALE" "${CELL_ARGS[@]}" --out "$OUT/standalone.json" | tee "$OUT/standalone.txt"
    if grep -q '"void": true' "$OUT/standalone.json"; then RC=1; fi
    ;;
  idle)
    node "$HERE/collect.mjs" idle --a "$OUT/A" --b "$OUT/B" --tolerance "$IDLE_TOLERANCE" \
      --out "$OUT/idle.json" | tee "$OUT/idle.txt" || RC=1
    ;;
esac
log "$MODE $LABEL done (exit $RC)"
exit $RC
