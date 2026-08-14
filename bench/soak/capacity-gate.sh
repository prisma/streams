#!/bin/bash
# The decisive R25-H/R26-11 capacity campaign: sustained ingest, a
# 3-minute absorber pause under load, restart at maximum backlog, and
# exact op-ledger reconciliation. One region (Singapore by default —
# the prior chaos campaign's venue and the slowest PoP).
#
# Acceptance (evaluate-capacity.py):
#   A. catch-up retirement rate >= 1.25x the steady admitted ingest
#      rate, in EXACT frame bytes (post-pause window); OR
#   B. typed maintenance shedding held the durable backlog under its
#      hard caps throughout (shed attributed by error code, never
#      inferred from a merged throttle count);
#   AND reopen after the max-backlog restart is bounded;
#   AND the exact ledger reconciliation is clean;
#   AND recovery drains within its fixed window.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
S=${SOAK_HOME:?set SOAK_HOME}
export SOAK_RUN_ID=${SOAK_RUN_ID:-"cap-$(date -u +%Y%m%dT%H%M%SZ)-$$"}
export SOAK_PREFIX=${SOAK_PREFIX:-$SOAK_RUN_ID}
export BIN_TAG=${BIN_TAG:-$SOAK_RUN_ID}
R=${SOAK_CAP_REGION:-ap-southeast-1}
CONC=${SOAK_CAP_CONC:-64}
SECS=${SOAK_CAP_SECS:-7200}          # >= 2h sustained
PAUSE_AT=${SOAK_CAP_PAUSE_AT:-5400}  # pause deep into the run
PAUSE_SECS=${SOAK_CAP_PAUSE_SECS:-180}
REOPEN_BOUND=${SOAK_CAP_REOPEN_BOUND:-300}
# R27-3 legs: RESTART=0 for the incompressible pause/recovery run (its
# acceptance includes "no process exit"); INCOMPRESSIBLE=1 switches the
# generator to PRF pads so encoded-frame intensity actually reaches the
# configured bound.
RESTART=${SOAK_CAP_RESTART:-1}
export SOAK_INCOMPRESSIBLE=${SOAK_CAP_INCOMPRESSIBLE:-false}
export SOAK_RECORD_BYTES=${SOAK_CAP_RECORD_BYTES:-1024}
export SOAK_REGIONS="$R"
D="$S/results/$SOAK_RUN_ID"
mkdir -p "$D"
# A retried run id must not inherit a prior attempt's failure marker.
rm -f "$D/FAILED"
echo "$SOAK_RUN_ID" > "$S/current-run-id.txt"
echo "== capacity gate $SOAK_RUN_ID region=$R conc=$CONC secs=$SECS pause@${PAUSE_AT}s"

fail() {
  echo "CAPACITY GATE FAILED at stage: $1" | tee "$D/FAILED"
  echo "namespace preserved; tear down with: SOAK_RUN_ID=$SOAK_RUN_ID ./teardown.sh --yes" | tee -a "$D/FAILED"
  exit 1
}
AUTH=$(cat "$S/auth.txt")
bp() { curl -s --max-time 20 -H "Authorization: Bearer $AUTH" "$1/v1/debug/load"; }
# Pause control must survive local transport blips: two campaigns died
# at EXACTLY this step with curl exit 7 under set -e after an hour of
# poll traffic (macOS ephemeral-port exhaustion — the same failure mode
# reconcile.py's errno-49 backoff already covers; the server answered
# 200 in <1s when probed minutes later). A bounded until-loop with real
# sleeps lets TIME_WAIT sockets recycle; only ~3 minutes of continuous
# failure aborts, and each attempt's curl stderr lands in
# curl-errors.log so the next diagnosis starts from evidence.
ctl() { # ctl <stage> <url> — POST until HTTP 200, else fail <stage>
  local tries=0 code
  until code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
        -X POST -H "Authorization: Bearer $AUTH" "$2" \
        2>>"$D/curl-errors.log") && [ "$code" = 200 ]; do
    tries=$((tries+1))
    [ "$tries" -ge 30 ] && fail "$1"
    echo "  $1: attempt $tries code=${code:-curlfail}; retrying in 6s" | tee -a "$D/events.log"
    sleep 6
  done
}

# SOAK_SKIP_DEPLOY=1 resumes against an already-deployed cell (edge
# routing for a fresh version can wedge; a redeploy re-rolls it — the
# 2026-08-12 gen version 404'd at the edge indefinitely while its
# sibling server version routed fine).
if [ "${SOAK_SKIP_DEPLOY:-0}" != 1 ]; then
  "$HERE/build-upload.sh"                                   || fail build-upload
  python3 "$HERE/provision.py" --run-id "$SOAK_RUN_ID" "$R" || fail provision
  "$HERE/deploy-region.sh" "$R" server                      || fail server
  # Sustained fixed-concurrency shape; the ramp knobs stay off.
  SOAK_BENCH_SHAPE=cap SOAK_BENCH_CONC=$CONC BENCH_TIERS="" BENCH_SECS=$SECS \
    "$HERE/deploy-region.sh" "$R" gen                       || fail gen
fi
python3 "$HERE/verify-running.py" "$R"                    || fail verify
python3 "$HERE/release.py" "$R"                           || fail release
T0=$(date +%s)
SRV=$(cat "$S/url-server-$R.txt")

echo "== sustained window: sampling every 30s (pause at +${PAUSE_AT}s)"
PAUSED=0; RESTARTED=0
while :; do
  NOW=$(( $(date +%s) - T0 ))
  python3 "$HERE/poll.py" >> "$D/poll.log" 2>&1 || true
  if [ "$PAUSED" = 0 ] && [ "$NOW" -ge "$PAUSE_AT" ]; then
    PAUSED=1
    echo "== +${NOW}s ABSORBER PAUSE ($PAUSE_SECS s)" | tee -a "$D/events.log"
    date +%s >> "$D/pause-start.ts"
    ctl pause-on "$SRV/v1/debug/absorb-pause?on=1"
    # Dense sampling through the pause: the backlog growth curve and
    # any typed shed onset are the point of the exercise.
    END_P=$(( $(date +%s) + PAUSE_SECS ))
    KEY=$(cat "$S/skey.txt")
    while [ "$(date +%s)" -lt "$END_P" ]; do
      python3 "$HERE/poll.py" >> "$D/poll.log" 2>&1 || true
      # R27-3 availability probes: reads and the control plane must
      # stay admitted while appends shed. Recorded per sample; the
      # evaluator report includes them and any non-200 fails the gate.
      # `|| RC=000`: a probe curl that cannot CONNECT is local transport
      # trouble, not a server refusal — record it (probes.log shows 000)
      # but never let it kill the driver under set -e, and never count
      # it as a PROBE FAILURE (only a server-sent 4xx/5xx gates; a dead
      # server is separately caught by awsbench errors + reconcile).
      RC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 --retry 2 --retry-delay 2 \
            -H "Authorization: Bearer $AUTH" -H "Stream-Encryption-Key: $KEY" \
            "$SRV/v1/stream/soak-$R-0") || RC=000
      CC=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 --retry 2 --retry-delay 2 \
            -H "Authorization: Bearer $AUTH" "$SRV/v1/streams?limit=10") || CC=000
      echo "$(date +%s) read=$RC catalog=$CC" >> "$D/probes.log"
      case "$RC$CC" in *5*|*4*) echo "PROBE FAILURE read=$RC catalog=$CC" | tee -a "$D/events.log";; esac
      sleep 15
    done
    ctl pause-off "$SRV/v1/debug/absorb-pause?on=0"
    date +%s >> "$D/pause-end.ts"
    if [ "$RESTART" = 1 ]; then
      echo "== absorber resumed; RESTART at max backlog" | tee -a "$D/events.log"
      # Restart leg: redeploy (same version) forces a process replacement
      # while the durable backlog is at its maximum. Reopen time =
      # deploy-complete -> serving with the R26 marker + build identity.
      date +%s >> "$D/restart-start.ts"
      "$HERE/deploy-region.sh" "$R" server || fail restart-deploy
      date +%s >> "$D/restart-deployed.ts"
      SRV=$(cat "$S/url-server-$R.txt")
      RE_T0=$(date +%s)
      until curl -s --max-time 10 "$SRV/livez" 2>/dev/null | grep -q alive \
          && bp "$SRV" | grep -q maintenance_shards; do
        [ $(( $(date +%s) - RE_T0 )) -gt "$REOPEN_BOUND" ] && fail reopen-bound
        sleep 5
      done
      REOPEN=$(( $(date +%s) - RE_T0 ))
      echo "$REOPEN" > "$D/reopen-secs.txt"
      echo "== reopened in ${REOPEN}s (bound ${REOPEN_BOUND}s)" | tee -a "$D/events.log"
      # R27-3: repoint the generator at the replacement version so the
      # post-restart offered load CONTINUES (the R26 run collapsed to
      # ~25 req/s against the retired version-scoped domain). The op
      # ledger survives — same generator process.
      GEN=$(cat "$S/url-gen-$R.txt")
      ENC=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=''))" "$SRV")
      # Retarget MUST land (a stale generator target collapsed offered
      # load to ~25 req/s in R26) — but transport blips get the same
      # bounded tolerance as the pause controls, not instant set -e death.
      RT=0
      until OUT=$(curl -s --max-time 15 -X POST "$GEN/retarget?url=$ENC" 2>>"$D/curl-errors.log"); do
        RT=$((RT+1)); [ "$RT" -ge 30 ] && fail retarget
        sleep 6
      done
      echo "$OUT" | tee -a "$D/events.log"
      echo >> "$D/events.log"
    else
      echo "== absorber resumed; no restart leg this run (catch-up under FULL load)" | tee -a "$D/events.log"
    fi
    # Dense catch-up sampling: the resumed absorber clears a 300+MB
    # backlog in ~2 minutes at full pass throughput, and 30s samples
    # leave criterion A with fewer than the 3 qualifying intervals it
    # needs (the 20260814 rerun measured catch_rate=0 for exactly this
    # reason — the drain outran the sampler, not the other way around).
    END_C=$(( $(date +%s) + ${SOAK_CAP_DENSE_SECS:-600} ))
    while [ "$(date +%s)" -lt "$END_C" ]; do
      python3 "$HERE/poll.py" >> "$D/poll.log" 2>&1 || true
      sleep 10
    done
    RESTARTED=1
  fi
  [ "$NOW" -ge $(( SECS + 120 )) ] && break
  sleep 30
done
[ "$RESTARTED" = 1 ] || fail never-restarted

python3 "$HERE/harvest.py"                       || fail harvest
python3 "$HERE/recovery.py" "$R"                 || fail recovery
python3 "$HERE/reconcile.py" "$R"                || fail reconcile
SOAK_CAP_RESTART=$RESTART python3 "$HERE/evaluate-capacity.py" "$R" || fail evaluate
python3 "$HERE/mkreport.py" > "$D/report.md"     || true

if [ "${PRESERVE_SOAK:-0}" != 1 ]; then
  "$HERE/teardown.sh" --yes || fail teardown
fi
echo "== capacity gate $SOAK_RUN_ID COMPLETE: results in $D"
