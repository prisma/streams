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

"$HERE/build-upload.sh"                                   || fail build-upload
python3 "$HERE/provision.py" --run-id "$SOAK_RUN_ID" "$R" || fail provision
"$HERE/deploy-region.sh" "$R" server                      || fail server
# Sustained fixed-concurrency shape; the ramp knobs stay off.
SOAK_BENCH_SHAPE=cap SOAK_BENCH_CONC=$CONC BENCH_TIERS="" BENCH_SECS=$SECS \
  "$HERE/deploy-region.sh" "$R" gen                       || fail gen
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
    curl -s --max-time 15 -o /dev/null -X POST -H "Authorization: Bearer $AUTH" \
      "$SRV/v1/debug/absorb-pause?on=1"
    # Dense sampling through the pause: the backlog growth curve and
    # any typed shed onset are the point of the exercise.
    END_P=$(( $(date +%s) + PAUSE_SECS ))
    while [ "$(date +%s)" -lt "$END_P" ]; do
      python3 "$HERE/poll.py" >> "$D/poll.log" 2>&1 || true
      sleep 15
    done
    curl -s --max-time 15 -o /dev/null -X POST -H "Authorization: Bearer $AUTH" \
      "$SRV/v1/debug/absorb-pause?on=0"
    date +%s >> "$D/pause-end.ts"
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
    RESTARTED=1
  fi
  [ "$NOW" -ge $(( SECS + 120 )) ] && break
  sleep 30
done
[ "$RESTARTED" = 1 ] || fail never-restarted

python3 "$HERE/harvest.py"                       || fail harvest
python3 "$HERE/recovery.py" "$R"                 || fail recovery
python3 "$HERE/reconcile.py" "$R"                || fail reconcile
python3 "$HERE/evaluate-capacity.py" "$R"        || fail evaluate
python3 "$HERE/mkreport.py" > "$D/report.md"     || true

if [ "${PRESERVE_SOAK:-0}" != 1 ]; then
  "$HERE/teardown.sh" --yes || fail teardown
fi
echo "== capacity gate $SOAK_RUN_ID COMPLETE: results in $D"
