#!/bin/bash
# L6 spike M1: the arm-identity pair, rerun back to back (SPIKE-PLAN.md §2.1,
# review A6). The first unmodified run (L6S-unmod-...-20261011T132228Z) is
# void: its token-refresh loop's `sleep 240` was SIGTERMed by something
# outside the rig, the token expired 600 s later and half the load got 401.
# Both points uncapped (the unmodified s3lite has no bandwidth cap), each
# waits for every other heavy leg plus 120 s of quiet (spike-run.sh QUIET),
# with the token guard on (tools/token-guard.sh; the sibling q0 rigs relaunch
# within minutes of each other, so a 600 s quiet never came).
#
#   bash ~/.streams-k2/analysis/public-beta/l6/tools/m1-identity-queue.sh [FIRST]
set -uo pipefail
L6=/Users/sorenschmidt/.streams-k2/analysis/public-beta/l6
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
BASE_ROOT=/Users/sorenschmidt/.streams-k2/l6-base-root
RUN="$L6/tools/spike-run.sh M1 ${M1_RUN_MODE:-WAIT=1 QUIET=120} GUARD=1"
SA="PAYLOAD=b64rand RECORDS=10 RECORD_BYTES=1024"
COMMON="ARM=base SHARDS=1 STREAMS=8 MBPS=1 WAL_GAP=100 S3LITE_PUT_BPS=0"
POINTS=(
  "ROOT=$BASE_ROOT POINT=$W/bench/k2cost/points/L6S.env L6_LABEL=unmod $COMMON $SA"
  "$COMMON $SA"
)
FIRST=${1:-1}
echo "[m1-identity $(date '+%F %T')] ${#POINTS[@]} points, from $FIRST; HEAD $(git -C "$W" rev-parse --short HEAD)"
for i in "${!POINTS[@]}"; do
  n=$((i + 1))
  [ "$n" -ge "$FIRST" ] || continue
  if [ -e "$L6/STOP" ]; then
    echo "[m1-identity $(date '+%F %T')] STOP file present; stopping before point $n"
    exit 0
  fi
  echo "[m1-identity $(date '+%F %T')] point $n/${#POINTS[@]}: ${POINTS[$i]}"
  rc=0
  # shellcheck disable=SC2086
  $RUN ${POINTS[$i]} || rc=$?
  echo "[m1-identity $(date '+%F %T')] point $n done rc=$rc"
done
echo "[m1-identity $(date '+%F %T')] QUEUE DONE"
