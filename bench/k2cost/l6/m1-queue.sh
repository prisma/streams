#!/bin/bash
# L6 spike M1: the base-arm grid, one point at a time (SPIKE-PLAN.md M1,
# review B4/A6). Each point waits for any other heavy leg on the host
# (spike-run.sh WAIT=1). Touch $L6/STOP to stop after the running point.
#
#   bash ~/.streams-k2/analysis/public-beta/l6/tools/m1-queue.sh [FIRST]
#     FIRST = 1-based index of the first point to run (default 1)
set -uo pipefail
L6=/Users/sorenschmidt/.streams-k2/analysis/public-beta/l6
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
BASE_ROOT=/Users/sorenschmidt/.streams-k2/l6-base-root
RUN="$L6/tools/spike-run.sh M1 WAIT=1"
SA="PAYLOAD=b64rand RECORDS=10 RECORD_BYTES=1024"
SB="PAYLOAD=corpus RECORDS=10 RECORD_BYTES=1024"
SC="PAYLOAD=corpus RECORDS=100 RECORD_BYTES=1024"
SD="PAYLOAD=corpus RECORDS=10 RECORD_BYTES=2048"
COMMON="ARM=base SHARDS=1 STREAMS=8"
POINTS=(
  # identity pair, back to back, both uncapped: unmodified 50fab9d9, then the instrumented build
  "ROOT=$BASE_ROOT POINT=$W/bench/k2cost/points/L6S.env L6_LABEL=unmod S3LITE_PUT_BPS=0 $COMMON MBPS=1 WAL_GAP=100 $SA"
  "S3LITE_PUT_BPS=0 $COMMON MBPS=1 WAL_GAP=100 $SA"
  # the grid at the 25 MB/s PUT cap (L6S.env default)
  "$COMMON MBPS=1 WAL_GAP=100 $SA"
  "$COMMON MBPS=1 WAL_GAP=1000 $SA"
  "$COMMON MBPS=1 WAL_GAP=100 $SB"
  "$COMMON MBPS=1 WAL_GAP=1000 $SB"
  "$COMMON MBPS=1 WAL_GAP=100 $SC"
  "$COMMON MBPS=1 WAL_GAP=1000 $SC"
  "$COMMON MBPS=1 WAL_GAP=100 $SD"
  "$COMMON MBPS=1 WAL_GAP=1000 $SD"
  "$COMMON MBPS=0.5 WAL_GAP=100 PAYLOAD=corpus RECORDS=1 RECORD_BYTES=1024"
  "$COMMON RATE=400 WAL_GAP=100 PAYLOAD=corpus RECORDS=1 RECORD_BYTES=300"
)
FIRST=${1:-1}
echo "[m1-queue $(date '+%F %T')] ${#POINTS[@]} points, from $FIRST; HEAD $(git -C "$W" rev-parse --short HEAD)"
for i in "${!POINTS[@]}"; do
  n=$((i + 1))
  [ "$n" -ge "$FIRST" ] || continue
  if [ -e "$L6/STOP" ]; then
    echo "[m1-queue $(date '+%F %T')] STOP file present; stopping before point $n"
    exit 0
  fi
  echo "[m1-queue $(date '+%F %T')] point $n/${#POINTS[@]}: ${POINTS[$i]}"
  # shellcheck disable=SC2086
  $RUN ${POINTS[$i]}
  echo "[m1-queue $(date '+%F %T')] point $n done rc=$?"
done
echo "[m1-queue $(date '+%F %T')] QUEUE DONE"
