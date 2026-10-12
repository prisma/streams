#!/bin/bash
# L6 spike M5: the arm-b census, one point at a time (SPIKE-PLAN.md M5 with
# SPIKE-PLAN-REVIEW.md B4/C1/C3/A2; the base arm comes from M1). Each point
# waits for any other heavy leg on the host (spike-run.sh WAIT=1). Touch
# $L6/STOP to stop after the running point.
#
#   bash ~/.streams-k2/analysis/public-beta/l6/tools/m5-queue.sh [FIRST]
#     FIRST = 1-based index of the first point to run (default 1)
#
# Deviations from the plan's extra-run table, each recorded in M5/NOTES.md:
# - "S-d with hseg PUTs capped at 25 MB/s": since review B4 every L6S point
#   (both arms) runs with s3lite's 25 MB/s cap on every PUT body, so the grid's
#   S-d/100 ms point IS the capped point; the extra run is the uncapped S-d
#   (S3LITE_PUT_BPS=0), which P7's "<= 0.4 s uncapped" bar needs.
# - The pages control: at WAL_GAP=100 the 48-object seal trigger fires every
#   ~4.8 s, before either memtable dimension can reach SlateDB's 8 MiB, so a
#   100 ms control cannot freeze by construction (pre-registered here). The
#   control that can show the F1 signature runs at WAL_GAP=1000 (S-f singles,
#   ~0.55 MB/s of memtable rows: 8 MiB in ~15 s, before the 45 s age trigger),
#   beside the same short point with the memtable trigger. Both are short
#   points (300 s load, 120 s tail, 60 s floor): they measure P4 only. The
#   plan's 100 ms control also runs, short, last.
# - Arm identity: base arm, S-a/100 ms, uncapped, against M1's three
#   instrumented uncapped S-a runs (the build-mean rule of M1 finding 3).
set -uo pipefail
L6=/Users/sorenschmidt/.streams-k2/analysis/public-beta/l6
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
RUN="$L6/tools/spike-run.sh M5 WAIT=1 MIN_FREE_GIB=${M5_MIN_FREE_GIB:-15}"
SA="PAYLOAD=b64rand RECORDS=10 RECORD_BYTES=1024"
SB="PAYLOAD=corpus RECORDS=10 RECORD_BYTES=1024"
SC="PAYLOAD=corpus RECORDS=100 RECORD_BYTES=1024"
SD="PAYLOAD=corpus RECORDS=10 RECORD_BYTES=2048"
SF="RATE=400 PAYLOAD=corpus RECORDS=1 RECORD_BYTES=300"
B="ARM=b SHARDS=1 STREAMS=8"
SHORT="LOAD_SECS=300 TAIL_SECS=120 FLOOR_SECS=60"
POINTS=(
  "$B MBPS=1 WAL_GAP=100 $SA"
  "$B $SHORT WAL_GAP=1000 $SF L6_LABEL=b-pages-short L6_EXTRA=L6_SPILL_TRIGGER=pages"
  "$B $SHORT WAL_GAP=1000 $SF L6_LABEL=b-short"
  "$B MBPS=1 WAL_GAP=1000 $SA"
  "$B MBPS=1 WAL_GAP=100 $SC"
  "$B MBPS=1 WAL_GAP=1000 $SC"
  "$B MBPS=1 WAL_GAP=100 $SD"
  "$B MBPS=1 WAL_GAP=1000 $SD"
  "$B MBPS=1 WAL_GAP=100 $SB"
  "$B MBPS=1 WAL_GAP=1000 $SB"
  "$B MBPS=0.5 WAL_GAP=100 PAYLOAD=corpus RECORDS=1 RECORD_BYTES=1024"
  "$B WAL_GAP=100 $SF"
  "$B MBPS=1 WAL_GAP=100 $SD L6_LABEL=b-w128 L6_EXTRA=L6_SPILL_WAL_OBJECTS=128"
  "$B MBPS=1 WAL_GAP=100 $SD S3LITE_PUT_BPS=0"
  "ARM=base SHARDS=1 STREAMS=8 MBPS=1 WAL_GAP=100 $SA S3LITE_PUT_BPS=0 L6_LABEL=m5base"
  "$B $SHORT WAL_GAP=100 $SF L6_LABEL=b-pages-short L6_EXTRA=L6_SPILL_TRIGGER=pages"
)
FIRST=${1:-1}
echo "[m5-queue $(date '+%F %T')] ${#POINTS[@]} points, from $FIRST; HEAD $(git -C "$W" rev-parse --short HEAD)"
for i in "${!POINTS[@]}"; do
  n=$((i + 1))
  [ "$n" -ge "$FIRST" ] || continue
  if [ -e "$L6/STOP" ]; then
    echo "[m5-queue $(date '+%F %T')] STOP file present; stopping before point $n"
    exit 0
  fi
  echo "[m5-queue $(date '+%F %T')] point $n/${#POINTS[@]}: ${POINTS[$i]}"
  rc=0
  # shellcheck disable=SC2086
  $RUN ${POINTS[$i]} || rc=$?
  echo "[m5-queue $(date '+%F %T')] point $n done rc=$rc"
done
echo "[m5-queue $(date '+%F %T')] QUEUE DONE"
