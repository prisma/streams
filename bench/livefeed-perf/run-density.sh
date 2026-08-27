#!/bin/bash
# Campaign 1: the controlled A/B(/C) density sweep (round 12).
#
# Holds total subscribers at 1,000 and varies distribution; each shape
# runs the arm order A B B A C C (alternation kills host-drift bias;
# two C repeats anchor the B-vs-C regression gate). Runs are STRICTLY
# serial — the measured cgroup must own its quota.
#
#   RESULTS=<dir> [FAST=1] run-density.sh [shapes...]
#
# FAST=1 halves the idle/teardown observation windows (exploratory
# pass); the reportable campaign runs the full spec durations.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
RESULTS=${RESULTS:?set RESULTS dir}
mkdir -p "$RESULTS"
SHAPES=("$@")
[ ${#SHAPES[@]} -gt 0 ] || SHAPES=("1000x1" "500x2" "100x10" "10x100" "1x1000")
if [ "${FAST:-0}" = 1 ]; then
  export IDLE_SECS=300 TEARDOWN_SECS=300
else
  export IDLE_SECS=600 TEARDOWN_SECS=600
fi
export WARMUP_SECS=30 SPARSE_SECS=120 FANOUT_SECS=180 MIXED_SECS=180 SLOW_SECS=120
export FANOUT_DELIVERY_RATE=${FANOUT_DELIVERY_RATE:-1000} MIXED_BG_RATE=${MIXED_BG_RATE:-200}

for SHAPE in "${SHAPES[@]}"; do
  FEEDS=${SHAPE%x*}
  SUBS=${SHAPE#*x}
  i=0
  for ARM in a b b a c c; do
    i=$((i + 1))
    OUT="$RESULTS/$SHAPE-$ARM-$i"
    if [ -s "$OUT/manifest.json" ]; then
      echo "== skip $SHAPE $ARM #$i (manifest exists)"
      continue
    fi
    echo "== density $SHAPE arm $ARM run #$i -> $OUT"
    if ! bash "$HERE/run-one.sh" "$ARM" "$FEEDS" "$SUBS" "$OUT" "density-$SHAPE-$i"; then
      echo "RUN FAILED: $SHAPE $ARM #$i (continuing; analyze.py flags it)"
    fi
    sleep 20 # settle between runs
  done
done
echo "DENSITY_SWEEP_DONE"
