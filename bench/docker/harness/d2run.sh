#!/bin/bash
# Ladder D2: drive ~2.8x per-segment limit -> recursive splits to 4
# segments; then idle -> merges back. Requires fleet started with
# SCALE_COOLDOWN_SECS=120 SCALE_RATE_WINDOW_SECS=60. The splits and
# merges run on streams-1 outside fleet mode (a server in fleet mode
# splits no stream, edge change #119); the order check runs through the
# fleet formed over the same PATH_PREFIX afterwards (phases.sh).
set -e
S=$(dirname "$0")
STREAM=${1:-d2s}
. "$S/phases.sh"
ladder_solo
"$S/setup.sh" "$STREAM"
rm -f "/tmp/ladder-seqs-$STREAM.json"
echo "=== drive phase: 14k rec/s for 420s ==="
BATCH=200 python3 -u "$S/driver.py" "$STREAM" "$S/key.txt" 14000 420 100 32
echo "=== drive done; segmap now: ==="
python3 "$S/showmap.py" "$STREAM"
echo "=== idle merge phase: watching for 12 min ==="
for i in $(seq 1 24); do sleep 30; python3 "$S/showmap.py" "$STREAM" | head -3; done
echo "=== the fleet forms over the stream; order check through it ==="
ladder_fleet
python3 "$S/checker.py" "$STREAM" "$S/key.txt"
