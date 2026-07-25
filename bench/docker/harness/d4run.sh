#!/bin/bash
# Ladder D4: crash resumability.
# Phase A (deterministic window): streams get SCALE_FAULT_POINT=after_seal
#   -> every split attempt "crashes" between seal and map-save; the append
#   path must self-heal via resume_split with zero client errors.
# Phase B (real kill): docker kill a server mid-run; fencing + replay-to
#   must keep the driver at zero errors.
set -e
S=$(dirname "$0")
STREAM=${1:-d4s}
"$S/setup.sh" "$STREAM"
rm -f "/tmp/ladder-seqs-$STREAM.json"
echo "=== A: drive past hot with fault injection ON (compose override) ==="
BATCH=100 python3 -u "$S/driver.py" "$STREAM" "$S/key.txt" 4300 240 100 32
python3 "$S/showmap.py"
echo "=== checker (must PASS despite injected crash) ==="
python3 "$S/checker.py" "$STREAM" "$S/key.txt"
