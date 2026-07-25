#!/bin/bash
# Ladder D5: 30-min soak, mixed load + chaos (random server restarts every
# 3-5 min), RSS <= envelope, zero data loss, no stuck segments.
set -e
S=$(dirname "$0")
STREAM=${1:-d5s}
DUR=${2:-1800}
"$S/setup.sh" "$STREAM"
rm -f "/tmp/ladder-seqs-$STREAM.json"
BATCH=100 python3 -u "$S/driver.py" "$STREAM" "$S/key.txt" 3000 "$DUR" 100 32 &
DRV=$!
END=$(( $(date +%s) + DUR ))
i=0
while [ "$(date +%s)" -lt "$END" ]; do
  sleep $(( 180 + RANDOM % 120 ))
  V=$(( RANDOM % 3 + 1 ))
  echo "$(date +%T) chaos: restarting streams-$V"
  docker restart "slate-ladder-streams-$V-1" >/dev/null
  i=$((i+1))
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep streams || true
done
wait $DRV
echo "=== chaos events: $i ==="
python3 "$S/showmap.py"
python3 "$S/checker.py" "$STREAM" "$S/key.txt"
