#!/bin/bash
# Ladder D3: absorb-lag rebalance. streams-2 gets ABSORB_PAUSE=1 (via
# compose override) so any shard it serves accumulates lag under load;
# expect: rebalancer moves that shard to a peer within ~REBALANCE_LAG_SECS
# + 2 fleet ticks, appends keep flowing (replay-to), no client errors.
set -e
S=$(dirname "$0")
STREAM=${1:-d3s}
"$S/setup.sh" "$STREAM"
rm -f "/tmp/ladder-seqs-$STREAM.json"
# Drive below hot threshold (no splits wanted): 2k rec/s, 240 s.
BATCH=100 python3 -u "$S/driver.py" "$STREAM" "$S/key.txt" 2000 240 100 32 &
DRV=$!
echo "watching overrides.json + heartbeat lag..."
for i in $(seq 1 40); do
  sleep 6
  L=$(curl -s "http://127.0.0.1:9500/ladder/d1/fleet/streams-2.json" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("absorb_lag_max_secs",0))' 2>/dev/null || echo "?")
  O=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:9500/ladder/d1/fleet/overrides.json")
  echo "t=$((i*6))s streams-2 lag=${L}s overrides_http=$O"
  if [ "$O" = "200" ]; then
    echo "OVERRIDES PUBLISHED:"; curl -s "http://127.0.0.1:9500/ladder/d1/fleet/overrides.json"; echo
  fi
done
wait $DRV
python3 "$S/checker.py" "$STREAM" "$S/key.txt"
