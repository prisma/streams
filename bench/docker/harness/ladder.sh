#!/bin/bash
# Full ladder pass: D1 -> D2 -> D3 -> D4 -> D5 with fresh stream names.
# usage: ladder.sh <passtag>   (e.g. p2)
set -e
S=$(dirname "$0"); P=${1:?passtag}
CD=/Users/sorenschmidt/code/streams/bench/docker
LOG="$S/ladder-$P.log"
say() { echo "[$(date +%T)] $*" | tee -a "$LOG"; }

say "=== fresh world for pass $P (emulator recreated; a pass writes ~2 GB raw and the in-memory store must start empty) ==="
# STOP servers BEFORE wiping the world: live servers write manifests
# into the fresh empty bucket referencing wiped SSTs (p6: poisoned
# manifests, compaction 404 loops, wedged engines from minute one).
(cd "$CD" && docker compose stop streams-1 streams-2 streams-3) >>"$LOG" 2>&1
(cd "$CD" && docker compose up -d --force-recreate s3lite) >>"$LOG" 2>&1
sleep 3
(cd "$CD" && docker compose up -d --force-recreate streams-1 streams-2 streams-3) >>"$LOG" 2>&1
sleep 12
curl -s -X PUT "http://127.0.0.1:9500/ladder/ladder-fleet/fleet/desired.json" -d '{"count":3,"reason":"ladder","epoch":1,"computed_at_ms":0}' -o /dev/null
sleep 5

say "=== D1 ($P) ==="
"$S/setup.sh" "d1$P"
rm -f "/tmp/ladder-seqs-d1$P.json"
BATCH=100 python3 -u "$S/driver.py" "d1$P" "$S/key.txt" 4300 360 100 32 | tee -a "$LOG"
sleep 20
python3 "$S/checker.py" "d1$P" "$S/key.txt" --expect-segments 2 | tail -4 | tee -a "$LOG"

say "=== D2 ($P) ==="
"$S/setup.sh" "d2$P"
rm -f "/tmp/ladder-seqs-d2$P.json"
BATCH=200 python3 -u "$S/driver.py" "d2$P" "$S/key.txt" 14000 420 100 32 | tee -a "$LOG"
say "D2 merge watch (10 min)"
sleep 600
python3 "$S/showmap.py" | tee -a "$LOG"
python3 "$S/checker.py" "d2$P" "$S/key.txt" | tail -4 | tee -a "$LOG"

say "=== D3 ($P): absorb-pause on the OWNER of the test stream ==="
# Deterministic: create the stream, discover which instance actually owns
# its segment shard (probe all three; the one that ACKs is the owner —
# replay-to from a single port is unreliable while possession settles
# after a restart, which cost p5/p7 their D3 rungs), then pause THAT one.
D3STREAM="d3$P"
"$S/setup.sh" "$D3STREAM" >>"$LOG" 2>&1
K=$(cat "$S/key.txt")
sleep 20   # let ring + possession settle before resolving ownership
OWNER=""
for attempt in 1 2 3 4 5; do
  for n in 1 2 3; do
    port=$((8100+n))
    # READ probe: same ring resolution, writes nothing. An append probe
    # left one extra record in the stream and the order check counted it
    # as unexpected (p7b D3: 601,601 drained vs 601,600 sent).
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 \
      "http://127.0.0.1:$port/v1/stream/$D3STREAM?limit=1" \
      -H "Stream-Encryption-Key: $K")
    if [ "$code" = "200" ] || [ "$code" = "204" ]; then OWNER="streams-$n"; break; fi
  done
  [ -n "$OWNER" ] && break
  say "  ownership unresolved (attempt $attempt); settling..."
  sleep 10
done
[ -n "$OWNER" ] || { say "D3 FAIL: could not resolve owner of $D3STREAM"; exit 1; }
say "D3 stream: $D3STREAM owned by $OWNER — pausing its absorber (runtime, no restart)"
# Pause WITHOUT restarting: restarting the owner hands its shards to the
# peers, leaving the paused instance with no absorber to lag (p8 D3).
OWNPORT=$((8100 + ${OWNER##*-}))
curl -s -m 10 -X POST "http://127.0.0.1:$OWNPORT/v1/debug/absorb-pause?on=1" | tee -a "$LOG"; echo | tee -a "$LOG"
rm -f "/tmp/ladder-seqs-$D3STREAM.json"
BATCH=100 python3 -u "$S/driver.py" "$D3STREAM" "$S/key.txt" 2000 300 100 32 | tee -a "$LOG"
say "overrides at end:"; curl -s "http://127.0.0.1:9500/ladder/ladder-fleet/fleet/overrides.json" | tee -a "$LOG"; echo | tee -a "$LOG"
MOVES=$(for i in 1 2 3; do docker logs --since 12m "slate-ladder-streams-$i-1" 2>&1; done | grep -c "rebalancer: moving shard" || true)
say "D3 rebalancer moves observed: $MOVES"
if [ "$MOVES" -lt 1 ]; then say "D3 FAIL: no rebalance move fired (rung vacuous)"; exit 1; fi
python3 "$S/checker.py" "$D3STREAM" "$S/key.txt" | tail -3 | tee -a "$LOG"
curl -s -m 10 -X POST "http://127.0.0.1:$OWNPORT/v1/debug/absorb-pause?on=0" >>"$LOG" 2>&1
sleep 5

say "=== D4 ($P): fault-injected splits ==="
(cd "$CD" && docker compose -f compose.yml -f compose.d4.yml up -d) >>"$LOG" 2>&1
sleep 5
"$S/setup.sh" "d4$P"
rm -f "/tmp/ladder-seqs-d4$P.json"
BATCH=100 python3 -u "$S/driver.py" "d4$P" "$S/key.txt" 4300 300 100 32 | tee -a "$LOG"
python3 "$S/showmap.py" | tee -a "$LOG"
python3 "$S/checker.py" "d4$P" "$S/key.txt" --expect-segments 2 | tail -3 | tee -a "$LOG"
(cd "$CD" && docker compose up -d --force-recreate streams-1 streams-2 streams-3) >>"$LOG" 2>&1
sleep 8

say "=== D5 ($P): 30-min chaos soak ==="
bash "$S/d5run.sh" "d5$P" 1800 | tee -a "$LOG"
say "=== ladder pass $P complete ==="
