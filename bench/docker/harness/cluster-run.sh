#!/bin/bash
# Compute-cluster validation (SCALING.md 8, final rung): repeat D1/D3/D5
# against the 4-instance Prisma Compute fleet through its front door.
# Requires: cluster-deploy.sh already run; CLUSTER_URL/CLUSTER_AUTH set.
set -e
S=$(dirname "$0")
LOG="$S/cluster-run.log"
say() { echo "[$(date +%T)] $*" | tee -a "$LOG"; }
CLUSTER_URLS=${CLUSTER_URLS:-$(cat "$S/cluster-urls.json" 2>/dev/null)}
[ -n "$CLUSTER_URLS" ] || { echo "no cluster-urls.json — run cluster-deploy.sh first"; exit 1; }
: "${CLUSTER_AUTH:?set CLUSTER_AUTH}"
export CLUSTER_URLS CLUSTER_AUTH
# Any instance answers control-plane calls; ring redirects handle the rest.
CLUSTER_URL=$(python3 -c "import json,os;print(sorted(json.loads(os.environ['CLUSTER_URLS']).values())[0])")
K=$(cat "$S/key.txt")
TAG=${TAG:-r2}

create() {  # create a scaled stream through the front door
  curl -s -o /dev/null -w "create $1: %{http_code}\n" -X PUT "$CLUSTER_URL/v1/stream/$1" \
    -H "Authorization: Bearer $CLUSTER_AUTH" -H "Stream-Encryption-Key: $K" \
    -H "Stream-Scaling: auto" -H "Content-Type: application/json" | tee -a "$LOG"
}

# Ring-stability gate: Compute cold-starts instances one at a time, so
# the live set (and therefore shard ownership) churns for minutes after
# deploy. Driving load through that churn moves shards repeatedly under
# writes. Wait until all 4 are live and the ring is unchanged for 60 s.
say "waiting for a stable 4-instance ring..."
STABLE=0; LAST=""
for i in $(seq 1 60); do
  RING=$(curl -s -m 25 "$CLUSTER_URL/operator/data.json" -H "Authorization: Bearer $CLUSTER_AUTH" 2>/dev/null \
    | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: print(''); raise SystemExit
out=[]
def w(o):
    if isinstance(o,dict):
        for k,v in o.items():
            if k=='ring_active': out.append(v)
            w(v)
w(d)
print(','.join(sorted(out[0])) if out and out[0] else '')" 2>/dev/null)
  N=$(echo "$RING" | awk -F, '{print NF}')
  if [ "$RING" = "$LAST" ] && [ "$N" = "4" ]; then STABLE=$((STABLE+1)); else STABLE=0; fi
  LAST="$RING"
  say "  ring=[$RING] stable_ticks=$STABLE"
  [ "$STABLE" -ge 6 ] && break
  sleep 10
done
[ "$STABLE" -ge 6 ] || { say "ring never stabilized at 4 instances; aborting"; exit 1; }
say "ring stable — starting load"

say "=== C1: split under load (4-instance cluster) ==="
create ${TAG}c1
rm -f /tmp/ladder-seqs-${TAG}c1.json
BATCH=100 python3 -u "$S/driver.py" "${TAG}c1" "$S/key.txt" 4300 360 100 32 | tee -a "$LOG"
sleep 20
python3 "$S/checker.py" "${TAG}c1" "$S/key.txt" | tail -3 | tee -a "$LOG"

say "=== C3: rebalance under absorb lag ==="
say "  (cloud instances: lag arises from real backpressure, no ABSORB_PAUSE)"
create ${TAG}c3
rm -f /tmp/ladder-seqs-${TAG}c3.json
BATCH=100 python3 -u "$S/driver.py" "${TAG}c3" "$S/key.txt" 6000 420 100 48 | tee -a "$LOG"
say "  overrides:"; curl -s "$CLUSTER_URL/operator/data.json" -H "Authorization: Bearer $CLUSTER_AUTH" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(json.dumps(d,indent=1)[:800])" | tee -a "$LOG"
python3 "$S/checker.py" "${TAG}c3" "$S/key.txt" | tail -3 | tee -a "$LOG"

say "=== C5: 30-min soak ==="
create ${TAG}c5
rm -f /tmp/ladder-seqs-${TAG}c5.json
BATCH=100 python3 -u "$S/driver.py" "${TAG}c5" "$S/key.txt" 3000 1800 100 32 | tee -a "$LOG"
python3 "$S/checker.py" "${TAG}c5" "$S/key.txt" | tail -3 | tee -a "$LOG"
say "=== cluster validation complete ==="
