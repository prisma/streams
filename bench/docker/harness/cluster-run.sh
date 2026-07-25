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

create() {  # create a scaled stream through the front door
  curl -s -o /dev/null -w "create $1: %{http_code}\n" -X PUT "$CLUSTER_URL/v1/stream/$1" \
    -H "Authorization: Bearer $CLUSTER_AUTH" -H "Stream-Encryption-Key: $K" \
    -H "Stream-Scaling: auto" -H "Content-Type: application/json" | tee -a "$LOG"
}

say "=== C1: split under load (4-instance cluster) ==="
create c1
rm -f /tmp/ladder-seqs-c1.json
BATCH=100 python3 -u "$S/driver.py" c1 "$S/key.txt" 4300 360 100 32 | tee -a "$LOG"
sleep 20
python3 "$S/checker.py" c1 "$S/key.txt" | tail -3 | tee -a "$LOG"

say "=== C3: rebalance under absorb lag ==="
say "  (cloud instances: lag arises from real backpressure, no ABSORB_PAUSE)"
create c3
rm -f /tmp/ladder-seqs-c3.json
BATCH=100 python3 -u "$S/driver.py" c3 "$S/key.txt" 6000 420 100 48 | tee -a "$LOG"
say "  overrides:"; curl -s "$CLUSTER_URL/operator/data.json" -H "Authorization: Bearer $CLUSTER_AUTH" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(json.dumps(d,indent=1)[:800])" | tee -a "$LOG"
python3 "$S/checker.py" c3 "$S/key.txt" | tail -3 | tee -a "$LOG"

say "=== C5: 30-min soak ==="
create c5
rm -f /tmp/ladder-seqs-c5.json
BATCH=100 python3 -u "$S/driver.py" c5 "$S/key.txt" 3000 1800 100 32 | tee -a "$LOG"
python3 "$S/checker.py" c5 "$S/key.txt" | tail -3 | tee -a "$LOG"
say "=== cluster validation complete ==="
