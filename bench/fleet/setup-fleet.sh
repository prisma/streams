#!/bin/bash
# #113 fleet campaign, step 1: infrastructure. Creates the REGION-SET
# project (buckets inherit the project's create-time region —
# BUCKETS-SINGLE-REGION.md), its single-region data bucket + key, the
# bun-installed app dirs, and uploads the pilot (lb/gen) binary.
#
#   SOAK_HOME=~/.streams-soak bench/fleet/setup-fleet.sh
set -euo pipefail
S=${SOAK_HOME:?set SOAK_HOME}
TOKEN=$(cat "$S/platform-token.txt")
W=wksp_cmrj21kxd3scrwfdvkx9wgi54
REPO=$(cd "$(dirname "$0")/../.." && pwd)

if [ ! -s "$S/proj-fleet.txt" ]; then
  P=$(curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -H "User-Agent: curl/8.7.1" \
    -d '{"name":"streams-fleet-fra","workspaceId":"'"$W"'","region":"eu-central-1"}' \
    https://api.prisma.io/v1/projects | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['id'])")
  echo "$P" > "$S/proj-fleet.txt"
  echo "project: $P (eu-central-1)"
fi
P=$(cat "$S/proj-fleet.txt")
# Confirm the region actually took — a region-less project would pin the
# bucket US-shaped and quietly quadruple every store op from fra.
R=$(curl -s -H "Authorization: Bearer $TOKEN" -H "User-Agent: curl/8.7.1" \
  "https://api.prisma.io/v1/projects/$P" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['defaultRegion'])")
[ "$R" = "eu-central-1" ] || { echo "FATAL: project region is $R, not eu-central-1"; exit 1; }

if [ ! -s "$S/bkey-fleet.json" ]; then
  BID=$(curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -H "User-Agent: curl/8.7.1" -d '{"projectId":"'"$P"'","name":"streams-fleet-data"}' \
    https://api.prisma.io/v1/buckets | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['id'])")
  echo "$BID" > "$S/bucket-fleet.id"
  curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -H "User-Agent: curl/8.7.1" -d '{"role":"read_write","name":"fleet"}' \
    "https://api.prisma.io/v1/buckets/$BID/keys" > "$S/bkey-fleet.json"
  echo "bucket: $BID"
fi

for app in app-server app-lb; do
  d="$S/fleet-$app"
  if [ ! -d "$d/node_modules" ]; then
    rm -rf "$d"; cp -R "$REPO/deploy/$app" "$d"
    (cd "$d" && bun install --silent)
    echo "prepared $d"
  fi
done
echo "setup complete: project=$(cat "$S/proj-fleet.txt")"
