#!/usr/bin/env bash
# Reproduce (or re-test) "new services are never published to the edge".
#
#   PRISMA_API_TOKEN=… ./repro.sh [region]
#
# Deploys the 15-line hello app to a fresh project and polls its URLs.
# Exits 0 if the service becomes reachable (i.e. the platform is healthy
# again), 1 if it stays 404 — which is the bug in README.md.
set -euo pipefail
REGION="${1:-us-east-1}"
CLI="bunx --bun @prisma/compute-cli@latest"
: "${PRISMA_API_TOKEN:?export PRISMA_API_TOKEN (raw management-API token)}"
cd "$(dirname "$0")"

NAME="edge-probe-$RANDOM"
echo "== creating project $NAME"
PROJ=$($CLI projects create --name "$NAME" --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])')
echo "   $PROJ"

cleanup() {
  echo "== cleanup"
  SVC=$($CLI services list --project "$PROJ" --json 2>/dev/null \
        | python3 -c 'import json,sys;d=json.load(sys.stdin)["data"];print(d[0]["id"] if d else "")' || true)
  # NOTE: the id is POSITIONAL. `--service cps_…` fails and leaves it running.
  [ -n "$SVC" ] && $CLI services destroy "$SVC" --project "$PROJ" >/dev/null 2>&1 || true
  curl -sf -o /dev/null -X DELETE -H "Authorization: Bearer $PRISMA_API_TOKEN" \
    "https://api.prisma.io/v1/projects/$PROJ" || true
  echo "   destroyed service + project"
}
trap cleanup EXIT

echo "== deploying hello app to $REGION"
# --http-port MUST be explicit: flag-less deploys are mapped to port
# 3000 server-side (2026-08-03 finding), which renders as the same
# no-service 404 this script exists to detect. Keep the probe honest.
OUT=$($CLI deploy --project "$PROJ" --service-name hello --region "$REGION" --http-port 8080 2>&1)
echo "$OUT" | tail -6
URL=$(echo "$OUT" | grep -o 'https://[a-z0-9.-]*prisma.build' | tail -1)
CPV=$(echo "$OUT" | grep -o 'cpv_[a-z0-9]*' | head -1)
echo "== polling $URL (version $CPV)"

for i in $(seq 1 15); do
  CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$URL/health" || echo 000)
  echo "   attempt $i: $CODE"
  if [ "$CODE" = "200" ]; then
    echo "REACHABLE — the platform is publishing new services again."
    exit 0
  fi
  sleep 8
done

cat <<EOF

UNREACHABLE after ~2 minutes — the bug in README.md is still present.
The app itself is fine; confirm with the boot log:

  wss://api.prisma.io/v1/deployments/$CPV/logs

(the CLI's own \`logs\` command cannot fetch this — it sends the project
id where the API wants the cpv id). You should see "hello app listening
on 0.0.0.0:8080" with no request activity while the URL 404s.
EOF
exit 1
