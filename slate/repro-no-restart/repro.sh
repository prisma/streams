#!/bin/bash
# Reproduces: app process death => service domain unbound, version still
# "running", no restart, until a fresh deploy.
# Usage: PRISMA_API_TOKEN=... ./repro.sh <project-id>
set -euo pipefail
P=${1:?project id}
echo "== 1. deploy"
OUT=$(bunx @prisma/compute-cli deploy --project "$P" --service-name repro-no-restart \
  --region ap-southeast-1 --path . --http-port 8080 --json 2>/dev/null)
URL=$(echo "$OUT" | grep -oE '"appEndpointDomain"\s*:\s*"[^"]+"' | cut -d'"' -f4)
SVC=$(echo "$OUT" | grep -oE '"appId"\s*:\s*"[^"]+"' | head -1 | cut -d'"' -f4)
echo "service=$SVC url=$URL"
sleep 5

echo "== 2. verify healthy"
curl -sS -m 15 "$URL/health"; echo

echo "== 3. trigger a hard process exit"
curl -sS -m 15 "$URL/crash"; echo
sleep 10

echo "== 4. observed behavior"
echo "-- version status (expect: still 'running'):"
bunx @prisma/compute-cli versions list --project "$P" --service "$SVC" 2>&1 | tail -3 || true
echo "-- service responses for 60s (expect: HTML 404 'There is no service on this URL', no recovery):"
for i in 1 2 3 4 5 6; do
  CODE=$(curl -s -o /tmp/repro-body.txt -w '%{http_code}' -m 10 "$URL/health" || true)
  echo "t+$((i*10))s  HTTP $CODE  $(head -c 60 /tmp/repro-body.txt | tr -d '\n')"
  sleep 10
done

echo "== 5. recovery requires a fresh deploy"
bunx @prisma/compute-cli deploy --project "$P" --service "$SVC" \
  --region ap-southeast-1 --path . --http-port 8080 >/dev/null 2>&1
sleep 8
curl -sS -m 15 "$URL/health"; echo
echo "== done"
