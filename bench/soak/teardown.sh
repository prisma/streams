#!/bin/bash
# Delete everything a soak run created: services, buckets, projects.
#
#   ./teardown.sh          # dry run: list what would be deleted
#   ./teardown.sh --yes    # actually delete
#
# A six-region run leaves 6 projects, 6 buckets and 12 services standing.
# None of it expires on its own, so teardown is part of the run, not an
# afterthought (bench/soak/README.md invariant 7).
set -euo pipefail
S=${SOAK_HOME:?set SOAK_HOME}
GO=${1:-}
REGIONS=${SOAK_REGIONS:-us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1}
TOKEN=$(cat "$S/platform-token.txt")
export PRISMA_API_TOKEN=$TOKEN

say() { echo "$@"; }
# The dry run echoes commands, and those commands carry the platform token.
# Redact it: a pasted dry-run transcript must never leak a credential
# (RUNBOOK section 12).
run() {
  if [ "$GO" = "--yes" ]; then
    eval "$@"
  else
    say "  would run: ${*//$TOKEN/<PRISMA_API_TOKEN>}"
  fi
}

for r in $REGIONS; do
  say "== $r"
  P=$(cat "$S/proj-$r.txt" 2>/dev/null || true)
  [ -z "$P" ] && { say "  no project file, skipping"; continue; }

  for role in gen server; do
    SV=$(cat "$S/svc-$role-$r.txt" 2>/dev/null || true)
    [ -z "$SV" ] && continue
    say "  service $role: $SV"
    run "bunx --bun @prisma/compute-cli services delete --project '$P' --service '$SV' --yes >/dev/null 2>&1 || true"
  done

  # bkey-<r>.json holds the *key* id, not the bucket id -- resolve the
  # bucket by listing the project's buckets.
  BKT=$(curl -s -H "Authorization: Bearer $TOKEN" \
        "https://api.prisma.io/v1/buckets?projectId=$P" \
        | python3 -c "import json,sys;print(' '.join(b['id'] for b in json.load(sys.stdin).get('data',[])))" 2>/dev/null || true)
  for b in $BKT; do
    say "  bucket: $b"
    run "curl -s -X DELETE -H 'Authorization: Bearer $TOKEN' 'https://api.prisma.io/v1/buckets/$b' >/dev/null"
  done
  [ -z "$BKT" ] && say "  bucket: none found for this project"

  say "  project: $P"
  run "curl -s -X DELETE -H 'Authorization: Bearer $TOKEN' 'https://api.prisma.io/v1/projects/$P' >/dev/null"
done

[ "$GO" = "--yes" ] || say "
DRY RUN. Re-run with --yes to delete."
