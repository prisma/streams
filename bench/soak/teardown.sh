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

RUN_ID=${SOAK_RUN_ID:?set SOAK_RUN_ID to the campaign id being torn down}
for r in $REGIONS; do
  say "== $r"
  P=$(cat "$S/proj-$r.txt" 2>/dev/null || true)
  [ -z "$P" ] && { say "  no project file, skipping"; continue; }
  # Refuse projects this campaign did not deploy (stamp mismatch), and
  # anything explicitly preserved. Inherited project files from earlier
  # campaigns destroyed the SIN 404 specimen (2026-07-27); never again.
  STAMP=$(cat "$S/proj-$r.txt.campaign" 2>/dev/null || true)
  if [ "$STAMP" != "$RUN_ID" ]; then
    say "  REFUSING: project $P stamped '$STAMP' != campaign '$RUN_ID'"
    continue
  fi
  if grep -q "$P" "$S/preserve.txt" 2>/dev/null; then
    say "  REFUSING: project $P is in preserve.txt"
    continue
  fi

  # R25-G: resolve services from `services list` — the authoritative
  # source — never from a cache file. The flat cache this used to read
  # could name a DIFFERENT campaign's service (the cross-project id bug)
  # or, once cleaned, silently skip destruction and leak the services.
  # These are campaign-created projects gated by the run-id stamp above,
  # so everything listed in them belongs to this campaign.
  SVCS=$(bunx --bun @prisma/compute-cli@0.39.0 services list --project "$P" 2>/dev/null \
         | awk '/^cps_/ {print $1}')
  for SV in $SVCS; do
    say "  service: $SV"
    # `services delete` refuses while versions are running, and the project
    # then refuses to delete because "active deployments exist". `destroy`
    # stops and deletes the versions first. Do not swallow its output: the
    # first version of this script hid the failure behind >/dev/null and
    # reported a clean teardown that had deleted nothing.
    # `|| say WARN`: one flaky destroy must not abort the whole loop
    # under set -e — the 20260814 teardown died on a transient failure
    # BEFORE reaching ap-southeast-1, left that region's receipt alive,
    # and the next campaign's provision refused to start. A missed
    # delete is an orphan to re-run teardown for; a dead loop is a
    # wedged campaign pipeline.
    run "bunx --bun @prisma/compute-cli@0.39.0 services destroy '$SV' --project '$P' 2>&1 | grep -viE 'resolving|resolved|saved lockfile'" \
      || say "  WARN: service destroy failed for $SV (re-run teardown to retry)"
  done

  # bkey-<r>.json holds the *key* id, not the bucket id -- resolve the
  # bucket by listing the project's buckets.
  BKT=$(curl -s -H "Authorization: Bearer $TOKEN" \
        "https://api.prisma.io/v1/buckets?projectId=$P" \
        | python3 -c "import json,sys;print(' '.join(b['id'] for b in json.load(sys.stdin).get('data',[])))" 2>/dev/null || true)
  for b in $BKT; do
    say "  bucket: $b"
    run "curl -s -o /dev/null -w '    bucket delete: %{http_code}\\n' --retry 3 --retry-all-errors --retry-delay 4 -X DELETE -H 'Authorization: Bearer $TOKEN' 'https://api.prisma.io/v1/buckets/$b'" \
      || say "  WARN: bucket delete failed for $b (re-run teardown to retry)"
  done
  [ -z "$BKT" ] && say "  bucket: none found for this project"

  say "  project: $P"
  # The receipt is retired ONLY when the project delete confirms 2xx or
  # 404 (already gone): rm-ing it on a failed delete would let the next
  # provision create a NEW project while this one still exists — the
  # silent-orphan variant of the ghost-reuse problem the receipt guards
  # against. On failure the receipt stays and provision keeps refusing,
  # which is loud and correct: re-run teardown.
  if [ "$GO" = "--yes" ]; then
    PDC=$(curl -s -o /dev/null -w '%{http_code}' --retry 3 --retry-all-errors --retry-delay 4 \
          -X DELETE -H "Authorization: Bearer $TOKEN" \
          "https://api.prisma.io/v1/projects/$P") || PDC=000
    say "    project delete: $PDC"
    case "$PDC" in
      2*|404) rm -f "$S/receipts/$r.json";;
      *) say "  WARN: project delete returned $PDC; receipt kept (re-run teardown)";;
    esac
  else
    say "  would run: project delete + receipt retire for $P"
  fi
done

[ "$GO" = "--yes" ] || say "
DRY RUN. Re-run with --yes to delete."
