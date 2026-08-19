#!/bin/bash
# SR2-5: release-candidate promotion — the certification boundary.
#
# `slate` stays an unprotected integration branch; a RELEASE CANDIDATE
# is promoted only from a commit whose full check list is green, as an
# annotated rc tag (immutable ref). Usage:
#
#   scripts/promote-rc.sh <commit-ish> <rc-tag>     # e.g. v0.2.0-rc.2
#
# The gate verifies, at <commit-ish>:
#   1. the GitHub CI run is fully green (rust, mt-cert-1000,
#      durable-streams-server-conformance, product-field-gate,
#      sdk-package — and the nightly noisy job when present);
#   2. the LOCAL release gate passes on a fresh checkout of that
#      commit (fmt, clippy fingerprints, full suite, bare mt audit);
#   3. the noisy-neighbor campaign passes at contract scale.
# Legs the cell cannot run alone (fleet owner-movement campaign,
# billing reconciliation against a live month, external security
# review) are listed in the tag message as EXPLICIT outstanding items
# — the tag records what was and was not certified.
set -euo pipefail
cd "$(dirname "$0")/.."
COMMIT=${1:?usage: promote-rc.sh <commit-ish> <rc-tag>}
TAG=${2:?usage: promote-rc.sh <commit-ish> <rc-tag>}
SHA=$(git rev-parse --verify "$COMMIT^{commit}")

echo "== promote-rc: $SHA -> $TAG =="
echo "== 1/3 GitHub checks at $SHA =="
RUN=$(gh run list --commit "$SHA" --limit 1 --json databaseId,conclusion \
      --jq '.[0]')
[ -n "$RUN" ] || { echo "FAIL: no CI run for $SHA"; exit 1; }
CONCL=$(echo "$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['conclusion'])")
[ "$CONCL" = "success" ] || { echo "FAIL: CI run conclusion=$CONCL"; exit 1; }
echo "ok   CI fully green"

echo "== 2/3 local release gate at $SHA =="
git diff --quiet || { echo "FAIL: dirty tree"; exit 1; }
CUR=$(git rev-parse HEAD)
[ "$CUR" = "$SHA" ] || { echo "FAIL: check out $SHA first (HEAD=$CUR)"; exit 1; }
bash scripts/release-gate.sh
bash scripts/multitenancy-audit.sh

echo "== 3/3 noisy-neighbor campaign (contract scale) =="
node scripts/mt-noisy-campaign.mjs

git tag -a "$TAG" "$SHA" -m "release candidate $TAG

Certified at $SHA:
- CI fully green (rust incl. mt audit + identity lint, mt-cert-1000,
  DS conformance, product field gate, sdk package matrix)
- local release gate + bare multitenancy audit
- noisy-neighbor campaign at contract scale (locked thresholds)

NOT certified by this tag (schedule before GA):
- fleet owner-movement campaign at scale
- billing reconciliation against a live month
- external security review
- FLEET_AUTH_MODE=workload deployment posture (binary refuses static
  under STREAMS_RELEASE_POSTURE=1; the platform must mint the JWTs)"
echo "PROMOTE_RC_OK: created annotated tag $TAG at $SHA (push with: git push origin $TAG)"
