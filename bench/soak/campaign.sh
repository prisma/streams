#!/bin/bash
# The campaign lifecycle owner (R25-G). One entry point that owns the
# run id, builds+verifies both binaries, provisions region-pinned cells
# with creation receipts, deploys sequentially, confirms load, samples
# for the soak window, harvests, RECONCILES (always — absorber lag is
# not a durability boundary), reports, and tears down on success.
# Failed runs preserve the namespace and leave a forensic manifest.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
S=${SOAK_HOME:?set SOAK_HOME to a scratch dir outside the repo}
export SOAK_RUN_ID=${SOAK_RUN_ID:-"soak-$(date -u +%Y%m%dT%H%M%SZ)-$$"}
export SOAK_PREFIX=${SOAK_PREFIX:-$SOAK_RUN_ID}
export BIN_TAG=${BIN_TAG:-$SOAK_RUN_ID}
REGIONS=${SOAK_REGIONS:-"us-east-1 us-west-1 eu-central-1 eu-west-3 ap-southeast-1 ap-northeast-1"}
SOAK_MINUTES=${SOAK_MINUTES:-35}
mkdir -p "$S/results/$SOAK_RUN_ID"
echo "$SOAK_RUN_ID" > "$S/current-run-id.txt"
echo "== campaign $SOAK_RUN_ID  regions: $REGIONS"

fail() {
  echo "CAMPAIGN FAILED at stage: $1" | tee "$S/results/$SOAK_RUN_ID/FAILED"
  echo "namespace preserved for forensics; tear down manually with:" \
       "SOAK_RUN_ID=$SOAK_RUN_ID ./teardown.sh --yes" \
       | tee -a "$S/results/$SOAK_RUN_ID/FAILED"
  exit 1
}

"$HERE/build-upload.sh"                            || fail build-upload
python3 "$HERE/provision.py" --run-id "$SOAK_RUN_ID" $REGIONS || fail provision
for r in $REGIONS; do "$HERE/deploy-region.sh" "$r" server || fail "server-$r"; done
for r in $REGIONS; do "$HERE/deploy-region.sh" "$r" gen    || fail "gen-$r"; done
SOAK_REGIONS="$REGIONS" python3 "$HERE/verify-running.py" $REGIONS || fail verify

echo "== sampling for $SOAK_MINUTES minutes"
END=$(( $(date +%s) + SOAK_MINUTES * 60 ))
while [ "$(date +%s)" -lt "$END" ]; do
  SOAK_REGIONS="$REGIONS" python3 "$HERE/poll.py" \
    >> "$S/results/$SOAK_RUN_ID/poll.log" 2>&1 || true
  sleep 45
done

SOAK_REGIONS="$REGIONS" python3 "$HERE/harvest.py"    || fail harvest
SOAK_REGIONS="$REGIONS" python3 "$HERE/reconcile.py" $REGIONS || fail reconcile
SOAK_REGIONS="$REGIONS" python3 "$HERE/mkreport.py" \
  > "$S/results/$SOAK_RUN_ID/report.md"               || fail report

if [ "${PRESERVE_SOAK:-0}" != 1 ]; then
  SOAK_REGIONS="$REGIONS" "$HERE/teardown.sh" --yes   || fail teardown
fi
echo "== campaign $SOAK_RUN_ID complete: results/$SOAK_RUN_ID/report.md"
