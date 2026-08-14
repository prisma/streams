#!/bin/bash
# Drive capacity-gate.sh to a REAL verdict.
#
# Infra-stage failures (provision/server/gen/verify/release) are
# platform/CLI transients: tear the half-built namespace down and try
# again with a fresh run id. Verdict-stage outcomes (evaluate, harvest,
# reconcile, recovery — or a clean PASS) are results: stop and report.
# A death with NO stage marker means the driver itself died — that is
# a harness bug and deserves eyes, so it also stops.
#
# The 20260814 certification burned four operator round-trips on
# transient provision/deploy failures before this wrapper existed.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
S=${SOAK_HOME:?set SOAK_HOME}
MAX=${CAP_RETRY_MAX:-6}
# The 20260814 network came in waves: minutes of dead connects, then
# minutes of clean 200s. Launching a try into a bad wave wastes the
# 4-minute build+provision spend, so gate each try on the platform API
# answering twice, 20s apart (one success can be the edge of a wave).
net_stable() {
  local ok=0 t=0
  while [ "$t" -lt 20 ]; do
    if curl -s -o /dev/null --max-time 8 https://api.prisma.io; then
      ok=$((ok+1))
      [ "$ok" -ge 2 ] && return 0
      sleep 20
    else
      ok=0
      echo "CAPACITY RETRY: platform API unreachable; waiting 30s"
      sleep 30
    fi
    t=$((t+1))
  done
  echo "CAPACITY RETRY: network never stabilized"
  return 1
}
for TRY in $(seq 1 "$MAX"); do
  net_stable || exit 1
  # Each try mints its own namespace: a retry must never adopt the
  # failed attempt's run id (R26-9).
  unset SOAK_RUN_ID SOAK_PREFIX BIN_TAG
  "$HERE/capacity-gate.sh"
  EC=$?
  RUN=$(cat "$S/current-run-id.txt" 2>/dev/null || echo unknown)
  if [ "$EC" -eq 0 ]; then
    echo "CAPACITY RETRY: PASS on try $TRY ($RUN)"
    exit 0
  fi
  STAGE=$(sed -n 's/.*FAILED at stage: //p' "$S/results/$RUN/FAILED" 2>/dev/null | head -1)
  echo "CAPACITY RETRY: try $TRY/$MAX failed at stage '${STAGE:-none}' ($RUN)"
  case "$STAGE" in
    provision|server|gen|verify|release)
      SOAK_RUN_ID=$RUN "$HERE/teardown.sh" --yes || true
      # Provision refuses while any receipt from the failed try
      # survives, and teardown deliberately KEEPS a receipt when it
      # cannot confirm the project delete (2xx/404). Burning fresh
      # tries against a known-blocked provision is how the 20260814
      # wrapper run exhausted itself in 3 minutes — re-run teardown
      # until this run's receipts actually clear, then proceed.
      for TDT in 1 2 3 4 5; do
        BLOCKED=$(grep -l "\"runId\": \"$RUN\"" "$S"/receipts/*.json 2>/dev/null | wc -l | tr -d ' ')
        [ "$BLOCKED" = 0 ] && break
        echo "CAPACITY RETRY: $BLOCKED receipt(s) of $RUN still present; teardown again ($TDT/5)"
        sleep 30
        SOAK_RUN_ID=$RUN "$HERE/teardown.sh" --yes || true
      done
      sleep 20
      ;;
    *)
      echo "CAPACITY RETRY: terminal outcome at stage '${STAGE:-none}' — stopping for analysis"
      exit "$EC"
      ;;
  esac
done
echo "CAPACITY RETRY: exhausted $MAX tries on infra stages"
exit 1
