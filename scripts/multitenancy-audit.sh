#!/bin/bash
# Machine-enforced multitenancy conversion audit (review round, MT map).
#
# The 274-site conversion map (docs/MULTITENANCY-MAP.md) decays as line
# numbers move; this audit does not. It fingerprints every code site in
# the categories the layout-4 conversion must drain, and compares them
# against a reviewed baseline:
#
#   NEW fingerprints  -> FAIL (someone added a bare-name identity site)
#   GONE fingerprints -> progress (reported, and the baseline should be
#                        regenerated in the same commit that converts
#                        the sites: scripts/multitenancy-audit.sh --regen)
#
# Categories:
#   stream-hash        crypto::stream_hash callers outside crypto.rs —
#                      every name-derived identity roots here; Stage 3
#                      shrinks this to the allowlisted exceptions
#                      (history.rs tick-stagger, routing-key hashing)
#   registry-bare-name what could REINTRODUCE bare-name registry
#                      identity now that Stage 3 deleted the &str
#                      overloads (the type system carries converted
#                      call sites): registry calls passing a string
#                      literal, and registry methods declared over
#                      &str names
#   global-name-maps   process-global maps keyed by bare stream name
#                      (scaler sketches/cooldowns/hot keys)
#   tenant-fallback    deployment-global tenant identity (env
#                      ACCOUNT_ID/PROJECT_ID, acct_local/proj_local
#                      defaults); gone by Stage 7
#   internal-target    internal RPC identity headers; Stage 4 adds the
#                      project dimension (streams-internal-project)
#
# Fingerprint = category \t file \t normalized-source-text. Line
# numbers are deliberately absent. A fingerprint changes when its line
# is edited — regenerating the baseline is part of the reviewed diff.
set -euo pipefail
cd "$(dirname "$0")/.."
BASELINE=scripts/mt-audit-baseline.txt

scan() {
  local cat="$1" pat="$2"
  shift 2
  grep -nH -E "$pat" "$@" 2>/dev/null |
    sed -E 's/^([^:]+):[0-9]+:[[:space:]]*/\1\t/' |
    sed -E 's/[[:space:]]+/ /g' |
    awk -v c="$cat" -F'\t' '{print c "\t" $1 "\t" $2}' || true
}

collect() {
  {
    scan stream-hash 'stream_hash\(' src/*.rs src/config/*.rs src/dst/*.rs src/bin/*.rs |
      grep -v $'\tsrc/crypto.rs\t' || true
    scan registry-bare-name \
      'registry[[:space:]]*\.[[:space:]]*(get|recreate|update|cas_update[a-z_]*|mutate_incarnation|invalidate|list_page)\("' \
      src/*.rs src/config/*.rs src/dst/*.rs
    scan registry-bare-name \
      'fn (get|recreate|update|cas_update[a-z_]*|mutate_incarnation|invalidate|list_page)[^(]*\([^)]*name[^)]*&str' \
      src/registry.rs
    scan global-name-maps 'HashMap<String' src/scaler3.rs src/registry.rs
    scan tenant-fallback '(acct_local|proj_local|"ACCOUNT_ID"|"PROJECT_ID")' src/*.rs src/config/*.rs
    scan internal-target 'streams-internal-(epoch|seg|identity|project)' src/*.rs src/config/*.rs
  } | LC_ALL=C sort -u
}

CURRENT=$(mktemp)
collect > "$CURRENT"

if [ "${1:-}" = "--regen" ]; then
  cp "$CURRENT" "$BASELINE"
  echo "multitenancy-audit: baseline regenerated ($(wc -l < "$BASELINE" | tr -d ' ') fingerprints)"
  exit 0
fi

[ -f "$BASELINE" ] || { echo "multitenancy-audit: missing $BASELINE (run --regen once)"; exit 1; }

NEW=$(comm -13 "$BASELINE" "$CURRENT")
GONE=$(comm -23 "$BASELINE" "$CURRENT")

echo "multitenancy-audit: per-category remaining:"
awk -F'\t' '{n[$1]++} END {for (c in n) printf "  %-20s %d\n", c, n[c]}' "$CURRENT" | LC_ALL=C sort

if [ -n "$GONE" ]; then
  echo "multitenancy-audit: $(echo "$GONE" | wc -l | tr -d ' ') fingerprint(s) converted/moved since baseline:"
  echo "$GONE" | sed 's/^/  - /' | head -20
  echo "  (regenerate the baseline in the converting commit: scripts/multitenancy-audit.sh --regen)"
fi

if [ -n "$NEW" ]; then
  echo "multitenancy-audit: FAIL — new bare-name identity site(s):"
  echo "$NEW" | sed 's/^/  + /'
  echo "Convert the site to the tenant-qualified types (src/tenant.rs +"
  echo "the RouteHash/SegmentHash layout-4 constructors), or — only for"
  echo "a reviewed identity-neutral exception — regenerate the baseline"
  echo "in this same commit and justify it in the commit message."
  exit 1
fi
echo "MT_AUDIT_OK"
