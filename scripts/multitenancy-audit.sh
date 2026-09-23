#!/bin/bash
# Machine-enforced multitenancy conversion audit (review round, MT map).
#
# The 274-site conversion map (docs/MULTITENANCY-MAP.md) decays as line
# numbers move; this audit does not. It fingerprints every code site in
# the categories the layout-4 conversion must drain, and compares them
# against a reviewed baseline:
#
#   NEW fingerprints  -> FAIL (someone added a bare-name identity site)
#   GONE fingerprints -> FAIL until the baseline is regenerated in the
#                        commit that converts or moves the sites
#                        (scripts/multitenancy-audit.sh --regen), so a
#                        move is reviewed, never counted as progress
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

# Every source file, at any depth: a site moved into a subdirectory is
# the same site, not a converted one.
SOURCES=()
while IFS= read -r source; do
  SOURCES+=("$source")
done < <(find src -type f -name '*.rs' | LC_ALL=C sort)

scan() {
  local cat="$1" pat="$2" hits status=0
  shift 2
  hits=$(grep -nH -E "$pat" "$@") || status=$?
  # grep exits 1 for "no match"; anything above is an error, never silence.
  if (( status > 1 )); then
    echo "multitenancy-audit: grep failed ($status) scanning $cat" >&2
    exit 2
  fi
  [ -n "$hits" ] || return 0
  # file \t normalized text: split at the first two colons, collapse the
  # TEXT's whitespace only, so the field separator survives.
  printf '%s\n' "$hits" | awk -v c="$cat" '{
    i = index($0, ":"); file = substr($0, 1, i - 1); rest = substr($0, i + 1)
    j = index(rest, ":"); text = substr(rest, j + 1)
    gsub(/[[:space:]]+/, " ", text); sub(/^ /, "", text); sub(/ $/, "", text)
    print c "\t" file "\t" text
  }'
}

existing() {
  local path
  for path in "$@"; do [ -f "$path" ] && printf '%s\n' "$path"; done
  return 0
}

collect() {
  local registry_sources=() map_sources=()
  while IFS= read -r source; do registry_sources+=("$source"); done \
    < <(existing src/registry.rs; find src/registry -type f -name '*.rs' 2>/dev/null | LC_ALL=C sort)
  while IFS= read -r source; do map_sources+=("$source"); done \
    < <(existing src/scaler3.rs src/registry.rs)
  {
    scan stream-hash 'stream_hash\(' "${SOURCES[@]}" |
      awk -F'\t' '$2 != "src/crypto.rs"'
    scan registry-bare-name \
      'registry[[:space:]]*\.[[:space:]]*(get|recreate|update|cas_update[a-z_]*|mutate_incarnation|invalidate|list_page)\("' \
      "${SOURCES[@]}"
    if (( ${#registry_sources[@]} )); then
      scan registry-bare-name \
        'fn (get|recreate|update|cas_update[a-z_]*|mutate_incarnation|invalidate|list_page)[^(]*\([^)]*name[^)]*&str' \
        "${registry_sources[@]}"
    fi
    if (( ${#map_sources[@]} )); then
      scan global-name-maps 'HashMap<String' "${map_sources[@]}"
    fi
    scan tenant-fallback '(acct_local|proj_local|"ACCOUNT_ID"|"PROJECT_ID")' "${SOURCES[@]}"
    scan internal-target 'streams-internal-(epoch|seg|identity|project)' "${SOURCES[@]}"
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

FAILED=0
if [ -n "$GONE" ]; then
  echo "multitenancy-audit: FAIL — $(echo "$GONE" | wc -l | tr -d ' ') fingerprint(s) converted or moved since the baseline:"
  echo "$GONE" | sed 's/^/  - /'
  echo "Regenerate the baseline in the converting commit (scripts/multitenancy-audit.sh --regen)"
  echo "so the conversion — or the move — is part of the reviewed diff."
  FAILED=1
fi

if [ -n "$NEW" ]; then
  echo "multitenancy-audit: FAIL — new bare-name identity site(s):"
  echo "$NEW" | sed 's/^/  + /'
  echo "Convert the site to the tenant-qualified types (src/tenant.rs +"
  echo "the RouteHash/SegmentHash layout-4 constructors), or — only for"
  echo "a reviewed identity-neutral exception — regenerate the baseline"
  echo "in this same commit and justify it in the commit message."
  FAILED=1
fi
(( FAILED == 0 )) || exit 1
echo "MT_AUDIT_OK"
