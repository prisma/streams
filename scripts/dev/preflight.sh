#!/usr/bin/env bash
# Pre-push preflight: the cheap checks scripts/quality.sh runs, in parallel,
# plus what CI will select for this change. About a minute on a warm tree.
#
#   scripts/dev/preflight.sh            # compare with merge-base(HEAD, origin/slate)
#   QUALITY_BASE_REF=<rev> scripts/dev/preflight.sh
#
# Advisory only: PREFLIGHT_OK is not QUALITY_OK. It exists so a ratchet,
# receipt, inventory or registration mistake surfaces in a minute instead of
# at the end of scripts/quality.sh (minutes) or in CI (up to hours). It runs
# the gates' own commands and scripts; it never edits the tree (the syntax
# scanner build and logs under target/preflight/ are the only writes).
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/lib/python.sh
OUT=target/preflight
mkdir -p "$OUT"
rm -f "$OUT"/*.log

base_ref=${QUALITY_BASE_REF:-origin/slate}
echo "preflight: HEAD $(git rev-parse --short HEAD), base merge-base(HEAD, $base_ref) = $(git merge-base HEAD "$base_ref" | cut -c1-8)"
echo "preflight: $(git status --porcelain | wc -l | tr -d ' ') uncommitted path(s); $(git rev-list --count "$base_ref"..HEAD) commit(s) ahead of $base_ref"

# The scanner feeds the ratchet, the architecture gate and the planner.
if ! cargo build --locked -q -p streams-quality-syntax > "$OUT/scanner.log" 2>&1; then
  echo "PREFLIGHT_FAIL: syntax scanner build; see $OUT/scanner.log" >&2
  tail -n 40 "$OUT/scanner.log" >&2
  exit 1
fi

names=()
pids=()
check() {
  local name=$1
  shift
  ( "$@" ) > "$OUT/$name.log" 2>&1 &
  names+=("$name")
  pids+=("$!")
}
clippy_ratchet() {
  cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings \
    > "$OUT/clippy.jsonl" || true
  # gate.py refuses a failed build and prints what the compiler refused.
  python3 scripts/quality/gate.py --clippy "$OUT/clippy.jsonl"
}
plan() {
  python3 scripts/quality/verification_plan.py --out "$OUT/plan"
  python3 - "$OUT/plan/plan.json" <<'PY'
import json, sys
plan = json.load(open(sys.argv[1]))
legs = [name for name in ('mutants', 'miri', 'properties_fuzz') if plan.get(name)]
print('CI invariant legs selected:', ', '.join(legs) or 'none')
print('mutation owners CI will run:', ', '.join(plan.get('selected_mutation_owners', [])) or 'none')
missing = plan.get('unregistered_mutation_source_files', [])
if missing:
    print('REGISTER FIRST (CI refuses these; add rows to scripts/quality/mutation_owners.py):')
    for path in missing:
        print('  ' + path)
    sys.exit(1)
PY
}

check fmt cargo fmt --all -- --check
check formal python3 scripts/quality/formal.py check
check architecture python3 scripts/architecture-gate.py --check
check inventory python3 scripts/test-inventory.py --check
check evidence python3 scripts/review-evidence.py --check
check scenarios python3 scripts/scenario-map-report.py --check
check mt-audit bash scripts/multitenancy-audit.sh
check clippy-ratchet clippy_ratchet
check ci-plan plan

failed=0
for i in "${!names[@]}"; do
  if wait "${pids[$i]}"; then
    printf 'PASS  %s\n' "${names[$i]}"
  else
    failed=1
    printf 'FAIL  %s  (%s)\n' "${names[$i]}" "$OUT/${names[$i]}.log"
    tail -n 25 "$OUT/${names[$i]}.log" | sed 's/^/      /'
  fi
done
grep -h 'CI invariant legs\|mutation owners CI\|REGISTER FIRST\|^  src/' "$OUT/ci-plan.log" || true
tail -n 1 "$OUT/formal.log"

if [[ -f scripts/dev/formal_batch.py ]]; then
  echo '--- formal cost of this change (what CI re-runs, what goes stale)'
  python3 scripts/dev/formal_batch.py cost --base "$(git merge-base HEAD "$base_ref")" || true
fi
if [[ -f scripts/dev/impact.py ]]; then
  echo '--- change impact (python3 scripts/dev/impact.py --verbose for per-file detail)'
  python3 scripts/dev/impact.py --base "$base_ref" || true
fi

if (( failed )); then
  echo PREFLIGHT_FAIL
  exit 1
fi
echo 'PREFLIGHT_OK (advisory: scripts/gate.sh is the commit gate)'
