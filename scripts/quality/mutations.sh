#!/usr/bin/env bash
# Test changed canonical read owners against a saved actual PR merge-base diff.
# A zero-match scope is reported, never called a passing mutation experiment.
set -euo pipefail
cd "$(dirname "$0")/../.."
QUALITY_MUTANTS_OUT=${QUALITY_MUTANTS_OUT:-target/quality-mutations}
mkdir -p "$QUALITY_MUTANTS_OUT"
QUALITY_MUTANTS_OUT=$(cd "$QUALITY_MUTANTS_OUT" && pwd)
python3 scripts/quality/verification_plan.py --out "$QUALITY_MUTANTS_OUT"
expected=$(python3 -c 'import tomllib; print(tomllib.load(open("quality-tools.toml","rb"))["tools"]["cargo-mutants"])')
[[ "$(cargo mutants --version)" == "cargo-mutants $expected" ]]
# cargo-mutants preserves lexical #[path] names. Git emits the same PR hunks
# with that path prefix; compare both selected inventories before any execution.
PREFIX=tools/quality-invariants/src/../../../
BASE=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["merge_base"])' "$QUALITY_MUTANTS_OUT/plan.json")
git diff --no-ext-diff --binary --src-prefix="a/$PREFIX" --dst-prefix="b/$PREFIX" "$BASE" -- > "$QUALITY_MUTANTS_OUT/harness-pr.diff"
TOTAL=0
for owner in postings batch retained rollup_allocation; do
  case "$owner" in
    postings) file=src/postings/validated.rs; filter=postings:: ;;
    batch) file=src/application/read_batch.rs; filter=application::read_batch:: ;;
    retained) file=src/retained_bytes.rs; filter=retained_bytes:: ;;
    rollup_allocation) file=src/rollup/allocation.rs; filter=rollup_allocation:: ;;
  esac
  output="$QUALITY_MUTANTS_OUT/$owner"
  mkdir -p "$output"
  cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
    --file "$file" --package streams-slate > "$output/selected.json"
  cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/harness-pr.diff" \
    --file "$PREFIX$file" --package streams-quality-invariants > "$output/harness-selected.json"
  count=$(python3 - "$output/selected.json" "$output/harness-selected.json" <<'PYTHON'
import json, os, sys
def selection(path):
    raw = open(path).read().strip()
    rows = json.loads(raw) if raw else []
    return sorted(json.dumps(dict(file=os.path.normpath(row['file']),
        **{k:row[k] for k in ('function','span','replacement','genre')}), sort_keys=True) for row in rows)
canonical, harness = map(selection, sys.argv[1:])
if canonical != harness:
    raise SystemExit('harness mutation scope differs from canonical PR source')
print(len(canonical))
PYTHON
)
  if [[ "$count" == 0 ]]; then
    printf '%s: no executable mutants in the actual diff\n' "$owner"
    continue
  fi
  TOTAL=$((TOTAL + count))
  cargo mutants --cargo-arg=--locked --cargo-arg=--lib --cargo-arg="--target-dir=$QUALITY_MUTANTS_OUT/build" --baseline run --in-diff "$QUALITY_MUTANTS_OUT/harness-pr.diff" \
    --file "$PREFIX$file" --package streams-quality-invariants --cargo-test-arg="$filter" \
    --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output "$output"
done
if [[ "$TOTAL" == 0 ]]; then
  echo 'No executable mutations selected; register the changed critical owner before claiming mutation verification.' >&2
  exit 1
fi
