#!/usr/bin/env bash
# Test changed canonical owners against a saved actual PR merge-base diff.
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
for owner in postings_codec postings batch retained quota cursors queue rollup_allocation rollup_storage tasks; do
  case "$owner" in
    postings_codec) file=src/postings.rs; filter=postings:: ;;
    postings) file=src/postings/validated.rs; filter=postings:: ;;
    batch) file=src/application/read_batch.rs; filter=application::read_batch:: ;;
    retained) file=src/retained_bytes.rs; filter=retained_bytes:: ;;
    quota) file=src/quota/bucket.rs; filter=quota_bucket:: ;;
    cursors) file=src/product_cursor/decode.rs; filter=product_cursor:: ;;
    queue) file=src/queue.rs; filter=queue:: ;;
    rollup_allocation) file=src/rollup/allocation.rs; filter=rollup_allocation:: ;;
    rollup_storage) file=src/rollup/storage.rs; filter=rollup_storage:: ;;
    tasks) file=src/tasks.rs; filter=tasks:: ;;
  esac
  output="$QUALITY_MUTANTS_OUT/$owner"
  mkdir -p "$output"
  cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
    --file "$file" --package streams-slate > "$output/selected.json"
  package=streams-quality-invariants
  mutation_file="$PREFIX$file"
  mutation_diff="$QUALITY_MUTANTS_OUT/harness-pr.diff"
  if [[ "$owner" == tasks ]]; then
    # Supervisor fixtures use actual Tokio task handles and process signals.
    # Run them in the service crate, rather than rewriting a stand-in model.
    package=streams-slate
    mutation_file="$file"
    mutation_diff="$QUALITY_MUTANTS_OUT/pr.diff"
    count=$(python3 -c 'import json,sys; text=open(sys.argv[1]).read().strip(); print(len(json.loads(text) if text else []))' "$output/selected.json")
  else
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
  fi
  if [[ "$count" == 0 ]]; then
    printf '%s: no executable mutants in the actual diff\n' "$owner"
    continue
  fi
  TOTAL=$((TOTAL + count))
  cargo mutants --cargo-arg=--locked --cargo-arg=--lib --cargo-arg="--target-dir=$QUALITY_MUTANTS_OUT/build" --baseline run --in-diff "$mutation_diff" \
    --file "$mutation_file" --package "$package" --cargo-test-arg="$filter" \
    --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output "$output"
done
if [[ "$TOTAL" == 0 ]]; then
  echo 'No executable mutations selected; register the changed critical owner before claiming mutation verification.' >&2
  exit 1
fi
