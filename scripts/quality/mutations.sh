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
REGISTERED=(src/bin/pilot/benchmark.rs src/bin/pilot/benchmark/config.rs src/bin/pilot/benchmark/window.rs src/bin/pilot/generator.rs src/bin/pilot/generator/membership.rs)
for owner in postings_codec postings batch retained quota cursors queue rollup_allocation rollup_storage tasks touch read_accumulator read_spool shard_directory history_partition ops scaler postings_cache sharddir crypto tail_ring shard bootstrap read_request http_read queue_cleanup transaction_append record; do
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
    touch) file=src/touch.rs; filter=touch:: ;;
    read_accumulator) file=src/billing/read_accumulator.rs; filter=billing ;;
    read_spool) file=src/billing/read_spool.rs; filter=billing ;;
    shard_directory) file=src/shard_directory.rs; filter=shard_directory:: ;;
    history_partition) file=src/shard/history_partition.rs; filter=shard:: ;;
    ops) file=src/ops.rs; filter=ops:: ;;
    scaler) file=src/scaler3.rs; filter=scaler3:: ;;
    bootstrap) file=src/bootstrap.rs; filter=bootstrap:: ;;
    read_request) file=src/application/read_request.rs; filter=application::read_request:: ;;
    http_read) file=src/http/read.rs; filter=http::read:: ;;
    queue_cleanup) file=src/shard/transaction/queue/cleanup.rs; filter=shard:: ;;
    transaction_append) file=src/shard/transaction/append.rs; filter=shard:: ;;
    record) file=src/shard/record.rs; filter=shard:: ;;
    postings_cache) file=src/postings_cache.rs; filter=postings_cache:: ;;
    sharddir) file=src/sharddir.rs; filter=sharddir:: ;;
    crypto) file=src/crypto.rs; filter=crypto:: ;;
    shard) file=src/shard.rs; filter=shard:: ;;
    tail_ring) file=src/shard/tail_ring.rs; filter="shard:: dst_tests::reads_ring::" ;;
  esac
  REGISTERED+=("$file")
  output="$QUALITY_MUTANTS_OUT/$owner"
  mkdir -p "$output"
  cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
    --file "$file" --package streams-slate > "$output/selected.json"
  package=streams-quality-invariants
  mutation_file="$PREFIX$file"
  mutation_diff="$QUALITY_MUTANTS_OUT/harness-pr.diff"
  if [[ "$owner" == tasks || "$owner" == touch || "$owner" == read_accumulator || "$owner" == read_spool || "$owner" == shard_directory || "$owner" == history_partition || "$owner" == ops || "$owner" == scaler || "$owner" == postings_cache || "$owner" == sharddir || "$owner" == crypto || "$owner" == tail_ring || "$owner" == shard || "$owner" == bootstrap || "$owner" == read_request || "$owner" == http_read || "$owner" == queue_cleanup || "$owner" == transaction_append || "$owner" == record ]]; then
    # These owners use actual service clocks, task handles and storage types.
    # Keep their code and tests in the service crate without substitute models.
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
  # An owner whose behaviour is exercised by more than one test module names
  # every filter. cargo test takes one test name itself; the rest follow the
  # separator so libtest runs the tests matching any of them.
  read -r -a test_filters <<< "$filter"
  test_args=("--cargo-test-arg=${test_filters[0]}")
  if (( ${#test_filters[@]} > 1 )); then
    test_args+=("--cargo-test-arg=--" "${test_filters[@]:1}")
    for ((i = 2; i < ${#test_args[@]}; i++)); do test_args[i]="--cargo-test-arg=${test_args[i]}"; done
  fi
  cargo mutants --cargo-arg=--locked --cargo-arg=--lib --cargo-arg="--target-dir=$QUALITY_MUTANTS_OUT/build" --baseline run --in-diff "$mutation_diff" \
    --file "$mutation_file" --package "$package" "${test_args[@]}" \
    --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output "$output"
done
# The benchmark owns its workers and measurement window in the pilot binary.
output="$QUALITY_MUTANTS_OUT/pilot-benchmark"
mkdir -p "$output"
cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
  --file src/bin/pilot/benchmark.rs --file src/bin/pilot/benchmark/config.rs \
  --file src/bin/pilot/benchmark/window.rs --package streams-slate > "$output/selected.json"
count=$(python3 -c 'import json,sys; raw=open(sys.argv[1]).read().strip(); print(len(json.loads(raw)) if raw else 0)' "$output/selected.json")
if [[ "$count" != 0 ]]; then
  TOTAL=$((TOTAL + count))
  cargo mutants --cargo-arg=--locked --cargo-arg=--bin=pilot \
    --cargo-arg="--target-dir=$QUALITY_MUTANTS_OUT/pilot-build" --baseline run \
    --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" --file src/bin/pilot/benchmark.rs \
    --file src/bin/pilot/benchmark/config.rs --file src/bin/pilot/benchmark/window.rs \
    --package streams-slate --cargo-test-arg=benchmark:: --profile quality \
    --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output "$output"
else
  echo 'pilot-benchmark: no executable mutants in the actual diff'
fi
# The pilot generator owns its worker lifetime and final measurement accounting.
# Its binary tests cover HTTP and RAII; the integration test compiles the same
# terminal membership source with Loom primitives. Neither is a library test.
output="$QUALITY_MUTANTS_OUT/pilot-generator"
mkdir -p "$output"
cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
  --file src/bin/pilot/generator.rs --file src/bin/pilot/generator/membership.rs \
  --package streams-slate > "$output/selected.json"
count=$(python3 -c 'import json,sys; raw=open(sys.argv[1]).read().strip(); print(len(json.loads(raw)) if raw else 0)' "$output/selected.json")
if [[ "$count" != 0 ]]; then
  TOTAL=$((TOTAL + count))
  cargo mutants --cargo-arg=--locked --cargo-arg=--bin=pilot --cargo-arg=--test=pilot_membership \
    --cargo-arg="--target-dir=$QUALITY_MUTANTS_OUT/pilot-build" --baseline run \
    --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" --file src/bin/pilot/generator.rs \
    --file src/bin/pilot/generator/membership.rs --package streams-slate \
    --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output "$output"
else
  echo 'pilot-generator: no executable mutants in the actual diff'
fi
if [[ "$TOTAL" == 0 ]]; then
  # An empty selection is reported, never claimed as a passing experiment. It
  # is acceptable only when every executable critical file in the actual diff
  # belongs to a registered owner whose scope simply selected no mutant.
  uncovered=$(python3 - "$QUALITY_MUTANTS_OUT/plan.json" "${REGISTERED[@]}" <<'PYTHON'
import json, sys
plan = json.load(open(sys.argv[1]))
registered = set(sys.argv[2:])
print('\n'.join(sorted(set(plan.get('mutation_source_files', [])) - registered)))
PYTHON
)
  if [[ -n "$uncovered" ]]; then
    printf 'No executable mutations selected; register the changed critical owner before claiming mutation verification:\n%s\n' "$uncovered" >&2
    exit 1
  fi
  echo 'No executable mutations in the registered owners of this diff; no mutation experiment is claimed.'
fi
