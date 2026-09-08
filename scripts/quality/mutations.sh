#!/usr/bin/env bash
# Test changed canonical read owners against a saved actual PR merge-base diff.
# A zero-match scope is reported, never called a passing mutation experiment.
set -euo pipefail
cd "$(dirname "$0")/../.."
QUALITY_MUTANTS_OUT=${QUALITY_MUTANTS_OUT:-target/quality-mutations}
mkdir -p "$QUALITY_MUTANTS_OUT"
python3 scripts/quality/verification_plan.py --out "$QUALITY_MUTANTS_OUT"
expected=$(python3 -c 'import tomllib; print(tomllib.load(open("quality-tools.toml","rb"))["tools"]["cargo-mutants"])')
[[ "$(cargo mutants --version)" == "cargo-mutants $expected" ]]
TOTAL=0
for owner in postings batch retained; do
  case "$owner" in
    postings) file=src/postings/validated.rs; filter=postings:: ;;
    batch) file=src/application/read_batch.rs; filter=application::read_batch:: ;;
    retained) file=src/retained_bytes.rs; filter=retained_bytes:: ;;
  esac
  output="$QUALITY_MUTANTS_OUT/$owner"
  mkdir -p "$output"
  cargo mutants --list --json --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
    --file "$file" --package streams-slate > "$output/selected.json"
  count=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' "$output/selected.json")
  if [[ "$count" == 0 ]]; then
    printf '%s: no executable mutants in the actual diff\n' "$owner"
    continue
  fi
  TOTAL=$((TOTAL + count))
  cargo mutants --cargo-arg=--locked --cargo-arg=--lib --baseline run --in-diff "$QUALITY_MUTANTS_OUT/pr.diff" \
    --file "$file" --package streams-slate --cargo-test-arg="$filter" \
    --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output "$output"
done
if [[ "$TOTAL" == 0 ]]; then
  echo 'No executable mutations selected; register the changed critical owner before claiming mutation verification.' >&2
  exit 1
fi
