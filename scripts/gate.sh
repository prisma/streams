#!/bin/bash
# The one commit gate — fail-closed at every stage (review finding 5):
# formatting must already be clean, a clippy BUILD failure fails the
# gate (compiler errors are not warning fingerprints), and each test leg
# must show the tests it names RAN: cargo exits 0 with `ok. 0 passed`
# when a filter or --exact name matches nothing. Output lands in $OUT
# (or the first argument), default target/gate/gate.txt in this checkout,
# so concurrent checkouts never share it. Its last line is GATEDONE or
# GATEFAIL-<stage>; the terminal gets one line per stage and, on failure,
# the tail of the log that failed.
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
cd "$HERE"
source scripts/lib/python.sh
OUT=${1:-${OUT:-target/gate/gate.txt}}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"
stage() { echo "gate: $1 ... (log: $2)"; }
fail() {
  echo "GATEFAIL-$1" >> "$OUT"
  echo "GATEFAIL-$1: see $2" >&2
  tail -n 60 "$2" >&2
  exit 1
}
# The release targets every leg below runs, built by one cargo invocation
# inside quality.sh while its other checks run.
TARGETS=(--bins --test pilot_membership)
# One mandatory entry point for local and CI source/dependency policy.
stage quality "$OUT"
if ! QUALITY_RELEASE_TARGETS="--lib ${TARGETS[*]}" scripts/quality.sh >> "$OUT" 2>&1; then
  fail quality "$OUT"
fi
stage suite "$OUT.suite.log"
if ! cargo test --locked --release --lib -- --skip post_split_throughput_scales > "$OUT.suite.log" 2>&1; then
  fail suite "$OUT.suite.log"
fi
grep -E '^test result: ok' "$OUT.suite.log" >> "$OUT"
# The suite holds every inventoried test but the one capacity leg below.
if ! python3 scripts/quality/tests_ran.py "$OUT.suite.log" \
  --inventory docs/refactor/test-inventory.json --skipped 1 >> "$OUT" 2>&1; then
  fail suite-ran "$OUT"
fi
# CI's suite also runs every other target: the bins' unit tests and
# tests/pilot_membership.rs (with the pilot Loom models). The floor proves
# the selection still runs; it is not a test count to maintain.
stage targets "$OUT.targets.log"
if ! scripts/test-leg.sh "$OUT.targets.log" --min 100 -- --locked --release "${TARGETS[@]}" >> "$OUT" 2>&1; then
  fail targets "$OUT.targets.log"
fi
# The capacity-mechanism measurement OWNS the machine — its own stated
# precondition. Inside the parallel suite, contention lands one-sidedly
# on the post-split phase (it needs two committers' worth of CPU) and
# only ever understates the ratio: round-9 measured 1.73-1.80 in-suite
# against 1.8x, with healthy baselines. External host load still
# depresses it — the test's own failure text says how to distinguish.
stage capacity "$OUT.capacity.log"
if ! cargo test --locked --release --lib post_split_throughput_scales -- \
  --exact dst::dst_tests::topology_scaling::post_split_throughput_scales > "$OUT.capacity.log" 2>&1; then
  fail capacity "$OUT.capacity.log"
fi
grep -E '^test result: ok' "$OUT.capacity.log" >> "$OUT"
if ! python3 scripts/quality/tests_ran.py "$OUT.capacity.log" \
  --exact dst::dst_tests::topology_scaling::post_split_throughput_scales >> "$OUT" 2>&1; then
  fail capacity-ran "$OUT"
fi
echo GATEDONE >> "$OUT"
echo GATEDONE
