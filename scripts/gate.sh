#!/bin/bash
# The one commit gate — fail-closed at every stage (review finding 5):
# formatting must already be clean, a clippy BUILD failure fails the
# gate (compiler errors are not warning fingerprints), and the suite
# summary line must literally read ok. Output lands in $OUT.
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-/tmp/gate.txt}
cd "$HERE"
: > "$OUT"
# One mandatory entry point for local and CI source/dependency policy.
if ! scripts/quality.sh >> "$OUT" 2>&1; then
  echo GATEFAIL-quality >> "$OUT"
  exit 1
fi
if ! cargo test --locked --release --lib -- --skip post_split_throughput_scales > "$OUT.suite.log" 2>&1; then
  echo GATEFAIL-suite >> "$OUT"
  exit 1
fi
grep -E '^test result: ok' "$OUT.suite.log" >> "$OUT"
# The capacity-mechanism measurement OWNS the machine — its own stated
# precondition. Inside the parallel suite, contention lands one-sidedly
# on the post-split phase (it needs two committers' worth of CPU) and
# only ever understates the ratio: round-9 measured 1.73-1.80 in-suite
# against 1.8x, with healthy baselines. External host load still
# depresses it — the test's own failure text says how to distinguish.
if ! cargo test --locked --release --lib post_split_throughput_scales -- \
  --exact dst::dst_tests::topology_scaling::post_split_throughput_scales > "$OUT.capacity.log" 2>&1; then
  echo GATEFAIL-capacity >> "$OUT"
  exit 1
fi
grep -E '^test result: ok' "$OUT.capacity.log" >> "$OUT"
echo GATEDONE >> "$OUT"
