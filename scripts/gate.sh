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
# The 2026-08-25 incident class: ci.yml was invalid YAML (an unquoted
# `sse::` scalar), GitHub created ZERO jobs, and the local gate kept
# reporting green. Every commit gate now validates workflow syntax
# when actionlint is available (CI enforces it unconditionally via
# .github/workflows/workflow-lint.yml).
if command -v actionlint > /dev/null 2>&1; then
  # Bare invocation, matching the independent workflow-lint job:
  # actionlint auto-detects the project and lints every workflow file
  # (.yml AND .yaml — an explicit .yml glob silently misses .yaml).
  if ! actionlint >> "$OUT" 2>&1; then
    echo GATEFAIL-actionlint >> "$OUT"
    exit 1
  fi
else
  echo "actionlint: not installed locally; CI enforces it via workflow-lint.yml" >> "$OUT"
fi
# Round-13: the RC evidence verifier must refuse every tampered field
# — its mutation self-test runs on every commit (sub-second, pure py).
if ! python3 scripts/verify-rc-evidence.py --self-test --repo . >> "$OUT" 2>&1; then
  echo GATEFAIL-evidence-verifier >> "$OUT"
  exit 1
fi
if ! cargo fmt --check > /tmp/fmt.out 2>&1; then
  cat /tmp/fmt.out
  echo GATEFAIL-fmt >> "$OUT"
  exit 1
fi
set +e
cargo test --release --bin streams-slate -- --skip post_split_throughput_scales 2>&1 \
  | tee /tmp/gate-full.log \
  | grep -E "^test result|^test .* FAILED|^failures:$" >> "$OUT"
TEST_STATUS=${PIPESTATUS[0]}
set -e
if [ "$TEST_STATUS" -ne 0 ] || ! grep -q "^test result: ok" "$OUT"; then
  echo GATEFAIL-suite >> "$OUT"
  exit 1
fi
# The capacity-mechanism measurement OWNS the machine — its own stated
# precondition. Inside the parallel suite, contention lands one-sidedly
# on the post-split phase (it needs two committers' worth of CPU) and
# only ever understates the ratio: round-9 measured 1.73-1.80 in-suite
# against 1.8x, with healthy baselines. External host load still
# depresses it — the test's own failure text says how to distinguish.
set +e
cargo test --release --bin streams-slate post_split_throughput_scales -- \
  --exact dst::dst_tests::post_split_throughput_scales 2>&1 \
  | tee /tmp/gate-capacity.log \
  | grep -E "^test result|^failures:$" >> "$OUT"
CAP_STATUS=${PIPESTATUS[0]}
set -e
if [ "$CAP_STATUS" -ne 0 ]; then
  echo GATEFAIL-capacity >> "$OUT"
  exit 1
fi
if ! cargo clippy --release --bin streams-slate --all-targets > /dev/null 2> /tmp/clippy.out; then
  cat /tmp/clippy.out
  echo GATEFAIL-clippy-build >> "$OUT"
  exit 1
fi
NEW=$(python3 scripts/clippy-fingerprints.py /tmp/clippy.out | comm -13 scripts/clippy-baseline-fingerprints.txt -)
if [ -n "$NEW" ]; then echo "NEW FINGERPRINTS: $NEW" >> "$OUT"; echo GATEFAIL-clippy >> "$OUT"; exit 1; fi
echo "NEW FINGERPRINTS: none" >> "$OUT"
bash scripts/multitenancy-audit.sh 2>&1 | tail -5 >> "$OUT"
grep -q MT_AUDIT_OK "$OUT" || { echo GATEFAIL-audit >> "$OUT"; exit 1; }
echo GATEDONE >> "$OUT"
