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
if ! cargo fmt --check > /tmp/fmt.out 2>&1; then
  cat /tmp/fmt.out
  echo GATEFAIL-fmt >> "$OUT"
  exit 1
fi
set +e
cargo test --release --bin streams-slate 2>&1 | tee /tmp/gate-full.log \
  | grep -E "^test result|^test .* FAILED|^failures:$" >> "$OUT"
TEST_STATUS=${PIPESTATUS[0]}
set -e
if [ "$TEST_STATUS" -ne 0 ] || ! grep -q "^test result: ok" "$OUT"; then
  echo GATEFAIL-suite >> "$OUT"
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
