#!/bin/bash
# The one commit gate: full release suite (failure NAMES preserved —
# a prior chain kept only the summary line and a one-off flake cost a
# re-run to even identify), clippy fingerprint diff (fail-closed), and
# the bare multitenancy audit. Output lands in $OUT (default
# /tmp/gate.txt); exits nonzero on any failed stage.
set -o pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-/tmp/gate.txt}
cd "$HERE"
: > "$OUT"
cargo fmt
cargo test --release --bin streams-slate 2>&1 | tee /tmp/gate-full.log \
  | grep -E "^test result|^test .* FAILED|^failures:$" >> "$OUT"
grep -q "^test result: ok" "$OUT" || { echo GATEFAIL-suite >> "$OUT"; exit 1; }
cargo clippy --release --bin streams-slate --all-targets 2>&1 > /dev/null | cat > /tmp/clippy.out
NEW=$(python3 scripts/clippy-fingerprints.py /tmp/clippy.out | comm -13 scripts/clippy-baseline-fingerprints.txt -)
if [ -n "$NEW" ]; then echo "NEW FINGERPRINTS: $NEW" >> "$OUT"; echo GATEFAIL-clippy >> "$OUT"; exit 1; fi
echo "NEW FINGERPRINTS: none" >> "$OUT"
bash scripts/multitenancy-audit.sh 2>&1 | tail -5 >> "$OUT"
grep -q MT_AUDIT_OK "$OUT" || { echo GATEFAIL-audit >> "$OUT"; exit 1; }
echo GATEDONE >> "$OUT"
