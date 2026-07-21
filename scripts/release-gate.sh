#!/bin/sh
# The local half of the release gate (AWS-readyness.md §5, §7).
# The other half — the single-instance saturation benchmark on Compute —
# is scripts/bench-fra-ab.sh and is mandatory for any change that can touch
# the serving path.
set -e
cd "$(dirname "$0")/.."

echo "== fmt =="
cargo fmt --check

echo "== clippy (all targets) =="
cargo clippy --all-targets 2>&1 | tee /tmp/clippy.out | tail -3
# The branch carries a small pre-existing warning set; the gate is
# "no NEW warnings": compare against the recorded count.
BASELINE=$(cat scripts/clippy-warning-baseline.txt)
COUNT=$(grep -c '^warning' /tmp/clippy.out || true)
if [ "$COUNT" -gt "$BASELINE" ]; then
  echo "FAIL: clippy warnings grew $BASELINE -> $COUNT"; exit 1
fi
echo "clippy warnings: $COUNT (baseline $BASELINE)"

echo "== tests =="
cargo test --bin streams-slate

echo "== supply chain =="
cargo deny check

echo "RELEASE_GATE_LOCAL_OK"
