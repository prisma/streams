#!/bin/sh
# The LOCAL half of the release gate: fmt, clippy (fingerprint-gated),
# the Rust/DST binary suite, and supply-chain checks. It does NOT run
# the Durable Streams conformance corpus, SDK smoke, field/capacity/
# handoff campaigns, or cross-owner fan-out — those produce artifacts
# that scripts/rc-certify.sh verifies against the release binary. R30
# review: this scope statement must match what the script runs.
set -e
cd "$(dirname "$0")/.."

echo "== fmt =="
cargo fmt --check

echo "== clippy (all targets) =="
# R30: no pipeline — POSIX sh reports the LAST command's status, so
# `cargo clippy | tee | tail` passed even when clippy itself failed to
# compile (a genuine false-green in the release gate).
if ! cargo clippy --all-targets > /tmp/clippy.out 2>&1; then
  cat /tmp/clippy.out
  echo "FAIL: clippy did not complete"
  exit 1
fi
tail -3 /tmp/clippy.out
# R30: gate on exact warning FINGERPRINTS (message + file), not a
# count. Counts were twice unsound: a stale-low baseline blocked clean
# trees (round 18: recorded 114, real 345; R29: recorded 221 warm vs
# 229 cold), and a count can stay flat while one warning disappears
# and a NEW one appears. The baseline file is the reviewed allowlist;
# refresh it only with a fingerprint diff in the commit message.
python3 scripts/clippy-fingerprints.py /tmp/clippy.out > /tmp/clippy-fps.txt
if ! NEW=$(comm -13 scripts/clippy-baseline-fingerprints.txt /tmp/clippy-fps.txt); then
  echo "FAIL: fingerprint comparison failed"
  exit 1
fi
if [ -n "$NEW" ]; then
  echo "FAIL: NEW clippy warnings (not in the reviewed baseline):"
  echo "$NEW"
  exit 1
fi
GONE=$(comm -23 scripts/clippy-baseline-fingerprints.txt /tmp/clippy-fps.txt || true)
if [ -n "$GONE" ]; then
  echo "note: $(echo "$GONE" | wc -l | tr -d ' ') baseline warning(s) no longer fire; refresh the baseline"
fi
echo "clippy: no new warning fingerprints"

echo "== tests =="
cargo test --bin streams-slate

echo "== supply chain =="
cargo deny check

echo "RELEASE_GATE_LOCAL_OK"
