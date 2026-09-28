#!/usr/bin/env bash
# The LOCAL half of the release gate: everything the commit gate checks
# (scripts/quality.sh: fmt, workflow lint, clippy with -D warnings over the
# workspace and the source ratchet, rustdoc, cargo machete and deny, the
# architecture/scenario/inventory/evidence gates, the multitenancy audit and
# formal receipts' validity), plus what only a release requires: every
# claimed formal result current (`check --fresh`), the Rust/DST suite in the
# debug profile over every target, and the capacity mechanism gate isolated
# so its measurement owns the machine. It does NOT run the Durable Streams
# conformance corpus, SDK smoke, field/capacity/handoff campaigns, or
# cross-owner fan-out — those produce artifacts that scripts/rc-certify.sh
# verifies against the release binary. R30 review: this scope statement
# must match what the script runs.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/python.sh

echo "== commit-gate policy (scripts/quality.sh) =="
scripts/quality.sh

# A release needs a current receipt for every claimed formal result; the
# commit gate (quality.sh) only reports staleness, and the formal CI job
# runs what each change affects. Re-record with
# `python3 scripts/dev/formal_batch.py rerecord`.
echo "== formal receipts current =="
python3 scripts/quality/formal.py check --fresh

# Each leg proves it ran what it names (scripts/test-leg.sh): cargo exits
# 0 with `ok. 0 passed` when a filter or --exact name matches nothing.
echo "== tests =="
scripts/test-leg.sh target/release-gate/suite.log \
  --inventory docs/refactor/test-inventory.json --skipped 1 \
  -- --locked --lib -- --skip post_split_throughput_scales
scripts/test-leg.sh target/release-gate/targets.log --min 100 \
  -- --locked --bins --test pilot_membership

echo "== capacity mechanism gate (owns the machine) =="
scripts/test-leg.sh target/release-gate/capacity.log --exact dst::dst_tests::topology_scaling::post_split_throughput_scales \
  -- --locked --lib post_split_throughput_scales -- --exact dst::dst_tests::topology_scaling::post_split_throughput_scales

echo "RELEASE_GATE_LOCAL_OK"
