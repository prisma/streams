#!/usr/bin/env bash
# Emit the exact artifact provenance for a release report, so the
# numbers can never drift from the commit they describe (round-14
# review: the report named one commit, the reviewed archive another).
#
#   scripts/release-provenance.sh            # facts only
#   RUN_SUITE=1 scripts/release-provenance.sh # also run the Rust suite
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

echo "server_commit:      $(git rev-parse HEAD)"
echo "server_dirty:       $([ -z "$(git status --porcelain)" ] && echo no || echo YES)"
echo "slatedb_pin:         $(grep -oE 'rev = \"[0-9a-f]{40}\"' Cargo.toml | head -1 | grep -oE '[0-9a-f]{40}' || echo unknown)"
echo "layout_version:     $(grep -oE 'LAYOUT_VERSION: u32 = [0-9]+' src/registry.rs | grep -oE '[0-9]+$' | head -1 || echo unknown)"
echo "conformance_pin:    $(grep -oE '@durable-streams/server-conformance-tests[\"@: ]+[0-9.]+' conformance/package.json | grep -oE '[0-9.]+$' | head -1 || echo unknown)"

# SDK tarball SHA (build + pack into a temp dir, no publish).
if [ "${RUN_SDK:-1}" = "1" ]; then
  ( cd sdk && npm run build >/dev/null 2>&1 && npm pack --pack-destination /tmp >/dev/null 2>&1 ) || true
  TARBALL=$(ls -t /tmp/prisma-streams-*.tgz 2>/dev/null | head -1 || true)
  [ -n "$TARBALL" ] && echo "sdk_tarball_sha256: $(shasum -a 256 "$TARBALL" | cut -d' ' -f1)" || echo "sdk_tarball_sha256: (pack skipped)"
fi

# DST scenario totals, by test-name family, from the source of truth.
echo "dst_scenario_tests: $(grep -c '#\[tokio::test' src/dst/dst_tests.rs)"

if [ "${RUN_SUITE:-0}" = "1" ]; then
  echo "--- running cargo test --release (this is slow) ---"
  cargo test --release 2>&1 | grep -E '^test result' | \
    awk '{p+=$4; f+=$6} END {print "rust_suite_passed:  " p; print "rust_suite_failed:  " f}'
fi
