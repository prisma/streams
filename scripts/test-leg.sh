#!/usr/bin/env bash
# One filtered `cargo test` leg that proves it ran what it names.
#
#   scripts/test-leg.sh <log> [tests_ran.py options] -- <cargo test arguments>
#
# cargo exits 0 with `test result: ok. 0 passed` when a filter or an
# --exact name matches nothing, so a reviewed rename silently drops the
# leg that named the test. The log is kept; scripts/quality/tests_ran.py
# judges it (every result ok, the floor reached, each --exact name ran).
set -euo pipefail
cd "$(dirname "$0")/.."
log=$1
shift
expect=()
while [[ $# -gt 0 && $1 != -- ]]; do
  expect+=("$1")
  shift
done
if [[ $# -eq 0 ]]; then
  echo "usage: $0 <log> [tests_ran.py options] -- <cargo test arguments>" >&2
  exit 2
fi
shift
mkdir -p "$(dirname "$log")"
cargo test "$@" 2>&1 | tee "$log"
python3 scripts/quality/tests_ran.py "$log" "${expect[@]}"
