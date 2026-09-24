#!/usr/bin/env bash
# Common local/CI entry. All output kept locally; no implicit evidence upload.
set -euo pipefail
cd "$(dirname "$0")/.."
QUALITY_OUT=${QUALITY_OUT:-target/quality}
mkdir -p "$QUALITY_OUT"
python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); import config; problems=config.check(); print("\n".join(problems)); sys.exit(bool(problems))'
# Bare invocation discovers every workflow, including both .yml and .yaml.
if ! actionlint; then
  echo 'QUALITY_FAIL: workflow lint (actionlint)' >&2
  exit 1
fi
cargo fmt --all -- --check
cargo test --locked -p streams-quality-syntax
cargo build --locked -p streams-quality-syntax
python3 -m unittest discover -s scripts/quality -v
# The formal-verification manifest names real harnesses, models and
# assumptions, and every claimed result has a structurally valid receipt:
# an invalid or missing receipt fails here. A stale receipt (inputs changed
# since it was recorded) is only reported: the formal CI job re-runs what a
# change affects, and scripts/release-gate.sh requires `check --fresh`
# (verification/README.md, "Three levels of enforcement").
python3 scripts/quality/formal.py check
# The JSON goes to a file, so a failed clippy would otherwise stop here with
# no finding on screen: the ratchet always reads it and prints what the
# compiler refused (its rendered file:line and help), then both statuses
# decide. A failed build is never success (gate.py refuses it too).
clippy_status=0
cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings \
  > "$QUALITY_OUT/clippy.jsonl" || clippy_status=$?
ratchet_status=0
python3 scripts/quality/gate.py --clippy "$QUALITY_OUT/clippy.jsonl" || ratchet_status=$?
if (( clippy_status != 0 || ratchet_status != 0 )); then
  echo "QUALITY_FAIL: clippy exit $clippy_status, ratchet exit $ratchet_status" >&2
  exit 1
fi
# rustdoc is a compiler too, and nothing else here runs it: an unclosed
# tag or a link to a renamed item is a warning only it reports. Private
# items are documented because most of this crate is pub(crate) — the
# public-only build would check almost none of its prose.
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items
cargo machete
cargo deny --locked --workspace check
for gate in architecture-report architecture-gate scenario-map-report test-inventory review-evidence; do
  python3 "scripts/$gate.py" --self-test
  if [[ "$gate" != architecture-report ]]; then python3 "scripts/$gate.py" --check; fi
done
python3 scripts/verify-rc-evidence.py --self-test --repo .
bash scripts/multitenancy-audit.sh
scripts/test-leg.sh "$QUALITY_OUT/mt-lint.log" --exact mt_lint::multitenancy_identity_lint \
  -- --locked --release --lib multitenancy_identity_lint
printf 'QUALITY_OK\n'
