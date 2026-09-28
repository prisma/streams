#!/usr/bin/env bash
# Common local/CI entry. All output kept locally; no implicit evidence upload.
#
# Every step runs and fails closed; the order puts the cheap checks first so
# a mistake surfaces in seconds, and the slowest compile (the release
# lib-test harness for the mt-lint leg, which gate.sh's suite reuses) runs in
# the background meanwhile. A failed step prints `QUALITY_FAIL: <command>`;
# success is the final line `QUALITY_OK`.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/python.sh
QUALITY_OUT=${QUALITY_OUT:-target/quality}
mkdir -p "$QUALITY_OUT"
trap 'echo "QUALITY_FAIL: exit $? at scripts/quality.sh:$LINENO: $BASH_COMMAND" >&2' ERR
python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); import config; problems=config.check(); print("\n".join(problems)); sys.exit(bool(problems))'
# Compiles only; the mt-lint leg below waits for it and runs the test.
# scripts/gate.sh widens it (QUALITY_RELEASE_TARGETS) to the targets its own
# legs run, so one cargo invocation builds them all in parallel.
read -r -a release_targets <<< "${QUALITY_RELEASE_TARGETS:---lib}"
cargo test --locked --release "${release_targets[@]}" --no-run > "$QUALITY_OUT/release-build.log" 2>&1 &
release_build=$!
# A step that fails before that wait must not leave the compile running:
# it would hold the build lock and CPU into the next run. rustc children go
# first (cargo forwards no signal to them); once the job has been reaped on
# the success path there is nothing to stop.
stop_release_build() {
  if kill -0 "$release_build" 2>/dev/null; then
    pkill -TERM -P "$release_build" 2>/dev/null || true
    kill -TERM "$release_build" 2>/dev/null || true
    wait "$release_build" 2>/dev/null || true
  fi
}
trap stop_release_build EXIT
# Bare invocation discovers every workflow, including both .yml and .yaml.
if ! actionlint; then
  echo 'QUALITY_FAIL: workflow lint (actionlint)' >&2
  exit 1
fi
cargo fmt --all -- --check
cargo test --locked -p streams-quality-syntax
cargo build --locked -p streams-quality-syntax
# The formal-verification manifest names real harnesses, models and
# assumptions, and every claimed result has a structurally valid receipt:
# an invalid or missing receipt fails here. A stale receipt (inputs changed
# since it was recorded) is only reported: the formal CI job re-runs what a
# change affects, and scripts/release-gate.sh requires `check --fresh`
# (verification/README.md, "Three levels of enforcement").
python3 scripts/quality/formal.py check
# The source ratchet, architecture, scenario, inventory and evidence gates
# are pure Python over the tree: seconds, so they run before any compile.
for gate in architecture-report architecture-gate scenario-map-report test-inventory review-evidence; do
  python3 "scripts/$gate.py" --self-test
  if [[ "$gate" != architecture-report ]]; then python3 "scripts/$gate.py" --check; fi
done
python3 scripts/verify-rc-evidence.py --self-test --repo .
bash scripts/multitenancy-audit.sh
python3 -m unittest discover -s scripts/quality -v
python3 -m unittest discover -s scripts/effective-config -v
python3 -m unittest discover -s scripts/dev -v
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
# rustdoc never prunes its output: its search index grew about 2 MB per run
# and every run re-read all of it (488 s on a long-used checkout against 15 s
# from clean), so it always starts from an empty output directory.
doc_dir="${CARGO_TARGET_DIR:-target}/doc"
rm -rf -- "${doc_dir:?}"
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items
cargo machete
cargo deny --locked --workspace check
if ! wait "$release_build"; then
  echo "QUALITY_FAIL: release lib-test build; see $QUALITY_OUT/release-build.log" >&2
  tail -n 40 "$QUALITY_OUT/release-build.log" >&2
  exit 1
fi
scripts/test-leg.sh "$QUALITY_OUT/mt-lint.log" --exact mt_lint::multitenancy_identity_lint \
  -- --locked --release --lib multitenancy_identity_lint
printf 'QUALITY_OK\n'
