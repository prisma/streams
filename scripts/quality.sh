#!/usr/bin/env bash
# Common local/CI entry. All output kept locally; no implicit evidence upload.
set -euo pipefail
cd "$(dirname "$0")/.."
QUALITY_OUT=${QUALITY_OUT:-target/quality}
mkdir -p "$QUALITY_OUT"
python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); import config; problems=config.check(); print("\n".join(problems)); sys.exit(bool(problems))'
cargo fmt --all -- --check
cargo test --locked -p streams-quality-syntax
cargo build --locked -p streams-quality-syntax
python3 -m unittest discover -s scripts/quality -v
cargo clippy --locked --workspace --all-targets --message-format=json > "$QUALITY_OUT/clippy.jsonl"
python3 scripts/quality/gate.py --clippy "$QUALITY_OUT/clippy.jsonl"
cargo machete
cargo deny --locked --workspace check
for gate in architecture-report architecture-gate scenario-map-report test-inventory review-evidence; do
  python3 "scripts/$gate.py" --self-test
  if [[ "$gate" != architecture-report ]]; then python3 "scripts/$gate.py" --check; fi
done
python3 scripts/verify-rc-evidence.py --self-test --repo .
bash scripts/multitenancy-audit.sh
cargo test --locked --release --lib multitenancy_identity_lint
printf 'QUALITY_OK\n'
