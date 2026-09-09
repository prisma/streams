#!/usr/bin/env bash
# Pinned source builds, with upstream lockfiles. Add target/quality-tools/bin to PATH.
set -euo pipefail
cd "$(dirname "$0")/.."
TOOL_ROOT="$(pwd)/target/quality-tools"
VERSIONS=$(mktemp)
trap 'rm -f "$VERSIONS"' EXIT
python3 -c 'import tomllib; p=tomllib.load(open("quality-tools.toml","rb")); [print(k,v) for k,v in p["tools"].items()]' > "$VERSIONS"
while read -r tool version; do
  cargo install "$tool" --version "=$version" --locked --root "$TOOL_ROOT"
done < "$VERSIONS"
python3 scripts/install-actionlint.py
printf 'Installed pinned tools in %s/bin\n' "$TOOL_ROOT"
