#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

fail=0

echo "Checking Result policy in src/ ..."

if rg -n --glob '*.ts' 'throw new Error\(' src; then
  echo
  echo "Result policy violation: use typed Result/Err or dsError, not throw new Error(...) in src/."
  fail=1
fi

if rg -n --glob '*.ts' '\.unwrap\(' src; then
  echo
  echo "Result policy violation: unwrap() is prohibited in src/ runtime paths."
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "Result policy checks passed."
