#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

fail=0

echo "Checking Result policy in src/ ..."

search() {
  local pattern="$1"
  if command -v rg >/dev/null 2>&1; then
    rg -n --glob '*.ts' "$pattern" src
  else
    grep -R -n -E --include='*.ts' "$pattern" src
  fi
}

if search 'throw new Error\('; then
  echo
  echo "Result policy violation: use typed Result/Err or dsError, not throw new Error(...) in src/."
  fail=1
fi

if search '\.unwrap\('; then
  echo
  echo "Result policy violation: unwrap() is prohibited in src/ runtime paths."
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "Result policy checks passed."
