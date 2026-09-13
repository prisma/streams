#!/usr/bin/env bash
# Mutation selection/execution is owned by one typed Python table. The shell is
# intentionally only the stable local/CI entry point.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec python3 scripts/quality/mutation_driver.py \
  --out "${QUALITY_MUTANTS_OUT:-target/quality-mutations}"
