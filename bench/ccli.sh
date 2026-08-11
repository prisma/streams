#!/bin/bash
# compute-cli wrapper that always carries the platform token. Without
# PRISMA_API_TOKEN the CLI answers "Resource Not Found" / "No compute
# services found" for resources that plainly exist — during the run-1
# fleet teardown that cost 30 s inside the kill window and nearly turned
# a clean stop into a mid-flight one. Route every campaign-script CLI
# call through here.
#
#   bench/ccli.sh services list --project <proj>
set -euo pipefail
S=${SOAK_HOME:?set SOAK_HOME}
if [ -s "$S/platform-token.txt" ]; then
  export PRISMA_API_TOKEN=$(cat "$S/platform-token.txt")
fi
exec bunx --bun @prisma/compute-cli@0.39.0 "$@"
