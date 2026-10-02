#!/bin/bash
# K2 field runs: deploy-cell.sh <run-id> <cell> [--kill N | --only N]
# Thin entry point; the logic and its documentation are in deploy.py.
# Python 3.9+ with boto3 (/usr/bin/python3 on the owner's Mac has both);
# compute-cli calls hold $K2_FIELD_HOME/.cli.lock (bunx races its package cache).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
exec python3 "$HERE/deploy.py" "$@"
