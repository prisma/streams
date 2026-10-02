#!/bin/bash
# K2 field runs: gen.sh <run-id> <cell> <plan> [--replace]
# Thin entry point; the logic and its documentation are in gen.py.
# Python 3.9+ with boto3 (/usr/bin/python3 on the owner's Mac has both);
# compute-cli calls hold $K2_FIELD_HOME/.cli.lock (bunx races its package cache).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
exec python3 "$HERE/gen.py" "$@"
