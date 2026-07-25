#!/bin/bash
# create the scaling stream on the ladder fleet + seed desired.json
set -e
S=$(dirname "$0")
K=$(cat "$S/key.txt")
STREAM=${1:-d1s}
# seed fleet desired count (pre-server ideally; harmless later)
curl -s -X PUT "http://127.0.0.1:9500/ladder/ladder-fleet/fleet/desired.json" -d '{"count":3}' -o /dev/null -w "desired.json %{http_code}\n"
curl -s -X PUT "http://127.0.0.1:8101/v1/stream/$STREAM" \
  -H "Stream-Encryption-Key: $K" -H "Stream-Scaling: auto" \
  -H "Content-Type: application/json" -o /dev/null -w "create $STREAM %{http_code}\n"
