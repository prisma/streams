#!/bin/bash
# Sample aggregate accepted rec/s + live segment count every 15s.
OUT=$1; DUR=$2
END=$(( $(date +%s) + DUR ))
prev=""
while [ "$(date +%s)" -lt "$END" ]; do
  TOT=0
  for p in 8101 8102 8103; do
    T=$(curl -s --max-time 4 http://127.0.0.1:$p/v1/debug/usage 2>/dev/null | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except: print(0); raise SystemExit
print(sum(s["records"] for s in d["streams"]))' 2>/dev/null || echo 0)
    TOT=$((TOT + T))
  done
  SEGS=$(curl -s --max-time 4 http://127.0.0.1:8101/v1/debug/scaler 2>/dev/null | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except: print("?"); raise SystemExit
print(sum(len(e["segments"]) for e in d["ewmas"] if e["parent"]=="d2t"))' 2>/dev/null || echo "?")
  NOW=$(date +%s)
  if [ -n "$prev" ]; then
    RATE=$(( (TOT - prevtot) / (NOW - prev) ))
    echo "$(date +%T) accepted_rec_s=$RATE live_segs_on_8101=$SEGS total=$TOT" >> "$OUT"
  fi
  prev=$NOW; prevtot=$TOT
  sleep 15
done
