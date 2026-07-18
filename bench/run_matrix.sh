#!/bin/bash
# Benchmark matrix: append latency/throughput at several concurrency levels,
# read replay, and durability lag — against both servers via s3lite (25ms).
# Usage: run_matrix.sh <results-dir> [run-tag]
set -u
RESULTS=${1:?results dir}
TAG=${2:-r$(date +%H%M%S)}
BENCH=/Users/sorenschmidt/code/streams/slate/target/release/bench
S3STATS=http://127.0.0.1:9500/_s3lite/stats
mkdir -p "$RESULTS"

snap() { curl -s -m 5 "$S3STATS"; }

run_append() {
  local label=$1 url=$2 conc=$3 prefix=$4
  local pre post
  pre=$(snap)
  $BENCH --url "$url" --mode append --concurrency "$conc" --streams 16 \
    --payload-bytes 256 --entries 1 --duration-secs 15 --warmup-secs 3 \
    --prefix "$prefix" --label "$label-c$conc" --json \
    >> "$RESULTS/append.jsonl" 2>> "$RESULTS/errors.log"
  post=$(snap)
  echo "{\"label\":\"$label-c$conc\",\"pre\":$pre,\"post\":$post}" >> "$RESULTS/s3stats.jsonl"
  echo "done: $label conc=$conc"
}

for conc in 1 16 64 256; do
  run_append slate http://127.0.0.1:8090 "$conc" "sl-$TAG-c$conc"
done
for conc in 1 16 64 256; do
  run_append old http://127.0.0.1:8081 "$conc" "old-$TAG-c$conc"
done

# Read replay of the concurrency-64 data set
$BENCH --url http://127.0.0.1:8090 --mode read --streams 16 --prefix "sl-$TAG-c64" \
  --label slate-read > "$RESULTS/read-slate.json" 2>> "$RESULTS/errors.log"
$BENCH --url http://127.0.0.1:8081 --mode read --streams 16 --prefix "old-$TAG-c64" \
  --label old-read > "$RESULTS/read-old.json" 2>> "$RESULTS/errors.log"

# Durability lag
$BENCH --url http://127.0.0.1:8090 --mode durability --prefix "sl-$TAG" \
  > "$RESULTS/durability-slate.txt" 2>&1
$BENCH --url http://127.0.0.1:8081 --mode durability --prefix "old-$TAG" \
  > "$RESULTS/durability-old.txt" 2>&1

echo "MATRIX COMPLETE"
