#!/bin/bash
# L6 spike: host-load sampler (every 30 s) while the M1 queue runs: load
# averages, free memory, and which other heavy legs are running (q0 rigs,
# gates, suites, mutations, other rig points), to logs/hostload.jsonl.
# Stops by itself once the queue log says QUEUE DONE.
#   hostload.sh [QUEUE_LOG]   (default logs/m1-queue.log)
L6=/Users/sorenschmidt/.streams-k2/analysis/public-beta/l6
OUT=$L6/logs/hostload.jsonl
QLOG=${1:-$L6/logs/m1-queue.log}
while :; do
  t=$(date +%s)
  load=$(sysctl -n vm.loadavg | tr -d '{}' | awk '{print $1","$2","$3}')
  free=$(vm_stat | awk '/Pages free/ {gsub("\\.","",$3); print $3}')
  others=$(pgrep -fl 'q0-rss|gate\.sh|quality\.sh|cargo-mutants|mutations\.sh|formal_batch|cargo (test|build|clippy)' | grep -v pgrep | awk '{print $2"/"$3}' | sort | uniq -c | awk '{print $2"x"$1}' | tr '\n' ' ')
  l6=$(pgrep -f 'run-local\.sh' >/dev/null && echo 1 || echo 0)
  printf '{"t":%s,"load":[%s],"free_pages":%s,"l6_point_running":%s,"others":"%s"}\n' "$t" "$load" "${free:-0}" "$l6" "$others" >> "$OUT"
  grep -q 'QUEUE DONE' "$QLOG" 2>/dev/null && exit 0
  sleep 30
done
