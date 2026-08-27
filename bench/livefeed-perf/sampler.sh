#!/bin/sh
# In-container /proc sampler for the SERVER process: RSS/PSS, CPU
# seconds, fds, context switches every 10 s to $OUT/proc.jsonl. The
# cgroup view (accurate under --cpus/--memory) rides along when
# present. POSIX sh — the server container is minimal.
PID=$1
OUT=$2
while kill -0 "$PID" 2>/dev/null; do
  TS=$(date +%s%3N 2>/dev/null || date +%s000)
  RSS_KB=$(awk '/^VmRSS/{print $2}' "/proc/$PID/status" 2>/dev/null)
  PSS_KB=$(awk '/^Pss:/{print $2}' "/proc/$PID/smaps_rollup" 2>/dev/null)
  FDS=$(ls "/proc/$PID/fd" 2>/dev/null | wc -l | tr -d ' ')
  CSW=$(awk '/^voluntary_ctxt_switches/{v=$2} /^nonvoluntary_ctxt_switches/{n=$2} END{print v+n}' "/proc/$PID/status" 2>/dev/null)
  STAT=$(cat "/proc/$PID/stat" 2>/dev/null)
  UT=$(echo "$STAT" | awk '{print $14}')
  ST=$(echo "$STAT" | awk '{print $15}')
  THREADS=$(echo "$STAT" | awk '{print $20}')
  CG_MEM=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || echo "")
  CG_CPU=$(awk '/^usage_usec/{print $2}' /sys/fs/cgroup/cpu.stat 2>/dev/null || echo "")
  echo "{\"t\":$TS,\"rss_kb\":${RSS_KB:-0},\"pss_kb\":${PSS_KB:-0},\"fds\":${FDS:-0},\"csw\":${CSW:-0},\"utime\":${UT:-0},\"stime\":${ST:-0},\"threads\":${THREADS:-0},\"cg_mem\":\"${CG_MEM}\",\"cg_cpu_usec\":\"${CG_CPU}\"}" >> "$OUT"
  sleep 10
done
