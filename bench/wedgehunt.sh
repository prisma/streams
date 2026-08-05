#!/bin/bash
# #115: hunt the close-group liveness wedge. Recreates the observed
# trigger — the close-group family under heavy scheduler pressure — in
# a tight loop of parallel test-binary copies. A copy that exceeds
# MAX_SECS is killed and recorded as a HIT (the 2026-08-05 catch showed
# the wedge disables tokio timers, so in-test watchdogs cannot be
# relied on to fire); FAILED/panicked output is a hit too.
#
#   bench/wedgehunt.sh <test-binary> <logdir> [copies=6] [max_secs=120]
set -u
BIN=${1:?test binary}
LOG=${2:?log dir}
COPIES=${3:-6}
MAX_SECS=${4:-120}
mkdir -p "$LOG"
FILTER="a_failed_group a_fence_in_a_failed the_gather_window commit_gate a_failed_close close_"
i=0
hits=0
while :; do
  i=$((i+1))
  pids=()
  for c in $(seq 1 "$COPIES"); do
    RUST_BACKTRACE=1 "$BIN" --test-threads=8 $FILTER \
      > "$LOG/iter$i-copy$c.log" 2>&1 &
    pids+=($!)
  done
  waited=0
  while :; do
    alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && break
    if [ "$waited" -ge "$MAX_SECS" ]; then
      hits=$((hits+1))
      for idx in "${!pids[@]}"; do
        p=${pids[$idx]}
        if kill -0 "$p" 2>/dev/null; then
          c=$((idx+1))
          sample "$p" 3 -file "$LOG/HIT-$hits-iter$i-copy$c-sample.txt" 2>/dev/null
          cp "$LOG/iter$i-copy$c.log" "$LOG/HIT-$hits-iter$i-copy$c.log" 2>/dev/null
          kill -9 "$p" 2>/dev/null
        fi
      done
      echo "$(date '+%F %T') HIT $hits (timeout) at iter $i" >> "$LOG/hits.txt"
      break
    fi
    sleep 2; waited=$((waited+2))
  done
  wait 2>/dev/null
  if grep -lE "FAILED|panicked" "$LOG"/iter$i-copy*.log > /dev/null 2>&1; then
    hits=$((hits+1))
    cp $(grep -lE "FAILED|panicked" "$LOG"/iter$i-copy*.log) "$LOG/HIT-$hits-iter$i.log" 2>/dev/null
    echo "$(date '+%F %T') HIT $hits (failure) at iter $i" >> "$LOG/hits.txt"
  fi
  rm -f "$LOG"/iter$i-copy*.log
  echo "$(date '+%F %T') iter $i done (hits=$hits)" >> "$LOG/progress.txt"
done
