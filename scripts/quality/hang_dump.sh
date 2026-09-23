#!/usr/bin/env bash
# Evidence for a hung test binary (follow-up F-G): a DST test has twice run
# past libtest's 60 s notice and never finished while every other test of the
# suite passed, and the job burned its whole 60-minute deadline with no clue
# where it was stuck. Run this beside the suite: once a process matching
# PATTERN has lived BUDGET seconds, every thread's stack is printed and the
# process is killed, so the step fails in minutes with the stuck frames in the
# log. A suite that finishes first leaves nothing to dump; the caller kills
# this watcher when its step ends.
#
# Usage: scripts/quality/hang_dump.sh PATTERN BUDGET_SECONDS
set -u
pattern="$1"
budget="$2"
while true; do
  # Anchored on the executable (the command line's first word), so a
  # shell or this watcher that merely names the pattern never matches.
  pid=$(pgrep -f "^[^ ]*${pattern}( |$)" | head -1)
  if [ -n "$pid" ]; then
    age=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')
    if [ -n "$age" ] && [ "$age" -gt "$budget" ]; then
      echo "::error::$pattern (pid $pid) still running after ${age}s; every thread's stack follows"
      command -v gdb >/dev/null || sudo apt-get install -y -qq gdb >/dev/null 2>&1
      sudo gdb -p "$pid" -batch -ex 'set pagination off' -ex 'thread apply all bt' 2>&1 | head -4000
      kill -9 "$pid"
      exit 0
    fi
  fi
  sleep 10
done
