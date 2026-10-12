#!/bin/zsh
# L6 spike M4: the acceptance filter (spill arm, three runs; base arm once)
# and the whole lib suite on both arms, one after another, alone.
set -u
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
M4=/Users/sorenschmidt/.streams-k2/analysis/public-beta/l6/M4
cd $W
F=(dst::dst_tests::reads_history:: dst::dst_tests::reads_applied_history:: dst::dst_tests::reads_ring:: dst::dst_tests::read_history_lifecycle:: dst::dst_tests::history_recovery:: dst::dst_tests::history_absorption:: dst::dst_tests::livefeed_history:: dst::dst_tests::consumer_runs:: dst::dst_tests::durability_gather:: spill::)
run() { # label env... -- args
  local log=$1; shift
  { echo "HEAD $(git rev-parse HEAD) $(date -u +%FT%TZ)"; uptime; echo "cmd: $*"; "$@"; echo "exit $?"; date -u +%FT%TZ; uptime; } > $log 2>&1
  find target/debug/deps -name 'streams_slate-*.rcgu.o' -mmin +1 -delete 2>/dev/null
}
for i in 1 2 3; do run $M4/acceptance-spill-$i.log env L6_SPILL=1 DST_L6_TRACE=1 cargo test --locked --lib -- $F; done
run $M4/acceptance-base.log env -u L6_SPILL cargo test --locked --lib -- $F
run $M4/lib-suite-spill.log env L6_SPILL=1 cargo test --locked --lib
run $M4/lib-suite-base.log env -u L6_SPILL cargo test --locked --lib
echo SUITES-DONE
