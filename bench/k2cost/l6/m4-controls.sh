#!/bin/zsh
# L6 spike M4: negative controls for the read path. Each control breaks one
# mechanism in the committed tree with a perl substitution, runs the test
# that must catch it (dev profile, L6_SPILL=1), records the first failure,
# and restores the file with `git checkout`. Run on a clean, committed
# tree, alone:
#   zsh $L6/tools/m4-controls.sh > $L6/M4/controls.log 2>&1
# ONLY="C1 C3" runs just those controls. A control whose break does not
# build is reported BUILD-FAILED, never CAUGHT.
set -u
ONLY=${ONLY:-}
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
cd $W
if [[ -n "$(git status --porcelain)" ]]; then echo "dirty tree; refusing"; exit 2; fi
echo "controls at $(git rev-parse HEAD) $(date -u +%FT%TZ)"; uptime

control() {
  local name=$1 file=$2 from=$3 to=$4 filter=$5
  if [[ -n "$ONLY" && " $ONLY " != *" ${name%% *} "* ]]; then return; fi
  echo "=== $name"
  echo "    break: $file: $from  ->  $to"
  FROM="$from" TO="$to" perl -0pi -e 's/\Q$ENV{FROM}\E/$ENV{TO}/ or die "pattern not found\n"' $file || { echo "    PATTERN-NOT-FOUND"; git checkout -- $file; return; }
  local out t0=$SECONDS
  out=$(L6_SPILL=1 nice -n 10 cargo test --locked --lib -- ${=filter} 2>&1)
  local summary=$(print -r -- "$out" | grep -E '^test result' | tail -1)
  local first=$(print -r -- "$out" | grep -E "panicked at|^error" | head -1)
  local msg=$(print -r -- "$out" | grep -A3 "panicked at" | sed -n 2,4p | tr '\n' ' ' | cut -c1-400)
  echo "    result: ${summary:-no test result (build failed?)} ($((SECONDS - t0)) s)"
  echo "    first failure: $first"
  echo "    message: $msg"
  if [[ -z "$summary" ]]; then echo "    BUILD-FAILED"; print -r -- "$out" | grep -E '^error' -A3 | head -6 | sed 's/^/      /'
  elif print -r -- "$out" | grep -qE 'test result: ok'; then echo "    CONTROL-NOT-CAUGHT"; else echo "    CAUGHT"; fi
  git checkout -- $file
  # each dev rebuild leaves ~1.5 GB of .rcgu.o hard links behind (M3 NOTES §7)
  find target/debug/deps -name "streams_slate-*.rcgu.o" -delete 2>/dev/null
}

R=src/shard/spill/read.rs
control "C1 index rows scanned at Remote whatever the read's visibility" $R \
  'Deliver::Applied => DurabilityLevel::Memory,' 'Deliver::Applied => DurabilityLevel::Remote,' \
  'shard::spill::read_tests::dst_a_zero_stale'
control "C2 a missing row below the boundary is an error, not an honest partial" $R \
  '                // visibility would see (rBc G2-V). Never consumed.
                return Ok((plans, Stop::MissingRow));' '                return Err(SpillReadError::Store("expired".into()));' \
  'shard::spill::read_tests::dst_negative_control shard::spill::read_tests::missing_rows'
control "C3 a missing row is consumed as expired" $R \
  '        out.completed = out.stop == Stop::Reached && due >= upto;' '        if out.stop == Stop::MissingRow { out.last = Some(upto - 1); out.stop = Stop::Reached; due = upto; }
        out.completed = out.stop == Stop::Reached && due >= upto;' \
  'shard::spill::read_tests::dst_negative_control'
control "C4 a 404 with a live row is not retryable" $R \
  '                    return Err(SpillReadError::Retryable(format!(' '                    return Err(SpillReadError::Store(format!(' \
  'shard::spill::read_tests::missing_rows'
control "C5 no cap on GETs in flight" $R \
  'stream::iter(gets).buffered(MAX_IN_FLIGHT)' 'stream::iter(gets).buffered(1 << 20)' \
  'shard::spill::read_tests::a_read_has_at_most'
control "C6 pages not clipped to the window" $R \
  'let Some(slice) = PageSlice::clip(page, from, upto) else {' 'let Some(slice) = PageSlice::clip(page, 0, u64::MAX) else {' \
  'shard::spill::read_tests::sealed_ranges'
control "C7 the start anchor ignored (a read GETs from the chunk's first page)" $R \
  '    let (page, first, start) = row
        .start_for(due)' '    let (page, first, start) = None::<crate::spill::object::Anchor>' \
  'shard::spill::read_tests::a_read_inside'
control "C8 the default lane's filter is off" $R \
  'if lane.is_none_or(|key| key == slice.page().routing_key()) {' 'if true {' \
  'shard::spill::read_tests::the_default_lane'
control "C9 keyed history reads are not refused" src/application/read.rs \
  'Some(rk) if !rk.is_empty() => return Err(crate::spill::keyed_unsupported(plan.engine)),' 'Some(rk) if rk.len() > usize::MAX - 1 => return Err(crate::spill::keyed_unsupported(plan.engine)),' \
  'shard::spill::read_tests::the_default_lane'
control "C10 the one-advance trim lag kept (HIS-013 re-targeted)" src/shard/transaction/maintenance.rs \
  'f.trim_safe_to = f.absorbed;
            f.trimmed = f.absorbed;' 'f.trimmed = f.trim_safe_to;' \
  'dst::dst_tests::reads_ring::a_duplicate'
control "C11 the fixture's absorber runs on the spill arm" src/history/test_support.rs \
  'if shard.spill_adopt_absorber_cadence(' 'if false && shard.spill_adopt_absorber_cadence(' \
  'shard::spill::read_tests::the_dst_fixture'
control "C12 the startup seed starts no age clock" src/shard/spill.rs \
  '                        if unabsorbed {
                            sp.note_unabsorbed();' '                        if false {
                            sp.note_unabsorbed();' \
  'dst::dst_tests::history_recovery::sparse_records'
control "C13 the startup seed is not retried" src/shard/spill.rs \
  '                        seed_at = Instant::now() + seed_wait;' '                        seeded = true;' \
  'dst::dst_tests::history_recovery::dirty_scan_retries'
echo "done $(date -u +%FT%TZ)"; uptime
git status --short
