#!/bin/zsh
# L6 spike M3: negative controls. Each control breaks one mechanism in the
# committed tree with a perl substitution, runs the test that must catch it
# (dev profile, L6_SPILL=1), records the first failure, and restores the
# file with `git checkout`. Run on a clean, committed tree, alone:
#   zsh $L6/tools/m3-controls.sh > $L6/M3/controls.log 2>&1
# ONLY="C13 C2" runs just those controls. A control whose break does not
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
  out=$(L6_SPILL=1 nice -n 10 cargo test --locked --lib -- $filter 2>&1)
  local summary=$(print -r -- "$out" | grep -E '^test result' | tail -1)
  local first=$(print -r -- "$out" | grep -E "panicked at|^error" | head -1)
  local msg=$(print -r -- "$out" | grep -A3 "panicked at" | sed -n 2,4p | tr '\n' ' ' | cut -c1-400)
  echo "    result: ${summary:-no test result (build failed?)} ($((SECONDS - t0)) s)"
  echo "    first failure: $first"
  echo "    message: $msg"
  if [[ -z "$summary" ]]; then echo "    BUILD-FAILED"; print -r -- "$out" | grep -E '^error' -A3 | head -6 | sed 's/^/      /'
  elif print -r -- "$out" | grep -qE 'test result: ok'; then echo "    CONTROL-NOT-CAUGHT"; else echo "    CAUGHT"; fi
  git checkout -- $file
}

control "C1 no awaited WAL flush before the memtable flush (step 7a)" src/shard/spill.rs \
  'async fn spill_make_durable(&self) -> Result<(), SealError> {' 'async fn spill_make_durable(&self) -> Result<(), SealError> { if true { return Ok(()); }' \
  'shard::spill::tests::flushdrop'
control "C2 no page tombstones" src/shard/transaction/maintenance.rs \
  'for last in &chunk.page_lasts {' 'for last in chunk.page_lasts.iter().take(0) {' \
  'shard::spill::tests::flushdrop'
control "C3 the one-advance trim lag kept" src/shard/transaction/maintenance.rs \
  'f.trim_safe_to = f.absorbed;
            f.trimmed = f.absorbed;' 'f.trimmed = f.trim_safe_to;' \
  'shard::spill::tests::tiling'
control "C4 the side table is never consulted" src/shard/spill.rs \
  'Some(d) if d.upto == upto => side.remove(&(hash, from)),' 'Some(d) if d.upto == upto + 1 => side.remove(&(hash, from)),' \
  'shard::spill::tests::tiling'
control "C5 index row keyed by the chunk's first offset" src/shard/spill.rs \
  'index_key: index_key(&input.hash, chunk.entry.last),' 'index_key: index_key(&input.hash, chunk.entry.first),' \
  'shard::spill::tests::tiling'
control "C6 chunks not capped at max_pages_per_chunk" src/shard/spill.rs \
  'let max_pages = sp.settings.max_pages_per_chunk.max(1);' 'let max_pages = usize::MAX;' \
  'shard::spill::tests::tiling'
control "C7 the estimate counts raw key+value bytes only" src/spill/estimate.rs \
  'let entries_size = kv_bytes + ENTRY_META * entries;' 'let entries_size = kv_bytes;' \
  'spill::estimate_tests::the_memtable_estimate'
control "C8 the estimate omits the bloom filter" src/spill/estimate.rs \
  'let filter = if entries >= MIN_FILTER_KEYS {' 'let filter = if false {' \
  'spill::estimate_tests::the_memtable_estimate'
control "C9 the memtable trigger counts page bytes (the design's original)" src/shard/spill.rs \
  'TriggerMode::Memtable => (self.mem_estimate, Trigger::Memtable),' 'TriggerMode::Memtable => (self.pages, Trigger::Memtable),' \
  'shard::spill::tests::the_pages_control'
control "C10 the 5 s ticker still flushes on the spill arm" src/shard.rs \
  'if !ticker.l6_spill && appended != last_appended {' 'if appended != last_appended {' \
  'shard::spill::tests::immutable_flushes'
control "C11 deadlines wait for a running seal" src/shard/spill.rs \
  'if let Some(kind) = m.deadline(&sp.settings) {' 'if let Some(kind) = m.deadline(&sp.settings).filter(|_| !sp.running.load(Ordering::Acquire)) {' \
  'shard::spill::tests::deadlines'
control "C12 failed seals retry without backoff" src/shard/spill.rs \
  '*retry = if ok {' '*retry = if true {' \
  'shard::spill::tests::the_age'
control "C13 a PUT whose commit never lands is deleted (orphans uncounted)" src/shard/spill.rs \
  '        sp.check_abort(SealStep::AfterPut)?;' '        if sp.check_abort(SealStep::AfterPut).is_err() { let _ = object_store::ObjectStoreExt::delete(self.data_store.as_ref(), &object_store::path::Path::from(object::object_path(&crate::sharddir::shard_db_path(&self.prefix), id))).await; return Err(SealError::Aborted(SealStep::AfterPut)); }' \
  'shard::spill::tests::tiling'
echo "done $(date -u +%FT%TZ)"; uptime
git status --short
