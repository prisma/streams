#!/bin/zsh
# L6 spike M2: negative controls. Each control breaks one mechanism in the
# committed tree with a one-line perl substitution, runs the test that must
# catch it (dev profile, L6_SPILL=1), records the first failure line, and
# restores the file with `git checkout`. Run on a clean, committed tree:
#   zsh $L6/M2/controls.sh > $L6/M2/controls.log 2>&1
set -u
W=/Users/sorenschmidt/.streams-k2/wt/l6-spike
cd $W
if [[ -n "$(git status --porcelain)" ]]; then echo "dirty tree; refusing"; exit 2; fi
echo "controls at $(git rev-parse HEAD) $(date -u +%FT%TZ)"; uptime

control() {
  local name=$1 file=$2 from=$3 to=$4 filter=$5
  echo "=== $name"
  echo "    break: $file: $from  ->  $to"
  FROM="$from" TO="$to" perl -0pi -e 's/\Q$ENV{FROM}\E/$ENV{TO}/ or die "pattern not found\n"' $file || { echo "    PATTERN-NOT-FOUND"; git checkout -- $file; return; }
  local out
  out=$(L6_SPILL=1 nice -n 10 cargo test --locked --lib -- $filter 2>&1)
  local summary=$(print -r -- "$out" | grep -E '^test result' | tail -1)
  local first=$(print -r -- "$out" | grep -E "panicked at|^error" | head -1)
  local msg=$(print -r -- "$out" | grep -A3 "panicked at" | sed -n 2,4p | tr '\n' ' ')
  echo "    result: ${summary:-no test result (build failed?)}"
  echo "    first failure: $first"
  echo "    message: $msg"
  if print -r -- "$out" | grep -qE 'test result: ok'; then echo "    CONTROL-NOT-CAUGHT"; else echo "    CAUGHT"; fi
  git checkout -- $file
}

control "C1 ring evicts unabsorbed batches" src/shard/tail_ring.rs \
  'Some(b) if b.next <= absorbed => {' 'Some(_) => {' \
  'spill_ring_'
control "C2 no shard cap (one shard takes the process budget)" src/shard/tail_ring.rs \
  's.shard_cap_bytes
            .min(s.process_bytes / shards)' 's.process_bytes' \
  'spill_ring_share'
control "C3 no floor" src/shard/tail_ring.rs \
  '.max(s.shard_floor_bytes.min(s.shard_cap_bytes))' '' \
  'spill_ring_share'
control "C4 spill arm copies pages (put, not put_bytes)" src/shard/transaction/append.rs \
  'self.batch.put_bytes(Bytes::from(key), bytes);' 'self.batch.put(key, bytes);' \
  'spill_pages_share'
control "C5 decoder skips the chunk CRC" src/spill/object.rs \
  'if crc32c(&chunk[..body_len]) != stored_crc {' 'if false && crc32c(&chunk[..body_len]) != stored_crc {' \
  'every_single_bit_flip'
control "C6 decoder skips the trailer CRC" src/spill/object.rs \
  'if crc32c(&trailer[..20]) != stored_trailer_crc {' 'if false && crc32c(&trailer[..20]) != stored_trailer_crc {' \
  'every_single_bit_flip'
control "C7 chunk last taken from the first page" src/spill/object.rs \
  'let last = chunk.pages.last().map_or' 'let last = chunk.pages.first().map_or' \
  'index_rows_of_random_consecutive_seals'
control "C8 anchor byte omits the directory" src/spill/object.rs \
  'byte: (CHUNK_HEADER_LEN + dir_len + at as usize) as u32,' 'byte: (CHUNK_HEADER_LEN + at as usize) as u32,' \
  'anchors_mark'
control "C9 slicing-by-16 table order swapped" src/spill/crc32c.rs \
  '^ t(14, a >> 8)
            ^ t(13, a >> 16)' '^ t(13, a >> 8)
            ^ t(14, a >> 16)' \
  'spill::crc32c_tests::'
control "C10 index key tag collides with the page tag" src/spill/index.rs \
  "pub(crate) const INDEX_TAG: u8 = b'i';" "pub(crate) const INDEX_TAG: u8 = b'p';" \
  'spill::index_tests::'
echo "done $(date -u +%FT%TZ)"; uptime
git status --short
