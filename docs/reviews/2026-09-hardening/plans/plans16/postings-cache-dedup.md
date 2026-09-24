# Item 75: one owner each for PostingsCache removal, eviction, coverage and the uncached fallback

Tree: `slate` = `origin/slate` = `24c4c77a`, clean. The plan is read-only work: nothing was edited, built or run.
All line numbers below are from `src/postings_cache.rs` at `24c4c77a` (950 lines) unless another file is named.

Summary. All four of the reviewer's claims hold on the current tree. Three parts of the reviewer's framing are stale:
- The file has 950 lines, not 913.
- Rank 1 has already landed (`d2e87129` + `3dfb970f`), so "land before rank 1" no longer applies.
- The cited eviction site in `spawn_load` (`:705-728`) now lives in `PostingsCache::finish_load` (`:750-776`).

The Change can be built as written, with two adjustments:
1. `Inner::effective_to` takes the read window, `(key, from, upto) -> Option<u64>`. It answers "how far this entry proves this read, if it proves all of it". That way the coverage predicate also exists once, and every new operator sits in one helper that tests can reach. Callers in `runs_for` change no operator-bearing line.
2. The single uncached fallback is the existing tail of `runs_for`, reached by `break`. It is not a new `uncached()` function, which would need an eight-argument `too_many_arguments` exception.

The work is three commits:
- C1: pins only, green on the old code.
- C2: `Inner::remove` + `Inner::evict_to_budget`, red-first on a scan-count witness.
- C3: `Inner::effective_to` + one fallback. This is a pure refactor.

No contract changes. No decisions for Søren.

---

## 1. Problem (verified on 24c4c77a)

### 1.1 Removal and its poisoning are repeated at every removal site (real)

`SegWarm.clean` guards the absence proof (`:85-105`: "AND none of this segment's entries were evicted (an evicted entry's key could re-appear and falsely claim its pre-eviction history was empty)"). Each removal site has to remember to poison it by hand:

- Budget eviction in `install_chunk` (`:449-468`):
  ```rust
  while g.total_bytes > self.max_bytes && g.slices.len() > 1 {
      let victim = g
          .slices
          .iter()
          .min_by_key(|(_, e)| e.last_used)
          .map(|(k, _)| *k);
      match victim {
          Some(v) => {
              if let Some(e) = g.slices.remove(&v) {
                  g.total_bytes -= e.slice.decoded_bytes;
                  self.evictions.fetch_add(1, Ordering::Relaxed);
              }
              if let Some(w) = g.warm.get_mut(&v.0) {
                  w.clean = false;
              }
          }
          None => break,
      }
  }
  ```
- Budget eviction in `finish_load` (`:758-775`). The same body, but the victim filter excludes the just-published key (`.filter(|(k, _)| **k != key)`) and there is no `len() > 1` floor. It has its own `-=`, `fetch_add` and `w.clean = false` at `:768-774`.
- Idle sweep in `sweep_idle` (`:871-879`). The same three steps again:
  ```rust
  for k in dead {
      if let Some(e) = g.slices.remove(&k) {
          g.total_bytes -= e.slice.decoded_bytes;
          self.evictions.fetch_add(1, Ordering::Relaxed);
      }
      if let Some(w) = g.warm.get_mut(&k.0) {
          w.clean = false;
      }
  }
  ```
- Invalid-install purge in `install_chunk` (`:307-317`). This is the fourth removal. It deletes the warm record outright (`g.warm.remove(&inc.0)`) and then repeats `slices.remove` + `total_bytes -=` (`:311-315`). It does not count evictions, and the refactor keeps it that way.

`w.clean = false` appears 4 times (`:400`, `:463`, `:773`, `:877`). The one at `:400` is the failed-seam poison in the extend arm. It poisons without removing anything, so it is not a removal site (see §8). `total_bytes` is written at `:173` (publish), `:313`, `:414`, `:437`, `:459`, `:769` and `:873`.

### 1.2 Eviction is O(n) per victim under the process-wide lock (real)

Both eviction loops rescan the whole map for every victim (`:451-455` and `:759-764`: `.iter()…min_by_key(|(_, e)| e.last_used)`), so a pass that frees k entries does k·n work. The lock is process-wide in production:
- `runtime.rs:190` builds one cache from `config.postings.cache_bytes`.
- `bootstrap.rs:527` hands it to every engine as `shared_postings_cache: Some(shared_postings)`.
- `shard.rs:1441` uses it.

One `install_chunk` extends every key in an absorbed chunk, so once the cache sits at its budget, each chunk evicts roughly (bytes added ÷ average entry) victims, and each of them costs a full scan while every engine's readers and absorbers wait.

### 1.3 The warm-bridge coverage is computed twice (real)

- In the lookup decision (`:543-564`):
  ```rust
  let warm_to = g.warm.get(&key.0).filter(|w| w.clean).map(|w| (w.from, w.to));
  let covered = g.slices.get_mut(&key).and_then(|e| {
      let mut effective_to = e.slice.indexed_to_offset;
      if let Some((wf, wt)) = warm_to && wf <= effective_to && wt > effective_to {
          effective_to = wt;
      }
      if e.slice.covered_from <= from && upto <= effective_to {
  ```
- In the post-wake check (`:637-662`), the same bridge is recomputed as `eff`, followed by the same predicate `e.slice.covered_from <= from && upto <= eff` (`:656`).

### 1.4 The uncached fallback is written three times (real)

The Bypass arm (`:590-601`), the "finished-but-short" branch (`:666-677`) and the trailing "Persistent contention" block (`:679-689`) are identical:
```rust
self.misses.fetch_add(1, Ordering::Relaxed);
let (runs, _enc, pt, corrupt) =
    load_runs(self, part, route, inc, kh, want_bucket, upto).await?;
if corrupt {
    return Ok(CacheRuns::Corrupt);
}
return Ok(CacheRuns::Runs { runs: RunWindow::new(runs, from, pt), provable_to: pt });
```

### 1.5 Complete use sites (production, cfg(test), DST, tools, fuzz, bench)

Everything the refactor touches is private to `src/postings_cache.rs`: `Inner`, `Entry`, `SegWarm`, `finish_load`, the eviction loops, the runs_for internals and the `load_runs` fallback. The entry points other files call keep their signatures:

| Caller | Uses |
|---|---|
| `src/runtime.rs:157,190,243-244` | `PostingsCache::new`, `POSTINGS_CACHE_BYTES` |
| `src/shard.rs:1037,1042,1073-1074,1275,1441-1442` | `new`, shared cache field |
| `src/shard.rs:1720-1721` | `sweep_idle(POSTINGS_CACHE_IDLE)` (return value discarded) |
| `src/bootstrap.rs:527` | shares one cache across engines |
| `src/history.rs:977-1000` | `runs_for`, `CacheRuns::{Runs, Corrupt}` |
| `src/history/gather.rs:657-658` | `install_chunk` |
| `src/application/read.rs:731` | passes `&plan.engine.postings_cache` |
| `src/http.rs:976` | `stats()` (shape unchanged) |
| `src/history/postings_validation_tests.rs:66,272-278` | `new`, `runs_for` (cfg(test)) |
| `src/dst/tests/reads_history.rs:229,347,446,468,541-542`, `reads_raw.rs:528`, `fixture_http.rs:434-440` | `sweep_idle(ZERO)`, counters, shared cache |
| `src/postings_cache/owned_load.rs:32` | `load_runs` (untouched) |
| `src/postings_cache/{tests,admission_tests,straddle_tests}.rs` | reach into `inner` (`total_bytes`, `slices`, `warm`), `publish_load`, `load_runs` |

`git grep` outside `src/` (tools/, fuzz/, benches, tests/) finds only docs. `docs/MULTITENANCY-MAP.md:91` cites stale line numbers and needs no change.

---

## 2. Contract decision

**Nothing observable changes.** Everything below stays identical:
- **Victim order.** Least recent first. Recency ties go to the entry the map yields first, because `min_by_key` returns the first minimum and `HashMap::remove` never reorders the entries that remain.
- **Victim set.** An install keeps the last entry standing. A load publish never takes the entry it published and may take every other entry.
- **Poisoning.** Every victim and every swept entry poisons its segment. The invalid-install purge deletes the warm record.
- **Eviction counter.** Counts each budget victim and each swept entry. It does not count the purge.
- **Lookup.** The Hit/Bypass/Wait/Lead decisions, the miss/hit/coalesced/index_loads counts, and every `CacheRuns` answer are unchanged.
- **Surface.** `stats()` and `/v1/debug` keep their shape.

Two internal differences:
- **Cost.** An over-budget pass now costs O(n) to build a heap in one scan, plus O(log n) per victim, instead of O(n) per victim. The heap is a transient allocation of about 56 bytes per candidate: `Reverse<(Instant, usize, Key)>`. Taking the largest numbers in this plan (the 64 MiB default budget and the 176-byte smallest entry), that peaks near 20 MB. It is allocated only on an over-budget pass and dropped before the guard is released. A pass under budget returns before scanning, exactly like today's `while` guard.
- **Counter timing.** `install_chunk` adds its eviction count once, after `drop(g)`, next to `warm_installs` and `warm_extends`. Today it adds per victim under the lock. The total when the call returns is the same.

Pruning the heap to its needed victims was considered and rejected; see §8.

---

## 3. Tests: pins first (C1), one red (C2), non-vacuity controls

### 3.1 Pins committed in C1 (all pass on the unchanged production code)

**New file `src/postings_cache/eviction_tests.rs`** (`#![cfg(test)]`, `use super::*; use std::collections::HashSet;`), about 190 lines.

Helpers:
- `run(start, count)`.
- `runs_past_100(n)`: `n` one-offset runs at `100, 102, …`.
- `key(segment: u8, id: usize) -> Key`.
- `resident(cache, key, bytes, last_used)`: hand-inserts an `Entry` for `[0, 100)` with `ValidatedRuns::empty()` and `decoded_bytes = bytes`, adds `bytes` to `total_bytes`, and `or_insert`s a clean warm record `{from: 0, to: 100, clean: true, admitted_all: true}`.
- `grow_to(cache, key, n, load: bool)`: grows a resident key to `n` runs past 100.
  - With `load`: `cache.finish_load(key, 0, Some((ValidatedRuns::new(runs_past_100(n)).unwrap(), 101 + 2n)))`.
  - Otherwise: `cache.install_chunk(SegmentHash(key.0), 100, 101 + 2n, vec![(key.1, runs_past_100(n))])`.
- `eviction_view(cache, grown, grown_bytes, newest) -> (Vec<(Key, Instant, usize)>, usize)`: calls `g.slices.reserve(1)` first. hashbrown reserves one slot before it replaces an existing key, so without this the grown entry's re-insert could rehash and change the map order that breaks ties. It then records the map order, with the grown entry at `(newest, grown_bytes)`.
- `reference_victims(order, total, max, spare) -> Vec<Key>`: the old algorithm run over the snapshot `Vec`, as the oracle:
  - Condition: `while total > max && order.len() > 1`.
  - Victim: first-minimum `last_used` among entries other than `spare`, removed with `Vec::remove`, which preserves order.

  For the load path, stopping at `len() > 1` is equivalent to the old `None => break`: with the published entry resident, one entry left means every other candidate is gone.

Tests:

1. **`quality_eviction_takes_the_least_recent_first_and_keeps_the_books`**. A `proptest!` with `Config::with_cases(1024)`; the `quality_` prefix puts it in rust-quality's `--lib quality_` leg.
   - Inputs:
     - `entries in vec((0u8..4, 176usize..120_000, 1u64..5), 2..48)`: segment, weight, and age in ms. Four ages produce many recency ties.
     - `grown in 0usize..8_000`.
     - `load in bool::ANY`.
   - Body: calls `evicts_like_a_minimum_scan(&entries, grown, load)`:
     - Builds residents at `base - age ms` in `PostingsCache::new(1)` (the 1 MiB floor).
     - Takes the view: the grown entry is `key(entries[0].0, 0)`, at `newest = base + 1h`.
     - Grows the entry by install or by load.
   - Asserts:
     - The resident set equals view − `reference_victims(view, total, max_bytes, load.then_some(grow))`: "evicted other than least recent first in map order".
     - `total_bytes == Σ decoded_bytes`: "the resident weight is the sum of the resident entries".
     - `total_bytes <= max_bytes || slices.len() == 1`.
     - `evictions == victims.len()`.
     - For every warm record, `clean == !victims.any(same segment)`: "a victim's segment keeps its absence proof, or a bystander loses it".
2. **`a_lone_slice_larger_than_the_budget_stays_resident`**
   - Setup: `resident(key(5, 0), 176, base)`, then `grow_to(.., 40_000, false)`.
   - Expected: `slices.len() == 1`, `total_bytes == 40_000*32 + 176 == 1_280_176 > max_bytes`, `evictions == 0`.
3. **`a_load_publish_evicts_until_the_budget_holds_and_no_further`**
   - Setup (budget 1,048,576):
     - `oldest` = 1,000 B at base−3 ms.
     - `older` = 1,048,400 B at base−2 ms.
     - `loaded` = 176 B at base−1 ms, all in segment `[1; 16]`.
     - Then `grow_to(loaded, 0, true)`, which publishes an empty extension. The weight stays 176, so the total is `max + 1_000`.
   - Expected:
     - `total_bytes == max_bytes`: "landed exactly on the budget".
     - `oldest` is gone.
     - `older` is resident: "evicted past the budget".
     - `loaded` is resident.
     - `evictions == 1`.
     - `!warm[[1;16]].clean`.

**Appended to `src/postings_cache/tests.rs`** (read-path pins for C3), about 65 lines, reusing `mem_db`, `ids`, `run` and `runs_of`:

4. **`a_short_owned_load_answers_its_reader_directly`**
   - Read: `ids(17)`, empty store, `runs_for(.., 0, upto, upto)` with `window = LOAD_MAX_BUCKETS*BUCKET_OFFSETS = 4_194_304` and `upto = window + BUCKET_OFFSETS = 4_259_840`.
   - Expected:
     - `provable_to == 4_194_304`: "the reader must answer with the direct load's proof".
     - No runs.
     - `debug_slice == Some((0, 4_194_304, 0))`.
     - `hits == 0`, `misses == 2`, `index_loads == 2`: "one owned load, one direct load".
   - What it pins: the post-wake "published but short" branch goes straight to the uncached tail. It does not lead a second load.
5. **`the_clean_warm_window_proves_an_absent_key_to_its_frontier`**
   - Setup: `ids(18)`; install `[0,100)` with `kh: run(5,3)`, then `[100,200)` with only `other = ids(19).2`.
   - Phase 1: `runs_of(18, 0, 200) == [run(5,3)]`, `index_loads == 0` ("bridged by the clean window"), `debug_slice == Some((0, 100, 1))` ("a bridge is a proof, not an extension").
   - Phase 2: set `inner.warm[inc].clean = false`. The same read returns `[run(5,3)]` with `index_loads == 1` ("a dirty window must not bridge").
6. **`a_warm_window_that_restarted_past_a_key_proves_nothing_for_it`**
   - Setup: `ids(22)`; install `[0,100)` with `kh: run(5,3)`, then `[150,250)` with only `other = ids(25).2`. This is a gap, so the window resets to `from 150`.
   - Expected: `runs_of(22, 0, 250) == [run(5,3)]` and `index_loads == 1` ("the window starts past the key's frontier").

After C1, `cargo test --lib postings_cache::` runs 24 tests: the 18 already there plus 6 new.

### 3.2 Red test (C2): `an_eviction_pass_scans_the_map_once` in `eviction_tests.rs`

- Setup:
  - 4,096 residents `key(2, id)` of 208 B at base−2 ms, plus `grow = key(3, 0)` of 176 B.
  - `grow_to(grow, 8_192, false)`. The total goes from 852,144 to 1,114,288, which is 65,712 over budget, so ⌈65,712/208⌉ = 316 victims.
- Asserts:
  - `evictions == 316`.
  - `total_bytes <= max_bytes`.
  - `inner.eviction_scans == 1`: "one pass, one scan".
- The witness is a new `#[cfg(test)] eviction_scans: u64` on `Inner`, incremented once per over-budget pass.
- Red procedure: stage on the C1 tree, uncommitted.
  - Add the field and its `#[cfg(test)] eviction_scans: 0` initialiser.
  - Put `#[cfg(test)] g.eviction_scans += 1;` as the first statement of both old `while` bodies (`:450` and `:758`).
  - Add the test.
  - Expected red: `assertion `left == right` failed: one pass, one scan  left: 316 right: 1`. `evictions == 316` passes on both trees, which proves the scenario matches.
- This departs from the reviewer's First step in two ways. It counts map scans, not "visits": that is exact and needs no n·log n bound. It uses 4k entries, not 100k, because the red is proven by count, not by time, so the red run is fast.

### 3.3 Non-vacuity controls (temporary edits, never committed)

On the **C1 tree** (old production code), each pin must be able to fail:

| Id | Deliberate break | Must fail |
|---|---|---|
| NV-A | `:454` and `:763` `.min_by_key(` → `.max_by_key(` | `quality_eviction_…` ("evicted other than least recent first in map order") |
| NV-B | delete the poisoning block `:772-774` in `finish_load` | `quality_eviction_…` (load cases) and `a_load_publish_evicts_until_the_budget_holds_and_no_further` (warm assert) |
| NV-C | `:546` `.filter(\|w\| w.clean)` → `.filter(\|_\| true)` | `the_clean_warm_window_proves_an_absent_key_to_its_frontier` ("a dirty window must not bridge", left 0 right 1) |
| NV-D | `:666` `if !still_inflight {` → `if false {` | `a_short_owned_load_answers_its_reader_directly` (provable_to left 4259840 right 4194304) |
| NV-E | `:450` `len() > 1` → `len() > 0` | `a_lone_slice_larger_than_the_budget_stays_resident` |
| NV-F | `:758` `>` → `>=` | `a_load_publish_evicts_until_the_budget_holds_and_no_further` ("evicted past the budget") |

The post-wake coverage site (`:641-658`) is pinned through its consequence, the "finished but short" branch (NV-D). Its bridge half has no separate read-path pin; after C3 it is the same code as the decision site's (`Inner::effective_to`), which #5 and #6 pin.

On the **C3 tree**:

| Id | Deliberate break | Must fail |
|---|---|---|
| NV-1 | `evict_to_budget`: `seen` → `usize::MAX - seen` (reversed tie order) | `quality_eviction_…`: the minimal case shrinks to tied entries over budget |
| NV-2 | `Inner::remove`: delete the `w.clean = false` block | `quality_eviction_…`, `eviction_poisons_fresh_claims`, `warm_extension_bridges_matchfree_hole`, `a_load_publish_evicts…` |
| NV-3 | `Inner::effective_to`: drop `w.clean &&` | `the_clean_warm_window…` phase 2 |
| NV-4 | `runs_for`: `if !still_inflight { break; }` → `{ continue; }` | `a_short_owned_load_answers_its_reader_directly` |

A failing proptest writes `proptest-regressions/postings_cache/eviction_tests.txt`. Delete it after the NV runs; it must not be committed. `git status` must show only the intended files.

---

## 4. Edits, file by file, in commit order

Budgets:
- `src/postings_cache.rs`: 950 lines now; 1,000 is the crossing limit. Estimates: C1 952, C2 about 964, C3 about 923.
- `tests.rs`: 761, rising to about 826.
- `eviction_tests.rs`: about 190, then about 215.
- None of the files over 1,000 lines is touched. No DST file changes.

### C1: "The postings cache's eviction order, budget books and demand bridge are pinned before they move"

1. `src/postings_cache/eviction_tests.rs` (new): helpers and tests 1–3 from §3.1.
2. `src/postings_cache/tests.rs`: append tests 4–6.
3. `src/postings_cache.rs`: insert `#[cfg(test)]\nmod eviction_tests;` between `admission_tests` and `straddle_tests` (+2). It sits among the trailing test items, so `trailing_test_prefix` is unchanged and the planner files it under `production_unchanged_files`. **No mutants are selected by C1.**
4. `docs/quality/owners.json`: insert two rows in place, right after the `src/postings_cache/tests.rs` macro-dsl row (about line 1628). Do not re-sort; keep `ensure_ascii=False`.
   ```json
   {"category": "unresolved-glob", "count": 1, "owner": "crate",
    "path": "src/postings_cache/eviction_tests.rs",
    "reason": "Postings cache test module imports its enclosing production owner; the compiler resolves the parent exports and the fixtures pin eviction order, the resident weight books and victim poisoning.",
    "syntax": "compiler-resolved import; syntax cannot infer exports"},
   {"category": "macro-dsl", "count": 1, "owner": "crate::macro(proptest::proptest)",
    "path": "src/postings_cache/eviction_tests.rs",
    "reason": "Postings cache eviction property; generated residents with tied recencies, grown by a write-through install or a load publish, drive the production eviction against a least-recent-first minimum-scan oracle, the resident-weight books and every segment's absence proof.",
    "syntax": "proptest::proptest"}
   ```
   Use the same multi-line layout and key order as the neighbouring rows.

### C2: "An eviction pass orders its victims in one scan, and every removal poisons its segment in one place"

`src/postings_cache.rs` only (plus the red test in `eviction_tests.rs`):

- Imports (`:21`): `use std::cmp::Reverse;` + `use std::collections::{BinaryHeap, HashMap};`.
- `struct Inner`: add
  ```rust
  /// Test-only: how often an eviction pass scanned `slices` for victims;
  /// one per over-budget pass, never one per victim.
  #[cfg(test)]
  eviction_scans: u64,
  ```
  In `PostingsCache::new`'s `Inner { … }` literal, add `#[cfg(test)] eviction_scans: 0,`.
- `impl Inner`, after `publish_load` (no overlap with its body lines):
  ```rust
  /// Drops one resident entry. Every removal poisons its segment's warm
  /// absence proof: the key could re-appear in a later install, and a fresh
  /// entry must not then claim its pre-removal history was empty.
  fn remove(&mut self, key: &Key) {
      if let Some(entry) = self.slices.remove(key) {
          self.total_bytes -= entry.slice.decoded_bytes;
      }
      if let Some(w) = self.warm.get_mut(&key.0) {
          w.clean = false;
      }
  }

  /// Evicts least-recently-used entries until the resident weight fits
  /// `max_bytes`, and says how many left. `spare` (the entry a load just
  /// published) is never a victim, and the last entry standing stays: a
  /// slice larger than the whole budget is still served. The order is built
  /// in one scan because the lock is process-wide: a pass that frees
  /// thousands of entries must not rescan the map per victim. Recency ties
  /// go to the entry the map yields first, as a minimum scan's would.
  fn evict_to_budget(&mut self, max_bytes: usize, spare: Option<Key>) -> u64 {
      if self.total_bytes <= max_bytes {
          return 0;
      }
      #[cfg(test)]
      self.eviction_scans += 1;
      let mut order: BinaryHeap<_> = self
          .slices
          .iter()
          .filter(|(key, _)| Some(**key) != spare)
          .enumerate()
          .map(|(seen, (key, entry))| Reverse((entry.last_used, seen, *key)))
          .collect();
      let mut evicted = 0;
      while self.total_bytes > max_bytes && self.slices.len() > 1 {
          let Some(Reverse((_, _, victim))) = order.pop() else {
              break;
          };
          self.remove(&victim);
          evicted += 1;
      }
      evicted
  }
  ```
  - Why this matches the old victim sequence: the heap orders `(last_used, map position)`. Repeated first-minimum scans produce the same sequence, because `remove` never reorders the entries that remain.
  - Why the victim set is unchanged:
    - `install_chunk` passes `None` and keeps the old `len() > 1` floor.
    - `finish_load` passes `Some(key)`. With the published entry resident, the same floor stops exactly when no other candidate remains, which is the old `None => break`.
  - The loop is bounded by the heap, not by map state. Every mutant that stops `remove` from shrinking the map still terminates after at most `n + 1` pops (§5).
- `install_chunk` invalid path, `:311-315` → `for key in victims { g.remove(&key); }`. The warm record was just deleted, so the poison inside `remove` is a no-op. Evictions are still not counted.
- `install_chunk` eviction, `:449-469` → replace with:
  ```rust
  // Weight eviction (Inner::evict_to_budget poisons each victim's segment).
  let evicted = g.evict_to_budget(self.max_bytes, None);
  drop(g);
  self.evictions.fetch_add(evicted, Ordering::Relaxed);
  ```
  The two `warm_*` `fetch_add`s follow unchanged.
- `finish_load`, `:758-775` → replace with:
  ```rust
  let evicted = g.evict_to_budget(self.max_bytes, Some(key));
  self.evictions.fetch_add(evicted, Ordering::Relaxed);
  ```
  Keep `if !published { return; }` byte-identical. Doc (`:741-745`): replace "then evicts to budget - least-recent first, never the entry just published. Every victim poisons its segment's warm absence proof (see install_chunk)." with "then evicts to budget (`Inner::evict_to_budget`), never the entry just published." Doc lines sit outside the body, so they select no mutant.
- `sweep_idle`, `:871-879` → replace with:
  ```rust
  for k in dead { g.remove(&k); self.evictions.fetch_add(1, Ordering::Relaxed); }
  ```
  Formatted over four lines.
- `eviction_tests.rs`: add `an_eviction_pass_scans_the_map_once` (§3.2).

Ratcheted scopes C2 touches, and the remedy for each:

| Scope | Exception | Effect of C2 | Remedy |
|---|---|---|---|
| `install_chunk` | `unwrap_used` (fingerprints paths and call-sites) | new path `evicted`; `None` path count goes 2 → 3 (the old `None => break` is a `Pat::Ident`, not a path) | **Re-decide reason:** `PostingsCache::install_chunk; a poisoned cache index may hold a half-applied install or eviction pass, victims gone but their segments still clean; recovering it could serve a truncated slice or a warm absence proof the eviction broke` |
| `install_chunk` | `too_many_lines` | about 161 → 142 code lines (still > 100); scope_lines and syntax_facts shrink | none |
| `install_chunk` | `excessive_nesting` | extend arm keeps its let-else + `if let` at depth 5–6 | none |
| `finish_load` | `unwrap_used` | new call-site `Some(key)`, new path `evicted` | **Re-decide reason:** `PostingsCache::finish_load; a poisoned cache index may hold a half-applied publish or eviction pass, its marker or victims half cleared; recovering it could wedge the key or serve a warm absence proof the eviction broke` |
| `sweep_idle` | `unwrap_used` | paths only shrink (`g` 7→5, `k` 3→2, `e` 2→1, `w` 2→1, `Some` 2→0); no new path, call-site or unwrap | none (confirm with the gate; if it reports growth here, re-decide its reason) |
| new `Inner::{remove, evict_to_budget}` | none needed: ≤ 25 lines, depth ≤ 4, ≤ 3 args, no unwrap | — | — |

`stats`, `maybe_prefetch`, `spawn_load`, `load_runs` and `publish_load` are untouched.

### C3: "A cached read decides coverage in one place and falls back uncached from one place"

`src/postings_cache.rs` only:

- `impl Inner`, add:
  ```rust
  /// How far the resident entry for `key` proves a read of [from, upto),
  /// when it proves all of it. Past the entry's own frontier, the segment's
  /// clean warm window still covers the key when the window starts at or
  /// before that frontier: its absence from every install since IS the
  /// proof of no matches there. (The install-side bridge fires only on the
  /// key's NEXT appearance; a key that never re-appears would otherwise go
  /// cold at every new chunk.)
  fn effective_to(&self, key: &Key, from: u64, upto: u64) -> Option<u64> {
      let slice = &self.slices.get(key)?.slice;
      let own = slice.indexed_to_offset;
      let bridge = self.warm.get(&key.0).filter(|w| w.clean && w.from <= own);
      let effective_to = bridge.map_or(own, |w| w.to.max(own));
      (slice.covered_from <= from && upto <= effective_to).then_some(effective_to)
  }
  ```
  `w.to.max(own)` replaces the old `wt > effective_to` test. Both give the same value, and it removes the `>`→`>=` mutant, which would otherwise be equivalent.
- `runs_for` decision, `:536-556`. Delete the comment block (it moves into the doc above), the `warm_to` binding and the `effective_to` recomputation. Then:
  ```rust
  let mut g = self.inner.lock().unwrap();
  let covering = g.effective_to(&key, from, upto);
  let covered = g.slices.get_mut(&key).and_then(|e| {       // line unchanged
      if let Some(effective_to) = covering {                // was :556
          e.last_used = Instant::now();                     // :557-564 unchanged
  ```
  The closure header and the Bypass `else if e.slice.covered_from > from` keep their exact text and indentation, so their operators are not selected.
- `Decision::Bypass => { … :590-601 }` → `Decision::Bypass => break,`.
- Post-wake, `:637-662` → replace with:
  ```rust
  let (ready, still_inflight) = {
      let g = self.inner.lock().unwrap();
      (
          g.effective_to(&key, from, upto).is_some(),
          g.inflight.contains_key(&key),
      )
  };
  ```
- `:666-677` → `if !still_inflight { break; }`. The `if !still_inflight {` line stays unchanged.
- `:679` comment becomes: `// Uncached, once: the cursor is behind the resident slice, a finished load left nothing that serves it, or contention outlasted the re-check loop.` The tail `:680-689` is unchanged.

Ratcheted scopes in `runs_for`:

| Exception | Effect | Remedy |
|---|---|---|
| `unwrap_used` | new path `covering` | **Re-decide reason:** `PostingsCache::runs_for; a poisoned cache index may hold a half-published load or a torn warm record; recovering it could answer a read from coverage no load or install proved` |
| `too_many_lines` | 159 → about 109 code lines (counted from a draft of the new body; clippy counts non-blank, non-comment lines of the body block) | still fulfilled. If clippy reports `unfulfilled_lint_expectations` (count ≤ 100), delete this attribute; that is the only allowed response. |
| `excessive_nesting` | closure block inside `let decision = { … }` inside the `for` stays at depth 5 | none |
| `too_many_arguments`, `let_underscore_must_use` | unchanged signature; both `let _ = rx.changed().await` remain | none; metrics shrink |

Architecture function budget (200 physical lines): `install_chunk` (195) and `runs_for` (183) both shrink. `architecture-policy.json` has no budgets for this file, so nothing becomes obsolete.

---

## 5. Mutation analysis

Setup: cargo-mutants 27.1.0, `--in-diff`, owner `postings_cache`, filter `postings_cache::`, profile `quality` (inherits dev, so overflow checks are on), `--timeout 90`.
- C1 selects nothing: the new files are `#![cfg(test)]` and the `mod` line is in the trailing test prefix.
- The receipt for a C1..C3 push lists `mutation_source_files: ["src/postings_cache.rs"]`, owner `postings_cache` only. `owned_load.rs` is unchanged.

| # | Mutant | Killer |
|---|---|---|
| 1 | `replace Inner::remove with ()` | `load_publish_evicts_least_recent_others_only_over_budget` ("victim gone"); `o4a_…` (`slices.is_empty()`); proptest |
| 2-3 | `remove`: `-=` → `+=`, `/=` | `load_publish_evicts…` (`total_bytes == 2*176`); `o4a_…` (`total_bytes == 0`); proptest books |
| 4-5 | `evict_to_budget -> u64` with `0`, `1` | `load_publish_evicts…` (victim gone); `million_key_shape_holds_process_budget` |
| 6 | early `<=` → `>` | every over-budget test (returns 0) |
| 7 | `!=` → `==` (spare filter) | load: `load_publish_evicts…` "never evicts itself"; install: `eviction_poisons_fresh_claims` (`evictions >= 1`) |
| 8-9 | `total > max` → `==`, `<` | no eviction: `load_publish_evicts…`, `million_key…` |
| 10 | `total > max` → `>=` | `a_load_publish_evicts_until_the_budget_holds_and_no_further` |
| 11 | `&&` → `\|\|` | `load_publish_evicts…` (`inc30` survives); proptest |
| 12-13 | `len() > 1` → `==`, `<` | no eviction with many entries: `million_key…`, `eviction_poisons…` |
| 14 | `len() > 1` → `>=` | `a_lone_slice_larger_than_the_budget_stays_resident` |
| 15 | `evicted += 1` → `-=` | overflow panic (quality profile); in the spawned load, the guard unwinds and poisons the mutex, so the test's next lock panics |
| 16 | `evicted += 1` → `*=` | `load_publish_evicts…` (`evictions == 1`); proptest (`evictions == victims`) |
| (17-18) | cfg(test) `eviction_scans += 1` → `-=`, `*=` (if cargo-mutants does not skip the `#[cfg(test)]` statement) | overflow panic / `an_eviction_pass_scans_the_map_once` (`== 1`) |
| 19 | `effective_to` with `None` | `warm_install_serves_from_zero_without_index_load` (`index_loads == 0`, `hits == 1`); `the_clean_warm_window…` phase 1 |
| 20-21 | `effective_to` with `Some(0)`, `Some(1)` (every resident read Hits) | `noncontiguous_chunk_resets_absence_claim` (`index_loads >= 1`); `the_clean_warm_window…` phase 2 |
| 22 | `w.clean && w.from <= own`: `&&` → `\|\|` | `the_clean_warm_window…` phase 2 |
| 23 | `w.from <= own` → `>` | `the_clean_warm_window…` phase 1; `a_warm_window_that_restarted_past_a_key…` |
| 24 | `covered_from <= from` → `>` | `warm_install_serves_from_zero…` |
| 25 | range `&&` → `\|\|` | `noncontiguous_chunk_resets_absence_claim`; `a_short_owned_load_answers_its_reader_directly` (the reader would lead again and answer 4,259,840) |
| 26 | `upto <= effective_to` → `>` | `warm_install_serves_from_zero…` |
| 27 | `replace install_chunk with ()` | `warm_install_serves_from_zero…` |
| 28 | `replace finish_load with ()` | `cold_load_claims_only_to_its_absorbed_target` (`debug_slice == Some((0,100,0))`) |
| 29-30 | `sweep_idle -> usize` with `0`, `1` | `warm_extension_bridges_matchfree_hole` ("post-sweep reads must consult the store") |
| 31 | `runs_for -> anyhow::Result<CacheRuns>` with `Ok(Default::default())` | unviable (no `Default` for `CacheRuns`) |
| 32 | `PostingsCache::new` whole-body replacement (touched by the cfg(test) initialiser) | unviable (no `Default`) |

Expected result: 28 caught (30 if 17-18 are generated), 2 unviable, 0 missed, 0 timeout.

**No equivalent mutants.** Each operator changes an observable outcome:
- #10 is observable only when a pass lands exactly on the budget, which #3 constructs.
- #14 is observable only when a single entry exceeds the budget, which #2 constructs.
- `w.to.max(own)` removes the one would-be equivalent (`wt > own` → `>=`).

Operator lines in `runs_for`, `finish_load`, `install_chunk` and `sweep_idle` that keep their text are not selected:
- the Bypass `>` at `:559`
- `if !still_inflight` at `:666`
- `if !published` at `:755`
- `k.0 == inc.0` at `:310`

The deleted old copies carry no mutants.

**Boundedness.**
- `evict_to_budget`'s `while` pops from a heap of at most n entries and breaks on `None`. Under #1/#11/#14, and under any mutant that leaves `total_bytes` over budget, it ends after at most n+1 iterations.
- `sweep_idle`'s and the purge's loops iterate over a collected `Vec`.
- `runs_for` stays bounded by `for _ in 0..4`:
  - Under #19-26 a wrong verdict costs at most four Leads and one uncached load.
  - Under #28 the stale marker's receivers return at once: the value was already sent, or the sender dropped.
- No selected mutant touches `transaction/mod.rs`'s unboundable committer stages.
- The postings_cache:: test run takes about 2-4 s at baseline. The new property is synchronous: about 47 entries, at most 8k runs per case, 1,024 cases.

---

## 6. Ledgers

- `docs/quality/owners.json`: the two rows in C1 (§4). No other rows move: the `spawn_load` `tokio::spawn` effect row and the `stats` `serde_json::json` row are untouched.
- `docs/quality/source-allowances.json`: no rows vacated. Only reasoned `#[expect]`s change, and those are reviewed in-source, not baselined.
- `scripts/quality/mutation_owners.py`: no change. No new production file. `eviction_tests.rs` is `#![cfg(test)]`, so the planner classifies it `production_unchanged` like `tests.rs`, `admission_tests.rs` and `straddle_tests.rs`, none of which have rows. Control 7.6 confirms this.
- `docs/refactor/test-inventory.json`, `review-mechanisms.json`, `test-additions.json`, `src/dst/tests/README.md`: no change (no `src/dst` test touched; no postings_cache test is pinned there).
- `docs/refactor/test-scenario-map.json` / `scenario-dispositions.json`: no change. HIS-027 names `million_key_shape_holds_process_budget` in `src/postings_cache/tests.rs`, which is kept, and only symbol existence is checked.
- `docs/refactor/architecture-policy.json`: no change (§4).
- `proptest-regressions/`: nothing, unless a genuine failure is minimised (commit it then). Delete files left by NV runs.
- `scripts/mt-audit-baseline.txt`: no postings_cache rows; no change.

---

## 7. Controls

```sh
SCRATCH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad
export PATH=$SCRATCH/pybin:$PATH          # python3 >= 3.11 shim for the gate
```

1. **C1 pins on the old code:** `cargo test --locked --lib postings_cache:: 2>&1 | tee $SCRATCH/p75-c1.log` → `test result: ok. 24 passed; 0 failed`. Then run the property as CI does: `cargo test --locked --release --lib quality_eviction_takes_the_least_recent_first_and_keeps_the_books` → `1 passed`.
2. **NV-A..NV-F on C1** (§3.3), one at a time: `cargo test --locked --lib postings_cache::` → at least the named tests fail, with the quoted messages. Older tests may fail as well; that is fine. Then `git checkout -- src/postings_cache.rs && rm -rf proptest-regressions/postings_cache/eviction_tests.txt`.
3. **C2 red:** apply the staging from §3.2 on the C1 tree, then `cargo test --locked --lib postings_cache::eviction_tests::an_eviction_pass_scans_the_map_once 2>&1 | tee $SCRATCH/p75-red.log` → `FAILED … one pass, one scan … left: 316 … right: 1`.
4. **C2 and C3 green:** after each commit, `cargo test --locked --lib postings_cache::` → 25 passed. Then run NV-1..NV-4 on C3 (§3.3) and restore.
5. **Pre-flight:** `cargo fmt --all -- --check`, then `scripts/quality.sh` (about 3 min) → exit 0.
   - No `accepted exception grew` line. If one names `sweep_idle`, re-decide its reason as in §4.
   - No `unfulfilled_lint_expectations`. If one names `runs_for`'s `too_many_lines`, delete that attribute.
   - No `file growth`.
6. **Planner, CI's selection** (after committing C1..C3): `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`. Expected in `target/quality-plan/plan.json`:
   - `mutation_source_files == ["src/postings_cache.rs"]`
   - `selected_mutation_owners == ["postings_cache"]`
   - `unregistered_mutation_source_files == []`
   - `production_unchanged_files` ⊇ `["src/postings_cache/eviction_tests.rs", "src/postings_cache/tests.rs"]`
7. **Mutants,** with the same env: `QUALITY_MUTANTS_OUT=$SCRATCH/mutants-p75 scripts/quality/mutations.sh 2>&1 | tee $SCRATCH/mutants-p75.log; echo MUT_EXIT=$?`.
   - Expected: `Found 30 mutants to test` (or 32), `… 28 caught, 2 unviable` (or 30/2), `MUT_EXIT=0`.
   - `mutants-p75/postings_cache/mutants.out/{missed,timeout}.txt` empty.
   - Do not edit the tree or run the gate while this runs.
8. **Full gate** (after the mutation run, never concurrently): `OUT=$SCRATCH/gate-p75.txt scripts/gate.sh; tail -5 $SCRATCH/gate-p75.txt` → no `GATEFAIL-*`. The suite and the capacity leg pass.
9. **After the push:** `gh run list --branch slate --json headSha,status,conclusion,name,createdAt` for the pushed sha. ci, rust-quality and workflow-lint must all be green for that exact sha. rust-quality's mutation step should take about 1-1.5 h for 30 mutants; the job timeout is 240 min.

---

## 8. Out of scope

- **Narrowing the six function-wide `unwrap_used` expectations** to one `PostingsCache::lock()` (the precedent is `registry/cache.rs::slots`, "the one place the cache decides what a poisoned lock means"). It would remove the fingerprint ratchet from this file for good. It is not done here because converting `stats` and `maybe_prefetch` selects their whole-body mutants, which have no `postings_cache::` killers today (no stats test, no prefetch-count test). Worth a follow-up with those two tests.
- **Failed-seam poison at `:399-401`.** It poisons without removing, so it stays where it is. Routing it through a `poison(segment)` helper would add an abstraction with two callers.
- **The warm-record bound scan at `:330-342`** (`min_by_key` over at most 8,192 warm records when a new segment arrives with the map full). It is bounded by a constant, not by n.
- **`sweep_idle` runs from every engine's flush ticker** (`shard.rs:1720`) against the shared cache: E engines make E full scans per tick. That is one scan per sweep, not per victim.
- **The install-side bridge at `:392-393`** is the same rule as `effective_to` applied at `chunk_from`. Reusing it would rewrite the deepest arm of `install_chunk` under its ratchets for no behavioural gain.
- **Bounded-memory victim selection** (a max-heap pruned to the victims needed). Its pruning mutants (for example `-` → `/` in the prune test) only change memory, so they would be equivalent without yet another test-only witness. The full heap is the smallest single-pass design with no equivalent mutants.
- **Known correctness hole, unchanged here** (recorded in `d2e87129`'s message). The demand bridge checks `w.clean` but not `w.admitted_all`. How it happens:
  1. The cache is over its admission line, so a key's fresh install into window chunk C is skipped.
  2. `admitted_all` becomes false, but `clean` stays true.
  3. A later byte-capped or 64-bucket-capped cold load publishes the key short of C.
  4. `effective_to` lifts its coverage across C, although the key had matches there.

  After C3 this is a one-line decision in `Inner::effective_to` (require `admitted_all`, trading warm hits under pressure). It needs its own red test (a capped load under a skipped-admission window) and item. This plan preserves today's behaviour exactly.

## 9. Decisions for Søren

None. No status code, body, `/v1/debug`, `/metrics` or `stats()` shape changes, and no policy changes. The transient eviction heap (§2) stays within the owner and is described in the C2 message. The admission/bridge hole in §8 is a candidate decision for a separate item; this plan does not make it.

---

## Skeptic corrections (C1..C11)

Verified against `24c4c77a` (= `origin/slate`, clean). Every quoted line in §1 matches `src/postings_cache.rs` (950 lines): the three removal copies (`:449-468`, `:758-775`, `:871-879`), the purge (`:307-317`), the four `w.clean = false` (`:400/:463/:773/:877`), the seven `total_bytes` writes, both bridge copies (`:543-564`, `:637-662`) and the three fallbacks (`:590-601`, `:666-677`, `:679-689`). The §1.5 use-site list is complete for code; `git grep` also finds only doc or mod mentions in `src/config/model.rs:149`, `src/failpoints.rs:12` and `src/lib.rs:48`. The heap's tie order matches first-minimum `min_by_key` plus hashbrown's order-preserving `remove`, so the victim sequence is identical. The `len() > 1` floor with the spare resident matches the old `None => break`. The C1 pins (tests 1-5) trace as the plan claims, and so do the red test's 316 victims. The ratchet table is correct: re-deciding `install_chunk`, `finish_load` and `runs_for` covers their new keys. `sweep_idle` does not grow: its paths shrink, and method-call sites are fingerprinted only for `unwrap`/`unwrap_err` (`scripts/quality/source_rules.py:143-172`). The three re-decided reasons have exactly two `;`. `excessive_nesting` stays fulfilled everywhere; clippy counts the `impl` block as a level, so `runs_for`'s `if let Some(effective_to) = covering {` inside the closure is at level 6. The new `Inner` fns stay at ≤ 4. Line budgets hold: C2 ≈ 960, C3 ≈ 925, `tests.rs` ≈ 826. No file over 1,000 lines is touched. The corrections:

**C1 (substantive, §2 / §9): the heap is not "only an over-budget pass" in practice. It is an unbudgeted, per-publish transient.**
- Once the cache sits at its budget, **every** load publish (`finish_load`, k ≈ 1 victim) and every growing install is an over-budget pass.
- For k = 1, the old code did one scan with no allocation (`:759-764`). C2 instead collects n × 56 B into a Vec and heapifies it, under the process-wide lock.
- `.filter().enumerate().map()` has a `size_hint` lower bound of 0, so `collect()` grows the Vec by doubling. Peak capacity is up to 2n, about 42 MB at the 64 MiB default: 381k entries × 56 B × 2. That is up to about 64% of `cache_bytes` on top of the cache itself.
- `bootstrap.rs:811-845` sums `cfg.postings.cache_bytes` as a *fixed* memory bound against the shed line (the "OOM review" budget line), and this transient is outside it.
- Required:
  - (a) Build as `let mut v = Vec::with_capacity(self.slices.len()); v.extend(…); BinaryHeap::from(v)`. This caps the peak at 1× and adds no operator mutant.
  - (b) Replace §2's cost paragraph with the honest steady-state picture: the k = 1 publish gets slower and pays the allocation, and large-k installs get much faster.
  - (c) Move the transient (≈ 32% of `cache_bytes`, scaling with `POSTINGS_CACHE_BYTES`) into §9 as a memory-posture note for Søren. §9's "None" is wrong on this point, although no edge contract changes.
  - Alternative worth offering there: a candidate set bounded by `total - max_bytes` (O(k) memory). Its pruning mutants can be killed with one more `#[cfg(test)]` peak-candidates witness beside `eviction_scans`, so §8's "would be equivalent" rejection is not final.

**C2 (§5 killers): `million_key_shape_holds_process_budget` never evicts, so it kills none of #4-5, #8-9, #12-13.**
- `admit_fresh` is decided once per install (`:329`). One install adds at most 500 keys × 240 B ≈ 120 KB (`tests.rs:151-161`, 185).
- So the resident weight peaks near 1 MiB + 120 KB against a 2 MiB budget (`tests.rs:172`). No publish crosses it either.
- Strike it from those rows. The mutants are still killed:
  - #4-5 and #8-9 by `load_publish_evicts_least_recent_others_only_over_budget` (`tests.rs:603-638`: "victim gone" and `evictions == 1`).
  - #12-13 by the same test (3 residents, so `len()==1`/`<1` evicts nothing and `evictions` stays 0 ≠ 1) and by `eviction_poisons_fresh_claims` (`tests.rs:242-245`).
  - All of them by test 1 and the red test.

**C3 (§3.3 NV-2): `warm_extension_bridges_matchfree_hole` does not fail under NV-2.**
- `sweep_idle(Duration::ZERO)` also runs `g.warm.retain(|_, w| w.touched >= cutoff)` (`:880`), which drops every warm record whether or not it was poisoned.
- The next install at `[300,400)` then starts a fresh window from 300 (`:343-349`), and the read from 0 Bypasses and loads either way.
- Remove that test from NV-2's list. NV-2 stays non-vacuous through test 1, `eviction_poisons_fresh_claims` and test 3.
- For the same reason, the test's comment "poison via a sweep" (`tests.rs:137`) describes the retain, not the poison. Do not cite it as a poisoning pin anywhere.

**C4 (§3.3): pin #6 (`a_warm_window_that_restarted_past_a_key_proves_nothing_for_it`) has no non-vacuity control.**
- Add **NV-G on C1**: at `:551`, replace `wf <= effective_to` with `true` (keep `wf` bound as `_wf` to avoid an unused warning).
- Traced: the window is `{150, 250, clean}` and `wt 250 > 100`, so `eff = 250`. The read Hits and `index_loads` stays 0, so the test fails "the window starts past the key's frontier".
- Buildable. Revert it with the others.

**C5 (§5 mutant count): diff sliding can select `publish_load`'s whole-body mutants.**
- `Inner::remove`/`evict_to_budget` are inserted directly after `publish_load`'s closing `    }` (`:175`), and `effective_to` goes after another `    }`.
- git may slide the hunk so that the old closing brace shows as an added line. That selects `replace Inner::publish_load -> bool with true/false` (and, in C3, `evict_to_budget`'s again).
- Both are killed by `publish_load_merges_into_the_resident_entry_or_publishes_nothing` (`tests.rs:521-571`, which asserts both results).
- Expect 30-34 mutants. Before the long run, run the driver's own `cargo mutants --list --json --in-diff target/quality-plan/pr.diff --file src/postings_cache.rs --package streams-slate` (`scripts/quality/mutation_driver.py:56-63`) and reconcile the list with §5.

**C6 (§7 control 6): the plan receipt will also say `properties_fuzz: true`.**
- `src/postings` is a CODEC_PREFIX (`scripts/quality/verification_plan.py:21-23`), and `src/postings_cache.rs` matches it by string prefix, so `codec` is true and the properties/fuzz-corpus step runs in rust-quality on this push.
- Add it to the expected plan and to the CI time estimate.

**C7 (§5 #25 wording).** Under `&&`→`||`, the short-load reader does not "lead again". Post-wake `ready` turns true, and iteration 2 Hits with `provable_to = upto = 4_259_840` and `hits == 1`. It is still killed by test 4 (`provable_to` 4,259,840 ≠ 4,194,304).

**C8 (§6 wording).** The `spawn_load` `tokio::spawn` effect row and the `stats` `serde_json::json` macro-dsl row are in `docs/quality/source-allowances.json:1438-1444` and `:2922-2928`, not in `owners.json`. They are untouched and still counted, so `--prune` stays a no-op. No ledger is missed.

**C9 (§3.1, owners rows).** Keep the proptest body exactly as `tests.rs:471-503` does:
- Full `proptest::` paths, and the body calls the helper, which uses only `assert!`/`assert_eq!`.
- `prop_assert*!` is not in `EXPRESSION_MACROS` (`source_rules.py:14-19, 52-54`), and `use proptest::prelude::*` is an unresolved glob. Either adds an unregistered source occurrence beyond the two rows in §4.

**C10 (§3.1 test 5 and helpers).**
- Poke `warm[inc].clean = false` as one statement, so the temporary guard drops before `runs_of(..).await`: `cache.inner.lock().unwrap().warm.get_mut(&inc.0).unwrap().clean = false;`.
- Return from `eviction_view` before `grow_to`. A guard held across the await is `await_holding_lock` (denied, `Cargo.toml:137`). A guard held into `install_chunk`/`finish_load` is a same-thread std-mutex deadlock, which would hang the mutation run as a TIMEOUT.
- `resident()` must also set `SegWarm.touched`.

**C11 (style, §4 docs).** Doc comments state why the code exists:
- `Inner::remove` should lead with "Every removal poisons its segment's warm absence proof: the key could re-appear…", not "Drops one resident entry."
- `evict_to_budget` should lead with the process-wide-lock reason, not "Evicts least-recently-used entries…".
- Also note that `eviction_scans` counts passes by placement. Keep its increment immediately before the only `self.slices.iter()`, so the witness stays adjacent to the scan it stands for.

**Checked and correct (no change):**
- The ratchet remedies.
- The `too_many_lines` fallback for `runs_for` (≈ 109, so deleting the attribute is the only allowed response if it drops to ≤ 100).
- Boundedness: the heap-bounded `while`, collected-Vec loops, `for _ in 0..4`. The stale-marker receivers under #28 return at once, since the version is already bumped or the sender is dropped.
- The overflow-panic kills for `-=` mutants: the quality profile inherits dev, and a poisoned mutex fails the waiter's next `lock().unwrap()`.
- `PostingsCache::new` and `runs_for` whole-body mutants are unviable (no `Default`; no `error_values` configured).
- No DST, test-inventory, review-mechanisms, test-additions, scenario, architecture-policy or `mutation_owners.py` change is needed. The new `#![cfg(test)]` file is `production_unchanged` via `normalized_source` (`production_changes.py:66-67`).
- The admission/bridge hole in §8 is real and recorded in `d2e87129`'s message. It belongs in its own item.

**Verdict: ready-with-corrections.** C1 must be settled before C2 lands: use `with_capacity`, give §2 the honest cost, and add the §9 posture note. C2-C5 fix the evidence (killer attributions, one missing NV, the mutant count). C6-C11 are receipt, wording and hygiene fixes.
