# Item 66: DST tests that assert on process-global history counters

Tree: `slate` @ `fba7b844` (8 ahead of `origin/slate` = `fb18840d`, which is also the merge base). Everything below was read on that tree; line numbers are current (the reviewer's are partly stale). Nothing was run: a mutation run and the gate own the tree.

**Summary.** Two test-only commits. No production code, wire, metric or JSON change, and no mutants. The only ledger is `docs/refactor/test-inventory.json` (one `function_sha256` per commit). For the gather test this plan does **not** follow the reviewer's `GatherOutcome.postings_bytes` Change. That Change can be built, but only by also changing what `/v1/debug` `postings.bytes_written` means (§2). The smaller correct fix reads the pages that gather stored back from the engine's own partition. The sparse-key test takes the reviewer's per-engine `PostingsCache` counter.

---

## 1. Problem (verified)

### 1.1 The counters are process statics

`src/history.rs:886-894`:

```rust
pub(crate) static POSTINGS_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
...
pub(crate) static READ_FRAMES_MATCHED: AtomicU64 = AtomicU64::new(0);
pub(crate) static POSTINGS_CORRUPT: AtomicU64 = AtomicU64::new(0);
```

CI runs the whole library test binary as one process on parallel threads. `.github/workflows/ci.yml:98` has `cargo test --release -- --skip post_split_throughput_scales`, and no `RUST_TEST_THREADS` or `--test-threads` is set anywhere. So every DST test shares these atomics with every test running beside it.

### 1.2 Site A: the assertion can never fail in CI (reviewer's `reads_history.rs:241-244`, still exact)

`src/dst/tests/reads_history.rs:241-244`, in `dst::dst_tests::reads_history::sparse_key_reads_page_with_bounded_spans`:

```rust
    assert!(
        crate::history::READ_FRAMES_MATCHED.load(Ordering::Relaxed) > 0,
        "the postings planner path must have served this"
    );
```

- The counter has exactly one writer, `src/history/canonical_span.rs:22` (`READ_FRAMES_MATCHED.fetch_add(1, Relaxed)` inside `ResultPage::inspect`). It is reached only via `execute_postings_plan` (`src/history/postings_read.rs:52`).
- The corruption envelope (`read_history2_keyed_envelope`, `history.rs:1066+`) never increments it. So in an isolated run the assertion does mean "the planner matched at least one frame".
- In the CI process, the atomic is already positive once any earlier keyed planner read has matched a frame. Examples from this same file: `repeated_keyed_reads_hit_the_postings_cache` (8 `"hot"` matches) and `keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records` (`"cold"`).
- The absolute `> 0` check is therefore always true in CI, even if this test's reads never reached the planner. Control C2 (§3) shows this deterministically.

### 1.3 Site B: an upper bound on a process-wide delta (reviewer's `history_gather.rs:561-572`, now 561-574)

`src/dst/tests/history_gather.rs`, in `dst::dst_tests::history_gather::keyed_frames_no_longer_count_twice_against_the_budget`:

```rust
561    let before = crate::history::POSTINGS_BYTES_WRITTEN.load(Ordering::Relaxed);
562    let g1 = absorber.absorb_gather_v2(&hashes).await.expect("gather 1");
...
568    let postings = crate::history::POSTINGS_BYTES_WRITTEN.load(Ordering::Relaxed) - before;
569    let canonical: u64 = g1.advanced.iter().map(|(_, _, b)| *b).sum();
570    assert!(postings > 0, "keyed frames must produce postings pages");
571    assert!(
572        postings * 100 <= canonical * 8,
573        "postings bytes must stay within the 8% batch-1 gate: {postings} vs {canonical}"
574    );
```

- **The only writer** is `src/history/gather.rs:149` (the reviewer's "gather.rs:144"): `POSTINGS_BYTES_WRITTEN.fetch_add(postings_bytes, Ordering::Relaxed);` in `stage_postings`. Every gather in the process calls it, once per staged chunk. Every routing key, including the default `""`, gets pages (`gather.rs:124-128` doc).
- **This gather's own contribution:** two streams, one `"k1"` record each. That is 2 pages of 36 B: `encode_page` (`postings.rs:150`) writes a 30 B header plus the varints 0, 1, F (3 B for a frame of about 16.6 KiB) and 0. Total 72 B.
- **The budget:** `canonical` is about 2 × 16.6 KiB, about 34,000 B, so 8% is about 2,720 B. The slack is about 2,650 B.
- **How a neighbour fails it:** any gather elsewhere in the process that stages more than about 2.65 KB of postings between lines 561 and 568 fails the test. One is in this same file: `gather_parallel_reads_preserve_outcomes_across_reopen` runs two 96-stream default-key gathers (`gather_after_reopen`, 2 KiB frames, 35 B pages). Each stages 3,360 B through the same `fetch_add`.
- **Timing:** the window is this test's own gather (an in-memory store with no injected latency, so milliseconds). The failure depends on timing but is real, which is what the reviewer said ("can fail from a neighbour").
- **The lower bound `postings > 0`** is also only as strong as the global delta: a neighbour's pages can satisfy it.

### 1.4 Site C, same class, not cited by the reviewer

`src/dst/tests/reads_history.rs:353` and `:363-366`, in `corrupt_postings_fall_back_to_the_envelope`:

```rust
    let before = crate::history::POSTINGS_CORRUPT.load(Ordering::Relaxed);
...
    assert!(
        crate::history::POSTINGS_CORRUPT.load(Ordering::Relaxed) > before,
        "corruption must be counted"
    );
```

- The writers are `history.rs:1015` (`read_history2_keyed`) and `history.rs:1052` (`read_history2_keyed_cached`, `CacheRuns::Corrupt` arm).
- The unit tests in `src/history/postings_validation_tests.rs` drive both functions over overlapping or invalid pages in the same process.
- This is a lower bound on a global delta. It cannot fail falsely, but it proves nothing in the suite.
- It is kept out of scope in §8, with the reason and a follow-up.

### 1.5 Complete use-site list of the history statics (grep of `src/`, prefixed and bare names)

| Static | Writers | Production readers | Test assertions |
|---|---|---|---|
| `READ_FRAMES_MATCHED` | `history/canonical_span.rs:22` | `http.rs:994` (`/v1/debug` `postings.read_frames_matched`) | **`dst/tests/reads_history.rs:242`** (site A) |
| `POSTINGS_BYTES_WRITTEN` | `history/gather.rs:149` | `http.rs:988` (`postings.bytes_written`) | **`dst/tests/history_gather.rs:561,568`** (site B) |
| `POSTINGS_CORRUPT` | `history.rs:1015`, `history.rs:1052` | `http.rs:995` (`postings.corrupt`) | **`dst/tests/reads_history.rs:353,364`** (site C) |
| `POSTINGS_PAGES_WRITTEN` / `POSTINGS_RUNS_WRITTEN` | `history/gather.rs:136` / `:141` | `http.rs:989` / `:990` | none |
| `CANONICAL_BYTES_WRITTEN` | `history/gather.rs:497` | `http.rs:991` | none |
| `READ_SPANS_MAX` / `READ_FRAMES_SCANNED` | `history/postings_read.rs:85` / `canonical_span.rs:13` | `http.rs:992` / `:993` | none |
| `ABSORB_BYTES_TOTAL` | `history/gather.rs:561` | `http.rs:1575`, `ops.rs:473` | none |
| `ABSORB_ZERO_ROUTE_DROPPED`, `GATHER_LAST_*`, `HISTORY_FLUSH_*`, `INGEST_BYTES_TOTAL` | `history/worker.rs`, `history/gather.rs`, `shard/transaction/publish.rs:85`, `http.rs:1483` | `http.rs`, `ops.rs` | none |

No unit test under `src/history/**` or `src/postings_cache/**` asserts on any of these statics. The `cfg(test)` tests there use per-instance `PostingsCache` counters.

## 2. Contract decision

**Typed per-owner facts replace the process totals:**

- **Site A: `engine.postings_cache.hits`** (`PostingsCache::hits`, `src/postings_cache.rs:181`).
  - The cache belongs to the engine. `ShardConfig::default()` leaves `shared_postings_cache: None` (`shard.rs:1080`), so `ShardEngine::start` builds a fresh `PostingsCache::new(..)` (`shard.rs:1457-1458`).
  - `hits` is incremented only at `postings_cache.rs:582`, the `Decision::Hit` arm (`:581`), which always returns `CacheRuns::Runs`. `read_history2_keyed_cached` hands that to `execute_postings_plan` (`history.rs:1055-1057`).
  - So `hits > 0` means "at least one page of this engine's keyed reads was served by the postings planner", attributed to this engine.
  - Corrupt-verdict paths count only `misses`, and `install_chunk` never counts hits. The absolute value on a cache this test built is therefore already the delta the reviewer asked for, and no `before` snapshot is needed.
- **Site B: postings pages the gather stored**, read back from this engine's history partition.
  - `engine.history_partition()` is the same `Db` the absorber flushed into.
  - The test sums `kv.value.len()` over `crate::postings::postings_range(RouteHash(h), SegmentHash(h), &rk_hash("k1"), 0, upto)` for each `(h, upto, _)` in `g1.advanced`.
  - That equals the `PageBuilder::finish` total (`postings.rs:454-470` sums `encode_page(..).len()`, the stored value). The preconditions are ones the test itself sets up: `append_sized` routes each stream by its own hash (`fixture_storage.rs:210`), the gather keys a stream's pages by `SegmentHash(plan.hash)` (`gather.rs:134`), and each stream holds only `"k1"` frames.
  - HIS-016 is a *storage* ratio (`docs/refactor/SCENARIO-MAP.md:86`), and this measures what is actually stored. No absorber runs in this test (`_absorb_rx` is held and never started), so every such page is g1's.

**No wire change.** `http.rs`, `ops.rs` and every `/v1/debug` / `/metrics` key and meaning stay as they are. No production file is touched.

**Why not the reviewer's `GatherOutcome.postings_bytes`:**

1. **No room in `history.rs`.** `GatherOutcome` is declared at `src/history.rs:715-736`, and `history.rs` is 1,713 lines, equal to its base. The field needs a verbatim move of the struct into `history/gather.rs` first. That is fine on its own.
2. **A test-only field is dead code.** The field's only reader would be the `#[cfg(test)]` DST test. In the non-test `lib` build, rustc reports ``field `postings_bytes` is never read`` (`dead_code`), and `-D warnings` fails.
   - The same lint already forced `#[expect(dead_code, ...)]` on `Absorber` (`history.rs:754-757`).
   - The reviewer's way to give it a production reader is to publish `POSTINGS_BYTES_WRITTEN` from it after the commit.
3. **Publishing after commit skews the gate ratios.** It moves `bytes_written` to commit time, while `pages_written`, `runs_written` and `canonical_bytes_written` are still counted at stage time (`gather.rs:136,141,497`).
   - `bench/costab/keyed-compare.py:92-96` computes `p_bytes / canon` and `canon + p_bytes + 65 * p_pages` for the 8% and 55% gates.
   - After a failed `write_with_options` or `flush`, the numerator would leave out a batch that the denominator still counts.
   - Doing it consistently means moving all four counters. That changes what four `/v1/debug` values mean on failed commits, and it would be for a defect that exists only in a test. It is offered as D1 (§9).

## 3. Red tests (controls on the current tree) and pinning

Production behaviour does not change. Both committed tests pass on current production code, because the defect was in their oracles, not in the product. The red evidence is therefore four deterministic, **uncommitted** controls. Each one models what a neighbour test does to the process static, or breaks the path under test, and the old and new oracles give opposite verdicts. Undo every control with `git checkout -- <file>` before committing.

Run each control with its `--exact` name, for example: `cargo test --release --lib -- --exact dst::dst_tests::history_gather::keyed_frames_no_longer_count_twice_against_the_budget`.

### C1a: site B, old oracle, neighbour model (current tree, RED)

After `history_gather.rs:562` (`let g1 = ...`) insert:

```rust
crate::history::POSTINGS_BYTES_WRITTEN.fetch_add(4096, Ordering::Relaxed); // one neighbour gather's pages
```

This is exactly what a neighbour's `stage_postings` does at `gather.rs:149`. Expected:

```
thread 'dst::dst_tests::history_gather::keyed_frames_no_longer_count_twice_against_the_budget' panicked at src/dst/tests/history_gather.rs:572:5:
postings bytes must stay within the 8% batch-1 gate: 4168 vs <C>
test result: FAILED. 0 passed; 1 failed
```

Trace:

- 4168 = 4096 + 72. g1 stages 2 pages of 36 B (§1.3), the injection adds 4096, and line 568 subtracts `before`.
- `<C>` is `g1.advanced`'s raw frame bytes, about 34,000.
- The check fails for any C < 52,100, because 416,800 > 8·C.
- The first assertion, `postings > 0`, still passes.

### C1b: site B, new oracle (after commit 1, GREEN)

Insert the same line after `let g1 = ...`, written fully qualified as `std::sync::atomic::Ordering::Relaxed`, because commit 1 removes the file's `Ordering` import. Expected: `test result: ok. 1 passed`. The oracle reads this engine's partition, so the process static no longer takes part.

### C2a: site A, old oracle, planner bypassed (current tree)

Sabotage **S**, in `src/history.rs` `read_history2_keyed_cached`. Directly after `let kh = crate::postings::rk_hash(rk);` (line 1045) insert:

```rust
if from < upto { return read_history2_keyed_envelope(part, route, inc, rk, from, upto, max_bytes).await; }
```

The condition is always true there (line 1043 already returned for `from >= upto`), but the compiler cannot see that, so no unreachable-code or unused-variable warnings appear.

Under S every keyed history page is served by the exact-key envelope:

- `execute_segment` (`application/read.rs:189-199`) serves `[0, absorbed)` from history.
- `wait_all_absorbed` made `absorbed == next`.
- `SCAN_WINDOW` is 4096 offsets, so the drain makes at least 3 history reads.
- The envelope filters by the exact key bytes and completes each window within `MAX_SCAN_BATCH_BYTES` (8 MiB). So `got == expected` still holds.

Results:

- **S alone, run with `--exact`, RED:** `panicked at src/dst/tests/reads_history.rs:241:5:` / `the postings planner path must have served this`. Nothing else in the process has run `canonical_span::read`.
- **S plus the neighbour model, GREEN (this is the vacuity):** before line 241, insert `crate::history::READ_FRAMES_MATCHED.fetch_add(1, Ordering::Relaxed);` (one frame another test matched). Expected: `test result: ok. 1 passed`. In the CI process the old assertion passes with the planner switched off.

### C2b: site A, new oracle (after commit 2)

- **With S, with or without the neighbour line, RED:** `panicked at src/dst/tests/reads_history.rs:246:5:`, followed by:

  ```
  the postings planner must have served a page from this engine's cache: {"bytes":B,"coalesced_waiters":0,"entries":E,"evictions":0,"hits":0,"index_bytes_read":0,"index_loads":0,"misses":0,"prefetch_completed":0,"prefetch_started":0,"warm_extends":X,"warm_installs":Y}
  ```

  - Line 246 applies without the neighbour line, which would shift it by one.
  - The keys are in `serde_json` `BTreeMap` order, since no `preserve_order` feature is enabled.
  - The values that decide the result are `"hits":0` and `"misses":0`: under S, `runs_for` is never called.
  - B, E, X and Y are whatever the absorber's write-through `install_chunk` left behind (B and E are non-zero). They do not affect the verdict.
- **Without S, GREEN:** `hits >= 1`.
  - The first page at `from = 0` finds the `"sp"` slice installed by the first chunk (offset 0 is `"sp"`). It has `covered_from = 0` (segment base 0, born in-process), and the clean warm frontier reaches `absorbed ≥ upto`, so the lookup is `Decision::Hit`.
  - A cold `Lead` would also end in a `Hit` after its load publishes.
  - The idle sweep is 600 s (`POSTINGS_CACHE_IDLE`) and capacity is 64 MiB, so nothing is evicted mid-test.
  - The same mechanism is already pinned at `reads_history.rs:476` (`assert!(cache.hits.load(..) >= 1)`).

### Compile and grep proofs

- After commit 1, `history_gather.rs` has no `Ordering` import. Any process-global `.load(Ordering::…)` reintroduced there fails to compile (E0433).
- `grep -rnE "history::(READ_FRAMES_MATCHED|POSTINGS_BYTES_WRITTEN)" src/dst` returns nothing. The only remaining `history::` static in `src/dst` is `POSTINGS_CORRUPT`, site C (§8).

### Pinning tests (must stay green)

- The two edited tests.
- The rest of both modules (15 tests): `dst_tests::history_gather::` (10) and `dst_tests::reads_history::` (5).
- `postings_cache::` unit tests (the counters read here are unchanged).

## 4. Edits, in commit order

### Ceilinged files

None of these is touched. Budget = growth allowed versus `origin/slate`.

| File | HEAD | base | Budget | Touched |
|---|---:|---:|---:|---|
| `src/http.rs` | 3,366 | 3,369 | +3 | no |
| `src/product.rs` | 4,205 | 4,205 | 0 | no |
| `src/shard.rs` | 3,196 | 3,196 | 0 | no |
| `src/billing.rs` | 2,201 | 2,201 | 0 | no |
| `src/history.rs` | 1,713 | 1,713 | 0 | no (the reason §2 rejects the move) |
| `src/auth.rs` | 1,676 | 1,676 | 0 | no |
| `src/registry.rs` | 1,492 | 1,501 | +9 | no |
| `src/sse/feed.rs` | 1,170 | 1,195 | +25 | no |
| `src/fleet.rs` | 1,143 | 1,143 | 0 | no |

### Touched test files

Both are under the 1,000-line DST ceiling.

- `src/dst/tests/history_gather.rs`: 888 → 901.
- `src/dst/tests/reads_history.rs`: 582 → 588.

### Clippy `too_many_lines`

Counted as clippy counts: body lines between the braces that are neither blank nor comment-only. The count was checked against the existing `#[expect(too_many_lines)]` on `repeated_keyed_reads_hit_the_postings_cache` (110) and against `the_first_advance_seals_the_history_layout` (100, no expect).

| Test | Before | After |
|---|---:|---:|
| `keyed_frames_no_longer_count_twice_against_the_budget` | 54 | 64 |
| `sparse_key_reads_page_with_bounded_spans` | 91 | 93 |

The maximum nesting in the new code is 2 (`for` → `while let`).

### Ratcheted scopes

None is touched:

- Neither edited test carries any `#[expect]`.
- Neither file has a module-level `#![expect]`.
- `dst_tests.rs` declares `mod history_gather;` and `mod reads_history;` bare.
- The nearby `#[expect(clippy::too_many_lines)]` (`reads_history.rs:374`) and `#[expect(clippy::excessive_nesting)]` (`reads_history.rs:52`, `wait_absorbed`) sit on other items and are unchanged.
- No `unwrap_used` or `expect_used` scope exists in either file. Test `.expect` is allowed by `allow-expect-in-tests`.

### Commit 1: the keyed budget test reads its own gather's stored pages

Subject: "The keyed postings budget is judged on its own gather's stored pages, not a process-wide delta". End the message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

`src/dst/tests/history_gather.rs`:

1. Delete line 9, `use std::sync::atomic::Ordering;`. Lines 561 and 568 were its only uses; leaving it would trigger `unused_imports` under `-D warnings`.
2. Delete line 561 (`let before = ...`).
3. Replace line 568 (`let postings = ... - before;`) with:

```rust
    // The gate is judged on the pages this gather stored, read back from
    // this engine's own partition (append_sized routes each stream by its
    // own hash): POSTINGS_BYTES_WRITTEN is process-wide, and every gather
    // a concurrently running test commits moves it.
    let part = engine.history_partition().await.expect("partition");
    let k1 = crate::postings::rk_hash("k1");
    let mut postings = 0u64;
    for (hash, upto, _) in &g1.advanced {
        let route = crate::crypto::RouteHash(*hash);
        let segment = crate::crypto::SegmentHash(*hash);
        let (lo, hi) = crate::postings::postings_range(route, segment, &k1, 0, *upto);
        let mut pages = part.scan(lo..hi).await.expect("postings scan");
        while let Some(page) = pages.next().await.expect("postings page") {
            postings += page.value.len() as u64;
        }
    }
```

Lines 569-574 (the `canonical` sum and both assertions, messages included) stay byte-identical. Every API used here already exists and is used this way in the crate: `history_partition` (`shard.rs:2037`), `Db::scan` (`billing_controller.rs:650`), `DbIterator::next` (`history.rs:1001`), `postings_range` and `rk_hash` (`postings.rs:75`, `:51`, both `pub(crate)`), and the `Copy` newtypes `RouteHash` and `SegmentHash` (`crypto.rs:293`, `:324`).

Ledger in the same commit: `python3 scripts/test-inventory.py --write`. The only change is the `function_sha256` of `keyed_frames_no_longer_count_twice_against_the_budget`. `configuration` is unchanged: no new line matches `FaultPlan::|failpoint|\.require\(|\bseed\b|start_paused|worker_threads`.

### Commit 2: the sparse-key test proves its own engine's planner served it

Subject: "The sparse-key test proves its own engine's planner served it, not a process-wide counter".

`src/dst/tests/reads_history.rs`: replace lines 241-244 with:

```rust
    // Hits on this engine's own slice cache (ShardConfig::default() shares
    // none): each is a page the postings planner served. READ_FRAMES_MATCHED
    // is process-wide, so any other test's keyed read already made it
    // positive.
    let cache = &engine.postings_cache;
    assert!(
        cache.hits.load(Ordering::Relaxed) > 0,
        "the postings planner must have served a page from this engine's cache: {}",
        cache.stats()
    );
```

The `Ordering` import stays; it is still used at lines 353, 364 and 467+. The idiom (`let cache = &engine.postings_cache;`, then `cache.hits.load`, with `cache.stats()` in the message) matches `reads_history.rs:460-476`.

Ledger in the same commit: `python3 scripts/test-inventory.py --write`. The only change is the `function_sha256` of `sparse_key_reads_page_with_bounded_spans`.

The two commits are independent. They can be squashed if preferred, with a single inventory rewrite.

## 5. Mutation analysis

- **No production function body changes, so cargo-mutants generates no mutants in the diff.**
- **Planner selection:** `scripts/quality/verification_plan.py` selects paths in `CRITICAL_PREFIXES` or registered in `mutation_owners.OWNERS`. `src/dst/tests/*.rs` is neither: no `src/dst` prefix, and no owner row names these files. So this diff adds nothing to `mutation_source_files`, and our commits alone yield `"mutants": false`, `"loom": false`, `"miri": false`, `"properties_fuzz": false`, `"compiler": true`. `docs/refactor/test-inventory.json` is not tooling for the planner (`plan()` lines 70-72).
- **No owner rows or filters change.**
- **Side effect:** owner `http_read` (`mutation_owners.py:88`) lists `dst_tests::reads_history::` among its test filters. In the scheduled rotation, a `src/http/read.rs` or read-path mutant that sends keyed history reads around the engine's postings cache is now killed deterministically by `sparse_key_reads_page_with_bounded_spans` (`hits == 0`). Before, a neighbour-incremented static could let it survive. This is only a gain; no disposition is needed.
- A push's plan still covers the 8 commits already ahead of `origin/slate`. Nothing here changes those selections.

## 6. Ledgers

| Ledger | Change |
|---|---|
| `docs/refactor/test-inventory.json` | 2 `function_sha256` values (one per commit), rewritten with `scripts/test-inventory.py --write` |
| `docs/refactor/review-mechanisms.json` | none (no pins on either test) |
| `docs/refactor/test-scenario-map.json`, `SCENARIO-MAP.md`, `scenario-dispositions.json` | none (names unchanged; HIS-016/HIS-017/COST-004 mappings stand) |
| `docs/quality/owners.json`, `source-allowances.json` | none (no static added or removed, no new effect/glob/macro site) |
| `docs/refactor/architecture-policy.json`, `WIRE-MATRIX.md` | none |
| `src/dst/tests/README.md` | none (no new module) |
| `scripts/quality/mutation_owners.py` | none |

## 7. Controls: commands and expected output

Run these after the in-tree mutation and gate runs finish.

1. `cargo fmt --all -- --check`: no output, exit 0.
2. `cargo clippy --locked --workspace --all-targets --message-format=short -- -D warnings`: exit 0. In particular there is no `unused_imports` in `history_gather.rs` and no `too_many_lines` (64 and 93).
3. Run the edited test in commit 1:

   ```
   scripts/test-leg.sh target/legs/item66-gather.log --exact dst::dst_tests::history_gather::keyed_frames_no_longer_count_twice_against_the_budget -- --release --lib keyed_frames_no_longer_count_twice_against_the_budget -- --exact dst::dst_tests::history_gather::keyed_frames_no_longer_count_twice_against_the_budget
   ```

   Expected: `test result: ok. 1 passed`, and the `tests_ran.py` verdict is OK.
4. Run the edited test in commit 2:

   ```
   scripts/test-leg.sh target/legs/item66-sparse.log --exact dst::dst_tests::reads_history::sparse_key_reads_page_with_bounded_spans -- --release --lib sparse_key_reads_page_with_bounded_spans -- --exact dst::dst_tests::reads_history::sparse_key_reads_page_with_bounded_spans
   ```

   Expected: `test result: ok. 1 passed`.
5. Run both whole modules:

   ```
   scripts/test-leg.sh target/legs/item66-modules.log --min 15 -- --release --lib -- dst_tests::history_gather:: dst_tests::reads_history::
   ```

   Expected: `test result: ok. 15 passed; 0 failed`.
6. Run the controls in §3 (C1a, C1b, C2a, C2b), each with its `--exact` name. The expected verdicts and messages are the ones listed there. Revert each with `git checkout -- src/dst/tests/history_gather.rs src/dst/tests/reads_history.rs src/history.rs` and confirm `git status` is clean apart from the commits.
7. `python3 scripts/test-inventory.py --check`: `test-inventory: OK (504 tests, 0 ignored)`. `git show --stat HEAD~1 HEAD -- docs/refactor/test-inventory.json` shows 1 insertion and 1 deletion per commit.
8. `python3 scripts/scenario-map-report.py --check`: OK, unchanged.
9. `grep -rnE "history::(READ_FRAMES_MATCHED|POSTINGS_BYTES_WRITTEN)" src/dst`: no output.
10. Run CI's plan before pushing:

    ```
    cargo build --locked -p streams-quality-syntax && QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=fba7b844 python3 scripts/quality/verification_plan.py --out target/quality-plan
    ```

    Expected: `"changed_rust_files": ["src/dst/tests/history_gather.rs", "src/dst/tests/reads_history.rs"]`, `"mutation_source_files": []`, `"mutants": false`.
    Then repeat with `QUALITY_BEFORE_SHA=fb18840d` to see the real push plan, which includes the 8 prior commits' selections.

## 8. Out of scope

- **Site C (`corrupt_postings_fall_back_to_the_envelope`, `POSTINGS_CORRUPT > before`).**
  - There is no per-engine owner of a corruption count. `PostingsCache` counts `misses` and `index_loads` on corrupt and valid cold loads alike. `POSTINGS_CORRUPT` is incremented only in `history.rs`, which has a zero line budget.
  - A per-engine discriminator exists (`cache.hits == 0` after the sweep: "a corrupt load publishes NOTHING", `postings_cache.rs:696`, `spawn_load` doc). But it proves the envelope path, not "counted".
  - The test also sits at 98 of 100 clippy lines, so adding it means restructuring the test (for example, moving the 27-line `AppendReq` literal that it and the sparse test duplicate into a fixture helper).
  - The existing assertion cannot fail falsely, so leaving it is safe.
  - Follow-up (D2): a per-cache corruption counter.
- **Same defect class, other owners.**
  - `src/rollup/tests.rs:792-800`: `ARTIFACT_MISMATCHES == before + 1` is an *exact* equality on a global delta, so it can fail from a neighbour. It deserves its own item.
  - `dst/tests/billing_maintenance.rs:386-455` (`INGEST_FRAME_BYTES_TOTAL` and `ABSORBED_FRAME_BYTES_TOTAL`, documented "grew by at least") and `dst/tests/billing_controller.rs:372-394` / `runtime_sweep.rs:831` (`WALK_CLOSE_SUBMITS` lower bounds) only ever pass on neighbour activity; they never fail from it.
- **The sparse test doc's span bound "≤ 8 per response" is not asserted.** The only runtime signal, `READ_SPANS_MAX`, is process-wide (`fetch_max`). The bound is structural in `PlanCfg` and property-pinned by `src/postings/tests.rs:134` `fragmented_key_stays_exact_and_bounded` (HIS-017).
- **Duplicate absorbed-byte counters.**
  - `CANONICAL_BYTES_WRITTEN` (stage time, `gather.rs:497`) and `ABSORB_BYTES_TOTAL` (after commit, `gather.rs:561`) count the same `chunk_raw` bytes and differ only on a failed commit.
  - All four `*_WRITTEN` counters are published at stage time, so a batch whose `write_with_options` or `flush` fails is counted, and counted again when retried.
  - This is telemetry accuracy, not test isolation. See D1.

## 9. Decisions for Søren

This plan changes no product, raw, wire or metric edge, so none is required. Two optional decisions, each with the backward-compatible alternative stated:

- **D1: the typed per-gather fact the reviewer asked for.**
  - Move `GatherOutcome` verbatim from `history.rs:715-736` into `history/gather.rs` (history.rs 1,713 → 1,690).
  - Carry `postings_bytes`, `postings_pages` and `postings_runs` on it.
  - Publish all four `*_WRITTEN` counters in `commit()` beside `ABSORB_BYTES_TOTAL`, with `CANONICAL_BYTES_WRITTEN` taken from `absorbed_bytes`. The test then asserts on `g1.postings_bytes`. The red is compile-level: ``error[E0609]: no field `postings_bytes` on type `GatherOutcome` ``.
  - **What changes:** `/v1/debug` `postings.{bytes,pages,runs,canonical_bytes}_written` would count committed batches only. The JSON shape stays the same. The meaning changes only on failed commits, and the `keyed-compare.py` ratios stay consistent.
  - **Backward-compatible alternative:** this plan (stage-time telemetry kept, oracle in the test).
  - **Why only all four:** moving bytes alone, as the review literally says, would skew the 8% and 55% gate ratios on failed commits.
- **D2: make site C's "corruption must be counted" meaningful in the suite.**
  - Add a per-cache corruption counter, counted where `load_runs` returns `corrupt = true` (`postings_cache.rs:917,921,941`). Expose it as `corrupt_loads` in `PostingsCache::stats()`, which is an additive key in `/v1/debug` `postings.cache`.
  - Then assert its delta in the test (restructured below 100 lines).
  - `postings_cache.rs` is critical and registered (owner `postings_cache`, filter `postings_cache::`), so this needs killing tests for the in-diff `load_runs` and `stats` mutants.
  - **Backward-compatible alternative:** keep the global lower bound, which cannot fail falsely.

---

## Skeptic corrections (C1..C9)

I checked every claim against the tree read-only (`fba7b844`). These hold as written: the static declarations (`history.rs:887,893,894`); both sites' quoted lines (`reads_history.rs:241-244`, `history_gather.rs:9,561-574`); the complete use-site list (my own grep of `src/`, which includes `src/dst` and `cfg(test)`, turned up no other reader or writer of the three statics); the 36 B page arithmetic (`postings.rs:150-172`); the fact that `READ_FRAMES_MATCHED` is reached only through `execute_postings_plan` (`postings_read.rs:53` is the sole `canonical_span::read` caller); the `hits` semantics (`postings_cache.rs:581-588`, the sole `hits.fetch_add`, which always returns `Runs` into `execute_postings_plan` at `history.rs:1055-1057`); the fresh per-engine cache (`shard.rs:1080,1457-1458`); `route == hash` for `append_sized` (`fixture_storage.rs:210` → `transaction/append.rs:234`); and the claim that no `#[expect]` scope covers either edited test (the only expects are `reads_history.rs:52,374` and `history_gather.rs:340`, all on other items, with no `#![expect]`). `mod dst` is `#[cfg(test)]` (`lib.rs:33-34`), so cargo-mutants generates no mutants from these files. Neither path is in `CRITICAL_PREFIXES` or `mutation_owners.OWNERS` (verification_plan.py:22-31,86-90). There are no `review-mechanisms.json` pins. `scenario-map-report.py --check` validates symbols only, not line numbers (lines 208-219), so the shifted line numbers in `test-scenario-map.json` do not matter. The inventory's `configuration` regex (`test-inventory.py:126-127`) matches no new line, and `mechanisms` only matches `mech::`. `serde_json` has no `indexmap` dependency in `Cargo.lock`, so the `stats()` key order is sorted, as C2b states. Both controls C2a and S compile: `read_history2_keyed_envelope` takes exactly `(part, route, inc, rk, from, upto, max_bytes)` and returns the same tuple type (`history.rs:1069-1078`).

**C1: the base moved. `origin/slate` is now `fba7b844` (it was pushed; `git rev-parse origin/slate` = HEAD). The merge base is HEAD, and nothing is ahead.**
- The budget table in §4 is stale. `http.rs` is 3,366 against base 3,366 (budget 0, not +3). `registry.rs` is 1,492/1,492 (budget 0, not +9). `sse/feed.rs` is 1,170/1,170 (budget 0, not +25). None of the three is touched, so nothing breaks, but the table must say 0.
- §7 step 10: drop the second run with `QUALITY_BEFORE_SHA=fb18840d` and the "8 prior commits' selections" sentence. The push plan base is whatever `origin/slate` is at push time. Today that is `fba7b844`. If sibling items land first, it is their tip.
- Line 3 and §5's last bullet ("8 ahead") are stale.

**C2: C1a's red output can be stated exactly, and the §1.3 budget numbers are off.**
- Each frame is 16,441 B. `encrypt_with_nonce` (`crypto.rs:517-553`) builds a header of 1+8+8+4+2+2 (`"k1"`)+12 = 37 B, then a 4 B length and the ciphertext, which is 16,384 plaintext + 16 tag.
- Compression is off: `ShardConfig::default()` sets `frame_compression: Disabled` (`shard.rs:1087`), and the test uses that default.
- So C = 32,882. The 8% gate allows 2,630 B, and the slack over g1's 72 B is 2,558 B (§1.3 says "~2,720" and "~2,650").
- The varint for 16,441 is 3 B, so the 36 B page stands.
- The exact C1a output is `postings bytes must stay within the 8% batch-1 gate: 4168 vs 32882` at `src/dst/tests/history_gather.rs:572:5`. Replace `<C>` with 32882.
- The neighbour argument still holds: 96 × 35 B = 3,360 > 2,558.

**C3: no control proves that the new site-B upper bound is live.**
- C1b only shows that the new oracle ignores the static. Nothing shows it still goes red when the stored pages exceed the gate.
- Add C1c, uncommitted, applied after commit 1. Directly after `let g1 = ...;`, write one extra 4,096 B value into this engine's partition at a k1 postings key, using the idiom at `reads_history.rs:335-350`:
  - key: `postings_key(RouteHash(hashes[0]), SegmentHash(hashes[0]), &rk_hash("k1"), 0, 1)`
  - write with `WriteBatch` and `write_with_options(.., &WriteOptions::default())`, then `part.flush()`
- Expected RED: `postings bytes must stay within the 8% batch-1 gate: 4168 vs 32882` at the assertion's shifted line. Pin the line after writing the control.
- Its old-oracle counterpart is GREEN, because the static never sees a direct write. That is the discriminating pair showing the new oracle measures *stored* bytes, which is what HIS-016 needs.

**C4: the new site-B oracle is narrower than the one it replaces.**
- It scans only `rk_hash("k1")`. The old global delta, in isolation, counted every page the gather staged.
- A regression that also emits pages for another key would pass the new test unseen. Examples: a stray default-key page, or a frame noted under two hashes.
- Fix: scan the stream's whole postings keyspace instead:
  - `lo = postings_key(route, segment, &crate::crypto::RoutingKeyHash([0; 16]), 0, 0)`
  - `hi = postings_key(route, segment, &crate::crypto::RoutingKeyHash([0xFF; 16]), u64::MAX, u64::MAX)`
  - scan `lo..=hi`
- `RoutingKeyHash` is `pub(crate)` with a `pub` field (`crypto.rs:361`). The `'p'` tag byte after route‖segment (`postings.rs:62-64`) keeps canonical record keys out of the range.
- The equality with `PageBuilder::finish`'s total then holds without the "each stream holds only k1 frames" precondition, and `let k1` goes away.
- Recount the file (≈901-903) and the function (≈64-66 lines, still < 100) after rustfmt.

**C5: wrong citation for the `Db::scan` precedent.** `billing_controller.rs:650` is `src/dst/tests/billing_controller.rs:650` (there is no `src/billing_controller.rs`). The production precedent with a `Vec<u8>` range is `src/shard.rs:511`. The closest precedent for the whole edit (`history_partition()` plus a postings key in a DST test) is `src/dst/tests/reads_history.rs:335-350`. Cite that.

**C6: the commit 1 comment is inaccurate.** It says "every gather a concurrently running test *commits* moves it". The static moves at *stage* time (`gather.rs:149` in `stage_postings`), before `commit()` writes anything (`gather.rs:531-554`). Say "stages" instead. The point is that a neighbour moves it even when its commit later fails.

**C7: commit 2 is also missing the attribution trailer.** Commit 2 must end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, like commit 1. The same applies if the two commits are squashed.

**C8: D2 cost is under-stated (optional decision only).**
- `src/postings_cache.rs` is 950 lines, so the ceiling leaves 50 lines of headroom.
- `PostingsCache::stats` sits under a fn-scope `#[expect(clippy::unwrap_used)]` (`postings_cache.rs:233-236`). Adding a `corrupt_loads` load inside it adds new fingerprinted `unwrap_site:path` and `ordinary-call` keys (`source_rules.py:160-185`), so the check fails with "accepted exception grew". The fix is to re-decide the reason text, or read the counter outside the scope.
- `load_runs` carries `#[expect(clippy::too_many_arguments)]` (`postings_cache.rs:888-891`), and its `syntax_facts` ratchet grows with any added increment. The same remedy applies.
- The additive `/v1/debug` `postings.cache.corrupt_loads` key is a contract change for Søren, which D2 already says.

**C9: the out-of-scope list is correct but should be surfaced.** `src/rollup/tests.rs:792-800` asserts `ARTIFACT_MISMATCHES == before + 1`, an exact equality on a global. It is written at `billing.rs:1279` and read at `http.rs:1973`. It can fail falsely whenever another rollup test publishes a mismatched artifact at the same time. Record it as a numbered follow-up item, not just a bullet.

### Verdict

**ready-with-corrections.** The plan is test-only, touches no ceilinged file, adds no ratcheted scope, key or mutant, and its only ledger (the `test-inventory.json` sha rewrite, one per commit) is correct. Controls C1a, C1b, C2a and C2b are buildable and deterministic, and my traces give the verdicts the plan predicts. Before execution, apply C1 (the base and budgets are stale), C2 (the exact red value), C3 (add C1c) and C4 (widen the scan). C5 to C7 are text fixes.
