# Item 35: one stream's unreadable row fails the whole gather and backs off its lane

Tree: `slate` @ `6669d3b5`. That is 10 commits ahead of `origin/slate` = `aaf2baa5`, which is also the merge base. There is an uncommitted `scripts/quality/mutation_owners.py` edit (offsets/segmap/telemetry_batch rows); it is not ours and must not be touched. The unpushed commits do not change any file this plan edits: `git diff origin/slate --stat` lists none of `src/history*`, `src/shard/record*` or `src/dst/tests/history_gather.rs`. Every file named below equals the merge base. Line numbers are current; the reviewer's are partly stale. I only read the tree. Nothing was built or run.

**Summary.** The problem is real. There are four commits:

- C1 moves `GatherOutcome` verbatim. This makes `history.rs` headroom, and the move is the only change to that file.
- C2 is a typed read-error refactor in `src/shard/record.rs`.
- C3 is a verbatim extraction in `worker.rs` that deletes the `gather_due` nesting exception.
- C4 is the fix. It adds `GatherOutcome.failed` with a typed `StreamGatherFailure`, isolates rows that fail admission and postings pages that fail their self-check, and validates before any `WriteBatch` put. It also adds per-stream backoff, a per-stream warn, and a runtime-scoped counter.

There is no wire change. The only mutation-selected file is `src/shard/record.rs`, and its in-diff mutants are all unviable. The only ledger is `docs/refactor/test-inventory.json`.

The reviewer's "isolate `ErrorKind::Data`" is **wrong** as stated (§2.1). The rest of the Change is right.

---

## 1. Problem (verified)

### 1.1 The gather aborts on the first per-stream error

`src/history/gather.rs:350-354`. The reviewer cited 342-347, which is stale; those lines are now the `deferred_budget` extend:

```rust
            let got = self.read_wave(wave, per_stream).await;
            self.pace_between_waves(&mut pacing).await;
            for (plan, read) in wave.iter().zip(got) {
                self.stage_chunk(&mut staged, reservation, plan, &read?)?;
            }
```

There are two abort points, and neither says which stream failed:

- **`&read?`**. `read_wave` (`gather.rs:425-434`) returns `Vec<Result<FrameReadResult, slatedb::Error>>` from `read_frames_range`. A stored row that fails admission is refused in `src/shard/record.rs:171-172`:

  ```rust
      while let Some(kv) = iter.next().await? {
          let frame = CheckedFrame::from_row(&kv.key, &prefix[..17], kv.value)?;
  ```

  The `?` goes through `record.rs:24-28`:

  ```rust
  impl From<RecordCorruption> for slatedb::Error {
      fn from(error: RecordCorruption) -> Self {
          Self::data(error.to_string())
      }
  }
  ```

  So the typed `RecordCorruption` is **flattened** into `slatedb::Error { kind: Data, msg: "stored record corruption: Frame" }`. After that it cannot be told apart from any other slatedb `Data` error without string matching.
- **`stage_chunk(..)?`**. `stage_chunk` (`gather.rs:453-508`) fails only through `stage_postings(...)?` at `gather.rs:496`. That call runs the two self-checks at `gather.rs:139-143` (`"postings page failed self-decode during gather"`, `"overlapping postings during gather"`). It runs **after** `stage_rows` has already put the chunk's canonical rows into the shared batch (`gather.rs:494-496`), and after `POSTINGS_PAGES_WRITTEN` has been bumped (`gather.rs:136`):

  ```rust
          let mut pages = PageBuilder::default();
          let last = stage_rows(&mut staged.wb, plan, chunk, &mut pages);
          let runs = stage_postings(&mut staged.wb, plan, pages)?;
  ```

Either error drops the whole `Staged` batch. Every lane-mate read in the same wave is discarded, including the ones already staged.

### 1.2 The worker backs off the whole lane and logs only a count

`src/history/worker.rs:398-413` (the reviewer's 398-413 is exact):

```rust
                Err(e) => {
                    let msg = e.to_string();
                    if absorb_error_is_fence(&e) {
                        tracing::warn!("v2 gather fence-class ({} streams): {msg}", v2_lane.len());
                        // Engine is dying; the exit path clears pending.
                    } else {
                        tracing::warn!("v2 gather failed ({} streams): {msg}", v2_lane.len());
                        for h in v2_lane {
                            if let Some(p) = pending.get_mut(h) {
                                p.failures = p.failures.saturating_add(1);
                                let shift = p.failures.min(6);
                                p.retry_after = Some(now + self.cfg.tick * 2u32.pow(shift));
                            }
                        }
                    }
                }
```

The message names no stream, because `RecordCorruption`'s Display is `"stored record corruption: {self:?}"`. Every lane member gets the **same** `retry_after`, so they are all due again on the same tick (`due_streams`, `history.rs:703`). They are sorted into the same lane (`classify_due`, up to `V2_LANE_PER_TICK = 1024`) and fail again. The corrupt stream's lane-mates therefore stall until the lane cap happens to split them away from it. Streams that become due at the same instant get pulled in as well. The worst case is 1,024 streams stalled by one row. They back off at tick·2^6 = 320 s, and the reads are repeated and discarded every time.

### 1.3 `GatherOutcome` has no per-stream failure class

`src/history.rs:715-735`. The reviewer's 714-734 is off by one:

```rust
#[derive(Default)]
pub(crate) struct GatherOutcome {
    pub(crate) advanced: Vec<([u8; 16], u64, u64)>,
    pub(crate) no_work: Vec<[u8; 16]>,
    pub(crate) deferred_budget: Vec<[u8; 16]>,
    ...
    pub(crate) partial: Vec<([u8; 16], u64)>,
}
```

### 1.4 Complete use-site list

| Item | Sites |
|---|---|
| `read_frames_range` | defined `record.rs:129`; re-exported `shard.rs:23`; **only production caller** `gather.rs:432` (`read_wave`); tests `record_scan_tests.rs:127,168`, `reads_ring.rs:149,165,258,269,370` (all `.expect/.unwrap` → need `Debug`) |
| `From<RecordCorruption> for slatedb::Error` | `record.rs:24`; used by `?` at `record.rs:172` (read_frames_range) and `record.rs:287` (`read_frames_until`, the product/raw read path whose error string reaches `application/read.rs:243` `.map_err(|e| e.to_string())`) |
| `RecordCorruption` | `record.rs:12-28,36-72`; `record/checked.rs:3,22,31`; `record_scan_tests.rs:184-…` |
| `stage_chunk` | defined `gather.rs:453`; called `gather.rs:353` only |
| `stage_rows` / `stage_postings` | `gather.rs:96` / `129`; called `gather.rs:495` / `496` only |
| `GatherOutcome` | defined `history.rs:722`; used only in `gather.rs:12,63,290,315,323,371,531` (the worker gets it by inference; there are no path uses in `src/dst`) |
| `PendingAbsorb.failures/retry_after` writes | `worker.rs:71-72` (signal), `381-382` (partial reset), `386-388` (partial insert), `408-410` (lane backoff); `gather.rs:221-222` (seed); read `history.rs:703`; tests `bounded_discovery_tests.rs:17-18,114-115` |
| `absorb_error_is_fence` | `history.rs:742`; `worker.rs:400`; tests `history.rs:1188-1194` |

### 1.5 Neighbours I checked that are *not* part of this problem

- `plan_reads` (`gather.rs:368-382`) calls `self.shard.stream_handle(*hash).await?`. A corrupt tail row (`shard.rs:156-168`, `slatedb::Error::data("invalid persisted tail encoding")`) would abort the lane too. But `classify_due` has already dropped any stream whose handle fails (`worker.rs:250-252`, `let Ok(handle) = … else { continue; }`). So the lane reaches the gather with resident handles, and the path is only reachable if a handle is evicted between classify and plan. That case is out of scope (§8).
- The ring path (`tail_ring.rs:155`, `from_ring(...).ok()?`) turns a malformed ring entry into a miss, then a DB scan. It never errors.

---

## 2. Contract decision

### 2.1 What is isolated, typed

The reviewer's Change says "isolate `ErrorKind::Data`". **That is wrong.** At the pinned slatedb rev (`0717cc1`, `slatedb/src/error.rs:640-730`), `ErrorKind::Data` also covers:

- `object_store::Error::NotFound` (`:645`). An SST collected under a concurrent compaction is transient and has nothing to do with one stream.
- `ChecksumMismatch` (`:726`) and `BlockDecompressionError` (`:709`). These are SST-block faults that span many streams' rows.
- `ManifestMissing` (`:716`), `InvalidDBState`, and `WalTruncated`.

None of these is deterministic for one stream. Isolating them would blame a stream's data for a store fault. And after `record.rs:24-28` flattens `RecordCorruption`, "the row's own bytes are wrong" can only be recovered by string matching.

The contract, which is buildable and typed:

1. **`src/shard/record.rs`:** a new `RangeReadError { Corrupt(RecordCorruption), Store(slatedb::Error) }` is the error of `read_frames_range`. The gather is its only production caller. `Corrupt` is built only by `From<RecordCorruption>`, which is used only at the `from_row(..)?` site. `read_frames_until` and `From<RecordCorruption> for slatedb::Error` are **unchanged**, so the product/raw read error text is byte-identical.
2. **`src/history/gather.rs`:** a new `StreamGatherFailure { Corrupt(RecordCorruption), PostingsSelfDecode, PostingsOverlap }`. It is carried in a new `GatherOutcome.failed: Vec<([u8; 16], StreamGatherFailure)>`.

   Each verdict is a pure function of that stream's own durable bytes, so the same read fails the same way on every retry:
   - `Corrupt`: the row passed slatedb's block checksums but fails frame admission.
   - `PostingsSelfDecode` and `PostingsOverlap`: `PageBuilder`, `encode_page` and `decode_page_abs` are deterministic in the chunk's admitted frames.

   **Everything else still aborts the whole gather exactly as today.** That includes every `slatedb::Error` kind (among them `Data`, `NotFound`, checksum and fence/`Closed`), `history_partition()`, `stream_handle` in `plan_reads`, and the batch write and flush. `absorb_error_is_fence` still sees the untouched `slatedb::Error` in the anyhow chain, because the `Store` arm converts the inner error, never the enum.
3. **Validate before any put.** A `Corrupt` read never reaches `stage_chunk`. For the postings verdicts, `stage_chunk` builds and self-checks the chunk's pages before `staged.wb` is touched. `check_postings(pages: PageBuilder) -> Result<ChunkPostings, StreamGatherFailure>` takes no `WriteBatch`. The only puts, `stage_rows` and `stage_postings(.., ChunkPostings)`, run after it returned `Ok`, and `stage_postings` can only get a `ChunkPostings` from `check_postings`. That ordering is a compile-level proof.

   Without the reorder, an isolated postings refusal would still commit orphan canonical rows plus a prefix of the chunk's pages. Stale pages at a different `page_first` in the same bucket would later read as overlapping pages, which means `POSTINGS_CORRUPT` and an envelope fallback.
4. **Worker.** The streams in `failed` stay pending and back off alone, with the same tick·2^min(n,6) formula now in one `back_off` helper. Each gets one named warn: stream hash prefix, typed reason, failure count. Their lane-mates settle normally. The lane-wide `Err` arm is unchanged.
5. **Counter.** `HistoryResources.gather_streams_failed: AtomicU64` counts `failed.len()` per gather. `HistoryResources` is runtime-scoped in production (`shard.rs:1441`, `cfg.shared_history`) and per-engine in fixtures. So the counter is **not** a process global:
   - No owners.json `global` row is needed.
   - DST tests can assert it exactly. Item 66 removed DST asserts on process-global history counters because they could never fail under the parallel harness.
   - It sits where `/v1/debug/absorb` already reads (`state.runtime.history.budget…`, `http.rs:1382-1396`).

### 2.2 Wire and metrics: **no wire change**

- No status code, body, `/metrics` name or `/v1/debug` JSON shape changes. `read_frames_until` and its `From` impl are untouched, so HTTP read errors keep their text.
- The counter is not exposed. Exposing it is Decision 1 (§9).
- `/v1/debug/load` `postings.pages_written/runs_written/bytes_written` keep their shape. In C4 the three `fetch_add`s move after the self-check, so pages of a refused chunk are no longer counted as "written". Only an encoder defect reaches that branch. Before, such a gather aborted after counting pages that were never written. This is a correction of the value, not a shape change.
- Logs: one new warn line per failed stream. The existing lane-wide warns are byte-identical.

---

## 3. Red tests

### 3.A `dst::dst_tests::history_gather::one_corrupt_row_fails_only_its_stream` (new, `src/dst/tests/history_gather.rs`, appended; this is the reviewer's First step)

Change the fixture import to `use super::fixture_storage::{append_sized, mem, open_engine, open_engine_with_settings, skey, wait_all_absorbed};`. That line stays within 100 columns, so there is no added line.

```rust
/// Item 35: a stored row that fails admission fails identically on every
/// retry, so it may cost only its own stream; the lane-mates read beside
/// it advance in the same flush and the corrupt stream's boundary holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_corrupt_row_fails_only_its_stream() {
    let store = mem();
    let key = skey();
    let (a, bad, c) = ([0xB1u8; 16], [0xB2u8; 16], [0xB3u8; 16]);
    // ShardConfig::default(): ring off, per-engine HistoryResources.
    let engine = open_engine(store.clone(), "dst-corrupt-row").await;
    for h in [a, bad, c] {
        append_sized(&engine, h, &key, "", 1024).await;
    }
    engine
        .db
        .put(crate::shard::record_key(&bad, 0), b"invalid frame")
        .await
        .expect("overwrite the row")
        .await_durable()
        .await
        .expect("durable overwrite");
    let absorber = crate::history::Absorber::new(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig::default(),
    );
    let outcome = absorber
        .absorb_gather_v2(&[a, bad, c])
        .await
        .expect("a corrupt row in one stream must not fail the lane's gather");
    let advanced: Vec<[u8; 16]> = outcome.advanced.iter().map(|(h, _, _)| *h).collect();
    assert_eq!(advanced, vec![a, c], "the corrupt stream's lane-mates advance in the same flush");
    assert!(
        matches!(
            outcome.failed.as_slice(),
            [(h, crate::history::StreamGatherFailure::Corrupt(
                crate::shard::record::RecordCorruption::Frame
            ))] if *h == bad
        ),
        "only the corrupt stream is left out, with the row's own reason"
    );
    assert!(outcome.no_work.is_empty() && outcome.deferred_budget.is_empty() && outcome.partial.is_empty());
    let counted = engine.history_resources.gather_streams_failed.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(counted, 1, "the engine's own resources count the one stream left out");
    wait_all_absorbed(&engine, &[a, c]).await;
    let handle = engine.stream_handle(bad).await.expect("handle");
    assert_eq!(handle.state.lock().unwrap().durable.absorbed, 0, "the corrupt stream's boundary holds");
    engine.begin_close();
}
```

Budget: about 50 lines, taking the file from 906 to ≤ 960. The hard cap is 1,000 (94 lines).

**Red on HEAD (and after C1-C3).** As written, the test does not compile on HEAD:

- `error[E0609]: no field 'failed' on type 'GatherOutcome'`
- `error[E0433]: failed to resolve: could not find 'StreamGatherFailure' in 'history'`
- `error[E0609]: no field 'gather_streams_failed' on type 'Arc<HistoryResources>'`

For the runtime red, run the test with the `failed` `matches!` and the `counted` lines removed. Everything above the first new-API line compiles on HEAD. Expected:

```text
thread 'dst::dst_tests::history_gather::one_corrupt_row_fails_only_its_stream' panicked at src/dst/tests/history_gather.rs:<L>:<C>:
a corrupt row in one stream must not fail the lane's gather: Data error: stored record corruption: Frame
```

Trace:

1. `absorb_gather_v2` → `absorb_gather_v2_with` → `plan_reads`. All three plans are `from 0, upto 1`. `read_par` is 8, so there is one wave.
2. The zip loop stages `a`. Then `bad`'s read goes `read_frames_range` → `ring_read` returns `None` (ring disabled, `tail_ring_bytes: 0`, `shard.rs:1086`) → DB scan → `CheckedFrame::from_row` → `decode_row` passes key width and namespace → `decode_at(b"invalid frame", 0)`.
3. `decode_frame` fails, or `ciphertext.len() < 16` because the value is 13 bytes. Either way the result is `RecordCorruption::Frame` (`record.rs:56-66`).
4. `From` produces `slatedb::Error::data("stored record corruption: Frame")`. `&read?` (`gather.rs:353`) turns it into `anyhow::Error`.
5. `.expect` prints `anyhow`'s Debug, which is the slatedb Display `"{kind}: {msg}"` = `Data error: stored record corruption: Frame`. The error has no source chain. A stack backtrace follows only if `RUST_BACKTRACE` is set.

### 3.B `history::worker::lane_isolation_tests::corrupt_row_backs_off_only_its_stream` (new file `src/history/worker/lane_isolation_tests.rs`, declared at the end of `worker.rs`)

It uses explicit imports and no glob, so it needs no `unresolved-glob` owners row. It **compiles on HEAD**. It needs only `Absorber::new`, the private `gather_due` (visible to a child of `worker`), and `PendingAbsorb` with its private fields (visible to descendants of `history`).

```rust
//! Item 35: a stream whose stored row fails admission backs off alone;
//! the lane-mates gathered beside it retire with the same flush.
use super::{Absorber, PendingAbsorb};
use crate::history::{AbsorberConfig, KeyCache, absorber_channel};
use crate::shard::{AppendFinish, AppendReq, ShardConfig, ShardEngine, record_key};
use bytes::Bytes;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn append_one(engine: &ShardEngine, hash: [u8; 16]) {
    let (reply, ack) = tokio::sync::oneshot::channel();
    let request = AppendReq {
        usage: Default::default(),
        hash,
        // Current-format streams carry a name-level route from the first append.
        route: hash,
        enqueued_at: Instant::now(),
        entries: vec![Bytes::from_static(b"lane isolation payload")],
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 1,
        subkey: [7; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 22,
        finish: AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp: reply,
    };
    assert!(engine.try_enqueue(request).is_ok(), "enqueue");
    ack.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_row_backs_off_only_its_stream() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = slatedb::Db::builder("item35-lane", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let (absorb_tx, _absorb_rx) = absorber_channel();
    let engine = ShardEngine::start(
        "item35-lane".into(), Arc::new(db), store.clone(), ShardConfig::default(),
        absorb_tx, None, Default::default(),
    );
    let (a, bad, c) = ([0xC1; 16], [0xC2; 16], [0xC3; 16]);
    for hash in [a, bad, c] {
        append_one(&engine, hash).await;
    }
    engine.db.put(record_key(&bad, 0), b"invalid frame").await.unwrap().await_durable().await.unwrap();
    let cfg = AbsorberConfig::default();
    let tick = cfg.tick;
    let absorber = Absorber::new(store, engine.clone(), Arc::new(KeyCache::default()), cfg);
    let now = Instant::now();
    let entry = || PendingAbsorb { bytes: 1, since: now, failures: 0, retry_after: None };
    let mut pending: HashMap<_, _> = [a, bad, c].into_iter().map(|h| (h, entry())).collect();
    absorber.gather_due(&mut pending, now, &[a, bad, c]).await;
    assert!(
        !pending.contains_key(&a) && !pending.contains_key(&c),
        "a lane-mate of a corrupt row must retire with the flush that absorbed it"
    );
    let backoff = |p: &PendingAbsorb| (p.failures, p.retry_after);
    assert_eq!(pending.get(&bad).map(backoff), Some((1, Some(now + tick * 2))), "the corrupt stream backs off alone");
    let later = now + tick * 2;
    absorber.gather_due(&mut pending, later, &[bad]).await;
    assert_eq!(pending.get(&bad).map(backoff), Some((2, Some(later + tick * 4))), "its backoff doubles on its next failure");
    engine.begin_close();
}
```

Budget: about 100 lines, in a new file with a 1,000 cap. The second call covers the lone-lane case: the gather returns `Ok` with `advanced` empty and `failed = [bad]`, taking the early return at `gather.rs:358`.

**Red on HEAD (runtime).**

```text
thread 'history::worker::lane_isolation_tests::corrupt_row_backs_off_only_its_stream' panicked at src/history/worker/lane_isolation_tests.rs:<L>:<C>:
a lane-mate of a corrupt row must retire with the flush that absorbed it
```

Trace:

1. `gather_due` reserves the budget, then `absorb_gather_v2_with` returns `Err(Data error: stored record corruption: Frame)`, exactly as in §3.A.
2. `worker.rs:398` → `absorb_error_is_fence` is false (kind `Data`, not `Closed`) → `worker.rs:405-412` sets `failures = 1, retry_after = now + tick·2` on **a, bad, c**.
3. `a` is still in `pending`, so the first assert fails.

### 3.C `history::gather::postings_refusal_tests::{pages_that_fail_their_self_decode_refuse_the_chunk, pages_that_overlap_refuse_the_chunk}` (new file `src/history/gather/postings_refusal_tests.rs`, declared at the end of `gather.rs`)

```rust
//! Item 35: a chunk whose postings pages fail their own round trip is
//! refused whole, before any of its rows or pages reach the shared batch.
use super::{StreamGatherFailure, check_postings};
use crate::postings::{BUCKET_OFFSETS, PageBuilder, rk_hash};

#[test]
fn pages_that_fail_their_self_decode_refuse_the_chunk() {
    let mut pages = PageBuilder::default();
    // A zero-byte frame yields a run with no matching bytes, which the page
    // decoder refuses: the encoder's own output fails its round trip.
    pages.note_frame(rk_hash(""), 0, 0);
    assert_eq!(check_postings(pages).err(), Some(StreamGatherFailure::PostingsSelfDecode));
}

#[test]
fn pages_that_overlap_refuse_the_chunk() {
    let mut pages = PageBuilder::default();
    // Out of order across a bucket seam: each page decodes, but the second
    // starts below the first one's end.
    pages.note_frame(rk_hash(""), BUCKET_OFFSETS + 1, 10);
    pages.note_frame(rk_hash(""), 0, 10);
    assert_eq!(check_postings(pages).err(), Some(StreamGatherFailure::PostingsOverlap));
}
```

Traces:

- **Self-decode.** `note_frame(k, 0, 0)` gives `record_count 1, matching_frame_bytes 0`. `encode_page` then `decode_page` returns `None` at `postings.rs:212`, so `decode_page_abs` returns `None`, which maps to `PostingsSelfDecode`.
- **Overlap.** In `note_frame`, the bucket change closes page `(1, B+1)` and opens `(0, 0)`. The `debug_assert` at `postings.rs:409` holds because `run_next` was reset. `finish` emits `[(B+1…), (0…)]`, and both pages decode. `append_page_runs`: `prev_end = B+2 > 0` returns `None` (`postings.rs:265-270`), so the result is `PostingsOverlap`.

**Red:** on HEAD, E0432, unresolved imports `super::StreamGatherFailure` and `super::check_postings`. These paths do not exist on HEAD. The two tests pin the new typed classification. With admitted frames, offsets are strictly ascending and `len > 0`, so neither refusal can be reached end to end. I am recording that honestly: the `stage_chunk` arm that pushes a postings refusal is covered by these unit tests plus the type-level ordering in §2.1.3. No runtime scenario covers it.

### 3.D Pins for the refactors (C1-C3) and the valid path of the reorder (C4)

- **C1 (verbatim move).** `cargo check`. `git show C1 --color-moved=zebra` shows the 21 `GatherOutcome` lines as moved, not changed. `rg -n GatherOutcome src` then lists only `gather.rs`.
- **C2 (typed read error).**
  - `shard::record_scan_tests::r08a_database_record_corruption_refuses_progress_without_mutation` has its absorber assertion strengthened to `Err(RangeReadError::Corrupt(_))`; it covers all four corruption shapes.
  - `shard::record_scan_tests::r08a_valid_filtered_miss_keeps_legitimate_progress`.
  - `dst::dst_tests::reads_ring::` (all five `read_frames_range` call sites).
  - Every `dst::dst_tests::history_gather::` test and `history::tests::absorber_exits_when_shard_engine_is_fenced` (fence classification through the `Store` arm).
  - Compile proof: `read_frames_until`'s signature and `From<RecordCorruption> for slatedb::Error` are unchanged, and `src/application/` is untouched.
- **C3 (verbatim extraction).**
  - `history::tests::absorber_exits_when_shard_engine_is_fenced` (fence arm).
  - `dst::dst_tests::history_absorption::absorber_drains_records_larger_than_the_per_stream_gather_cap` (partial arm).
  - `…::absorber_sweep_recovers_streams_whose_signals_were_lost` (advanced retire).
  - `…::tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch`.
  - `dst::dst_tests::history_recovery::budget_deferred_streams_absorb_on_the_next_tick` (deferred stays pending).
  - 3.B (lane backoff arithmetic; it fails identically before and after C3).
  - `git show C3 --color-moved=dimmed-zebra -w`.
- **C4 valid path (rows and pages still staged identically).**
  - `dst::dst_tests::reads_history::sparse_key_reads_page_with_bounded_spans`, `repeated_keyed_reads_hit_the_postings_cache`, `keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records`, and `corrupt_postings_fall_back_to_the_envelope`.
  - `dst::dst_tests::history_gather::keyed_frames_no_longer_count_twice_against_the_budget`, `v2_gather_packs_to_the_aggregate_budget`, `an_oversized_chunk_gathers_alone`, and `a_refused_oversized_chunk_defers_and_sizes_the_next_reservation`.
  - `dst::dst_tests::history_absorption::acked_records_survive_absorption_into_history`.

---

## 4. Edits, file by file, in commit order

Ceilinged files: every file over 1,000 lines may not grow by one line against `origin/slate`. Current counts:

| File | Lines | Touched by this plan |
|---|---|---|
| `src/history.rs` | 1,713 | yes |
| `src/http.rs` | 3,155 | no |
| `src/product.rs` | 4,205 | no |
| `src/shard.rs` | 3,186 | no |
| `src/billing.rs` | 2,157 | no |
| `src/auth.rs` | 1,676 | no |
| `src/registry.rs` | 1,492 | no |
| `src/sse/feed.rs` | 1,165 | no |
| `src/fleet.rs` | 1,142 | no |

The other touched files are all under 1,000: `gather.rs` 601, `worker.rs` 418, `record.rs` 306, `record_scan_tests.rs` 277, `history_gather.rs` 906 (DST, same 1,000 ceiling).

All commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

### C1: "GatherOutcome moves beside the gather that fills it, verbatim"

- **`src/history.rs`:** delete lines 715-736 (the 6-line doc, `#[derive(Default)]`, the struct and the trailing blank line). 1,713 → **1,691**. This is the headroom rank 76 would otherwise be needed for. Our change does not depend on rank 76. If rank 76's step B (budget types into `history/budget.rs`) lands first, only C4's `HistoryResources` field moves with it.
- **`src/history/gather.rs`:** paste the same 21 lines plus a blank after `type WarmChunk` (before `struct Staged`, which holds it), and drop `GatherOutcome` from the `use super::{…}` list (lines 8-15). 601 → about **623**.
- **Ratchets.** The `unwrap_used` scopes in `gather.rs` shift down, but their size and facts are unchanged:
  - `seed_from_dirty_index` (183-228)
  - `roll_back_stranded_mark` (264-281)
  - `plan_read` (389-417)
  - `raise_lane_marks` (586-600)

  Fingerprints are `qualified\0value`, not line numbers. The names those scopes use (`PendingAbsorb`, `MAX_PENDING_STREAMS`, `DISCOVERY_PAGE_STREAMS`, `RouteHash`, `ReadPlan`) keep their imports. **Do not alias or re-import them.**

### C2: "read_frames_range keeps a row's own corruption typed; the gather still aborts on it"

- **`src/shard/record.rs`** (306 → about **327**). After `impl From<RecordCorruption> for slatedb::Error` (line 28), insert:

  ```rust
  /// A range read's refusal split by owner (item 35): `Corrupt` is the
  /// row's own bytes failing admission, so the same read fails the same way
  /// on every retry; `Store` is whatever the database said, with its kind
  /// (fence, unavailability, SST data) intact for the caller's policy.
  #[derive(Debug)]
  pub(crate) enum RangeReadError {
      Corrupt(RecordCorruption),
      Store(slatedb::Error),
  }
  impl From<RecordCorruption> for RangeReadError {
      fn from(error: RecordCorruption) -> Self {
          Self::Corrupt(error)
      }
  }
  impl From<slatedb::Error> for RangeReadError {
      fn from(error: slatedb::Error) -> Self {
          Self::Store(error)
      }
  }
  ```

  In `read_frames_range`, change **only** the return type at line 135: `Result<FrameReadResult, slatedb::Error>` → `Result<FrameReadResult, RangeReadError>`. The three `?` sites (lines 169, 171, 172) stay byte-identical and convert through the `From` impls.

  **Ratchet.** `#[expect(clippy::indexing_slicing, reason = "read_frames_range; …")]` (125-128) ratchets scope_lines, nested_items and syntax_facts. A signature-only edit keeps all three equal. The one type path is replaced by one, or by none if a single-segment type is not a fact. **Do not** use `.map_err(RangeReadError::…)` at the `?` sites: each would add method-call and path facts inside the scope and fail with "accepted exception grew".
- **`src/history/gather.rs`** (about +8):
  - Add `use crate::shard::record::RangeReadError;`.
  - Change `read_wave`'s return type to `Vec<Result<FrameReadResult, RangeReadError>>`.
  - Replace the loop body at line 353 with:

    ```rust
                let chunk = match read {
                    Ok(chunk) => chunk,
                    Err(RangeReadError::Corrupt(corruption)) => {
                        return Err(slatedb::Error::from(corruption).into());
                    }
                    Err(RangeReadError::Store(error)) => return Err(error.into()),
                };
                self.stage_chunk(&mut staged, reservation, plan, &chunk)?;
    ```

    `slatedb::Error::from(corruption)` is the same value the old `?` built, so the message, kind and fence classification are identical. Nesting stays ≤ 4: fn, while, for, arm block.
- **`src/shard/record_scan_tests.rs`:**
  - Add `record::RangeReadError` to the `use super::{…}` list (lines 2-5). This is outside any exception scope.
  - Replace lines 146-149 with the 4-line `assert!(matches!(absorber.unwrap(), Err(RangeReadError::Corrupt(_))));`, formatted across 4 lines.

  **Ratchet.** The test fn has `#[expect(disallowed_methods)]` and `#[expect(excessive_nesting)]` (82-90). Its scope_lines are unchanged (4 lines out, 4 in). The facts go down: `assert_eq!` plus 4 method calls, a literal and `slatedb::ErrorKind::Data`, become `assert!` plus 1 method call and 1-2 paths. If the source gate still reports growth, drop the message literal first. Do not re-word the reason.

### C3: "gather_due settles an outcome and a failed gather in their own functions; its nesting exception goes"

- **`src/history/worker.rs`** (418 → about **430**):
  - Delete `#[expect(clippy::excessive_nesting, reason = "Absorber::gather_due; …")]` (306-309).
  - Replace `gather_due`'s match (343-415) with:

    ```rust
            match self.absorb_gather_v2_with(v2_lane, &mut _reservation).await {
                Ok(outcome) => self.settle_gather(pending, &outcome),
                Err(e) => self.settle_gather_error(pending, &e, now, v2_lane),
            }
    ```
  - Move the `Ok` arm's body (345-396, comments included, dedent only) into `fn settle_gather(&self, pending: &mut HashMap<[u8; 16], PendingAbsorb>, outcome: &GatherOutcome)`.
  - Move the `Err` arm's body (399-413) into `fn settle_gather_error(&self, pending: &mut HashMap<[u8; 16], PendingAbsorb>, e: &anyhow::Error, now: Instant, v2_lane: &[[u8; 16]])`. That is five inputs counting `self`, so it passes the ≤ 5 rule. The fence branch becomes an early `return` after its warn, and the non-fence loop calls a new module-level:

    ```rust
    /// Exponential retry after a failed absorb: tick·2^n, capped at 2^6, so a
    /// persistent failure costs one read per backoff window, not one per tick.
    fn back_off(p: &mut PendingAbsorb, now: Instant, tick: Duration) {
        p.failures = p.failures.saturating_add(1);
        let shift = p.failures.min(6);
        p.retry_after = Some(now + tick * 2u32.pow(shift));
    }
    ```

    These are verbatim lines 408-410.
  - Add `use super::gather::GatherOutcome;`. The `gather` module is private to `history`, and `worker` is a sibling inside it.
- **Nesting after C3.** `gather_due`: fn → if → if = 3. `settle_gather`: fn → for → if / closure = 3. `settle_gather_error`: fn → for → if-let = 3. So the removed expectation would have been **unfulfilled**, and `unfulfilled_lint_expectations` is denied. Deleting it is required, and it removes an exception.
- **Ratchets.** `run` (too_many_lines + unwrap_used, 12-203) and `classify_due` (disallowed_methods + unwrap_used, 204-305) are not edited. Neither of them uses `GatherOutcome`, so the new import changes none of their facts. `gather_due`'s exception disappears, and a vanished contract is not compared. The three new fns carry no exception.

### C4: "One stream's corrupt row backs off that stream alone; its lane-mates advance in the same flush"

**`src/history.rs`** (1,691 → **1,697**; ceiling 1,713; net −16 against base):

- After `mod gather;` (line 13), add:

  ```rust
  #[cfg(test)]
  pub(crate) use gather::StreamGatherFailure;
  ```

  This gives the DST test a path. It is test-only because a `pub(crate) use` with no production reader is `unused_imports`.
- Add to `HistoryResources` (215-222), after `paused`:

  ```rust
      /// Streams a gather left out for their own stored bytes (item 35),
      /// counted runtime-wide beside the budget they were gathered under.
      pub gather_streams_failed: AtomicU64,
  ```

  Initialize it as `gather_streams_failed: AtomicU64::new(0),` in `with_body_limit`. `AtomicU64` is already imported at line 882. `HistoryResources` has no exception, and its hand-written Debug uses `finish_non_exhaustive`, so it needs no change.

**`src/history/gather.rs`** (about 631 → about **695**):

1. Add `StreamGatherFailure`. Place it after `GatherOutcome`:

   ```rust
   /// Why a gather left one stream out (item 35). Each verdict is a pure
   /// function of that stream's own durable bytes, so the same read fails the
   /// same way on every retry: it costs that stream its turn and its backoff,
   /// never its lane-mates'. Store, fence, partition and flush errors are NOT
   /// here — they still abort the whole gather.
   #[derive(Debug, PartialEq, Eq)]
   pub(crate) enum StreamGatherFailure {
       /// A stored row failed canonical admission (key width, namespace,
       /// frame or offset).
       Corrupt(RecordCorruption),
       /// The chunk's postings pages did not decode back to what was encoded.
       PostingsSelfDecode,
       /// The chunk's postings pages decoded to overlapping runs.
       PostingsOverlap,
   }
   ```

   `PartialEq` is needed for the tests. It is also load-bearing: dead-code analysis ignores derived `Debug`, so with `Debug` alone, `Corrupt`'s payload would be "never read" in non-test builds under `-D warnings`.
2. Add a field to `GatherOutcome`, after `partial`, and add one clause to the struct doc:

   ```rust
       /// Streams left out for their own stored bytes, with the typed reason.
       /// They MUST stay pending with their own backoff: retiring them strands
       /// their backlog; retrying them every tick re-reads a chunk that fails
       /// the same way.
       pub(crate) failed: Vec<([u8; 16], StreamGatherFailure)>,
   ```

   Also add a `failed` sentence to `absorb_gather_v2_with`'s classification doc (300-310).
3. Change imports: add `RecordCorruption` to the `crate::shard::record` import. `PageBuilder` is already imported.
4. The loop in `absorb_gather_v2_with` becomes:

   ```rust
               for (plan, read) in wave.iter().zip(got) {
                   match read {
                       Ok(chunk) => self.stage_chunk(&mut staged, reservation, plan, &chunk),
                       // The row's own bytes failed admission: it costs this
                       // stream its turn, never its lane-mates'.
                       Err(RangeReadError::Corrupt(corruption)) => staged
                           .out
                           .failed
                           .push((plan.hash, StreamGatherFailure::Corrupt(corruption))),
                       Err(RangeReadError::Store(error)) => return Err(error.into()),
                   }
               }
   ```

   After the `while`, next to `GATHER_LAST_PACE_MS.store`, add:

   ```rust
           self.shard
               .history_resources
               .gather_streams_failed
               .fetch_add(staged.out.failed.len() as u64, Ordering::Relaxed);
   ```

   The function grows from 54 to about 64 lines.
5. Split `stage_rows` (96-119) and `stage_postings` (129-151) so that nothing is put before the check:
   - `fn note_frames(plan: &ReadPlan, chunk: &FrameReadResult, pages: &mut PageBuilder) -> u64`. This is the old loop without `wb.put`; it returns `last`. Doc: nothing is staged yet, because a chunk whose pages fail their self-check must leave no canonical row in the shared batch.
   - `fn stage_rows(wb: &mut WriteBatch, plan: &ReadPlan, chunk: &FrameReadResult)`. This is the old `wb.put` loop only.
   - `struct ChunkPostings { pages: crate::postings::Pages, runs: KeyRuns, bytes: u64 }`. The fields are private to `gather`.
   - `fn check_postings(pages: PageBuilder) -> Result<ChunkPostings, StreamGatherFailure>`. It does `finish()`, then for `&emitted` it runs `decode_page_abs(..).ok_or(StreamGatherFailure::PostingsSelfDecode)?` and `append_page_runs(..).ok_or(StreamGatherFailure::PostingsOverlap)?`. It takes no `WriteBatch` and bumps no counter.
   - `fn stage_postings(wb: &mut WriteBatch, plan: &ReadPlan, checked: ChunkPostings) -> KeyRuns`. It bumps `POSTINGS_PAGES_WRITTEN` (`pages.len()`), `POSTINGS_RUNS_WRITTEN` (the sum of `runs[i].1.len()`, which equals the old per-page `abs.len()` sum because `append_page_runs` pushes every run), puts every page, and bumps `POSTINGS_BYTES_WRITTEN`.
6. `stage_chunk` returns `()`. It can no longer fail; keeping `Result` would trip `unnecessary_wraps`. After the unchanged empty/budget/`try_grow` prefix (460-490), its tail becomes:

   ```rust
           let mut pages = PageBuilder::default();
           let last = note_frames(plan, chunk, &mut pages);
           // The pages prove their own round trip BEFORE any of the chunk
           // enters the shared batch: a refused chunk leaves nothing behind.
           let checked = match check_postings(pages) {
               Ok(checked) => checked,
               Err(failure) => {
                   staged.out.failed.push((plan.hash, failure));
                   return;
               }
           };
           staged.bytes = batch_bytes;
           #[cfg(test)]
           trace_gather(plan, chunk);
           stage_rows(&mut staged.wb, plan, chunk);
           let runs = stage_postings(&mut staged.wb, plan, checked);
           // … unchanged: CANONICAL_BYTES_WRITTEN, warm_installs, advanced, partial
   ```

   Two lines move below the check: `staged.bytes = batch_bytes` and the test-only trace. A refused chunk therefore neither counts toward the batch nor prints as staged. The extra `try_grow` it already won is released with the reservation. The function is about 66 lines, nesting 2.
7. Declare the test module at the end of the file:

   ```rust
   #[cfg(test)]
   mod postings_refusal_tests;
   ```

**`src/history/gather/postings_refusal_tests.rs`:** new, about 30 lines (§3.C).

**`src/history/worker.rs`** (about 430 → about **446**):

- `settle_gather` gains a `now: Instant` parameter (4 inputs), and the call site passes `now`. Append:

  ```rust
          // Item 35: a stream left out for its own stored bytes backs off
          // alone; its lane-mates above settled with this flush.
          for (h, failure) in &outcome.failed {
              let Some(p) = pending.get_mut(h) else { continue };
              back_off(p, now, self.cfg.tick);
              tracing::warn!(
                  "v2 gather left {} out: {failure:?}; failure {} backs it off alone",
                  crate::crypto::hex(&h[..4]),
                  p.failures,
              );
          }
  ```

  Nesting is 3. Extend the doc by one sentence.
- Declare `#[cfg(test)] mod lane_isolation_tests;` at the end of the file.

**`src/history/worker/lane_isolation_tests.rs`:** new, about 100 lines (§3.B).

**`src/dst/tests/history_gather.rs`:** 906 → ≤ **960** (§3.A).

**Ratchets in C4.**

- No exception scope is edited. `trace_gather`'s `#[cfg(test)] #[expect(disallowed_methods)]` (154-173) is not touched; only its call site moves.
- The `unwrap_used` scopes in `gather.rs` do not reference any changed import.
- `Absorber`'s struct-wide `#[expect(dead_code)]` (`history.rs` ~753) is untouched: no `Absorber` field is added, and the counter lives in `HistoryResources`.
- The new test files have no exceptions, and every function is under 100 lines.
- Clippy checks: no `_ =>` arm on `RangeReadError` or `StreamGatherFailure`; no bool params; `settle_gather_error` has 5 inputs.

---

## 5. Mutation analysis

**Selected files.** `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:21-31`) do not include `src/history` or `src/dst`, and `mutation_owners.py` registers no path under them. So `history.rs`, `history/gather.rs`, `history/worker.rs`, the two new `history/{gather,worker}/…_tests.rs` files and `dst/tests/history_gather.rs` select **no mutants**. The new files need **no owner rows**; `test_verification_plan.py:80` already uses `src/history.rs` as the non-mutation example.

Selected by this change:

- **`src/shard/record.rs`.** Registered owner `record`, filter `shard::`. It is also in `CODEC_PREFIXES`, so `properties_fuzz` is selected. In-diff candidates:
  1. `replace <impl From<RecordCorruption> for RangeReadError>::from -> Self with Default::default()`: **Unviable**, because `RangeReadError` has no `Default`.
  2. `replace <impl From<slatedb::Error> for RangeReadError>::from -> Self with Default::default()`: **Unviable**.
  3. Possibly `replace read_frames_range -> Result<FrameReadResult, RangeReadError> with Ok(Default::default())`, if the line after the modified signature counts as adjacent. **Unviable**, because `FrameReadResult` has no `Default` (`record.rs:78-82`).

  There are no operators on inserted lines and no binary or unary mutants. Missed = 0, timeout = 0. No equivalent mutant needs restructuring. The baseline must pass the `shard::` tests, which include the updated r08a.
- **`src/shard/record_scan_tests.rs`.** Registered owner `record_scan_tests`, filter `shard::`. The module is `#[cfg(test)] mod record_scan_tests;` (`shard.rs:3155-3156`), so cargo-mutants generates nothing. The expected driver line is `record_scan_tests: no executable mutants in the selected scope`.

**Owner rows and filters:** no change. Leave the uncommitted `mutation_owners.py` edit exactly as it is; it belongs to the other unpushed work.

The push's `plan.json` also carries whatever items 90/93/41/89 already select (offsets, segmap, telemetry_batch, scaler3, auth_feed, …). This plan adds only the two `src/shard/record*` paths.

---

## 6. Ledgers (all in C4 unless noted)

| Ledger | Change |
|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` in C4. It adds `one_corrupt_row_fails_only_its_stream` (scenarios `[]`), going from 517 to 518. Only `src/dst/**` is inventoried, so 3.B, 3.C and the r08a edit (C2) are not. Regenerate on top of HEAD, whose own commits already touched this file. |
| `docs/refactor/review-mechanisms.json` | none. No history_gather or record_scan test is sha-pinned (grep is empty). |
| `docs/quality/owners.json` | none. There is no new static (the counter is a `HistoryResources` field), no glob import (explicit `use`), no by-path module, and no macro-dsl (`tracing::` and expression macros are exempt). |
| `docs/quality/source-allowances.json` | none. No occurrence is vacated. `crate::Absorber::run`'s `tokio::select` and `classify_due`'s `std::env::var` are untouched. |
| `docs/refactor/architecture-policy.json` | none. No function is over 200 lines, no file over 1,000, and there are no `http`/`product` edges. |
| `docs/refactor/WIRE-MATRIX.md` | none (no wire change). |
| Scenario map / dispositions | none. There are no renames. Mapping 3.A to a new `HIS-0xx` is optional. |
| `src/dst/tests/README.md` | none. There is no new DST module, and `history_gather` is already listed at line 11. |

---

## 7. Controls (for the implementer, after the mutation and gate runs release the tree)

Run each block after its commit. Expected outputs are in comments.

```sh
# Line budgets. Expected after C4: history.rs 1697 (≤1713), gather.rs ≈695,
# worker.rs ≈446, record.rs ≈327, record_scan_tests.rs 277±1, history_gather.rs ≤960.
wc -l src/history.rs src/history/gather.rs src/history/worker.rs src/shard/record.rs \
      src/shard/record_scan_tests.rs src/dst/tests/history_gather.rs \
      src/history/gather/postings_refusal_tests.rs src/history/worker/lane_isolation_tests.rs

cargo fmt --all -- --check                                   # no output, exit 0
cargo clippy --locked --workspace --all-targets -- -D warnings
# exit 0; in particular no unfulfilled_lint_expectations (C3) and no excessive_nesting

# C2 pins
cargo test --locked -p streams-slate --lib shard::record_scan_tests::       # all ok
cargo test --locked -p streams-slate --lib dst::dst_tests::reads_ring::     # all ok

# Red-first. Run BEFORE C4 (on C3), with 3.A trimmed as in §3.A, and 3.B/3.C added:
cargo test --locked -p streams-slate --lib history::worker::lane_isolation_tests:: \
  # FAILED: "a lane-mate of a corrupt row must retire with the flush that absorbed it"
cargo test --locked -p streams-slate --lib dst::dst_tests::history_gather::one_corrupt_row_fails_only_its_stream -- --exact \
  # FAILED: "a corrupt row in one stream must not fail the lane's gather: Data error: stored record corruption: Frame"
# 3.C: compile error E0432 unresolved imports super::StreamGatherFailure, super::check_postings

# Green after C4
cargo test --locked -p streams-slate --lib history::worker::lane_isolation_tests::        # 1 passed
cargo test --locked -p streams-slate --lib history::gather::postings_refusal_tests::      # 2 passed
cargo test --locked -p streams-slate --lib dst::dst_tests::history_gather::one_corrupt_row_fails_only_its_stream -- --exact  # 1 passed
cargo test --locked -p streams-slate --lib -- dst_tests::history_ dst_tests::reads_history history::   # all ok

# Ledgers and gates
python3 scripts/test-inventory.py --write     # test-inventory: wrote 518 tests
python3 scripts/test-inventory.py --check     # test-inventory: OK (518 tests, <n> ignored)
python3 scripts/architecture-gate.py --check  # OK
bash scripts/quality.sh                       # ... QUALITY_OK
# (gate.py prints no "accepted exception grew", "file growth", or "unregistered source occurrence")

# CI's plan before push (memory rule)
python3 scripts/quality/verification_plan.py --out target/quality-plan
# mutation_source_files ⊇ ["src/shard/record.rs", "src/shard/record_scan_tests.rs"]
# (plus the unpushed commits' own); unregistered_mutation_source_files: []; properties_fuzz: true
QUALITY_MUTANTS_OUT=target/quality-mutations bash scripts/quality/mutations.sh
# record: ≤3 mutants, all Unviable; record_scan_tests: no executable mutants; missed 0, timeout 0
```

---

## 8. Out of scope (deliberately)

- **`ErrorKind::Data` isolation (the reviewer's wording).** It is rejected (§2.1). Store-level `Data` (NotFound, checksum, manifest, WAL) keeps aborting the lane with lane-wide backoff.
- **Corrupt tail rows in `plan_reads`.** These are `stream_handle` → `stored_tail` → untyped `slatedb::Error::data`. They are reachable only when a handle is evicted between `classify_due`, which already skips failing handles, and `plan_reads`. Typing that path would edit `shard.rs`, which is at its 3,186 ceiling.
- **Reader side.** Product/raw reads of the corrupt stream still fail as today through `read_frames_until`. Repair and operator tooling for a corrupt row are also out.
- **The lane-wide warn still logs a count.** After C4 that branch only ever carries a lane-scoped error, so a count is the right detail there.
- **Narrower variant.** If a reviewer prefers the narrowest scope, drop `PostingsSelfDecode`/`PostingsOverlap`. That removes C4 steps 5-6 and §3.C, and `stage_chunk` keeps `anyhow::Result`; `RecordCorruption` isolation is unaffected. I recommend keeping them: they are deterministic per stream, so without isolation an encoder defect on one stream's shape stalls its lane-mates exactly as §1.2 describes. The reorder is also what keeps a refused chunk from leaving orphan rows and pages.

## 9. Decisions for Søren (genuine edge/policy changes; none of these land now)

1. **Expose the counter on `/v1/debug/absorb`.** This would be an additive field `absorber.streamsFailedTotal` = `state.runtime.history.gather_streams_failed`, in the camelCase idiom of that block (`http.rs:1398-1416`).

   *Cost:* `http.rs` is at its 3,155 ceiling, so the `/absorb` handler closure (≈`http.rs:1341-1440`) first needs a verbatim move into `src/http/debug.rs` (46 lines; owner `http_debug`, filter `debug_surface_`). The moved handler then becomes mutation-selected and needs `debug_surface_` coverage of the absorb JSON.

   *Backward-compatible alternative (what lands):* the runtime counter plus the per-stream warn, with no JSON change.
2. **Policy for a permanently corrupt stream.** After C4 it backs off alone, capped at tick·64 = 320 s at the 5 s tick. But it stays pending, so its absorb age keeps feeding:
   - `oldest_eligible` → `set_shard_lag` (`worker.rs:155-170`), which drives the rebalancer's victim choice;
   - the durable no-progress / LagSecs latch, which the worker comments describe as able to shed the whole instance.

   Before C4 this was strictly worse, because the whole lane stalled. Whether to quarantine such a stream (exclude it from the lag and latch signals and surface it for repair) changes shedding and admission behaviour at the edge.

   *Backward-compatible alternative (what lands):* today's lag accounting, unchanged.

---

## Skeptic corrections (C1..C9)

I checked this against `slate` @ `6669d3b5`, with merge base `aaf2baa5`. I only read files; nothing was built or run. The following were **confirmed**:

- **Line counts** (`wc -l`, also at `origin/slate`): history.rs 1,713; gather.rs 601; worker.rs 418; record.rs 306; record_scan_tests.rs 277; dst/tests/history_gather.rs 906; ops.rs 922.
- **The unpushed commits touch none of these files.**
- **The quoted `gather.rs:350-354`, `record.rs:24-28/171-172` and `history.rs:715-736` are accurate.** `GatherOutcome` is 22 lines including the blank line at 736.
- **The use-site lists are complete** after a grep over src, tools, fuzz, bench and src/dst.
  - `read_frames_range` has one production caller, `gather.rs:432`. Its test callers are `reads_ring.rs:149/165/258/269/370` and `record_scan_tests.rs:127/168`.
  - The other `from_row(..)?` sites (`history.rs:937/1086` and `canonical_span.rs:57`) return `anyhow`. They do not go through `From<RecordCorruption> for slatedb::Error`.
  - Nothing in tools/, fuzz/, the quality-invariants by-path harness or docs/RUNBOOK/deploy refers to the gather error text or the backoff.
- **The slatedb `ErrorKind::Data` mapping at the pinned rev supports §2.1.** In `0717cc1` `error.rs`, `NotFound`, `ChecksumMismatch`, `BlockDecompressionError`, `ManifestMissing`, `InvalidDBState` and `WalTruncated` all map to `Data`, and Display is `"{kind}: {msg}"`.
- **The runtime red traces hold.**
  - 3.A prints `...: Data error: stored record corruption: Frame`.
  - 3.B prints `a lane-mate of a corrupt row must retire with the flush that absorbed it`.
  - The 3.C encoder traces hold: `postings.rs:212` zero-bytes refusal and `:265` `prev_end` overlap. `validated::validate` accepts both single-run pages.
- **Every named pin test exists.**
- **Nesting and argument counts after C3 hold**, and the `excessive_nesting` expect must go.
- **Mutation selection is correct.**
  - `src/history*` and `src/dst` are outside `CRITICAL_PREFIXES` and have no owner row, including in the uncommitted `mutation_owners.py` edit.
  - The record.rs in-diff mutants are only the `FnValue` replacements. They are all unviable: there is no `Default` on `RangeReadError` or `FrameReadResult`, and there is no `.cargo/mutants.toml` `error_values`.
  - `record_scan_tests` sits behind `#[cfg(test)]`, so it has 0 mutants and the driver prints the "no executable mutants" line.
- **The `exception_growth` ratchet ignores a vanished identity.** Contracts are keyed by `(path, qualified, kind, value)` and compared only when present in both.

The corrections follow.

**C1. The multitenancy audit fails on 3.B. This is a missed ledger and makes the `quality.sh` control unbuildable.**

- `src/history/worker/lane_isolation_tests.rs` (§3.B) adds `key_hash: crate::crypto::stream_hash(""),`.
- `scripts/multitenancy-audit.sh:84` scans *every* `src/**/*.rs` for `stream_hash\(`. The resulting row, `stream-hash<TAB>src/history/worker/lane_isolation_tests.rs<TAB>key_hash: crate::crypto::stream_hash(""),`, is not in `scripts/mt-audit-baseline.txt`.
- So `scripts/quality.sh:42` prints `multitenancy-audit: FAIL — new bare-name identity site(s)` and never reaches `QUALITY_OK`. The §7 control `bash scripts/quality.sh  # ... QUALITY_OK` cannot pass as written.

Remedy, in C4 with the test: run `bash scripts/multitenancy-audit.sh --regen` and justify it in the commit message. The justification is an identity-neutral test fixture for the routing-key hash of the empty key. The precedent is the existing baseline row for `src/history/controller_tests.rs` (`key_hash: crate::crypto::stream_hash(""),`, from `controller_tests.rs:84`). Expected after the regen: `MT_AUDIT_OK`, with 78 fingerprints (77 → 78).

Do not dodge the audit with a hand-rolled `[0; 16]` or `rk_hash("").0`.

**C2. "Validate before any put" is not a compile-level proof as specified (§2.1.3, §4 C4 steps 5-6).**

`ChunkPostings`' fields are private to `gather`, but every function in `gather.rs` is inside that module. So `stage_chunk` could still build a `ChunkPostings { .. }` literal. Worse, `stage_rows(wb, plan, chunk)` takes no `ChunkPostings`, so nothing stops a later edit from calling it before `check_postings`.

Remedy:

1. Replace `stage_rows` + `stage_postings` with one `fn stage_checked(wb: &mut WriteBatch, plan: &ReadPlan, chunk: &FrameReadResult, checked: ChunkPostings) -> KeyRuns`. It performs *all* of the chunk's puts: rows first, then pages, the same batch order as today. Every `wb.put` in the chunk path then consumes the check's output.
2. For a real type-level proof, move `ChunkPostings` and `check_postings` into a child module, for example `src/history/gather/chunk_check.rs` with private fields and a read-only accessor. Then `gather.rs` cannot build the literal. This new file needs no owner row, because it is not under a critical prefix.

   If you would rather not add the module, reword §2.1.3 to "ordering by construction within one function, checked by review". Do not call it a compile-level proof.

Either way, 3.C's import becomes `super::chunk_check::{check_postings, ..}` or stays as it is.

**C3. The C2 edit to `record_scan_tests.rs`: rustfmt output and ratchet reasoning.**

- **Format.** `        assert!(matches!(absorber.unwrap(), Err(RangeReadError::Corrupt(_))));` is 79 columns, so rustfmt keeps it on **one** line. Written "across 4 lines", it fails `cargo fmt --all -- --check`. The test fn's scope shrinks by 3 lines, and the file goes 277 → about 274-275.
- **Ratchet reasoning.** The ratchet argument in §4 C2 is wrong, but the conclusion holds. Macro bodies are opaque to the syntax scanner (`tools/quality-syntax/src/scan.rs:278-285` records only `macro`, `macro-tokens` and the macro path). So the method calls inside `assert_eq!(..)` were never facts.
  - Before: `assert_eq!` gives 3 facts.
  - After: `assert!(matches!(..))` gives 3 facts. `matches!` is inside the opaque tokens.
  - So `syntax_facts` is unchanged and `scope_lines` shrinks. The fallback "drop the message literal first" is moot; delete it.

**C4. Make the per-stream warn independent of how the pinned rustc (1.98.1) treats derived impls in dead-code analysis.**

§4 C4 step 1 relies on derived `PartialEq` counting as a read of `Corrupt(RecordCorruption)`'s payload in non-test builds, because the production reader is `{failure:?}`, a derived `Debug`, which dead-code analysis ignores. That is compiler-version-sensitive, and nothing in the plan checks it.

Remedy: give `StreamGatherFailure` a hand-written `impl std::fmt::Display`. It should be an exhaustive `match` with no `_ =>` arm, and the `Corrupt(c)` arm writes `c`. Log `{failure}` in the worker warn. The payload is then read explicitly in production, and the log line matches `RecordCorruption`'s own Display idiom. Keep `#[derive(Debug, PartialEq, Eq)]` for the tests. This costs about 10 lines in `gather.rs`, which stays at ~705, well under 1,000.

**C5. The quote in §1.2 is not verbatim.** At `worker.rs:402-403` the comment is two lines, `// Engine is dying; the exit path` / `// clears pending.`, at 24-space indentation. The plan collapses it into one line. Quote the exact lines, or mark the block as abridged. C3's "verbatim move" of this arm must keep both lines.

**C6. The `history_gather.rs` budget is tighter than stated, and the import claim is mis-argued.**

- **Import.** The combined `use super::fixture_storage::{..., open_engine, ...};` is 113 columns as one line. rustfmt therefore keeps the 3-line block form, with an 87-column inner line. The net line change is still 0, but not because it "stays within 100 columns".
- **Line count.** In 3.A, rustfmt splits four lines that are over 100 columns:
  - the `no_work && deferred_budget && partial` assert (about 105 columns)
  - `let counted = …load(..)` (about 106)
  - the `durable.absorbed` `assert_eq!` (about 101)
  - the `matches!` pattern

  Expect about 62-66 lines, so 906 → about 970, not "≤ 960". It is still under 1,000. Fix the §7 control's expected value to ≤ 975.

**C7. The counter has no production reader until Decision 1, and Decision 1's cost analysis misses the existing runtime-history metrics surface.**

- **The reader.** `gather_streams_failed` is written in production but read only by the DST test. No operator can see it until Decision 1. State that explicitly in the C4 commit message. If Søren prefers, the counter can be dropped from C4, and 3.A can assert `outcome.failed` alone. The warn still names each stream.
- **The cheaper surface.** A cheaper Decision-1 route than moving `/v1/debug/absorb` out of `http.rs` is `src/ops.rs::collect_snapshot` (ops.rs:428-469). It already reads `state.runtime.history.budget` into snapshot gauges, and ops.rs has 78 lines of headroom. The route has its own costs, which is why it remains a decision:
  - `collect_snapshot` carries `#[expect(clippy::too_many_lines, reason = "collect_snapshot; ...")]` (ops.rs:387-390). Its `scope_lines` ratchet grows by the ~4 added lines, so it needs a re-decided reason or a verbatim move of the history-gauge block into a helper first.
  - `src/ops` is a critical prefix (owner `ops`, filter `ops::`). The edited body selects that function's whole-body mutant and needs an `ops::` test that sees the new gauge.
  - A new snapshot gauge name, for example `gather_streams_failed_total`, is an additive ops/metrics contract change.

  List it next to the `http.rs` option.

**C8. State the synchronization disposition.** The plan never says why no Loom or held-commit test is required. It should say:

- `pending` is owned by the one absorber task, and `settle_gather` and `settle_gather_error` run inline on it.
- The counter is a Relaxed monotone `fetch_add` with no ordering obligation.
- `failed` streams are never passed to `submit_absorbed_batch_v2` or `raise_lane_marks`, so the committer and lane-mark protocol see exactly the subset they saw for `advanced` before.
- No new cross-task state exists.

Add that paragraph after §2.1.4, so that "synchronization change" is explicitly ruled out rather than skipped.

**C9. Minor control fixes (§7).**

- After C4, `rg -n GatherOutcome src` also lists `worker.rs`, because C3 adds `use super::gather::GatherOutcome;`. Restrict the C1 control to "after C1".
- The long assert and `ShardEngine::start(..)` lines in 3.B are over 100 columns. Run `cargo fmt --all` before the `--check` control. Otherwise the check fails on the plan's literal text.

**Missed ledgers:** `scripts/mt-audit-baseline.txt` (C1). All the other ledger dispositions in §6 check out:

- test-inventory goes 517 → 518. Only `src/dst/**` is inventoried; I checked `scripts/test-inventory.py:138`.
- review-mechanisms has no pins on these tests.
- owners.json and source-allowances.json need no new effects, globals, globs or macro-dsl, and no rows are vacated. `crate::trace_gather` and `crate::Absorber::classify_due` are untouched.
- `docs/quality/legacy-diagnostics*.json` holds five frozen `crate::Absorber::gather_due` excessive_nesting rows. That is the adoption genesis, which already carries stale rows (for example `src/history/span_cache/tests.rs`). It is not an obsolescence-checked allowance, so leave it.
- `docs/quality/verification.json` per-file hashes are an adoption record with no gate consumer, so leave that too.

**Unbuildable controls as written:** `bash scripts/quality.sh → QUALITY_OK` (fails in the multitenancy audit until C1). `cargo fmt --all -- --check` also fails if the C2 record_scan assert is hand-wrapped (C3) or the 3.A/3.B text is pasted unformatted (C6, C9).

**Verdict: ready-with-corrections.**

- The diagnosis, the typed `RangeReadError` split, the rejection of blanket `ErrorKind::Data`, the ceiling handling (history.rs 1,713 → 1,697), the ratchet dispositions and the mutation analysis are sound.
- C1 blocks the gate and must be fixed.
- C2 and C4 are small design tightenings that should land in C4.
- The rest are wording and control fixes.
