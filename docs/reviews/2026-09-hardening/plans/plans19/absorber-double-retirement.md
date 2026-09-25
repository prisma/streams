# Release hold: the absorber's double retirement (the capacity run's one-off 500)

Tree: `slate` @ `eda79ccf` (= origin/slate). This is a read-only plan: nothing below was built or run.
Inputs: investigation commits `8a2791b0` (split_boundary_outcomes harness, findings F1/F2) and `ca80f9d4` (the red regression).
On branch `worktree-wf_e38ad9fe-ae6-3` they sit as `706430f7` and `bc2907f7`.
- `bc2907f7`'s test file is byte-identical to `ca80f9d4` (`git diff ca80f9d4 bc2907f7 -- src/history/bounded_discovery_tests.rs` is empty).
- `706430f7` differs from `8a2791b0` in two split scenarios only: `assert!(split)` became an `eprintln!`, and the two inventory hashes changed.

**Where this departs from the task's options.** The task offers three ways to fix this and asks me to pick. I pick two layers, and I reject option (1) used on its own. The reason is one fact found by reading the tree (§1.4). If the committer simply drops an advance that overlaps its boundary, the stream's history partition ends up with overlapping postings pages. Readers treat overlapping pages as corruption: they count `POSTINGS_CORRUPT` and fall back to a full envelope scan of the bucket, permanently. So option (1) removes the 500 but degrades reads in every instance of the race.

- **Commit 1 (committer, exactness backstop):** option (1). An advance is retired only when it starts exactly at the stream's absorbed boundary. Any other advance is dropped whole.
- **Commit 2 (absorber, the causal fix):** option (2), made exact. A lane mark is rolled back only when every batch this absorber submitted has *settled*, meaning it was durably published or refused. The committer holds a receipt for each batch until that point. The rollback moves out of the dirty-index rescan and into gather planning.
- **Option (3), containment:** not adopted. After commit 1, an absorbed op can diverge only through genuine ledger corruption, and failing closed stays the documented posture (D2).

Commit 2 is the causal fix: it stops the regather that overlaps an in-flight advance. Commit 1 makes retirement exact whatever the absorber does. It covers the paths commit 2 cannot reach (a mark pruned for an evicted handle, and a refusal that happens while another batch is unsettled), and it removes the latent over-count ("phantom") that the current tree creates after a refused group (§1.5).

---

## 1. Problem (verified on eda79ccf)

### 1.1 The rescan rolls back a mark whose batch is still queued

`src/history/worker.rs:47` and `:83-87`: once the absorber has seeded, it runs a rescan every 120 ticks. The comment on `RESCAN_EVERY` says ~10 min at the 5 s production tick; at the capacity rig's 20 ms tick that is 2.4 s.
```rust
47        const RESCAN_EVERY: u32 = 120; // ~10 min at the 5 s tick
83                    if (!seeded && tick_n >= seed_next_tick)
84                        || (seeded && tick_n.is_multiple_of(RESCAN_EVERY))
85                        || absorber.discovery_after.lock().unwrap().is_some()
87                        match absorber.seed_from_dirty_index(&mut pending).await {
```
`src/history/gather.rs:275-283`: for each dirty row (durable `absorbed` read from the DB's dirty index) that still has backlog:
```rust
279            let (recs, bytes) = self.backlog_of(h, absorbed, next).await;
283            self.roll_back_stranded_mark(h, absorbed);
```
`gather.rs:339-351`: the rollback compares the mark only against that durable value:
```rust
341        if let Some((mark, _v2)) = submitted.get(&h)
342            && *mark > absorbed
...
350            submitted.remove(&h);
```
The mark's own doc (`gather.rs:331-334`) rests on a premise that is false:
> "A genuine in-flight advance re-submitted after this is harmless — the committer ignores non-advancing boundaries and the history write is idempotent."

The committer ignores only an advance whose `upto <= prev`. An advance that re-covers part of the range and then extends past it *is* applied (§1.3). The `submitted` doc in `src/history.rs:748-751` rests on the same premise: "re-absorbing is idempotent".

### 1.2 The next gather plans from the stale durable boundary

`gather.rs:476-491` (`plan_read`): `from = max(v2 mark, st.durable.absorbed)`. With the mark removed, `from` is the durable boundary. The new chunk re-reads the range the queued batch already covers. `gather.rs:592` records `(hash, last + 1, chunk_raw)`, where `chunk_raw` is the bytes of `[from, last]`. `gather.rs:660-663` submits the batch, fire-and-forget, then raises the mark.

### 1.3 The committer retires the whole batch whenever `upto > prev`

The path from submission to retirement:
- `src/shard.rs:1970-1977` sends `CommitOp::AbsorbedBatch { streams, v2: true }`.
- `src/shard/transaction/prepare.rs:16-25` expands it to per-stream `Absorbed { hash, upto, bytes, v2 }`.
- `src/shard/transaction/mod.rs:176-178` calls `self.absorbed(&mut local, hash, upto, bytes, v2)`.
- `src/shard/transaction/maintenance.rs:186-202`:
```rust
186        if lane_ok && upto > prev_absorbed {
187            let Some(remaining) = local.fields.unabsorbed_bytes.checked_sub(bytes) else {
188                self.accounting_diverged = Some(format!(
...
200            local.fields.absorbed = upto.min(local.fields.next);
201            local.fields.unabsorbed_bytes = remaining;
202            local.frames.retired_bytes += bytes;
```
The op carries no `from`, so the committer cannot tell that `bytes` covers `[from, prev)`, which the queued advance has already retired. That overlap is retired twice and the stream's ledger under-counts. When the ledger is too small, at once or at the next catch-up, `checked_sub` fails and `accounting_diverged` is set. Then `finalize.rs:5-8` rejects the whole group:
```rust
5        if let Some(message) = &self.accounting_diverged {
6            tracing::error!("maintenance accounting diverged: {message}");
7            self.reject("maintenance accounting diverged");
```
`mod.rs:186-188` answers every reply in the group with `AppendErr::Internal(...)`. That includes appends, closes and seal fences. At the edge:
- raw appends: `contract.rs:298` maps `AppendErr::Internal` to `AppendCode::Internal`, and `http.rs:27` turns `FailureClass::Internal` into 500 `internal`;
- product appends: `product.rs:2354` renders `("append_failed", "append failed", None, false)` (500, not retryable).

A seal close in the same group fails the same way: "the failed close also makes execute_split return false" (8a2791b0). Because the under-count is stored durably in the tail's `unabsorbed_bytes` and summed again on rebuild, the backlog left on the sealed parent survives restarts.

### 1.4 Traced run and deterministic red (from ca80f9d4)

In the traced run, the mark was rolled back at 1481 while the durable boundary was 1455. The next gather ran `from=1455`, and the committer logged `ADVANCE prev=1481 upto=1502` retiring [1455,1502), so [1455,1481) was retired twice. Four groups were rejected at the catch-up. Across 20 loaded runs there were 37 rollbacks of in-flight marks, 10 runs logged a divergence, and 23 groups were rejected.

The deterministic red is `history::bounded_discovery_tests::a_rescan_during_an_inflight_advance_never_fails_an_append`, which the investigation left `#[ignore]`d:
- G1 gathers [0,4), and the test holds its advance in the committer with `test_hold_commit`.
- Four more records are applied behind a held WAL write, then released.
- `seed_from_dirty_index` rolls the mark back.
- G2 gathers [0,8).
- An append queued behind both advances answers `Err(Internal("maintenance accounting diverged"))`.
- Control `a_regather_from_the_submitted_mark_retires_each_byte_once` passes.

**Secondary finding (inferred by reading; not observed): why the committer's drop alone is not enough.** Postings pages are keyed `<route><inc> 'p' <rk_hash> <bucket> <page_first>` (`src/postings.rs:10`). `page_first` is the first offset at which a key appears *in that chunk* (`postings.rs:373-381`, `PageBuilder::note_frame`). Pages from different chunks tile only when each gather starts where the previous gather ended. A reader loads every page of the key in the bucket, including pages above its boundary (`postings.rs:72-88`, `postings_range`). It treats any overlap as corruption (`postings.rs:259-268`, `append_page_runs`) and falls back to the envelope scan:
- `history.rs:944-958`: `POSTINGS_CORRUPT` then `read_history2_keyed_envelope`;
- `history.rs:992-998`: `CacheRuns::Corrupt`, same fallback;
- `postings_cache.rs:919-921`.

With commit 1 alone, the race runs like this:
1. G1 [0,4) is applied.
2. G2 [0,8) is dropped. Its pages, which start below 4 and extend to 8, stay in the partition.
3. The next contiguous gather starts at 4 and writes pages that start at or after 4.
4. Every key present in both halves now has overlapping pages. That bucket's keyed reads use the envelope scan permanently.

The current tree has the same exposure when the two advances land in separate groups and the second diverges (the traced run's shape). Commit 2 removes the regather, so pages keep tiling.

### 1.5 Latent over-count on the refusal path (current tree)

Suppose a group carrying G1 [0,4) is refused: a billing row read fails, a divergence occurs, or the `fail_next_absorbed_group` failpoint fires. The mark stays at 4. The next gather, G2, plans from 4 and covers [4,8), leaving a gap.
- Current tree: `upto 8 > prev 0`, so the committer moves the boundary 0→8 and retires only the bytes of [4,8). The bytes of [0,4) stay in the ledger forever, as an over-count.
- On a quiet shard, `ShardMaintenance::no_progress_secs` (`shard.rs:373-379`) then grows without bound once real absorption stops. The LagSecs latch (`absorb_lag_secs`, default 900) can engage.
- Commit 1 drops the gap advance. Commit 2 rolls the mark back at the next settled plan and regathers from 0. Pin: R4 (§3).

### 1.6 Full use-site list of what changes

| Item | Sites |
|---|---|
| `CommitOp::AbsorbedBatch` | def `shard.rs:927-930`; build `shard.rs:1976`; close drain `shard.rs:2464`; expand `prepare.rs:16-25`; stage `mod.rs:145`, `mod.rs:180` |
| `CommitOp::Absorbed` | def `shard.rs:947-953`; build `prepare.rs:18-23`; stage `mod.rs:139`, `mod.rs:176-178`; close drain `shard.rs:2463` |
| `ShardEngine::submit_absorbed_batch_v2` | def `shard.rs:1962-1978`; callers `gather.rs:661`, `dst/tests/fixture_storage.rs:200` (`absorb_through`), `dst/tests/reads_history.rs:93,96,102` |
| `absorb_through` (fixture) | `reads_ring.rs:340,343,354`; `billing_maintenance.rs:405,509,592,625` (signature kept, see §4) |
| `CommitTransaction::absorbed` | only caller `mod.rs:178` |
| `GatherOutcome.advanced` (type unchanged) | `gather.rs:440,592,650,661,663,666`; `worker.rs:368`; `history_gather.rs:77,114,400,406,473-478,537,548,562,798,806,828,860,870,902`; `bounded_discovery_tests.rs` (`a.1`) |
| lane marks `submitted` | `gather.rs:340` (rollback), `:485` (plan_read), `:681` (raise); `worker.rs:136` (prune) |
| `roll_back_stranded_mark` | only caller `gather.rs:283` (commit 2 moves it to `plan_reads`) |
| `DurableEffects` | def `commit_plan.rs:34-45`; built `mod.rs:83`; consumed `commit_plan.rs:52-78` (`reply`/`reject`), `publish.rs:95` (InFlightGroup), `shard.rs:3016-3040` (`dispatch_durable`, partial moves then drop), `shard.rs:1896` (`begin_close`), `commit_handoff.rs:39-50` (`attach` moves acks only) |

`src/history/**` is not a critical prefix and has no mutation owner. `scripts/quality/verification_plan.py:22-31` lists `src/shard`, not `src/history`, and `scripts/quality/mutation_owners.py` registers only `src/shard/history_partition.rs`. The shard files are registered with the test filter `shard::`: `shard`, `commit_plan`, `transaction_maintenance`, `transaction_prepare`, `transaction_group` (`mutation_owners.py:89,110,113,114,116`).

---

## 2. Contract decision

**Committer (commit 1).** An absorbed advance now carries `CopiedBytes { from, len }`, meaning "I copied `len` stored frame bytes starting at offset `from`". `retire_absorbed` (in `commit_plan.rs`, a pure function) applies it only when `from == tail.absorbed`: the boundary moves to `upto.min(next)` and the ledger loses exactly `len`. Other advances:
- **Detached** (`from != absorbed`): dropped whole with a WARN. Nothing moves.
- **Diverged** (`len` greater than the ledger when `from == absorbed`): the group is rejected as today (`accounting_diverged`). This is genuine corruption and still fails closed; see D2.
- **Duplicate** (`upto <= absorbed`) and **cross-layout**: unchanged.

Invariant: every byte in `[absorbed, next)` leaves the ledger exactly once, whatever the absorber submits.

**Absorber (commit 2).**
- Each submitted batch carries a `SubmitReceipt`. The committer keeps the receipt of each advance it retired in the group's `DurableEffects`. `dispatch_durable` drops it after it has published `st.durable`; a refused group drops it with the refusal. Any other advance (detached, duplicate, cross-layout) drops its receipt at staging.
- `Submissions` counts the absorber's unsettled batches.
- `plan_reads` rolls a mark back only when `Submissions::settled()`, i.e. when no submitted advance can still land. The dirty-index rescan no longer rolls back. It only seeds pending work, and the gather that follows performs the rollback.

**At the edge.** Nothing on the wire changes: no status, code, header or body. An append co-grouped with an absorbed advance can no longer answer 500 `internal` or `append_failed` through this path, and a seal close can no longer fail a split through it. Genuine ledger corruption still answers 500 exactly as today, pinned by `over_retirement_fails_the_group_and_preserves_the_boundary`. The observable differences are in logs and timing:
- "rolling back stranded absorb mark" now appears only for genuinely stranded marks, and at plan time rather than at the rescan;
- a new WARN "dropped an absorb advance that does not start at the boundary" should never appear once both commits land;
- a refused batch heals at the next settled gather rather than at the next rescan (≤10 min in production).

`docs/refactor/WIRE-MATRIX.md` is unchanged. Its `500 internal` rows cover other causes.

---

## 3. Red tests, pins and non-vacuity controls

To capture the reds in commit 1 without a test that differs between the red and green runs, a helper builds each advance:
```rust
fn advance(hash, from, upto, len) -> CommitOp
```
On the pre-fix tree it builds the old tuple `(hash, upto, len)` and ignores `from`. In the commit it builds `CopiedBytes::new(from, len)`. The test bodies are identical in both runs. The implementer pastes the exact red output into the commit message, as in d4d631df.

### Commit 0 (prerequisite)
Cherry-pick `bc2907f7` (= ca80f9d4) onto slate unchanged. It is test-only: R1 ignored, plus its control. `8a2791b0` (the split harness) is a separate item (D8).

### Commit 1 reds
- **R1** `history::bounded_discovery_tests::a_rescan_during_an_inflight_advance_never_fails_an_append`. Un-ignore it.
  - Pre-fix, as recorded in ca80f9d4 (25/25):
    `a_rescan_during_an_inflight_advance_never_fails_an_append ... FAILED`
    `an append co-grouped with a re-gathered advance was refused: Err(Internal("maintenance accounting diverged"))`
  - After commit 1: ok. G1 is exact, G2 is detached, and the append commits.
- **R2a** (new) `shard::maintenance_tests::an_advance_that_overlaps_the_boundary_retires_nothing_and_fails_no_append`. It drives `ShardEngine::commit_group` on the test task, which is deterministic, as `transaction_tests.rs` does.
  - Setup: 8 appends of 100-byte payloads. `b(i..j)` is read from the stored rows with `engine.db.get(record_key(&h, i))`.
  - Group B = [`advance(h,0,4,b(0..4))`, `advance(h,0,8,b(0..8))`, one append].
  - Asserts, in order:
    - the answer is Ok (`"an append co-grouped with an overlapping advance was refused: {answer:?}"`);
    - applied `absorbed == 4`, `history_v2 == true`, `unabsorbed_bytes == b(4..8) + b(8)`, and the maintenance snapshot equals it;
    - `trimmed == 0`.
  - Group C goes through the channel: `submit_absorbed_batch_v2(vec![(h, 8, CopiedBytes::new(4, b(4..8)))])`, then a sentinel `try_enqueue` append, awaited under a 20 s `tokio::time::timeout`.
  - Asserts after group C: `absorbed == 8`, `(trim_safe_to, trimmed) == (4, 4)`, `record_key(h,0)` is gone, `engine.trim_deletes_total == 4`, and the ledger equals `b(8) + b(sentinel)`.
  - Expected pre-fix red:
    `test shard::maintenance_tests::an_advance_that_overlaps_the_boundary_retires_nothing_and_fails_no_append ... FAILED`
    `an append co-grouped with an overlapping advance was refused: Err(Internal("maintenance accounting diverged"))`
- **R2b** (new) `shard::maintenance_tests::an_advance_that_skips_offsets_is_dropped_whole`.
  - Fresh stream h2 with 2 records. One group: [`advance(h2,1,2,b(1))`, append].
  - Asserts: the answer is Ok, and `assert_eq!((t.absorbed, t.history_v2), (0, false), "an advance that skips offsets moved the boundary")`, and `unabsorbed_bytes == b(0)+b(1)+b(2)`.
  - Expected pre-fix red: `assertion \`left == right\` failed: an advance that skips offsets moved the boundary` / `left: (2, true)` / `right: (0, false)`.
- **Unit test (pin)** `shard::commit_plan::tests::retire_absorbed_retires_only_a_copy_that_starts_at_the_boundary`. Start from `absorbed=4, next=8, ledger=100`. Cases:
  - from 4 → Exact; the tail moves to 6 and the ledger to 40;
  - from 2 → Detached; the tail is unchanged;
  - from 6 → Detached; the tail is unchanged;
  - from 4 with len 101 → Diverged; the tail is unchanged;
  - `upto > next` → absorbed = next.
- **Loom** (new file `src/shard/commit_plan/loom_tests.rs`, `#![cfg(test)]`, explicit imports, no statics). Settings follow precedent: `max_threads` 2, `max_branches` 1000, `preemption_bound` Some(2), no permutation or duration cutoff. The main thread is the absorber and one spawned thread is the committer.
  - The committer runs the actual `retire_absorbed` on an actual `TailFields` under a Loom mutex (8 records of distinct sizes). It publishes `durable` under a second Loom mutex after each apply, as `dispatch_durable` does after staging. It pops queued advances from a `Mutex<VecDeque>`, yielding while the queue is empty.
  - The absorber's steps are transcribed from `plan_read`, `raise_lane_marks` and the rescan rule.
  - `quality_loom_an_unconditional_rollback_never_retires_a_byte_twice`:
    - absorber: G1 [0,4) with mark 4; then the ungated rescan rule (roll back if `mark > durable`), which is today's absorber and also the prune path; then G2 from `max(mark, durable)` to 8;
    - committer: two applies, guarded by `upto > absorbed` (transcribing `absorbed`'s guard);
    - invariants: never `Diverged`, and `ledger == bytes(absorbed..8)` after every apply;
    - after the join, heal passes until `absorbed == 8`, then `ledger == 0`.
  - Non-vacuity control `quality_loom_an_upto_only_retirement_counts_a_regathered_overlap_twice`: the same model with the pre-fix rule (retire `len` whenever `upto > absorbed`). It records a `checked_sub` failure into an `Arc<AtomicBool>` captured by the model closure, and asserts after `check()` that some interleaving diverged.

### Commit 2 reds
- **R3** (new) `history::bounded_discovery_tests::a_rescan_during_an_inflight_advance_regathers_from_the_submitted_mark`. `append_behind_a_regather` is extended to return a `Regather { answer, tail, last_record_bytes }`: the applied tail read before `begin_close`, and the stored length of record 8. R1 and its control destructure it and ignore the extra fields.
  - Asserts: the answer is Ok; then `assert_eq!(tail.absorbed, 8, "the regather after a rescan started below the in-flight advance")`, `(trim_safe_to, trimmed) == (4, 4)`, and `unabsorbed_bytes == last_record_bytes`.
  - Red on the commit-1 tree: `... started below the in-flight advance` / `left: 4` / `right: 8`.
  - Red on eda79ccf: R1's `Err(Internal(...))`.
- **R4** (new) `history::bounded_discovery_tests::a_refused_advance_is_regathered_from_the_durable_boundary`.
  - Steps:
    1. Append 4 records and call `engine.fail_next_absorbed_group()`.
    2. G1 gathers [0,4) and its group is refused.
    3. Wait for `engine.group_failures_tripped() >= 1`, then for a sentinel append (offset 4). The sentinel is FIFO behind the refused group, so G1's receipt is gone.
    4. G2 gathers.
    5. A second sentinel (offset 5) is appended and awaited.
  - Asserts: `assert_eq!(t.absorbed, 5, "a refused advance was not regathered from the durable boundary")`, then `assert_eq!(t.unabsorbed_bytes, b(5), "a refused advance left a phantom backlog")`, and the maintenance snapshot equals it.
  - Red on eda79ccf: absorbed is 5, but `a refused advance left a phantom backlog` / `left: <b(0..4)+b(5)>` / `right: <b(5)>` (the byte values are the stored frame lengths).
  - Red on the commit-1 tree: `... not regathered from the durable boundary` / `left: 0` / `right: 5`.
- **R5a (pin)** `shard::maintenance_tests::a_receipted_advance_settles_only_after_its_group_is_durable`. It uses a FaultStore and `store.hold_class(Put, Wal, 1)` after 4 durable appends.
  1. `let s = Arc::new(Submissions::default()); let r = s.submit();`
  2. Submit `(h, 4, CopiedBytes::new(0, b(0..4)).receipted(r))`.
  3. Wait (bounded) for applied `absorbed == 4`, then assert `!s.settled()`.
  4. `store.release_hold()`, wait (bounded) for durable `absorbed == 4`, then wait (bounded) for `s.settled()`.
- **R5b (pin)** `shard::maintenance_tests::a_refused_advance_settles_with_its_group`: `fail_next_absorbed_group`, submit a receipted [0,4), wait for tripped ≥ 1, then (bounded) for `s.settled()`; applied `absorbed == 0`.
- **Unit test (pin)** `shard::commit_plan::tests::a_batch_settles_when_its_last_receipt_drops`:
  - a default `Submissions` is settled;
  - after `submit()` it is not settled; a clone keeps it unsettled; dropping the last clone settles it;
  - two batches settle independently.
- **Loom** (extend the same file). Settlement runs the actual `Submissions<loom AtomicU64>` (`begin`/`settle`/`settled`). The committer calls `settle()` after publishing `durable`, which models the receipt dropped at the end of `dispatch_durable`'s group iteration. `ReceiptInner`'s `Drop` wiring and the custody in `DurableEffects` are covered by R5a, R5b and the unit test, not by Loom.
  - `quality_loom_an_unsettled_batch_keeps_its_lane_mark`: the absorber reads `settled()`, then `durable` (lock 1, rollback), then `durable` again (lock 2, `from`), as `plan_reads` does with `resident_absorbed` and `plan_read`. Invariant: the committer never sees a `Detached` advance. Final state is exact.
  - Control `quality_loom_an_unconditional_rollback_regathers_under_an_unsettled_batch`: the gate is replaced by `true`. It records that some interleaving produced `Detached`.
  - `quality_loom_a_refused_batch_is_regathered_from_the_durable_boundary`: the committer refuses G1 (settles without applying). G2 either rolls back and runs [0,8) exactly, or plans unsettled from 4 and is dropped as Detached. The post-join heal is then exact. Invariants: never `Diverged`; final `absorbed == 8`, ledger 0.
  - Ordering: `settle` uses Release and `settled` uses Acquire. As a local experiment (not committed), run the gated model with both Relaxed and record the result in the commit message. Mutex coherence should make Relaxed pass as well.

### Planted non-vacuity controls (local, not committed; record them in the messages)
- Commit 1: make `retire_absorbed` ignore `from`. R2a, R2b, the Loom model and the unit test fail, and so does R1.
- Commit 2:
  - replace the `settled` gate in `plan_reads` with `true`: R3 fails (`left: 4`);
  - make `CopiedBytes::into_receipt` return `None`: R5a fails (`settled` before durable);
  - remove the `plan_reads` rollback: R4 fails (`left: 0`).

---

## 4. Edits file by file, in commit order

The exception contract applies throughout: no `#[expect]` reason is edited, and no `exception-growth.json` row is added. Line counts are physical lines: `shard.rs` 3139 and `history.rs` 1656 must not grow; every other file stays ≤ 1000.

### Commit 1: "The committer retires an absorbed advance only from its own boundary, so no rescan can retire a byte twice"

**`src/shard/commit_plan.rs`** (236 → ~310; no file-level exception)
- `pub(crate) struct CopiedBytes { pub(crate) from: u64, pub(crate) len: u64 }` with `pub(crate) fn new(from, len)`. Derive nothing; in particular no `Default`, so a `new` mutant is unviable.
- `pub(crate) type AbsorbedAdvance = ([u8; 16], u64, CopiedBytes);`
- `#[derive(Clone, Copy, Debug, Eq, PartialEq)] pub(super) enum AbsorbRetirement { Exact, Detached, Diverged }`.
- `pub(super) fn retire_absorbed(tail: &mut TailFields, upto: u64, copied: &CopiedBytes) -> AbsorbRetirement`:
  - `from != tail.absorbed` → Detached;
  - `checked_sub` fails → Diverged;
  - otherwise set `absorbed = upto.min(next)` and the ledger to the remainder, and return Exact.
- The unit test goes into the existing inline `mod tests`, which already has an unresolved-glob allowance.
- Add `#[cfg(test)] mod loom_tests;`.

**`src/shard/commit_plan/loom_tests.rs`** (new, ~140). `#![cfg(test)]` means production is unchanged, so this file is not a mutation source (precedent: `src/quota/pin/loom_tests.rs`). Explicit `use super::{...}` means no glob row is needed. The control flag is an `Arc<AtomicBool>` captured by the closure, so there is no static and no `global` row.

**`src/shard.rs`** (3139 → 3139)
- Line 33: add `CopiedBytes` to the `pub(crate) use commit_plan::{...}`. The line becomes 109 characters, so rustfmt splits it into 3 lines: +2.
- Lines 34-37 (private `use commit_plan::{...}`): add `AbsorbedAdvance`. The inner lines repack to 95 and 83 characters, still 2 lines: +0.
- `CommitOp` (scope of `#[expect(clippy::large_enum_variant)]`):
  - `streams: Vec<([u8; 16], u64, u64)>` → `streams: Vec<AbsorbedAdvance>`: same line, path facts go from 4 to 2;
  - `bytes: u64` → `bytes: CopiedBytes`: same line, same path count;
  - the field and variant names `upto`/`bytes`/`v2` stay unchanged, so `expand` and `stage` compile with identical text;
  - the `AbsorbedBatch` doc (`:919-926`, 8 lines) is rewritten to 6 lines naming the entry (hash, new upto, copied bytes): −2;
  - the `bytes` field doc becomes one line, `/// Copied bytes; retired only when they start at the absorbed boundary.`: ±0.
  - Doc lines are attributes, not scope lines. Scope metrics: lines =, items =, facts −2.
- `submit_absorbed_batch_v2` (scope of `let_underscore_must_use`):
  - the parameter becomes `streams: Vec<AbsorbedAdvance>`. Writing `CopiedBytes` in the tuple would make the line 101 characters, rustfmt would wrap it to +3 lines, and that would be scope growth; the alias avoids it. The line is ~84 characters;
  - the doc line "Entries are (hash, new upto, frame bytes copied)" becomes "(hash, new upto, copied bytes)";
  - the body is unchanged. Scope: lines =, facts −2.
- Net physical lines: +2 −2 = 0.

**`src/shard/transaction/maintenance.rs`** (229 → ~245)
- Add `use crate::shard::commit_plan::{AbsorbRetirement, retire_absorbed};`. A descendant of `shard` may use the private module.
- `absorbed` (contracts `too_many_arguments` and `cfg_attr(test, expect(disallowed_methods))`):
  - the signature changes `bytes: u64` → `bytes: CopiedBytes` (same line and fact count; arity stays 6, so the expectation is still fulfilled);
  - the `#[cfg(test)]` `DST_DRAIN_TRACE` block stays *in* `absorbed` unchanged, which keeps the owners.json effect row `crate::CommitTransaction < '_ >::absorbed` / `std::env::var` valid;
  - the lane check and the cross-layout drop are unchanged;
  - lines 186-217 are replaced by:
    ```rust
    if lane_ok && upto > prev_absorbed {
        let moved = self.advance_boundary(local, hash, upto, bytes);
        if moved && v2 {
            local.fields.history_v2 = true;
        }
    }
    ```
  - Scope: code lines 67 → ~36, facts fall sharply, items stay 0. This shrinks.
- New `fn advance_boundary(&mut self, local, hash, upto, bytes: CopiedBytes) -> bool` (5 arguments, no exception, ~45 lines, nesting ≤ 3). It moves the retirement block out of the excepted scope, which RUST-QUALITY.md sanctions: "moving code out of its scope". It needs no new exception, so no owner is re-attached.
  - `let prev_absorbed = local.fields.absorbed;` then `match retire_absorbed(&mut local.fields, upto, &bytes)`:
  - Exact → continue;
  - Detached → `tracing::warn!(shard, stream, from, upto, prev, "dropped an absorb advance that does not start at the boundary")` and `return false`;
  - Diverged → set `accounting_diverged` with the existing message text (`retire_bytes={bytes.len}`) and `return false`.
  - Then, moved verbatim: `#[cfg(test)] group_has_absorbed = true`, `retired_bytes += bytes.len`, `trim_safe_to = max(prev)`, the budgeted trim loop and budget update, then `true`.

**`src/history/gather.rs`** (694 → ~712; not critical)
- Line 18: import `CopiedBytes`.
- `Staged` gets `copies: Vec<([u8; 16], u64, CopiedBytes)>` (doc: the committer's view of the same advances) and is initialized at `:391-397`.
- `stage_chunk` (`:592`): also push `(plan.hash, last + 1, CopiedBytes::new(plan.from, chunk_raw))`. `plan.from` is where the copy starts; offsets below the frontier are dense (`record.rs:140-143`).
- `commit` (`:621`): destructure `copies` and submit them instead of `out.advanced.clone()`.
- Rewrite `roll_back_stranded_mark`'s doc (`:323-334`; attributes only, so no scope change) to replace the false premise with "the committer drops an advance that does not start at its boundary".

**`src/dst/tests/fixture_storage.rs`** (301 → ~305). `absorb_through(engine, hash, upto, retired_bytes)` keeps its signature, so none of its 7 callers change. That matters because 4 of them sit in `#[expect]`-scoped DST tests. It derives `from` from the handle's `applied.absorbed`, and its doc says so. Every caller waits for the previous advance, or holds the committer, before calling it.

**`src/dst/tests/reads_history.rs`** (568). Its three calls become `CopiedBytes::new(0,0)`, `::new(3,0)`, `::new(3,0)`; for b, the lane check drops it first, so `absorb_lane_dropped == 1` holds. `the_first_advance_seals_the_history_layout` has no `#[expect]`; only its inner `wait_absorbed` has one, and that is untouched.

**`src/shard/maintenance_tests.rs`** (222 → ~360). Add R2a and R2b plus helpers (`rig`, `append_op`, `advance`, `stored_bytes`). The AppendReq field list is copied from ca80f9d4's `enqueue_record`. Each test function stays ≤ 100 lines and nesting ≤ 4, so no new `#[expect]`.

**`src/history/bounded_discovery_tests.rs`**. Remove R1's `#[ignore = ...]` and update its doc to the past tense.

Ratcheted scopes **not touched**, verified text-identical:
- `CommitTransaction::expand` (`unwrap_used` and `excessive_nesting`; the closure `|(hash, upto, bytes)| CommitOp::Absorbed { hash, upto, bytes, v2 }` is unchanged);
- `CommitTransaction::stage` (`match_same_arms`);
- `run`, `publish`, `StreamOverlay::new`, `DurableEffects::{reply, reject}`, `committer_loop`, `dispatch_durable`;
- `plan_read`, `raise_lane_marks`, `Absorber::run`;
- every DST test that carries an `#[expect]`.

### Commit 2: "A lane mark is rolled back only when every submitted batch has settled, so a regather never starts under an advance in flight"

**`src/shard/commit_plan.rs`** (~310 → ~400)
- `pub(crate) trait SettlementWord { load; fetch_add; fetch_sub }`, with an impl for `std::sync::atomic::AtomicU64`. This follows the `CounterWord` and `CustodyWord` precedents.
- `#[derive(Default)] pub(crate) struct Submissions<W = AtomicU64> { unsettled: W }` with:
  - `begin()` (`fetch_add` 1, Relaxed);
  - `settle()` (`fetch_sub` 1, Release);
  - `settled() -> bool` (`load` Acquire `== 0`);
  - and, for `Submissions` (std) only, `submit(self: &Arc<Self>) -> SubmitReceipt`, which calls `begin()` and wraps `Arc<ReceiptInner>`.
- `struct ReceiptInner(Arc<Submissions>)` with `impl Drop { self.0.settle() }`, and `#[derive(Clone)] pub(crate) struct SubmitReceipt(Arc<ReceiptInner>)`.
- `CopiedBytes` gains a private `receipt: Option<SubmitReceipt>`; `new` sets it to None. Add `pub(crate) fn receipted(self, SubmitReceipt) -> Self` and `pub(super) fn into_receipt(self) -> Option<SubmitReceipt>`.
- `DurableEffects` gains `pub receipts: Vec<SubmitReceipt>`. The struct derives Default and implements no `Drop`, so the partial moves in `dispatch_durable` still compile. The receipts drop when `group` drops at the end of each iteration, *after* `:3016-3019` has published `st.durable`. `reject` and `begin_close` drop them with the refusal.
- Add the unit test, and the Loom additions in `loom_tests.rs`.

**`src/shard.rs`** (3139 → 3139). Add `Submissions` inside the already-split `pub(crate) use commit_plan::{...}`. The inner line is 96 characters, still one line: +0.

**`src/shard/transaction/maintenance.rs`**. In `advance_boundary`'s Exact path, after the moved lines: `if let Some(receipt) = bytes.into_receipt() { self.effects.receipts.push(receipt); }`. `advance_boundary` is a new, non-excepted function. The detached, duplicate and cross-layout paths drop the receipt at staging; those advances can never land.

**`src/history.rs`** (1656 → 1656)
- Line 27 import adds `Submissions`: ±0.
- Add the field `submissions: Arc<Submissions>` with a 2-line doc: +3.
- `new()` gets `submissions: Default::default(),`: +1.
- The `submitted` doc (`:736-751`, 16 lines) is revised to 12 lines: −4. It restates the premise the fix makes true: the committer retires an advance only from its boundary, and a mark is rolled back only while `submissions` is settled. It keeps the lane-scoped rationale.
- The `Absorber` struct and `new` carry no exception.

**`src/history/gather.rs`**
- `seed_from_dirty_index` (`unwrap_used`): delete line 283 (`self.roll_back_stranded_mark(h, absorbed);`). This removes 1 line and some facts; fingerprints are only removed.
- `plan_reads` (no exception): compute `let settled = self.submissions.settled();` once. For each stream, after `stream_handle`:
  ```rust
  if settled && let Some(durable) = self.shard.resident_absorbed(hash) {
      self.roll_back_stranded_mark(*hash, durable);
  }
  ```
  `resident_absorbed` already owns its unwraps (`shard.rs:2426-2434`), so no new unwrap appears outside a scope.
- `commit`: `let receipt = self.submissions.submit();` and map each copy through `receipted(receipt.clone())` before `submit_absorbed_batch_v2`.
- Rewrite `roll_back_stranded_mark`'s doc to describe the plan-time, settlement-gated rule (attributes only).

**Tests**: R3 and R4 plus the helper change in `bounded_discovery_tests.rs` (~267 → ~370); R5a and R5b in `maintenance_tests.rs` (→ ~450).

Ratcheted scopes touched in commit 2: `seed_from_dirty_index` shrinks, `roll_back_stranded_mark` gets a doc-only change, and the `shard.rs` re-export line is not a scope. Nothing grows.

---

## 5. Mutation analysis (`cargo mutants --in-diff`, owners filtered by `shard::`)

Changed critical files are `src/shard.rs`, `src/shard/commit_plan.rs` and `src/shard/transaction/maintenance.rs`. The owners selected are `shard`, `commit_plan` and `transaction_maintenance`. The test-only files (`maintenance_tests.rs`, `commit_plan/loom_tests.rs`) carry `#![cfg(test)]`, so they count as production-unchanged and are not sources. `src/history/**` is not critical and not registered. R1, R3 and R4 therefore do not count for mutants; every in-diff mutant must die under `shard::` tests.

Every new test awaits under a timeout of 20 s or less, and the Loom models are bounded by their branch limit. A mutant that panics the committer task drops the queued replies (RecvError), so it fails fast instead of hitting the 90 s `--timeout`.

| Mutant | Killed by |
|---|---|
| `retire_absorbed`: `!=`→`==` | unit test; R2a (G1 detached, `absorbed` 0 ≠ 4); Loom model |
| `retire_absorbed` → default | unviable (`AbsorbRetirement` has no Default) |
| `CopiedBytes::new` → default | unviable (no Default) |
| `absorbed` → `()` | R2a (`absorbed` stays 0) |
| `moved && v2` → `\|\|` | R2b (`history_v2` becomes true on a skip advance) |
| `advance_boundary` → `true` | R2b (`history_v2` true; tail must not change) and R2a (`absorbed` 0) |
| `advance_boundary` → `false` | R2a (`absorbed` 0, `history_v2` false) |
| `retired_bytes += len` → `-=` | underflow panic in `commit_group` on the test task (R2a) |
| `retired_bytes += len` → `*=` | reconciliation `finalize.rs:80-90` diverges, so the append in R2a is Err |
| `trimmed + allowed` → `-` | underflow panic (R2a group C; the sentinel sees RecvError) |
| `trimmed + allowed` → `*` | R2a `trimmed == 4` fails |
| `trim_budget -=` → `+=` | R2a `trim_deletes_total == 4` fails (trim_used = 0) |
| `trim_budget -=` → `/=` | division by zero on the first advance (delta 0), a panic on the test task |
| `submit_absorbed_batch_v2` → `()` (selected only if the signature line counts) | R2a group C and R5a go through it |
| (commit 2) `Submissions::begin` → `()` | R5a (`settled` before durable) |
| `settle` → `()` | R5a / R5b bounded wait |
| `settled` → true / false; `== 0` → `!=` | unit test, R5a |
| `ReceiptInner::drop` → `()` | R5a / R5b bounded wait |
| `into_receipt` → `None` | R5a (settles at staging, before durable) |
| `SettlementWord for AtomicU64`: `load`→0/1, `fetch_add`/`fetch_sub`→0/1 | R5a (settled too early, or never) |
| `receipted`, `submit` → default | unviable |

- The `shard.rs` edits (types, docs, re-export) produce no executable mutants.
- `expand` and `stage` are text-identical, so their whole-function mutants are not selected. This keeps the hazard named in `stage`'s `match_same_arms` reason out of scope.
- The unmutated baseline must pass all `shard::` tests.

---

## 6. Ledgers, each in the same commit as its change

- **`docs/refactor/test-inventory.json`** (covers `src/dst/**` only). In commit 1, only `the_first_advance_seals_the_history_layout` changes its `function_sha256`: run `python3 scripts/test-inventory.py --write` and review that the diff is exactly that entry. Commit 2 changes no DST test text, and `--check` must print OK.
- **`docs/refactor/review-mechanisms.json`**. Add a mechanism, recommended but see D7: `id: "absorb-retirement-settlement"`.
  - `obligations`: the owner's release-hold ID (placeholder `"HOLD-SPLIT-500"`, D7).
  - `owner`: "ShardEngine committer absorbed-boundary retirement; history Absorber lane marks".
  - `mechanism`: held commit gate with a queued gathered advance, plus a held WAL put under applied records, plus the dirty-index rescan, plus a refused absorbed group (`fail_next_absorbed_group`).
  - `entered_proof`: WAL hold engaged with `applied.next == 8`; G1 gathered before the gate releases; `group_failures_tripped >= 1` and a sentinel answered after the refused group.
  - `oracle`: co-grouped append Ok; boundary and ledger equal the exact bytes of `[absorbed, next)`; the regather starts at the mark; a refused advance is regathered from the durable boundary.
  - `configuration`: `{records: 8, payload_bytes: 100, flush_interval_ms: 5, worker_threads: 4}`.
  - `limitations`: in-memory store; the capacity rate is covered by the control loop in §7.
  - `execution`: `requires_final_head_receipt`.
  - `tests`: R1, R2a, R2b (commit 1), then R3, R4, R5a, R5b (commit 2). Compute each `sha256` with `scripts/test-inventory.py`'s `functions()` (the same hash `review-evidence.py` checks), and run `python3 scripts/review-evidence.py --check`.
- **`docs/quality/owners.json`**: no rows. The new test module uses explicit imports and has no statics. There are no new spawns or environment reads. The `absorbed` `std::env::var` row stays valid because the trace stays in `absorbed`. The glob rows for `maintenance_tests.rs` and `bounded_discovery_tests.rs` already exist.
- **`scripts/quality/mutation_owners.py`**: no rows. There is no new production file; `commit_plan/loom_tests.rs` is `#![cfg(test)]`, as `quota/pin/loom_tests.rs` is.
- **`docs/refactor/WIRE-MATRIX.md`**: unchanged (§2).
- **`docs/quality/exception-growth.json`**: untouched. No growth, as shown in §4.

---

## 7. Controls (commands and expected output)

1. **Red capture** (local, before each fix, with that commit's tests applied):
   - Commit 1 reds: `cargo test --locked --release --lib -- --include-ignored history::bounded_discovery_tests::a_rescan shard::maintenance_tests::an_advance_that` gives 3 FAILED with the messages in §3.
   - Commit 2 reds, on the commit-1 tree: `cargo test --locked --release --lib history::bounded_discovery_tests::a_re` gives R3 FAILED (`left: 4`) and R4 FAILED (`left: 0`).
2. **Green**: `cargo test --locked --release --lib history::bounded_discovery_tests:: shard::maintenance_tests:: shard::commit_plan::` passes. This includes R1's control and `r09_discovery_pages_progress_without_exceeding_pending_capacity`.
3. **Loops**: `for i in $(seq 25); do cargo test --locked --release --lib history::bounded_discovery_tests:: shard::maintenance_tests:: || break; done` gives 25/25 (ca80f9d4's red was 25/25 deterministic).
4. **Loom**: `cargo test --locked --release --lib quality_loom_` passes. The existing commit_handoff, quota pin and sweep custody models plus the 5 new ones pass, and the two control tests pass because they found the failing interleaving.
5. **Planted controls** (§3, not committed): each named test fails, and the output goes into the commit message.
6. **Quality**: `QUALITY_OUT=target/q scripts/quality.sh` exits 0. That covers fmt, clippy `-D warnings`, `gate.py` (no exception growth, no file growth, no unregistered occurrence), rustdoc `-D warnings`, `test-inventory --check` OK, and `review-evidence --check` OK.
7. **Mutation plan as CI will see it**: `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py --out target/qm-plan`. Expected `plan.json`:
   - `mutation_source_files` = `["src/shard.rs","src/shard/commit_plan.rs","src/shard/transaction/maintenance.rs"]`;
   - `selected_mutation_owners` = `["shard","commit_plan","transaction_maintenance"]`, in the `OWNERS` table order (`mutation_owners.py:89,110,113`). That is exactly these three owners, with no `transaction_prepare` or `transaction_group`, because `prepare.rs` and `mod.rs` are untouched;
   - `unregistered_mutation_source_files` = `[]`.
8. **Mutation run**: `QUALITY_MUTANTS_OUT=target/qm scripts/quality/mutations.sh` prints `Mutation verification executed N selected mutant(s) across 3 registered owner(s).` The cargo-mutants outcome shows 0 missed and 0 timeout.
9. **Full gate**: `OUT=target/gate.txt scripts/gate.sh` ends with `GATEDONE`: the suite with `tests_ran --skipped 1`, plus the capacity leg `dst::dst_tests::topology_scaling::post_split_throughput_scales`.
10. **Release-hold acceptance**, reusing the investigation's local, uncommitted diagnostic patch (WARN tracing plus window markers): 20 runs of `cargo test --locked --release --lib post_split_throughput_scales -- --exact dst::dst_tests::topology_scaling::post_split_throughput_scales` under the same host load (two looping lib suites). Expected:
    - 20/20 pass with ratio ≥ 1.8 (band 1.82-2.01 before the fix);
    - 0 lines `maintenance accounting diverged`;
    - 0 lines `dropped an absorb advance that does not start at the boundary`;
    - 0 lines `rolling back stranded absorb mark`, since no group is refused in that workload (before the fix: 37 rollbacks, 23 rejected groups);
    - zero 500s.
11. **If 8a2791b0 is on slate**: `cargo test --locked --release --lib split_boundary_outcomes` gives outcomes unchanged, with no phase answering 500.

---

## 8. Out of scope

- F1 and F2 from 8a2791b0: producer and Stream-Seq lineage not recognized on the high child's engine, and a retired parent's written group refused 503 `shard_moving` although it became durable. These are separate items with their own ignored reds.
- Gating the `worker.rs` prune (`:136-142`), which drops a mark for an evicted handle regardless of settlement. Commit 1 keeps the ledger exact on that path. Gating it would edit `Absorber::run`'s excepted scope (D6).
- Per-stream settlement (D5).
- Auditing or repairing partitions that already hold overlapping postings pages, or tails that already hold an under-counted ledger from runs before the fix (D4).
- `/v1/debug/load` counters for detached drops and plan-time rollbacks (D3).
- Containment of genuine divergence (D2).
- Batch 13 and the other release-safety items.

---

## 9. Decisions for the owner

- **D1 — Adopt both layers.** Recommended: yes. Commit 1 (committer exactness) plus commit 2 (settlement-gated rollback), pushed together after commit 0.
  - Commit 1 alone ends the 500 but leaves overlapping postings pages in every race, with permanent envelope fallbacks counted as `POSTINGS_CORRUPT` (§1.4).
  - Commit 2 alone leaves the refusal-under-load and eviction-prune paths able to over-count or diverge.
- **D2 — Containment.** Keep fail-closed on genuine divergence; recommended: yes. After commit 1, an absorbed op diverges only if the ledger is already corrupt. The alternative, dropping a diverging absorbed op, keeps co-grouped appends 200 but silently strands that stream: every later advance would diverge too. That would reverse R26-3 (`over_retirement_fails_the_group_and_preserves_the_boundary`).
- **D3 — Observability counters.** Add counters for detached drops and plan-time rollbacks to `/v1/debug/load`? This needs `ShardEngine` fields in `shard.rs` and an `http.rs` export, and both files may not grow without compensation or an approved row. Recommended: follow-up; the WARNs suffice for the acceptance loop.
- **D4 — Pre-fix data.** Rig and soak namespaces from before the fix may hold tails with under-counted `unabsorbed_bytes`, which the committer will now correctly refuse as Diverged at catch-up, and partitions with overlapping pages. Recreate them (recommended; production is greenfield), or authorize a one-time rebuild of tail ledgers from row sizes?
- **D5 — Healing latency.** The rollback is gated globally: while *any* batch is unsettled, no mark is rolled back. On short-tick rigs under sustained load, a refused group heals only at a settled plan, and if gap gathers happen meanwhile, commit 1 drops them, which can overlap pages for that stream. Accept (recommended), or add per-stream settlement? The per-stream version needs a new lock, and so a new exception, or a lock-free per-stream structure. A lighter refinement is to keep such streams pending (`deferred_budget`) instead of `no_work` while unsettled.
- **D6 — Worker prune.** Leave the prune ungated (recommended; ledger-safe after commit 1), or approve an edit inside `Absorber::run`? That means `too_many_lines` and `unwrap_used` growth, which needs an owner row.
- **D7 — Mechanism entry.** Add the `review-mechanisms.json` entry (recommended), and choose its obligation ID (placeholder `HOLD-SPLIT-500`).
- **D8 — Investigation commits.** Land commit 0 (the ca80f9d4/bc2907f7 cherry-pick) as its own commit (recommended) or fold it into commit 1. Separately, decide when to land 8a2791b0/706430f7 (the split harness with the F1/F2 reds ignored).
- **D9 — Acceptance evidence.** Is §7.10 (20 capacity runs under load with zero divergence, drop and rollback lines) the acceptance evidence for lifting the release hold, or does the owner want a longer loop?

---

## Skeptic corrections (C1..C14)

Checked against the tree at `slate`, which has moved to `46f668b3`, one commit past `eda79ccf` (C12), and against `ca80f9d4`/`bc2907f7` and `8a2791b0`/`706430f7`. Nothing was built or run.

**What holds up:**
- Every quoted line matches the tree: `worker.rs:47,83-87`, `gather.rs:275-283,339-351,476-491,592,661`, `maintenance.rs:186-202`, `finalize.rs:5-8`, `contract.rs:298`, `http.rs:27`, `product.rs:2354`, `postings.rs:259-268`, `history.rs:944-958`.
- `git diff d4d631df eda79ccf -- src/shard* src/history* src/dst/fault_store.rs src/dst/tests/fixture_storage.rs` is empty, so the red regression still applies to slate as written. The AppendReq field list in `ca80f9d4` matches `shard.rs`.
- The investigation's causal trace holds. The overlay starts from `applied` (`overlay.rs:44`), and the committer compares only `upto > prev` (`maintenance.rs:186`).
- Commit 1's exact-start rule closes the double count.
- Commit 2's global gate is sound for its purpose:
  - `Absorber::start_owned` gives one absorber per engine as a required task (`history.rs:819-825`).
  - That absorber is the only production producer of `AbsorbedBatch` (`gather.rs:661`, confirmed by grep).
  - Gathers and rescans run in one task (`worker.rs:83-172`).
  - `applied` moves at local write (`publish.rs:52`), and `durable` moves only in `dispatch_durable` (`shard.rs:3017-3020`).
  - So "no receipt outstanding" does imply `applied.absorbed == durable.absorbed` for this absorber's streams.
- Every refusal path drops the op or its effects, and with them the receipt: `reject_op` (`mod.rs:59-64`), `finish` reject (`finalize.rs:7,21,29`), write error (`finalize.rs:184`), the retired-handoff path in `publish` (`publish.rs:26-31`), `begin_close` (`shard.rs:1896`), and the committer close drain (`shard.rs:2463`).
- `attach` (`commit_handoff.rs:39-50`) moves acks only. This is safe because a receipt only rides an Exact advance, and an Exact advance always changes the tail row, so its group has writes (`finalize.rs:98-115`).
- The red and green arithmetic for R1, R2a group B, R2b, R3 and R4 checks out, including R4's phantom `b(0..4)+b(5)` on the current tree.

- **C1 — R2a does not compile on the pre-fix tree, so the red cannot be captured as §3 describes.** Group C calls `submit_absorbed_batch_v2(vec![(h, 8, CopiedBytes::new(4, b(4..8)))])` directly in the test body. `CopiedBytes` does not exist before commit 1. That contradicts §3's rule that "the test bodies are identical in both runs".
  - Fix: route group C through the same switchable helper, for example `submit_advance(&engine, h, from, upto, len)`.
  - The pre-fix red then stops, as recorded, at group B's first assertion.

- **C2 — `advance_boundary(bytes: CopiedBytes)` fails clippy in commit 1.** In commit 1 the value is only borrowed: `retire_absorbed(.., &bytes)`, then `bytes.len` and `bytes.from` are read. `clippy::needless_pass_by_value` is `warn` in `Cargo.toml` `[workspace.lints.clippy]`, and CI runs with `-D warnings`. A new `#[expect]` for it would be exception growth.
  - Fix, either way:
    - derive `Clone, Copy` on `CopiedBytes` in commit 1 (it still has no `Default`, so the `new` mutant stays unviable) and drop `Copy` in commit 2, where `into_receipt(self)` consumes it; or
    - take `&CopiedBytes` in commit 1 and switch to by-value in commit 2.
  - `absorbed` itself is fine: it moves `bytes` into `advance_boundary` on the lane-ok branch.

- **C3 — Ledger: the R1 pin goes stale in commit 2.** §6 pins R1's `sha256` in the new `review-mechanisms.json` entry during commit 1. Commit 2 then edits R1's body (the `Regather` destructuring in §3), which changes `function_sha256`. `scripts/review-evidence.py:176-180` then fails with "mechanism test changed or missing".
  - Commit 2 must re-pin R1, and its control if listed.
  - Consider pinning the shared helper `append_behind_a_regather` under `support_functions` (`review-evidence.py:183-188`), so a helper edit cannot silently change what R1, R3 and the control prove.

- **C4 — Use-site list: `absorb_through` has 9 callers, not 7.**
  - Missing: `src/dst/tests/runtime_sweep.rs:114` and `src/dst/tests/billing_maintenance.rs:727`. Both are safe, because `from` is derived from `applied.absorbed` on a settled or fresh engine.
  - State explicitly that the D2 pin `over_retirement_fails_the_group_and_preserves_the_boundary` survives. It calls `absorb_through` at `billing_maintenance.rs:592` with the commit gate held and the rider append queued first. The rider does not move `absorbed`, so `from = 0 == absorbed`: the advance is Exact, then Diverged, then the group is refused, as today.
  - Also state that `from` is sampled at call time, not at staging. Any future caller that submits behind an unapplied advance gets Detached, not a retirement.

- **C5 — D5 understates the residual harm, and "heals at the next settled gather" (§2) is inaccurate.**
  - After a refusal, `settle_gather` (`worker.rs:317-327`) removes the stream from `pending`, because G1 counted as advanced. The stream heals only at a settled plan *that includes it*, and it is re-added only by a signal or by the 120-tick rescan.
  - At the rig's load, P(unsettled at plan time) is high: the investigation found an in-flight mark at roughly half of all rescans.
  - While unsettled, every new append makes a gap gather: `from = mark > durable` (`gather.rs:483-490`). Commit 1 drops each one as Detached, but its postings pages are already written (`gather.rs:640-661`). Once the stream finally regathers from `durable`, those pages overlap the new ones: permanent `POSTINGS_CORRUPT` and envelope fallback (`postings.rs:259-268`, `history.rs:944-958`). There is no bound on how many.
  - Choose one fix before D5 can be recommended as "Accept":
    - (a) **Bucketed per-stream settlement.** `Submissions` holds a fixed lock-free `[AtomicU64; N]` indexed by hash prefix. A receipt carries its batch's bucket indices, `begin`/`settle` update each one, and rollback of stream X is gated on `bucket(X) == 0`. This needs no new lock, map or exception, and the same Loom model covers it. It is exact up to false sharing, which only errs toward not rolling back. N is the owner's memory trade-off.
    - (b) **A bounded forced settlement.** After K consecutive unsettled plans, `plan_reads` waits, bounded, for `settled()`. The absorber is the only submitter, so waiting drains its in-flight batches.
  - Either way, add a red: a refused batch under a continuously busy absorber heals within a bounded number of gathers with zero Detached drops.
  - If the owner accepts D5 as it stands, the decision text must say "gap gathers during an unsettled window leave overlapping pages". The evidence should be a §7.10 variant that injects `fail_next_absorbed_group` under load and counts `POSTINGS_CORRUPT`.

- **C6 — Loom and test boundedness against mutation TIMEOUT.** CI fails on TIMEOUT (`--timeout 90`, `mutation_driver.py:52`). Every `shard::` test, including the new Loom models, runs for every mutant.
  - "Heal passes until `absorbed == 8`" must be an explicit bounded loop, for example at most 2 passes and then an assert. After `join`, plain Rust code does not count toward Loom's branch limit, so a mutant such as `settled → false` or `== 0 → !=` would spin forever.
  - The committer's pop loop should pop exactly the number of queued advances rather than yield until the queue is non-empty.
  - Record the Loom models' wall time under `--profile quality` in the commit message.

- **C7 — Loom adequacy.** The constraint asks for a model of the *actual* transition. Here that transition is dropping the last receipt, which settles the batch with Release, while `settled()` loads with Acquire. The plan calls `settle()` by hand instead.
  - Make `ReceiptInner<W>` and `SubmitReceipt<W>` generic over the settlement word, as `Submissions<W>` already is, so the model drops an actual receipt.
  - The cost is a type parameter. It closes the one gap §3 concedes ("`ReceiptInner`'s Drop wiring … not by Loom").
  - Import `TailFields` as `crate::shard::TailFields`, not through `super::`, where it is only glob-imported (`commit_plan.rs:3`). That keeps the new file free of unresolved-glob rows.

- **C8 — Possible missed mutant on the retained guard.** Keep `if lane_ok && upto > prev_absorbed {` (`maintenance.rs:186`) byte-identical, so that it stays a diff context line.
  - If the hunk swallows that line, the `>` → `>=` mutant is selected. Under that mutant a zero-length duplicate at the boundary becomes Exact and pulls `trim_safe_to` up to `absorbed`, collapsing the one-pass trim lag. Only `dst::…reads_ring` observes that, and the `shard::` filter does not run it, so the mutant would be MISSED.
  - Cheap insurance: add to R2a a duplicate `advance(h, 4, 4, 0)` after group B and assert that `trim_safe_to` stays 0.

- **C9 — The §7.10 acceptance criteria are not reproducible from the tree.**
  - The capacity DST test installs no tracing subscriber. `tracing_subscriber` appears only in `src/main.rs` and `src/sse/test_log.rs`. The "local, uncommitted diagnostic patch" the WARN counts depend on is not in the tree.
  - Commit the test-only subscriber, or give the patch exactly, before D9 treats these counts as release-hold evidence.
  - Two legitimate sources of nonzero counts need a written disposition up front, not an unqualified "0 lines":
    - Split retirement rejects the parent's in-flight groups with `Moved`, which settles their receipts. A plan racing close can then roll back.
    - The eviction-driven prune (`worker.rs:136-142` together with `shard.rs:2391-2410`) can produce a Detached drop.

- **C10 — The invariant is overclaimed.** §2 says "every byte … leaves the ledger exactly once, whatever the absorber submits". The committer trusts `len`, so exactness holds only when `len` equals the stored bytes of `[from, upto)`.
  - Restate the invariant as conditional on that.
  - Cite `chunk_cost` (`gather.rs:136-143`) over the same rows as the source of truth. `append.rs:223` counts the same `frame.len()`.

- **C11 — Documented contracts left out.**
  - `docs/HISTORY-V2.md:151-152` says "retries stay idempotent via the submitted high-water mark", and `:224` says "re-absorption idempotent". These are the false premise the plan corrects in the code docs (`gather.rs:331-334`, `history.rs:748-751`).
  - Commit 2 should amend them: an overlapping re-absorption is dropped by the committer, and pages tile only when each gather starts at the previous gather's end. It should also fix the stale `Vec<(IncarnationHash, Offset)>` shape at `:149`.

- **C12 — Base drift.** `slate` and `origin/slate` are now at `46f668b3`, which touches only `src/auth.rs`, `src/auth/lease.rs` and `WIRE-MATRIX.md` §2.18. None of the plan's files are affected, and `shard.rs` (3139) and `history.rs` (1656) are unchanged. Rebase commit 0 onto it and recheck the "WIRE-MATRIX unchanged" claim against the new row.

- **C13 — Missing green control.** The existing R25-D pin `dst::…billing_maintenance::absorbed_boundary_and_maintenance_retire_atomically` (`billing_maintenance.rs:234-340`) exercises exactly the path commit 2 moves.
  - Today its heal is the rescan rollback (tick 20 ms, rescan every 2.4 s, 10 s budget). After commit 2 the rescan only seeds pending, and the settled plan performs the rollback.
  - §7.2 filters on `history::`, `shard::` and `commit_plan::`, so this test runs only in §7.9. Add it to §7.2 and §7.3 for commit 2, since it is the regression pin for "a refused batch still heals".

- **C14 — Minor: lock contention in `plan_reads`.** `resident_absorbed` (`shard.rs:2430-2434`) takes the engine-wide `streams` mutex once per stream on every settled plan, up to 1,024 per tick, alongside the committer and readers. This is acceptable, but note it. If C5(a) lands, the rollback is gated per stream and runs only for streams whose mark is ahead, which bounds the extra lookups.

**Verdict: ready-with-corrections.** The causal trace is verified. Commit 1 plus commit 2 closes the double-retirement mechanism: no interleaving I could build with the single-task absorber produces a double retirement, a ledger divergence or a co-grouped 500. The ratchet, ceiling and mutation analyses hold, apart from C2 (a clippy failure) and C8 (a possible missed mutant). C1 and C3 are mechanical blockers for the reds and ledgers. C5 is a substantive gap in the D5 recommendation that the owner must decide with the page-overlap consequence stated.
