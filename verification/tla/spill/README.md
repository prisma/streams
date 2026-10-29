# Seal spill models (group `spill`): TLA-016, TLA-018 (layout 6A)

Layout 6A replaced the absorber, history-v2 and the trims with the sealer
(`src/shard/seal.rs`). These two models replace `history/HistoryAbsorb.tla`
(TLA-016) and `history/ReadCompose.tla` (TLA-018) under the same ids. Each
result is a bounded model check of the recorded instance under the recorded
assumptions; the models do not prove that the Rust code refines them, and
they do not verify SlateDB.

## TLA-016: `SealSpill.tla`

| Model | Code |
|---|---|
| `wal`, `dur`: groups applied in order, durable as a prefix | one `WriteBatch` per group (`transaction/finalize.rs` `write`); ASM-SLATEDB-DURABLE (a)(b) |
| `AppendRec`: tail put + one merge operand per stream, published to `pend` | `finalize.rs` `stage_pending_rows`, `overlay.rs` `publish_into`, `spill/pending.rs` `PendingMerge` |
| `Dispatch`, `acked` | durable dispatch publishes tails (`handle_load.rs` `publish_durable`); a record is acknowledged then |
| `Snapshot` (skip on a gap) | `seal/steps.rs` `seal_snapshot`, `durable_pages` |
| `Put`, `PutFails` | `seal_windows` step 4 (`PutMode::Create`) |
| `Publish` | `publish_directory` (step 5, before step 6) |
| `Commit(A)`: live check, all or nothing; remainder put (`del` when empty); 'K' rows; drop or refusal withdraws | `transaction/seal.rs` `stage_seal`, `moved_since_snapshot`; `finalize.rs` `stage_remainder`; `withdraw_directory` |
| `RemainderBase` (mutation point F1) | `stage_remainder`: `st.pending.from(sealed.upto)` |
| `Seal7a`, `Seal7b`, `AbortApplied` | `seal_windows` 7a (`wal_flush` + `wait_sealed_durable`), `seal_steps` 7b (`forced_flush(false)`) |
| `DeadlineWal`, `DeadlineMem` (flush lock across both) | `deadline_flush` -> `forced_flush(true)` |
| `Takes7a` (mutation point F2) | `SealHooks::skip_7a` |
| `L0Pages` | SlateDB `flush.rs` `iter_imm_table` retention (boundary = `last_remote_persisted_seq`) |
| `CrashRestart` | `seal/open.rs` `seal_open` (directory from 'K' rows), `handle_load.rs` `load_tail` (`PendingPages::tiled`) |

Invariants: `I1_AckedReadableAfterRestart` (every acknowledged record is in
the durable store exactly once, in its place: one durable chunk whose object
holds it, or its position in the durable pending row), `I2_PendingRowTiles`,
`I3_DirectoryTiles`, `MemoryMirrorsStore`, `MemoryDirectoryCovers`,
`F2_SealFlushLeavesNoSealedPage`.

The committer's live-tail refusal is a defensive check: it is unreachable
in this scope. Absorbed moves only through serial seals, next only grows,
and the snapshot skips a stream whose applied boundary passed its durable
one (a seal that applied and then failed 7a). The model keeps the refusal
branch and its directory withdrawal, but no check claims it is reached
(`Witness_SealRefused` is a definition only).

TLA-016-F5, accepted as a cost (owner, 2026-10-28):
Witness_DeadlineCarriesSealedPages is reachable. A deadline flush whose 7a
runs before a seal's commit applies, and whose memtable flush runs after,
writes that seal's sealed operands to an L0. The remainder put sits above
the retention boundary, so the older operands are kept. They stay there
until compaction; every read sees the put, so nothing is lost. `seal.rs`'s
module comment states this. The witness stays as the record of the
accepted behaviour.

## TLA-018: `SealRead.tla`

| Model | Code |
|---|---|
| `ReadStart` (R1: P, end, to and the capture of the unsealed pages under one lock) | `application/read.rs` `execute_segment`; `seal/read.rs` `read_stream_window`, `unsealed_pages` |
| `ReadSealed`: ring if it holds the part, else `DirWalk` | `tail_ring.rs` `ring_read`; `read_sealed`, `plan_sealed`, `take_pages` |
| key summary skip (consumed through the chunk) | `plan_sealed` `Step::Skip`; `spill/directory.rs` `KeySummary::may_hold` |
| honest partial at a hole | `plan_sealed` (`entry.first > due` stops the plan) |
| `ReadUnsealed` over the captured pages, `PendWalk` errors | `read_unsealed` (`RecordCorruption::Misplaced` / `Missing`) |
| `ReadFinish` (cursor = last + 1) | `ReadPage::scanned_through` |
| `PublishEarly`, `HoleConsumed`, `Summary`, `UnsealedSource` | mutation points (MC_SealRead.tla) |

Invariants: `VisibleOnly`, `NoSkip`, `NoShortRead`, `ExactDeliveredPrefix`,
`NoReadError`.

TLA-018-F4 (fixed in "a read captures its unsealed pages with its boundary
under one lock (TLA-018-F4)"; regression
`shard::seal::read_tests::a_read_that_captured_its_boundary_before_a_seal_commits_serves_exact_data`):

- **Before the fix:** `read_stream_window` read P under the state lock and
  took the lock again in `read_unsealed`. A seal committing in between
  dropped the pages `[P, new boundary)` from memory, and the read failed as
  `RecordCorruption::Missing` or `Misplaced` on a healthy store. Nothing
  wrong was served.
- **The fix:** P, the end and the pages from `max(from, P)` below `to`
  (bounded by the budget plus one page) are captured under one lock, and
  the unsealed part reads only the capture.
- **Checks:** every baseline now includes `NoReadError`.
  `Witness_UnsealedReadRacesSeal` shows that the race (a seal commits
  between R1 and the unsealed part) is still reachable and the read
  succeeds. The pre-fix read is the negative control `nc-second-lock`
  (`UnsealedSource <- MutSecondLock`), which breaks `NoReadError`.

`nc-hole-consumed` substitutes two operators, unlike every other control,
and does so on purpose. While step 5 precedes step 6 there is never a hole
below the boundary, so `HoleConsumed` alone changes no behaviour and could
not fail. The late directory alone is masked for `NoSkip` by the honest
partial: it serves and claims nothing, and breaks `NoShortRead` instead
(`nc-late-directory`). Only together do they show that the honest-partial
rule is what keeps a hole from becoming a skip.

## Out of scope (both models)

Object GC and refcount rows (objects are immutable once PUT); handle
eviction and reload; native queues; more than one shard; the byte ledger;
SlateDB compaction; active snapshots or transactions that lower the flush
retention boundary; liveness (backoff, triggers); crash in TLA-018 (reads of
a closed engine are refused by `refuse_closed_read`); multi-record pages,
anchors, ranged GETs and chunk caps (a per-read page budget drawn at random
stands for every early stop).
