# Assumption ledger

Every check in `manifest.json` relies on the assumptions it names here
(roadmap §2.3, §7.7). Each entry records its scope, its origin, what enforces
it or is evidence for it, what would invalidate it, and its standing:
**established** (a production boundary or a cited contract enforces it) or
**unestablished** (the dependent result is a conditional design check).
`scripts/quality/formal.py check` rejects a manifest that names an ID not
defined here.

## Kani execution scope

### ASM-KANI-SCOPE

- **Scope:** every Kani obligation.
- **Statement:** a Kani harness checks one sequential execution of the named
  production functions under the Kani 0.68.0 / CBMC 6.11.0 semantics. It does
  not cover concurrency, panic unwinding or foreign functions. Kani's automatic
  checks (arithmetic overflow, bounds, unwinding) stay enabled. Loop bounds are
  set by `#[kani::unwind]`, and the unwinding assertions confirm they are
  sufficient.
- **Origin:** roadmap §1.3, §2.5; Kani *Rust Feature Support*.
- **Enforcement / evidence:** the driver treats an unwinding failure,
  unsupported construct or unsatisfied cover as incomplete, never as a pass.
- **Invalidation:** a Kani or CBMC upgrade; a harness that starts reaching
  async, atomic, FFI or panic-recovery code.
- **Standing:** established (tool contract).

### ASM-KANI-COMPILER

- **Scope:** every Kani obligation.
- **Statement:** Kani compiles the unchanged crate with its own dated nightly
  (`nightly-2026-08-21`, rustc 1.100.0-nightly), not the production 1.98.1
  compiler. The verified functions are plain integer, array and enum code whose
  semantics do not differ between these compilers. The production build and its
  tests remain separate, required evidence.
- **Origin:** roadmap §2.9, §7.2.
- **Enforcement / evidence:** `quality-tools.toml` `[formal]` pins both. The
  ordinary gates still compile and test the same functions with 1.98.1.
- **Invalidation:** a Kani release pinning another nightly; a harnessed
  function adopting a nightly-sensitive feature.
- **Standing:** established for the current harnesses.

## Numeric domains

### ASM-OFFSET-DOMAIN

- **Scope:** KANI-001, KANI-002, KANI-003.
- **Statement:** an offset token is `(epoch: u32, rawSeq: u64)`, where the epoch
  is the segment ordinal and `rawSeq` is the scan index `next`. Every value of
  both is admitted. The unused `in_block` word and the two pad bits are written
  as zero. The parser ignores them. Canonical-token (alias) rejection is
  KANI-004, which is still planned.
- **Origin:** `src/offsets.rs`, `docs/PER-KEY-ORDERING.md` §3, `src/segmap.rs`
  (ordinals allocated up to `u32::MAX` with `checked_add` on split).
- **Enforcement / evidence:** the `Offset` field is private and
  `Offset::before` is total. The harnesses quantify over the full domain.
- **Invalidation:** a wider segment ordinal, a nonzero `in_block`, or a change
  to the token layout.
- **Standing:** established.

### ASM-READ-NOW-SENTINEL

- **Scope:** KANI-002 (a tracked domain question, not a verified property).
- **Statement:** the read planner uses scan index `u64::MAX` as its in-band "now"
  sentinel (`ReadCommand::position_in`, `read_request.rs`, `read_remote.rs`). The
  KANI-002 harness proves the codec, not this caller convention.
- **Origin:** source inspection during KANI-002.
- **Enforcement / evidence:** none beyond the unreachability of a real
  `2^64 - 1`-record stream.
- **Invalidation:** a separate `Now` carried through the planner, which removes
  this entry.
- **Standing:** unestablished; owner decision pending. See
  `regressions/KANI-002/README.md`.

### ASM-PRODUCER-ROW

- **Scope:** KANI-036, KANI-037, KANI-038.
- **Statement:** the remembered producer state is the latest committed row
  `(epoch, seq, last offset, request hash)` for one (stream, routing key,
  producer id) lane. `u64::MAX` as the offset and `[0; 16]` as the hash are the
  legacy "unknown" sentinels. No older per-sequence result is retained.
- **Origin:** `src/shard.rs` `ProducerRows`, `src/shard/transaction/append.rs`
  `accept_append`, roadmap §1.5.
- **Enforcement / evidence:** the harnesses quantify over every row value,
  including both sentinels. What the committer loads into `current` is the
  subject of TLA-007 (planned).
- **Invalidation:** a producer-row codec change or retained per-sequence
  results.
- **Standing:** established for the decision function; the loading path is not
  covered here.

## Dependency contracts used by the TLA+ models

The TLA+ groups add their entries below this line. Each model README maps its
actions to these contracts.

### ASM-SLATEDB-DURABLE

- **Scope:** TLA-005, TLA-006, TLA-011.
- **Statement:** (a) `Db::write_with_options` returning `Ok` means the batch
  is applied (visible to later default reads) in seqnum order, not durable.
  (b) `DbStatus.durable_seq` is monotone and covers exactly the seqnum prefix
  whose WAL SST (or L0 SST plus manifest) is in the object store. (c) After a
  fatal WAL flush error, the flusher's `run_lifecycle` writes `closed_result`
  (so `close_reason` is set and reads and writes fail) before its cleanup drops
  the unflushed buffers. A PUT whose reply was lost may still have landed, so
  the next open may recover any written prefix. A batch applied before its
  `db.write` returned `Err` may also land, through a WAL flush already in
  flight or the engine's own `Db::close`. (d) Liveness only: while the WAL
  flusher lives, a written batch eventually lands, and while the Db is open its
  durability is reported.
- **Origin:** pinned SlateDB rev `0717cc1`: `db_status.rs`
  (`report_durable_seq`), `db.rs` `DbWalObserver` (WalFlushed ->
  `advance_durable_seq`), `wal_buffer.rs` (`WalFlushHandler::cleanup`:
  `mark_closed`, then `WalClosed`), `dispatcher.rs` `run_lifecycle`
  (`closed_result.write_result` before `cleanup`).
- **Enforcement / evidence:** SlateDB. The shard engine only reads
  `durable_seq` and `close_reason` (`src/shard.rs` pump and `acker_loop`).
  Repository held-WAL tests in `src/shard/retirement_tests.rs` and
  `src/dst/tests/durability_fences.rs`.
- **Invalidation:** a SlateDB revision or settings change; disabling the shard
  WAL; a write path that waits for durability differently.
- **Standing:** (a)-(c) established by upstream source inspection and the
  repository integration tests; (d) conditional, used only by liveness.

### ASM-SLATEDB-FENCE

- **Scope:** TLA-011.
- **Statement:** `Db::builder().build()` bumps the manifest `writer_epoch`
  (CAS), writes an empty fence WAL at the first absent WAL id with
  put-if-absent, refreshes the manifest (failing if a newer epoch exists) and
  replays the WAL up to the fence. Every later WAL SST write is put-if-absent
  on the writer's next id, so an older writer whose next id is taken fails with
  `Fenced` and closes. A conditional PUT that landed but whose reply was lost
  is retried and reported `Fenced` (`AlreadyExists`), with the batch durable.
- **Origin:** pinned SlateDB `fence.rs` (`WriterFencer::fence`),
  `wal/writer_init.rs` (`fence_and_init`), `manifest/store.rs`
  (`FenceableManifest::init_writer`), `tablestore.rs`
  (`write_sst_in_object_store`, `PutMode::Create`, `AlreadyExists` ->
  `Fenced`).
- **Enforcement / evidence:** SlateDB plus the object store's conditional put.
  Repository tests `history::tests::absorber_exits_when_shard_engine_is_fenced`
  and `src/dst/tests/producer_handoff.rs`.
- **Invalidation:** a SlateDB upgrade; an object store without put-if-absent;
  the WAL disabled.
- **Standing:** established by upstream source inspection and repository
  tests, **conditional on ASM-OBJSTORE-CAS**, which is unestablished. Every
  TLA-011 result is therefore a conditional design check.

### ASM-OBJSTORE-CAS

- **Scope:** TLA-005 (an ambiguous WAL PUT), TLA-011 (the fence).
- **Statement:** a conditional create is atomic. A create on an existing path
  fails with `AlreadyExists`. An ambiguous reply (the PUT landed, the reply was
  lost) surfaces as an error (fatal, or a spurious `Fenced` on retry), never as
  success for a PUT that did not land.
- **Origin:** `object_store` 0.14 `PutMode::Create` and the provider's
  conditional-write support.
- **Enforcement / evidence:** the object-store provider. The durability group
  holds no provider evidence. The models include both ambiguous outcomes:
  TLA-005 `Restart` recovers an unreported prefix after `WalFail`, and TLA-011
  `Land` has the `AmbiguousPut` outcome.
- **Invalidation:** a provider or client change; a retry layer that turns
  `AlreadyExists` into success.
- **Standing:** **unestablished**.

### ASM-DURABILITY-1

- **Scope:** TLA-005, TLA-006, TLA-011.
- **Statement:** the four `CommitHandoff` methods are linearizable under the
  `in_flight` mutex.
- **Origin:** `src/shard/commit_handoff.rs`.
- **Enforcement / evidence:** `src/shard/commit_handoff/loom_tests.rs` (three
  Loom schedules, preemption bound 2).
- **Invalidation:** a lock-order or handoff change.
- **Standing:** established within Loom's bounds.

### ASM-DURABILITY-2

- **Scope:** TLA-005, TLA-006.
- **Statement:** one committer task per engine processes groups strictly in
  sequence, so staging reads one applied state and registered seqnums increase.
- **Origin:** `src/shard.rs` `committer_loop`, which awaits `commit_group`.
- **Enforcement / evidence:** code structure. `PendingSortedInv` is a
  structural check of the modelled consequence, not evidence for it.
- **Invalidation:** a concurrent committer.
- **Standing:** established by inspection.

### ASM-DURABILITY-3

- **Scope:** TLA-005, TLA-006.
- **Statement:** once `dispatch_durable` has claimed groups, their effects run
  to completion synchronously. The only later await is the `cfg(test)`
  completion checkpoint.
- **Origin:** `src/shard.rs` `dispatch_durable`.
- **Enforcement / evidence:** code structure. The models still split the claim
  and the replies, which is more permissive.
- **Invalidation:** an await inserted after the claim.
- **Standing:** established by inspection.

### ASM-DURABILITY-4

- **Scope:** TLA-005.
- **Statement:** a stream handle referenced by a registered group cannot be
  evicted. Every `InFlightGroup` holds an `Arc<StreamHandle>` in
  `effects.tails`, and `evict_idle_handles` evicts only at `strong_count == 1`.
  One applied mirror per stream is therefore faithful while groups are pending.
- **Origin:** `src/shard.rs` `evict_idle_handles`;
  `src/shard/transaction/finalize.rs` `stage_stream_rows`.
- **Enforcement / evidence:** code structure.
- **Invalidation:** an eviction policy change.
- **Standing:** established by inspection.

### ASM-DURABILITY-5

- **Scope:** TLA-011.
- **Statement:** appends are never relayed between peers. A foreign owner
  yields `NotOwner` and the client re-routes; `src/peer.rs` relays reads only.
- **Origin:** `src/application/append/contract.rs` `from_resolve`;
  `src/shard_directory.rs` `resolve`; `src/peer.rs`.
- **Enforcement / evidence:** code structure.
- **Invalidation:** an append relay.
- **Standing:** established by inspection.

### ASM-DURABILITY-6

- **Scope:** TLA-011.
- **Statement:** a restarted process has an empty ownership view until its
  first fleet tick. It is then in bootstrapping mode and serves every prefix.
- **Origin:** `src/ownership.rs` `OwnershipService::new`; `src/fleet.rs`
  `set_view`.
- **Enforcement / evidence:** code structure. Modelled as `view[n] = "none"`
  after `Crash(n)`.
- **Invalidation:** readiness gating on the first ring view.
- **Standing:** established by inspection.

### ASM-DURABILITY-7

- **Scope:** TLA-005, TLA-006.
- **Statement:** a `write_with_options` that returns `Err` either failed
  before applying the batch (closed Db, exited batch writer, `EmptyBatch`, or
  a clock or WAL-buffer error before the memtable write), leaving the Db
  unchanged, or
  failed after appending it to the WAL buffer and memtable and advancing
  `last_committed_seq` (`maybe_freeze_current_memtable()?` returning
  `InvalidDBState` or a WAL status error, the `write-batch-post-commit`
  failpoint, or a panic that drops the reply). In the second case the batch
  writer exits, the Db closes and the batch may still land. SlateDB's
  `Ok(Err)` validation answers (sequence numbers, transaction conflicts, the
  segment antichain) are unreachable from the committer. No check assumes that
  `Err` means "not applied".
- **Origin:** pinned SlateDB `batch_write.rs` (`WriteBatchEventHandler::handle`,
  `DbInner::write_batch`), `db.rs` `write_with_options`.
- **Enforcement / evidence:** SlateDB.
- **Invalidation:** a SlateDB write-path change.
- **Standing:** established by upstream source inspection.

### ASM-DURABILITY-8

- **Scope:** TLA-005, TLA-006.
- **Statement:** no verdict is staged from a batch whose `db.write` returned
  `Err`. After a post-apply error, pinned SlateDB keeps default
  (Memory-level) reads of that batch open until `run_lifecycle` writes
  `closed_result`. `CommitTransaction::write_failed` calls `begin_close`
  before it answers the failed group, so `CommitTransaction::run` answers every
  later group `Moved` and `attach` returns `Retired`. The models contain both
  the window and this guard. They assume, without checking, that the guard
  covers every staging read: the committer is the only task that stages
  verdicts from Db reads, and every staging read (the producer-row and
  sequence-row loads, `stream_handle` reloads, `seal_fence`, the billing-row
  read) runs in a later group, after `run`'s `is_closed` check. Only the
  producer-row load is modelled.
- **Origin:** `src/shard/transaction/finalize.rs` (`write`, `write_failed`),
  `src/shard/transaction/mod.rs` (`run`), `src/shard.rs` (`committer_loop`,
  `begin_close`); pinned SlateDB `batch_write.rs` and `dispatcher.rs`. The
  earlier form of this entry assumed the window did not exist; TLA-005-F5
  refuted it for the post-apply error paths, and commit "A failed commit write
  retires its engine before any later group can stage from it" added the guard.
- **Enforcement / evidence:** the regression
  `shard::retirement_tests::tla005_f5_a_failed_write_answers_nothing_from_its_batch`
  holds the window open with SlateDB's `write-batch-post-commit` failpoint and
  checks both the duplicate and the reused-sequence form. The controls
  `TLA-005/nc-write-error-keeps-staging` (D2),
  `TLA-005/nc-write-error-keeps-staging-refusal` (D4) and
  `TLA-006/nc-write-error-keeps-staging` (`SuccessIsDurable`) show that the
  guard is necessary.
- **Invalidation:** a write-error path that answers the group, or lets the
  committer take another group, before it retires the engine; a second task
  that stages verdicts; a change to SlateDB's post-apply error order or to the
  default read options.
- **Standing:** established by inspection and the regression test, for the
  committer. Readers outside the committer are not covered: an evicted handle
  reloaded by a reader on the retired engine still seeds its mirrors from a
  Memory-level read (ASM-DURABILITY-9; the retired-engine read case belongs to
  TLA-018).

### ASM-DURABILITY-9

- **Scope:** TLA-005 (the scope of `PubVisibleDurable`).
- **Statement:** when `stream_handle` reloads an evicted handle, it seeds
  `StreamState.durable` from a Memory-level `db.get` of the tail row. On a live
  engine no evictable handle has applied-but-not-durable rows, so that seed
  equals the durable tail.
- **Origin:** `src/shard.rs` `stream_handle`, `evict_idle_handles`.
- **Enforcement / evidence:** eviction requires `strong_count == 1`. Every
  registered group and the transaction being staged pin their handles. A
  `db.write` error retires the engine before the failed group releases its
  handles (`write_failed`), so the remaining exposure is a reload on a retired
  engine.
- **Invalidation:** eviction or reload policy changes.
- **Standing:** established by inspection for a live engine; not modelled (the
  model keeps the handle resident). The retired-engine reload is recorded as
  out of scope here and belongs to TLA-018.

### ASM-DURABILITY-10

- **Scope:** TLA-006 liveness only.
- **Statement:** the acker observes `close_reason` and calls `begin_close`.
  The committer, the completer and the retirer keep being scheduled. WAL
  failures, group failures, retirements and aborts are finitely many in any
  liveness argument. Retries of unknown outcomes are client (public) retries,
  not background reconciliation.
- **Origin:** `src/shard.rs` acker, pump and committer loops;
  `src/shard/lifecycle.rs` `RequiredExit`.
- **Enforcement / evidence:** the Tokio runtime and the engine task
  supervisor.
- **Invalidation:** a change to the acker, pump or committer ownership.
- **Standing:** conditional (liveness only).
