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

- **Scope:** shard engine clauses (a)-(d): TLA-005, TLA-006, TLA-011. History
  partition clauses (e)-(j): TLA-016, TLA-018, TLA-019.
- **Statement, shard engine:** (a) `Db::write_with_options` returning `Ok`
  means the batch is applied (visible to later default reads) in seqnum order,
  not durable. (b) `DbStatus.durable_seq` is monotone and covers exactly the
  seqnum prefix whose WAL SST (or L0 SST plus manifest) is in the object
  store. (c) After a fatal WAL flush error, the flusher's `run_lifecycle`
  writes `closed_result` (so `close_reason` is set and reads and writes fail)
  before its cleanup drops the unflushed buffers. A PUT whose reply was lost
  may still have landed, so the next open may recover any written prefix. A
  batch applied before its `db.write` returned `Err` may also land, through a
  WAL flush already in flight or the engine's own `Db::close`. (d) Liveness
  only: while the WAL flusher lives, a written batch eventually lands, and
  while the Db is open its durability is reported.
- **Statement, history partitions and reads:** (e) A `WriteBatch` is applied
  to the memtable atomically and becomes durable atomically. (f) Remote
  durability is a prefix of applied order: `durable_seq` is monotone, and a
  durable batch implies every earlier batch is durable. (g)
  `DurabilityLevel::Remote` reads see exactly the durable prefix;
  `DurabilityLevel::Memory` reads see applied state, tombstones included. (h)
  On a WAL-disabled DB (the history partitions), `flush()` returning `Ok`
  means every write applied before the call is in an SST the manifest
  references and survives a crash; the memtable is lost on a crash. (i)
  Durable data never rolls back. (j) A scan reads one snapshot: the state and
  the sequence bound captured when it is created, so a delete applied or made
  durable later does not remove a row from a running scan.
- **Origin:** pinned SlateDB rev `0717cc1`: `db_status.rs`
  (`report_durable_seq`), `db.rs` `DbWalObserver` (WalFlushed ->
  `advance_durable_seq`), `wal_buffer.rs` (`WalFlushHandler::cleanup`:
  `mark_closed`, then `WalClosed`), `dispatcher.rs` `run_lifecycle`
  (`closed_result.write_result` before `cleanup`), `config.rs`
  `DurabilityLevel`, and `reader.rs` `prepare_max_seq` and
  `scan_with_options` for (j). The repository relies on it in the shard pump
  and `acker_loop` (`src/shard.rs`), the commit pipeline
  (`src/shard/transaction/`), `src/shard/record.rs` (`read_frames_until`,
  `visible_absorbed`) and `Absorber::commit` (`src/history/gather.rs`).
- **Enforcement / evidence:** SlateDB. The shard engine only reads
  `durable_seq` and `close_reason`. Repository held-WAL tests in
  `src/shard/retirement_tests.rs` and `src/dst/tests/durability_fences.rs`,
  and the DST durability and persistence suites
  (`src/dst/tests/durability_*.rs`, `src/dst/tests/persistence_faults.rs`).
- **Invalidation:** a SlateDB revision or settings change; disabling the shard
  WAL or enabling it on history partitions; a write path that waits for
  durability differently.
- **Standing:** (a)-(c) and (e)-(j) established by upstream source inspection
  and the repository integration tests, not verified here; (d) conditional,
  used only by liveness.

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

- **Scope:** TLA-001, and TLA-002 and TLA-003 through TLA-001 (the seal model
  applies every registry decision at its conditional PUT); TLA-005 (an
  ambiguous WAL PUT); TLA-011 (the fence); TLA-019 (`ForkPin`: every registry
  step is one conditional write).
- **Statement:** (a) A conditional create is atomic. A create on an existing
  path fails with `AlreadyExists`. An ambiguous reply (the PUT landed, the
  reply was lost) surfaces as an error (fatal, or a spurious `Fenced` on
  retry), never as success for a PUT that did not land. (b) A descriptor GET
  returns one committed version with its ETag, or fails. A conditional update
  (`PutMode::Update` with that ETag) commits atomically if and only if the
  stored ETag still matches; otherwise it answers `Precondition`. Any other PUT
  error may or may not have committed. A GET may return no ETag. An ETag
  repeats only for byte-identical content. So `Registry::mutate_incarnation` is
  one conditional update of a stream descriptor, bound to the incarnation it
  observed; a changed incarnation is reported (`IncarnationChanged`) and
  writes nothing.
- **Origin:** the `object_store` crate (`PutMode::Create`, `PutMode::Update`)
  and the provider's conditional-write support; `src/registry.rs`
  `ConditionalUpdateToken`, `mutate_incarnation` and `recreate`;
  `src/application/creation/anchor.rs` (`anchor::install`) and
  `src/application/creation/deletion.rs` (`delete_transition`,
  `release_fork_ref`).
- **Enforcement / evidence:** the object-store provider, for which no
  conformance evidence was reviewed. The registry code maps these outcomes as
  TLA-001 models them (lost reply, failed dispatch, missing ETag, failed read),
  and `registry::tests::r08_*` exercise that mapping on the in-memory store.
  The registry's conditional-write tests and the DST fork suites
  (`src/dst/tests/fork_cleanup.rs`) cover the repository's use of it. The
  durability models include both ambiguous outcomes: TLA-005 `Restart`
  recovers an unreported prefix after `WalFail`, and TLA-011 `Land` has the
  `AmbiguousPut` outcome.
- **Invalidation:** an object-store client or provider change; a registry CAS
  change; a retry layer that turns `AlreadyExists` or `Precondition` into
  success; a registry write that is not conditional on the observed
  incarnation.
- **Standing:** **unestablished** for the provider's conditional writes.
  TLA-001, TLA-002, TLA-003, TLA-005, TLA-011 and TLA-019 are conditional on
  it; the repository's use of them is established by code reading and tests.

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

### ASM-SLATEDB-GC

- **Scope:** TLA-019 (`ReachGC`), and TLA-018 (`RError`).
- **Statement:** (i) The compacted-SST collector deletes an SST only if
  neither the latest manifest nor any checkpoint's manifest references it and
  its id time is below `min(now − min_age, compaction low watermark, newest
  L0)`. Each pass reads the compactions store, then the manifest, then lists
  the objects. (ii) One writer's SST id times are monotone, and a newly
  uploaded L0 is newer than every L0 of the manifest it is committed to.
  (iii) A read that needs a deleted SST fails with an error unless the
  blocks it needs are in the block or object cache, whose bytes are that
  SST's immutable bytes. It never returns a short or empty success. (iv) The
  writer `Db`'s in-memory manifest view merges the stored manifest only on
  the `PollManifest` tick and in the conflict reload inside the writer's own
  manifest write. The embedded compactor does not refresh it. (v) Each
  collector task first drops the checkpoints whose expiry has passed, then
  treats every SST named by the latest manifest or by a remaining
  checkpoint's manifest as live.
- **Origin:** SlateDB rev `0717cc1`: `garbage_collector/compacted_gc.rs`
  (collector); `retrying_object_store.rs` (`NotFound` is not retried),
  `error.rs` (`NotFound` becomes `Error::data`) and `tablestore.rs` (a
  filter-cache miss falls through to a read that propagates the error) for
  (iii); `memtable_flusher/manifest_writer.rs` and `compactor.rs` for (iv);
  `garbage_collector.rs` (`remove_expired_checkpoints`),
  `garbage_collector/compacted_gc.rs` (`list_active_l0_and_compacted_ssts`)
  and `manifest/store.rs` (`read_referenced_manifests`) for (v).
  Repository settings: `history_settings` in `src/history.rs`, shard DBs in
  `src/config/validation.rs`.
- **Enforcement / evidence:** reading the pinned upstream code. A real-code
  diagnostic at `ab73296` observed (iii), (iv) and (v): the writer's view
  still named four compacted-away L0s, the collector deleted none of them
  while the compactor's checkpoint existed, and after that checkpoint was
  deleted it removed them and a read over the stale view failed with an
  object-store `NotFound`.
- **Invalidation:** a SlateDB pin change; a change to `min_age`, the GC
  interval or `manifest_poll_interval`; adding checkpoints, `DbReader`s or
  `refresh_manifest` calls; mapping a storage error to an empty result
  (control `nc_swallow_read_error`).
- **Standing:** established by code reading; not verified.

### ASM-SLATEDB-COMPACTION-CHECKPOINT

- **Scope:** TLA-019 (`ReachGC`, the reader clause `LiveReadViewProtected`).
- **Statement:** Before each compaction commit the embedded compactor writes
  a checkpoint on the pre-compaction manifest with a 900 s lifetime, so the
  SSTs a compaction replaces stay live for 900 s after it commits
  (ASM-SLATEDB-GC (v)). The history partition's writer view is refreshed at
  least every `manifest_poll_interval` (300 s), and a history read holds its
  view for less than the remaining 600 s. A read therefore never needs an
  SST the collector deleted. The model uses ticks of 300 s: a 3-tick
  checkpoint, a refresh within 1 tick of the view going stale and a read
  within 1 tick.
- **Origin:** SlateDB rev `0717cc1` `compactor_state_protocols.rs`
  (`write_manifest`, reached through `write_state_safely` →
  `write_manifest_safely`), whose comment calls the 900 s lifetime an
  interim choice. Repository: `manifest_poll_interval` 300 s for history
  partitions (`src/history.rs:501`) and billing (`src/billing.rs:1516`), and
  `manifest_poll_ms` for shard DBs (`src/config/validation.rs:51`).
- **Enforcement / evidence:** the real-code tests
  `dst::dst_tests::read_history_lifecycle::tla019_pin_history_scan_survives_compaction_gc_on_stale_view`
  and `tla019_pin_keyed_history_read_survives_compaction_gc_on_stale_view`
  (`src/dst/tests/read_history_lifecycle.rs`): after a compaction the
  writer's view still names the replaced L0s, a checkpoint names them with
  a lifetime greater than twice the history `manifest_poll_interval`, a
  collection with compacted `min_age` 0 deletes nothing and the stale-view
  read returns every record; with that checkpoint deleted the read fails
  with `NotFound`. In the model, `baseline-small` and `baseline-expanded`
  check the reader clause with the checkpoint, and
  `nc-no-compaction-checkpoint` shows the clause fails without it.
- **Invalidation:** a SlateDB change that shortens, removes or reconfigures
  the compactor checkpoint; a longer `manifest_poll_interval`; manifest
  polls that fail repeatedly (object-store errors), so the view stays stale
  past the checkpoint; a history read that holds its view longer than the
  checkpoint's remaining lifetime. `baseline-timing-lapse` covers those
  cases: a read can then fail, but never completes short.
- **Standing:** established by code reading and the real-code tests above;
  it rests on an upstream constant that the code calls interim.

### ASM-HISTORY-FENCED-VIEW

- **Scope:** TLA-018 (ownership move), TLA-019 (`OrphanUpload`).
- **Statement:** Opening a writer on a shard DB or history partition bumps
  its writer epoch. The old writer's later manifest and WAL writes fail, so
  an SST it uploads afterwards is never referenced. The old `Db` never merges
  the new writer's state into its own reads, so a page served by the fenced
  engine sees a frozen, self-consistent state, or fails.
- **Origin:** SlateDB rev `0717cc1` (`manifest/store.rs`
  `FenceableManifest`, `fence.rs`).
- **Enforcement / evidence:** upstream contract, exercised by the DST
  ownership and fencing suites (`src/dst/tests/durability_fences.rs`).
- **Invalidation:** a SlateDB pin change; production reads through a
  `DbReader` or a snapshot that refreshes.
- **Standing:** established as an upstream contract; not verified here.

### ASM-HISTORY-GC-CLOCK

- **Scope:** TLA-019 (`ReachGC`).
- **Statement:** SST id times, `min_age` and the collector's `now` share one
  discrete clock. The collector runs in the writer's process, so skew and
  reversal are not modelled. A writer may stall between an upload and its
  manifest commit for longer than `min_age`; the generation condition must
  cover that.
- **Origin:** upstream ULID SST ids; the repository embeds the collector in
  the writer process.
- **Enforcement / evidence:** none for skew across hosts.
- **Invalidation:** a detached or standalone collector on another host.
- **Standing:** unestablished for multi-host operation.

### ASM-HISTORY-ACTORS

- **Scope:** TLA-016 and TLA-019 liveness checks.
- **Statement:** The retrying in-process actors are the absorber task (a
  5 s tick that gathers pending streams every tick, with a dirty-index
  rescan every 120 ticks or every tick while paging), the committer, the WAL
  flusher, the acker and dispatch loop, and the collector's interval
  scheduler. Their attempts are fairly scheduled in the liveness checks. No
  background actor repays a fork-reference debt: a debt on a child
  tombstone is repaid only when a client issues `DELETE` for that child
  again (`delete_lifecycle` → `repair_tombstone`, its only caller). Only
  `LiveSpecClientRetries` assumes that client retry, and it says so.
- **Origin:** `src/history/worker.rs`, `ShardEngine::spawn_required`
  (`src/shard.rs`), `src/application/creation/deletion.rs`.
- **Enforcement / evidence:** required-task supervision for the in-process
  actors. The client retry is not enforced: after a `DELETE` that returned
  success the client has no signal to retry (TLA-019-F4).
- **Invalidation:** a change to task ownership or the rescan cadence; adding
  a sweeper for tombstone debts.
- **Standing:** established for the in-process actors; unestablished for
  client `DELETE` retries.

### ASM-HISTORY-WRITER

- **Scope:** TLA-018 (the coarse writer `WHistFlush`/`WAdvance`).
- **Statement:** Canonical and postings rows are durable and contiguous below
  every committed absorbed boundary, and an advance follows the flush that
  covers it.
- **Origin / enforcement:** TLA-016's `H3_AbsorbedBackedByDurableHistory`
  and `LastRecoverableCopy`, which pass for the recorded instances.
- **Invalidation:** a change to the gather, flush and submit order, or to
  `CommitTransaction::absorbed`.
- **Standing:** established for the modelled instances only.

### ASM-HISTORY-POSTINGS-CACHE

- **Scope:** TLA-018 keyed reads.
- **Statement:** `PostingsCache::runs_for` returns either the true runs of
  the key restricted to `[from, provable_to)`, with `provable_to` at most the
  requested `upto`, or `Corrupt`. A segment's warm window `[from, to)` is an
  absence proof: every chunk in it was installed contiguously in this
  process and recorded every key it carried, so a slice ending inside it may
  be bridged to its end. An install that records nothing for a key raises
  `from` past its chunk.
- **Origin:** `src/postings_cache.rs` (`runs_for`, `install_chunk`,
  `publish_load`).
- **Enforcement / evidence:** the cache's unit tests, including
  `postings_cache::tests::a_regather_install_after_an_eviction_proves_nothing_below_its_rows`
  and the three `*_bridge_never_crosses_*` tests added with the fix
  "A postings-cache bridge never crosses a chunk whose runs no slice
  recorded". TLA-016's `CacheNeverProvesFalseAbsence` checks the gather's
  write-through install; it failed before the TLA-016-F3 fix and passes
  after it. The admission line, capped and merging loads, and single-flight
  are not modelled (TLA-020, planned).
- **Invalidation:** a change to the cache's coverage or warm-window rules,
  or to the range the gather names for an install.
- **Standing:** established for the install path in the modelled instance;
  the rest of the cache is unverified. TLA-018's keyed results are
  conditional on it.

### ASM-HISTORY-RING

- **Scope:** TLA-018 (durable ring reads).
- **Statement:** `proves_durable_ring` implies that the ring returned dense,
  durable copies of `[from, last]` for this engine and incarnation.
- **Origin:** `src/shard/record.rs`, `src/shard/tail_ring.rs`.
- **Enforcement / evidence:** the `src/dst/tests/reads_ring.rs` tests;
  KANI-022 is planned.
- **Invalidation:** a change to the ring's retention or density proof.
- **Standing:** established by tests only.

### ASM-HISTORY-REABSORB

- **Scope:** TLA-016.
- **Statement:** A gather reads only Remote-durable rows or durable ring
  copies, and a durable frame at an offset never changes. Re-absorbing a
  range therefore rewrites identical canonical bytes, so the model tracks
  row presence only.
- **Origin:** `read_frames_range` (`src/shard/record.rs`);
  ASM-SLATEDB-DURABLE (e).
- **Invalidation:** a gather that reads applied rows; a rewrite of stored
  frames.
- **Standing:** established by code reading.

### ASM-HISTORY-EVICTION

- **Scope:** TLA-016 (`MarkPrune`).
- **Statement:** `evict_idle_handles` evicts only a handle that nothing but
  the map references. Committer batches, dispatch, ring publication, readers
  and waiters hold clones; a queued `AbsorbedBatch` holds none. A reloaded
  handle reads the Memory-level tail row for both `durable` and `applied`.
- **Origin:** `ShardEngine::evict_idle_handles` and
  `ShardEngine::stream_handle` (`src/shard.rs`).
- **Invalidation:** a change to handle ownership or to the reload path.
- **Standing:** established by code reading; that dispatch holds a clone
  until publication rests on the function's documentation.

### ASM-SEAL-REPLY-ORDER

- **Scope:** TLA-002, TLA-003.
- **Statement:** the shard committer releases a group's replies only after
  the group is durable, in queue order: acknowledgements (append and duplicate
  acknowledgements, fence and close acknowledgements) and every refusal that
  rests on committer state, `SealSuperseded` included. A no-write group joins
  the newest earlier barrier. Two refusals are sent at staging: a deferred
  content error (`BadBody`, `CtMismatch`), which rests on no committer state,
  and `Internal` for an unreadable fence row. A group whose write fails
  retires its engine and answers `Internal`; a group rejected without a
  retirement answers `Internal` and drops its streams' cached seal fences; a
  group stranded by an engine close answers `Moved` and may still become
  durable.
- **Origin:** `src/shard/commit_plan.rs` `DurableEffects` (36-79);
  `src/shard/transaction/append.rs` 96-101 and 139-141;
  `src/shard/transaction/maintenance.rs` 109-146 and 191-223;
  `src/shard/transaction/mod.rs` `reject` (191-207);
  `src/shard/transaction/finalize.rs` (`join_prior_barrier`, `write_failed`);
  `src/shard.rs` `begin_close` (1876-1935).
- **Enforcement / evidence:** source inspection; DST
  `a_fence_waits_for_durability_before_reporting_closed`,
  `a_superseded_final_waits_for_its_fence_to_be_durable`, and
  `shard::durability_frontier_tests::a_superseded_close_waits_for_its_fence_to_be_durable`
  and `a_failed_fence_group_refuses_nothing`.
- **Invalidation:** a change to where a committer reply is sent, to the commit
  pipeline, or to SlateDB's durability reporting.
- **Standing:** established (source and tests). Most configurations stage an
  element and make it durable in one step, which is exact now that no
  definitive refusal precedes its group's durability. `MaxHeldFence = 1`
  separates the two steps for one fence group (durable, lost with its engine,
  or rejected without a retirement); those configurations pass too.

### ASM-SEAL-ENGINE-HANDOFF

- **Scope:** TLA-002, TLA-003.
- **Statement:** once the shard's engine is retired or its owner process
  crashes, nothing that engine queued commits later, except groups it had
  already staged; those may still become durable while their callers are told
  `Moved`. The next `resolve` in the (new) owner opens a new engine over the
  same durable state.
- **Origin:** `src/shard_directory.rs` `resolve` (238-297) and `retire`
  (434-456); `src/shard.rs` `begin_close` (1876-1935) and the committer's
  shutdown path (2467-2500); SlateDB writer fencing on open.
- **Enforcement / evidence:** source inspection of the engine. SlateDB writer
  fencing is TLA-011's subject; this group does not check it.
- **Invalidation:** a SlateDB revision; an open or fencing change; an engine
  close that drains its queue into the database.
- **Standing:** established (source) for the engine; conditional on SlateDB
  writer fencing.

### ASM-SEAL-FENCE-ROW

- **Scope:** TLA-002, TLA-003.
- **Statement:** a segment's durable seal-fence row (`seal_fence_key`,
  `<hash16> 'G'`) is written only by `CommitTransaction::fence`, always as
  `max(cached, requested)`, and read only by `CommitTransaction::seal_fence`.
  No deletion, prefix scan, fork, split, merge or history path removes or
  lowers it.
- **Origin:** commit "A seal takeover's fence outlives the engine that recorded
  it"; `src/shard.rs` 189-195; `src/shard/transaction/maintenance.rs` 89-108
  and 136-167.
- **Enforcement / evidence:** source inspection: one writer and one reader, and
  every shard prefix scan starts with a sentinel or a longer prefix. The golden
  test `golden_layout4_seal_fence_key_bytes` pins the key.
- **Invalidation:** a new writer, deleter or scanner of per-segment rows; a key
  layout change.
- **Standing:** established (source).

### ASM-SEAL-QUEUE

- **Scope:** TLA-002, TLA-003.
- **Statement:** fences, closes and appends for a segment travel one FIFO
  committer queue (`try_seal_fence`, `try_close` and `try_enqueue` all go
  through `try_command`).
- **Origin:** `src/shard.rs` 1804-1856.
- **Enforcement / evidence:** source inspection.
- **Invalidation:** separate queues or priorities.
- **Standing:** established (source).

### ASM-SEAL-OWNER

- **Scope:** TLA-002, TLA-003.
- **Statement:** ownership is decided only at submission. Request handlers do
  all registry work (claim, renewal, the product `seal_auth` check) in whatever
  process handles the request. The first ownership check is
  `ShardDirectory::resolve` in `submit`: `NotOwner` for a non-owner, a fresh
  engine for the owner. The takeover's fence needs local ownership. A relayed
  segment close carries its generation and is modelled as a close queued at the
  owner.
- **Origin:** `src/application/append/submit.rs` 18-25;
  `src/shard_directory.rs` 238-297; `src/application/lifecycle.rs` 874-879;
  `src/application/topology.rs` 72-80.
- **Enforcement / evidence:** source inspection.
- **Invalidation:** an ownership check before the claim; server-side forwarding
  of appends.
- **Standing:** established (source).

### ASM-SEAL-VALIDITY

- **Scope:** TLA-003 (and TLA-002, where every request is valid).
- **Statement:** whether a final passes validation depends on its bytes and on
  the configuration of the process that handles it: the record ceiling
  (`MAX_RECORD_PAYLOAD_BYTES`) and the per-stream ingest limits
  (`LIMIT_BYTES_PER_SEC`, `LIMIT_RECS_PER_SEC`, `LIMIT_BURST_SECS`), fixed at
  boot. An exact retry (same operation id) can pass on one instance and fail on
  another, for example during a rolling configuration change. A raw close with
  content always has a producer (its own or the synthetic `rawseal` lane), so
  every content refusal except ingest capacity is deferred to the committer.
- **Origin:** `src/application/append/content.rs` 14-127 (`parse_content`,
  `stored_records`); `src/application/append/close.rs` 101-112;
  `src/usage.rs` 327-340; `src/product.rs` 1661-1767.
- **Enforcement / evidence:** source inspection, confirmed by two independent
  refutation attempts of TLA-003-F4.
- **Invalidation:** one validation configuration for the whole fleet;
  validation that no longer depends on process settings.
- **Standing:** established (source).

### ASM-SEAL-CLOCK

- **Scope:** TLA-002, TLA-003.
- **Statement:** a lease can lapse at any time. No bound is assumed on queue
  residence or on the time a handler spends between its claim check and
  `try_enqueue`.
- **Origin:** roadmap L10; `src/registry.rs` `SEAL_CLAIM_MS` (298).
- **Enforcement / evidence:** by construction: `Lapse` is an unconstrained
  environment action.
- **Invalidation:** adopting a time-based argument, which needs an owner
  decision.
- **Standing:** established (conservative over-approximation).

### ASM-SEAL-RECOVERY-ACTOR

- **Scope:** the TLA-002 liveness checks only.
- **Statement:** a plain `:seal` client retries forever. Every other client and
  every fault is bounded, so faults cease. The liveness shapes run on one
  instance.
- **Origin:** roadmap TLA-002 ("a specified retrying/reconciling actor").
- **Enforcement / evidence:** configuration.
- **Invalidation:** a change to who reconciles an abandoned claim.
- **Standing:** a configuration of the claim, not a product guarantee.

### ASM-SEAL-EPOCH

- **Scope:** TLA-001, TLA-002, TLA-003.
- **Statement:** `stream_epoch` is drawn fresh from 16 random bytes on every
  create and recreate and is never reused.
- **Origin:** `src/application/creation.rs` `fresh_desc` (207-220).
- **Enforcement / evidence:** the runtime's entropy source.
- **Invalidation:** deterministic or reused epochs.
- **Standing:** established (probabilistic).

### ASM-SEAL-PATH

- **Scope:** TLA-001.
- **Statement:** `desc_path` is injective over (project, name), so two projects
  that share a name never share a descriptor object.
- **Origin:** `src/registry.rs` `desc_path` (882-898).
- **Enforcement / evidence:** source inspection (hex of both components);
  `registry::tests::same_name_two_projects_share_no_identity`.
- **Invalidation:** a path layout change.
- **Standing:** established (source), not machine-checked (candidate for
  KANI-033).

### ASM-SEAL-NODELETE

- **Scope:** TLA-001, TLA-002, TLA-003.
- **Statement:** descriptor objects are never physically deleted; deletion
  writes a tombstone. `MutationResult::Missing` is therefore unreachable after
  creation and is not modelled.
- **Origin:** `src/registry.rs`; grep finds no `store.delete` on descriptor
  paths.
- **Enforcement / evidence:** source inspection.
- **Invalidation:** a hard-delete path for descriptors.
- **Standing:** established (source).

### ASM-SEAL-OPID

- **Scope:** TLA-002, TLA-003.
- **Statement:** operation ids and synthetic lanes are functions of (surface,
  content, coordination): the model treats the hashes as injective on their
  inputs, and an exact retry gets the same id.
- **Origin:** `src/application/lifecycle/claims.rs` `seal_op_id_full` and
  `seal_op_id_semantic` (132-176); `src/application/append/close.rs` 29-54 and
  101-112.
- **Enforcement / evidence:** KANI-043 (planned) for the preimages; hash
  collision freedom is a cryptographic assumption (roadmap §1.5).
- **Invalidation:** a change to an operation id's preimage.
- **Standing:** **unestablished** (KANI-043 pending).

### ASM-SEAL-DECIDE-FN

- **Scope:** TLA-001 (and through it TLA-002, TLA-003).
- **Statement:** every `decide` closure passed to `mutate_incarnation` is
  `impl Fn(&StreamDesc)` and captures no interior mutability, so a value
  returned with `Applied` is the winning attempt's own decision.
- **Origin:** `src/registry.rs` 1157-1165; the production call sites.
- **Enforcement / evidence:** the type system plus source inspection (no call
  site captures a `Cell`, `RefCell`, atomic or `Mutex`);
  `registry::tests::typed_mutation_never_leaks_a_lost_attempts_decision`. Not
  TLC evidence.
- **Invalidation:** a decide closure with interior mutability, or a caller that
  reads captured state.
- **Standing:** established (source inspection).
