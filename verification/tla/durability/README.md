# Durability group: TLA-005, TLA-006, TLA-011

This directory holds the TLA+ models and TLC configurations of three roadmap
obligations (`docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md` §4). Their
checks, bounds and statuses are in `verification/manifest.json`. Their
assumptions are in `verification/assumptions.md`. The receipts of the last
complete, matching runs are in `verification/receipts/`.

The models were first written against code with the defect TLA-005-F5. Commit
f574d73, "A failed commit write retires its engine before any later group can
stage from it", fixed it. The models now describe the fixed code. The pre-fix
committer is a negative control.

| ID | Model | Root module | Status |
|---|---|---|---|
| TLA-005 | Commit groups, dependency barriers, and observable replies | `CommitGroups.tla` | pass-with-recorded-scope. The TLA-005-F5 schedules are in the small and refusal-retry baselines and pass. `nc-write-error-keeps-staging` reproduces them on the pre-fix committer |
| TLA-006 | Commit handoff, retirement, and already-durable completion | `HandoffRetirement.tla` | pass-with-recorded-scope |
| TLA-011 | Serving possession, owner fencing, and shard movement | `ServingOwnership.tla` | pass-with-recorded-scope, as a **conditional design check**. Write authority rests on ASM-SLATEDB-FENCE, which rests on the unestablished ASM-OBJSTORE-CAS |

`pass-with-recorded-scope` means:
- Every baseline completed a full TLC search, with no states left and no
  invariant, property or deadlock violation.
- Every negative control violated exactly its named property.
- Every reachability witness was reached.

Nothing beyond the recorded constants, bounds, assumptions and exclusions is
claimed. No TLC run proves that the Rust code refines these models. The bridge
is the action-to-code tables below. Their file:line references were checked
against commit ab73296.

## 1. How to run

The driver is `scripts/quality/formal.py`. It writes TLC logs under `--out`
(in `target/`, which is not committed) and TLC metadata to a temporary
directory.

```sh
python3 scripts/quality/formal.py check                  # manifest, files, config/property agreement
python3 scripts/quality/formal.py check --fresh          # also require current receipts
python3 scripts/quality/formal.py run --id TLA-005 --id TLA-006 --id TLA-011 \
    --record --out target/formal/durability              # every check; writes the receipts
python3 scripts/quality/formal.py run --id TLA-006 --role witness   # one role of one obligation
```

`TLA-011/baseline-expanded` took 2,491 s on a loaded host; its timeout is
10,800 s. It belongs in the scheduled lane (roadmap §7.4). Every other check
finished within about 6 minutes under the same load.

The driver has no coverage option. Coverage runs call TLC directly from this
directory, for example:

```sh
java -XX:+UseParallelGC -Xmx3g -cp ../../../target/quality-tools/tla2tools.jar tlc2.TLC \
    -workers 2 -metadir <dir outside the repository> -coverage 1 \
    -config MC_CommitGroups_small.cfg MC_CommitGroups.tla
```

## 2. Shared module

`CommitHandoff.tla` is the one shared abstraction: the four methods of
`src/shard/commit_handoff.rs::CommitHandoff` as pure operators over
`[terminal, pending]`. All three models use it.

| Operator | Production | Atomicity justification |
|---|---|---|
| `PublicationOpen`, `Register` | `CommitHandoff::publication` (commit_handoff.rs:35-37), called by `CommitTransaction::publish` (publish.rs:17-103) | Held under the `ShardEngine::in_flight` std mutex through every applied-mirror update and the push; never across an await |
| `AttachVerdict`, `AttachToNewest` | `CommitHandoff::attach` (commit_handoff.rs:39-52), called by `CommitTransaction::join_prior_barrier` (finalize.rs:45-60) | Under `in_flight`, with `dispatch_gate` held |
| `TakeDurable`, `AfterTakeDurable` | `CommitHandoff::take_durable` (commit_handoff.rs:54-60), called by `ShardEngine::dispatch_durable` (shard.rs:3011) | Under `in_flight`, with `dispatch_gate` held for the whole dispatch |
| `RetireStranded`, `RetireHandoff` | `CommitHandoff::retire` (commit_handoff.rs:62-68), called by `ShardEngine::begin_close` (shard.rs:1879-1885) | Under `in_flight`, together with the `closed` flag store |

Linearizability of the four methods under the mutex is Loom's claim
(`src/shard/commit_handoff/loom_tests.rs`, ASM-DURABILITY-1), not TLC's.
`take_durable` is modelled as the longest prefix with `seq <= durable_seq`.
That equals `Vec::partition_point` when `pending` is sorted.
`PendingSortedInv` is only a structural check: registration order makes it
true by construction, so it is not evidence for ASM-DURABILITY-2.

## 3. Assumptions

The entries are in `verification/assumptions.md` under "Dependency contracts
used by the TLA+ models".

| ID | Used by | Standing |
|---|---|---|
| ASM-SLATEDB-DURABLE | 005, 006, 011 | (a)-(c) established by upstream source inspection and repository tests; (d) conditional, liveness only |
| ASM-SLATEDB-FENCE | 011 | Established by source inspection and repository tests, **conditional on ASM-OBJSTORE-CAS** |
| ASM-OBJSTORE-CAS | 005, 011 | **Unestablished**. Both ambiguous outcomes are modelled: an unreported prefix recovered after a WAL failure, and a conditional WAL PUT that landed and then reported `Fenced` |
| ASM-DURABILITY-1 | 005, 006, 011 | The `CommitHandoff` methods are linearizable. Established within Loom's bounds |
| ASM-DURABILITY-2 to -6 | see the ledger | Established by inspection |
| ASM-DURABILITY-7 | 005, 006 | `db.write` can fail after applying the batch. Established by upstream source inspection |
| ASM-DURABILITY-8 | 005, 006 | No verdict is staged from a batch whose `db.write` failed. The post-apply read window and the `write_failed` guard are modelled; the models assume the guard covers every staging read. Established for the committer by inspection and the F5 regression test. Readers outside the committer are not covered |
| ASM-DURABILITY-9 | 005 (scope of `PubVisibleDurable`) | A handle reload seeds `durable` from a Memory-level read. Established for a live engine; not modelled |
| ASM-DURABILITY-10 | 006 liveness | The engine's actors keep running. Conditional |

ASM-DURABILITY-10 was called ASM-RECOVERY-ACTOR in this group's earlier
drafts. The history group uses that ID with another meaning. No model uses
ASM-CLOCK: nothing decides on elapsed time, and deadlines are
nondeterministic `GiveUp` steps.

## 4. TLA-005 -- Commit groups, dependency barriers, and observable replies

**Claim.** One stream is served by a sequence of engine incarnations, with
restarts in between. The claim has these parts:
- A verdict never becomes observable before the durable state that justifies
  it. This covers an append success, a duplicate success, an idempotent close
  success, a fresh close success, and the state-dependent refusals `Closed`
  and `ProducerSeqReused`.
- A transaction with no writes waits for the durability of the newest
  registered group.
- A group that fails never releases its own verdicts or its dependents'
  verdicts. Failures include a `db.write` error before or after apply, a
  pre-write reject, a fatal WAL error, and retirement.
- A reply binds the exact operation, routing key and offset. A close success
  reports exactly the final durable tail.
- A retry after a lost or abandoned response resolves to exactly one committed
  copy.
- A definitively refused operation never appears, even when its retry is sent.
- The durable mirrors that dispatch publishes never run ahead of durability.
  These are what `Deliver::Durable` readers, tail waiters and touch journals
  see.
- An acknowledgement is sent only after the acknowledged record, or the close,
  is published in those mirrors on the engine that sends it.

**Scope of that claim.**
- Every shape uses the pinned SlateDB order for a post-apply `db.write` error:
  default reads keep returning the failed batch until `closed_result` is
  written.
- `PubVisibleDurable` covers the durable mirrors set by `dispatch_durable`. It
  does not cover the opt-in `Deliver::Applied` read mode, which reads at
  Memory level by design, nor the handle-reload seeding of
  `StreamState.durable` (ASM-DURABILITY-9).
- The retired-engine read case belongs to TLA-018.

**Requirement anchors.** DST-EXPANSION-SPEC §9.1: D1-D4 per verdict kind; D5,
met only error-for-error for the direct replies of TLA-005-F4; D6, D7, D8 and
D10; and W1's publication order, the tail-before-ack order in
`dispatch_durable` (shard.rs:3020-3056).

**Assumptions used.** ASM-SLATEDB-DURABLE (a)-(c), ASM-OBJSTORE-CAS,
ASM-DURABILITY-1 to -4 and -7 to -9. The safety checks use no fairness.

**Observation boundary.**
- A verdict is observable when the server sends it on the request's oneshot
  (`sent`, `nsent`).
- Client receipt (`cview`) is a later, lossy step (`Deliver`, `LoseReply`,
  `GiveUp`).
- Durable truth, `DurableNow`, is the stream state recovered from the object
  store: the last landed WAL batch of the current incarnation, or the state
  recovered at the last restart.
- Invariants are evaluated in every state, across restarts.

**Model.** The requests form a fixed universe:

| Request | Meaning |
|---|---|
| `A` | The final append: record `a`, key `k1`, producer `p` epoch 1 seq 0, hash `hA`, `AppendFinish::Close` |
| `R` | A's retry. It is issued only after A's outcome is unknown to A's client: the deadline passed, the reply was lost, or the answer was non-definitive. The deadline can pass while A is still queued, so R can be queued behind A |
| `X` | A plain append: record `x`, key `k2`, no producer |
| `Y` | Producer `p` seq 0 with hash `hY` |
| `S` | Y's retry |
| `C` | A producer-less, empty `AppendFinish::Close` request. `try_enqueue` turns it into `CommitOp::Close` (shard.rs:1807-1820), staged by `CommitTransaction::close` (maintenance.rs:172-201). It closes by writing the tail, with no record. On a closed stream it is the idempotent no-write success |

The committer drains any non-empty prefix of its channel into one group, then
stages it in order against the applied mirror. A producer row missing from the
applied state is loaded from the Db. While `closed_result` is unwritten, the
load returns SlateDB's committed state, including a batch whose `db.write`
failed after apply. After that the load fails, and the request is answered
`Internal` directly (append.rs:42-44).

**Atomicity / linearization table.**

| Model action | Production function(s) | Atomicity justification or dependency contract |
|---|---|---|
| `Issue(r)` | `ShardEngine::try_enqueue`, `try_command` (shard.rs:1804-1857); `application/append/submit.rs:79-85` | `try_command` checks `is_closed`, then `try_send`. A refusal is non-definitive. An op still in the channel when the committer exits is answered `Moved` (shard.rs:2472-2496) |
| `Deliver`, `LoseReply`, `GiveUp` | Client and transport; the `submit.rs` append deadline (submit.rs:86-93) | Response loss after dispatch; a client deadline with no server cancellation |
| `CommitTake` | `committer_loop` (shard.rs:2467-2566); `CommitTransaction::run` and `stage` (mod.rs:50-189); `append` (append.rs:19-144); `decide_producer` (commit_plan.rs:88-143); `accept_append` (append.rs:145-245); `close` (maintenance.rs:172-201); `load_producer_chain` (shard.rs:2088-2100) | Committer-local: staging touches only the overlay and `DurableEffects` (ASM-DURABILITY-2). A closed engine rejects the whole group with `Moved` (mod.rs:52-57, shard.rs:2502-2509). A producer-row load reads the Db with default options. `DbRead` is the latest applied batch until `closed_result` is written. A failed load answers `Internal` directly. A `billing_rows` failure (mod.rs:58-67) answers the whole group `Internal` with no state change and is not modelled separately |
| `CommitWriteOk` | `CommitTransaction::write` (finalize.rs:170-195): the one `db.write_with_options` | ASM-SLATEDB-DURABLE (a): the batch is applied, not durable |
| `CommitWriteFail` | `write` -> `Err` -> `write_failed` (finalize.rs:196, 202-206) | ASM-DURABILITY-7, pre-apply: the Db is unchanged. `write_failed` calls `begin_close`, then rejects the group `Internal`, in one step with the stranded groups' `Moved` (see `WriteError`). Unbounded when the Db is closed or the batch writer has exited |
| `CommitWriteFailApplied` | slatedb `DbInner::write_batch` (batch_write.rs:262-306): WAL buffer and memtable, then `track_recent_committed_write_batch` (:290), which makes the batch visible to default reads, then `maybe_freeze_current_memtable()?` (:306). `WriteBatchEventHandler::handle` answers `Err` (:134) before `run_lifecycle` writes `closed_result` (dispatcher.rs:358). Then `write_failed` (finalize.rs:202-206) | ASM-DURABILITY-7, post-apply. The batch writer exits (`writerDead`); reads stay open until `DbCloseResult`. The retirement is in the same step as the error. Between the error and `retire()` only dispatch claims of earlier durable groups and other retirements can interleave. They do not depend on the failed batch and are modelled as earlier steps. `WriteErrorRetires` is the mutation point |
| `CommitPreWriteReject` | `finish`: accounting divergence (finalize.rs:5-9) and `stage_maintenance` (finalize.rs:25-31) | Nothing is written. The group is answered `Internal` and the engine stays open |
| `DbCloseResult` | `run_lifecycle` writes `closed_result` (dispatcher.rs:358) | A separate step, because it runs on the batch-writer task after the committer was answered |
| `CommitPublish` | `CommitTransaction::publish` (publish.rs:17-103) | One step under `in_flight`. A terminal handoff rejects with `Moved` and publishes nothing (publish.rs:25-31) |
| `CommitAttach` | `finish` -> `has_writes` false -> `join_prior_barrier` (finalize.rs:12-19, 45-60) | The gate, `attach` and the reply are one step. The gate excludes the dispatcher for the whole step, and a Durable verdict stays justified if retirement interleaves, because durability is monotone. TLA-006 splits this step |
| `WalLand`, `WalReport`, `WalFail` | SlateDB WAL flush (the pump's `flush_with_options`, shard.rs:1554; SlateDB `flush_interval`), `DbStatus.durable_seq`, `close_reason` | ASM-SLATEDB-DURABLE (b)(c). Landing stops within the incarnation only when the WAL flusher is dead. `durable_seq` is not reported after the Db closed |
| `BeginClose` | `ShardEngine::begin_close` (shard.rs:1876-1934), called from `acker_loop` on `close_reason` (shard.rs:3085-3089) or externally (`ShardDirectory::retire`, `RequiredExit`) | `retire()` runs under `in_flight`. The retirer alone owns the stranded groups and rejects them `Moved` (shard.rs:1917-1919). One step, because no other actor can reach the stranded vector |
| `DispatchClaim`, `DispatchVisible`, `DispatchReply` | `dispatch_durable` (shard.rs:3006-3071): `take_durable` under the gate (:3011), then the ring publish (:3026) and `handle.state.durable` (:3046), then the acks (:3055) | The claim is one step under `in_flight`. Visibility and replies are separate steps, which is more permissive than production (ASM-DURABILITY-3). `ReplyPhases` is the mutation point of that order |
| `Restart` | A process crash, or a reopen after retirement: `Db::builder().build()` replays the object store | Erases every volatile variable. Any written prefix from `landed` on may have landed: an in-flight PUT, the close flush, or a PUT whose failure reply was lost |

**Constants and bounds.** Every configuration binds the five mutation points
to the `Real*` operators, except the one point a control replaces.

| Config | Reqs | MaxRestarts | MaxWalFail | MaxWriteFail | MaxRetire |
|---|---|---|---|---|---|
| `MC_CommitGroups_small.cfg` (baseline; most controls and witnesses) | A, R, X | 1 | 1 | 1 | 1 |
| `MC_CommitGroups_expanded.cfg` ("requests"; also `witness_seq_reused_refusal`) | A, R, X, Y | 1 | 1 | 0 | 0 |
| `MC_CommitGroups_requests_faults.cfg` | A, R, X, Y | 0 | 1 | 1 | 1 |
| `MC_CommitGroups_faults.cfg` | A, R, X | 2 | 1 | 1 | 1 |
| `MC_CommitGroups_close.cfg` (also the D3 control and the close witnesses) | A, R, C | 1 | 1 | 1 | 1 |
| `MC_CommitGroups_refusal_retry.cfg` (also `nc_failed_release_d10`, `nc_write_error_keeps_staging_refusal` and `witness_refused_retry_resolved`) | A, Y, S | 1 | 1 | 1 | 1 |

`MaxWriteFail` bounds group failures: a `db.write` error on an open Db
(before or after apply) or a pre-write reject. The small and refusal-retry
shapes have the constants of the configurations that reproduced TLA-005-F5
(`read_window` and `read_window_refusal` in the earlier revision). The pinned
post-apply read window is now part of every shape, so those two
configurations were merged into these baselines.

Every baseline checks these invariants: `TypeOK`, `PendingSortedInv`,
`AtMostOneSettlement`, `D1_AppendAckDurable`, `D2_DuplicateAckDurable`,
`D3_CloseAckDurable`, `D4_RefusalDurable`, `D7_ExactlyOnce`,
`D8_ReplyBinding`, `D10_RefusedNeverAppears`, `PubVisibleDurable` and
`AckAfterVisibility`.

How the other requirements are covered:
- **D5** is checked as D1-D4 per verdict kind. The failed-group control shows
  that a released dependent breaks D4.
- **D6** is D1/D2 checked in every state, after a lost reply and across
  restarts, plus `Witness_LostAckThenRetryResolved`.
- **D3** is independent of D1/D2, because C closes without a record.
- **D10** can fail, because S and R re-send refused operations.

**Negative controls.** Each is a module that EXTENDS the unmodified
`CommitGroups` and binds one mutation point in its cfg. Each cfg checks one
target invariant.

| Control | Shape | Mutation | Must violate | Why that property |
|---|---|---|---|---|
| `nc_write_error_keeps_staging` | small | `WriteErrorRetires <- MutWriteErrorKeepsStaging` (FALSE): the pre-fix committer answers a failed `db.write` `Internal` and keeps staging | `D2_DuplicateAckDurable` | A's write fails after apply. R loads A's producer row from the failed batch, and attach on the empty open handoff answers Durable. The TLA-005-F5 duplicate schedule, 8 states |
| `nc_write_error_keeps_staging_refusal` | refusal-retry | The same module | `D4_RefusalDurable` | The same window gives Y a `ProducerSeqReused` that no durable state justifies. The TLA-005-F5 refusal schedule, 7 states |
| `nc_skip_barrier` | small | `AttachOp <- MutAttachSkipBarrier`: attach answers Durable whenever the handoff is not terminal | `D2_DuplicateAckDurable` | R's duplicate verdict is released while A's group is registered but not landed |
| `nc_publish_early` | small | `PublishVisibleOp <- MutPublishVisibleAtRegistration` | `PubVisibleDurable` | Readers see A before its WAL batch lands |
| `nc_failed_release` | small | `StrandedResultOp <- MutReleaseStranded`: retirement sends staged verdicts instead of `Moved` | `D4_RefusalDurable` | X's `Closed` refusal is staged behind A's group, which never becomes durable, and is released when that group fails |
| `nc_failed_release_d10` | refusal-retry | The same module | `D10_RefusedNeverAppears` | Y's `ProducerSeqReused` is justified only by A's failed group but is released. After the restart, S is accepted and `y` becomes durable |
| `nc_idem_close_unbarriered` | close | `AttachOp <- MutAttachSkipBarrierForIdempotentClose`: a no-write group whose verdicts are all idempotent closes skips the barrier | `D3_CloseAckDurable` | C's idempotent success is sent while A's closing group is still pending. D1/D2 cannot see this |
| `nc_reply_before_visible` | small | `ReplyPhases <- MutReplyBeforeVisible`: acks may be sent straight after the claim | `AckAfterVisibility` | An acknowledged record is not yet in the durable mirrors of the engine that sent the ack |

**Reachability witnesses.** Each runs on the unmodified model, and TLC is
expected to report it violated:
- `Witness_DuplicateAckReceived`
- `Witness_ClosedRefusalReceived`
- `Witness_LostAckThenRetryResolved`
- `Witness_AttachedReplyReleased`: a verdict attached to a pending group is
  later received as a success.
- `Witness_DependentRejectedWithGroup`
- `Witness_UnreportedBatchRecovered`
- `Witness_WalFailureRetires`: the acker retires the engine because the WAL
  flusher failed.
- `Witness_FailedWriteRecovered`: a batch whose write failed after apply is
  recovered by the next open.
- `Witness_ProducerLoadFailsInternal`
- `Witness_SeqReusedRefusal` (requests shape)
- `Witness_IdempotentCloseAfterAttach` (close shape)
- `Witness_CloseByWriteRefusesFinalAppend` (close shape)
- `Witness_RefusedRetryResolved` (refusal-retry shape)
- `Witness_WriteErrorStrandsGroup`: a `db.write` error retires the engine and
  an earlier registered group is answered `Moved`, although it might have
  become durable. This is the cost the fix commit names.
- `Witness_ReadWindowGroupMoved`: after a post-apply error, a group whose
  producer row only the failed batch holds is taken while default reads still
  return that row, and it is answered `Moved` instead of being staged.

**Exclusions (not claimed).**
- Only one stream and two routing keys. Multi-stream groups rely on the prefix
  claim order in `dispatch_durable`; that is argued, not modelled.
- Only one producer epoch and seq 0. Stale epochs, epoch start, gaps and
  older-duplicate replay metadata belong to KANI-036..038 and TLA-007.
- Seal claims, seal fences and `SealSuperseded` belong to TLA-002/003 (see
  TLA-005-F4).
- Queue ops and their `queue_acks` use the same mechanism and are not
  modelled. History absorption and trim belong to TLA-016/018.
- Only the producer-row staging read is modelled. The sequence-row load,
  `stream_handle` reloads, `seal_fence` and the billing-row read run in the
  same committer, after `run`'s `is_closed` check, so the same guard covers
  them (ASM-DURABILITY-8). The model keeps the handle resident.
- Timing (gather windows, wedge shedding) and memory ordering (Loom) are not
  modelled.
- SlateDB itself is represented only by its dependency contract.

## 5. TLA-006 -- Commit handoff, retirement, and already-durable completion

**Claim.** One engine runs a committer, a durable completer, a retirer, and a
supervisor that may abort the committer after retirement. Requesters may
cancel. The claim has these parts:
- Every effect batch has exactly one terminal owner: no reply is settled
  twice.
- Every success is justified by the durability of the WAL sequence it depends
  on.
- **Every success sent after retirement was decided while the handoff was
  live.** "Decided" means the group was claimed by `take_durable`, or `attach`
  answered Durable. "Live" is read from `terminal` at that decision step. This
  is the §1.5 boundary: a late durable response is legal, but a new success
  after the close boundary is not.
- Retirement blocks new publications and drains every registered group.
- Work that was merely applied when retirement drained it is never answered
  with success, even when it was already durable.
- Under the stated fairness, every issued request is eventually settled
  server-side.

**Requirement anchors.** D1-D5, the single-engine part of T11-T12, and R1-R3.

**Assumptions used.** ASM-SLATEDB-DURABLE, parts (a)-(c) for safety and (d)
for liveness; ASM-DURABILITY-1 to -3, -7 and -8; ASM-DURABILITY-10, for
liveness only.

**Observation boundary.** Server-side settlement of each request's oneshot
(`nsent`, `res`, `sentDep`, `afterRetire`). A cancellation drops the receiver.
The server still settles it, and the model counts that settlement.

**Model.** Request content is abstracted. Requests in `Writers` stage a
storage write; the others stage a no-write verdict (TLA-005 owns which
verdict). `dep` is the WAL seq whose durability justifies a reply. A no-write
verdict's `dep` is the newest batch its staging read can see: the newest
written batch while the Db is readable, else the newest registered group
(`StagingDep`). The two differ only in the post-apply read window.

The ghost `decidedLive` is set **only** by the claim step and the
attach-Durable step. Each ORs in `~ho.terminal` read in the same step,
independently of what the possibly mutated `TakeOp` and `AttachOp` return.
That is what makes `LateSuccessWasClaimedLive` falsifiable (TLA-006-F3).

**Atomicity / linearization table.**

| Model action | Production function(s) | Atomicity justification |
|---|---|---|
| `Issue`, `Cancel` | `try_command` (shard.rs:1837-1857); receiver drop in `submit.rs` | As TLA-005 |
| `Take` | `committer_loop` (shard.rs:2467-2566), `CommitTransaction::run` (mod.rs:50-57) | As TLA-005 |
| `WriteOk` | `CommitTransaction::write` (finalize.rs:170-195) | ASM-SLATEDB-DURABLE (a). `lateWrite` records a batch written after retirement |
| `WriteRefused` | `write` on a closed Db or after the batch writer exited (`check_closed`), then `write_failed` (finalize.rs:202-206) | Nothing is applied. The committer retires the handoff (`RetireOnWriteError`) and answers `Internal`. Fair, because the committer keeps running |
| `WriteFailPreApply` | `write` -> `Err` on an open Db (`EmptyBatch`, or a clock or WAL-buffer error before the memtable write), then `write_failed` | ASM-DURABILITY-7, pre-apply. The committer retires the handoff and answers `Internal`. A bounded fault |
| `WriteFailApplied` | slatedb `write_batch` post-apply error, then `write_failed` | ASM-DURABILITY-7, post-apply. The batch writer exits; reads stay open until `DbCloseResult`. The committer retires the handoff in the same step |
| `PreWriteReject` | The committer's own rejects before the write: accounting divergence and `stage_maintenance` (finalize.rs:5-9, 25-31) | Nothing is written and the engine stays open. A bounded fault |
| `DbCloseResult` | `run_lifecycle` writes `closed_result` (dispatcher.rs:358) | A separate step, as in TLA-005 |
| `Publish` | `CommitTransaction::publish` (publish.rs:17-103) via `PublicationOp` | One `in_flight` step |
| `AttachGate`, `AttachDecide`, `AttachReply` | `join_prior_barrier` (finalize.rs:45-60): `dispatch_gate.lock().await`, then `attach` under `in_flight` via `AttachOp`, then `reply`/`reject` after unlocking while still holding the gate | Three steps, so retirement may interleave between the verdict and the reply |
| `Claim`, `Replies` | `dispatch_durable` (shard.rs:3006-3071) via `TakeOp`, used by the acker and the pump. Both serialize on `dispatch_gate`, so one completer process is faithful | The claim runs under `in_flight`; the effects run after unlocking, under the gate |
| `AckerClose`, `ExternalClose` | `begin_close` (shard.rs:1876-1934), called from `acker_loop` on `close_reason` (shard.rs:3085-3089), or from `ShardDirectory::retire` / `RequiredExit` (lifecycle.rs:122-135) | `retire()` and the `closed` store run under `in_flight`, via `StrandedOp` (`DoBeginClose`) |
| `RejectStranded` | `begin_close` after unlocking: `group.effects.reject(AppendErr::Moved)` (shard.rs:1917-1919) | A separate step, because other actors interleave. After a write error production rejects them inside the committer's call, so the separate step is more permissive |
| `AbortCommitter` | `drive_shutdown` (tasks/shutdown.rs:176-209) aborts a worker still running after `WORKER_GRACE` (lifecycle.rs:10, 53-75) | Only at an await of a staged transaction: handle or producer loads, `db.write`, or the gate. The dropped `db.write` may still apply. Every owned sender and the channel are dropped |
| `Land`, `Report`, `WalFail` | SlateDB, as TLA-005 | ASM-SLATEDB-DURABLE |

**Constants and bounds.**

| Config | Reqs | Writers | MaxWalFail | MaxWriteFail | MaxRetire | MaxAbort | Spec |
|---|---|---|---|---|---|---|---|
| `MC_HandoffRetirement_small.cfg` (also every safety control and witness) | r1, r2, r3 | r1, r3 | 1 | 1 | 1 | 1 | `Spec` |
| `MC_HandoffRetirement_expanded.cfg` | r1..r4 | r1, r3 | 1 | 1 | 2 | 1 | `Spec` |
| `MC_HandoffRetirement_liveness.cfg` (also the assumption-removal control) | r1, r2, r3 | r1, r3 | 1 | 1 | 1 | 1 | `LiveSpec`, PROPERTY `AllIssuedSettle` |

`MaxWriteFail` bounds group failures: a `db.write` error on an open Db, before
or after apply, or a pre-write reject.

The safety invariants are `TypeOK`, `PendingSortedInv` (structural),
`SettledAtMostOnce`, `SuccessIsDurable`, `StrandedNeverSucceeds`,
`LateSuccessWasClaimedLive`, `RetiredEngineFrozen` (a ghost that restates the
publication guard) and `RetiredHandoffEmpty`.

**Liveness.** `LiveSpec` puts weak fairness on the committer and completer
steps (`Take`, `WriteOk`, `WriteRefused`, `Publish`, `AttachGate`,
`AttachDecide`, `AttachReply`, `Claim`, `Replies`), on retirement
(`AckerClose`, `RejectStranded`) and on storage progress (`Land` while the WAL
flusher lives, `Report` while the Db is open). These actions get **no**
fairness: `Issue`, `Cancel`, `WalFail`, `WriteFailPreApply`,
`WriteFailApplied`, `PreWriteReject`, `DbCloseResult`, `ExternalClose` and
`AbortCommitter`. The faults are bounded, so they eventually cease. Nothing is
fair on a success outcome, and there is no symmetry reduction. The property is
conditional on ASM-SLATEDB-DURABLE (d) and ASM-DURABILITY-10.

**Negative controls.** All run on the small shape, one target each.

| Control | Mutation | Must violate | Why |
|---|---|---|---|
| `nc_write_error_keeps_staging` | `WriteErrorRetires <- MutWriteErrorKeepsStaging` (FALSE): the pre-fix committer | `SuccessIsDurable` | r1's write fails after apply. r2's no-write verdict is staged while reads still see r1's batch, so it depends on that batch. Attach on the empty open handoff answers Durable, and the success is sent before the batch lands |
| `nc_reclaim_claimed` | `StrandedOp <- MutStrandedReclaim`: retirement also takes the groups the completer claimed | `SettledAtMostOnce` | The claimed batch is answered by the completer and also rejected by the retirer |
| `nc_attach_ignore_terminal` | `AttachOp <- MutAttachIgnoreTerminal` (the review's mutant M1) | `LateSuccessWasClaimedLive` | A no-write verdict that reached the attach step after retirement is answered Durable. Nothing was pending, so `SuccessIsDurable` does not catch it |
| `nc_admit_after_close_acked` | `PublicationOp <- MutPublicationAlways` **and** `TakeOp <- MutTakeIgnoreTerminal` | `LateSuccessWasClaimedLive` | A batch admitted after the close boundary is claimed and acknowledged by the retired engine: a new, unauthorized success |
| `nc_admit_after_close` | `PublicationOp <- MutPublicationAlways` only | `RetiredEngineFrozen` | The guard-restating form. `take_durable` still refuses a terminal handoff, so the admitted group is never acknowledged. This control only shows that the ghost detects the missing guard |
| `nc_no_storage_progress` (assumption removal) | `LiveSpecWithoutLanding`: `LiveSpec` without `WF(Land)` | `AllIssuedSettle` (temporal) | A registered group whose WAL never lands is never settled, so the liveness claim is not vacuous |

**Reachability witnesses.** Each runs on the unmodified model and is expected
to be violated:
- `Witness_LateDurableReply`: a group claimed while live sends its success
  after retirement.
- `Witness_LateDurableAttachReply`
- `Witness_UnclaimedDurableRejected`
- `Witness_NonDurableRejected`
- `Witness_CancelledRequesterSettled`
- `Witness_LateWriteLands`
- `Witness_AbortDropsReplies`
- `Witness_PreWriteRejectThenSuccess`: after the committer's own pre-write
  reject, the engine stays open and a later group is acknowledged.
- `Witness_WriteErrorStrandsGroup`: a `db.write` error retires the handoff and
  a group it stranded is answered `Moved`.

**Exclusions.**
- Request semantics belong to TLA-005.
- Memory ordering and the Tokio channel are covered by Loom and the held-WAL
  integration tests.
- A panic inside `dispatch_durable` after a claim drops the senders, so the
  outcome is unknown. It is not modelled.
- Pump timing is not modelled.
- More than one engine belongs to TLA-011.

## 6. TLA-011 -- Serving possession, owner fencing, and shard movement

**Status: conditional design check.** Durable write authority is the storage
fence (ASM-SLATEDB-FENCE). That fence is conditional on the object store's
conditional create (ASM-OBJSTORE-CAS). No provider evidence is held for
ASM-OBJSTORE-CAS, so every claim below is conditional on it (roadmap §7.7).

**Claim.** The setting has:
- two nodes, one shard prefix, stale per-node ownership views and one override
  move;
- node crashes, after which the restarted process bootstraps with no ring;
- delayed old work: queued requests, buffered WAL batches, a crashed process's
  in-flight WAL PUT, and a storage close abandoned with a PUT in flight;
- an ambiguous conditional WAL PUT;
- both managed-fleet mode and single-instance (no-ring) mode.

Under those conditions, the claim has these parts:
- Routing preference alone never grants durable write authority. That
  authority is the storage fence.
- Every acknowledgement from any engine is recoverable at its acknowledged
  offset from the object store.
- The original and its retry commit at most once across owners.
- Every serving engine of a higher writer epoch already covers every
  acknowledgement made by a lower epoch. An old owner may still send late
  durable replies (the TLA-006 exception), but it never acknowledges a write
  the new owner did not recover.
- Every WAL data batch was written under its writer's fence, with no newer
  fence in between.
- Uncertain movement is never turned into success. No success exists without
  durable evidence. Every non-serving outcome (NotOwner, Wait/opening, Moved,
  Internal, timeout) is non-definitive in the product mapping
  (`definitively_rejected`, contract.rs:285-297).

**Requirement anchors.** T11, in the refined sense of TLA-011-F3; T12, D1, D7,
P4 and R1; T10, only in the sense that a stale router cannot make a producer
operation lost or duplicated. Per-key reordering is TLA-012's subject.

**Assumptions used.** ASM-SLATEDB-FENCE (the load-bearing one),
ASM-SLATEDB-DURABLE (a)-(c), ASM-OBJSTORE-CAS (unestablished),
ASM-DURABILITY-1, -5 and -6. Safety only; no fairness.

**Observation boundary.** `acks` holds every success reply any engine sends
(server-side), with the sending engine and its writer epoch. Storage truth is
`Replay(wal)`: the object-store WAL replayed in id order, with the last write
winning, so an overwritten acknowledged offset is visible.

**Model.**
- `view[n]` is a node, or `"none"`. `"none"` means no ring: `effective_owner()`
  is None, so the node serves everything.
- Fleet mode starts with every view at `InitOwner`. `Move` changes the fleet
  authority (the override CAS), and `Observe(n)` publishes it at node n later.
- Single mode keeps every view at `"none"`.
- Each committer group holds one request. Grouping is TLA-005's subject.
- WAL landing and the `durable_seq` report are one step here. TLA-005
  separates them.

**Atomicity / linearization table.**

| Model action | Production function(s) | Atomicity justification / dependency |
|---|---|---|
| `Move`, `Observe(n)` | Rebalancer override CAS; fleet loop `set_view` (fleet.rs:797); `OwnershipService::set_view` (ownership.rs:156) | A complete authority snapshot is replaced under one lock |
| `Send(r, n)` | `ShardDirectory::resolve` (shard_directory.rs:238-296): a foreign owner -> `NotOwner`; a live resident -> `try_enqueue`; otherwise `OpenGate::get_or_open` -> Wait. Also `effective_owner`/`foreign_owner` (ownership.rs:76-99) | Resolution reads the view and the serving map. Appends are never relayed (ASM-DURABILITY-5) |
| `GiveUp(r)` | Client deadline | The server continues |
| `Commit(e)` | The committer: staging, `db.write`, then `publish` or `join_prior_barrier` (as TLA-005). On a closed Db, `write_failed` (finalize.rs:202-206) retires the engine and answers `Internal` | Merged into one step. Retirement between the write and the publish only turns the reply into `Moved`, which the close path already produces. The closed-Db branch closes the engine in the same step (`CloseEngineWith`) |
| `Land(e)` | SlateDB WAL SST put-if-absent at the writer's next id (`tablestore.rs` `write_sst_in_object_store`, `PutMode::Create`); the `durable_seq` report | ASM-SLATEDB-FENCE, via `LandOp`. Three outcomes: the batch lands; the id is taken, giving `Fenced`; or, with `AmbiguousPut`, the batch lands but the reply is lost, and the retry sees `AlreadyExists`, a spurious `Fenced` with the batch durable and never reported |
| `Claim(e)`, `Reply(e)` | `dispatch_durable`, via `ClaimOp` | As TLA-006. `Reply` stays enabled after close (the late durable response) |
| `AckerClose(e)`, `Yield(n)` | `acker_loop` on `close_reason` (Fenced); the fleet-tick "possession yields" (fleet.rs:818-843) and `resolve`'s retire on a foreign owner, both via `ShardDirectory::retire` (shard_directory.rs:434) -> `begin_close` | Serving-map removal and `begin_close` are one step. The stranded groups and the queue get `Moved` |
| `Terminate(e)` | Engine storage close (`EngineTasks::begin_close`, lifecycle.rs:53-75, under `WORKER_GRACE`); the `OpenGate` `closing` gate (sharddir.rs:253) | A node cannot reopen until its old engine has terminated. A close may flush, may find the Db already closed, or may be **abandoned** (the grace is exceeded, or a non-fence failure) with a PUT in flight. Then any prefix of the buffer, as one SST, may still land later |
| `OpenStart(n)`, `OpenFence(e)`, `OpenReady(e)` | `OpenGate::get_or_open` (sharddir.rs:479), single-flight. The opener is `Db::builder().build()` (bootstrap.rs:485-489): `FenceableManifest::init_writer`, `fence_and_init`, the final refresh (`RefreshOp`), replay. Then `ShardEngine::start` and serving-map insertion | Three steps, via `ReadyGate` and `RefreshOp`. Epoch order and fence order can differ, and a superseded opener fails its refresh |
| `Crash(n)` | Process crash | Volatile engine state is erased. Any prefix of the buffer, as one in-flight SST, may still land (`ZombieLand`). The restarted process bootstraps with no ring (ASM-DURABILITY-6) |
| `ZombieLand(e)` | A dead engine's in-flight conditional PUT reaching the object store | One conditional PUT at the dead writer's next id: all its batches land, or none do if the id is taken |

**Constants and bounds.**

| Config | Mode | Reqs | MaxEngines | MaxCrashes | MaxMoves | MaxTries | AmbiguousPut |
|---|---|---|---|---|---|---|---|
| `MC_ServingOwnership_small.cfg` | fleet | A, R | 3 | 1 | 1 | 1 | TRUE |
| `MC_ServingOwnership_expanded.cfg` | fleet | A, R, X | 3 | 1 | 1 | 2 | TRUE |
| `MC_ServingOwnership_single.cfg` | single | A, R, X | 3 | 1 | 0 | 1 | TRUE |

The invariants are `TypeOK`, `PendingSortedInv` (structural), `AckedDurable`,
`ExactlyOnceAcrossOwners`, `HigherEpochCoversAcks` and
`DataUnderWriterAuthority`. The last restates `RealLand`'s put-if-absent
guard at the storage boundary, so it is a sanity check of the WAL shape, not
independent evidence.

**Negative controls.** One target each.

| Control | Shape | Mutation | Must violate | Why |
|---|---|---|---|---|
| `nc_ring_skips_refresh` | small | `RefreshOp <- MutRefreshByRing`: an opener whose managed ring names it the owner skips the final manifest refresh | `HigherEpochCoversAcks` | A superseded opener serves because the ring says so. It fences last, then lands and acknowledges A, while the higher-epoch owner, already serving, never replayed A |
| `nc_ring_grants_storage` | expanded | `LandOp <- MutLandByRing`: a writer whose view says it owns the prefix has its WAL put accepted after a newer fence | `AckedDurable` | The old owner lands and acknowledges A after the new owner's fence. The new owner never replayed A, and its own write at the same offset overwrites the acknowledged record. X is needed: with only A and R, both writes would be record `a` |
| `nc_serve_before_fence` | small | `ReadyGate <- MutReadyGateNoFence`: the new owner serves after the epoch CAS, without the fence WAL | `HigherEpochCoversAcks` | The old writer still lands and acknowledges a batch the new owner never replayed |
| `nc_ring_authorizes` (a durability-evidence control) | small | `ClaimOp <- MutClaimByRing`: while the view says mine, acknowledge every registered group without `durable_seq` | `AckedDurable` | This isolates "an ack needs durability evidence". Any premature ack gives the same violation |

**Reachability witnesses.** Each runs on the unmodified model and is expected
to be violated:
- `Witness_TakeoverInstalls`
- `Witness_StaleWriteFenced`
- `Witness_LateDurableReplyAfterTakeover`, run twice: once with a
  crash-bootstrap, and once with no crashes on the plain override-move path.
- `Witness_DuplicateResolvedByNewOwner`
- `Witness_ZombieWriteLands`: a crashed process's PUT lands later.
- `Witness_AbandonedCloseWriteLands`: an abandoned storage close's PUT lands
  later.
- `Witness_AmbiguousPutRecovered`: a PUT that reported `Fenced` is replayed by
  a later owner.
- `Witness_CrashBetweenFenceAndServe`: a move is interrupted by a crash.
- `Witness_SupersededOpenFails`
- `Witness_TakeoverInstalls` again, in single mode: both nodes open and serve
  in turn.

**Exclusions.**
- Only one shard prefix and one stream, with one request per committer group.
- No WAL or `db.write` failures other than fencing, a write on a closed Db and
  the ambiguous PUT. TLA-005 covers the others.
- No OpenGate holdoff, deadline or reaper timing.
- No SlateDB compaction, L0 flush or GC. The WAL replay stands for the whole
  recovered state.
- Reads and SSE cut-offs belong to TLA-018/027. The peer relay of reads is not
  modelled.
- The fleet controller's own correctness belongs to TLA-039.

## 7. Results

TLA-005 and TLA-011 come from one run of

```sh
python3 scripts/quality/formal.py run --id TLA-005 --id TLA-006 --id TLA-011 --record --out target/formal/durability
```

on commit ab73296 with this group's files uncommitted (the receipts record
`dirty_tree: true`). It reported `FORMAL_OK: 65 check(s) across 3
obligation(s)`. Afterwards a stale assumption ID was fixed in a comment of
`MC_HandoffRetirement_nc_no_storage_progress.tla`, which changed TLA-006's
inputs, so TLA-006 was run again with `run --id TLA-006 --record`
(`FORMAL_OK: 18 check(s) across 1 obligation(s)`). The TLA-006 rows are from
that run. `formal.py check --fresh` then reported all three receipts current.
Two earlier starts of the combined command were stopped by hand before they
finished (to fix the TLA-005 `input_scope` text and to raise timeouts); their
partial results are not reported.

Tools: TLC 2.19 (tla2tools 1.7.4, the `[formal]` pin) on Java 17.0.1, 2
workers per check. The host was shared with other verification jobs (load
average 25 to 105), so the seconds are wall-clock time under load, not
performance data. For a violation, the state counts are where the
breadth-first search stopped and the depth is the counterexample length. The
temporal control reports "Temporal properties were violated"; its cfg checks
exactly one property, so the violation is attributable to it.

### TLA-005

Receipt `verification/receipts/TLA-005.json`, run on ab73296 (dirty tree: true).

| Check | Role | Expected | Verdict | Distinct states | Generated | Depth | Seconds |
|---|---|---|---|---|---|---|---|
| `TLA-005/baseline-small` (`MC_CommitGroups_small.cfg`) | baseline | pass | pass | 1,510,380 | 4,210,724 | 30 | 93.9 |
| `TLA-005/baseline-expanded-requests` (`MC_CommitGroups_expanded.cfg`) | baseline | pass | pass | 4,250,335 | 12,718,338 | 33 | 322.8 |
| `TLA-005/baseline-expanded-requests-faults` (`MC_CommitGroups_requests_faults.cfg`) | baseline | pass | pass | 4,083,910 | 13,523,135 | 30 | 320.1 |
| `TLA-005/baseline-expanded-faults` (`MC_CommitGroups_faults.cfg`) | baseline | pass | pass | 5,760,039 | 16,708,415 | 31 | 275.2 |
| `TLA-005/baseline-close` (`MC_CommitGroups_close.cfg`) | baseline | pass | pass | 1,085,300 | 2,987,953 | 24 | 37.1 |
| `TLA-005/baseline-refusal-retry` (`MC_CommitGroups_refusal_retry.cfg`) | baseline | pass | pass | 1,031,436 | 2,840,636 | 24 | 37.4 |
| `TLA-005/nc-write-error-keeps-staging` (`MC_CommitGroups_nc_write_error_keeps_staging.cfg`) | negative-control | violation D2_DuplicateAckDurable | violation D2_DuplicateAckDurable | 2,906 | 4,994 | 8 | 1.9 |
| `TLA-005/nc-write-error-keeps-staging-refusal` (`MC_CommitGroups_nc_write_error_keeps_staging_refusal.cfg`) | negative-control | violation D4_RefusalDurable | violation D4_RefusalDurable | 1,702 | 2,748 | 7 | 1.6 |
| `TLA-005/nc-skip-barrier` (`MC_CommitGroups_nc_skip_barrier.cfg`) | negative-control | violation D2_DuplicateAckDurable | violation D2_DuplicateAckDurable | 10,871 | 19,040 | 9 | 1.9 |
| `TLA-005/nc-publish-early` (`MC_CommitGroups_nc_publish_early.cfg`) | negative-control | violation PubVisibleDurable | violation PubVisibleDurable | 197 | 252 | 5 | 1.3 |
| `TLA-005/nc-failed-release` (`MC_CommitGroups_nc_failed_release.cfg`) | negative-control | violation D4_RefusalDurable | violation D4_RefusalDurable | 1,967 | 3,058 | 7 | 1.4 |
| `TLA-005/nc-failed-release-d10` (`MC_CommitGroups_nc_failed_release_d10.cfg`) | negative-control | violation D10_RefusedNeverAppears | violation D10_RefusedNeverAppears | 234,700 | 484,524 | 13 | 5.9 |
| `TLA-005/nc-idem-close-unbarriered` (`MC_CommitGroups_nc_idem_close_unbarriered.cfg`) | negative-control | violation D3_CloseAckDurable | violation D3_CloseAckDurable | 5,696 | 9,392 | 8 | 1.6 |
| `TLA-005/nc-reply-before-visible` (`MC_CommitGroups_nc_reply_before_visible.cfg`) | negative-control | violation AckAfterVisibility | violation AckAfterVisibility | 17,692 | 31,306 | 9 | 2.4 |
| `TLA-005/witness-duplicate-ack-received` (`MC_CommitGroups_witness_duplicate_ack_received.cfg`) | witness | violation Witness_DuplicateAckReceived | violation Witness_DuplicateAckReceived | 30,749 | 57,004 | 10 | 2.7 |
| `TLA-005/witness-closed-refusal-received` (`MC_CommitGroups_witness_closed_refusal_received.cfg`) | witness | violation Witness_ClosedRefusalReceived | violation Witness_ClosedRefusalReceived | 19,055 | 33,922 | 9 | 2.0 |
| `TLA-005/witness-lost-ack-then-retry-resolved` (`MC_CommitGroups_witness_lost_ack_then_retry_resolved.cfg`) | witness | violation Witness_LostAckThenRetryResolved | violation Witness_LostAckThenRetryResolved | 584,540 | 1,380,416 | 15 | 14.3 |
| `TLA-005/witness-attached-reply-released` (`MC_CommitGroups_witness_attached_reply_released.cfg`) | witness | violation Witness_AttachedReplyReleased | violation Witness_AttachedReplyReleased | 463,981 | 1,061,940 | 15 | 13.2 |
| `TLA-005/witness-dependent-rejected-with-group` (`MC_CommitGroups_witness_dependent_rejected_with_group.cfg`) | witness | violation Witness_DependentRejectedWithGroup | violation Witness_DependentRejectedWithGroup | 2,071 | 3,193 | 7 | 1.5 |
| `TLA-005/witness-unreported-batch-recovered` (`MC_CommitGroups_witness_unreported_batch_recovered.cfg`) | witness | violation Witness_UnreportedBatchRecovered | violation Witness_UnreportedBatchRecovered | 201 | 256 | 5 | 1.2 |
| `TLA-005/witness-wal-failure-retires` (`MC_CommitGroups_witness_wal_failure_retires.cfg`) | witness | violation Witness_WalFailureRetires | violation Witness_WalFailureRetires | 39 | 42 | 4 | 1.1 |
| `TLA-005/witness-failed-write-recovered` (`MC_CommitGroups_witness_failed_write_recovered.cfg`) | witness | violation Witness_FailedWriteRecovered | violation Witness_FailedWriteRecovered | 233 | 310 | 5 | 1.3 |
| `TLA-005/witness-producer-load-fails-internal` (`MC_CommitGroups_witness_producer_load_fails_internal.cfg`) | witness | violation Witness_ProducerLoadFailsInternal | violation Witness_ProducerLoadFailsInternal | 71 | 84 | 5 | 1.3 |
| `TLA-005/witness-seq-reused-refusal` (`MC_CommitGroups_witness_seq_reused_refusal.cfg`) | witness | violation Witness_SeqReusedRefusal | violation Witness_SeqReusedRefusal | 7,672 | 14,722 | 8 | 2.5 |
| `TLA-005/witness-idempotent-close-after-attach` (`MC_CommitGroups_witness_idempotent_close_after_attach.cfg`) | witness | violation Witness_IdempotentCloseAfterAttach | violation Witness_IdempotentCloseAfterAttach | 365,256 | 813,602 | 14 | 9.7 |
| `TLA-005/witness-close-by-write-refuses-final-append` (`MC_CommitGroups_witness_close_by_write_refuses_final_append.cfg`) | witness | violation Witness_CloseByWriteRefusesFinalAppend | violation Witness_CloseByWriteRefusesFinalAppend | 103,768 | 203,542 | 11 | 4.1 |
| `TLA-005/witness-refused-retry-resolved` (`MC_CommitGroups_witness_refused_retry_resolved.cfg`) | witness | violation Witness_RefusedRetryResolved | violation Witness_RefusedRetryResolved | 294,682 | 637,539 | 13 | 7.9 |
| `TLA-005/witness-write-error-strands-group` (`MC_CommitGroups_witness_write_error_strands_group.cfg`) | witness | violation Witness_WriteErrorStrandsGroup | violation Witness_WriteErrorStrandsGroup | 8,860 | 15,326 | 8 | 1.6 |
| `TLA-005/witness-read-window-group-moved` (`MC_CommitGroups_witness_read_window_group_moved.cfg`) | witness | violation Witness_ReadWindowGroupMoved | violation Witness_ReadWindowGroupMoved | 1,183 | 1,787 | 7 | 1.3 |

### TLA-006

Receipt `verification/receipts/TLA-006.json`, run on ab73296 (dirty tree: true).

| Check | Role | Expected | Verdict | Distinct states | Generated | Depth | Seconds |
|---|---|---|---|---|---|---|---|
| `TLA-006/baseline-small` (`MC_HandoffRetirement_small.cfg`) | baseline | pass | pass | 139,982 | 418,916 | 26 | 11.2 |
| `TLA-006/baseline-expanded` (`MC_HandoffRetirement_expanded.cfg`) | baseline | pass | pass | 1,811,810 | 5,907,626 | 32 | 128.2 |
| `TLA-006/liveness-small` (`MC_HandoffRetirement_liveness.cfg`) | baseline | pass | pass | 139,982 | 418,916 | 26 | 139.5 |
| `TLA-006/nc-write-error-keeps-staging` (`MC_HandoffRetirement_nc_write_error_keeps_staging.cfg`) | negative-control | violation SuccessIsDurable | violation SuccessIsDurable | 8,940 | 18,410 | 9 | 2.5 |
| `TLA-006/nc-reclaim-claimed` (`MC_HandoffRetirement_nc_reclaim_claimed.cfg`) | negative-control | violation SettledAtMostOnce | violation SettledAtMostOnce | 20,074 | 49,046 | 11 | 3.6 |
| `TLA-006/nc-admit-after-close` (`MC_HandoffRetirement_nc_admit_after_close.cfg`) | negative-control | violation RetiredEngineFrozen | violation RetiredEngineFrozen | 730 | 1,188 | 6 | 1.7 |
| `TLA-006/nc-admit-after-close-acked` (`MC_HandoffRetirement_nc_admit_after_close_acked.cfg`) | negative-control | violation LateSuccessWasClaimedLive | violation LateSuccessWasClaimedLive | 16,038 | 38,321 | 11 | 3.3 |
| `TLA-006/nc-attach-ignore-terminal` (`MC_HandoffRetirement_nc_attach_ignore_terminal.cfg`) | negative-control | violation LateSuccessWasClaimedLive | violation LateSuccessWasClaimedLive | 2,708 | 5,175 | 7 | 2.3 |
| `TLA-006/nc-no-storage-progress` (`MC_HandoffRetirement_nc_no_storage_progress.cfg`) | negative-control | violation AllIssuedSettle | violation (temporal) | 4,127 | 8,014 |  | 4.7 |
| `TLA-006/witness-late-durable-reply` (`MC_HandoffRetirement_witness_late_durable_reply.cfg`) | witness | violation Witness_LateDurableReply | violation Witness_LateDurableReply | 2,874 | 5,326 | 8 | 2.1 |
| `TLA-006/witness-late-durable-attach-reply` (`MC_HandoffRetirement_witness_late_durable_attach_reply.cfg`) | witness | violation Witness_LateDurableAttachReply | violation Witness_LateDurableAttachReply | 2,975 | 5,544 | 8 | 2.2 |
| `TLA-006/witness-unclaimed-durable-rejected` (`MC_HandoffRetirement_witness_unclaimed_durable_rejected.cfg`) | witness | violation Witness_UnclaimedDurableRejected | violation Witness_UnclaimedDurableRejected | 8,346 | 18,127 | 9 | 2.6 |
| `TLA-006/witness-non-durable-rejected` (`MC_HandoffRetirement_witness_non_durable_rejected.cfg`) | witness | violation Witness_NonDurableRejected | violation Witness_NonDurableRejected | 2,100 | 3,842 | 7 | 2.1 |
| `TLA-006/witness-cancelled-requester-settled` (`MC_HandoffRetirement_witness_cancelled_requester_settled.cfg`) | witness | violation Witness_CancelledRequesterSettled | violation Witness_CancelledRequesterSettled | 2,657 | 4,865 | 8 | 2.3 |
| `TLA-006/witness-late-write-lands` (`MC_HandoffRetirement_witness_late_write_lands.cfg`) | witness | violation Witness_LateWriteLands | violation Witness_LateWriteLands | 734 | 1,201 | 6 | 1.9 |
| `TLA-006/witness-abort-drops-replies` (`MC_HandoffRetirement_witness_abort_drops_replies.cfg`) | witness | violation Witness_AbortDropsReplies | violation Witness_AbortDropsReplies | 316 | 471 | 6 | 1.8 |
| `TLA-006/witness-pre-write-reject-then-success` (`MC_HandoffRetirement_witness_pre_write_reject_then_success.cfg`) | witness | violation Witness_PreWriteRejectThenSuccess | violation Witness_PreWriteRejectThenSuccess | 28,484 | 71,532 | 12 | 4.6 |
| `TLA-006/witness-write-error-strands-group` (`MC_HandoffRetirement_witness_write_error_strands_group.cfg`) | witness | violation Witness_WriteErrorStrandsGroup | violation Witness_WriteErrorStrandsGroup | 8,148 | 17,526 | 9 | 3.0 |

### TLA-011

Receipt `verification/receipts/TLA-011.json`, run on ab73296 (dirty tree: true).

| Check | Role | Expected | Verdict | Distinct states | Generated | Depth | Seconds |
|---|---|---|---|---|---|---|---|
| `TLA-011/baseline-small` (`MC_ServingOwnership_small.cfg`) | baseline | pass | pass | 228,406 | 854,392 | 30 | 13.5 |
| `TLA-011/baseline-expanded` (`MC_ServingOwnership_expanded.cfg`) | baseline | pass | pass | 21,322,712 | 108,036,075 | 37 | 2490.8 |
| `TLA-011/baseline-single` (`MC_ServingOwnership_single.cfg`) | baseline | pass | pass | 915,472 | 3,819,021 | 27 | 92.1 |
| `TLA-011/nc-ring-authorizes` (`MC_ServingOwnership_nc_ring_authorizes.cfg`) | negative-control | violation AckedDurable | violation AckedDurable | 1,171 | 3,801 | 9 | 1.8 |
| `TLA-011/nc-ring-grants-storage` (`MC_ServingOwnership_nc_ring_grants_storage.cfg`) | negative-control | violation AckedDurable | violation AckedDurable | 572,864 | 2,267,456 | 17 | 35.8 |
| `TLA-011/nc-serve-before-fence` (`MC_ServingOwnership_nc_serve_before_fence.cfg`) | negative-control | violation HigherEpochCoversAcks | violation HigherEpochCoversAcks | 22,356 | 65,236 | 12 | 3.3 |
| `TLA-011/nc-ring-skips-refresh` (`MC_ServingOwnership_nc_ring_skips_refresh.cfg`) | negative-control | violation HigherEpochCoversAcks | violation HigherEpochCoversAcks | 18,556 | 57,113 | 14 | 3.2 |
| `TLA-011/witness-takeover-installs` (`MC_ServingOwnership_witness_takeover_installs.cfg`) | witness | violation Witness_TakeoverInstalls | violation Witness_TakeoverInstalls | 1,045 | 3,371 | 9 | 2.0 |
| `TLA-011/witness-stale-write-fenced` (`MC_ServingOwnership_witness_stale_write_fenced.cfg`) | witness | violation Witness_StaleWriteFenced | violation Witness_StaleWriteFenced | 3,734 | 11,595 | 11 | 2.2 |
| `TLA-011/witness-late-durable-reply-after-takeover` (`MC_ServingOwnership_witness_late_durable_reply_after_takeover.cfg`) | witness | violation Witness_LateDurableReplyAfterTakeover | violation Witness_LateDurableReplyAfterTakeover | 22,344 | 71,118 | 14 | 3.1 |
| `TLA-011/witness-late-reply-after-override-move` (`MC_ServingOwnership_witness_late_reply_after_override_move.cfg`) | witness | violation Witness_LateDurableReplyAfterTakeover | violation Witness_LateDurableReplyAfterTakeover | 2,660 | 6,607 | 14 | 2.0 |
| `TLA-011/witness-duplicate-resolved-by-new-owner` (`MC_ServingOwnership_witness_duplicate_resolved_by_new_owner.cfg`) | witness | violation Witness_DuplicateResolvedByNewOwner | violation Witness_DuplicateResolvedByNewOwner | 30,312 | 97,276 | 14 | 4.0 |
| `TLA-011/witness-zombie-write-lands` (`MC_ServingOwnership_witness_zombie_write_lands.cfg`) | witness | violation Witness_ZombieWriteLands | violation Witness_ZombieWriteLands | 1,236 | 3,986 | 9 | 1.8 |
| `TLA-011/witness-abandoned-close-write-lands` (`MC_ServingOwnership_witness_abandoned_close_write_lands.cfg`) | witness | violation Witness_AbandonedCloseWriteLands | violation Witness_AbandonedCloseWriteLands | 6,061 | 18,913 | 11 | 2.1 |
| `TLA-011/witness-ambiguous-put-recovered` (`MC_ServingOwnership_witness_ambiguous_put_recovered.cfg`) | witness | violation Witness_AmbiguousPutRecovered | violation Witness_AmbiguousPutRecovered | 7,243 | 22,362 | 12 | 2.2 |
| `TLA-011/witness-crash-between-fence-and-serve` (`MC_ServingOwnership_witness_crash_between_fence_and_serve.cfg`) | witness | violation Witness_CrashBetweenFenceAndServe | violation Witness_CrashBetweenFenceAndServe | 81 | 235 | 5 | 1.5 |
| `TLA-011/witness-superseded-open-fails` (`MC_ServingOwnership_witness_superseded_open_fails.cfg`) | witness | violation Witness_SupersededOpenFails | violation Witness_SupersededOpenFails | 299 | 1,035 | 7 | 1.3 |
| `TLA-011/witness-single-both-nodes-serve` (`MC_ServingOwnership_witness_single_both_nodes_serve.cfg`) | witness | violation Witness_TakeoverInstalls | violation Witness_TakeoverInstalls | 695 | 2,606 | 8 | 1.6 |

## 8. Coverage

Coverage runs call TLC directly with `-coverage 1` (section 1) on the
unmodified models, with the constants of the named baseline. They ran on the
same model files as the receipts in section 7, and each completed its search.
TLC reports per-action counts and per-expression evaluation counts. Counts are
keyed by action and source range, taking the maximum over call sites, so an
operator body reached from two actions is listed under each.

Not rerun with coverage: TLA-005 requests, requests_faults and faults (their
actions are those of small, which left no expression at zero), TLA-006
liveness (the `Spec` and constants of small) and TLA-011 expanded (the
actions of small and single).

The changed steps fired: in TLA-005 small, `CommitWriteFailApplied` (9,757
distinct states), `CommitWriteFail` (19,478), `CommitPreWriteReject` (5,932)
and `DbCloseResult` (60,490); in TLA-006 small, `WriteFailApplied` (939),
`WriteFailPreApply` (926), `WriteRefused` (2,227), `PreWriteReject` (1,409)
and `DbCloseResult` (3,838); in TLA-011 single, the closed-Db branch of
`Commit` (ServingOwnership:207-210 under `Commit`). In TLA-011 small that
branch is never enabled, and in single mode `Move`, `Observe` and `Yield` are
disabled by the constants. `Quiescent` is the stuttering step of a settled
state, so it never produces a new state.

**TLA-005**

| Run | Distinct states | Expressions | Zero-count expressions | Actions with 0 distinct states |
|---|---|---|---|---|
| small | 1,510,380 | 711 | none | `Quiescent` |
| close | 1,085,300 | 657 | none | `Quiescent` |
| refusal_retry | 1,031,436 | 688 | none | `Quiescent` |

Union over the runs: 745 expressions; zero in every run: none. Actions that never produced a new state: `Quiescent`.

**TLA-006**

| Run | Distinct states | Expressions | Zero-count expressions | Actions with 0 distinct states |
|---|---|---|---|---|
| small | 139,982 | 566 | none | `Quiescent` |
| expanded | 1,811,810 | 580 | none | `Quiescent` |

Union over the runs: 581 expressions; zero in every run: none. Actions that never produced a new state: `Quiescent`.

**TLA-011**

| Run | Distinct states | Expressions | Zero-count expressions | Actions with 0 distinct states |
|---|---|---|---|---|
| small | 228,406 | 435 | `Commit` ServingOwnership:207:11-208:43; `Commit` ServingOwnership:210:11-210:75 | `Quiescent` |
| single | 915,472 | 465 | `Move` ServingOwnership:157:8-157:32; `Move` ServingOwnership:158:8-158:25; `Move` ServingOwnership:159:8-160:71; `Observe` ServingOwnership:165:8-165:43; `Observe` ServingOwnership:166:8-167:83; `Terminate` ServingOwnership:326:16-327:73; `Yield` ServingOwnership:131:11-133:75; `Yield` ServingOwnership:134:11-137:32; `Yield` ServingOwnership:207:11-208:43; `Yield` ServingOwnership:210:11-210:75; `Yield` ServingOwnership:312:8-313:65 | `Move`, `Observe`, `Yield`, `Quiescent` |

Union over the runs: 513 expressions; zero in every run: none. Actions that never produced a new state: `Quiescent`.

## 9. Findings

Each finding has a classification (roadmap §2.7) and a disposition. There is
no open production defect in this group.

**TLA-005-F1 -- No counterexample in the fixed model** (classification: none)

The six baselines explored their complete state spaces with no violation or
deadlock. Every shape includes the pinned post-apply read window. Each of the
eight controls violated exactly its target, and all 15 witnesses were reached.

Disposition: result. Mapped real-code tests are listed below.

**TLA-005-F2 -- `db.write` can return `Err` after the batch was applied** (classification: unjustified-assumption, in the first model draft)

The first draft assumed that a `write_with_options` error means the batch was
not applied. In pinned SlateDB (0717cc1), `DbInner::write_batch` appends the
batch to the WAL buffer and the memtable, advances `last_committed_seq`
(`track_recent_committed_write_batch`, batch_write.rs:290), and only then
evaluates `maybe_freeze_current_memtable()?` (batch_write.rs:306). If that
fails, the handler returns `Err`, the batch writer exits and the Db closes,
while the batch may still land. `CommitTransaction::write` answers `Internal`
(non-definitive) and publishes no mirrors. `Witness_FailedWriteRecovered` shows
that the next open can recover the batch, so the outcome is unknown and a
producer retry deduplicates it.

Disposition: the model was corrected (`CommitWriteFailApplied`,
`WriteFailApplied`) and ASM-DURABILITY-7 records the contract. The F5
regression test reaches this error through SlateDB's `write-batch-post-commit`
failpoint and checks that the original is answered `Internal`. Recovery of the
batch after a reopen is shown by the model only.

Code: `src/shard/transaction/finalize.rs:170-206`, `src/application/append/contract.rs:276`, `src/application/append/contract.rs:285-297`.

**TLA-005-F3 -- An empty-entry producer Accept is classified as a no-write transaction** (classification: none; source-inspection note, latent)

This is not a TLC result. For an Accept with no entries, `accept_append`
stages the producer row and returns its success before any record is written.
`has_writes()` counts tail changes, records, touches and `extra_writes`, but
not producer-row puts. The batch is dropped, and the success is released as a
no-write verdict, which still waits on the prior barrier. The only production
caller found that stages such a request is stream creation with a JSON `[]`
initial body (`json_entries(body, true)` plus the synthetic init producer). No
customer-visible effect was found: there are no records to deduplicate, and a
replayed create re-sends the same empty append. Public appends cannot reach
this path, because `content.rs` defers an empty body as `BadBody` when a
producer is present. The model's C request carries no producer.

Disposition: kept as a recorded observation. Optional hardening, owner
decision: a unit test in `src/shard/transaction_tests.rs` that pins one of two
behaviours. Either an empty-entry producer append persists its producer row,
or it is refused before staging.

Code: `src/shard/transaction/finalize.rs:35-40`, `src/shard/transaction/append.rs:152-187`, `src/application/creation/initialization.rs:63`, `src/application/creation/initialization.rs:120-125`, `src/application/creation.rs:248-256`, `src/application/append/content.rs:64-70`.

**TLA-005-F4 -- Replies sent outside `DurableEffects`; D5 holds only error-for-error; `SealSuperseded` is an open D4/D10 question** (classification: none; scope note)

`CommitTransaction` sends some replies directly instead of staging them.
1. `Internal` on a handle, producer, sequence or seal-fence load failure, and
   on a billing-row read failure. It is non-definitive and changes no state.
   The model includes the producer-load case
   (`Witness_ProducerLoadFailsInternal`).
2. Deferred `CtMismatch` and `BadBody`, sent only after `decide_producer`
   returned Accept (append.rs:55-70, 96-102). Accept also depends on checks
   against the applied producer row, which can run ahead of durable state. If
   the group it depends on fails, durable state might have answered another
   error. The outcome is an error either way, so DST D5 ("no response may rely
   on state from that group") is met only error-for-error.
3. `SealSuperseded` (append.rs:139-142; maintenance.rs:112-127, 184-188) is
   `FailureClass::Conflict`, so it is definitively rejected. It is decided from
   the engine's seal-fence cache (maintenance.rs:89-110). `fence` raises that
   cache while staging, before its row is durable (maintenance.rs:136-167), and
   the refusal is sent without a barrier. That breaks the letter of DST
   D4/D10. Since commit 234f69a the cache is reloaded from a durable row by a
   fresh engine, but the staging-time raise is unchanged.

None of these replies is a success, a duplicate or a closed/reused refusal
about durable stream state, so D1-D4 as modelled are unaffected.

Disposition: open questions handed on. TLA-002/003 own the `SealSuperseded`
barrier question. The D5 error-for-error reading needs a spec-owner decision.

**TLA-005-F5 -- After a post-apply `db.write` error, the next group staged from the failed batch** (classification: production-defect; **fixed**)

On the pre-fix code, the small shape violated `D2_DuplicateAckDurable` in 8
states and the refusal-retry shape violated `D4_RefusalDurable` in 7 states.
The duplicate schedule:
1. A is issued. Its client deadline passes while A is still queued.
2. The client sends the retry R, which is queued behind A.
3. The committer takes group {A}. `db.write_with_options` applies the batch
   and makes it visible to default reads (batch_write.rs:262-290). Then
   `maybe_freeze_current_memtable()?` fails (:306; `InvalidDBState`, a panic,
   or the `write-batch-post-commit` failpoint). The handler sends `Err` to the
   committer (:134). `run_lifecycle` writes `closed_result` only later
   (dispatcher.rs:358). A is answered `Internal`.
4. Nothing stopped the committer. It took {R}. A was never published, so the
   producer row was loaded with a Memory-level `db.get` and returned A's row.
   `decide_producer` answered a duplicate.
5. R's group had no writes. `attach` found an open, empty handoff and answered
   Durable, so R's duplicate success was sent.
6. A was not durable and might never land: an acknowledged duplicate of an
   operation that does not exist.
With Y (another hash) in place of R, the same window gave a
`ProducerSeqReused` that no durable state justified. The window exists for a
post-apply `InvalidDBState` or panic. It does not exist for a dead WAL
flusher, whose `run_lifecycle` writes `closed_result` before its buffer
reports closed.

Disposition: fixed by commit f574d73, "A failed commit write retires its
engine before any later group can stage from it". `write_failed`
(finalize.rs:199-206) calls `begin_close` before it answers the failed group,
so `run` answers every later group `Moved` and `attach` returns `Retired`.
- Regression:
  `shard::retirement_tests::tla005_f5_a_failed_write_answers_nothing_from_its_batch`.
  It drives the real committer with SlateDB's `write-batch-post-commit`
  failpoint, holds the batch writer's exit window open, and checks both the
  duplicate and the reused-sequence form.
- Model: the small and refusal-retry baselines contain both schedules and
  pass. `TLA-005/nc-write-error-keeps-staging` (D2, 8 states) and
  `TLA-005/nc-write-error-keeps-staging-refusal` (D4, 7 states) reproduce the
  pre-fix behaviour. `TLA-006/nc-write-error-keeps-staging` shows the same
  hazard as a `SuccessIsDurable` violation. `Witness_ReadWindowGroupMoved`
  shows that the window is reached and the group is answered `Moved`.
- Consequences of the fix, modelled: an earlier applied-but-not-durable group
  is answered `Moved` (non-definitive) instead of possibly settling durable
  (`Witness_WriteErrorStrandsGroup` in TLA-005 and TLA-006). The engine's own
  `Db::close` can record a clean close, and its flush may make the failed batch
  durable, which the `Internal` reply allows (`WalLand` stays enabled after the
  error; `Witness_FailedWriteRecovered`).
- Out of scope, recorded: an evicted handle reloaded by a reader outside the
  committer still seeds its mirrors from a Memory-level read. After the fix
  this can only happen on the retired engine (ASM-DURABILITY-9; TLA-018).

Code: `src/shard/transaction/finalize.rs:170-206`, `src/shard/transaction/mod.rs:50-57`, `src/shard/transaction/append.rs:27-70`, `src/shard.rs:1876-1934`, `src/shard.rs:2088-2100`, `src/shard.rs:2467-2566`, `src/shard/commit_handoff.rs:39-52`, `slatedb@0717cc1:slatedb/src/batch_write.rs:121-135`, `slatedb@0717cc1:slatedb/src/batch_write.rs:262-332`, `slatedb@0717cc1:slatedb/src/dispatcher.rs:337-358`, `slatedb@0717cc1:slatedb/src/wal_buffer.rs:562-570`.
Traces: `evidence/TLA-005_pre-fix_read-window.trace.txt` and `evidence/TLA-005_pre-fix_read-window-refusal.trace.txt` (pre-fix model), `evidence/TLA-005_nc-write-error-keeps-staging.trace.txt`, `evidence/TLA-005_nc-write-error-keeps-staging-refusal.trace.txt`, `evidence/TLA-006_nc-write-error-keeps-staging.trace.txt` (current model).

**TLA-005-F6 -- The DST group-failure injection does not go through `write_failed`** (classification: none; test-mapping note)

`fail_next_group_for` arms `failpoint_tripped` (finalize.rs:20-24,
`cfg(test)`), which rejects a group before the write and does not retire the
engine. The mapped `durability_failures` tests therefore exercise the
non-retiring path, which the models call `CommitPreWriteReject` and
`PreWriteReject`. The retirement in `write_failed` is exercised by the F5
regression only.

Disposition: recorded. No change is implied.

Existing real-code tests mapped to TLA-005: `dst::dst_tests::durability_fences::idempotent_successes_wait_for_durability`, `dst::dst_tests::durability_fences::state_dependent_conflicts_wait_for_durability`, `dst::dst_tests::durability_fences::a_fence_waits_for_durability_before_reporting_closed`, `dst::dst_tests::durability_failures::a_failed_group_write_fails_its_duplicate_too`, `dst::dst_tests::durability_failures::a_reuse_verdict_dies_with_its_failed_group`, `dst::dst_tests::durability_failures::a_failed_group_fails_the_close_and_its_retry_together`, `shard::retirement_tests::r17b_live_duplicate_waits_for_actual_remote_durability`, `shard::retirement_tests::r17b_retirement_after_duplicate_attachment_owns_every_reply`, `dst::dst_tests::producer_handoff::ambiguous_commit_survives_handoff_and_dedupes`, and the F5 regression above.

**TLA-006-F1 -- No baseline counterexample (safety small and expanded, liveness small)** (classification: none)

The explorations completed with no violation or deadlock. They include the
write-error retirement and the post-apply read window. The legitimate late
durable response is reachable (`Witness_LateDurableReply`,
`Witness_LateDurableAttachReply`). So is the rejection of groups that were
unclaimed-durable or merely applied. The safety controls show that each of
these is necessary: the terminal publication guard, `retire()` owning only
unclaimed groups, `attach` honouring terminal, `take_durable` honouring
terminal, and the retirement after a write error.

Disposition: result.

**TLA-006-F2 -- Settlement liveness depends on WAL progress** (classification: none; recorded dependency)

The assumption-removal control shows that `AllIssuedSettle` fails when a
written WAL batch may never land while the Db stays open: registered groups
are then never settled server-side. The engine's wedge signals shed new
appends with a retryable 429 but do not settle the waiting ones, which end at
the client deadline as outcome unknown. That matches the product contract (no
false success), but the liveness claim is conditional on ASM-SLATEDB-DURABLE
(d).

Disposition: recorded. The existing held-WAL tests already hold the WAL.

Code: `src/shard.rs:1940-1981`, `src/application/append/submit.rs:69-93`.
Trace: `evidence/TLA-006_nc-no-storage-progress.trace.txt`.

**TLA-006-F3 -- The first version's `LateSuccessWasClaimedLive` was vacuous** (classification: specification-defect, in the model; fixed)

In the first version, the ghost behind `LateSuccessWasClaimedLive` was set by
the same two steps that produce every "ok", and neither read `terminal`. The
review's mutant M1, an attach that ignores terminal, passed every retirement
invariant.

Disposition: fixed in the model. The ghost is now `decidedLive`, the
`~ho.terminal` read at each decision step, and `AttachOp` and `TakeOp` are
mutation points. `nc_attach_ignore_terminal` (M1) and
`nc_admit_after_close_acked` violate `LateSuccessWasClaimedLive`.

Code: `src/shard/commit_handoff.rs:39-60`, `src/shard/transaction/finalize.rs:45-60`, `src/shard.rs:3006-3011`.
Trace: `evidence/TLA-006_nc-attach-ignore-terminal.trace.txt`.

Existing real-code tests mapped to TLA-006: `shard::commit_handoff::loom_tests::quality_loom_held_wal_retry_cannot_turn_retirement_into_durability`, `shard::commit_handoff::loom_tests::quality_loom_publication_and_durable_claim_have_one_terminal_owner`, `shard::commit_handoff::loom_tests::quality_handoff_attachment_moves_each_reply_once`, `shard::retirement_tests::r17b_durable_dispatch_claim_before_retirement_keeps_its_completion`, `shard::retirement_tests::r17b_late_successful_write_settles_without_publishing_retired_effects`, `shard::retirement_tests::r17b_retirement_before_duplicate_attachment_cannot_erase_remote_dependency`, and the F5 regression.

**TLA-011-F1 -- No baseline counterexample in fleet mode (small, expanded) or single mode** (classification: none; conditional design check)

The explorations completed with no violation or deadlock. Takeover by an
override move, stale-writer fencing, late durable replies from an old owner
(with and without crashes), duplicate resolution by a new owner, delayed PUTs
from a crashed process and from an abandoned close, an ambiguous conditional
PUT recovered later, a move interrupted by a crash, a superseded open, and
both nodes serving in no-ring mode are all reachable. Four controls show that
each of these breaks a named property: acknowledging without durability
evidence, letting ring preference replace the manifest refresh, letting ring
preference stand in for WAL put-if-absent, and serving before the fence WAL.

Disposition: result, conditional on ASM-SLATEDB-FENCE and the unestablished
ASM-OBJSTORE-CAS.

**TLA-011-F2 -- Bootstrapping and no-ring serving rest entirely on SlateDB fencing** (classification: none; recorded dependency)

A restarted process has an empty ownership view until its first fleet tick,
so `effective_owner()` is None and the process opens and fences any prefix a
stale router sends it. The model reaches exactly this, and all safety
properties hold, but only through ASM-SLATEDB-FENCE. `nc_ring_grants_storage`
and `nc_ring_skips_refresh` show what breaks if storage or the opener defers to
ring preference. The cost of the design is availability, not safety.

Disposition: recorded. A readiness gate on the first ring view would be a
design change for the owner to decide.

Code: `src/ownership.rs:76-99`, `src/fleet.rs:747-843`, `src/shard_directory.rs:238-296`.
Trace: `evidence/TLA-011_nc-ring-grants-storage.trace.txt`.

**TLA-011-F3 -- DST T11 contradicts the roadmap §1.5 late-durable-response boundary** (classification: specification-defect, in the requirement text)

`Witness_LateDurableReplyAfterTakeover` reaches a state where an engine of a
lower writer epoch sends a success while an engine of a higher epoch already
serves. This happens after an override move with no crash, and after a
crash-bootstrap. That is the designed behaviour: roadmap §1.5 and TLA-006
permit an old engine to finish responding for work claimed durable before
retirement. DST T11 ("exactly one owner epoch may acknowledge") forbids it.
The model checks the refined property instead (`HigherEpochCoversAcks` and
`AckedDurable`).

Disposition: open, owner decision. Proposed rewording for T11 in
`docs/dst/DST-EXPANSION-SPEC.md`: "At most one owner epoch may acknowledge
newly authorized writes; a retired epoch may complete responses for writes it
claimed durable before a newer epoch fenced it."

Code: `src/shard.rs:3006-3011`, `src/shard/commit_handoff.rs:1-13`.

Existing real-code tests mapped to TLA-011: `dst::dst_tests::producer_handoff::producer_state_survives_a_handoff_and_suppresses_duplicates`, `dst::dst_tests::producer_handoff::ambiguous_commit_survives_handoff_and_dedupes`, `history::tests::absorber_exits_when_shard_engine_is_fenced`, `shard_directory::directory_tests::foreign_shard_is_refused_with_its_owner_and_never_opened`, `dst::dst_tests::runtime_retirement::retirement_arms_the_holdoff_and_a_stale_close_cannot_evict_a_replacement`.

## 10. Review disposition

The adversarial review of the first model version raised one blocker, four
major and eleven minor issues. The review text ended at the start of its
`defect_assessments` list, so no item from that list could be addressed.

| # | Severity | Issue | Disposition |
|---|---|---|---|
| 1 | blocker | TLA-006 `LateSuccessWasClaimedLive` was a tautology | Fixed in the model (TLA-006-F3) |
| 2 | major | TLA-005 did not model the idempotent close, so D3 was implied by D1/D2 | Fixed: request C, the close shape, two witnesses and `nc_idem_close_unbarriered` |
| 3 | major | An unstated assumption that staging reads equal the applied mirrors | Modelled; it found TLA-005-F5, now fixed in the code. The read window is in every shape, and ASM-DURABILITY-8 states what the fixed code relies on |
| 4 | major | TLA-011 rested on the unestablished ASM-OBJSTORE-CAS without saying so; the ambiguous conditional PUT was missing | Fixed: labelled a conditional design check here, in the manifest `input_scope` and in the ledger. `Land` has the `AmbiguousPut` outcome, with `Witness_AmbiguousPutRecovered` |
| 5 | major | The checks were not registered in the driver or the ledger | Fixed: the obligations are in `verification/manifest.json`, the entries in `verification/assumptions.md`, and receipts in `verification/receipts/` |
| 6 | minor | D10 could not fail | Fixed: S and R re-send refused operations; `nc_failed_release_d10` |
| 7 | minor | The F4 justification was incomplete | Fixed by rewording TLA-005-F4 |
| 8 | minor | `nc_ring_authorizes` was a premature-ack control | Relabelled a durability-evidence control; `nc_ring_skips_refresh` added |
| 9 | minor | `nc_admit_after_close` targets a guard-restating ghost | Documented as such; `nc_admit_after_close_acked` added |
| 10 | minor | Coverage was only per action, and one TLA-011 branch was at zero | Expression-level coverage in section 8 |
| 11 | minor | Witnesses were weaker than their names; `AckAfterVisibility` had no control | Witnesses strengthened; `nc_reply_before_visible` added |
| 12 | minor | TLA-006 `WriteFail` fired only on a closed Db | Split into `WriteRefused`, `WriteFailPreApply`, `WriteFailApplied` and `PreWriteReject` |
| 13 | minor | The `PubVisibleDurable` claim was broader than the model's visibility sources | Scoped to the mirrors set by dispatch; ASM-DURABILITY-9 records the reload seeding |
| 14 | minor | Some invariants and assumptions were restated by construction; ASM-SLATEDB-DURABLE (c) was too strong | Described as structural or guard-restating; (c) reworded; `Restart` recovers any written prefix |
| 15 | minor | The requests shape never combined the reused refusal with write faults or retirement | `requests_faults` shape added |
| 16 | minor | `Terminate` forced a successful close flush; delayed PUTs existed only for crashes | `Terminate` may abandon a close with a PUT in flight; `Witness_AbandonedCloseWriteLands` |

## 11. Files

- Models: `CommitHandoff.tla` (shared), `CommitGroups.tla` (TLA-005),
  `HandoffRetirement.tla` (TLA-006), `ServingOwnership.tla` (TLA-011).
- Model-checking roots: `MC_<Model>.tla` for baselines and witnesses, and one
  `MC_<Model>_nc_<name>.tla` per mutant. Each extends the unmodified model.
- Configurations: one `.cfg` per check, named in `verification/manifest.json`.
- `evidence/`: trimmed TLC counterexamples. Each header records the command,
  the input hashes and the verdict. The negative-control traces are from the
  driver run in section 7. `TLA-005_pre-fix_read-window*.trace.txt` are the
  TLA-005-F5 counterexamples of the pre-fix model.
