# Seal group: TLA-001, TLA-002, TLA-003

These models implement three seal obligations of
[the formal-verification roadmap](../../../docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md).
The manifest entries, checks and bounds are in
[`verification/manifest.json`](../../manifest.json), the assumptions in
[`verification/assumptions.md`](../../assumptions.md), and the last complete
runs in `verification/receipts/TLA-00N.json`.

The models describe the code after three fixes that the models found:
"A seal takeover's fence outlives the engine that recorded it" (TLA-002-F1),
"A raw close that takes over an abandoned final claim writes its own record"
(TLA-003-F2) and "A product seal refuses an over-ceiling final before it
publishes its intent" (TLA-003-F3). Each fix's earlier behaviour is now a
negative control.

## Status

| ID | Title | Status | Result |
|---|---|---|---|
| TLA-001 | Registry CAS, attempt-local outcomes, and incarnation fencing | pass-with-recorded-scope | Both baselines pass. Each negative control fails on its property. Every witness is reached. |
| TLA-002 | Seal claims, renewal, and competing takeover reservations | counterexample | Every baseline passes, including engine replacement, crash failover and the liveness shapes. F1 is fixed. **F2** (new) is open: a `SealSuperseded` refusal can precede the durability of the fence that caused it, and an older generation of the same operation can then close the segment with no claim standing. |
| TLA-003 | Final-record sealing, ambiguous append outcomes, and owed debt | counterexample | Every baseline passes. The two shapes with configuration skew between instances exclude the properties F4 or F5 violate. F2 and F3 are fixed. **F4** and **F5** are open and need an owner decision. |

## How to run

From the repository root:

```bash
python3 scripts/quality/formal.py check                      # manifest, files, config/property agreement
python3 scripts/quality/formal.py run --id TLA-001           # every check of one obligation
python3 scripts/quality/formal.py run --id TLA-002 --role witness
python3 scripts/quality/formal.py run --id TLA-001 --id TLA-002 --id TLA-003 --record
```

The driver runs TLC 2.19 with two workers, deadlock checking on, and each
check's `timeout_seconds` (default 1800). A timeout or an unfinished search is
never a pass. `--record` writes a receipt only when every check of the
obligation matches its expected verdict. A `known-defect` check matches when it
still violates its named property.

## Results

The receipts were recorded on `ab73296` with uncommitted changes: the Kani
proof modules committed ahead of this work, the TLA-003-F3 fix, and these
verification files. Line numbers in this README refer to that tree.

### TLA-001

Receipt `verification/receipts/TLA-001.json`: run on `ab73296773bc` plus uncommitted changes, TLC 2.19, Java 17.0.1. Seconds are wall time on a shared machine whose load varied.

| Check | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|
| `baseline-small` | baseline | pass | pass | 104,025 | 5.4 |
| `baseline-expanded` | baseline | pass | pass | 7,153,286 | 315.9 |
| `nc-leaked-outcome` | negative-control | violation:UniqueAllocation | violation:UniqueAllocation | 2,678 | 1.4 |
| `nc-dropped-incarnation-check` | negative-control | violation:IncarnationFenced | violation:IncarnationFenced | 5,404 | 1.4 |
| `nc-missing-etag-overwrite` | negative-control | violation:AllocatorCountsWrites | violation:AllocatorCountsWrites | 977 | 1.2 |
| `witness-LoserRetriesThenApplies` | witness | violation:Witness_LoserRetriesThenApplies | violation:Witness_LoserRetriesThenApplies | 2,690 | 1.4 |
| `witness-AmbiguousAfterWrite` | witness | violation:Witness_AmbiguousAfterWrite | violation:Witness_AmbiguousAfterWrite | 71 | 1.0 |
| `witness-StaleMutatorFenced` | witness | violation:Witness_StaleMutatorFenced | violation:Witness_StaleMutatorFenced | 3,109 | 1.3 |
| `witness-MissingTokenRefused` | witness | violation:Witness_MissingTokenRefused | violation:Witness_MissingTokenRefused | 16 | 1.1 |
| `witness-Recreated` | witness | violation:Witness_Recreated | violation:Witness_Recreated | 937 | 1.2 |
| `witness-ConflictExhausted` | witness | violation:Witness_ConflictExhausted | violation:Witness_ConflictExhausted | 11,977 | 1.6 |
| `witness-DeclinedOnTombstone` | witness | violation:Witness_DeclinedOnTombstone | violation:Witness_DeclinedOnTombstone | 390 | 1.2 |
| `witness-ReplacementAllocatesWhileStaleFenced` | witness | violation:Witness_ReplacementAllocatesWhileStaleFenced | violation:Witness_ReplacementAllocatesWhileStaleFenced | 13,538 | 2.0 |
| `witness-AmbiguousRecreateRetried` | witness | violation:Witness_AmbiguousRecreateRetried | violation:Witness_AmbiguousRecreateRetried | 3,128 | 1.3 |

### TLA-002

Receipt `verification/receipts/TLA-002.json`: run on `ab73296773bc` plus uncommitted changes, TLC 2.19, Java 17.0.1. Seconds are wall time on a shared machine whose load varied.

| Check | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|
| `baseline-small` | baseline | pass | pass | 895,724 | 64.0 |
| `baseline-faults` | baseline | pass | pass | 3,875,480 | 246.1 |
| `baseline-expanded` | baseline | pass | pass | 2,306,562 | 158.3 |
| `baseline-xproc` | baseline | pass | pass | 523,650 | 41.0 |
| `baseline-retire` | baseline | pass | pass | 5,811,402 | 419.4 |
| `baseline-engine` | baseline | pass | pass | 3,773,720 | 310.7 |
| `baseline-liveness-small` | baseline | pass | pass | 3,394 | 1.9 |
| `baseline-liveness-contenders` | baseline | pass | pass | 1,125,110 | 175.5 |
| `baseline-liveness-faults` | baseline | pass | pass | 15,713 | 2.9 |
| `baseline-xproc-held` | baseline | pass | pass | 1,094,008 | 56.1 |
| `baseline-liveness-held` | baseline | pass | pass | 6,024 | 1.8 |
| `nc-no-newest-reservation` | negative-control | violation:NewestInstall | violation:NewestInstall | 17,953 | 1.7 |
| `nc-no-newest-reservation-live-fenced` | negative-control | violation:LiveClaimNeverFenced | violation:LiveClaimNeverFenced | 37,512 | 2.3 |
| `nc-install-before-fence-durable` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 5,671 | 1.2 |
| `nc-install-before-fence-inqueue` | negative-control | violation:QueuedFinalDecidedBeforeReplacement | violation:QueuedFinalDecidedBeforeReplacement | 3,526 | 1.3 |
| `nc-engine-resident-fence-xproc` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 21,937 | 2.0 |
| `nc-engine-resident-fence-retire` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 26,738 | 2.1 |
| `nc-engine-resident-fence-release` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 400,103 | 16.3 |
| `nc-engine-resident-fence-false-success` | negative-control | violation:SuccessProvesOutcome | violation:SuccessProvesOutcome | 2,856,637 | 123.6 |
| `witness-TakeoverInstalls` | witness | violation:Witness_TakeoverInstalls | violation:Witness_TakeoverInstalls | 2,379 | 1.2 |
| `witness-CompetingReservations` | witness | violation:Witness_CompetingReservations | violation:Witness_CompetingReservations | 2,337 | 1.1 |
| `witness-LowerReservationRestarts` | witness | violation:Witness_LowerReservationRestarts | violation:Witness_LowerReservationRestarts | 8,400 | 1.4 |
| `witness-OldFinalSealedOnBehalf` | witness | violation:Witness_OldFinalSealedOnBehalf | violation:Witness_OldFinalSealedOnBehalf | 35,006 | 2.5 |
| `witness-StaleClaimSuperseded` | witness | violation:Witness_StaleClaimSuperseded | violation:Witness_StaleClaimSuperseded | 5,184 | 1.4 |
| `witness-ExactRenewalAfterReservation` | witness | violation:Witness_ExactRenewalAfterReservation | violation:Witness_ExactRenewalAfterReservation | 1,442 | 1.1 |
| `witness-FinalSealCompletes` | witness | violation:Witness_FinalSealCompletes | violation:Witness_FinalSealCompletes | 3,520 | 1.3 |
| `witness-PlainSealCompletes` | witness | violation:Witness_PlainSealCompletes | violation:Witness_PlainSealCompletes | 475 | 0.9 |
| `witness-EngineRetiredMidFlight` | witness | violation:Witness_EngineRetiredMidFlight | violation:Witness_EngineRetiredMidFlight | 1,143 | 1.1 |
| `witness-CommittedButAnsweredMoved` | witness | violation:Witness_CommittedButAnsweredMoved | violation:Witness_CommittedButAnsweredMoved | 473 | 0.9 |
| `witness-OwnershipMovedToOtherProcess` | witness | violation:Witness_OwnershipMovedToOtherProcess | violation:Witness_OwnershipMovedToOtherProcess | 16 | 1.0 |
| `witness-NotOwnerRedirect` | witness | violation:Witness_NotOwnerRedirect | violation:Witness_NotOwnerRedirect | 320 | 0.9 |
| `witness-StaleFinalRefusedAfterReplacement` | witness | violation:Witness_StaleFinalRefusedAfterReplacement | violation:Witness_StaleFinalRefusedAfterReplacement | 13,772 | 1.8 |
| `witness-FenceUnverifiedRetainsClaim` | witness | violation:Witness_FenceUnverifiedRetainsClaim | violation:Witness_FenceUnverifiedRetainsClaim | 639 | 1.1 |
| `known-defect-F2-refusal-before-fence-durable` | known-defect | violation:ClosureAuthorized | violation:ClosureAuthorized | 128,919 | 5.7 |
| `known-defect-F2-release-after-durable-final` | known-defect | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 289,272 | 10.2 |

### TLA-003

Receipt `verification/receipts/TLA-003.json`: run on `ab73296773bc` plus uncommitted changes, TLC 2.19, Java 17.0.1. Seconds are wall time on a shared machine whose load varied.

| Check | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|
| `baseline-lanes` | baseline | pass | pass | 661,936 | 70.6 |
| `baseline-expanded` | baseline | pass | pass | 1,631,963 | 251.3 |
| `baseline-lanes-engine` | baseline | pass | pass | 1,694,525 | 600.3 |
| `baseline-renewal` | baseline | pass | pass | 4,966,567 | 1012.1 |
| `baseline-renewal-product` | baseline | pass | pass | 3,483,721 | 667.7 |
| `baseline-product-lanes` | baseline | pass | pass | 702,029 | 130.9 |
| `baseline-product-release` | baseline | pass | pass | 17,197 | 4.9 |
| `baseline-shared-lane` | baseline | pass | pass | 3,676,695 | 472.9 |
| `baseline-validation` | baseline | pass | pass | 10,676 | 3.4 |
| `baseline-validation-capacity` | baseline | pass | pass | 1,603 | 2.4 |
| `baseline-validation-ceiling` | baseline | pass | pass | 64,862 | 16.1 |
| `baseline-validation-product` | baseline | pass | pass | 13,166 | 8.0 |
| `nc-gap-definitive` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 448 | 3.1 |
| `nc-moved-definitive` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 730 | 3.4 |
| `nc-release-by-operation-only` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 2,413 | 4.8 |
| `nc-duplicate-completes-final` | negative-control | violation:SealedFinalHasItsRecord | violation:SealedFinalHasItsRecord | 206,124 | 51.0 |
| `nc-admission-refusal-release` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 41,916 | 14.1 |
| `nc-admission-refusal-false-closed` | negative-control | violation:FinalClosedTruthful | violation:FinalClosedTruthful | 35,585 | 11.8 |
| `nc-admission-refusal-orphaned` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 268,091 | 44.3 |
| `nc-admission-refusal-plain-over-final` | negative-control | violation:PlainCannotCompleteOwedFinal | violation:PlainCannotCompleteOwedFinal | 1,408,244 | 363.8 |
| `nc-admission-refusal-without-lapse` | negative-control | violation:FinalClosedTruthful | violation:FinalClosedTruthful | 2,538 | 5.2 |
| `witness-InvalidRefusedBeforeIntent` | witness | violation:Witness_InvalidRefusedBeforeIntent | violation:Witness_InvalidRefusedBeforeIntent | 29 | 2.7 |
| `witness-CommitAfterCancel` | witness | violation:Witness_CommitAfterCancel | violation:Witness_CommitAfterCancel | 10,712 | 6.2 |
| `witness-LostReplyThenRetrySucceeds` | witness | violation:Witness_LostReplyThenRetrySucceeds | violation:Witness_LostReplyThenRetrySucceeds | 324,571 | 80.8 |
| `witness-GapRetainsClaim` | witness | violation:Witness_GapRetainsClaim | violation:Witness_GapRetainsClaim | 613 | 2.2 |
| `witness-NonClosingDuplicateReleased` | witness | violation:Witness_NonClosingDuplicateReleased | violation:Witness_NonClosingDuplicateReleased | 73,128 | 14.7 |
| `witness-FinalSealCompletes` | witness | violation:Witness_FinalSealCompletes | violation:Witness_FinalSealCompletes | 72,819 | 12.5 |
| `witness-PlainSealCompletes` | witness | violation:Witness_PlainSealCompletes | violation:Witness_PlainSealCompletes | 544 | 2.2 |
| `witness-DefinitiveRelease` | witness | violation:Witness_DefinitiveRelease | violation:Witness_DefinitiveRelease | 20,474 | 6.2 |
| `witness-TakeoverInstalls` | witness | violation:Witness_TakeoverInstalls | violation:Witness_TakeoverInstalls | 6,840 | 3.6 |
| `witness-SeqReusedReleased` | witness | violation:Witness_SeqReusedReleased | violation:Witness_SeqReusedReleased | 12,756 | 4.7 |
| `witness-SharedLaneDuplicate` | witness | violation:Witness_SharedLaneDuplicate | violation:Witness_SharedLaneDuplicate | 49,558 | 8.2 |
| `witness-RawMovedRetainsClaim` | witness | violation:Witness_RawMovedRetainsClaim | violation:Witness_RawMovedRetainsClaim | 76 | 1.8 |
| `witness-NotOwnerRedirect` | witness | violation:Witness_NotOwnerRedirect | violation:Witness_NotOwnerRedirect | 212 | 1.7 |
| `witness-RawTakeoverWritesItsRecord` | witness | violation:Witness_RawTakeoverWritesItsRecord | violation:Witness_RawTakeoverWritesItsRecord | 159,276 | 27.8 |
| `witness-RetryAfterMarkRunsItsSeal` | witness | violation:Witness_RetryAfterMarkRunsItsSeal | violation:Witness_RetryAfterMarkRunsItsSeal | 175,639 | 44.9 |
| `witness-OrphanedCloseHealed` | witness | violation:Witness_OrphanedCloseHealed | violation:Witness_OrphanedCloseHealed | 91,678 | 19.6 |
| `nc-ceiling-after-intent-intent` | negative-control | violation:IntentOnlyAfterValidation | violation:IntentOnlyAfterValidation | 26 | 2.7 |
| `nc-ceiling-after-intent-closed-without-claim` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 7,830 | 5.2 |
| `known-defect-F4-invalid-retry-renews` | known-defect | violation:IntentOnlyAfterValidation | violation:IntentOnlyAfterValidation | 252 | 3.0 |
| `known-defect-F5-release-while-deliverable` | known-defect | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 2,014 | 3.6 |
| `known-defect-F5-closed-without-claim` | known-defect | violation:ClosureAuthorized | violation:ClosureAuthorized | 6,997 | 4.6 |
| `known-defect-F5-plain-over-final` | known-defect | violation:PlainCannotCompleteOwedFinal | violation:PlainCannotCompleteOwedFinal | 24,011 | 8.0 |

## Findings

### TLA-002-F1 (fixed): the seal fence did not survive an engine replacement

- **Defect.** The fence a takeover raises lived only in the engine's
  `seal_fences` map. A claim-authorized final that had passed its claim check
  but not yet entered a queue met a new engine with fence 0 after an engine
  replacement or a cross-instance crash failover. It then committed below the
  takeover's fence.
- **Consequences before the fix.** A segment closed under a superseded
  generation (`ClosureAuthorized`). Owed debt dropped after the record was
  durable (`ReleaseOnlyWhenUndeliverable`). A 200 `sealed` answer for a
  seal-with-final whose record was never written (`SuccessProvesOutcome`).
- **Fix.** Commit "A seal takeover's fence outlives the engine that recorded
  it". Each segment has a durable fence row. Every fence writes
  `max(cached, requested)` in its commit group, so its reply waits for the
  row. A new engine reads the row on a cache miss; a read error answers
  `Internal`, which never releases a claim.
- **Model.** `PersistFence` writes `seg.fence`; `OpenedEngineFence` reads it.
  `MaxFenceReadFail` lets any fence consult answer `Internal`.
- **Checks.** `baseline-xproc`, `baseline-retire` and `baseline-engine` pass
  every property. The witnesses `StaleFinalRefusedAfterReplacement` (a
  stalled final refused by a fence an earlier engine wrote) and
  `FenceUnverifiedRetainsClaim` are reached.
- **Pre-fix behaviour.** The negative controls `nc-engine-resident-fence-*`
  substitute fence 0 for a new engine and fail on `ClosureAuthorized`,
  `ReleaseOnlyWhenUndeliverable` and `SuccessProvesOutcome`.
- **Regressions.** `dst::dst_tests::seal_fencing::a_fence_survives_engine_replacement`
  and `shard::durability_frontier_tests::a_seal_fence_survives_engine_replacement`.
  The pre-fix traces are `evidence/TLA-002-F1_pre-fix_*`.
- **F1(e), a weakness seen only under F1.** `install_reserved_claim` does not
  re-check `owes_final` (`lifecycle.rs` 362-366). Before the fix, a takeover
  could install over a claim whose final a stale append had committed and
  marked. On the engine shape with the pre-fix fence, `InstallOnlyOverOwedClaim`
  fails in 15 states. On the fixed model it holds in every TLA-002 and TLA-003
  baseline, so the situation is no longer reachable. The missing re-check stays
  a defence-in-depth observation, not a finding.

### TLA-002-F2 (new, open): a `SealSuperseded` refusal can precede the durability of its fence

- **Classification.** Production-defect candidate: a remaining window in the
  F1 fix. It is a TLC counterexample checked against the code. It has no
  executable reproduction. The durability group raised the same question
  (their TLA-005-F4); this is the answer.
- **Checks.** `MaxHeldFence = 1` lets one fence group be staged before it is
  durable.
  - `known-defect-F2-refusal-before-fence-durable` (two instances, one crash)
    violates `ClosureAuthorized` in 19 states. Trace:
    `evidence/TLA-002-F2_refusal-before-fence-durable.txt`.
  - `known-defect-F2-release-after-durable-final` (one instance, one engine
    replacement) violates `ReleaseOnlyWhenUndeliverable` in 19 states: the
    older generation commits on the new engine first, then the newer
    attempt's refusal releases the claim although the record is durable.
    Trace: `evidence/TLA-002-F2_release-after-durable-final.txt`.
  - `baseline-xproc-held` passes every other safety property in the
    two-instance shape: `SuccessProvesOutcome`, `FinalClosedTruthful`,
    `FenceBarrier` and `ReleaseOnlyWhenUndeliverable` hold there. A direct TLC
    run of the one-instance shape with `MaxHeldFence = 1` (not in the manifest,
    7,202,668 states) found no violation other than `ClosureAuthorized`,
    `ReleaseOnlyWhenUndeliverable` and `PlainCannotCompleteOwedFinal`.
  - `baseline-liveness-held` passes: the window does not stop the collection
    from sealing. The takeover's install declines on the changed claim,
    `claim_seal` restarts, and the next iteration installs afresh.
- **Code path.**
  1. The committer stages a fence: it raises the engine cache
     (`maintenance.rs` 153-155) and puts the row in its group.
  2. A later claim-authorized append in the same group, or in a group staged
     before that one is durable, is refused `SealSuperseded` from the raised
     cache and answered at once (`transaction/append.rs` 139-142;
     `maintenance.rs` 185-188 for a close). Acknowledgements wait for
     durability; this refusal does not.
  3. `SealSuperseded` is definitive. `seal_final` releases the exact claim
     (`lifecycle.rs` 126-135); `complete_raw_close` does the same
     (`raw_close.rs` 37-46).
  4. The fence's group never becomes durable: the owner crashes before the
     WAL flush, or the write fails (`finalize.rs` `write_failed`).
  5. The next engine reads the old row. An older generation of the same
     operation, stalled before its enqueue, passes the fence and closes the
     segment while no claim stands.
- **Schedule (the trace).** P0's request on instance B claims generation 1
  and stalls before `submit`. P0's exact retry on A renews the claim to 2 and
  stalls too. The claim lapses. T1 reserves 3 and fences on A. The
  generation-2 final enqueues behind the fence, is refused `SealSuperseded`
  from the staged cache, and releases (P0, 2). A crashes before the fence group
  is durable, and the ring moves the shard to B. The generation-1 final
  enqueues on B's new engine, whose row reads 0, and closes the segment.
- **Consequences.** A closed segment with no claim and no seal. P0's
  generation-1 mark declines (`InvalidClaim`, 500). A later plain `:seal` can
  publish Sealed over P0's record (`PlainCannotCompleteOwedFinal`), as in F5.
  An exact retry of P0 before that completes the seal. Without an older
  attempt in flight the early release is harmless: the outcome equals a
  refusal by a durable fence, whose takeover then replaces the claim.
- **Why the baselines pass.** Baselines stage a committer element and make it
  durable in one step (ASM-SEAL-REPLY-ORDER). That is exact for every answer
  except this one.
- **Proposed regression.** In `src/dst/tests/seal_fencing.rs`: two exact
  product seal-with-final requests of one operation, both parked at
  `Fp::CloseBeforeEnqueue` (the second renewed the claim). Age the claim and
  start a takeover. Hold the commit gate (`test_hold_commit`) so the fence and
  the second request's final share one group, and arm `fail_next_group_for`
  for the segment so that group fails. Retire the engine
  (`state.shards.retire`), release the first request, and assert that it is
  refused and the segment stays open.
- **Decision needed (owner).** Options:
  1. Send `SealSuperseded` through the durability barrier (`effects.acks`), so
     a lost group answers `Moved` or `Internal`, which retain the claim.
  2. Treat `SealSuperseded` as retaining the claim. The takeover's install
     replaces it anyway.
  3. TLA-003-F5's options (b) or (c) also close this case.

  The `FinalDisposition` comment in `claims.rs` says every definitive verdict
  is durability-barriered. `SealSuperseded` is not.

### TLA-003-F2 (fixed): a raw close that took over another operation's claim refused its own final

- **Defect.** `prepare_close` computed `sealed_reject_new` from the admission
  snapshot. When that showed another operation's claim, a raw
  close-with-content carried `Some(Sealing)`. `install_intent` then took the
  claim over, installed afresh or renewed, but kept the refusal. The committer
  refused the operation's own final as `Closed`, and the definitive
  disposition released the claim it had just taken.
- **Consequences before the fix.** A false 409 `Stream-Closed: true`
  (`FinalClosedTruthful`). Deliverable debt released
  (`ReleaseOnlyWhenUndeliverable`). With a concurrent exact retry, the owed
  final dropped after its record was durable (`ClosureAuthorized`), then a
  plain seal over it (`PlainCannotCompleteOwedFinal`). One form needs no lease
  lapse.
- **Fix.** Commit "A raw close that takes over an abandoned final claim writes
  its own record". When `begin_sealing_for_close` returns this operation's
  generation for a Final intent, the plan owes that final and the Sealing
  refusal is cleared (`close.rs` 200-211).
- **Model.** `ClaimedFinalPlan` applies that update after a raw final's claim
  CAS, in `FClaim` and in `TInstall`.
- **Checks.** `baseline-renewal`, `baseline-shared-lane`,
  `baseline-product-release` and `baseline-validation` pass every property.
  The witnesses `RawTakeoverWritesItsRecord` (a raw close that took over a
  lapsed claim is sealed under its own operation) and
  `RetryAfterMarkRunsItsSeal` are reached.
- **Pre-fix behaviour.** The negative controls `nc-admission-refusal-*` keep
  the admission values. On the renewal shape they fail on
  `ReleaseOnlyWhenUndeliverable`, `FinalClosedTruthful`, `ClosureAuthorized`
  and `PlainCannotCompleteOwedFinal`. On shape PW, restricted to behaviours
  with no lease lapse, they fail on `FinalClosedTruthful`: X's claim is
  released by its own `ProducerSeqReused` refusal after W's admission saw it.
  (The first revision's no-lapse trace used F3's release, which the F3 fix
  removed.)
- **Regression.** `dst::dst_tests::seal_fencing::a_raw_close_that_takes_over_an_abandoned_final_claim_seals_with_its_record`.
  The renewal forms (a concurrent exact retry) have no real-code test. The
  pre-fix traces are `evidence/TLA-003-F2*_pre-fix_*`.

### TLA-003-F3 (fixed): a product seal-with-final published its claim before the record ceiling was checked

- **Defect.** The product pre-intent checks measured ingest capacity on the
  exact final body but not `MAX_RECORD_PAYLOAD_BYTES`. An over-ceiling final
  installed its claim; the append path then refused the record as a deferred
  `BadBody`, and the definitive refusal released the claim.
- **Consequences before the fix.** A claim written by an invalid request
  (`IntentOnlyAfterValidation`). With record ceilings that differ between
  instances it was worse, as F5 on the product surface: Y's original on A
  claims and stalls before its enqueue; its exact retry on B renews the claim,
  fails B's ceiling and releases the claim; the original's older generation
  then commits with no claim standing (`ClosureAuthorized`, 13 states).
- **Fix.** Commit "A product seal refuses an over-ceiling final before it
  publishes its intent". `content::stored_records` is the one definition of
  the records an append stores and of their refusal. The product seal builds
  the final's exact wire body once, checks it with `stored_records` right
  after the capacity check and before the claim (`product.rs` 1757-1767), and
  submits that same body.
- **Model.** `ProductPreIntentRefuses` now includes `"ceiling"`.
- **Checks.** `baseline-validation` and `baseline-validation-product` pass
  every property.
- **Pre-fix behaviour.** The negative controls `nc-ceiling-after-intent-*`
  drop `"ceiling"` from the pre-intent checks and fail on
  `IntentOnlyAfterValidation` and `ClosureAuthorized`. Their traces are
  `evidence/TLA-003-F3_pre-fix_*`.
- **Regression.** `dst::dst_tests::seal_cancellation::an_over_ceiling_product_final_is_refused_before_its_seal_intent`.
- **Raw surface.** A fresh raw close has no such gap: `parse_content` runs
  before `install_intent`, which runs only when nothing was deferred. Only the
  exact-retry renewal touches a claim before content is validated (F4, F5).

### TLA-003-F4 (open, owner decision): a raw exact retry renews the owed claim before its content is validated

- **Scope, narrowed.** A raw close with content always has a producer (its own
  or the synthetic `rawseal` lane, `close.rs` 101-112). `parse_content`
  therefore defers every content refusal (`content.rs` 30-73). The only early
  refusal it can give an exact retry is `permanently_unadmittable`
  (`content.rs` 81-89; `usage.rs` 327-340). That depends on the instance's
  ingest limits (`LIMIT_BYTES_PER_SEC`, `LIMIT_RECS_PER_SEC`,
  `LIMIT_BURST_SECS`), fixed at boot. F4 needs ingest-capacity skew between
  instances, for example during a rolling configuration change.
- **Schedule.** The original on A installs (op, g0) and stalls before its
  enqueue. The exact retry on B renews to g1 (`close.rs` 74-99), then
  `parse_content` answers 413 with no release. A's original commits under g0
  (the renewal raised no fence). Its `mark_final_committed(op, g0)` declines
  `InvalidClaim`, so the client gets 503 `SealIncomplete` with its record
  durable. The claim stays owed under a fresh lease until an exact retry lands
  on a capable instance, or a takeover completes it.
- **Check.** `known-defect-F4-invalid-retry-renews` (shape V4) violates
  `IntentOnlyAfterValidation` in 7 states. `baseline-validation-capacity`
  shows no other property fails in that shape.
- **Proposed regression.** Two rigs over one store with different ingest
  limits. Park the original on rig A at `Fp::CloseBeforeEnqueue`, send the
  exact retry to rig B, and assert that the refusal left the claim's
  generation and `claimed_ms` unchanged.

### TLA-003-F5 (open, owner decision): a renewed exact retry refused by its instance's record ceiling releases the claim while the original can still commit

- **Schedule.** B owns the shard. The original on A installs (op, g0) and
  stalls before its enqueue. The exact retry on B, whose
  `MAX_RECORD_PAYLOAD_BYTES` is lower, renews to g1. Its record becomes a
  deferred `BadBody`; the committer refuses it definitively (the original has
  not committed, so the producer lane accepts). `complete_raw_close` calls
  `abandon_seal_intent(op, g1)` and clears the claim (`raw_close.rs` 37-47,
  `lifecycle.rs` 486-517). The ring moves the shard to A. The original's g0
  final commits and closes the segment with no claim standing, and its mark
  declines (503).
- **Checks** (shape V5): `known-defect-F5-release-while-deliverable`
  (`ReleaseOnlyWhenUndeliverable`, 10 states),
  `known-defect-F5-closed-without-claim` (`ClosureAuthorized`, 13 states) and
  `known-defect-F5-plain-over-final` (`PlainCannotCompleteOwedFinal`: a plain
  `:seal` then publishes over the record). `baseline-validation-ceiling` shows
  that every other property holds in that shape.
- **Healing.** An exact retry on a capable instance repairs the state: it
  installs a fresh claim, which with the F2 fix owes its final. The committer
  answers the duplicate of the closed tail, and the retry marks and seals
  under its own operation. The witness `OrphanedCloseHealed` reaches this. It
  shows the repair is possible, not that it always happens. Before the F2 fix
  that retry joined as a plain close, which its own live claim refused as a
  conflict.
- **Proposed regression.** Two rigs over one store, rig B with a lower record
  ceiling and owning the shard. Park the original on rig A at
  `Fp::CloseBeforeEnqueue`, send the exact retry to rig B, move ownership to A,
  release the original, and assert that the claim still stands when the
  segment closes.

### Decision needed for F4, F5 and TLA-002-F2

Each option fixes a different part:

- (a) Validate configuration-dependent content before renewing. This fixes F4
  only.
- (b) Make a renewal raise the shard fence, so an older generation of the same
  operation can no longer commit. This removes the closed-without-claim forms
  of F5 and TLA-002-F2 (for the same operation). F5's release of deliverable
  debt remains.
- (c) Do not abandon a claim on a definitive refusal of a renewed exact retry
  while an older attempt may be in flight. This fixes F5 and TLA-002-F2 at the
  release.

### Model corrections and observations

- **M1** (TLA-002, resolved). `claim_seal` is bounded (six iterations,
  `lifecycle.rs` 200). Liveness uses `MaxClaimLoop = 6`. Safety uses 0: any
  number of restarts, any of which may give up.
- **M2** (TLA-003, resolved). The first `SuccessProvesOutcome` required a raw
  duplicate acknowledgement to be backed by a closing record with the
  request's bytes. That is not what the acknowledgement claims. The property
  now requires, for `dupAck`, a sealed and closed stream and a durable producer
  tuple. Trace: `evidence/TLA-003-M2_dupack-after-foreign-close.txt`.
- **O1.** Takeover reservations are obstruction-free, not wait-free. Each new
  reservation defeats every lower install (`LowerReservationRestarts`). The
  liveness claim therefore needs one persistent recovery actor.

## Model

### Structure

- **TLA-001 is the substrate.** `RegistryCas.tla` keeps the retry loop
  non-atomic. Each attempt is a GET followed by a separate conditional PUT
  whose reply can be lost after the write commits. It shows that a
  `mutate_incarnation` call is equivalent to applying its decision at the
  winning PUT. `SealProtocol.tla` therefore applies every registry decision
  at its CAS, with a lost-reply and an unavailable-read branch at every
  registry step.
- **One incarnation.** Segment identities include the epoch, so a recreated
  incarnation never shares a committer stream with the old one. Delete and
  recreate races are checked once, in TLA-001.
- **Processes and ownership.** `owner` is the process the ring assigns the
  shard to; `eng` is its engine (queue and fence cache). Each handler slot
  lives in one process (`HomeOf`). Registry work happens in the handling
  process; ownership is checked only at `submit` (ASM-SEAL-OWNER). A crash
  erases that process's handlers, and its engine if it owned the shard. An
  engine replacement erases the queue and cache and may move ownership;
  handlers survive.
- **Operation identity.** A final's claim id is `OpId(o)`, a function of
  (surface, content, coordination); its producer lane is the client's or the
  synthetic `rawseal` lane (ASM-SEAL-OPID).
- **Validity per instance.** `Validity[slot]` is the verdict under the
  handling instance's configuration (ASM-SEAL-VALIDITY): `ok`, `pre` (product
  pre-intent refusal), `capacity` (ingest capacity), `ceiling` (record
  ceiling, and on the raw surface every deferred content refusal).
- **Committer.** One queue element per step: decide, apply, durable, reply
  (ASM-SEAL-REPLY-ORDER). `MaxHeldFence = 1` splits a fence into staging and
  durability for the TLA-002-F2 check.
- **Merged steps.** Verdict classification with its release, mark or answer;
  the fence reply with the mark on behalf; `publish_sealed`'s CAS with its
  proof read (monotonic).
- **Time.** Lease lapse is an unconstrained environment event (ASM-SEAL-CLOCK).
- **History.** `hist` records what properties read. `wit` and `wset` are
  recorded only in witness configurations and read by no action or baseline
  property.

### TLA-001 mapping

| Action | Production | Atomicity |
|---|---|---|
| `Start`, `StartDelete` | callers of `Registry::mutate_incarnation` (claim, reservation, install, renew, release, mark, publish; deletion) | local |
| `Read` | `registry.rs` `mutate_incarnation` 1166-1197: `invalidate`, `store.get` (failure: `ReadUnavailable`), `decode_desc`, epoch check (1178), `decide` (1181), identity re-check (1186), `ConditionalUpdateToken::from_etag` (1195) | one GET (ASM-OBJSTORE-CAS); the rest is local and pure (ASM-SEAL-DECIDE-FN) |
| `Cas` | `mutate_incarnation` 1198-1233: `put_opts(PutMode::Update)`; `Ok` is `Applied`, `Precondition` re-reads and re-decides, other errors are `AmbiguousCompletion`, five attempts end in `Conflict` (1235) | atomic compare-and-write; the reply is separate |
| `BeginRecreate`, `RecreateRead`, `RecreateCas` | `creation/claim.rs` `resolve` recreate arm (44-83) → `Registry::recreate` (1009-1062); the client's retry resolves against its own replacement | same contract, with read-failure, lost-reply and failed-dispatch branches |

### TLA-002 and TLA-003 mapping

| Action | Production | Atomicity |
|---|---|---|
| `Issue` | a product `:seal` or seal-with-final (`product.rs`), or a raw POST with `stream-closed` | local |
| `FValidate` | product: the pre-intent checks (`product.rs` 1661-1767: capacity 1744-1756, record ceiling 1757-1767), reached only by a request they refuse. Raw: `close::prepare_close` (`append/close.rs` 19-135: `is_owed_final` 68-72, owed renewal 74-99, `sealed_reject_new` 114-123) → `content::parse_content` (`content.rs` 14-91: deferred refusals 30-73 through `stored_records` 99-127, capacity 81-89) → `install_intent`, skipped when sealed, owed or deferred (`close.rs` 181) | a read of a possibly cached descriptor; staleness is an earlier read plus delay |
| `FClaim` | product: `seal_final` → `enter_sealing` (`lifecycle.rs` 383) → `claim_seal` (193) → `enter_sealing_cas` (168) → `decide_claim` (`claims.rs` 68-126). Raw, fresh: `install_intent` (`close.rs` 166-226) → `begin_sealing_for_close` (`lifecycle.rs` 414); the plan then owes the final (`close.rs` 200-211). Raw, owed: `renew_owed_claim` (`lifecycle.rs` 449), then `parse_content` | one `mutate_incarnation`; the local verdict after a renewal is merged |
| `TReserve` | `take_over_abandoned` reservation CAS (`lifecycle.rs` 272-289); a decline returns to `claim_seal`'s loop (200) | one CAS |
| `TFence` | `fence_segment_for_key` (856-893): `resolve(Adoption::Internal)` (874-879; a non-owner answers Resumable) → `try_seal_fence` (`shard.rs` 1829) | enqueue at the owner |
| `ProcessFence` | `CommitTransaction::fence` (`maintenance.rs` 136-167): `seal_fence` (89-108: the cache, else the durable row; a read error answers `Internal`), raise the cache, write the row in the group, acknowledge in `effects.acks` | one queue element; the reply after the group's durability |
| `FenceGroupDurable` | the fence group's durability releasing its reply (only with `MaxHeldFence = 1`) | separate step for the F2 window |
| `TFenceLost` | a fence answered `Moved` or `Internal` ("fence refused") or dropped → Resumable (`lifecycle.rs` 888-892) | local |
| `TInstall` | `install_reserved_claim` (349-381) with the newest-reservation check (365); a raw final's plan then owes its final (`close.rs` 200-211) | one CAS |
| `TBehalfMark` | a closed fence: `mark_final_committed(old)` (526) then `run_seal(old)` (`lifecycle.rs` 297-313) | the reply is local; one CAS |
| `FCheck` | product `prepare_close` `seal_auth` check (`close.rs` 55-67); a failure is `SealSuperseded` and `seal_final` releases exactly (`lifecycle.rs` 126-135) | a read, then the release CAS |
| `Enqueue` | `execute_once` (`append.rs` 246-395) → `submit` (`submit.rs` 18-25: `NotOwner` for a non-owner) → sheds → `try_enqueue` (79-85) | enqueue at the owner; unbounded pre-queue window |
| `ProcessAppend` | `CommitTransaction::append` (`transaction/append.rs` 19-144): `decide_producer` (`commit_plan.rs` 88-143), closed tail (71-95), deferred content error (96-102), `seal_authorizes` (139-142; `maintenance.rs` 112-127; `seal_authorized` `commit_plan.rs` 145-150) | one queue element |
| `ProcessClose` | `CommitTransaction::close` (`maintenance.rs` 172-201): the fence is consulted only for an open segment | one queue element |
| `FAnswer`, `FRelease`, `FMark`, `FRawDup` | `seal_final` (`lifecycle.rs` 84-161) and `complete_raw_close` (`raw_close.rs` 15-87): `final_err_disposition` (`claims.rs` 226-245), `definitively_rejected` (`contract.rs` 285-297, via `product.rs` 1813), `abandon_seal_intent` (486-517), `mark_final_committed` (526-566) | local classification merged with its CAS |
| `RsPrep`, `RsClose`, `RsCloseFailed`, `RsPublish` | `run_seal` (576) → `prepare_execution` (593-701) → `close_claimed_segments` (705-770) → `topology::seal_segment_identity` (`topology.rs` 43-91) → `close_segment_on_engine` (99) or `relay_segment_close` (143) → `publish_sealed` (774-848) | read and CAS merged; enqueue at the owner; CAS and proof read merged |
| `APrep`, `AReceive` | product `refuse_if_sealed` (`product.rs` 1937); raw `sealed_reject_new` | a read; local |
| `Lapse` | `decide_claim` `abandoned` (`claims.rs` 95) | environment |
| `Cancel`, `Timeout` | a dropped handler; `APPEND_TIMEOUT` (`submit.rs` 86-93) | the handler vanishes; queued work stays |
| `Crash` | a process crash | erases that process's handlers, and the engine if it owned the shard |
| `Replace` | `ShardDirectory::retire` (`shard_directory.rs` 434-456) → `begin_close` (`shard.rs` 1876-1935): queued and stranded groups answer `Moved`; a stranded append, close or fence may still become durable. The next `resolve` opens an engine whose `seal_fences` cache (`shard.rs` 1178, 1389) is empty and reads the row | erases queue and cache; may move ownership; handlers survive |

### Properties

- TLA-001: `UniqueAllocation` (no two callers are told they allocated the same
  generation), `IncarnationFenced` (a mutation validated against incarnation E
  stores only into E) and `AllocatorCountsWrites` (the stored allocator equals
  the committed allocations). `AttemptLocalResult` and
  `NegativeOutcomesWroteNothing` hold by construction (ASM-SEAL-DECIDE-FN).
- `ClosureAuthorized`: a closed segment belongs to the claim holder or to the
  sealed operation (L5, L6, L15).
- `FenceBarrier`: after a takeover observed its fence answer "not closed" for
  generation f, no effect below f closes the segment (L6, L7, L9, L10).
- `QueuedFinalDecidedBeforeReplacement`: a takeover installs only after every
  final queued ahead of its fence is decided (L6, L7).
- `NewestInstall`: only the newest reservation installs (L8).
- `LiveClaimNeverFenced`: the current, unlapsed claim's generation is never
  refused as superseded (L6, L8).
- `SealedFinalHasItsRecord`, `PlainCannotCompleteOwedFinal` (L3, L12).
- `SuccessProvesOutcome`: every 2xx answer proves its outcome on the durable
  state (L15).
- `ReleaseOnlyWhenUndeliverable`: owed debt is released only when the record
  is not durable and either no instance can deliver it or a newer reservation
  exists (L11, L12, P8, D6, D7).
- `FinalClosedTruthful`: a final is told "stream closed" only when that is
  true.
- `IntentOnlyAfterValidation`: no claim is written by a request that fails
  validation on its instance (L1).
- `InstallOnlyOverOwedClaim`: a takeover never installs over a marked claim
  (F1(e)).
- `Structural` holds by construction; `HandlersProgress` rules out a stuck
  handler; `EventuallySealed` is the liveness property.

### Instances

**TLA-001** (`MC_RegistryCas`, `MC_RegistryCas3`): see the manifest's
`input_scope`.

**TLA-002** (`MC_SealTakeover`): P0 is a product seal-with-final with two
handler slots (the original and an exact retry). T1 is a competing product
seal-with-final and T2 a plain `:seal`. In the two-instance layout P0's retry
runs on B and everything else on A, which owns the shard at start. Safety
configurations bound generations at 5 (6 in `expanded`) and set
`MaxClaimLoop = 0`. Liveness configurations set `MaxClaimLoop = 6`, no
constraint and no symmetry. T2 retries forever; weak fairness is placed only on
the committer, each handler's own step, lease time and T2's issue.

| Config | Instances | Faults |
|---|---|---|
| `small` | 1 | 1 crash |
| `faults` | 1 | 1 registry fault, 1 cancellation, 1 enqueue failure |
| `expanded` | 1 | 1 crash, 1 registry fault; generations ≤ 6 |
| `xproc` | 2 | 1 crash (failover) |
| `retire` | 1 | 1 crash, 1 engine replacement, 1 failed fence-row read |
| `engine` | 2 | 1 crash, 1 engine replacement; plus `HandlersProgress` |
| `live_small` | 1 | P0 one request; 1 crash, 1 engine replacement |
| `live_contenders` | 1 | P0 and T1 one request each; 1 engine replacement |
| `live_faults` | 1 | P0 one request; 1 registry fault, 1 enqueue failure, 1 cancellation, 1 timeout |
| `held_xproc`, `kd_refusal_before_fence_durable` | 2 | 1 crash, 1 staged fence group |
| `kd_release_after_durable_final` | 1 | 1 crash, 1 engine replacement, 1 staged fence group |
| `live_held` | 1 | as `live_small`, plus 1 staged fence group |

**TLA-003** (`MC_FinalSeal`): shapes A, A2, B, C, P, PW, SL, V, V4, V5 and VP are
described in `MC_FinalSeal.tla` and the manifest's `input_scope`. Generations
are bounded at 5. There is no TLA-003 liveness check: an exact retry that
renews forever makes the generation space unbounded.

### Negative controls

Each control is an `MC_*` module that extends the unmodified instance and
substitutes one operator; its configuration checks only the target property.

| Control | Substitution | Violates |
|---|---|---|
| TLA-001 `nc-leaked-outcome` | `ReturnedResult <- LeakedReturnedResult` | `UniqueAllocation` |
| TLA-001 `nc-dropped-incarnation-check` | `IncarnationMatches <- AnyIncarnation` | `IncarnationFenced` |
| TLA-001 `nc-missing-etag-overwrite` | `UnconditionalOnMissingToken <- OverwriteOnMissingToken` | `AllocatorCountsWrites` |
| TLA-002 `nc-no-newest-reservation`, `-live-fenced` | `InstallAllowed <- InstallWithoutNewest` | `NewestInstall`, `LiveClaimNeverFenced` |
| TLA-002 `nc-install-before-fence-durable`, `-inqueue` | `FenceAcknowledged <- FenceNotAwaited` | `ClosureAuthorized`, `QueuedFinalDecidedBeforeReplacement` |
| TLA-002 `nc-engine-resident-fence-*` (F1 pre-fix) | `OpenedEngineFence <- EngineResidentFence` | `ClosureAuthorized` (xproc, retire), `ReleaseOnlyWhenUndeliverable`, `SuccessProvesOutcome` (engine) |
| TLA-003 `nc-gap-definitive` | `RawDisposition <- GapIsDefinitive` | `ReleaseOnlyWhenUndeliverable` |
| TLA-003 `nc-moved-definitive` | `RawDisposition <- MovedIsDefinitive` | `ReleaseOnlyWhenUndeliverable` |
| TLA-003 `nc-release-by-operation-only` | `ReleaseMatches <- ReleaseByOpOnly` | `ClosureAuthorized` |
| TLA-003 `nc-duplicate-completes-final` | `AckCompletesFinal <- AnyDuplicateCompletes` | `SealedFinalHasItsRecord` |
| TLA-003 `nc-admission-refusal-*` (F2 pre-fix) | `ClaimedFinalPlan <- AdmissionPlanKept` | `ReleaseOnlyWhenUndeliverable`, `FinalClosedTruthful`, `ClosureAuthorized`, `PlainCannotCompleteOwedFinal` (shape B); `FinalClosedTruthful` with no lease lapse (shape PW, `CONSTRAINT LeaseNeverLapses`) |
| TLA-003 `nc-ceiling-after-intent-*` (F3 pre-fix) | `ProductPreIntentRefuses <- CeilingAfterIntent` | `IntentOnlyAfterValidation`, `ClosureAuthorized` (shape VP) |

### Excluded, and not claimed

- TLA-001: `Registry::create` racing; the `Missing` outcome
  (ASM-SEAL-NODELETE); the contents of the real `decide` closures; the
  descriptor cache; provider conformance for conditional writes; at most three
  stale and two replacement racers on one name and one delete/recreate cycle.
- TLA-002 and TLA-003: topology interaction (TLA-004); multi-segment closes;
  relay transport failures beyond an enqueue failure; producer epochs and
  `Stream-Seq`; multi-record finals; operation-id hash construction
  (KANI-043); generation exhaustion (KANI-041); HTTP rendering beyond the
  response class; liveness across instances.
- The fence-row read failure may happen at any fence consult (production:
  only on a cache miss). This over-approximates.
- The staged-fence window (`MaxHeldFence`) lets only an immediate refusal at
  the head of the queue be decided while the fence's group is not durable. It
  is enough to exhibit TLA-002-F2 and is not a complete model of pipelined
  groups.
- Bounds: two contenders, at most three requests per operation, generations
  ≤ 5 or 6, at most one fault of each kind, at most two instances.

## Coverage

Coverage runs used TLC's `-coverage 1` directly, outside the driver, on the
19 safety baselines of `SealProtocol.tla` (the liveness shapes are excluded).
Each run passed. An action counts as covered when some run generated a state
with it; an expression counts as evaluated when some run evaluated it. The runs
are diagnostic; every verdict above comes from the receipts.

| Coverage run | Verdict | Distinct states | Actions with no generated state (of 31) |
|---|---|---|---|
| `TLA-002/baseline-small` | pass | 895,724 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-faults` | pass | 3,875,480 | APrep, AReceive, Crash, FAnswer, FRawDup, FValidate, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-expanded` | pass | 2,306,562 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-xproc` | pass | 523,650 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-retire` | pass | 5,811,402 | APrep, AReceive, Cancel, FRawDup, FValidate, FenceGroupDurable, Timeout |
| `TLA-002/baseline-engine` | pass | 3,773,720 | APrep, AReceive, Cancel, FRawDup, FValidate, FenceGroupDurable, Timeout |
| `TLA-002/baseline-xproc-held` | pass | 1,094,008 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-003/baseline-lanes` | pass | 661,936 | FCheck, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-expanded` | pass | 1,631,963 | FCheck, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-lanes-engine` | pass | 1,694,525 | Cancel, Crash, FCheck, FenceGroupDurable, Timeout |
| `TLA-003/baseline-renewal` | pass | 4,966,567 | APrep, AReceive, FAnswer, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-renewal-product` | pass | 3,483,721 | APrep, AReceive, FAnswer, FRawDup, FValidate, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-product-lanes` | pass | 702,029 | FAnswer, FRawDup, FValidate, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-product-release` | pass | 17,197 | FAnswer, FRawDup, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-shared-lane` | pass | 3,676,695 | APrep, AReceive, FAnswer, FenceGroupDurable, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-validation` | pass | 10,676 | APrep, AReceive, FAnswer, FCheck, FRelease, FenceGroupDurable, ProcessFence, Replace, RsCloseFailed, TBehalfMark, TFence, TFenceLost, TInstall, TReserve, Timeout |
| `TLA-003/baseline-validation-capacity` | pass | 1,603 | APrep, AReceive, FAnswer, FCheck, FRelease, FenceGroupDurable, ProcessFence, Replace, RsCloseFailed, TBehalfMark, TFence, TFenceLost, TInstall, TReserve, Timeout |
| `TLA-003/baseline-validation-ceiling` | pass | 64,862 | APrep, AReceive, Crash, FCheck, FenceGroupDurable, TFenceLost, Timeout |
| `TLA-003/baseline-validation-product` | pass | 13,166 | APrep, AReceive, Crash, FAnswer, FRawDup, FenceGroupDurable, TFenceLost, Timeout |

Union of the 19 runs: 1,543 expressions, 2 never evaluated. Every action
generated states in at least one run.

```
916:14-916:25  desc' = desc
918:14-918:29  UNCHANGED faults
```

Both belong to `RsPrep`'s `OwedFinal` refusal (`prepare_execution`,
`lifecycle.rs` 633-638). A final-bearing operation enters `run_seal` under its
own id only after its record is marked (`FMark`, `TBehalfMark`); raw
duplicates and plain seals run it as `PLAIN`. The previous revision reached
this branch only through F1(e), in the engine shape. No run of the fixed model
reaches it, which agrees with `InstallOnlyOverOwedClaim` holding. The
production guard stays as defence in depth.

`RegistryCas.tla` has not changed since the previous revision, so its coverage
was not rerun. Those runs (`baseline-small`, `baseline-expanded`) left five
expressions unevaluated: the missing-ETag overwrite branch, reachable only in
`nc-missing-etag-overwrite` (production refuses a missing ETag, `registry.rs`
1195); a recreate that loses its CAS, with the conflict it returns (no
concurrent creator is in the instance; `Registry::create` racing is excluded);
and the body of the LET-bound `record` action, which TLC counts at its
references.

## Files

| File | Contents |
|---|---|
| `RegistryCas.tla` | TLA-001 specification |
| `MC_RegistryCas.tla`, `MC_RegistryCas3.tla` | TLA-001 instances (small, expanded) |
| `MC_RegistryCas_Nc*.tla` | TLA-001 negative controls |
| `SealProtocol.tla` | TLA-002 and TLA-003 specification |
| `MC_SealTakeover.tla` | TLA-002 instances |
| `MC_SealTakeover_Nc*.tla` | TLA-002 negative controls (`NcEngineFence`: F1 pre-fix) |
| `MC_FinalSeal.tla` | TLA-003 instances |
| `MC_FinalSeal_Nc*.tla` | TLA-003 negative controls (`NcAdmissionRefusal`: F2 pre-fix; `NcCeilingAfterIntent`: F3 pre-fix) |
| `*.cfg` | one configuration per check; `_nc_` controls, `_w_` witnesses, `_kd_` known defects |
| `evidence/` | TLC counterexample traces (`*.txt`) and per-step views (`*.summary.txt`); `pre-fix` in a name marks behaviour a later commit fixed |
