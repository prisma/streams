# Seal group: TLA-001, TLA-002, TLA-003

These models implement three seal obligations of
[the formal-verification roadmap](../../../docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md).
The manifest entries, checks and bounds are in
[`verification/manifest.json`](../../manifest.json), the assumptions in
[`verification/assumptions.md`](../../assumptions.md), and the last complete
runs in `verification/receipts/TLA-00N.json`.

The models describe the code after these fixes:

- four fixes the models found: "A seal takeover's fence outlives the engine
  that recorded it" (TLA-002-F1), "A SealSuperseded refusal waits until the
  fence behind it is durable" (TLA-002-F2, a gap in the F1 fix), "A raw close that takes over an
  abandoned final claim writes its own record" (TLA-003-F2) and "A product seal
  refuses an over-ceiling final before it publishes its intent" (TLA-003-F3);
- the fix of TLA-003-F4 and TLA-003-F5, model counterexamples that stayed
  open until they were reproduced on real code with two instances: "A seal retry refused by its own instance's
  limits neither renews nor releases the claim";
- two fixes found outside the models, whose earlier behaviour the models now
  express: "Only a close can resume an owed final" (TLA-003-F6) and "Registry
  conditional writes never mistake their own committed write for a refusal"
  (TLA-001-F1).

Each fix's earlier behaviour is a negative control.

## Status

| ID | Title | Status | Result |
|---|---|---|---|
| TLA-001 | Registry CAS, attempt-local outcomes, and incarnation fencing | pass-with-recorded-scope | Both baselines pass, now also checking `RecreateAnswerTruthful`. Each negative control fails on its property, including the pre-fix client retry (F1). Every witness is reached. |
| TLA-002 | Seal claims, renewal, and competing takeover reservations | pass-with-recorded-scope | Every baseline passes, including engine replacement, crash failover, a fence group lost or rejected before its durability, and the liveness shapes. F1 and F2 are fixed; each fix's earlier behaviour is a negative control. |
| TLA-003 | Final-record sealing, ambiguous append outcomes, and owed debt | pass-with-recorded-scope | Every baseline passes every safety property, including the three shapes with configuration skew between instances (V4, V5, V5A) and the plain append with a final's operation id (OP). F2 to F6 are fixed; each fix's earlier behaviour is a negative control. No known defect remains. |

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

The tables below are rendered from the receipts
`verification/receipts/TLA-001.json`, `TLA-002.json` and `TLA-003.json`. Each
was recorded once, with `formal.py run --record` on a clean tree: TLA-001 on
`1d20f76f` (16 of 16 checks matched), TLA-002 and TLA-003 on `3f386070` (39 of
39 and 47 of 47). Their inputs are unchanged at `74294069`, where
`formal.py check --fresh` reports them current. Tools: TLC 2.19 (tla2tools
1.7.4) on Java 17.0.1, aarch64-apple-darwin, 2 workers per check.

Seconds are wall time under load: up to five driver processes ran in parallel
on an 8-core laptop. Violation searches use two workers, so their state
counts vary slightly between runs. The TLA-002 baseline counts equal those of
the schema 1 receipt recorded before the closure's model changes: the changes
add only raw and producer-less paths, which the product-only TLA-002 shapes
never take. The timeouts of `TLA-001/baseline-expanded` (3000 s) and
`TLA-002/baseline-faults` (3600 s) are at least twice the times an earlier,
more heavily loaded run observed (1210.8 s and 1628.9 s); the recorded times
are in the tables.

### TLA-001

| Check | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|
| `baseline-small` | baseline | pass | pass | 108,329 | 4.2 |
| `baseline-expanded` | baseline | pass | pass | 7,643,540 | 198.6 |
| `nc-leaked-outcome` | negative-control | violation:UniqueAllocation | violation:UniqueAllocation | 2,658 | 1.3 |
| `nc-dropped-incarnation-check` | negative-control | violation:IncarnationFenced | violation:IncarnationFenced | 5,149 | 1.2 |
| `nc-missing-etag-overwrite` | negative-control | violation:AllocatorCountsWrites | violation:AllocatorCountsWrites | 980 | 1.3 |
| `nc-client-retry` | negative-control | violation:AllocatorCountsWrites | violation:AllocatorCountsWrites | 692 | 1.1 |
| `nc-client-retry-recreate` | negative-control | violation:RecreateAnswerTruthful | violation:RecreateAnswerTruthful | 5,130 | 1.4 |
| `witness-LoserRetriesThenApplies` | witness | violation:Witness_LoserRetriesThenApplies | violation:Witness_LoserRetriesThenApplies | 2,690 | 1.3 |
| `witness-AmbiguousAfterWrite` | witness | violation:Witness_AmbiguousAfterWrite | violation:Witness_AmbiguousAfterWrite | 61 | 1.0 |
| `witness-StaleMutatorFenced` | witness | violation:Witness_StaleMutatorFenced | violation:Witness_StaleMutatorFenced | 3,154 | 1.3 |
| `witness-MissingTokenRefused` | witness | violation:Witness_MissingTokenRefused | violation:Witness_MissingTokenRefused | 16 | 1.0 |
| `witness-Recreated` | witness | violation:Witness_Recreated | violation:Witness_Recreated | 1,572 | 1.2 |
| `witness-ConflictExhausted` | witness | violation:Witness_ConflictExhausted | violation:Witness_ConflictExhausted | 12,110 | 1.6 |
| `witness-DeclinedOnTombstone` | witness | violation:Witness_DeclinedOnTombstone | violation:Witness_DeclinedOnTombstone | 399 | 1.2 |
| `witness-ReplacementAllocatesWhileStaleFenced` | witness | violation:Witness_ReplacementAllocatesWhileStaleFenced | violation:Witness_ReplacementAllocatesWhileStaleFenced | 14,231 | 1.8 |
| `witness-AmbiguousRecreateRetried` | witness | violation:Witness_AmbiguousRecreateRetried | violation:Witness_AmbiguousRecreateRetried | 3,055 | 1.3 |

### TLA-002

| Check | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|
| `baseline-small` | baseline | pass | pass | 895,724 | 122.7 |
| `baseline-faults` | baseline | pass | pass | 3,875,480 | 275.6 |
| `baseline-expanded` | baseline | pass | pass | 2,306,562 | 199.8 |
| `baseline-xproc` | baseline | pass | pass | 523,650 | 50.0 |
| `baseline-retire` | baseline | pass | pass | 5,811,402 | 563.4 |
| `baseline-engine` | baseline | pass | pass | 3,773,720 | 629.8 |
| `baseline-liveness-small` | baseline | pass | pass | 3,394 | 3.4 |
| `baseline-liveness-contenders` | baseline | pass | pass | 1,125,110 | 587.5 |
| `baseline-liveness-faults` | baseline | pass | pass | 15,713 | 5.5 |
| `baseline-xproc-held` | baseline | pass | pass | 1,210,271 | 120.8 |
| `baseline-retire-held` | baseline | pass | pass | 7,770,035 | 749.2 |
| `baseline-liveness-held` | baseline | pass | pass | 8,791 | 4.2 |
| `nc-no-newest-reservation` | negative-control | violation:NewestInstall | violation:NewestInstall | 17,981 | 3.1 |
| `nc-no-newest-reservation-live-fenced` | negative-control | violation:LiveClaimNeverFenced | violation:LiveClaimNeverFenced | 37,587 | 4.5 |
| `nc-install-before-fence-durable` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 5,035 | 2.0 |
| `nc-install-before-fence-inqueue` | negative-control | violation:QueuedFinalDecidedBeforeReplacement | violation:QueuedFinalDecidedBeforeReplacement | 3,310 | 1.9 |
| `nc-engine-resident-fence-xproc` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 22,007 | 3.6 |
| `nc-engine-resident-fence-retire` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 26,596 | 3.7 |
| `nc-engine-resident-fence-release` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 400,101 | 30.5 |
| `nc-engine-resident-fence-false-success` | negative-control | violation:SuccessProvesOutcome | violation:SuccessProvesOutcome | 2,856,586 | 235.1 |
| `nc-refusal-at-staging-closure` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 141,175 | 11.5 |
| `nc-refusal-at-staging-release` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 309,657 | 19.6 |
| `nc-cache-survives-rejected-group` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 185,994 | 14.4 |
| `witness-TakeoverInstalls` | witness | violation:Witness_TakeoverInstalls | violation:Witness_TakeoverInstalls | 2,412 | 1.8 |
| `witness-CompetingReservations` | witness | violation:Witness_CompetingReservations | violation:Witness_CompetingReservations | 2,322 | 1.9 |
| `witness-LowerReservationRestarts` | witness | violation:Witness_LowerReservationRestarts | violation:Witness_LowerReservationRestarts | 8,160 | 2.6 |
| `witness-OldFinalSealedOnBehalf` | witness | violation:Witness_OldFinalSealedOnBehalf | violation:Witness_OldFinalSealedOnBehalf | 34,893 | 4.5 |
| `witness-StaleClaimSuperseded` | witness | violation:Witness_StaleClaimSuperseded | violation:Witness_StaleClaimSuperseded | 5,350 | 2.4 |
| `witness-ExactRenewalAfterReservation` | witness | violation:Witness_ExactRenewalAfterReservation | violation:Witness_ExactRenewalAfterReservation | 1,414 | 1.7 |
| `witness-FinalSealCompletes` | witness | violation:Witness_FinalSealCompletes | violation:Witness_FinalSealCompletes | 3,484 | 2.0 |
| `witness-PlainSealCompletes` | witness | violation:Witness_PlainSealCompletes | violation:Witness_PlainSealCompletes | 568 | 1.4 |
| `witness-EngineRetiredMidFlight` | witness | violation:Witness_EngineRetiredMidFlight | violation:Witness_EngineRetiredMidFlight | 1,218 | 1.6 |
| `witness-CommittedButAnsweredMoved` | witness | violation:Witness_CommittedButAnsweredMoved | violation:Witness_CommittedButAnsweredMoved | 498 | 1.4 |
| `witness-OwnershipMovedToOtherProcess` | witness | violation:Witness_OwnershipMovedToOtherProcess | violation:Witness_OwnershipMovedToOtherProcess | 16 | 1.3 |
| `witness-NotOwnerRedirect` | witness | violation:Witness_NotOwnerRedirect | violation:Witness_NotOwnerRedirect | 335 | 1.4 |
| `witness-StaleFinalRefusedAfterReplacement` | witness | violation:Witness_StaleFinalRefusedAfterReplacement | violation:Witness_StaleFinalRefusedAfterReplacement | 13,464 | 3.0 |
| `witness-FenceUnverifiedRetainsClaim` | witness | violation:Witness_FenceUnverifiedRetainsClaim | violation:Witness_FenceUnverifiedRetainsClaim | 805 | 1.4 |
| `witness-SupersededAfterFenceDurable` | witness | violation:Witness_SupersededAfterFenceDurable | violation:Witness_SupersededAfterFenceDurable | 15,011 | 3.0 |
| `witness-FenceGroupRejected` | witness | violation:Witness_FenceGroupRejected | violation:Witness_FenceGroupRejected | 4,342 | 2.2 |

### TLA-003

| Check | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|
| `baseline-lanes` | baseline | pass | pass | 676,449 | 50.5 |
| `baseline-expanded` | baseline | pass | pass | 1,650,041 | 129.7 |
| `baseline-lanes-engine` | baseline | pass | pass | 1,716,205 | 206.0 |
| `baseline-renewal` | baseline | pass | pass | 4,994,511 | 419.7 |
| `baseline-renewal-product` | baseline | pass | pass | 3,483,721 | 297.5 |
| `baseline-product-lanes` | baseline | pass | pass | 702,029 | 52.9 |
| `baseline-product-release` | baseline | pass | pass | 17,197 | 3.1 |
| `baseline-shared-lane` | baseline | pass | pass | 3,745,651 | 244.9 |
| `baseline-validation` | baseline | pass | pass | 10,676 | 2.8 |
| `baseline-validation-capacity` | baseline | pass | pass | 1,276 | 1.8 |
| `baseline-validation-ceiling` | baseline | pass | pass | 43,081 | 4.4 |
| `baseline-validation-product` | baseline | pass | pass | 13,166 | 3.3 |
| `baseline-validation-ceiling-dup` | baseline | pass | pass | 150,973 | 10.2 |
| `baseline-op-id` | baseline | pass | pass | 23,913 | 3.3 |
| `nc-gap-definitive` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 510 | 1.4 |
| `nc-moved-definitive` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 560 | 1.4 |
| `nc-release-by-operation-only` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 2,313 | 1.6 |
| `nc-duplicate-completes-final` | negative-control | violation:SealedFinalHasItsRecord | violation:SealedFinalHasItsRecord | 201,985 | 14.0 |
| `nc-admission-refusal-release` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 36,773 | 4.3 |
| `nc-admission-refusal-false-closed` | negative-control | violation:FinalClosedTruthful | violation:FinalClosedTruthful | 36,080 | 4.2 |
| `nc-admission-refusal-any-release-orphaned` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 270,979 | 17.5 |
| `nc-admission-refusal-any-release-plain-over-final` | negative-control | violation:PlainCannotCompleteOwedFinal | violation:PlainCannotCompleteOwedFinal | 1,408,757 | 92.2 |
| `nc-admission-refusal-without-lapse` | negative-control | violation:FinalClosedTruthful | violation:FinalClosedTruthful | 2,539 | 1.8 |
| `witness-InvalidRefusedBeforeIntent` | witness | violation:Witness_InvalidRefusedBeforeIntent | violation:Witness_InvalidRefusedBeforeIntent | 17 | 1.3 |
| `witness-CommitAfterCancel` | witness | violation:Witness_CommitAfterCancel | violation:Witness_CommitAfterCancel | 10,380 | 2.3 |
| `witness-LostReplyThenRetrySucceeds` | witness | violation:Witness_LostReplyThenRetrySucceeds | violation:Witness_LostReplyThenRetrySucceeds | 322,323 | 27.4 |
| `witness-GapRetainsClaim` | witness | violation:Witness_GapRetainsClaim | violation:Witness_GapRetainsClaim | 553 | 1.5 |
| `witness-NonClosingDuplicateReleased` | witness | violation:Witness_NonClosingDuplicateReleased | violation:Witness_NonClosingDuplicateReleased | 72,938 | 6.9 |
| `witness-FinalSealCompletes` | witness | violation:Witness_FinalSealCompletes | violation:Witness_FinalSealCompletes | 72,699 | 6.5 |
| `witness-PlainSealCompletes` | witness | violation:Witness_PlainSealCompletes | violation:Witness_PlainSealCompletes | 584 | 1.4 |
| `witness-DefinitiveRelease` | witness | violation:Witness_DefinitiveRelease | violation:Witness_DefinitiveRelease | 18,204 | 3.2 |
| `witness-TakeoverInstalls` | witness | violation:Witness_TakeoverInstalls | violation:Witness_TakeoverInstalls | 6,952 | 2.4 |
| `witness-SeqReusedReleased` | witness | violation:Witness_SeqReusedReleased | violation:Witness_SeqReusedReleased | 13,766 | 2.7 |
| `witness-SharedLaneDuplicate` | witness | violation:Witness_SharedLaneDuplicate | violation:Witness_SharedLaneDuplicate | 50,382 | 4.7 |
| `witness-RawMovedRetainsClaim` | witness | violation:Witness_RawMovedRetainsClaim | violation:Witness_RawMovedRetainsClaim | 77 | 1.3 |
| `witness-NotOwnerRedirect` | witness | violation:Witness_NotOwnerRedirect | violation:Witness_NotOwnerRedirect | 245 | 1.4 |
| `witness-RawTakeoverWritesItsRecord` | witness | violation:Witness_RawTakeoverWritesItsRecord | violation:Witness_RawTakeoverWritesItsRecord | 155,490 | 12.6 |
| `witness-RetryAfterMarkRunsItsSeal` | witness | violation:Witness_RetryAfterMarkRunsItsSeal | violation:Witness_RetryAfterMarkRunsItsSeal | 169,928 | 14.0 |
| `nc-ceiling-after-intent-intent` | negative-control | violation:IntentOnlyAfterValidation | violation:IntentOnlyAfterValidation | 27 | 1.1 |
| `nc-ceiling-after-intent-closed-without-claim` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 7,867 | 2.3 |
| `nc-renew-before-validation-intent` | negative-control | violation:IntentOnlyAfterValidation | violation:IntentOnlyAfterValidation | 272 | 1.3 |
| `nc-any-attempt-releases-release` | negative-control | violation:ReleaseOnlyWhenUndeliverable | violation:ReleaseOnlyWhenUndeliverable | 1,192 | 1.4 |
| `nc-any-attempt-releases-closed-without-claim` | negative-control | violation:ClosureAuthorized | violation:ClosureAuthorized | 4,919 | 1.8 |
| `nc-any-attempt-releases-plain-over-final` | negative-control | violation:PlainCannotCompleteOwedFinal | violation:PlainCannotCompleteOwedFinal | 17,278 | 2.6 |
| `nc-plain-append-resumes-owed-final` | negative-control | violation:SealingRefusesNewAppends | violation:SealingRefusesNewAppends | 522 | 1.4 |
| `witness-SameIdPlainAppendRefused` | witness | violation:Witness_SameIdPlainAppendRefused | violation:Witness_SameIdPlainAppendRefused | 129 | 1.2 |
| `witness-CeilingRetryCompletesCommittedFinal` | witness | violation:Witness_CeilingRetryCompletesCommittedFinal | violation:Witness_CeilingRetryCompletesCommittedFinal | 3,730 | 2.1 |

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

### TLA-002-F2 (fixed): a `SealSuperseded` refusal preceded the durability of its fence

- **Classification.** Fix-introduced defect: a gap in the TLA-002-F1 fix,
  found by the model and reproduced on the real code. It is not counted as a
  pre-existing defect.
- **Defect.** The committer raises the fence cache while it stages the
  fence's group. A stale claim-authorized final refused `SealSuperseded` from
  that cache was answered at once, before the fence row was durable.
  `SealSuperseded` is definitive, so the handler released its claim. If the
  fence group was then lost (the owner crashed before the WAL flush, or the
  write failed), the next engine read the old row, and an older generation of
  the same operation passed it. A group rejected without an engine retirement
  (a maintenance-accounting divergence) also left the raised fence cached with
  no row behind it. The durability group raised the same schedule as their
  TLA-005-F4.
- **Consequences before the fix.** A segment closed by the older generation
  with no claim standing (`ClosureAuthorized`, 19 states); with an engine
  replacement instead of a crash, the release came after that record was
  durable (`ReleaseOnlyWhenUndeliverable`, 19 states); a later plain `:seal`
  could publish over the record (`PlainCannotCompleteOwedFinal`).
  `SuccessProvesOutcome`, `FinalClosedTruthful`, `FenceBarrier` and liveness
  held.
- **Fix.** Commit "A SealSuperseded refusal waits until the fence behind it is
  durable". `seal_authorizes` and `close` put `SealSuperseded` in the group's
  replies (`maintenance.rs` 109-146, 191-223), so it is sent once everything
  staged before it is durable; a failed group answers `Internal` and a retired
  engine `Moved`, which keep the claim. An unreadable fence row still answers
  `Internal` at once. `CommitTransaction::reject` drops the rejected group's
  cached seal fences (`transaction/mod.rs` 191-207), so the next consult
  re-reads the row.
- **Model.** `ImmediateRefusal` no longer includes `SealSuperseded`.
  `MaxHeldFence = 1` stages one fence group before its durability; the group
  then becomes durable (`FenceGroupDurable`), is lost with its engine
  (`Replace`, `Crash`), or is rejected without a retirement
  (`FenceGroupRejected`), after which the cache is `FenceAfterRejectedGroup`,
  the row.
- **Checks.** `baseline-xproc-held`, `baseline-retire-held` and
  `baseline-liveness-held` pass every property. The witnesses
  `SupersededAfterFenceDurable` and `FenceGroupRejected` are reached.
- **Pre-fix behaviour.** `nc-refusal-at-staging-closure` and
  `nc-refusal-at-staging-release` send `SealSuperseded` at staging and fail on
  `ClosureAuthorized` and `ReleaseOnlyWhenUndeliverable`.
  `nc-cache-survives-rejected-group` keeps a rejected group's fence cached and
  fails on `ClosureAuthorized` (20 states). The traces are
  `evidence/TLA-002-F2_pre-fix_*`.
- **Regressions.** `dst::dst_tests::seal_fencing::a_superseded_final_waits_for_its_fence_to_be_durable`,
  `shard::durability_frontier_tests::a_superseded_close_waits_for_its_fence_to_be_durable`
  and `shard::durability_frontier_tests::a_failed_fence_group_refuses_nothing`.

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
  refusal is cleared (`close.rs` 204-207 today).
- **Model.** `ClaimedFinalPlan` applies that update after a raw final's claim
  CAS, in `FClaim` and in `TInstall`.
- **Checks.** `baseline-renewal`, `baseline-shared-lane`,
  `baseline-product-release` and `baseline-validation` pass every property.
  The witnesses `RawTakeoverWritesItsRecord` (a raw close that took over a
  lapsed claim is sealed under its own operation) and
  `RetryAfterMarkRunsItsSeal` are reached.
- **Pre-fix behaviour.** The negative controls `nc-admission-refusal-*` keep
  the admission values. On the renewal shape they fail on
  `ReleaseOnlyWhenUndeliverable` and `FinalClosedTruthful`. On shape PW,
  restricted to behaviours with no lease lapse, they fail on
  `FinalClosedTruthful`: X's claim is released by its own `ProducerSeqReused`
  refusal after W's admission saw it. (The first revision's no-lapse trace
  used F3's release, which the F3 fix removed.)
- **The orphaned-final and plain-over-final forms need two pre-fix
  behaviours.** In their traces (`evidence/TLA-003-F2b_*`, `F2c_*`) the exact
  retry that RENEWED the claim is refused `Closed` by its kept Sealing refusal
  and releases the claim; the takeover attempt's final then commits with no
  claim standing. Since the F5 fix a renewing attempt releases nothing. On
  the current model the F2-only substitution no longer reaches
  `ClosureAuthorized` or `PlainCannotCompleteOwedFinal`: both searches
  complete on shape B with 9,009,123 distinct states and no violation. Either
  fix alone blocks these forms. The controls
  `nc-admission-refusal-any-release-orphaned` and `-plain-over-final`
  (`MC_FinalSeal_NcAdmissionRefusalAnyRelease`) therefore restore the code as
  it stood before the F2 fix, which also predates the F5 fix:
  `ClaimedFinalPlan <- AdmissionPlanKept` and
  `RefusalReleases <- AnyAttemptReleases`. They fail on `ClosureAuthorized`
  and `PlainCannotCompleteOwedFinal`. They are the only controls that
  substitute two operators.
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
  exact-retry renewal touched a claim before content was validated (F4, F5,
  both since fixed).

### TLA-003-F4 (fixed): a raw exact retry renewed the owed claim before its content was validated

- **Defect.** An owed exact retry renewed the claim in `prepare_close`, before
  `parse_content`. A raw close with content always has a producer, so the only
  early refusal `parse_content` gives it is ingest capacity
  (`permanently_unadmittable`), which depends on the instance's `LIMIT_*`
  settings. With ingest-capacity skew between instances (a rolling
  configuration change): the original on A installs (op, g0) and stalls before
  its enqueue; the exact retry on B renews to g1, then answers 413 with no
  release; A's original commits under g0 (the renewal raised no fence) and its
  mark declines, so the client gets 503 `SealIncomplete` with its record
  durable, and the claim stays owed under a fresh lease.
- **Reproduced on real code** with two instances over one store (generation 1
  to 2, then 503 with the record durable).
- **Fix.** Commit "A seal retry refused by its own instance's limits neither
  renews nor releases the claim", option (a) of the earlier decision.
  `prepare_close` only finds the owed claim (now only for a close, F6) and
  keeps the generation it observed (`close.rs` 71-85); `install_intent` renews
  only when nothing was refused or deferred (`close.rs` 174-176,
  `renew_owed_final` 225-252). An ingest 413 is answered before the claim is
  touched. An over-ceiling record stays a deferred refusal: the retry carries
  the observed generation without renewing, so the committer still answers a
  committed final as its duplicate, and the retry marks and seals it.
- **Model.** `OwedRetryValidatedFirst(v) == v \in {"capacity", "ceiling"}`:
  `FValidate` answers a capacity-refused owed retry at once and sends a
  deferred one to the queue at the observed generation; only a valid one
  reaches the renewal in `FClaim`.
- **Checks.** `baseline-validation-capacity` (shape V4) and
  `baseline-validation-ceiling` (V5) now check every safety property and pass.
  `baseline-validation-ceiling-dup` (V5A: the original commits before the
  retry reaches B's committer) passes, and its witness
  `CeilingRetryCompletesCommittedFinal` shows the deferred retry answered the
  duplicate and marking the committed final under the observed generation.
- **Pre-fix behaviour.** `nc-renew-before-validation-intent`
  (`OwedRetryValidatedFirst <- NothingValidatedFirst`, shape V4) fails on
  `IntentOnlyAfterValidation`. Trace:
  `evidence/TLA-003-F4_pre-fix_renew-before-validation.txt`.
- **Regressions.**
  `dst::dst_tests::seal_cancellation::an_ingest_refused_exact_retry_leaves_the_claim_to_its_original`
  (fails on the unfixed code) and the guard
  `dst::dst_tests::seal_cancellation::a_ceiling_limited_exact_retry_still_completes_a_committed_final`.

### TLA-003-F5 (fixed): a retry refused by its instance's record ceiling released the claim while the original could still commit

- **Defect.** B owns the shard. The original on A installs (op, g0) and stalls
  before its enqueue. The exact retry on B, whose `MAX_RECORD_PAYLOAD_BYTES`
  is lower, renewed to g1; its record became a deferred `BadBody`, which the
  committer refused definitively (the original had not committed), and
  `complete_raw_close` abandoned the claim. The ring moves the shard to A; the
  original's g0 final commits and closes the segment with no claim standing;
  its mark declines (503), and a plain `:seal` can then publish over the
  record.
- **Reproduced on real code** with two instances over one store (the segment
  closed with `sealed=false`, `sealing=None`, and A answered 503).
- **Fix.** The same commit, option (c): `begin_sealing_for_close` reports
  whether this attempt installed the claim (`lifecycle.rs` 431-432,
  `ClosePlan::installed_claim`, `close.rs` 196-198), and `complete_raw_close`
  releases on a definitive refusal only for the installing attempt
  (`raw_close.rs` 45-55). Its content was valid where the claim was taken, so
  its refusal rests on committer state every attempt shares. A joined or
  renewed retry releases nothing. With (a) alone F5 still fails: the deferred
  retry would release the original's own generation.
- **Model.** `RefusalReleases(hr) == hr.inst`; `inst` is set by `FClaim` when
  `decide_claim` installed (not renewed) the claim and by `TInstall` for a raw
  final's takeover (`EnterSeal::Installed`). The product surface is unchanged:
  its checks run before the claim (F3).
- **Checks.** `baseline-validation-ceiling` and
  `baseline-validation-ceiling-dup` pass every safety property.
- **Pre-fix behaviour.** `nc-any-attempt-releases-*`
  (`RefusalReleases <- AnyAttemptReleases`, shape V5) fail on
  `ReleaseOnlyWhenUndeliverable`, `ClosureAuthorized` and
  `PlainCannotCompleteOwedFinal`. With fix (a) in place, the retry releases
  the generation it observed, which is the original's. Traces:
  `evidence/TLA-003-F5_pre-fix_*`.
- **Regression.**
  `dst::dst_tests::seal_cancellation::a_ceiling_refused_exact_retry_leaves_the_claim_to_its_original`
  (fails on the unfixed code).
- **The healing witness is retired.** `OrphanedCloseHealed` showed that an
  exact retry on a capable instance could repair F5's closed-without-claim
  state. That state is no longer reachable in V5, so the witness check was
  removed (a TLC run of it on the fixed model completed without reaching it).

### TLA-003-F6 (fixed, found outside the models): a plain append with an owed final's bytes passed as its exact retry

- **Defect.** A raw append's semantic operation id covers the body, content
  type, routing key and coordination headers but not `Stream-Closed` (the
  request hash's close argument is a constant). `prepare_close` treated any
  append with an owed final's exact bytes as that final's exact retry,
  including a plain append without `Stream-Closed`: it skipped the Sealing
  refusal, renewed the claim and was admitted at that generation. The parked
  final then committed too, so the records landed twice, and the close
  answered 503 with the stream left unsealed.
- **Found** while fixing F4/F5 and reproduced on real code.
- **Fix.** Commit "Only a close can resume an owed final": the owed-claim
  filter requires a close (`close.rs` 71-80). Operation ids are unchanged.
- **Model.** Shape OP has a raw append N1 without producer headers and with
  X's bytes, so `SemanticOpId(N1) = OpId(X)`. Producer-less raw appends have
  no lane and are refused at admission while Sealing (`close.rs` 100-112).
  `ResumesOwedFinal(close, sameOp) == close /\ sameOp` gates both `FValidate`
  and `APrep`. The new property `SealingRefusesNewAppends` (L4, with L2)
  states that an ordinary append admitted from a Sealing or Sealed snapshot
  never commits. No earlier property caught the defect: the final's own record
  still closes the segment under X's claim, so `SealedFinalHasItsRecord`,
  `ClosureAuthorized` and the release properties hold; what breaks is the
  Sealing refusal, and the double landing follows from it.
- **Checks.** `baseline-op-id` passes every safety property; every TLA-003
  baseline now checks `SealingRefusesNewAppends`. The witness
  `SameIdPlainAppendRefused` reaches the refusal.
- **Pre-fix behaviour.** `nc-plain-append-resumes-owed-final`
  (`ResumesOwedFinal <- AnyRequestResumes`) fails on
  `SealingRefusesNewAppends`: N1 renews X's claim and lands its record under
  the renewed generation while X's final is still owed. Trace:
  `evidence/TLA-003-F6_pre-fix_plain-append-resumes-owed-final.txt`.
- **Regression.**
  `dst::dst_tests::seal_cancellation::a_plain_append_with_the_owed_finals_body_is_refused_during_sealing`.

### TLA-001-F1 (fixed, found by the provider contract suite): a committed conditional PUT could be answered `Precondition`

- **Defect.** object_store 0.14.1 re-sent a conditional PUT after a 5xx, 429
  or 408 (an update also after 409) with the original precondition. If the
  first request committed, the retry was refused, so the caller saw
  `Precondition` or `AlreadyExists` for its own write. `mutate_incarnation`
  then re-read and applied a non-idempotent decision twice (reproduced: a
  counter went from 1 to 3), and `recreate` found its own replacement live
  and declined, answering a create as a lost race without writing its body or
  publishing Ready. TLA-001's `Cas` step assumed a single request
  (ASM-OBJSTORE-CAS clause (b)), which the client layer did not provide.
- **Fix.** Commit "Registry conditional writes never mistake their own
  committed write for a refusal": `S3Store` (`src/bootstrap/s3_store.rs`)
  sends `PutMode::Create`, `PutMode::Update` and `CopyMode::Create` through a
  client with `max_retries: 0`, so `Precondition` and `AlreadyExists` answer
  the only request; any other failure reaches the caller as a possibly
  committed error (`AmbiguousCompletion`).
- **Model.** `Cas` and `RecreateCas` stay single-request atomic steps
  (`CommittedAnsweredPrecondition == FALSE`). The new property
  `RecreateAnswerTruthful` states that a recreate call is never answered as
  declined after its own PUT committed; the baselines check it.
- **Pre-fix behaviour.** `nc-client-retry` and `nc-client-retry-recreate`
  (`CommittedAnsweredPrecondition <- ClientRetriesConditional`: a committed
  PUT whose reply was lost comes back `Precondition`) fail on
  `AllocatorCountsWrites` (the mutator re-reads its own write and allocates
  again) and `RecreateAnswerTruthful`. Traces: `evidence/TLA-001-F1_pre-fix_*`.
- **Regression.**
  `bootstrap::tests::provider_contract::s3lite_through_the_production_client_meets_the_provider_contract`
  (its HTTP cases `mutate_incarnation_applies_once` and
  `recreate_never_declines_against_itself` fail with the default retries).

### Response classes (no model change)

Commit "A seal refused by another operation's terminal seal is a definitive
409" maps `AlreadySealed` and `OtherOperation` on the product surface to 409
`sealed`, not retryable. The models answer these as `error`, and a client's
next request is never conditioned on the answer's class (`Issue`), so the
change does not alter any reachable state. `SuccessProvesOutcome` constrains
only 2xx answers.

### What remains open

- docs/seal-transitions.md "Limit reductions and accepted finals" records
  three open obligations. The models check the first and third as they stand
  and do not decide the second:
  (1) under a reduced ingest capacity, an exact retry of a final that already
  committed is refused 413 rather than acknowledged as a duplicate. In shape
  V4 this is an `invalid` answer that touches no claim; every property holds.
  (2) If every instance's limits fall below an accepted final, nothing
  delivers the record, and no attempt tells the original client. The models
  have no property about telling the client; `ReleaseOnlyWhenUndeliverable`
  treats such a record as undeliverable, and the takeover replaces its claim.
  (3) The product seal's release rule depends on its checks running before the
  claim; shapes C, VP and the TLA-002 shapes check the product surface only
  with those checks in place.
- `ASM-OBJSTORE-CAS` stays unestablished until the provider contract suite
  passes against the production provider.

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
  (ASM-SEAL-REPLY-ORDER). `MaxHeldFence = 1` splits one fence into staging and
  durability; while it is staged only an immediate refusal is decided behind
  it.
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
| `Cas` | `mutate_incarnation` 1198-1233: `put_opts(PutMode::Update)`; `Ok` is `Applied`, `Precondition` re-reads and re-decides, other errors are `AmbiguousCompletion`, five attempts end in `Conflict` (1235). The server's store sends every conditional request once (`src/bootstrap/s3_store.rs` 90-101, 138-144: `S3Store::put_opts` and `copy_opts` route `PutMode::Create`, `PutMode::Update` and `CopyMode::Create` to the client built with `max_retries: 0`, 50-67), so `Precondition` answers the only request | one request: atomic compare-and-write; the reply is separate |
| `BeginRecreate`, `RecreateRead`, `RecreateCas` | `creation/claim.rs` `resolve` recreate arm (44-83) → `Registry::recreate` (1009-1062), its PUT through the same single-request client; the client's retry resolves against its own replacement | same contract, with read-failure, lost-reply and failed-dispatch branches |

### TLA-002 and TLA-003 mapping

| Action | Production | Atomicity |
|---|---|---|
| `Issue` | a product `:seal` or seal-with-final (`product.rs`), or a raw POST with `stream-closed` | local |
| `FValidate` | product: the pre-intent checks (`product.rs` 1661-1767: capacity 1744-1756, record ceiling 1757-1767), reached only by a request they refuse. Raw: `close::prepare_close` (`append/close.rs` 19-122: the owed claim, only for a close, 71-81; the observed generation 82-85; `sealed_reject_new` 100-112) → `content::parse_content` (`content.rs` 14-91: deferred refusals 30-73 through `stored_records` 99-127, capacity 81-89) → `install_intent` (152-223): an owed retry renews only when nothing was refused or deferred (174-176); a fresh close installs, skipped when sealed, owed or deferred (177) | a read of a possibly cached descriptor; staleness is an earlier read plus delay |
| `FClaim` | product: `seal_final` → `enter_sealing` (`lifecycle.rs` 383) → `claim_seal` (193) → `enter_sealing_cas` (168) → `decide_claim` (`claims.rs` 68-126). Raw, fresh: `install_intent` → `begin_sealing_for_close` (`lifecycle.rs` 416-441, answering whether it installed, 431-432); the plan records `installed_claim` and owes the final (`close.rs` 196-207). Raw, owed: `renew_owed_final` (`close.rs` 225-252) → `renew_owed_claim` (`lifecycle.rs` 450), after `parse_content` | one `mutate_incarnation`; the local verdict after a renewal is merged |
| `TReserve` | `take_over_abandoned` reservation CAS (`lifecycle.rs` 272-289); a decline returns to `claim_seal`'s loop (200) | one CAS |
| `TFence` | `fence_segment_for_key` (856-893): `resolve(Adoption::Internal)` (874-879; a non-owner answers Resumable) → `try_seal_fence` (`shard.rs` 1829) | enqueue at the owner |
| `ProcessFence` | `CommitTransaction::fence` (`maintenance.rs` 155-186): `seal_fence` (89-108: the cache, else the durable row; a read error answers `Internal`), raise the cache, write the row in the group, acknowledge in `effects.acks` | one queue element; the reply after the group's durability |
| `FenceGroupDurable`, `FenceGroupRejected` | the staged fence group becomes durable and its replies are released (`DurableEffects`), or it is rejected without an engine retirement: `CommitTransaction::reject` (`transaction/mod.rs` 191-207) answers `Internal` and drops the cached fences (only with `MaxHeldFence = 1`) | separate steps for a staged fence group |
| `TFenceLost` | a fence answered `Moved` or `Internal` ("fence refused") or dropped → Resumable (`lifecycle.rs` 888-892) | local |
| `TInstall` | `install_reserved_claim` (349-381) with the newest-reservation check (365); take_over_abandoned answers `Installed` (328), so a raw final's plan has `installed_claim` and owes its final (`close.rs` 196-207) | one CAS |
| `TBehalfMark` | a closed fence: `mark_final_committed(old)` (526) then `run_seal(old)` (`lifecycle.rs` 297-313) | the reply is local; one CAS |
| `FCheck` | product `prepare_close` `seal_auth` check (`close.rs` 58-70); a failure is `SealSuperseded` and `seal_final` releases exactly (`lifecycle.rs` 126-135) | a read, then the release CAS |
| `Enqueue` | `execute_once` (`append.rs` 246-395) → `submit` (`submit.rs` 18-25: `NotOwner` for a non-owner) → sheds → `try_enqueue` (79-85) | enqueue at the owner; unbounded pre-queue window |
| `ProcessAppend` | `CommitTransaction::append` (`transaction/append.rs` 19-142): `decide_producer` (`commit_plan.rs` 88-143), closed tail (71-95), deferred content error (96-102), `seal_authorizes` (139-141; `maintenance.rs` 109-146: `SealSuperseded` joins the group's replies; `seal_authorized` `commit_plan.rs` 145-150) | one queue element; a refusal other than `BadBody` or `Internal` waits for the group's durability |
| `ProcessClose` | `CommitTransaction::close` (`maintenance.rs` 191-223): the fence is consulted only for an open segment; `SealSuperseded` joins the group's replies | one queue element |
| `FAnswer`, `FRelease`, `FMark`, `FRawDup` | `seal_final` (`lifecycle.rs` 84-161) and `complete_raw_close` (`raw_close.rs` 23-96: a definitive refusal releases only for the installing attempt, 45-55): `final_err_disposition` (`claims.rs` 241-260), `definitively_rejected` (`contract.rs` 285-297, via `product.rs` 1813), `abandon_seal_intent` (`lifecycle.rs` 487-518), `mark_final_committed` (527-567) | local classification merged with its CAS |
| `RsPrep`, `RsClose`, `RsCloseFailed`, `RsPublish` | `run_seal` (576) → `prepare_execution` (593-701) → `close_claimed_segments` (705-770) → `topology::seal_segment_identity` (`topology.rs` 43-91) → `close_segment_on_engine` (99) or `relay_segment_close` (143) → `publish_sealed` (774-848) | read and CAS merged; enqueue at the owner; CAS and proof read merged |
| `APrep`, `AReceive` | product `refuse_if_sealed` (`product.rs` 1937); raw `prepare_close`: the owed-claim filter requires a close (`close.rs` 71-80), `sealed_reject_new`, and without a producer the closed-tail refusal at once (100-112) | a read; local (the pre-fix renewal branch merges its CAS) |
| `Lapse` | `decide_claim` `abandoned` (`claims.rs` 95) | environment |
| `Cancel`, `Timeout` | a dropped handler; `APPEND_TIMEOUT` (`submit.rs` 86-93) | the handler vanishes; queued work stays |
| `Crash` | a process crash | erases that process's handlers, and the engine if it owned the shard |
| `Replace` | `ShardDirectory::retire` (`shard_directory.rs` 434-456) → `begin_close` (`shard.rs` 1876-1935): queued and stranded groups answer `Moved`; a stranded append, close or fence may still become durable. The next `resolve` opens an engine whose `seal_fences` cache (`shard.rs` 1178, 1389) is empty and reads the row | erases queue and cache; may move ownership; handlers survive |

### Properties

- TLA-001: `UniqueAllocation` (no two callers are told they allocated the same
  generation), `IncarnationFenced` (a mutation validated against incarnation E
  stores only into E), `AllocatorCountsWrites` (the stored allocator equals
  the committed allocations) and `RecreateAnswerTruthful` (a recreate call is
  never answered as declined after its own PUT committed; TLA-001-F1). `AttemptLocalResult` and
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
- `SealingRefusesNewAppends`: an ordinary append admitted from a Sealing or
  Sealed snapshot never commits (L4, with L2's operation identity; F6).
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
| `held_xproc` | 2 | 1 crash, 1 staged fence group |
| `held_retire` | 1 | 1 crash, 1 engine replacement, 1 staged fence group |
| `live_held` | 1 | as `live_small`, plus 1 staged fence group |

**TLA-003** (`MC_FinalSeal`): shapes A, A2, B, C, P, PW, SL, V, V4, V5, V5A, VP
and OP are described in `MC_FinalSeal.tla` and the manifest's `input_scope`.
V5A is V5 with A owning the shard at start (its configurations set
`InitOwner = A`), so the original can commit before the retry reaches a
committer. Generations
are bounded at 5. There is no TLA-003 liveness check: an exact retry that
renews forever makes the generation space unbounded.

### Negative controls

Each control is an `MC_*` module that extends the unmodified instance and
substitutes one operator (two for `nc-admission-refusal-any-release-*`, see
TLA-003-F2); its configuration checks only the target property.

| Control | Substitution | Violates |
|---|---|---|
| TLA-001 `nc-leaked-outcome` | `ReturnedResult <- LeakedReturnedResult` | `UniqueAllocation` |
| TLA-001 `nc-dropped-incarnation-check` | `IncarnationMatches <- AnyIncarnation` | `IncarnationFenced` |
| TLA-001 `nc-missing-etag-overwrite` | `UnconditionalOnMissingToken <- OverwriteOnMissingToken` | `AllocatorCountsWrites` |
| TLA-001 `nc-client-retry`, `-recreate` (F1 pre-fix) | `CommittedAnsweredPrecondition <- ClientRetriesConditional` | `AllocatorCountsWrites`, `RecreateAnswerTruthful` |
| TLA-002 `nc-no-newest-reservation`, `-live-fenced` | `InstallAllowed <- InstallWithoutNewest` | `NewestInstall`, `LiveClaimNeverFenced` |
| TLA-002 `nc-install-before-fence-durable`, `-inqueue` | `FenceAcknowledged <- FenceNotAwaited` | `ClosureAuthorized`, `QueuedFinalDecidedBeforeReplacement` |
| TLA-002 `nc-engine-resident-fence-*` (F1 pre-fix) | `OpenedEngineFence <- EngineResidentFence` | `ClosureAuthorized` (xproc, retire), `ReleaseOnlyWhenUndeliverable`, `SuccessProvesOutcome` (engine) |
| TLA-002 `nc-refusal-at-staging-*` (F2 pre-fix) | `ImmediateRefusal <- RefusalAtStaging` | `ClosureAuthorized` (xproc, staged fence), `ReleaseOnlyWhenUndeliverable` (retire, staged fence) |
| TLA-002 `nc-cache-survives-rejected-group` (F2 pre-fix) | `FenceAfterRejectedGroup <- CacheSurvivesRejection` | `ClosureAuthorized` (xproc, staged fence) |
| TLA-003 `nc-gap-definitive` | `RawDisposition <- GapIsDefinitive` | `ReleaseOnlyWhenUndeliverable` |
| TLA-003 `nc-moved-definitive` | `RawDisposition <- MovedIsDefinitive` | `ReleaseOnlyWhenUndeliverable` |
| TLA-003 `nc-release-by-operation-only` | `ReleaseMatches <- ReleaseByOpOnly` | `ClosureAuthorized` |
| TLA-003 `nc-duplicate-completes-final` | `AckCompletesFinal <- AnyDuplicateCompletes` | `SealedFinalHasItsRecord` |
| TLA-003 `nc-admission-refusal-release`, `-false-closed`, `-without-lapse` (F2 pre-fix) | `ClaimedFinalPlan <- AdmissionPlanKept` | `ReleaseOnlyWhenUndeliverable`, `FinalClosedTruthful` (shape B); `FinalClosedTruthful` with no lease lapse (shape PW, `CONSTRAINT LeaseNeverLapses`) |
| TLA-003 `nc-admission-refusal-any-release-*` (the code before the F2 fix, which also predates F5's) | `ClaimedFinalPlan <- AdmissionPlanKept` and `RefusalReleases <- AnyAttemptReleases` | `ClosureAuthorized`, `PlainCannotCompleteOwedFinal` (shape B) |
| TLA-003 `nc-ceiling-after-intent-*` (F3 pre-fix) | `ProductPreIntentRefuses <- CeilingAfterIntent` | `IntentOnlyAfterValidation`, `ClosureAuthorized` (shape VP) |
| TLA-003 `nc-renew-before-validation-intent` (F4 pre-fix) | `OwedRetryValidatedFirst <- NothingValidatedFirst` | `IntentOnlyAfterValidation` (shape V4) |
| TLA-003 `nc-any-attempt-releases-*` (F5 pre-fix) | `RefusalReleases <- AnyAttemptReleases` | `ReleaseOnlyWhenUndeliverable`, `ClosureAuthorized`, `PlainCannotCompleteOwedFinal` (shape V5) |
| TLA-003 `nc-plain-append-resumes-owed-final` (F6 pre-fix) | `ResumesOwedFinal <- AnyRequestResumes` | `SealingRefusesNewAppends` (shape OP) |

### Excluded, and not claimed

- TLA-001: `Registry::create` racing; the `Missing` outcome
  (ASM-SEAL-NODELETE); the contents of the real `decide` closures; the
  descriptor cache; provider conformance for conditional writes; at most three
  stale and two replacement racers on one name and one delete/recreate cycle.
  The single-request client is an input to the model (a constant operator),
  not something TLC checks: that `S3Store` routes every conditional request to
  the no-retry client is shown by source inspection and the provider contract
  suite. Creation's `PutMode::Create` path (`Registry::create`) is not
  modelled, so its lost-race-to-itself form of F1 has only the real-code
  regression.
- TLA-002 and TLA-003: topology interaction (TLA-004); multi-segment closes;
  relay transport failures beyond an enqueue failure; producer epochs and
  `Stream-Seq`; multi-record finals; operation-id hash construction
  (KANI-043); generation exhaustion (KANI-041); HTTP rendering beyond the
  response class; liveness across instances. Configuration skew is fixed per
  handler slot (`Validity`); an instance whose limits change while a request
  is in flight is not modelled. A plain append with a final's semantic id is
  modelled without producer headers (shape OP, as in the reproduction); with a
  producer it would share the final's producer tuple, so the committer would
  deduplicate one of the two.
- The fence-row read failure may happen at any fence consult (production:
  only on a cache miss). This over-approximates.
- The staged-fence window (`MaxHeldFence`) stages one fence group at a time
  and decides only an immediate refusal at the head of the queue behind it.
  It is not a complete model of pipelined groups.
- Bounds: two contenders, at most three requests per operation, generations
  ≤ 5 or 6, at most one fault of each kind, at most two instances.

## Coverage

Coverage runs use TLC's `-coverage 1` directly, outside the driver. They are
diagnostic: every verdict above comes from the driver runs. An action counts as
covered when some run generated a state with it. An expression counts as
evaluated when some run evaluated it or an expression containing it.

### TLA-003 (`SealProtocol.tla`, current model)

All 14 TLA-003 safety baselines were rerun on the current model, and each
passed:

| Coverage run | Verdict | Distinct states | Actions with no generated state (of 32) |
|---|---|---|---|
| `TLA-003/baseline-lanes` | pass | 676,449 | FCheck, FRawDup, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-expanded` | pass | 1,650,041 | FCheck, FRawDup, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-lanes-engine` | pass | 1,716,205 | Cancel, Crash, FCheck, FRawDup, FenceGroupDurable, FenceGroupRejected, Quiescent, TBehalfMark, Timeout |
| `TLA-003/baseline-renewal` | pass | 4,994,511 | APrep, AReceive, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-renewal-product` | pass | 3,483,721 | APrep, AReceive, FAnswer, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-product-lanes` | pass | 702,029 | FAnswer, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-product-release` | pass | 17,197 | FRawDup, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-shared-lane` | pass | 3,745,651 | APrep, AReceive, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost |
| `TLA-003/baseline-validation` | pass | 10,676 | APrep, AReceive, FAnswer, FCheck, FRelease, FenceGroupDurable, FenceGroupRejected, ProcessFence, Replace, RsCloseFailed, TBehalfMark, TFence, TFenceLost, TInstall, TReserve, Timeout |
| `TLA-003/baseline-validation-capacity` | pass | 1,276 | APrep, AReceive, FAnswer, FCheck, FRelease, FenceGroupDurable, FenceGroupRejected, ProcessFence, Replace, RsCloseFailed, TBehalfMark, TFence, TFenceLost, TInstall, TReserve, Timeout |
| `TLA-003/baseline-validation-ceiling` | pass | 43,081 | APrep, AReceive, Crash, FCheck, FenceGroupDurable, FenceGroupRejected, TFenceLost, Timeout |
| `TLA-003/baseline-validation-product` | pass | 13,166 | APrep, AReceive, Crash, FAnswer, FRawDup, FenceGroupDurable, FenceGroupRejected, TFenceLost, Timeout |
| `TLA-003/baseline-validation-ceiling-dup` | pass | 150,973 | APrep, AReceive, Crash, FCheck, FenceGroupDurable, FenceGroupRejected, Timeout |
| `TLA-003/baseline-op-id` | pass | 23,913 | FCheck, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost, Timeout |

Union of the 14 runs:

```
module SealProtocol: 1657 expressions, 23 never evaluated, 23 maximal
actions with zero generated distinct states: ['FenceGroupDurable', 'FenceGroupRejected'] (of 32)
```

The 23 unevaluated expressions fall into four groups:

- `RsPrep`'s `OwedFinal` refusal (`prepare_execution`, `lifecycle.rs`
  633-638), unreached as in the previous revision. `InstallOnlyOverOwedClaim`
  holds, and the production guard stays as defence in depth.
- `APrep`'s pre-fix branch, where a plain append resumes an owed final and
  renews the claim. It is reachable only under the F6 control's substitution.
- The fence-row read failure (`MaxFenceReadFail`) and the staged fence group
  (`MaxHeldFence`, `FenceGroupDurable`, `FenceGroupRejected`). Every TLA-003
  shape sets both bounds to 0. TLA-002's `retire`, `engine` and `*-held`
  shapes reach them.
- `FClaim`'s pre-fix "capacity after renewal" answer is unreachable in the
  unmodified model: `FValidate` answers an owed retry over capacity before it
  reaches the renewal. TLC reports that `RespondH` call at the operator's
  definition, so the coverage union cannot show it separately. The F4 control
  reaches it.

### TLA-002 (not rerun)

TLA-002's eight safety baselines were not rerun with `-coverage`. Their state
counts in the recorded receipt equal the previous revision's, so their
reachable graphs are unchanged; the previous revision's per-run table
(action names unchanged) still describes them. Line numbers in
`SealProtocol.tla` moved.

| Coverage run (previous revision) | Verdict | Distinct states | Actions with no generated state (of 32) |
|---|---|---|---|
| `TLA-002/baseline-small` | pass | 895,724 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-faults` | pass | 3,875,480 | APrep, AReceive, Crash, FAnswer, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-expanded` | pass | 2,306,562 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-xproc` | pass | 523,650 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Replace, RsCloseFailed, TFenceLost, Timeout |
| `TLA-002/baseline-retire` | pass | 5,811,402 | APrep, AReceive, Cancel, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Timeout |
| `TLA-002/baseline-engine` | pass | 3,773,720 | APrep, AReceive, Cancel, FRawDup, FValidate, FenceGroupDurable, FenceGroupRejected, Timeout |
| `TLA-002/baseline-xproc-held` | pass | 1,210,271 | APrep, AReceive, Cancel, FAnswer, FRawDup, FValidate, Replace, RsCloseFailed, Timeout |
| `TLA-002/baseline-retire-held` | pass | 7,770,035 | APrep, AReceive, Cancel, FRawDup, FValidate, Timeout |

### TLA-001 (`RegistryCas.tla`, current model)

`baseline-small` (108,329 distinct states) and `baseline-expanded`
(7,643,540) were rerun with `-coverage 1`; both passed, and every action
generated states. The union leaves 10 expressions unevaluated:

- the missing-ETag overwrite branch (reachable only in
  `nc-missing-etag-overwrite`; production refuses a missing ETag,
  `registry.rs` 1195);
- a recreate that loses its CAS, with the conflict it returns (the instance
  has no concurrent creator);
- the two client-retry branches of `Cas` and `RecreateCas` (reachable only
  in `nc-client-retry*`: `CommittedAnsweredPrecondition` is `FALSE`).

## Files

| File | Contents |
|---|---|
| `RegistryCas.tla` | TLA-001 specification |
| `MC_RegistryCas.tla`, `MC_RegistryCas3.tla` | TLA-001 instances (small, expanded) |
| `MC_RegistryCas_Nc*.tla` | TLA-001 negative controls (`NcClientRetry`: F1 pre-fix) |
| `SealProtocol.tla` | TLA-002 and TLA-003 specification |
| `MC_SealTakeover.tla` | TLA-002 instances |
| `MC_SealTakeover_Nc*.tla` | TLA-002 negative controls (`NcEngineFence`: F1 pre-fix; `NcRefusalAtStaging`, `NcCacheSurvivesReject`: F2 pre-fix) |
| `MC_FinalSeal.tla` | TLA-003 instances |
| `MC_FinalSeal_Nc*.tla` | TLA-003 negative controls (`NcAdmissionRefusal`: F2 pre-fix; `NcAdmissionRefusalAnyRelease`: the code before the F2 fix, with its pre-F5 release; `NcCeilingAfterIntent`: F3 pre-fix; `NcRenewBeforeValidation`: F4 pre-fix; `NcAnyAttemptReleases`: F5 pre-fix; `NcPlainResumesOwed`: F6 pre-fix) |
| `*.cfg` | one configuration per check; `_nc_` controls, `_w_` witnesses (no known defect is open) |
| `evidence/` | TLC counterexample traces (`*.txt`) and per-step views (`*.summary.txt`); `pre-fix` in a name marks behaviour a later commit fixed |
