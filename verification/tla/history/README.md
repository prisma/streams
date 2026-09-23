# History tier models (group `history`): TLA-016, TLA-018, TLA-019

These TLA+ models cover the destructive-maintenance boundary of the history
tier. Records move from the shard log into the shared history-v2 partition,
the absorbed boundary is published, hot copies are trimmed, the postings-slice
cache is warmed, reads are served across the moving boundary, and storage
objects and fork pins are reclaimed. Each result is a bounded model check of
the recorded instance under the recorded assumptions (roadmap §1.2). The
models do not prove that the Rust code refines them, and they do not verify
upstream SlateDB.

The obligations, checks, bounds and assumptions are in
`verification/manifest.json`; the assumption entries are in
`verification/assumptions.md`; the receipts of the recorded runs are in
`verification/receipts/`.

## Status

| Obligation | Status | Production defects found | Open |
|---|---|---|---|
| TLA-016 | `pass-with-recorded-scope` | TLA-016-F1 (fixed): the absorbed advance retired the chunk's bytes, not the range it moved over. TLA-016-F3 (fixed): the postings warm install claimed coverage over a trimmed head it never read. | — |
| TLA-018 | `counterexample` | TLA-018-F1 (fixed): an applied keyed read skipped durable records trimmed by a non-durable advance. TLA-018-F3 (open). | TLA-018-F3: a stale applied cursor is accepted once the new owner's tail passes it. It needs a cursor-format decision. |
| TLA-019 | `pass-with-recorded-scope` | None. TLA-019-F1 was a model abstraction gap: the compactor's checkpoint protects the SSTs a stale writer view still names. | — |

The open defect has a `known-defect` check that must keep violating its
property. Each fixed defect has a passing baseline and a negative control that
reproduces the pre-fix behaviour. The group also records two specification
defects (TLA-016-F2, TLA-018-F2), two unjustified assumptions (TLA-019-F2,
TLA-019-F4), one scope gap (TLA-019-F3), and two defects found in passing by
the fixes that the model cannot express: overlapping postings pages after a
rescan rollback (open) and a cache bridge over dropped runs (fixed).

## How to run

```bash
python3 scripts/quality/formal.py check                     # manifest, files, config/property agreement
python3 scripts/quality/formal.py run --id TLA-016          # every check of one obligation
python3 scripts/quality/formal.py run --id TLA-018 --role witness
python3 scripts/quality/formal.py run --id TLA-019 --record # writes verification/receipts/TLA-019.json
```

One configuration directly, for a trace or with `-coverage 1` for per-action
counts:

```bash
java -XX:+UseParallelGC -Xmx3g -cp target/quality-tools/tla2tools.jar tlc2.TLC \
  -workers 2 -config verification/tla/history/<cfg> verification/tla/history/<MC_module>.tla
```

The tools are pinned in `quality-tools.toml` `[formal]`: TLC 2.19
(`tla2tools.jar` 1.7.4) on Java 11 or newer. The modules use only the standard
modules `Naturals`, `Sequences`, `FiniteSets` and `TLC`. No configuration uses
symmetry, a state constraint or an action constraint, and no run disables
deadlock checking. Legitimate quiescence is an explicit stuttering
`Terminated` step guarded by a `Settled` predicate.

Configuration names say what a check is:

| Prefix | Role | Meaning |
|---|---|---|
| none | `baseline` | The unmodified model must pass (a complete search). Liveness configurations are baselines too. |
| `kd_` | `known-defect` | The unmodified model must keep violating the named property while the production defect is open. |
| `nc_` | `negative-control` | One operator is replaced (`CONSTANT Op <- MutOp`) by a broken variant from the `MC_*` module; the named property must fail. |
| `probe_` | `negative-control` | As `nc_`, but the replaced operator is a dependency contract or client premise, not production code. It shows the premise is load-bearing. |
| `w_` | `witness` | The named `Witness_*` invariant must be violated: the behaviour is reachable. |

<!-- RESULTS -->
## Results

Each table is rendered from the obligation's receipt. The driver ran every
check with 2 workers while other TLC and Kani jobs shared the 8-core machine,
so the seconds are wall time under load. For a violation, the distinct states
are those explored until TLC found it. `(temporal)` marks a liveness
violation, which TLC reports without the property name; each such
configuration checks one property.

### TLA-016 (`pass-with-recorded-scope`)

Receipt `verification/receipts/TLA-016.json`: 33 checks, every verdict as expected, run on `ab73296` with uncommitted changes; TLC 2.19 on Java 17.0.1, 2 workers; 52 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-small` | `HistoryAbsorb` / `small` | baseline | pass | pass | 8,380,652 | 691 |
| `baseline-expanded` | `HistoryAbsorb` / `expanded` | baseline | pass | pass | 16,566,674 | 1525 |
| `ledger-small` | `HistoryAbsorb` / `ledger_small` | baseline | pass | pass | 8,380,652 | 429 |
| `ledger-overlap` | `HistoryAbsorb` / `ledger_overlap` | baseline | pass | pass | 579,554 | 22 |
| `liveness-small` | `HistoryAbsorb` / `liveness` | baseline | pass | pass | 134,989 | 62 |
| `baseline-cache` | `HistoryAbsorb` / `cache` | baseline | pass | pass | 2,338,359 | 98 |
| `baseline-cache-cap2` | `HistoryAbsorb` / `cache_cap2` | baseline | pass | pass | 4,234,784 | 166 |
| `nc-publish-before-flush` | `HistoryAbsorb` / `nc_publish_before_flush` | negative-control | violation `H3_AbsorbedBackedByDurableHistory` | violation `H3_AbsorbedBackedByDurableHistory` | 466 | 1 |
| `nc-canonical-only` | `HistoryAbsorb` / `nc_canonical_only` | negative-control | violation `H3_AbsorbedBackedByDurableHistory` | violation `H3_AbsorbedBackedByDurableHistory` | 629 | 1 |
| `nc-trim-to-proposed` | `HistoryAbsorb` / `nc_trim_to_proposed` | negative-control | violation `StaleReaderRangeIntact` | violation `StaleReaderRangeIntact` | 1,084 | 2 |
| `nc-duplicate-collapse` | `HistoryAbsorb` / `nc_duplicate_collapse` | negative-control | violation `StaleReaderRangeIntact` | violation `StaleReaderRangeIntact` | 17,863 | 2 |
| `nc-tick-trim-to-absorbed` | `HistoryAbsorb` / `nc_tick_trim_to_absorbed` | negative-control | violation `StaleReaderRangeIntact` | violation `StaleReaderRangeIntact` | 1,748 | 2 |
| `nc-lastcopy-publish-before-flush` | `HistoryAbsorb` / `nc_lastcopy_publish_before_flush` | negative-control | violation `LastRecoverableCopy` | violation `LastRecoverableCopy` | 18,465 | 2 |
| `nc-lastcopy-trim-past-absorbed` | `HistoryAbsorb` / `nc_lastcopy_trim_past_absorbed` | negative-control | violation `LastRecoverableCopy` | violation `LastRecoverableCopy` | 11 | 1 |
| `nc-retire-chunk-bytes` | `HistoryAbsorb` / `nc_retire_chunk_bytes` | negative-control | violation `LedgerExact` | violation `LedgerExact` | 17,286 | 2 |
| `nc-retire-chunk-bytes-overlap` | `HistoryAbsorb` / `nc_retire_chunk_bytes_overlap` | negative-control | violation `LedgerExact` | violation `LedgerExact` | 9,391 | 2 |
| `nc-retire-chunk-bytes-liveness` | `HistoryAbsorb` / `nc_retire_chunk_bytes_liveness` | negative-control | violation `AbsorptionCompletes` | violation (temporal) | 150,281 | 62 |
| `nc-warm-install-from-plan` | `HistoryAbsorb` / `nc_warm_install_from_plan` | negative-control | violation `CacheNeverProvesFalseAbsence` | violation `CacheNeverProvesFalseAbsence` | 94,714 | 5 |
| `witness-TrimDurable` | `HistoryAbsorb` / `w_TrimDurable` | witness | violation `Witness_TrimDurable` | violation `Witness_TrimDurable` | 27,236 | 3 |
| `witness-FullyAbsorbedDurable` | `HistoryAbsorb` / `w_FullyAbsorbedDurable` | witness | violation `Witness_FullyAbsorbedDurable` | violation `Witness_FullyAbsorbedDurable` | 40,777 | 3 |
| `witness-LostPublicationRecovered` | `HistoryAbsorb` / `w_LostPublicationRecovered` | witness | violation `Witness_LostPublicationRecovered` | violation `Witness_LostPublicationRecovered` | 260,308 | 8 |
| `witness-FlushFailRecovered` | `HistoryAbsorb` / `w_FlushFailRecovered` | witness | violation `Witness_FlushFailRecovered` | violation `Witness_FlushFailRecovered` | 141,440 | 6 |
| `witness-StaleDuplicateIgnored` | `HistoryAbsorb` / `w_StaleDuplicateIgnored` | witness | violation `Witness_StaleDuplicateIgnored` | violation `Witness_StaleDuplicateIgnored` | 308,748 | 10 |
| `witness-CrashRecovered` | `HistoryAbsorb` / `w_CrashRecovered` | witness | violation `Witness_CrashRecovered` | violation `Witness_CrashRecovered` | 53,357 | 3 |
| `witness-GatherOverTrimmedPrefix` | `HistoryAbsorb` / `w_GatherOverTrimmedPrefix` | witness | violation `Witness_GatherOverTrimmedPrefix` | violation `Witness_GatherOverTrimmedPrefix` | 97,824 | 5 |
| `witness-StaleSnapshotLosesTail` | `HistoryAbsorb` / `w_StaleSnapshotLosesTail` | witness | violation `Witness_StaleSnapshotLosesTail` | violation `Witness_StaleSnapshotLosesTail` | 36,490 | 3 |
| `witness-TwoAdvancesNotDurable` | `HistoryAbsorb` / `w_TwoAdvancesNotDurable` | witness | violation `Witness_TwoAdvancesNotDurable` | violation `Witness_TwoAdvancesNotDurable` | 16,461 | 2 |
| `witness-MarkRolledBackInFlight` | `HistoryAbsorb` / `w_MarkRolledBackInFlight` | witness | violation `Witness_MarkRolledBackInFlight` | violation `Witness_MarkRolledBackInFlight` | 692 | 1 |
| `witness-EvictedMarkPrunedInFlight` | `HistoryAbsorb` / `w_EvictedMarkPrunedInFlight` | witness | violation `Witness_EvictedMarkPrunedInFlight` | violation `Witness_EvictedMarkPrunedInFlight` | 700 | 1 |
| `witness-RingGatherBelowTrim` | `HistoryAbsorb` / `w_RingGatherBelowTrim` | witness | violation `Witness_RingGatherBelowTrim` | violation `Witness_RingGatherBelowTrim` | 155,100 | 8 |
| `witness-WarmBridgeCovers` | `HistoryAbsorb` / `w_WarmBridgeCovers` | witness | violation `Witness_WarmBridgeCovers` | violation `Witness_WarmBridgeCovers` | 10,867 | 2 |
| `witness-MisStartedAdvanceCompletes` | `HistoryAbsorb` / `w_MisStartedAdvanceCompletes` | witness | violation `Witness_MisStartedAdvanceCompletes` | violation `Witness_MisStartedAdvanceCompletes` | 40,536 | 3 |
| `witness-InstallStartsAbovePlan` | `HistoryAbsorb` / `w_InstallStartsAbovePlan` | witness | violation `Witness_InstallStartsAbovePlan` | violation `Witness_InstallStartsAbovePlan` | 77,829 | 5 |

### TLA-018 (`counterexample`)

Receipt `verification/receipts/TLA-018.json`: 26 checks, every verdict as expected, run on `ab73296` with uncommitted changes; TLC 2.19 on Java 17.0.1, 2 workers; 77 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-durable-keyed-small` | `ReadCompose` / `durable_keyed_small` | baseline | pass | pass | 12,482,967 | 1294 |
| `baseline-durable-unfiltered-small` | `ReadCompose` / `durable_unfiltered_small` | baseline | pass | pass | 3,777,005 | 294 |
| `baseline-applied-keyed-small` | `ReadCompose` / `applied_keyed_small` | baseline | pass | pass | 11,351,025 | 541 |
| `baseline-applied-unfiltered-small` | `ReadCompose` / `applied_unfiltered_small` | baseline | pass | pass | 3,045,185 | 101 |
| `baseline-durable-keyed-expanded` | `ReadCompose` / `durable_keyed_expanded` | baseline | pass | pass | 19,356,657 | 668 |
| `baseline-durable-unfiltered-expanded` | `ReadCompose` / `durable_unfiltered_expanded` | baseline | pass | pass | 6,582,468 | 519 |
| `baseline-applied-unfiltered-expanded` | `ReadCompose` / `applied_unfiltered_expanded` | baseline | pass | pass | 18,856,204 | 1009 |
| `known-defect-stale-applied-cursor` | `ReadCompose` / `kd_stale_applied_cursor` | known-defect | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 2,386,543 | 100 |
| `nc-old-history-view` | `ReadCompose` / `nc_old_history_view` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 565,256 | 30 |
| `nc-filtered-race-never` | `ReadCompose` / `nc_filtered_race_never` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 197,406 | 10 |
| `nc-short-index-accepted` | `ReadCompose` / `nc_short_index_accepted` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 8,231 | 3 |
| `nc-applied-race-remote` | `ReadCompose` / `nc_applied_race_remote` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 48,792 | 5 |
| `nc-applied-race-remote-unfiltered` | `ReadCompose` / `nc_applied_race_remote_unfiltered` | negative-control | violation `TailGapExplained` | violation `TailGapExplained` | 43,613 | 4 |
| `probe-lost-durable-postings` | `ReadCompose` / `probe_lost_postings` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 7,256 | 2 |
| `probe-lost-durable-canonical` | `ReadCompose` / `probe_lost_canonical` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 9,098 | 2 |
| `witness-BoundaryRaceAdopted` | `ReadCompose` / `w_BoundaryRaceAdopted` | witness | violation `Witness_BoundaryRaceAdopted` | violation `Witness_BoundaryRaceAdopted` | 9,627 | 3 |
| `witness-UnfilteredRaceAdopted` | `ReadCompose` / `w_UnfilteredRaceAdopted` | witness | violation `Witness_BoundaryRaceAdopted` | violation `Witness_BoundaryRaceAdopted` | 155,709 | 10 |
| `witness-AppliedRaceAdopted` | `ReadCompose` / `w_AppliedRaceAdopted` | witness | violation `Witness_AppliedRaceAdopted` | violation `Witness_AppliedRaceAdopted` | 4,113 | 2 |
| `witness-LargeFirstRecordDelivered` | `ReadCompose` / `w_LargeFirstRecordDelivered` | witness | violation `Witness_LargeFirstRecordDelivered` | violation `Witness_LargeFirstRecordDelivered` | 134 | 1 |
| `witness-EnvelopeServed` | `ReadCompose` / `w_EnvelopeServed` | witness | violation `Witness_EnvelopeServed` | violation `Witness_EnvelopeServed` | 1,230 | 2 |
| `witness-ShortIndexPartial` | `ReadCompose` / `w_ShortIndexPartial` | witness | violation `Witness_ShortIndexPartial` | violation `Witness_ShortIndexPartial` | 1,445 | 2 |
| `witness-ReadFromFencedEngine` | `ReadCompose` / `w_ReadFromFencedEngine` | witness | violation `Witness_ReadFromFencedEngine` | violation `Witness_ReadFromFencedEngine` | 369 | 2 |
| `witness-RingServed` | `ReadCompose` / `w_RingServed` | witness | violation `Witness_RingServed` | violation `Witness_RingServed` | 113 | 2 |
| `witness-ReaderCompletes` | `ReadCompose` / `w_ReaderCompletes` | witness | violation `Witness_ReaderCompletes` | violation `Witness_ReaderCompletes` | 3,720 | 2 |
| `witness-TrimBelowReaderCursor` | `ReadCompose` / `w_TrimBelowReaderCursor` | witness | violation `Witness_TrimBelowReaderCursor` | violation `Witness_TrimBelowReaderCursor` | 10,489 | 3 |
| `witness-ReadErrorCurrentEngine` | `ReadCompose` / `w_ReadErrorCurrentEngine` | witness | violation `Witness_ReadErrorCurrentEngine` | violation `Witness_ReadErrorCurrentEngine` | 43 | 2 |

### TLA-019 (`pass-with-recorded-scope`)

Receipt `verification/receipts/TLA-019.json`: 33 checks, every verdict as expected, run on `ab73296` with uncommitted changes; TLC 2.19 on Java 17.0.1, 2 workers; 32 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-small` | `ReachGC` / `small` | baseline | pass | pass | 3,938,288 | 96 |
| `baseline-expanded` | `ReachGC` / `expanded` | baseline | pass | pass | 27,990,301 | 616 |
| `baseline-timing-lapse` | `ReachGC` / `lapse` | baseline | pass | pass | 3,594,585 | 86 |
| `liveness-small` | `ReachGC` / `liveness` | baseline | pass | pass | 637,394 | 119 |
| `liveness-expanded` | `ReachGC` / `liveness_expanded` | baseline | pass | pass | 4,826,314 | 822 |
| `nc-no-compaction-checkpoint` | `ReachGC` / `nc_no_compaction_checkpoint` | negative-control | violation `LiveReadViewProtected` | violation `LiveReadViewProtected` | 257,708 | 4 |
| `nc-advance-on-upload` | `ReachGC` / `nc_advance_on_upload` | negative-control | violation `HistoryBacked` | violation `HistoryBacked` | 148 | 1 |
| `nc-swallow-read-error` | `ReachGC` / `nc_swallow_read_error` | negative-control | violation `NoFalseCompleteRead` | violation `NoFalseCompleteRead` | 451,553 | 6 |
| `probe-upstream-short-read` | `ReachGC` / `probe_upstream_short_read` | negative-control | violation `NoFalseCompleteRead` | violation `NoFalseCompleteRead` | 482,475 | 6 |
| `nc-no-generation-condition` | `ReachGC` / `nc_no_generation` | negative-control | violation `ManifestRefsPresent` | violation `ManifestRefsPresent` | 24,456 | 2 |
| `nc-ignore-checkpoint-pin` | `ReachGC` / `nc_ignore_checkpoint_pin` | negative-control | violation `CheckpointPinned` | violation `CheckpointPinned` | 1,119,614 | 12 |
| `nc-stale-inventory` | `ReachGC` / `nc_stale_inventory` | negative-control | violation `EligibleEventuallyReclaimed` | violation (temporal) | 566,760 | 72 |
| `witness-CompactedInputReclaimed` | `ReachGC` / `w_CompactedInputReclaimed` | witness | violation `Witness_CompactedInputReclaimed` | violation `Witness_CompactedInputReclaimed` | 573,358 | 7 |
| `witness-OrphanReclaimed` | `ReachGC` / `w_OrphanReclaimed` | witness | violation `Witness_OrphanReclaimed` | violation `Witness_OrphanReclaimed` | 180,308 | 3 |
| `witness-HistoryServedAfterReclaim` | `ReachGC` / `w_HistoryServedAfterReclaim` | witness | violation `Witness_HistoryServedAfterReclaim` | violation `Witness_HistoryServedAfterReclaim` | 1,190,546 | 13 |
| `witness-ReaderViewErrors` | `ReachGC` / `w_ReaderViewErrors` | witness | violation `Witness_ReaderViewErrors` | violation `Witness_ReaderViewErrors` | 526,801 | 6 |
| `witness-StaleWriterViewRead` | `ReachGC` / `w_StaleWriterViewRead` | witness | violation `Witness_StaleWriterViewRead` | violation `Witness_StaleWriterViewRead` | 668 | 1 |
| `witness-QuietDeadZoneRetains` | `ReachGC` / `w_QuietDeadZoneRetains` | witness | violation `Witness_QuietDeadZoneRetains` | violation `Witness_QuietDeadZoneRetains` | 22,535 | 2 |
| `witness-LateCommitAfterGcView` | `ReachGC` / `w_LateCommitAfterGcView` | witness | violation `Witness_LateCommitAfterGcView` | violation `Witness_LateCommitAfterGcView` | 2,030 | 1 |
| `witness-CheckpointProtectsReadView` | `ReachGC` / `w_CheckpointProtectsReadView` | witness | violation `Witness_CheckpointProtectsReadView` | violation `Witness_CheckpointProtectsReadView` | 125,897 | 2 |
| `fork-baseline` | `ForkPin` / `baseline` | baseline | pass | pass | 16,900 | 1 |
| `fork-liveness-client-retries` | `ForkPin` / `liveness_client_retries` | baseline | pass | pass | 16,900 | 3 |
| `nc-ignore-fork-pin` | `ForkPin` / `nc_ignore_fork_pin` | negative-control | violation `ForkPinRespected` | violation `ForkPinRespected` | 67 | 1 |
| `nc-install-ignores-incarnation` | `ForkPin` / `nc_install_ignores_incarnation` | negative-control | violation `ForkPinRespected` | violation `ForkPinRespected` | 279 | 1 |
| `probe-no-client-retry` | `ForkPin` / `probe_no_client_retry` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 16,900 | 2 |
| `witness-fork-SoftDeleteRetainedForFork` | `ForkPin` / `w_SoftDeleteRetainedForFork` | witness | violation `Witness_SoftDeleteRetainedForFork` | violation `Witness_SoftDeleteRetainedForFork` | 152 | 1 |
| `witness-fork-TwoChildrenPinSource` | `ForkPin` / `w_TwoChildrenPinSource` | witness | violation `Witness_TwoChildrenPinSource` | violation `Witness_TwoChildrenPinSource` | 3,116 | 1 |
| `witness-fork-ForkCascadeTombstone` | `ForkPin` / `w_ForkCascadeTombstone` | witness | violation `Witness_ForkCascadeTombstone` | violation `Witness_ForkCascadeTombstone` | 157 | 1 |
| `witness-fork-InstallAfterChildDeleted` | `ForkPin` / `w_InstallAfterChildDeleted` | witness | violation `Witness_InstallAfterChildDeleted` | violation `Witness_InstallAfterChildDeleted` | 54 | 1 |
| `witness-fork-InstallDeclinedOnRecreatedSource` | `ForkPin` / `w_InstallDeclinedOnRecreatedSource` | witness | violation `Witness_InstallDeclinedOnRecreatedSource` | violation `Witness_InstallDeclinedOnRecreatedSource` | 134 | 1 |
| `witness-fork-DebtClearedOnRecreatedSource` | `ForkPin` / `w_DebtClearedOnRecreatedSource` | witness | violation `Witness_DebtClearedOnRecreatedSource` | violation `Witness_DebtClearedOnRecreatedSource` | 280 | 1 |
| `witness-fork-PermanentPinWithoutRetry` | `ForkPin` / `w_PermanentPinWithoutRetry` | witness | violation `Witness_PermanentPinWithoutRetry` | violation `Witness_PermanentPinWithoutRetry` | 333 | 1 |
| `witness-fork-PinAfterSuccessfulDelete` | `ForkPin` / `w_PinAfterSuccessfulDelete` | witness | violation `Witness_PinAfterSuccessfulDelete` | violation `Witness_PinAfterSuccessfulDelete` | 244 | 1 |
<!-- /RESULTS -->

## Findings

| ID | Classification | Disposition |
|---|---|---|
| TLA-016-F1 | production defect | **Fixed** by "An absorption advance retires exactly the bytes of the range it moves the boundary over" (`6371da0`). Baselines `ledger-small`, `ledger-overlap`, `liveness-small` pass; controls `nc-retire-chunk-bytes*` reproduce the pre-fix behaviour. |
| TLA-016-F3 | production defect (latent) | **Fixed** by "A re-gather warms the postings cache only over the rows it staged": the warm install is named by the rows the gather staged (`src/history/gather.rs`). `baseline-cache` passes; `nc-warm-install-from-plan` reproduces the pre-fix behaviour. |
| TLA-016-F2 | specification defect (documentation) | Recorded. The `trim_safe_to` comments overstate what the one-advance lag protects. |
| (cache bridge) | production defect, found in passing during the TLA-016-F3 fix | **Fixed** by "A postings-cache bridge never crosses a chunk whose runs no slice recorded". Outside what the model can express (admission line, capped and merging loads); covered by real-code regressions. |
| TLA-018-F1 | production defect | **Fixed** by "An applied read revalidates its tail scan at the level it scanned, so it never skips a durable record" (`9cea1b6`). `baseline-applied-keyed-small` passes; controls `nc-applied-race-remote*` reproduce the pre-fix behaviour. |
| TLA-018-F3 | production defect | **Open; needs an owner decision** on the cursor format. `known-defect-stale-applied-cursor` violates `ExactDurablePrefix`. Real-code reproduction: `verification/regressions/TLA-018-F3/README.md`. |
| TLA-018-F2 | specification defect | Recorded. H11 is not enforced by the reader; it holds only through durability and the cache contract. |
| TLA-019-F1 | abstraction gap (not reproduced) | **Withdrawn as a defect.** The model lacked the compactor's checkpoint. With it, `LiveReadViewProtected` passes under ASM-SLATEDB-COMPACTION-CHECKPOINT; `nc-no-compaction-checkpoint` reproduces the earlier counterexample. |
| TLA-019-F4 | unjustified assumption | Recorded. Releasing a fork pin after an interrupted or raced `DELETE` depends on the client repeating `DELETE`. |
| TLA-019-F2 | unjustified assumption | Recorded. H14 convergence holds only while the partition keeps writing. |
| TLA-019-F3 | scope gap (owner question) | Recorded. There is no physical reclamation policy for a hard-deleted incarnation's rows. |

### TLA-016-F1 — the absorbed advance retired the chunk's bytes, not the range it moved over (fixed)

**Defect.** `CommitOp::Absorbed` carried the byte count of the gather's chunk
`[plan.from, upto)`. `CommitTransaction::absorbed` retired it against
`[prev_absorbed, upto)` without knowing where the chunk started. The two
differ when the lane mark and the committed boundary disagree:

- *Under-retirement.* The group carrying an advance is refused, and the next
  gather plans from the raised lane mark. The advance `0 → 2` retires only
  `[1, 2)`'s bytes. Nothing repairs the phantom backlog, and it feeds
  maintenance backpressure.
- *Over-retirement.* A rescan rolls a lane mark back, or the sweep prunes it
  for an evictable handle, while the advance that raised it is still queued.
  The re-gather covers more than the queued chunk and retires those bytes
  again. The understated ledger then refuses every advance that reaches
  `next` as "maintenance accounting diverged", together with every other
  operation in the group. A quiet stream never absorbs its last record.

**Fix, as modelled.** Each advance carries its chunk start. When
`from == prev_absorbed` the chunk's bytes are exact and trusted. Otherwise the
committer sums the stored record rows of `[prev_absorbed, min(upto, next))`
at Memory level (`stored_frame_bytes`; `ScanOptions::default()` is Memory in
the pinned SlateDB), and a failed read or a missing row refuses the group. In
the model this is `RetireBytes` and `StoredFrameBytes`. The model shows that
the trusted count is exact: `LedgerExact` holds in every tail view, with
Remote-scan gathers that skip trimmed rows and with ring gathers that return
rows below the trim point. `NoMissingRetiredRow` shows that the new refusal
fires only on a failed read (a transient error, covered by `CommitReject`),
never on a missing row, so it cannot stall absorption.

**Checks.** `ledger-small` (under-retirement shape), `ledger-overlap`
(over-retirement shape with no lost publication), `liveness-small` and
`baseline-expanded` (ring gathers, with `LedgerExact`) pass. The pre-fix
behaviour is `MutRetireChunkBytes`; `nc-retire-chunk-bytes` and
`nc-retire-chunk-bytes-overlap` violate `LedgerExact`, and
`nc-retire-chunk-bytes-liveness` violates `AbsorptionCompletes`.
`witness-MisStartedAdvanceCompletes` shows an advance whose chunk did not
start at the boundary retiring the stored bytes, followed by complete
absorption.

**Regressions.**
`dst::dst_tests::billing_maintenance::refused_absorbed_chunk_leaves_no_phantom_backlog`,
`history::bounded_discovery_tests::rolled_back_mark_regather_keeps_the_ledger_exact`
and
`dst::dst_tests::history_absorption::misaligned_absorbed_chunk_retires_stored_bytes_or_refuses_the_group`.

**Pre-fix evidence.** `evidence/TLA-016_ledger_under_retire.trace.txt`,
`evidence/TLA-016_ledger_over_retire.trace.txt` and
`evidence/TLA-016_liveness_stall.trace.txt`, recorded on the pre-fix model.

**Recorded, not fixed.** The fix found that a rescan rollback followed by a
re-gather can leave overlapping postings pages on its own, which a keyed
reader then reads as corrupt. The model abstracts postings pages to per-offset
coverage, so it cannot express page overlap (see the TLA-016 exclusions).

The first version of this model proposed retiring `newAbs − prev` directly.
The landed fix reads the stored rows instead, and the model now follows the
landed code. A rejected fix design that ignored mis-started advances is not
modelled.

### TLA-016-F3 — the warm install claimed postings coverage over a trimmed head it never read (fixed)

**Defect.** `stage_chunk` recorded the write-through cache install as
`(segment, plan.from, last + 1, runs)`, but the runs came only from the
frames the scan returned. A Remote scan skips rows below `trimmed`, so after
a stale plan the first frame can lie above `plan.from`. `install_chunk` then
restarted the warm window (`w.to != chunk_from`) and gave a key whose slice
had been evicted a fresh slice covering `[plan.from, last + 1)` without the
trimmed head's records. `runs_for` served that slice as a `Hit`, so a keyed
read, `deliver=durable` included, received a complete page without a durable
record and its cursor moved past it.

**Fix, as modelled.** `stage_rows` returns the range of offsets it staged,
and `stage_chunk` names the install by that range
(`src/history/gather.rs`, `stage_rows` and `stage_chunk`). The staged rows
are dense: a ring hit proves its window dense, and a Remote scan reads one
snapshot (ASM-SLATEDB-DURABLE (j)). The advance still carries `plan.from`;
the committer recounts stored bytes whenever that differs from the boundary
(TLA-016-F1). `install_chunk`'s eviction taint was not changed: it is not
what protects correctness, because the idle sweep can drop a segment's warm
record along with its slices, and the defect reproduced without a taint. In
the model the install start is the operator `WarmInstallFrom`, now the first
staged offset, and the gather's Remote scan reads the snapshot taken when it
starts.

**Checks.** `baseline-cache` (one-record gathers) and `baseline-cache-cap2`
(two-record gathers, so an install names a multi-row range) check
`CacheNeverProvesFalseAbsence` with every other safety property and pass.
The pre-fix behaviour is `MutWarmInstallFromPlan`;
`nc-warm-install-from-plan` violates
`CacheNeverProvesFalseAbsence` (`evidence/TLA-016_cache_false_absence.trace.txt`,
recorded on the pre-fix model). `witness-InstallStartsAbovePlan` shows a
re-gather whose scan skipped a trimmed head warming the cache over its staged
rows only.

**Regressions.**
`history::bounded_discovery_tests::stale_regather_never_warms_a_trimmed_head_as_absent`
drives the real absorber: two queued one-record gathers, a mark rollback,
both advances durable with row 0 trimmed, an idle sweep that evicts the
slice, and a stale re-gather that reads only row 1. On the unfixed code a
durable keyed read from 0 returned `[1]` as a complete page with cursor 2.
`postings_cache::tests::a_regather_install_after_an_eviction_proves_nothing_below_its_rows`
pins the cache contract: an install named from the first staged offset
still sends the read to the store after a gap reset.

**Found in passing during the F3 fix: a bridge could cross a chunk whose
runs no slice recorded (fixed).** The warm window `[from, to)` is the absence
proof for the demand bridge in `runs_for` and the install bridge in
`install_chunk`. Two install paths dropped a key's runs and left the window
clean: over the admission line a fresh install was skipped (only
`admitted_all`, which fresh claims read, recorded that), and a chunk carrying
a key whose slice ended below the window start dropped that key's runs (for
example a slice from a capped cold load, with the window starting past it
after a restart). A later load ending inside the dropped chunk was then
bridged over the key's records. Reproduced through the real absorber: a
durable keyed read returned `[]` where `[1]` was owed, as a complete page.
Fixed by "A postings-cache bridge never crosses a chunk whose runs no slice
recorded": `admitted_all` is gone, and any install that drops a key's runs
raises the window's `from` past its chunk, so the unchanged bridge conditions
refuse a bridge that starts below it. Regressions:
`postings_cache::tests::a_demand_bridge_never_crosses_an_unadmitted_install`,
`postings_cache::tests::an_install_bridge_never_crosses_an_unadmitted_install`,
`postings_cache::tests::a_bridge_never_crosses_a_chunk_that_found_its_key_short`
and
`history::bounded_discovery_tests::a_warm_bridge_never_crosses_an_unadmitted_install`.
The model could not find this: it has no admission line (every fresh install
is admitted), no capped loads and no loads that merge into a resident slice.
`InstallChunk` now raises `from` when it drops a key's runs, as the code
does. The drop path is reachable in the cache shapes, but a scratch run of
`cache` and `cache_cap2` without the raise also passed (2,327,172 and
4,208,811 distinct states, against 2,338,359 and 4,234,784 with it), so no
configuration here distinguishes the two and there is no negative control
for this fix.

### TLA-016-F2 — the `trim_safe_to` comments only hold for one-advance-stale readers (specification defect)

`src/shard.rs:64-67` and the `TailFields::trim_safe_to` comment
(`src/shard.rs:601-607`) say the one-advance lag means "in-flight readers
holding a stale absorbed snapshot never lose their range".
`StaleReaderRangeIntact` (a snapshot at most one advance stale) passes, but
`witness-StaleSnapshotLosesTail` shows that a reader whose snapshot is two
or more advances stale finds part of its tail range trimmed. Reads stay correct
because every tail page is revalidated against the absorbed boundary at the
scan's own visibility (TLA-018; the applied path gained that check with the
TLA-018-F1 fix). The comments should say that the lag is defence in depth and
that the read's revalidation carries the guarantee. No code regression is
needed.

### TLA-018-F1 — `deliver=applied` keyed reads skipped durable records trimmed by a non-durable advance (fixed)

**Defect.** An applied tail scan runs at Memory level, so it sees the row
deletes of a trim that an applied, not yet durable, absorption advance staged.
`absorption_race` checked the scan only against the Remote-durable absorbed
boundary. With two one-chunk advances applied but not durable, an applied
keyed read from 0 delivered records 1 and 2 and moved both cursors past the
durable, acknowledged record 0. The unfiltered applied branch saw the hole
but answered an honest partial with no progress.

**Fix, as modelled.** `Deliver::durability()` is the one mapping used by the
scan and by `ShardEngine::visible_absorbed` (`src/shard/record.rs:212-217`,
`:233-256`), so the race check reads the absorbed boundary at the scan's own
level. An applied scan that sees an applied trim adopts the applied boundary
and re-serves the prefix from history, which the gather flushed before it
submitted the advance. In the model this is `RaceBoundary`.

**Checks.** `baseline-applied-keyed-small` now passes. `TailGapExplained`
(every tail gap is explained by the boundary at the scan's visibility) holds
in every baseline, applied ones included. The pre-fix behaviour is
`MutRaceBoundaryRemote`: `nc-applied-race-remote` violates
`ExactDurablePrefix` (the keyed skip) and `nc-applied-race-remote-unfiltered`
violates `TailGapExplained` (the stalled unfiltered partial).
`witness-AppliedRaceAdopted` shows an applied read adopting a boundary above
the Remote-durable one and re-serving the trimmed prefix from history. The
former witness `UnexplainedGapPartial` is unreachable on the fixed model and
is replaced by `TailGapExplained` and the unfiltered control.

**Regression.**
`dst::dst_tests::reads_applied::applied_keyed_read_never_skips_rows_trimmed_by_a_non_durable_advance`.

**Pre-fix evidence.** `evidence/TLA-018_applied_keyed_skip.trace.txt`,
recorded on the pre-fix model.

### TLA-018-F3 — a stale applied cursor is refused only while the new tail is below it (open, needs a decision)

**Check.** `known-defect-stale-applied-cursor`
(`MC_ReadCompose_kd_stale_applied_cursor.cfg`) violates `ExactDurablePrefix`
(`evidence/TLA-018_applied_stale_cursor.trace.txt`). The old owner has record
0 durable and record 1 applied only. An applied read delivers record 1 as
pending and returns a session cursor past it. Ownership moves, and record 1
is lost with the old memtable. The new owner appends a different record 1 and
a record 2. The client continues from its session cursor, which the new
owner accepts because its tail is no longer below it. The next page delivers
record 2 and moves the durable cursor past offset 1, whose durable record the
client never received.

**Code path.** The only guard is
`if command.visibility == Deliver::Applied && start > end { return Err(CursorBeyondTail) }`
in `ReadService::execute_read` (`src/application/read_request.rs:291-293`),
where `end` is the current owner's end. A `KIND_KEY_V2` cursor carries no
owner incarnation and no durable frontier.
`a_stale_applied_cursor_is_refused_after_crash_restart`
(`src/dst/tests/reads_applied.rs`) presents the cursor before any new append,
so it never reaches the race.

**Real-code reproduction and decision.**
`verification/regressions/TLA-018-F3/README.md` quotes a test that appends two
records on the restarted server before presenting the stale cursor. It gets
`200` with `[{"n":20}]` and a durable cursor of 3 instead of
`409 cursor_beyond_tail`. The proposed design is a `KIND_KEY_V3` cursor that
binds an applied session cursor to the minting owner incarnation or the
durable frontier. It changes a persisted, client-visible format, so the owner
must decide what it carries, whether a stale cursor is refused or rewound, and
how existing `KIND_KEY_V2` cursors are treated.

### TLA-018-F2 — H11 is not enforced at the reader (specification defect)

The keyed reader treats zero postings pages as proof that a range has no
matches (`docs/ROUTING-V3.md`; `read_history2_keyed`, `src/history.rs:972`).
The unfiltered scan, the corruption envelope and `execute_postings_plan` all
skip a missing canonical row and still complete. The probes show the
consequence: `probe-lost-durable-postings` and `probe-lost-durable-canonical`
each produce a false complete page. Before their fixes, TLA-016-F3 and the
cache-bridge defect were production paths to the same observable. H11 ("missing postings cannot produce a false complete
result") therefore holds for corrupt pages (served through the envelope),
unproven load windows (honest partials) and postings never made durable (H3,
TLA-016). It does not hold for rows or pages lost after durability or for a
slice that proves false absence; those rest on ASM-SLATEDB-DURABLE,
ASM-SLATEDB-GC and ASM-HISTORY-POSTINGS-CACHE. The DST text of H11 should be
narrowed, or the index should carry a positive coverage marker.

### TLA-019-F1 — the writer's stale manifest view and GC (abstraction gap, not reproduced)

**What the first model found.** History reads use the writer `Db`'s
in-memory manifest view, which merges the stored manifest only on the
`PollManifest` tick (300 s) and in the conflict reload inside the writer's
own manifest write; the embedded compactor does not refresh it. The model
let the collector delete a compacted-away input that such a stale view still
named, so a read over it would fail until the next refresh. Two independent
refutation attempts from code reading failed.

**Why it does not happen.** The model omitted a step of the pinned SlateDB:
before every compaction commit the compactor writes a checkpoint on the
pre-compaction manifest with a 900 s lifetime (`compactor_state_protocols.rs`
`write_manifest`), and the collector treats every SST named by an unexpired
checkpoint's manifest as live (ASM-SLATEDB-GC (v)). Replaced SSTs therefore
outlive the compaction commit by 900 s, while the writer's poll refreshes
its view within 300 s. A real-code diagnostic at `ab73296` (a 1-byte cache,
a 20 ms compactor poll, four absorbed appends) observed exactly this: the
stored manifest had no L0s and one compacted run, the writer's view still
named four L0s, one compactor checkpoint with a 15-minute lifetime existed,
a collector pass with `min_age` 0 deleted nothing, and the read over the
stale view returned `[0, 1, 2, 3]`. After the checkpoint was deleted, the
collector removed the replaced SSTs, the same read failed with an
object-store `NotFound`, and `refresh_manifest()` restored it.

**Model now.** `CompactCommit` writes the checkpoint (`cck`), the collector
counts unexpired checkpoints (`CheckpointRefs`), the writer view is
refreshed within one tick (300 s) of going stale, and a read ends within one
tick (ASM-SLATEDB-COMPACTION-CHECKPOINT). `LiveReadViewProtected` is part of
`baseline-small` and `baseline-expanded` and passes.
`witness-CheckpointProtectsReadView` shows a collector pass that would
delete an input a live read still uses, but for the checkpoint.
`nc-no-compaction-checkpoint` removes the checkpoint and reproduces the
earlier counterexample (`evidence/TLA-019_reader_view_deleted.trace.txt`,
recorded on the model without the checkpoint).

**Residual.** The protection rests on an upstream interim constant (the
code's comment calls the 900 s lifetime temporary) and on the view being
refreshed and the read finishing within the lifetime. Repeated failed
manifest polls, or a read that holds its view for more than 600 s, would
break it. `baseline-timing-lapse` models that case with a one-tick
checkpoint: a read can then need a deleted SST (`witness-ReaderViewErrors`),
and it fails rather than completing short (`NoFalseCompleteRead` passes;
`nc-swallow-read-error` and `probe-upstream-short-read` show what would
break that). That is an availability risk, not data loss.

**Regressions.**
`dst::dst_tests::read_history_lifecycle::tla019_pin_history_scan_survives_compaction_gc_on_stale_view`
and
`dst::dst_tests::read_history_lifecycle::tla019_pin_keyed_history_read_survives_compaction_gc_on_stale_view`
pin the protection on the real code. After the compactor commits, the
writer's view still names the four replaced L0s; a checkpoint names all of
them with a lifetime (900 s observed) greater than twice the history
`manifest_poll_interval` (300 s, read from the partition settings); a
collector pass with compacted `min_age` 0 deletes nothing and the stale-view
read returns every record. As a control, deleting that checkpoint and
collecting again makes the same read fail with the object store's
`NotFound`, and `refresh_manifest` restores it.

### TLA-019-F4 — releasing a fork pin depends on a client repeating `DELETE` (unjustified assumption)

`fork-liveness-client-retries` passes only because `RetryDelete`, the
client, is weakly fair (the F8 premise). `probe-no-client-retry` drops that
premise and violates `RefEventuallyReleased`
(`evidence/TLA-019_fork_no_retry.trace.txt`, which takes the second path
below). There are two paths:

1. The child's `DELETE` dies after its tombstone CAS and before
   `release_fork_ref` (`witness-fork-PermanentPinWithoutRetry`). The client
   saw an error, so a retry is plausible.
2. The child's `DELETE` already returned success
   (`witness-fork-PinAfterSuccessfulDelete`,
   `evidence/TLA-019_fork_pin_after_successful_delete.trace.txt`). It ran
   while the creator was between its pre-check and the install CAS. The
   in-request release found the reference absent on a live source, which is
   inconclusive, so the tombstone kept its debt and `delete_lifecycle`
   returned `Ok(())`. The creator's install then landed and the creator died
   before its post-check. Only another `DELETE` of the already deleted child
   repairs the pin, and the client has no signal to send one.
   `dst::dst_tests::fork_cleanup::a_crashed_creators_late_reference_is_repaired_by_delete_retry`
   pins that repair and calls it "the retry the client already owns".

`repair_tombstone` has no other caller (`deletion.rs:273-274`, `:359`), and
`deletion.rs:355-358` says the only request a client will retry is the
original delete of the leaf. Until someone deletes the child again, the
source stays soft-deleted: its name cannot be recreated (F5) and its data is
retained. F8 ("deletion debt is recoverable by retrying the original public
operation") is met as written; the catalog's "eventually reclaimable" is not.
Owner question: add a background sweep of tombstones with
`parent_ref_pending`, or make the `DELETE` response report an inconclusive
release.

### TLA-019-F2 — H14 convergence holds only under continued write activity (unjustified assumption)

`witness-QuietDeadZoneRetains` is reachable. An unreferenced SST that is newer
than the most recent compaction start, or than the newest compacted L0,
survives every later collector pass on a partition that receives no further
flush or compaction. A fenced writer's orphan is an example.
`EligibleEventuallyReclaimed` therefore includes the upstream eligibility
premise in its antecedent; under it the liveness checks pass, and
`nc-stale-inventory` shows that a frozen inventory breaks them. H14 ("cannot
be suppressed by a … refresh dead zone") should say that the residue is
bounded but can last indefinitely without later activity.

### TLA-019-F3 — no physical reclamation policy for hard-deleted incarnations' rows (scope gap)

Hard deletion only writes a registry tombstone (`deletion.rs:465-479`). No
code deletes a deleted incarnation's shard-log or history rows; the only row
deletes are absorbed-boundary trims. The catalog's "eligible unreachable
objects eventually become reclaimable under the adopted policy" has no
adopted policy to check for these rows. This is an owner question, not a
defect claim.

---

## TLA-016 — History absorption, publication, and safe hot-data trimming

Modules `HistoryAbsorb.tla` and `MC_HistoryAbsorb.tla`.

### Claim

For one stream incarnation, the model checks that:

- (H3) the absorbed boundary, in every applied, durable and published view,
  covers only offsets whose canonical frames **and** postings are durable in
  history (`H3_AbsorbedBackedByDurableHistory`);
- the last recoverable copy is never trimmed: every record below `next` is in
  the shard log or durable in history, in the Remote-durable and the applied
  view (`LastRecoverableCopy`);
- (H4, guard check) `trimmed ≤ trim_safe_to ≤` the absorbed value before the
  most recent advance, and every tail view stays loadable by `stored_tail`
  (`H4_TrimWithinPreviousBoundary`, `StoredTailValid`);
- (guard check) a reader whose published snapshot is at most one advance
  stale finds its tail range intact (`StaleReaderRangeIntact`; older
  snapshots are not protected, F2);
- a lane mark never claims more than durable history
  (`MarkBackedByHistory`);
- durable and published frontiers are monotone (`FrontiersMonotone`);
- the per-stream `unabsorbed_bytes` ledger equals the bytes in
  `[absorbed, next)` in every view, even when absorption repeats, restarts
  from a stale boundary or loses a publication (`LedgerExact`), and the
  committer never finds a row of the range it retires missing
  (`NoMissingRetiredRow`);
- the postings-slice cache never proves the absence of a record that history
  holds below the durable absorbed boundary (`CacheNeverProvesFalseAbsence`;
  it failed before the TLA-016-F3 fix);
- (liveness) once faults cease, every appended record is absorbed and the
  advance is durable and published (`AbsorptionCompletes`).

These hold under crashes between every step, failed and ambiguous history
flushes, refused commit groups, the dirty-index rescan and rollback, marks
pruned for evicted handles, delayed `AbsorbedBatch` messages, stale re-plans
from the published boundary, trims between a gather's plan and its scan,
gathers served by the durable ring or by a Remote scan, per-stream caps of
one or two records, exhausted trim budgets, cache evictions and cold loads.

**Requirement anchors:** H1, H3, H4, H13; H2 and H11 through the cache claim.

### Observation boundary

The shard DB tail row is observed at four levels: `A`, the committer's
applied overlay base (`handle.state.applied`); `pend`, the applied but not
yet durable groups in WAL order; `D`, the Remote-durable prefix that
`DurabilityLevel::Remote` reads see; and `P`, the published
`handle.state.durable` written by `dispatch_durable`. History rows (canonical
`hc`, postings `hp`) are `none`, `mem` (in the WAL-less partition memtable)
or `dur`. The absorber's gather, lane mark, dirty-index observation and the
committer channel are volatile. The channel carries `(from, upto, bytes)`.
The postings-slice cache is modelled by the claims it makes: per key
`covered_from`, `indexed_to_offset` and the runs, and the segment's warm
record (`from`, `to`, `clean`). The reader is its published snapshot only;
the merged read is TLA-018.

### Atomicity / linearization table

| Model action | Production function(s) | Atomicity justification / dependency contract |
|---|---|---|
| `CustomerAppend` | `CommitTransaction::append` (`src/shard/transaction/append.rs:19`), `stage_stream_rows` and `write` (`finalize.rs:65`, `:170`), `publish` (`publish.rs:17`) | One `WriteBatch` per commit group, applied atomically (ASM-SLATEDB-DURABLE e). Groups carry one operation each; coalescing only removes crash points. |
| `WalDurable` | SlateDB WAL flush; `durable_seq` from `db.subscribe()` in `ShardEngine::acker_loop` (`src/shard.rs:3077`) | Remote durability is a prefix of applied order (ASM-SLATEDB-DURABLE f). |
| `Dispatch` | `ShardEngine::dispatch_durable` (`src/shard.rs:3006`) | Publishes `handle.state.durable` in group order under the handle mutex. May lag durability. |
| `Crash` | Process crash, engine close (including `write_failed` → `begin_close`, `finalize.rs:196-206`) or ownership move | Loses applied groups, the committer channel, lane marks and the partition memtable (WAL disabled, `history_settings`, `src/history.rs:499`). Keeps `D`, durable history rows and dirty rows. The process-wide postings cache is wiped (process crash) or kept (engine close or move); both are explored. |
| `HistoryBackgroundFlush` | SlateDB memtable flush (size-triggered, or the final flush in `Db::close`) | The whole memtable goes to L0 atomically (ASM-SLATEDB-DURABLE h). |
| `AbsorberPlan` | `Absorber::plan_reads` → `stream_handle` → `plan_read` (`src/history/gather.rs:374`, `:399`) | Reads `st.durable` (= `P`) and the lane mark under their mutexes: `from = max(mark, P.abs)`, `upto = P.next`. The ring-or-scan choice is made here. |
| `AbsorberRead`, `AbsorberReadEnd` | `read_wave` → `read_frames_range` (`gather.rs:431`; `src/shard/record.rs:129`) | A ring hit (`ring_read`, `record.rs:153`) returns the window densely, including rows a trim has deleted. Otherwise one `DurabilityLevel::Remote` scan (`record.rs:156-181`), observed row by row, over the snapshot taken when it starts (ASM-SLATEDB-DURABLE j): it skips the rows trimmed by then and no later ones. The per-stream byte cap is `Cap` equal-size records. |
| `AbsorberStage` | `stage_chunk` → `stage_rows` + `stage_postings` (`gather.rs:459`, `:100`, `:135`) | Canonical rows and postings pages go into one `WriteBatch`, atomic in the memtable. |
| `AbsorberFlushOk` / `AbsorberFlushFail` | `Absorber::commit`: `write_with_options`, then `part.flush()` (`gather.rs:540`, `:549`, `:563`); error path in `gather_due` (`src/history/worker.rs:398-414`) | Flush `Ok` means every earlier write is durable. `Err` is ambiguous: rows may or may not be durable. No install, no submit, no mark raise. |
| `AbsorberSubmit` (with `InstallChunk`) | After the flush: `postings_cache.install_chunk(inc, chunk_from, chunk_to, runs)` over the staged range `stage_rows` returned (`gather.rs:124`, recorded at `:504-506`; installed at `:574-577`; `src/postings_cache.rs:278`); then `submit_absorbed_batch_v2` with `(hash, plan.from, last + 1, chunk_raw)` (`gather.rs:507-510`, `:580`; `src/shard.rs:2015`); then `raise_lane_marks` (`gather.rs:582`) | One step: no await separates the install loop from the send. A crash while the send is blocked leaves claims about rows that are already durable, which is the kept-cache branch of `Crash`. The send is fire-and-forget. The install's start is the operator `WarmInstallFrom` (F3). |
| `RescanObserve`, `RescanRollback` | `seed_from_dirty_index` → `scan_dirty_streams_page` (Memory read, `src/shard.rs:2153`) → `roll_back_stranded_mark` (`gather.rs:193`, `:218`, `:274`) | Two steps: the committer runs between the row read and the rollback. Both run in the absorber task, never during a gather. |
| `MarkPrune` | `submitted.retain(pending ‖ resident && P.abs < mark)` in `Absorber::run` (`worker.rs:135-141`); `evict_idle_handles` (`src/shard.rs:2405`); reload in `stream_handle` (`src/shard.rs:2343`) | Atomic under the map mutex. Prunes when `P.abs ≥ mark` or the handle is evictable (ASM-HISTORY-EVICTION). A reloaded handle reads the Memory-level tail, which then equals `D` and `P`. The model ignores `pending`, so it prunes more often than production. |
| `CommitAbsorbed` | `CommitTransaction::absorbed` (`src/shard/transaction/maintenance.rs:213`) with `stored_frame_bytes` (`:309-345`), then `finish`/`write` | The advance, `trim_safe_to`, the budgeted trims (`:292-302`), `unabsorbed_bytes` and the dirty row go in one batch. A chunk that starts at the boundary retires its own bytes; any other retires the stored rows of `[prev_absorbed, min(upto, next))` read at Memory level (`:260-271`). A missing row or a `checked_sub` failure (`:273-281`) refuses the whole group (`finalize.rs:3-9`). The stored read does not see earlier writes of its own group, but those are trims below the boundary and appends at or above `next`, which never touch `[prev_absorbed, upto)`; one operation per group therefore loses nothing. |
| `CommitReject` | The operation carrying the advance never lands and the absorber is not told. Causes: a group refusal (closed engine or billing read failure in `CommitTransaction::run`, `src/shard/transaction/mod.rs:52-66`; another operation's accounting divergence; a failed `stored_frame_bytes` read; the test failpoint `fail_next_absorbed_group`), or `stage()` dropping only this op when `stream_handle` fails (`mod.rs:150-158`; `reject_op`'s `_ => {}`, `mod.rs:127`) | Whole group refused, or this op alone dropped. Either way the advance is lost silently. |
| `TrimStep` | `TrimTick` → `expand` (`prepare.rs:11`) → `CommitTransaction::trim` (`maintenance.rs:346-355`) | Budgeted deletes in one batch. |
| `CacheEvict` | Weight eviction at the end of `install_chunk` (`postings_cache.rs:449-468`), or the idle sweep | Removes one slice and taints the segment's warm window (`clean = false`). |
| `CacheLoad` | `runs_for` → `Decision::Lead` → `spawn_load` → `publish_load` (`postings_cache.rs`) | A cold load of all the key's pages up to the reader's absorbed boundary. Capped loads and loads that merge into a resident slice are not modelled. |
| `ReaderSnap` / `ReaderRelease` | The snapshot in `execute_segment` (`src/application/read.rs:129`) | Taken under the handle mutex. |

### Assumptions

ASM-SLATEDB-DURABLE, ASM-HISTORY-ACTORS (liveness), ASM-HISTORY-REABSORB and
ASM-HISTORY-EVICTION. The cache property checks ASM-HISTORY-POSTINGS-CACHE for
the install path; it does not assume it.

### Constants per configuration

Every configuration uses `N = 3`, `MaxChan = 2`, `TrimBudgets = {0,1,3}` and
keys `K1, K1, K2` for offsets 0, 1, 2. Records are one byte each, so the
exact ledger is `next − abs`.

| Config | InitNext | MaxPend | Crashes | FlushFail | Refusals | Reads | Cap | Ring | Cache | Evictions | Spec |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `small`, `ledger_small`, controls, witnesses (except ring and cache) | 1 | 2 | 1 | 1 | 1 | 1 | 2 | no | no | 0 | `Spec` |
| `expanded` | 1 | 3 | 1 | 1 | 1 | 2 | 1 | yes | no | 0 | `Spec` |
| `w_RingGatherBelowTrim` | 1 | 2 | 1 | 1 | 1 | 1 | 2 | yes | no | 0 | `Spec` |
| `ledger_overlap`, `nc_retire_chunk_bytes_overlap` | 1 | 2 | 0 | 0 | 0 | 1 | 2 | no | no | 0 | `Spec` |
| `cache`, `nc_warm_install_from_plan`, `w_WarmBridgeCovers`, `w_InstallStartsAbovePlan` | 1 | 2 | 0 | 0 | 0 | 0 | 1 | yes | yes | 1 | `Spec` |
| `cache_cap2` | 1 | 2 | 0 | 0 | 0 | 0 | 2 | yes | yes | 1 | `Spec` |
| `liveness`, `nc_retire_chunk_bytes_liveness` | 1 | 2 | 0 | 0 | 0 | 0 | 2 | no | no | 0 | `LiveSpec` |

`TrimBudgets` is the set of per-operation allowances that the shared
`trim_global_budget` and `max_trim_per_op` can leave; 0 means exhausted.

**Liveness scope (`LiveSpec`).** Faults cease because crashes, flush
failures, refusals and appends are bounded; the liveness shape has none of
the first three. Fairness is on actor attempts, never on a success outcome:
`WF` on `WalDurable`, `Dispatch`, `AbsorberRead`, `AbsorberReadEnd`,
`AbsorberStage`, the flush attempt, `RescanRollback` and `ReaderRelease`;
`SF` on `AbsorberPlan` (each tick gathers pending streams after at most one
dirty-index page, so rescans cannot starve it), on `AbsorberSubmit`, on the
committer's handling of a message (`CommitAbsorbed ∨ CommitReject`), on
`TrimStep` and on `RescanObserve`, which are only intermittently enabled. No
fairness on appends, crashes, prunes, evictions or loads.

### Negative controls

| Control | Operator substituted | Must violate | Why |
|---|---|---|---|
| `nc_publish_before_flush` | `SubmitReady <- MutSubmitBeforeFlush`: submit once rows are staged, before `part.flush()` | `H3_AbsorbedBackedByDurableHistory` | The boundary covers rows that are only in the WAL-less memtable. |
| `nc_canonical_only` | `PostingsWrite <- MutPostingsNotWritten`: postings dropped from the batch | `H3_AbsorbedBackedByDurableHistory` | The boundary covers offsets with no durable postings. |
| `nc_trim_to_proposed` | `AdvanceTrimTarget <- MutTrimToProposed`: the advance trims toward the new boundary | `StaleReaderRangeIntact` | A one-advance-stale reader loses its range. |
| `nc_duplicate_collapse` | `SafeRaisedOnDuplicate <- MutSafeRaisedOnDuplicate`: a duplicate raises `trim_safe_to` to the live boundary (the 2026-07-27 regression pinned by `reads_ring.rs::a_duplicate_absorbed_op_does_not_advance_the_trim`) | `StaleReaderRangeIntact` | The one-advance lag collapses. |
| `nc_tick_trim_to_absorbed` | `TickTrimTarget <- MutTickTrimToAbsorbed` | `StaleReaderRangeIntact` | The trim tick's `trim_safe_to` guard is load-bearing. |
| `nc_lastcopy_publish_before_flush` | `SubmitReady <- MutSubmitBeforeFlush` | `LastRecoverableCopy` | The advance trims rows whose history copy is only in the memtable; a crash loses the last copy. |
| `nc_lastcopy_trim_past_absorbed` | `TickTrimTarget <- MutTickTrimToNext` | `LastRecoverableCopy` | Rows history does not hold are deleted. |
| `nc_retire_chunk_bytes` | `RetireBytes <- MutRetireChunkBytes` (pre-fix F1) | `LedgerExact` | A refused advance and a re-plan from the raised mark under-retire. |
| `nc_retire_chunk_bytes_overlap` | `RetireBytes <- MutRetireChunkBytes` | `LedgerExact` | A stale re-plan over a queued chunk over-retires, with no lost publication. |
| `nc_retire_chunk_bytes_liveness` | `RetireBytes <- MutRetireChunkBytes` | `AbsorptionCompletes` | After an over-retirement every advance to `next` is refused. |
| `nc_warm_install_from_plan` | `WarmInstallFrom <- MutWarmInstallFromPlan` (pre-fix F3) | `CacheNeverProvesFalseAbsence` | A stale re-gather over a trimmed head gives an evicted key a slice that proves a durable record absent. |

`LastRecoverableCopy` cannot be broken by a trim mutation that stays within
the absorbed boundary, because H3 then guarantees the history copy. Its two
controls attack H3's ordering or the absorbed ceiling instead.

### Witnesses

| Witness | Behaviour shown reachable |
|---|---|
| `TrimDurable` | A physical trim becomes remotely durable. |
| `FullyAbsorbedDurable` | Everything is absorbed and durable. |
| `LostPublicationRecovered` | A refused or dropped commit operation loses an advance, then full absorption and a trim follow. |
| `FlushFailRecovered` | A failed or ambiguous history flush, then full absorption. |
| `StaleDuplicateIgnored` | A stale or duplicate advance arrives and absorption still completes. |
| `CrashRecovered` | A crash destroys in-flight work (a queued message, an applied-not-durable advance or a running gather), then full absorption. |
| `GatherOverTrimmedPrefix` | A Remote-scan gather re-reads a range whose head is already trimmed. |
| `StaleSnapshotLosesTail` | A snapshot more than one advance stale has part of its tail range trimmed (F2). |
| `TwoAdvancesNotDurable` | Two advances are applied but not yet remotely durable. |
| `MarkRolledBackInFlight` | The rescan rolls back a mark whose advance is still queued. |
| `EvictedMarkPrunedInFlight` | A mark is pruned for an evictable handle while its advance is still queued. |
| `RingGatherBelowTrim` | A ring-served gather returns rows below the durable trim point. |
| `WarmBridgeCovers` | A clean warm window extends a slice's proven coverage past its `indexed_to_offset`. |
| `MisStartedAdvanceCompletes` | An advance whose chunk did not start at the boundary retires the stored bytes, and absorption then completes (the F1 fix path). |
| `InstallStartsAbovePlan` | A re-gather whose scan skipped a trimmed head warms the cache over the rows it staged, a range starting above `plan.from` (the F3 fix path). |

### Exclusions and what is not claimed

- One stream, 3 offsets and equal-size records. Multi-stream `AbsorbedBatch`
  coalescing and the global trim budget appear only as the nondeterministic
  per-operation allowance. Cross-stream effects (the shard maintenance row,
  other streams' operations in a refused group) are inferred from code, not
  modelled.
- The pending roster, due and threshold selection, pacing, budget deferral
  and the v1/v2 lane seal are not modelled. The absorber may gather whenever
  there is published unabsorbed data, which over-approximates scheduling.
  Discovery liveness is TLA-017.
- Postings page layout (buckets, split pages, overlapping pages) is
  abstracted to per-offset coverage. The model therefore cannot show the
  overlapping pages that a rescan rollback plus re-gather can leave (recorded
  with F1). The cache's admission line, capped loads, loads merging into a
  resident slice, the warm-record cap and idle expiry, and failed seams are
  not modelled. The cache-bridge defect found during the F3 fix lived in
  that gap.
- SlateDB behaviour is assumed (ASM-SLATEDB-DURABLE), not verified.

---

## TLA-018 — Exact composition of history, hot storage, and read visibility

Modules `ReadCompose.tla` and `MC_ReadCompose.tla`.

### API semantics (defined before asserting completeness)

- **`deliver=durable`** (the default everywhere: raw route, product reads,
  scans, forks, peers). A page serves only Remote-durable records. The
  continuation cursor promises the whole eligible prefix: every record of the
  selected key below it was delivered exactly once, with its durable content.
- **`deliver=applied`** (product reads and long-poll only; SSE and forks
  refuse it: `src/product.rs:2626`, `:2667`; `read_request.rs:154-155`). The
  tail may include applied, not yet durable records, marked with
  `Prisma-Pending-From`. Only the durable resume cursor carries a promise:
  `min(consumed, handle.durable.next)`, with `handle.durable.next` read after
  the page (`read_request.rs:576-579`). Records at or beyond it may be
  replaced after a crash or ownership move. The code's stated intent is that
  applied reads never see less than a durable reader
  (`src/shard/record.rs:285-288`).
- **Filter.** Product reads always pass `Some(routing_key)`, including the
  default `""`. `None` (unfiltered) is the engine and replay path.
- **Retention.** Hot rows below `trimmed` are gone from the shard log. A
  reader whose snapshot is older must recover them from history. Scan
  snapshots (`read_scan.rs`) are durable pages over a frozen end, covered by
  the durable model; their cursor and signature checks are TLA-021.

### Claim

For a client paging through one incarnation, with two routing keys, a large
first record, corrupt or short postings windows, the durable tail ring, one
ownership move, trims and absorption racing each read, history read errors
on the current engine (small shapes) and at least two storage observations
per page:

- `ExactDurablePrefix`: every eligible record below the promising cursor
  (`pos` in durable mode, `dpos` in applied mode) was delivered with its
  durable content, and no ineligible record was delivered (violated in
  applied mode by F3);
- `NoDuplicateDelivery`: no record in the promised prefix is delivered twice,
  and no page reports a consumed position at or below a record it delivered;
- `NoFabricatedRecord`: only records of the selected key are delivered;
- `PageCeiling`: a page exceeds the requested bytes only through its single
  first record (the `PageBudget` exception);
- `TailGapExplained`: in both modes every tail gap is explained by the
  absorbed boundary at the scan's visibility, so the honest-partial branch is
  never needed;
- `HistoryCoversBoundary`: sanity check that the coarse writer keeps
  TLA-016's H3.

**Requirement anchors:** H1, H2, H9, H10 (abstracted), H11, D8, D9.

### Observation boundary

The reader observes its engine's handle snapshot (`P`, and `A` for the
applied end), the live history partition (`hF`), tail rows one at a time
(Remote sees `D.trimmed`, Memory sees `A.trimmed`), the durable tail ring,
and the absorbed boundary at the scan's visibility (`D.abs` for durable,
`A.abs` for applied). After an ownership move, the old engine's views are
frozen (ASM-HISTORY-FENCED-VIEW) or the read fails.

### Atomicity / linearization table

| Model action | Production function(s) | Atomicity justification / dependency contract |
|---|---|---|
| `WAppend`, `WDurable`, `WDispatch`, `WAdvance`, `WTrim` | As TLA-016 (`CommitTransaction::append`, `absorbed` and `trim`; WAL durability; `dispatch_durable`) | As TLA-016. |
| `WHistFlush` | A gather: one `WriteBatch` and `part.flush()` (`absorb_gather_v2_with` through `Absorber::commit`, `src/history/gather.rs:317-590`) | Collapsed into one step that raises the contiguous durable frontier `hF`. Justified by TLA-016's H3 and `LastRecoverableCopy` (ASM-HISTORY-WRITER). |
| `WMove` | Ownership move: the new engine opens the shard DB, fencing the old writer, and loads the durable tail | ASM-HISTORY-FENCED-VIEW. The old engine keeps frozen, self-consistent views. |
| `WLosePostings`, `WLoseCanonical` | **Not production.** Probe actions only | They violate ASM-SLATEDB-DURABLE or ASM-SLATEDB-GC. |
| `RStart` | Snapshot in `execute_segment` (`src/application/read.rs:129`, `:145-164`); page loop in `ReadService::execute_read` (`read_request.rs`) | Taken under the handle mutex. |
| `RHist` | `decode_history_range` (`read.rs:719`) → `read_history2_scan` (`src/history.rs:921`) or `read_history2_keyed_cached` (`history.rs:1031`) → `PostingsCache::runs_for` (`src/postings_cache.rs:508`) → `execute_postings_plan` (`src/history/postings_read.rs:13`), or the corruption envelope; `PageBudget` | One step: rows below the boundary are immutable. A missing canonical row is skipped silently by every source, as in production (`history.rs:921-948`; `postings_read.rs:80-96`). Postings runs are abstracted by ASM-HISTORY-POSTINGS-CACHE. |
| `RTailStart` | `ring_read` (`src/shard/tail_ring.rs:97`) and `proves_durable_ring` (`src/shard/record.rs:104`), or the start of `read_frames_until` (`record.rs:272`) | The ring returns durable copies with a density proof (ASM-HISTORY-RING); durable mode only (`record.rs:307`). |
| `RTailStep` | One row of the scan at `deliver.durability()`: Remote for durable, Memory for applied (`record.rs:325`, `:212-217`) | Per row: the iterator is not treated as a snapshot. |
| `RTailCheck` | `absorption_race` (`read.rs:789`) → `ShardEngine::visible_absorbed(hash, visibility)` (`read.rs:827`, `:837-841`; `record.rs:233-256`), then the loop decision in `execute_segment` | A `get` of the tail row at the scan's own level. The operator `RaceBoundary` is this read (F1 fix). |
| `EndPage` | `page_progress` (`read.rs:625`), then `durable_resume.after = next.after.min(floor)` with `floor` read after the page (`read_request.rs:576-579`) | One page and one end-of-page handle read. |
| `RReconnect` | The client resumes from `Prisma-Durable-Cursor` (applied mode) | Client behaviour. |
| `RError` | The page fails: the old engine is closed, or (history leg, current engine too when `AllowReadError`) a storage read fails, for example a transient object-store error | ASM-SLATEDB-GC (iii): an error, never a short success. The page ends with no delivery. |

There is no action for the cursor guard `start > end` in `execute_read`
(`read_request.rs:291-293`): the model's `RStart` requires `pos < ReadEndNow`,
which is the same refusal, and F3 is the case it lets through.

### Assumptions

ASM-SLATEDB-DURABLE, ASM-HISTORY-FENCED-VIEW, ASM-SLATEDB-GC,
ASM-HISTORY-WRITER, ASM-HISTORY-POSTINGS-CACHE (the keyed results are
conditional on it; TLA-016 checks the install path, TLA-020 is planned) and
ASM-HISTORY-RING.

### Constants per configuration

Every configuration uses `N = 3`, keys `K1, K2, K1` and sizes `3, 1, 1` for
offsets 0, 1, 2 (offset 0 exceeds the page), `Req = 2`, `MaxPend = 2`,
`MaxMoves = 1`, `MaxLoops = 2` (production allows 16) and
`TrimBudgets = {0,1,3}`.

| Config | Mode | Filter | InitNext | Pages | Ring | Corrupt | Short index | Read errors | Probe |
|---|---|---|---|---|---|---|---|---|---|
| `durable_keyed_small` and its controls and witnesses | durable | K1 | 3 | 4 | yes | yes | yes | yes | — |
| `durable_unfiltered_small`, `nc_old_history_view`, `w_UnfilteredRaceAdopted` | durable | none | 3 | 4 | yes | no | no | yes | — |
| `applied_keyed_small`, `nc_applied_race_remote`, `w_AppliedRaceAdopted` | applied | K1 | 3 | 4 | (n/a) | yes | yes | yes | — |
| `applied_unfiltered_small`, `nc_applied_race_remote_unfiltered` | applied | none | 3 | 4 | (n/a) | no | no | yes | — |
| `durable_keyed_expanded` | durable | K1 | 1 (2 appends during reads) | 5 | yes | yes | yes | no | — |
| `durable_unfiltered_expanded` | durable | none | 1 (2 appends) | 5 | yes | no | no | no | — |
| `applied_unfiltered_expanded`, `kd_stale_applied_cursor` | applied | none | 1 (2 appends; applied suffix lost on the move) | 5 | (n/a) | no | no | no | — |
| `probe_lost_postings` | durable | K1 | 3 | 4 | yes | yes | yes | yes | postings |
| `probe_lost_canonical` | durable | none | 3 | 4 | yes | no | no | yes | canonical |

`applied_unfiltered_expanded` checks every property except
`ExactDurablePrefix`, which `kd_stale_applied_cursor` checks alone while F3 is
open. History read errors are off in the expanded shapes to keep them within
the time budget. An `RError` only ends a page before any delivery, so it adds
prefixes of existing behaviours and cannot create a violation of these
properties; the small shapes and `witness-ReadErrorCurrentEngine` exercise
it. `WAppend` and `RReconnect` are disabled by construction in the small
shapes (no appends, no pending records); the expanded applied shape exercises
them. `WLosePostings` and `WLoseCanonical` are enabled only in the probes.

### Negative controls and probes

| Control | Operator substituted | Must violate | Why |
|---|---|---|---|
| `nc_old_history_view` (unfiltered) | `HistView <- MutHistViewAtPageStart`: the history leg reads a view captured at page start while the race check adopts a newer boundary | `ExactDurablePrefix` | A new absorbed frontier with an old history view. |
| `nc_filtered_race_never` (keyed) | `FilteredRace <- MutFilteredRaceNever`: the filtered tail scan is accepted without the absorbed boundary | `ExactDurablePrefix` | Advances past an unreturned eligible record. |
| `nc_short_index_accepted` (keyed) | `ShortIndexAccepted <- MutShortIndexAccepted`: `provable_to < upto` is treated as complete | `ExactDurablePrefix` | Unproven postings become an empty success. |
| `nc_applied_race_remote` (applied keyed) | `RaceBoundary <- MutRaceBoundaryRemote` (pre-fix F1) | `ExactDurablePrefix` | A durable record trimmed by an applied, not durable advance is skipped. |
| `nc_applied_race_remote_unfiltered` (applied unfiltered) | `RaceBoundary <- MutRaceBoundaryRemote` (pre-fix F1) | `TailGapExplained` | The page ends as an honest partial with no progress. |
| `probe_lost_postings` (keyed; **probe**) | `AllowLostPostings = TRUE`: a durable postings page disappears after the advance (a dependency-contract mutation) | `ExactDurablePrefix` | The reader cannot detect a lost page; a slice that falsely proves absence (TLA-016-F3) has the same observable. |
| `probe_lost_canonical` (unfiltered; **probe**) | `AllowLostCanonical = TRUE`: a durable canonical row disappears | `ExactDurablePrefix` | Every history source skips a missing row and reports completion (F2). |

### Witnesses

| Witness (config) | Behaviour shown reachable |
|---|---|
| `BoundaryRaceAdopted` (durable keyed) / `UnfilteredRaceAdopted` (durable unfiltered) | A tail page raced a trim, the newer durable boundary was adopted, and the gap was re-served from history. |
| `AppliedRaceAdopted` (applied keyed) | An applied read adopted an absorbed boundary above the Remote-durable one and re-served the trimmed prefix from history (the F1 fix path). |
| `LargeFirstRecordDelivered` | A 3-byte record is delivered on a 2-byte page. |
| `EnvelopeServed` | Corrupt postings force the §8.6 canonical envelope. |
| `ShortIndexPartial` | A postings window shorter than the request yields an honest partial. |
| `ReadFromFencedEngine` | A page completes on a fenced engine's frozen views and delivers records. |
| `RingServed` | The durable ring serves a window. |
| `ReaderCompletes` | The client reaches the end with every eligible record delivered. |
| `TrimBelowReaderCursor` | A durable trim lands at or above the reader's cursor during its tail scan. |
| `ReadErrorCurrentEngine` | A page on the current engine fails in its history leg and delivers nothing. |

### Exclusions and what is not claimed

- One incarnation and one segment, with no split lineage (TLA-012, TLA-015),
  fork stitching (TLA-015), SSE feeds (TLA-027) or peer relays; the peer
  owner runs the same `execute_segment`.
- The postings cache is abstracted by ASM-HISTORY-POSTINGS-CACHE; TLA-016
  checks the install's claims, and single-flight, load merging and the idle
  sweep are TLA-020. The ring's density proof is assumed
  (ASM-HISTORY-RING; KANI-022).
- `SCAN_WINDOW` clipping, byte-capped history scans and the 16-iteration race
  bound are modelled as honest partials or `MaxLoops = 2`. These only add
  partial pages.
- Decode and authentication errors are not modelled. They are errors, never
  a completed page.
- No liveness: a reader that keeps receiving honest partials is not checked
  for progress. `TailGapExplained` covers the stall that F1 caused.

---

## TLA-019 — Reachability-based garbage collection and reader/fork protection

Modules `ReachGC.tla`/`MC_ReachGC.tla` (physical object graph) and
`ForkPin.tla`/`MC_ForkPin.tla` (registry fork pin). They share no state; the
obligation is split by invariant ownership.

### What the repository actually owns

The only physical deletions the repository performs itself are shard-log row
trims (`maintenance.rs`), which TLA-016 covers. Every other object deletion is
SlateDB's internal collector, configured per DB: history partitions in
`history_settings` (`src/history.rs:464-535`; GC interval
`HISTORY_GC_INTERVAL_SECS`, 600 s by default; upstream `min_age` 300 s;
`manifest_poll_interval` 300 s at `:501`), shard DBs in
`src/config/validation.rs`. Stream deletion is logical: a registry tombstone
(`src/application/creation/deletion.rs`); rows are never deleted. Production
creates no user checkpoints and no `DbReader`s (the compactor writes its own
checkpoint before each compaction commit), and history reads go through the
open writer's in-memory view. Fork retention is registry-level:
`fork_children` references and the incarnation-bound CAS decisions in
`delete_transition`, `anchor::install` and `release_fork_ref`.

The repository-owned decisions checked here are: the absorbed boundary
advances only after the covering flush is in the manifest; history reads use
the writer's unregistered, poll-refreshed view; a deleted-SST read error is
propagated; the partition GC and poll settings; and the fork-pin protocol.
The protection of a stale view comes from upstream: the compactor's
checkpoint on the pre-compaction manifest.

### Claim

- `ManifestRefsPresent`: nothing the live manifest references is deleted,
  including an L0 uploaded before and committed after a collector pass
  observed an older manifest, and a compaction output.
- `HistoryBacked`: every offset below the absorbed boundary is held by a
  present object of the live manifest (the uncompleted history transition and
  the last copy, composed with TLA-016).
- `NoFalseCompleteRead`: a production read never completes while silently
  missing rows of a deleted SST.
- `LiveReadViewProtected` (the catalog's reader clause): nothing a live read
  view references is deleted. It holds because of the compactor's checkpoint
  and the timing in ASM-SLATEDB-COMPACTION-CHECKPOINT (see F1).
- `CheckpointPinned` (upstream contract; expanded shape only): an upstream
  checkpoint pin protects its objects.
- `EligibleEventuallyReclaimed` (upstream-contract liveness): an SST that is
  unreferenced, not named by an unexpired checkpoint (user or compactor) and
  meets the upstream eligibility premise is eventually deleted, even with a
  stale inventory. This checks the modelled collector, not repository code,
  and is not H14 evidence.
- ForkPin `ForkPinRespected` / `ReadyHoldsRef`: an anchored or Ready fork
  points at, and is referenced by, the current, not tombstoned incarnation it
  forked, across a source's deletion and recreation under the same name.
- ForkPin `RefEventuallyReleased` / `SoftSourceEventuallyTombstoned`
  (liveness, under the client-retry premise): once a child is gone, its
  reference is released, and a soft-deleted source whose children are gone is
  tombstoned.

**Requirement anchors:** F6, H13, H14, R4; F5, F8 and F10 through ForkPin.

### Observation boundary

ReachGC: object existence (`none` / `present` / `deleted`), id times, the
stored manifest's references, the compactor's checkpoint (`cck`: the
pre-compaction manifest and its expiry tick), the writer `Db`'s in-memory
view `wv` and the tick it went stale, the compactions store (low
watermark), one user checkpoint pin, one production read (its captured
view, start tick and outcome), and the collector's per-pass snapshot
(cutoff, observed references, inventory). One tick stands for 300 s. ForkPin: the source name's current
incarnation and lifecycle, its `fork_children`, each child's lifecycle and
forked incarnation, each creator's progress, each child tombstone's
`parent_ref_pending` debt, and whether a `DELETE` is past its tombstone CAS.

### Atomicity / linearization table

| Model action | Production function(s) | Atomicity justification / dependency contract |
|---|---|---|
| `FlushUpload`, `FlushCommit` | SlateDB memtable flush: L0 upload, then the manifest write; a conflict with the compactor's newer manifest makes the writer load and merge it first, so `wv` is current afterwards. The history partition flushes from `Absorber::commit` (`src/history/gather.rs:563`) | Two separate steps. The manifest write does not re-check object existence (ASM-SLATEDB-GC). |
| `CompactStart` / `CompactUpload` / `CompactCommit` | Embedded compactor configured by `history_settings`; its commit is `write_manifest` in SlateDB `compactor_state_protocols.rs`: a checkpoint on the stored manifest with a 900 s lifetime, then the manifest swap | The job is recorded (low watermark) before the output upload. The checkpoint write and the swap are two CAS writes, merged into one step: in between, the live manifest still names the inputs. Mutation point `CompactionCheckpoint`. The compactor does not refresh the writer's view (ASM-SLATEDB-GC iv). |
| `WriterPoll` | `PollManifest` every `manifest_poll_interval` (300 s), which merges the stored manifest (`src/history.rs:501`) | Atomic merge under the DB state lock. It may happen at any time; `Tick` forces it within `PollInterval` ticks of the view going stale (ASM-SLATEDB-COMPACTION-CHECKPOINT). |
| `Tick` | Wall-clock time | Blocked while the writer view has been stale for `PollInterval` ticks or a read has run for `ReadSpan` ticks; forgets an expired compactor checkpoint. |
| `OrphanUpload` | A fenced old writer's flush whose manifest CAS fails (ASM-HISTORY-FENCED-VIEW) | The object exists but is never referenced. |
| `Advance` | `CommitTransaction::absorbed`, submitted only after `part.flush()` returned `Ok` (TLA-016 H3). Mutation point `AdvanceBacked` | — |
| `CkCreate` / `CkRelease` | Upstream user-checkpoint API. **The repository never creates one** | Models the upstream contract only (`UseCheckpoint`). The compactor's checkpoint is `CompactCommit`'s. |
| `ReadBegin` / `ReadEnd` | History reads through the writer `Db` (`decode_history_range`, `src/application/read.rs:719`; `read_history2*`, `src/history.rs:900-1103`) | The read captures `wv` and ends within `ReadSpan` ticks. A deleted SST in the view yields the upstream outcome (mutation point `UpstreamDeletedRead`), as the repository handles it (`map_err(\|e\| e.to_string())?`, mutation point `RepoOnDeletedRead`). |
| `GcReadCompactions` → `GcReadManifest` → `GcList` → `GcDelete`* → `GcFinish` | Upstream `GarbageCollector::run_gc_task` → `remove_expired_checkpoints`, then `CompactedGcTask::collect` (SlateDB `0717cc1`, `garbage_collector.rs`, `garbage_collector/compacted_gc.rs`) | Compactions are read before the manifest; the manifest read includes the manifests of unexpired checkpoints (`CheckpointRefs`; expiry is checked at that step, which can only make deletion earlier); then the list, then per-object deletes. At most one pass per tick. |
| `ForkBegin` | `fork::prepare` validates the live current incarnation (`src/application/creation/fork.rs:28`) | — |
| `ForkInstall` | `anchor::install`: `mutate_incarnation(source, forked epoch)` (`src/application/creation/anchor.rs:78`) | One CAS bound to the forked incarnation (ASM-OBJSTORE-CAS). Idempotent when already installed; declines on a soft, tombstoned or recreated source. Mutation point `InstallFence`. |
| `ForkPostCheck` | `anchor.rs:135-182`: if the child vanished, release the fresh reference; otherwise require the source name's current descriptor to list the fork id | The release is one `release_fork_ref` CAS. The presence check is by name, with no epoch check. |
| `CreatorCrash` | The create request dies before its post-check | Bounded fault. |
| `SourceDelete` | `delete_lifecycle` → `delete_transition` (`deletion.rs:249`, `:465`) | Soft versus tombstone is decided inside the CAS. Mutation point `DeleteDecision`. |
| `SourceRecreate` | A create under the same name after the tombstone; blocked while soft-deleted (F5) | New incarnation with no children. |
| `ChildDelete` | `delete_transition` on the child: the tombstone records `parent_ref_pending` in the same write | One CAS. |
| `InRequestRelease` | The same request's `release_fork_ref(source, fork_id, source_epoch)` and `clear_parent_debt` when conclusive (`deletion.rs:341-342`, `:72`, `:37`) | The epoch check and the CAS are bound to one snapshot; an incarnation change is conclusive. One step. |
| `RequestAbandon` | A crash or cancellation after the tombstone CAS | Bounded fault. The debt persists on the tombstone. |
| `RetryDelete` | The **client** re-issues `DELETE` → `delete_lifecycle` → `repair_tombstone` (`deletion.rs:273-274`, `:359`) | There is no background sweeper. Its fairness is the operator `ClientRetryFairness`. |

### Assumptions

ASM-SLATEDB-GC, ASM-SLATEDB-COMPACTION-CHECKPOINT, ASM-HISTORY-FENCED-VIEW,
ASM-HISTORY-GC-CLOCK, ASM-HISTORY-ACTORS and ASM-OBJSTORE-CAS.

### Constants per configuration

| Config | MinAge | Horizon | MaxTime | CkLife | User checkpoint | Orphan | Reader | Spec |
|---|---|---|---|---|---|---|---|---|
| ReachGC `small`, `nc_no_compaction_checkpoint`, `nc_advance_on_upload`, `nc_no_generation`, witnesses except `ReaderViewErrors` | 1 | 2 | 4 | 3 | no | yes | yes | `Spec` |
| ReachGC `lapse`, `nc_swallow_read_error`, `probe_upstream_short_read`, `w_ReaderViewErrors` | 1 | 2 | 4 | 1 | no | yes | yes | `Spec` |
| ReachGC `expanded`, `nc_ignore_checkpoint_pin` | 2 | 2 | 5 | 3 | yes | yes | yes | `Spec` |
| ReachGC `liveness`, `nc_stale_inventory` | 1 | 2 | 4 | 3 | no | yes | no\* | `LiveSpec` |
| ReachGC `liveness_expanded` | 2 | 2 | 5 | 3 | yes | yes | no\* | `LiveSpec` |
| ForkPin (all) | children `{F1, F2}`, `MaxEpoch = 2`, `MaxCrashes = 1` | | | | | | | `Spec` / `LiveSpecClientRetries` |

Every ReachGC configuration uses `PollInterval = 1` and `ReadSpan = 1`: one
tick is 300 s, the writer view is refreshed within one tick of going stale,
and a read ends within one tick. `CkLife = 3` is the 900 s compactor
checkpoint. The `lapse` shape's one-tick checkpoint stands for the cases the
timing assumption excludes (repeated failed polls, or a read that outlasts
the checkpoint).

\*The collector never consults the production read view, so the view cannot
affect reclamation; it is omitted from the liveness state space.

**Liveness scope.** ReachGC: writer, compactor, orphan and pin activity stop
at `Horizon`. `WF` on `Tick`, each collector step and `ReadEnd`; no fairness
on any writer, delete-success or pin step. ForkPin (`LiveSpecClientRetries`):
creator and delete crashes are bounded, so they cease. `WF` on each child's
in-process request steps (`ForkInstall ∨ ForkPostCheck ∨ InRequestRelease`)
and on `RetryDelete`, which is the client re-issuing `DELETE`
(`ClientRetryFairness`). That is the F8 premise, and it is unestablished
(F4). No fairness on deletes, forks, recreation or success outcomes.

### Negative controls and probes

| Control | Layer | Operator substituted | Must violate |
|---|---|---|---|
| `nc_no_compaction_checkpoint` | upstream contract (the model before the checkpoint was added) | `CompactionCheckpoint <- MutNoCompactionCheckpoint`: the compactor commits without checkpointing the pre-compaction manifest | `LiveReadViewProtected` |
| `nc_advance_on_upload` | repository | `AdvanceBacked <- MutAdvanceOnUpload`: the boundary advances once the covering SST is uploaded, before the manifest commit | `HistoryBacked` |
| `nc_swallow_read_error` (lapse shape) | repository | `RepoOnDeletedRead <- MutRepoSwallowReadError`: a deleted-SST read error becomes an empty completed result | `NoFalseCompleteRead` |
| `probe_upstream_short_read` (lapse shape) | **probe** (ASM-SLATEDB-GC iii) | `UpstreamDeletedRead <- MutUpstreamShortRead`: upstream returns a short success for a deleted SST | `NoFalseCompleteRead` |
| `nc_no_generation` | upstream contract | `GcEligible <- MutEligibleAgeOnly`: delete from an older reachability snapshot using only `min_age` | `ManifestRefsPresent` |
| `nc_ignore_checkpoint_pin` | upstream contract (a user checkpoint, which the repository never creates) | `GcRefs <- MutRefsIgnoreCheckpoint`: the collector ignores every checkpoint's manifest | `CheckpointPinned` |
| `nc_stale_inventory` | upstream contract | `GcInventory <- MutInventoryFrozen`: after the first list the collector trusts that inventory forever | `EligibleEventuallyReclaimed` |
| ForkPin `nc_ignore_fork_pin` | repository | `DeleteDecision <- MutDeleteIgnoresRefs`: the source is tombstoned regardless of fork references | `ForkPinRespected` |
| ForkPin `nc_install_ignores_incarnation` | repository | `InstallFence <- MutInstallIgnoresIncarnation`: the install CAS is not bound to the forked incarnation | `ForkPinRespected` |
| ForkPin `probe_no_client_retry` | **probe** (the F8 client premise) | `ClientRetryFairness <- NoClientRetry`: no client repeats `DELETE` | `RefEventuallyReleased` |

TLC reports a liveness violation as "Temporal properties were violated"
without naming the property; each temporal control's configuration checks
exactly one property.

### Witnesses

| Witness | Behaviour shown reachable |
|---|---|
| `CompactedInputReclaimed` | A compacted-away L0 is deleted. |
| `OrphanReclaimed` | A fenced writer's orphan is deleted. |
| `HistoryServedAfterReclaim` | The absorbed boundary covers offset 0 through the compaction output after L1 is deleted. |
| `ReaderViewErrors` (lapse shape) | With a checkpoint shorter than the view's staleness plus a read, a read needs an SST the collector deleted and fails. |
| `StaleWriterViewRead` | A read begins on a writer view that still lists a compacted-away input. |
| `QuietDeadZoneRetains` | An unreferenced SST survives the final collector pass on a quiet partition (F2). |
| `LateCommitAfterGcView` | A collector pass lists an L0 its observed manifest does not reference while the live manifest already does; the generation condition protects it. |
| `CheckpointProtectsReadView` | A collector pass has listed a compacted-away input that a live read view references and that is past the age and generation cutoff; only the compactor's checkpoint keeps it. |
| ForkPin `SoftDeleteRetainedForFork` | The source is soft-deleted while a fork is Ready. |
| ForkPin `TwoChildrenPinSource` | Two Ready forks both pin a soft-deleted source. |
| ForkPin `ForkCascadeTombstone` | A child's release tombstones the soft source. |
| ForkPin `InstallAfterChildDeleted` | A creator's install lands after its child was deleted. |
| ForkPin `InstallDeclinedOnRecreatedSource` | An install is declined because the source was deleted and recreated. |
| ForkPin `DebtClearedOnRecreatedSource` | A child's debt is cleared conclusively because the incarnation it forked is gone. |
| ForkPin `PermanentPinWithoutRetry` | A child is gone, its reference still pins a soft-deleted source, and only a client retry can release it (F4). |
| ForkPin `PinAfterSuccessfulDelete` | A child's `DELETE` returned success, then the creator's late install landed and the creator died, so the reference pins the source with no client signal to retry (F4). |

### Exclusions and what is not claimed

- Upstream SlateDB GC, compaction, checkpoints and fencing are an assumed
  interface (ASM-SLATEDB-GC, ASM-HISTORY-FENCED-VIEW), established by reading
  the pinned code, not verified here. WAL-SST, manifest and compactions-file
  collection, WAL fence objects and clone detach are not modelled.
- One partition, four objects and one compaction of L0 inputs. Sorted-run
  inputs are not modelled; the compactor's checkpoint names the whole
  pre-compaction manifest, so they are protected the same way. The block
  cache is not modelled; it can only turn a failing read into a successful
  one. The shard DB's and the billing DB's collectors and reads have the
  same contract with other settings and are not separately modelled.
- Time is discrete (300 s ticks) and rounded against the protection: a view
  that went stale in tick `k` may stay stale through tick `k + 1`, a read
  that began in tick `j` may run through tick `j + 1`, and the checkpoint
  written in tick `k` counts as expired from tick `k + 3`, up to one tick
  early.
- Topology parent/child retention (H13) is out of scope: there is no physical
  range split or clone today. Multi-level fork ancestry, recursive cascade
  debt, `expires_at` expiry and creator resumption are TLA-013 and TLA-014.
- Reclamation of hard-deleted incarnations' rows is not claimed (F3).

---

## Coverage

Coverage was measured by rerunning baselines directly with `tlc2.TLC
-coverage 1` on the final models (2 workers), outside the driver. Every run
completed its search. `Terminated` is the stuttering quiescence step and never
adds a state.

| Model | Configurations | Actions | Actions that added no state |
|---|---|---|---|
| HistoryAbsorb | `small` (8,380,652 states), `cache_cap2` (4,234,784) | 22 | none across the two runs; `small` has no cache actions, `cache_cap2` no crash, flush failure, refusal or reader |
| ReadCompose | `durable_keyed_small` (12,482,967), `applied_unfiltered_expanded` (18,856,204) | 16 | `WLosePostings`, `WLoseCanonical`: probe-only actions, disabled in every baseline; they fire in the probes' counterexamples |
| ReachGC | `small` (3,938,288), `expanded` (27,990,301) | 18 | none across the two runs; `small` has no user checkpoint (`CkCreate`, `CkRelease`) |
| ForkPin | `baseline` (16,900) | 11 | none |

The action counts exclude `Init` and `Terminated`.

## Files

| File | Purpose |
|---|---|
| `HistoryAbsorb.tla`, `MC_HistoryAbsorb.tla` | TLA-016 model and its wrapper (`Mut*` control operators, key map) |
| `ReadCompose.tla`, `MC_ReadCompose.tla` | TLA-018 model and wrapper |
| `ReachGC.tla`, `MC_ReachGC.tla` | TLA-019 physical object graph, the upstream GC contract and the repository's reliance on it, with wrapper |
| `ForkPin.tla`, `MC_ForkPin.tla` | TLA-019 registry fork pin, with wrapper |
| `MC_*_<shape>.cfg` | baselines, liveness configurations included |
| `MC_*_kd_*.cfg` | known-defect checks (one property each; only TLA-018-F3 today) |
| `MC_*_nc_*.cfg`, `MC_*_probe_*.cfg` | negative controls and probes (one operator substituted, one property each) |
| `MC_*_w_*.cfg` | reachability witnesses (one `Witness_*` invariant each) |
| `evidence/*.trace.txt` | TLC traces of the findings, trimmed to the trace (below) |
| `../../manifest.json`, `../../assumptions.md`, `../../receipts/TLA-01{6,8,9}.json` | obligations and checks, assumption entries, receipts of the recorded runs |
| `../../regressions/TLA-018-F3/README.md` | the open cursor defect's real-code reproduction and the decision needed |

| Evidence trace | Finding | Recorded on |
|---|---|---|
| `TLA-016_ledger_under_retire.trace.txt`, `TLA-016_ledger_over_retire.trace.txt`, `TLA-016_liveness_stall.trace.txt` | TLA-016-F1 | the pre-fix model (labelled) |
| `TLA-016_cache_false_absence.trace.txt` | TLA-016-F3 | the pre-fix model (labelled) |
| `TLA-018_applied_keyed_skip.trace.txt` | TLA-018-F1 | the pre-fix model (labelled) |
| `TLA-018_applied_stale_cursor.trace.txt` | TLA-018-F3 (open) | the current model: the driver log of `known-defect-stale-applied-cursor` |
| `TLA-019_reader_view_deleted.trace.txt` | TLA-019-F1 (withdrawn) | the model without the compactor checkpoint (labelled) |
| `TLA-019_fork_no_retry.trace.txt`, `TLA-019_fork_pin_after_successful_delete.trace.txt` | TLA-019-F4 | the current model: the driver logs of `probe-no-client-retry` and `witness-fork-PinAfterSuccessfulDelete` |
