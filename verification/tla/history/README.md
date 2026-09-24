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
| TLA-016 | `pass-with-recorded-scope` | TLA-016-F1 (fixed): the absorbed advance retired the chunk's bytes, not the range it moved over. TLA-016-F3 (fixed): the postings warm install claimed coverage over a trimmed head it never read. Overlapping postings pages after a re-gather from a stale boundary (found with F1; fixed by `d16559b3`, readers admit overlapping pages that agree). | — |
| TLA-018 | `pass-with-recorded-scope` | TLA-018-F1 (fixed): an applied keyed read skipped durable records trimmed by a non-durable advance. TLA-018-F3 (fixed): a stale applied cursor was accepted once the new owner's tail passed it. | TLA-018-F2 (open obligation, owner decision): H11 holds at the reader only through durability and the cache contract; `docs/dst/DST-EXPANSION-SPEC.md` §9.12.2. |
| TLA-019 | `pass-with-recorded-scope` | TLA-019-F4 (fixed): releasing a fork pin after an interrupted or raced `DELETE` depended on the client repeating `DELETE`; a background reconciler now releases it. TLA-019-F1 was an abstraction mismatch, withdrawn conditionally: the compactor's checkpoint protects the SSTs a stale writer view still names while ASM-SLATEDB-COMPACTION-CHECKPOINT holds. | Open service obligations (`docs/READINESS.md`): TLA-019-F2, GC convergence without further writes (H14); TLA-019-F3, no physical reclamation policy for hard-deleted incarnations' rows. |

No defect in this group is open, so no check has the `known-defect` role.
Each fixed defect has a passing baseline and a negative control that
reproduces the pre-fix behaviour. The group also records two documentation
defects, both fixed in comments (TLA-016-F2, and TLA-016-F4 in a fix's
comments), one open obligation that needs an owner decision (TLA-018-F2,
H11), two open service obligations (TLA-019-F2, H14 without further writes;
TLA-019-F3, physical reclamation), and two defects found in passing by the
fixes, both fixed: overlapping postings pages after a re-gather from a
stale boundary (TLA-016 now models the pages and the reader's admission of
agreeing overlaps) and a cache bridge over dropped runs (the model cannot
express it). TLA-019-F4 was first recorded as an unjustified assumption (the
client retry); the fix made it production work, and it is listed as fixed.

File and line references in the TLA-018 and TLA-019 findings and mapping
tables are at `d9aeaef` (the fixes landed in `55881d7`, `0d40dc2` and
`8a03e0d`; between `55881d7` and `d9aeaef` only `src/shard.rs` and
`src/history.rs` moved among the cited files, and the cited `src/history.rs`
lines did not).

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

Receipt `verification/receipts/TLA-016.json`: 33 checks, every verdict as expected, run on `ab73296` with uncommitted changes; TLC 2.19 on Java 17.0.1, 2 workers; 52 min of TLC wall time in total. This table predates the model changes of `f1de3dcb`, which bring the manifest to 46 checks; that commit reports all 46 matching in a run without `--record`. The receipt is to be recorded on the frozen source.

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

### TLA-018 (`pass-with-recorded-scope`)

Not yet recorded as a receipt: the table is from `formal.py run --id TLA-018 --id TLA-019` (without `--record`) on `55881d7` with the uncommitted model changes, which `verification/receipts/TLA-018.json` does not yet reflect (it still records the pre-fix run on `ab73296`). 29 checks, every verdict as expected; TLC 2.19 on Java 17.0.1, 2 workers, on a machine with a load average of 25 to 60 from other jobs; 107 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-durable-keyed-small` | `ReadCompose` / `durable_keyed_small` | baseline | pass | pass | 12,482,967 | 1660 |
| `baseline-durable-unfiltered-small` | `ReadCompose` / `durable_unfiltered_small` | baseline | pass | pass | 3,777,005 | 475 |
| `baseline-applied-keyed-small` | `ReadCompose` / `applied_keyed_small` | baseline | pass | pass | 11,351,025 | 522 |
| `baseline-applied-unfiltered-small` | `ReadCompose` / `applied_unfiltered_small` | baseline | pass | pass | 3,045,185 | 102 |
| `baseline-durable-keyed-expanded` | `ReadCompose` / `durable_keyed_expanded` | baseline | pass | pass | 19,356,657 | 737 |
| `baseline-durable-unfiltered-expanded` | `ReadCompose` / `durable_unfiltered_expanded` | baseline | pass | pass | 6,582,468 | 344 |
| `baseline-applied-unfiltered-expanded` | `ReadCompose` / `applied_unfiltered_expanded` | baseline | pass | pass | 19,384,259 | 889 |
| `baseline-applied-keyed-expanded` | `ReadCompose` / `applied_keyed_expanded` | baseline | pass | pass | 53,598,423 | 1618 |
| `nc-old-history-view` | `ReadCompose` / `nc_old_history_view` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 567,556 | 11 |
| `nc-filtered-race-never` | `ReadCompose` / `nc_filtered_race_never` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 205,787 | 5 |
| `nc-short-index-accepted` | `ReadCompose` / `nc_short_index_accepted` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 6,946 | 2 |
| `nc-applied-race-remote` | `ReadCompose` / `nc_applied_race_remote` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 55,998 | 2 |
| `nc-applied-race-remote-unfiltered` | `ReadCompose` / `nc_applied_race_remote_unfiltered` | negative-control | violation `TailGapExplained` | violation `TailGapExplained` | 43,188 | 2 |
| `nc-no-continuation-check` | `ReadCompose` / `nc_no_continuation_check` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 2,540,301 | 41 |
| `probe-lost-durable-postings` | `ReadCompose` / `probe_lost_postings` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 7,447 | 2 |
| `probe-lost-durable-canonical` | `ReadCompose` / `probe_lost_canonical` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 10,842 | 2 |
| `witness-BoundaryRaceAdopted` | `ReadCompose` / `w_BoundaryRaceAdopted` | witness | violation `Witness_BoundaryRaceAdopted` | violation `Witness_BoundaryRaceAdopted` | 8,799 | 2 |
| `witness-UnfilteredRaceAdopted` | `ReadCompose` / `w_UnfilteredRaceAdopted` | witness | violation `Witness_BoundaryRaceAdopted` | violation `Witness_BoundaryRaceAdopted` | 161,672 | 4 |
| `witness-AppliedRaceAdopted` | `ReadCompose` / `w_AppliedRaceAdopted` | witness | violation `Witness_AppliedRaceAdopted` | violation `Witness_AppliedRaceAdopted` | 4,099 | 1 |
| `witness-LargeFirstRecordDelivered` | `ReadCompose` / `w_LargeFirstRecordDelivered` | witness | violation `Witness_LargeFirstRecordDelivered` | violation `Witness_LargeFirstRecordDelivered` | 137 | 1 |
| `witness-EnvelopeServed` | `ReadCompose` / `w_EnvelopeServed` | witness | violation `Witness_EnvelopeServed` | violation `Witness_EnvelopeServed` | 1,148 | 1 |
| `witness-ShortIndexPartial` | `ReadCompose` / `w_ShortIndexPartial` | witness | violation `Witness_ShortIndexPartial` | violation `Witness_ShortIndexPartial` | 1,768 | 1 |
| `witness-ReadFromFencedEngine` | `ReadCompose` / `w_ReadFromFencedEngine` | witness | violation `Witness_ReadFromFencedEngine` | violation `Witness_ReadFromFencedEngine` | 495 | 1 |
| `witness-RingServed` | `ReadCompose` / `w_RingServed` | witness | violation `Witness_RingServed` | violation `Witness_RingServed` | 150 | 1 |
| `witness-ReaderCompletes` | `ReadCompose` / `w_ReaderCompletes` | witness | violation `Witness_ReaderCompletes` | violation `Witness_ReaderCompletes` | 4,297 | 1 |
| `witness-TrimBelowReaderCursor` | `ReadCompose` / `w_TrimBelowReaderCursor` | witness | violation `Witness_TrimBelowReaderCursor` | violation `Witness_TrimBelowReaderCursor` | 10,577 | 2 |
| `witness-ReadErrorCurrentEngine` | `ReadCompose` / `w_ReadErrorCurrentEngine` | witness | violation `Witness_ReadErrorCurrentEngine` | violation `Witness_ReadErrorCurrentEngine` | 46 | 1 |
| `witness-ContinuedAcrossMove` | `ReadCompose` / `w_ContinuedAcrossMove` | witness | violation `Witness_ContinuedAcrossMove` | violation `Witness_ContinuedAcrossMove` | 355,396 | 6 |
| `witness-StaleContinuationResynced` | `ReadCompose` / `w_StaleContinuationResynced` | witness | violation `Witness_StaleContinuationResynced` | violation `Witness_StaleContinuationResynced` | 463,670 | 8 |

### TLA-019 (`pass-with-recorded-scope`)

Not yet recorded as a receipt: the table is from `formal.py run --id TLA-019` (without `--record`) on `d93490f` with the uncommitted fork-debt recreation fix and model changes, which `verification/receipts/TLA-019.json` does not yet reflect. 50 checks, every verdict as expected; TLC 2.19 on Java 17.0.1, 2 workers; 43 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-small` | `ReachGC` / `small` | baseline | pass | pass | 3,938,288 | 53 |
| `baseline-expanded` | `ReachGC` / `expanded` | baseline | pass | pass | 27,990,301 | 368 |
| `baseline-timing-lapse` | `ReachGC` / `lapse` | baseline | pass | pass | 3,594,585 | 36 |
| `liveness-small` | `ReachGC` / `liveness` | baseline | pass | pass | 637,394 | 69 |
| `liveness-expanded` | `ReachGC` / `liveness_expanded` | baseline | pass | pass | 4,826,314 | 732 |
| `nc-no-compaction-checkpoint` | `ReachGC` / `nc_no_compaction_checkpoint` | negative-control | violation `LiveReadViewProtected` | violation `LiveReadViewProtected` | 234,706 | 3 |
| `nc-advance-on-upload` | `ReachGC` / `nc_advance_on_upload` | negative-control | violation `HistoryBacked` | violation `HistoryBacked` | 162 | 1 |
| `nc-swallow-read-error` | `ReachGC` / `nc_swallow_read_error` | negative-control | violation `NoFalseCompleteRead` | violation `NoFalseCompleteRead` | 463,603 | 4 |
| `probe-upstream-short-read` | `ReachGC` / `probe_upstream_short_read` | negative-control | violation `NoFalseCompleteRead` | violation `NoFalseCompleteRead` | 508,278 | 5 |
| `nc-no-generation-condition` | `ReachGC` / `nc_no_generation` | negative-control | violation `ManifestRefsPresent` | violation `ManifestRefsPresent` | 29,992 | 1 |
| `nc-ignore-checkpoint-pin` | `ReachGC` / `nc_ignore_checkpoint_pin` | negative-control | violation `CheckpointPinned` | violation `CheckpointPinned` | 1,123,210 | 8 |
| `nc-stale-inventory` | `ReachGC` / `nc_stale_inventory` | negative-control | violation `EligibleEventuallyReclaimed` | violation (temporal) | 566,760 | 58 |
| `witness-CompactedInputReclaimed` | `ReachGC` / `w_CompactedInputReclaimed` | witness | violation `Witness_CompactedInputReclaimed` | violation `Witness_CompactedInputReclaimed` | 460,934 | 4 |
| `witness-OrphanReclaimed` | `ReachGC` / `w_OrphanReclaimed` | witness | violation `Witness_OrphanReclaimed` | violation `Witness_OrphanReclaimed` | 202,160 | 2 |
| `witness-HistoryServedAfterReclaim` | `ReachGC` / `w_HistoryServedAfterReclaim` | witness | violation `Witness_HistoryServedAfterReclaim` | violation `Witness_HistoryServedAfterReclaim` | 1,253,118 | 10 |
| `witness-ReaderViewErrors` | `ReachGC` / `w_ReaderViewErrors` | witness | violation `Witness_ReaderViewErrors` | violation `Witness_ReaderViewErrors` | 625,102 | 5 |
| `witness-StaleWriterViewRead` | `ReachGC` / `w_StaleWriterViewRead` | witness | violation `Witness_StaleWriterViewRead` | violation `Witness_StaleWriterViewRead` | 701 | 1 |
| `witness-QuietDeadZoneRetains` | `ReachGC` / `w_QuietDeadZoneRetains` | witness | violation `Witness_QuietDeadZoneRetains` | violation `Witness_QuietDeadZoneRetains` | 22,804 | 1 |
| `witness-LateCommitAfterGcView` | `ReachGC` / `w_LateCommitAfterGcView` | witness | violation `Witness_LateCommitAfterGcView` | violation `Witness_LateCommitAfterGcView` | 1,924 | 1 |
| `witness-CheckpointProtectsReadView` | `ReachGC` / `w_CheckpointProtectsReadView` | witness | violation `Witness_CheckpointProtectsReadView` | violation `Witness_CheckpointProtectsReadView` | 118,239 | 2 |
| `fork-baseline` | `ForkPin` / `baseline` | baseline | pass | pass | 3,374,329 | 64 |
| `fork-baseline-legacy` | `ForkPin` / `baseline_legacy` | baseline | pass | pass | 41,723,978 | 895 |
| `fork-liveness-client-retries` | `ForkPin` / `liveness_client_retries` | baseline | pass | pass | 72,328 | 8 |
| `fork-liveness-reconciler` | `ForkPin` / `liveness_reconciler` | baseline | pass | pass | 72,328 | 10 |
| `fork-liveness-reconciler-recreated` | `ForkPin` / `liveness_reconciler_recreated` | baseline | pass | pass | 49,238 | 8 |
| `fork-liveness-backfill` | `ForkPin` / `liveness_backfill` | baseline | pass | pass | 564,266 | 107 |
| `fork-liveness-backfill-recreated` | `ForkPin` / `liveness_backfill_recreated` | baseline | pass | pass | 367,371 | 68 |
| `nc-ignore-fork-pin` | `ForkPin` / `nc_ignore_fork_pin` | negative-control | violation `ForkPinRespected` | violation `ForkPinRespected` | 53 | 1 |
| `nc-install-ignores-incarnation` | `ForkPin` / `nc_install_ignores_incarnation` | negative-control | violation `ForkPinRespected` | violation `ForkPinRespected` | 348 | 1 |
| `nc-no-reconciler` | `ForkPin` / `nc_no_reconciler` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 63,987 | 4 |
| `nc-settle-inconclusive` | `ForkPin` / `nc_settle_inconclusive` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 51,288 | 4 |
| `nc-no-backfill` | `ForkPin` / `nc_no_backfill` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 55,550 | 4 |
| `nc-recreate-without-index` | `ForkPin` / `nc_recreate_without_index` | negative-control | violation `OwedRefIndexed` | violation `OwedRefIndexed` | 299 | 1 |
| `nc-recreate-without-index-liveness` | `ForkPin` / `nc_recreate_without_index_liveness` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 45,896 | 4 |
| `nc-reconcile-live-child` | `ForkPin` / `nc_reconcile_live_child` | negative-control | violation `ReadyHoldsRef` | violation `ReadyHoldsRef` | 115 | 1 |
| `nc-release-current-name-id` | `ForkPin` / `nc_release_current_name_id` | negative-control | violation `ReadyHoldsRef` | violation `ReadyHoldsRef` | 1,395 | 1 |
| `nc-no-write-ahead-marker` | `ForkPin` / `nc_no_write_ahead_marker` | negative-control | violation `OwedRefIndexed` | violation `OwedRefIndexed` | 47 | 1 |
| `witness-fork-SoftDeleteRetainedForFork` | `ForkPin` / `w_SoftDeleteRetainedForFork` | witness | violation `Witness_SoftDeleteRetainedForFork` | violation `Witness_SoftDeleteRetainedForFork` | 116 | 1 |
| `witness-fork-TwoChildrenPinSource` | `ForkPin` / `w_TwoChildrenPinSource` | witness | violation `Witness_TwoChildrenPinSource` | violation `Witness_TwoChildrenPinSource` | 4,469 | 1 |
| `witness-fork-ForkCascadeTombstone` | `ForkPin` / `w_ForkCascadeTombstone` | witness | violation `Witness_ForkCascadeTombstone` | violation `Witness_ForkCascadeTombstone` | 382 | 1 |
| `witness-fork-InstallAfterChildDeleted` | `ForkPin` / `w_InstallAfterChildDeleted` | witness | violation `Witness_InstallAfterChildDeleted` | violation `Witness_InstallAfterChildDeleted` | 77 | 1 |
| `witness-fork-InstallDeclinedOnRecreatedSource` | `ForkPin` / `w_InstallDeclinedOnRecreatedSource` | witness | violation `Witness_InstallDeclinedOnRecreatedSource` | violation `Witness_InstallDeclinedOnRecreatedSource` | 122 | 1 |
| `witness-fork-DebtClearedOnRecreatedSource` | `ForkPin` / `w_DebtClearedOnRecreatedSource` | witness | violation `Witness_DebtClearedOnRecreatedSource` | violation `Witness_DebtClearedOnRecreatedSource` | 583 | 1 |
| `witness-fork-PermanentPinWithoutRetry` | `ForkPin` / `w_PermanentPinWithoutRetry` | witness | violation `Witness_PermanentPinWithoutRetry` | violation `Witness_PermanentPinWithoutRetry` | 742 | 1 |
| `witness-fork-PinAfterSuccessfulDelete` | `ForkPin` / `w_PinAfterSuccessfulDelete` | witness | violation `Witness_PinAfterSuccessfulDelete` | violation `Witness_PinAfterSuccessfulDelete` | 582 | 1 |
| `witness-fork-ReconcilerReleasesLatePin` | `ForkPin` / `w_ReconcilerReleasesLatePin` | witness | violation `Witness_ReconcilerReleasesLatePin` | violation `Witness_ReconcilerReleasesLatePin` | 1,053 | 1 |
| `witness-fork-ReconcilerReleasesReplacedName` | `ForkPin` / `w_ReconcilerReleasesReplacedName` | witness | violation `Witness_ReconcilerReleasesReplacedName` | violation `Witness_ReconcilerReleasesReplacedName` | 451 | 1 |
| `witness-fork-BackfillReleased` | `ForkPin` / `w_BackfillReleased` | witness | violation `Witness_BackfillReleased` | violation `Witness_BackfillReleased` | 859 | 1 |
| `witness-fork-RecreationIndexesDebt` | `ForkPin` / `w_RecreationIndexesDebt` | witness | violation `Witness_RecreationIndexesDebt` | violation `Witness_RecreationIndexesDebt` | 140 | 1 |
| `witness-fork-OldBinaryOverwroteDebt` | `ForkPin` / `w_OldBinaryOverwroteDebt` | witness | violation `Witness_OldBinaryOverwroteDebt` | violation `Witness_OldBinaryOverwroteDebt` | 39 | 1 |
<!-- /RESULTS -->

## Findings

| ID | Classification | Disposition |
|---|---|---|
| TLA-016-F1 | production defect | **Fixed** by "An absorption advance retires exactly the bytes of the range it moves the boundary over" (`6371da0`). Baselines `ledger-small`, `ledger-overlap`, `liveness-small` pass; controls `nc-retire-chunk-bytes*` reproduce the pre-fix behaviour. |
| TLA-016-F3 | production defect (latent) | **Fixed** by "A re-gather warms the postings cache only over the rows it staged": the warm install is named by the rows the gather staged (`src/history/gather.rs`). `baseline-cache` passes; `nc-warm-install-from-plan` reproduces the pre-fix behaviour. |
| TLA-016-F2 | documentation defect | **Fixed in comments** by `63202168` ("The invariant docs state what the models showed and what stays an owner decision"): the `trim_safe_to` comments now say the one-advance lag protects a snapshot at most one advance stale, and that `absorption_race` carries the guarantee for staler readers. |
| TLA-016-F4 | documentation defect, found while modelling the receipt fix `d9aeaef` | **Fixed**: the `settle_submissions` and `stored_frame_bytes` comments no longer claim the bound. The fix bounds a recount per refusal, but consecutive refusals that are each settled late leave consecutive holes, so a recount is not bounded by the chunks in flight at any one refusal. `witness-RecountBeyondInFlight`. |
| (overlapping pages) | production defect, recorded with the TLA-016-F1 fix | **Fixed** by "Overlapping postings pages from a re-gather admit when they agree" (`d16559b3`). A re-gather from a stale boundary writes pages that overlap an earlier chunk's, and the reader refused them as corrupt. Readers now admit an overlapping page that lists exactly the admitted offsets over the common span. Modelled (`Pages`, ASM-HISTORY-PAGES): `baseline-pages-regather` checks `PagesAdmit` with the rescan, prune and restart re-gathers; `witness-OverlapAdmitted{Rescan,Prune,NewOwner}` show overlaps that admit; `probe-scan-per-row` shows the admission rests on dense chunks. |
| (cache bridge) | production defect, found in passing during the TLA-016-F3 fix | **Fixed** by "A postings-cache bridge never crosses a chunk whose runs no slice recorded". Outside what the model can express (admission line, capped and merging loads); covered by real-code regressions. |
| TLA-018-F1 | production defect | **Fixed** by "An applied read revalidates its tail scan at the level it scanned, so it never skips a durable record" (`9cea1b6`). `baseline-applied-keyed-small` passes; controls `nc-applied-race-remote*` reproduce the pre-fix behaviour. |
| TLA-018-F3 | production defect | **Fixed** by "A provisional read cursor proves the history it continues, or answers an explicit resync" (`55881d7`). `baseline-applied-unfiltered-expanded` (the former known-defect shape) and `baseline-applied-keyed-expanded` check `ExactDurablePrefix` and pass; `nc-no-continuation-check` reproduces the pre-fix acceptance. Regressions in `dst::dst_tests::reads_applied_history`; `verification/regressions/TLA-018-F3/README.md`. |
| TLA-018-F2 | open obligation (owner decision) | **Open; needs an owner decision.** H11 is adopted as written and only partly enforced: the reader does not detect a page or canonical row lost after durability, which holds only through durability and the cache contract. The owner either keeps H11 and adds a coverage mechanism (option A) or deliberately revises the contract under roadmap §2.10 (option B); `docs/dst/DST-EXPANSION-SPEC.md` §9.12.2. Until then no report counts H11 as met. |
| TLA-019-F1 | abstraction mismatch (not reproduced) | **Withdrawn as a defect, conditionally.** The model lacked the compactor's checkpoint. With it, `LiveReadViewProtected` passes under ASM-SLATEDB-COMPACTION-CHECKPOINT, a pinned dependency contract (an upstream 900 s constant the code calls interim, a refresh within 300 s, a read within the remaining 600 s), not permanent reader pinning; `baseline-timing-lapse` shows a read beyond it fails rather than completing short. `nc-no-compaction-checkpoint` reproduces the earlier counterexample. |
| TLA-019-F4 | unjustified assumption, then production work | **Fixed** by "A background reconciler releases fork references that deleted children still owe" (`0d40dc2`) and "Fork-reference debt from before the index is backfilled, and stale debt raises an alert" (`8a03e0d`). `fork-liveness-reconciler` (the former `probe-no-client-retry` shape, with no client retry), `fork-liveness-reconciler-recreated` and `fork-liveness-backfill` pass; `nc-no-reconciler`, `nc-settle-inconclusive` and `nc-no-backfill` violate `RefEventuallyReleased`. The residual found by the model (pre-index debt overwritten by a recreation of the child's name before the backfill indexes it) is **fixed** too: the recreate CAS indexes the debt it overwrites; `fork-baseline-legacy` and `fork-liveness-backfill-recreated` pass and `nc-recreate-without-index*` reproduce the pre-fix loss. |
| TLA-019-F2 | open service obligation (H14 without further writes) | **Open.** H14 has been shown only while the partition keeps writing. H14 is kept; acceptance criteria are in `docs/dst/DST-EXPANSION-SPEC.md` §9.12.3 and `docs/READINESS.md`. |
| TLA-019-F3 | open service obligation (missing policy) | **Open.** There is no physical reclamation policy for a hard-deleted incarnation's rows. Acceptance criteria are in `docs/READINESS.md`, "Service obligations from formal verification (open)". |

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

**Found in passing, fixed later.** The fix found that a rescan rollback
followed by a re-gather can leave overlapping postings pages on its own,
which a keyed reader then read as corrupt. `d16559b3` makes readers admit an
overlapping page that agrees; see
[Overlapping postings pages after a stale re-gather](#overlapping-postings-pages-after-a-stale-re-gather-fixed).

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

### TLA-016-F2 — the `trim_safe_to` comments only held for one-advance-stale readers (documentation defect, fixed in comments)

`src/shard.rs:64-67` and the `TailFields::trim_safe_to` comment
(`src/shard.rs:601-607`) say the one-advance lag means "in-flight readers
holding a stale absorbed snapshot never lose their range".
`StaleReaderRangeIntact` (a snapshot at most one advance stale) passes, but
`witness-StaleSnapshotLosesTail` shows that a reader whose snapshot is two
or more advances stale finds part of its tail range trimmed. Reads stay correct
because every tail page is revalidated against the absorbed boundary at the
scan's own visibility (TLA-018; the applied path gained that check with the
TLA-018-F1 fix). `63202168` corrected both comments: the lag keeps the
range of a snapshot at most one advance stale, and a staler reader relies on
`absorption_race`, which revalidates each tail page against the absorbed
boundary at its scan's visibility. No code regression is needed.

### TLA-016-F4 — the receipt fix bounds a recount per refusal, not across consecutive late refusals (documentation defect, comments corrected)

`d9aeaef` ("A refused absorption group rolls its lane marks back, so a
recount covers only chunks in flight") answers each `AbsorbedBatch` receipt
when its group lands and drops it on every refusal. `settle_submissions`
rolls a mark that still rests on a refused chunk back to replay that chunk,
capped at its end. Its comments state the resulting bound:
`settle_submissions` (`src/history/gather.rs:359-362`) says "no recount spans
more than the chunks in flight when the refusal happened", and the
`stored_frame_bytes` comment (`src/shard/transaction/maintenance.rs:338-341`)
says an advance recounts "just the chunks in flight then: two, while the
committer answers within a tick".

The model confirms the aligned case. Once every submission is settled, and
no refusal was settled late and no op was dropped, the mark rests at or
below the boundary, so the next chunk starts there and recounts nothing
(`SettledMarkAtBoundary`, in `baseline-small`, `baseline-expanded`,
`baseline-recount` and `baseline-pages`; `nc-no-settle` is the pre-fix
absorber and violates it). `witness-RefusalReplayed` shows a refused chunk
rolled back, replayed from the boundary and absorbed with no recount.

The stated bound does not hold across refusals. A refusal that the absorber
settles after the stream's next chunk has raised the mark leaves the refused
chunk as a hole. The next such refusal adds another hole.
`witness-RecountBeyondInFlight` (4 records, cap 1 record, 1 queued batch):

1. Chunk `[0,1)` is submitted. Chunk `[1,2)` is planned and flushed while
   the channel is full.
2. The committer refuses `[0,1)`, and `[1,2)` is submitted (mark 2). The
   settle finds `[0,1)` refused with the mark at 2, so the mark stays.
3. `[2,3)` is planned. The committer refuses `[1,2)`, and `[2,3)` is
   submitted (mark 3). Settling that refusal would leave the mark too,
   because it rests on 3, not on 2.
4. `[2,3)` lands at boundary 0 and recounts `[0,3)`. That is three chunks,
   while at most two (one queued, one gathering) were in flight at either
   refusal.

Each late refusal needs the committer to answer after the absorber's next
tick has planned, so the committer must lag at least one tick and keep
refusing. In that regime the recount grows by one chunk per consecutive
late refusal and is bounded only by the stream's backlog, as it was before
the fix. The gather in progress counts too: with two queued batches, two
refusals and a third chunk that lands before any settle already recount
three chunks. So the LAG report's proposed bound, `Cap × MaxChan`, is not
a bound even without consecutive refusals. Even with the gather counted, as
the model's `RecountWithinInFlight` does (`Cap × (MaxChan + 1)`), the bound
is false on the fixed model, so it is checked only as the witness. The
code's cost statement for a committer that answers within a tick is
unaffected. Both comments now say so, and that consecutive late refusals
accumulate. No code regression is needed; `evidence/TLA-016_recount_beyond_in_flight.trace.txt` is the
driver's trace.

### Overlapping postings pages after a stale re-gather (fixed)

**Defect.** The TLA-016-F1 fix recorded that a rescan rollback followed by a
re-gather can leave overlapping postings pages. A page is keyed by its first
offset. Two gathers that cut the same rows into different chunks therefore
leave pages under different keys over the same offsets. The cold index load
(`append_page_runs`) refused any page starting below the accumulated end as
corrupt, so that key's reads fell back to the envelope scan for good.

**Fix.** "Overlapping postings pages from a re-gather admit when they agree"
(`d16559b3`). Each page is complete over its own span. `keep_past`
(`src/postings.rs`) admits an overlapping page only when its offsets over
the common span equal the offsets already admitted, and keeps only its part
past them. A disagreeing overlap is still corruption.

**Model.** With `Pages`, each staged chunk writes one page per routing key,
`<<key, first offset, offsets>>`, into a store keyed by (key, first offset).
A later page under the same key replaces the earlier one, and the memtable
view overlays the durable view (ASM-HISTORY-PAGES). `PagesAdmit` folds the
reader's admission over each key's pages in key order, at every state:

- a page that starts below the accumulated end must list exactly the
  accumulated offsets over `[first, min(end, its end))`;
- only its part past the end is added.

`NoStraddledChunk` says that no page of a key starts inside another page's
span.

**Checks.**

- `baseline-pages-regather` has 3 records, cap 2, ring or scan gathers, the
  rescan, the prune and a crash (a new owner). Each of those re-gathers from
  a stale boundary. It passes `PagesAdmit` with every ledger and history
  property.
- `baseline-pages` has 4 records, 2 refused groups and a failed or ambiguous
  flush, and no stale re-gather. It passes both `PagesAdmit` and
  `NoStraddledChunk`, so the receipt rollback's replay, capped at
  `replay_to`, never overlaps pages at all.
- `nc-replay-unbounded` replays up to the durable end instead. It overlaps
  the pages that a failed flush left above the refused chunk, which the
  non-ambiguous failure keeps in the memtable.
- `probe-scan-per-row` replaces the scan's one snapshot (ASM-SLATEDB-DURABLE
  (j)) with a trim point read row by row, with every record under one key.
  After a rescan rollback, the stale re-gather reads row 0, the queued
  advances land and trim row 1, and the scan stages `{0, 2}`. Its first-0
  page disagrees with row 1's page, and the index is refused. So the
  admission rests on dense chunks, which the one-snapshot scan and the
  dense ring window provide.

**Admitted overlaps.** Each witness needs only 3 records, cap 2 and
Remote-scan gathers, with no refusal or flush failure. It ends with an
overlap admitted and the stream fully absorbed and durable
(`Witness_OverlapAdmitted`). Records 0 and 1 are key K1 and record 2 is K2.
In each trace, the first gather plans while the published end is still 1.
It flushes K1's page `{0}` for chunk `[0,1)` and submits it (mark 1). The
chunk ends at 1 because of the published end at plan time, not the cap.
Then:

- `witness-OverlapAdmittedRescan`, the schedule of
  `a_rescan_regather_across_a_chunk_in_flight_keeps_the_index_readable`:
  1. Dispatch publishes end 3. The next gather plans `[1,3)` from the mark.
  2. Meanwhile the committer applies the `[0,1)` advance and it becomes
     durable (absorbed 1), but it is not dispatched, so the published
     absorbed stays 0.
  3. `[1,3)` flushes K1's page `{1}` (first offset 1) and K2's page `{2}`,
     and submits (mark 3). Its advance is queued.
  4. The dirty-index rescan reads the row at Memory level (absorbed 1).
     `roll_back_stranded_mark` removes the mark because 3 > 1.
  5. The next gather plans from the published boundary 0 and reads `[0,2)`.
     While it reads, the queued `[1,3)` advance lands and becomes durable
     (absorbed 3).
  6. The gather stages K1's page `{0, 1}`. It replaces the first-0 page and
     overlaps the first-1 page `{1}`, and the two agree over `[1,2)`, so
     the key's pages admit.
- `witness-OverlapAdmittedPrune`. `[1,3)` is submitted while `[0,1)` is
  still queued. The sweep then prunes the mark: no group is
  applied-but-undispatched, so the handle is evictable
  (ASM-HISTORY-EVICTION; a queued batch holds no handle). The re-gather from
  0 stages the same overlapping page while both queued advances land. The
  model's prune ignores `pending`. In production this path also needs the
  stream to have left the pending map, which happens once a gather reached
  its durable end. `d16559b3` names only the rescan and the new owner; its
  admission does not depend on which schedule made the overlap.
- `witness-OverlapAdmittedNewOwner`, the schedule of
  `a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable`.
  The second gather is `[1,2)`, because the published end is 2. Its K1 page
  `{1}` becomes durable through a memtable flush. The engine then closes or
  moves before either advance is durable, and the lane goes with it. The
  new owner plans from the durable boundary 0 and stages `[0,2)`, whose page
  `{0, 1}` overlaps the inherited first-1 page. It then absorbs `[2,3)`, and
  both advances land.

The traces are `evidence/TLA-016_overlap_admitted_rescan.trace.txt`,
`evidence/TLA-016_overlap_admitted_prune.trace.txt` and
`evidence/TLA-016_overlap_admitted_new_owner.trace.txt` (the driver's logs
on the current model).

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

### TLA-018-F3 — a stale applied cursor was accepted once the new tail passed it (fixed)

**Defect.** The old owner has record 0 durable and record 1 applied only. An
applied read delivers record 1 as pending and returns a session cursor past
it. Ownership moves, and record 1 is lost with the old memtable. The new owner
appends a different record 1 and a record 2. The client continues from its
session cursor, which the new owner accepted because its tail was no longer
below it: the only guard was `start > end` against the current owner's end,
and a `KIND_KEY_V2` cursor carried no owner identity and no durable frontier.
The next page delivered record 2 and moved the durable cursor past offset 1,
whose durable record the client never received. On the real code the
restarted server answered `200` with `[{"n":20}]` and a durable cursor of 3
(`evidence/TLA-018_applied_stale_cursor.trace.txt`, recorded on the pre-fix
model; `verification/regressions/TLA-018-F3/README.md`).

**Fix, as modelled.** The owner decided to tell provisional continuation
apart from durable replay, and to answer an incompatible continuation with
explicit resynchronisation instead of moving the durable cursor past unseen
records:

- A page that ends past the durable frontier returns a `Continuation`
  (`src/application/read_continuation.rs:104-109`): the writer history that
  served it (the shard prefix and `ShardEngine.writer_epoch`,
  `src/shard.rs:1135`, `:1374`), the recovery offset (the durable resume
  cursor), a digest start and a keyed, chained digest of the records the
  client observed from the digest start (`Continuation::after_page`,
  `read_continuation.rs:120-151`, called at `read_request.rs:655-666`).
  Product reads mint it as a `KIND_KEY_V3` cursor.
- `check_entry_start` (`read_request.rs:693-707`, called at `:350-352`)
  verifies it before the read: the same writer history continues; another
  writer continues only if its re-read of `[from, at)` holds exactly the
  observed records and the digest reaches down to the recovery offset
  (`verify_continuation`, `:716-751`; `Continuation::observed_in`,
  `read_continuation.rs:176-186`). Otherwise the read fails with
  `ReadFailure::HistoryReplaced` (`:747-750`): `409 cursor_beyond_tail` with
  reason `history_replaced` and the durable recovery cursor.
- A V2 token is a durable position; with `deliver=applied`, one beyond the
  durable frontier is refused (`read_request.rs:704-706`).

In the model the client holds `peng` (the engine that served the provisional
suffix; the engine number stands for the writer epoch, which every ownership
move changes), `pfrom` (the digest start) and `pdig` (the observed content per
offset of `[pfrom, pos)`, standing for the digest). `EndPage` computes them as
`after_page` does; `StartAllowed` is `check_entry_start`, with `ObservedIn`
as `observed_in` over the current engine's applied view; `RResync` is the
refusal followed by the client's resume from the recovery cursor.

**Checks.** `baseline-applied-unfiltered-expanded`, the shape of the former
known-defect check, now checks `ExactDurablePrefix` with every other property
and passes; `baseline-applied-keyed-expanded` checks the keyed read in the
same shape. `ContinuationFits` (every baseline) checks that a client past its
durable cursor always holds a continuation that fits its position. The
pre-fix behaviour is `MutNoContinuationCheck`: `nc-no-continuation-check`
violates `ExactDurablePrefix`. `witness-StaleContinuationResynced` shows a
lost, rewritten suffix refused although the replacement tail has passed the
cursor; `witness-ContinuedAcrossMove` shows a continuation that another
engine served being proven by the re-read and continuing without resync.

**Regressions.**
`dst::dst_tests::reads_applied_history::a_stale_unfiltered_continuation_is_refused_after_the_replacement_tail_passes_it`,
`dst::dst_tests::reads_applied_history::a_stale_keyed_continuation_is_refused_after_the_replacement_tail_passes_it`
and
`dst::dst_tests::reads_applied_history::an_owner_change_that_loses_nothing_keeps_the_continuation`
drive the real server through a held shard WAL `PUT` and an ownership
replacement (product read, durable mode, SSE, both raw renderings and the
relay); the stale-cursor tests fail on the unfixed code.
`application::read_continuation::tests` pins the continuation algebra.

**What remains.** A V2 session cursor minted before the fix over a suffix
that was later lost cannot be detected once the durable frontier passes it:
the fix treats a V2 token at or below the frontier as a durable position
(pinned in `stale_continuation_after_replacement` and documented in
`docs/GUIDE-COMPOSER.md`). The model's clients mint only V2 durable positions
and V3 continuations, so this migration case is exactly the pre-fix control,
not a baseline behaviour. Restoring the object store to an older snapshot can
repeat a writer epoch (ASM-HISTORY-FENCED-VIEW).

### TLA-018-F2 — H11 is not fully enforced at the reader (open obligation, owner decision)

The keyed reader treats zero postings pages as proof that a range has no
matches (`docs/ROUTING-V3.md`; `read_history2_keyed`, `src/history.rs:972`).
The unfiltered scan, the corruption envelope and `execute_postings_plan` all
skip a missing canonical row and still complete. The probes show the
consequence: `probe-lost-durable-postings` and `probe-lost-durable-canonical`
each produce a false complete page. Before their fixes, TLA-016-F3 and the
cache-bridge defect were production paths to the same observable.

H11 ("missing postings cannot produce a false complete result") therefore
holds, as checked here, for corrupt pages (served through the envelope),
unproven load windows (honest partials) and postings never made durable (H3,
TLA-016). For rows or pages lost after durability, and for a slice that
proves false absence, it holds only through ASM-SLATEDB-DURABLE,
ASM-SLATEDB-GC and ASM-HISTORY-POSTINGS-CACHE.

H11 stays as adopted, and it is only partly enforced. This is an open
obligation for the requirement's owner, framed in
`docs/dst/DST-EXPANSION-SPEC.md` §9.12.2. The two options are:

1. **Option A**, keep H11 and add an evidence mechanism: a per-chunk coverage
   record with a no-false-negative key filter, checked by the reader, so a
   lost page or row becomes an honest partial or an error instead of a
   complete page. That is production work with a format change; the probes
   would then become baselines.
2. **Option B**, deliberately revise the contract under roadmap §2.10: H11
   excludes durable loss and names ASM-SLATEDB-DURABLE, ASM-SLATEDB-GC and
   ASM-HISTORY-POSTINGS-CACHE as its premises. That weakens a
   customer-visible guarantee, so it needs the owner's recorded decision and
   compensating checks.

Until the owner decides, TLA-018 claims H11 only in the scoped sense above,
and no report may count H11 as met.

### TLA-019-F1 — the writer's stale manifest view and GC (abstraction mismatch, not reproduced; withdrawn conditionally)

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

### TLA-019-F4 — releasing a fork pin depended on a client repeating `DELETE` (fixed)

**Finding.** `fork-liveness-client-retries` passed only because `RetryDelete`,
the client, was weakly fair (the F8 premise); without it `RefEventuallyReleased`
failed (`evidence/TLA-019_fork_no_retry.trace.txt`, recorded on the pre-fix
model). There were two paths:

1. The child's `DELETE` dies after its tombstone CAS and before
   `release_fork_ref` (`witness-fork-PermanentPinWithoutRetry`).
2. The child's `DELETE` already returned success
   (`witness-fork-PinAfterSuccessfulDelete`,
   `evidence/TLA-019_fork_pin_after_successful_delete.trace.txt`). It ran
   while the creator was between its pre-check and the install CAS; the
   in-request release found the reference absent on a live source, which is
   inconclusive, so the tombstone kept its debt and `delete_lifecycle`
   returned `Ok(())`. The creator's install then landed and the creator died
   before its post-check. The client had no signal to repeat the `DELETE`,
   and the source stayed soft-deleted: its name could not be recreated (F5)
   and its data was retained.

The review ranked it service work. Both witnesses stay reachable: they are
the states the reconciler now repairs.

**Fix, as modelled.**

- A fork-debt index (`src/registry/fork_debt.rs`) holds one marker per child
  incarnation naming the release it may owe (source name, source epoch, fork
  id). `delete_lifecycle` writes it before the tombstone
  (`record_fork_debt`, `deletion.rs:290-294`); a failed write fails the
  delete before anything changed. In the model this is `IndexDebt`, and
  `ChildDelete` requires the marker (mutation point `WriteAhead`).
- A conclusive release in the same request, or a repeated `DELETE`, removes
  the marker, best effort (`settle_marker`, `deletion.rs:350-352`, `:387-390`,
  `:451-459`). In the model `InRequestRelease` settles it when conclusive and
  may fail to (mutation point `SettleMarker`).
- The supervised `fork-debt-reconcile` task (`reconcile.rs:326-356`) pages
  the markers. Per marker (`settle`, `:202-261`): a tombstone with debt runs
  `repair_tombstone`, exactly what a repeated `DELETE` runs, and drops the
  marker once the debt is paid; a tombstone without debt drops it; a live,
  initializing, sealing or retained child is deferred; a recreated name or
  missing descriptor is released from the marker, fenced to the source
  incarnation. An absent reference on a live source is inconclusive and keeps
  the marker. In the model this is `Reconcile(c)` (mutation points
  `MarkerView` and `ReleaseId`), weakly fair per marker
  (`ReconcilerFairness`, ASM-HISTORY-ACTORS).
- Tombstones from before the index carry debt with no marker. Each reconciler
  round first runs one step of the one-time backfill
  (`Registry::backfill_fork_debt`, `fork_debt.rs:263-321`;
  `reconcile.rs:275-318`), which walks the catalog and indexes every
  debt-bearing tombstone, then records completion and never runs again. In
  the model, with `Legacy`, child deletes before `Rollout` write no marker,
  `Backfill(c)` indexes such a tombstone and `BackfillFinish` completes the
  walk (mutation point `BackfillOn`).
- A recreation of the child's name overwrites its tombstone and the debt on
  it. `Registry::recreate`, the one recreate CAS every create surface uses,
  now indexes a debt the stored tombstone still carries before it writes the
  replacement (`src/registry.rs:1034`; `Registry::index_overwritten_debt`,
  `src/registry/fork_debt.rs:186-194`); a failed marker write fails the
  recreation before anything changed. This closes the residual the model
  found in the first version of this fix (below). In the model `ForkBegin`
  of a child that recreates a name marks the overwritten debt when the new
  binary runs (mutation point `IndexOverwritten`).

**Checks.** `fork-liveness-reconciler` (the former `probe-no-client-retry`
shape: no client retry) and `fork-liveness-reconciler-recreated` (the child's
name is recreated, so the debt survives only as a marker) pass
`RefEventuallyReleased` and `SoftSourceEventuallyTombstoned`;
`fork-liveness-backfill` passes them for pre-index debt.
`fork-liveness-client-retries` still passes: a client retry remains a valid
repair. `fork-baseline` adds `OwedRefIndexed`: a deleted child's reference
that still pins the incarnation it forked, with no creator left, always has a
marker, or, until the backfill completes, a pre-index tombstone that still
carries the debt. `fork-baseline-legacy` checks it in the legacy shape with
a recreated child name, and `fork-liveness-backfill-recreated` checks the
liveness properties there. Controls, each a hand mutation the commits name: `nc-no-reconciler`,
`nc-settle-inconclusive` and `nc-no-backfill` violate
`RefEventuallyReleased`; `nc-reconcile-live-child` and
`nc-release-current-name-id` violate `ReadyHoldsRef`;
`nc-no-write-ahead-marker` violates `OwedRefIndexed`;
`nc-recreate-without-index` (the pre-fix recreation) violates
`OwedRefIndexed` and `nc-recreate-without-index-liveness` violates
`RefEventuallyReleased`. Witnesses:
`witness-fork-ReconcilerReleasesLatePin` (the path-2 schedule released with
no client retry), `witness-fork-ReconcilerReleasesReplacedName`,
`witness-fork-BackfillReleased`, `witness-fork-RecreationIndexesDebt` (a
recreation after the rollout indexes a debt the backfill had not reached)
and `witness-fork-OldBinaryOverwroteDebt` (the exclusion below is
reachable).

**Regressions.**
`dst::dst_tests::fork_cleanup::a_crashed_creators_late_reference_is_released_without_a_client_retry`
(fails before the fix),
`dst::dst_tests::fork_cleanup::the_reconciler_never_releases_a_reference_a_live_creator_or_new_child_holds`,
`dst::dst_tests::fork_cleanup::an_interrupted_reconcile_pass_is_completed_after_a_restart`,
`dst::dst_tests::fork_debt::a_tombstone_older_than_the_index_is_backfilled_and_released`,
`dst::dst_tests::fork_debt::a_recreated_name_keeps_the_debt_of_the_unindexed_tombstone_it_replaced`
(fails before the recreation fix: the reconciler and the backfill ran for
10 s and the source stayed soft-deleted, pinned by the replaced child's
reference),
`dst::dst_tests::fork_debt::the_backfill_resumes_after_a_restart` and
`dst::dst_tests::fork_debt::a_debt_that_outlives_its_circles_raises_the_stale_alert`.
`dst::dst_tests::fork_cleanup::a_crashed_creators_late_reference_is_repaired_by_delete_retry`
still pins the client-retry repair.

**Residual found by the model, then fixed.** In the first version of this
fix, a debt-bearing tombstone with no marker, because an old binary wrote
it, was lost if its child's name was recreated before the backfill indexed
it: the recreation overwrote the tombstone and its debt, and nothing else
recorded the release. The model reached it after the rollout (the former
witness `UnindexedDebtOverwritten`), and
`a_recreated_name_keeps_the_debt_of_the_unindexed_tombstone_it_replaced`
reproduced it on the real code. The recreate CAS now indexes the debt first
(above). What remains is outside any repair: a debt the old binary itself
overwrote before the rollout has no record left. The properties exclude it
explicitly (`LostByOldBinary`, the ghost `lostOld`), and
`witness-fork-OldBinaryOverwroteDebt` shows it is reachable. The commit's own
known limits also stand: a marker whose creator died
before installing its reference stays pending until an operator confirms it
inert, and the `fork_debt_stale` alert is evaluated only in the telemetry
cadence.

### TLA-019-F2 — H14 convergence holds only under continued write activity (open service obligation)

`witness-QuietDeadZoneRetains` is reachable. An unreferenced SST that is newer
than the most recent compaction start, or than the newest compacted L0,
survives every later collector pass on a partition that receives no further
flush or compaction. A fenced writer's orphan is an example.
`EligibleEventuallyReclaimed` therefore includes the upstream eligibility
premise in its antecedent; under it the liveness checks pass, and
`nc-stale-inventory` shows that a frozen inventory breaks them. H14 is
kept. The DST text now says it was shown only under continued writes, and
convergence on a partition with no further writes is an open service
obligation with acceptance criteria (`docs/dst/DST-EXPANSION-SPEC.md`
§9.12.3, `docs/READINESS.md`). When a mechanism lands,
`witness-QuietDeadZoneRetains` becomes a control of the old behaviour, and a
`ReachGC` baseline without the continued-write premise must pass.

### TLA-019-F3 — no physical reclamation policy for hard-deleted incarnations' rows (open service obligation)

Hard deletion only writes a registry tombstone (`deletion.rs:465-479`). No
code deletes a deleted incarnation's shard-log or history rows; the only row
deletes are absorbed-boundary trims. The catalog's "eligible unreachable
objects eventually become reclaimable under the adopted policy" has no
adopted policy to check for these rows. This is an open service obligation
for the owner, not a defect claim. Its acceptance criteria are in
`docs/READINESS.md`, "Service obligations from formal verification (open)".

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
- the absorber's unanswered submissions are exactly the committer channel's
  batches, in order (`SubsMatchChan`, part of `TypeOK`);
- once every submission is settled, and no refusal was settled late and no
  op was dropped, the lane mark rests at or below the applied boundary, so a
  refused chunk is replayed from the boundary and its advance recounts
  nothing (`SettledMarkAtBoundary`; it fails without the receipt rollback,
  `nc-no-settle`);
- the reader admits every routing key's postings pages as one index,
  including the overlapping pages that a re-gather from a stale boundary
  (rescan rollback, eviction prune, new owner) leaves (`PagesAdmit` in both
  page shapes; `d16559b3` admits agreeing overlaps; it fails when a scan
  loses its snapshot, `probe-scan-per-row`);
- with no crash, rescan or prune, no page overlaps another at all: the
  refusal rollback's replay, capped at `replay_to`, rewrites exactly the
  refused chunk's pages (`NoStraddledChunk` in the page shape; it fails
  without the cap, `nc-replay-unbounded`);
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
flushes, refused commit groups (the absorber is told through the dropped
receipt), dropped `Absorbed` ops in groups that land (it is not told),
refusals settled before or after the stream's next chunk is planned, the
dirty-index rescan and rollback, marks
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
or `dur`. The absorber's gather, lane mark, submissions, dirty-index
observation and the committer channel are volatile. The channel carries
`(from, upto, bytes)`, and each batch's receipt is the matching submission
`[f, u, st]` in `subs`. Its status is `pending`, or `refused` (dropped
unanswered) until the absorber settles it. A landed (answered) receipt
leaves `subs` when it is answered: settling it would only remove it. The lane mark is `[from, replay]`
(`LaneMark { from, replay_to }`; `replay = 0` is none). With `Pages`, the
postings pages are `<<key, first offset, offsets>>` under the
(key, first offset) page key, in the memtable (`pgMem`) or durable
(`pgDur`). A reader sees the memtable over the durable pages, and
`PagesAdmit` is the cold index load's admission (`append_page_runs`,
`keep_past`) evaluated over that view in every state.
The postings-slice cache is modelled by the claims it makes: per key
`covered_from`, `indexed_to_offset` and the runs, and the segment's warm
record (`from`, `to`, `clean`). The reader is its published snapshot only;
the merged read is TLA-018.

### Atomicity / linearization table

| Model action | Production function(s) | Atomicity justification / dependency contract |
|---|---|---|
| `CustomerAppend` | `CommitTransaction::append` (`src/shard/transaction/append.rs:19`), `stage_stream_rows` and `write` (`finalize.rs:79`, `:184`), `publish` (`publish.rs:17`) | One `WriteBatch` per commit group, applied atomically (ASM-SLATEDB-DURABLE e). Groups carry one operation each; coalescing only removes crash points. |
| `WalDurable` | SlateDB WAL flush; `durable_seq` from `db.subscribe()` in `ShardEngine::acker_loop` (`src/shard.rs:3077`) | Remote durability is a prefix of applied order (ASM-SLATEDB-DURABLE f). |
| `Dispatch` | `ShardEngine::dispatch_durable` (`src/shard.rs:3006`) | Publishes `handle.state.durable` in group order under the handle mutex. May lag durability. |
| `Crash` | Process crash, engine close (including `write_failed` → `begin_close`, `finalize.rs:211-220`) or ownership move; the absorber task ends with the engine (`worker.rs:27-29`) | Loses applied groups, the committer channel, the lane (marks and unsettled submissions) and the partition memtable, pages included (WAL disabled, `history_settings`, `src/history.rs:499`). Keeps `D`, durable history rows and dirty rows. The process-wide postings cache is wiped (process crash) or kept (engine close or move); both are explored. |
| `HistoryBackgroundFlush` | SlateDB memtable flush (size-triggered, or the final flush in `Db::close`) | The whole memtable goes to L0 atomically (ASM-SLATEDB-DURABLE h). |
| `AbsorberSettle` | `Absorber::settle_submissions` (`src/history/gather.rs:368`) with `Lane::replay` (`:95`), called by the pump tick right before `classify_due` and `gather_due` (`src/history/worker.rs:164`) | Atomic under the lane mutex, with `try_recv` only, so it never waits on the committer. Every answered submission leaves the lane; the model removes a landed one already when the committer answers it, because settling it never moves a mark. Each refused chunk `[f, u)`, in submission order, rolls a mark with `from = u` back to `[from ↦ f, replay ↦ u]`; a mark a later chunk already raised stays. Re-pending the stream is outside the model (no roster). The absorber is one task, so settling never runs during a gather. |
| `AbsorberPlan` | `Absorber::plan_reads` → `stream_handle` → `plan_read` (`gather.rs:503`, `:529`) | Reads `st.durable` (= `P`) and the lane mark under their mutexes: `from = max(mark.from, P.abs)`; `upto = min(replay, P.next)` while `replay > from`, else `P.next` (the operator `PlanUpto`). The ring-or-scan choice is made here. Each tick settles and then gathers once (`worker.rs:164-166`), so the model enables a plan only when every answered receipt is settled. A receipt answered between the settle and the plan commutes with the plan, which reads no receipt, so no behaviour is lost. |
| `AbsorberRead`, `AbsorberReadEnd` | `read_wave` → `read_frames_range` (`gather.rs:561`; `src/shard/record.rs:129`) | A ring hit (`ring_read`, `record.rs:153`) returns the window densely, including rows a trim has deleted. Otherwise one `DurabilityLevel::Remote` scan (`record.rs:156-181`), observed row by row, over the snapshot taken when it starts (ASM-SLATEDB-DURABLE j): it skips the rows trimmed by then and no later ones. The per-stream byte cap is `Cap` equal-size records. |
| `AbsorberStage` | `stage_chunk` → `stage_rows` + `stage_postings` (`gather.rs:589`, `:166`, `:201`) | Canonical rows and postings pages go into one `WriteBatch`, atomic in the memtable. With `Pages` each key's page replaces any page under the same (key, first offset) (ASM-HISTORY-PAGES). |
| `AbsorberFlushOk` / `AbsorberFlushFail` | `Absorber::commit`: `write_with_options`, then `part.flush()` (`gather.rs:670`, `:679`, `:693`); error path in `gather_due` (`worker.rs:383`) | Flush `Ok` means every earlier write is durable. `Err` is ambiguous: rows and pages may or may not be durable, and if not they stay in the memtable. No install, no submit, no mark raise. |
| `AbsorberSubmit` (with `InstallChunk`) | After the flush: `postings_cache.install_chunk(inc, chunk_from, chunk_to, runs)` over the staged range `stage_rows` returned (recorded in `stage_chunk`, `gather.rs:631-636`; installed at `:704-707`; `src/postings_cache.rs:274`); then `submit_absorbed_batch_v2` with `(hash, plan.from, last + 1, chunk_raw)`, which returns the batch's receipt (`gather.rs:709-712`; `src/shard.rs:2011`); then `raise_lane_marks(advanced, receipt)` (`gather.rs:713`, `:731`) | One step: no await separates the install loop from the send, nor the send's return from the raise. A crash while the send is blocked leaves claims about rows that are already durable, which is the kept-cache branch of `Crash`. The raise sets `mark.from = max(mark.from, u)`, clears a `replay` it has passed and records the submission as pending. A send the closed queue refuses drops the receipt (a refused group), which the engine-close branch of `Crash` covers. The install's start is the operator `WarmInstallFrom` (F3). |
| `RescanObserve`, `RescanRollback` | `seed_from_dirty_index` → `scan_dirty_streams_page` (Memory read, `src/shard.rs:2153`) → `roll_back_stranded_mark` (`gather.rs:259`, `:284`, `:341`) | Two steps: the committer runs between the row read and the rollback. Both run in the absorber task, never during a gather. Unchanged by the receipt fix: a mark with `from` above the observed boundary is removed, `replay` with it. It is the only heal for a dropped op (`CommitDropOne`). Enabled by `Rescan`. |
| `MarkPrune` | `prune_lane_marks` (`gather.rs:406`), called every sweep tick (`worker.rs:124-126`); `evict_idle_handles` (`src/shard.rs:2405`); reload in `stream_handle` (`src/shard.rs:2343`) | Atomic under the lane mutex. Keeps a mark while the stream is pending or its resident absorbed boundary trails `mark.from`. Prunes when `P.abs ≥ mark.from` or the handle is evictable (ASM-HISTORY-EVICTION). A reloaded handle reads the Memory-level tail, which then equals `D` and `P`. The model ignores `pending`, so it prunes more often than production. Enabled by `Prune`. |
| `CommitAbsorbed` | `CommitTransaction::absorbed` (`src/shard/transaction/maintenance.rs:235`) with `stored_frame_bytes` (`:331`), then `finish`/`write`; the receipt collected by `expand` (`prepare.rs:13`, `:27`) and answered by `answer_landed` (`finalize.rs:44`) | The advance, `trim_safe_to`, the budgeted trims (`:314-324`), `unabsorbed_bytes` and the dirty row go in one batch. A chunk that starts at the boundary retires its own bytes; any other retires the stored rows of `[prev_absorbed, min(upto, next))` read at Memory level with 2 MiB × 4 read-ahead (`:282-287`, `21c5e61`). A missing row or a `checked_sub` failure (`:288-302`) refuses the whole group (`finalize.rs:5-8`) and drops the receipt (`refused`). A written group answers it before `publish` (`finalize.rs:208`), and a no-write group, where every advance was already at or below its boundary, answers it too (`finalize.rs:18`) (`landed`). The stored read does not see earlier writes of its own group, but those are trims below the boundary and appends at or above `next`, which never touch `[prev_absorbed, upto)`; one operation per group therefore loses nothing. |
| `CommitRefuseGroup` | The whole group carrying the advance is refused and its receipt dropped unanswered (`refused`): a closed engine or a billing-row pre-read failure in `CommitTransaction::run` (`src/shard/transaction/mod.rs:58-73`), another operation's accounting divergence or a failed `stored_frame_bytes` read (`finalize.rs:5-8`), the `stage_maintenance` divergence (`finalize.rs:27-33`), or the test failpoint `fail_next_absorbed_group` (`finalize.rs:23-26`) | Nothing is written; the absorber learns of it at its next settle. The divergence of this advance itself is the refused branch of `CommitAbsorbed`. Bounded by `MaxRejects`. |
| `CommitDropOne` | `stage()` drops only this `Absorbed` op when `stream_handle` fails (`mod.rs:157-165`; `reject_op`'s `_ => {}`, `mod.rs:134`) while the rest of the group lands and answers the receipt (`landed`) | The advance is lost and the absorber is **not** told; only the rescan rollback heals the mark. A layout-sealed lane drop (`maintenance.rs:257-273`) has the same shape but needs two lanes, which the model does not have. Bounded by `MaxDrops` within `MaxRejects`. |
| `TrimStep` | `TrimTick` → `expand` (`prepare.rs:13`, `:38`) → `CommitTransaction::trim` (`maintenance.rs:375-384`) | Budgeted deletes in one batch. |
| `CacheEvict` | Weight eviction at the end of `install_chunk` (`postings_cache.rs`), or the idle sweep | Removes one slice and taints the segment's warm window (`clean = false`). |
| `CacheLoad` | `runs_for` → `Decision::Lead` → `spawn_load` → `publish_load` (`postings_cache.rs`) | A cold load of all the key's pages up to the reader's absorbed boundary. Capped loads and loads that merge into a resident slice are not modelled. |
| `ReaderSnap` / `ReaderRelease` | The snapshot in `execute_segment` (`src/application/read.rs:130`) | Taken under the handle mutex. |

### Assumptions

ASM-SLATEDB-DURABLE, ASM-HISTORY-ACTORS (liveness), ASM-HISTORY-REABSORB,
ASM-HISTORY-EVICTION and ASM-HISTORY-PAGES (the page shapes, the overlap
witnesses and the scan probe). The cache property checks ASM-HISTORY-POSTINGS-CACHE for the
install path; it does not assume it.

### Constants per configuration

Every configuration uses `InitNext = 1`, `TrimBudgets = {0,1,3}` and keys
`K1, K1, K2` for offsets 0, 1, 2 (and `K1` for offset 3 where `N = 4`).
Records are one byte each, so the exact ledger is `next − abs`. *Refusals*
(`MaxRejects`) bounds refused groups and dropped ops together; *Drops*
(`MaxDrops`) bounds the dropped ops among them. *Rescan* and *Prune* enable
the dirty-index rescan rollback and the lane-mark prune; *Pages* models the
page ranges. Every configuration not listed with *Pages* has it off and
*Rescan* and *Prune* on.

| Config | N | MaxPend | MaxChan | Crashes | FlushFail | Refusals | Drops | Reads | Cap | Ring | Cache | Evictions | Pages | Rescan / Prune | Spec |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `small`, `ledger_small`, controls, witnesses (except as below) | 3 | 2 | 2 | 1 | 1 | 1 | 1 | 1 | 2 | no | no | 0 | no | yes / yes | `Spec` |
| `expanded` | 3 | 3 | 2 | 1 | 1 | 1 | 1 | 2 | 1 | yes | no | 0 | no | yes / yes | `Spec` |
| `w_RingGatherBelowTrim` | 3 | 2 | 2 | 1 | 1 | 1 | 1 | 1 | 2 | yes | no | 0 | no | yes / yes | `Spec` |
| `ledger_overlap`, `nc_retire_chunk_bytes_overlap` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 1 | 2 | no | no | 0 | no | yes / yes | `Spec` |
| `cache`, `nc_warm_install_from_plan`, `w_WarmBridgeCovers`, `w_InstallStartsAbovePlan` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 1 | yes | yes | 1 | no | yes / yes | `Spec` |
| `cache_cap2` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | yes | yes | 1 | no | yes / yes | `Spec` |
| `pages`, `nc_replay_unbounded` | 4, 3 | 2 | 2 | 0 | 1 | 2 | 0 | 0 | 2 | yes | no | 0 | yes | no / no | `Spec` |
| `pages_regather` | 3 | 2 | 2 | 1 | 0 | 0 | 0 | 0 | 2 | yes | no | 0 | yes | yes / yes | `Spec` |
| `probe_scan_per_row` (every record under K1) | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | no | no | 0 | yes | yes / no | `Spec` |
| `recount` | 4 | 1 | 2 | 0 | 0 | 2 | 1 | 0 | 1 | no | no | 0 | no | yes / no | `Spec` |
| `w_RecountBeyondInFlight` | 4 | 2 | 1 | 0 | 0 | 2 | 0 | 0 | 1 | no | no | 0 | no | yes / yes | `Spec` |
| `w_OverlapAdmittedRescan`, `…Prune`, `…NewOwner` | 3 | 2 | 2 | 0, 0, 1 | 0 | 0 | 0 | 0 | 2 | no | no | 0 | yes | yes / no, no / yes, no / no | `Spec` |
| `liveness`, `nc_retire_chunk_bytes_liveness` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | no | no | 0 | no | yes / yes | `LiveSpec` |
| `liveness_refusal` | 3 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 2 | no | no | 0 | no | yes / no | `LiveSpec` |

`recount` and `liveness_refusal` hold one applied-not-durable group and
have no prune, and `liveness_refusal` holds one queued batch, to keep the
four-record and liveness searches tractable. An earlier version of this
model kept landed receipts until the settle. On that version, `recount` with
two groups and the prune passed 9 million distinct states without
finishing, and `liveness_refusal` with two queued batches ran its temporal
check of 1,026,377 states for more than 20 minutes without finishing. The
model now drops a landed receipt when it is answered. Settling a landed
receipt never moves a mark, so this reduction is stuttering-equivalent, and
it gives back exactly the earlier state counts of the shapes with no
refusal (`ledger_overlap` 579,554, `cache_cap2` 4,234,784). The larger
shapes were not re-measured on it.

The `pages` shape has no crash, rescan or prune because each of them
re-gathers from a stale boundary and overlaps pages (the
`w_OverlapAdmitted*` witnesses), which would violate its `NoStraddledChunk`.
`pages_regather` has them and checks `PagesAdmit` instead. `pages` has no
dropped op because without the rescan a dropped op strands the mark and
absorption stops (a deadlock, not a property result).

`TrimBudgets` is the set of per-operation allowances that the shared
`trim_global_budget` and `max_trim_per_op` can leave; 0 means exhausted.

**Liveness scope (`LiveSpec`).** Faults cease because crashes, flush
failures, refusals, drops and appends are bounded. The `liveness` shape has
none of the first four; `liveness_refusal` has one refused group or one
dropped op. Fairness is on actor attempts, never on a success outcome:
`WF` on `WalDurable`, `Dispatch`, `AbsorberRead`, `AbsorberReadEnd`,
`AbsorberStage`, the flush attempt, `RescanRollback` and `ReaderRelease`.
`SF` applies to `AbsorberSettle` and `AbsorberPlan`: each tick settles and
then gathers pending streams after at most one dirty-index page, so rescans,
which disable both between their two steps, cannot starve them. A
weakly fair settle let a rescan loop keep a refused receipt unsettled forever,
a model artefact. `SF` also applies to `AbsorberSubmit`, to the committer's
handling of a message (`CommitAbsorbed ∨ CommitRefuseGroup ∨ CommitDropOne`),
to `TrimStep` and to `RescanObserve`, which are only intermittently enabled.
No fairness on appends, crashes, prunes, evictions or loads.

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
| `nc_no_settle` | `SettleRollsBack <- MutSettleNeverRollsBack` (pre-fix receipts, before `d9aeaef`) | `SettledMarkAtBoundary` | The absorber never learns of a refused group, so its mark stays above the boundary and the next advance recounts the refused chunk. |
| `nc_replay_unbounded` | `PlanUpto <- MutPlanUptoIgnoresReplay`: the replay reads to the durable end | `NoStraddledChunk` | A refused chunk's replay re-gathers past its end, over the pages a failed flush left above it (the reason for `replay_to`). |
| `probe_scan_per_row` | `ScanView <- MutScanPerRow`: the Remote scan reads the trim point row by row instead of one snapshot (ASM-SLATEDB-DURABLE (j)) | `PagesAdmit` | A trim landing mid-scan drops a row from the middle of a stale re-gather's chunk, whose page then disagrees with the earlier page of that row, and the reader refuses the key's index. The admission of overlapping pages rests on dense chunks. |

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
| `RefusalReplayed` | A refused chunk is rolled back and replayed from the boundary; every advance starts at the boundary (no recount) and absorption completes. |
| `LateRefusalRecount` | A refusal settled after the stream's next chunk was planned leaves the mark raised; that chunk's advance starts above the boundary and recounts the refused chunk with its own. |
| `RecountBeyondInFlight` | Two consecutive late refusals: an advance recounts 3 chunks, more than the 2 in flight at either refusal (TLA-016-F4). |
| `OverlapAdmittedRescan`, `OverlapAdmittedPrune`, `OverlapAdmittedNewOwner` | A re-gather from a stale boundary (after the rescan rollback, the eviction prune, or by a new owner) writes a page that overlaps an earlier chunk's; the reader admits the key's pages and absorption completes (`d16559b3`). |

### Exclusions and what is not claimed

- One stream, 3 offsets and equal-size records. Multi-stream `AbsorbedBatch`
  coalescing and the global trim budget appear only as the nondeterministic
  per-operation allowance. Cross-stream effects (the shard maintenance row,
  other streams' operations in a refused group) are inferred from code, not
  modelled.
- The pending roster, due and threshold selection, pacing, budget deferral
  and the v1/v2 lane seal are not modelled. The absorber may gather whenever
  there is published unabsorbed data, which over-approximates scheduling.
  In particular settling does not re-pend a stream, and the prune ignores
  `pending`. Discovery liveness is TLA-017.
- Time is not modelled: the tick, and how far the committer lags it, are
  free. So a refusal may be settled before or after the stream's next chunk
  is planned (a late refusal) as often as refusals allow. Production needs a
  committer lagging at least one tick for the late case (TLA-016-F4).
- The committer handles one batch per group. Coalescing several batches
  into one group, which refuses or lands them together, is approximated by
  consecutive single-batch groups with the same outcome, reachable only
  where the refusal bound allows as many refusals.
- Postings page encoding, buckets and the 32 KiB page split are abstracted.
  With `Pages`, a page is its key, first offset and offsets
  (ASM-HISTORY-PAGES). The reader is the admission predicate over each key's
  pages in the store. The cold load's own read, its caps and the envelope
  fallback are not modelled. The cache's admission line, capped
  loads, loads merging into a resident slice, the warm-record cap and idle
  expiry, and failed seams are not modelled. The cache-bridge defect found during the F3 fix lived in
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
  the page (`read_request.rs:648-652`). Records at or beyond it may be
  replaced after a crash or ownership move. The code's stated intent is that
  applied reads never see less than a durable reader
  (`src/shard/record.rs:285-288`). A page that ends past the durable resume
  cursor returns a provisional continuation (`KIND_KEY_V3`, TLA-018-F3 fix)
  bound to the writer history that served the suffix and a digest of what
  the client observed; the next read continues it only on that history or
  after a re-read proves the observation, and otherwise answers
  `409 cursor_beyond_tail` (`history_replaced`) with the durable recovery
  cursor. A V2 token is a durable position.
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
  durable content, and no ineligible record was delivered, across an
  ownership move that loses and rewrites an applied suffix (it failed before
  the TLA-018-F3 fix);
- `ContinuationFits`: a client whose position is past its durable cursor
  holds a continuation that fits it (`Continuation::fits`), and no other
  client holds one;
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

**Requirement anchors:** H1, H2, H9, H10 (abstracted), H11 (in the scoped
sense of TLA-018-F2), D8, D9.

### Observation boundary

The reader observes its engine's handle snapshot (`P`, and `A` for the
applied end), the live history partition (`hF`), tail rows one at a time
(Remote sees `D.trimmed`, Memory sees `A.trimmed`), the durable tail ring,
and the absorbed boundary at the scan's visibility (`D.abs` for durable,
`A.abs` for applied). After an ownership move, the old engine's views are
frozen (ASM-HISTORY-FENCED-VIEW) or the read fails. The client keeps its
position, its durable cursor, the content it last received per offset, and
its continuation: the serving engine, the digest start and the digest.

### Atomicity / linearization table

| Model action | Production function(s) | Atomicity justification / dependency contract |
|---|---|---|
| `WAppend`, `WDurable`, `WDispatch`, `WAdvance`, `WTrim` | As TLA-016 (`CommitTransaction::append`, `absorbed` and `trim`; WAL durability; `dispatch_durable`) | As TLA-016. |
| `WHistFlush` | A gather: one `WriteBatch` and `part.flush()` (`absorb_gather_v2_with` through `Absorber::commit`, `src/history/gather.rs:317-590`) | Collapsed into one step that raises the contiguous durable frontier `hF`. Justified by TLA-016's H3 and `LastRecoverableCopy` (ASM-HISTORY-WRITER). |
| `WMove` | Ownership move: the new engine opens the shard DB, fencing the old writer, and loads the durable tail | ASM-HISTORY-FENCED-VIEW. The old engine keeps frozen, self-consistent views. |
| `WLosePostings`, `WLoseCanonical` | **Not production.** Probe actions only | They violate ASM-SLATEDB-DURABLE or ASM-SLATEDB-GC. |
| `RStart` | `ReadService::execute_read` (`read_request.rs:259-452`): `tail_state`, then `check_entry_start` (`:350-352`, `:693-707`) and the `start > end` guard (`:353-355`); snapshot in `execute_segment` (`src/application/read.rs:129`, `:145-164`) | Taken under the handle mutex. `StartAllowed` is `check_entry_start` (mutation point `ContinuationCheck`): the same engine continues; another engine continues only if `ObservedIn`, `verify_continuation`'s re-read of `[pfrom, pos)` (`:716-751`) on the current engine's applied view, matches the digest (`Continuation::observed_in`, `read_continuation.rs:176-186`); a V2 position starts only at or below `handle.durable.next` (`:704-706`). The re-read is one atomic observation of the current engine; it reads the same history and tail rows as a page. |
| `RHist` | `decode_history_range` (`read.rs:719`) → `read_history2_scan` (`src/history.rs:921`) or `read_history2_keyed_cached` (`history.rs:1031`) → `PostingsCache::runs_for` (`src/postings_cache.rs:508`) → `execute_postings_plan` (`src/history/postings_read.rs:13`), or the corruption envelope; `PageBudget` | One step: rows below the boundary are immutable. A missing canonical row is skipped silently by every source, as in production (`history.rs:921-948`; `postings_read.rs:80-96`). Postings runs are abstracted by ASM-HISTORY-POSTINGS-CACHE. |
| `RTailStart` | `ring_read` (`src/shard/tail_ring.rs:97`) and `proves_durable_ring` (`src/shard/record.rs:104`), or the start of `read_frames_until` (`record.rs:272`) | The ring returns durable copies with a density proof (ASM-HISTORY-RING); durable mode only (`record.rs:307`). |
| `RTailStep` | One row of the scan at `deliver.durability()`: Remote for durable, Memory for applied (`record.rs:325`, `:212-217`) | Per row: the iterator is not treated as a snapshot. |
| `RTailCheck` | `absorption_race` (`read.rs:789`) → `ShardEngine::visible_absorbed(hash, visibility)` (`read.rs:827`, `:837-841`; `record.rs:233-256`), then the loop decision in `execute_segment` | A `get` of the tail row at the scan's own level. The operator `RaceBoundary` is this read (F1 fix). |
| `EndPage` | `page_progress` (`read.rs:625`), then `durable_resume.after = next.after.min(floor)` with `floor` read after the page (`read_request.rs:641-652`), then `Continuation::after_page` (`:655-666`; `read_continuation.rs:120-151`) | One page and one end-of-page handle read. The continuation's history is the page's engine (`WriterHistory::of(&engine)`); the digest restarts at the recovery offset when it reached the page start, else carries the continuation the page began at, else starts at the page start. |
| `RReconnect` | The client resumes from `Prisma-Durable-Cursor` (applied mode), a V2 position | Client behaviour. |
| `RResync` | `ReadFailure::HistoryReplaced` (`read_request.rs:747-750`), rendered as `409 cursor_beyond_tail` with `history_replaced` and the recovery cursor; the client resumes there | Enabled when another engine serves: a failing or partial re-read also refuses. Redelivery from the recovery cursor overwrites the client's content at and after it. |
| `RError` | The page fails: the old engine is closed, or (history leg, current engine too when `AllowReadError`) a storage read fails, for example a transient object-store error | ASM-SLATEDB-GC (iii): an error, never a short success. The page ends with no delivery. |

There is no separate action for the refusals at `RStart`: the model's
`RStart` requires `pos < ReadEndNow` (the `start > end` guard and the
empty-page case) and `StartAllowed`; `RResync` is the continuation refusal.
The V2 refusal past the durable frontier is not reachable in the model: the
client's V2 positions are durable cursors, which never exceed any later
`P.next` (a move publishes `P' = D`).

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
| `applied_unfiltered_expanded`, `nc_no_continuation_check`, `w_ContinuedAcrossMove`, `w_StaleContinuationResynced` | applied | none | 1 (2 appends; applied suffix lost on the move) | 5 | (n/a) | no | no | no | — |
| `applied_keyed_expanded` | applied | K1 | 1 (2 appends; applied suffix lost on the move) | 5 | (n/a) | yes | yes | no | — |
| `probe_lost_postings` | durable | K1 | 3 | 4 | yes | yes | yes | yes | postings |
| `probe_lost_canonical` | durable | none | 3 | 4 | yes | no | no | yes | canonical |

Every baseline checks every property, `ExactDurablePrefix` included; before
the TLA-018-F3 fix `applied_unfiltered_expanded` left it to a known-defect
check. History read errors are off in the expanded shapes to keep them within
the time budget. An `RError` only ends a page before any delivery, so it adds
prefixes of existing behaviours and cannot create a violation of these
properties; the small shapes and `witness-ReadErrorCurrentEngine` exercise
it. `WAppend` and `RReconnect` are disabled by construction in the small
shapes (no appends, no pending records); the expanded applied shapes exercise
them, and only they produce continuations over a suffix a move can lose. `WLosePostings` and `WLoseCanonical` are enabled only in the probes.

### Negative controls and probes

| Control | Operator substituted | Must violate | Why |
|---|---|---|---|
| `nc_old_history_view` (unfiltered) | `HistView <- MutHistViewAtPageStart`: the history leg reads a view captured at page start while the race check adopts a newer boundary | `ExactDurablePrefix` | A new absorbed frontier with an old history view. |
| `nc_filtered_race_never` (keyed) | `FilteredRace <- MutFilteredRaceNever`: the filtered tail scan is accepted without the absorbed boundary | `ExactDurablePrefix` | Advances past an unreturned eligible record. |
| `nc_short_index_accepted` (keyed) | `ShortIndexAccepted <- MutShortIndexAccepted`: `provable_to < upto` is treated as complete | `ExactDurablePrefix` | Unproven postings become an empty success. |
| `nc_applied_race_remote` (applied keyed) | `RaceBoundary <- MutRaceBoundaryRemote` (pre-fix F1) | `ExactDurablePrefix` | A durable record trimmed by an applied, not durable advance is skipped. |
| `nc_applied_race_remote_unfiltered` (applied unfiltered) | `RaceBoundary <- MutRaceBoundaryRemote` (pre-fix F1) | `TailGapExplained` | The page ends as an honest partial with no progress. |
| `nc_no_continuation_check` (applied unfiltered expanded) | `ContinuationCheck <- MutNoContinuationCheck` (pre-fix F3): only `start > end` guards the entry span | `ExactDurablePrefix` | A continuation over a lost, rewritten suffix is accepted once the new tail passes it. |
| `probe_lost_postings` (keyed; **probe**) | `AllowLostPostings = TRUE`: a durable postings page disappears after the advance (a dependency-contract mutation) | `ExactDurablePrefix` | The reader cannot detect a lost page (H11 open obligation, DST-EXPANSION-SPEC §9.12.2); a slice that falsely proves absence (TLA-016-F3) has the same observable. |
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
| `ContinuedAcrossMove` (applied unfiltered expanded) | A continuation that another engine served is proven by the current engine's re-read, and the read continues without resync (an owner change that lost nothing). |
| `StaleContinuationResynced` (applied unfiltered expanded) | A continuation whose suffix the move lost and rewrote is refused with the recovery cursor although the replacement tail has passed it (the F3 schedule, fixed). |

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
- The continuation digest is modelled as the observed content per offset
  (`pdig`), and the writer history as the engine number. A digest collision,
  a repeated writer epoch after an object-store restore
  (ASM-HISTORY-FENCED-VIEW), the 8 MiB bound on the verification re-read
  (a larger range resyncs) and relays to owners that report no history
  (`WriterHistory::UNKNOWN`, whose continuation is always re-verified) are not modelled; each
  only adds a resync or relies on the assumption.
- Reads start at the beginning, never at `now`; a session started at `now`
  has a digest start above its recovery offset and always resyncs after an
  owner change until the frontier passes its start (unit-tested in
  `read_continuation.rs`). V2 session cursors minted before the fix over a
  later-lost suffix stay undetectable once the frontier passes them; that
  pre-fix behaviour is `nc_no_continuation_check`.
- One ownership move per behaviour; a second move would lose a second suffix
  and is not explored.

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
`delete_transition`, `anchor::install` and `release_fork_ref`, with the
fork-debt index (`src/registry/fork_debt.rs`) and the `fork-debt-reconcile`
task (`src/application/creation/reconcile.rs`) that settles what a deleted
child still owes.

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
- ForkPin `OwedRefIndexed`: a deleted child's reference that still pins
  the incarnation it forked, with no creator left to release it, always has a
  fork-debt marker, or, until the backfill completes, a pre-index tombstone
  that still carries the debt (no crash, settlement or recreation after the
  rollout leaves debt the reconciler cannot find).
- ForkPin `RefEventuallyReleased` / `SoftSourceEventuallyTombstoned`
  (liveness, under the reconciler's fairness and with no client retry; also
  under the client-retry premise alone when no child name is recreated): once
  a child is gone, its reference is released, and a soft-deleted source whose
  children are gone is tombstoned. With pre-index debt this holds through the
  backfill and the recreation's indexing, except for a debt the old binary
  overwrote before the rollout, which no record survives (`LostByOldBinary`).

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
`parent_ref_pending` debt, whether a `DELETE` is past its tombstone CAS,
each child incarnation's fork-debt marker, which child incarnation holds each
child name (`Prev`), and whether the binary with the index is deployed.

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
| `ForkBegin` | `fork::prepare` validates the live current incarnation (`src/application/creation/fork.rs:28`); for a recreated name, `Registry::recreate` (`src/registry.rs:1003-1061`), which indexes the debt of the stored tombstone first (`:1034`; `fork_debt.rs:186-194`) | For a child incarnation that recreates a name (`Prev`), the create overwrites the previous incarnation's tombstone and its debt; only its marker remains. The marker write and the CAS are two writes merged into one step: a crash between them leaves the marker beside the unchanged tombstone, which the reconciler repairs as usual. Mutation point `IndexOverwritten`. Before the rollout the old binary writes no marker (`lostOld`). |
| `ForkInstall` | `anchor::install`: `mutate_incarnation(source, forked epoch)` (`src/application/creation/anchor.rs:78`) | One CAS bound to the forked incarnation (ASM-OBJSTORE-CAS). Idempotent when already installed; declines on a soft, tombstoned or recreated source. Mutation point `InstallFence`. |
| `ForkPostCheck` | `anchor.rs:134-182`: if the child incarnation vanished (lookup by name, bound to its epoch), release the fresh reference; otherwise require the source name's current descriptor to list the fork id | The release is one `release_fork_ref` CAS. The source presence check is by name, with no epoch check. |
| `CreatorCrash` | The create request dies before its post-check | Bounded fault. |
| `SourceDelete` | `delete_lifecycle` → `delete_transition` (`deletion.rs:249`, `:492`) | Soft versus tombstone is decided inside the CAS. Mutation point `DeleteDecision`. |
| `SourceRecreate` | A create under the same name after the tombstone; blocked while soft-deleted (F5) | New incarnation with no children. |
| `IndexDebt` | `delete_lifecycle` writes the child incarnation's fork-debt marker before the tombstone write (`record_fork_debt`, `deletion.rs:290-294`; `fork_debt.rs:148-176`) | One PUT. A failed write fails the delete before anything changed; a delete that dies here leaves a marker on a live child, which the reconciler defers. |
| `ChildDelete` | `delete_transition` on the child: the tombstone records `parent_ref_pending` in the same write | One CAS. After the rollout it requires the marker (mutation point `WriteAhead`); before it (`Legacy`) the old binary writes none. |
| `InRequestRelease` | The same request's `release_fork_ref(source, fork_id, source_epoch)`, `clear_parent_debt` when conclusive, then `settle_marker` (`deletion.rs:350-352`, `:72`, `:37`, `:451-459`) | The epoch check and the CAS are bound to one snapshot; an incarnation change is conclusive. One step. The marker removal is best effort (it may fail and stay) and only after a conclusive release (mutation point `SettleMarker`). |
| `RequestAbandon` | A crash or cancellation after the tombstone CAS | Bounded fault. The debt persists on the tombstone and the marker in the index. |
| `RetryDelete` | The **client** re-issues `DELETE` → `delete_lifecycle` → `repair_tombstone` (`deletion.rs:273-274`, `:372`) | Only while the name still holds the child's tombstone. Its fairness is the operator `ClientRetryFairness`, used only by `LiveSpecClientRetries`. |
| `Reconcile` | `fork-debt-reconcile` (`reconcile.rs:326-356`), one marker of a page (`settle`, `:202-261`): `repair_tombstone` for a tombstone with debt, marker removal for one without, `release_fork_ref` from the marker for a recreated name, deferral for a live child | One step per marker: each CAS in it is idempotent and a restart re-reads the marker. Mutation points `MarkerView`, `ReleaseId`, `SettleMarker`. Weakly fair per marker (`ReconcilerFairness`, ASM-HISTORY-ACTORS). |
| `Rollout` / `Backfill` / `BackfillFinish` | Deploying the binary with the index; one backfill step per reconciler round (`Registry::backfill_fork_debt`, `fork_debt.rs:263-321`; `reconcile.rs:275-296`) indexing each debt-bearing tombstone it walks, until the walk records `complete` and never runs again (`fork_debt.rs:279-281`, `:298-303`) | The walk's page order, persisted progress and ETag-conditional write are abstracted: until it completes, the backfill may index any unindexed debt-bearing tombstone that still holds its name, and it completes only when none is left (after the rollout no delete creates one). Without `Legacy` the index predates every delete, so the backfill starts complete. Mutation point `BackfillOn`. |

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
| ForkPin `baseline`, safety controls, witnesses except the legacy ones | children `{F1, F2, F3}`, F3 recreating F1's name; `MaxEpoch = 2`, `MaxCrashes = 1` | | | | | | | `Spec` |
| ForkPin `liveness_reconciler`, `nc_no_reconciler`, `nc_settle_inconclusive` | children `{F1, F2}` (distinct names) | | | | | | | `LiveSpecReconciler` |
| ForkPin `liveness_reconciler_recreated` | children `{F1, F3}`, F3 recreating F1's name | | | | | | | `LiveSpecReconciler` |
| ForkPin `liveness_client_retries` | children `{F1, F2}` | | | | | | | `LiveSpecClientRetries` |
| ForkPin `liveness_backfill`, `nc_no_backfill`, `w_BackfillReleased` | children `{F1, F2}`, `Legacy` | | | | | | | `LiveSpecReconciler` / `Spec` |
| ForkPin `baseline_legacy` | children `{F1, F2, F3}`, F3 recreating F1's name, `Legacy` | | | | | | | `Spec` |
| ForkPin `liveness_backfill_recreated`, `nc_recreate_without_index`, `nc_recreate_without_index_liveness`, `w_RecreationIndexesDebt`, `w_OldBinaryOverwroteDebt` | children `{F1, F3}`, F3 recreating F1's name, `Legacy` | | | | | | | `LiveSpecReconciler` / `Spec` |

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
on any writer, delete-success or pin step. ForkPin: creator and delete
crashes are bounded, so they cease. `WF` on each child's in-process request
steps (`ForkInstall ∨ ForkPostCheck ∨ InRequestRelease`). `LiveSpecReconciler`
adds `WF` on each marker's `Reconcile` and `Backfill` step, on
`BackfillFinish` and on `Rollout`,
and no fairness on the client: nobody repeats `DELETE`. `LiveSpecClientRetries`
has `WF` on `RetryDelete` (`ClientRetryFairness`, the F8 premise) and none on
the reconciler. No fairness on deletes, forks, recreation or success
outcomes. The 3-child shape is used for safety only: a scratch liveness run
of it was stopped after about 10 minutes with 0.86 million distinct states
found and the queue still growing, so each liveness property is checked in two 2-child shapes, one with
distinct names and one with a recreated name.

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
| ForkPin `nc_no_reconciler` | repository (pre-fix F4) | `ReconcilerFairness <- NoReconciler`: the reconciler never runs, and no client repeats `DELETE` | `RefEventuallyReleased` |
| ForkPin `nc_settle_inconclusive` | repository | `SettleMarker <- MutSettleAlways`: a marker is removed after an inconclusive release | `RefEventuallyReleased` |
| ForkPin `nc_no_backfill` (legacy shape) | repository (pre-8a03e0d) | `BackfillOn <- MutNoBackfill`: pre-index debt is never indexed | `RefEventuallyReleased` |
| ForkPin `nc_reconcile_live_child` | repository | `MarkerView <- MutMarkerViewNoDefer`: a live child's marker is paid from the marker | `ReadyHoldsRef` |
| ForkPin `nc_release_current_name_id` | repository | `ReleaseId <- MutReleaseCurrentNameId`: a recreated name's marker releases the fork id of the incarnation that now holds the name | `ReadyHoldsRef` |
| ForkPin `nc_no_write_ahead_marker` | repository | `WriteAhead <- MutNoWriteAhead`: the tombstone is written without the marker ahead of it | `OwedRefIndexed` |
| ForkPin `nc_recreate_without_index` (legacy, recreated name) | repository (pre-fix recreation) | `IndexOverwritten <- MutNoIndexOverwritten`: the recreate CAS overwrites a debt-bearing tombstone without indexing its debt | `OwedRefIndexed` |
| ForkPin `nc_recreate_without_index_liveness` (legacy, recreated name) | repository (pre-fix recreation) | as above | `RefEventuallyReleased` |

The former probe `probe_no_client_retry` (no client retry, no reconciler)
is retired: with the reconciler the same premise is the passing baseline
`fork-liveness-reconciler`, and `nc_no_reconciler` is its pre-fix behaviour.

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
| ForkPin `PermanentPinWithoutRetry` | A child is gone and its reference still pins a soft-deleted source with no request in flight: before the fix only a client retry released it (F4); now its marker does. |
| ForkPin `PinAfterSuccessfulDelete` | A child's `DELETE` returned success, then the creator's late install landed and the creator died, so the reference pins the source with no client signal to retry (the F4 schedule). |
| ForkPin `ReconcilerReleasesLatePin` | The reconciler releases the reference of a child whose `DELETE` had returned success, with no client retry. |
| ForkPin `ReconcilerReleasesReplacedName` | The reconciler releases, from the marker, the reference of a child incarnation whose name was recreated. |
| ForkPin `BackfillReleased` (legacy shape) | A pre-index debt is indexed by the backfill and released by the reconciler. |
| ForkPin `RecreationIndexesDebt` (legacy shape, recreated name) | After the rollout, a recreation of a child's name indexes the debt of a tombstone the backfill has not reached. |
| ForkPin `OldBinaryOverwroteDebt` (legacy shape, recreated name) | Before the rollout, the old binary's recreation overwrites a debt nothing recorded; the properties exclude that reference. |

The former witness `UnindexedDebtOverwritten` (the residual after the
rollout) is retired: with the fix it is unreachable, and
`nc-recreate-without-index` reproduces it.

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
- The reconciler's paging (64 markers a pass, a circle every
  `FORK_DEBT_SWEEP_SECS`), the backfill's page walk and progress object, the
  ancestor walk inside `repair_tombstone`, unparseable markers (skipped and
  left in place) and the `fork_debt_stale` alert are not modelled; one
  reconciler step settles one marker, weakly fair per marker. A debt the old
  binary overwrote before the rollout is excluded from the claims.
- A recreation that races another instance's old-binary delete of the same
  name during a mixed-version rollout is not modelled; the recreate CAS
  indexes whatever tombstone it reads.

---

## Coverage

Coverage was measured by rerunning baselines directly with `tlc2.TLC
-coverage 1` on the final models (2 workers), outside the driver. Every run
completed its search. `Terminated` is the stuttering quiescence step and never
adds a state.

| Model | Configurations | Actions | Actions that added no state |
|---|---|---|---|
| HistoryAbsorb | `small` (31,506,682 states), `cache_cap2` (4,234,784), `pages_regather` (1,103,474), on the model with batch receipts (`d9aeaef`) and pages | 25 | none across the three runs; `small` has no cache actions, `cache_cap2` no crash, flush failure, refusal, drop or reader, `pages_regather` no cache, flush failure, refusal, drop or reader; `AbsorberSettle` fires only where a group is refused, so only in `small` |
| ReadCompose | `durable_keyed_small` (12,482,967, before the TLA-018-F3 fix), `applied_unfiltered_expanded` (19,384,259, rerun after the fix) | 17 | `WLosePostings`, `WLoseCanonical`: probe-only actions, disabled in every baseline; they fire in the probes' counterexamples. `RResync` (new) adds 12,884 distinct states in `applied_unfiltered_expanded` |
| ReachGC | `small` (3,938,288), `expanded` (27,990,301) | 18 | none across the two runs; `small` has no user checkpoint (`CkCreate`, `CkRelease`) |
| ForkPin | `baseline` (3,374,329), `liveness_backfill` (564,266), both after the TLA-019-F4 fix | 16 | none across the two runs; `baseline` has no `Rollout`, `Backfill` or `BackfillFinish` (the index predates every delete), which `liveness_backfill` covers |

The action counts exclude `Init` and `Terminated`.

## Files

| File | Purpose |
|---|---|
| `HistoryAbsorb.tla`, `MC_HistoryAbsorb.tla` | TLA-016 model and its wrapper (`Mut*` control operators, key map) |
| `ReadCompose.tla`, `MC_ReadCompose.tla` | TLA-018 model and wrapper |
| `ReachGC.tla`, `MC_ReachGC.tla` | TLA-019 physical object graph, the upstream GC contract and the repository's reliance on it, with wrapper |
| `ForkPin.tla`, `MC_ForkPin.tla` | TLA-019 registry fork pin, with wrapper |
| `MC_*_<shape>.cfg` | baselines, liveness configurations included |
| `MC_*_kd_*.cfg` | known-defect checks (one property each; none today) |
| `MC_*_nc_*.cfg`, `MC_*_probe_*.cfg` | negative controls and probes (one operator substituted, one property each) |
| `MC_*_w_*.cfg` | reachability witnesses (one `Witness_*` invariant each) |
| `evidence/*.trace.txt` | TLC traces of the findings, trimmed to the trace (below) |
| `../../manifest.json`, `../../assumptions.md`, `../../receipts/TLA-01{6,8,9}.json` | obligations and checks, assumption entries, receipts of the recorded runs |
| `../../regressions/TLA-018-F3/README.md` | the cursor defect's real-code reproduction, its fix and the regressions that pin it |

| Evidence trace | Finding | Recorded on |
|---|---|---|
| `TLA-016_ledger_under_retire.trace.txt`, `TLA-016_ledger_over_retire.trace.txt`, `TLA-016_liveness_stall.trace.txt` | TLA-016-F1 | the pre-fix model (labelled) |
| `TLA-016_cache_false_absence.trace.txt` | TLA-016-F3 | the pre-fix model (labelled) |
| `TLA-016_recount_beyond_in_flight.trace.txt` | TLA-016-F4 | the current model: the driver log of `witness-RecountBeyondInFlight` |
| `TLA-016_overlap_admitted_rescan.trace.txt`, `TLA-016_overlap_admitted_prune.trace.txt`, `TLA-016_overlap_admitted_new_owner.trace.txt` | overlapping postings pages (fixed by `d16559b3`) | the current model: the driver logs of `witness-OverlapAdmittedRescan`, `-Prune` and `-NewOwner` |
| `TLA-018_applied_keyed_skip.trace.txt` | TLA-018-F1 | the pre-fix model (labelled) |
| `TLA-018_applied_stale_cursor.trace.txt` | TLA-018-F3 | the pre-fix model (labelled): the driver log of the former `known-defect-stale-applied-cursor` |
| `TLA-019_reader_view_deleted.trace.txt` | TLA-019-F1 (withdrawn) | the model without the compactor checkpoint (labelled) |
| `TLA-019_fork_no_retry.trace.txt`, `TLA-019_fork_pin_after_successful_delete.trace.txt` | TLA-019-F4 | the pre-fix model (labelled): the driver logs of the former `probe-no-client-retry` and of `witness-fork-PinAfterSuccessfulDelete` |
