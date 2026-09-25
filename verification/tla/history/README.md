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
| TLA-016 | `pass-with-recorded-scope` | TLA-016-F1 (fixed by slate's `9c6675d7` and `b5751e75`, adopted by the merge `3a24eace`; our `6371da0a` is superseded): the absorbed advance retired the chunk's bytes, not the range it moved over. TLA-016-F3 (fixed): the postings warm install claimed coverage over a trimmed head it never read; slate's code still had it, and the merge re-applied our warm-install rule. Overlapping postings pages after a re-gather (fixed by `d16559b3`, readers admit overlapping pages that agree; they still arise from a refused chain, the ungated prune and a new owner). | Follow-ups, not defects: heal latency of a refused quiet stream (up to one rescan period) and settlement-bucket sharing. |
| TLA-018 | `pass-with-recorded-scope` | TLA-018-F1 (fixed): an applied keyed read skipped durable records trimmed by a non-durable advance. TLA-018-F3 (fixed): a stale applied cursor was accepted once the new owner's tail passed it. | TLA-018-F2 (open obligation, owner decision): H11 holds at the reader only through durability and the cache contract; `docs/dst/DST-EXPANSION-SPEC.md` §9.12.2. |
| TLA-019 | `pass-with-recorded-scope` | TLA-019-F4 (fixed): releasing a fork pin after an interrupted or raced `DELETE` depended on the client repeating `DELETE`; a background reconciler now releases it. TLA-019-F1 was an abstraction mismatch, withdrawn conditionally: the compactor's checkpoint protects the SSTs a stale writer view still names while ASM-SLATEDB-COMPACTION-CHECKPOINT holds. | Open service obligations (`docs/READINESS.md`): TLA-019-F2, GC convergence without further writes (H14); TLA-019-F3, no physical reclamation policy for hard-deleted incarnations' rows. |

No defect in this group is open, so no check has the `known-defect` role.
Each fixed defect has a passing baseline and a negative control that
reproduces the pre-fix behaviour. The group also records two documentation
defects: TLA-016-F2, fixed in comments, and TLA-016-F4, fixed in a fix's
comments and moot since the merge dropped the recount it concerned. It
records one open obligation that needs an owner decision (TLA-018-F2,
H11), two open service obligations (TLA-019-F2, H14 without further writes;
TLA-019-F3, physical reclamation), and two defects found in passing by the
fixes, both fixed: overlapping postings pages after a re-gather from a
stale boundary (TLA-016 now models the pages and the reader's admission of
agreeing overlaps) and a cache bridge over dropped runs (the model cannot
express it). TLA-019-F4 was first recorded as an unjustified assumption (the
client retry); the fix made it production work, and it is listed as fixed.

File and line references in the TLA-016 mapping table and its F1 and F3
findings are at `3a24eace`, the merge of `slate` whose absorber the model now
maps. Those in the TLA-018 and TLA-019 findings and mapping
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

Each table is rendered from the obligation's receipt, except TLA-016's,
which is rendered from an unrecorded run of the model reworked for the merge
`3a24eace` (see below). Each receipt was
recorded with `formal.py run --record` on a clean tree: TLA-016 (pre-merge
model) on `3f386070`, TLA-019 on `1d20f76f`, and TLA-018 again on `cb4c6b47`, after a
mutation-lane test landed in `read_continuation.rs`. `formal.py check --fresh`
reports all three current at `cb4c6b47`. Tools: TLC
2.19 (tla2tools 1.7.4) on Java 17.0.1, aarch64-apple-darwin. The driver ran
every check with 2 workers while up to five driver processes shared an 8-core
laptop, so the seconds are wall time under load. For a violation, the
distinct states are those explored until TLC found it. `(temporal)` marks a liveness
violation, which TLC reports without the property name; each such
configuration checks one property.

### TLA-016 (`pass-with-recorded-scope`)

Not yet recorded for the reworked model. The table is rendered from an
unrecorded driver run (`formal.py run --id TLA-016 --out
target/formal/mg-absorb`, without `--record`) on the working tree over
`3a24eace` with the model reworked for the merge: 43 checks, every verdict
as expected; 31 min of TLC wall time in total, while other agents' runs
loaded the same 8-core laptop (load average above 30). The receipt
`verification/receipts/TLA-016.json` still describes the pre-merge model's 46
checks on `3f386070` and is stale until it is re-recorded.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-small` | `HistoryAbsorb` / `small` | baseline | pass | pass | 5,254,914 | 245.6 |
| `baseline-expanded` | `HistoryAbsorb` / `expanded` | baseline | pass | pass | 12,949,808 | 442.9 |
| `ledger-small` | `HistoryAbsorb` / `ledger_small` | baseline | pass | pass | 19,579,296 | 882.6 |
| `ledger-overlap` | `HistoryAbsorb` / `ledger_overlap` | baseline | pass | pass | 174,438 | 6.2 |
| `liveness-small` | `HistoryAbsorb` / `liveness` | baseline | pass | pass | 39,145 | 10.7 |
| `liveness-refusal` | `HistoryAbsorb` / `liveness_refusal` | baseline | pass | pass | 174,625 | 53.0 |
| `baseline-cache` | `HistoryAbsorb` / `cache` | baseline | pass | pass | 468,267 | 22.2 |
| `baseline-cache-cap2` | `HistoryAbsorb` / `cache_cap2` | baseline | pass | pass | 1,231,603 | 58.2 |
| `baseline-pages` | `HistoryAbsorb` / `pages` | baseline | pass | pass | 294,373 | 12.7 |
| `baseline-pages-regather` | `HistoryAbsorb` / `pages_regather` | baseline | pass | pass | 945,441 | 34.7 |
| `nc-publish-before-flush` | `HistoryAbsorb` / `nc_publish_before_flush` | negative-control | violation `H3_AbsorbedBackedByDurableHistory` | violation `H3_AbsorbedBackedByDurableHistory` | 369 | 1.0 |
| `nc-canonical-only` | `HistoryAbsorb` / `nc_canonical_only` | negative-control | violation `H3_AbsorbedBackedByDurableHistory` | violation `H3_AbsorbedBackedByDurableHistory` | 603 | 1.0 |
| `nc-trim-to-proposed` | `HistoryAbsorb` / `nc_trim_to_proposed` | negative-control | violation `StaleReaderRangeIntact` | violation `StaleReaderRangeIntact` | 912 | 1.0 |
| `nc-duplicate-collapse` | `HistoryAbsorb` / `nc_duplicate_collapse` | negative-control | violation `StaleReaderRangeIntact` | violation `StaleReaderRangeIntact` | 17,784 | 1.8 |
| `nc-tick-trim-to-absorbed` | `HistoryAbsorb` / `nc_tick_trim_to_absorbed` | negative-control | violation `StaleReaderRangeIntact` | violation `StaleReaderRangeIntact` | 1,460 | 1.1 |
| `nc-lastcopy-publish-before-flush` | `HistoryAbsorb` / `nc_lastcopy_publish_before_flush` | negative-control | violation `LastRecoverableCopy` | violation `LastRecoverableCopy` | 13,636 | 1.9 |
| `nc-lastcopy-trim-past-absorbed` | `HistoryAbsorb` / `nc_lastcopy_trim_past_absorbed` | negative-control | violation `LastRecoverableCopy` | violation `LastRecoverableCopy` | 9 | 0.9 |
| `nc-retire-chunk-bytes` | `HistoryAbsorb` / `nc_retire_chunk_bytes` | negative-control | violation `LedgerExact` | violation `LedgerExact` | 13,991 | 1.8 |
| `nc-retire-chunk-bytes-overlap` | `HistoryAbsorb` / `nc_retire_chunk_bytes_overlap` | negative-control | violation `LedgerExact` | violation `LedgerExact` | 5,924 | 1.5 |
| `nc-retire-chunk-bytes-liveness` | `HistoryAbsorb` / `nc_retire_chunk_bytes_liveness` | negative-control | violation `AbsorptionCompletes` | violation (temporal) | 46,405 | 10.6 |
| `nc-warm-install-from-plan` | `HistoryAbsorb` / `nc_warm_install_from_plan` | negative-control | violation `CacheNeverProvesFalseAbsence` | violation `CacheNeverProvesFalseAbsence` | 53,912 | 2.8 |
| `nc-ungated-rollback` | `HistoryAbsorb` / `nc_ungated_rollback` | negative-control | violation `DetachedOnlyAfterFault` | violation `DetachedOnlyAfterFault` | 4,035 | 1.3 |
| `nc-settle-before-publish` | `HistoryAbsorb` / `nc_settle_before_publish` | negative-control | violation `NoRegatherUnderInFlight` | violation `NoRegatherUnderInFlight` | 667 | 1.1 |
| `nc-no-plan-rollback` | `HistoryAbsorb` / `nc_no_plan_rollback` | negative-control | violation `AbsorptionCompletes` | violation (temporal) | 20,012 | 4.5 |
| `probe-scan-per-row` | `HistoryAbsorb` / `probe_scan_per_row` | negative-control | violation `PagesAdmit` | violation `PagesAdmit` | 23,559 | 2.4 |
| `witness-TrimDurable` | `HistoryAbsorb` / `w_TrimDurable` | witness | violation `Witness_TrimDurable` | violation `Witness_TrimDurable` | 22,098 | 2.0 |
| `witness-FullyAbsorbedDurable` | `HistoryAbsorb` / `w_FullyAbsorbedDurable` | witness | violation `Witness_FullyAbsorbedDurable` | violation `Witness_FullyAbsorbedDurable` | 41,145 | 2.6 |
| `witness-LostPublicationRecovered` | `HistoryAbsorb` / `w_LostPublicationRecovered` | witness | violation `Witness_LostPublicationRecovered` | violation `Witness_LostPublicationRecovered` | 172,665 | 6.3 |
| `witness-FlushFailRecovered` | `HistoryAbsorb` / `w_FlushFailRecovered` | witness | violation `Witness_FlushFailRecovered` | violation `Witness_FlushFailRecovered` | 120,171 | 4.3 |
| `witness-StaleDuplicateIgnored` | `HistoryAbsorb` / `w_StaleDuplicateIgnored` | witness | violation `Witness_StaleDuplicateIgnored` | violation `Witness_StaleDuplicateIgnored` | 306,211 | 7.6 |
| `witness-CrashRecovered` | `HistoryAbsorb` / `w_CrashRecovered` | witness | violation `Witness_CrashRecovered` | violation `Witness_CrashRecovered` | 64,162 | 3.0 |
| `witness-GatherOverTrimmedPrefix` | `HistoryAbsorb` / `w_GatherOverTrimmedPrefix` | witness | violation `Witness_GatherOverTrimmedPrefix` | violation `Witness_GatherOverTrimmedPrefix` | 93,738 | 3.8 |
| `witness-StaleSnapshotLosesTail` | `HistoryAbsorb` / `w_StaleSnapshotLosesTail` | witness | violation `Witness_StaleSnapshotLosesTail` | violation `Witness_StaleSnapshotLosesTail` | 28,471 | 2.3 |
| `witness-TwoAdvancesNotDurable` | `HistoryAbsorb` / `w_TwoAdvancesNotDurable` | witness | violation `Witness_TwoAdvancesNotDurable` | violation `Witness_TwoAdvancesNotDurable` | 12,929 | 1.8 |
| `witness-EvictedMarkPrunedInFlight` | `HistoryAbsorb` / `w_EvictedMarkPrunedInFlight` | witness | violation `Witness_EvictedMarkPrunedInFlight` | violation `Witness_EvictedMarkPrunedInFlight` | 598 | 1.1 |
| `witness-RingGatherBelowTrim` | `HistoryAbsorb` / `w_RingGatherBelowTrim` | witness | violation `Witness_RingGatherBelowTrim` | violation `Witness_RingGatherBelowTrim` | 136,678 | 4.8 |
| `witness-WarmBridgeCovers` | `HistoryAbsorb` / `w_WarmBridgeCovers` | witness | violation `Witness_WarmBridgeCovers` | violation `Witness_WarmBridgeCovers` | 6,366 | 1.7 |
| `witness-InstallStartsAbovePlan` | `HistoryAbsorb` / `w_InstallStartsAbovePlan` | witness | violation `Witness_InstallStartsAbovePlan` | violation `Witness_InstallStartsAbovePlan` | 43,107 | 3.0 |
| `witness-RefusalHealed` | `HistoryAbsorb` / `w_RefusalHealed` | witness | violation `Witness_RefusalHealed` | violation `Witness_RefusalHealed` | 174,224 | 5.9 |
| `witness-MateDelaysRollback` | `HistoryAbsorb` / `w_MateDelaysRollback` | witness | violation `Witness_MateDelaysRollback` | violation `Witness_MateDelaysRollback` | 821,067 | 18.7 |
| `witness-OverlapAdmittedRefusedChain` | `HistoryAbsorb` / `w_OverlapAdmittedRefusedChain` | witness | violation `Witness_OverlapAdmitted` | violation `Witness_OverlapAdmitted` | 2,571 | 1.5 |
| `witness-OverlapAdmittedPrune` | `HistoryAbsorb` / `w_OverlapAdmittedPrune` | witness | violation `Witness_OverlapAdmitted` | violation `Witness_OverlapAdmitted` | 6,740 | 1.7 |
| `witness-OverlapAdmittedNewOwner` | `HistoryAbsorb` / `w_OverlapAdmittedNewOwner` | witness | violation `Witness_OverlapAdmitted` | violation `Witness_OverlapAdmitted` | 3,991 | 1.8 |

### TLA-018 (`pass-with-recorded-scope`)

Receipt `verification/receipts/TLA-018.json`, recorded on `cb4c6b47`: 29 checks, every verdict as expected; 70 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-durable-keyed-small` | `ReadCompose` / `durable_keyed_small` | baseline | pass | pass | 12,482,967 | 177.3 |
| `baseline-durable-unfiltered-small` | `ReadCompose` / `durable_unfiltered_small` | baseline | pass | pass | 3,777,005 | 57.5 |
| `baseline-applied-keyed-small` | `ReadCompose` / `applied_keyed_small` | baseline | pass | pass | 11,351,025 | 157.8 |
| `baseline-applied-unfiltered-small` | `ReadCompose` / `applied_unfiltered_small` | baseline | pass | pass | 3,045,185 | 41.2 |
| `baseline-durable-keyed-expanded` | `ReadCompose` / `durable_keyed_expanded` | baseline | pass | pass | 19,356,657 | 251.9 |
| `baseline-durable-unfiltered-expanded` | `ReadCompose` / `durable_unfiltered_expanded` | baseline | pass | pass | 6,582,468 | 91.0 |
| `baseline-applied-unfiltered-expanded` | `ReadCompose` / `applied_unfiltered_expanded` | baseline | pass | pass | 19,384,259 | 243.5 |
| `baseline-applied-keyed-expanded` | `ReadCompose` / `applied_keyed_expanded` | baseline | pass | pass | 53,598,423 | 720.5 |
| `nc-old-history-view` | `ReadCompose` / `nc_old_history_view` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 567,582 | 7.1 |
| `nc-filtered-race-never` | `ReadCompose` / `nc_filtered_race_never` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 209,827 | 3.2 |
| `nc-short-index-accepted` | `ReadCompose` / `nc_short_index_accepted` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 6,586 | 1.1 |
| `nc-applied-race-remote` | `ReadCompose` / `nc_applied_race_remote` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 48,203 | 1.5 |
| `nc-applied-race-remote-unfiltered` | `ReadCompose` / `nc_applied_race_remote_unfiltered` | negative-control | violation `TailGapExplained` | violation `TailGapExplained` | 42,301 | 1.4 |
| `nc-no-continuation-check` | `ReadCompose` / `nc_no_continuation_check` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 2,566,966 | 25.3 |
| `probe-lost-durable-postings` | `ReadCompose` / `probe_lost_postings` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 7,254 | 1.1 |
| `probe-lost-durable-canonical` | `ReadCompose` / `probe_lost_canonical` | negative-control | violation `ExactDurablePrefix` | violation `ExactDurablePrefix` | 8,401 | 1.1 |
| `witness-BoundaryRaceAdopted` | `ReadCompose` / `w_BoundaryRaceAdopted` | witness | violation `Witness_BoundaryRaceAdopted` | violation `Witness_BoundaryRaceAdopted` | 9,545 | 1.1 |
| `witness-UnfilteredRaceAdopted` | `ReadCompose` / `w_UnfilteredRaceAdopted` | witness | violation `Witness_BoundaryRaceAdopted` | violation `Witness_BoundaryRaceAdopted` | 153,749 | 2.6 |
| `witness-AppliedRaceAdopted` | `ReadCompose` / `w_AppliedRaceAdopted` | witness | violation `Witness_AppliedRaceAdopted` | violation `Witness_AppliedRaceAdopted` | 3,262 | 0.9 |
| `witness-LargeFirstRecordDelivered` | `ReadCompose` / `w_LargeFirstRecordDelivered` | witness | violation `Witness_LargeFirstRecordDelivered` | violation `Witness_LargeFirstRecordDelivered` | 149 | 0.8 |
| `witness-EnvelopeServed` | `ReadCompose` / `w_EnvelopeServed` | witness | violation `Witness_EnvelopeServed` | violation `Witness_EnvelopeServed` | 1,233 | 0.9 |
| `witness-ShortIndexPartial` | `ReadCompose` / `w_ShortIndexPartial` | witness | violation `Witness_ShortIndexPartial` | violation `Witness_ShortIndexPartial` | 2,082 | 0.9 |
| `witness-ReadFromFencedEngine` | `ReadCompose` / `w_ReadFromFencedEngine` | witness | violation `Witness_ReadFromFencedEngine` | violation `Witness_ReadFromFencedEngine` | 469 | 0.8 |
| `witness-RingServed` | `ReadCompose` / `w_RingServed` | witness | violation `Witness_RingServed` | violation `Witness_RingServed` | 156 | 0.8 |
| `witness-ReaderCompletes` | `ReadCompose` / `w_ReaderCompletes` | witness | violation `Witness_ReaderCompletes` | violation `Witness_ReaderCompletes` | 3,676 | 1.0 |
| `witness-TrimBelowReaderCursor` | `ReadCompose` / `w_TrimBelowReaderCursor` | witness | violation `Witness_TrimBelowReaderCursor` | violation `Witness_TrimBelowReaderCursor` | 9,834 | 1.0 |
| `witness-ReadErrorCurrentEngine` | `ReadCompose` / `w_ReadErrorCurrentEngine` | witness | violation `Witness_ReadErrorCurrentEngine` | violation `Witness_ReadErrorCurrentEngine` | 46 | 0.8 |
| `witness-ContinuedAcrossMove` | `ReadCompose` / `w_ContinuedAcrossMove` | witness | violation `Witness_ContinuedAcrossMove` | violation `Witness_ContinuedAcrossMove` | 361,764 | 3.7 |
| `witness-StaleContinuationResynced` | `ReadCompose` / `w_StaleContinuationResynced` | witness | violation `Witness_StaleContinuationResynced` | violation `Witness_StaleContinuationResynced` | 426,808 | 4.4 |

### TLA-019 (`pass-with-recorded-scope`)

Receipt `verification/receipts/TLA-019.json`, recorded on `1d20f76f`: 50 checks, every verdict as expected; 79 min of TLC wall time in total.

| Check | Module / config | Role | Expected | Verdict | Distinct states | Seconds |
|---|---|---|---|---|---|---|
| `baseline-small` | `ReachGC` / `small` | baseline | pass | pass | 3,938,288 | 70.1 |
| `baseline-expanded` | `ReachGC` / `expanded` | baseline | pass | pass | 27,990,301 | 520.6 |
| `baseline-timing-lapse` | `ReachGC` / `lapse` | baseline | pass | pass | 3,594,585 | 65.1 |
| `liveness-small` | `ReachGC` / `liveness` | baseline | pass | pass | 637,394 | 135.3 |
| `liveness-expanded` | `ReachGC` / `liveness_expanded` | baseline | pass | pass | 4,826,314 | 1446.9 |
| `nc-no-compaction-checkpoint` | `ReachGC` / `nc_no_compaction_checkpoint` | negative-control | violation `LiveReadViewProtected` | violation `LiveReadViewProtected` | 234,329 | 4.3 |
| `nc-advance-on-upload` | `ReachGC` / `nc_advance_on_upload` | negative-control | violation `HistoryBacked` | violation `HistoryBacked` | 162 | 1.2 |
| `nc-swallow-read-error` | `ReachGC` / `nc_swallow_read_error` | negative-control | violation `NoFalseCompleteRead` | violation `NoFalseCompleteRead` | 559,136 | 8.1 |
| `probe-upstream-short-read` | `ReachGC` / `probe_upstream_short_read` | negative-control | violation `NoFalseCompleteRead` | violation `NoFalseCompleteRead` | 557,407 | 8.2 |
| `nc-no-generation-condition` | `ReachGC` / `nc_no_generation` | negative-control | violation `ManifestRefsPresent` | violation `ManifestRefsPresent` | 25,510 | 2.3 |
| `nc-ignore-checkpoint-pin` | `ReachGC` / `nc_ignore_checkpoint_pin` | negative-control | violation `CheckpointPinned` | violation `CheckpointPinned` | 1,120,994 | 13.6 |
| `nc-stale-inventory` | `ReachGC` / `nc_stale_inventory` | negative-control | violation `EligibleEventuallyReclaimed` | violation (temporal) | 528,296 | 81.7 |
| `witness-CompactedInputReclaimed` | `ReachGC` / `w_CompactedInputReclaimed` | witness | violation `Witness_CompactedInputReclaimed` | violation `Witness_CompactedInputReclaimed` | 573,183 | 8.0 |
| `witness-OrphanReclaimed` | `ReachGC` / `w_OrphanReclaimed` | witness | violation `Witness_OrphanReclaimed` | violation `Witness_OrphanReclaimed` | 207,691 | 3.9 |
| `witness-HistoryServedAfterReclaim` | `ReachGC` / `w_HistoryServedAfterReclaim` | witness | violation `Witness_HistoryServedAfterReclaim` | violation `Witness_HistoryServedAfterReclaim` | 1,145,809 | 15.7 |
| `witness-ReaderViewErrors` | `ReachGC` / `w_ReaderViewErrors` | witness | violation `Witness_ReaderViewErrors` | violation `Witness_ReaderViewErrors` | 445,802 | 6.9 |
| `witness-StaleWriterViewRead` | `ReachGC` / `w_StaleWriterViewRead` | witness | violation `Witness_StaleWriterViewRead` | violation `Witness_StaleWriterViewRead` | 716 | 1.2 |
| `witness-QuietDeadZoneRetains` | `ReachGC` / `w_QuietDeadZoneRetains` | witness | violation `Witness_QuietDeadZoneRetains` | violation `Witness_QuietDeadZoneRetains` | 22,590 | 1.8 |
| `witness-LateCommitAfterGcView` | `ReachGC` / `w_LateCommitAfterGcView` | witness | violation `Witness_LateCommitAfterGcView` | violation `Witness_LateCommitAfterGcView` | 1,680 | 1.3 |
| `witness-CheckpointProtectsReadView` | `ReachGC` / `w_CheckpointProtectsReadView` | witness | violation `Witness_CheckpointProtectsReadView` | violation `Witness_CheckpointProtectsReadView` | 127,118 | 3.0 |
| `fork-baseline` | `ForkPin` / `baseline` | baseline | pass | pass | 3,374,329 | 117.0 |
| `fork-baseline-legacy` | `ForkPin` / `baseline_legacy` | baseline | pass | pass | 41,723,978 | 1697.2 |
| `fork-liveness-client-retries` | `ForkPin` / `liveness_client_retries` | baseline | pass | pass | 72,328 | 16.1 |
| `fork-liveness-reconciler` | `ForkPin` / `liveness_reconciler` | baseline | pass | pass | 72,328 | 20.4 |
| `fork-liveness-reconciler-recreated` | `ForkPin` / `liveness_reconciler_recreated` | baseline | pass | pass | 49,238 | 14.7 |
| `fork-liveness-backfill` | `ForkPin` / `liveness_backfill` | baseline | pass | pass | 564,266 | 219.1 |
| `fork-liveness-backfill-recreated` | `ForkPin` / `liveness_backfill_recreated` | baseline | pass | pass | 367,371 | 133.4 |
| `nc-ignore-fork-pin` | `ForkPin` / `nc_ignore_fork_pin` | negative-control | violation `ForkPinRespected` | violation `ForkPinRespected` | 53 | 1.1 |
| `nc-install-ignores-incarnation` | `ForkPin` / `nc_install_ignores_incarnation` | negative-control | violation `ForkPinRespected` | violation `ForkPinRespected` | 283 | 1.3 |
| `nc-no-reconciler` | `ForkPin` / `nc_no_reconciler` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 33,861 | 4.3 |
| `nc-settle-inconclusive` | `ForkPin` / `nc_settle_inconclusive` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 25,263 | 4.2 |
| `nc-no-backfill` | `ForkPin` / `nc_no_backfill` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 26,427 | 4.2 |
| `nc-recreate-without-index` | `ForkPin` / `nc_recreate_without_index` | negative-control | violation `OwedRefIndexed` | violation `OwedRefIndexed` | 256 | 1.1 |
| `nc-recreate-without-index-liveness` | `ForkPin` / `nc_recreate_without_index_liveness` | negative-control | violation `RefEventuallyReleased` | violation (temporal) | 296,005 | 70.2 |
| `nc-reconcile-live-child` | `ForkPin` / `nc_reconcile_live_child` | negative-control | violation `ReadyHoldsRef` | violation `ReadyHoldsRef` | 124 | 1.1 |
| `nc-release-current-name-id` | `ForkPin` / `nc_release_current_name_id` | negative-control | violation `ReadyHoldsRef` | violation `ReadyHoldsRef` | 1,765 | 1.6 |
| `nc-no-write-ahead-marker` | `ForkPin` / `nc_no_write_ahead_marker` | negative-control | violation `OwedRefIndexed` | violation `OwedRefIndexed` | 47 | 1.0 |
| `witness-fork-SoftDeleteRetainedForFork` | `ForkPin` / `w_SoftDeleteRetainedForFork` | witness | violation `Witness_SoftDeleteRetainedForFork` | violation `Witness_SoftDeleteRetainedForFork` | 90 | 1.1 |
| `witness-fork-TwoChildrenPinSource` | `ForkPin` / `w_TwoChildrenPinSource` | witness | violation `Witness_TwoChildrenPinSource` | violation `Witness_TwoChildrenPinSource` | 3,491 | 1.4 |
| `witness-fork-ForkCascadeTombstone` | `ForkPin` / `w_ForkCascadeTombstone` | witness | violation `Witness_ForkCascadeTombstone` | violation `Witness_ForkCascadeTombstone` | 421 | 1.3 |
| `witness-fork-InstallAfterChildDeleted` | `ForkPin` / `w_InstallAfterChildDeleted` | witness | violation `Witness_InstallAfterChildDeleted` | violation `Witness_InstallAfterChildDeleted` | 75 | 1.0 |
| `witness-fork-InstallDeclinedOnRecreatedSource` | `ForkPin` / `w_InstallDeclinedOnRecreatedSource` | witness | violation `Witness_InstallDeclinedOnRecreatedSource` | violation `Witness_InstallDeclinedOnRecreatedSource` | 110 | 1.3 |
| `witness-fork-DebtClearedOnRecreatedSource` | `ForkPin` / `w_DebtClearedOnRecreatedSource` | witness | violation `Witness_DebtClearedOnRecreatedSource` | violation `Witness_DebtClearedOnRecreatedSource` | 666 | 1.1 |
| `witness-fork-PermanentPinWithoutRetry` | `ForkPin` / `w_PermanentPinWithoutRetry` | witness | violation `Witness_PermanentPinWithoutRetry` | violation `Witness_PermanentPinWithoutRetry` | 924 | 1.2 |
| `witness-fork-PinAfterSuccessfulDelete` | `ForkPin` / `w_PinAfterSuccessfulDelete` | witness | violation `Witness_PinAfterSuccessfulDelete` | violation `Witness_PinAfterSuccessfulDelete` | 537 | 1.2 |
| `witness-fork-ReconcilerReleasesLatePin` | `ForkPin` / `w_ReconcilerReleasesLatePin` | witness | violation `Witness_ReconcilerReleasesLatePin` | violation `Witness_ReconcilerReleasesLatePin` | 448 | 1.3 |
| `witness-fork-ReconcilerReleasesReplacedName` | `ForkPin` / `w_ReconcilerReleasesReplacedName` | witness | violation `Witness_ReconcilerReleasesReplacedName` | violation `Witness_ReconcilerReleasesReplacedName` | 680 | 1.1 |
| `witness-fork-BackfillReleased` | `ForkPin` / `w_BackfillReleased` | witness | violation `Witness_BackfillReleased` | violation `Witness_BackfillReleased` | 792 | 1.1 |
| `witness-fork-RecreationIndexesDebt` | `ForkPin` / `w_RecreationIndexesDebt` | witness | violation `Witness_RecreationIndexesDebt` | violation `Witness_RecreationIndexesDebt` | 125 | 1.0 |
| `witness-fork-OldBinaryOverwroteDebt` | `ForkPin` / `w_OldBinaryOverwroteDebt` | witness | violation `Witness_OldBinaryOverwroteDebt` | violation `Witness_OldBinaryOverwroteDebt` | 41 | 1.0 |
<!-- /RESULTS -->

## Findings

| ID | Classification | Disposition |
|---|---|---|
| TLA-016-F1 | production defect | **Fixed** by slate's "The committer retires an absorbed advance only from its own boundary" (`9c6675d7`) and "A lane mark is rolled back only when no submitted advance of its stream can still land" (`b5751e75`), adopted by the merge `3a24eace`; our `6371da0a` (the stored-row recount) is superseded and was dropped. Baselines `ledger-small`, `ledger-overlap`, `liveness-small` pass; controls `nc-retire-chunk-bytes*` (slate's planted "retire ignoring from") reproduce the pre-fix behaviour. |
| TLA-016-F3 | production defect (latent) | **Fixed** by "A re-gather warms the postings cache only over the rows it staged" (`b47c2d2a`). Slate's code still installed from `plan.from`; the merge re-applied the rule (`note_frames` returns the staged range, `src/history/gather.rs`). `baseline-cache` passes; `nc-warm-install-from-plan` (slate's install) reproduces the defect, now through the ungated prune. |
| TLA-016-F2 | documentation defect | **Fixed in comments** by `63202168` ("The invariant docs state what the models showed and what stays an owner decision"): the `trim_safe_to` comments now say the one-advance lag protects a snapshot at most one advance stale, and that `absorption_race` carries the guarantee for staler readers. |
| TLA-016-F4 | documentation defect, found while modelling the receipt fix `d9aeaef` | **Fixed** in comments, and **moot** since the merge: the recount it bounded is gone, and a mis-started advance is dropped whole. Its witness and properties were removed with the mechanism. |
| (overlapping pages) | production defect, recorded with the TLA-016-F1 fix | **Fixed** by "Overlapping postings pages from a re-gather admit when they agree" (`d16559b3`, kept by the merge). Overlaps still arise from a refused chain (slate's recorded residual, `882004d9`), the ungated sweep prune and a new owner; `keep_past` admits them. `baseline-pages` and `baseline-pages-regather` check `PagesAdmit`; `witness-OverlapAdmitted{RefusedChain,Prune,NewOwner}` show overlaps that admit; `probe-scan-per-row` shows the admission rests on dense chunks. |
| (cache bridge) | production defect, found in passing during the TLA-016-F3 fix | **Fixed** by "A postings-cache bridge never crosses a chunk whose runs no slice recorded" (kept by the merge). Outside what the model can express (admission line, capped and merging loads); covered by real-code regressions. |
| (heal latency, bucket sharing) | follow-ups recorded by the merge, not defects | **Open follow-ups.** A refused quiet stream heals only at the next dirty-index rescan (`liveness-refusal` needs the rescan's fairness); a shared settlement bucket delays a rollback (`witness-MateDelaysRollback`, `ledger-small`). See [Follow-ups recorded by the merge](#follow-ups-recorded-by-the-merge-not-defects). |
| TLA-018-F1 | production defect | **Fixed** by "An applied read revalidates its tail scan at the level it scanned, so it never skips a durable record" (`9cea1b6`). `baseline-applied-keyed-small` passes; controls `nc-applied-race-remote*` reproduce the pre-fix behaviour. |
| TLA-018-F3 | production defect | **Fixed** by "A provisional read cursor proves the history it continues, or answers an explicit resync" (`55881d7`). `baseline-applied-unfiltered-expanded` (the former known-defect shape) and `baseline-applied-keyed-expanded` check `ExactDurablePrefix` and pass; `nc-no-continuation-check` reproduces the pre-fix acceptance. Regressions in `dst::dst_tests::reads_applied_history`; `verification/regressions/TLA-018-F3/README.md`. |
| TLA-018-F2 | open obligation (owner decision) | **Open; needs an owner decision.** H11 is adopted as written and only partly enforced: the reader does not detect a page or canonical row lost after durability, which holds only through durability and the cache contract. The owner either keeps H11 and adds a coverage mechanism (option A) or deliberately revises the contract under roadmap §2.10 (option B); `docs/dst/DST-EXPANSION-SPEC.md` §9.12.2. Until then no report counts H11 as met. |
| TLA-019-F1 | abstraction mismatch (not reproduced) | **Withdrawn as a defect, conditionally.** The model lacked the compactor's checkpoint. With it, `LiveReadViewProtected` passes under ASM-SLATEDB-COMPACTION-CHECKPOINT, a pinned dependency contract (an upstream 900 s constant the code calls interim, a refresh within 300 s, a read within the remaining 600 s), not permanent reader pinning; `baseline-timing-lapse` shows a read beyond it fails rather than completing short. `nc-no-compaction-checkpoint` reproduces the earlier counterexample. |
| TLA-019-F4 | unjustified assumption, then production work | **Fixed** by "A background reconciler releases fork references that deleted children still owe" (`0d40dc2`) and "Fork-reference debt from before the index is backfilled, and stale debt raises an alert" (`8a03e0d`). `fork-liveness-reconciler` (the former `probe-no-client-retry` shape, with no client retry), `fork-liveness-reconciler-recreated` and `fork-liveness-backfill` pass; `nc-no-reconciler`, `nc-settle-inconclusive` and `nc-no-backfill` violate `RefEventuallyReleased`. A recreation over an expired fork child now indexes its release too (real-code regression only; expiry is not modelled); expiry alone still never releases a fork's reference (open, owner decision). The residual found by the model (pre-index debt overwritten by a recreation of the child's name before the backfill indexes it) is **fixed** too: the recreate CAS indexes the debt it overwrites; `fork-baseline-legacy` and `fork-liveness-backfill-recreated` pass and `nc-recreate-without-index*` reproduce the pre-fix loss. |
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

**Fix in the code (slate, adopted by the merge).** `9c6675d7` ("The
committer retires an absorbed advance only from its own boundary, so no
regather can retire a byte twice") and `b5751e75` ("A lane mark is rolled
back only when no submitted advance of its stream can still land, so a
regather never starts under an advance in flight"). Each advance carries
`CopiedBytes { from, len }`, and `retire_absorbed`
(`src/shard/commit_plan.rs:304`) moves the boundary only when `from` is the
stream's absorbed boundary (Exact); any other advance is dropped whole
(Detached), and one claiming more than the ledger refuses its group
(Diverged). `b5751e75` removes the causal path of the over-retirement: the
rescan no longer rolls marks back, and a plan rolls a stranded mark back
only while the stream's settlement bucket shows no advance in flight. Our
fix `6371da0a` (a recount of the stored rows by `stored_frame_bytes` for a
mis-started advance, with the receipt and replay machinery of `d9aeaeff`)
is **superseded**: the merge dropped it.

**Model.** The mutation point is `Retires` (baseline `m.f = prev`).
`LedgerExact` holds in every tail view in every baseline, with refusals,
dropped ops, a crash, flush failures, the ungated prune and ring gathers
that return rows below the trim point. `DetachedOnlyAfterFault` says the
drop happens only after a lost advance or the prune.

**Checks.** `baseline-small`, `baseline-expanded`, `ledger-small` (with a
bucket mate), `ledger-overlap` (no lost publication, the prune only) and
`liveness-small` pass. `MutRetireIgnoringFrom` is slate's own planted
control (`if copied.from != tail.absorbed && false`):
`nc-retire-chunk-bytes` (under-retirement after a refused group) and
`nc-retire-chunk-bytes-overlap` (over-retirement after the prune) violate
`LedgerExact`, and `nc-retire-chunk-bytes-liveness` violates
`AbsorptionCompletes` (every later advance is Diverged).

**Regressions.**
`dst::dst_tests::billing_maintenance::refused_absorbed_chunk_leaves_no_phantom_backlog`
(its doc now describes the Detached drop and the plan-time heal),
`shard::commit_plan::tests::retire_absorbed_retires_only_a_copy_that_starts_at_the_boundary`,
`shard::maintenance_tests::an_advance_that_overlaps_the_boundary_retires_nothing_and_fails_no_append`,
`shard::maintenance_tests::an_advance_that_skips_offsets_is_dropped_whole`,
`history::bounded_discovery_tests::a_regather_from_the_submitted_mark_retires_each_byte_once`,
`…::a_rescan_during_an_inflight_advance_never_fails_an_append`,
`…::a_rescan_during_an_inflight_advance_regathers_from_the_submitted_mark`,
`…::a_refused_advance_is_regathered_from_the_durable_boundary`,
`…::a_refused_advance_heals_at_its_next_gather_under_a_busy_absorber`, and
the Loom models in `src/shard/commit_plan/loom_tests.rs`. Our
`rolled_back_mark_regather_keeps_the_ledger_exact` and
`misaligned_absorbed_chunk_retires_stored_bytes_or_refuses_the_group` were
dropped with the recount.

**Pre-fix evidence.** `evidence/TLA-016_ledger_under_retire.trace.txt`,
`evidence/TLA-016_ledger_over_retire.trace.txt` and
`evidence/TLA-016_liveness_stall.trace.txt`, recorded on the pre-fix model
(before `6371da0a`; they show the defect, which slate's fix also removes).

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

**Status after the merge.** The defect **still stands in slate's code**:
slate's `stage_chunk` installs from `plan.from`. The merge re-applied our
rule: `note_frames` returns the staged rows `first..last + 1`
(`src/history/gather.rs:156-175`), and `stage_chunk` names the install by
that range (`gather.rs:612-614`); the advance still carries
`CopiedBytes::new(plan.from, …)` (`:616-618`). Under slate's gate a
settled rollback never re-gathers while an advance can land, so the stale
plan that exposes the defect now needs the ungated sweep prune of a
non-resident handle (D6); a new owner plans from a boundary nothing can
move under it. The staged rows are dense: a ring hit proves its window
dense, and a Remote scan reads one snapshot (ASM-SLATEDB-DURABLE (j)). In
the model the install start is the operator `WarmInstallFrom`.

**Checks.** `baseline-cache` (one-record gathers) and `baseline-cache-cap2`
(two-record gathers) check `CacheNeverProvesFalseAbsence` with every other
safety property and pass. `MutWarmInstallFromPlan` is slate's install:
`nc-warm-install-from-plan` violates `CacheNeverProvesFalseAbsence` (the
prune removes the mark of two queued one-record chunks, the stale re-gather
plans from 0, both advances land and trim row 0, the slice is evicted, and
the install over `[0, 2)` holds only row 1).
`witness-InstallStartsAbovePlan` shows the fixed install starting above
`plan.from`. `evidence/TLA-016_cache_false_absence.trace.txt` was recorded
on the pre-fix, pre-merge model.

**Regressions.**
`history::bounded_discovery_tests::stale_regather_never_warms_a_trimmed_head_as_absent`
drives the real absorber: two queued one-record gathers, the mark removed
directly (standing for the ungated prune of a non-resident handle), both
advances durable with row 0 trimmed, an idle sweep that evicts the slice,
and a stale re-gather that reads only row 1. With the install from
`plan.from` (the pre-fix code, which slate's branch still had) a durable
keyed read from 0 returned `[1]` as a complete page with cursor 2.
`postings_cache::tests::a_regather_install_after_an_eviction_proves_nothing_below_its_rows`
pins the cache contract.

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
recorded" (kept by the merge): `admitted_all` is gone, and any install that
drops a key's runs raises the window's `from` past its chunk, so the
unchanged bridge conditions refuse a bridge that starts below it.
Regressions:
`postings_cache::tests::a_demand_bridge_never_crosses_an_unadmitted_install`,
`postings_cache::tests::an_install_bridge_never_crosses_an_unadmitted_install`,
`postings_cache::tests::a_bridge_never_crosses_a_chunk_that_found_its_key_short`
and
`history::bounded_discovery_tests::a_warm_bridge_never_crosses_an_unadmitted_install`.
The model could not find this: it has no admission line (every fresh install
is admitted), no capped loads and no loads that merge into a resident slice.
`InstallChunk` raises `from` when it drops a key's runs, as the code does,
but no configuration here distinguishes the two (a scratch run of the
pre-merge `cache` and `cache_cap2` without the raise also passed), so there
is no negative control for this fix.

### TLA-016-F2 — the `trim_safe_to` comments only held for one-advance-stale readers (documentation defect, fixed in comments)

`src/shard.rs:64-67` and the `TailFields::trim_safe_to` comment
(`src/shard.rs:601-607`) (lines at `d9aeaef`) said the one-advance lag
means "in-flight readers holding a stale absorbed snapshot never lose their
range". `StaleReaderRangeIntact` (a snapshot at most one advance stale)
passes, but `witness-StaleSnapshotLosesTail` shows that a reader whose
snapshot is two or more advances stale finds part of its tail range
trimmed. Reads stay correct because every tail page is revalidated against
the absorbed boundary at the scan's own visibility (TLA-018; the applied
path gained that check with the TLA-018-F1 fix). `63202168` corrected both
comments: the lag keeps the range of a snapshot at most one advance stale,
and a staler reader relies on `absorption_race`, which revalidates each tail
page against the absorbed boundary at its scan's visibility. No code
regression is needed. Unchanged by the merge.

### TLA-016-F4 — the receipt fix bounded a recount per refusal, not across consecutive late refusals (moot since the merge)

`d9aeaef` ("A refused absorption group rolls its lane marks back, so a
recount covers only chunks in flight") claimed that no `stored_frame_bytes`
recount spans more than the chunks in flight at a refusal. The pre-merge
model showed that consecutive refusals, each settled after the stream's
next chunk had raised the mark, leave consecutive holes, so a recount grew
by one chunk per late refusal (`witness-RecountBeyondInFlight`), and the
comments were corrected. The merge dropped the recount, the answered
receipts and `settle_submissions`: an advance that does not start at the
boundary is now dropped whole and never reads stored rows. The finding, its
witness and the pre-merge `SettledMarkAtBoundary` and
`RecountWithinInFlight` properties are gone with the mechanism; the
driver trace that `f1de3dcb` recorded as
`evidence/TLA-016_recount_beyond_in_flight.trace.txt` is removed from the
tree and remains in the repository history at `3a24eace`.

### Overlapping postings pages after a re-gather (fixed by `keep_past`; still arise)

**Defect.** A page is keyed by its first offset, so two gathers that cut
the same rows into different chunks leave pages under different keys over
the same offsets. The cold index load (`append_page_runs`) refused any page
starting below the accumulated end as corrupt, so that key's reads fell
back to the envelope scan for good.

**Fix.** "Overlapping postings pages from a re-gather admit when they agree"
(`d16559b3`, kept by the merge). Each page is complete over its own span.
`keep_past` (`src/postings.rs:295`) admits an overlapping page only when its
offsets over the common span equal the offsets already admitted, and keeps
only its part past them. A disagreeing overlap is still corruption.

**They still arise after the merge.** Slate's gate stops the rescan's
re-gather under an advance in flight, but three sources remain, and slate
records the first as a residual (`882004d9`): no rollback deletes the pages
a dropped or lost advance already flushed.

- *A refused chain* (`witness-OverlapAdmittedRefusedChain`, the schedule of
  `a_regather_across_a_detached_chunk_keeps_the_index_readable`): chunk
  `[0,1)` is flushed and submitted (mark 1), and `[1,2)` is planned from
  the mark, flushed and submitted (mark 2) while the first is still queued.
  The committer refuses the first group, and the chained advance, which no
  longer starts at the boundary, is Detached. Both receipts are settled, so
  the next plan rolls the mark back and re-gathers `[0,2)`: its K1 page
  `{0, 1}` overlaps the chained chunk's first-1 page `{1}`, the two agree,
  and absorption completes.
- *The ungated prune* (`witness-OverlapAdmittedPrune`): `[0,1)` and `[1,3)`
  are queued, the stream has left the roster and its handle is evictable,
  so the sweep prunes the mark (D6); the rescan re-pends the stream and the
  re-gather from 0 stages `{0, 1}` over the first-1 page.
- *A new owner* (`witness-OverlapAdmittedNewOwner`, the schedule of
  `a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable`):
  the engine closes with `[0,1)` flushed and `[1,2)`'s page flushed by a
  memtable flush, neither advance durable; the new owner plans from 0 and
  stages `{0, 1}` over the inherited first-1 page.

A bucket-sharing stream adds a fourth path without a new mechanism: a
refused stream whose bucket is held by another stream's advance gathers
from its stranded mark, is Detached, and leaves pages under its eventual
heal (`882004d9`).

**Model.** With `Pages`, each staged chunk writes one page per routing key,
`<<key, first offset, offsets>>`, into a store keyed by (key, first offset)
(ASM-HISTORY-PAGES). `PagesAdmit` folds the reader's admission over each
key's pages in key order, at every state. The pre-merge property
`NoStraddledChunk` (no page starts inside another's span) held only in a
shape without crash, rescan or prune; with slate's refused chains it no
longer holds in any shape with a refusal, so it is now the witness
`Witness_OverlapAdmitted` (`Straddled /\ PagesAdmit /\ D.abs = N`).

**Checks.** `baseline-pages` (4 records, 2 refusals or drops, a failed or
ambiguous flush) and `baseline-pages-regather` (the prune, a crash and a
refusal) pass `PagesAdmit` with every ledger and history property; the
three overlap witnesses show the overlaps; `probe-scan-per-row` shows the
admission rests on dense chunks. The traces are
`evidence/TLA-016_overlap_admitted_refused_chain.trace.txt`,
`evidence/TLA-016_overlap_admitted_prune.trace.txt` and
`evidence/TLA-016_overlap_admitted_new_owner.trace.txt` (the driver's logs
on the current model).

### Follow-ups recorded by the merge (not defects)

- **Heal latency.** A refused or dropped advance of a quiet stream is no
  longer re-pended at once: the stream left the roster when its gather
  reached the end, no signal follows, and only the dirty-index rescan
  (every 120 ticks, about 10 minutes) re-pends it, after which the settled
  plan rolls the mark back. The model proves the heal happens
  (`liveness-refusal`, with the rescan fair; without that fairness the same
  shape violates `AbsorptionCompletes`) and cannot bound how soon. Until
  then the stream's records stay in the shard log and its ledger counts
  them; nothing is lost.
- **Bucket sharing.** 1,024 buckets serve all of an absorber's streams. A
  refused stream whose bucket also holds another stream's in-flight
  advance keeps its stranded mark (`witness-MateDelaysRollback`); a gather
  meanwhile is Detached and leaves overlapping pages. `ledger-small` checks
  the ledger, the gate and `DetachedOnlyAfterFault` with a bucket mate
  toggling freely; the liveness shapes have no mate, because a mate that
  never settles would hold the rollback back forever. Under a lagging
  committer the delay can repeat; it is safe with `keep_past`.

### TLA-018-F1 — `deliver=applied` keyed reads skipped durable records trimmed by a non-durable advance (fixed)

**Defect.** An applied tail scan runs at Memory level, so it sees the row
deletes of a trim that an applied, not yet durable, absorption advance staged.
`absorption_race` checked the scan only against the Remote-durable absorbed
boundary. With two one-chunk advances applied but not durable, an applied
keyed read from 0 delivered records 1 and 2 and moved both cursors past the
durable, acknowledged record 0. The unfiltered applied branch saw the hole
but answered an honest partial with no progress.

**Fix, as modelled.** `Deliver::durability()` is the one mapping used by the
scan and by `ShardEngine::visible_absorbed` (`src/shard/record.rs:232-237`,
`:253-276`), so the race check reads the absorbed boundary at the scan's own
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
  `src/shard.rs:1131`, `:1361`), the recovery offset (the durable resume
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
matches (`docs/ROUTING-V3.md`; `read_history2_keyed`, `src/history.rs:916`).
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
  (`Registry::backfill_fork_debt`, `fork_debt.rs:290-348`;
  `reconcile.rs:275-318`), which walks the catalog and indexes every
  debt-bearing tombstone, then records completion and never runs again. In
  the model, with `Legacy`, child deletes before `Rollout` write no marker,
  `Backfill(c)` indexes such a tombstone and `BackfillFinish` completes the
  walk (mutation point `BackfillOn`).
- A recreation of the child's name overwrites its tombstone and the debt on
  it. `Registry::recreate`, the one recreate CAS every create surface uses,
  now indexes a debt the stored tombstone still carries before it writes the
  replacement (`src/registry.rs:1018`; `Registry::index_overwritten_debt`,
  `src/registry/fork_debt.rs:193-201`); a failed marker write fails the
  recreation before anything changed. Since the merge of slate at
  `3a24eace`, the replaced incarnation's closure debt is recorded first
  (`record_replaced`, `src/registry.rs:1017`), and its failure also fails the
  recreation before the marker and the CAS. This closes the residual the model
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
`witness-fork-OldBinaryOverwroteDebt` shows it is reachable.

**Found in passing, fixed: an expired fork child's reference.** A fork child
that expires is never deleted: expiry writes nothing, and a `DELETE` of an
expired name answers gone without reaching `delete_lifecycle`
(`src/application/creation/deletion.rs:15-21`). Its reference on the source
is therefore never released and no marker names it. Recreating the name
(`recreatable` accepts an expired descriptor without children,
`src/application/creation.rs:160-162`) overwrote that incarnation, so the
reference pinned the source for good. Reproduced on the real code by
`dst::dst_tests::fork_debt::a_recreated_name_releases_the_reference_its_expired_fork_held`:
a fork with `Stream-TTL: 1` expires, its `DELETE` is refused, the source's
`DELETE` soft-deletes it, the name is recreated, and before the fix the
reconciler and the backfill ran for 10 s while the source stayed
soft-deleted with the expired child's reference. `index_overwritten_debt`
now also indexes the release of an overwritten incarnation that died without
a delete; the reconciler pays it from the marker as for any recreated name.
The model has no expiry (see the exclusions), so this fix has no model
check; the regression is its evidence.

**Open, found in passing: expiry alone never releases a fork's reference.**
Without a recreation of its name, an expired fork child keeps its reference
forever: nothing deletes an expired stream, a client `DELETE` is refused as
gone, and no marker exists for the reconciler. A source soft-deleted while
such a reference exists stays retained (its name cannot be recreated, F5,
and its data is kept) until someone recreates the child's name. This is not
fixed here; it needs an owner decision on whether expiry releases fork
references (for example, the reconciler or a TTL sweep indexing expired fork
children).

The commit's own
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

Modules `HistoryAbsorb.tla` and `MC_HistoryAbsorb.tla`. Since the merge of
`slate` (`3a24eace`), the model maps slate's absorber: an advance retires
only from the stream's own boundary (`retire_absorbed`: Exact, Detached or
Diverged, `9c6675d7`), each submitted advance carries a settlement receipt
counted per stream bucket (`Submissions`, `b5751e75`), and a stranded lane
mark is rolled back at plan time only while the stream's bucket is settled.
The dirty-index rescan only seeds work. The mechanism the model mapped
before the merge (the committer's `stored_frame_bytes` recount, the lane's
answered receipts and `settle_submissions`, the replay-capped `LaneMark`, the
rescan's rollback) is no longer in the code and no longer in the model.

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
- the settlement receipts are exactly the advances that can still move or
  publish the boundary: an applied group holds one iff it retired an Exact
  advance, and a durable group still holds one iff dispatch has not
  published the boundary it moved (`ReceiptsMatchInFlight`, part of
  `TypeOK`; a queued batch holds one by construction);
- the per-stream `unabsorbed_bytes` ledger equals the bytes in
  `[absorbed, next)` in every view, whatever range a re-gather starts at and
  whatever advances are lost (`LedgerExact`; it fails when the committer
  retires an advance that does not start at the boundary,
  `nc-retire-chunk-bytes*`);
- the settlement gate: a gather never starts below a queued advance's end
  or below the applied boundary (`NoRegatherUnderInFlight`; it fails when
  receipts settle before dispatch publishes, `nc-settle-before-publish`).
  The sweep prune is not gated on settlement (slate's decision D6), so the
  property is checked until a prune has removed a mark whose advance was in
  flight, which `witness-EvictedMarkPrunedInFlight` shows is reachable;
- the committer drops an advance as Detached only after a refused group or
  a dropped op lost an advance of the stream, or after that ungated prune
  (`DetachedOnlyAfterFault`; it fails with the gate removed,
  `nc-ungated-rollback`, which is slate's own planted control);
- the reader admits every routing key's postings pages as one index,
  including the overlapping pages a refused chain, the ungated prune or a
  new owner leaves (`PagesAdmit` in both page shapes; `keep_past`,
  `d16559b3`; it fails when a scan loses its snapshot,
  `probe-scan-per-row`). Overlaps themselves are reachable, so the former
  `NoStraddledChunk` is now the witness `Witness_OverlapAdmitted`;
- the postings-slice cache never proves the absence of a record that history
  holds below the durable absorbed boundary (`CacheNeverProvesFalseAbsence`;
  it fails with slate's warm install from `plan.from`, TLA-016-F3);
- durable and published frontiers are monotone (`FrontiersMonotone`);
- (liveness) once faults cease, every appended record is absorbed and the
  advance is durable and published (`AbsorptionCompletes`). A refused
  advance of a quiet stream heals only through the dirty-index rescan
  (`liveness-refusal`; it fails without the plan-time rollback,
  `nc-no-plan-rollback`).

These hold under crashes between every step, failed and ambiguous history
flushes, refused commit groups and dropped `Absorbed` ops (both settle their
receipts), chained advances behind a refused one, marks pruned for evicted
handles while their advances are queued, a bucket-sharing stream holding the
rollback gate closed, delayed `AbsorbedBatch` messages, stale re-plans from
the published boundary, trims between a gather's plan and its scan, gathers
served by the durable ring or by a Remote scan, per-stream caps of one or two
records, exhausted trim budgets, cache evictions and cold loads.

**Requirement anchors:** H1, H3, H4, H13; H2 and H11 through the cache claim.

### Observation boundary

The shard DB tail row is observed at four levels: `A`, the committer's
applied overlay base (`handle.state.applied`); `pend`, the applied but not
yet durable groups in WAL order; `D`, the Remote-durable prefix that
`DurabilityLevel::Remote` reads see; and `P`, the published
`handle.state.durable` written by `dispatch_durable`. History rows (canonical
`hc`, postings `hp`) are `none`, `mem` (in the WAL-less partition memtable)
or `dur`. The absorber's gather, its lane mark (`Absorber::submitted`, a
number; `NoMark = 0` is no entry), its pending-roster entry (`due`) and the
committer channel are volatile. The channel carries
`(from, upto, bytes)`, the `CopiedBytes` of each advance. The receipts of
this stream are one per channel entry, a `held` flag per applied group, and
`durHeld`, the receipts of durable groups not yet dispatched; the bucket
counter is zero when none is outstanding and the bucket mate (`mateBusy`,
with `BucketMate`) is idle. With `Pages`, the postings pages are
`<<key, first offset, offsets>>` under the (key, first offset) page key, in
the memtable (`pgMem`) or durable (`pgDur`). A reader sees the memtable over
the durable pages, and `PagesAdmit` is the cold index load's admission
(`append_page_runs`, `keep_past`) evaluated over that view in every state.
The postings-slice cache is modelled by the claims it makes: per key
`covered_from`, `indexed_to_offset` and the runs, and the segment's warm
record (`from`, `to`, `clean`). The reader is its published snapshot only;
the merged read is TLA-018.

### Atomicity / linearization table

File and line references are at `3a24eace`.

| Model action | Production function(s) | Atomicity justification / dependency contract |
|---|---|---|
| `CustomerAppend` | `CommitTransaction::append` (`src/shard/transaction/append.rs:19`), `finish` and `write` (`finalize.rs:3`, `:170`), `publish` (`publish.rs:17`) | One `WriteBatch` per commit group, applied atomically (ASM-SLATEDB-DURABLE e). Groups carry one operation each; coalescing only removes crash points. |
| `WalDurable` | SlateDB WAL flush; `durable_seq` from `db.subscribe()` in `ShardEngine::acker_loop` (`src/shard.rs:3020`) | Remote durability is a prefix of applied order (ASM-SLATEDB-DURABLE f). A group's receipts stay in its `DurableEffects` (`src/shard/commit_plan.rs:47`) past this point: `durHeld` counts them (the operator `HeldPastWal`). |
| `Dispatch` | `ShardEngine::dispatch_durable` (`src/shard.rs:2949`): tails at `:2988-2991`, `AbsorbSignal` sent at `:3004-3006`, the group and its receipts dropped at the end of the iteration (`:3012`) | Publishes `handle.state.durable` in group order under the handle mutex, then drops the group's receipts (ASM-HISTORY-SETTLEMENT); so `durHeld` returns to 0. May lag durability. The signal of an append group re-pends the stream; the model never drops it (`try_send` can; the rescan covers that). A signal that arrives during a gather is handled by the absorber's select loop after the gather settles (`worker.rs:62-77`), so it re-pends the stream after that gather. |
| `Crash` | Process crash, engine close (including `write_failed` → `begin_close`, `finalize.rs:202-206`) or ownership move; the absorber task ends with the engine (`worker.rs:28-31`) | Loses applied groups, the committer channel, the lane marks, the pending roster, the absorber's `Submissions` (one per engine, `src/history.rs:750`) and the partition memtable, pages included (WAL disabled, `history_settings`, `src/history.rs:456`). Keeps `D`, durable history rows and dirty rows. The process-wide postings cache is wiped (process crash) or kept (engine close or move); both are explored. |
| `HistoryBackgroundFlush` | SlateDB memtable flush (size-triggered, or the final flush in `Db::close`) | The whole memtable goes to L0 atomically (ASM-SLATEDB-DURABLE h). |
| `MateToggle` | Another stream whose hash maps to the same one of the 1,024 settlement buckets (`Submissions::bucket`, `commit_plan.rs:247`) submits an advance or settles its last one | An environment step, enabled by `BucketMate`. Sharing only delays a rollback, never permits one. |
| `AbsorberPlan` | `Absorber::plan_reads` (`src/history/gather.rs:468-487`): `stream_handle`, then, while `Submissions::settled` (`commit_plan.rs:264`), `roll_back_stranded_mark` (`gather.rs:354`) with `resident_absorbed` (`src/shard.rs:2402`); then `plan_read` (`gather.rs:498`) | One step: the gate, the published boundary, the rollback and `from = max(mark, P.abs)`, `upto = P.next`. Only this task submits, so a settled bucket stays settled until it submits again, and a group holding a receipt is the only thing that moves `P.abs`; the Release drop and Acquire load make every boundary published before the last receipt dropped visible (ASM-HISTORY-SETTLEMENT). The rollback removes a mark above `P.abs` (operator `RollbackGate`). An empty window is `no_work`, which leaves the roster (`settle_gather`, `worker.rs:400-405`). The ring-or-scan choice is made here. `resident_absorbed` returning `None` (an eviction between the load and the check) only skips a rollback, like the bucket mate. |
| `AbsorberRead`, `AbsorberReadEnd` | `read_wave` → `read_frames_range` (`gather.rs:530`; `src/shard/record.rs:149`) | A ring hit (`ring_read`, `record.rs:173`) returns the window densely, including rows a trim has deleted. Otherwise one `DurabilityLevel::Remote` scan (`record.rs:175-186`), observed row by row, over the snapshot taken when it starts (ASM-SLATEDB-DURABLE j): it skips the rows trimmed by then and no later ones. The per-stream byte cap is `Cap` equal-size records. |
| `AbsorberStage` | `stage_chunk` (`gather.rs:558`) → `note_frames` (`:156`), `check_postings` (`:191`), `stage_checked` (`:210`) | Canonical rows and postings pages go into one `WriteBatch`, atomic in the memtable. With `Pages` each key's page replaces any page under the same (key, first offset) (ASM-HISTORY-PAGES). An empty chunk is `no_work`. `check_postings`' own-chunk refusal (`failed`) is not modelled. |
| `AbsorberFlushOk` / `AbsorberFlushFail` | `Absorber::commit`: `write_with_options`, then `part.flush()` (`gather.rs:647`, `:657`, `:671`); error path `settle_gather_error` (`worker.rs:421`) | Flush `Ok` means every earlier write is durable. `Err` is ambiguous: rows and pages may or may not be durable, and if not they stay in the memtable. No install, no submit, no mark raise; the stream stays pending. |
| `AbsorberSubmit` (with `InstallChunk`) | After the flush: `postings_cache.install_chunk(inc, chunk_from, chunk_to, runs)` over the staged range `note_frames` returned (recorded in `stage_chunk`, `gather.rs:612-614`; installed at `:682-686`; `src/postings_cache.rs:274`); `Submissions::submit` per advance, the receipt riding `CopiedBytes::new(plan.from, chunk_raw)` (`gather.rs:616-618`, `:689-695`; `commit_plan.rs:252`); `submit_absorbed_batch_v2` (`src/shard.rs:1970`); `raise_lane_marks` (`gather.rs:697`, `:714`); then `settle_gather` (`worker.rs:348`) | One step: no await separates the install loop from the count and the send, nor the send's return from the raise. A crash while the send is blocked leaves claims about rows that are already durable, which is the kept-cache branch of `Crash`; a send the closed queue refuses drops its receipt with the batch (engine close). The raise sets `mark = max(mark, u)`. The roster keeps the stream after a partial chunk (`u < plan.upto`) or when a signal arrived during the gather; otherwise it leaves. The install's start is the operator `WarmInstallFrom` (F3). |
| `RescanSeed` | `seed_from_dirty_index` (`gather.rs:271`) every `RESCAN_EVERY` = 120 ticks and at a new owner's start (`worker.rs:47`, `:83-113`): `scan_dirty_streams_page` (`src/shard.rs:2108`), `backlog_of` → `tail_fields` (`gather.rs:318`; `src/shard.rs:2158`, a Memory-level read) | Merges a stream whose row shows unabsorbed records into the roster (`or_insert`). It no longer touches lane marks. It is the only thing that re-pends a quiet stream whose advance was refused or dropped. |
| `MarkPrune` | The sweep's `submitted.retain` (`worker.rs:125-143`); `evict_idle_handles` (`src/shard.rs:2354`); reload in `stream_handle` (`src/shard.rs:2292`) | Atomic under the lane mutex. Keeps a mark while the stream is pending or its resident absorbed boundary trails the mark; prunes when the stream is not pending and `P.abs ≥ mark` or the handle is evictable (ASM-HISTORY-EVICTION). Not gated on settlement (D6). A reloaded handle reads the Memory-level tail, which then equals `D` and `P`. |
| `CommitAbsorbed` | `CommitTransaction::absorbed` (`src/shard/transaction/maintenance.rs:236`, guard `:274`) → `advance_boundary` (`:287`) → `retire_absorbed` (`commit_plan.rs:304`); `expand` (`prepare.rs:16-24`) | An advancing op (`upto > absorbed`) whose `from` is the applied boundary is Exact: boundary to `min(upto, next)`, ledger minus `len`, `trim_safe_to` raised to the previous boundary, budgeted trims (`maintenance.rs:324-334`), and the group keeps the receipt (`:338-340`; `held`). If `len` exceeds the ledger it is Diverged: `accounting_diverged` refuses the whole group in `finish` (`finalize.rs:5-8`). Any other `from` is Detached: a warning, nothing moves. Detached, Diverged and non-advancing ops drop their receipt at staging. The mutation point is `Retires`. |
| `CommitRefuseGroup` | The whole group carrying the advance is refused before it is written and its receipts drop with it: a closed engine or a billing-row pre-read failure in `CommitTransaction::run` (`src/shard/transaction/mod.rs:53-68`), another operation's accounting divergence (`finalize.rs:5-8`), the `stage_maintenance` divergence (`finalize.rs:25-31`), or the test failpoint `fail_next_absorbed_group` (`finalize.rs:20-23`); `reject` (`mod.rs:196`) | Nothing is written; the stream's mark is stranded until its bucket settles and a plan runs. Bounded by `MaxRejects`. |
| `CommitDropOne` | `stage()` drops only this `Absorbed` op when `stream_handle` fails (`mod.rs:153-158`; `reject_op`'s `_ => {}`, `mod.rs:128`) while the rest of the group lands | The receipt drops at staging, so the stream settles as for a refusal. A layout-sealed lane drop (`maintenance.rs:257-273`) has the same shape but needs two lanes, which the model does not have. Bounded by `MaxDrops` within `MaxRejects`. |
| `TrimStep` | `TrimTick` → `expand` (`prepare.rs:25-43`) → `CommitTransaction::trim` (`maintenance.rs:343`) | Budgeted deletes in one batch. |
| `CacheEvict` | Weight eviction at the end of `install_chunk` (`postings_cache.rs`), or the idle sweep | Removes one slice and taints the segment's warm window (`clean = false`). |
| `CacheLoad` | `runs_for` → `Decision::Lead` → `spawn_load` → `publish_load` (`postings_cache.rs:502`) | A cold load of all the key's pages up to the reader's absorbed boundary. Capped loads and loads that merge into a resident slice are not modelled. |
| `ReaderSnap` / `ReaderRelease` | The snapshot in `execute_segment` (`src/application/read.rs:130`) | Taken under the handle mutex. |

### Assumptions

ASM-SLATEDB-DURABLE, ASM-HISTORY-ACTORS (liveness, including the rescan
cadence), ASM-HISTORY-REABSORB, ASM-HISTORY-EVICTION,
ASM-HISTORY-SETTLEMENT (the plan's gate and the receipts' drop after
publication) and ASM-HISTORY-PAGES (the page shapes, the overlap witnesses
and the scan probe). The cache property checks
ASM-HISTORY-POSTINGS-CACHE for the install path; it does not assume it.

### Constants per configuration

Every configuration uses `InitNext = 1`, `TrimBudgets = {0,1,3}` and keys
`K1, K1, K2` for offsets 0, 1, 2 (and `K1` for offset 3 where `N = 4`).
Records are one byte each, so the exact ledger is `next − abs`. *Refusals*
(`MaxRejects`) bounds refused groups and dropped ops together; *Drops*
(`MaxDrops`) bounds the dropped ops among them. *Prune* enables the sweep's
lane-mark prune, *Mate* a bucket-sharing stream (`BucketMate`), *Pages* the
page ranges. The dirty-index rescan is always on.

| Config | N | MaxPend | MaxChan | Crashes | FlushFail | Refusals | Drops | Reads | Cap | Ring | Cache | Evictions | Pages | Prune | Mate | Spec |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `small`, controls, witnesses (except as below) | 3 | 2 | 2 | 1 | 1 | 1 | 1 | 1 | 2 | no | no | 0 | no | yes | no | `Spec` |
| `expanded` | 3 | 3 | 2 | 1 | 1 | 1 | 1 | 2 | 1 | yes | no | 0 | no | yes | no | `Spec` |
| `ledger_small`, `w_MateDelaysRollback` | 3 | 2 | 2 | 1 | 1 | 1 | 1 | 1 | 2 | no | no | 0 | no | yes | yes | `Spec` |
| `w_RingGatherBelowTrim` | 3 | 2 | 2 | 1 | 1 | 1 | 1 | 1 | 2 | yes | no | 0 | no | yes | no | `Spec` |
| `ledger_overlap`, `nc_retire_chunk_bytes_overlap`, `nc_ungated_rollback`, `nc_settle_before_publish` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 1 | 2 | no | no | 0 | no | yes | no | `Spec` |
| `cache`, `nc_warm_install_from_plan`, `w_WarmBridgeCovers`, `w_InstallStartsAbovePlan` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 1 | yes | yes | 1 | no | yes | no | `Spec` |
| `cache_cap2` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | yes | yes | 1 | no | yes | no | `Spec` |
| `pages` | 4 | 2 | 2 | 0 | 1 | 2 | 1 | 0 | 2 | yes | no | 0 | yes | no | no | `Spec` |
| `pages_regather` | 3 | 2 | 2 | 1 | 0 | 1 | 0 | 0 | 2 | yes | no | 0 | yes | yes | no | `Spec` |
| `probe_scan_per_row` (every record under K1) | 3 | 2 | 3 | 0 | 0 | 0 | 0 | 0 | 2 | no | no | 0 | yes | yes | no | `Spec` |
| `w_OverlapAdmittedRefusedChain`, `…Prune`, `…NewOwner` | 3 | 2 | 2 | 0, 0, 1 | 0 | 1, 0, 0 | 0 | 0 | 2 | no | no | 0 | yes | no, yes, no | no | `Spec` |
| `liveness`, `nc_retire_chunk_bytes_liveness` | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | no | no | 0 | no | yes | no | `LiveSpec` |
| `liveness_refusal`, `nc_no_plan_rollback` | 3 | 2 | 2 | 0 | 0 | 1 | 1 | 0 | 2 | no | no | 0 | no | yes | no | `LiveSpec` |

The state spaces are far smaller than the pre-merge model's (`small`
5,254,914 distinct states against 31,506,682): the model no longer carries
the lane's receipt sequence, the replay end or the rescan's two-step
observation. So `liveness_refusal` now has two queued batches, two
applied-not-durable groups and the prune, which the pre-merge model could
not afford, and `pages` has a dropped op, which now heals through the
rescan instead of deadlocking.

`TrimBudgets` is the set of per-operation allowances that the shared
`trim_global_budget` and `max_trim_per_op` can leave; 0 means exhausted.

**Liveness scope (`LiveSpec`).** Faults cease because crashes, flush
failures, refusals, drops and appends are bounded. The `liveness` shape has
none of the first four; `liveness_refusal` has one refused group or one
dropped op. Fairness is on actor attempts, never on a success outcome:
`WF` on `WalDurable`, `Dispatch`, `AbsorberRead`, `AbsorberReadEnd`,
`AbsorberStage`, the flush attempt, `RescanSeed` and `ReaderRelease`; `SF`
on `AbsorberPlan`, `AbsorberSubmit`, the committer's handling of a message
(`CommitAbsorbed ∨ CommitRefuseGroup ∨ CommitDropOne`) and `TrimStep`, which
are only intermittently enabled. No fairness on appends, crashes, prunes,
the bucket mate, evictions or loads; the liveness shapes have no bucket
mate, because a mate that stays busy forever would hold the rollback back
forever. The rescan's fairness is load-bearing: a refused advance leaves a
quiet stream out of the roster with its mark stranded above the boundary,
and only the rescan re-pends it. A scratch run of `liveness_refusal` without
`WF(RescanSeed)` violates `AbsorptionCompletes` (14,317 distinct states);
in production the heal waits for the next rescan, up to 120 ticks (about 10
minutes at the 5 s tick).

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
| `nc_retire_chunk_bytes` | `Retires <- MutRetireIgnoringFrom`: `retire_absorbed` ignores `from` (pre-fix F1; slate's planted control for `9c6675d7`) | `LedgerExact` | A refused advance and a re-plan from the raised mark under-retire. |
| `nc_retire_chunk_bytes_overlap` | `Retires <- MutRetireIgnoringFrom` | `LedgerExact` | A re-gather after the ungated prune starts below a queued chunk and over-retires, with no lost publication. |
| `nc_retire_chunk_bytes_liveness` | `Retires <- MutRetireIgnoringFrom` | `AbsorptionCompletes` | After an over-retirement every advance to `next` is Diverged and its group refused. |
| `nc_warm_install_from_plan` | `WarmInstallFrom <- MutWarmInstallFromPlan`: slate's install from `plan.from` (pre-fix F3) | `CacheNeverProvesFalseAbsence` | A re-gather after the ungated prune, over a head trimmed after its plan, gives an evicted key a slice that proves a durable record absent. |
| `nc_ungated_rollback` | `RollbackGate <- MutRollbackUngated`: the plan rolls a mark back whether or not the bucket is settled (the pre-`b5751e75` rescan rule; slate's planted control) | `DetachedOnlyAfterFault` | With no fault at all, the re-gather starts under the queued advance, which lands first, and the committer drops the re-gather as Detached (its pages overlap). |
| `nc_settle_before_publish` | `HeldPastWal <- MutSettleAtWal`: a group's receipts settle once it is remote-durable, before dispatch publishes its tails | `NoRegatherUnderInFlight` | The plan sees the bucket settled while `P.abs` still lags the durable boundary, rolls the mark back and re-gathers below the applied boundary. This is the order `882004d9` says no test isolates. |
| `nc_no_plan_rollback` | `RollbackGate <- MutRollbackNever` (slate's planted "rollback removed") | `AbsorptionCompletes` | Now that the rescan only seeds work, nothing else heals a stranded mark. In the counterexample a dropped op leaves the mark at 2 over boundary 0; every later plan starts at the mark and finds no work, and the rescan re-pends the stream forever (with new data the gather would be Detached instead). |
| `probe_scan_per_row` | `ScanView <- MutScanPerRow`: the Remote scan reads the trim point row by row instead of one snapshot (ASM-SLATEDB-DURABLE (j)) | `PagesAdmit` | After the ungated prune, a stale re-gather reads row 0; the three queued advances land and trim row 1; the scan stages `{0, 2}`, whose first-0 page disagrees with row 1's page, and the index is refused. The admission rests on dense chunks. Re-evaluated for the merge: the gated rollback never re-gathers while an advance can land, so the probe now needs the prune and three queued batches. |

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
| `GatherOverTrimmedPrefix` | A Remote-scan gather re-reads a range whose head is already trimmed (after the ungated prune). |
| `StaleSnapshotLosesTail` | A snapshot more than one advance stale has part of its tail range trimmed (F2). |
| `TwoAdvancesNotDurable` | Two advances are applied but not yet remotely durable. |
| `EvictedMarkPrunedInFlight` | A mark is pruned for an evictable handle while its advance is still queued (the ungated prune, D6). |
| `RingGatherBelowTrim` | A ring-served gather returns rows below the durable trim point. |
| `WarmBridgeCovers` | A clean warm window extends a slice's proven coverage past its `indexed_to_offset`. |
| `InstallStartsAbovePlan` | A re-gather whose scan skipped a trimmed head warms the cache over the rows it staged, a range starting above `plan.from` (the F3 fix path). |
| `RefusalHealed` | A refused advance strands the mark; the settled plan rolls it back and absorption completes. |
| `MateDelaysRollback` | A plan keeps a stranded mark only because a bucket-sharing stream has an advance in flight, and absorption still completes. |
| `OverlapAdmittedRefusedChain`, `OverlapAdmittedPrune`, `OverlapAdmittedNewOwner` | A re-gather over pages an earlier chunk wrote (after a refused chain, the ungated prune, or by a new owner) writes a page that overlaps that chunk's; the reader admits the key's pages and absorption completes (`d16559b3`). |

### Exclusions and what is not claimed

- One stream, 3 offsets and equal-size records. Multi-stream `AbsorbedBatch`
  coalescing and the global trim budget appear only as the nondeterministic
  per-operation allowance. The other streams of a settlement bucket are the
  `mateBusy` toggle. Cross-stream effects (the shard maintenance row, other
  streams' operations in a refused group) are inferred from code, not
  modelled.
- The pending roster is one flag. Due and threshold selection, backoff,
  pacing, budget deferral, `check_postings`' own-chunk refusal and the
  v1/v2 lane seal are not modelled. Signals are never dropped (the rescan
  covers a dropped signal). Discovery liveness is TLA-017.
- Time is not modelled: the tick, the rescan cadence and how far the
  committer lags are free. The liveness result says a refused quiet stream
  heals eventually, not how soon.
- The committer handles one batch per group. Coalescing several batches
  into one group, which refuses or lands them together, is approximated by
  consecutive single-batch groups with the same outcome, reachable only
  where the refusal bound allows as many refusals.
- Postings page encoding, buckets and the 32 KiB page split are abstracted.
  With `Pages`, a page is its key, first offset and offsets
  (ASM-HISTORY-PAGES). The reader is the admission predicate over each key's
  pages in the store. The cold load's own read, its caps and the envelope
  fallback are not modelled. `keep_past`'s admission of an agreeing
  duplicate page inside one chunk (`check_postings`) is not modelled. The
  cache's admission line, capped loads, loads merging into a resident
  slice, the warm-record cap and idle expiry, and failed seams are not
  modelled. The cache-bridge defect found during the F3 fix lived in that
  gap.
- The plan's gate is one atomic step with the published-boundary read
  (ASM-HISTORY-SETTLEMENT); memory orderings are not modelled.
- SlateDB behaviour is assumed (ASM-SLATEDB-DURABLE), not verified.

---

## TLA-018 — Exact composition of history, hot storage, and read visibility

Modules `ReadCompose.tla` and `MC_ReadCompose.tla`.

The merge of slate at `3a24eace` changes nothing this model maps. `read.rs`,
`read_request.rs`, `read_continuation.rs`, the postings reader and the tail
ring are unchanged. `record.rs` only splits the gather's range-read error by
owner, and `history.rs` changes only the absorber's own state. Slate's typed
cursor verdicts rename the decoder's refusals. Its offset codec
(`offsets::encode`, `parse`, `parse_scalar`) carries the same
`(segment, after)` position the model abstracts as an offset. The
single-epoch descriptor (`796211d7`) removes branches no read reached.

### API semantics (defined before asserting completeness)

- **`deliver=durable`** (the default everywhere: raw route, product reads,
  scans, forks, peers). A page serves only Remote-durable records. The
  continuation cursor promises the whole eligible prefix: every record of the
  selected key below it was delivered exactly once, with its durable content.
- **`deliver=applied`** (product reads and long-poll only; SSE and forks
  refuse it: `src/product.rs:2395`, `:2436`; `read_request.rs:154-155`). The
  tail may include applied, not yet durable records, marked with
  `Prisma-Pending-From`. Only the durable resume cursor carries a promise:
  `min(consumed, handle.durable.next)`, with `handle.durable.next` read after
  the page (`read_request.rs:648-652`). Records at or beyond it may be
  replaced after a crash or ownership move. The code's stated intent is that
  applied reads never see less than a durable reader
  (`src/shard/record.rs:305-308`). A page that ends past the durable resume
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
| `WHistFlush` | A gather: one `WriteBatch` and `part.flush()` (`absorb_gather_v2_with` through `Absorber::commit`, `src/history/gather.rs:399-705`) | Collapsed into one step that raises the contiguous durable frontier `hF`. Justified by TLA-016's H3 and `LastRecoverableCopy` (ASM-HISTORY-WRITER). |
| `WMove` | Ownership move: the new engine opens the shard DB, fencing the old writer, and loads the durable tail | ASM-HISTORY-FENCED-VIEW. The old engine keeps frozen, self-consistent views. |
| `WLosePostings`, `WLoseCanonical` | **Not production.** Probe actions only | They violate ASM-SLATEDB-DURABLE or ASM-SLATEDB-GC. |
| `RStart` | `ReadService::execute_read` (`read_request.rs:259-452`): `tail_state`, then `check_entry_start` (`:350-352`, `:693-707`) and the `start > end` guard (`:353-355`); snapshot in `execute_segment` (`src/application/read.rs:130`, `:146-165`) | Taken under the handle mutex. `StartAllowed` is `check_entry_start` (mutation point `ContinuationCheck`): the same engine continues; another engine continues only if `ObservedIn`, `verify_continuation`'s re-read of `[pfrom, pos)` (`:716-751`) on the current engine's applied view, matches the digest (`Continuation::observed_in`, `read_continuation.rs:176-186`); a V2 position starts only at or below `handle.durable.next` (`:704-706`). The re-read is one atomic observation of the current engine; it reads the same history and tail rows as a page. |
| `RHist` | `decode_history_range` (`read.rs:720`) → `read_history2_scan` (`src/history.rs:865`) or `read_history2_keyed_cached` (`history.rs:975`) → `PostingsCache::runs_for` (`src/postings_cache.rs:502`) → `execute_postings_plan` (`src/history/postings_read.rs:13`), or the corruption envelope; `PageBudget` | One step: rows below the boundary are immutable. A missing canonical row is skipped silently by every source, as in production (`history.rs:865-892`; `postings_read.rs:80-96`). Postings runs are abstracted by ASM-HISTORY-POSTINGS-CACHE. |
| `RTailStart` | `ring_read` (`src/shard/tail_ring.rs:97`) and `proves_durable_ring` (`src/shard/record.rs:124`), or the start of `read_frames_until` (`record.rs:292`) | The ring returns durable copies with a density proof (ASM-HISTORY-RING); durable mode only (`record.rs:327`). |
| `RTailStep` | One row of the scan at `deliver.durability()`: Remote for durable, Memory for applied (`record.rs:345`, `:232-237`) | Per row: the iterator is not treated as a snapshot. |
| `RTailCheck` | `absorption_race` (`read.rs:790`) → `ShardEngine::visible_absorbed(hash, visibility)` (`read.rs:828`, `:838-842`; `record.rs:253-276`), then the loop decision in `execute_segment` | A `get` of the tail row at the scan's own level. The operator `RaceBoundary` is this read (F1 fix). |
| `EndPage` | `page_progress` (`read.rs:626`), then `durable_resume.after = next.after.min(floor)` with `floor` read after the page (`read_request.rs:641-652`), then `Continuation::after_page` (`:655-666`; `read_continuation.rs:120-151`) | One page and one end-of-page handle read. The continuation's history is the page's engine (`WriterHistory::of(&engine)`); the digest restarts at the recovery offset when it reached the page start, else carries the continuation the page began at, else starts at the page start. |
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
`history_settings` (`src/history.rs:456-527`; GC interval
`HISTORY_GC_INTERVAL_SECS`, 600 s by default; upstream `min_age` 300 s;
`manifest_poll_interval` 300 s at `:493`), shard DBs in
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
| `FlushUpload`, `FlushCommit` | SlateDB memtable flush: L0 upload, then the manifest write; a conflict with the compactor's newer manifest makes the writer load and merge it first, so `wv` is current afterwards. The history partition flushes from `Absorber::commit` (`src/history/gather.rs:671`) | Two separate steps. The manifest write does not re-check object existence (ASM-SLATEDB-GC). |
| `CompactStart` / `CompactUpload` / `CompactCommit` | Embedded compactor configured by `history_settings`; its commit is `write_manifest` in SlateDB `compactor_state_protocols.rs`: a checkpoint on the stored manifest with a 900 s lifetime, then the manifest swap | The job is recorded (low watermark) before the output upload. The checkpoint write and the swap are two CAS writes, merged into one step: in between, the live manifest still names the inputs. Mutation point `CompactionCheckpoint`. The compactor does not refresh the writer's view (ASM-SLATEDB-GC iv). |
| `WriterPoll` | `PollManifest` every `manifest_poll_interval` (300 s), which merges the stored manifest (`src/history.rs:493`) | Atomic merge under the DB state lock. It may happen at any time; `Tick` forces it within `PollInterval` ticks of the view going stale (ASM-SLATEDB-COMPACTION-CHECKPOINT). |
| `Tick` | Wall-clock time | Blocked while the writer view has been stale for `PollInterval` ticks or a read has run for `ReadSpan` ticks; forgets an expired compactor checkpoint. |
| `OrphanUpload` | A fenced old writer's flush whose manifest CAS fails (ASM-HISTORY-FENCED-VIEW) | The object exists but is never referenced. |
| `Advance` | `CommitTransaction::absorbed`, submitted only after `part.flush()` returned `Ok` (TLA-016 H3). Mutation point `AdvanceBacked` | — |
| `CkCreate` / `CkRelease` | Upstream user-checkpoint API. **The repository never creates one** | Models the upstream contract only (`UseCheckpoint`). The compactor's checkpoint is `CompactCommit`'s. |
| `ReadBegin` / `ReadEnd` | History reads through the writer `Db` (`decode_history_range`, `src/application/read.rs:720`; `read_history2*`, `src/history.rs:844-1047`) | The read captures `wv` and ends within `ReadSpan` ticks. A deleted SST in the view yields the upstream outcome (mutation point `UpstreamDeletedRead`), as the repository handles it (`map_err(\|e\| e.to_string())?`, mutation point `RepoOnDeletedRead`). |
| `GcReadCompactions` → `GcReadManifest` → `GcList` → `GcDelete`* → `GcFinish` | Upstream `GarbageCollector::run_gc_task` → `remove_expired_checkpoints`, then `CompactedGcTask::collect` (SlateDB `0717cc1`, `garbage_collector.rs`, `garbage_collector/compacted_gc.rs`) | Compactions are read before the manifest; the manifest read includes the manifests of unexpired checkpoints (`CheckpointRefs`; expiry is checked at that step, which can only make deletion earlier); then the list, then per-object deletes. At most one pass per tick. |
| `ForkBegin` | `fork::prepare` validates the live current incarnation (`src/application/creation/fork.rs:28`); for a recreated name, `Registry::recreate` (`src/registry.rs:984-1040`), which first records the replaced incarnation's closure debt (`:1017`; `registry/replaced.rs:129-153`) and then indexes the debt of the stored tombstone (`:1018`; `fork_debt.rs:193-201`), both before its CAS | For a child incarnation that recreates a name (`Prev`), the create overwrites the previous incarnation's tombstone and its debt; only its marker remains. The marker write and the CAS are two writes merged into one step: a crash between them leaves the marker beside the unchanged tombstone, which the reconciler repairs as usual. The closure-debt record is billing state and touches no fork reference. If it fails, the recreation fails before the marker and the CAS, as a create that never ran. Mutation point `IndexOverwritten`. Before the rollout the old binary writes no marker (`lostOld`). |
| `ForkInstall` | `anchor::install`: `mutate_incarnation(source, forked epoch)` (`src/application/creation/anchor.rs:78`) | One CAS bound to the forked incarnation (ASM-OBJSTORE-CAS). Idempotent when already installed; declines on a soft, tombstoned or recreated source. Mutation point `InstallFence`. |
| `ForkPostCheck` | `anchor.rs:134-182`: if the child incarnation vanished (lookup by name, bound to its epoch), release the fresh reference; otherwise require the source name's current descriptor to list the fork id | The release is one `release_fork_ref` CAS. The source presence check is by name, with no epoch check. |
| `CreatorCrash` | The create request dies before its post-check | Bounded fault. |
| `SourceDelete` | `delete_lifecycle` → `delete_transition` (`deletion.rs:249`, `:492`) | Soft versus tombstone is decided inside the CAS. Mutation point `DeleteDecision`. |
| `SourceRecreate` | A create under the same name after the tombstone; blocked while soft-deleted (F5) | New incarnation with no children. |
| `IndexDebt` | `delete_lifecycle` writes the child incarnation's fork-debt marker before the tombstone write (`record_fork_debt`, `deletion.rs:290-294`; `fork_debt.rs:150-176`) | One PUT. A failed write fails the delete before anything changed; a delete that dies here leaves a marker on a live child, which the reconciler defers. |
| `ChildDelete` | `delete_transition` on the child: the tombstone records `parent_ref_pending` in the same write | One CAS. After the rollout it requires the marker (mutation point `WriteAhead`); before it (`Legacy`) the old binary writes none. |
| `InRequestRelease` | The same request's `release_fork_ref(source, fork_id, source_epoch)`, `clear_parent_debt` when conclusive, then `settle_marker` (`deletion.rs:350-352`, `:72`, `:37`, `:451-459`) | The epoch check and the CAS are bound to one snapshot; an incarnation change is conclusive. One step. The marker removal is best effort (it may fail and stay) and only after a conclusive release (mutation point `SettleMarker`). |
| `RequestAbandon` | A crash or cancellation after the tombstone CAS | Bounded fault. The debt persists on the tombstone and the marker in the index. |
| `RetryDelete` | The **client** re-issues `DELETE` → `delete_lifecycle` → `repair_tombstone` (`deletion.rs:273-274`, `:372`) | Only while the name still holds the child's tombstone. Its fairness is the operator `ClientRetryFairness`, used only by `LiveSpecClientRetries`. |
| `Reconcile` | `fork-debt-reconcile` (`reconcile.rs:326-356`), one marker of a page (`settle`, `:202-261`): `repair_tombstone` for a tombstone with debt, marker removal for one without, `release_fork_ref` from the marker for a recreated name, deferral for a live child | One step per marker: each CAS in it is idempotent and a restart re-reads the marker. Mutation points `MarkerView`, `ReleaseId`, `SettleMarker`. Weakly fair per marker (`ReconcilerFairness`, ASM-HISTORY-ACTORS). |
| `Rollout` / `Backfill` / `BackfillFinish` | Deploying the binary with the index; one backfill step per reconciler round (`Registry::backfill_fork_debt`, `fork_debt.rs:290-348`; `reconcile.rs:275-296`) indexing each debt-bearing tombstone it walks, until the walk records `complete` and never runs again (`fork_debt.rs:306-308`, `:326-330`) | The walk's page order, persisted progress and ETag-conditional write are abstracted: until it completes, the backfill may index any unindexed debt-bearing tombstone that still holds its name, and it completes only when none is left (after the rollout no delete creates one). Without `Legacy` the index predates every delete, so the backfill starts complete. Mutation point `BackfillOn`. |

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
  In particular ForkPin has no expiry: a child dies only by `ChildDelete`.
  The recreation of an expired fork child's name (fixed) is covered by a
  real-code regression only, and the open expiry leak above is not modelled.
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
| HistoryAbsorb | `small` (5,254,914 states), `ledger_small` (19,579,296, with the bucket mate), `cache_cap2` (1,231,603), `pages_regather` (945,441), on the model reworked for the merge `3a24eace` | 23 | none across the four runs; `small` has no cache actions and no bucket mate, `ledger_small` no cache actions, `cache_cap2` no crash, flush failure, refusal, drop, reader or mate, `pages_regather` no cache, flush failure, drop, reader or mate. Within `CommitAbsorbed` the Detached branch fires (276,692 times in `small`); the Diverged branch never does, as `LedgerExact` implies, and fires only under the `Retires` mutation |
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
| `TLA-016_ledger_under_retire.trace.txt`, `TLA-016_ledger_over_retire.trace.txt`, `TLA-016_liveness_stall.trace.txt` | TLA-016-F1 | the pre-fix, pre-merge model (labelled) |
| `TLA-016_cache_false_absence.trace.txt` | TLA-016-F3 | the pre-fix, pre-merge model (labelled) |
| `TLA-016_overlap_admitted_refused_chain.trace.txt`, `TLA-016_overlap_admitted_prune.trace.txt`, `TLA-016_overlap_admitted_new_owner.trace.txt` | overlapping postings pages (fixed by `d16559b3`; they still arise) | the current model: the driver logs of `witness-OverlapAdmittedRefusedChain`, `-Prune` and `-NewOwner` |
| `TLA-018_applied_keyed_skip.trace.txt` | TLA-018-F1 | the pre-fix model (labelled) |
| `TLA-018_applied_stale_cursor.trace.txt` | TLA-018-F3 | the pre-fix model (labelled): the driver log of the former `known-defect-stale-applied-cursor` |
| `TLA-019_reader_view_deleted.trace.txt` | TLA-019-F1 (withdrawn) | the model without the compactor checkpoint (labelled) |
| `TLA-019_fork_no_retry.trace.txt`, `TLA-019_fork_pin_after_successful_delete.trace.txt` | TLA-019-F4 | the pre-fix model (labelled): the driver logs of the former `probe-no-client-retry` and of `witness-fork-PinAfterSuccessfulDelete` |
