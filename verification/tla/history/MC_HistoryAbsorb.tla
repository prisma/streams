-------------------------- MODULE MC_HistoryAbsorb --------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-016.  The baseline, known-defect and     *)
(* witness cfgs load this module WITHOUT any override except the key map;  *)
(* each negative-control cfg substitutes exactly one baseline operator     *)
(* with one of the Mut* operators below (Op <- MutOp).                     *)
(***************************************************************************)
EXTENDS HistoryAbsorb

\* Routing keys of offsets 0..2 (only the cache and page models read them):
\* records 0 and 1 belong to K1, record 2 to K2.
MCKeys == {"K1", "K2"}
MCKeyOf == [o \in 0..2 |-> IF o = 2 THEN "K2" ELSE "K1"]
\* The same map over four offsets (the N = 4 page shape): record 3 belongs
\* to K1.
MCKeyOf4 == [o \in 0..3 |-> IF o = 2 THEN "K2" ELSE "K1"]
\* Every record under one key (the scan-snapshot probe): a page then spans
\* every row between its first and last offset.
MCKeyOfOne == [o \in 0..2 |-> "K1"]

\* NC publish-before-flush: the gather submits AbsorbedBatch as soon as the
\* rows are staged in the (WAL-less) partition memtable, before part.flush().
MutSubmitBeforeFlush(a) == a.ph \in {"staged", "flushed"}

\* NC canonical-only: the postings pages are dropped from the WriteBatch, so
\* only canonical frames become durable for the advanced range.
MutPostingsNotWritten(h, S) == h

\* NC trim-to-proposed: the advancing op trims toward the newly proposed
\* absorbed boundary instead of trim_safe_to (the previous boundary).
MutTrimToProposed(safe, newAbs) == newAbs

\* NC tick-trim-to-absorbed: the TrimTick trims toward the current absorbed
\* boundary instead of trim_safe_to (drops the one-advance lag).
MutTickTrimToAbsorbed(t) == t.abs

\* NC trim-past-absorbed: the TrimTick trims toward next, past the absorbed
\* boundary (removes rows history does not hold).
MutTickTrimToNext(t) == t.next

\* Historical regression control (2026-07-27 boundary race, see
\* src/dst/tests/reads_ring.rs a_duplicate_absorbed_op_does_not_advance_the_trim):
\* a duplicate Absorbed op raises trim_safe_to to the LIVE boundary.
MutSafeRaisedOnDuplicate == TRUE

\* NC pre-fix TLA-016-F1 (slate's planted control for 9c6675d7,
\* `if copied.from != tail.absorbed && false`): retire_absorbed ignores
\* `from`, so every advancing op retires its chunk's byte count from the
\* boundary, whatever range it moves the boundary over.
MutRetireIgnoringFrom(m, prev) == TRUE

\* NC pre-fix TLA-016-F3 (before the fix that names the warm install by the
\* rows the gather staged): the install claims coverage from plan.from,
\* whatever the scan returned.
MutWarmInstallFromPlan(a) == a.from

\* NC ungated rollback (slate's planted control for b5751e75, the plan_reads
\* gate replaced by `true`; the pre-fix rescan rule): a stranded-looking
\* mark is rolled back even while an advance of the stream is in flight.
MutRollbackUngated == TRUE

\* NC no plan rollback (slate's planted control "plan_reads rollback
\* removed"): nothing rolls a stranded mark back, now that the rescan only
\* seeds work.
MutRollbackNever == FALSE

\* NC settle-before-publish: a group's receipts settle once it is remote-
\* durable instead of after dispatch_durable has published its tails (the
\* order 882004d9 records as resting on the drop at the end of the dispatch
\* loop's iteration).
MutSettleAtWal(h) == FALSE

\* PROBE scan-per-row (dependency contract ASM-SLATEDB-DURABLE (j)): the
\* Remote scan observes the trim point row by row instead of reading the
\* snapshot taken when it started, so a trim landing mid-scan can drop a
\* row from the middle of a chunk.
MutScanPerRow(a) == [set |-> TRUE, trimmed |-> D.trimmed,
                     next |-> IF a.snap.set THEN a.snap.next ELSE D.next]
=============================================================================
