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
\* The same map over four offsets (the N = 4 shapes: recount, pages and the
\* recount witness): record 3 belongs to K1.
MCKeyOf4 == [o \in 0..3 |-> IF o = 2 THEN "K2" ELSE "K1"]
\* Every record under one key (the scan-snapshot probe): a page then spans
\* every row between its first and last offset.
MCKeyOfOne == [o \in 0..2 |-> "K1"]

\* NC publish-before-flush: the gather submits AbsorbedBatch as soon as the
\* rows are staged in the (WAL-less) partition memtable, before part.flush().
MutSubmitBeforeFlush(a) == a.ph \in {"staged", "flushed"}

\* NC canonical-only: stage_postings is dropped from the WriteBatch, so only
\* canonical frames become durable for the advanced range.
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

\* NC pre-fix TLA-016-F1 (before "An absorption advance retires exactly the
\* bytes of the range it moves the boundary over"): the committer retires
\* the byte count of the gather's chunk [from, upto), whatever range it
\* actually moves the boundary over.
MutRetireChunkBytes(m, prev, newAbs) == m.b

\* NC pre-fix TLA-016-F3 (before the fix that names the warm install by the
\* rows the gather staged): the install claims coverage from plan.from,
\* whatever the scan returned.
MutWarmInstallFromPlan(a) == a.from

\* NC pre-fix receipts (before "A refused absorption group rolls its lane
\* marks back, so a recount covers only chunks in flight"): the absorber
\* never learns that a group was refused, so settling rolls no mark back;
\* only the dirty-index rescan heals it.
MutSettleNeverRollsBack == FALSE

\* NC replay-unbounded: plan_read ignores replay_to, so the replay of a
\* refused chunk re-reads up to the durable end and can straddle pages a
\* failed (ambiguous) flush already wrote above the refused chunk's end.
MutPlanUptoIgnoresReplay(m, from, next) == next

\* PROBE scan-per-row (dependency contract ASM-SLATEDB-DURABLE (j)): the
\* Remote scan observes the trim point row by row instead of reading the
\* snapshot taken when it started, so a trim landing mid-scan can drop a
\* row from the middle of a chunk.
MutScanPerRow(a) == [set |-> TRUE, trimmed |-> D.trimmed,
                     next |-> IF a.snap.set THEN a.snap.next ELSE D.next]
=============================================================================
