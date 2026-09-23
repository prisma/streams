-------------------------- MODULE MC_HistoryAbsorb --------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-016.  The baseline, known-defect and     *)
(* witness cfgs load this module WITHOUT any override except the key map;  *)
(* each negative-control cfg substitutes exactly one baseline operator     *)
(* with one of the Mut* operators below (Op <- MutOp).                     *)
(***************************************************************************)
EXTENDS HistoryAbsorb

\* Routing keys of offsets 0..2 (only the cache model reads them): records 0
\* and 1 belong to K1, record 2 to K2.
MCKeys == {"K1", "K2"}
MCKeyOf == [o \in 0..2 |-> IF o = 2 THEN "K2" ELSE "K1"]

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
=============================================================================
