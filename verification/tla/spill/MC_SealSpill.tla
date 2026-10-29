---------------------------- MODULE MC_SealSpill ----------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-016 (the seal spill).  Baseline and      *)
(* witness cfgs load this module without overrides; each negative-control  *)
(* cfg substitutes exactly one baseline operator with a Mut* operator      *)
(* below (Op <- MutOp).                                                    *)
(***************************************************************************)
EXTENDS SealSpill

\* NC prebuilt remainder (REVIEW-6A F1; the test build's
\* `SealHooks::prebuilt_remainder`, SealAdvance::snapshot_remainder): the
\* remainder starts from the pages at or above `upto` that the snapshot
\* knew, so a page applied between the snapshot and the commit is lost from
\* the pending row the put leaves.
MutRemainderFromSnapshot(s, w) == w.rem

\* NC skip 7a (REVIEW-6A F2; the test build's `SealHooks::skip_7a`): no
\* forced memtable flush is preceded by an awaited WAL flush, and a seal
\* does not wait for its group's dispatch before 7b.
MutSkip7a == FALSE
=============================================================================
