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

\* NC durable boundary (REVIEW-6A F2; SlateDB before slatedb/slatedb#2208):
\* a memtable flush keeps every version above the WAL's durable progress,
\* so a flush that overtakes the WAL keeps the operands a not yet durable
\* remainder put shadows.
MutDurableBoundary == dur

\* NC remainder keeps the index (S3a §4.1, §7): the seal's remainder commit
\* never deletes a stream's 'W' row, so a stream the seal emptied stays in
\* the unsealed index.
MutRemainderKeepsIndex(b, st) == IF WOp(b, st) = "del" THEN "none" ELSE WOp(b, st)
=============================================================================
