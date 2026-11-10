------------------------ MODULE MC_QueueReclamation ------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-QR (QueueReclamation).  Baseline and     *)
(* witness cfgs load this module without overrides; each negative-control  *)
(* cfg substitutes exactly one baseline operator with a Mut* operator      *)
(* below (Op <- MutOp).  The numbers are S3b-DESIGN §11.2's.               *)
(***************************************************************************)
EXTENDS QueueReclamation

\* NC 1: retirement without the reference check: a chunk behind the frontier
\* is given back while a delivery still holds it.
MutUnreferenced(o) == TRUE

\* NC 3: purge_due acting on a Trimming row: a live queue's chunks below its
\* frontier reclaimed as if it were dead.
MutPurgeActsOn(s) == s \in {"Requested", "Purged", "Trimming"}

\* NC 5: the trim reading the frontier after the references: an offset
\* leased between the two reads is given back.
MutFrontierAtCommit == TRUE

\* NC 8: the purge reading 'k' after its row deletes: every chunk the trim
\* released is decremented a second time.
MutKAtReclaim == TRUE

\* NC 10: the cursor advancing past an unconsumed hint.
MutCursorAfter(taken) == nextHint
=============================================================================
