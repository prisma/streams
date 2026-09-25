-------------------- MODULE MC_FinalSeal_NcDupCompletes --------------------
(* NEGATIVE CONTROL (TLA-003c): any duplicate success is treated as final-   *)
(* close completion (seal_final / complete_raw_close no longer require the  *)
(* acknowledgement to be CLOSED).  A duplicate of an earlier NON-closing     *)
(* append then marks the final committed and the seal publishes under an    *)
(* operation whose record never closed the segment.                         *)
(* Expected: SealedFinalHasItsRecord is violated.                           *)
EXTENDS MC_FinalSeal

AnyDuplicateCompletes(rep) == rep.closed \/ rep.dup
=============================================================================
