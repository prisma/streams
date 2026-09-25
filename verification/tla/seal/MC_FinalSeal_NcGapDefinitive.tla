------------------- MODULE MC_FinalSeal_NcGapDefinitive -------------------
(* NEGATIVE CONTROL (TLA-003a): final_err_disposition classifies a producer *)
(* gap as a permanent refusal.  The raw close then releases its owed-final  *)
(* claim while its record can still be written: the segment is open, the   *)
(* producer tuple is unspent, and the predecessor may still commit.          *)
(* Expected: ReleaseOnlyWhenUndeliverable is violated.                      *)
EXTENDS MC_FinalSeal

GapIsDefinitive(err) ==
    IF err \in {"Closed", "SealSuperseded", "ProducerSeqReused", "BadBody", "ProducerGap"}
    THEN "definitive" ELSE "ambiguous"
=============================================================================
