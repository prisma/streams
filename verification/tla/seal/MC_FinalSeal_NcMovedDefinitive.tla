------------------ MODULE MC_FinalSeal_NcMovedDefinitive ------------------
(* NEGATIVE CONTROL (TLA-003d): final_err_disposition classifies an         *)
(* ownership move (AppendErr::Moved) as a permanent refusal.  A raw close   *)
(* whose engine was replaced then releases its owed-final claim although    *)
(* the move says nothing about the record (it may even have become durable  *)
(* while being answered Moved).                                             *)
(* Expected: ReleaseOnlyWhenUndeliverable is violated.                      *)
EXTENDS MC_FinalSeal

MovedIsDefinitive(err) ==
    IF err \in {"Closed", "SealSuperseded", "ProducerSeqReused", "BadBody", "Moved"}
    THEN "definitive" ELSE "ambiguous"
=============================================================================
