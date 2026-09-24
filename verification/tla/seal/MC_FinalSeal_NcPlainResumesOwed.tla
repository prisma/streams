------------------ MODULE MC_FinalSeal_NcPlainResumesOwed ------------------
(* NEGATIVE CONTROL (the behaviour before "Only a close can resume an owed  *)
(* final"): prepare_close's owed-claim filter does not require a close.    *)
(* A raw append without Stream-Closed carrying an owed final's bytes and   *)
(* coordination has that final's semantic operation id, so it passes as    *)
(* the final's exact retry: it skips the Sealing refusal, renews the claim *)
(* and lands its record; the parked final can then land too.               *)
(* Expected: SealingRefusesNewAppends (shape OP).                          *)
EXTENDS MC_FinalSeal

AnyRequestResumes(close, sameOp) == sameOp
=============================================================================
