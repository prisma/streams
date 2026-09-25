-------------------- MODULE MC_FinalSeal_NcReleaseByOp --------------------
(* NEGATIVE CONTROL (TLA-003b): abandon_seal_intent matches the operation   *)
(* id only, not the generation.  A definitively refused OLD attempt (e.g.   *)
(* fenced by a takeover reservation) then releases the claim its concurrent *)
(* exact retry has just renewed, and the retry's final still commits.       *)
(* Expected: ClosureAuthorized is violated.                                 *)
EXTENDS MC_FinalSeal

ReleaseByOpOnly(c, op, g) == c.op = op
=============================================================================
