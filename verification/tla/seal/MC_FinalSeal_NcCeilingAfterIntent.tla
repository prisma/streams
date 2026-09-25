----------------- MODULE MC_FinalSeal_NcCeilingAfterIntent -----------------
(* NEGATIVE CONTROL (TLA-003-F3, the behaviour before "A product seal       *)
(* refuses an over-ceiling final before it publishes its intent"): the     *)
(* product pre-intent checks measure ingest capacity but not the           *)
(* per-record ceiling.  An over-ceiling final publishes its claim, and the *)
(* append path refuses the record afterwards as a deferred BadBody, whose  *)
(* definitive disposition releases the claim.  With record ceilings that   *)
(* differ between instances, an exact retry renews the claim and releases  *)
(* it while the original's older generation can still commit.             *)
(* Expected: IntentOnlyAfterValidation and ClosureAuthorized (shape VP).   *)
EXTENDS MC_FinalSeal

CeilingAfterIntent(v) == v \in {"pre", "capacity"}
=============================================================================
