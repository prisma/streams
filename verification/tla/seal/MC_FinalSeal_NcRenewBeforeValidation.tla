---------------- MODULE MC_FinalSeal_NcRenewBeforeValidation ----------------
(* NEGATIVE CONTROL (TLA-003-F4, the behaviour before "A seal retry refused *)
(* by its own instance's limits neither renews nor releases the claim"): a *)
(* raw exact retry of an owed final renews the claim in prepare_close,     *)
(* before parse_content validates its content on the handling instance.    *)
(* An exact retry over its instance's ingest capacity then renews the      *)
(* claim (a fresh generation and lease) and is refused 413 afterwards.     *)
(* Expected: IntentOnlyAfterValidation (shape V4).                         *)
EXTENDS MC_FinalSeal

NothingValidatedFirst(v) == FALSE
=============================================================================
