----------------- MODULE MC_FinalSeal_NcAnyAttemptReleases -----------------
(* NEGATIVE CONTROL (TLA-003-F5, the behaviour before "A seal retry refused *)
(* by its own instance's limits neither renews nor releases the claim"):   *)
(* complete_raw_close releases the claim on every attempt's definitive     *)
(* refusal, not only the installing attempt's.  An exact retry refused     *)
(* BadBody by its own instance's lower record ceiling then releases the    *)
(* claim (at the generation it observed, which is the original's) while   *)
(* the original, valid where it installed the claim, can still commit.    *)
(* Expected: ReleaseOnlyWhenUndeliverable, ClosureAuthorized and           *)
(* PlainCannotCompleteOwedFinal (shape V5).                                *)
EXTENDS MC_FinalSeal

AnyAttemptReleases(hr) == TRUE
=============================================================================
