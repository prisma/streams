------------- MODULE MC_FinalSeal_NcAdmissionRefusalAnyRelease -------------
(* NEGATIVE CONTROL (TLA-003-F2 as the code stood before "A raw close that  *)
(* takes over an abandoned final claim writes its own record"): that code  *)
(* kept the admission plan (ClaimedFinalPlan <- AdmissionPlanKept) AND     *)
(* released on every attempt's definitive refusal (RefusalReleases <-      *)
(* AnyAttemptReleases, the behaviour before the TLA-003-F5 fix).  F2's     *)
(* orphaned-final and plain-seal-over-final forms need both: the renewing  *)
(* exact retry is refused Closed by its kept Sealing refusal and releases  *)
(* the claim the takeover attempt then commits under.  With either fix     *)
(* alone the forms are unreachable (nc-admission-refusal-release and its    *)
(* siblings substitute only the F2 operator).                              *)
(* Expected: ClosureAuthorized, PlainCannotCompleteOwedFinal (shape B).    *)
EXTENDS MC_FinalSeal_NcAdmissionRefusal, MC_FinalSeal_NcAnyAttemptReleases
=============================================================================
