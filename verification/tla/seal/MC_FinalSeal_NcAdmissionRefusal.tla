------------------ MODULE MC_FinalSeal_NcAdmissionRefusal ------------------
(* NEGATIVE CONTROL (TLA-003-F2, the behaviour before "A raw close that    *)
(* takes over an abandoned final claim writes its own record"): after      *)
(* begin_sealing_for_close gives a raw close-with-content its generation   *)
(* (installed, taken over or renewed), the plan keeps the admission        *)
(* snapshot's values: owed_final stays false and the Sealing refusal       *)
(* seen at admission stays set.  The committer then refuses the            *)
(* operation's own final as Closed and the definitive disposition releases *)
(* the claim it just took.                                                 *)
(* Expected: ReleaseOnlyWhenUndeliverable, FinalClosedTruthful,            *)
(* ClosureAuthorized, PlainCannotCompleteOwedFinal (renewal shape) and     *)
(* FinalClosedTruthful without a lease lapse (validation shape).           *)
EXTENDS MC_FinalSeal

AdmissionPlanKept(hr) == hr
=============================================================================
