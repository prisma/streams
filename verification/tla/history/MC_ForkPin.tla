----------------------------- MODULE MC_ForkPin -----------------------------
(* Model-checking wrapper for the TLA-019 fork-pin layer.  Baselines load  *)
(* this module unmodified; each negative control substitutes one operator. *)
EXTENDS ForkPin

MCChildren == {"F1", "F2"}

\* NC fork pin: the source is tombstoned regardless of live fork references.
MutDeleteIgnoresRefs(hasRef) == "deleted"

\* NC incarnation fence: anchor::install's CAS is not bound to the
\* incarnation the child forked, so a delayed install lands on a recreated
\* source (the child then anchors to a replacement it never forked).
MutInstallIgnoresIncarnation(c) == TRUE

\* ASSUMPTION PROBE (the F8 client premise, not production): no client ever
\* repeats DELETE, so only the in-process request steps are fair.
NoClientRetry == TRUE
=============================================================================
