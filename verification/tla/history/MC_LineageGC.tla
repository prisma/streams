---------------------------- MODULE MC_LineageGC ----------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-019's two-holder arm (S6).  The baseline *)
(* cfg loads this module without overrides; each negative control          *)
(* substitutes exactly one baseline operator (Op <- MutOp).                *)
(***************************************************************************)
EXTENDS LineageGC

\* REPOSITORY NC (sweeper one holder): the sweeper reads one holder's list
\* and logs, not every leaf's.
MutOneHolder == {h \in Holders : leaf[h] /\ hasList[h] /\ h # "C1"}

\* REPOSITORY NC (release before zero): every decrement stages a release,
\* not only the one that removes the row.
MutStagesAlways(n) == TRUE

\* REPOSITORY NC (no version grace): the sweeper deletes at a version whose
\* grace has not passed, while a retired leaf's read still runs.
MutNoGrace == TRUE

\* REPOSITORY NC (merge drops the sibling): the merged leaf's count is one
\* source's, not the sum.
MutCountOneSource(o) == rc["C0"][o]

\* REPOSITORY NC (list after the CAS): M becomes a leaf before its list
\* exists.
MutListAfterCas == FALSE

\* REPOSITORY NC (list missing holds nothing): M never writes its list, and
\* the sweeper reads a leaf without one as holding nothing.
MutNoList == FALSE

\* REPOSITORY NC (pending carried): the merge carries the sources' 'E' rows,
\* so M logs a release of an object the other source still counts.
MutCarryPending == pending["C0"] \cup pending["C1"]
=============================================================================
