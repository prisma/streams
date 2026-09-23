--------------------------- MODULE MC_ReadCompose ---------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-018.  Baseline cfgs load this module     *)
(* without overrides; each negative-control cfg substitutes exactly one    *)
(* baseline operator with a Mut* operator below (Op <- MutOp).             *)
(***************************************************************************)
EXTENDS ReadCompose

\* Two routing keys; offset 1 belongs to the other key.
MCKeyOf == [o \in 0..2 |-> IF o = 1 THEN "K2" ELSE "K1"]
\* Offset 0 is larger than a whole requested page (first-record exception).
MCSize == [o \in 0..2 |-> IF o = 0 THEN 3 ELSE 1]

\* NC new-frontier/old-history-view: the history leg reads a history view
\* captured at page start while the race check adopts a newer boundary.
MutHistViewAtPageStart == rd.hsnap

\* NC advance-past-unreturned: the filtered tail scan is accepted without
\* consulting the absorbed boundary.
MutFilteredRaceNever(d, c) == FALSE

\* NC missing-postings-as-empty-success: a postings window that proves less
\* than the requested range is treated as complete.
MutShortIndexAccepted == TRUE

\* NC pre-fix TLA-018-F1 (before "An applied read revalidates its tail scan
\* at the level it scanned, so it never skips a durable record"): the race
\* check always reads the Remote-durable absorbed boundary, although an
\* applied scan runs at Memory level and sees applied trims.
MutRaceBoundaryRemote == VD.abs
=============================================================================
