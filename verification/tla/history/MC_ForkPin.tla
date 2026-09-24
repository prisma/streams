----------------------------- MODULE MC_ForkPin -----------------------------
(* Model-checking wrapper for the TLA-019 fork-pin layer.  Baselines load  *)
(* this module unmodified; each negative control substitutes one operator. *)
EXTENDS ForkPin

\* Three child incarnations: F3 recreates F1's name after F1's tombstone.
MCChildren == {"F1", "F2", "F3"}
MCPrev == [c \in MCChildren |-> IF c = "F3" THEN "F1" ELSE "-"]
\* Two child incarnations under distinct names (no child-name recreation).
MCChildren2 == {"F1", "F2"}
MCPrev2 == [c \in MCChildren2 |-> "-"]
\* Two child incarnations of ONE name: F3 recreates F1's name.
MCChildrenR == {"F1", "F3"}
MCPrevR == [c \in MCChildrenR |-> IF c = "F3" THEN "F1" ELSE "-"]

\* NC fork pin: the source is tombstoned regardless of live fork references.
MutDeleteIgnoresRefs(hasRef) == "deleted"

\* NC incarnation fence: anchor::install's CAS is not bound to the
\* incarnation the child forked, so a delayed install lands on a recreated
\* source (the child then anchors to a replacement it never forked).
MutInstallIgnoresIncarnation(c) == TRUE

\* NC write-ahead marker: the child's tombstone is written without the
\* fork-debt index marker ahead of it.
MutNoWriteAhead(c) == TRUE

\* NC reconciler defers nothing: a marker whose child is still alive is
\* paid from the marker as if the name had been recreated.
MutMarkerViewNoDefer(c) ==
    IF Replaced(c) THEN "replaced"
    ELSE IF child[c] = "gone" THEN "tombstone" ELSE "replaced"

\* NC release by the name's current fork id: the replaced-name branch
\* releases the reference of the incarnation that now holds the name.
MutReleaseCurrentNameId(c) ==
    IF Replaced(c) THEN CHOOSE d \in Children : Prev[d] = c ELSE c

\* NC marker dropped on an inconclusive release.
MutSettleAlways(conclusive) == TRUE

\* NC the one-time backfill never runs.
MutNoBackfill == FALSE

\* NC pre-fix recreation (before the recreate CAS indexed the fork debt of
\* the tombstone it overwrites): recreating a child's name overwrites a
\* debt-bearing tombstone the backfill has not indexed, and the debt is lost.
MutNoIndexOverwritten == FALSE

\* NC no reconciler: the reconciler task never runs (no fairness on it).
NoReconciler == TRUE
=============================================================================
