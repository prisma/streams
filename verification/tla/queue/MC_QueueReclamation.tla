------------------------ MODULE MC_QueueReclamation ------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-QR (QueueReclamation).  Baseline and     *)
(* witness cfgs load this module without overrides; each negative-control  *)
(* cfg substitutes exactly one baseline operator with a Mut* operator      *)
(* below (Op <- MutOp).  The numbers are S3b-DESIGN §11.2's.               *)
(***************************************************************************)
EXTENDS QueueReclamation

\* NC 1: retirement without the reference check: a chunk behind the frontier
\* is given back while a delivery still holds it.
MutUnreferenced(o) == TRUE

\* NC 2: retirement by coarse counts: only open deliveries are counted, so a
\* scheduled message's bit no longer holds its chunk.
MutUnreferencedCoarse(o) == o \notin (refs \ sched)

\* NC 3: purge_due acting on a Trimming row: a live queue's chunks below its
\* frontier reclaimed as if it were dead.
MutPurgeActsOn(s) == s \in {"Requested", "Purged", "Trimming"}

\* NC 4: a consolidation (or the cleaner) moving a queue chunk, counting the
\* old object down without reading 'k'.
MutQueueChunksMove == TRUE

\* NC 5: the trim reading the frontier after the references: an offset
\* leased between the two reads is given back.
MutFrontierAtCommit == TRUE

\* NC 6: the fold ignoring 'k': the index's view drops only what lies below
\* the prefix, so the folds keep released chunks above it and the split's
\* recount counts them again.
MutEntryDropped(o) == o < reclaimed

\* NC 6, by state: the view reading 'k' only while Trimming: a split between
\* the purge's request and its reclaim counts the released chunks again.
MutEntryDroppedTrimmingOnly(o) == o < reclaimed \/ (hstate = "Trimming" /\ o \in rel)

\* NC 7: ClearDebt without the generation: a pass clears the whole debt,
\* including what an ack recorded after the pass captured it.
MutClearByGeneration == FALSE

\* NC 8: the purge reading 'k' after its row deletes: every chunk the trim
\* released is decremented a second time.
MutKAtReclaim == TRUE

\* NC 9: the cleaner's commit reading the prefix and the 'H' state again but
\* the released intervals as its gather read them: a chunk the trim released
\* into 'k' meanwhile is moved, its old object counted down a second time.
MutStillLive(o) == alive /\ hstate \in {"none", "Trimming"} /\ ~(o < reclaimed)

\* NC 9, its prefix twin: the cleaner's commit reading 'k' again but not the
\* prefix: a chunk the trim released past `reclaimed` meanwhile is moved.
MutStillLivePrefix(o) == alive /\ hstate \in {"none", "Trimming"} /\ o \notin rel

\* NC 11: the purge's last step never taken: the purge stops in "rows", a
\* stuck state that is not Settled, so Terminated must not mask it.
MutReclaimLast == FALSE

\* NC 10: the cursor advancing past an unconsumed hint.
MutCursorAfter(taken) == nextHint
=============================================================================
