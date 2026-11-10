--------------------------- MODULE QueueReclamation ---------------------------
(***************************************************************************)
(* TLA-QR -- a native queue's sealed chunks given back (S3b part 2 step 3,  *)
(* S3b-DESIGN rev 3 §4.3, §5.9, §7.3, §7.4, §11.2).                        *)
(*                                                                         *)
(* One queue in one shard log, one chunk per offset (a queue's chunks tile *)
(* its offsets and never join).  A seal puts the queue's unsealed offsets  *)
(* into a new object; an object of `Shared` also holds another stream's    *)
(* live chunk for good, so it may never be deleted.                         *)
(*                                                                         *)
(* The queue's needed set N = {o < frontier : o referenced} u [frontier,   *)
(* next) only shrinks: the frontier moves on, leasing an offset (a         *)
(* reference) or passing it, and a reference resolves.  Every batch that   *)
(* moves the frontier over sealed offsets, resolves one, or seals, writes  *)
(* a hint naming them.                                                     *)
(*                                                                         *)
(* The trim pass (gc/trim.rs) runs in steps a committer batch can fall      *)
(* between: it takes `refs` and the hints after its cursor (at most        *)
(* PassHints), reads the frontier (`TrimStart`), then each candidate's      *)
(* references (`TrimCheck`, at most PassChunks), and commits one batch:    *)
(* the decrements, the prefix `reclaimed` and released offsets `rel` (the  *)
(* 'H' Trimming row and 'k'), the hints it read deleted, the unexamined     *)
(* hinted again, the cursor at the last hint it read.  A crash loses a     *)
(* pass in progress and nothing else.                                      *)
(*                                                                         *)
(* A delete fences the queue (nothing is needed any more); the purge then  *)
(* asks (Trimming -> Requested, the prefix kept), purges (head = the       *)
(* sealed end), and reclaims in F4's order: it reads the released offsets  *)
(* before it deletes the queue's rows, decrements every unreleased chunk   *)
(* in [reclaimed, head), and deletes 'k' with the last decrement.          *)
(*                                                                         *)
(* Reads pin the object of a needed sealed chunk while they run; the       *)
(* collector deletes an object at refcount zero that no read pins.         *)
(*                                                                         *)
(* Mutation points (MC_QueueReclamation.tla): Unreferenced (control 1),    *)
(* PurgeActsOn (3), FrontierAtCommit (5), KAtReclaim (8), CursorAfter      *)
(* (10).  Controls 2, 4, 6, 7 and 9 concern the cleaner, the folds' and    *)
(* recount's view of 'k', and the collector's debt: they come with part 2  *)
(* steps 4-5, where that code lands.                                       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    N,            \* offsets 0..N-1
    MaxObjs,      \* seals at most
    Shared,       \* objects that also hold another stream's live chunk
    PassHints,    \* hints one pass reads at most
    PassChunks,   \* chunks one pass examines at most
    MaxReads,     \* reads at once
    MaxCrashes,
    MaxHintId     \* hint ids drawn at most (a state constraint)

ASSUME /\ N \in Nat \ {0} /\ MaxObjs \in Nat \ {0} /\ Shared \subseteq 1..MaxObjs
       /\ PassHints \in Nat \ {0} /\ PassChunks \in Nat \ {0}
       /\ MaxReads \in Nat /\ MaxCrashes \in Nat /\ MaxHintId \in Nat

Offs == 0..(N-1)
Objs == 1..MaxObjs
NoPass == [phase |-> "none"]

VARIABLES
    next, frontier, refs, alive,   \* the committer's queue
    sealed, objOf, nobj,           \* seals: the boundary, each chunk's object
    R, deleted, pins, reads,       \* refcounts, deleted objects, read pins
    hstate, reclaimed, rel,        \* 'H' (state, prefix) and 'k' (released above it)
    hints, nextHint, cursor,       \* 'Z' and the trim's cursor
    tr,                            \* the trim pass in progress
    ph, phead, krel,               \* the purge: phase, head, the 'k' it read
    crashes

vars == <<next, frontier, refs, alive, sealed, objOf, nobj, R, deleted, pins,
          reads, hstate, reclaimed, rel, hints, nextHint, cursor, tr, ph,
          phead, krel, crashes>>

Needed(o) == alive /\ o < next /\ (o >= frontier \/ o \in refs)
Released(o) == o < reclaimed \/ o \in rel
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Neighbour(b) == IF b \in Shared THEN 1 ELSE 0
LiveOf(b) == {o \in 0..(sealed-1) : objOf[o] = b /\ ~Released(o)}

\* --- Mutation points (baseline definitions) --------------------------------
\* Retire a candidate only when nothing references it (trim.rs queue_refs_in).
Unreferenced(o) == o \notin refs
\* The purge acts on Requested and Purged rows, never on Trimming (§7.4).
PurgeActsOn(s) == s \in {"Requested", "Purged"}
\* The frontier is read at the pass's start, before the references (§4.3).
FrontierAtCommit == FALSE
\* The purge reads 'k' before it deletes the queue's rows (F4).
KAtReclaim == FALSE
\* The cursor stands at the last hint the pass read.
CursorAfter(taken) == Max({h[1] : h \in taken})

TypeOK ==
    /\ next \in 0..N /\ frontier \in 0..N /\ refs \subseteq Offs
    /\ sealed \in 0..next /\ objOf \in [Offs -> 0..MaxObjs] /\ nobj \in 0..MaxObjs
    /\ R \in [Objs -> Nat] /\ deleted \subseteq Objs /\ pins \in [Objs -> Nat]
    /\ hstate \in {"none", "Trimming", "Requested", "Purged"}
    /\ reclaimed \in 0..N /\ rel \subseteq Offs

Init ==
    /\ next = 0 /\ frontier = 0 /\ refs = {} /\ alive = TRUE
    /\ sealed = 0 /\ objOf = [o \in Offs |-> 0] /\ nobj = 0
    /\ R = [b \in Objs |-> Neighbour(b)] /\ deleted = {}
    /\ pins = [b \in Objs |-> 0] /\ reads = {}
    /\ hstate = "none" /\ reclaimed = 0 /\ rel = {}
    /\ hints = {} /\ nextHint = 1 /\ cursor = 0
    /\ tr = NoPass /\ ph = "none" /\ phead = 0 /\ krel = {}
    /\ crashes = 0

\* A hint naming [lo, hi) (nothing when empty).
Hint(lo, hi) ==
    IF lo < hi
    THEN /\ hints' = hints \cup {<<nextHint, lo, hi>>}
         /\ nextHint' = nextHint + 1
    ELSE UNCHANGED <<hints, nextHint>>

Gc == <<R, deleted, pins, reads, hstate, reclaimed, rel, cursor, tr, ph, phead,
        krel, crashes>>

\* ---------------------------------------------------------------------------
\* The committer.

Send ==
    /\ alive /\ next < N
    /\ next' = next + 1
    /\ UNCHANGED <<frontier, refs, alive, sealed, objOf, nobj, hints, nextHint>>
    /\ UNCHANGED Gc

Seal ==
    /\ alive /\ sealed < next /\ nobj < MaxObjs
    /\ \E s \in (sealed + 1)..next :
          LET b == nobj + 1 IN
          /\ objOf' = [o \in Offs |-> IF sealed <= o /\ o < s THEN b ELSE objOf[o]]
          /\ R' = [R EXCEPT ![b] = @ + (s - sealed)]
          /\ nobj' = b /\ sealed' = s
          /\ Hint(sealed, s)
    /\ UNCHANGED <<next, frontier, refs, alive, deleted, pins, reads, hstate,
                   reclaimed, rel, cursor, tr, ph, phead, krel, crashes>>

\* The frontier steps over its offset, leasing it (a reference) or passing it.
Step ==
    /\ alive /\ frontier < next
    /\ \E lease \in BOOLEAN :
          refs' = IF lease THEN refs \cup {frontier} ELSE refs
    /\ frontier' = frontier + 1
    /\ IF frontier < sealed THEN Hint(frontier, frontier + 1)
       ELSE UNCHANGED <<hints, nextHint>>
    /\ UNCHANGED <<next, alive, sealed, objOf, nobj>>
    /\ UNCHANGED Gc

\* A reference resolves (an ack, an expiry, a scheduled message's end).
Resolve(o) ==
    /\ alive /\ o \in refs
    /\ refs' = refs \ {o}
    /\ IF o < sealed THEN Hint(o, o + 1) ELSE UNCHANGED <<hints, nextHint>>
    /\ UNCHANGED <<next, frontier, alive, sealed, objOf, nobj>>
    /\ UNCHANGED Gc

\* The queue's delete fences it: nothing of it is needed any more.
Delete ==
    /\ alive
    /\ alive' = FALSE
    /\ UNCHANGED <<next, frontier, refs, sealed, objOf, nobj, hints, nextHint>>
    /\ UNCHANGED Gc

Committer == <<next, frontier, refs, alive, sealed, objOf, nobj, hints, nextHint>>

\* ---------------------------------------------------------------------------
\* The trim pass (gc/trim.rs), under `refs`: the purge cannot run beside it.

\* The lowest `k` of `ids` (all of them when fewer).
Lowest(ids, k) ==
    IF Cardinality(ids) <= k THEN ids
    ELSE CHOOSE c \in SUBSET ids : Cardinality(c) = k /\ \A x \in c, y \in ids \ c : x < y

TrimStart ==
    /\ alive /\ tr = NoPass /\ ph = "none" /\ hstate \in {"none", "Trimming"}
    /\ LET after == {h \in hints : h[1] > cursor}
           cut == Lowest({h[1] : h \in after}, PassHints)
           taken == {h \in after : h[1] \in cut}
           named == {o \in 0..(sealed-1) : \E h \in taken : h[2] <= o /\ o < h[3]}
           cands == {o \in named \cup ({reclaimed} \cap 0..(sealed-1)) : ~Released(o)}
       IN /\ after # {}
          /\ tr' = [phase |-> "check", fs |-> frontier, taken |-> taken,
                    todo |-> cands, done |-> 0, unref |-> {}, again |-> {}]
    /\ UNCHANGED <<ph, phead, krel>>
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, pins, reads, hstate, reclaimed, rel, cursor, crashes>>

\* The next candidate, in offset order: examined while the cap allows, its
\* references read now; else left for a new hint.
TrimCheck ==
    /\ tr.phase = "check" /\ tr.todo # {}
    /\ LET o == Min(tr.todo)
       IN IF tr.done >= PassChunks
          THEN tr' = [tr EXCEPT !.todo = @ \ {o}, !.again = @ \cup {o}]
          ELSE tr' = [tr EXCEPT !.todo = @ \ {o}, !.done = @ + 1,
                         !.unref = IF Unreferenced(o) THEN @ \cup {o} ELSE @]
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, pins, reads, hstate, reclaimed, rel, cursor, ph,
                   phead, krel, crashes>>

\* The prefix from `r` once the released offsets `k` are absorbed.
RECURSIVE Prefix(_, _)
Prefix(r, k) == IF r \in k THEN Prefix(r + 1, k) ELSE r

\* The retired: unreferenced when checked and behind the frontier the pass
\* read first (a broken FrontierAtCommit reads it now, after the
\* references).
Retired == {o \in tr.unref : o < IF FrontierAtCommit THEN frontier ELSE tr.fs}

TrimCommit ==
    /\ tr.phase = "check" /\ tr.todo = {}
    /\ LET k == rel \cup Retired
           r == Prefix(reclaimed, k)
           again == tr.again
       IN /\ R' = [b \in Objs |-> R[b] - Cardinality({o \in Retired : objOf[o] = b})]
          /\ reclaimed' = r
          /\ rel' = {o \in k : o >= r}
          /\ hstate' = IF Retired # {} THEN "Trimming" ELSE hstate
          /\ cursor' = CursorAfter(tr.taken)
          /\ IF again # {}
             THEN /\ hints' = (hints \ tr.taken) \cup {<<nextHint, Min(again), Max(again) + 1>>}
                  /\ nextHint' = nextHint + 1
             ELSE /\ hints' = hints \ tr.taken
                  /\ UNCHANGED nextHint
    /\ tr' = NoPass
    /\ UNCHANGED <<next, frontier, refs, alive, sealed, objOf, nobj, deleted, pins,
                   reads, ph, phead, krel, crashes>>

\* ---------------------------------------------------------------------------
\* The purge (§5.9; purge.rs), under `refs`: never beside a pass.

PurgeRequest ==
    /\ ~alive /\ ph = "none" /\ tr = NoPass
    /\ ph' = "asked"
    /\ hstate' = "Requested"
    /\ UNCHANGED <<phead, krel>>
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, pins, reads, reclaimed, rel, cursor, tr, crashes>>

\* Step 1 (the committer): the tail goes; `head` is the sealed end.
PurgeStep ==
    /\ ph = "asked" /\ tr = NoPass /\ PurgeActsOn(hstate)
    /\ ph' = "purged" /\ phead' = sealed /\ hstate' = "Purged"
    /\ UNCHANGED krel
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, pins, reads, reclaimed, rel, cursor, tr, crashes>>

\* A purge pass acting on a live queue's Trimming row (only a broken
\* PurgeActsOn allows it): it reclaims [reclaimed, frontier) as if dead.
PurgeOnTrimming ==
    /\ alive /\ tr = NoPass /\ ph = "none" /\ hstate = "Trimming"
    /\ PurgeActsOn(hstate)
    /\ LET due == {o \in reclaimed..(frontier-1) : o < sealed /\ o \notin rel}
       IN /\ R' = [b \in Objs |-> R[b] - Cardinality({o \in due : objOf[o] = b})]
          /\ reclaimed' = Max({reclaimed, frontier}) /\ rel' = {o \in rel : o >= frontier}
    /\ UNCHANGED <<hstate, cursor, tr, ph, phead, krel, deleted, pins, reads, crashes>>
    /\ UNCHANGED Committer

\* Reclaim, first batch: 'k' read (F4: before the row deletes), the queue's
\* rows deleted.
ReclaimRows ==
    /\ ph = "purged" /\ tr = NoPass
    /\ krel' = IF KAtReclaim THEN {} ELSE rel
    /\ ph' = "rows"
    /\ UNCHANGED <<phead>>
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, pins, reads, hstate, reclaimed, rel, cursor, tr, crashes>>

\* Reclaim, last batch: every chunk in [reclaimed, head) outside the 'k' it
\* read decremented; 'k' deleted with it; the prefix at head.
ReclaimDecrement ==
    /\ ph = "rows" /\ tr = NoPass
    /\ LET due == {o \in reclaimed..(phead-1) : o \notin krel}
       IN R' = [b \in Objs |-> R[b] - Cardinality({o \in due : objOf[o] = b})]
    /\ reclaimed' = Max({reclaimed, phead}) /\ rel' = {}
    /\ ph' = "done"
    /\ UNCHANGED <<phead, krel, hstate, cursor, tr, deleted, pins, reads, crashes>>
    /\ UNCHANGED Committer

\* ---------------------------------------------------------------------------
\* Reads and the collector.

ReadStart(o) ==
    /\ Cardinality(reads) < MaxReads
    /\ Needed(o) /\ o < sealed /\ objOf[o] \notin deleted
    /\ reads' = reads \cup {<<o, objOf[o]>>}
    /\ pins' = [pins EXCEPT ![objOf[o]] = @ + 1]
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, hstate, reclaimed, rel, cursor, tr, ph, phead, krel, crashes>>

ReadEnd(r) ==
    /\ r \in reads
    /\ reads' = reads \ {r}
    /\ pins' = [pins EXCEPT ![r[2]] = @ - 1]
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, hstate, reclaimed, rel, cursor, tr, ph, phead, krel, crashes>>

Collect(b) ==
    /\ b \in 1..nobj /\ b \notin deleted /\ R[b] = 0 /\ pins[b] = 0
    /\ deleted' = deleted \cup {b}
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, pins, reads, hstate, reclaimed, rel, cursor, tr, ph, phead, krel, crashes>>

\* A crash loses the pass in progress (its batch was never written).
Crash ==
    /\ crashes < MaxCrashes /\ tr # NoPass
    /\ tr' = NoPass /\ crashes' = crashes + 1
    /\ UNCHANGED Committer
    /\ UNCHANGED <<R, deleted, pins, reads, hstate, reclaimed, rel, cursor, ph, phead, krel>>

Next ==
    \/ Send \/ Seal \/ Step \/ Delete \/ \E o \in Offs : Resolve(o)
    \/ TrimStart \/ TrimCheck \/ TrimCommit
    \/ PurgeRequest \/ PurgeStep \/ PurgeOnTrimming \/ ReclaimRows \/ ReclaimDecrement
    \/ \E o \in Offs : ReadStart(o)
    \/ \E r \in reads : ReadEnd(r)
    \/ \E b \in Objs : Collect(b)
    \/ Crash

Spec == Init /\ [][Next]_vars

\* Bounds the hint ids a behaviour draws (each step, resolution, seal and
\* re-hint takes one).
HintBound == nextHint <= MaxHintId

\* ---------------------------------------------------------------------------
\* Invariants (S3b-DESIGN §11.2).

\* No chunk is given back while it is referenced or ahead of the frontier.
NoChunkTrimmedWhileReferencedOrAhead ==
    \A o \in 0..(sealed-1) : Released(o) => ~Needed(o)

\* Each live object's count is exactly its unreleased chunks plus another
\* stream's.
RefcountExact ==
    \A b \in 1..nobj : b \notin deleted => R[b] = Cardinality(LiveOf(b)) + Neighbour(b)

\* No object is deleted while a chunk of it is needed, another stream holds
\* it, or a read pins it.
NoDeleteWhileReferenced ==
    \A b \in deleted :
       /\ b \notin Shared /\ pins[b] = 0
       /\ \A o \in 0..(sealed-1) : objOf[o] = b => ~Needed(o)

\* Every running read's object is there.
ReadsServed == \A r \in reads : r[2] \notin deleted

\* The prefix stands at a chunk boundary within the sealed chunks.
ReclaimedIsABoundary == reclaimed <= sealed

\* The released offsets lie above the prefix, which has absorbed them.
IntervalsDisjointAndAboveReclaimed ==
    \A o \in rel : o > reclaimed

\* No hint at or below the cursor waits (the cursor never passes one).
CursorBehindEveryHint == \A h \in hints : h[1] > cursor

\* EveryHintHandled, as a safety property: a live queue's sealed chunk no
\* longer needed and not released is named by a hint after the cursor, or
\* is in the pass in progress.
EveryHintHandled ==
    \A o \in 0..(sealed-1) :
       (alive /\ ~Needed(o) /\ ~Released(o) /\ hstate \in {"none", "Trimming"}) =>
          \/ \E h \in hints : h[1] > cursor /\ h[2] <= o /\ o < h[3]
          \/ (tr # NoPass /\ \E h \in tr.taken : h[2] <= o /\ o < h[3])

\* ---------------------------------------------------------------------------
\* Witnesses: each must be violated, showing the behaviour is reachable.

Witness_ChunkRetired == ~(hstate = "Trimming" /\ reclaimed > 0)
Witness_IntervalAboveHeld == ~(rel # {})
Witness_SharedObjectKept == ~(\E b \in Shared : b <= nobj /\ R[b] = 1 /\ LiveOf(b) = {})
Witness_ObjectCollected == ~(deleted # {})
Witness_PurgeReclaimed == ~(ph = "done" /\ krel # {})
=============================================================================
