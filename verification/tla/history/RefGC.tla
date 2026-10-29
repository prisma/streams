------------------------------- MODULE RefGC -------------------------------
(***************************************************************************)
(* TLA-019 (physical layer, layout 6A package 6): reference-counted        *)
(* garbage collection of segment objects (`src/shard/seal/gc.rs`,          *)
(* `gc/purge.rs`, `gc/sweep.rs`, `src/spill/gc.rs`), replacing ReachGC     *)
(* (history2's partition GC, retired with history2).                       *)
(*                                                                         *)
(* Two seal objects S1 = {a1, b1} and S2 = {a2, b2} hold chunks of streams *)
(* A and B; a consolidation object K = {ka} supersedes a1 and a2; a third  *)
(* seal object O = {b3} may be put by a writer that is then fenced.  Each  *)
(* object's 'R' row (rc, -1 when absent) counts its unreclaimed chunks;    *)
(* a decrement to zero deletes the row and writes a 'G' row stamped with   *)
(* the pin clock.                                                          *)
(*                                                                         *)
(* Actions: a seal's PUT and commit (refused for a purged stream), the     *)
(* consolidation's PUT and its commit under the refcount lock (re-check:   *)
(* its chunks are still the picks and its stream is not dead), a purge in  *)
(* two steps (the committer's purge, then the reclaim under the lock,      *)
(* which decrements the stream's maximal indexed chunks), a fold or merge  *)
(* dropping decremented entries, the collector's delete of a 'G' object    *)
(* (no running read started at or before its zeroing), a fence of the     *)
(* writer (its in-flight operations die), the orphan sweep (an object no   *)
(* row names, of a fenced opening or not in flight), and readers that      *)
(* register a pin, plan their picks from the index, and fetch.             *)
(*                                                                         *)
(* Time is abstracted: the purge delay (ASM-PURGE-DELAY) outlasts every   *)
(* read of a stream admitted before its death; the grace (ASM-HSEG-GRACE)  *)
(* covers reads outside this process and reads of a fenced opening, so a   *)
(* fence ends this                                                         *)
(* model's readers; within the process the pin alone protects a read.      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Readers, MaxClock

Streams == {"A", "B"}
SealObjs == {"S1", "S2", "O"}
Objs == SealObjs \cup {"K"}
Chunks == {"a1", "b1", "a2", "b2", "b3", "ka"}
Obj == [a1 |-> "S1", b1 |-> "S1", a2 |-> "S2", b2 |-> "S2", b3 |-> "O", ka |-> "K"]
Strm == [a1 |-> "A", b1 |-> "B", a2 |-> "A", b2 |-> "B", b3 |-> "B", ka |-> "A"]
\* The chunks a chunk supersedes (a consolidation copies whole chunks).
Covers(c) == IF c = "ka" THEN {"a1", "a2"} ELSE {}
In(o) == {c \in Chunks : Obj[c] = o}
\* Each reader reads one stream.
RStream(r) == IF r = 1 THEN "A" ELSE "B"

VARIABLES
    stored,     \* the object is in the store
    committed,  \* its commit landed (its 'K' and 'R' rows)
    inflight,   \* its operation runs in the current opening (a claim)
    epochOf,    \* the opening that put it
    epoch,      \* the current opening
    rc,         \* its 'R' row: unreclaimed chunks, -1 when absent
    garb,       \* its 'G' row
    zeroAt,     \* the pin clock when its count reached zero
    clock,      \* the pin clock
    indexed,    \* chunks the index (directory, run-index objects) lists
    dead,       \* per stream: live, requested, purged, reclaimed
    rd,         \* per reader: phase, start, plan
    swept       \* objects the orphan sweep deleted (history)

vars == <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock,
          indexed, dead, rd, swept>>

Superseded(c) == \E d \in Chunks : committed[Obj[d]] /\ c \in Covers(d)
Reclaimed(c) == dead[Strm[c]] = "reclaimed"
\* What an 'R' row must count: committed, neither superseded nor reclaimed.
Counted(o) == {c \in In(o) : committed[o] /\ ~Superseded(c) /\ ~Reclaimed(c)}

\* The maximal indexed chunks of `s`: what a reader picks and what a
\* reclaim decrements (`spill::gc::live_entries`).
Maximal(s) == {c \in indexed : Strm[c] = s /\ ~\E d \in indexed : c \in Covers(d)}

\* ---- hooks the negative controls substitute ----
\* A reader's picks.
Picks(s) == Maximal(s)
\* The collector may delete `o`: no running read started at or before its
\* zeroing.
PinOK(o) == \A r \in Readers : rd[r].phase \in {"started", "planned"} => rd[r].start > zeroAt[o]
\* The consolidation's re-check: its chunks are still the picks and its
\* stream is not dead.
ConsAdmits == Maximal("A") \cap {"a1", "a2"} = {"a1", "a2"} /\ dead["A"] = "live"
\* The committer admits a seal's chunk only for a stream not purged.
SealAdmits(o) == \A c \in In(o) : dead[Strm[c]] \in {"live", "requested"}
\* The sweep may delete an unnamed object: of a fenced opening, or not in
\* flight.
SweepEligible(o) == epochOf[o] < epoch \/ o \notin inflight

TypeOK ==
    /\ stored \in [Objs -> BOOLEAN]
    /\ committed \in [Objs -> BOOLEAN]
    /\ inflight \subseteq Objs
    /\ epochOf \in [Objs -> 0..1]
    /\ epoch \in 0..1
    /\ rc \in [Objs -> -1..2]
    /\ garb \in [Objs -> BOOLEAN]
    /\ zeroAt \in [Objs -> 0..MaxClock]
    /\ clock \in 0..MaxClock
    /\ indexed \subseteq Chunks
    /\ dead \in [Streams -> {"live", "requested", "purged", "reclaimed"}]
    /\ swept \subseteq Objs

Init ==
    /\ stored = [o \in Objs |-> FALSE]
    /\ committed = [o \in Objs |-> FALSE]
    /\ inflight = {}
    /\ epochOf = [o \in Objs |-> 0]
    /\ epoch = 0
    /\ rc = [o \in Objs |-> -1]
    /\ garb = [o \in Objs |-> FALSE]
    /\ zeroAt = [o \in Objs |-> 0]
    /\ clock = 0
    /\ indexed = {}
    /\ dead = [s \in Streams |-> "live"]
    /\ rd = [r \in Readers |-> [phase |-> "idle", start |-> 0, plan |-> {}]]
    /\ swept = {}

\* Decrement `by[o]` chunks from each object's row; a row reaching zero is
\* deleted with a 'G' row stamped with the clock, and the clock advances.
Decrement(by) ==
    LET zero == {o \in Objs : rc[o] > 0 /\ rc[o] - by[o] = 0 /\ by[o] > 0}
    IN /\ rc' = [o \in Objs |-> IF o \in zero THEN -1 ELSE rc[o] - by[o]]
       /\ garb' = [o \in Objs |-> garb[o] \/ o \in zero]
       /\ zeroAt' = [o \in Objs |-> IF o \in zero THEN clock ELSE zeroAt[o]]
       /\ clock' = IF zero = {} THEN clock ELSE clock + 1

Put(o) ==
    /\ o \in SealObjs \/ (o = "K" /\ {"a1", "a2"} \subseteq indexed)
    /\ ~stored[o] /\ ~committed[o] /\ o \notin swept
    /\ stored' = [stored EXCEPT ![o] = TRUE]
    /\ inflight' = inflight \cup {o}
    /\ epochOf' = [epochOf EXCEPT ![o] = epoch]
    /\ UNCHANGED <<committed, epoch, rc, garb, zeroAt, clock, indexed, dead, rd, swept>>

\* A seal's commit: the committer stages it whether or not the object is
\* still stored (it never reads the store).
SealCommit(o) ==
    /\ o \in SealObjs /\ o \in inflight /\ epochOf[o] = epoch
    /\ SealAdmits(o)
    /\ committed' = [committed EXCEPT ![o] = TRUE]
    /\ rc' = [rc EXCEPT ![o] = Cardinality(In(o))]
    /\ indexed' = indexed \cup In(o)
    /\ inflight' = inflight \ {o}
    /\ UNCHANGED <<stored, epochOf, epoch, garb, zeroAt, clock, dead, rd, swept>>

\* An operation that fails after its PUT (refused, a failed batch): its
\* claim drops and its object is an orphan.
Abort(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ UNCHANGED <<stored, committed, epochOf, epoch, rc, garb, zeroAt, clock, indexed, dead, rd, swept>>

\* The consolidation's commit, under the refcount lock (atomic here).
ConsCommit ==
    /\ "K" \in inflight /\ epochOf["K"] = epoch
    /\ ConsAdmits
    /\ committed' = [committed EXCEPT !["K"] = TRUE]
    /\ indexed' = indexed \cup {"ka"}
    /\ inflight' = inflight \ {"K"}
    /\ LET by == [o \in Objs |-> Cardinality({c \in Covers("ka") : Obj[c] = o})]
       IN /\ \A o \in Objs : by[o] > 0 => rc[o] >= by[o]
          /\ rc' = [o \in Objs |-> IF o = "K" THEN 1
                                   ELSE IF rc[o] - by[o] = 0 /\ by[o] > 0 THEN -1
                                   ELSE rc[o] - by[o]]
          /\ garb' = [o \in Objs |-> garb[o] \/ (o # "K" /\ by[o] > 0 /\ rc[o] - by[o] = 0)]
          /\ zeroAt' = [o \in Objs |-> IF o # "K" /\ by[o] > 0 /\ rc[o] - by[o] = 0
                                       THEN clock ELSE zeroAt[o]]
          /\ clock' = IF \E o \in Objs : o # "K" /\ by[o] > 0 /\ rc[o] - by[o] = 0
                      THEN clock + 1 ELSE clock
    /\ UNCHANGED <<stored, epochOf, epoch, dead, rd, swept>>

\* A dead incarnation (deleted or replaced: irrevocable).
Request(s) ==
    /\ dead[s] = "live"
    /\ dead' = [dead EXCEPT ![s] = "requested"]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, rd, swept>>

\* The purge waits its delay past the close (ASM-PURGE-DELAY): a read of
\* the stream admitted before its death is over by then.
PurgeStep(s) ==
    /\ dead[s] = "requested"
    /\ \A r \in Readers : RStream(r) = s => rd[r].phase \in {"idle", "done", "failed"}
    /\ dead' = [dead EXCEPT ![s] = "purged"]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, rd, swept>>

\* The reclaim, under the refcount lock: every maximal indexed chunk of the
\* stream decremented once.
Reclaim(s) ==
    /\ dead[s] = "purged"
    /\ LET by == [o \in Objs |-> Cardinality({c \in Maximal(s) : Obj[c] = o})]
       IN /\ \A o \in Objs : by[o] > 0 => rc[o] >= by[o]
          /\ Decrement(by)
    /\ dead' = [dead EXCEPT ![s] = "reclaimed"]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, indexed, rd, swept>>

\* A fold or merge drops an entry already decremented (covered by an
\* indexed consolidated chunk, or of a reclaimed stream).
Drop(c) ==
    /\ c \in indexed
    /\ (\E d \in indexed : c \in Covers(d)) \/ Reclaimed(c)
    /\ indexed' = indexed \ {c}
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, dead, rd, swept>>

Collect(o) ==
    /\ garb[o] /\ PinOK(o)
    /\ stored' = [stored EXCEPT ![o] = FALSE]
    /\ garb' = [garb EXCEPT ![o] = FALSE]
    /\ UNCHANGED <<committed, inflight, epochOf, epoch, rc, zeroAt, clock, indexed, dead, rd, swept>>

\* A new owner fences the writer: its in-flight operations die, and its
\* in-process readers are over before the new owner's grace runs out.
Fence ==
    /\ epoch = 0
    /\ epoch' = 1
    /\ inflight' = {}
    /\ rd' = [r \in Readers |-> [phase |-> "idle", start |-> 0, plan |-> {}]]
    /\ UNCHANGED <<stored, committed, epochOf, rc, garb, zeroAt, clock, indexed, dead, swept>>

Sweep(o) ==
    /\ stored[o] /\ rc[o] = -1 /\ ~garb[o]
    /\ SweepEligible(o)
    /\ stored' = [stored EXCEPT ![o] = FALSE]
    /\ swept' = swept \cup {o}
    /\ UNCHANGED <<committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, dead, rd>>

RStart(r) ==
    /\ rd[r].phase = "idle" /\ dead[RStream(r)] = "live"
    /\ rd' = [rd EXCEPT ![r] = [phase |-> "started", start |-> clock, plan |-> {}]]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, dead, swept>>

RPlan(r) ==
    /\ rd[r].phase = "started"
    /\ rd' = [rd EXCEPT ![r].phase = "planned", ![r].plan = Picks(RStream(r))]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, dead, swept>>

\* The fetch: a chunk whose object is gone answers an error (never a short
\* success); the read is then over, and the reader may read again.
RFetch(r) ==
    /\ rd[r].phase = "planned"
    /\ rd' = [rd EXCEPT ![r].phase = IF \A c \in rd[r].plan : stored[Obj[c]]
                                     THEN "done" ELSE "failed"]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, dead, swept>>

RAgain(r) ==
    /\ rd[r].phase = "done"
    /\ rd' = [rd EXCEPT ![r] = [phase |-> "idle", start |-> 0, plan |-> {}]]
    /\ UNCHANGED <<stored, committed, inflight, epochOf, epoch, rc, garb, zeroAt, clock, indexed, dead, swept>>

Next ==
    \/ \E o \in Objs : Put(o) \/ SealCommit(o) \/ Abort(o) \/ Collect(o) \/ Sweep(o)
    \/ ConsCommit
    \/ \E s \in Streams : Request(s) \/ PurgeStep(s) \/ Reclaim(s)
    \/ \E c \in Chunks : Drop(c)
    \/ Fence
    \/ \E r \in Readers : RStart(r) \/ RPlan(r) \/ RFetch(r) \/ RAgain(r)

Spec == Init /\ [][Next]_vars

\* ---- properties ----
\* Every 'R' row counts exactly its object's committed chunks that are
\* neither superseded nor reclaimed (absent when there are none).
RefcountExact ==
    \A o \in Objs : rc[o] = IF Counted(o) = {} THEN -1 ELSE Cardinality(Counted(o))

\* An object with a counted chunk is never deleted.
NoDeleteWhileReferenced == \A o \in Objs : Counted(o) # {} => stored[o]

\* Only an object with no counted chunk awaits its delete.
GarbageUnreferenced == \A o \in Objs : garb[o] => Counted(o) = {}

\* A read never meets a deleted object (with NoFalseCompleteRead in the
\* code: a fetch of a deleted object answers an error, never a short page).
ReadsServed == \A r \in Readers : rd[r].phase # "failed"

\* ---- witnesses (expected violated on the unmodified model) ----
Witness_GarbageDeleted == ~\E o \in Objs : committed[o] /\ ~stored[o] /\ o \notin swept
Witness_OrphanSwept == swept = {}
Witness_FencedOrphanSwept == ~\E o \in swept : epochOf[o] < epoch
Witness_PurgeReclaimed == ~\E s \in Streams : dead[s] = "reclaimed"
Witness_PinnedReadServed ==
    ~\E r \in Readers : rd[r].phase = "done" /\ \E c \in rd[r].plan : garb[Obj[c]]
=============================================================================
