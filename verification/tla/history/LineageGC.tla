----------------------------- MODULE LineageGC -----------------------------
(***************************************************************************)
(* TLA-019, two-holder arm (S6-DESIGN rev 6 §3-§4, §8): objects of a       *)
(* retired home A that the leaves after it hold, their releases, the       *)
(* merge of two holders and the lineage sweeper (`src/shard/seal/gc.rs`    *)
(* stage_unreferenced, `gc/release.rs`, `gc/lineage.rs`,                   *)
(* `src/spill/lineage.rs`).                                                *)
(*                                                                         *)
(* A split left two leaves C0 and C1 holding A's objects: C0 holds a chunk *)
(* of o1 and of o2, C1 a chunk of o2 (a shared object). Each leaf's 'R'    *)
(* row counts its own chunks (L2) and its reference list under A names     *)
(* what it held at birth (L5). A decrement to zero writes an 'E' row       *)
(* stamped with the leaf's pin clock; the collector logs a release once no *)
(* read of that leaf started at or before the zeroing; a leaf holding      *)
(* nothing of A writes its released-all marker. C0 and C1 may merge into   *)
(* M: M's count is the sum, its list is written before the CAS that makes  *)
(* it a leaf, the sources retire with their in-flight reads, and the       *)
(* topology version restarts its grace. The sweeper deletes an object of A *)
(* that no leaf with a list holds (list less logged releases, nothing once *)
(* released-all), and only at a version whose grace passed: the grace      *)
(* outlasts every read of a retired leaf (ASM-HSEG-GRACE), which is        *)
(* abstracted as `Age` waiting for those reads to end.                     *)
(*                                                                         *)
(* `live` is the ground truth: chunks of the object a current leaf has not *)
(* decremented. Placement is abstract (ASM-ROUTE-PREFIX: a row's leading   *)
(* 24 bits are its segment route's; discharged by KANI-106).               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS MaxClock

Holders == {"C0", "C1", "M"}
Sources == {"C0", "C1"}
Objs == {"o1", "o2"}
\* The chunks each source holds at birth.
Born == [h \in Holders |-> [o \in Objs |->
            IF h = "C0" THEN 1 ELSE IF h = "C1" /\ o = "o2" THEN 1 ELSE 0]]

VARIABLES
    stored,   \* the object is in the store under A
    live,     \* chunks of it no current leaf decremented (ground truth)
    rc,       \* per holder, its 'R' row's count
    leaf,     \* the holder is a leaf of the current topology version
    listed,   \* per holder, its reference list under A (absent: hasList false)
    hasList,
    pending,  \* per holder, its 'E' rows
    zeroAt,   \* per holder and object, its pin clock at the zeroing
    clock,    \* per holder, its pin clock
    logged,   \* per holder, the objects its release logs name
    all,      \* per holder, its released-all marker
    rd,       \* per holder, its reader: phase, object, start
    aged      \* the current version's grace passed

vars == <<stored, live, rc, leaf, listed, hasList, pending, zeroAt, clock,
          logged, all, rd, aged>>

\* ---- operators the negative controls substitute ----
\* The holders the sweeper reads: every leaf with a list.
SweepHolders == {h \in Holders : leaf[h] /\ hasList[h]}
\* A decrement that leaves `n` chunks stages a release when the row is gone.
StagesRelease(n) == n = 0
\* The sweeper waits for the version's grace.
GraceOK == aged
\* The merged leaf's count: the sources' sum.
MergedCount(o) == rc["C0"][o] + rc["C1"][o]
\* The merge writes M's list before its CAS.
ListAtCas == TRUE
\* The merge writes a list for M at all.
ListWritten == TRUE
\* The sources' 'E' rows the merged leaf carries.
MergedPending == {}

Init ==
    /\ stored = [o \in Objs |-> TRUE]
    /\ live = [o \in Objs |-> Born["C0"][o] + Born["C1"][o]]
    /\ rc = Born
    /\ leaf = [h \in Holders |-> h \in Sources]
    /\ listed = [h \in Holders |-> {o \in Objs : Born[h][o] > 0}]
    /\ hasList = [h \in Holders |-> h \in Sources]
    /\ pending = [h \in Holders |-> {}]
    /\ zeroAt = [h \in Holders |-> [o \in Objs |-> 0]]
    /\ clock = [h \in Holders |-> 0]
    /\ logged = [h \in Holders |-> {}]
    /\ all = [h \in Holders |-> FALSE]
    /\ rd = [h \in Holders |-> [phase |-> "idle", obj |-> "o1", start |-> 0]]
    /\ aged = TRUE

\* A leaf's read plans with an object it references, under its pin; the
\* open-time detector (L5) keeps a leaf without its list from serving.
ReadStart(h, o) ==
    /\ leaf[h] /\ hasList[h] /\ rc[h][o] > 0 /\ rd[h].phase = "idle"
    /\ rd' = [rd EXCEPT ![h] = [phase |-> "reading", obj |-> o, start |-> clock[h]]]
    /\ UNCHANGED <<stored, live, rc, leaf, listed, hasList, pending, zeroAt, clock,
                   logged, all, aged>>

\* The read fetches its object and ends (a deleted object fails it).
ReadEnd(h) ==
    /\ rd[h].phase = "reading"
    /\ rd' = [rd EXCEPT ![h] = [@ EXCEPT !.phase = IF stored[rd[h].obj] THEN "idle" ELSE "failed"]]
    /\ UNCHANGED <<stored, live, rc, leaf, listed, hasList, pending, zeroAt, clock,
                   logged, all, aged>>

\* A purge reclaim, consolidation or cleaner commit decrements one chunk
\* (stage_decrements); at zero the row goes and an 'E' row is staged.
Dec(h, o) ==
    /\ leaf[h] /\ rc[h][o] > 0 /\ clock[h] < MaxClock
    /\ rc' = [rc EXCEPT ![h][o] = @ - 1]
    /\ live' = [live EXCEPT ![o] = @ - 1]
    /\ IF StagesRelease(rc[h][o] - 1)
       THEN /\ pending' = [pending EXCEPT ![h] = @ \cup {o}]
            /\ zeroAt' = [zeroAt EXCEPT ![h][o] = clock[h]]
            /\ clock' = [clock EXCEPT ![h] = @ + 1]
       ELSE UNCHANGED <<pending, zeroAt, clock>>
    /\ UNCHANGED <<stored, leaf, listed, hasList, logged, all, rd, aged>>

\* The collector logs a due release once no read of this holder that may
\* hold the object runs (its pin: started at or before the zeroing).
Log(h, o) ==
    /\ leaf[h] /\ o \in pending[h]
    /\ ~(rd[h].phase = "reading" /\ rd[h].obj = o /\ rd[h].start <= zeroAt[h][o])
    /\ logged' = [logged EXCEPT ![h] = @ \cup {o}]
    /\ pending' = [pending EXCEPT ![h] = @ \ {o}]
    /\ UNCHANGED <<stored, live, rc, leaf, listed, hasList, zeroAt, clock, all, rd, aged>>

\* A leaf with no 'R' or 'E' row of A marks itself released-all.
MarkAll(h) ==
    /\ leaf[h] /\ hasList[h] /\ ~all[h]
    /\ \A o \in Objs : rc[h][o] = 0
    /\ pending[h] = {}
    /\ all' = [all EXCEPT ![h] = TRUE]
    /\ UNCHANGED <<stored, live, rc, leaf, listed, hasList, pending, zeroAt, clock,
                   logged, rd, aged>>

\* C0 and C1 merge into M (§7): the counts sum, the sources' 'E' rows are
\* dropped (an object a source zeroed is in M's holdings only through the
\* other source's count; otherwise the sweeper deletes it one grace after
\* the merge's version), M's list (what it holds) is written before the CAS,
\* the sources retire with their in-flight reads, the version's grace
\* restarts.
Merge ==
    /\ leaf["C0"] /\ leaf["C1"]
    /\ rc' = [rc EXCEPT !["M"] = [o \in Objs |-> MergedCount(o)]]
    /\ pending' = [pending EXCEPT !["M"] = MergedPending]
    /\ listed' = [listed EXCEPT !["M"] = {o \in Objs : MergedCount(o) > 0}]
    /\ hasList' = [hasList EXCEPT !["M"] = ListWritten /\ ListAtCas]
    /\ leaf' = [h \in Holders |-> h = "M"]
    /\ aged' = FALSE
    /\ UNCHANGED <<stored, live, zeroAt, clock, logged, all, rd>>

\* A late list (the control writing it after the CAS).
LateList ==
    /\ leaf["M"] /\ ~hasList["M"] /\ ListWritten
    /\ hasList' = [hasList EXCEPT !["M"] = TRUE]
    /\ UNCHANGED <<stored, live, rc, leaf, listed, pending, zeroAt, clock, logged,
                   all, rd, aged>>

\* The version's grace passes once every retired leaf's read has ended
\* (ASM-HSEG-GRACE).
Age ==
    /\ ~aged
    /\ \A h \in Holders : ~leaf[h] => rd[h].phase # "reading"
    /\ aged' = TRUE
    /\ UNCHANGED <<stored, live, rc, leaf, listed, hasList, pending, zeroAt, clock,
                   logged, all, rd>>

Held(o) ==
    \E h \in SweepHolders : ~all[h] /\ o \in listed[h] /\ o \notin logged[h]

\* The lineage sweeper deletes an object no holder still holds.
Sweep(o) ==
    /\ stored[o] /\ GraceOK /\ ~Held(o)
    /\ stored' = [stored EXCEPT ![o] = FALSE]
    /\ UNCHANGED <<live, rc, leaf, listed, hasList, pending, zeroAt, clock, logged,
                   all, rd, aged>>

Next ==
    \/ \E h \in Holders, o \in Objs : ReadStart(h, o) \/ Dec(h, o) \/ Log(h, o)
    \/ \E h \in Holders : ReadEnd(h) \/ MarkAll(h)
    \/ Merge \/ LateList \/ Age
    \/ \E o \in Objs : Sweep(o)

Spec == Init /\ [][Next]_vars

\* ---- properties ----
TypeOK ==
    /\ stored \in [Objs -> BOOLEAN]
    /\ live \in [Objs -> 0..2]
    /\ rc \in [Holders -> [Objs -> 0..2]]
    /\ \A h \in Holders : pending[h] \subseteq Objs /\ logged[h] \subseteq Objs

\* Every current leaf's counts sum to the chunks still live (L2 at a merge).
RefcountExact == \A o \in Objs : live[o] = IF leaf["M"] THEN rc["M"][o]
                                           ELSE rc["C0"][o] + rc["C1"][o]

\* An object a current leaf still has a chunk of is never deleted.
NoDeleteWhileReferenced == \A o \in Objs : live[o] > 0 => stored[o]

\* Only a zero count stages a release.
GarbageUnreferenced == \A h \in Holders, o \in Objs : o \in pending[h] => rc[h][o] = 0

\* A read never meets a deleted object.
ReadsServed == \A h \in Holders : rd[h].phase # "failed"

\* A leaf with a positive count under A has its list there.
HolderListed == \A h \in Holders, o \in Objs : leaf[h] /\ rc[h][o] > 0 => hasList[h]

\* ---- witnesses (expected violated on the unmodified model) ----
\* o2, shared by both sources, is deleted after both released it.
Witness_AncestorDeleted == stored["o2"]
\* Every object of A deleted after a merge.
Witness_LineageDrained == ~(leaf["M"] /\ \A o \in Objs : ~stored[o])
=============================================================================
