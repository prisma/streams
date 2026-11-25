---------------------------- MODULE RingMembership ----------------------------
(***************************************************************************)
(* TLA-049 -- Ring membership moves nothing (§22, GATE-DECISION-s22-       *)
(* addendum-2026-11-19 §7, rev 2's K10).                                   *)
(*                                                                          *)
(* Three instances n1..n3 (ordinals 1..3), two leaves p and q, a fixed     *)
(* rendezvous order per leaf (ring_pick: a leaf goes to the first          *)
(* instance of its order that is in the ring, so a subset's pick is the    *)
(* full ring's pick whenever that instance is in the subset).             *)
(*                                                                          *)
(* The overrides document (one object, read-after-write, CAS): `members`   *)
(* (the ring of record, C1) and one entry per leaf naming an instance, or  *)
(* none. An entry naming an instance outside a reader's ring is void.     *)
(* The desired count is a separate object; instance i is "counted" iff     *)
(* i <= count.                                                             *)
(*                                                                          *)
(* Each instance publishes its own view: a ring and the entries of ONE     *)
(* document read (C2). Its ring is listed /\ qualified (C1): listed by     *)
(* that document, and live by the instance's own beat reads, itself by     *)
(* its listing alone once booted (C13). The tick's pass reads beats        *)
(* (ReadBeats: every listed and counted instance is a named read, C15),    *)
(* then the count and the document, and publishes (Publish). A refresh    *)
(* (a forward's version header, view::refresh_for) and a handoff's mirror  *)
(* publish a newer document over the candidate set of the last pass.      *)
(* The membership task's CASes (pin, no-op drop, join, release) are made   *)
(* against the document of the pass that computed them; a CAS against a   *)
(* changed document fails and leaves nothing, so each is modelled as one   *)
(* step with that pass's publication (C5, C16).                            *)
(*                                                                          *)
(* An engine is abstract: open or not. An instance opens a leaf only when  *)
(* its own view gives it that leaf (open_viewed), it is not handing that   *)
(* leaf off, and it is not sealed (C10). A view that gives a leaf away     *)
(* closes the engine at the next tick ("possession yields at tick").     *)
(*                                                                          *)
(* A planned move (src/fleet/handoff.rs) is giver first: MoveStart takes   *)
(* the target's move-in slot (K3) and holds the leaf, MoveClose closes the *)
(* giver's engine (cold: nothing to close, the engine-less move, C9),      *)
(* MoveCas writes the entry (or removes it when the target is the leaf's   *)
(* ring home: Change::Return) only while the document still gives the     *)
(* leaf to the giver, and mirrors; MoveEnd ends the hold and frees the    *)
(* slot. Abstraction line (K10): a move ends at the target's Ready, or at *)
(* its hold's lapse.                                                       *)
(*                                                                          *)
(* Flagged steps (each excuses every leaf from then on, in `excused`):     *)
(* Crash, Misjudge (a live instance judged dead), JoinFallback (JOIN_PHASE *)
(* past: join with something uncovered), LeaveFallback (LEAVE_PHASE past: *)
(* release while holding), DeadRemoval of a live instance, and an in-place *)
(* restart's return. A planned drain (G1, C12) is no flag.                 *)
(*                                                                          *)
(* MembershipMovesNothing: no instance has an engine open for a leaf while *)
(* its own view gives that leaf to another, or another live instance's own *)
(* view gives it to that instance, unless a flagged step touched the leaf.*)
(*                                                                          *)
(* Mutation points: Design "beat" is rev 1 (membership claimed in a beat   *)
(* field, pin_for, pins dropped past the count, today's no-op drop);       *)
(* CoverageGuard FALSE is today's ring change; ThirdPartyNoOp lets anyone  *)
(* drop a no-op entry; RefreshKeepsRing pairs a newer document with the   *)
(* last published ring (CE4); SealOnRelease FALSE releases with no seal.  *)
(* RingRule "evidence" disqualifies a listed instance only on evidence: a  *)
(* beat read stale, draining or withdrawn; an absent or unread beat of a   *)
(* listed instance counts live (a rule the ruling does not state; see the  *)
(* obligation's scope). "ruled" is the ruling's listed /\ qualified.       *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Design,            \* "record" (the ruling) | "beat" (rev 1)
    RingRule,          \* "evidence" (R1, by incarnation) | "name" (evidence by name) | "ruled"
    CoverRule,         \* "ruled" | "owners"
    DrainNoJoin,       \* TRUE: a draining instance never joins
    AnnounceAfterRemoval, \* TRUE: a drain announces only once its removal is read back
    CoverageGuard,     \* FALSE: a joiner joins with nothing checked (today's ring change)
    ThirdPartyNoOp,    \* TRUE: any listed instance drops a no-op entry naming another
    NoOpCounted,       \* FALSE: the no-op test is over the ring alone (today's moves.rs)
    RefreshKeepsRing,  \* TRUE: a refresh or mirror keeps the last published ring (CE4)
    SealOnRelease,     \* FALSE: the release's check does not stop opens
    SlotOn,            \* TRUE: K3's move-in slot (step 3b, not yet code); FALSE: the code's moves
    InitCount,         \* desired count at start
    InitUp,            \* instances running and listed at start
    InitEntryQ,        \* "none" or the instance an entry for q names at start
    MaxCountChanges,
    MaxMoves,          \* planned moves by members within the count (a leaver's are free)
    MaxCrashes,
    MaxRestarts,       \* restarts after a crash or a drain's stop (M2)
    MaxMisjudges,
    JoinFallbackOn,
    LeaveFallbackOn,
    DeadRemovalOn,
    DrainOn

Nodes == {"n1", "n2", "n3"}
Leaves == {"p", "q"}
None == "none"
Ord(n) == CASE n = "n1" -> 1 [] n = "n2" -> 2 [] n = "n3" -> 3
Order(x) == IF x = "p" THEN <<"n1", "n3", "n2">> ELSE <<"n3", "n2", "n1">>
Pos(x, n) == CHOOSE i \in 1..3 : Order(x)[i] = n
Home(x, R) == IF R = {} THEN None ELSE CHOOSE n \in R : \A m \in R : Pos(x, n) <= Pos(x, m)

ASSUME /\ Design \in {"record", "beat"} /\ RingRule \in {"ruled", "name", "evidence"}
       /\ CoverRule \in {"ruled", "owners"}
       /\ InitCount \in 1..3 /\ InitUp \subseteq Nodes /\ InitEntryQ \in Nodes \cup {None}

NoEntry == [to |-> None, pin |-> None]
NoHand == [x |-> None, t |-> None, st |-> None]
NoView == [ok |-> FALSE, ring |-> {}, entries |-> [y \in Leaves |-> NoEntry]]

OwnerOf(R, E, x) == IF R = {} THEN None ELSE IF E[x].to \in R THEN E[x].to ELSE Home(x, R)
Owner(v, x) == IF v.ok THEN OwnerOf(v.ring, v.entries, x) ELSE None

VARIABLES
    doc,          \* [members, entries]: the overrides document
    count,        \* desired count
    alive,        \* process running
    boot,         \* "off" | "deciding" (C13: leaves itself out) | "up"
    qual,         \* per instance: instances it judged live at its last beat read
    cand,         \* per instance: the candidate set of its last pass (Standing)
    dead,         \* per reader, per instance: the incarnation of a beat it holds read
                  \* stale or draining (0: none, M1)
    candDead,     \* per reader: `dead` as of its last pass
    inc,          \* per instance: its incarnation (boot id), 0 before its first start
    view,         \* per instance: its own published view
    eng,          \* per instance, per leaf: engine open
    hand,         \* per instance: the planned move it runs
    slot,         \* per target: the giver holding its move-in slot (K3)
    sealed,       \* per instance: the release's seal (C10)
    draining,     \* per instance: a planned drain began (signal)
    announced,    \* per instance: this incarnation's beat says draining
    claim,        \* Design "beat": the beat's member claim
    seen,         \* Design "beat": per reader, per instance, the claim last read
    counters,     \* [cc, mv, cr, mj]: count changes, moves, crashes, misjudges
    excused,      \* ghost: leaves a flagged step touched
    ghost         \* ghost record for the witnesses

vars == <<doc, count, alive, boot, qual, cand, dead, candDead, inc, view, eng, hand, slot, sealed, draining,
          announced, claim, seen, counters, excused, ghost>>

Live == {n \in Nodes : alive[n] /\ ~announced[n]}
Counted(c) == {n \in Nodes : Ord(n) <= c}
Listed == {m \in Nodes : doc.members[m] # 0}
(* A boot whose name is listed writes no beat until its decision (the join  *)
(* ruling, Addendum 2): what a reader reads of it meanwhile is the previous *)
(* boot's beat, still fresh or already stale (evidence against that boot).  *)
Silent == {m \in Nodes : alive[m] /\ boot[m] = "deciding" /\ doc.members[m] # 0}
(* R1b: an instance is a member iff the document lists it with its own boot. *)
Member(n) == inc[n] # 0 /\ doc.members[n] = inc[n]
Up(n) == alive[n] /\ boot[n] = "up"
Leaving(n) == Member(n) /\ (Ord(n) > count \/ draining[n])

(* The ring an instance's view carries for document d over its beat reads: *)
(* Q the instances it read live, D the incarnation of each beat it holds    *)
(* read stale or draining. R1: a listed member leaves the ring only on a    *)
(* held beat of its listed incarnation; "name": on any such beat; "ruled":  *)
(* listed /\ read live.                                                     *)
RingRec(n, d, Q, D) ==
    LET self == IF inc[n] # 0 /\ d.members[n] = inc[n] /\ boot[n] = "up" THEN {n} ELSE {}
        listed == {m \in Nodes \ {n} : d.members[m] # 0}
        others == CASE RingRule = "ruled" -> listed \cap Q
                    [] RingRule = "name" -> {m \in listed : D[m] = 0}
                    [] OTHER -> {m \in listed : D[m] # d.members[m]}
    IN self \cup others
RingBeat(n, Q, sn) ==
    {m \in Nodes : IF m = n THEN claim[n] /\ boot[n] = "up" ELSE m \in Q /\ sn[m]}
Ring(n, d, Q, D, sn) == IF Design = "record" THEN RingRec(n, d, Q, D) ELSE RingBeat(n, Q, sn)
MkView(n, d, Q, D, sn) ==
    [ok |-> TRUE, ring |-> Ring(n, d, Q, D, sn), entries |-> d.entries]

(* An instance's ring as of its last beat read, against the live document. *)
NowRing(n) == Ring(n, doc, qual[n], dead[n], seen[n])
DocOwner(n, x) == OwnerOf(NowRing(n), doc.entries, x)

(* A flagged step excuses every leaf from then on: the ruling's "unless a  *)
(* flagged step lies between". A liveness misjudgement also voids entries *)
(* naming the misjudged instance written later, so no narrower set holds. *)
Excuse(c) == excused' = Leaves

-----------------------------------------------------------------------------
Init ==
    /\ doc = [members |-> [m \in Nodes |-> IF Design = "record" /\ m \in InitUp THEN 1 ELSE 0],
              entries |-> [x \in Leaves |-> IF x = "q" /\ InitEntryQ # None
                                            THEN [to |-> InitEntryQ, pin |-> None] ELSE NoEntry]]
    /\ count = InitCount
    /\ alive = [n \in Nodes |-> n \in InitUp]
    /\ boot = [n \in Nodes |-> IF n \in InitUp THEN "up" ELSE "off"]
    /\ qual = [n \in Nodes |-> IF n \in InitUp THEN InitUp ELSE {}]
    /\ cand = qual
    /\ dead = [n \in Nodes |-> [m \in Nodes |-> 0]]
    /\ candDead = dead
    /\ inc = [n \in Nodes |-> IF n \in InitUp THEN 1 ELSE 0]
    /\ claim = [n \in Nodes |-> n \in InitUp]
    /\ seen = [n \in Nodes |-> [m \in Nodes |-> m \in InitUp /\ n \in InitUp]]
    /\ view = [n \in Nodes |-> NoView]
    /\ eng = [n \in Nodes |-> [x \in Leaves |-> FALSE]]
    /\ hand = [n \in Nodes |-> NoHand]
    /\ slot = [n \in Nodes |-> None]
    /\ sealed = [n \in Nodes |-> FALSE]
    /\ draining = [n \in Nodes |-> FALSE]
    /\ announced = [n \in Nodes |-> FALSE]
    /\ counters = [cc |-> 0, mv |-> 0, cr |-> 0, mj |-> 0, rs |-> 0]
    /\ excused = {}
    /\ ghost = [slotRefused |-> FALSE, joinerWaited |-> FALSE, returnDuringJoin |-> FALSE,
                joinFallback |-> FALSE, leaveFallback |-> FALSE, deadRemoved |-> FALSE,
                drainInLeave |-> FALSE, droppedMidJoin |-> FALSE, readyEnd |-> FALSE,
                drained |-> FALSE, rejoined |-> FALSE,
                crashUnremoved |-> FALSE, crashRemoved |-> FALSE]

-----------------------------------------------------------------------------
(* The environment *)

SetCount(c) ==
    /\ counters.cc < MaxCountChanges /\ c # count
    /\ count' = c
    /\ counters' = [counters EXCEPT !.cc = @ + 1]
    /\ ghost' = [ghost EXCEPT !.droppedMidJoin = @ \/
                    \E j \in Nodes : Up(j) /\ ~Member(j) /\ Ord(j) <= count /\ Ord(j) > c]
    /\ UNCHANGED <<dead, candDead, inc, doc, alive, boot, qual, cand, view, eng, hand, slot, sealed, draining,
                   announced, claim, seen, excused>>

(* The platform starts an instance the count names: a first start, or a    *)
(* restart after a crash or after a drain's stop (M2), as a new incarnation *)
(* whose first beat lands before anything else it does (R3's read-back).   *)
(* Readers keep what they read until their next beat read.                  *)
Start(n) ==
    /\ ~alive[n] /\ Ord(n) <= count
    /\ inc[n] = 0 \/ counters.rs < MaxRestarts
    /\ alive' = [alive EXCEPT ![n] = TRUE]
    /\ boot' = [boot EXCEPT ![n] = "deciding"]
    /\ \E S \in SUBSET (Silent \ {n}) :
        /\ qual' = [qual EXCEPT ![n] = (Live \ S) \cup {n}]
        /\ cand' = [cand EXCEPT ![n] = (Live \ S) \cup {n}]
        /\ dead' = [dead EXCEPT ![n] = [m \in Nodes |-> IF m \in S THEN inc[m] - 1 ELSE IF m # n /\ inc[m] # 0 /\ m \notin Live THEN inc[m] ELSE 0]]
        /\ candDead' = [candDead EXCEPT ![n] = [m \in Nodes |-> IF m \in S THEN inc[m] - 1 ELSE IF m # n /\ inc[m] # 0 /\ m \notin Live THEN inc[m] ELSE 0]]
    /\ inc' = [inc EXCEPT ![n] = @ + 1]
    /\ draining' = [draining EXCEPT ![n] = FALSE]
    /\ announced' = [announced EXCEPT ![n] = FALSE]
    /\ seen' = [seen EXCEPT ![n] = [m \in Nodes |-> m \in Live /\ claim[m]]]
    /\ view' = [view EXCEPT ![n] = NoView]
    /\ counters' = IF inc[n] = 0 THEN counters ELSE [counters EXCEPT !.rs = @ + 1]
    /\ UNCHANGED <<doc, count, eng, hand, slot, sealed, claim,
                   excused, ghost>>

(* C13 with R1a: a boot that finds its name listed with another boot id is  *)
(* in place by one CAS that replaces the boot id (C13's young test; its     *)
(* return is crash class, excused), or removes the listing and joins. An    *)
(* unlisted boot is a joiner. Decided once that CAS is read back. R3: no    *)
(* such CAS while a drain or stop has begun (a new process has neither).   *)
Decide(n) ==
    /\ alive[n] /\ boot[n] = "deciding"
    /\ boot' = [boot EXCEPT ![n] = "up"]
    /\ IF Design = "record" /\ n \in Listed
       THEN \/ /\ Excuse(n)
               /\ doc' = [doc EXCEPT !.members[n] = inc[n]]
            \/ /\ doc' = [doc EXCEPT !.members[n] = 0]
               /\ UNCHANGED excused
       ELSE UNCHANGED <<doc, excused>>
    /\ claim' = [claim EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<dead, candDead, inc, count, alive, qual, cand, view, eng, hand, slot, sealed, draining, announced,
                   seen, counters, ghost>>

Crash(n) ==
    /\ alive[n] /\ counters.cr < MaxCrashes
    /\ Excuse(n)
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ boot' = [boot EXCEPT ![n] = "off"]
    /\ eng' = [eng EXCEPT ![n] = [x \in Leaves |-> FALSE]]
    /\ hand' = [hand EXCEPT ![n] = NoHand]
    /\ slot' = [t \in Nodes |-> IF slot[t] = n THEN None ELSE slot[t]]
    /\ sealed' = [sealed EXCEPT ![n] = FALSE]
    /\ view' = [view EXCEPT ![n] = NoView]
    /\ counters' = [counters EXCEPT !.cr = @ + 1]
    \* R4's two crashes of a drain that has not announced (witnesses only).
    /\ ghost' = [ghost EXCEPT
           !.crashUnremoved = @ \/ (draining[n] /\ ~announced[n] /\ doc.members[n] # 0),
           !.crashRemoved = @ \/ (draining[n] /\ ~announced[n] /\ doc.members[n] = 0)]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, qual, cand, draining, announced, claim, seen>>

(* A live instance judged dead by a reader (a clock, a lost beat).          *)
Misjudge(n, m) ==
    /\ Up(n) /\ alive[m] /\ m # n /\ m \in qual[n] /\ counters.mj < MaxMisjudges
    /\ Excuse(m)
    /\ qual' = [qual EXCEPT ![n] = @ \ {m}]
    /\ dead' = [dead EXCEPT ![n][m] = inc[m]]
    /\ counters' = [counters EXCEPT !.mj = @ + 1]
    /\ UNCHANGED <<candDead, inc, doc, count, alive, boot, cand, view, eng, hand, slot, sealed, draining,
                   announced, claim, seen, ghost>>

-----------------------------------------------------------------------------
(* The tick *)

(* Its pass's beat reads: every listed and counted instance by name (C15).  *)
ReadBeats(n) ==
    /\ alive[n] /\ boot[n] # "off"
    /\ \E S \in SUBSET (Silent \ {n}) :
        /\ qual' = [qual EXCEPT ![n] = (Live \ S) \cup {n}]
        /\ dead' = [dead EXCEPT ![n] = [m \in Nodes |-> IF m \in S THEN inc[m] - 1 ELSE IF m # n /\ inc[m] # 0 /\ m \notin Live THEN inc[m] ELSE 0]]
    /\ \/ Design = "record" /\ UNCHANGED seen
       \/ Design = "beat" /\ \E S \in SUBSET Nodes :
              seen' = [seen EXCEPT ![n] = [m \in Nodes |-> IF m \in S THEN m \in Live /\ claim[m]
                                                           ELSE seen[n][m]]]
    /\ UNCHANGED <<candDead, inc, doc, count, alive, boot, cand, view, eng, hand, slot, sealed, draining,
                   announced, claim, counters, excused, ghost>>

PublishFrom(n, d) ==
    /\ view' = [view EXCEPT ![n] = MkView(n, d, qual[n], dead[n], seen[n])]
    /\ cand' = [cand EXCEPT ![n] = qual[n]]
    /\ candDead' = [candDead EXCEPT ![n] = dead[n]]

Publish(n) ==
    /\ alive[n] /\ boot[n] # "off"
    /\ PublishFrom(n, doc)
    /\ UNCHANGED <<dead, inc, doc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* A refresh or a handoff mirror: a newer document over the last pass's    *)
(* candidates (C2), or (control CE4) the last published ring.             *)
RefreshView(n, d) ==
    [ok |-> TRUE,
     ring |-> IF RefreshKeepsRing THEN view[n].ring
              ELSE Ring(n, d, cand[n], candDead[n], seen[n]),
     entries |-> d.entries]

Refresh(n) ==
    /\ Up(n) /\ view[n].ok
    /\ view' = [view EXCEPT ![n] = RefreshView(n, doc)]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, eng, hand, slot, sealed, draining,
                   announced, claim, seen, counters, excused, ghost>>

Yield(n, x) ==
    /\ eng[n][x] /\ Owner(view[n], x) # n /\ hand[n].x # x
    /\ eng' = [eng EXCEPT ![n][x] = FALSE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, hand, slot, sealed, draining,
                   announced, claim, seen, counters, excused, ghost>>

Open(n, x) ==
    /\ Up(n) /\ ~eng[n][x] /\ Owner(view[n], x) = n /\ hand[n].x # x
    /\ ~(SealOnRelease /\ sealed[n])
    /\ eng' = [eng EXCEPT ![n][x] = TRUE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, hand, slot, sealed, draining,
                   announced, claim, seen, counters, excused, ghost>>

-----------------------------------------------------------------------------
(* The membership task, Design "record" (C5-C8, C10-C12, C14)              *)

Unlisted == {n \in Counted(count) : n \notin Listed}  \* listed under any boot counts as listed
(* The join ruling, Addendum 44: a document that lists nobody leaves        *)
(* nothing to cover, since no instance could pin. A control overrides it.   *)
NobodyCovers == TRUE
NobodyCoversOff == FALSE
(* C5 as ruled: a leaf it would win by ring with no entry naming a ring    *)
(* member. CoverRule "owners": every leaf whose owner the join changes, so *)
(* a (void) entry naming the joiner is uncovered too (rev 1's J2).         *)
Uncovered(j) ==
    LET R == NowRing(j) IN
    IF Design = "record" /\ NobodyCovers /\ Listed = {} THEN {}
    ELSE IF CoverRule = "ruled"
    THEN {x \in Leaves : Home(x, R \cup {j}) = j /\ doc.entries[x].to \notin R}
    ELSE {x \in Leaves : OwnerOf(R \cup {j}, doc.entries, x) # OwnerOf(R, doc.entries, x)}
(* A draining instance never joins (a rule the ruling does not state: the  *)
(* membership task runs between a drain's announce and its stop).         *)
Joiner(j) ==
    /\ Up(j) /\ Ord(j) <= count /\ ~(DrainNoJoin /\ draining[j])
    /\ IF Design = "record" THEN doc.members[j] = 0 ELSE ~claim[j]

(* C7: one CAS naming itself for each leaf the document gives it that the  *)
(* ring with every unlisted counted ordinal would give to another.        *)
PinsOf(m) ==
    {x \in Leaves : /\ DocOwner(m, x) = m /\ doc.entries[x].to # m
                    /\ OwnerOf(NowRing(m) \cup Unlisted, doc.entries, x) # m}
Pin(m) ==
    /\ Design = "record" /\ Up(m) /\ Member(m) /\ ~Leaving(m)
    /\ PinsOf(m) # {}
    /\ LET d == [doc EXCEPT !.entries = [x \in Leaves |->
                    IF x \in PinsOf(m) THEN [to |-> m, pin |-> None] ELSE @[x]]]
       IN /\ doc' = d /\ PublishFrom(m, d)
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* C6 (b): the named instance drops its entry when it is the ring home over *)
(* ring \cup counted ordinals; (control) any listed instance may.          *)
NoOpDrop(n, x) ==
    /\ Design = "record" /\ Up(n) /\ Member(n)
    /\ LET to == doc.entries[x].to IN
       /\ to \in Listed
       /\ IF ThirdPartyNoOp THEN TRUE ELSE to = n
       /\ Home(x, NowRing(n) \cup (IF NoOpCounted THEN Counted(count) \cup Listed ELSE {})) = to
    /\ LET d == [doc EXCEPT !.entries[x] = NoEntry]
       IN /\ doc' = d /\ PublishFrom(n, d)
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* C6 (c): anyone drops an entry naming an instance the document does not list. *)
DropUnlisted(n, x) ==
    /\ Design = "record" /\ Up(n)
    /\ doc.entries[x].to # None /\ doc.entries[x].to \notin Listed
    /\ LET d == [doc EXCEPT !.entries[x] = NoEntry]
       IN /\ doc' = d /\ PublishFrom(n, d)
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* C5: the join CAS, against the pass's document, then the view with itself. *)
JoinStep(j, fallback) ==
    /\ Design = "record" /\ Joiner(j)
    /\ IF fallback THEN JoinFallbackOn /\ Uncovered(j) # {}
       ELSE ~CoverageGuard \/ Uncovered(j) = {}
    /\ LET d == [doc EXCEPT !.members[j] = inc[j]]
       IN /\ doc' = d /\ PublishFrom(j, d)
    /\ excused' = IF fallback THEN Leaves ELSE excused
    /\ ghost' = [ghost EXCEPT !.joinFallback = @ \/ fallback,
                               !.rejoined = @ \/ (ghost.drained /\ inc[j] > 1)]
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters>>
Join(j) == JoinStep(j, FALSE)
JoinFallback(j) == JoinStep(j, TRUE)

(* Witness only: a joiner that finds something uncovered while a holder's  *)
(* engine is open waits (no state change but the ghost).                  *)
JoinerWaits(j) ==
    /\ Design = "record" /\ Joiner(j) /\ ~ghost.joinerWaited
    /\ \E x \in Uncovered(j) : \E h \in Nodes : Member(h) /\ eng[h][x]
    /\ ghost' = [ghost EXCEPT !.joinerWaited = TRUE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, hand, slot, sealed, draining,
                   announced, claim, seen, counters, excused>>

(* C10: the seal, one step under the gate: holds nothing, no move running. *)
Seal(n) ==
    /\ Up(n) /\ Leaving(n) /\ ~sealed[n]
    /\ \A x \in Leaves : ~eng[n][x]
    /\ hand[n] = NoHand
    /\ sealed' = [sealed EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, hand, slot, draining,
                   announced, claim, seen, counters, excused, ghost>>

Unseal(n) ==
    /\ sealed[n] /\ ~Leaving(n)
    /\ sealed' = [sealed EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, hand, slot, draining,
                   announced, claim, seen, counters, excused, ghost>>

(* C10, C12: the CAS removing itself, then its view. A drain's announce is *)
(* its own step (Announce, M3), before or after this one.                 *)
(* LEAVE_PHASE's fallback releases holding (flagged).                      *)
ReleaseStep(n, fallback) ==
    /\ Up(n) /\ Leaving(n)
    /\ IF fallback THEN LeaveFallbackOn /\ ~sealed[n] /\ \E x \in Leaves : eng[n][x]
       ELSE sealed[n]
    /\ IF Design = "record"
       THEN LET d == [doc EXCEPT !.members[n] = 0]
            IN /\ doc' = d /\ PublishFrom(n, d) /\ UNCHANGED claim
       ELSE /\ claim' = [claim EXCEPT ![n] = FALSE]
            /\ view' = [view EXCEPT ![n] = [@ EXCEPT !.ring = @ \ {n}]]
            /\ UNCHANGED <<cand, candDead, doc>>
    /\ sealed' = [sealed EXCEPT ![n] = FALSE]
    /\ excused' = IF fallback THEN Leaves ELSE excused
    /\ ghost' = [ghost EXCEPT !.leaveFallback = @ \/ fallback]
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, draining, announced, seen, counters>>
Release(n) == ReleaseStep(n, FALSE)
LeaveFallback(n) == ReleaseStep(n, TRUE)

(* G1, M3: a draining instance that holds nothing announces (its beat says *)
(* draining): before its removal lands (sealed) or after it.              *)
Announce(n) ==
    /\ alive[n] /\ draining[n] /\ ~announced[n] /\ hand[n] = NoHand
    /\ \A x \in Leaves : ~eng[n][x]
    /\ IF AnnounceAfterRemoval THEN ~Member(n) /\ doc.members[n] = 0 ELSE sealed[n] \/ ~Member(n)
    /\ announced' = [announced EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, hand, slot, sealed,
                   draining, claim, seen, counters, excused, ghost>>

(* A drained instance stops once it announced and its removal landed (an   *)
(* orderly stop, no flag). The platform may start the name again (M2).     *)
Stop(n) ==
    /\ alive[n] /\ announced[n] /\ ~Member(n) /\ \A x \in Leaves : ~eng[n][x]
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ boot' = [boot EXCEPT ![n] = "off"]
    /\ view' = [view EXCEPT ![n] = NoView]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, qual, cand, eng, hand, slot, sealed, draining, announced, claim,
                   seen, counters, excused, ghost>>

(* One planned drain per behaviour (the model's bound).                     *)
Drain(n) ==
    /\ DrainOn /\ Up(n) /\ Member(n) /\ \A m \in Nodes : ~draining[m] /\ ~ghost.drained
    /\ draining' = [draining EXCEPT ![n] = TRUE]
    /\ ghost' = [ghost EXCEPT !.drainInLeave = @ \/ Ord(n) > count, !.drained = TRUE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, hand, slot, sealed,
                   announced, claim, seen, counters, excused>>

(* C14 with R1c and M4: a ring member removes a listed instance on evidence *)
(* only, a held beat of the listed boot read stale or draining (the bound's *)
(* wait abstracted); removing a live incarnation is flagged.               *)
DeadRemoval(n, m) ==
    /\ DeadRemovalOn /\ Design = "record" /\ Up(n) /\ Member(n)
    /\ m # n /\ doc.members[m] # 0 /\ dead[n][m] = doc.members[m]
    /\ LET d == [doc EXCEPT !.members[m] = 0]
       IN /\ doc' = d /\ PublishFrom(n, d)
    /\ excused' = IF alive[m] /\ inc[m] = doc.members[m] THEN Leaves ELSE excused
    /\ ghost' = [ghost EXCEPT !.deadRemoved = TRUE]
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters>>

-----------------------------------------------------------------------------
(* The membership task, Design "beat" (rev 1: J1-J5, §3.1, L3)              *)

BeatJoiners(m) == {n \in qual[m] : Ord(n) <= count /\ ~seen[m][n] /\ n # m}
BeatPinsOf(m) ==
    LET A == NowRing(m) P == A \cup BeatJoiners(m) IN
    {x \in Leaves : /\ OwnerOf(A, doc.entries, x) = m /\ Home(x, P) # m
                    /\ doc.entries[x].to \notin A}
BeatPin(m) ==
    /\ Design = "beat" /\ Up(m) /\ claim[m] /\ BeatPinsOf(m) # {}
    /\ LET P == NowRing(m) \cup BeatJoiners(m)
           d == [doc EXCEPT !.entries = [x \in Leaves |->
                    IF x \in BeatPinsOf(m) THEN [to |-> m, pin |-> Home(x, P)] ELSE @[x]]]
       IN /\ doc' = d /\ PublishFrom(m, d)
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* §3.1 rule 3: the named instance drops a pin whose joiner is past the count. *)
BeatPinDrop(n, x) ==
    /\ Design = "beat" /\ Up(n) /\ doc.entries[x].to = n /\ doc.entries[x].pin # None
    /\ Ord(doc.entries[x].pin) > count
    /\ LET d == [doc EXCEPT !.entries[x] = NoEntry]
       IN /\ doc' = d /\ PublishFrom(n, d)
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* moves.rs:76-82: any member drops an entry naming the ring home; pins exempt. *)
BeatNoOpDrop(n, x) ==
    /\ Design = "beat" /\ Up(n) /\ claim[n]
    /\ doc.entries[x].to # None /\ doc.entries[x].pin = None
    /\ doc.entries[x].to = Home(x, NowRing(n))
    /\ LET d == [doc EXCEPT !.entries[x] = NoEntry]
       IN /\ doc' = d /\ PublishFrom(n, d)
    /\ UNCHANGED <<dead, inc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* J2-J3: coverage over its pass's reads, its own view with itself, then   *)
(* the claim; peers see the claim at their next read of its beat.         *)
BeatUncovered(j) ==
    LET A == NowRing(j) IN
    {x \in Leaves : \/ /\ Home(x, A \cup {j}) = j /\ OwnerOf(A, doc.entries, x) # None
                       /\ doc.entries[x].to \notin A
                    \/ doc.entries[x].to = j}
BeatJoin(j) ==
    /\ Design = "beat" /\ Joiner(j)
    /\ ~CoverageGuard \/ BeatUncovered(j) = {}
    /\ claim' = [claim EXCEPT ![j] = TRUE]
    /\ view' = [view EXCEPT ![j] = [ok |-> TRUE, ring |-> NowRing(j) \cup {j},
                                    entries |-> doc.entries]]
    /\ cand' = [cand EXCEPT ![j] = qual[j]]
    /\ candDead' = [candDead EXCEPT ![j] = dead[j]]
    /\ UNCHANGED <<dead, inc, doc, count, alive, boot, qual, eng, hand, slot, sealed, draining, announced,
                   seen, counters, excused, ghost>>

-----------------------------------------------------------------------------
(* Planned moves, giver first (return-home, count balance, the lag and room *)
(* moves, a leaver's handoffs (C9), the drain's). C11: the target is in the *)
(* giver's ring and within the count.                                       *)

MoveTarget(g, x, t) ==
    /\ Up(g) /\ hand[g] = NoHand /\ Owner(view[g], x) = g
    /\ t \in view[g].ring /\ t # g /\ Ord(t) <= count /\ ~Leaving(t)
    /\ t \in qual[g]  \* R1d: a target is a member the giver holds a fresh beat of
    /\ (Leaving(g) \/ counters.mv < MaxMoves)

MoveStart(g, x, t) ==
    /\ MoveTarget(g, x, t) /\ (SlotOn => slot[t] = None)
    /\ hand' = [hand EXCEPT ![g] = [x |-> x, t |-> t, st |-> "held"]]
    /\ slot' = IF SlotOn THEN [slot EXCEPT ![t] = g] ELSE slot
    /\ counters' = IF Leaving(g) THEN counters ELSE [counters EXCEPT !.mv = @ + 1]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, sealed, draining, announced,
                   claim, seen, excused, ghost>>

(* K3, rev 2's witness: a second giver is refused the slot while a first   *)
(* move-in to the same target runs (no state change but the ghost).        *)
SlotRefused(g, x, t) ==
    /\ MoveTarget(g, x, t) /\ slot[t] # None /\ slot[t] # g /\ ~ghost.slotRefused
    /\ ghost' = [ghost EXCEPT !.slotRefused = TRUE]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, hand, slot, sealed, draining,
                   announced, claim, seen, counters, excused>>

MoveClose(g) ==
    /\ alive[g] /\ hand[g].st = "held"
    /\ eng' = [eng EXCEPT ![g][hand[g].x] = FALSE]
    /\ hand' = [hand EXCEPT ![g].st = "closed"]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, slot, sealed, draining, announced,
                   claim, seen, counters, excused, ghost>>

(* The CAS, only while the document still gives the leaf to the giver; a  *)
(* target that is the leaf's ring home gets no entry (Change::Return) or    *)
(* an explicit one naming it (Change::To).                                 *)
MoveCas(g) ==
    /\ alive[g] /\ hand[g].st = "closed"
    /\ LET x == hand[g].x t == hand[g].t
           R == NowRing(g)
       IN IF DocOwner(g, x) = g
          THEN \E e \in {[to |-> t, pin |-> None]} \cup (IF t = Home(x, R) THEN {NoEntry} ELSE {}) :
               LET d == [doc EXCEPT !.entries[x] = e] IN
               /\ doc' = d
               /\ view' = [view EXCEPT ![g] = RefreshView(g, d)]
               /\ hand' = [hand EXCEPT ![g].st = "moved"]
               /\ UNCHANGED slot
               /\ ghost' = [ghost EXCEPT !.returnDuringJoin = @ \/
                               (e = NoEntry /\ \E j \in Nodes \ {t} : Joiner(j))]
          ELSE /\ hand' = [hand EXCEPT ![g] = NoHand]
               /\ slot' = [slot EXCEPT ![t] = None]
               /\ UNCHANGED <<doc, view, ghost>>
    /\ UNCHANGED <<dead, candDead, inc, count, alive, boot, qual, cand, eng, sealed, draining, announced, claim,
                   seen, counters, excused>>

(* K10: the move ends at the target's Ready, or at its hold's lapse.       *)
MoveEnd(g) ==
    /\ alive[g] /\ hand[g].st = "moved"
    /\ hand' = [hand EXCEPT ![g] = NoHand]
    /\ slot' = [slot EXCEPT ![hand[g].t] = None]
    /\ ghost' = [ghost EXCEPT !.readyEnd = @ \/ eng[hand[g].t][hand[g].x]]
    /\ UNCHANGED <<dead, candDead, inc, doc, count, alive, boot, qual, cand, view, eng, sealed, draining, announced,
                   claim, seen, counters, excused>>

-----------------------------------------------------------------------------
Next ==
    \/ \E c \in 1..3 : SetCount(c)
    \/ \E n \in Nodes :
        \/ Start(n) \/ Decide(n) \/ Crash(n) \/ ReadBeats(n) \/ Publish(n) \/ Refresh(n)
        \/ Pin(n) \/ Join(n) \/ JoinFallback(n) \/ JoinerWaits(n) \/ Seal(n) \/ Unseal(n)
        \/ Release(n) \/ LeaveFallback(n) \/ Announce(n) \/ Stop(n) \/ Drain(n)
        \/ BeatPin(n) \/ BeatJoin(n)
        \/ MoveClose(n) \/ MoveCas(n) \/ MoveEnd(n)
        \/ \E x \in Leaves : Yield(n, x) \/ Open(n, x) \/ NoOpDrop(n, x) \/ DropUnlisted(n, x)
                             \/ BeatPinDrop(n, x) \/ BeatNoOpDrop(n, x)
        \/ \E m \in Nodes : Misjudge(n, m) \/ DeadRemoval(n, m)
        \/ \E x \in Leaves, t \in Nodes : MoveStart(n, x, t) \/ SlotRefused(n, x, t)

Spec == Init /\ [][Next]_vars

(* M5: weak fairness on the steps the code retries: the platform's start,  *)
(* the boot's decision, the tick's beat read and publication, the pin, the *)
(* join, both drops and a running move's steps. The environment (the count, *)
(* crashes, drains, requests) and the refresh are not fair.                 *)
Fair ==
    \A n \in Nodes :
        /\ WF_vars(Start(n)) /\ WF_vars(Decide(n)) /\ WF_vars(ReadBeats(n))
        /\ WF_vars(Publish(n)) /\ WF_vars(Pin(n)) /\ WF_vars(Join(n))
        /\ WF_vars(MoveClose(n)) /\ WF_vars(MoveCas(n)) /\ WF_vars(MoveEnd(n))
        /\ \A x \in Leaves : WF_vars(NoOpDrop(n, x)) /\ WF_vars(DropUnlisted(n, x))
LSpec == Spec /\ Fair

(* A running joiner within the count becomes a member, unless it stops or  *)
(* the count leaves it out.                                                 *)
JoinCompletes ==
    \A j \in Nodes :
        (alive[j] /\ boot[j] = "up" /\ Ord(j) <= count /\ doc.members[j] = 0)
            ~> (Member(j) \/ ~alive[j] \/ Ord(j) > count)

(* The configurations' action constraint: no step leaves a state a flagged *)
(* step reached. The invariant excuses every leaf from there, so a later   *)
(* state can violate nothing; the flagged state itself is still checked.  *)
FlagStops == excused = {}

-----------------------------------------------------------------------------
TypeOK ==
    /\ \A m \in Nodes : doc.members[m] \in 0..inc[m]
    /\ \A x \in Leaves : doc.entries[x].to \in Nodes \cup {None}
    /\ count \in 1..3
    /\ \A n \in Nodes : boot[n] \in {"off", "deciding", "up"} /\ qual[n] \subseteq Nodes
    /\ \A n \in Nodes : hand[n].st \in {None, "held", "closed", "moved"}

MembershipMovesNothing ==
    \A n \in Nodes, x \in Leaves :
        (eng[n][x] /\ x \notin excused) =>
            /\ Owner(view[n], x) \in {n, None}
            /\ \A m \in Nodes \ {n} : alive[m] => Owner(view[m], x) # m

(* The same, but a sealed instance's own view is no claim: it opens nothing *)
(* until it unseals (C10), and a draining one never unseals. A proposal for  *)
(* M3's intermediate state (announced, removal not landed), not ruled.      *)
MovesNothingUnsealed ==
    \A n \in Nodes, x \in Leaves :
        (eng[n][x] /\ x \notin excused) =>
            /\ Owner(view[n], x) \in {n, None}
            /\ \A m \in Nodes \ {n} : (alive[m] /\ ~sealed[m]) => Owner(view[m], x) # m

(* An engine is open only on an instance whose view gives it the leaf, or  *)
(* that holds it for a running move.                                        *)
NoTwoEngines ==
    \A x \in Leaves : \A n, m \in Nodes : (n # m /\ eng[n][x] /\ eng[m][x]) => x \in excused

(* Witnesses: each is violated in some reachable state. *)
Witness_JoinerWaitsOnUnpinnedHolder == ~ghost.joinerWaited
Witness_CountDropsMidJoin == ~(ghost.droppedMidJoin /\ excused = {})
Witness_ReturnRacesJoin == ~(ghost.returnDuringJoin /\ excused = {})
Witness_JoinFallback == ~ghost.joinFallback
Witness_LeaveFallback == ~ghost.leaveFallback
Witness_DeadRemoval == ~ghost.deadRemoved
Witness_DrainDuringLeave == ~(ghost.drainInLeave /\ \E n \in Nodes : announced[n])
Witness_SlotRefused == ~ghost.slotRefused
Witness_MoveEndsAtReady == ~ghost.readyEnd
Witness_RejoinsAfterDrain == ~(ghost.rejoined /\ excused = {})
(* R4: a drain that stops without announcing, its removal not landed, and  *)
(* one whose removal landed, stopping before it announced: both crashes.   *)
Witness_DrainCrashUnremoved == ~ghost.crashUnremoved
Witness_DrainCrashRemoved == ~ghost.crashRemoved
Witness_JoinerServes == ~(\E n \in Nodes : n \notin InitUp /\ \E x \in Leaves : eng[n][x])
=============================================================================
