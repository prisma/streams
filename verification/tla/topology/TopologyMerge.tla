---------------------------- MODULE TopologyMerge ----------------------------
(***************************************************************************)
(* TLA-048 (S6-DESIGN rev 5 §7.2-§7.4, package 6's chain at 570cce46b and  *)
(* 74f226387): a merge of two sibling leaves and its recovery             *)
(* (`src/topology/merge.rs`, `recover/merge.rs`, `fence.rs`, `actor.rs`,   *)
(* `policy_loop.rs`, `fleet/colocate.rs`, `fleet.rs`'s eager move-in), and *)
(* the co-location that puts both halves on one instance first.           *)
(*                                                                         *)
(* Two sources S0 and S1 (the halves `…0`, `…1`), one record each (r0 in   *)
(* S0, r1 in S1). Each source has its own log with its own writer: an     *)
(* engine is `cur` (the latest writer), `stale` (fenced by a later open),  *)
(* or `maint` (a claimant's maintenance open: the latest writer, never     *)
(* served). Writer epochs are abstract: an open makes every other engine  *)
(* of that source stale, which is all the epoch bump does.                *)
(*                                                                         *)
(* Co-location (S6 §7.2 step 0, 74f226387, and the lead's fix of          *)
(* 2026-11-10): when the halves live on two instances, the policy on the  *)
(* owner of …0 moves both to itself if it has room for the half and the  *)
(* merged leaf M (resident + 2 <= Cap), else (PartnerFallback) to the     *)
(* owner of …1 if it has room, else (AskWhenNoRoom) asks for an instance; *)
(* the fleet adds Spare and count balance moves load onto it. A local    *)
(* pair at its cap asks too. The merge starts only once the move's        *)
(* cooldown passed (`fresh` false) and the actor holds both engines.      *)
(*                                                                         *)
(* The actor (`Started::finish`, `run`): still_owned at the pause, both   *)
(* barriers, both checkpoints (a fenced source's flush fails: roll back), *)
(* the union clone of M from both checkpoints (unfenced, into the attempt's *)
(* fresh home), still_owned before 'T', 'T' into S0 then S1 (a fenced     *)
(* write: Unfinished, the intent stays live, both engines close), the     *)
(* topology CAS (Unfinished when a claimant already published), and the   *)
(* terminal CAS. still_owned re-claims a lease only the actor's own clock  *)
(* saw lapse, unless a claimant took it. The actor may crash and its      *)
(* instance may drop out of the ring while the actor runs on (a           *)
(* partition: a slow actor).                                              *)
(*                                                                         *)
(* The claimant (`recover::merge`): an opener of a source whose fence     *)
(* finds a lapsed intent, or a 'T' the topology has not published, opens   *)
(* the OTHER source for maintenance (fencing it) before it claims by CAS  *)
(* and decides from both: published -> Reclaimed; a 'T' with M complete  *)
(* -> write the missing 'T', publish, Reclaimed; no 'T' -> roll back with *)
(* the takeover GC (M's path and both checkpoints deleted), RolledBack.  *)
(* A claim CAS lost to a renewal or another claimant, or an engine fenced *)
(* under it, abandons the attempt; the held open re-reads later.         *)
(*                                                                         *)
(* A published merge is reclaimed with nothing opened (#251 B6,            *)
(* recover::merge): after its claim the claimant reads the stored topology *)
(* and, finding the merge published, ends the intent Reclaimed before any  *)
(* open. The model's claimant still fences the other source on that path,  *)
(* which fences more than the code does, the safe direction (Fable, B6     *)
(* ratification section 5).                                                *)
(*                                                                         *)
(* Who opens a source: a client request routed to its effective owner     *)
(* (RequestOpen: no fairness, a merged pair is cold), and the fleet tick's *)
(* eager move-in of an OVERRIDDEN prefix by its effective owner           *)
(* (EagerOpen: weakly fair, the tick retries), and with the fix accepted   *)
(* on 2026-11-10 (LapsedOpen) a source of any live, lapsed intent: the     *)
(* first TLA-048 run showed a dead or Unfinished actor's cold local pair   *)
(* otherwise stays live for good. The heartbeat's reclaim of               *)
(* a published intent (833bcdc5b) is weakly fair. Liveness assumes weak   *)
(* fairness only for steps the code retries.                              *)
(*                                                                         *)
(* Abstractions: the claimant's act is one step (its writes are guarded   *)
(* by both its engines still being current); one actor process (a new     *)
(* attempt starts only after the previous actor stopped); M's own engine  *)
(* is abstract (it serves on any instance routing by the merged topology). *)
(* An instance opens only the sources it owns by a published view (push 4, *)
(* DESIGN-DECISION-c3-incomplete-rig Addendum 9): a beat's                 *)
(* open_unopened_sources returns while no ring is known, so a fresh        *)
(* instance's first beat fences no foreign source, and the model's         *)
(* openers, which act for the sources' effective owners, need no case for  *)
(* an instance that knows no ring.                                         *)
(* No source is opened after the publish (OpenOn and EagerOpen carry       *)
(* version 1): the code guards that in ShardDirectory::open_or_wait and    *)
(* fence::open_held, which refuse a prefix that is no leaf (#251 R3-d).    *)
(* No source write follows its 'T' (a source's log is final from the 'T': *)
(* the gate's final flag, set under the exclusive hold, refuses every      *)
(* index batch after it; push 4, DESIGN-DECISION-c3-incomplete-rig         *)
(* Addendum 5), so a source's rows are what M copied.                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Insts,       \* every instance, Spare among them
    InitAlive,   \* the instances in the ring at the start
    Spare,       \* the instance the fleet adds when asked
    Ring0, Ring1, \* the ring's (rendezvous) pick for S0 and S1
    LoadA, LoadB, \* other shards held by "a" and "b" at the start
    Cap,         \* SHARDS_PER_INSTANCE_MAX
    MaxAtt,      \* merge attempts (each takes a fresh home)
    MaxCrashes,  \* crashes (an actor's or a claimant's instance), in all
    MaxParts,    \* partitions of the actor's instance from the ring
    MaxLapses,   \* lapses a still-working lease owner may suffer (eventual timeliness)
    MaxRestarts, \* restarts of a bystander instance (a deploy), in all
    BoundRestarts, \* apply that bound (restarts are finite; FALSE: unbounded)
    FirstOpenUnheld, \* a process's first open skips the hold (f42a3b297: the store is
                     \* known only after its first engine opens)
    BoundLapses, \* apply that bound (liveness checks; FALSE for the safety checks)
    RequestsFair, \* clients keep reaching the sources (a hot pair; FALSE: cold)
    ClaimRaceFair \* an opener that sees a lapse infinitely often eventually claims
                  \* (strong fairness on the fence's claim step: an assumption about
                  \* timing, see TLA-048's report; FALSE: weak fairness only)

\* The operators a negative control replaces: TRUE in the baseline.
CONSTANTS
    PartnerFallback,     \* co-locate onto the partner when the actor has no room
    AskWhenNoRoom,       \* ask for an instance when neither side has room
    ClaimFencesBoth,     \* the claimant fences the other source before it decides
    RollBackNeedsNoT,    \* the claimant rolls back only with 'T' in neither source
    ReclaimNeedsPublish, \* a reader ends a live intent only once it is published
    BarrierBeforeCkpt,   \* the checkpoints follow both barriers
    CloneBoth,           \* M is cloned from both checkpoints
    HeldUnderT,          \* an open whose log holds 'T' never serves
    EagerMoveIn,         \* the fleet tick's eager open runs at all
    LapsedOpen,          \* it also opens the sources of a live, lapsed intent (the fix)
    HoldBeforeOpen,      \* an open waits BEFORE its SlateDB open (no epoch bump) while a
                         \* live intent's lease or claim is fresh (option (b), 2026-11-10)
    MaintOpenHolds,      \* the claimant's maintenance open goes through the same hold
    MaintExemptOwn,      \* ... except under the claimant's own claim (no self-wait)
    ClaimBeforeMaint,    \* the claimant CASes its claim before its maintenance open
                         \* (option (b)'s order; FALSE: 570cce46b's, fence both first)
    HeartbeatReclaim     \* the heartbeat ends a published intent (833bcdc5b)

Src == {0, 1}
Other(s) == 1 - s
Homes == 1..MaxAtt
NoOne == "none"

VARIABLES
    alive,      \* the instances in the ring
    ovr,        \* [Src -> Insts \cup {NoOne}]: fleet/overrides.json's entry
    fresh,      \* an override younger than the move cooldown exists
    asked,      \* the policy asked the fleet for an instance
    load,       \* [Insts -> Nat]: other shards each instance holds
    eng,        \* [Insts -> [Src -> {"none", "cur", "stale", "maint"}]]
    held,       \* [Insts -> [Src -> BOOLEAN]]: the fence holds that engine
    installed,  \* [Insts -> {1, 2}]: the topology each instance routes by
    plog,       \* [Src -> BOOLEAN]: the source's record is durable in its log
    acked,      \* records acked to a client (a subset of Src: r0, r1)
    ckpt,       \* [Homes -> [Src -> {0, 1, 2}]]: the attempt's topo- checkpoint
                \*   of each source: 0 none, 1 without the record, 2 with it
    mfiles,     \* [Homes -> BOOLEAN]: M's path holds a complete clone
    mrecs,      \* [Homes -> SUBSET Src]: the records M at that home holds
    T,          \* [Src -> 0..MaxAtt]: the attempt whose 'T' the source's log holds
    version,    \* the stored topology: 1 (S0, S1) or 2 (M)
    pubHome,    \* the home the stored topology's M lives at (0 before)
    intent,     \* the cell's intent
    pc,         \* the actor's step
    A,          \* the actor's instance
    aok,        \* [Src -> BOOLEAN]: the actor's writer of each source is current
    barrier,    \* [Src -> BOOLEAN]: the actor's barrier parks the source
    att,        \* attempts begun
    crashes, parts,
    cstep,      \* [Insts -> {"idle", "one", "both", "claimed1", "claimed"}]: a claimant's step
    csrc,       \* [Insts -> Src]: the source a claimant opened
    rolledBack, \* ghost: the homes whose attempt ended RolledBack
    inconsistent, \* ghost: a claimant found 'T' with M incomplete
    lapses,     \* lapses observed while the lease's owner was still working
    first,      \* [Insts -> BOOLEAN]: the process has opened no engine since it started
    restarts,   \* bystander restarts so far
    unheldFresh, \* ghost: an unheld first open fenced under a fresh lease or claim
    sib         \* ghost [st, who, src]: st "fenced" when a claimant's maintenance open
                \* fenced the open engine of another instance `who` (not a running
                \* actor) for source `src`; "reopened" once `who` opens `src` again
                \* through the hold (not as its first open after a start)

vars == <<alive, ovr, fresh, asked, load, eng, held, installed, plog, acked, ckpt,
          mfiles, mrecs, T, version, pubHome, intent, pc, A, aok, barrier, att,
          crashes, parts, cstep, csrc, rolledBack, inconsistent, lapses, first, restarts,
          unheldFresh, sib>>

placeVars == <<alive, ovr, fresh, asked, load>>
dataVars == <<plog, acked, ckpt, mfiles, mrecs, T, version, pubHome>>
actorVars == <<pc, A, aok, barrier, att>>
procVars == <<first, restarts, unheldFresh, sib>>
ghostVars == <<rolledBack, inconsistent, lapses, first, restarts, unheldFresh, sib>>

Live == intent.state = "live"
Lapsed == Live /\ intent.lapsed
Pick(s) == IF s = 0 THEN Ring0 ELSE Ring1
RingOwner(s) == IF Pick(s) \in alive THEN Pick(s) ELSE CHOOSE i \in alive : TRUE
Owner(s) == IF ovr[s] \in alive THEN ovr[s] ELSE RingOwner(s)
Resident(i) == load[i] + Cardinality({s \in Src : Owner(s) = i})
Room2(i) == Resident(i) + 2 <= Cap
RoomM(i) == Resident(i) + 1 <= Cap
ActorAlive == pc \in {"prepared", "owned1", "ckptEarly", "parked", "quiesced", "cloned",
                      "owned2", "t0", "t1", "published"}
BeforeT == pc \in {"prepared", "owned1", "ckptEarly", "parked", "quiesced", "cloned", "owned2"}
AnyT == T[0] # 0 \/ T[1] # 0

SourceServes(i, s) ==
    /\ eng[i][s] = "cur" /\ ~held[i][s] /\ installed[i] = 1
    /\ ~(i = A /\ barrier[s])
MServes(i) == version = 2 /\ installed[i] = 2 /\ i \in alive

LoadInit(i) == CASE i = "a" -> LoadA [] i = "b" -> LoadB [] OTHER -> 0

TypeOK ==
    /\ alive \subseteq Insts
    /\ eng \in [Insts -> [Src -> {"none", "cur", "stale", "maint"}]]
    /\ held \in [Insts -> [Src -> BOOLEAN]]
    /\ installed \in [Insts -> {1, 2}]
    /\ T \in [Src -> 0..MaxAtt]
    /\ version \in {1, 2}
    /\ intent.state \in {"none", "live", "done"}
    /\ cstep \in [Insts -> {"idle", "one", "both", "claimed1", "claimed"}]

Init ==
    /\ alive = InitAlive
    /\ ovr = [s \in Src |-> NoOne] /\ fresh = FALSE /\ asked = FALSE
    /\ load = [i \in Insts |-> LoadInit(i)]
    /\ eng = [i \in Insts |-> [s \in Src |-> IF i = Pick(s) THEN "cur" ELSE "none"]]
    /\ held = [i \in Insts |-> [s \in Src |-> FALSE]]
    /\ installed = [i \in Insts |-> 1]
    /\ plog = [s \in Src |-> FALSE] /\ acked = {}
    /\ ckpt = [h \in Homes |-> [s \in Src |-> 0]]
    /\ mfiles = [h \in Homes |-> FALSE] /\ mrecs = [h \in Homes |-> {}]
    /\ T = [s \in Src |-> 0] /\ version = 1 /\ pubHome = 0
    /\ intent = [state |-> "none", owner |-> NoOne, lapsed |-> FALSE, outcome |-> "",
                 home |-> 0]
    /\ pc = "idle" /\ A = NoOne /\ aok = [s \in Src |-> FALSE]
    /\ barrier = [s \in Src |-> FALSE] /\ att = 0
    /\ crashes = 0 /\ parts = 0
    /\ cstep = [i \in Insts |-> "idle"] /\ csrc = [i \in Insts |-> 0]
    /\ rolledBack = {} /\ inconsistent = FALSE /\ lapses = 0
    \* A process holding a shard at the start has opened an engine already.
    /\ first = [i \in Insts |-> LoadInit(i) = 0 /\ \A s \in Src : Pick(s) # i]
    /\ restarts = 0 /\ unheldFresh = FALSE /\ sib = [st |-> "no", who |-> NoOne, src |-> 0]

\* An open of source s on i: every other engine of s is fenced, the actor's
\* writer among them; the fence holds it under a live intent or a 'T'.
Fenced(s, i, kind) ==
    /\ eng' = [j \in Insts |-> [t \in Src |->
                 IF t # s THEN eng[j][t]
                 ELSE IF j = i THEN kind
                 ELSE IF eng[j][t] = "none" THEN "none" ELSE "stale"]]
    /\ aok' = [aok EXCEPT ![s] = FALSE]

---------------------------------------------------------------------------
\* Clients.

AppendSrc(s, i) ==
    /\ s \notin acked /\ SourceServes(i, s)
    /\ plog' = [plog EXCEPT ![s] = TRUE] /\ acked' = acked \cup {s}
    /\ UNCHANGED <<placeVars, eng, held, installed, ckpt, mfiles, mrecs, T, version,
                   pubHome, intent, actorVars, crashes, parts, cstep, csrc, ghostVars>>

AppendM(s, i) ==
    /\ s \notin acked /\ MServes(i)
    /\ mrecs' = [mrecs EXCEPT ![pubHome] = @ \cup {s}] /\ acked' = acked \cup {s}
    /\ UNCHANGED <<placeVars, eng, held, installed, plog, ckpt, mfiles, T, version,
                   pubHome, intent, actorVars, crashes, parts, cstep, csrc, ghostVars>>

\* A request routed to s's effective owner opens it there.
OpenOn(i, s) ==
    /\ i \in alive /\ Owner(s) = i /\ eng[i][s] = "none" /\ installed[i] = 1
    /\ HoldBeforeOpen => (~Live \/ intent.lapsed \/ (FirstOpenUnheld /\ first[i]))
    /\ version = 1 /\ cstep[i] = "idle"
    /\ Fenced(s, i, "cur")
    /\ held' = [held EXCEPT ![i][s] = Live \/ (HeldUnderT /\ T[s] # 0)]
    /\ first' = [first EXCEPT ![i] = FALSE]
    /\ unheldFresh' = (unheldFresh \/ (Live /\ ~intent.lapsed))
    /\ sib' = IF sib.st = "fenced" /\ sib.who = i /\ sib.src = s /\ ~first[i]
              THEN [sib EXCEPT !.st = "reopened"] ELSE sib
    /\ UNCHANGED <<placeVars, installed, dataVars, intent, pc, A, barrier, att, crashes,
                   parts, cstep, csrc, rolledBack, inconsistent, lapses, restarts>>

RequestOpen(i, s) == OpenOn(i, s)

\* The fleet tick's eager open, by the prefix's effective owner when it does
\* not hold it open: an overridden prefix (fleet.rs, "Eager handoff"), and,
\* with the fix (LapsedOpen; kind-agnostic in the code), a source of a live
\* intent whose lease lapsed, so its fence claims and decides. Without it, a
\* dead or Unfinished actor's sources open only on a client request.
EagerOpen(i, s) ==
    /\ EagerMoveIn
    /\ ovr[s] # NoOne \/ (LapsedOpen /\ Lapsed)
    /\ OpenOn(i, s)

Install(i) ==
    /\ installed[i] < version
    /\ installed' = [installed EXCEPT ![i] = version]
    /\ UNCHANGED <<placeVars, eng, held, dataVars, intent, actorVars, crashes, parts,
                   cstep, csrc, ghostVars>>

\* A fenced engine closes, and so does one its instance no longer owns or
\* routes to; a running actor keeps its own, and a claimant its opening one.
CloseEngine(i, s) ==
    /\ \/ eng[i][s] = "stale"
       \/ eng[i][s] = "cur" /\ (installed[i] = 2 \/ Owner(s) # i)
    /\ ~(i = A /\ ActorAlive)
    /\ cstep[i] = "idle"
    /\ eng' = [eng EXCEPT ![i][s] = "none"]
    /\ held' = [held EXCEPT ![i][s] = FALSE]
    /\ UNCHANGED <<placeVars, installed, dataVars, intent, actorVars, crashes, parts,
                   cstep, csrc, ghostVars>>

---------------------------------------------------------------------------
\* The policy and the fleet (before the merge starts).

PolicyReady == version = 1 /\ ~Live /\ ~fresh /\ att < MaxAtt /\ ~ActorAlive

\* The policy on the owner of …0 co-locates a pair split across two instances.
Colocate ==
    /\ PolicyReady /\ Owner(0) # Owner(1)
    /\ LET a == Owner(0)
           p == Owner(1)
       IN IF Room2(a) THEN /\ ovr' = [s \in Src |-> a] /\ fresh' = TRUE
                           /\ UNCHANGED asked
          ELSE IF PartnerFallback /\ Room2(p)
               THEN /\ ovr' = [s \in Src |-> p] /\ fresh' = TRUE
                    /\ UNCHANGED asked
          ELSE /\ AskWhenNoRoom /\ ~asked /\ Spare \notin alive
               /\ asked' = TRUE /\ UNCHANGED <<ovr, fresh>>
    /\ UNCHANGED <<alive, load, eng, held, installed, dataVars, intent, actorVars,
                   crashes, parts, cstep, csrc, ghostVars>>

\* A local pair whose instance cannot hold M beside it asks for an instance.
AskForM ==
    /\ PolicyReady /\ Owner(0) = Owner(1) /\ ~RoomM(Owner(0))
    /\ AskWhenNoRoom /\ ~asked /\ Spare \notin alive
    /\ asked' = TRUE
    /\ UNCHANGED <<alive, ovr, fresh, load, eng, held, installed, dataVars, intent,
                   actorVars, crashes, parts, cstep, csrc, ghostVars>>

\* The move's cooldown passes.
Settle ==
    /\ fresh /\ fresh' = FALSE
    /\ UNCHANGED <<alive, ovr, asked, load, eng, held, installed, dataVars, intent,
                   actorVars, crashes, parts, cstep, csrc, ghostVars>>

\* The fleet adds the spare instance it was asked for.
FleetAdd ==
    /\ asked /\ Spare \notin alive
    /\ alive' = alive \cup {Spare} /\ asked' = FALSE
    /\ UNCHANGED <<ovr, fresh, load, eng, held, installed, dataVars, intent, actorVars,
                   crashes, parts, cstep, csrc, ghostVars>>

\* Count balance moves one other shard onto the emptier spare.
Balance(i) ==
    /\ Spare \in alive /\ i # Spare /\ load[i] > load[Spare]
    /\ load' = [load EXCEPT ![i] = @ - 1, ![Spare] = @ + 1]
    /\ UNCHANGED <<alive, ovr, fresh, asked, eng, held, installed, dataVars, intent,
                   actorVars, crashes, parts, cstep, csrc, ghostVars>>

---------------------------------------------------------------------------
\* The actor.

Begin ==
    /\ PolicyReady /\ Owner(0) = Owner(1)
    /\ LET a == Owner(0) IN
       /\ RoomM(a)
       /\ \A s \in Src : eng[a][s] = "cur" /\ ~held[a][s] /\ T[s] = 0
       /\ cstep[a] = "idle"
       /\ intent' = [state |-> "live", owner |-> a, lapsed |-> FALSE, outcome |-> "",
                     home |-> att + 1]
       /\ A' = a
    /\ att' = att + 1 /\ pc' = "prepared"
    /\ aok' = [s \in Src |-> TRUE] /\ barrier' = [s \in Src |-> FALSE]
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, crashes, parts, cstep,
                   csrc, ghostVars>>

\* The actor's own lease check (still_owned): a lease only its clock saw
\* lapse is re-claimed; one a claimant took ends the run.
StillOwned(from, to) ==
    /\ pc = from /\ Live /\ intent.owner = A
    /\ intent' = [intent EXCEPT !.lapsed = FALSE] /\ pc' = to
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, A, aok, barrier, att,
                   crashes, parts, cstep, csrc, ghostVars>>

\* Before 'T', a failed step (a lost lease, a fenced source's flush) rolls
\* back: the barriers resume, M's lists go (M's files stay, never a leaf),
\* and the terminal CAS lands only if the intent is still the actor's.
Abort ==
    /\ BeforeT
    /\ \/ pc \in {"prepared", "cloned"} /\ ~(Live /\ intent.owner = A)
       \/ pc = "parked" /\ ~(aok[0] /\ aok[1])
    /\ intent' = IF Live /\ intent.owner = A
                 THEN [intent EXCEPT !.state = "done", !.outcome = "rb"] ELSE intent
    /\ rolledBack' = IF Live /\ intent.owner = A THEN rolledBack \cup {intent.home}
                     ELSE rolledBack
    /\ pc' = "stopped" /\ barrier' = [s \in Src |-> FALSE]
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, A, aok, att, crashes,
                   parts, cstep, csrc, inconsistent, lapses, procVars>>

Park ==
    /\ \/ pc = "owned1" /\ BarrierBeforeCkpt
       \/ pc = "ckptEarly"
    /\ barrier' = [s \in Src |-> TRUE] /\ pc' = IF pc = "owned1" THEN "parked" ELSE "quiesced"
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, intent, A, aok, att,
                   crashes, parts, cstep, csrc, ghostVars>>

\* Both sources flushed and checkpointed; a fenced source's flush fails.
Checkpoint ==
    /\ \/ pc = "parked"
       \/ pc = "owned1" /\ ~BarrierBeforeCkpt
    /\ aok[0] /\ aok[1]
    /\ ckpt' = [ckpt EXCEPT ![intent.home] =
                  [s \in Src |-> IF plog[s] THEN 2 ELSE 1]]
    /\ pc' = IF pc = "parked" THEN "quiesced" ELSE "ckptEarly"
    /\ UNCHANGED <<placeVars, eng, held, installed, plog, acked, mfiles, mrecs, T,
                   version, pubHome, intent, A, aok, barrier, att, crashes, parts,
                   cstep, csrc, ghostVars>>

\* The union clone of M from both checkpoints, unfenced; a path that already
\* holds a clone is left as it is.
Clone ==
    /\ pc = "quiesced"
    /\ LET h == intent.home IN
       /\ mfiles' = [mfiles EXCEPT ![h] = TRUE]
       /\ mrecs' = IF mfiles[h] THEN mrecs
                   ELSE [mrecs EXCEPT ![h] =
                           {s \in Src : ckpt[h][s] = 2 /\ (CloneBoth \/ s = 0)}]
    /\ pc' = "cloned"
    /\ UNCHANGED <<placeVars, eng, held, installed, plog, acked, ckpt, T, version,
                   pubHome, intent, A, aok, barrier, att, crashes, parts, cstep, csrc,
                   ghostVars>>

\* A 'T' write: only a current writer's lands; a failed one leaves the merge
\* Unfinished (it only rolls forward): the intent stays live and the dropped
\* barriers close both sources.
Unfinished ==
    /\ pc' = "unfinished" /\ barrier' = [s \in Src |-> FALSE]
    /\ eng' = [eng EXCEPT ![A] = [s \in Src |-> "none"]]
    /\ held' = [held EXCEPT ![A] = [s \in Src |-> FALSE]]

Retire(s, from, to) ==
    /\ pc = from
    /\ IF aok[s]
       THEN /\ T' = [T EXCEPT ![s] = intent.home] /\ pc' = to
            /\ UNCHANGED <<eng, held, barrier>>
       ELSE /\ Unfinished /\ UNCHANGED T
    /\ UNCHANGED <<placeVars, installed, plog, acked, ckpt, mfiles, mrecs, version,
                   pubHome, intent, A, aok, att, crashes, parts, cstep, csrc, ghostVars>>

\* The topology CAS; a claimant's earlier publish makes the actor's `merged`
\* fail (Unfinished). The sources close once the held operations re-route.
Publish ==
    /\ pc = "t1"
    /\ IF version = 1
       THEN /\ version' = 2 /\ pubHome' = intent.home
            /\ installed' = [installed EXCEPT ![A] = 2] /\ pc' = "published"
            /\ barrier' = [s \in Src |-> FALSE]
            /\ eng' = [eng EXCEPT ![A] = [s \in Src |-> "none"]]
            /\ held' = [held EXCEPT ![A] = [s \in Src |-> FALSE]]
       ELSE /\ Unfinished /\ UNCHANGED <<version, pubHome, installed>>
    /\ UNCHANGED <<placeVars, plog, acked, ckpt, mfiles, mrecs, T, intent, A, aok, att,
                   crashes, parts, cstep, csrc, ghostVars>>

Reclaim ==
    /\ pc = "published"
    /\ intent' = IF Live /\ intent.owner = A
                 THEN [intent EXCEPT !.state = "done", !.outcome = "pub"] ELSE intent
    /\ pc' = "done"
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, A, aok, barrier, att,
                   crashes, parts, cstep, csrc, ghostVars>>

\* The renewal loop, while the actor's steps run.
Renew ==
    /\ ActorAlive /\ Lapsed /\ intent.owner = A
    /\ intent' = [intent EXCEPT !.lapsed = FALSE]
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, actorVars, crashes, parts,
                   cstep, csrc, ghostVars>>

\* The lease's owner is still at work: the actor while it runs, a claimant
\* between its claim and its act.
Working == \/ intent.owner = A /\ ActorAlive
           \/ intent.owner \in Insts /\ cstep[intent.owner] \in {"claimed1", "claimed"}

\* A lapse is observed whatever its owner does: the lease does not fence. A
\* stopped owner's lease lapses (weakly fair); a working owner's lapses only
\* MaxLapses times (eventual timeliness: a working owner eventually finishes
\* within its lease; with none at all, no lease protocol is live).
LeaseLapse ==
    /\ Live /\ ~intent.lapsed
    /\ (BoundLapses /\ Working) => lapses < MaxLapses
    /\ intent' = [intent EXCEPT !.lapsed = TRUE]
    /\ lapses' = IF BoundLapses /\ Working THEN lapses + 1 ELSE lapses
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, actorVars, crashes, parts,
                   cstep, csrc, rolledBack, inconsistent, procVars>>

\* An instance crashes (and restarts with nothing open): its actor, if it ran
\* there, and its claimant are gone.
Crash(i) ==
    /\ crashes < MaxCrashes /\ i \in alive
    /\ \/ i = A /\ ActorAlive
       \/ cstep[i] # "idle"
    /\ crashes' = crashes + 1
    /\ eng' = [eng EXCEPT ![i] = [s \in Src |-> "none"]]
    /\ held' = [held EXCEPT ![i] = [s \in Src |-> FALSE]]
    /\ cstep' = [cstep EXCEPT ![i] = "idle"]
    /\ first' = [first EXCEPT ![i] = TRUE]
    /\ IF i = A /\ ActorAlive
       THEN pc' = "dead" /\ barrier' = [s \in Src |-> FALSE]
       ELSE UNCHANGED <<pc, barrier>>
    /\ UNCHANGED <<placeVars, installed, dataVars, intent, A, aok, att, parts, csrc,
                   rolledBack, inconsistent, lapses, restarts, unheldFresh, sib>>

\* A bystander process restarts (a deploy): neither a running actor nor a
\* claimant, it drops its engines and its next open is its first (unheld under
\* FirstOpenUnheld). Finitely often when BoundRestarts; never fair.
Restart(i) ==
    /\ i \in alive /\ cstep[i] = "idle" /\ ~(i = A /\ ActorAlive)
    /\ BoundRestarts => restarts < MaxRestarts
    /\ restarts' = IF BoundRestarts THEN restarts + 1 ELSE restarts
    /\ eng' = [eng EXCEPT ![i] = [s \in Src |-> "none"]]
    /\ held' = [held EXCEPT ![i] = [s \in Src |-> FALSE]]
    /\ first' = [first EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<placeVars, installed, dataVars, intent, actorVars, crashes, parts, cstep,
                   csrc, rolledBack, inconsistent, lapses, unheldFresh, sib>>

\* The actor's instance drops out of the ring while the actor runs on.
Partition ==
    /\ ActorAlive /\ A \in alive /\ parts < MaxParts /\ Cardinality(alive) >= 2
    /\ alive' = alive \ {A} /\ parts' = parts + 1
    /\ UNCHANGED <<ovr, fresh, asked, load, eng, held, installed, dataVars, intent,
                   actorVars, crashes, cstep, csrc, ghostVars>>

Heal ==
    /\ A # NoOne /\ A \notin alive /\ A \in InitAlive
    /\ alive' = alive \cup {A}
    /\ UNCHANGED <<ovr, fresh, asked, load, eng, held, installed, dataVars, intent,
                   actorVars, crashes, parts, cstep, csrc, ghostVars>>

---------------------------------------------------------------------------
\* The open-path fence and the claimant.

\* A held open re-reads the fence: it serves once nothing names it, and a
\* lapsed intent or an unpublished 'T' starts a claim.
FenceStep(i, s) ==
    /\ eng[i][s] = "cur" /\ held[i][s] /\ cstep[i] = "idle" /\ installed[i] = 1
    /\ CASE T[s] = 0 /\ ~Live ->
              /\ held' = [held EXCEPT ![i][s] = FALSE] /\ UNCHANGED <<cstep, csrc>>
         [] (T[s] = 0 /\ Lapsed) \/ (T[s] # 0 /\ version = 1 /\ (~Live \/ Lapsed)) ->
              /\ cstep' = [cstep EXCEPT ![i] = "one"] /\ csrc' = [csrc EXCEPT ![i] = s]
              /\ UNCHANGED held
         [] OTHER -> FALSE
    /\ UNCHANGED <<placeVars, eng, installed, dataVars, intent, actorVars, crashes, parts,
                   ghostVars>>

\* The maintenance open of the other source (never served): it fences that
\* source's previous writer, the actor's among them.
ClaimFence(i) ==
    /\ cstep[i] = IF ClaimBeforeMaint THEN "claimed1" ELSE "one"
    /\ MaintOpenHolds => (~Live \/ intent.lapsed \/ (MaintExemptOwn /\ intent.owner = i))
    /\ IF ClaimFencesBoth
       THEN /\ Fenced(Other(csrc[i]), i, "maint")
            /\ held' = [held EXCEPT ![i][Other(csrc[i])] = TRUE]
       ELSE UNCHANGED <<eng, aok, held>>
    /\ cstep' = [cstep EXCEPT ![i] = IF ClaimBeforeMaint THEN "claimed" ELSE "both"]
    /\ LET o == Other(csrc[i])
           Sibs == {j \in Insts \ {i} : eng[j][o] = "cur" /\ ~(j = A /\ ActorAlive)}
       IN sib' = IF ClaimFencesBoth /\ Sibs # {}
                 THEN [st |-> "fenced", who |-> CHOOSE j \in Sibs : TRUE, src |-> o]
                 ELSE sib
    /\ UNCHANGED <<placeVars, installed, dataVars, intent, pc, A, barrier, att, crashes,
                   parts, csrc, rolledBack, inconsistent, lapses, first, restarts,
                   unheldFresh>>

\* The claimant can still write source s: its engine there is the current
\* writer. Only a roll forward writes a source ('T'); the claim CAS, the
\* reads, a roll back's deletions and the publish do not (claim_and_act
\* checks no engine before them).
CanWrite(i, s) == eng[i][s] \in {"cur", "maint"}
ForwardBlocked(i) == \E s \in Src : T[s] = 0 /\ ~CanWrite(i, s)

\* Close the claimant's maintenance engine and end its claim attempt.
EndClaim(i) ==
    /\ cstep' = [cstep EXCEPT ![i] = "idle"]
    /\ eng' = [eng EXCEPT ![i][Other(csrc[i])] =
                  IF @ = "maint" THEN "none" ELSE @]
    /\ held' = [held EXCEPT ![i][Other(csrc[i])] =
                  IF eng[i][Other(csrc[i])] = "maint" THEN FALSE ELSE @]

\* The claim CAS: over a lapsed intent it takes it; with none live it goes on
\* from 'T' alone; a lease renewed or taken meanwhile fails it.
ClaimCAS(i) ==
    /\ cstep[i] = IF ClaimBeforeMaint THEN "one" ELSE "both"
    /\ LET next == IF ClaimBeforeMaint THEN "claimed1" ELSE "claimed" IN
       IF Lapsed
       THEN /\ intent' = [intent EXCEPT !.owner = i, !.lapsed = FALSE]
            /\ cstep' = [cstep EXCEPT ![i] = next]
            /\ UNCHANGED <<eng, held>>
       ELSE IF ~Live
       THEN /\ cstep' = [cstep EXCEPT ![i] = next]
            /\ UNCHANGED <<intent, eng, held>>
       ELSE /\ EndClaim(i) /\ UNCHANGED intent
    /\ UNCHANGED <<placeVars, installed, dataVars, actorVars, crashes, parts, csrc,
                   ghostVars>>

\* A roll forward whose 'T' write is fenced fails; the held open re-reads later.
Abandon(i) ==
    /\ cstep[i] = "claimed" /\ version = 1 /\ AnyT /\ ForwardBlocked(i)
    /\ EndClaim(i)
    /\ UNCHANGED <<placeVars, installed, dataVars, intent, actorVars, crashes, parts,
                   csrc, ghostVars>>

\* S6 §7.4's table, from both fenced sources.
Act(i) ==
    /\ cstep[i] = "claimed"
    /\ LET o == Other(csrc[i])
           h == IF Live THEN intent.home
                ELSE IF T[csrc[i]] # 0 THEN T[csrc[i]] ELSE T[o]
           mine == Live /\ intent.owner = i
           Ended(out) == IF mine THEN [intent EXCEPT !.state = "done", !.outcome = out]
                         ELSE intent
           allT == T[0] # 0 /\ T[1] # 0
       IN CASE ~Live /\ ~AnyT /\ version = 1 ->
                 \* the intent ended meanwhile and nothing retired: nothing to recover
                 /\ UNCHANGED <<T, version, pubHome, installed, mfiles, mrecs, ckpt, intent,
                                rolledBack, inconsistent>>
                 /\ EndClaim(i)
            [] version = 2 ->
                 /\ intent' = Ended("pub")
                 /\ UNCHANGED <<T, version, pubHome, installed, mfiles, mrecs, ckpt,
                                rolledBack, inconsistent>>
                 /\ EndClaim(i)
            [] AnyT /\ (RollBackNeedsNoT \/ allT) /\ mfiles[h] /\ ~ForwardBlocked(i) ->
                 \* roll forward: the missing 'T', the publish, Reclaimed
                 /\ T' = [s \in Src |-> h]
                 /\ version' = 2 /\ pubHome' = h
                 /\ installed' = [installed EXCEPT ![i] = 2]
                 /\ intent' = Ended("pub")
                 /\ UNCHANGED <<mfiles, mrecs, ckpt, rolledBack, inconsistent>>
                 /\ EndClaim(i)
            [] AnyT /\ (RollBackNeedsNoT \/ allT) /\ ~mfiles[h] ->
                 /\ inconsistent' = TRUE
                 /\ UNCHANGED <<T, version, pubHome, installed, mfiles, mrecs, ckpt,
                                intent, rolledBack>>
                 /\ EndClaim(i)
            [] ~AnyT \/ ~(RollBackNeedsNoT \/ allT) ->
                 \* roll back with the takeover GC; serve once the intent ended
                 /\ mfiles' = [mfiles EXCEPT ![h] = FALSE]
                 /\ mrecs' = [mrecs EXCEPT ![h] = {}]
                 /\ ckpt' = [ckpt EXCEPT ![h] = [s \in Src |-> 0]]
                 /\ intent' = Ended("rb")
                 /\ rolledBack' = IF mine THEN rolledBack \cup {h} ELSE rolledBack
                 /\ UNCHANGED <<T, version, pubHome, installed, inconsistent>>
                 /\ cstep' = [cstep EXCEPT ![i] = "idle"]
                 /\ eng' = [eng EXCEPT ![i][o] = IF @ = "maint" THEN "none" ELSE @]
                 /\ held' = [held EXCEPT ![i][o] = IF eng[i][o] = "maint" THEN FALSE ELSE @,
                                         ![i][csrc[i]] = Live /\ ~mine]
                    \* still held while another's live intent names it
            [] OTHER -> FALSE  \* a roll forward whose 'T' write is fenced: Abandon
    /\ UNCHANGED <<placeVars, plog, acked, actorVars, crashes, parts, csrc, lapses,
                   procVars>>

\* The heartbeat's watch (and every split's or merge's start) ends a live
\* intent whose operation the stored topology names (833bcdc5b).
ReclaimPublished ==
    /\ HeartbeatReclaim
    /\ Live /\ (version = 2 \/ ~ReclaimNeedsPublish)
    /\ intent' = [intent EXCEPT !.state = "done", !.outcome = "pub"]
    /\ UNCHANGED <<placeVars, eng, held, installed, dataVars, actorVars, crashes, parts,
                   cstep, csrc, ghostVars>>

---------------------------------------------------------------------------
Next ==
    \/ \E s \in Src, i \in Insts :
          \/ AppendSrc(s, i) \/ AppendM(s, i) \/ RequestOpen(i, s) \/ EagerOpen(i, s)
          \/ CloseEngine(i, s) \/ FenceStep(i, s)
    \/ \E i \in Insts : Install(i) \/ Balance(i) \/ Crash(i) \/ Restart(i)
                        \/ ClaimFence(i) \/ ClaimCAS(i) \/ Abandon(i) \/ Act(i)
    \/ Colocate \/ AskForM \/ Settle \/ FleetAdd
    \/ Begin \/ StillOwned("prepared", "owned1") \/ StillOwned("cloned", "owned2")
    \/ Abort \/ Park \/ Checkpoint \/ Clone
    \/ Retire(0, "owned2", "t0") \/ Retire(1, "t0", "t1") \/ Publish \/ Reclaim
    \/ Renew \/ LeaseLapse \/ Partition \/ Heal \/ ReclaimPublished

Spec == Init /\ [][Next]_vars

\* Weak fairness only where the code retries: the actor's steps, the lease's
\* observation, the fence's re-reads and the claimant's steps, closes,
\* installs, the fleet tick (eager move-in, co-location, settle, balance),
\* the platform adding an instance, the heartbeat's reclaim, a partition's
\* heal. A client request is fair only for a hot pair (RequestsFair).
Fairness ==
    /\ WF_vars(Begin) /\ WF_vars(StillOwned("prepared", "owned1"))
    /\ WF_vars(StillOwned("cloned", "owned2")) /\ WF_vars(Abort) /\ WF_vars(Park)
    /\ WF_vars(Checkpoint) /\ WF_vars(Clone) /\ WF_vars(Retire(0, "owned2", "t0"))
    /\ WF_vars(Retire(1, "t0", "t1")) /\ WF_vars(Publish) /\ WF_vars(Reclaim)
    /\ WF_vars(LeaseLapse) /\ WF_vars(Heal) /\ WF_vars(ReclaimPublished)
    /\ WF_vars(Colocate) /\ WF_vars(AskForM) /\ WF_vars(Settle) /\ WF_vars(FleetAdd)
    /\ \A i \in Insts :
          /\ WF_vars(Install(i)) /\ WF_vars(Balance(i)) /\ WF_vars(ClaimFence(i))
          /\ WF_vars(ClaimCAS(i)) /\ WF_vars(Abandon(i)) /\ WF_vars(Act(i))
    /\ \A i \in Insts, s \in Src :
          /\ WF_vars(EagerOpen(i, s)) /\ WF_vars(CloseEngine(i, s))
          /\ IF ClaimRaceFair THEN SF_vars(FenceStep(i, s)) ELSE WF_vars(FenceStep(i, s))
          /\ RequestsFair => WF_vars(RequestOpen(i, s))

LiveSpec == Spec /\ Fairness

---------------------------------------------------------------------------
\* Properties.

\* Every acked record is readable in the leaf of the stored topology that
\* covers it: its source before the merge, M after.
AckedReadable ==
    \A s \in acked :
        IF version = 1 THEN plog[s] ELSE s \in mrecs[pubHome] /\ mfiles[pubHome]

\* One owner per key range: never a source beside M, never two source writers.
NoDualServe ==
    /\ ~((\E i \in Insts, s \in Src : SourceServes(i, s)) /\ (\E j \in Insts : MServes(j)))
    /\ \A s \in Src : Cardinality({i \in Insts : SourceServes(i, s)}) <= 1

\* A source whose log holds 'T' never serves.
RetiredNeverServes == \A s \in Src : T[s] # 0 => ~\E i \in Insts : SourceServes(i, s)

\* No attempt that rolled back has a durable 'T', before or after its end.
NoRollbackWithT == \A h \in rolledBack : T[0] # h /\ T[1] # h

\* A claimant never finds 'T' with M incomplete ('T' follows M's clone).
NeverInconsistent == ~inconsistent

\* An intent ends with 'T' in a source only once the stored topology published M: no
\* 'T'd source is left with no live intent and no publish, which only a client request
\* could reopen (TLA-045's EndAfterT branch; here every terminal write is a CAS on the
\* writer's own lease or claim, and a roll back needs 'T' in neither source).
NoTEndedUnpublished == (intent.state = "done" /\ AnyT) => version = 2

\* Witnesses (each violated on the unmodified model: the state is reachable).
Witness_Published == ~(version = 2 /\ intent.state = "done" /\ acked = Src)
Witness_ClaimRolledBack ==
    ~(intent.outcome = "rb" /\ intent.owner # A /\ \E i \in Insts, s \in Src : SourceServes(i, s))
Witness_ForwardFromOneT == ~(version = 2 /\ intent.owner # A /\ intent.owner \in Insts)
Witness_UnfinishedThenForward == ~(version = 2 /\ pc = "unfinished")
\* An actor that never crashed ended Unfinished with no 'T' durable (its 'T'
\* write fenced), and the claimant the eager open started rolled it back.
Witness_UnfinishedThenRolledBack ==
    ~(pc = "unfinished" /\ crashes = 0 /\ intent.outcome = "rb")
Witness_PartnerColocated == ~(version = 2 /\ A = Ring1 /\ Ring0 # Ring1)
Witness_SpareAdded == ~(version = 2 /\ Spare \in alive)
\* A process's unheld first open fenced the sources under a fresh lease or claim.
Witness_UnheldFirstOpen == ~unheldFresh
\* A claimant's maintenance open fenced another instance's open engine of that
\* source (the fenced sibling), that engine closed, and the source was opened
\* again, through the same pre-open hold.
Witness_FencedSiblingReopens == sib.st # "reopened"

\* Liveness.
\* A started (or lapsed) merge eventually ends.
OperationCompletes == Live ~> (intent.state = "done")
\* After any 'T', the merge is eventually published.
ForwardAfterT == AnyT ~> (version = 2)
\* A cold pair eventually merges (co-location configs: no crash, no request).
MergeHappens == <>(version = 2)
=============================================================================
