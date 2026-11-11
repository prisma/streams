------------------------ MODULE TopologyReconfiguration ------------------------
(***************************************************************************)
(* TLA-045 (S4-DESIGN rev 2.1 §12): the split; the merge is TLA-048.       *)
(* A shard split and its recovery (`src/topology/split.rs`, `actor.rs`,    *)
(* `fence.rs`, `recover.rs`, `ops.rs`, `store.rs`, `intent.rs`).           *)
(*                                                                         *)
(* One leaf P splits into its halves P0 and P1; two records, r0 and r1,    *)
(* one in each half. The actor runs on instance Actor; another instance   *)
(* can open the parent's log (a move, a takeover after a crash).          *)
(*                                                                         *)
(* The actor's steps, as `Started::finish` runs them: Begin (the home CAS, *)
(* then the intent), Park (the committer parks after the barrier's group), *)
(* Checkpoint (`topo-<op_id>`), Clone (both children, seeded, from the     *)
(* checkpoint; a clone writes the child path and CASes the parent's        *)
(* manifest, which no writer epoch fences: S4-REVIEW-rev2 N1; a clone into *)
(* a path that already holds a clone resumes it), Retire (`'T'`, a write   *)
(* to the parent's log, which a newer writer epoch fences), Publish (the   *)
(* topology CAS, then the barrier's release to the children) and Reclaim. *)
(* The actor may crash once, anywhere. Once the topology names the split, *)
(* any instance that reads its live intent ends it (ReclaimPublished,     *)
(* `topology::reclaim`): a split's start and every heartbeat's watch.     *)
(*                                                                         *)
(* The lease does not fence (S4-REVIEW finding 1): LeaseLapse stays        *)
(* enabled while the actor runs and renews, and a slow actor's steps stay  *)
(* enabled after a claim; only the writer epoch stops its `'T'`. The claim *)
(* is an action of an open: an opener of the parent first bumps the       *)
(* writer epoch (fencing every older writer), then re-reads `'T'` and the  *)
(* intent (`fence::read`), and claims a lapsed intent by CAS               *)
(* (`recover::claim`) before it rolls back (no `'T'`: the children's paths *)
(* and the checkpoint deleted, the intent terminal) or forward (`'T'`: the *)
(* topology `'T'` names published; `'T'` alone suffices, any intent).      *)
(*                                                                         *)
(* Homes are allocated per attempt and never reused; a stale actor's late  *)
(* clone into a rolled-back attempt's paths is never published (the       *)
(* never-leaf contract, ASM-LINEAGE-CONTRACT).                            *)
(*                                                                         *)
(* The 'T' outcome (push 4; DESIGN-DECISION-c3-incomplete-rig Addendum 6): *)
(* an attempted 'T' counts as written, and the actor never resumes after   *)
(* it. Retire is atomic here (it writes 'T' at aepoch = epoch, or the      *)
(* actor stops with no 'T'), and the actor has no rollback of its own      *)
(* after Retire: in the code any put or flush error of the 'T' write is    *)
(* Retiring::After, Unfinished with no resume and no clear. 'T' landed:    *)
(* Retire, the actor stops or crashes, the claimant rolls forward; it did  *)
(* not: the actor stops with no 'T', the claimant rolls back. No source    *)
(* write follows 'T' (Addendum 5: the gate's final flag, set under the     *)
(* exclusive hold, refuses every index batch after it), so the children's  *)
(* rows are what the clones copied.                                        *)
(*                                                                         *)
(* Placement and routing are abstract: a record lives in the half its hash *)
(* names (ASM-ROUTE-PREFIX, KANI-106); a request is served by the engine   *)
(* of the leaf its instance's installed topology names. No liveness beyond *)
(* ForwardAfterRetire and OperationCompletes, under `Fairness`.            *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Insts,      \* instances that may open the parent's log
    Actor,      \* the instance the split runs on
    MaxEpoch,   \* writer epochs the parent's log may reach
    MaxHomes    \* split attempts (each takes fresh homes)

\* The operators a negative control replaces (`MC_TopologyReconfiguration`):
\* TRUE in the baseline.
CONSTANTS
    FenceAtOpen,        \* an open is held while an operation names the parent
    PublishNeedsT,      \* the publish follows the parent's 'T'
    RollBackNeedsNoT,   \* the claimant rolls back only without 'T'
    BarrierBeforeCkpt,  \* the checkpoint follows the barrier's park
    FreshHome,          \* each attempt takes a home no attempt took
    ClaimAfterFence,    \* the claim decides from a read after the opener's epoch bump
    ClaimOpens,         \* the claimant opens (fences) the parent before it decides
    ForwardFromT,       \* 'T' alone rolls forward, whatever the intent
    ActorStaysFenced,   \* a fenced actor never re-opens the parent mid-split
    HeldUnderT          \* an open whose log holds 'T' never serves

Recs == {"r0", "r1"}
Half(r) == IF r = "r0" THEN 0 ELSE 1
Homes == 1..MaxHomes
Paths == Homes \X {0, 1}
None == {"none"}  \* an absent path (a set, so it compares with path contents)

VARIABLES
    version,    \* the stored topology: 1 (P a leaf) or 2 (P0 and P1)
    published,  \* the home the stored topology's children live at (0 before)
    installed,  \* [Insts -> {1, 2}]: the topology each instance routes by
    nextHome,   \* the stored topology's `next_home`
    intent,     \* the cell's intent
    retired,    \* the parent log's 'T': the home it names, 0 = none
    epoch,      \* the parent log's writer epoch
    engine,     \* [Insts -> 0..MaxEpoch]: each instance's parent engine's epoch, 0 = none
    held,       \* [Insts -> BOOLEAN]: the fence holds that engine's open
    plog,       \* records durable in the parent's log
    acked,      \* records acked to a client
    child,      \* [Paths -> None or a set of records]: a child path's contents
    pc,         \* the actor's step
    aepoch,     \* the actor's writer epoch on the parent
    ahome,      \* the actor's attempt's home
    acopy,      \* the checkpoint the actor cloned from (in its memory)
    barrier,    \* the actor's barrier holds the parent's appends
    crashed,    \* the actor crashed (once)
    used,       \* ghost: the homes attempts took
    attempts    \* ghost: the attempts begun

vars == <<version, published, installed, nextHome, intent, retired, epoch, engine,
          held, plog, acked, child, pc, aepoch, ahome, acopy, barrier, crashed,
          used, attempts>>

Live == intent.state = "live"
Lapsed == Live /\ intent.lapsed

\* An engine of the parent serves: it is the current writer, the fence let it
\* serve, its instance routes P to it, and the actor's barrier does not park it.
ParentServes(i) ==
    /\ engine[i] # 0 /\ engine[i] = epoch
    /\ ~held[i]
    /\ installed[i] = 1
    /\ ~(i = Actor /\ barrier)

\* The children serve half h on an instance that routes by the published
\* topology, from the paths it names.
ChildServes(i, h) ==
    /\ version = 2 /\ installed[i] = 2
    /\ child[<<published, h>>] # None

TypeOK ==
    /\ version \in {1, 2}
    /\ published \in 0..MaxHomes
    /\ installed \in [Insts -> {1, 2}]
    /\ nextHome \in 0..MaxHomes
    /\ intent.state \in {"none", "live", "done"}
    /\ retired \in 0..MaxHomes
    /\ epoch \in 1..MaxEpoch
    /\ engine \in [Insts -> 0..MaxEpoch]
    /\ held \in [Insts -> BOOLEAN]
    /\ plog \subseteq Recs /\ acked \subseteq Recs
    /\ pc \in {"idle", "prepared", "parked", "quiesced", "cloned", "retired",
               "published", "done", "stopped", "dead"}

Init ==
    /\ version = 1 /\ published = 0
    /\ installed = [i \in Insts |-> 1]
    /\ nextHome = 0
    /\ intent = [state |-> "none", owner |-> Actor, lapsed |-> FALSE, home |-> 0]
    /\ retired = 0
    /\ epoch = 1
    /\ engine = [i \in Insts |-> IF i = Actor THEN 1 ELSE 0]
    /\ held = [i \in Insts |-> FALSE]
    /\ plog = {} /\ acked = {}
    /\ child = [p \in Paths |-> None]
    /\ pc = "idle" /\ aepoch = 1 /\ ahome = 0 /\ acopy = {}
    /\ barrier = FALSE /\ crashed = FALSE
    /\ used = {} /\ attempts = 0

---------------------------------------------------------------------------
\* Clients and routing.

\* An append of r acked by the parent: written to its log in the committer's group.
AppendParent(r, i) ==
    /\ r \notin acked /\ ParentServes(i)
    /\ plog' = plog \cup {r} /\ acked' = acked \cup {r}
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   engine, held, child, pc, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* An append of r acked by its child (a held append re-routed, or a later one).
AppendChild(r, i) ==
    /\ r \notin acked /\ ChildServes(i, Half(r))
    /\ child' = [child EXCEPT ![<<published, Half(r)>>] = @ \cup {r}]
    /\ acked' = acked \cup {r}
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   engine, held, plog, pc, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* An instance revalidates its topology (the fleet tick, or a forward hop).
Install(i) ==
    /\ installed[i] < version
    /\ installed' = [installed EXCEPT ![i] = version]
    /\ UNCHANGED <<version, published, nextHome, intent, retired, epoch, engine,
                   held, plog, acked, child, pc, aepoch, ahome, acopy, barrier,
                   crashed, used, attempts>>

---------------------------------------------------------------------------
\* The actor (`split::start`, then `Started::finish`).

ActorAlive == pc \notin {"idle", "done", "stopped", "dead"}

\* Begin: the home CAS (next_home += 1 attempt's homes), then the intent; the
\* actor's engine is the parent's current, serving writer.
Begin ==
    /\ pc \in {"idle", "stopped", "done"} /\ version = 1 /\ retired = 0 /\ ~Live
    /\ ParentServes(Actor)
    /\ attempts < MaxHomes
    /\ LET h == IF FreshHome \/ nextHome = 0 THEN nextHome + 1 ELSE nextHome
       IN /\ nextHome' = h
          /\ intent' = [state |-> "live", owner |-> Actor, lapsed |-> FALSE, home |-> h]
          /\ ahome' = h
          /\ used' = used \cup {h}
    /\ attempts' = attempts + 1
    /\ pc' = "prepared" /\ aepoch' = engine[Actor]
    /\ UNCHANGED <<version, published, installed, retired, epoch, engine, held,
                   plog, acked, child, acopy, barrier, crashed>>

\* Park: the committer parks after the group that carries the barrier.
Park ==
    /\ pc = "prepared"
    /\ barrier' = TRUE /\ pc' = "parked"
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   engine, held, plog, acked, child, aepoch, ahome, acopy, crashed,
                   used, attempts>>

\* Checkpoint: the WAL flushed, then `topo-<op_id>` of everything.
Checkpoint ==
    /\ \/ pc = "parked"
       \/ ~BarrierBeforeCkpt /\ pc = "prepared"
    /\ acopy' = plog
    /\ pc' = "quiesced"
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   engine, held, plog, acked, child, aepoch, ahome, barrier, crashed,
                   used, attempts>>

\* NC only (BarrierBeforeCkpt false): the barrier parks after the checkpoint.
ParkLate ==
    /\ ~BarrierBeforeCkpt /\ pc = "quiesced" /\ ~barrier
    /\ barrier' = TRUE
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   engine, held, plog, acked, child, pc, aepoch, ahome, acopy, crashed,
                   used, attempts>>

\* Clone: both children from the checkpoint the actor took, into its home's
\* paths, unfenced; a path that already holds a clone is resumed as it is.
Clone ==
    /\ pc = "quiesced" /\ (BarrierBeforeCkpt \/ barrier)
    /\ child' = [p \in Paths |->
                    IF p[1] = ahome /\ child[p] = None
                    THEN {r \in acopy : Half(r) = p[2]}
                    ELSE child[p]]
    /\ pc' = "cloned"
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   engine, held, plog, acked, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* Retire: 'T' written to the parent's log, which only the current writer can;
\* a fenced actor stops (its own rollback deletes only what a claimant would).
Retire ==
    /\ pc = "cloned"
    /\ IF aepoch = epoch
       THEN /\ retired' = ahome /\ pc' = "retired"
       ELSE /\ pc' = "stopped" /\ UNCHANGED retired
    /\ UNCHANGED <<version, published, installed, nextHome, intent, epoch, engine,
                   held, plog, acked, child, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* Publish: the topology CAS (idempotent: a claimant may have published), then
\* the barrier's release to the children.
Publish ==
    /\ \/ pc = "retired"
       \/ ~PublishNeedsT /\ pc = "cloned"
    /\ IF version = 1
       THEN /\ version' = 2 /\ published' = ahome
       ELSE UNCHANGED <<version, published>>
    /\ installed' = [installed EXCEPT ![Actor] = 2]
    /\ barrier' = FALSE /\ pc' = "published"
    /\ UNCHANGED <<nextHome, intent, retired, epoch, engine, held, plog, acked,
                   child, aepoch, ahome, acopy, crashed, used, attempts>>

\* Reclaim: the checkpoint deleted, the intent CASed terminal if still the actor's.
Reclaim ==
    /\ pc = "published"
    /\ intent' = IF Live /\ intent.owner = Actor THEN [intent EXCEPT !.state = "done"]
                 ELSE intent
    /\ pc' = "done"
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   held, plog, acked, child, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* ReclaimPublished (`topology::reclaim`): an instance that reads a live
\* intent whose split the stored topology already names (a split's start, the
\* heartbeat's intent watch) CASes it terminal, as the actor's Reclaim would.
\* Without it an actor that dies after its publish leaves the intent live for
\* good: nothing opens a retired parent, so no claimant runs.
ReclaimPublished(i) ==
    /\ Live /\ version = 2
    /\ intent' = [intent EXCEPT !.state = "done"]
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   held, plog, acked, child, pc, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* A crash, once: the actor, its engine and its barrier are gone.
Crash ==
    /\ ActorAlive /\ ~crashed
    /\ pc' = "dead" /\ barrier' = FALSE /\ crashed' = TRUE
    /\ engine' = [engine EXCEPT ![Actor] = 0]
    /\ held' = [held EXCEPT ![Actor] = FALSE]
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   plog, acked, child, aepoch, ahome, acopy, used, attempts>>

\* The actor renews its lease while it runs and still owns the intent.
Renew ==
    /\ ActorAlive /\ Live /\ intent.owner = Actor /\ intent.lapsed
    /\ intent' = [intent EXCEPT !.lapsed = FALSE]
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   held, plog, acked, child, pc, aepoch, ahome, acopy, barrier,
                   crashed, used, attempts>>

\* A lapse is observed whatever the actor does: the lease does not fence.
LeaseLapse ==
    /\ Live /\ ~intent.lapsed
    /\ intent' = [intent EXCEPT !.lapsed = TRUE]
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   held, plog, acked, child, pc, aepoch, ahome, acopy, barrier,
                   crashed, used, attempts>>

---------------------------------------------------------------------------
\* Opens of the parent and the fence (`fence::fenced`, `hold_until_served`).

\* An open of the parent's log by i: SlateDB bumps the writer epoch, which
\* fences every older writer; it happens only once the intent is absent,
\* terminal or lapsed (the hold before the open), and the fence holds the
\* open while a live intent names the parent, or while its log holds 'T'.
Open(i) ==
    /\ engine[i] = 0 /\ epoch < MaxEpoch
    /\ installed[i] = 1
    \* The hold before the open (TLA-048 option (b), change 1, `fence::hold_before_open`): an
    \* open waits, its log unopened, while the intent is live and not lapsed.
    /\ ~(Live /\ ~intent.lapsed)
    \* A running actor never re-opens the parent it was fenced on (a control's).
    /\ (i = Actor /\ ActorStaysFenced) => ~ActorAlive
    /\ epoch' = epoch + 1
    /\ engine' = [engine EXCEPT ![i] = epoch + 1]
    /\ held' = [held EXCEPT ![i] = FenceAtOpen /\ (Live \/ (HeldUnderT /\ retired # 0))]
    /\ IF i = Actor /\ pc = "dead" THEN pc' = "idle" ELSE UNCHANGED pc
    \* A running actor that re-opens writes through the new engine (a control's).
    /\ aepoch' = IF i = Actor /\ ActorAlive THEN epoch + 1 ELSE aepoch
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, plog,
                   acked, child, ahome, acopy, barrier, crashed, used, attempts>>

\* A fenced engine, or one whose instance no longer routes P, closes.
CloseEngine(i) ==
    /\ engine[i] # 0 /\ (engine[i] # epoch \/ installed[i] = 2)
    \* A running actor keeps its fenced engine (a control's: it closes it to re-open).
    /\ ~(i = Actor /\ ActorAlive /\ ActorStaysFenced)
    /\ engine' = [engine EXCEPT ![i] = 0]
    /\ held' = [held EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<version, published, installed, nextHome, intent, retired, epoch,
                   plog, acked, child, pc, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* The roll back's deletions and terminal CAS (`recover::roll_back`), then serve.
RollBack(i) ==
    /\ child' = [p \in Paths |-> IF p[1] = intent.home THEN None ELSE child[p]]
    /\ intent' = [intent EXCEPT !.state = "done", !.owner = i]
    /\ held' = [held EXCEPT ![i] = FALSE]

\* A held open re-reads the fence (`decide_and_recover`).
FenceStep(i) ==
    /\ engine[i] # 0 /\ engine[i] = epoch /\ held[i]
    /\ CASE retired = 0 /\ ~Live ->
                \* the operation ended: serve
                /\ held' = [held EXCEPT ![i] = FALSE]
                /\ UNCHANGED <<version, published, installed, intent, child>>
         [] retired = 0 /\ Lapsed ->
                \* claim and roll back
                /\ RollBack(i)
                /\ UNCHANGED <<version, published, installed>>
         [] retired # 0 /\ (Lapsed \/ (ForwardFromT /\ ~Live)) ->
                \* claim (or nothing to claim) and roll forward from 'T'
                /\ IF version = 1 THEN version' = 2 /\ published' = retired
                   ELSE UNCHANGED <<version, published>>
                /\ intent' = IF Live THEN [intent EXCEPT !.state = "done", !.owner = i]
                             ELSE intent
                /\ installed' = [installed EXCEPT ![i] = 2]
                /\ IF HeldUnderT THEN UNCHANGED held
                   ELSE held' = [held EXCEPT ![i] = FALSE]
                /\ UNCHANGED child
         [] OTHER -> FALSE
    /\ UNCHANGED <<nextHome, retired, epoch, engine, plog, acked, pc, aepoch, ahome,
                   acopy, barrier, crashed, used, attempts>>

\* The heartbeat's eager open (package 6, e05956fc3): an instance opens a live
\* intent's source it owns and holds closed, so a lapsed operation no request
\* reaches is still claimed. With the hold before the open it opens only once
\* the intent lapsed.
EagerOpen(i) == Live /\ intent.lapsed /\ Open(i)

\* NC only (HeldUnderT false): the fence serves an open whose log holds 'T'
\* before the topology that retires it, once no live intent names it.
ServeUnderT(i) ==
    /\ ~HeldUnderT
    /\ engine[i] # 0 /\ engine[i] = epoch /\ held[i] /\ retired # 0 /\ Lapsed
    /\ intent' = [intent EXCEPT !.state = "done", !.owner = i]
    /\ held' = [held EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   plog, acked, child, pc, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

\* NC only (ClaimOpens false): a claimant rolls back from a read of 'T' and the
\* intent, without opening (fencing) the parent.
ClaimUnfenced(i) ==
    /\ ~ClaimOpens
    /\ i # Actor /\ retired = 0 /\ Lapsed
    /\ RollBack(i)
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   plog, acked, pc, aepoch, ahome, acopy, barrier, crashed, used,
                   attempts>>

\* NC only (ClaimAfterFence false): the claimant read "no 'T'" before its epoch
\* bump, and decides from that read after the actor wrote 'T'.
DecideStale(i) ==
    /\ ~ClaimAfterFence
    /\ i # Actor /\ engine[i] = 0 /\ epoch < MaxEpoch /\ installed[i] = 1
    /\ Lapsed /\ retired # 0
    /\ epoch' = epoch + 1
    /\ engine' = [engine EXCEPT ![i] = epoch + 1]
    /\ RollBack(i)
    /\ UNCHANGED <<version, published, installed, nextHome, retired, plog, acked,
                   pc, aepoch, ahome, acopy, barrier, crashed, used, attempts>>

\* NC only (RollBackNeedsNoT false): the claimant rolls back despite 'T'.
RollBackAnyway(i) ==
    /\ ~RollBackNeedsNoT
    /\ engine[i] # 0 /\ engine[i] = epoch /\ held[i] /\ retired # 0 /\ Lapsed
    /\ RollBack(i)
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   plog, acked, pc, aepoch, ahome, acopy, barrier, crashed, used,
                   attempts>>

\* NC only (ForwardFromT false): a stale claimant's late terminal write, ending
\* a live intent after 'T' without a publish. The code cannot make it: every
\* terminal write is a CAS on the writer's own ETag, a rollback is decided only
\* without 'T' after the claimant's open fenced the parent, and the actor ends
\* RolledBack only before 'T' (NoTEndedUnpublished). It exists for the control
\* that 'T' alone must roll forward.
EndAfterT(i) ==
    /\ ~ForwardFromT
    /\ i # Actor /\ retired # 0 /\ version = 1 /\ Live /\ ~ActorAlive
    /\ intent' = [intent EXCEPT !.state = "done", !.owner = i]
    /\ UNCHANGED <<version, published, installed, nextHome, retired, epoch, engine,
                   held, plog, acked, child, pc, aepoch, ahome, acopy, barrier, crashed,
                   used, attempts>>

---------------------------------------------------------------------------
Next ==
    \/ \E r \in Recs, i \in Insts : AppendParent(r, i) \/ AppendChild(r, i)
    \/ \E i \in Insts : Install(i) \/ Open(i) \/ CloseEngine(i) \/ FenceStep(i)
    \/ \E i \in Insts : ServeUnderT(i) \/ ClaimUnfenced(i) \/ DecideStale(i)
                        \/ RollBackAnyway(i) \/ EndAfterT(i)
    \/ \E i \in Insts : ReclaimPublished(i)
    \/ Begin \/ Park \/ Checkpoint \/ ParkLate \/ Clone \/ Retire \/ Publish \/ Reclaim
    \/ Crash \/ Renew \/ LeaseLapse

Spec == Init /\ [][Next]_vars

\* Fairness for the liveness checks: the actor's steps, every opener's fence
\* re-read, closes, installs and reclaims progress; a lapse is eventually observed; the
\* other instance opens the parent of a lapsed live intent from its heartbeat
\* (EagerOpen); no client request is assumed.
Fairness ==
    /\ WF_vars(Park) /\ WF_vars(Checkpoint) /\ WF_vars(ParkLate) /\ WF_vars(Clone)
    /\ WF_vars(Retire) /\ WF_vars(Publish) /\ WF_vars(Reclaim) /\ WF_vars(LeaseLapse)
    /\ \A i \in Insts : WF_vars(FenceStep(i)) /\ WF_vars(Install(i)) /\ WF_vars(CloseEngine(i))
    /\ \A i \in Insts : WF_vars(ReclaimPublished(i))
    /\ \A i \in Insts \ {Actor} : WF_vars(EagerOpen(i))

LiveSpec == Spec /\ Fairness

---------------------------------------------------------------------------
\* Properties.

\* Every acked record is readable in the leaf of the stored topology that
\* covers its hash.
AckedReadable ==
    \A r \in acked :
        IF version = 1 THEN r \in plog
        ELSE child[<<published, Half(r)>>] # None /\ r \in child[<<published, Half(r)>>]

\* At most one engine serves a hash: never the parent beside a child, never
\* two parent engines.
NoDualServe ==
    /\ ~(\E i, j \in Insts : ParentServes(i) /\ (\E h \in {0, 1} : ChildServes(j, h)))
    /\ \A i, j \in Insts : (ParentServes(i) /\ ParentServes(j)) => i = j

\* Once the parent's 'T' is in its log, the parent never serves.
RetiredNeverServes == retired # 0 => ~\E i \in Insts : ParentServes(i)

\* The published children hold exactly the parent's records of their half,
\* plus the records they acked themselves.
ChildrenExact ==
    version = 2 =>
        \A h \in {0, 1} :
            /\ child[<<published, h>>] # None
            /\ {r \in plog : Half(r) = h} \subseteq child[<<published, h>>]
            /\ \A r \in child[<<published, h>>] : Half(r) = h /\ r \in acked

\* An intent never ends while 'T' stands unpublished: every terminal write is a
\* CAS, a rollback needs no 'T', a roll forward publishes first.
NoTEndedUnpublished == (intent.state = "done" /\ retired # 0) => version = 2

\* No two attempts took one home.
HomesUnique == Cardinality(used) = attempts

\* Witnesses (each violated on the unmodified model: the state is reachable).
\* A split published, both records acked and readable in the children.
Witness_Published == ~(version = 2 /\ intent.state = "done" /\ acked = Recs)
\* Another instance claimed a lapsed split and rolled it back; the parent serves.
Witness_ClaimRolledBack ==
    ~(intent.state = "done" /\ intent.owner # Actor /\ version = 1 /\ \E i \in Insts : ParentServes(i))
\* Another instance claimed a lapsed split after 'T' and rolled it forward.
Witness_ClaimRolledForward == ~(version = 2 /\ intent.owner # Actor)
\* A stale actor's late clone sits in an attempt's home that is never published.
Witness_StaleCloneUnpublished ==
    ~(version = 2 /\ \E p \in Paths : p[1] # published /\ child[p] # None)

\* The actor died after its publish and another reader ended its intent (the
\* claims and EndAfterT take the intent's owner; ReclaimPublished keeps it).
Witness_ReclaimedPublished ==
    ~(version = 2 /\ intent.state = "done" /\ intent.owner = Actor /\ pc = "dead")

\* After 'T', the split is eventually published.
ForwardAfterRetire == (retired # 0) ~> (version = 2)

\* A started split eventually ends (published or rolled back).
OperationCompletes == Live ~> (intent.state = "done")
=============================================================================
