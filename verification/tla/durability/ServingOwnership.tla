-------------------------- MODULE ServingOwnership --------------------------
(***************************************************************************)
(* TLA-011 -- Serving possession, owner fencing, and shard movement.        *)
(*                                                                          *)
(* Two nodes, ONE shard prefix, one stream in it. Each node has an          *)
(* ownership view (src/ownership.rs::OwnershipService: active ring plus     *)
(* overrides, reduced to "which node does this node believe owns the        *)
(* prefix"; "none" = no ring: single-instance or bootstrapping mode, where  *)
(* effective_owner() is None and the node serves everything). The fleet's  *)
(* authority (`desired`) moves by override; views observe it late.          *)
(*                                                                          *)
(* A node serves the prefix through at most one shard engine at a time      *)
(* (sharddir.rs::OpenGate single-flight + `closing` gate), opened by        *)
(* slatedb Db::builder().build(), which is modelled as the pinned fencing   *)
(* contract ASM-SLATEDB-FENCE:                                              *)
(*   OpenStart  manifest writer_epoch CAS (FenceableManifest::init_writer)  *)
(*   OpenFence  empty fence WAL at the first absent WAL id (fence_and_init) *)
(*   OpenReady  final manifest refresh (fails if a newer epoch exists),     *)
(*              WAL replay up to the fence, ShardEngine::start, serving-map *)
(*              insertion                                                   *)
(* and every WAL write (data or fence) is a put-if-absent on the next WAL   *)
(* id: `Land` succeeds only if the writer's next id is still absent,        *)
(* otherwise the writer's Db closes with Fenced.                            *)
(*                                                                          *)
(* Engines reuse the CommitHandoff abstraction. Each committer group holds  *)
(* one request here (grouping is TLA-005's subject). Requests:              *)
(*   "A" producer append (record "a" + producer row)                        *)
(*   "R" A's idempotent retry (duplicate when the producer row is present)  *)
(*   "X" plain append (record "x")                                          *)
(* Clients route through stale routers to either node and retry.           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC, CommitHandoff

CONSTANTS
    Nodes,          \* {"n1", "n2"}
    Mode,           \* "fleet" (managed ring + overrides) | "single" (no ring)
    Reqs,           \* subset of {"A", "R", "X"} containing "A"
    InitOwner,      \* fleet authority at start
    MaxEngines,     \* engine incarnations over the whole run
    MaxCrashes,     \* node process crashes
    MaxMoves,       \* override moves by the rebalancer
    MaxTries,       \* attempts of the retry R
    AmbiguousPut,   \* BOOLEAN: one WAL PUT may land with its reply lost (retry: Fenced)
    ClaimOp(_, _),  \* mutation point: which registered groups may be acknowledged
    ReadyGate,      \* mutation point: engine states from which serving may begin
    LandOp(_, _, _),\* mutation point: whether a WAL put by this writer succeeds
    RefreshOp(_, _, _) \* mutation point: the opener's final manifest refresh

ASSUME /\ Mode \in {"fleet", "single"} /\ InitOwner \in Nodes /\ AmbiguousPut \in BOOLEAN
       /\ Reqs \subseteq {"A", "R", "X"} /\ "A" \in Reqs

EngIds == 1..MaxEngines

OpOf(r) == IF r = "X" THEN "x" ELSE "a"
HasProducer(r) == r \in {"A", "R"}

NoProd == [has |-> FALSE, off |-> 0]
EmptyStream == [log |-> <<>>, prod |-> NoProd]

(* A WAL entry: a writer's fence, or one data batch (record put at `off`,  *)
(* plus the producer row when prod = TRUE).                                *)
Put(s, b) ==
    [log  |-> IF b.off < Len(s.log) THEN [s.log EXCEPT ![b.off + 1] = b.op]
              ELSE Append(s.log, b.op),
     prod |-> IF b.prod THEN [has |-> TRUE, off |-> b.off] ELSE s.prod]

RECURSIVE ReplayFrom(_, _, _)
ReplayFrom(w, i, s) ==
    IF i > Len(w) THEN s
    ELSE ReplayFrom(w, i + 1, IF w[i].kind = "data" THEN Put(s, w[i]) ELSE s)
Replay(w) == ReplayFrom(w, 1, EmptyStream)

NoBatch == [kind |-> "none", eng |-> 0, op |-> "none", off |-> 0, prod |-> FALSE, seq |-> 0]

NoEngine == [node |-> "none", st |-> "none", epoch |-> 0, fencePos |-> 0,
             nextWal |-> 0, applied |-> EmptyStream, ho |-> NewHandoff,
             q |-> <<>>, buf |-> <<>>, gseq |-> 0, reported |-> 0,
             dbClosed |-> FALSE, claimed |-> <<>>, zombie |-> <<>>]

(* Real (unmutated) operators. *)
(* dispatch_durable claims exactly the groups whose seq <= durable_seq.    *)
RealClaim(en, mine) ==
    [take |-> TakeDurable(en.ho, en.reported), ho |-> AfterTakeDurable(en.ho, en.reported)]
(* An engine begins serving only after its fence WAL is established.       *)
RealReadyGate == {"fenced"}
(* ASM-SLATEDB-FENCE: a WAL put is put-if-absent on the writer's next id;  *)
(* the writer's ownership view plays no part.                              *)
RealLand(en, mine, walLen) == en.nextWal = walLen + 1
(* FenceableManifest refresh: an opener whose writer epoch was superseded  *)
(* fails; the ring plays no part.                                          *)
RealRefresh(en, ringMine, epoch) == epoch = en.epoch

VARIABLES
    desired,      \* fleet authority for the prefix (override / ring pick)
    view,         \* per-node ownership view: a node, or "none" (no ring)
    moves,
    eng,          \* engine records, by incarnation id
    nEng,         \* engines allocated so far
    resident,     \* per-node serving-map resident engine (0: none)
    epochCtr,     \* manifest writer_epoch
    wal,          \* object-store WAL: Seq of entries, id = position
    crashes,
    cst,          \* client state per request
    tries,        \* attempts per request
    acks,         \* every success reply any engine ever sent (observable)
    everReady,    \* ghost: engines that began serving
    casFailed,    \* ghost: engines whose WAL put-if-absent lost (fenced)
    zombieLanded, \* ghost: states ("crashed"/"gone") of dead engines whose PUT landed later
    ambigPos      \* ghost: WAL position of a PUT that landed but reported Fenced (0: none)

vars == <<desired, view, moves, eng, nEng, resident, epochCtr, wal, crashes,
          cst, tries, acks, everReady, casFailed, zombieLanded, ambigPos>>

IsMine(n) == view[n] \in {"none", n}
(* The managed ring explicitly names this node (not bootstrapping).        *)
RingSaysMine(n) == view[n] = n
Min(a, b) == IF a < b THEN a ELSE b
MaxBuf == Cardinality(Reqs) + MaxTries  \* no buffer is ever longer
Other(n) == CHOOSE m \in Nodes : m # n

(* Client-side effect of an outcome for (r, attempt att). *)
Outcome(r, kind) ==
    IF kind \in {"ok", "dup"} THEN "done"
    ELSE CASE r = "A" -> "failed"
           [] r = "R" -> IF tries[r] < MaxTries THEN "idle" ELSE "done"
           [] OTHER -> "done"

(* Server sends a set of reply records [req, att, kind, off] from engine e. *)
SendReplies(e, rs) ==
    LET late == \E f \in EngIds : eng[f].st = "ready" /\ eng[f].epoch > eng[e].epoch
    IN /\ acks' = acks \cup {[r |-> x.req, kind |-> x.kind, off |-> x.off, eng |-> e,
                              ep |-> eng[e].epoch, late |-> late]
                             : x \in {y \in rs : y.kind \in {"ok", "dup"}}}
       /\ cst' = [r \in Reqs |->
                    IF cst[r] = "waiting" /\ \E x \in rs : x.req = r /\ x.att = tries[r]
                    THEN Outcome(r, (CHOOSE x \in rs : x.req = r /\ x.att = tries[r]).kind)
                    ELSE cst[r]]

Init ==
    /\ desired = InitOwner
    /\ view = [n \in Nodes |-> IF Mode = "fleet" THEN InitOwner ELSE "none"]
    /\ moves = 0
    /\ eng = [e \in EngIds |-> NoEngine]
    /\ nEng = 0
    /\ resident = [n \in Nodes |-> 0]
    /\ epochCtr = 0
    /\ wal = <<>>
    /\ crashes = 0
    /\ cst = [r \in Reqs |-> "idle"]
    /\ tries = [r \in Reqs |-> 0]
    /\ acks = {}
    /\ everReady = {} /\ casFailed = {} /\ zombieLanded = {} /\ ambigPos = 0

(* ---------------- fleet authority and views ---------------- *)
Move ==
    /\ Mode = "fleet" /\ moves < MaxMoves
    /\ desired' = Other(desired)
    /\ moves' = moves + 1
    /\ UNCHANGED <<view, eng, nEng, resident, epochCtr, wal, crashes, cst, tries,
                   acks, everReady, casFailed, zombieLanded, ambigPos>>

(* A fleet tick publishes a complete authority read (set_view). *)
Observe(n) ==
    /\ Mode = "fleet" /\ view[n] # desired
    /\ view' = [view EXCEPT ![n] = desired]
    /\ UNCHANGED <<desired, moves, eng, nEng, resident, epochCtr, wal, crashes,
                   cst, tries, acks, everReady, casFailed, zombieLanded, ambigPos>>

(* ---------------- clients ---------------- *)
CanSend(r) ==
    /\ cst[r] = "idle"
    /\ CASE r = "R" -> cst["A"] = "failed" /\ tries[r] < MaxTries
         [] OTHER -> tries[r] = 0

(* ShardDirectory::resolve: a foreign owner redirects (NotOwner); a live   *)
(* resident serves; otherwise the open is pending (retryable Wait).        *)
Send(r, n) ==
    /\ CanSend(r)
    /\ tries' = [tries EXCEPT ![r] = @ + 1]
    /\ IF IsMine(n) /\ resident[n] # 0 /\ eng[resident[n]].st = "ready"
       THEN /\ eng' = [eng EXCEPT ![resident[n]].q = Append(@, [req |-> r, att |-> tries[r] + 1])]
            /\ cst' = [cst EXCEPT ![r] = "waiting"]
       ELSE /\ cst' = [cst EXCEPT ![r] = IF r = "A" THEN "failed"
                                         ELSE IF r = "R" /\ tries[r] + 1 < MaxTries THEN "idle"
                                         ELSE "done"]
            /\ UNCHANGED eng
    /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, wal, crashes,
                   acks, everReady, casFailed, zombieLanded, ambigPos>>

(* Client deadline: the attempt's receiver is dropped; the server goes on. *)
GiveUp(r) ==
    /\ cst[r] = "waiting"
    /\ cst' = [cst EXCEPT ![r] = Outcome(r, "timeout")]
    /\ UNCHANGED <<desired, view, moves, eng, nEng, resident, epochCtr, wal,
                   crashes, tries, acks, everReady, casFailed, zombieLanded, ambigPos>>

(* ---------------- engine: retirement ---------------- *)
(* begin_close: terminal handoff, stranded groups and the requests `q`      *)
(* still queued are rejected with Moved (committer_loop's closed branch     *)
(* drains the channel), `extra` replies are sent with them, and the serving *)
(* map drops the resident.                                                  *)
CloseEngineWith(e, q, extra) ==
    LET en == eng[e]
        rs == RepliesOf(RetireStranded(en.ho))
              \cup {[req |-> q[i].req, att |-> q[i].att, kind |-> "moved", off |-> 0]
                    : i \in 1..Len(q)}
    IN /\ eng' = [eng EXCEPT ![e].st = "closed", ![e].ho = RetireHandoff(en.ho),
                             ![e].q = <<>>]
       /\ SendReplies(e, {[x EXCEPT !.kind = "moved"] : x \in rs} \cup extra)
       /\ resident' = [resident EXCEPT ![en.node] = IF @ = e THEN 0 ELSE @]

CloseEngine(e) == CloseEngineWith(e, eng[e].q, {})

(* ---------------- engine: committer ---------------- *)
Commit(e) ==
    LET en == eng[e]
        x  == Head(en.q)
        s  == en.applied
    IN
    /\ en.st = "ready" /\ en.q # <<>>
    /\ IF HasProducer(x.req) /\ s.prod.has
       THEN \* no-write duplicate verdict: join_prior_barrier
            LET rep == [req |-> x.req, att |-> x.att, kind |-> "dup", off |-> s.prod.off]
            IN IF AttachVerdict(en.ho) = "Pending"
               THEN /\ eng' = [eng EXCEPT ![e].ho = AttachToNewest(en.ho, {rep}),
                                          ![e].q = Tail(en.q)]
                    /\ UNCHANGED <<acks, cst, resident>>
               ELSE \* Durable (a ready engine is never terminal)
                    /\ eng' = [eng EXCEPT ![e].q = Tail(en.q)]
                    /\ SendReplies(e, {rep})
                    /\ UNCHANGED resident
       ELSE IF en.dbClosed
       THEN \* db.write on a closed Db fails: CommitTransaction::write_failed
            \* retires the engine (begin_close), then answers Internal
            \* (outcome unknown)
            /\ CloseEngineWith(e, Tail(en.q),
                               {[req |-> x.req, att |-> x.att, kind |-> "internal", off |-> 0]})
       ELSE \* accept: write + publish (applied mirrors, registration)
            LET off == Len(s.log)
                b == [kind |-> "data", eng |-> e, op |-> OpOf(x.req), off |-> off,
                      prod |-> HasProducer(x.req), seq |-> en.gseq + 1]
                rep == [req |-> x.req, att |-> x.att, kind |-> "ok", off |-> off]
            IN /\ eng' = [eng EXCEPT ![e].q = Tail(en.q),
                                     ![e].gseq = en.gseq + 1,
                                     ![e].buf = Append(en.buf, b),
                                     ![e].applied = Put(s, b),
                                     ![e].ho = Register(en.ho, [seq |-> en.gseq + 1,
                                                                replies |-> {rep}])]
               /\ UNCHANGED <<acks, cst, resident>>
    /\ UNCHANGED <<desired, view, moves, nEng, epochCtr, wal, crashes,
                   tries, everReady, casFailed, zombieLanded, ambigPos>>

(* ---------------- engine: storage ---------------- *)
(* The next buffered batch's WAL PUT: put-if-absent at nextWal. Landing and *)
(* the durable_seq report are merged (TLA-005 separates them). Outcomes:    *)
(*   landed and acknowledged by the object store;                           *)
(*   the id was taken (a newer fence or writer): AlreadyExists -> Fenced;   *)
(*   ambiguous (AmbiguousPut): the PUT landed but its reply was lost, and   *)
(*     the retry of the conditional PUT sees AlreadyExists -> a spurious    *)
(*     Fenced (tablestore.rs write_sst_in_object_store): the batch is       *)
(*     durable, never reported, and the writer's Db closes.                 *)
Land(e) ==
    LET en == eng[e] IN
    /\ en.st \in {"ready", "closed"} /\ ~en.dbClosed /\ en.buf # <<>>
    /\ IF LandOp(en, IsMine(en.node), Len(wal))
       THEN \/ /\ wal' = Append(wal, Head(en.buf))
               /\ eng' = [eng EXCEPT ![e].nextWal = Len(wal) + 2, ![e].buf = Tail(@),
                                     ![e].reported = Head(en.buf).seq]
               /\ UNCHANGED <<casFailed, ambigPos>>
            \/ /\ AmbiguousPut /\ ambigPos = 0
               /\ wal' = Append(wal, Head(en.buf))
               /\ eng' = [eng EXCEPT ![e].dbClosed = TRUE]
               /\ ambigPos' = Len(wal) + 1
               /\ UNCHANGED casFailed
       ELSE /\ eng' = [eng EXCEPT ![e].dbClosed = TRUE]        \* Fenced
            /\ casFailed' = casFailed \cup {e}
            /\ UNCHANGED <<wal, ambigPos>>
    /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, crashes, cst,
                   tries, acks, everReady, zombieLanded>>

(* ---------------- engine: durable dispatcher ---------------- *)
Claim(e) ==
    LET en == eng[e]
        c == ClaimOp(en, IsMine(en.node))
    IN /\ en.st = "ready" /\ en.claimed = <<>>
       /\ c.take # <<>>
       /\ eng' = [eng EXCEPT ![e].claimed = c.take, ![e].ho = c.ho]
       /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, wal, crashes,
                      cst, tries, acks, everReady, casFailed, zombieLanded, ambigPos>>

(* The claimed effects run to completion even if the engine closes: the   *)
(* legitimate late durable response.                                       *)
Reply(e) ==
    /\ eng[e].claimed # <<>> /\ eng[e].st \in {"ready", "closed"}
    /\ SendReplies(e, RepliesOf(eng[e].claimed))
    /\ eng' = [eng EXCEPT ![e].claimed = <<>>]
    /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, wal, crashes,
                   tries, everReady, casFailed, zombieLanded, ambigPos>>

(* ---------------- engine: close triggers ---------------- *)
(* The acker observes close_reason (e.g. Fenced) and closes the engine. *)
AckerClose(e) ==
    /\ eng[e].st = "ready" /\ eng[e].dbClosed
    /\ CloseEngine(e)
    /\ UNCHANGED <<desired, view, moves, nEng, epochCtr, wal, crashes, tries,
                   everReady, casFailed, zombieLanded, ambigPos>>

(* Possession yields to the ring (fleet tick, or resolve() on a request). *)
Yield(n) ==
    /\ resident[n] # 0 /\ ~IsMine(n)
    /\ CloseEngine(resident[n])
    /\ UNCHANGED <<desired, view, moves, nEng, epochCtr, wal, crashes, tries,
                   everReady, casFailed, zombieLanded, ambigPos>>

(* Workers and stores terminated (EngineTasks::begin_close, lifecycle.rs): *)
(* the storage close flushed the buffer, or the Db was already closed, or  *)
(* the close was abandoned (drive_shutdown past WORKER_GRACE, or a failed  *)
(* non-fence flush) with a WAL PUT still in flight: that PUT -- any prefix *)
(* of the buffer, one SST -- may still land later (delayed old work).      *)
Terminate(e) ==
    /\ eng[e].st = "closed" /\ eng[e].claimed = <<>>
    /\ \/ /\ eng[e].buf = <<>> \/ eng[e].dbClosed
          /\ eng' = [eng EXCEPT ![e].st = "gone", ![e].buf = <<>>]
       \/ /\ eng[e].buf # <<>> /\ ~eng[e].dbClosed
          /\ \E k \in 0..Len(eng[e].buf) :
               eng' = [eng EXCEPT ![e].st = "gone", ![e].buf = <<>>,
                                  ![e].zombie = SubSeq(eng[e].buf, 1, k)]
    /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, wal, crashes,
                   cst, tries, acks, everReady, casFailed, zombieLanded, ambigPos>>

(* ---------------- opening (Db::builder().build()) ---------------- *)
OpenStart(n) ==
    /\ IsMine(n) /\ resident[n] = 0 /\ nEng < MaxEngines
    /\ ~\E f \in EngIds : eng[f].node = n /\ eng[f].st \in {"epoch", "fenced", "closed"}
    /\ nEng' = nEng + 1
    /\ epochCtr' = epochCtr + 1
    /\ eng' = [eng EXCEPT ![nEng + 1] = [NoEngine EXCEPT !.node = n, !.st = "epoch",
                                                         !.epoch = epochCtr + 1]]
    /\ UNCHANGED <<desired, view, moves, resident, wal, crashes, cst, tries, acks,
                   everReady, casFailed, zombieLanded, ambigPos>>

OpenFence(e) ==
    /\ eng[e].st = "epoch"
    /\ wal' = Append(wal, [NoBatch EXCEPT !.kind = "fence", !.eng = e])
    /\ eng' = [eng EXCEPT ![e].st = "fenced", ![e].fencePos = Len(wal) + 1,
                          ![e].nextWal = Len(wal) + 2]
    /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, crashes, cst,
                   tries, acks, everReady, casFailed, zombieLanded, ambigPos>>

OpenReady(e) ==
    /\ eng[e].st \in ReadyGate
    /\ IF ~RefreshOp(eng[e], RingSaysMine(eng[e].node), epochCtr)
       THEN /\ eng' = [eng EXCEPT ![e].st = "failed"]     \* refresh: superseded
            /\ UNCHANGED <<resident, everReady>>
       ELSE /\ eng' = [eng EXCEPT ![e].st = "ready", ![e].applied = Replay(wal),
                                  ![e].nextWal = IF eng[e].fencePos > 0 THEN @
                                                 ELSE Len(wal) + 1]
            /\ resident' = [resident EXCEPT ![eng[e].node] = e]
            /\ everReady' = everReady \cup {e}
    /\ UNCHANGED <<desired, view, moves, nEng, epochCtr, wal, crashes, cst, tries,
                   acks, casFailed, zombieLanded, ambigPos>>

(* ---------------- node crash ---------------- *)
(* The process dies: every engine of the node loses its volatile state; an *)
(* in-flight WAL PUT (any prefix of the buffer, one SST) may still land    *)
(* later. The restarted process has no ring until its first fleet tick.    *)
(* A node has at most one live engine (OpenGate single-flight + closing    *)
(* gate), so one prefix length choice suffices.                            *)
Crash(n) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ \E k \in 0..MaxBuf :
         eng' = [e \in EngIds |->
                   IF eng[e].node = n /\ eng[e].st \in {"epoch", "fenced", "ready", "closed"}
                   THEN [eng[e] EXCEPT !.st = "crashed", !.q = <<>>, !.ho = NewHandoff,
                                       !.claimed = <<>>, !.buf = <<>>,
                                       !.zombie = IF eng[e].dbClosed THEN <<>>
                                                  ELSE SubSeq(eng[e].buf, 1, Min(k, Len(eng[e].buf)))]
                   ELSE eng[e]]
    /\ resident' = [resident EXCEPT ![n] = 0]
    /\ view' = [view EXCEPT ![n] = "none"]
    /\ UNCHANGED <<desired, moves, nEng, epochCtr, wal, cst, tries, acks,
                   everReady, casFailed, zombieLanded, ambigPos>>

(* Delayed old work: a dead engine's PUT reaches the object store. It is   *)
(* one conditional PUT at the dead writer's next id: it lands (all its     *)
(* batches) only if that id is still absent.                               *)
ZombieLand(e) ==
    /\ eng[e].st \in {"crashed", "gone"} /\ eng[e].zombie # <<>>
    /\ IF eng[e].nextWal = Len(wal) + 1
       THEN /\ wal' = wal \o eng[e].zombie
            /\ zombieLanded' = zombieLanded \cup {eng[e].st}
       ELSE UNCHANGED <<wal, zombieLanded>>
    /\ eng' = [eng EXCEPT ![e].zombie = <<>>]
    /\ UNCHANGED <<desired, view, moves, nEng, resident, epochCtr, crashes, cst,
                   tries, acks, everReady, casFailed, ambigPos>>

(* ---------------- legitimate quiescence ---------------- *)
Settled ==
    /\ \A r \in Reqs : cst[r] # "waiting" /\ ~CanSend(r)
    /\ \A e \in EngIds : /\ eng[e].q = <<>> /\ eng[e].claimed = <<>>
                         /\ eng[e].zombie = <<>>
                         /\ eng[e].st \notin {"epoch", "fenced", "closed"}
                         /\ eng[e].st = "ready" => (eng[e].ho.pending = <<>> \/ eng[e].dbClosed)

Quiescent == Settled /\ UNCHANGED vars

Next ==
    \/ Move \/ \E n \in Nodes : Observe(n) \/ Yield(n) \/ OpenStart(n) \/ Crash(n)
    \/ \E r \in Reqs : GiveUp(r) \/ \E n \in Nodes : Send(r, n)
    \/ \E e \in EngIds : \/ Commit(e) \/ Land(e) \/ Claim(e) \/ Reply(e)
                         \/ AckerClose(e) \/ Terminate(e)
                         \/ OpenFence(e) \/ OpenReady(e) \/ ZombieLand(e)
    \/ Quiescent

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* ---------------- properties ---------------- *)
TypeOK ==
    /\ nEng \in 0..MaxEngines /\ epochCtr \in 0..MaxEngines
    /\ \A e \in EngIds : eng[e].st \in {"none", "epoch", "fenced", "ready", "closed",
                                        "gone", "failed", "crashed"}
    /\ \A n \in Nodes : view[n] \in Nodes \cup {"none"}

PendingSortedInv == \A e \in EngIds : PendingSorted(eng[e].ho)

(* D1/T11: every acknowledged operation is recoverable at its acked offset *)
(* from the object store, whichever engine acknowledged it.                *)
AckedDurable ==
    LET s == Replay(wal)
    IN \A a \in acks : a.off < Len(s.log) /\ s.log[a.off + 1] = OpOf(a.r)

(* D7/P4: the original and its retry commit at most once across owners.   *)
ExactlyOnceAcrossOwners ==
    LET s == Replay(wal)
    IN Cardinality({i \in 1..Len(s.log) : s.log[i] = "a"}) <= 1

(* T11/T12: a serving engine of a higher writer epoch already covers every *)
(* acknowledgement made by a lower epoch -- old owners may still answer    *)
(* (late durable replies) but only for writes ordered before the fence.    *)
HigherEpochCoversAcks ==
    \A a \in acks : \A e \in EngIds :
        eng[e].st = "ready" /\ eng[e].epoch > a.ep
        => a.off < Len(eng[e].applied.log) /\ eng[e].applied.log[a.off + 1] = OpOf(a.r)

(* T11 at the storage boundary: every data batch in the WAL was written    *)
(* under its writer's fence with no newer fence in between.               *)
DataUnderWriterAuthority ==
    \A p \in 1..Len(wal) :
        wal[p].kind = "data" =>
            LET f == eng[wal[p].eng].fencePos
            IN /\ f > 0 /\ f < p
               /\ ~\E q \in (f + 1)..(p - 1) : wal[q].kind = "fence"

(* ---------------- reachability witnesses (expected VIOLATED) ---------------- *)
Witness_TakeoverInstalls ==
    ~(\E e1, e2 \in everReady : eng[e1].node # eng[e2].node)
Witness_StaleWriteFenced == casFailed = {}
Witness_LateDurableReplyAfterTakeover == ~(\E a \in acks : a.late)
Witness_DuplicateResolvedByNewOwner ==
    ~(\E a \in acks : a.kind = "dup" /\
        \E p \in 1..Len(wal) : wal[p].kind = "data" /\ wal[p].op = "a" /\ wal[p].eng # a.eng)
Witness_ZombieWriteLands == "crashed" \notin zombieLanded
(* A storage close abandoned with a PUT in flight: the PUT lands later.    *)
Witness_AbandonedCloseWriteLands == "gone" \notin zombieLanded
(* A PUT that landed but reported Fenced is recovered by a later owner.    *)
Witness_AmbiguousPutRecovered ==
    ~(ambigPos > 0 /\ \E f \in everReady : eng[f].fencePos > ambigPos)
Witness_CrashBetweenFenceAndServe ==
    ~(\E e \in EngIds : eng[e].st = "crashed" /\ eng[e].fencePos > 0 /\ e \notin everReady)
Witness_SupersededOpenFails == ~(\E e \in EngIds : eng[e].st = "failed")
=============================================================================
