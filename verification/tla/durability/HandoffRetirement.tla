-------------------------- MODULE HandoffRetirement --------------------------
(***************************************************************************)
(* TLA-006 -- Commit handoff, retirement, and already-durable completion.   *)
(*                                                                          *)
(* Actors of ONE shard engine (src/shard.rs::ShardEngine):                  *)
(*   committer   CommitTransaction::run -> write -> publish, or            *)
(*               join_prior_barrier for a transaction with no writes        *)
(*   completer   dispatch_durable (acker_loop and the flush pump; both     *)
(*               serialize on dispatch_gate, so one completer suffices)     *)
(*   retirer     begin_close (acker on close_reason, fleet move, shutdown,  *)
(*               RequiredExit, or the committer itself after a db.write     *)
(*               error), then rejection of the stranded groups              *)
(*   supervisor  aborts a committer still running after WORKER_GRACE        *)
(*   requesters  issue, and may cancel (drop the receiver) at any time      *)
(* plus the pinned SlateDB durability contract (applied write, WAL landing, *)
(* durable_seq report, fatal flush error).                                  *)
(*                                                                          *)
(* Request contents are abstracted: a request in Writers stages a storage   *)
(* write; any other request stages a no-write verdict (duplicate, refusal,  *)
(* idempotent close) whose truth depends on the applied state it read       *)
(* (TLA-005 owns which verdict that is). `dep` records, per reply, the WAL  *)
(* seq whose durability justifies it.                                       *)
(*                                                                          *)
(* Memory-order questions stay in Loom (commit_handoff/loom_tests.rs); here *)
(* each CommitHandoff method is one atomic step under the in_flight mutex.  *)
(*                                                                          *)
(* `decidedLive` records, AT THE DECISION STEP, whether the handoff was     *)
(* live (not terminal) when a success was decided: when the completer      *)
(* claimed the group (take_durable) or when attach returned Durable. It is  *)
(* read from `ho.terminal` in the same step, independently of what the      *)
(* take/attach operators return, so a mutated operator that ignores        *)
(* `terminal` is caught by LateSuccessWasClaimedLive.                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC, CommitHandoff

CONSTANTS
    Reqs,             \* request identities
    Writers,          \* requests whose staging produces a storage write
    MaxWalFail,       \* fatal WAL flush failures (close_reason)
    MaxWriteFail,     \* group failures: db.write errors on an open Db (before or
                      \* after apply), or the committer's own pre-write rejects
    MaxRetire,        \* external begin_close triggers (move, shutdown, task exit)
    MaxAbort,         \* supervisor aborts of the committer after retirement
    PublicationOp(_), \* mutation point: CommitHandoff::publication guard
    StrandedOp(_, _), \* mutation point: what begin_close takes ownership of
    AttachOp(_),      \* mutation point: CommitHandoff::attach verdict
    TakeOp(_, _),     \* mutation point: CommitHandoff::take_durable
    WriteErrorRetires \* mutation point: a db.write error retires the engine

ASSUME Writers \subseteq Reqs /\ WriteErrorRetires \in BOOLEAN

RealPublication(h)      == PublicationOpen(h)
RealStranded(h, d)      == RetireStranded(h)
RealAttach(h)           == AttachVerdict(h)
RealTake(h, d)          == [take |-> TakeDurable(h, d), rest |-> AfterTakeDurable(h, d)]
RealWriteErrorRetires   == TRUE   \* CommitTransaction::write_failed calls begin_close

VARIABLES
    queue,           \* committer mpsc channel
    txn,             \* committer's in-progress transaction
    committerAlive,  \* committer task not aborted
    ho,              \* CommitHandoff
    gate,            \* dispatch_gate holder: "free" | "disp" | "commit"
    disp,            \* completer's claimed groups
    strand,          \* groups owned by the retirer, awaiting rejection
    wal, landed, reported, dbClosed,
    walDead,         \* WAL flusher dead: nothing more lands
    writerDead,      \* batch writer exited after a post-apply error; reads stay
                     \* open until closed_result is written (DbCloseResult)
    appliedSeq,      \* seq of the newest registered (applied) group
    pubCount,        \* number of applied publications (registrations)
    pubAtRetire,     \* pubCount when the handoff became terminal
    issued, cancelled,
    nsent,           \* server-side settlements per request
    res,             \* kind of the first settlement
    sentDep,         \* dep of the first settlement
    afterRetire,     \* first settlement happened after retirement
    decidedLive,     \* success decided (claimed / attach Durable) while live
    strandedEver,    \* requests whose group retirement took over
    strandedDurable, \* ... of which the group was already reported durable
    lateWrite,       \* seq of the first batch written after retirement (0: none)
    walFails, retires, aborts, writeFails,
    preFailAt,       \* ghost: next WAL seq when a pre-write reject hit (0: none)
    writeRetire      \* ghost: a db.write error retired the handoff

vars == <<queue, txn, committerAlive, ho, gate, disp, strand, wal, landed,
          reported, dbClosed, walDead, writeFails, appliedSeq, pubCount, pubAtRetire, issued,
          cancelled, nsent, res, sentDep, afterRetire, decidedLive,
          strandedEver, strandedDurable, lateWrite, walFails, retires, aborts, preFailAt,
          writerDead, writeRetire>>

IdleTxn  == [phase |-> "idle", reqs |-> {}, writes |-> FALSE, seq |-> 0,
             dep |-> 0, verdict |-> "none"]
IdleDisp == [phase |-> "idle", groups |-> <<>>]
Elems(s) == {s[i] : i \in 1..Len(s)}
ReqsIn(rs) == {x.req : x \in rs}
Recs(S, d) == {[req |-> r, dep |-> d] : r \in S}

(* Server-side settlement of reply records (oneshot sends). A send to a    *)
(* cancelled requester is still a settlement: the receiver is gone.        *)
Settle(rs, kind) ==
    LET hit(r) == \E x \in rs : x.req = r
        first(r) == hit(r) /\ nsent[r] = 0
    IN /\ nsent' = [r \in Reqs |-> IF hit(r) THEN nsent[r] + 1 ELSE nsent[r]]
       /\ res' = [r \in Reqs |-> IF first(r) THEN kind ELSE res[r]]
       /\ sentDep' = [r \in Reqs |-> IF first(r)
                                     THEN (CHOOSE x \in rs : x.req = r).dep
                                     ELSE sentDep[r]]
       /\ afterRetire' = [r \in Reqs |-> IF first(r) THEN ho.terminal ELSE afterRetire[r]]

Init ==
    /\ queue = <<>> /\ txn = IdleTxn /\ committerAlive = TRUE
    /\ ho = NewHandoff /\ gate = "free" /\ disp = IdleDisp /\ strand = <<>>
    /\ wal = 0 /\ landed = 0 /\ reported = 0 /\ dbClosed = FALSE /\ walDead = FALSE
    /\ writerDead = FALSE /\ writeRetire = FALSE
    /\ writeFails = 0
    /\ appliedSeq = 0 /\ pubCount = 0 /\ pubAtRetire = 0
    /\ issued = [r \in Reqs |-> FALSE] /\ cancelled = [r \in Reqs |-> FALSE]
    /\ nsent = [r \in Reqs |-> 0] /\ res = [r \in Reqs |-> "none"]
    /\ sentDep = [r \in Reqs |-> 0] /\ afterRetire = [r \in Reqs |-> FALSE]
    /\ decidedLive = [r \in Reqs |-> FALSE]
    /\ strandedEver = {} /\ strandedDurable = {} /\ lateWrite = 0
    /\ walFails = 0 /\ retires = 0 /\ aborts = 0 /\ preFailAt = 0

(* ---------------- requesters ---------------- *)
(* ShardEngine::try_command refuses admission once closed (503, unknown). *)
Issue(r) ==
    /\ ~issued[r]
    /\ issued' = [issued EXCEPT ![r] = TRUE]
    /\ IF ho.terminal
       THEN /\ Settle(Recs({r}, 0), "unavail") /\ UNCHANGED queue
       ELSE /\ queue' = Append(queue, r)
            /\ UNCHANGED <<preFailAt, nsent, res, sentDep, afterRetire, writerDead, writeRetire>>
    /\ UNCHANGED <<preFailAt, txn, committerAlive, ho, gate, disp, strand, wal, landed,
                   reported, dbClosed, appliedSeq, pubCount, pubAtRetire,
                   cancelled, decidedLive, strandedEver, strandedDurable,
                   lateWrite, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

(* Client cancellation without server cancellation: the receiver drops.   *)
Cancel(r) ==
    /\ issued[r] /\ ~cancelled[r] /\ nsent[r] = 0
    /\ cancelled' = [cancelled EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, gate, disp, strand, wal,
                   landed, reported, dbClosed, appliedSeq, pubCount, pubAtRetire,
                   issued, nsent, res, sentDep, afterRetire, decidedLive,
                   strandedEver, strandedDurable, lateWrite, walFails, retires,
                   aborts, walDead, writeFails, writerDead, writeRetire>>

(* begin_close under in_flight: retire() and the closed flag. The stranded *)
(* groups move to the retirer, which rejects them in a later step.          *)
DoBeginClose ==
    LET s == StrandedOp(ho, disp)
        rq == ReqsIn(RepliesOf(s))
        durableRq == ReqsIn(UNION {s[i].replies : i \in {j \in 1..Len(s) : s[j].seq <= reported}})
    IN /\ strand' = strand \o s
       /\ strandedEver' = strandedEver \cup rq
       /\ strandedDurable' = strandedDurable \cup durableRq
       /\ ho' = RetireHandoff(ho)
       /\ pubAtRetire' = pubCount

(* CommitTransaction::write_failed, for every db.write error: log, then     *)
(* begin_close, then reject the group Internal. The retirement happens in   *)
(* the committer's step, before it can take another group; the stranded    *)
(* groups are rejected by RejectStranded, a later step (production rejects  *)
(* them inside the same call, so this is more permissive). A handoff that  *)
(* is already terminal is left as it is. WriteErrorRetires is the mutation  *)
(* point: before the fix the committer only rejected the group.            *)
RetireOnWriteError ==
    IF WriteErrorRetires /\ ~ho.terminal
    THEN DoBeginClose /\ writeRetire' = TRUE
    ELSE UNCHANGED <<strand, strandedEver, strandedDurable, ho, pubAtRetire, writeRetire>>

(* ---------------- committer ---------------- *)
(* A no-write verdict depends on the state its staging read: the applied    *)
(* mirror, or a default (Memory-level) db.get of a row the mirror lacks,    *)
(* which returns SlateDB's newest committed batch while the Db is readable. *)
(* The two differ only after a post-apply write error, before closed_result *)
(* is written; the conservative choice is the newer one.                    *)
StagingDep == IF dbClosed THEN appliedSeq ELSE wal

Take ==
    /\ committerAlive /\ txn.phase = "idle" /\ queue # <<>>
    /\ \E k \in 1..Len(queue) :
         LET S == Elems(SubSeq(queue, 1, k))
         IN /\ queue' = SubSeq(queue, k + 1, Len(queue))
            /\ IF ho.terminal
               THEN /\ Settle(Recs(S, 0), "moved")        \* closed branch / run()
                    /\ UNCHANGED txn
               ELSE /\ txn' = [phase |-> "staged", reqs |-> S,
                               writes |-> S \cap Writers # {}, seq |-> 0,
                               dep |-> StagingDep, verdict |-> "none"]
                    /\ UNCHANGED <<preFailAt, nsent, res, sentDep, afterRetire,
                                   writerDead, writeRetire>>
    /\ UNCHANGED <<preFailAt, committerAlive, ho, gate, disp, strand, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   decidedLive, strandedEver, strandedDurable, lateWrite,
                   walFails, retires, aborts, walDead, writeFails, writerDead, writeRetire>>

WriteOk ==
    /\ committerAlive /\ txn.phase = "staged" /\ txn.writes /\ ~dbClosed /\ ~writerDead
    /\ wal' = wal + 1
    /\ txn' = [txn EXCEPT !.phase = "written", !.seq = wal + 1]
    /\ lateWrite' = IF ho.terminal /\ lateWrite = 0 THEN wal + 1 ELSE lateWrite
    /\ UNCHANGED <<preFailAt, queue, committerAlive, ho, gate, disp, strand, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   nsent, res, sentDep, afterRetire, decidedLive, strandedEver,
                   strandedDurable, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

(* db.write on a closed Db, or after the batch writer exited: refused,     *)
(* nothing applied, Internal. The committer keeps being scheduled, so this  *)
(* is fair.                                                                  *)
WriteRefused ==
    /\ committerAlive /\ txn.phase = "staged" /\ txn.writes /\ (dbClosed \/ writerDead)
    /\ Settle(Recs(txn.reqs, 0), "internal")
    /\ txn' = IdleTxn
    /\ RetireOnWriteError
    /\ UNCHANGED <<preFailAt, queue, committerAlive, gate, disp, wal, landed,
                   reported, dbClosed, appliedSeq, pubCount, issued,
                   cancelled, decidedLive, lateWrite, walFails, retires, aborts, walDead,
                   writeFails, writerDead>>

(* A pre-apply db.write error on an OPEN Db (EmptyBatch, or a clock or     *)
(* WAL-buffer error before the memtable write): nothing applied. The Db     *)
(* stays open here, as after EmptyBatch; for the other errors the pinned    *)
(* SlateDB also ends its batch writer, so this over-approximates it. A      *)
(* bounded fault, never fair.                                                *)
WriteFailPreApply ==
    /\ committerAlive /\ txn.phase = "staged" /\ txn.writes /\ ~dbClosed /\ ~writerDead
    /\ writeFails < MaxWriteFail
    /\ writeFails' = writeFails + 1
    /\ Settle(Recs(txn.reqs, 0), "internal")
    /\ txn' = IdleTxn
    /\ RetireOnWriteError
    /\ UNCHANGED <<preFailAt, queue, committerAlive, gate, disp, wal, landed,
                   reported, dbClosed, appliedSeq, pubCount, issued,
                   cancelled, decidedLive, lateWrite, walFails, retires, aborts, walDead,
                   writerDead>>

(* slatedb write_batch appends to the WAL buffer and memtable and only then *)
(* runs maybe_freeze_current_memtable()?, whose error ends the batch       *)
(* writer: the committer sees Err (Internal) although the batch was applied *)
(* and may still land. closed_result is written later (DbCloseResult), so   *)
(* default reads return the batch in between (the pinned order of          *)
(* TLA-005-F5). A bounded fault, never fair.                                *)
WriteFailApplied ==
    /\ committerAlive /\ txn.phase = "staged" /\ txn.writes /\ ~dbClosed /\ ~writerDead
    /\ writeFails < MaxWriteFail
    /\ writeFails' = writeFails + 1
    /\ wal' = wal + 1
    /\ writerDead' = TRUE
    /\ lateWrite' = IF ho.terminal /\ lateWrite = 0 THEN wal + 1 ELSE lateWrite
    /\ Settle(Recs(txn.reqs, 0), "internal")
    /\ txn' = IdleTxn
    /\ RetireOnWriteError
    /\ UNCHANGED <<preFailAt, queue, committerAlive, gate, disp, landed, reported,
                   dbClosed, walDead, appliedSeq, pubCount, issued, cancelled,
                   decidedLive, walFails, retires, aborts>>

(* The committer's own rejects before the write (accounting divergence,    *)
(* stage_maintenance; finalize.rs): nothing is written, the group is       *)
(* answered Internal and the engine stays open. A bounded fault (it shares *)
(* MaxWriteFail with the db.write errors), never fair.                      *)
PreWriteReject ==
    /\ committerAlive /\ txn.phase = "staged" /\ txn.writes
    /\ writeFails < MaxWriteFail
    /\ writeFails' = writeFails + 1
    /\ preFailAt' = wal + 1
    /\ Settle(Recs(txn.reqs, 0), "internal")
    /\ txn' = IdleTxn
    /\ UNCHANGED <<queue, committerAlive, ho, gate, disp, strand, wal, landed,
                   reported, dbClosed, appliedSeq, pubCount, pubAtRetire, issued,
                   cancelled, decidedLive, strandedEver, strandedDurable,
                   lateWrite, walFails, retires, aborts, walDead, writerDead, writeRetire>>

(* run_lifecycle writes the batch writer's error into closed_result. *)
DbCloseResult ==
    /\ writerDead /\ ~dbClosed
    /\ dbClosed' = TRUE
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, gate, disp, strand, wal,
                   landed, reported, appliedSeq, pubCount, pubAtRetire, issued,
                   cancelled, nsent, res, sentDep, afterRetire, decidedLive,
                   strandedEver, strandedDurable, lateWrite, walFails, retires,
                   aborts, walDead, writeFails, writerDead, writeRetire>>

(* CommitTransaction::publish: applied mirrors + registration, one step.  *)
Publish ==
    /\ committerAlive /\ txn.phase = "written"
    /\ IF PublicationOp(ho)
       THEN /\ ho' = Register(ho, [seq |-> txn.seq, replies |-> Recs(txn.reqs, txn.seq)])
            /\ appliedSeq' = txn.seq
            /\ pubCount' = pubCount + 1
            /\ UNCHANGED <<preFailAt, nsent, res, sentDep, afterRetire, writerDead, writeRetire>>
       ELSE /\ Settle(Recs(txn.reqs, txn.seq), "moved")
            /\ UNCHANGED <<preFailAt, ho, appliedSeq, pubCount, writerDead, writeRetire>>
    /\ txn' = IdleTxn
    /\ UNCHANGED <<preFailAt, queue, committerAlive, gate, disp, strand, wal, landed, reported,
                   dbClosed, pubAtRetire, issued, cancelled, decidedLive,
                   strandedEver, strandedDurable, lateWrite, walFails, retires,
                   aborts, walDead, writeFails, writerDead, writeRetire>>

(* join_prior_barrier: (1) await the dispatch gate, (2) attach under the  *)
(* in_flight mutex, (3) after unlocking, reply or reject while the gate is *)
(* still held.                                                             *)
AttachGate ==
    /\ committerAlive /\ txn.phase = "staged" /\ ~txn.writes /\ gate = "free"
    /\ gate' = "commit"
    /\ txn' = [txn EXCEPT !.phase = "gated"]
    /\ UNCHANGED <<preFailAt, queue, committerAlive, ho, disp, strand, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   nsent, res, sentDep, afterRetire, decidedLive, strandedEver,
                   strandedDurable, lateWrite, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

AttachDecide ==
    /\ txn.phase = "gated"
    /\ LET v == AttachOp(ho)
       IN CASE v = "Pending" ->
                 /\ ho' = AttachToNewest(ho, Recs(txn.reqs, txn.dep))
                 /\ txn' = IdleTxn
                 /\ gate' = "free"
                 /\ UNCHANGED decidedLive
            [] v = "Durable" ->
                 /\ txn' = [txn EXCEPT !.phase = "replying", !.verdict = "Durable"]
                 /\ decidedLive' = [r \in Reqs |->
                                       decidedLive[r] \/ (r \in txn.reqs /\ ~ho.terminal)]
                 /\ UNCHANGED <<preFailAt, ho, gate, writerDead, writeRetire>>
            [] v = "Retired" ->
                 /\ txn' = [txn EXCEPT !.phase = "replying", !.verdict = "Retired"]
                 /\ UNCHANGED <<preFailAt, ho, gate, decidedLive, writerDead, writeRetire>>
    /\ UNCHANGED <<preFailAt, queue, committerAlive, disp, strand, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   nsent, res, sentDep, afterRetire, strandedEver, strandedDurable,
                   lateWrite, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

AttachReply ==
    /\ txn.phase = "replying"
    /\ Settle(Recs(txn.reqs, txn.dep), IF txn.verdict = "Durable" THEN "ok" ELSE "moved")
    /\ txn' = IdleTxn
    /\ gate' = "free"
    /\ UNCHANGED <<preFailAt, queue, committerAlive, ho, disp, strand, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   decidedLive, strandedEver, strandedDurable, lateWrite,
                   walFails, retires, aborts, walDead, writeFails, writerDead, writeRetire>>

(* ---------------- completer (dispatch_durable) ---------------- *)
Claim ==
    /\ disp.phase = "idle" /\ gate = "free"
    /\ LET c == TakeOp(ho, reported)
       IN /\ c.take # <<>>
          /\ disp' = [phase |-> "claimed", groups |-> c.take]
          /\ decidedLive' = [r \in Reqs |->
                                decidedLive[r] \/ (r \in ReqsIn(RepliesOf(c.take)) /\ ~ho.terminal)]
          /\ ho' = c.rest
    /\ gate' = "disp"
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, strand, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   nsent, res, sentDep, afterRetire, strandedEver, strandedDurable,
                   lateWrite, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

(* Ring/tail/usage/ack/signal/touch effects run after unlocking, gate held. *)
Replies ==
    /\ disp.phase = "claimed"
    /\ Settle(RepliesOf(disp.groups), "ok")
    /\ disp' = IdleDisp
    /\ gate' = "free"
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, strand, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   decidedLive, strandedEver, strandedDurable, lateWrite,
                   walFails, retires, aborts, walDead, writeFails, writerDead, writeRetire>>

(* ---------------- retirer (begin_close) ---------------- *)

(* The acker observes close_reason and closes the engine. *)
AckerClose ==
    /\ ~ho.terminal /\ dbClosed
    /\ DoBeginClose
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, gate, disp, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, issued, cancelled, nsent, res,
                   sentDep, afterRetire, decidedLive, lateWrite, walFails,
                   retires, aborts, walDead, writeFails, writerDead, writeRetire>>

(* Fleet move / shutdown / required-task exit. *)
ExternalClose ==
    /\ ~ho.terminal /\ retires < MaxRetire
    /\ DoBeginClose
    /\ retires' = retires + 1
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, gate, disp, wal, landed, reported,
                   dbClosed, appliedSeq, pubCount, issued, cancelled, nsent, res,
                   sentDep, afterRetire, decidedLive, lateWrite, walFails, aborts, walDead,
                   writeFails, writerDead, writeRetire>>

(* After unlocking: group.effects.reject(AppendErr::Moved) per stranded group. *)
RejectStranded ==
    /\ strand # <<>>
    /\ Settle(RepliesOf(strand), "moved")
    /\ strand' = <<>>
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, gate, disp, wal, landed,
                   reported, dbClosed, appliedSeq, pubCount, pubAtRetire, issued,
                   cancelled, decidedLive, strandedEver, strandedDurable,
                   lateWrite, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

(* drive_shutdown aborts a committer that outlives WORKER_GRACE. It can be *)
(* parked only at an await of a staged transaction (handle/producer loads, *)
(* db.write, the dispatch gate). Dropping it drops every oneshot sender it *)
(* owns and the channel; a dropped db.write may still have been applied.   *)
AbortCommitter ==
    /\ ho.terminal /\ committerAlive /\ aborts < MaxAbort
    /\ txn.phase = "staged"
    /\ \E applies \in (IF txn.writes /\ ~dbClosed /\ ~writerDead THEN {FALSE, TRUE} ELSE {FALSE}) :
         /\ wal' = IF applies THEN wal + 1 ELSE wal
         /\ lateWrite' = IF applies /\ lateWrite = 0 THEN wal + 1 ELSE lateWrite
    /\ Settle(Recs(txn.reqs \cup Elems(queue), 0), "dropped")
    /\ queue' = <<>>
    /\ txn' = IdleTxn
    /\ committerAlive' = FALSE
    /\ aborts' = aborts + 1
    /\ UNCHANGED <<preFailAt, ho, gate, disp, strand, landed, reported, dbClosed, appliedSeq,
                   pubCount, pubAtRetire, issued, cancelled, decidedLive,
                   strandedEver, strandedDurable, walFails, retires, walDead, writeFails,
                   writerDead, writeRetire>>

(* ---------------- storage ---------------- *)
Land ==
    /\ landed < wal /\ ~walDead
    /\ landed' = landed + 1
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, gate, disp, strand, wal,
                   reported, dbClosed, appliedSeq, pubCount, pubAtRetire, issued,
                   cancelled, nsent, res, sentDep, afterRetire, decidedLive,
                   strandedEver, strandedDurable, lateWrite, walFails, retires,
                   aborts, walDead, writeFails, writerDead, writeRetire>>

Report ==
    /\ reported < landed /\ ~dbClosed
    /\ reported' = landed
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, gate, disp, strand, wal, landed,
                   dbClosed, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   nsent, res, sentDep, afterRetire, decidedLive, strandedEver,
                   strandedDurable, lateWrite, walFails, retires, aborts, walDead, writeFails,
                   writerDead, writeRetire>>

WalFail ==
    /\ ~walDead /\ walFails < MaxWalFail
    /\ dbClosed' = TRUE
    /\ walDead' = TRUE
    /\ walFails' = walFails + 1
    /\ UNCHANGED <<preFailAt, queue, txn, committerAlive, ho, gate, disp, strand, wal, landed,
                   reported, appliedSeq, pubCount, pubAtRetire, issued, cancelled,
                   nsent, res, sentDep, afterRetire, decidedLive, strandedEver,
                   strandedDurable, lateWrite, retires, aborts, writeFails,
                   writerDead, writeRetire>>

(* ---------------- legitimate quiescence ---------------- *)
Settled ==
    /\ queue = <<>> /\ txn.phase = "idle" /\ disp.phase = "idle" /\ gate = "free"
    /\ strand = <<>> /\ ho.pending = <<>>
    /\ walDead \/ landed = wal
    /\ dbClosed \/ reported = landed
    /\ writerDead => dbClosed
    /\ \A r \in Reqs : issued[r]

Quiescent == Settled /\ UNCHANGED vars

Next ==
    \/ \E r \in Reqs : Issue(r) \/ Cancel(r)
    \/ Take \/ WriteOk \/ WriteRefused \/ WriteFailPreApply \/ WriteFailApplied
    \/ PreWriteReject \/ DbCloseResult \/ Publish
    \/ AttachGate \/ AttachDecide \/ AttachReply
    \/ Claim \/ Replies
    \/ AckerClose \/ ExternalClose \/ RejectStranded \/ AbortCommitter
    \/ Land \/ Report \/ WalFail
    \/ Quiescent

Spec == Init /\ [][Next]_vars

(* Liveness: fairness only on protocol actors that the implementation      *)
(* keeps scheduling (committer, completer, acker close, retirer rejection, *)
(* the committer's refused write on a closed Db) and on the storage        *)
(* contract's progress (a written WAL batch eventually lands while the WAL *)
(* flusher lives, and durable_seq is reported while the Db is open).       *)
(* Faults (WalFail, WriteFailPreApply, WriteFailApplied, PreWriteReject,   *)
(* ExternalClose, AbortCommitter), cancellation and issuing are bounded    *)
(* and unfair; DbCloseResult is not fair either (nothing waits on it);     *)
(* nothing is fair on a success outcome.                                   *)
LiveSpec ==
    /\ Spec
    /\ WF_vars(Take) /\ WF_vars(WriteOk) /\ WF_vars(WriteRefused) /\ WF_vars(Publish)
    /\ WF_vars(AttachGate) /\ WF_vars(AttachDecide) /\ WF_vars(AttachReply)
    /\ WF_vars(Claim) /\ WF_vars(Replies)
    /\ WF_vars(AckerClose) /\ WF_vars(RejectStranded)
    /\ WF_vars(Land) /\ WF_vars(Report)

----------------------------------------------------------------------------
(* ---------------- properties ---------------- *)
TypeOK ==
    /\ gate \in {"free", "disp", "commit"}
    /\ txn.phase \in {"idle", "staged", "written", "gated", "replying"}
    /\ disp.phase \in {"idle", "claimed"}
    /\ landed \in 0..wal /\ reported \in 0..landed
    /\ walDead => dbClosed

(* Structural sanity check: registration order makes seqs increase. *)
PendingSortedInv == PendingSorted(ho)

(* Each effect batch has one terminal owner: no reply is settled twice.    *)
SettledAtMostOnce == \A r \in Reqs : nsent[r] <= 1

(* A success reply is justified by the durability of the seq it depends on. *)
SuccessIsDurable == \A r \in Reqs : res[r] = "ok" => sentDep[r] <= landed

(* Work merely applied (registered, unclaimed) when retirement drained it  *)
(* never inherits the late-reply permission.                               *)
StrandedNeverSucceeds == \A r \in strandedEver : res[r] # "ok"

(* The section 1.5 boundary: a success sent after retirement was DECIDED   *)
(* while the handoff was live -- claimed by take_durable, or answered      *)
(* Durable by attach -- as read from `terminal` at the decision step.      *)
LateSuccessWasClaimedLive ==
    \A r \in Reqs : res[r] = "ok" /\ afterRetire[r] => decidedLive[r]

(* Retirement blocks new work: the retired incarnation publishes no new    *)
(* applied mirrors (r17b_late_successful_write asserts the same). This     *)
(* ghost restates the publication guard; the customer-visible consequence  *)
(* of removing that guard is checked by LateSuccessWasClaimedLive.         *)
RetiredEngineFrozen == ho.terminal => pubCount = pubAtRetire

RetiredHandoffEmpty == ho.terminal => ho.pending = <<>>

(* Liveness: every issued request is eventually settled server-side.       *)
AllIssuedSettle == \A r \in Reqs : issued[r] ~> (nsent[r] > 0)

(* ---------------- reachability witnesses (expected VIOLATED) ---------------- *)
Witness_LateDurableReply ==
    ~(\E r \in Reqs : res[r] = "ok" /\ afterRetire[r])
Witness_LateDurableAttachReply ==
    ~(\E r \in Reqs \ Writers : res[r] = "ok" /\ afterRetire[r])
Witness_UnclaimedDurableRejected ==
    ~(\E r \in strandedDurable : res[r] = "moved")
Witness_NonDurableRejected ==
    ~(\E r \in strandedEver \ strandedDurable : res[r] = "moved")
Witness_CancelledRequesterSettled ==
    ~(\E r \in Reqs : cancelled[r] /\ res[r] = "ok")
Witness_LateWriteLands ==
    ~(lateWrite > 0 /\ landed >= lateWrite)
Witness_AbortDropsReplies ==
    ~(\E r \in Reqs : res[r] = "dropped")
(* After the committer's own pre-write reject the engine stays open: a     *)
(* later group is written, becomes durable and is acknowledged on the same *)
(* engine.                                                                  *)
Witness_PreWriteRejectThenSuccess ==
    ~(preFailAt > 0 /\ \E r \in Writers : res[r] = "ok" /\ sentDep[r] >= preFailAt)
(* A db.write error retires the handoff, and a group it stranded is        *)
(* answered Moved.                                                          *)
Witness_WriteErrorStrandsGroup ==
    ~(writeRetire /\ \E r \in strandedEver : res[r] = "moved")
=============================================================================
