----------------------------- MODULE CommitGroups -----------------------------
(***************************************************************************)
(* TLA-005 -- Commit groups, dependency barriers, and observable replies.   *)
(*                                                                          *)
(* One shard engine incarnation at a time (restarts create a new one) with  *)
(* one committer (CommitTransaction::run), one durable dispatcher           *)
(* (ShardEngine::dispatch_durable, shared by the acker and the flush pump   *)
(* under `dispatch_gate`), retirement (ShardEngine::begin_close), the       *)
(* pinned SlateDB contract (write -> applied and readable, WAL PUT ->       *)
(* durable, DbStatus.durable_seq -> reported, closed_result -> close_reason *)
(* and failed reads) and clients with deadlines, response loss and retries. *)
(*                                                                          *)
(* One stream (one segment identity). Requests (a fixed universe):          *)
(*   "A" original final append: record "a", routing key k1, producer p     *)
(*       epoch 1 seq 0 with request hash hA, AppendFinish::Close           *)
(*   "R" the client's retry of A: SAME operation (same producer, seq, hash) *)
(*   "X" plain append: record "x", routing key k2, no producer              *)
(*   "Y" producer p seq 0 with a DIFFERENT hash hY (record "y", no close)   *)
(*   "S" the client's retry of Y: same operation as Y                       *)
(*   "C" producer-less empty AppendFinish::Close request: try_enqueue turns *)
(*       it into CommitOp::Close. It closes without writing a record; on a  *)
(*       closed stream it is the idempotent close success, a no-write       *)
(*       verdict                                                            *)
(* so the staged verdicts are: append success, duplicate success (with      *)
(* closed=true after A), idempotent close success, the state-dependent      *)
(* refusal Closed, and the state-dependent conflict ProducerSeqReused.      *)
(*                                                                          *)
(* Staging reads: the tail comes from the resident handle's applied mirror; *)
(* a producer row missing from the applied state is loaded with a default   *)
(* (Memory-level) db.get, which returns SlateDB's committed state --        *)
(* including a batch whose db.write returned Err after it was applied --    *)
(* until closed_result is written; after that the load fails and the        *)
(* request is answered Internal directly (append.rs:43). This is the pinned *)
(* SlateDB order. A db.write error retires the engine before the committer  *)
(* can take another group (CommitTransaction::write_failed), so no later    *)
(* group stages from that window (ASM-DURABILITY-8).                        *)
(*                                                                          *)
(* Observation boundary: a reply is OBSERVABLE when the server sends it on  *)
(* its oneshot (`sent`); client receipt is a later, lossy step (`cview`).   *)
(* Durable truth is the stream state recovered from the object store        *)
(* (DurableNow), which only the WAL-landing step extends.                   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC, CommitHandoff

CONSTANTS
    Reqs,                   \* subset of {"A","R","X","Y","S","C"} containing "A"
    MaxRestarts,            \* process restarts (crash, or reopen after retirement)
    MaxWalFail,             \* fatal WAL flush failures (SlateDB close_reason)
    MaxWriteFail,           \* group failures: db.write errors on an open Db, or
                            \* the committer's own pre-write rejects
    MaxRetire,              \* external retirements (ownership move, shutdown)
    AttachOp(_, _),         \* mutation point: CommitHandoff::attach verdict
    PublishVisibleOp(_, _), \* mutation point: reader-visible tail at registration
    StrandedResultOp(_),    \* mutation point: settlement of retired (stranded) groups
    ReplyPhases,            \* mutation point: dispatcher phases from which acks are sent
    WriteErrorRetires       \* mutation point: a db.write error retires the engine

ASSUME /\ Reqs \subseteq {"A", "R", "X", "Y", "S", "C"}
       /\ "A" \in Reqs
       /\ "S" \in Reqs => "Y" \in Reqs
       /\ MaxRestarts \in Nat /\ MaxWalFail \in Nat
       /\ MaxWriteFail \in Nat /\ MaxRetire \in Nat
       /\ WriteErrorRetires \in BOOLEAN

----------------------------------------------------------------------------
(* Request identities. R is the same operation as A, S the same as Y.        *)
OpOf(r)      == CASE r \in {"A", "R"} -> "a" [] r = "X" -> "x"
                  [] r \in {"Y", "S"} -> "y" [] r = "C" -> "none"
HasRecord(r) == r # "C"
KeyOf(r)     == IF r = "X" THEN "k2" ELSE "k1"
HashOf(r)    == CASE r \in {"A", "R"} -> "hA" [] r \in {"Y", "S"} -> "hY"
                  [] OTHER -> "none"
ClosesOf(r)  == r \in {"A", "R", "C"}
RetryOf(r)   == CASE r = "R" -> "A" [] r = "S" -> "Y" [] OTHER -> "none"

(* Stream state: the tail row, the record keys and the producer row.        *)
NoProd      == [hash |-> "none", last |-> 0]
EmptyStream == [log |-> <<>>, closed |-> FALSE, prod |-> NoProd]

(* Protocol verdict order (commit_plan.rs::decide_producer, then           *)
(* transaction/append.rs::append): duplicate/hash conflict precede the      *)
(* closed check; on a closed stream a producer-less empty close is the      *)
(* idempotent success and every other request is refused Closed.            *)
(* Stale epoch, epoch start and gaps cannot arise with one producer epoch   *)
(* and seq 0 (KANI-036..038 own that arithmetic).                           *)
Decide(r, s) ==
    IF HashOf(r) # "none" /\ s.prod.hash # "none"
    THEN IF s.prod.hash # HashOf(r) THEN "reused" ELSE "dup"
    ELSE IF s.closed THEN (IF r = "C" THEN "idem" ELSE "closed")
    ELSE "accept"

(* accept_append: one record at offset Len(log) (none for C); the producer  *)
(* row remembers the offset of the committing request.                      *)
AfterAccept(r, s) ==
    [log    |-> IF HasRecord(r)
                THEN Append(s.log, [op |-> OpOf(r), key |-> KeyOf(r)])
                ELSE s.log,
     closed |-> s.closed \/ ClosesOf(r),
     prod   |-> IF HashOf(r) # "none"
                THEN [hash |-> HashOf(r), last |-> Len(s.log)]
                ELSE s.prod]

(* Stage a whole group in queue order against an overlay (the group-local   *)
(* StreamOverlay starting from the applied mirror). Staged replies never    *)
(* escape here: they are DurableEffects owned by the transaction. A         *)
(* producer request whose row is in neither the applied state nor the       *)
(* overlay loads it from the Db (`db`: [open, st], the readable SlateDB     *)
(* state): a failed load answers that request Internal DIRECTLY.            *)
RECURSIVE StageFrom(_, _, _, _, _, _, _)
StageFrom(ops, s, rs, w, direct, loaded, db) ==
    IF ops = <<>>
    THEN [post |-> s, replies |-> rs, writes |-> w, direct |-> direct]
    ELSE LET r == Head(ops)
             needLoad == HashOf(r) # "none" /\ ~loaded /\ s.prod.hash = "none"
         IN IF needLoad /\ ~db.open
            THEN StageFrom(Tail(ops), s, rs, w, direct \cup {r}, loaded, db)
            ELSE LET s1 == IF needLoad THEN [s EXCEPT !.prod = db.st.prod] ELSE s
                     l1 == loaded \/ HashOf(r) # "none"
                     d  == Decide(r, s1)
                 IN IF d = "accept"
                    THEN LET s2 == AfterAccept(r, s1)
                         IN StageFrom(Tail(ops), s2,
                                rs \cup {[req |-> r, kind |-> "ok",
                                          off |-> Len(s1.log), closed |-> s2.closed]},
                                TRUE, direct, l1, db)
                    ELSE StageFrom(Tail(ops), s1,
                                rs \cup {[req |-> r, kind |-> d,
                                          off |-> IF d = "dup" THEN s1.prod.last ELSE Len(s1.log),
                                          closed |-> s1.closed]},
                                w, direct, l1, db)

----------------------------------------------------------------------------
(* Real (unmutated) protocol operators. Configurations bind the mutation    *)
(* points to these; negative controls bind them to mutants instead.         *)
RealAttach(h, rs)         == AttachVerdict(h)
RealPublishVisible(v, s)  == v          \* registration never publishes the durable tail
RealStrandedResult(x)     == "moved"    \* begin_close: effects.reject(AppendErr::Moved)
RealReplyPhases           == {"visible"} \* dispatch_durable: ring/tail publication, then acks
RealWriteErrorRetires     == TRUE       \* CommitTransaction::write_failed calls begin_close

----------------------------------------------------------------------------
VARIABLES
    store,      \* durable state recovered when this incarnation opened (persistent)
    wal,        \* Seq([seq, post, failed]) applied by db.write in this incarnation
    landed,     \* how many wal batches are actually in the object store
    reported,   \* DbStatus.durable_seq (a wal position in this incarnation)
    dbClosed,   \* closed_result written: close_reason set, reads and writes fail
    writerDead, \* SlateDB batch-writer task exited (later writes fail)
    walDead,    \* the WAL flusher is dead (fatal flush error): its buffer is dropped
    inc,        \* engine incarnation number
    queue,      \* committer mpsc channel
    applied,    \* applied mirrors (StreamState.applied, producer rows)
    ho,         \* CommitHandoff
    txn,        \* the committer's in-progress CommitTransaction
    gate,       \* dispatch_gate holder: "free" | "disp" | "commit"
    disp,       \* dispatcher: claimed groups and phase
    visible,    \* durable mirrors published by dispatch (StreamState.durable, ring, touch)
    cview,      \* client knowledge per request
    sent,       \* last reply the server sent per request (observable)
    nsent,      \* number of server sends per request
    restarts, walFails, writeFails, retires,
    attached,   \* ghost: requests whose verdict was attached to a pending group
    hist        \* ghost event tags (reachability witnesses only)

vars == <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc, queue,
          applied, ho, txn, gate, disp, visible, cview, sent, nsent, restarts,
          walFails, writeFails, retires, attached, hist>>

NoReply  == [kind |-> "none", off |-> 0, closed |-> FALSE, inc |-> 0]
IdleTxn  == [phase |-> "idle", replies |-> {}, post |-> EmptyStream,
             writes |-> FALSE, seq |-> 0]
IdleDisp == [phase |-> "idle", groups |-> <<>>]

Unknowns    == {"moved", "internal", "unavail"}     \* non-definitive outcomes
Definitive  == {"closed", "reused"}                  \* definitive refusals
Successes   == {"ok", "dup", "idem"}

DurableNow == IF landed = 0 THEN store ELSE wal[landed].post

(* What a default db.get returns: SlateDB's committed state, including a    *)
(* batch applied before its db.write returned Err; reads fail once         *)
(* closed_result is written (check_closed).                                *)
DbRead == [open |-> ~dbClosed,
          st   |-> IF wal = <<>> THEN store ELSE wal[Len(wal)].post]

(* Settle a set of staged replies; F chooses the sent kind per reply.       *)
SettleWith(rs, F(_)) ==
    /\ sent' = [r \in Reqs |->
                  IF \E x \in rs : x.req = r
                  THEN LET x == CHOOSE y \in rs : y.req = r
                       IN [kind |-> F(x), off |-> x.off, closed |-> x.closed, inc |-> inc]
                  ELSE sent[r]]
    /\ nsent' = [r \in Reqs |-> IF \E x \in rs : x.req = r THEN nsent[r] + 1 ELSE nsent[r]]

AsReplies(S) == {[req |-> r, kind |-> "none", off |-> 0, closed |-> FALSE] : r \in S}
Elems(s) == {s[i] : i \in 1..Len(s)}
ReqsOf(rs) == {x.req : x \in rs}

----------------------------------------------------------------------------
Init ==
    /\ store = EmptyStream
    /\ wal = <<>> /\ landed = 0 /\ reported = 0
    /\ dbClosed = FALSE /\ writerDead = FALSE /\ walDead = FALSE
    /\ inc = 1
    /\ queue = <<>> /\ applied = EmptyStream /\ ho = NewHandoff
    /\ txn = IdleTxn /\ gate = "free" /\ disp = IdleDisp
    /\ visible = EmptyStream
    /\ cview = [r \in Reqs |-> "idle"]
    /\ sent = [r \in Reqs |-> NoReply]
    /\ nsent = [r \in Reqs |-> 0]
    /\ restarts = 0 /\ walFails = 0 /\ writeFails = 0 /\ retires = 0
    /\ attached = {}
    /\ hist = {}

(* ---------------- clients ---------------- *)
(* A retry is issued only by its original's client, after the original's    *)
(* outcome is unknown to it (deadline, lost reply, or a non-definitive      *)
(* answer). The deadline can pass while the original is still queued.       *)
CanIssue(r) ==
    /\ cview[r] = "idle"
    /\ RetryOf(r) # "none" =>
         LET o == RetryOf(r)
         IN \/ cview[o] \in {"gaveup", "lost"}
            \/ (cview[o] = "received" /\ sent[o].kind \in Unknowns)

(* ShardEngine::try_command: is_closed() refuses admission (503, unknown).  *)
Issue(r) ==
    /\ CanIssue(r)
    /\ cview' = [cview EXCEPT ![r] = "waiting"]
    /\ IF ~ho.terminal
       THEN /\ queue' = Append(queue, r)
            /\ UNCHANGED <<sent, nsent>>
       ELSE /\ SettleWith(AsReplies({r}), LAMBDA x : "unavail")
            /\ UNCHANGED queue
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   applied, ho, txn, gate, disp, visible, restarts, walFails,
                   writeFails, retires, attached, hist>>

Deliver(r) ==
    /\ cview[r] = "waiting" /\ nsent[r] > 0
    /\ cview' = [cview EXCEPT ![r] = "received"]
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, ho, txn, gate, disp, visible, sent, nsent,
                   restarts, walFails, writeFails, retires, attached, hist>>

(* Response lost after the server dispatched it. *)
LoseReply(r) ==
    /\ cview[r] = "waiting" /\ nsent[r] > 0
    /\ cview' = [cview EXCEPT ![r] = "lost"]
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, ho, txn, gate, disp, visible, sent, nsent,
                   restarts, walFails, writeFails, retires, attached, hist>>

(* Client deadline: the receiver is dropped, the server keeps working. *)
GiveUp(r) ==
    /\ cview[r] = "waiting" /\ nsent[r] = 0
    /\ cview' = [cview EXCEPT ![r] = "gaveup"]
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, ho, txn, gate, disp, visible, sent, nsent,
                   restarts, walFails, writeFails, retires, attached, hist>>

(* ---------------- committer (CommitTransaction) ---------------- *)
(* committer_loop drains a non-empty prefix of the channel into ONE group.  *)
(* CommitTransaction::run rejects the whole group with Moved when the       *)
(* engine is closed (as does committer_loop's closed branch); otherwise it  *)
(* stages every op against the overlay. Only direct Internal replies (a     *)
(* failed producer-row load) escape during staging. The ghost tag marks a   *)
(* group answered Moved while the post-apply read window is open and a      *)
(* staging read would have returned a producer row that only the failed    *)
(* batch holds: the group the pre-fix committer decided from that batch.    *)
FailedRowReadable(ops) ==
    /\ writerDead /\ ~dbClosed
    /\ \E r \in Elems(ops) : /\ HashOf(r) # "none" /\ applied.prod.hash = "none"
                             /\ DbRead.st.prod.hash # "none"

CommitTake ==
    /\ txn.phase = "idle" /\ queue # <<>>
    /\ \E k \in 1..Len(queue) :
         LET ops == SubSeq(queue, 1, k)
         IN /\ queue' = SubSeq(queue, k + 1, Len(queue))
            /\ IF ho.terminal
               THEN /\ SettleWith(AsReplies(Elems(ops)), LAMBDA x : "moved")
                    /\ hist' = hist \cup (IF FailedRowReadable(ops)
                                          THEN {"moved-in-read-window"} ELSE {})
                    /\ UNCHANGED txn
               ELSE LET st == StageFrom(ops, applied, {}, FALSE, {}, FALSE, DbRead)
                    IN /\ txn' = [phase |-> "staged", replies |-> st.replies,
                                  post |-> st.post, writes |-> st.writes, seq |-> 0]
                       /\ SettleWith(AsReplies(st.direct), LAMBDA x : "internal")
                       /\ hist' = hist \cup
                            (IF st.direct # {} THEN {"producer-load-failed"} ELSE {})
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   applied, ho, gate, disp, visible, cview, restarts, walFails,
                   writeFails, retires, attached>>

(* CommitTransaction::write: the ONE db.write_with_options. Success means   *)
(* the batch is applied in SlateDB (memtable/WAL buffer), NOT durable.      *)
CommitWriteOk ==
    /\ txn.phase = "staged" /\ txn.writes /\ ~dbClosed /\ ~writerDead
    /\ wal' = Append(wal, [seq |-> Len(wal) + 1, post |-> txn.post, failed |-> FALSE])
    /\ txn' = [txn EXCEPT !.phase = "written", !.seq = Len(wal) + 1]
    /\ UNCHANGED <<store, landed, reported, dbClosed, writerDead, walDead, inc, queue,
                   applied, ho, gate, disp, visible, cview, sent, nsent, restarts,
                   walFails, writeFails, retires, attached, hist>>

(* CommitTransaction::write_failed, for every db.write error: log, then     *)
(* begin_close, then reject the group Internal (non-definitive).           *)
(* begin_close retires the handoff under in_flight and rejects the groups  *)
(* it strands with Moved, so CommitTake answers every later group Moved     *)
(* and no later group stages from the failed batch. One step: the committer *)
(* owns its group and the retirer owns the stranded groups. Between the     *)
(* error and retire() only dispatch claims of earlier, durable groups and   *)
(* other retirements can interleave; they do not depend on the failed batch *)
(* and are modelled as steps before this one. A handoff that is already     *)
(* terminal strands nothing. WriteErrorRetires is the mutation point:       *)
(* before the fix the committer only rejected the group.                    *)
WriteError(tags) ==
    LET retire   == WriteErrorRetires /\ ~ho.terminal
        stranded == IF retire THEN RepliesOf(RetireStranded(ho)) ELSE {}
    IN /\ ho' = IF retire THEN RetireHandoff(ho) ELSE ho
       /\ SettleWith(txn.replies \cup stranded,
                     LAMBDA x : IF x.req \in ReqsOf(txn.replies) THEN "internal"
                                ELSE StrandedResultOp(x))
       /\ txn' = IdleTxn
       /\ hist' = hist \cup tags
                       \cup (IF retire THEN {"write-error-retired"} ELSE {})
                       \cup (IF stranded # {} THEN {"write-error-stranded"} ELSE {})
                       \cup (IF \E x \in stranded : x.kind \in {"dup", "idem", "closed", "reused"}
                             THEN {"stranded-dependent"} ELSE {})

(* Error BEFORE the batch is applied (closed Db or exited batch writer,     *)
(* EmptyBatch, or a clock or WAL-buffer error before the memtable write):   *)
(* the Db is unchanged. An open Db stays open here, as after EmptyBatch;    *)
(* for the other errors the pinned SlateDB also ends its batch writer, so   *)
(* this over-approximates it.                                               *)
CommitWriteFail ==
    /\ txn.phase = "staged" /\ txn.writes
    /\ dbClosed \/ writerDead \/ writeFails < MaxWriteFail
    /\ writeFails' = IF dbClosed \/ writerDead THEN writeFails ELSE writeFails + 1
    /\ WriteError({})
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, gate, disp, visible, cview, restarts, walFails,
                   retires, attached>>

(* Error AFTER the batch was applied: slatedb batch_write.rs write_batch     *)
(* appends to the WAL buffer and memtable and advances last_committed_seq   *)
(* (the batch is visible to default reads), then maybe_freeze_current_       *)
(* memtable()? can fail (InvalidDBState), or a panic can drop the reply.    *)
(* The handler answers the committer, then the batch-writer task exits and  *)
(* only then does run_lifecycle write closed_result (dispatcher.rs), which  *)
(* is the later DbCloseResult step: default reads stay open in between.     *)
(* The batch may still land: a WAL flush already in flight, or the engine's *)
(* own storage close flushing the buffer.                                   *)
CommitWriteFailApplied ==
    /\ txn.phase = "staged" /\ txn.writes /\ ~dbClosed /\ ~writerDead
    /\ writeFails < MaxWriteFail
    /\ writeFails' = writeFails + 1
    /\ wal' = Append(wal, [seq |-> Len(wal) + 1, post |-> txn.post, failed |-> TRUE])
    /\ writerDead' = TRUE
    /\ WriteError({"failed-write-applied"})
    /\ UNCHANGED <<store, landed, reported, dbClosed, walDead, inc, queue, applied,
                   gate, disp, visible, cview, restarts, walFails, retires, attached>>

(* The committer's own rejects before the write (maintenance accounting    *)
(* divergence, stage_maintenance; finalize.rs): nothing is written, the     *)
(* group is answered Internal and the engine stays open.                    *)
CommitPreWriteReject ==
    /\ txn.phase = "staged" /\ txn.writes
    /\ writeFails < MaxWriteFail
    /\ writeFails' = writeFails + 1
    /\ SettleWith(txn.replies, LAMBDA x : "internal")
    /\ txn' = IdleTxn
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, ho, gate, disp, visible, cview, restarts, walFails,
                   retires, attached, hist>>

(* run_lifecycle writes the batch writer's error into closed_result. *)
DbCloseResult ==
    /\ writerDead /\ ~dbClosed
    /\ dbClosed' = TRUE
    /\ UNCHANGED <<store, wal, landed, reported, writerDead, walDead, inc, queue,
                   applied, ho, txn, gate, disp, visible, cview, sent, nsent,
                   restarts, walFails, writeFails, retires, attached, hist>>

(* CommitTransaction::publish: under in_flight, applied mirrors and the     *)
(* InFlightGroup registration are one step; a retired handoff rejects with  *)
(* Moved and publishes nothing (the batch may still become durable).        *)
CommitPublish ==
    /\ txn.phase = "written"
    /\ IF PublicationOpen(ho)
       THEN /\ ho' = Register(ho, [seq |-> txn.seq, post |-> txn.post,
                                   replies |-> txn.replies])
            /\ applied' = txn.post
            /\ visible' = PublishVisibleOp(visible, txn.post)
            /\ UNCHANGED <<sent, nsent>>
       ELSE /\ SettleWith(txn.replies, LAMBDA x : "moved")
            /\ UNCHANGED <<ho, applied, visible>>
    /\ txn' = IdleTxn
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, gate, disp, cview, restarts, walFails, writeFails, retires,
                   attached, hist>>

(* CommitTransaction::join_prior_barrier (no-write group): takes the        *)
(* dispatch gate, attaches under in_flight, and on Durable replies while    *)
(* still holding the gate. Atomic here: the gate excludes the dispatcher    *)
(* for the whole step and a verdict decided before a concurrent retirement  *)
(* stays justified (durability is monotone); TLA-006 splits this step.      *)
CommitAttach ==
    /\ txn.phase = "staged" /\ ~txn.writes /\ gate = "free"
    /\ LET v == AttachOp(ho, txn.replies)
       IN CASE v = "Retired" ->
                 /\ SettleWith(txn.replies, LAMBDA x : "moved")
                 /\ UNCHANGED <<ho, attached>>
            [] v = "Pending" ->
                 /\ ho' = AttachToNewest(ho, txn.replies)
                 /\ attached' = attached \cup ReqsOf(txn.replies)
                 /\ UNCHANGED <<sent, nsent>>
            [] v = "Durable" ->
                 /\ SettleWith(txn.replies, LAMBDA x : x.kind)
                 /\ UNCHANGED <<ho, attached>>
    /\ txn' = IdleTxn
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, gate, disp, visible, cview, restarts, walFails,
                   writeFails, retires, hist>>

(* ---------------- storage (pinned SlateDB contract) ---------------- *)
(* A WAL PUT lands: the next written batch is in the object store. Only a  *)
(* dead WAL flusher stops landing within the incarnation; a Db closed for   *)
(* another reason may still flush what its WAL buffer holds.                *)
WalLand ==
    /\ landed < Len(wal) /\ ~walDead
    /\ landed' = landed + 1
    /\ UNCHANGED <<store, wal, reported, dbClosed, writerDead, walDead, inc, queue,
                   applied, ho, txn, gate, disp, visible, cview, sent, nsent,
                   restarts, walFails, writeFails, retires, attached, hist>>

(* DbStatus.durable_seq catches up (DbWalObserver WalFlushed). *)
WalReport ==
    /\ reported < landed /\ ~dbClosed
    /\ reported' = landed
    /\ UNCHANGED <<store, wal, landed, dbClosed, writerDead, walDead, inc, queue,
                   applied, ho, txn, gate, disp, visible, cview, sent, nsent,
                   restarts, walFails, writeFails, retires, attached, hist>>

(* Fatal flush error: the WAL flusher's run_lifecycle writes closed_result  *)
(* BEFORE its cleanup marks the buffer closed (so reads and writes fail     *)
(* from this step on). A PUT whose reply was lost may still have landed:    *)
(* Restart may recover any written prefix (ASM-OBJSTORE-CAS).               *)
WalFail ==
    /\ ~walDead /\ walFails < MaxWalFail
    /\ dbClosed' = TRUE
    /\ walDead' = TRUE
    /\ walFails' = walFails + 1
    /\ UNCHANGED <<store, wal, landed, reported, writerDead, inc, queue, applied, ho,
                   txn, gate, disp, visible, cview, sent, nsent, restarts,
                   writeFails, retires, attached, hist>>

(* ---------------- retirement (ShardEngine::begin_close) ---------------- *)
(* Triggered by the acker observing close_reason, or externally (fleet     *)
(* move, shutdown, required-task exit). Under in_flight: retire() and the   *)
(* closed flag; the stranded groups are then rejected by the retirer.       *)
BeginClose ==
    /\ ~ho.terminal
    /\ dbClosed \/ retires < MaxRetire
    /\ retires' = IF dbClosed THEN retires ELSE retires + 1
    /\ LET rs == RepliesOf(RetireStranded(ho))
       IN /\ SettleWith(rs, StrandedResultOp)
          /\ hist' = hist \cup (IF \E x \in rs : x.kind \in {"dup", "idem", "closed", "reused"}
                                THEN {"stranded-dependent"} ELSE {})
                          \cup (IF dbClosed /\ walDead THEN {"acker-closed-on-wal-failure"}
                                ELSE {})
    /\ ho' = RetireHandoff(ho)
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, txn, gate, disp, visible, cview, restarts,
                   walFails, writeFails, attached>>

(* ---------------- durable dispatcher (dispatch_durable) ---------------- *)
(* Takes the dispatch gate, claims every group whose seq <= durable_seq.    *)
DispatchClaim ==
    /\ disp.phase = "idle" /\ gate = "free"
    /\ TakeDurable(ho, reported) # <<>>
    /\ disp' = [phase |-> "claimed", groups |-> TakeDurable(ho, reported)]
    /\ ho' = AfterTakeDurable(ho, reported)
    /\ gate' = "disp"
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, txn, visible, cview, sent, nsent, restarts,
                   walFails, writeFails, retires, attached, hist>>

(* ring_publish + handle.state.durable (+ touch journals) BEFORE the acks. *)
DispatchVisible ==
    /\ disp.phase = "claimed"
    /\ visible' = disp.groups[Len(disp.groups)].post
    /\ disp' = [disp EXCEPT !.phase = "visible"]
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, ho, txn, gate, cview, sent, nsent, restarts,
                   walFails, writeFails, retires, attached, hist>>

DispatchReply ==
    /\ disp.phase \in ReplyPhases
    /\ SettleWith(RepliesOf(disp.groups), LAMBDA x : x.kind)
    /\ disp' = IdleDisp
    /\ gate' = "free"
    /\ UNCHANGED <<store, wal, landed, reported, dbClosed, writerDead, walDead, inc,
                   queue, applied, ho, txn, visible, cview, restarts, walFails,
                   writeFails, retires, attached, hist>>

(* ---------------- process restart ---------------- *)
(* Crash at any point, or reopen after retirement. Volatile state (queue,   *)
(* applied mirrors, handoff, transaction, claims) is erased; the next open  *)
(* recovers the object store. An in-flight WAL PUT (or the close flush, or  *)
(* a PUT whose failure reply was lost) may have landed batches that were    *)
(* never reported: any written prefix from `landed` on may be recovered.    *)
Restart ==
    /\ restarts < MaxRestarts
    /\ \E k \in landed..Len(wal) :
         LET rec == IF k = 0 THEN store ELSE wal[k].post
         IN /\ store' = rec
            /\ applied' = rec
            /\ visible' = rec
            /\ hist' = hist \cup (IF k > reported THEN {"recovered-unreported"} ELSE {})
                             \cup (IF \E i \in 1..k : wal[i].failed
                                   THEN {"recovered-failed-write"} ELSE {})
    /\ restarts' = restarts + 1
    /\ inc' = inc + 1
    /\ wal' = <<>> /\ landed' = 0 /\ reported' = 0
    /\ dbClosed' = FALSE /\ writerDead' = FALSE /\ walDead' = FALSE
    /\ queue' = <<>> /\ ho' = NewHandoff /\ txn' = IdleTxn /\ gate' = "free"
    /\ disp' = IdleDisp
    /\ UNCHANGED <<cview, sent, nsent, walFails, writeFails, retires, attached>>

(* ---------------- legitimate quiescence ---------------- *)
Settled ==
    /\ queue = <<>> /\ txn.phase = "idle" /\ disp.phase = "idle" /\ gate = "free"
    /\ ho.pending = <<>>
    /\ walDead \/ landed = Len(wal)
    /\ dbClosed \/ reported = landed
    /\ writerDead => dbClosed
    /\ \A r \in Reqs : cview[r] # "waiting" /\ ~CanIssue(r)

Quiescent == Settled /\ UNCHANGED vars

Next ==
    \/ \E r \in Reqs : Issue(r) \/ Deliver(r) \/ LoseReply(r) \/ GiveUp(r)
    \/ CommitTake \/ CommitWriteOk \/ CommitWriteFail \/ CommitWriteFailApplied
    \/ CommitPreWriteReject \/ DbCloseResult \/ CommitPublish \/ CommitAttach
    \/ WalLand \/ WalReport \/ WalFail
    \/ BeginClose \/ Restart
    \/ DispatchClaim \/ DispatchVisible \/ DispatchReply
    \/ Quiescent

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* ---------------- properties ---------------- *)
HasOp(s, o)      == \E i \in 1..Len(s.log) : s.log[i].op = o
CountOp(s, o)    == Cardinality({i \in 1..Len(s.log) : s.log[i].op = o})
BindsAt(s, off, r) ==
    off < Len(s.log) /\ s.log[off + 1] = [op |-> OpOf(r), key |-> KeyOf(r)]
IsPrefix(a, b)   == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a

TypeOK ==
    /\ landed \in 0..Len(wal) /\ reported \in 0..landed
    /\ gate \in {"free", "disp", "commit"}
    /\ disp.phase \in {"idle", "claimed", "visible"}
    /\ txn.phase \in {"idle", "staged", "written"}
    /\ walDead => dbClosed
    /\ attached \subseteq Reqs
    /\ \A r \in Reqs : cview[r] \in {"idle", "waiting", "received", "lost", "gaveup"}

(* Structural sanity check (registration order makes seqs increase). *)
PendingSortedInv == PendingSorted(ho)

AtMostOneSettlement == \A r \in Reqs : nsent[r] <= 1

(* D1: an append success is observable only once its record is durable. *)
D1_AppendAckDurable ==
    \A r \in Reqs : sent[r].kind = "ok" /\ HasRecord(r) => HasOp(DurableNow, OpOf(r))

(* D2: a duplicate success (a no-write verdict) is observable only once    *)
(* the ORIGINAL write it reports is durable.                               *)
D2_DuplicateAckDurable ==
    \A r \in Reqs : sent[r].kind = "dup" => HasOp(DurableNow, OpOf(r))

(* D3: any success reporting closed=true -- the idempotent close of C, a   *)
(* fresh close, or a duplicate of the final append -- is barriered behind  *)
(* a durable close.                                                        *)
D3_CloseAckDurable ==
    \A r \in Reqs : sent[r].kind \in Successes /\ sent[r].closed => DurableNow.closed

(* D4: a state-dependent refusal is observable only once the state that   *)
(* justifies it is durable.                                                *)
D4_RefusalDurable ==
    \A r \in Reqs :
        /\ sent[r].kind = "closed" => DurableNow.closed
        /\ sent[r].kind = "reused" => DurableNow.prod.hash \notin {"none", HashOf(r)}

(* D7: the original and its retry resolve to exactly one committed copy,   *)
(* durably and in the serving engine's applied mirrors.                    *)
D7_ExactlyOnce ==
    \A o \in {"a", "x", "y"} : CountOp(DurableNow, o) <= 1 /\ CountOp(applied, o) <= 1

(* D8: an acknowledged offset holds exactly that operation and key; a      *)
(* close success reports exactly the final durable tail.                   *)
D8_ReplyBinding ==
    \A r \in Reqs :
        sent[r].kind \in Successes =>
            IF HasRecord(r) THEN BindsAt(DurableNow, sent[r].off, r)
            ELSE sent[r].off = Len(DurableNow.log)

(* D10: a definitively refused operation never appears durably (with the  *)
(* retries S and R it could, if a refusal ran ahead of its state).         *)
D10_RefusedNeverAppears ==
    \A r \in Reqs : sent[r].kind \in Definitive => ~HasOp(DurableNow, OpOf(r))

(* Durable publication: the durable mirrors set by dispatch (what          *)
(* Deliver::Durable readers, tail waiters and touch journals see) never    *)
(* run ahead of durability.                                                *)
PubVisibleDurable ==
    IsPrefix(visible.log, DurableNow.log) /\ (visible.closed => DurableNow.closed)

(* Tail/ring publication precedes the ack on the engine that sent it.      *)
AckAfterVisibility ==
    \A r \in Reqs : sent[r].kind \in Successes /\ sent[r].inc = inc
                    => /\ HasRecord(r) => HasOp(visible, OpOf(r))
                       /\ sent[r].closed => visible.closed

(* ---------------- reachability witnesses (expected VIOLATED) ---------------- *)
Witness_DuplicateAckReceived ==
    ~(\E r \in Reqs : sent[r].kind = "dup" /\ cview[r] = "received")
Witness_ClosedRefusalReceived ==
    ~(\E r \in Reqs : sent[r].kind = "closed" /\ cview[r] = "received")
Witness_LostAckThenRetryResolved ==
    ~("R" \in Reqs /\ cview["A"] = "lost" /\ sent["A"].kind = "ok"
      /\ sent["R"].kind = "dup" /\ cview["R"] = "received")
(* A verdict attached to a pending group is later received as a success.   *)
Witness_AttachedReplyReleased ==
    ~(\E r \in attached : sent[r].kind \in Successes /\ cview[r] = "received")
Witness_DependentRejectedWithGroup == ~("stranded-dependent" \in hist)
Witness_UnreportedBatchRecovered == ~("recovered-unreported" \in hist)
(* The acker retires the engine because the WAL flusher failed.            *)
Witness_WalFailureRetires == ~("acker-closed-on-wal-failure" \in hist)
Witness_SeqReusedRefusal == ~(\E r \in Reqs : sent[r].kind = "reused")
Witness_FailedWriteRecovered == ~("recovered-failed-write" \in hist)
(* C's idempotent close, attached behind the closing group, reaches C.     *)
Witness_IdempotentCloseAfterAttach ==
    ~("C" \in Reqs /\ "C" \in attached /\ sent["C"].kind = "idem"
      /\ cview["C"] = "received")
(* C closes the stream by writing; A is then refused Closed.               *)
Witness_CloseByWriteRefusesFinalAppend ==
    ~("C" \in Reqs /\ sent["C"].kind = "ok" /\ sent["A"].kind = "closed")
(* Y's refusal is lost and its retry S is refused the same way.            *)
Witness_RefusedRetryResolved ==
    ~("S" \in Reqs /\ sent["Y"].kind = "reused" /\ cview["Y"] = "lost"
      /\ sent["S"].kind = "reused" /\ cview["S"] = "received")
(* A producer-row load fails on a closed Db: Internal is sent directly.     *)
Witness_ProducerLoadFailsInternal == ~("producer-load-failed" \in hist)
(* A db.write error retires the engine and strands an earlier registered   *)
(* group, which is answered Moved although it might have become durable.   *)
Witness_WriteErrorStrandsGroup == ~("write-error-stranded" \in hist)
(* After a post-apply write error, a group whose producer row only the      *)
(* failed batch holds is taken while default reads still return that row,   *)
(* and it is answered Moved instead of being staged.                        *)
Witness_ReadWindowGroupMoved == ~("moved-in-read-window" \in hist)
=============================================================================
