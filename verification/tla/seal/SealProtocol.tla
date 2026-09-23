---------------------------- MODULE SealProtocol ----------------------------
(***************************************************************************)
(* TLA-002 (seal claims, renewal, competing takeover reservations) and     *)
(* TLA-003 (final-record sealing, ambiguous append outcomes, owed debt).   *)
(*                                                                         *)
(* One stream incarnation, one segment, one routing key, one shard served  *)
(* by one of several server processes.  Independent state is kept apart:   *)
(*   desc   -- the registry descriptor (claim, allocator, terminal state); *)
(*   seg, lanes -- the segment's durable SlateDB state, including its      *)
(*             durable seal-fence row;                                     *)
(*   owner  -- the process the ownership ring assigns the shard to;        *)
(*   eng    -- the owner's resident committer: queue and seal-fence cache  *)
(*             (volatile: lost when the engine is replaced);               *)
(*   h      -- request handlers, each living in one process (volatile:     *)
(*             lost when THAT process crashes, kept on engine replacement).*)
(* Registry mutations are linearised at their conditional PUT, justified   *)
(* by TLA-001 (RegistryCas): a mutate_incarnation call is equivalent to    *)
(* applying its pure `decide` at the winning CAS.  A registry reply can    *)
(* still be lost after the write committed.                                *)
(*                                                                         *)
(* Operations are identified the way production identifies them: a final  *)
(* op's claim id and producer lane are functions of (surface, content,     *)
(* coordination), so two operations with the same bytes but different      *)
(* coordination differ, and two producer-less finals with the same bytes   *)
(* share one synthetic lane.                                               *)
(*                                                                         *)
(* Production owners (see README.md for the full mapping):                 *)
(*   src/application/lifecycle.rs, lifecycle/claims.rs, lifecycle/raw_close.rs *)
(*   src/application/append.rs, append/close.rs, append/submit.rs,          *)
(*   append/content.rs, src/product.rs (seal-with-final handler)           *)
(*   src/shard/commit_plan.rs (decide_producer, seal_authorized)           *)
(*   src/shard/transaction/append.rs, transaction/maintenance.rs           *)
(*   src/shard.rs (seal_fences, begin_close), src/shard_directory.rs       *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

CONSTANTS
    FinalOps,    \* final-bearing seals (product seal-with-final / raw close-with-content)
    PlainOps,    \* plain seals (product :seal, raw close-only): operation id ""
    AppendOps,   \* ordinary non-closing producer appends
    Surface,     \* [FinalOps \cup AppendOps -> {"product", "raw"}]
    Content,     \* [FinalOps \cup AppendOps -> symbolic record bytes]
    Producer,    \* [FinalOps \cup AppendOps -> client producer lane, or NONE]
    SeqOf,       \* [FinalOps \cup AppendOps -> Nat]: producer sequence (synthetic lanes: 0)
    Slots,       \* [Ops -> SUBSET Nat]: concurrent handler slots per operation
    Budget,      \* [Ops -> Nat]: requests the client issues (ops in Unbounded ignore it)
    Unbounded,   \* ops whose client retries forever (the liveness recovery actor)
    Validity,    \* [HandlerIds -> {"ok", "pre", "capacity", "ceiling"}]: how that slot's
                 \* request fares under the configuration of the instance that handles it
                 \* (HomeOf).  "pre": refused by product.rs's instance-independent
                 \* pre-intent checks (key, routing key, producer headers; product only).
                 \* "capacity": exceeds that instance's per-stream ingest capacity
                 \* (permanently_unadmittable).  "ceiling": exceeds that instance's
                 \* MAX_RECORD_PAYLOAD_BYTES; on the raw surface it also stands for every
                 \* other content refusal, all of which are deferred for a close with content.
    Procs,       \* server processes
    HomeOf,      \* [HandlerIds -> Procs]: where that slot's requests are handled
    InitOwner,   \* the ring owner of the shard at start
    MaxGen,      \* generation ceiling (enforced by a TLC CONSTRAINT in safety configs)
    MaxCrash,    \* process crashes (that process's handlers lost; its engine too if owner)
    MaxRetire,   \* engine replacements without a crash (move, eviction, fatal close)
    MaxCancel,   \* client disconnects that drop a handler (server keeps queued work)
    MaxTimeout,  \* APPEND_TIMEOUT expiries while an append is queued
    MaxRegFault, \* registry faults (read unavailable / write committed but reply lost)
    MaxEnqFail,  \* engine opening / queue full / backpressure refusals before enqueue
    MaxFenceReadFail, \* reads of the durable fence row that fail (answered Internal)
    MaxHeldFence, \* fence groups staged before their durability (TLA-002-F2 window)
    MaxClaimLoop, \* claim_seal's iteration bound (production: `for _ in 0..6`);
                  \* 0 = safety over-approximation: unbounded restarts, and any
                  \* restart may instead give up (Resumable)
    WitnessMode, \* record witness-only history (never read by actions or properties)
    NONE, PLAIN

Ops == FinalOps \cup PlainOps \cup AppendOps
LaneOps == FinalOps \cup AppendOps
HandlerIds == UNION {{<<o, s>> : s \in Slots[o]} : o \in Ops}
HOp(hid) == hid[1]
Max(a, b) == IF a > b THEN a ELSE b

\* The claim operation id: product seal_op_id_full(record, key, producer) and
\* raw seal_op_id_semantic(request hash, key, coordination) are
\* domain-separated hashes of these components (claims.rs 132-176).
OpId(o) == IF o \in FinalOps THEN <<Surface[o], Content[o], Producer[o], SeqOf[o]>> ELSE PLAIN
\* The producer lane: the client's, or the synthetic `rawseal.{semantic id}`
\* lane (close.rs 101-112) whose id depends only on the bytes when the
\* request carries no coordination -- for a product final too
\* (close_identity None, product.rs submit_product_append).
LaneOf(o) == IF Producer[o] # NONE THEN Producer[o] ELSE <<"rawseal", Content[o]>>
\* The product request hash covers the seal flag (product_request_hash); a
\* product request without producer headers, and every raw request, has none.
ReqHash(o) == IF Surface[o] = "product" /\ Producer[o] # NONE
              THEN <<Content[o], o \in FinalOps>> ELSE NONE
Lanes == {LaneOf(o) : o \in LaneOps}

ASSUME /\ FinalOps \cap PlainOps = {} /\ FinalOps \cap AppendOps = {}
       /\ PlainOps \cap AppendOps = {}
       /\ PLAIN \notin Ops /\ NONE \notin Ops
       /\ \A o \in AppendOps : Producer[o] # NONE
       /\ \A o \in FinalOps : Producer[o] = NONE => SeqOf[o] = 0
       /\ \A o1, o2 \in FinalOps : o1 # o2 => OpId(o1) # OpId(o2)
       /\ \A hid \in HandlerIds : Validity[hid] \in {"ok", "pre", "capacity", "ceiling"}
       /\ \A hid \in HandlerIds : HOp(hid) \notin FinalOps => Validity[hid] = "ok"
       /\ \A hid \in HandlerIds : HOp(hid) \in FinalOps /\ Surface[HOp(hid)] = "raw"
                                  => Validity[hid] # "pre"
       /\ \A hid \in HandlerIds : HomeOf[hid] \in Procs
       /\ InitOwner \in Procs

VARIABLES
    desc,    \* registry descriptor: [sealed, sealOp, claim, counter]
    seg,     \* durable segment: [closed, closer (op id), finalRec (op whose record closed it),
             \*                   fence (the durable seal-fence row, seal_fence_key)]
    lanes,   \* durable producer rows: [Lanes -> [seq, hash]]
    eng,     \* the owner's committer: [fence (cache), q (queue), held (a fence group
             \*                         staged but not yet durable, or NONE)]
    owner,   \* the ring owner of the shard
    h,       \* request handlers
    bud,     \* remaining client requests per op
    faults,  \* remaining fault budgets
    hist,    \* history read by properties (never by protocol actions)
    wit,     \* witness-only history (WitnessMode)
    wset     \* witness-only history sets and values (WitnessMode)

vars == <<desc, seg, lanes, eng, owner, h, bud, faults, hist, wit, wset>>

(***************************************************************************)
(* Descriptor helpers                                                      *)
(***************************************************************************)
Owes(c) == c # NONE /\ c.fin /\ ~c.com
ClaimOp(d) == IF d.claim = NONE THEN NONE ELSE d.claim.op
Claim(op, fin, gen) == [op |-> op, fin |-> fin, com |-> FALSE, gen |-> gen, lap |-> FALSE]

(***************************************************************************)
(* Guards and classifications that negative controls replace.              *)
(***************************************************************************)

\* shard::commit_plan::seal_authorized
SealAuthorized(g, closing, fence) ==
    IF g # NONE THEN g >= fence ELSE ~closing \/ fence = 0

\* CommitTransaction::fence writes max(cached, requested) to the segment's
\* durable fence row in the fence's own commit group (maintenance.rs 136-167).
PersistFence(sg, g) == [sg EXCEPT !.fence = Max(@, g)]

\* A newly opened engine's seal_fences cache is empty; CommitTransaction::
\* seal_fence reads the durable row on a miss (maintenance.rs 89-108), so
\* the fence an engine enforces starts at the row.  The pre-fix control
\* (MC_SealTakeover_NcEngineFence) substitutes 0: the engine-resident map
\* that died with its engine (TLA-002-F1).
OpenedEngineFence(sg) == sg.fence

\* install_reserved_claim: the lapsed claim is unchanged AND the caller's
\* reservation is still the newest allocation.
InstallAllowed(d, old, res) ==
    /\ d.claim # NONE
    /\ d.claim.op = old.op /\ d.claim.gen = old.gen
    /\ d.counter = res

\* take_over_abandoned: install only after the fence's queue-ordered,
\* durability-barriered reply reported NOT closed.
FenceAcknowledged(hid) ==
    h[hid].rep # NONE /\ h[hid].rep.err = NONE /\ ~h[hid].rep.closed

\* abandon_seal_intent: exact operation AND generation.
ReleaseMatches(c, op, g) == c.op = op /\ c.gen = g

\* seal_final `if !ack.closed` / complete_raw_close `if !ack.closed`:
\* only a CLOSED acknowledgement can complete a final close.
AckCompletesFinal(rep) == rep.closed

\* complete_raw_close: owns_final = resumes_owed_final || (carries && !duplicate)
OwnsFinal(resumed, rep) == resumed \/ ~rep.dup

\* install_intent (close.rs 192-214): once begin_sealing_for_close returns
\* this operation's generation for a Final intent (installed, taken over or
\* renewed), the plan owes that final and the admission snapshot's Sealing
\* refusal is cleared (TLA-003-F2).  The pre-fix control
\* (MC_FinalSeal_NcAdmissionRefusal) keeps the admission-time values.
ClaimedFinalPlan(hr) == [hr EXCEPT !.resumed = TRUE, !.rej = FALSE]

\* product.rs pre-intent checks: which validity verdicts refuse a product
\* seal-with-final before its claim.  They measure ingest capacity
\* (permanently_unadmittable) and, through content::stored_records, the
\* per-record ceiling on the exact wire body the final append submits
\* (TLA-003-F3 fix).  The pre-fix control (MC_FinalSeal_NcCeilingAfterIntent)
\* drops "ceiling": the claim is published and the append path refuses the
\* record afterwards.
ProductPreIntentRefuses(v) == v \in {"pre", "capacity", "ceiling"}

\* lifecycle::claims::final_err_disposition (raw surface), restricted to
\* the committer errors this model produces.
RawDisposition(err) ==
    IF err \in {"Closed", "SealSuperseded", "ProducerSeqReused", "BadBody"}
    THEN "definitive" ELSE "ambiguous"

\* AppendFailure::from_commit + definitively_rejected: the class the client
\* is shown; the product surface's seal_final uses it as its disposition.
ClientKind(err) ==
    CASE err = "Closed" -> "closed"
      [] err = "SealSuperseded" -> "superseded"
      [] err = "ProducerSeqReused" -> "definitive"
      [] err = "BadBody" -> "invalid"
      [] err = "ProducerGap" -> "retryable"
      [] err = "Moved" -> "retryable"
      [] err = "Internal" -> "retryable"

DefinitiveKinds == {"closed", "superseded", "definitive", "invalid"}

Disposition(o, err) ==
    IF Surface[o] = "product"
    THEN IF ClientKind(err) \in DefinitiveKinds THEN "definitive" ELSE "ambiguous"
    ELSE RawDisposition(err)

(***************************************************************************)
(* Pure registry decisions                                                  *)
(***************************************************************************)

\* lifecycle::claims::decide_claim
DecideClaim(d, opid, fin) ==
    IF d.sealed
    THEN [kind |-> IF d.sealOp = opid /\ opid # PLAIN THEN "completed" ELSE "sealed",
          gen |-> NONE, next |-> d, old |-> NONE]
    ELSE IF d.claim # NONE
    THEN IF d.claim.op = opid \/ (opid = PLAIN /\ ~Owes(d.claim))
         THEN [kind |-> "ours", gen |-> d.counter + 1, old |-> NONE,
               next |-> [d EXCEPT !.counter = d.counter + 1,
                                  !.claim = [d.claim EXCEPT !.gen = d.counter + 1,
                                                            !.lap = FALSE]]]
         ELSE IF Owes(d.claim) /\ d.claim.lap
         THEN [kind |-> "abandoned", gen |-> NONE, next |-> d,
               old |-> [op |-> d.claim.op, gen |-> d.claim.gen]]
         ELSE [kind |-> "conflict", gen |-> NONE, next |-> d, old |-> NONE]
    ELSE [kind |-> "installed", gen |-> d.counter + 1, old |-> NONE,
          next |-> [d EXCEPT !.counter = d.counter + 1,
                             !.claim = Claim(opid, fin, d.counter + 1)]]

\* lifecycle::renew_owed_claim (raw exact retry of an owed final)
DecideRenewOwed(d, opid) ==
    IF d.claim # NONE /\ d.claim.op = opid /\ Owes(d.claim)
    THEN [ok |-> TRUE, gen |-> d.counter + 1,
          next |-> [d EXCEPT !.counter = d.counter + 1,
                             !.claim = [d.claim EXCEPT !.gen = d.counter + 1, !.lap = FALSE]]]
    ELSE [ok |-> FALSE, gen |-> NONE, next |-> d]

\* lifecycle::mark_final_committed; lap is only ever consulted while a claim
\* owes its record, so marking also canonicalises it.
DecideMark(d, opid, g) ==
    IF d.sealed /\ d.sealOp = opid THEN [ok |-> TRUE, next |-> d]
    ELSE IF d.claim = NONE THEN [ok |-> FALSE, next |-> d]
    ELSE IF d.claim.op # opid \/ d.claim.gen # g THEN [ok |-> FALSE, next |-> d]
    ELSE IF ~Owes(d.claim) THEN [ok |-> TRUE, next |-> d]
    ELSE [ok |-> TRUE,
          next |-> [d EXCEPT !.claim = [d.claim EXCEPT !.com = TRUE, !.lap = FALSE]]]

(***************************************************************************)
(* Records                                                                  *)
(***************************************************************************)
IdleH == [pc |-> "idle", gen |-> NONE, res |-> NONE, old |-> NONE, rep |-> NONE,
          resumed |-> FALSE, rej |-> FALSE, rop |-> NONE, orop |-> NONE,
          behalf |-> FALSE, rawdup |-> FALSE, org |-> NONE, iter |-> 0]

Rep(err, dup, closed) == [err |-> err, dup |-> dup, closed |-> closed]
MovedRep == Rep("Moved", FALSE, FALSE)

\* `ahead`: the takeovers whose fence was enqueued behind this element
\* (history only; never read by the committer).
Req(kind, hid, op, g, rej, dfr) ==
    [kind |-> kind, hid |-> hid, op |-> op, gen |-> g, rej |-> rej, dfr |-> dfr, ahead |-> {}]

StartPc(hid) ==
    LET o == HOp(hid) IN
    IF o \in FinalOps
    THEN IF Surface[o] = "raw" \/ ProductPreIntentRefuses(Validity[hid])
         THEN "validate" ELSE "claim"
    ELSE IF o \in PlainOps THEN "rs_prep" ELSE "a_prep"

Queued(hid) == \E i \in DOMAIN eng.q : eng.q[i].hid = hid
\* A request the committer still owes an answer: queued, or a staged fence.
Pending(hid) == Queued(hid) \/ (eng.held # NONE /\ eng.held.hid = hid)

(***************************************************************************)
(* History bookkeeping                                                      *)
(***************************************************************************)

WitUpd(fs) == IF WitnessMode THEN [f \in DOMAIN wit |-> wit[f] \/ f \in fs] ELSE wit

\* The record the segment was closed by carries o's bytes.
RecordIs(o) == seg.closed /\ seg.finalRec # NONE /\ Content[seg.finalRec] = Content[o]

\* What each 2xx answer proves (L15, and the customer view of L3/L12): a
\* product or owned raw final "success" -- sealed under exactly this
\* operation, closed by a record with this operation's bytes; the raw
\* duplicate-of-a-closed-tail acknowledgement (the Durable Streams
\* idempotent-producer answer: "this tuple is already appended, and the
\* stream is closed") -- the stream is sealed and closed and this
\* operation's producer tuple is durable; a plain seal -- sealed.
SuccessProven(o, kind, d) ==
    IF kind = "success" /\ o \in PlainOps THEN d.sealed
    ELSE IF kind = "success" /\ o \in FinalOps
    THEN d.sealed /\ d.sealOp = OpId(o) /\ RecordIs(o)
    ELSE IF kind = "dupAck"
    THEN d.sealed /\ seg.closed /\ lanes[LaneOf(o)].seq >= SeqOf[o]
    ELSE TRUE

\* A final op's "stream closed" refusal is truthful only if the stream is
\* closed, sealed, or being sealed by ANOTHER operation.
ClosedTruthful(o, d) ==
    seg.closed \/ d.sealed \/ (d.claim # NONE /\ d.claim.op # OpId(o))

RespHistOn(hs, o, kind, d) ==
    [hs EXCEPT
        !.badClosed = @ \/ (kind = "closed" /\ o \in FinalOps /\ ~ClosedTruthful(o, d)),
        !.badSuccess = @ \/ ~SuccessProven(o, kind, d)]

LostReplyW(o, kind) ==
    IF kind \in {"success", "dupAck"} /\ o \in wset.orphan THEN {"lostReplyRetry"} ELSE {}

\* A raw final that obtained its claim by taking over another operation's
\* lapsed claim is answered success, sealed under its own operation.
RawTookW(hid, kind, d) ==
    IF kind = "success" /\ hid \in wset.rawTook /\ d.sealed /\ d.sealOp = OpId(HOp(hid))
    THEN {"rawTookOwn"} ELSE {}

\* A final whose record closed the segment while no claim of its own stood
\* is later answered success.
HealW(o, kind) == IF kind = "success" /\ o \in wset.orphanClosed THEN {"healed"} ELSE {}

\* Answer the client and free the slot.  `d` is the descriptor after this
\* step, `hs` the history to extend, `fs` witness flags.
RespondH(hid, kind, d, hs, fs) ==
    /\ h' = [h EXCEPT ![hid] = IdleH]
    /\ hist' = RespHistOn(hs, HOp(hid), kind, d)
    /\ wit' = WitUpd(fs \cup LostReplyW(HOp(hid), kind) \cup RawTookW(hid, kind, d)
                     \cup HealW(HOp(hid), kind))

RespondW(hid, kind, d, fs) == RespondH(hid, kind, d, hist, fs)
Respond(hid, kind) == RespondW(hid, kind, desc, {})

\* A handler's queued requests stay in the queue when it goes away.
OrphanQ(q, hid) == [i \in DOMAIN q |-> IF q[i].hid = hid THEN [q[i] EXCEPT !.hid = NONE] ELSE q[i]]
OrphanEng(e, hid) ==
    [e EXCEPT !.q = OrphanQ(e.q, hid),
              !.held = IF e.held # NONE /\ e.held.hid = hid THEN [e.held EXCEPT !.hid = NONE]
                       ELSE e.held]

\* A freshly opened engine: empty queue, fence cache seeded from the row.
OpenedEngine(sg) == [fence |-> OpenedEngineFence(sg), q |-> <<>>, held |-> NONE]

\* A staged fence group of a closing engine may or may not become durable.
StrandedFence == IF eng.held = NONE THEN {seg} ELSE {seg, PersistFence(seg, eng.held.gen)}

OpenedW(sg) == IF WitnessMode THEN [wset EXCEPT !.openFence = OpenedEngineFence(sg)] ELSE wset

Init ==
    /\ desc = [sealed |-> FALSE, sealOp |-> NONE, claim |-> NONE, counter |-> 0]
    /\ seg = [closed |-> FALSE, closer |-> NONE, finalRec |-> NONE, fence |-> 0]
    /\ lanes = [l \in Lanes |-> [seq |-> -1, hash |-> NONE]]
    /\ eng = [fence |-> 0, q |-> <<>>, held |-> NONE]
    /\ owner = InitOwner
    /\ h = [hid \in HandlerIds |-> IdleH]
    /\ bud = [o \in Ops |-> Budget[o]]
    /\ faults = [crash |-> MaxCrash, retire |-> MaxRetire, cancel |-> MaxCancel,
                 timeout |-> MaxTimeout, reg |-> MaxRegFault, enq |-> MaxEnqFail,
                 fread |-> MaxFenceReadFail, held |-> MaxHeldFence]
    /\ hist = [fenceSeen |-> 0, staleEffect |-> FALSE, badInstall |-> FALSE,
               liveFenced |-> FALSE, earlyInstall |-> FALSE, badRelease |-> FALSE,
               badClosed |-> FALSE, badSuccess |-> FALSE, invalidIntent |-> FALSE,
               installOverMarked |-> FALSE]
    /\ wit = [install |-> FALSE, lower |-> FALSE, behalfSealed |-> FALSE,
              superseded |-> FALSE, renewAfterRes |-> FALSE, moved |-> FALSE,
              invalidRefused |-> FALSE, gap |-> FALSE, defRelease |-> FALSE,
              dupRelease |-> FALSE, seqReused |-> FALSE, lostReplyRetry |-> FALSE,
              rawRetained |-> FALSE, notOwner |-> FALSE, appliedMoved |-> FALSE,
              sharedLaneDup |-> FALSE, crossOwner |-> FALSE, refusedByRow |-> FALSE,
              fenceUnverified |-> FALSE, rawTookOwn |-> FALSE, markedRetry |-> FALSE,
              healed |-> FALSE]
    /\ wset = [resv |-> {}, orphan |-> {}, openFence |-> 0, rawTook |-> {},
               orphanClosed |-> {}]

(***************************************************************************)
(* Client                                                                   *)
(***************************************************************************)

\* The client issues a (first or exact-retry) request.  A lost response
\* only changes client knowledge, so retries are not conditioned on it.
Issue(hid) ==
    LET o == HOp(hid) IN
    /\ h[hid].pc = "idle"
    /\ o \in Unbounded \/ bud[o] > 0
    /\ bud' = IF o \in Unbounded THEN bud ELSE [bud EXCEPT ![o] = @ - 1]
    /\ h' = [h EXCEPT ![hid].pc = StartPc(hid),
                      ![hid].rop = IF o \in PlainOps THEN PLAIN ELSE NONE]
    /\ wset' = IF WitnessMode THEN [wset EXCEPT !.rawTook = @ \ {hid}] ELSE wset
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, faults, hist, wit>>

(***************************************************************************)
(* Final-bearing seal: admission, validation and claim                      *)
(***************************************************************************)

\* Product: product.rs pre-intent checks (key, routing key, producer
\* headers, ingest capacity, record ceiling) -- reached only by a request
\* that fails them.
\* Raw: append::execute_once -> close::prepare_close (admission snapshot:
\* is_owed_final, sealed_reject_new; an owed exact retry is RENEWED here)
\* -> content::parse_content (with a producer, which a close with content
\* always has, every content refusal is deferred; only ingest capacity is
\* refused here) -> install_intent (skipped when sealed, owed or deferred).
FValidate(hid) ==
    LET o == HOp(hid)
        c == desc.claim
        raw == Surface[o] = "raw"
        owedByMe == c # NONE /\ c.op = OpId(o) /\ Owes(c)
    IN
    /\ h[hid].pc = "validate"
    /\ IF ~raw
       THEN RespondW(hid, "invalid", desc, {"invalidRefused"})
       ELSE IF owedByMe
       THEN \* renew_owed_claim first; parse_content runs after the renewal
            /\ h' = [h EXCEPT ![hid].pc = "claim", ![hid].resumed = TRUE, ![hid].rej = FALSE]
            /\ UNCHANGED <<hist, wit>>
       ELSE IF Validity[hid] = "capacity"
       THEN RespondW(hid, "invalid", desc, {"invalidRefused"})
       ELSE IF desc.sealed \/ Validity[hid] = "ceiling"
       THEN \* install_intent skipped: no claim; sealed_reject_new from admission.
            /\ h' = [h EXCEPT ![hid].pc = "enqueue", ![hid].resumed = FALSE,
                              ![hid].rej = desc.sealed \/ c # NONE]
            /\ UNCHANGED <<hist, wit>>
       ELSE /\ h' = [h EXCEPT ![hid].pc = "claim", ![hid].resumed = FALSE,
                              ![hid].rej = c # NONE]
            /\ UNCHANGED <<hist, wit>>
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, wset>>

\* Continuation once a final op holds a generation.
FinalNext(o) == IF Surface[o] = "product" THEN "check" ELSE "enqueue"

\* The handler after a claim CAS gave this raw final its generation.
RawClaimed(o, hr) == IF Surface[o] = "raw" THEN ClaimedFinalPlan(hr) ELSE hr

\* enter_sealing -> claim_seal -> enter_sealing_cas(decide_claim)  (product)
\* install_intent -> begin_sealing_for_close -> claim_seal           (raw, fresh)
\* prepare_close -> renew_owed_claim, then parse_content            (raw, owed)
FClaim(hid) ==
    LET o == HOp(hid)
        oid == OpId(o)
        raw == Surface[o] = "raw"
        dc == DecideClaim(desc, oid, TRUE)
        rn == DecideRenewOwed(desc, oid)
        bad == Validity[hid] # "ok"
        renewW == IF \E r \in wset.resv : r[1] = oid THEN {"renewAfterRes"} ELSE {}
        wrote == IF raw /\ h[hid].resumed THEN rn.ok ELSE dc.kind \in {"installed", "ours"}
        hsW == [hist EXCEPT !.invalidIntent = @ \/ (wrote /\ bad)]
    IN
    /\ h[hid].pc = "claim"
    /\ \/ \* ---- the registry call completes and its reply is observed ----
          /\ IF raw /\ h[hid].resumed
             THEN IF rn.ok
                  THEN /\ desc' = rn.next
                       /\ IF Validity[hid] = "capacity"
                          THEN \* parse_content refuses AFTER the renewal; no release
                               RespondH(hid, "invalid", rn.next, hsW, renewW)
                          ELSE /\ h' = [h EXCEPT ![hid].gen = rn.gen, ![hid].pc = "enqueue"]
                               /\ hist' = hsW
                               /\ wit' = WitUpd(renewW)
                  ELSE \* "the seal this close was resuming has been superseded"
                       /\ desc' = desc
                       /\ RespondW(hid, "conflict", desc, {})
             ELSE IF dc.kind \in {"installed", "ours"}
             THEN /\ desc' = dc.next
                  /\ h' = [h EXCEPT ![hid] = RawClaimed(o, [@ EXCEPT !.gen = dc.gen,
                                                                     !.pc = FinalNext(o)])]
                  /\ hist' = hsW
                  /\ wit' = WitUpd(IF dc.kind = "ours" THEN renewW ELSE {})
             ELSE IF dc.kind = "abandoned"
             THEN /\ desc' = desc
                  /\ h' = [h EXCEPT ![hid].old = dc.old, ![hid].org = "claim",
                                    ![hid].pc = "reserve"]
                  /\ UNCHANGED <<hist, wit>>
             ELSE IF dc.kind = "completed" /\ ~raw
             THEN /\ desc' = desc
                  /\ RespondW(hid, "success", desc, {})
             ELSE IF dc.kind = "sealed" /\ ~raw
             THEN /\ desc' = desc
                  /\ RespondW(hid, "error", desc, {})
             ELSE IF dc.kind \in {"completed", "sealed"}
             THEN \* raw: begin_sealing_for_close -> Ok(None): append without a generation
                  /\ desc' = desc
                  /\ h' = [h EXCEPT ![hid].pc = "enqueue"]
                  /\ UNCHANGED <<hist, wit>>
             ELSE \* conflict
                  /\ desc' = desc
                  /\ RespondW(hid, "conflict", desc, {})
          /\ UNCHANGED faults
       \/ \* ---- the conditional PUT committed but its reply was lost ----
          /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = IF raw /\ h[hid].resumed THEN rn.next
                     ELSE IF dc.kind \in {"installed", "ours"} THEN dc.next ELSE desc
          /\ RespondH(hid, "retryable", desc', hsW, {})
       \/ \* ---- the descriptor could not be read ----
          /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = desc
          /\ RespondW(hid, "retryable", desc, {})
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

(***************************************************************************)
(* Takeover of an abandoned final-bearing claim (take_over_abandoned)       *)
(***************************************************************************)

\* Where claim_seal resumes after a declined reservation or installation.
RestartPc(hid) == IF h[hid].org = "claim" THEN "claim" ELSE "rs_prep"
Restarted(hid) ==
    [h EXCEPT ![hid].pc = RestartPc(hid), ![hid].old = NONE, ![hid].res = NONE,
              ![hid].rep = NONE, ![hid].org = NONE,
              ![hid].rop = IF h[hid].org = "rs" THEN h[hid].orop ELSE h[hid].rop,
              ![hid].orop = NONE,
              ![hid].iter = IF MaxClaimLoop = 0 THEN 0 ELSE @ + 1]

\* claim_seal loops at most MaxClaimLoop times; the takeover that comes back
\* with `None` on the last iteration ends in SealError::Resumable.
GivesUp(hid) == MaxClaimLoop > 0 /\ h[hid].iter + 1 >= MaxClaimLoop
MayGiveUp == MaxClaimLoop = 0

RestartOrGiveUp(hid) ==
    \/ /\ GivesUp(hid) \/ MayGiveUp
       /\ Respond(hid, "retryable")
    \/ /\ ~GivesUp(hid)
       /\ h' = Restarted(hid)
       /\ UNCHANGED <<hist, wit>>

\* 1. RESERVE: bump the allocator only; the lapsed claim must be unchanged.
TReserve(hid) ==
    LET c == desc.claim
        old == h[hid].old
        same == c # NONE /\ c.op = old.op /\ c.gen = old.gen
        r == desc.counter + 1
    IN
    /\ h[hid].pc = "reserve"
    /\ \/ /\ same
          /\ desc' = [desc EXCEPT !.counter = r]
          /\ h' = [h EXCEPT ![hid].res = r, ![hid].pc = "fence"]
          /\ wset' = IF WitnessMode THEN [wset EXCEPT !.resv = @ \cup {<<old.op, old.gen, r>>}]
                     ELSE wset
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ ~same
          /\ RestartOrGiveUp(hid)
          /\ UNCHANGED <<desc, faults, wset>>
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = IF same THEN [desc EXCEPT !.counter = r] ELSE desc
          /\ Respond(hid, "retryable")
          /\ UNCHANGED wset
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = desc
          /\ Respond(hid, "retryable")
          /\ UNCHANGED wset
    /\ UNCHANGED <<seg, lanes, eng, owner, bud>>

\* History only: every final queued now is ahead of this takeover's fence.
MarkAhead(q, hid) ==
    [i \in DOMAIN q |-> IF q[i].kind = "append" /\ q[i].op \in FinalOps
                        THEN [q[i] EXCEPT !.ahead = @ \cup {hid}] ELSE q[i]]

\* 2. FENCE: fence_segment_for_key resolves the shard HERE (Adoption::
\* Internal); a non-owner answers NotOwner -> Resumable.  try_seal_fence
\* travels the append queue.
TFence(hid) ==
    /\ h[hid].pc = "fence"
    /\ \/ /\ HomeOf[hid] = owner
          /\ eng' = [eng EXCEPT !.q = Append(MarkAhead(@, hid),
                                             Req("fence", hid, NONE, h[hid].res, FALSE, FALSE))]
          /\ h' = [h EXCEPT ![hid].pc = "fwait"]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ HomeOf[hid] # owner
          /\ RespondW(hid, "retryable", desc, {"notOwner"})
          /\ UNCHANGED <<eng, faults>>
       \/ /\ HomeOf[hid] = owner
          /\ faults.enq > 0
          /\ faults' = [faults EXCEPT !.enq = @ - 1]
          /\ Respond(hid, "retryable")
          /\ UNCHANGED eng
    /\ UNCHANGED <<desc, seg, lanes, owner, bud, wset>>

\* 3. The fence was refused (Internal: the fence row could not be read) or
\* lost with its engine (Moved, "fence dropped"): resumable.
TFenceLost(hid) ==
    /\ h[hid].pc = "fwait"
    /\ h[hid].rep # NONE /\ h[hid].rep.err # NONE
    /\ Respond(hid, "retryable")
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, wset>>

\* 4. install_reserved_claim.
InstalledClaim(hid) ==
    IF h[hid].org = "claim"
    THEN Claim(OpId(HOp(hid)), TRUE, h[hid].res)
    ELSE Claim(h[hid].orop, FALSE, h[hid].res)

AfterInstallPc(hid) ==
    IF h[hid].org = "claim" THEN FinalNext(HOp(hid)) ELSE "rs_close"

TInstall(hid) ==
    LET o == HOp(hid)
        ok == InstallAllowed(desc, h[hid].old, h[hid].res)
        nd == [desc EXCEPT !.claim = InstalledClaim(hid)]
        ext0 == [h[hid] EXCEPT !.pc = AfterInstallPc(hid), !.gen = h[hid].res,
                               !.rep = NONE, !.old = NONE, !.res = NONE,
                               !.org = NONE,
                               !.rop = IF h[hid].org = "rs" THEN h[hid].orop ELSE h[hid].rop,
                               !.orop = NONE]
        ext == [h EXCEPT ![hid] = IF h[hid].org = "claim" THEN RawClaimed(o, ext0) ELSE ext0]
        \* an element queued ahead of this takeover's fence is still undecided
        early == \E i \in DOMAIN eng.q : hid \in eng.q[i].ahead
        seen == IF h[hid].rep # NONE THEN h[hid].res ELSE 0
        hs == [hist EXCEPT !.fenceSeen = IF seen > @ THEN seen ELSE @]
        \* an install for a final-bearing request that fails validation (L1)
        badI == h[hid].org = "claim" /\ Validity[hid] # "ok"
        hsI == [hs EXCEPT !.badInstall = @ \/ (h[hid].res < eng.fence),
                          !.earlyInstall = @ \/ early,
                          !.invalidIntent = @ \/ badI,
                          !.installOverMarked = @ \/ (desc.claim # NONE /\ desc.claim.com)]
        lowerFs == IF desc.claim # NONE /\ desc.claim.op = h[hid].old.op
                      /\ desc.claim.gen = h[hid].old.gen THEN {"lower"} ELSE {}
        tookW == h[hid].org = "claim" /\ Surface[o] = "raw" /\ WitnessMode
    IN
    /\ h[hid].pc = "fwait"
    /\ FenceAcknowledged(hid)
    \* A handler that moved on no longer awaits its fence reply.
    /\ eng' = OrphanEng(eng, hid)
    /\ \/ /\ ok
          /\ desc' = nd
          /\ h' = ext
          /\ hist' = hsI
          /\ wit' = WitUpd({"install"})
          /\ wset' = IF tookW THEN [wset EXCEPT !.rawTook = @ \cup {hid}] ELSE wset
          /\ UNCHANGED faults
       \/ /\ ~ok
          /\ \/ /\ GivesUp(hid) \/ MayGiveUp
                /\ RespondH(hid, "retryable", desc, hs, lowerFs)
             \/ /\ ~GivesUp(hid)
                /\ h' = Restarted(hid)
                /\ hist' = hs
                /\ wit' = WitUpd(lowerFs)
          /\ UNCHANGED <<desc, faults, wset>>
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = IF ok THEN nd ELSE desc
          /\ RespondH(hid, "retryable", desc', IF ok THEN hsI ELSE hs, {})
          /\ UNCHANGED wset
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = desc
          /\ RespondH(hid, "retryable", desc, hs, {})
          /\ UNCHANGED wset
    /\ UNCHANGED <<seg, lanes, owner, bud>>

\* 3'. The fence reported CLOSED: the old final won its race after all.
\* Finish the OLD transition on its behalf: mark_final_committed(old), then
\* run_seal(old).  (The fence reply and the mark CAS are one step here: the
\* reply is handler-local, so nothing shared can interleave differently.)
TBehalfMark(hid) ==
    LET old == h[hid].old
        dm == DecideMark(desc, old.op, old.gen)
    IN
    /\ h[hid].pc = "fwait"
    /\ h[hid].rep # NONE /\ h[hid].rep.err = NONE /\ h[hid].rep.closed
    /\ \/ /\ dm.ok
          /\ desc' = dm.next
          /\ h' = [h EXCEPT ![hid].pc = "rs_prep", ![hid].behalf = TRUE, ![hid].rep = NONE,
                            ![hid].orop = IF h[hid].org = "rs" THEN h[hid].orop ELSE NONE,
                            ![hid].rop = old.op, ![hid].gen = old.gen,
                            ![hid].old = NONE, ![hid].res = NONE]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ ~dm.ok
          /\ desc' = desc
          /\ Respond(hid, "error")
          /\ UNCHANGED faults
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = dm.next
          /\ RespondW(hid, "retryable", desc', {})
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

(***************************************************************************)
(* Final append                                                             *)
(***************************************************************************)

\* abandon_seal_intent(op, generation): only the exact, uncommitted claim.
ReleasedDesc(o, g) ==
    LET c == desc.claim IN
    IF g # NONE /\ c # NONE /\ ReleaseMatches(c, OpId(o), g) /\ ~(c.fin /\ c.com)
    THEN [desc EXCEPT !.claim = NONE] ELSE desc

\* Ground truth for "can this operation's record still be written by an
\* exact retry": the segment is open and unsealed, the op's producer tuple
\* is not spent, and some slot of this op passes validation on the
\* instance that handles it.
RecordPossible(o) ==
    /\ ~seg.closed /\ ~desc.sealed
    /\ lanes[LaneOf(o)].seq < SeqOf[o]
    /\ \E hid \in HandlerIds : HOp(hid) = o /\ Validity[hid] = "ok"

\* o's own record is the durable record that closed the segment.
CommittedFinal(o) == seg.finalRec # NONE /\ OpId(seg.finalRec) = OpId(o)

\* Releasing the owed claim c of o is justified only if o's record is not
\* durable AND either no exact retry could ever deliver it, or a newer
\* reservation has been taken against c (a takeover holds the authority).
ReleaseJustified(o) ==
    LET c == desc.claim IN
    \/ ~Owes(c) \/ c.op # OpId(o)
    \/ /\ ~CommittedFinal(o)
       /\ ~RecordPossible(o) \/ desc.counter > c.gen

\* The release CAS followed by the answer; a failed release is only logged.
ReleaseAndRespond(hid, k, dupRel, extra) ==
    LET o == HOp(hid)
        g == h[hid].gen
        nd == ReleasedDesc(o, g)
        rem == nd # desc
        hs == [hist EXCEPT !.badRelease = @ \/ (rem /\ ~ReleaseJustified(o))]
        fs == IF ~rem THEN {}
              ELSE IF dupRel THEN {"dupRelease"} \cup extra
              ELSE IF k \in DefinitiveKinds THEN {"defRelease"} \cup extra
              ELSE extra
    IN
    \/ /\ desc' = nd
       /\ RespondH(hid, k, nd, hs, fs)
       /\ UNCHANGED faults
    \/ /\ faults.reg > 0
       /\ faults' = [faults EXCEPT !.reg = @ - 1]
       /\ desc' = desc
       /\ RespondW(hid, k, desc, {})

\* append::close::prepare_close seal_auth check (product): the trusted token
\* must still hold the claim.  A failure is SealSuperseded (Conflict, hence
\* definitive), and seal_final releases exactly (op, generation).
FCheck(hid) ==
    LET o == HOp(hid)
        c == desc.claim
        holds == c # NONE /\ c.op = OpId(o) /\ c.gen = h[hid].gen
    IN
    /\ h[hid].pc = "check"
    /\ IF holds
       THEN /\ h' = [h EXCEPT ![hid].pc = "enqueue", ![hid].rej = ~Owes(c)]
            /\ UNCHANGED <<desc, hist, wit, faults>>
       ELSE ReleaseAndRespond(hid, "superseded", FALSE, {})
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

\* append::submit::submit: ShardDirectory::resolve(Adoption::External) --
\* NotOwner redirect for a non-owner process, a fresh engine for the owner
\* -- then ShardEngine::try_enqueue.  Between the claim check and this step
\* the handler may wait arbitrarily long (the pre-queue window: TTL
\* renewal, route resolution, engine open, handle load).  A failure here is
\* never a committer verdict: the product disposition is ambiguous (NotOwner
\* is excluded from definitively_rejected) and the raw close returns before
\* complete_raw_close, so owed debt is retained.
Enqueue(hid) ==
    LET o == HOp(hid)
        raw == o \in FinalOps /\ Surface[o] = "raw"
        retained == o \in FinalOps /\ Owes(desc.claim) /\ desc.claim.op = OpId(o)
        rawR == IF raw /\ retained THEN {"rawRetained"} ELSE {}
    IN
    /\ h[hid].pc = "enqueue"
    /\ \/ /\ HomeOf[hid] = owner
          /\ eng' = [eng EXCEPT !.q = Append(@, Req("append", hid, o, h[hid].gen, h[hid].rej,
                                                    Validity[hid] = "ceiling"))]
          /\ h' = [h EXCEPT ![hid].pc = "await"]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ HomeOf[hid] # owner
          /\ RespondW(hid, "retryable", desc, {"notOwner"} \cup rawR)
          /\ UNCHANGED <<eng, faults>>
       \/ /\ HomeOf[hid] = owner
          /\ faults.enq > 0
          /\ faults' = [faults EXCEPT !.enq = @ - 1]
          /\ RespondW(hid, "retryable", desc, rawR)
          /\ UNCHANGED eng
    /\ UNCHANGED <<desc, seg, lanes, owner, bud, wset>>

\* What seal_final (product) / complete_raw_close (raw) do with the
\* committer's durable verdict `r`.
Verdict(hid, r) ==
    LET o == HOp(hid) IN
    IF r.err # NONE
    THEN IF Disposition(o, r.err) = "definitive" THEN "release" ELSE "answer"
    ELSE IF ~AckCompletesFinal(r)
    THEN IF Surface[o] = "product" \/ r.dup THEN "release" ELSE "answer"
    ELSE IF Surface[o] = "product" \/ OwnsFinal(h[hid].resumed, r) THEN "mark"
    ELSE "rawdup"

\* The answer given with (or instead of) a release.
VerdictKind(hid, r) ==
    IF r.err # NONE THEN ClientKind(r.err)
    ELSE IF Surface[HOp(hid)] = "product" THEN "definitive"   \* SequenceReused
    ELSE "dupOpen"                                             \* the original, open ack

AwaitingVerdict(hid) ==
    /\ h[hid].pc = "await"
    /\ HOp(hid) \in FinalOps
    /\ h[hid].rep # NONE

\* Ambiguous / transient verdicts leave the owed-final claim in place.
FAnswer(hid) ==
    LET o == HOp(hid)
        r == h[hid].rep
        retained == Owes(desc.claim) /\ desc.claim.op = OpId(o)
        fs == (IF r.err = "ProducerGap" /\ retained THEN {"gap"} ELSE {})
              \cup (IF r.err = "Moved" THEN {"moved"} ELSE {})
              \cup (IF r.err = "Moved" /\ Surface[o] = "raw" /\ retained
                    THEN {"rawRetained"} ELSE {})
              \cup (IF r.err = "Internal" /\ retained THEN {"fenceUnverified"} ELSE {})
    IN
    /\ AwaitingVerdict(hid)
    /\ Verdict(hid, r) = "answer"
    /\ RespondW(hid, VerdictKind(hid, r), desc, fs)
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, wset>>

\* A definitive refusal, or an acknowledgement that cannot complete the
\* final (a spent producer tuple): release the exact claim, then answer.
FRelease(hid) ==
    LET r == h[hid].rep IN
    /\ AwaitingVerdict(hid)
    /\ Verdict(hid, r) = "release"
    /\ ReleaseAndRespond(hid, VerdictKind(hid, r), r.err = NONE,
                         IF r.err = "ProducerSeqReused" THEN {"seqReused"} ELSE {})
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

\* The closing record answered as a duplicate was written by a DIFFERENT
\* operation on the same (synthetic) lane.
SharedLaneW(hid) ==
    LET o == HOp(hid) IN
    IF h[hid].rep.dup /\ seg.finalRec # NONE /\ OpId(seg.finalRec) # OpId(o)
       /\ LaneOf(seg.finalRec) = LaneOf(o)
    THEN {"sharedLaneDup"} ELSE {}

\* A raw exact retry whose claim was already marked marks again (already
\* done) and runs the seal under its own operation (TLA-003-F2 fix).
MarkedRetryW(hid) ==
    IF Surface[HOp(hid)] = "raw" /\ desc.claim # NONE /\ desc.claim.op = OpId(HOp(hid))
       /\ desc.claim.fin /\ desc.claim.com
    THEN {"markedRetry"} ELSE {}

\* A closed acknowledgement this operation owns: mark_final_committed(op,
\* generation) before any segment close, then run_seal(op, generation).
FMark(hid) ==
    LET o == HOp(hid)
        dm == DecideMark(desc, OpId(o), h[hid].gen)
    IN
    /\ AwaitingVerdict(hid)
    /\ Verdict(hid, h[hid].rep) = "mark"
    /\ \/ /\ dm.ok
          /\ desc' = dm.next
          /\ h' = [h EXCEPT ![hid].pc = "rs_prep", ![hid].rop = OpId(o), ![hid].rep = NONE]
          /\ wit' = WitUpd(SharedLaneW(hid) \cup MarkedRetryW(hid))
          /\ UNCHANGED <<faults, hist>>
       \/ /\ ~dm.ok
          /\ desc' = desc
          /\ RespondW(hid, IF Surface[o] = "product" THEN "error" ELSE "retryable", desc,
                      SharedLaneW(hid))
          /\ UNCHANGED faults
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = dm.next
          /\ RespondW(hid, "retryable", desc', {})
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

\* Raw duplicate of a closed tail it does not own: run_seal(None) as a plain
\* seal; the client is answered with the original acknowledgement.
FRawDup(hid) ==
    /\ AwaitingVerdict(hid)
    /\ Verdict(hid, h[hid].rep) = "rawdup"
    /\ h' = [h EXCEPT ![hid].pc = "rs_prep", ![hid].rop = PLAIN, ![hid].gen = NONE,
                      ![hid].rawdup = TRUE, ![hid].rep = NONE]
    /\ wit' = WitUpd(SharedLaneW(hid))
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, hist, wset>>

(***************************************************************************)
(* run_seal: prepare_execution -> close_claimed_segments -> publish_sealed  *)
(***************************************************************************)

\* How a completed run_seal answers its caller (`fs`: witness flags).
FinishRunSeal(hid, d, fs) ==
    LET o == HOp(hid)
        outerKind == IF o \in PlainOps THEN "success"
                     ELSE IF h[hid].rawdup THEN "dupAck" ELSE "success"
    IN
    IF ~h[hid].behalf
    THEN RespondW(hid, outerKind, d, fs)
    ELSE \* the takeover completed the OLD operation's seal on its behalf
         IF h[hid].orop = PLAIN
         THEN RespondW(hid, outerKind, d, fs)         \* AlreadySealed, op_id "" -> Ok(None)
         ELSE IF o \in FinalOps /\ Surface[o] = "raw" /\ h[hid].orop = NONE
         THEN \* begin_sealing_for_close -> Ok(None): append without a generation
              /\ h' = [h EXCEPT ![hid].pc = "enqueue", ![hid].gen = NONE,
                                ![hid].behalf = FALSE, ![hid].rop = NONE,
                                ![hid].org = NONE, ![hid].orop = NONE]
              /\ UNCHANGED hist
              /\ wit' = WitUpd(fs)
         ELSE RespondW(hid, "error", d, fs)           \* AlreadySealed / OtherOperation

\* prepare_execution (its read and claim_seal's CAS are merged: decide_claim
\* re-judges the stored descriptor, so only the adopt-vs-renew choice for a
\* standing claim could differ, and both yield a currently valid generation).
RsPrep(hid) ==
    LET rop == h[hid].rop
        c == desc.claim
        dc == DecideClaim(desc, rop, FALSE)
    IN
    /\ h[hid].pc = "rs_prep"
    /\ \/ /\ desc.sealed
          /\ desc' = desc
          /\ IF rop # PLAIN /\ desc.sealOp # rop
             THEN Respond(hid, "error")                     \* OtherOperation
             ELSE FinishRunSeal(hid, desc, {})
          /\ UNCHANGED faults
       \/ /\ ~desc.sealed /\ c # NONE /\ Owes(c) /\ c.op = rop
          /\ desc' = desc
          /\ Respond(hid, "error")                          \* OwedFinal
          /\ UNCHANGED faults
       \/ /\ ~desc.sealed /\ c # NONE /\ c.op = rop /\ ~Owes(c)
          \* adopt the standing generation
          /\ desc' = desc
          /\ h' = [h EXCEPT ![hid].gen = IF @ = NONE THEN c.gen ELSE @, ![hid].pc = "rs_close"]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ ~desc.sealed /\ ClaimOp(desc) # rop
          /\ IF dc.kind \in {"installed", "ours"}
             THEN /\ desc' = dc.next
                  /\ h' = [h EXCEPT ![hid].gen = dc.gen, ![hid].pc = "rs_close"]
                  /\ UNCHANGED <<hist, wit>>
             ELSE IF dc.kind = "abandoned"
             THEN /\ desc' = desc
                  /\ h' = [h EXCEPT ![hid].old = dc.old, ![hid].org = "rs",
                                    ![hid].orop = rop, ![hid].pc = "reserve"]
                  /\ UNCHANGED <<hist, wit>>
             ELSE \* conflict
                  /\ desc' = desc
                  /\ Respond(hid, "conflict")
          /\ UNCHANGED faults
       \/ /\ ~desc.sealed /\ ClaimOp(desc) # rop
          /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = IF dc.kind \in {"installed", "ours"} THEN dc.next ELSE desc
          /\ Respond(hid, "retryable")
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

\* close_claimed_segments -> seal_segment_identity -> close_segment_on_engine
\* (or relay_segment_close to the owner when this process is not the owner;
\* the relay enqueues the same CloseReq there).
RsClose(hid) ==
    /\ h[hid].pc = "rs_close"
    /\ \/ /\ eng' = [eng EXCEPT !.q = Append(@, Req("close", hid, h[hid].rop, h[hid].gen,
                                                    FALSE, FALSE))]
          /\ h' = [h EXCEPT ![hid].pc = "rs_cwait"]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ faults.enq > 0
          /\ faults' = [faults EXCEPT !.enq = @ - 1]
          /\ Respond(hid, "retryable")
          /\ UNCHANGED eng
    /\ UNCHANGED <<desc, seg, lanes, owner, bud, wset>>

\* The close was refused (a lapsed generation below the fence, or the fence
\* row could not be read) or lost with its engine: "segment did not close"
\* -- the seal stays resumable.
RsCloseFailed(hid) ==
    /\ h[hid].pc = "rs_cwait"
    /\ h[hid].rep # NONE /\ h[hid].rep.err # NONE
    /\ Respond(hid, "retryable")
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, wset>>

\* publish_sealed: CAS, then the proof read (merged: sealed/sealOp are
\* monotonic once set, so a later read can only confirm the same verdict).
RsPublish(hid) ==
    LET c == desc.claim
        rop == h[hid].rop
        can == ~desc.sealed /\ c # NONE /\ c.gen = h[hid].gen /\ ~Owes(c)
        nd == IF can THEN [desc EXCEPT !.sealed = TRUE, !.sealOp = c.op, !.claim = NONE]
              ELSE desc
        bw == IF h[hid].behalf /\ can /\ c.op = rop THEN {"behalfSealed"} ELSE {}
    IN
    /\ h[hid].pc = "rs_cwait"
    /\ h[hid].rep # NONE /\ h[hid].rep.err = NONE
    /\ \/ /\ desc' = nd
          /\ IF nd.sealed
             THEN IF rop # PLAIN /\ nd.sealOp # rop
                  THEN RespondW(hid, "error", nd, bw)          \* OtherOperation
                  ELSE FinishRunSeal(hid, nd, bw)
             ELSE RespondW(hid, "retryable", nd, {})       \* not terminal: resumable
          /\ UNCHANGED faults
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = nd
          /\ Respond(hid, "retryable")
       \/ /\ faults.reg > 0
          /\ faults' = [faults EXCEPT !.reg = @ - 1]
          /\ desc' = desc
          /\ Respond(hid, "retryable")
    /\ UNCHANGED <<seg, lanes, eng, owner, bud, wset>>

(***************************************************************************)
(* Ordinary producer append                                                 *)
(***************************************************************************)

\* Product: product.rs refuse_if_sealed answers 409 while the admission
\* descriptor is Sealing or Sealed.  Raw: prepare_close computes
\* sealed_reject_new (duplicates still resolve at the committer).
APrep(hid) ==
    LET o == HOp(hid)
        busy == desc.sealed \/ desc.claim # NONE
    IN
    /\ h[hid].pc = "a_prep"
    /\ IF Surface[o] = "product" /\ busy
       THEN RespondW(hid, "closed", desc, {})
       ELSE /\ h' = [h EXCEPT ![hid].pc = "enqueue", ![hid].rej = busy]
            /\ UNCHANGED <<hist, wit>>
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, wset>>

AReceive(hid) ==
    LET r == h[hid].rep IN
    /\ h[hid].pc = "await"
    /\ HOp(hid) \in AppendOps
    /\ r # NONE
    /\ RespondW(hid, IF r.err = NONE THEN "appended" ELSE ClientKind(r.err), desc, {})
    /\ UNCHANGED <<desc, seg, lanes, eng, owner, bud, faults, wset>>

(***************************************************************************)
(* Committer (one queue element per step: decide + apply + durable reply)   *)
(***************************************************************************)
Deliver(hid, rep) == IF hid = NONE THEN h ELSE [h EXCEPT ![hid].rep = rep]

\* A closing effect under a generation below a fence some takeover already
\* observed (queue-ordered, durable) as "not closed".
Stale(g) == hist.fenceSeen > 0 /\ (g = NONE \/ g < hist.fenceSeen)

\* The request carries the generation of the CURRENT, unlapsed claim.
LiveAt(g) == g # NONE /\ desc.claim # NONE /\ desc.claim.gen = g /\ ~desc.claim.lap

\* The fence row could not be read: CommitTransaction::seal_fence answers
\* Internal ("seal_fence_unverified") and nothing is decided.
Unverified == [rep |-> Rep("Internal", FALSE, FALSE), commit |-> FALSE, seg |-> seg]

\* Refusals the committer sends at staging, before its group is durable
\* (transaction/append.rs 96-101, 139-142; maintenance.rs 144-146, 178-188).
\* Every other answer waits for the group's durability (DurableEffects).
ImmediateRefusal(rep) == rep.err \in {"SealSuperseded", "BadBody", "Internal"}

\* CommitTransaction::close (transaction/maintenance.rs 172-201): an open
\* segment consults the fence; an already-closed one answers its
\* idempotent re-close without it.
CloseDecision(r) ==
    IF ~seg.closed /\ ~SealAuthorized(r.gen, TRUE, eng.fence)
    THEN [rep |-> Rep("SealSuperseded", FALSE, FALSE), commit |-> FALSE, seg |-> seg]
    ELSE [rep |-> Rep(NONE, FALSE, TRUE), commit |-> ~seg.closed,
          seg |-> IF seg.closed THEN seg
                  ELSE [seg EXCEPT !.closed = TRUE, !.closer = r.op, !.finalRec = NONE]]

CloseHist(r, d) ==
    [hist EXCEPT !.staleEffect = @ \/ (d.commit /\ Stale(r.gen)),
                 !.liveFenced = @ \/ (d.rep.err = "SealSuperseded" /\ LiveAt(r.gen))]

\* CommitTransaction::append (transaction/append.rs) with decide_producer
\* (commit_plan.rs): duplicate / hash conflict / gap / sealed_reject_new,
\* then the closed tail, then a deferred content error, then the seal
\* fence (seal_authorizes), then acceptance.
AppendDecision(r) ==
    LET o == r.op
        cur == lanes[LaneOf(o)]
        s == SeqOf[o]
        closing == o \in FinalOps
        dup == cur.seq >= 0 /\ s <= cur.seq
        reused == dup /\ s = cur.seq /\ ReqHash(o) # NONE /\ cur.hash # NONE
                  /\ ReqHash(o) # cur.hash
        gap == s > cur.seq + 1
        fenced == (r.gen # NONE \/ closing) /\ ~SealAuthorized(r.gen, closing, eng.fence)
    IN
    IF reused THEN [rep |-> Rep("ProducerSeqReused", FALSE, FALSE), commit |-> FALSE]
    ELSE IF dup THEN [rep |-> Rep(NONE, TRUE, seg.closed), commit |-> FALSE]
    ELSE IF gap THEN [rep |-> Rep("ProducerGap", FALSE, FALSE), commit |-> FALSE]
    ELSE IF r.rej \/ seg.closed THEN [rep |-> Rep("Closed", FALSE, FALSE), commit |-> FALSE]
    ELSE IF r.dfr THEN [rep |-> Rep("BadBody", FALSE, FALSE), commit |-> FALSE]
    ELSE IF fenced THEN [rep |-> Rep("SealSuperseded", FALSE, FALSE), commit |-> FALSE]
    ELSE [rep |-> Rep(NONE, FALSE, closing), commit |-> TRUE]

\* The append reaches seal_authorizes: it passed every earlier check and is
\* closing or claim-authorized.
ReachesFence(r) ==
    LET d == AppendDecision(r) IN
    (r.gen # NONE \/ r.op \in FinalOps) /\ (d.commit \/ d.rep.err = "SealSuperseded")

AppendLanes(r) == [lanes EXCEPT ![LaneOf(r.op)] = [seq |-> SeqOf[r.op], hash |-> ReqHash(r.op)]]
AppendSeg(r) ==
    IF r.op \in FinalOps
    THEN [seg EXCEPT !.closed = TRUE, !.closer = OpId(r.op), !.finalRec = r.op] ELSE seg
AppendHist(r, d) ==
    [hist EXCEPT !.staleEffect = @ \/ (d.commit /\ r.op \in FinalOps /\ Stale(r.gen)),
                 !.liveFenced = @ \/ (d.rep.err = "SealSuperseded" /\ LiveAt(r.gen))]

\* Witness-only: a final whose record closes the segment while no claim of
\* its own stands, and a stale final refused by a fence an earlier engine wrote.
AppendWset(r, d) ==
    IF WitnessMode /\ d.commit /\ r.op \in FinalOps
    THEN [wset EXCEPT
            !.orphan = IF r.hid = NONE THEN @ \cup {r.op} ELSE @,
            !.orphanClosed = IF ~desc.sealed /\ (desc.claim = NONE \/ desc.claim.op # OpId(r.op))
                             THEN @ \cup {r.op} ELSE @]
    ELSE wset
AppendW(r, d) ==
    (IF d.rep.err = "SealSuperseded" THEN {"superseded"} ELSE {})
    \cup (IF d.rep.err = "SealSuperseded" /\ r.op \in FinalOps /\ r.gen # NONE
             /\ r.gen < wset.openFence THEN {"refusedByRow"} ELSE {})

\* CommitTransaction::fence (transaction/maintenance.rs 136-167): the fence
\* raises the engine cache and writes the durable row in its commit group;
\* its reply waits for that group's durability.
ProcessFence ==
    LET r == Head(eng.q) IN
    /\ eng.q # <<>> /\ r.kind = "fence" /\ eng.held = NONE
    /\ \/ /\ eng' = [eng EXCEPT !.fence = Max(@, r.gen), !.q = Tail(@)]
          /\ seg' = PersistFence(seg, r.gen)
          /\ h' = Deliver(r.hid, Rep(NONE, FALSE, seg.closed))
          /\ UNCHANGED faults
       \/ \* seal_fence could not read the row: Internal, nothing raised or written
          /\ faults.fread > 0
          /\ faults' = [faults EXCEPT !.fread = @ - 1]
          /\ eng' = [eng EXCEPT !.q = Tail(@)]
          /\ h' = Deliver(r.hid, Unverified.rep)
          /\ UNCHANGED seg
       \/ \* The group is staged: the cache is raised now, the row and the reply
          \* wait for the group's durability (FenceGroupDurable), which an engine
          \* loss may prevent.  Immediate refusals already see the raised cache.
          /\ faults.held > 0
          /\ faults' = [faults EXCEPT !.held = @ - 1]
          /\ eng' = [eng EXCEPT !.fence = Max(@, r.gen), !.q = Tail(@),
                                !.held = [hid |-> r.hid, gen |-> r.gen]]
          /\ UNCHANGED <<seg, h>>
    /\ UNCHANGED <<desc, lanes, owner, bud, hist, wit, wset>>

\* The staged fence group becomes durable: the row is written and the
\* fence's reply released.  No commit was staged behind it meanwhile.
FenceGroupDurable ==
    /\ eng.held # NONE
    /\ seg' = PersistFence(seg, eng.held.gen)
    /\ h' = Deliver(eng.held.hid, Rep(NONE, FALSE, seg.closed))
    /\ eng' = [eng EXCEPT !.held = NONE]
    /\ UNCHANGED <<desc, lanes, owner, bud, faults, hist, wit, wset>>

ApplyClose(r, d) ==
    /\ eng.held = NONE \/ ImmediateRefusal(d.rep)
    /\ eng' = [eng EXCEPT !.q = Tail(eng.q)]
    /\ h' = Deliver(r.hid, d.rep)
    /\ seg' = d.seg
    /\ hist' = CloseHist(r, d)
    /\ wit' = WitUpd(IF d.rep.err = "SealSuperseded" THEN {"superseded"} ELSE {})

ProcessClose ==
    LET r == Head(eng.q) IN
    /\ eng.q # <<>> /\ r.kind = "close"
    /\ \/ /\ ApplyClose(r, CloseDecision(r))
          /\ UNCHANGED faults
       \/ /\ faults.fread > 0 /\ ~seg.closed
          /\ faults' = [faults EXCEPT !.fread = @ - 1]
          /\ ApplyClose(r, Unverified)
    /\ UNCHANGED <<desc, lanes, owner, bud, wset>>

ApplyAppend(r, d) ==
    /\ eng.held = NONE \/ ImmediateRefusal(d.rep)
    /\ eng' = [eng EXCEPT !.q = Tail(eng.q)]
    /\ h' = Deliver(r.hid, d.rep)
    /\ lanes' = IF d.commit THEN AppendLanes(r) ELSE lanes
    /\ seg' = IF d.commit THEN AppendSeg(r) ELSE seg
    /\ hist' = AppendHist(r, d)
    /\ wit' = WitUpd(AppendW(r, d))
    /\ wset' = AppendWset(r, d)

ProcessAppend ==
    LET r == Head(eng.q) IN
    /\ eng.q # <<>> /\ r.kind = "append"
    /\ \/ /\ ApplyAppend(r, AppendDecision(r))
          /\ UNCHANGED faults
       \/ /\ faults.fread > 0 /\ ReachesFence(r)
          /\ faults' = [faults EXCEPT !.fread = @ - 1]
          /\ ApplyAppend(r, Unverified)
    /\ UNCHANGED <<desc, owner, bud>>

(***************************************************************************)
(* Environment: time, cancellation, timeouts, crashes, engine replacement   *)
(***************************************************************************)

\* The claim's lease window elapses (claimed_ms older than SEAL_CLAIM_MS).
\* Only consulted while the claim owes its record.
Lapse ==
    /\ Owes(desc.claim) /\ ~desc.claim.lap
    /\ desc' = [desc EXCEPT !.claim.lap = TRUE]
    /\ UNCHANGED <<seg, lanes, eng, owner, h, bud, faults, hist, wit, wset>>

InFlight == \E hid \in HandlerIds : h[hid].pc # "idle"

\* The client disconnects: the handler future is dropped; queued work stays.
Cancel(hid) ==
    /\ h[hid].pc # "idle"
    /\ faults.cancel > 0
    /\ faults' = [faults EXCEPT !.cancel = @ - 1]
    /\ h' = [h EXCEPT ![hid] = IdleH]
    /\ eng' = OrphanEng(eng, hid)
    /\ UNCHANGED <<desc, seg, lanes, owner, bud, hist, wit, wset>>

\* APPEND_TIMEOUT: "append timed out; outcome unknown" (ambiguous).
Timeout(hid) ==
    /\ h[hid].pc = "await"
    /\ h[hid].rep = NONE
    /\ faults.timeout > 0
    /\ faults' = [faults EXCEPT !.timeout = @ - 1]
    /\ eng' = OrphanEng(eng, hid)
    /\ Respond(hid, "retryable")
    /\ UNCHANGED <<desc, seg, lanes, owner, bud, wset>>

\* Every handler the closing engine still owes an answer is answered Moved.
MovedAll == [hid \in HandlerIds |-> IF Pending(hid) THEN [h[hid] EXCEPT !.rep = MovedRep]
                                    ELSE h[hid]]

\* Engine replacement without a crash: ShardDirectory::retire (ownership
\* moved to another process, or away and back; fleet/sweep eviction; a
\* fatal db close followed by a reopen) -> begin_close rejects queued and
\* stranded groups with AppendErr::Moved.  A stranded group was already
\* applied and may still become durable while its caller is told Moved --
\* an append, a close, or a fence whose row then stands.
\* The next resolve in the (new) owner opens a NEW ShardEngine whose
\* seal_fences cache starts empty and reads the durable fence row.
\* Request handlers in every process survive.
Replace ==
    LET r == Head(eng.q)
        ad == AppendDecision(r)
        cd == CloseDecision(r)
        canApply == eng.held = NONE /\ eng.q # <<>>
                    /\ \/ r.kind = "append" /\ ad.commit
                       \/ r.kind = "close" /\ cd.commit
                       \/ r.kind = "fence"
    IN
    /\ InFlight
    /\ faults.retire > 0
    /\ faults' = [faults EXCEPT !.retire = @ - 1]
    /\ \E np \in Procs : owner' = np
    /\ h' = MovedAll
    /\ \/ /\ \E sg \in StrandedFence : seg' = sg
          /\ UNCHANGED <<lanes, hist>>
          /\ wit' = WitUpd(IF owner' # owner THEN {"crossOwner"} ELSE {})
       \/ /\ canApply
          /\ CASE r.kind = "append" ->
                    /\ lanes' = AppendLanes(r)
                    /\ seg' = AppendSeg(r)
                    /\ hist' = AppendHist(r, ad)
               [] r.kind = "close" ->
                    /\ seg' = cd.seg
                    /\ hist' = CloseHist(r, cd)
                    /\ UNCHANGED lanes
               [] r.kind = "fence" ->
                    /\ seg' = PersistFence(seg, r.gen)
                    /\ UNCHANGED <<lanes, hist>>
          /\ wit' = WitUpd({"appliedMoved"} \cup (IF owner' # owner THEN {"crossOwner"} ELSE {}))
    /\ eng' = OpenedEngine(seg')
    /\ wset' = OpenedW(seg')
    /\ UNCHANGED <<desc, bud>>

\* Process crash.  The crashed process's handlers are lost.  If it owned the
\* shard, its engine (queue + fence cache) is lost too, and the ring assigns
\* the shard to a surviving process -- or to the restarted one -- whose next
\* resolve opens a fresh engine; handlers in other processes survive (a
\* relayed close of theirs is answered with an error).  If it did not own
\* the shard, the owner keeps serving; relayed closes it had queued there
\* continue without a caller.  Registry and durable segment state survive;
\* a staged fence group may or may not have become durable.
Crash ==
    /\ InFlight
    /\ faults.crash > 0
    /\ faults' = [faults EXCEPT !.crash = @ - 1]
    /\ \E p \in Procs :
         IF p = owner
         THEN /\ \E np \in Procs : owner' = np
              /\ \E sg \in StrandedFence : seg' = sg
              /\ eng' = OpenedEngine(seg')
              /\ h' = [hid \in HandlerIds |->
                         IF HomeOf[hid] = p THEN IdleH
                         ELSE IF Pending(hid) THEN [h[hid] EXCEPT !.rep = MovedRep]
                         ELSE h[hid]]
              /\ wit' = WitUpd(IF owner' # p THEN {"crossOwner"} ELSE {})
              /\ wset' = OpenedW(seg')
         ELSE /\ owner' = owner
              /\ UNCHANGED seg
              /\ eng' = [eng EXCEPT !.q = [i \in DOMAIN eng.q |->
                           IF eng.q[i].hid # NONE /\ HomeOf[eng.q[i].hid] = p
                           THEN [eng.q[i] EXCEPT !.hid = NONE] ELSE eng.q[i]],
                                    !.held = IF eng.held # NONE /\ eng.held.hid # NONE
                                                /\ HomeOf[eng.held.hid] = p
                                             THEN [eng.held EXCEPT !.hid = NONE]
                                             ELSE eng.held]
              /\ h' = [hid \in HandlerIds |-> IF HomeOf[hid] = p THEN IdleH ELSE h[hid]]
              /\ UNCHANGED <<wit, wset>>
    /\ UNCHANGED <<desc, lanes, bud, hist>>

(***************************************************************************)
(* Specification                                                            *)
(***************************************************************************)
HandlerStep(hid) ==
    \/ FValidate(hid) \/ FClaim(hid) \/ FCheck(hid) \/ Enqueue(hid)
    \/ FAnswer(hid) \/ FRelease(hid) \/ FMark(hid) \/ FRawDup(hid)
    \/ TReserve(hid) \/ TFence(hid) \/ TFenceLost(hid) \/ TInstall(hid)
    \/ TBehalfMark(hid)
    \/ RsPrep(hid) \/ RsClose(hid) \/ RsCloseFailed(hid) \/ RsPublish(hid)
    \/ APrep(hid) \/ AReceive(hid)

Process == ProcessFence \/ FenceGroupDurable \/ ProcessClose \/ ProcessAppend

\* Legitimate quiescence: no handler, no queued work, no client request left.
Settled ==
    /\ \A hid \in HandlerIds : h[hid].pc = "idle"
    /\ eng.q = <<>> /\ eng.held = NONE
    /\ \A o \in Ops : o \notin Unbounded /\ bud[o] = 0
    /\ ~(Owes(desc.claim) /\ ~desc.claim.lap)

Quiescent == Settled /\ UNCHANGED vars

Next ==
    \/ \E hid \in HandlerIds : Issue(hid) \/ HandlerStep(hid) \/ Cancel(hid) \/ Timeout(hid)
    \/ Process
    \/ Lapse \/ Crash \/ Replace
    \/ Quiescent

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Fairness for the liveness configuration.  Faults have finite budgets, so *)
(* they cease; no fairness is placed on any fault, response or outcome.     *)
(* The committer, every handler's own protocol step, lease time and the    *)
(* recovery client's request issue are weakly fair.                         *)
(***************************************************************************)
Fairness ==
    /\ WF_vars(Process)
    /\ WF_vars(Lapse)
    /\ \A hid \in HandlerIds : WF_vars(HandlerStep(hid))
    /\ \A hid \in HandlerIds : HOp(hid) \in Unbounded => WF_vars(Issue(hid))

LiveSpec == Spec /\ Fairness

(***************************************************************************)
(* Safety properties                                                        *)
(***************************************************************************)
ClaimT == [op : {OpId(o) : o \in FinalOps} \cup {PLAIN}, fin : BOOLEAN, com : BOOLEAN,
           gen : Nat, lap : BOOLEAN]
TypeOK ==
    /\ desc.claim \in ClaimT \cup {NONE}
    /\ desc.sealOp \in {OpId(o) : o \in FinalOps} \cup {PLAIN, NONE}
    /\ desc.sealed \in BOOLEAN
    /\ seg.closed \in BOOLEAN
    /\ seg.finalRec \in FinalOps \cup {NONE}
    /\ seg.fence \in Nat
    /\ eng.fence \in Nat
    /\ owner \in Procs
    /\ \A hid \in HandlerIds : h[hid].pc \in
         {"idle", "validate", "claim", "check", "enqueue", "await",
          "reserve", "fence", "fwait", "rs_prep", "rs_close", "rs_cwait", "a_prep"}

\* L5/L6/L15: a physically closed segment belongs to the operation that
\* holds the claim or that the terminal descriptor names.
ClosureAuthorized ==
    seg.closed =>
        \/ desc.claim # NONE /\ desc.claim.op = seg.closer
        \/ desc.sealed /\ desc.sealOp = seg.closer

\* L3/L12/L15: a final-bearing terminal state names the operation whose
\* own record closed the segment -- never a duplicate or a close-only.
SealedFinalHasItsRecord ==
    desc.sealed /\ desc.sealOp # PLAIN =>
        seg.finalRec # NONE /\ OpId(seg.finalRec) = desc.sealOp

\* L3: a plain seal never completes over a committed final record.
PlainCannotCompleteOwedFinal ==
    desc.sealed /\ desc.sealOp = PLAIN => seg.finalRec = NONE

\* L6/L7/L9/L10: after a takeover observed the durable fence answer "not
\* closed" for generation f, no claim-authorized effect below f closes.
FenceBarrier == ~hist.staleEffect

\* L8: a takeover never installs a generation below the live fence.
NewestInstall == ~hist.badInstall

\* L6/L8 (usable authority): the committer never refuses as superseded a
\* request carrying the current, unlapsed claim's generation.  An exact
\* renewal's generation is revoked only by a lapse plus a newer reservation.
LiveClaimNeverFenced == ~hist.liveFenced

\* L6/L7: a takeover installs only after every final queued ahead of its
\* fence has been decided.
QueuedFinalDecidedBeforeReplacement == ~hist.earlyInstall

\* L15 (and L3/L12 as the customer sees them): every 2xx answer proves the
\* outcome it reports, measured on the durable descriptor and segment.
SuccessProvesOutcome == ~hist.badSuccess

\* L11/L12/P8/D6-D7: owed-final debt is released only when the operation's
\* record is not durable and can no longer be delivered by an exact retry
\* (segment closed or sealed, producer tuple spent, invalid on every
\* instance that handles it), or a newer reservation holds the takeover
\* authority -- measured on durable lane, segment and descriptor state, not
\* on response classes.
ReleaseOnlyWhenUndeliverable == ~hist.badRelease

\* A final op is told "stream closed" only when that is true.
FinalClosedTruthful == ~hist.badClosed

\* L1: no claim is written (installed, re-entered or renewed) by a request
\* that fails deterministic validation on the instance that handles it.
IntentOnlyAfterValidation == ~hist.invalidIntent

\* A takeover never installs over a claim whose final is already marked
\* committed (install_reserved_claim does not re-check owes_final, README
\* F1(e); the durable fence makes the situation unreachable).
InstallOnlyOverOwedClaim == ~hist.installOverMarked

\* Structural sanity (holds by construction; not counted as evidence):
\* Sealed only after a close acknowledgement, fences only carry allocations.
Structural == (desc.sealed => seg.closed) /\ eng.fence <= desc.counter
              /\ seg.fence <= desc.counter

\* Every non-idle handler can take its own next protocol step (no stuck
\* handler hidden behind always-available fault actions).
Waiting(hid) ==
    /\ h[hid].pc \in {"await", "fwait", "rs_cwait"}
    /\ h[hid].rep = NONE
    /\ Pending(hid)
HandlersProgress ==
    \A hid \in HandlerIds : h[hid].pc # "idle" => (Waiting(hid) \/ ENABLED HandlerStep(hid))

(***************************************************************************)
(* Liveness                                                                 *)
(***************************************************************************)
EventuallySealed == <>desc.sealed

(***************************************************************************)
(* Reachability witnesses (each EXPECTED to be violated; WitnessMode)       *)
(***************************************************************************)
Witness_TakeoverInstalls == ~wit.install
Witness_CompetingReservations ==
    ~\E a, b \in wset.resv : a # b /\ a[1] = b[1] /\ a[2] = b[2]
Witness_LowerReservationRestarts == ~wit.lower
Witness_OldFinalSealedOnBehalf == ~wit.behalfSealed
Witness_StaleClaimSuperseded == ~wit.superseded
Witness_ExactRenewalAfterReservation == ~wit.renewAfterRes
Witness_EngineRetiredMidFlight == ~wit.moved
Witness_CommittedButAnsweredMoved == ~wit.appliedMoved
Witness_OwnershipMovedToOtherProcess == ~wit.crossOwner
Witness_NotOwnerRedirect == ~wit.notOwner
Witness_FinalSealCompletes == ~(desc.sealed /\ desc.sealOp # PLAIN)
Witness_PlainSealCompletes == ~(desc.sealed /\ desc.sealOp = PLAIN)
Witness_InvalidRefusedBeforeIntent == ~wit.invalidRefused
Witness_CommitAfterCancel == wset.orphan = {}
Witness_LostReplyThenRetrySucceeds == ~wit.lostReplyRetry
Witness_GapRetainsClaim == ~wit.gap
Witness_DefinitiveRelease == ~wit.defRelease
Witness_NonClosingDuplicateReleased == ~wit.dupRelease
Witness_SeqReusedReleased == ~wit.seqReused
Witness_RawMovedRetainsClaim == ~wit.rawRetained
Witness_SharedLaneDuplicate == ~wit.sharedLaneDup
\* TLA-002-F1 fix: a stalled final below a fence an EARLIER engine wrote is
\* refused by the engine opened after the replacement.
Witness_StaleFinalRefusedAfterReplacement == ~wit.refusedByRow
\* TLA-002-F1 fix: an unreadable fence row answers Internal and the owed
\* claim is retained.
Witness_FenceUnverifiedRetainsClaim == ~wit.fenceUnverified
\* TLA-003-F2 fix: a raw close that took over another operation's lapsed
\* claim writes its own record and is sealed under its own operation.
Witness_RawTakeoverWritesItsRecord == ~wit.rawTookOwn
\* TLA-003-F2 fix: a raw exact retry after the mark completes under its op.
Witness_RetryAfterMarkRunsItsSeal == ~wit.markedRetry
\* TLA-003-F5: a segment closed by a final whose claim was released is
\* healed by a later exact retry (sealed under that operation).
Witness_OrphanedCloseHealed == ~wit.healed
=============================================================================
