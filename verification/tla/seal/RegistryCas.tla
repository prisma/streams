---------------------------- MODULE RegistryCas ----------------------------
(***************************************************************************)
(* TLA-001 -- Registry CAS, attempt-local outcomes and incarnation fencing *)
(*                                                                         *)
(* The shared authority substrate behind creation, sealing, topology and   *)
(* deletion: `Registry::mutate_incarnation` and `Registry::recreate`       *)
(* (src/registry.rs), driven against an object store whose conditional PUT *)
(* is atomic under ASM-OBJSTORE-CAS.                                       *)
(*                                                                         *)
(* The retry loop is NOT atomic.  Every attempt is a separate GET (the     *)
(* read linearisation point, where the incarnation check and the pure      *)
(* `decide` closure run on the snapshot) followed by a separate            *)
(* conditional PUT whose reply may be lost after the write committed.      *)
(* Objects are keyed by path; two projects share one stream name, so two   *)
(* paths exist.  The deleter tombstones incarnation E1 and recreates the   *)
(* same name (same key) as FreshEpoch (ABA on name+key).  Mutators         *)
(* validated against FreshEpoch race the stale E1 mutators on the          *)
(* replacement.                                                            *)
(*                                                                         *)
(* The mutation under test is a generation allocation -- the shape of      *)
(* decide_claim / take_over_abandoned's reservation / renew_owed_claim     *)
(* (src/application/lifecycle{,/claims}.rs): Write(counter+1) returning    *)
(* the allocated generation, Decline on a tombstone.                       *)
(*                                                                         *)
(* Evidence split (README "TLA-001"): UniqueAllocation, IncarnationFenced  *)
(* and AllocatorCountsWrites relate what callers are told to the durable   *)
(* object history and depend on the loop protocol.  AttemptLocalResult and *)
(* NegativeOutcomesWroteNothing hold by construction of this model (the    *)
(* decide closure is `impl Fn` over one snapshot); they are sanity checks, *)
(* and their production evidence is source inspection, not TLC.           *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Mutators,        \* allocation mutators (model values)
    Deleter,         \* the delete-then-recreate actor (model value)
    Paths,           \* descriptor object paths (one per project, same name)
    PathOf,          \* [Mutators \cup {Deleter} -> Paths]
    InitEpoch,       \* [Paths -> Epochs]: the live incarnation at start
    Expected,        \* [Mutators \cup {Deleter} -> Epochs]: epoch validated at admission
    FreshEpoch,      \* the epoch minted by the recreate (never reused)
    Epochs,
    MaxAttempts,     \* mutate_incarnation / recreate bound (production: 5)
    MaxGen,          \* allocation ceiling in this finite instance
    MaxLostReply,    \* PUT committed, reply lost        (fault budget)
    MaxDispatchFail, \* PUT failed before dispatch       (fault budget)
    MaxMissingEtag,  \* GET returned no ETag             (fault budget)
    MaxReadFail,     \* GET failed (ReadUnavailable)     (fault budget)
    MaxRecreateRetry,\* the deleter's client retries an ambiguous recreate
    NONE

Actors == Mutators \cup {Deleter}

ASSUME /\ Deleter \notin Mutators
       /\ FreshEpoch \in Epochs
       /\ \A p \in Paths : InitEpoch[p] # FreshEpoch
       /\ Expected[Deleter] = InitEpoch[PathOf[Deleter]]
       /\ MaxAttempts \in Nat \ {0}

VARIABLES
    obj,     \* [Paths -> descriptor object]  (durable, object store)
    act,     \* [Actors -> local state of one mutate_incarnation/recreate call]
    faults,  \* remaining fault budgets
    hist     \* history variables (observation only, never read by actions)

vars == <<obj, act, faults, hist>>

Outcomes == {"Applied", "Declined", "IncarnationChanged", "MissingToken",
             "Conflict", "Ambiguous", "ReadUnavailable",
             "Recreated", "RecreateDeclined", "RecreateAmbiguous"}

Obj == [epoch : Epochs, deleted : BOOLEAN, gen : 0..MaxGen, ver : Nat]

(***************************************************************************)
(* Guards that the negative controls replace (CONSTANT Op <- MutOp).       *)
(***************************************************************************)

\* The incarnation fence.  Production has two guards against the caller's
\* expected_epoch: the snapshot check (registry.rs 1178) and the check on
\* the decided descriptor (1186).  No production `decide` changes the epoch
\* (every closure starts from current.to_persisted()), so the second guard
\* is reachable only if the first is removed; this operator is both.
IncarnationMatches(a, o) == o.epoch = Expected[a]

\* ConditionalUpdateToken::from_etag: a missing ETag is an error, never an
\* unconditional PUT.  TRUE would downgrade the write to PutMode::Overwrite.
UnconditionalOnMissingToken == FALSE

\* What the caller is told: the value returned with Applied is the result
\* of THIS (winning) attempt's decide -- a local of the loop body.
ReturnedResult(a) == act[a].prop.result

\* Every conditional request (PutMode::Create, PutMode::Update,
\* CopyMode::Create) goes through the client with max_retries 0
\* (src/bootstrap/s3_store.rs), so a conditional PUT is ONE request:
\* Precondition (and AlreadyExists) is the provider's answer to the only
\* request that carried the precondition, and a committed PUT is never
\* answered with it.  TRUE is the behaviour before "Registry conditional
\* writes never mistake their own committed write for a refusal":
\* object_store re-sent a conditional PUT after a 5xx, 429 or 408 with the
\* original precondition, so a PUT whose first request committed came back
\* Precondition -- to the registry, "another writer won".
CommittedAnsweredPrecondition == FALSE

(***************************************************************************)
(* Pure decisions (the `decide` closures).                                 *)
(***************************************************************************)
\* Allocation: Decline on a tombstone, otherwise Write(counter+1).
DecideAlloc(o) ==
    IF o.deleted THEN [write |-> FALSE, result |-> NONE]
    ELSE [write |-> TRUE, result |-> o.gen + 1,
          next |-> [o EXCEPT !.gen = o.gen + 1]]

\* Deletion tombstone (creation::deletion through mutate_incarnation).
DecideDelete(o) ==
    IF o.deleted THEN [write |-> FALSE, result |-> NONE]
    ELSE [write |-> TRUE, result |-> NONE, next |-> [o EXCEPT !.deleted = TRUE]]

Decide(a, o) == IF a = Deleter THEN DecideDelete(o) ELSE DecideAlloc(o)

Idle == [pc |-> "idle", att |-> 0, snap |-> NONE, prop |-> NONE, out |-> NONE,
         res |-> NONE, first |-> NONE, uncond |-> FALSE]

Init ==
    /\ obj = [p \in Paths |-> [epoch |-> InitEpoch[p], deleted |-> FALSE,
                                gen |-> 0, ver |-> 1]]
    /\ act = [a \in Actors |-> Idle]
    /\ faults = [lost |-> MaxLostReply, fail |-> MaxDispatchFail,
                 etag |-> MaxMissingEtag, read |-> MaxReadFail,
                 recreate |-> MaxRecreateRetry]
    /\ hist = [wrote |-> [a \in Actors |-> NONE],     \* gen the store holds after this call's committed PUT
               cross |-> FALSE,                       \* a PUT landed on a foreign incarnation
               recreateRetried |-> FALSE,             \* an ambiguous recreate was retried
               recreateWrote |-> FALSE]               \* the CURRENT recreate call's PUT committed

Finish(a, out, res) ==
    act' = [act EXCEPT ![a].pc = "done", ![a].out = out, ![a].res = res,
                       ![a].snap = NONE, ![a].prop = NONE, ![a].uncond = FALSE]

(***************************************************************************)
(* Actions                                                                  *)
(***************************************************************************)

\* A request begins its registry call.  It validated Expected[a] at
\* admission: a mutator validated against the replacement can exist only
\* once the replacement does.
Start(a) ==
    /\ act[a].pc = "idle"
    /\ a # Deleter
    /\ Expected[a] = FreshEpoch => obj[PathOf[a]].epoch = FreshEpoch
    /\ act' = [act EXCEPT ![a].pc = "read"]
    /\ UNCHANGED <<obj, faults, hist>>

StartDelete ==
    /\ act[Deleter].pc = "idle"
    /\ act' = [act EXCEPT ![Deleter].pc = "read"]
    /\ UNCHANGED <<obj, faults, hist>>

\* One attempt's GET + decode + incarnation check + decide + token check.
\* src/registry.rs Registry::mutate_incarnation (loop body up to put_opts).
Read(a) ==
    /\ act[a].pc = "read"
    /\ LET o == obj[PathOf[a]]
           d == Decide(a, o)
           first == IF act[a].first = NONE THEN d.result ELSE act[a].first
       IN
       \/ \* the GET failed: MutationError::ReadUnavailable (nothing written)
          /\ faults.read > 0
          /\ faults' = [faults EXCEPT !.read = @ - 1]
          /\ Finish(a, "ReadUnavailable", NONE)
          /\ UNCHANGED <<obj, hist>>
       \/ /\ IF ~IncarnationMatches(a, o)
             THEN /\ Finish(a, "IncarnationChanged", NONE)
                  /\ UNCHANGED <<obj, faults, hist>>
             ELSE IF ~d.write
             THEN /\ Finish(a, "Declined", NONE)
                  /\ UNCHANGED <<obj, faults, hist>>
             ELSE \/ \* ETag present: conditional update armed against this version.
                     /\ act' = [act EXCEPT ![a].pc = "cas", ![a].snap = o,
                                           ![a].prop = d, ![a].first = first]
                     /\ UNCHANGED <<obj, faults, hist>>
                  \/ \* The store answered without an ETag.
                     /\ faults.etag > 0
                     /\ faults' = [faults EXCEPT !.etag = @ - 1]
                     /\ IF UnconditionalOnMissingToken
                        THEN act' = [act EXCEPT ![a].pc = "cas", ![a].snap = o,
                                                ![a].uncond = TRUE,
                                                ![a].prop = d, ![a].first = first]
                        ELSE Finish(a, "MissingToken", NONE)
                     /\ UNCHANGED <<obj, hist>>

\* The conditional PUT: one request (s3_store.rs), an atomic
\* compare-and-write under ASM-OBJSTORE-CAS; the reply is a separate
\* observable that may be lost.
Cas(a) ==
    /\ act[a].pc = "cas"
    /\ LET p == PathOf[a]
           o == obj[p]
           s == act[a].snap
           matches == act[a].uncond \/ o.ver = s.ver
           \* The object this PUT stores: the decided descriptor, as a new version.
           written == [act[a].prop.next EXCEPT !.ver = o.ver + 1]
           record == hist' = [hist EXCEPT
                        !.wrote[a] = written.gen,
                        !.cross = @ \/ (o.epoch # Expected[a])]
       IN
       \/ \* Precondition failed: re-read and re-decide from scratch.
          /\ ~matches
          /\ IF act[a].att + 1 < MaxAttempts
             THEN act' = [act EXCEPT ![a].pc = "read", ![a].att = @ + 1,
                                     ![a].snap = NONE, ![a].prop = NONE,
                                     ![a].uncond = FALSE]
             ELSE Finish(a, "Conflict", NONE)
          /\ UNCHANGED <<obj, faults, hist>>
       \/ \* Committed, reply delivered.
          /\ matches
          /\ obj' = [obj EXCEPT ![p] = written]
          /\ record
          /\ Finish(a, "Applied", ReturnedResult(a))
          /\ UNCHANGED faults
       \/ \* Committed, reply lost: MutationError::AmbiguousCompletion.
          /\ matches
          /\ faults.lost > 0
          /\ faults' = [faults EXCEPT !.lost = @ - 1]
          /\ obj' = [obj EXCEPT ![p] = written]
          /\ record
          /\ Finish(a, "Ambiguous", NONE)
       \/ \* Failed before dispatch: also AmbiguousCompletion (not a Conflict).
          /\ faults.fail > 0
          /\ faults' = [faults EXCEPT !.fail = @ - 1]
          /\ Finish(a, "Ambiguous", NONE)
          /\ UNCHANGED <<obj, hist>>
       \/ \* Only with a retrying client (never in the unmodified model): the
          \* first request committed, its reply was a 5xx, and the client's
          \* re-sent request was refused by that very write -- Precondition,
          \* so the loop re-reads and re-decides against its own write.
          /\ CommittedAnsweredPrecondition
          /\ matches
          /\ faults.lost > 0
          /\ faults' = [faults EXCEPT !.lost = @ - 1]
          /\ obj' = [obj EXCEPT ![p] = written]
          /\ record
          /\ IF act[a].att + 1 < MaxAttempts
             THEN act' = [act EXCEPT ![a].pc = "read", ![a].att = @ + 1,
                                     ![a].snap = NONE, ![a].prop = NONE,
                                     ![a].uncond = FALSE]
             ELSE Finish(a, "Conflict", NONE)

\* The deleter's tombstone call finished; it now recreates the same name
\* with the same key (fresh epoch): creation/claim.rs resolve -> Registry::
\* recreate.  After an ambiguous recreate the client retries the create
\* (a fresh request through the same resolve path).
BeginRecreate ==
    /\ act[Deleter].pc = "done"
    /\ \/ act[Deleter].out \in {"Applied", "Ambiguous", "Declined"}
       \/ /\ act[Deleter].out = "RecreateAmbiguous"
          /\ faults.recreate > 0
    /\ faults' = IF act[Deleter].out = "RecreateAmbiguous"
                 THEN [faults EXCEPT !.recreate = @ - 1] ELSE faults
    /\ act' = [act EXCEPT ![Deleter] = [Idle EXCEPT !.pc = "rread"]]
    /\ hist' = [hist EXCEPT !.recreateRetried = @ \/ act[Deleter].out = "RecreateAmbiguous",
                            !.recreateWrote = FALSE]
    /\ UNCHANGED obj

\* Registry::recreate: GET, `still_dead` judged on the STORED descriptor.
\* A live descriptor (including this client's own earlier recreate) is
\* returned as (false, current) and validated like an idempotent PUT.
RecreateRead ==
    /\ act[Deleter].pc = "rread"
    /\ LET o == obj[PathOf[Deleter]] IN
       \/ /\ faults.read > 0
          /\ faults' = [faults EXCEPT !.read = @ - 1]
          /\ Finish(Deleter, "RecreateAmbiguous", NONE)   \* Err(e) -> 500; nothing written
          /\ UNCHANGED <<obj, hist>>
       \/ /\ ~o.deleted
          /\ Finish(Deleter, "RecreateDeclined", NONE)
          /\ UNCHANGED <<obj, faults, hist>>
       \/ /\ o.deleted
          /\ act' = [act EXCEPT ![Deleter].pc = "rcas", ![Deleter].snap = o]
          /\ UNCHANGED <<obj, faults, hist>>
       \/ /\ o.deleted
          /\ faults.etag > 0
          /\ faults' = [faults EXCEPT !.etag = @ - 1]
          /\ Finish(Deleter, "MissingToken", NONE)
          /\ UNCHANGED <<obj, hist>>

\* The recreate's conditional PUT.  Any error other than Precondition is
\* returned as Err(e) whether or not the write committed (registry.rs 1055).
RecreateCas ==
    /\ act[Deleter].pc = "rcas"
    /\ LET p == PathOf[Deleter]
           o == obj[p]
           s == act[Deleter].snap
           fresh == [epoch |-> FreshEpoch, deleted |-> FALSE, gen |-> 0, ver |-> o.ver + 1]
       IN
       \/ /\ o.ver # s.ver
          /\ IF act[Deleter].att + 1 < MaxAttempts
             THEN act' = [act EXCEPT ![Deleter].pc = "rread", ![Deleter].att = @ + 1,
                                     ![Deleter].snap = NONE]
             ELSE Finish(Deleter, "Conflict", NONE)
          /\ UNCHANGED <<obj, faults, hist>>
       \/ /\ o.ver = s.ver
          /\ obj' = [obj EXCEPT ![p] = fresh]
          /\ Finish(Deleter, "Recreated", NONE)
          /\ hist' = [hist EXCEPT !.recreateWrote = TRUE]
          /\ UNCHANGED faults
       \/ \* committed, reply lost
          /\ o.ver = s.ver
          /\ faults.lost > 0
          /\ faults' = [faults EXCEPT !.lost = @ - 1]
          /\ obj' = [obj EXCEPT ![p] = fresh]
          /\ Finish(Deleter, "RecreateAmbiguous", NONE)
          /\ hist' = [hist EXCEPT !.recreateWrote = TRUE]
       \/ \* Only with a retrying client: committed, answered Precondition by
          \* the client's own re-sent request, so recreate re-reads and finds
          \* the replacement it wrote itself live.
          /\ CommittedAnsweredPrecondition
          /\ o.ver = s.ver
          /\ faults.lost > 0
          /\ faults' = [faults EXCEPT !.lost = @ - 1]
          /\ obj' = [obj EXCEPT ![p] = fresh]
          /\ hist' = [hist EXCEPT !.recreateWrote = TRUE]
          /\ IF act[Deleter].att + 1 < MaxAttempts
             THEN act' = [act EXCEPT ![Deleter].pc = "rread", ![Deleter].att = @ + 1,
                                     ![Deleter].snap = NONE]
             ELSE Finish(Deleter, "Conflict", NONE)
       \/ \* failed before dispatch
          /\ faults.fail > 0
          /\ faults' = [faults EXCEPT !.fail = @ - 1]
          /\ Finish(Deleter, "RecreateAmbiguous", NONE)
          /\ UNCHANGED <<obj, hist>>

\* The deleter's work ends without a (further) recreate attempt only when
\* its tombstone call could not decide, or its recreate reached a verdict.
DeleterSettled ==
    /\ act[Deleter].pc = "done"
    /\ \/ act[Deleter].out \in {"Recreated", "RecreateDeclined", "MissingToken",
                                "Conflict", "IncarnationChanged", "ReadUnavailable"}
       \/ act[Deleter].out = "RecreateAmbiguous" /\ faults.recreate = 0

\* A mutator validated against the replacement that never came to exist
\* is not a pending request.
MutatorSettled(a) ==
    \/ act[a].pc = "done"
    \/ act[a].pc = "idle" /\ Expected[a] = FreshEpoch /\ obj[PathOf[a]].epoch # FreshEpoch

\* Legitimate quiescence: every call has returned.  Any other state without
\* a successor is a stuck protocol and TLC reports it as a deadlock.
Quiescent ==
    /\ \A a \in Mutators : MutatorSettled(a)
    /\ DeleterSettled
    /\ UNCHANGED vars

Next ==
    \/ \E a \in Mutators : Start(a)
    \/ StartDelete
    \/ \E a \in Actors : Read(a) \/ Cas(a)
    \/ BeginRecreate \/ RecreateRead \/ RecreateCas
    \/ Quiescent

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety properties with TLC content                                       *)
(***************************************************************************)
TypeOK ==
    /\ obj \in [Paths -> Obj]
    /\ \A a \in Actors : act[a].pc \in {"idle", "read", "cas", "done", "rread", "rcas"}
    /\ \A a \in Actors : act[a].out \in Outcomes \cup {NONE}

\* No two callers are told they allocated the same generation of the same
\* incarnation.  A generation reported twice is a leaked allocation: two
\* holders of one fence value (C4, L5, L8's allocator).
UniqueAllocation ==
    \A a, b \in Mutators :
        (a # b /\ act[a].out = "Applied" /\ act[b].out = "Applied"
               /\ PathOf[a] = PathOf[b] /\ Expected[a] = Expected[b])
            => act[a].res # act[b].res

\* A mutation validated against incarnation E only ever stores into E:
\* measured on the object at the commit, not at the snapshot (L14, C4, T8).
IncarnationFenced == ~hist.cross

\* Every stored allocator equals the number of committed allocations made
\* against that incarnation of that path -- by callers that validated it.
\* A lost update (a PUT over a version it never read) or a stale write
\* that lands on the replacement breaks the count.
AllocatorCountsWrites ==
    \A p \in Paths :
        obj[p].gen = Cardinality({a \in Mutators :
                                    /\ PathOf[a] = p
                                    /\ Expected[a] = obj[p].epoch
                                    /\ hist.wrote[a] # NONE})

\* A recreate call is never answered as declined -- "a live descriptor this
\* call did not write" -- after its own conditional PUT committed.  (A NEW
\* request after an ambiguous answer may legitimately resolve against the
\* replacement it created: Witness_AmbiguousRecreateRetried.)  A declined
\* recreate skips the creation work (body, Ready publication), leaving the
\* stream Initializing.
RecreateAnswerTruthful ==
    ~(act[Deleter].out = "RecreateDeclined" /\ hist.recreateWrote)

(***************************************************************************)
(* Structural sanity (holds by construction of the model; README)           *)
(***************************************************************************)
\* The generation returned with Applied is the one the store now holds for
\* this call's committed write.
AttemptLocalResult ==
    \A a \in Mutators : act[a].out = "Applied" => act[a].res = hist.wrote[a]

\* Declined / IncarnationChanged / MissingToken / Conflict / ReadUnavailable
\* wrote nothing.  (Ambiguous makes no claim either way.)
NegativeOutcomesWroteNothing ==
    \A a \in Mutators :
        act[a].out \in {"Declined", "IncarnationChanged", "MissingToken", "Conflict",
                        "ReadUnavailable"}
            => hist.wrote[a] = NONE

(***************************************************************************)
(* Reachability witnesses: each is EXPECTED to be violated.                 *)
(***************************************************************************)
Witness_LoserRetriesThenApplies ==
    ~\E a \in Mutators : act[a].out = "Applied" /\ act[a].att >= 1
Witness_AmbiguousAfterWrite ==
    ~\E a \in Mutators : act[a].out = "Ambiguous" /\ hist.wrote[a] # NONE
Witness_StaleMutatorFenced ==
    ~\E a \in Mutators : act[a].out = "IncarnationChanged"
Witness_MissingTokenRefused ==
    ~\E a \in Mutators : act[a].out = "MissingToken"
Witness_Recreated ==
    ~\E p \in Paths : obj[p].epoch = FreshEpoch
Witness_ConflictExhausted ==
    ~\E a \in Mutators : act[a].out = "Conflict"
Witness_DeclinedOnTombstone ==
    ~\E a \in Mutators : act[a].out = "Declined"
\* ABA: a mutator validated against the replacement allocates on it while a
\* stale mutator validated against the tombstoned incarnation of the same
\* name and key is fenced.
Witness_ReplacementAllocatesWhileStaleFenced ==
    ~\E a, b \in Mutators :
        /\ Expected[a] = FreshEpoch /\ act[a].out = "Applied"
        /\ Expected[b] # FreshEpoch /\ PathOf[b] = PathOf[a]
        /\ act[b].out = "IncarnationChanged"
\* A recreate whose reply was lost is retried and resolves against the
\* replacement it created itself.
Witness_AmbiguousRecreateRetried ==
    ~(hist.recreateRetried /\ act[Deleter].out = "RecreateDeclined")
=============================================================================
