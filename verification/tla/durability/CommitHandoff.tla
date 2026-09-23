---------------------------- MODULE CommitHandoff ----------------------------
(***************************************************************************)
(* Shared abstraction of the terminal completion handoff                    *)
(* src/shard/commit_handoff.rs :: CommitHandoff.                            *)
(*                                                                          *)
(* A handoff value is a record                                              *)
(*     [terminal |-> BOOLEAN, pending |-> Seq(Group)]                       *)
(* where every Group is a record with at least                              *)
(*     seq     : the SlateDB write seqnum of the committed group            *)
(*     replies : a set of staged reply records (DurableEffects acks)        *)
(*                                                                          *)
(* Each operator below is the protocol transition of ONE method of          *)
(* CommitHandoff. Every production caller performs it while holding the     *)
(* engine's `in_flight` std mutex (ShardEngine::in_flight), never across an *)
(* await, so each is a single atomic step in the models that use it:       *)
(*                                                                          *)
(*   publication()   -> PublicationOpen / Register                          *)
(*                      (CommitTransaction::publish, transaction/publish.rs)*)
(*   attach()        -> AttachVerdict / AttachToNewest                      *)
(*                      (CommitTransaction::join_prior_barrier, finalize.rs)*)
(*   take_durable()  -> TakeDurable / AfterTakeDurable                      *)
(*                      (ShardEngine::dispatch_durable, shard.rs)           *)
(*   retire()        -> RetireStranded / RetireHandoff                      *)
(*                      (ShardEngine::begin_close, shard.rs)                *)
(*                                                                          *)
(* The models never rely on the Rust memory model: Loom                     *)
(* (commit_handoff/loom_tests.rs) owns the claim that the mutex makes these *)
(* four transitions linearizable.                                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences

NewHandoff == [terminal |-> FALSE, pending |-> <<>>]

(* CommitHandoff::publication: a registration slot exists iff not terminal. *)
PublicationOpen(h) == ~h.terminal

Register(h, g) == [h EXCEPT !.pending = Append(@, g)]

(* CommitHandoff::attach: the verdict for a transaction without writes.     *)
(* Terminal wins; otherwise a non-empty queue means "wait for the newest    *)
(* registered group"; an open empty queue is completed durable truth.       *)
AttachVerdict(h) ==
    IF h.terminal THEN "Retired"
    ELSE IF h.pending # <<>> THEN "Pending"
    ELSE "Durable"

(* Pending: the replies move onto the NEWEST registered group. *)
AttachToNewest(h, rs) ==
    [h EXCEPT !.pending[Len(h.pending)].replies = @ \cup rs]

(* CommitHandoff::take_durable: Vec::partition_point(seq <= d); nothing is  *)
(* claimable once terminal. The prefix form is exactly partition_point's    *)
(* result when `pending` is sorted (PendingSorted below is checked as an    *)
(* invariant by every model that uses these operators).                     *)
DurablePrefixLen(p, d) ==
    CHOOSE k \in 0..Len(p) :
        /\ \A i \in 1..k : p[i].seq <= d
        /\ (k = Len(p) \/ p[k + 1].seq > d)

TakeDurable(h, d) ==
    IF h.terminal THEN <<>>
    ELSE SubSeq(h.pending, 1, DurablePrefixLen(h.pending, d))

AfterTakeDurable(h, d) ==
    IF h.terminal THEN h
    ELSE [h EXCEPT !.pending = SubSeq(@, DurablePrefixLen(@, d) + 1, Len(@))]

(* CommitHandoff::retire: the first caller becomes terminal and owns every  *)
(* registered group; a later caller gets None (modelled as <<>>).           *)
RetireStranded(h) == IF h.terminal THEN <<>> ELSE h.pending

RetireHandoff(h) == [terminal |-> TRUE, pending |-> <<>>]

PendingSorted(h) ==
    \A i \in 1..(Len(h.pending) - 1) : h.pending[i].seq < h.pending[i + 1].seq

(* Union of the reply sets of a sequence of groups. *)
RepliesOf(gs) == UNION {gs[i].replies : i \in 1..Len(gs)}
=============================================================================
