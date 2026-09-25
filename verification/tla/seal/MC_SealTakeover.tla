-------------------------- MODULE MC_SealTakeover --------------------------
(* TLA-002 instances.  P0: the original final-bearing claimant (a product  *)
(* seal-with-final, content C0, no producer headers: synthetic lane), two  *)
(* concurrent handler slots and two requests (original + exact retry).     *)
(* Two takeover contenders: T1, a different product seal-with-final        *)
(* (content C1), and T2, a plain :seal.                                    *)
(*                                                                         *)
(* Process layouts: a single-instance deployment (every handler and the    *)
(* shard on A), and a two-instance deployment in which P0's exact retry is *)
(* handled by instance B while A owns the shard at start.                  *)
EXTENDS SealProtocol

CONSTANTS P0, T1, T2, A, B, C0, C1

MCFinalOps == {P0, T1}
MCPlainOps == {T2}
MCAppendOps == {}
MCSurface == [o \in {P0, T1} |-> "product"]
MCContent == [o \in {P0, T1} |-> IF o = P0 THEN C0 ELSE C1]
MCProducer == [o \in {P0, T1} |-> NONE]
MCSeqOf == [o \in {P0, T1} |-> 0]
MCSlots == [o \in {P0, T1, T2} |-> IF o = P0 THEN {1, 2} ELSE {1}]
MCBudget == [o \in {P0, T1, T2} |-> IF o = P0 THEN 2 ELSE 1]
MCValidity == [hid \in HandlerIds |-> "ok"]
MCNoUnbounded == {}
MCOneProc == {A}
MCTwoProcs == {A, B}
MCHomeA == [hid \in HandlerIds |-> A]
MCHomeSplit == [hid \in HandlerIds |-> IF hid = <<P0, 2>> THEN B ELSE A]
\* Liveness: the plain :seal client is the retrying recovery actor.
MCRecoveryActor == {T2}
\* One request per client.
MCBudgetOne == [o \in {P0, T1, T2} |-> 1]
\* Liveness: P0 issues one request and vanishes; T1 stays silent; T2 is the
\* retrying recovery actor.
MCBudgetLiveP0 == [o \in {P0, T1, T2} |-> IF o = T1 THEN 0 ELSE 1]

GenBound == desc.counter <= MaxGen
=============================================================================
