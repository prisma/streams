------------------ MODULE MC_ServingOwnership_nc_ring_authorizes ------------------
(* NEGATIVE CONTROL (TLA-011, durability-evidence control): while the node's *)
(* ownership view says it owns the prefix, its engine acknowledges every     *)
(* registered group without the storage durability evidence (durable_seq).  *)
(* This isolates "acknowledgement needs durable_seq", not ring preference as *)
(* such: the same violation follows from any premature ack (TLA-005's        *)
(* controls). The ring-preference controls are nc_ring_skips_refresh and     *)
(* nc_ring_grants_storage.                                                   *)
(* Expected: TLC reports AckedDurable violated.                              *)
EXTENDS ServingOwnership

MutClaimByRing(en, mine) ==
    IF mine /\ ~en.ho.terminal
    THEN [take |-> en.ho.pending, ho |-> [en.ho EXCEPT !.pending = <<>>]]
    ELSE RealClaim(en, mine)
=============================================================================
