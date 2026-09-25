--------------- MODULE MC_CommitGroups_nc_idem_close_unbarriered ---------------
(* NEGATIVE CONTROL (TLA-005, D3): "an idempotent close writes nothing, so   *)
(* it needs no barrier". A no-write group whose verdicts are all idempotent  *)
(* close successes is answered Durable at once instead of attaching to the   *)
(* newest registered group (the one that closed the stream). Every other     *)
(* no-write verdict (duplicates, refusals) keeps the real barrier, so D1/D2  *)
(* cannot catch it.                                                          *)
(* Expected: TLC reports D3_CloseAckDurable violated.                        *)
EXTENDS CommitGroups

MutAttachSkipBarrierForIdempotentClose(h, rs) ==
    IF ~h.terminal /\ rs # {} /\ \A x \in rs : x.kind = "idem"
    THEN "Durable"
    ELSE AttachVerdict(h)
=============================================================================
