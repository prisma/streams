---------------- MODULE MC_CommitGroups_nc_reply_before_visible ----------------
(* NEGATIVE CONTROL (TLA-005, W1 order): dispatch_durable sends the claimed  *)
(* acks before it publishes the durable tail ring and handle.state.durable   *)
(* (the reply step is enabled straight after the claim, and the claimed      *)
(* groups' visibility step is then skipped).                                 *)
(* Expected: TLC reports AckAfterVisibility violated.                        *)
EXTENDS CommitGroups

MutReplyBeforeVisible == {"claimed", "visible"}
=============================================================================
