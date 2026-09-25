-------------------- MODULE MC_CommitGroups_nc_skip_barrier --------------------
(* NEGATIVE CONTROL (TLA-005): a transaction with no new writes skips the    *)
(* prior barrier. CommitHandoff::attach answers Durable whenever the handoff *)
(* is not terminal, i.e. join_prior_barrier replies at once instead of       *)
(* attaching to the newest registered, not-yet-durable group.                *)
(* Expected: TLC reports D2_DuplicateAckDurable violated.                    *)
EXTENDS CommitGroups

MutAttachSkipBarrier(h, rs) == IF h.terminal THEN "Retired" ELSE "Durable"
=============================================================================
