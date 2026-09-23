------------ MODULE MC_HandoffRetirement_nc_admit_after_close_acked ------------
(* NEGATIVE CONTROL (TLA-006, customer-visible form of admit-after-close):   *)
(* CommitHandoff::publication hands out a registration slot after the close  *)
(* boundary AND take_durable ignores `terminal`, so a batch admitted after   *)
(* retirement is claimed and acknowledged by the retired engine: a new,      *)
(* unauthorized success rather than a late durable response.                 *)
(* Expected: TLC reports LateSuccessWasClaimedLive violated.                 *)
EXTENDS HandoffRetirement

MutPublicationAlways(h) == TRUE

MutTakeIgnoreTerminal(h, d) ==
    LET k == DurablePrefixLen(h.pending, d)
    IN [take |-> SubSeq(h.pending, 1, k),
        rest |-> [h EXCEPT !.pending = SubSeq(@, k + 1, Len(@))]]
=============================================================================
