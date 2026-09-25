------------------- MODULE MC_SealTakeover_NcEarlyInstall -------------------
(* NEGATIVE CONTROL (TLA-002b): the takeover installs its claim once the     *)
(* fence is ENQUEUED, before the fence's durable, queue-ordered reply        *)
(* ("not closed") has been observed.  A final queued AHEAD of the fence is   *)
(* then still undecided when the replacement installs, and can close the     *)
(* segment under the replacement's claim.                                    *)
(* Expected: ClosureAuthorized (nc_early_install.cfg) /                      *)
(* QueuedFinalDecidedBeforeReplacement (nc_early_install_inqueue.cfg).       *)
EXTENDS MC_SealTakeover

FenceNotAwaited(hid) ==
    IF h[hid].rep = NONE THEN TRUE              \* fence enqueued, reply not yet seen
    ELSE h[hid].rep.err = NONE /\ ~h[hid].rep.closed
=============================================================================
