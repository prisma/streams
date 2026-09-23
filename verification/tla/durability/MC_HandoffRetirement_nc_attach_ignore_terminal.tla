------------- MODULE MC_HandoffRetirement_nc_attach_ignore_terminal -------------
(* NEGATIVE CONTROL (TLA-006, the section 1.5 boundary): CommitHandoff::     *)
(* attach ignores `terminal`. After retirement the drained handoff is empty, *)
(* so a no-write transaction that reached the attach step answers Durable    *)
(* and replies success from the retired engine -- a success decided after   *)
(* retirement, on already-durable state (so SuccessIsDurable may still hold).*)
(* Expected: TLC reports LateSuccessWasClaimedLive violated.                 *)
EXTENDS HandoffRetirement

MutAttachIgnoreTerminal(h) == IF h.pending # <<>> THEN "Pending" ELSE "Durable"
=============================================================================
