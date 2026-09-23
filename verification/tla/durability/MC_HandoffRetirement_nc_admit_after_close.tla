---------------- MODULE MC_HandoffRetirement_nc_admit_after_close ----------------
(* NEGATIVE CONTROL (TLA-006, guard-restating form): a new batch is admitted *)
(* after the close boundary without valid authority: CommitHandoff::         *)
(* publication hands out a registration slot even when the handoff is        *)
(* terminal. take_durable still refuses a terminal handoff, so the admitted  *)
(* group is never acknowledged; the target ghost only shows that the         *)
(* publication guard was removed. The customer-visible form is               *)
(* MC_HandoffRetirement_nc_admit_after_close_acked.                          *)
(* Expected: TLC reports RetiredEngineFrozen violated.                       *)
EXTENDS HandoffRetirement

MutPublicationAlways(h) == TRUE
=============================================================================
