------------ MODULE MC_HandoffRetirement_nc_write_error_keeps_staging ------------
(* NEGATIVE CONTROL (TLA-006, the pre-fix behaviour of TLA-005-F5): after a  *)
(* db.write error the committer answers the group Internal and keeps         *)
(* staging, without retiring the engine. A no-write verdict staged while     *)
(* default reads still return the failed batch depends on that batch, and    *)
(* attach on the empty open handoff answers Durable.                         *)
(* Expected: TLC reports SuccessIsDurable violated.                          *)
EXTENDS HandoffRetirement

MutWriteErrorKeepsStaging == FALSE
=============================================================================
