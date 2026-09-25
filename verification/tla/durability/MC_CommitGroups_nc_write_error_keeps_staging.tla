-------------- MODULE MC_CommitGroups_nc_write_error_keeps_staging --------------
(* NEGATIVE CONTROL (TLA-005, the pre-fix behaviour of TLA-005-F5): after a  *)
(* db.write error the committer answers the group Internal and keeps         *)
(* staging, without retiring the engine. A later group then loads a          *)
(* producer row from the failed batch, which default reads still return      *)
(* until closed_result is written, and attach on the empty open handoff      *)
(* answers Durable.                                                          *)
(* Expected: D2_DuplicateAckDurable violated (small shape) and               *)
(* D4_RefusalDurable violated (refusal-retry shape).                         *)
EXTENDS CommitGroups

MutWriteErrorKeepsStaging == FALSE
=============================================================================
