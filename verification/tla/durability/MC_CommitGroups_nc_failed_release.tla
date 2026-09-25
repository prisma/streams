------------------- MODULE MC_CommitGroups_nc_failed_release -------------------
(* NEGATIVE CONTROL (TLA-005): a failed group releases its dependent         *)
(* responses. When retirement strands registered groups (fatal WAL failure,  *)
(* fencing, move), begin_close sends each staged verdict instead of          *)
(* rejecting it with AppendErr::Moved.                                       *)
(* Expected: TLC reports D4_RefusalDurable violated.                         *)
EXTENDS CommitGroups

MutReleaseStranded(x) == x.kind
=============================================================================
