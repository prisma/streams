-------------------- MODULE MC_CommitGroups_nc_publish_early --------------------
(* NEGATIVE CONTROL (TLA-005): publish before durability. The reader-visible *)
(* durable tail (handle.state.durable / tail ring / touch journal) is         *)
(* published at local registration (CommitTransaction::publish) instead of   *)
(* at durable dispatch.                                                      *)
(* Expected: TLC reports PubVisibleDurable violated.                         *)
EXTENDS CommitGroups

MutPublishVisibleAtRegistration(v, s) == s
=============================================================================
