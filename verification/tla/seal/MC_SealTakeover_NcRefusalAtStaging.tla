---------------- MODULE MC_SealTakeover_NcRefusalAtStaging ----------------
(* NEGATIVE CONTROL (TLA-002-F2, the behaviour before "A SealSuperseded      *)
(* refusal waits until the fence behind it is durable"): the committer sends  *)
(* SealSuperseded at staging, from a fence cache that a group not yet durable *)
(* raised.  The refused final releases its claim; if the fence group is then  *)
(* lost, an older generation of the same operation passes the old row.        *)
(* Expected: ClosureAuthorized (two instances, crash) and                     *)
(* ReleaseOnlyWhenUndeliverable (one instance, engine replacement).           *)
EXTENDS MC_SealTakeover

RefusalAtStaging(rep) == rep.err \in {"SealSuperseded", "BadBody", "Internal"}
=============================================================================
