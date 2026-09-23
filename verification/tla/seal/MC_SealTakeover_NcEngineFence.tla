------------------- MODULE MC_SealTakeover_NcEngineFence -------------------
(* NEGATIVE CONTROL (TLA-002-F1, the behaviour before "A seal takeover's   *)
(* fence outlives the engine that recorded it"): a newly opened engine     *)
(* enforces fence 0, as when the fence lived only in the engine-resident   *)
(* seal_fences map.  The row is still written but no engine reads it,      *)
(* which is observationally the pre-fix engine.  A final that passed its   *)
(* claim check before a replacement or a crash failover then commits       *)
(* below the takeover's fence.                                             *)
(* Expected: ClosureAuthorized (xproc, retire), ReleaseOnlyWhenUndeliverable *)
(* and SuccessProvesOutcome (engine shape).                                *)
EXTENDS MC_SealTakeover

EngineResidentFence(sg) == 0
=============================================================================
