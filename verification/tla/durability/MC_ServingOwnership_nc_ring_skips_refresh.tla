---------------- MODULE MC_ServingOwnership_nc_ring_skips_refresh ----------------
(* NEGATIVE CONTROL (TLA-011, T12): authorize solely from ring preference.   *)
(* An opener whose node's managed ring names it as the owner treats that as  *)
(* authority and skips the final manifest refresh that detects a newer       *)
(* writer epoch: a superseded opener starts serving because the ring says it *)
(* owns the prefix. Everything else (fencing, durability evidence) is real.  *)
(* Expected: TLC reports HigherEpochCoversAcks violated.                     *)
EXTENDS ServingOwnership

MutRefreshByRing(en, ringMine, epoch) == ringMine \/ epoch = en.epoch
=============================================================================
