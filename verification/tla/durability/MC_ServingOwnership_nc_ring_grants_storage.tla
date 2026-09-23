--------------- MODULE MC_ServingOwnership_nc_ring_grants_storage ---------------
(* NEGATIVE CONTROL (TLA-011): routing preference grants durable write       *)
(* authority. A writer whose ownership view says it owns the prefix has its  *)
(* WAL put accepted even after a newer writer's fence (as if the storage     *)
(* dependency deferred to the ring instead of put-if-absent WAL ids and the  *)
(* manifest writer epoch). Acknowledgement still waits for durability.       *)
(* Expected: TLC reports AckedDurable violated.                              *)
EXTENDS ServingOwnership

MutLandByRing(en, mine, walLen) == mine \/ en.nextWal = walLen + 1
=============================================================================
