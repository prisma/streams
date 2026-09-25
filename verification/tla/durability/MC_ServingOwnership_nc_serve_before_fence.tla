---------------- MODULE MC_ServingOwnership_nc_serve_before_fence ----------------
(* NEGATIVE CONTROL (TLA-011): the new owner begins before the dependency    *)
(* establishes fencing. After the manifest epoch CAS the engine replays and  *)
(* starts serving without writing its fence WAL, so the previous writer can  *)
(* still land (and acknowledge) WAL batches the new owner never replayed.    *)
(* Expected: TLC reports HigherEpochCoversAcks violated.                     *)
EXTENDS ServingOwnership

MutReadyGateNoFence == {"epoch", "fenced"}
=============================================================================
