------------ MODULE MC_ServingOwnership_nc_handoff_flips_before_close ------------
(* NEGATIVE CONTROL (TLA-011, F6): a planned move writes the document and   *)
(* its mirror while its engine is still parked, before the close (the order *)
(* F6 replaced). The other member opens the shard on the new document and   *)
(* fences the giver, whose registered batch then loses its WAL put.        *)
(* Expected: TLC reports HandOffFencesNothing violated.                     *)
EXTENDS ServingOwnership

MutFlipBeforeClose == {"parked", "closed"}
=============================================================================
