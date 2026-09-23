----------------- MODULE MC_HandoffRetirement_nc_reclaim_claimed -----------------
(* NEGATIVE CONTROL (TLA-006): retirement reclaims an already claimed batch. *)
(* begin_close takes ownership of the groups the completer has already      *)
(* claimed durable (and is dispatching) as well as the registered ones.      *)
(* Expected: TLC reports SettledAtMostOnce violated.                         *)
EXTENDS HandoffRetirement

MutStrandedReclaim(h, d) == RetireStranded(h) \o (IF h.terminal THEN <<>> ELSE d.groups)
=============================================================================
