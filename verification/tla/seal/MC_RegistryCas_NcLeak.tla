------------------------ MODULE MC_RegistryCas_NcLeak ------------------------
(* NEGATIVE CONTROL (TLA-001): the outcome captured by the FIRST attempt   *)
(* survives into a retry -- the round-14 `release_fork_ref` shape of an     *)
(* FnMut closure with a captured out-parameter.  The retry still re-decides *)
(* and stores a fresh generation, but the caller is told the generation its *)
(* losing attempt decided, which the winner of that race was also told.     *)
(* Expected: UniqueAllocation is violated.                                  *)
EXTENDS MC_RegistryCas

LeakedReturnedResult(a) ==
    IF act[a].first # NONE THEN act[a].first ELSE act[a].prop.result
=============================================================================
