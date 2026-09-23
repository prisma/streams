--------------------------- MODULE MC_RegistryCas ---------------------------
(* TLA-001 small instance.  Two projects share one stream name (two paths). *)
(* M1 and M2 race allocations on project P1's incarnation E1 while D        *)
(* tombstones E1 and recreates the same name and key as E2; M5 was          *)
(* validated against the replacement E2 and races the stale E1 mutators on  *)
(* it; M3 allocates on the same name in project P2 (incarnation E3).        *)
EXTENDS RegistryCas

CONSTANTS M1, M2, M3, M5, D, P1, P2, E1, E2, E3

MCMutators == {M1, M2, M3, M5}
MCPaths == {P1, P2}
MCEpochs == {E1, E2, E3}
MCPathOf == [a \in {M1, M2, M3, M5, D} |-> IF a = M3 THEN P2 ELSE P1]
MCInitEpoch == [p \in {P1, P2} |-> IF p = P1 THEN E1 ELSE E3]
MCExpected == [a \in {M1, M2, M3, M5, D} |->
                 IF a = M3 THEN E3 ELSE IF a = M5 THEN E2 ELSE E1]
=============================================================================
