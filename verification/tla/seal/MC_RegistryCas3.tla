-------------------------- MODULE MC_RegistryCas3 --------------------------
(* TLA-001 expanded instance: THREE allocation mutators race on project    *)
(* P1's incarnation E1 (M1, M2, M4) while D tombstones it and recreates the *)
(* same name and key as E2; TWO mutators validated against the replacement *)
(* (M5, M6) race each other and the stale E1 mutators on it.  P2 holds the *)
(* same name in another project (no mutator: path separation is exercised  *)
(* by the small instance).                                                 *)
EXTENDS RegistryCas

CONSTANTS M1, M2, M4, M5, M6, D, P1, P2, E1, E2, E3

MCMutators == {M1, M2, M4, M5, M6}
MCPaths == {P1, P2}
MCEpochs == {E1, E2, E3}
MCPathOf == [a \in {M1, M2, M4, M5, M6, D} |-> P1]
MCInitEpoch == [p \in {P1, P2} |-> IF p = P1 THEN E1 ELSE E3]
MCExpected == [a \in {M1, M2, M4, M5, M6, D} |-> IF a \in {M5, M6} THEN E2 ELSE E1]
=============================================================================
