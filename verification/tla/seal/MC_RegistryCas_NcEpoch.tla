----------------------- MODULE MC_RegistryCas_NcEpoch -----------------------
(* NEGATIVE CONTROL (TLA-001): drop mutate_incarnation's expected-epoch    *)
(* fence (both production guards, registry.rs 1178 and 1186).  A mutation  *)
(* validated against E1 may then decide on, and store into, the same-name, *)
(* same-key replacement E2.  Expected: IncarnationFenced is violated.      *)
EXTENDS MC_RegistryCas

AnyIncarnation(a, o) == TRUE
=============================================================================
