------------------------ MODULE MC_RegistryCas_NcEtag ------------------------
(* NEGATIVE CONTROL (TLA-001): a missing ETag downgrades the conditional   *)
(* update to an unconditional PUT (the failure ConditionalUpdateToken      *)
(* exists to refuse).  Two racers that read the same version then both     *)
(* store counter+1: one committed allocation is lost from the durable      *)
(* allocator.  Expected: AllocatorCountsWrites is violated.                *)
EXTENDS MC_RegistryCas

OverwriteOnMissingToken == TRUE
=============================================================================
