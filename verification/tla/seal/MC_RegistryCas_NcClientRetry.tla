--------------------- MODULE MC_RegistryCas_NcClientRetry ---------------------
(* NEGATIVE CONTROL (TLA-001, the behaviour before "Registry conditional     *)
(* writes never mistake their own committed write for a refusal"): the       *)
(* object-store client re-sends a conditional PUT after a 5xx with the       *)
(* original precondition, so a PUT whose first request committed is answered *)
(* Precondition.  mutate_incarnation then re-reads its own write and applies  *)
(* its decision again; recreate re-reads its own replacement and declines.   *)
(* Expected: AllocatorCountsWrites (a mutator's allocation is written twice) *)
(* and RecreateAnswerTruthful (a committed recreate answered as declined).   *)
EXTENDS MC_RegistryCas

ClientRetriesConditional == TRUE
=============================================================================
