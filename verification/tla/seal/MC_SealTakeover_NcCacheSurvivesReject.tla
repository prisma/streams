---------------- MODULE MC_SealTakeover_NcCacheSurvivesReject ----------------
(* NEGATIVE CONTROL (TLA-002-F2, the behaviour before "A SealSuperseded      *)
(* refusal waits until the fence behind it is durable"): a fence group        *)
(* rejected without retiring the engine leaves its raised fence in the cache  *)
(* with no row behind it.  A later refusal from that cache is barriered only  *)
(* by later groups, which do not carry the row; the refused final releases    *)
(* its claim, and after the next engine replacement an older generation of    *)
(* the same operation passes the old row.                                     *)
(* Expected: ClosureAuthorized.                                               *)
EXTENDS MC_SealTakeover

CacheSurvivesRejection(cache, sg) == cache
=============================================================================
