--------------------- MODULE MC_SealTakeover_NcNewest ---------------------
(* NEGATIVE CONTROL (TLA-002a): install_reserved_claim without the newest-   *)
(* reservation condition (`current.seal_gen_counter == reserved` removed).   *)
(* Two takeovers reserve against the same lapsed claim and both fence; the   *)
(* LOWER reservation may then install below the higher fence, and its own    *)
(* closes are refused: the collection is held Sealing by its own recovery.   *)
(* Expected: NewestInstall (nc_newest.cfg) / LiveClaimNeverFenced            *)
(* (nc_newest_live.cfg) is violated.                                         *)
EXTENDS MC_SealTakeover

InstallWithoutNewest(d, old, res) ==
    /\ d.claim # NONE
    /\ d.claim.op = old.op /\ d.claim.gen = old.gen
=============================================================================
