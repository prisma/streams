------------- MODULE MC_HandoffRetirement_nc_no_storage_progress -------------
(* ASSUMPTION-REMOVAL CONTROL (TLA-006 liveness): the same fairness as       *)
(* LiveSpec except that WAL landing gets NO fairness, i.e. the storage       *)
(* progress half (d) of ASM-SLATEDB-DURABLE is withdrawn.                    *)
(* It shows AllIssuedSettle is not vacuous: it genuinely depends on that     *)
(* assumption. Expected: TLC reports the temporal property violated.         *)
EXTENDS HandoffRetirement

LiveSpecWithoutLanding ==
    /\ Spec
    /\ WF_vars(Take) /\ WF_vars(WriteOk) /\ WF_vars(WriteRefused) /\ WF_vars(Publish)
    /\ WF_vars(AttachGate) /\ WF_vars(AttachDecide) /\ WF_vars(AttachReply)
    /\ WF_vars(Claim) /\ WF_vars(Replies)
    /\ WF_vars(AckerClose) /\ WF_vars(RejectStranded)
    /\ WF_vars(Report)
=============================================================================
