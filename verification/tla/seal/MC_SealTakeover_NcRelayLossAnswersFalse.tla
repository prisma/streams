---------------- MODULE MC_SealTakeover_NcRelayLossAnswersFalse ----------------
(* NEGATIVE CONTROL (F1-a, the relayed fence's loss mapping): a takeover   *)
(* coordinated on a process that does not own the segment answers a relay  *)
(* lost before it lands (no peer URL, a refused credential, ownership      *)
(* moved again, a transport error, a 409) as though the owner had replied  *)
(* "not closed", with nothing queued at the owner.  The takeover installs  *)
(* its claim, and the old final queued at the owner, which no raised fence *)
(* refuses, still closes the segment under the old operation.              *)
(* fence_relay.rs answers every such loss Resumable.                       *)
(* RelayLossAnswersFalse is TFence with its third arm (the relay lost      *)
(* before it lands) changed from a retryable answer to that reply; its     *)
(* other three arms are TFence's (SealProtocol.tla), unchanged.            *)
(* Expected: ClosureAuthorized (xproc).                                    *)
EXTENDS MC_SealTakeover

RelayLossAnswersFalse(hid) ==
    /\ h[hid].pc = "fence"
    /\ \/ /\ HomeOf[hid] = owner
          /\ eng' = [eng EXCEPT !.q = Append(MarkAhead(@, hid),
                                             Req("fence", hid, NONE, h[hid].res, FALSE, FALSE))]
          /\ h' = [h EXCEPT ![hid].pc = "fwait"]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ HomeOf[hid] # owner
          /\ eng' = [eng EXCEPT !.q = Append(MarkAhead(@, hid),
                                             Req("fence", hid, NONE, h[hid].res, FALSE, FALSE))]
          /\ h' = [h EXCEPT ![hid].pc = "fwait"]
          /\ UNCHANGED <<faults, hist, wit>>
       \/ /\ HomeOf[hid] # owner
          /\ h' = [h EXCEPT ![hid].pc = "fwait", ![hid].rep = Rep(NONE, FALSE, FALSE)]
          /\ UNCHANGED <<eng, faults, hist, wit>>
       \/ /\ HomeOf[hid] = owner
          /\ faults.enq > 0
          /\ faults' = [faults EXCEPT !.enq = @ - 1]
          /\ Respond(hid, "retryable")
          /\ UNCHANGED eng
    /\ UNCHANGED <<desc, seg, lanes, owner, bud, wset>>
=============================================================================
