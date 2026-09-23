--------------------------- MODULE MC_FinalSeal ---------------------------
(* TLA-003 instances.  Operation ids and producer lanes are derived from     *)
(* (surface, content, coordination) by SealProtocol's OpId / LaneOf.        *)
(*                                                                           *)
(* Shape A ("lanes"): raw close-with-content X writes its final record with  *)
(* client producer (L, seq 1).  N0 = (L, 0) is its predecessor, which may    *)
(* still be in flight (producer gap); N1 = (L, 1) is an earlier NON-closing  *)
(* raw append with the same producer tuple and the same bytes as X.  S is an *)
(* unrelated plain seal.  A2 puts X's two requests on two concurrent slots,  *)
(* the exact retry on instance B.                                            *)
(*                                                                           *)
(* Shape B ("renewal"): X (raw) and Y (product) carry the same bytes under   *)
(* different coordination (producer lanes LX and LY, hence distinct          *)
(* operation ids).  X has two concurrent handler slots, so its exact retry   *)
(* renews while the original attempt is still in flight.  S is a plain seal. *)
(* Shape C: shape B with X on the product surface.                           *)
(*                                                                           *)
(* Validation shapes.  Validity is the verdict of an operation's request     *)
(* under the configuration of the instance that handles it: the record       *)
(* ceiling and the ingest capacity are per-process settings, so an exact     *)
(* retry handled by another instance can fare differently.                   *)
(*   V  (one instance): X raw close-with-content, two concurrent requests;   *)
(*      Y product seal-with-final over the record ceiling (refused before its *)
(*      claim since the TLA-003-F3 fix); Z product refused by the             *)
(*      instance-independent pre-intent checks.                               *)
(*   V4 (capacity skew): X's original on A; its exact retry on B, whose       *)
(*      ingest capacity refuses it (TLA-003-F4).  Z as in V.                  *)
(*   V5 (ceiling skew): X's original on A; its exact retry on B, whose record *)
(*      ceiling defers a BadBody (TLA-003-F5).  S a plain seal.  B owns the   *)
(*      shard at start, so the retry reaches a committer.                     *)
(*   VP (product ceiling skew): Y's original on A; its exact retry on B, over *)
(*      B's record ceiling (TLA-003-F3, fixed: B refuses it before any claim).*)
(*      S a plain seal.  B owns the shard at start.                           *)
(*                                                                           *)
(* Shape P ("product lanes"): product seal-with-final X on client producer   *)
(* (L, 0); N1 a product append and N2 a raw append on the same tuple with    *)
(* the same bytes (N1's request hash differs by the seal flag; N2 has none). *)
(*                                                                           *)
(* Shape PW: X and N1 of shape P, plus a raw close-with-content W that is    *)
(* admitted while X's claim stands; X's claim is released without a lapse.  *)
(*                                                                           *)
(* Shape SL ("shared lane"): raw close-with-content X and product            *)
(* seal-with-final Y, both without producer headers and with the same bytes: *)
(* one synthetic lane, two operation ids.                                    *)
EXTENDS SealProtocol

CONSTANTS X, Y, Z, S, W, N0, N1, N2, L, LX, LY, A, B, CX, CY, CN

GenBound == desc.counter <= MaxGen
\* State constraint for the no-lapse negative control only: behaviours in
\* which no claim's lease ever lapses.
LeaseNeverLapses == desc.claim = NONE \/ ~desc.claim.lap
MCNoUnbounded == {}
OneProc == {A}
TwoProcs == {A, B}
HomeA == [hid \in HandlerIds |-> A]
HomeSplitX == [hid \in HandlerIds |-> IF hid = <<X, 2>> THEN B ELSE A]
HomeSplitY == [hid \in HandlerIds |-> IF hid = <<Y, 2>> THEN B ELSE A]
AllOk == [hid \in HandlerIds |-> "ok"]

\* ---- shape A ----
AFinalOps == {X}
APlainOps == {S}
AAppendOps == {N0, N1}
ASurface == [o \in {X, N0, N1} |-> "raw"]
AContent == [o \in {X, N0, N1} |-> IF o = N0 THEN CN ELSE CX]
AProducer == [o \in {X, N0, N1} |-> L]
ASeqOf == [o \in {X, N0, N1} |-> IF o = N0 THEN 0 ELSE 1]
ASlots == [o \in {X, S, N0, N1} |-> {1}]
ASlots2 == [o \in {X, S, N0, N1} |-> IF o = X THEN {1, 2} ELSE {1}]
ABudget == [o \in {X, S, N0, N1} |-> IF o = X THEN 2 ELSE 1]

\* ---- shape B ----
BFinalOps == {X, Y}
BPlainOps == {S}
BAppendOps == {}
BSurface == [o \in {X, Y} |-> IF o = X THEN "raw" ELSE "product"]
BContent == [o \in {X, Y} |-> CX]
BProducer == [o \in {X, Y} |-> IF o = X THEN LX ELSE LY]
BSeqOf == [o \in {X, Y} |-> 0]
BSlots == [o \in {X, Y, S} |-> IF o = X THEN {1, 2} ELSE {1}]
BBudget == [o \in {X, Y, S} |-> IF o = X THEN 2 ELSE 1]

\* ---- shape C: shape B with X on the product surface ----
CSurface == [o \in {X, Y} |-> "product"]

\* ---- validation shapes ----
VFinalOps == {X, Y, Z}
V4FinalOps == {X, Z}
V5FinalOps == {X}
VPFinalOps == {Y}
VPlainOps == {}
V5PlainOps == {S}
VAppendOps == {}
VSurface == [o \in {X, Y, Z} |-> IF o = X THEN "raw" ELSE "product"]
VContent == [o \in {X, Y, Z} |-> IF o = X THEN CX ELSE IF o = Y THEN CY ELSE CN]
VProducer == [o \in {X, Y, Z} |-> NONE]
VSeqOf == [o \in {X, Y, Z} |-> 0]
VSlots == [o \in {X, Y, Z} |-> IF o = X THEN {1, 2} ELSE {1}]
VBudget == [o \in {X, Y, Z} |-> IF o = X THEN 2 ELSE 1]
V5Slots == [o \in {X, S} |-> IF o = X THEN {1, 2} ELSE {1}]
\* X: the original, the exact retry on B, and one more exact retry.
V5Budget == [o \in {X, S} |-> IF o = X THEN 3 ELSE 1]
VPSlots == [o \in {Y, S} |-> IF o = Y THEN {1, 2} ELSE {1}]
VPBudget == [o \in {Y, S} |-> IF o = Y THEN 3 ELSE 1]

\* The verdict of op o's request on instance p.
VAt(o, p) == IF o = Y THEN "ceiling" ELSE IF o = Z THEN "pre" ELSE "ok"
V4At(o, p) == IF o = X /\ p = B THEN "capacity" ELSE IF o = Z THEN "pre" ELSE "ok"
V5At(o, p) == IF o = X /\ p = B THEN "ceiling" ELSE "ok"
VPAt(o, p) == IF o = Y /\ p = B THEN "ceiling" ELSE "ok"
VValidity == [hid \in HandlerIds |-> VAt(HOp(hid), HomeOf[hid])]
V4Validity == [hid \in HandlerIds |-> V4At(HOp(hid), HomeOf[hid])]
V5Validity == [hid \in HandlerIds |-> V5At(HOp(hid), HomeOf[hid])]
VPValidity == [hid \in HandlerIds |-> VPAt(HOp(hid), HomeOf[hid])]

\* ---- shape P ----
PFinalOps == {X}
PPlainOps == {S}
PAppendOps == {N1, N2}
PSurface == [o \in {X, N1, N2} |-> IF o = N2 THEN "raw" ELSE "product"]
PContent == [o \in {X, N1, N2} |-> CX]
PProducer == [o \in {X, N1, N2} |-> L]
PSeqOf == [o \in {X, N1, N2} |-> 0]
PSlots == [o \in {X, S, N1, N2} |-> {1}]
PBudget == [o \in {X, S, N1, N2} |-> IF o = X THEN 2 ELSE 1]

\* ---- shape PW: X and N1 of shape P, plus a raw close-with-content W ----
\* (no producer headers, other bytes).  X's claim can be released without
\* any lease lapse (its tuple holds N1's request: ProducerSeqReused) while
\* W's admission snapshot still shows it.
PWFinalOps == {X, W}
PWAppendOps == {N1}
PWSurface == [o \in {X, W, N1} |-> IF o = W THEN "raw" ELSE "product"]
PWContent == [o \in {X, W, N1} |-> IF o = W THEN CN ELSE CX]
PWProducer == [o \in {X, W, N1} |-> IF o = W THEN NONE ELSE L]
PWSeqOf == [o \in {X, W, N1} |-> 0]
PWSlots == [o \in {X, W, N1} |-> {1}]
PWBudget == [o \in {X, W, N1} |-> 1]

\* ---- shape SL ----
SLFinalOps == {X, Y}
SLPlainOps == {S}
SLAppendOps == {}
SLSurface == [o \in {X, Y} |-> IF o = X THEN "raw" ELSE "product"]
SLContent == [o \in {X, Y} |-> CX]
SLProducer == [o \in {X, Y} |-> NONE]
SLSeqOf == [o \in {X, Y} |-> 0]
SLSlots == [o \in {X, Y, S} |-> {1}]
SLBudget == [o \in {X, Y, S} |-> IF o = S THEN 1 ELSE 2]
=============================================================================
