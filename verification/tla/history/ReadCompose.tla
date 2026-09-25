----------------------------- MODULE ReadCompose -----------------------------
(***************************************************************************)
(* TLA-018 -- exact composition of history, hot storage and read           *)
(* visibility.                                                             *)
(*                                                                         *)
(* A client pages through one stream incarnation with the production       *)
(* merged read (application::read::execute_segment): snapshot the handle,  *)
(* serve [cursor, boundary) from history, scan the shard-log tail, then    *)
(* revalidate the scan against the absorbed boundary read at the scan's    *)
(* own visibility (absorption_race -> ShardEngine::visible_absorbed)       *)
(* before accepting it.  Every storage observation is a separate step, so  *)
(* absorption, trimming, durability, dispatch and an ownership move        *)
(* interleave with the reader.                                             *)
(*                                                                         *)
(* The writer is deliberately coarser than TLA-016 (HistoryAbsorb): the    *)
(* gather/flush/submit pipeline is one step that raises the contiguous     *)
(* durable history frontier hF, and an advance commits only below hF.      *)
(* That abstraction is justified by TLA-016's H3/LastRecoverableCopy       *)
(* result (history canonical+postings rows are durable and contiguous      *)
(* below every committed absorbed boundary); it is recorded as a           *)
(* dependency in README.md, not re-proved here.                            *)
(*                                                                         *)
(* API semantics (defined before asserting completeness):                  *)
(*   Mode = "durable": every page serves only remotely durable records;    *)
(*     the continuation cursor `pos` promises the whole eligible prefix.   *)
(*   Mode = "applied": the tail may include applied-but-not-durable        *)
(*     records (Prisma-Pending-From); only the durable resume cursor       *)
(*     `dpos` = min(scanned, handle.durable.next) carries the promise.     *)
(*     A page that ends past the durable frontier returns a provisional    *)
(*     continuation (KIND_KEY_V3, TLA-018-F3 fix): the engine that served  *)
(*     it (`peng`, standing for the writer epoch), the digest start        *)
(*     (`pfrom`) and the observation digest (`pdig`, what the client saw   *)
(*     in [pfrom, pos)).  The next page continues it only on the same      *)
(*     engine, or after a re-read on the current engine matches the       *)
(*     digest; otherwise the server answers resync with the durable        *)
(*     recovery cursor.  A position with no identity (V2) is a durable     *)
(*     position and starts an applied read only at or below the frontier. *)
(*   Filter = "none" is the unfiltered replay path; Filter = a routing key *)
(*   is the keyed path (product reads always pass Some(routing_key)).      *)
(*                                                                         *)
(* Mutation points: HistView, FilteredRace, ShortIndexAccepted,            *)
(* RaceBoundary, ContinuationCheck.                                         *)
(* Assumption probes (dependency contract, not production): AllowLost*.    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    N, InitNext,
    KeyOf,            \* [0..N-1 -> routing key]
    Size,             \* [0..N-1 -> plaintext bytes]
    Req,              \* requested page bytes (PageBudget::new)
    Filter,           \* "none" or one routing key
    Mode,             \* "durable" | "applied"
    MaxPend, MaxMoves, MaxPages, MaxLoops, TrimBudgets,
    AllowRing,        \* the durable tail ring may serve a window
    AllowCorrupt,     \* a postings page in range fails to decode
    AllowShortIndex,  \* the postings load window proves less than requested
    AllowLostPostings, \* ASSUMPTION PROBE: durable postings lost afterwards
    AllowLostCanonical, \* ASSUMPTION PROBE: durable canonical rows lost afterwards
    AllowReadError     \* the current engine's history read fails with a storage error

ASSUME /\ N \in Nat \ {0} /\ InitNext \in 0..N
       /\ Mode \in {"durable", "applied"}
       /\ Req \in Nat \ {0}
       /\ MaxPend \in Nat \ {0} /\ MaxMoves \in Nat /\ MaxPages \in Nat
       /\ MaxLoops \in Nat \ {0}
       /\ TrimBudgets \subseteq 0..N /\ \E b \in TrimBudgets : b > 0
       /\ AllowLostPostings \in BOOLEAN /\ AllowLostCanonical \in BOOLEAN
       /\ AllowReadError \in BOOLEAN

Offs == 0..(N-1)
Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x
SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y
RECURSIVE SeqOf(_)
SeqOf(S) == IF S = {} THEN <<>> ELSE <<SetMin(S)>> \o SeqOf(S \ {SetMin(S)})
RECURSIVE SumSize(_)
SumSize(S) == IF S = {} THEN 0 ELSE Size[SetMin(S)] + SumSize(S \ {SetMin(S)})

Eligible(o) == Filter = "none" \/ KeyOf[o] = Filter

TailT == [next : 0..N, abs : 0..N, safe : 0..N, trimmed : 0..N]
InitTail == [next |-> InitNext, abs |-> 0, safe |-> 0, trimmed |-> 0]
InitGen == [o \in Offs |-> IF o < InitNext THEN 1 ELSE 0]
IdleRd == [ph |-> "idle", eng |-> 0, B |-> 0, E |-> 0, cur |-> 0, cn |-> 0,
           pg |-> {}, tcur |-> 0, tseen |-> {}, maxb |-> 0, loops |-> 0,
           hsnap |-> 0]

VARIABLES
    \* writer / storage (current engine)
    A, pend, D, P,
    hF,         \* durable contiguous history frontier (canonical + postings)
    gen,        \* append generation per offset (content identity)
    eng,        \* current shard-engine incarnation (ownership epoch)
    old,        \* frozen views of the previous (fenced) engine
    moves,
    lostPost,   \* assumption probe only: postings lost after durability
    lostCanon,  \* assumption probe only: canonical rows lost after durability
    \* reader / client
    rd, pos, dpos, dgen, pages,
    \* the client's provisional continuation (meaningful iff pos > dpos)
    peng,       \* the engine (writer epoch) that served the provisional suffix
    pfrom,      \* the digest start: the first offset the digest covers
    pdig,       \* the observation digest: [o |-> content seen at o in [pfrom, pos), 0 if none]
    \* ghosts
    dupSeen, underClaim, ceilBreach, raceAdopted, unexplainedGap,
    envelopeUsed, shortPartial, bigDelivered, readOld, ringUsed,
    readErr,    \* ghost: a page on the CURRENT engine failed in its history leg
    contVerified, \* ghost: another engine's continuation was proven by a re-read
    refusedStale  \* ghost: a continuation the tail had passed was refused (resync)

wvars == <<A, pend, D, P, hF, gen, eng, old, moves, lostPost, lostCanon>>
kvars == <<peng, pfrom, pdig>>
cvars == <<rd, pos, dpos, dgen, pages, kvars>>
gvars == <<dupSeen, underClaim, ceilBreach, raceAdopted, unexplainedGap,
           envelopeUsed, shortPartial, bigDelivered, readOld, ringUsed, readErr,
           contVerified, refusedStale>>
vars == <<wvars, cvars, gvars>>

-----------------------------------------------------------------------------
(* Views the reader observes: its own engine's, frozen after a move.        *)
Cur == rd.eng = eng
VA == IF Cur THEN A ELSE old.A
VD == IF Cur THEN D ELSE old.D
VP == IF Cur THEN P ELSE old.P
VhF == IF Cur THEN hF ELSE old.hF
VGen(o) == IF Cur THEN gen[o] ELSE old.gen[o]

(* Mutation points (baseline definitions).                                  *)
\* The history leg reads the live partition through the engine's open Db.
HistView == VhF
\* absorption_race, filtered branch: race iff visible_absorbed > cursor.
FilteredRace(d, c) == d > c
\* execute_postings_plan: complete only if provable_to >= upto.
ShortIndexAccepted == FALSE
\* absorption_race reads the tail row at the scan's own visibility
\* (Deliver::durability(), shared by read_frames_until and
\* ShardEngine::visible_absorbed): Remote for a durable read, Memory for an
\* applied read, which sees applied trims before they are durable.
RaceBoundary == IF Mode = "durable" THEN VD.abs ELSE VA.abs
\* check_entry_start (read_request.rs): a continuation must prove its
\* history, and a V2 position past the durable frontier is refused in
\* applied mode (TLA-018-F3 fix).  FALSE is the pre-fix read, which only
\* refused a start beyond the current owner's tail.
ContinuationCheck == TRUE

-----------------------------------------------------------------------------
(* PageBudget contract (src/application/read_budget.rs): the first record  *)
(* of a page is admitted up to the record limit; later ones need room.     *)
Remaining(pg) == IF SumSize(pg) >= Req THEN 0 ELSE Req - SumSize(pg)
Full(pg) == Remaining(pg) = 0
Admit(pg, o) == pg = {} \/ Size[o] <= Remaining(pg)

(* Deliver candidates in offset order under the budget.  Results: the page *)
(* set, the consumed-next position, and whether the range completed.  With *)
(* early = TRUE a byte-capped scan may also stop after any delivery.       *)
RECURSIVE Walk(_, _, _, _)
Walk(pg, s, endCn, early) ==
    IF s = <<>> THEN {[pg |-> pg, cn |-> endCn, complete |-> TRUE]}
    ELSE LET o == Head(s) IN
         IF ~Admit(pg, o) THEN {[pg |-> pg, cn |-> o, complete |-> FALSE]}
         ELSE LET pg2 == pg \cup {o} IN
              (IF early THEN {[pg |-> pg2, cn |-> o + 1, complete |-> FALSE]} ELSE {})
              \cup Walk(pg2, Tail(s), endCn, early)

(* History candidates in [lo, hi) by source.                               *)
HistSources ==
    IF Filter = "none" THEN {"scan"}
    ELSE {"index"} \cup (IF AllowCorrupt THEN {"envelope"} ELSE {})
\* A missing canonical row is skipped silently by every source: the scan
\* and the envelope never see it, and execute_postings_plan consumes a
\* planned span "fully even if nothing matched" (postings_read.rs).
Cands(src, lo, hi) ==
    CASE src = "scan"     -> {o \in lo..(hi-1) : o < HistView /\ o \notin lostCanon}
      [] src = "index"    -> {o \in lo..(hi-1) : o < HistView /\ o \notin lostPost
                                                  /\ o \notin lostCanon /\ KeyOf[o] = Filter}
      [] src = "envelope" -> {o \in lo..(hi-1) : o < HistView /\ o \notin lostCanon
                                                  /\ KeyOf[o] = Filter}

TailVisible(o) ==
    IF Mode = "durable" THEN o >= VD.trimmed /\ o < VD.next
    ELSE o >= VA.trimmed /\ o < VA.next

ReadEndNow == IF Mode = "durable" THEN P.next ELSE Max(A.next, P.next)
DurCursor == IF Mode = "durable" THEN pos ELSE dpos

(* Provisional continuation (src/application/read_continuation.rs).        *)
NoDigest == [o \in Offs |-> 0]
\* The client holds a continuation: its position is past the durable cursor.
HasCont == Mode = "applied" /\ pos > dpos
\* Continuation::observed_in over verify_continuation's re-read of
\* [pfrom, pos) on the CURRENT engine at the command's (applied) visibility:
\* the digest must reach down to the recovery position, the read must cover
\* the whole range, and it must hold exactly the observed records.
ObservedIn ==
    /\ pfrom <= dpos
    /\ pos <= Max(A.next, P.next)
    /\ \A o \in pfrom..(pos - 1) : pdig[o] = IF Eligible(o) THEN gen[o] ELSE 0
\* Whether the entry span may start at pos.
StartAllowed ==
    \/ ~ContinuationCheck
    \/ Mode = "durable"
    \/ IF pos > dpos THEN peng = eng \/ ObservedIn   \* ReadStart::Continue
                     ELSE pos <= P.next             \* ReadStart::Position (V2)

-----------------------------------------------------------------------------
Init ==
    /\ A = InitTail /\ D = InitTail /\ P = InitTail /\ pend = <<>>
    /\ hF = 0 /\ gen = InitGen /\ eng = 1 /\ moves = 0 /\ lostPost = {} /\ lostCanon = {}
    /\ old = [A |-> InitTail, D |-> InitTail, P |-> InitTail, hF |-> 0, gen |-> InitGen]
    /\ rd = IdleRd /\ pos = 0 /\ dpos = 0 /\ dgen = [o \in Offs |-> 0] /\ pages = 0
    /\ peng = 0 /\ pfrom = 0 /\ pdig = NoDigest
    /\ dupSeen = FALSE /\ underClaim = FALSE /\ ceilBreach = FALSE
    /\ raceAdopted = FALSE /\ unexplainedGap = FALSE /\ envelopeUsed = FALSE
    /\ shortPartial = FALSE /\ bigDelivered = FALSE /\ readOld = FALSE
    /\ ringUsed = FALSE /\ readErr = FALSE
    /\ contVerified = FALSE /\ refusedStale = FALSE

NewGroup(T) == Len(pend) < MaxPend /\ A' = T /\ pend' = Append(pend, T)

-----------------------------------------------------------------------------
(* Writer (coarse; see header).                                             *)

WAppend ==
    /\ A.next < N
    /\ NewGroup([A EXCEPT !.next = A.next + 1])
    /\ gen' = [gen EXCEPT ![A.next] = @ + 1]
    /\ UNCHANGED <<D, P, hF, eng, old, moves, lostPost, lostCanon, cvars, gvars>>

WDurable ==
    /\ pend # <<>>
    /\ D' = Head(pend) /\ pend' = Tail(pend)
    /\ UNCHANGED <<A, P, hF, gen, eng, old, moves, lostPost, lostCanon, cvars, gvars>>

WDispatch ==
    /\ P # D /\ P' = D
    /\ UNCHANGED <<A, pend, D, hF, gen, eng, old, moves, lostPost, lostCanon, cvars, gvars>>

WHistFlush ==      \* gather + one WriteBatch (canonical+postings) + part.flush()
    /\ hF < P.next
    /\ \E h \in (hF + 1)..P.next : hF' = h
    /\ UNCHANGED <<A, pend, D, P, gen, eng, old, moves, lostPost, lostCanon, cvars, gvars>>

WAdvance ==        \* CommitOp::Absorbed applied by the committer
    /\ A.abs < hF
    /\ \E u \in (A.abs + 1)..hF, allowed \in TrimBudgets :
         LET prev == A.abs
             safe == Max(A.safe, prev)
         IN NewGroup([A EXCEPT !.abs = Min(u, A.next), !.safe = safe,
                               !.trimmed = Max(A.trimmed, Min(safe, A.trimmed + allowed))])
    /\ UNCHANGED <<D, P, hF, gen, eng, old, moves, lostPost, lostCanon, cvars, gvars>>

WTrim ==           \* CommitOp::TrimStep
    /\ A.trimmed < Min(A.safe, A.abs)
    /\ \E allowed \in TrimBudgets \ {0} :
         NewGroup([A EXCEPT !.trimmed = Min(Min(A.safe, A.abs), A.trimmed + allowed)])
    /\ UNCHANGED <<D, P, hF, gen, eng, old, moves, lostPost, lostCanon, cvars, gvars>>

WMove ==           \* ownership change: the old engine is fenced, views frozen
    /\ moves < MaxMoves
    /\ moves' = moves + 1
    /\ old' = [A |-> A, D |-> D, P |-> P, hF |-> hF, gen |-> gen]
    /\ eng' = eng + 1
    /\ A' = D /\ P' = D /\ pend' = <<>>
    /\ UNCHANGED <<D, hF, gen, lostPost, lostCanon, cvars, gvars>>

WLosePostings ==   \* assumption probe: a durable postings page disappears
    /\ AllowLostPostings
    /\ \E o \in Offs :
         /\ o < hF /\ o \notin lostPost /\ Filter # "none" /\ KeyOf[o] = Filter
         /\ lostPost' = lostPost \cup {o}
    /\ UNCHANGED <<A, pend, D, P, hF, gen, eng, old, moves, lostCanon, cvars, gvars>>

WLoseCanonical ==  \* assumption probe: a durable canonical row disappears
    /\ AllowLostCanonical
    /\ \E o \in Offs :
         /\ o < hF /\ o \notin lostCanon
         /\ lostCanon' = lostCanon \cup {o}
    /\ UNCHANGED <<A, pend, D, P, hF, gen, eng, old, moves, lostPost, cvars, gvars>>

-----------------------------------------------------------------------------
(* Reader / client                                                          *)

\* The durable cursor is min(consumed, handle.durable.next) read when the
\* page ENDS: ResolvedRead::execute overwrites page_progress's page-start
\* clamp with `next.after.min(floor)`, `floor` read after the page
\* (src/application/read_request.rs:642-652).  A page ending past it also
\* returns a continuation (Continuation::after_page, read_continuation.rs:
\* 120-151, called at read_request.rs:655-666): recover = the durable
\* cursor; the digest restarts at it when it reached the page start,
\* otherwise carries the continuation the page began at, otherwise starts
\* at the page start; it folds the page's records from there.
EndPage(cnF, pgF) ==
    LET start    == pos                 \* the page began at the client's position
        nd       == IF Mode = "applied" THEN Min(cnF, VP.next) ELSE cnF
        cont     == cnF > nd
        incoming == pos > dpos          \* the page began at a continuation
        from     == IF nd >= start THEN nd ELSE IF incoming THEN pfrom ELSE start
    IN
    /\ peng' = IF cont THEN rd.eng ELSE 0
    /\ pfrom' = IF cont THEN from ELSE 0
    /\ pdig' = IF cont
               THEN [o \in Offs |-> IF o >= from /\ o < cnF
                                    THEN IF o \in pgF THEN VGen(o)
                                         ELSE IF o < start THEN pdig[o] ELSE 0
                                    ELSE 0]
               ELSE NoDigest
    /\ contVerified' = contVerified /\ refusedStale' = refusedStale
    /\ dupSeen' = (dupSeen \/ \E o \in pgF : dgen[o] # 0 /\ (Mode = "durable" \/ o < dpos))
    /\ underClaim' = (underClaim \/ \E o \in pgF : o >= cnF)
    /\ ceilBreach' = (ceilBreach \/ (SumSize(pgF) > Req /\ Cardinality(pgF) > 1))
    /\ bigDelivered' = (bigDelivered \/ \E o \in pgF : Size[o] > Req)
    /\ readOld' = (readOld \/ (~Cur /\ pgF # {}))
    /\ dgen' = [o \in Offs |-> IF o \in pgF THEN VGen(o) ELSE dgen[o]]
    /\ pos' = cnF
    /\ dpos' = nd
    /\ rd' = IdleRd
    /\ readErr' = readErr

\* execute_read resolves the engine, reads the tail state, and checks the
\* entry span's start (check_entry_start, read_request.rs:350-352 and
\* :693-751) before the `start > end` guard (:353-355).  The continuation is
\* carried, not replaced: the page's EndPage mints the next one.
RStart ==
    /\ rd.ph = "idle"
    /\ pages < MaxPages
    /\ pos < ReadEndNow
    /\ StartAllowed
    /\ rd' = [ph |-> "hist", eng |-> eng, B |-> P.abs, E |-> ReadEndNow,
              cur |-> pos, cn |-> pos, pg |-> {}, tcur |-> pos, tseen |-> {},
              maxb |-> 0, loops |-> 0, hsnap |-> hF]
    /\ pages' = pages + 1
    /\ contVerified' = (contVerified \/ (ContinuationCheck /\ HasCont /\ peng # eng))
    /\ UNCHANGED <<wvars, pos, dpos, dgen, kvars, dupSeen, underClaim, ceilBreach,
                   raceAdopted, unexplainedGap, envelopeUsed, shortPartial,
                   bigDelivered, readOld, ringUsed, readErr, refusedStale>>

RReconnect ==      \* applied mode: resume from the durable cursor (a V2 position)
    /\ Mode = "applied"
    /\ rd.ph = "idle"
    /\ pos > dpos
    /\ pos' = dpos /\ peng' = 0 /\ pfrom' = 0 /\ pdig' = NoDigest
    /\ UNCHANGED <<wvars, rd, dpos, dgen, pages, gvars>>

\* verify_continuation refuses: another engine serves, and its re-read of
\* [pfrom, pos) did not prove the observation (a mismatch, a short or
\* partial re-read).  The read answers 409 cursor_beyond_tail with reason
\* history_replaced and the recovery cursor (ReadFailure::HistoryReplaced,
\* read_request.rs:747-750); the client resumes there.  Redelivery at and
\* after dpos then overwrites dgen.  A re-read that proves the observation
\* can still come back partial, so the refusal is enabled whenever the
\* engine differs.
RResync ==
    /\ ContinuationCheck
    /\ rd.ph = "idle"
    /\ HasCont
    /\ peng # eng
    /\ pos' = dpos /\ peng' = 0 /\ pfrom' = 0 /\ pdig' = NoDigest
    /\ refusedStale' = (refusedStale \/ (~ObservedIn /\ pos < ReadEndNow))
    /\ UNCHANGED <<wvars, rd, dpos, dgen, pages, dupSeen, underClaim, ceilBreach,
                   raceAdopted, unexplainedGap, envelopeUsed, shortPartial,
                   bigDelivered, readOld, ringUsed, readErr, contVerified>>

\* The page fails: its engine was fenced or closed, or (history leg) the
\* engine's history read fails with a storage error (a transient
\* object-store failure, say).  By ASM-SLATEDB-GC (iii) a read that needs a
\* missing SST errors; it never returns a short success.
RError ==
    /\ rd.ph # "idle"
    /\ ~Cur \/ (AllowReadError /\ rd.ph = "hist")
    /\ rd' = IdleRd
    /\ readErr' = (readErr \/ Cur)
    /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, dupSeen, underClaim, ceilBreach,
                   raceAdopted, unexplainedGap, envelopeUsed, shortPartial,
                   bigDelivered, readOld, ringUsed, contVerified, refusedStale>>

RHist ==           \* decode_history_range over [cur, min(boundary, end))
    /\ rd.ph = "hist"
    /\ LET hup == Min(rd.B, rd.E) IN
       IF rd.cur >= hup \/ Full(rd.pg)
         THEN /\ rd' = [rd EXCEPT !.ph = "tailstart"]
              /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, gvars>>
         ELSE
           \/ \E src \in HistSources :
                \E res \in Walk(rd.pg, SeqOf(Cands(src, rd.cur, hup)), hup, TRUE) :
                  /\ envelopeUsed' = (envelopeUsed \/ src = "envelope")
                  /\ IF res.complete
                       THEN /\ rd' = [rd EXCEPT !.pg = res.pg, !.cn = Max(rd.cn, hup),
                                                !.cur = hup, !.ph = "tailstart"]
                            /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, dupSeen,
                                           underClaim, ceilBreach, raceAdopted,
                                           unexplainedGap, shortPartial,
                                           bigDelivered, readOld, ringUsed, readErr,
                                           contVerified, refusedStale>>
                       ELSE /\ EndPage(Max(rd.cn, res.cn), res.pg)
                            /\ UNCHANGED <<wvars, pages, raceAdopted, unexplainedGap,
                                           shortPartial, ringUsed>>
           \/ /\ AllowShortIndex /\ Filter # "none"
              /\ \E pt \in (rd.cur + 1)..(hup - 1) :
                   \E res \in Walk(rd.pg, SeqOf(Cands("index", rd.cur, pt)), pt, TRUE) :
                     /\ shortPartial' = TRUE
                     /\ IF res.complete /\ ShortIndexAccepted
                          THEN /\ rd' = [rd EXCEPT !.pg = res.pg, !.cn = Max(rd.cn, hup),
                                                   !.cur = hup, !.ph = "tailstart"]
                               /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, dupSeen,
                                              underClaim, ceilBreach, raceAdopted,
                                              unexplainedGap, envelopeUsed,
                                              bigDelivered, readOld, ringUsed, readErr,
                                              contVerified, refusedStale>>
                          ELSE /\ EndPage(Max(rd.cn, IF res.complete THEN pt ELSE res.cn),
                                          res.pg)
                               /\ UNCHANGED <<wvars, pages, raceAdopted, unexplainedGap,
                                              envelopeUsed, ringUsed>>

RTailStart ==
    /\ rd.ph = "tailstart"
    /\ IF Full(rd.pg) \/ rd.cur >= rd.E
         THEN /\ EndPage(rd.cn, rd.pg)
              /\ UNCHANGED <<wvars, pages, raceAdopted, unexplainedGap, envelopeUsed,
                             shortPartial, ringUsed>>
         ELSE
           \/ /\ rd' = [rd EXCEPT !.ph = "tail", !.tcur = rd.cur, !.tseen = {},
                                  !.maxb = Remaining(rd.pg)]
              /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, gvars>>
           \/ /\ AllowRing /\ Mode = "durable"      \* ring_read + proves_durable_ring
              /\ \E e \in (rd.cur + 1)..Min(rd.E, VP.next) :
                   \E res \in Walk(rd.pg, SeqOf({o \in rd.cur..(e-1) : Eligible(o)}), e, FALSE) :
                     /\ ringUsed' = TRUE
                     /\ EndPage(Max(rd.cn, res.cn), res.pg)
                     /\ UNCHANGED <<wvars, pages, raceAdopted, unexplainedGap,
                                    envelopeUsed, shortPartial>>

RTailStep ==       \* read_frames_until: one row of the Remote/Memory scan
    /\ rd.ph = "tail"
    /\ rd.tcur < rd.E
    /\ SumSize(rd.tseen) < rd.maxb
    /\ rd' = [rd EXCEPT !.tcur = rd.tcur + 1,
                        !.tseen = IF TailVisible(rd.tcur) THEN rd.tseen \cup {rd.tcur}
                                  ELSE rd.tseen]
    /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, gvars>>

RTailCheck ==      \* absorption_race, then decode or re-serve from history
    /\ rd.ph = "tail"
    /\ rd.tcur = rd.E \/ SumSize(rd.tseen) >= rd.maxb
    /\ LET d     == RaceBoundary                  \* visible_absorbed
           dense == /\ rd.tseen # {}
                    /\ SetMin(rd.tseen) = rd.cur
                    /\ Cardinality(rd.tseen) = SetMax(rd.tseen) - SetMin(rd.tseen) + 1
           race  == IF Filter = "none" THEN ~dense ELSE FilteredRace(d, rd.cur)
       IN IF race
            THEN IF d > rd.B
                   THEN IF rd.loops < MaxLoops
                          THEN /\ rd' = [rd EXCEPT !.B = d, !.loops = rd.loops + 1,
                                                   !.ph = "hist", !.tseen = {}]
                               /\ raceAdopted' = TRUE
                               /\ UNCHANGED <<wvars, pos, dpos, dgen, pages, kvars, dupSeen,
                                              underClaim, ceilBreach, unexplainedGap,
                                              envelopeUsed, shortPartial,
                                              bigDelivered, readOld, ringUsed, readErr,
                                              contVerified, refusedStale>>
                          ELSE /\ EndPage(rd.cn, rd.pg)
                               /\ UNCHANGED <<wvars, pages, raceAdopted, unexplainedGap,
                                              envelopeUsed, shortPartial, ringUsed>>
                   ELSE /\ unexplainedGap' = TRUE      \* honest partial
                        /\ EndPage(rd.cn, rd.pg)
                        /\ UNCHANGED <<wvars, pages, raceAdopted, envelopeUsed,
                                       shortPartial, ringUsed>>
            ELSE \E res \in Walk(rd.pg, SeqOf({o \in rd.tseen : Eligible(o)}),
                                 IF rd.tseen = {} THEN rd.cn ELSE SetMax(rd.tseen) + 1,
                                 FALSE) :
                   /\ EndPage(Max(rd.cn, res.cn), res.pg)
                   /\ UNCHANGED <<wvars, pages, raceAdopted, unexplainedGap,
                                  envelopeUsed, shortPartial, ringUsed>>

-----------------------------------------------------------------------------
Settled ==
    /\ A.next = N /\ pend = <<>> /\ A = D /\ P = D
    /\ hF = N /\ D.abs = N /\ D.trimmed = Min(D.safe, D.abs)
    /\ rd.ph = "idle"
    /\ \/ pages = MaxPages
       \/ pos >= ReadEndNow /\ ~(Mode = "applied" /\ pos > dpos)

Terminated == Settled /\ UNCHANGED vars

Next ==
    \/ WAppend \/ WDurable \/ WDispatch \/ WHistFlush \/ WAdvance \/ WTrim
    \/ WMove \/ WLosePostings \/ WLoseCanonical
    \/ RStart \/ RReconnect \/ RResync \/ RError \/ RHist \/ RTailStart \/ RTailStep
    \/ RTailCheck
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties                                                               *)

TypeOK ==
    /\ A \in TailT /\ D \in TailT /\ P \in TailT
    /\ Len(pend) <= MaxPend /\ \A i \in 1..Len(pend) : pend[i] \in TailT
    /\ hF \in 0..N /\ lostPost \subseteq Offs /\ lostCanon \subseteq Offs
    /\ rd.ph \in {"idle", "hist", "tailstart", "tail"}
    /\ rd.pg \subseteq Offs /\ rd.tseen \subseteq Offs
    /\ pos \in 0..N /\ dpos \in 0..N /\ dpos <= pos
    /\ peng \in 0..(MaxMoves + 1) /\ pfrom \in 0..N /\ pdig \in [Offs -> Nat]

\* Continuation::fits (read_continuation.rs:154-156): a continuation the
\* client holds belongs to its position, and no position past the durable
\* cursor lacks one.
ContinuationFits ==
    IF HasCont THEN dpos < pos /\ pfrom <= pos /\ peng # 0
               ELSE peng = 0 /\ pfrom = 0 /\ pdig = NoDigest

\* Writer-side dependency from TLA-016 (sanity: the coarse writer keeps it).
HistoryCoversBoundary ==
    /\ A.abs <= hF /\ D.abs <= hF /\ P.abs <= hF
    /\ A.trimmed <= A.safe /\ A.safe <= A.abs /\ A.abs <= A.next

\* Exact coverage of the promised prefix: every eligible offset below the
\* durability-promising cursor was delivered exactly with its durable
\* content, and no ineligible offset was delivered.
ExactDurablePrefix ==
    \A o \in Offs :
        o < DurCursor =>
            IF Eligible(o) THEN dgen[o] # 0 /\ dgen[o] = gen[o] /\ o < D.next
                           ELSE dgen[o] = 0

\* No record is delivered twice inside the promised prefix, and a page never
\* reports a consumed position at or below a record it delivered.
NoDuplicateDelivery == ~dupSeen /\ ~underClaim

\* Only records of the requested routing key are ever delivered.
NoFabricatedRecord == \A o \in Offs : dgen[o] # 0 => Eligible(o)

\* Hard page ceiling: a page exceeds the requested bytes only through its
\* single first record (the PageBudget large-record exception).
PageCeiling == ~ceilBreach

\* Every tail gap is explained by the absorbed boundary read at the scan's
\* visibility (a trim is visible at a level only with the advance that
\* justifies it), so the honest-partial branch is never needed and a reader
\* always progresses as far as a durable reader would.
TailGapExplained == ~unexplainedGap

-----------------------------------------------------------------------------
(* Reachability witnesses (expected violated on the unmodified model).      *)
Witness_BoundaryRaceAdopted == ~raceAdopted
Witness_LargeFirstRecordDelivered == ~bigDelivered
Witness_EnvelopeServed == ~envelopeUsed
Witness_ShortIndexPartial == ~shortPartial
Witness_ReadFromFencedEngine == ~readOld
Witness_RingServed == ~ringUsed
Witness_ReaderCompletes ==
    ~(pos = N /\ \A o \in Offs : Eligible(o) => dgen[o] # 0)
Witness_TrimBelowReaderCursor == ~(rd.ph = "tail" /\ VD.trimmed > rd.cur)
Witness_ReadErrorCurrentEngine == ~readErr
\* A continuation served by another (fenced) engine was proven by the
\* current engine's re-read, and the read continued without resync.
Witness_ContinuedAcrossMove == ~contVerified
\* A continuation whose suffix was lost and rewritten was refused although
\* the replacement tail had passed it (the pre-fix acceptance, F3).
Witness_StaleContinuationResynced == ~refusedStale
\* An applied read adopts an absorbed boundary that is applied but not yet
\* Remote-durable, and re-serves the trimmed prefix from history.
Witness_AppliedRaceAdopted ==
    ~(Mode = "applied" /\ rd.ph = "hist" /\ rd.loops > 0 /\ rd.B > VD.abs)
=============================================================================
