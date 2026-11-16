---------------------------- MODULE QueueDelivery ----------------------------
(***************************************************************************)
(* TLA-QD -- a native queue's delivery on its stream's pages (S3b part 1   *)
(* and part 2 step 1, S3b-DESIGN rev 3 §4-§6, §11.1).                      *)
(*                                                                         *)
(* One queue in one shard log.  The committer applies each operation as    *)
(* one commit group, and answers only once the group is durable, so a      *)
(* crash loses no answered state: it loses the handlers' replies in flight *)
(* (a claim's leases the handler had not yet handed out or given back).    *)
(*                                                                         *)
(* A message is one offset of the queue's stream, whose page holds its     *)
(* body (`page`: stored; part 2 step 1 deletes no page, step 3's trim     *)
(* will); `kind` says                                                      *)
(* whether it was sent immediate or scheduled (a future deliverAt), which  *)
(* the committer knows only at the send and later only from the range     *)
(* rows' bitmaps.  The state the committer keeps (native_queue/rows.rs,    *)
(* range.rs):                                                              *)
(*   - the frontier: every immediate offset below it was leased, cancelled *)
(*     or expired (lane.rs; StateRow.frontier);                            *)
(*   - the range rows 'x', one per RangeSize offsets: `opn` (open          *)
(*     deliveries), `sch` (unresolved scheduled), and the bitmaps `skip`   *)
(*     (scheduled offsets at or past the frontier), `cancel` (immediate    *)
(*     offsets cancelled untaken) and `sbits` (unresolved scheduled);      *)
(*   - the scheduled index 'a' (`aIdx`) and its cursor;                    *)
(*   - one delivery row 'd' per open delivery: Leased, Retrying or Failed, *)
(*     its attempts, releases, internal unleases and lease generation;     *)
(*   - the gauges queued_messages, queued_bytes and failed.                *)
(*                                                                         *)
(* A claim (claim.rs) leases the least-keyed due candidate among the due   *)
(* retries, the scheduled index after its cursor and the lane's page at    *)
(* the frontier, keyed (due, offset).  The frontier's step at a page the   *)
(* claim does not lease (lane.rs at_frontier, claim.rs nq_frontier_step)   *)
(* is `LanePass`: a scheduled offset is passed (its page goes when the     *)
(* scheduled message is resolved), a cancelled one is passed with its page *)
(* and bytes, an expired one enters the gap.  A sweep (sweep.rs) expires   *)
(* leases, retries past their cycle, the lane, scheduled entries and       *)
(* failures.                                                               *)
(*                                                                         *)
(* The claim's handler (application/queues/delivery.rs) opens each leased  *)
(* body with the request's key: an unreadable body is failed alone with    *)
(* cause `unreadable` (settle.rs SettleKind::Unreadable), a transient      *)
(* failure gives the lease back (`UnleaseInternal`, settle.rs nq_unlease): *)
(* its attempt back, its releases untouched, failed instead at the         *)
(* UnleaseLimit-th time (INTERNAL_UNLEASES, 8 in code).                    *)
(*                                                                         *)
(* Part 2 seals the queue's stream (§6): `sealed` is its sealed boundary;  *)
(* the committer holds the pages at or above it and none below.  A         *)
(* handler's `Prepare` reads the pages from the frontier up to the         *)
(* boundary, perhaps fewer, into `prep`; the committer serves the lane only *)
(* at its own frontier, from a page it holds or one prepared, and steps    *)
(* the frontier only there (lane.rs nq_lane_run), so a stale window, read  *)
(* at an older frontier, is harmless (§6.3).                               *)
(* A body's size is one byte: queued_bytes and queued_messages differ by   *)
(* the cancelled untaken messages the frontier has not yet passed.         *)
(*                                                                         *)
(* Mutation points (MC_QueueDelivery.tla): LaneOpen (control 1),           *)
(* HeadLive (2), CancelMarks (3), ScheduledClaimKeepsSbits (4), GenOK (5), *)
(* UnleaseReleases (6), LaneServes (7), UnleaseCounts (8).                 *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    N,              \* offsets 0..N-1
    RangeSize,      \* offsets per range row
    MaxAttempts,    \* the group's max_attempts
    FreeReleases,   \* FREE_RELEASES (2 in code)
    UnleaseLimit,   \* INTERNAL_UNLEASES (8 in code)
    MaxClock,       \* the queue clock runs 0..MaxClock
    Retention,      \* retention: a message's retention end is availableAt + Retention
    FailRetention,  \* failure retention; the hard expiry adds it to the retention end
    LeaseTime,      \* a claim's lease length
    MaxUnreadable,  \* bodies sent that never open
    MaxCrashes      \* crashes that lose the handlers' replies in flight

ASSUME /\ N \in Nat \ {0} /\ RangeSize \in Nat \ {0}
       /\ MaxAttempts \in Nat \ {0} /\ FreeReleases \in Nat
       /\ UnleaseLimit \in Nat \ {0} /\ MaxClock \in Nat
       /\ Retention \in Nat \ {0} /\ FailRetention \in Nat
       /\ LeaseTime \in Nat \ {0} /\ MaxUnreadable \in Nat /\ MaxCrashes \in Nat

Offs == 0..(N-1)
Ranges == 0..((N-1) \div RangeSize)
RangeOf(o) == o \div RangeSize
InRange(r) == {o \in Offs : RangeOf(o) = r}
Min(a, b) == IF a <= b THEN a ELSE b
NoRow == [st |-> "none"]
NoKey == <<-1, -1>>
\* Lexicographic order on (time, offset) keys.
KeyLess(k1, k2) == k1[1] < k2[1] \/ (k1[1] = k2[1] /\ k1[2] < k2[2])

VARIABLES
    clock,      \* the queue's clock
    next,       \* the next offset a send takes
    frontier,   \* StateRow.frontier
    kind,       \* "none" | "imm" | "sched", by offset (what the send was)
    avail,      \* availableAt: the send time, or the scheduled deliverAt
    unread,     \* the body never opens under the request's key
    page,       \* the page row 'p' is stored
    skip, cancel, sbits,  \* the range rows' bitmaps, as sets of offsets
    opn, sch,   \* the range rows' open and scheduled counts, by range
    aIdx,       \* the scheduled index 'a'
    cursor,     \* the scheduled cursor (cursor_at, cursor_off), or NoKey
    d,          \* the delivery row 'd' by offset, or NoRow
    qm, qb, fm, \* queued_messages, queued_bytes, failed
    outcome,    \* ghost: "none" until the message is resolved, then "ended"
                \* (acked, discarded, failure-expired), "cancelled" or "expired"
    leases,     \* ghost: leases taken of each body that never opens
    firsts,     \* ghost: first deliveries per offset
    inflight,   \* claim replies the handler has not yet acted on: <<o, gen>>
    tokens,     \* by offset, the newest lease receipt (generation) the client
                \* received, 0 for none: the client settles with it, current or
                \* stale (a stale receipt's generation is any older one: the
                \* check compares it with the current one alone)
    replayed,   \* ghost: a settle with a stale receipt changed a delivery
    unleaseBroke, \* ghost: an internal unlease touched releases or kept the attempt
    unreadSent, crashes,
    sealed,     \* the queue stream's sealed boundary (absorbed)
    prep,       \* the pages a claim's handler read below it (S3b §6.2)
    sealedLeased \* ghost: a lane lease was served from a sealed page

delivery == <<clock, next, frontier, kind, avail, unread, page, skip, cancel, sbits,
              opn, sch, aIdx, cursor, d, qm, qb, fm, outcome, leases, firsts,
              inflight, tokens, replayed, unleaseBroke, unreadSent, crashes>>
vars == <<delivery, sealed, prep, sealedLeased>>

RetentionEnd(o) == avail[o] + Retention
HardExpiry(o) == RetentionEnd(o) + FailRetention
Held(o) == d[o].st \in {"L", "R", "F"}

\* --- Mutation points (baseline definitions) --------------------------------
\* A lane lease opens the range row's reference (claim.rs nq_claim_lane).
LaneOpen(r) == opn[r] + 1
\* Whether the page at the frontier is live (lane.rs at_frontier's last arm).
HeadLive(f) == f \notin skip /\ f \notin cancel /\ RetentionEnd(f) > clock
\* A cancel of an untaken immediate message marks it (admin.rs nq_cancel_one).
CancelMarks(o) == cancel \cup {o}
\* A scheduled lease keeps its entry's bits until the message ends.
ScheduledClaimKeepsSbits(o) == sbits
\* A settle acts only on the lease its receipt names (settle.rs).
GenOK(o, g) == d[o].st = "L" /\ d[o].gen = g
\* An internal unlease leaves releases as they are.
UnleaseReleases(row) == row.rel
\* An internal unlease counts on the row.
UnleaseCounts(row) == row.iu + 1
\* The lane is served only at the frontier (lane.rs nq_lane_run).
LaneServes(o) == o = frontier

\* The committer holds the page at `o`, or a handler prepared it.
Available(o) == o >= sealed \/ o \in prep

TypeOK ==
    /\ clock \in 0..MaxClock /\ next \in 0..N /\ frontier \in 0..N
    /\ kind \in [Offs -> {"none", "imm", "sched"}]
    /\ page \in [Offs -> BOOLEAN] /\ unread \in [Offs -> BOOLEAN]
    /\ skip \subseteq Offs /\ cancel \subseteq Offs /\ sbits \subseteq Offs
    /\ aIdx \subseteq Offs
    /\ outcome \in [Offs -> {"none", "ended", "cancelled", "expired"}]
    /\ sealed \in 0..next /\ prep \subseteq Offs

Init ==
    /\ clock = 0 /\ next = 0 /\ frontier = 0
    /\ kind = [o \in Offs |-> "none"] /\ avail = [o \in Offs |-> 0]
    /\ unread = [o \in Offs |-> FALSE] /\ page = [o \in Offs |-> FALSE]
    /\ skip = {} /\ cancel = {} /\ sbits = {}
    /\ opn = [r \in Ranges |-> 0] /\ sch = [r \in Ranges |-> 0]
    /\ aIdx = {} /\ cursor = NoKey
    /\ d = [o \in Offs |-> NoRow]
    /\ qm = 0 /\ qb = 0 /\ fm = 0
    /\ outcome = [o \in Offs |-> "none"]
    /\ leases = [o \in Offs |-> 0] /\ firsts = [o \in Offs |-> 0]
    /\ inflight = {} /\ tokens = [o \in Offs |-> 0]
    /\ replayed = FALSE /\ unleaseBroke = FALSE
    /\ unreadSent = 0 /\ crashes = 0
    /\ sealed = 0 /\ prep = {} /\ sealedLeased = FALSE

\* ---------------------------------------------------------------------------
\* Sends (send.rs).

SendImmediate(u) ==
    /\ next < N
    /\ u => unreadSent < MaxUnreadable
    /\ kind' = [kind EXCEPT ![next] = "imm"]
    /\ avail' = [avail EXCEPT ![next] = clock]
    /\ unread' = [unread EXCEPT ![next] = u]
    /\ page' = [page EXCEPT ![next] = TRUE]
    /\ next' = next + 1
    /\ qm' = qm + 1 /\ qb' = qb + 1
    /\ unreadSent' = IF u THEN unreadSent + 1 ELSE unreadSent
    /\ UNCHANGED <<clock, frontier, skip, cancel, sbits, opn, sch, aIdx, cursor,
                   d, fm, outcome, leases, firsts, inflight, tokens, replayed,
                   unleaseBroke, crashes>>

SendScheduled(at) ==
    /\ next < N
    /\ at > clock /\ at <= MaxClock
    /\ kind' = [kind EXCEPT ![next] = "sched"]
    /\ avail' = [avail EXCEPT ![next] = at]
    /\ page' = [page EXCEPT ![next] = TRUE]
    /\ aIdx' = aIdx \cup {next}
    /\ skip' = skip \cup {next}
    /\ sbits' = sbits \cup {next}
    /\ sch' = [sch EXCEPT ![RangeOf(next)] = @ + 1]
    /\ next' = next + 1
    /\ qm' = qm + 1 /\ qb' = qb + 1
    /\ UNCHANGED <<clock, frontier, unread, cancel, opn, cursor, d, fm, outcome,
                   leases, firsts, inflight, tokens, replayed, unleaseBroke,
                   unreadSent, crashes>>

\* ---------------------------------------------------------------------------
\* The frontier's step at a page it does not lease (claim.rs nq_frontier_step,
\* also the sweep's lane expiry, sweep.rs nq_sweep_lane).

LanePass ==
    /\ frontier < next
    /\ Available(frontier)
    /\ ~HeadLive(frontier)
    /\ LET f == frontier IN
       \/ /\ f \in skip
          /\ skip' = skip \ {f}
          /\ UNCHANGED <<cancel, qm, qb, outcome>>
       \/ /\ f \notin skip /\ f \in cancel
          /\ cancel' = cancel \ {f}
          /\ qb' = qb - 1
          /\ UNCHANGED <<skip, qm, outcome>>
       \/ /\ f \notin skip /\ f \notin cancel
          \* Untaken past its retention end: into the gap.
          /\ qm' = qm - 1 /\ qb' = qb - 1
          /\ outcome' = [outcome EXCEPT ![f] = "expired"]
          /\ UNCHANGED <<skip, cancel>>
    /\ frontier' = frontier + 1
    /\ UNCHANGED <<clock, next, kind, avail, unread, page, sbits, opn, sch, aIdx,
                   cursor, d, fm, leases, firsts, inflight, tokens, replayed,
                   unleaseBroke, unreadSent, crashes>>

\* ---------------------------------------------------------------------------
\* Ending and failing a delivery (deliveries.rs).

\* The page of a message that ended stays in the queue's stream (part 2
\* step 1: no resolution deletes a page; step 3's trim reclaims them).
PageAfterEnd(o) == page

\* End a message with a delivery row: the row goes, the range's reference
\* closes, a scheduled message's entry and bits go (nq_end_message).
EndHeld(o, how) ==
    /\ d' = [d EXCEPT ![o] = NoRow]
    /\ opn' = [opn EXCEPT ![RangeOf(o)] = @ - 1]
    /\ IF kind[o] = "sched"
       THEN /\ aIdx' = aIdx \ {o} /\ sbits' = sbits \ {o}
            /\ sch' = [sch EXCEPT ![RangeOf(o)] = @ - 1]
       ELSE UNCHANGED <<aIdx, sbits, sch>>
    /\ page' = PageAfterEnd(o)
    /\ IF d[o].st = "F"
       THEN /\ fm' = fm - 1 /\ UNCHANGED <<qm, qb>>
       ELSE /\ qm' = qm - 1 /\ qb' = qb - 1 /\ UNCHANGED fm
    /\ outcome' = [outcome EXCEPT ![o] = how]

\* A delivery fails (nq_fail): out of the queued gauges, into failed.
FailRow(o, row) ==
    /\ d' = [d EXCEPT ![o] = [row EXCEPT !.st = "F", !.until = 0, !.due = 0,
                                         !.fexp = Min(clock + FailRetention, HardExpiry(o))]]
    /\ qm' = qm - 1 /\ qb' = qb - 1 /\ fm' = fm + 1

\* Due again at `due` (nq_retry_at).
RetryRow(o, row, due) == d' = [d EXCEPT ![o] = [row EXCEPT !.st = "R", !.due = due, !.until = 0]]

\* The policy retry, or failure when the attempts or the cycle are spent
\* (nq_retry_or_fail, a one-tick backoff).
RetryOrFail(o, row) ==
    IF row.att >= MaxAttempts \/ clock >= row.cycle
    THEN FailRow(o, row)
    ELSE /\ RetryRow(o, row, Min(clock + 1, row.cycle))
         /\ UNCHANGED <<qm, qb, fm>>

\* ---------------------------------------------------------------------------
\* Claims (claim.rs).

DueRetries == {o \in Offs : d[o].st = "R" /\ d[o].due <= clock}
DueScheduled == {o \in aIdx : avail[o] <= clock /\ KeyLess(cursor, <<avail[o], o>>)
                              /\ d[o].st = "none"}
LaneHead == {o \in Offs : LaneServes(o) /\ o < next /\ Available(o) /\ HeadLive(o)}
KeyOf(o) == IF o \in DueRetries THEN <<d[o].due, o>> ELSE <<avail[o], o>>
Candidates == DueRetries \cup DueScheduled \cup LaneHead
Least(S) == CHOOSE o \in S : \A p \in S : p = o \/ KeyLess(KeyOf(o), KeyOf(p))

\* A new lease of `row`, its attempt counted, its generation moved on.
Leased(o, row) == [row EXCEPT !.st = "L", !.att = row.att + 1, !.gen = row.gen + 1,
                              !.due = 0, !.until = Min(clock + LeaseTime, HardExpiry(o))]
FirstRow(o) == [st |-> "none", att |-> 0, rel |-> 0, iu |-> 0, gen |-> 0,
                until |-> 0, due |-> 0, fexp |-> 0, cycle |-> RetentionEnd(o)]

LeaseOut(o, row) ==
    /\ d' = [d EXCEPT ![o] = Leased(o, row)]
    /\ leases' = IF unread[o] THEN [leases EXCEPT ![o] = @ + 1] ELSE leases
    /\ inflight' = inflight \cup {<<o, row.gen + 1>>}

Claim ==
    /\ Candidates # {}
    /\ LET o == Least(Candidates) IN
       \/ \* A due retry: failed instead at its cycle deadline.
          /\ o \in DueRetries
          /\ IF clock >= d[o].cycle
             THEN /\ FailRow(o, d[o])
                  /\ UNCHANGED <<leases, inflight>>
             ELSE /\ LeaseOut(o, d[o])
                  /\ UNCHANGED <<qm, qb, fm>>
          /\ UNCHANGED <<frontier, page, skip, cancel, sbits, opn, sch, aIdx,
                         cursor, outcome, firsts>>
       \/ \* A due scheduled entry: the cursor passes it; expired into the gap.
          /\ o \in DueScheduled /\ o \notin DueRetries
          /\ cursor' = <<avail[o], o>>
          /\ IF RetentionEnd(o) <= clock
             THEN /\ aIdx' = aIdx \ {o} /\ sbits' = sbits \ {o}
                  /\ sch' = [sch EXCEPT ![RangeOf(o)] = @ - 1]
                  /\ page' = PageAfterEnd(o)
                  /\ qm' = qm - 1 /\ qb' = qb - 1
                  /\ outcome' = [outcome EXCEPT ![o] = "expired"]
                  /\ UNCHANGED <<d, opn, leases, firsts, inflight>>
             ELSE /\ LeaseOut(o, FirstRow(o))
                  /\ opn' = [opn EXCEPT ![RangeOf(o)] = @ + 1]
                  /\ firsts' = [firsts EXCEPT ![o] = @ + 1]
                  /\ sbits' = ScheduledClaimKeepsSbits(o)
                  /\ UNCHANGED <<aIdx, sch, page, qm, qb, outcome>>
          /\ UNCHANGED <<frontier, skip, cancel, fm>>
       \/ \* The lane's live page at the frontier.
          /\ o \in LaneHead /\ o \notin DueRetries /\ o \notin DueScheduled
          /\ LeaseOut(o, FirstRow(o))
          /\ opn' = [opn EXCEPT ![RangeOf(o)] = LaneOpen(RangeOf(o))]
          /\ firsts' = [firsts EXCEPT ![o] = @ + 1]
          /\ frontier' = IF o + 1 > frontier THEN o + 1 ELSE frontier
          /\ UNCHANGED <<page, skip, cancel, sbits, sch, aIdx, cursor, qm, qb, fm,
                         outcome>>
    /\ UNCHANGED <<clock, next, kind, avail, unread, tokens, replayed,
                   unleaseBroke, unreadSent, crashes>>

\* ---------------------------------------------------------------------------
\* The claim's handler (delivery.rs): each reply in flight is handed to the
\* client, failed alone as unreadable, or given back after a transient
\* failure.  A crash loses them all.

HandOut ==
    \E r \in inflight :
       /\ ~unread[r[1]]
       /\ tokens' = [tokens EXCEPT ![r[1]] = r[2]]
       /\ inflight' = inflight \ {r}
       /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, page, skip,
                      cancel, sbits, opn, sch, aIdx, cursor, d, qm, qb, fm,
                      outcome, leases, firsts, replayed, unleaseBroke,
                      unreadSent, crashes>>

FailUnreadable ==
    \E r \in inflight :
       /\ unread[r[1]]
       /\ inflight' = inflight \ {r}
       /\ IF GenOK(r[1], r[2])
          THEN FailRow(r[1], d[r[1]])
          ELSE UNCHANGED <<d, qm, qb, fm>>
       /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, page, skip,
                      cancel, sbits, opn, sch, aIdx, cursor, outcome, leases,
                      firsts, tokens, replayed, unleaseBroke, unreadSent, crashes>>

UnleaseInternal ==
    \E r \in inflight :
       LET o == r[1] IN
       /\ inflight' = inflight \ {r}
       /\ IF GenOK(o, r[2])
          THEN LET row == [d[o] EXCEPT !.iu = UnleaseCounts(d[o]),
                                       !.rel = UnleaseReleases(d[o])] IN
               /\ IF row.iu >= UnleaseLimit
                  THEN FailRow(o, row)
                  ELSE /\ RetryRow(o, [row EXCEPT !.att = row.att - 1], clock)
                       /\ UNCHANGED <<qm, qb, fm>>
               /\ unleaseBroke' = (unleaseBroke \/ d'[o].rel # d[o].rel
                                   \/ (d'[o].st = "R" /\ d'[o].att # d[o].att - 1))
          ELSE UNCHANGED <<d, qm, qb, fm, unleaseBroke>>
       /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, page, skip,
                      cancel, sbits, opn, sch, aIdx, cursor, outcome, leases,
                      firsts, tokens, replayed, unreadSent, crashes>>

Crash ==
    /\ crashes < MaxCrashes /\ inflight # {}
    /\ inflight' = {}
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, page, skip, cancel,
                   sbits, opn, sch, aIdx, cursor, d, qm, qb, fm, outcome, leases,
                   firsts, tokens, replayed, unleaseBroke, unreadSent>>

\* ---------------------------------------------------------------------------
\* Settles (settle.rs), with the newest receipt the client holds: a stale
\* one is a replay, which the generation check answers without a change.

SettleUnchanged == UNCHANGED <<clock, next, frontier, kind, avail, unread, skip,
                               cancel, cursor, leases, firsts, inflight, tokens,
                               unleaseBroke, unreadSent, crashes>>

\* The ghost: a settle whose receipt is not the current lease changed it.
Replay(o, g) == replayed' = (replayed \/ d[o].gen # g)

Ack(t) ==
    LET o == t[1] IN
    /\ GenOK(o, t[2])
    /\ EndHeld(o, "ended")
    /\ Replay(o, t[2])
    /\ SettleUnchanged

Fail(t) ==
    LET o == t[1] IN
    /\ GenOK(o, t[2])
    /\ FailRow(o, d[o])
    /\ Replay(o, t[2])
    /\ UNCHANGED <<page, sbits, opn, sch, aIdx, outcome>>
    /\ SettleUnchanged

RetryNow(t) ==
    LET o == t[1] IN
    LET row == [d[o] EXCEPT !.rel = 0] IN
    /\ GenOK(o, t[2])
    /\ IF row.att >= MaxAttempts \/ clock + 1 > row.cycle
       THEN FailRow(o, row)
       ELSE /\ RetryRow(o, row, clock + 1) /\ UNCHANGED <<qm, qb, fm>>
    /\ Replay(o, t[2])
    /\ UNCHANGED <<page, sbits, opn, sch, aIdx, outcome>>
    /\ SettleUnchanged

Release(t) ==
    LET o == t[1] IN
    /\ GenOK(o, t[2])
    /\ IF d[o].rel < FreeReleases
       THEN /\ RetryRow(o, [d[o] EXCEPT !.att = @ - 1, !.rel = @ + 1],
                        Min(clock + 1, d[o].cycle))
            /\ UNCHANGED <<qm, qb, fm>>
       ELSE RetryOrFail(o, [d[o] EXCEPT !.rel = 0])
    /\ Replay(o, t[2])
    /\ UNCHANGED <<page, sbits, opn, sch, aIdx, outcome>>
    /\ SettleUnchanged

Extend(t) ==
    LET o == t[1] IN
    /\ GenOK(o, t[2])
    /\ d' = [d EXCEPT ![o].until = Min(clock + LeaseTime, HardExpiry(o))]
    /\ Replay(o, t[2])
    /\ UNCHANGED <<page, sbits, opn, sch, aIdx, outcome, qm, qb, fm>>
    /\ SettleUnchanged

\* Discard a failed delivery (§5.7): it ends.
Discard(o) ==
    /\ d[o].st = "F"
    /\ EndHeld(o, "ended")
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, skip, cancel, cursor,
                   leases, firsts, inflight, tokens, replayed, unleaseBroke,
                   unreadSent, crashes>>

\* ---------------------------------------------------------------------------
\* Cancel (admin.rs nq_cancel_one): a held or failed delivery ends; an
\* unresolved scheduled message ends; an untaken immediate one at or past the
\* frontier is marked for the frontier, which passes it with its bytes.

Cancel(o) ==
    /\ o < next /\ outcome[o] = "none"
    /\ \/ /\ Held(o)
          /\ EndHeld(o, "cancelled")
          /\ UNCHANGED cancel
       \/ /\ ~Held(o) /\ o \in aIdx
          /\ aIdx' = aIdx \ {o} /\ sbits' = sbits \ {o}
          /\ sch' = [sch EXCEPT ![RangeOf(o)] = @ - 1]
          /\ page' = PageAfterEnd(o)
          /\ qm' = qm - 1 /\ qb' = qb - 1
          /\ outcome' = [outcome EXCEPT ![o] = "cancelled"]
          /\ UNCHANGED <<d, opn, fm, cancel>>
       \/ /\ ~Held(o) /\ o \notin aIdx /\ kind[o] = "imm"
          /\ o >= frontier /\ o \notin skip /\ o \notin cancel
          /\ cancel' = CancelMarks(o)
          /\ qm' = qm - 1
          /\ outcome' = [outcome EXCEPT ![o] = "cancelled"]
          /\ UNCHANGED <<d, opn, fm, qb, aIdx, sbits, sch, page>>
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, skip, cursor,
                   leases, firsts, inflight, tokens, replayed, unleaseBroke,
                   unreadSent, crashes>>

\* ---------------------------------------------------------------------------
\* Sweeps (sweep.rs).

SweepLease(o) ==
    /\ d[o].st = "L" /\ d[o].until <= clock
    /\ RetryOrFail(o, [d[o] EXCEPT !.rel = 0])
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, page, skip, cancel,
                   sbits, opn, sch, aIdx, cursor, outcome, leases, firsts,
                   inflight, tokens, replayed, unleaseBroke, unreadSent, crashes>>

SweepRetry(o) ==
    /\ d[o].st = "R" /\ clock >= d[o].cycle
    /\ FailRow(o, d[o])
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, page, skip, cancel,
                   sbits, opn, sch, aIdx, cursor, outcome, leases, firsts,
                   inflight, tokens, replayed, unleaseBroke, unreadSent, crashes>>

\* The scheduled cursor's next entry, untaken past retention, into the gap.
ExpireScheduled ==
    LET S == {o \in aIdx : KeyLess(cursor, <<avail[o], o>>)} IN
    /\ S # {}
    /\ LET o == CHOOSE x \in S : \A p \in S : p = x \/ KeyLess(<<avail[x], x>>, <<avail[p], p>>) IN
       /\ d[o].st = "none" /\ RetentionEnd(o) <= clock
       /\ cursor' = <<avail[o], o>>
       /\ aIdx' = aIdx \ {o} /\ sbits' = sbits \ {o}
       /\ sch' = [sch EXCEPT ![RangeOf(o)] = @ - 1]
       /\ page' = PageAfterEnd(o)
       /\ qm' = qm - 1 /\ qb' = qb - 1
       /\ outcome' = [outcome EXCEPT ![o] = "expired"]
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, skip, cancel, opn,
                   d, fm, leases, firsts, inflight, tokens, replayed,
                   unleaseBroke, unreadSent, crashes>>

ExpireFailure(o) ==
    /\ d[o].st = "F" /\ d[o].fexp <= clock
    /\ EndHeld(o, "ended")
    /\ UNCHANGED <<clock, next, frontier, kind, avail, unread, skip, cancel, cursor,
                   leases, firsts, inflight, tokens, replayed, unleaseBroke,
                   unreadSent, crashes>>

\* ---------------------------------------------------------------------------
\* Part 2: the sealer and the handler's preparation (S3b-DESIGN §6.2).

\* A seal takes the pages from the boundary up to some later offset.
Seal ==
    /\ sealed < next
    /\ \E s \in (sealed + 1)..next : sealed' = s
    /\ UNCHANGED <<delivery, prep, sealedLeased>>

\* A claim's (or a sweep's) handler reads the sealed pages from the stored
\* frontier on, up to the boundary or fewer (its byte budget).
Prepare ==
    /\ \E c \in frontier..(IF sealed > frontier THEN sealed ELSE frontier) :
          prep' = {o \in Offs : frontier <= o /\ o < c}
    /\ UNCHANGED <<delivery, sealed, sealedLeased>>

Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<next, frontier, kind, avail, unread, page, skip, cancel, sbits,
                   opn, sch, aIdx, cursor, d, qm, qb, fm, outcome, leases, firsts,
                   inflight, tokens, replayed, unleaseBroke, unreadSent, crashes,
                   sealed, prep, sealedLeased>>

DeliveryStep ==
    \/ \E u \in BOOLEAN : SendImmediate(u)
    \/ \E at \in 1..MaxClock : SendScheduled(at)
    \/ LanePass
    \/ Claim
    \/ HandOut \/ FailUnreadable \/ UnleaseInternal \/ Crash
    \/ \E o \in Offs : tokens[o] > 0 /\
          LET t == <<o, tokens[o]>> IN
          Ack(t) \/ Fail(t) \/ RetryNow(t) \/ Release(t) \/ Extend(t)
    \/ \E o \in Offs : Discard(o) \/ Cancel(o) \/ SweepLease(o) \/ SweepRetry(o)
                       \/ ExpireFailure(o)
    \/ ExpireScheduled

\* A lane lease served below the boundary came from a prepared page.
SealedLease == \E o \in 0..(sealed - 1) : firsts'[o] > firsts[o] /\ kind[o] = "imm"

Next ==
    \/ /\ DeliveryStep
       /\ UNCHANGED <<sealed, prep>>
       /\ sealedLeased' = (sealedLeased \/ SealedLease)
    \/ Seal
    \/ Prepare
    \/ Tick

Spec == Init /\ [][Next]_vars

\* Bounds the state space where a control lets leases repeat without end.
LeaseBound == \A o \in Offs : leases[o] <= MaxAttempts + UnleaseLimit + 1

\* ---------------------------------------------------------------------------
\* Invariants (S3b-DESIGN §4.2, §11.1).

Unresolved == {o \in 0..(next-1) : outcome[o] = "none"}
FailedSet == {o \in Offs : d[o].st = "F"}

\* Every unresolved message is still deliverable or held, and its body is
\* stored.
NoMessageLost ==
    \A o \in Unresolved :
       /\ page[o]
       /\ \/ Held(o)
          \/ o \in aIdx
          \/ (kind[o] = "imm" /\ o >= frontier)

\* I1: the frontier never passes the next send.
NoSendAtOrBelowFrontier == frontier <= next

ReplayedSettleChangesNothing == ~replayed

\* No lease or failure outlives the message's hard expiry.
NothingOutlivesHardExpiry ==
    \A o \in Offs :
       /\ d[o].st = "L" => d[o].until <= HardExpiry(o)
       /\ d[o].st = "F" => d[o].fexp <= HardExpiry(o)

\* I3: every immediate offset below the frontier was leased, cancelled or
\* expired, and an expired one is past its retention end (the clock only
\* moves on, so it was then too).
FrontierNeverPassesALiveImmediate ==
    \A o \in 0..(frontier-1) :
       kind[o] = "imm" =>
          \/ firsts[o] >= 1
          \/ outcome[o] = "cancelled"
          \/ (outcome[o] = "expired" /\ RetentionEnd(o) <= clock)

\* I4: a cancelled message is never leased, and is leased at most once first.
CancelledNeverLeased ==
    \A o \in Offs :
       /\ firsts[o] <= 1
       /\ outcome[o] = "cancelled" => d[o].st = "none"
       /\ outcome[o] = "cancelled" /\ kind[o] = "imm" /\ firsts[o] = 0 =>
             \/ o \in cancel
             \/ o < frontier

UnleaseKeepsBudgets == ~unleaseBroke

\* I7 (amended, F8).
AccountingExact ==
    /\ qm = Cardinality(Unresolved \ FailedSet)
    /\ qb = qm + Cardinality(cancel)
    /\ fm = Cardinality(FailedSet)

\* I6: the range rows match the delivery rows and the scheduled index.
RangeRowsMatch ==
    /\ \A r \in Ranges :
          /\ opn[r] = Cardinality({o \in InRange(r) : d[o].st # "none"})
          /\ sch[r] = Cardinality(sbits \cap InRange(r))
    /\ sbits = aIdx
    /\ \A o \in skip \cup cancel : o >= frontier

\* I9 as a bound: a body that never opens is leased at most MaxAttempts +
\* UnleaseLimit times before it is failed.
PoisonBounded ==
    \A o \in Offs : unread[o] => leases[o] <= MaxAttempts + UnleaseLimit

\* ---------------------------------------------------------------------------
\* Witnesses: each must be violated, showing the behaviour is reachable.

Witness_UnreadableFailed == ~\E o \in Offs : unread[o] /\ d[o].st = "F"
Witness_UnleasedThenFailed == ~\E o \in Offs : d[o].st = "F" /\ d[o].iu >= UnleaseLimit
Witness_CancelledPassed == ~\E o \in Offs : outcome[o] = "cancelled" /\ kind[o] = "imm"
                                            /\ o < frontier /\ firsts[o] = 0
Witness_ExpiredIntoGap == ~\E o \in Offs : outcome[o] = "expired" /\ kind[o] = "imm"
Witness_ScheduledLeasedAndPassed ==
    ~\E o \in Offs : kind[o] = "sched" /\ d[o].st = "L" /\ o < frontier
Witness_StaleReceiptHeld == ~\E o \in Offs : tokens[o] > 0 /\ d[o].st = "L" /\ d[o].gen # tokens[o]
Witness_SealedPageLeased == ~sealedLeased

\* The premise of the checks' deadlock_check: false (README): below
\* MaxClock time can always move, through `Tick`, a bare disjunct of `Next`.
\* A guard added to `Tick` breaks it.
TickEnabled == clock < MaxClock => ENABLED Tick
=============================================================================
