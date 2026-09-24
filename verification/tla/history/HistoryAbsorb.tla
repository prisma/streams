---------------------------- MODULE HistoryAbsorb ----------------------------
(***************************************************************************)
(* TLA-016 -- history absorption, publication and safe hot-data trimming.   *)
(*                                                                         *)
(* One stream incarnation with N record offsets.  Three independently      *)
(* durable stores/steps are kept separate:                                 *)
(*                                                                         *)
(*   * the shard DB tail row, as four views: the committer's APPLIED       *)
(*     overlay base A, the queue of applied-but-not-yet-durable commit     *)
(*     groups `pend` (SlateDB WAL order), the remotely DURABLE prefix D,   *)
(*     and the PUBLISHED handle state P (handle.state.durable, written by  *)
(*     dispatch_durable);                                                  *)
(*   * the shared history-v2 partition (WAL disabled), canonical rows hc   *)
(*     and postings rows hp kept separately, each "none" | "mem" | "dur";  *)
(*   * the absorber's volatile gather state, lane mark [from, replay],     *)
(*     the submissions whose receipts it has not settled, and the          *)
(*     committer channel carrying AbsorbedBatch messages.                  *)
(*                                                                         *)
(* Optionally (ModelCache) the engine's decoded postings-slice cache is    *)
(* modelled at the level of the COVERAGE CLAIMS it makes: the write-through *)
(* install the gather performs after its flush (PostingsCache::            *)
(* install_chunk), weight eviction, and a reader-side cold load.  Its      *)
(* safety property is that the cache never proves the absence of a record *)
(* that history holds below the durable absorbed boundary.                 *)
(*                                                                         *)
(* Optionally (Pages) the postings pages are modelled per routing key as  *)
(* <<key, first offset, offsets>>, stored under (key, first offset) like   *)
(* the real page key (ASM-HISTORY-PAGES), so a re-gather whose pages       *)
(* overlap an earlier chunk's is visible (NoStraddledChunk), and so is     *)
(* whether a reader admits every key's pages (PagesAdmit: append_page_runs *)
(* / keep_past admit an overlapping page only when it agrees over the     *)
(* common span).                                                           *)
(*                                                                         *)
(* Crash erases every volatile thing (applied groups, channel, marks,      *)
(* submissions, partition memtable) and preserves the durable prefix D and *)
(* durable history rows.  Faults: failed/ambiguous history flush, a        *)
(* refused commit group (the absorber is told through the dropped batch    *)
(* receipt), a dropped Absorbed op in a group that lands (the absorber is  *)
(* not told), crash, stale/duplicate publisher (the dirty-index rescan's   *)
(* roll_back_stranded_mark, or a mark pruned for an evicted handle, lets a *)
(* gather re-plan from a stale published boundary), trims between a plan   *)
(* and its scan, per-stream-cap gathers, gathers served by the durable     *)
(* tail ring, exhausted global trim budget.  Every record is one byte, so  *)
(* the tail's exact unabsorbed_bytes ledger `ub` must equal next - abs.   *)
(*                                                                         *)
(* The action -> production-function mapping and the atomicity table are  *)
(* in README.md.  Mutation points used by the negative controls are the    *)
(* operators SubmitReady, PostingsWrite, AdvanceTrimTarget,                *)
(* TickTrimTarget, SafeRaisedOnDuplicate, RetireBytes, WarmInstallFrom,    *)
(* PlanUpto, SettleRollsBack and ScanView; the MC module overrides them    *)
(* through the cfg (Op <- MutOp).  This module never enables a mutation.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    N,             \* record offsets of the modelled stream: 0..N-1
    InitNext,      \* records already durable at Init
    MaxPend,       \* applied-but-not-durable commit groups the WAL buffer holds
    MaxChan,       \* AbsorbedBatch messages in flight on the committer channel
    MaxCrashes,    \* process crash/restart bound
    MaxFlushFail,  \* failed (possibly ambiguous) history flush bound
    MaxRejects,    \* refused groups plus dropped ops carrying an advance (shared bound)
    MaxDrops,      \* of those, ops stage() drops alone in a group that lands
    TrimBudgets,   \* per-op trim allowances the shared global budget can leave
    MaxReads,      \* reader snapshot bound
    Cap,           \* per-stream gather cap, in (equal-size) records per chunk
    GatherRing,    \* the durable tail ring may serve a gather window
    ModelCache,    \* model the postings warm-install cache claims
    Keys,          \* routing keys (cache model)
    KeyOf,         \* [0..N-1 -> Keys] (cache model)
    MaxEvict,      \* cache weight evictions (cache model)
    Pages,         \* model the postings page ranges chunks write (NoStraddledChunk)
    Rescan,        \* the dirty-index rescan observes rows and rolls marks back
    Prune          \* the sweep prunes lane marks (prune_lane_marks)

ASSUME /\ N \in Nat \ {0}
       /\ InitNext \in 0..N
       /\ MaxPend \in Nat \ {0}
       /\ MaxChan \in Nat \ {0}
       /\ MaxCrashes \in Nat /\ MaxFlushFail \in Nat /\ MaxRejects \in Nat
       /\ MaxDrops \in Nat
       /\ TrimBudgets \subseteq 0..N /\ \E b \in TrimBudgets : b > 0
       /\ MaxReads \in Nat
       /\ Cap \in Nat \ {0}
       /\ GatherRing \in BOOLEAN /\ ModelCache \in BOOLEAN
       /\ KeyOf \in [0..(N-1) -> Keys]
       /\ MaxEvict \in Nat
       /\ Pages \in BOOLEAN /\ Rescan \in BOOLEAN /\ Prune \in BOOLEAN

Offs == 0..(N-1)
Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x
SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y

(* Tail row fields (src/shard.rs TailFields): next, absorbed, trim_safe_to, *)
(* trimmed, unabsorbed_bytes (ub).  gprev is a GHOST: the absorbed value    *)
(* immediately before the most recent advancing Absorbed op (H4 anchor).    *)
TailT == [next : 0..N, abs : 0..N, safe : 0..N, trimmed : 0..N, gprev : 0..N,
          ub : 0..N]
InitTail == [next |-> InitNext, abs |-> 0, safe |-> 0, trimmed |-> 0, gprev |-> 0,
             ub |-> InitNext]
NoObs == N + 1
Rows == {"none", "mem", "dur"}
NoSnap == [set |-> FALSE, trimmed |-> 0, next |-> 0]
IdleAb == [ph |-> "idle", from |-> 0, upto |-> 0, cur |-> 0, chunk |-> {}, ring |-> FALSE,
           snap |-> NoSnap]
IdleRd == [ph |-> "idle", B |-> 0]
(* Postings-slice cache (src/postings_cache.rs): one slice per key of this  *)
(* segment (covered_from cf, indexed_to_offset it, runs as offset sets) and *)
(* the segment's write-through warm record (SegWarm: from, to, clean).      *)
NoSlice == [has |-> FALSE, cf |-> 0, it |-> 0, runs |-> {}]
InitSl == [k \in Keys |-> NoSlice]
InitWm == [set |-> FALSE, from |-> 0, to |-> 0, clean |-> TRUE]
(* The v2 lane mark (gather.rs LaneMark): where the stream's next chunk    *)
(* starts, and replay > 0 (replay_to) caps it at a refused chunk's end.    *)
(* NoMark is "no entry": a mark only matters through from and replay, and  *)
(* Lane::replay never matches it because every chunk ends above 0.         *)
NoMark == [from |-> 0, replay |-> 0]
SubSt == {"pending", "refused"}
IsPending(e) == e.st = "pending"
IsRefused(e) == e.st = "refused"

VARIABLES
    A,          \* applied tail: the committer's StreamOverlay base (state.applied)
    pend,       \* applied, not yet remotely durable groups (tail after each group)
    D,          \* remotely durable tail (WAL durable prefix; Remote reads)
    P,          \* published tail: handle.state.durable (dispatch_durable)
    hc,         \* history canonical row per offset
    hp,         \* history postings coverage per offset
    ab,         \* absorber gather in progress
    mark,       \* Absorber lane: the stream's v2 LaneMark [from, replay] (NoMark = none)
    subs,       \* Absorber lane: submitted chunks [f, u, st] pending or refused-unsettled, oldest first
    obs,        \* dirty-index rescan: absorbed read from the row (NoObs = none)
    chan,       \* committer channel: AbsorbedBatch [f |-> from, u |-> upto, b |-> bytes]
    rd,         \* reader holding a published absorbed snapshot
    sl,         \* postings-slice cache: per key slice (cache model)
    wm,         \* postings-slice cache: the segment's warm record (cache model)
    pgMem,      \* pages <<key, first offset, offsets>> in the partition memtable (Pages)
    pgDur,      \* pages durable in the partition (Pages)
    crashes, flushFails, rejects, drops, reads, evicts,
    crashLoss,  \* ghost: a crash destroyed in-flight absorption work
    rejectLoss, \* ghost: a committer operation carrying an advance was refused/dropped
    dupSeen,    \* ghost: a non-advancing (stale/duplicate) Absorbed op arrived
    diverged,   \* ghost: the committer refused a group (accounting diverged)
    prunedInFlight, \* ghost: a mark was pruned while its AbsorbedBatch was queued
    misStart,   \* ghost: an advance whose chunk start was not the boundary applied
    missingRow, \* ghost: the committer found a retired range's row missing
    lateInstall, \* ghost: a warm install named a range starting above plan.from
    late,       \* ghost: refused chunks settled while the mark no longer rested on them
    rolledBack, \* ghost: settling a refused chunk rolled the mark back to replay it
    lateRecount, \* ghost: an advance started above the boundary behind a late refusal
    overInFlight \* ghost: a mis-started advance moved over more than Cap * (MaxChan + 1)

vars == <<A, pend, D, P, hc, hp, ab, mark, subs, obs, chan, rd, sl, wm, pgMem, pgDur,
          crashes, flushFails, rejects, drops, reads, evicts,
          crashLoss, rejectLoss, dupSeen, diverged, prunedInFlight, misStart, missingRow,
          lateInstall, late, rolledBack, lateRecount, overInFlight>>
cacheVars == <<sl, wm, evicts>>
pageVars == <<pgMem, pgDur>>
recountVars == <<late, rolledBack, lateRecount, overInFlight>>
ghostVars == <<crashLoss, rejectLoss, dupSeen, diverged, prunedInFlight, misStart, missingRow,
               lateInstall, recountVars>>

\* The committer's scan of the stored record rows of [lo, hi): every row at
\* or above the applied trim point and below the applied end is present.
Unreadable == N + 1
StoredFrameBytes(lo, hi) ==
    IF \A o \in lo..(hi - 1) : o >= A.trimmed /\ o < A.next THEN hi - lo ELSE Unreadable

-----------------------------------------------------------------------------
(* Mutation points (baseline definitions).                                 *)

\* The gather publishes (submit_absorbed_batch_v2) only after part.flush().
SubmitReady(a) == a.ph = "flushed"
\* stage_postings writes the postings pages into the SAME WriteBatch.
WriteRows(h, S) == [o \in Offs |-> IF o \in S /\ h[o] = "none" THEN "mem" ELSE h[o]]
PostingsWrite(h, S) == WriteRows(h, S)
\* CommitTransaction::absorbed trims toward trim_safe_to (previous boundary).
AdvanceTrimTarget(safe, newAbs) == safe
\* CommitTransaction::trim targets trim_safe_to.min(absorbed).
TickTrimTarget(t) == Min(t.safe, t.abs)
\* A non-advancing Absorbed op changes nothing (the `upto > prev_absorbed` guard).
SafeRaisedOnDuplicate == FALSE
\* CommitTransaction::absorbed retires the bytes of the range it moves the
\* boundary over, [prev_absorbed, upto): the chunk's own byte count when
\* the chunk starts at the applied boundary, otherwise stored_frame_bytes,
\* the stored record rows of [prev_absorbed, min(upto, next)) at the
\* committer's read level (Unreadable when a row is missing, which refuses
\* the group).
RetireBytes(m, prev, newAbs) == IF m.f = prev THEN m.b ELSE StoredFrameBytes(prev, newAbs)
\* The start of the range the gather names for PostingsCache::install_chunk
\* after its flush (TLA-016-F3 fix): stage_rows returns the offsets it
\* staged, from the first staged frame to last + 1, so the install claims
\* only rows the scan read.  The staged rows are dense (one Remote scan
\* snapshot, or a ring window with a density proof) but start above
\* plan.from when the scan skipped a head a trim deleted after the plan.
WarmInstallFrom(a) == SetMin(a.chunk)
\* Absorber::plan_read's read end: while a rolled-back mark's replay_to lies
\* above the planned start, the read stops at it, so the replay re-reads
\* exactly the refused chunk; otherwise it reads to the durable end.
PlanUpto(m, from, next) == IF m.replay > from THEN Min(m.replay, next) ELSE next
\* Absorber::settle_submissions rolls a mark that still rests on a refused
\* chunk's end back to replay that chunk (Lane::replay).
SettleRollsBack == TRUE
\* The Remote scan's view for its next row: the snapshot of D taken when it
\* was created (its first row), ASM-SLATEDB-DURABLE (j).
ScanView(a) == IF a.snap.set THEN a.snap
               ELSE [set |-> TRUE, trimmed |-> D.trimmed, next |-> D.next]

-----------------------------------------------------------------------------
FlushAll(h) == [o \in Offs |-> IF h[o] = "mem" THEN "dur" ELSE h[o]]
LoseMem(h) == [o \in Offs |-> IF h[o] = "mem" THEN "none" ELSE h[o]]
\* The pages a staged chunk writes (stage_postings): one per routing key it
\* carries, keyed by (key, first offset), listing the key's staged offsets.
\* Page contents and buckets are abstracted to the offset set.
ChunkPages(c) ==
    {<<k, SetMin({o \in c : KeyOf[o] = k}), {o \in c : KeyOf[o] = k}>> :
        k \in {KeyOf[o] : o \in c}}
SameSlot(p, q) == p[1] = q[1] /\ p[2] = q[2]
\* A write under an existing (key, first offset) replaces that page.
Overwrite(S, new) == {p \in S : ~\E q \in new : SameSlot(p, q)} \cup new
\* A flush makes the partition memtable durable: rows and pages alike.
FlushPages == /\ pgDur' = Overwrite(pgDur, pgMem)
              /\ pgMem' = {}
\* What a reader of the partition sees: the memtable over the durable pages.
EffPages == Overwrite(pgDur, pgMem)
TrimDebt(t) == t.trimmed < TickTrimTarget(t)
TailStates == {A, D, P} \cup {pend[i] : i \in 1..Len(pend)}
\* A handle referenced by nobody but the map may be evicted: no applied group
\* is waiting for durability or dispatch (committer batches and dispatch hold
\* handle clones, src/shard.rs evict_idle_handles).  A queued AbsorbedBatch
\* holds none.
Evictable == pend = <<>> /\ P = D
\* The oldest unanswered submission: the one whose batch heads the channel.
FirstPending ==
    CHOOSE i \in 1..Len(subs) : subs[i].st = "pending" /\
                                \A j \in 1..(i - 1) : subs[j].st # "pending"
\* The committer answers (or drops) the receipt of the batch it handles.
\* Settling a landed receipt only removes it and never moves a mark, so the
\* model removes it when it is answered (a stuttering-equivalent reduction);
\* a refused one stays until the absorber settles it.
Answer(st) == subs' = IF st = "landed"
                        THEN [i \in 1..(Len(subs) - 1) |->
                                IF i < FirstPending THEN subs[i] ELSE subs[i + 1]]
                        ELSE [subs EXCEPT ![FirstPending].st = st]

Init ==
    /\ A = InitTail /\ D = InitTail /\ P = InitTail
    /\ pend = <<>>
    /\ hc = [o \in Offs |-> "none"] /\ hp = [o \in Offs |-> "none"]
    /\ ab = IdleAb /\ mark = NoMark /\ subs = <<>> /\ obs = NoObs /\ chan = <<>> /\ rd = IdleRd
    /\ sl = InitSl /\ wm = InitWm /\ pgMem = {} /\ pgDur = {}
    /\ crashes = 0 /\ flushFails = 0 /\ rejects = 0 /\ drops = 0 /\ reads = 0 /\ evicts = 0
    /\ crashLoss = FALSE /\ rejectLoss = FALSE /\ dupSeen = FALSE
    /\ diverged = FALSE /\ prunedInFlight = FALSE /\ misStart = FALSE /\ missingRow = FALSE
    /\ lateInstall = FALSE
    /\ late = 0 /\ rolledBack = FALSE /\ lateRecount = FALSE
    /\ overInFlight = FALSE

(* A new applied commit group: one WriteBatch applied to the memtable and   *)
(* queued for the WAL (CommitTransaction::write + publish).                 *)
NewGroup(T) == /\ Len(pend) < MaxPend
               /\ A' = T
               /\ pend' = Append(pend, T)

-----------------------------------------------------------------------------
(* Postings-slice cache claims (ModelCache).                                *)

\* PostingsCache::install_chunk(inc, chunk_from, chunk_to, per_key): the
\* absorber hands over the runs it encoded from the frames it actually read
\* (stage_postings), for the range [cf0, ct) it names.  A gap in the warm
\* window (w.to # chunk_from) restarts it clean; a fresh slice claims from
\* the warm base only while that base is 0, else from chunk_from; a resident
\* slice extends if adjacent or bridged by a clean warm window.  The window
\* [from, to) is the absence proof for bridges, so an install that drops a
\* key's runs (a resident slice it cannot bridge) raises `from` past its
\* chunk.  The admission line, which also drops fresh installs, is not
\* modelled: every fresh install is admitted.
InstallChunk(cf0, ct, chunk) ==
    LET gap    == ~wm.set \/ wm.to # cf0
        wFrom  == IF gap THEN cf0 ELSE wm.from
        wClean == IF gap THEN TRUE ELSE wm.clean
        fresh  == IF wClean /\ wFrom = 0 THEN 0 ELSE cf0
        runsK(k) == {o \in chunk : KeyOf[o] = k}
        bridgeable(k) == sl[k].it >= cf0 \/ (wClean /\ sl[k].it >= wFrom)
        dropped == \E k \in Keys : runsK(k) # {} /\ sl[k].has /\ sl[k].it < ct
                                    /\ ~bridgeable(k)
    IN /\ wm' = [set |-> TRUE, from |-> IF dropped THEN ct ELSE wFrom, to |-> ct,
                 clean |-> wClean]
       /\ sl' = [k \in Keys |->
                   IF runsK(k) = {} THEN sl[k]
                   ELSE IF sl[k].has
                     THEN IF sl[k].it >= ct THEN sl[k]
                          ELSE IF bridgeable(k)
                            THEN [sl[k] EXCEPT !.it = ct,
                                    !.runs = {o \in sl[k].runs : o < sl[k].it}
                                             \cup {o \in runsK(k) : o >= sl[k].it}]
                            ELSE sl[k]
                     ELSE [has |-> TRUE, cf |-> fresh, it |-> ct, runs |-> runsK(k)]]

\* runs_for's demand-side bridge: a slice ending inside a clean warm window
\* is treated as covered up to the window's end.
EffTo(k) == IF wm.set /\ wm.clean /\ wm.from <= sl[k].it /\ wm.to > sl[k].it
              THEN wm.to ELSE sl[k].it

CacheEvict ==      \* weight eviction of one slice; taints the warm window
    /\ ModelCache /\ evicts < MaxEvict
    /\ \E k \in Keys :
         /\ sl[k].has
         /\ sl' = [sl EXCEPT ![k] = NoSlice]
         /\ wm' = [wm EXCEPT !.clean = FALSE]
    /\ evicts' = evicts + 1
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, subs, obs, chan, rd, pageVars, crashes,
                   flushFails, rejects, drops, reads, ghostVars>>

CacheLoad ==       \* runs_for Lead: cold store load of the key's postings pages
    /\ ModelCache
    /\ \E k \in Keys, to \in {P.abs, D.abs} :
         /\ ~sl[k].has /\ to > 0
         /\ sl' = [sl EXCEPT ![k] = [has |-> TRUE, cf |-> 0, it |-> to,
                     runs |-> {o \in Offs : o < to /\ KeyOf[o] = k /\ hp[o] # "none"}]]
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, subs, obs, chan, rd, wm, pageVars, crashes,
                   flushFails, rejects, drops, reads, evicts, ghostVars>>

-----------------------------------------------------------------------------
(* Shard log / committer / durability pipeline                             *)

CustomerAppend ==
    /\ A.next < N
    /\ NewGroup([A EXCEPT !.next = A.next + 1, !.ub = A.ub + 1])
    /\ UNCHANGED <<D, P, hc, hp, ab, mark, subs, obs, chan, rd, cacheVars, pageVars, crashes,
                   flushFails, rejects, drops, reads, ghostVars>>

WalDurable ==      \* SlateDB remote WAL flush: durable_seq covers the oldest group
    /\ pend # <<>>
    /\ D' = Head(pend)
    /\ pend' = Tail(pend)
    /\ UNCHANGED <<A, P, hc, hp, ab, mark, subs, obs, chan, rd, cacheVars, pageVars, crashes,
                   flushFails, rejects, drops, reads, ghostVars>>

Dispatch ==        \* ShardEngine::dispatch_durable publishes handle.state.durable
    /\ P # D
    /\ P' = D
    /\ UNCHANGED <<A, pend, D, hc, hp, ab, mark, subs, obs, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

Crash ==           \* process crash/restart or ownership move of the shard engine
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ A' = D /\ P' = D /\ pend' = <<>>
    \* The absorber task ends with the engine: its lane (marks and unsettled
    \* submissions) goes with it.
    /\ chan' = <<>> /\ ab' = IdleAb /\ mark' = NoMark /\ subs' = <<>> /\ obs' = NoObs
    /\ hc' = LoseMem(hc) /\ hp' = LoseMem(hp) /\ pgMem' = {}
    /\ rd' = IdleRd
    \* The postings cache is process-wide: a process crash wipes it, an
    \* engine close or ownership move of this shard leaves it.
    /\ \/ sl' = InitSl /\ wm' = InitWm
       \/ UNCHANGED <<sl, wm>>
    /\ crashLoss' = (crashLoss \/ chan # <<>> \/ A.abs > D.abs \/ ab.ph # "idle")
    /\ UNCHANGED <<D, pgDur, flushFails, rejects, drops, reads, evicts, rejectLoss, dupSeen,
                   diverged, prunedInFlight, misStart, missingRow, lateInstall, recountVars>>

HistoryBackgroundFlush ==   \* memtable flush not requested by the gather (or close)
    /\ (\E o \in Offs : hc[o] = "mem" \/ hp[o] = "mem") \/ pgMem # {}
    /\ hc' = FlushAll(hc) /\ hp' = FlushAll(hp) /\ FlushPages
    /\ UNCHANGED <<A, pend, D, P, ab, mark, subs, obs, chan, rd, cacheVars, crashes, flushFails,
                   rejects, drops, reads, ghostVars>>

-----------------------------------------------------------------------------
(* Absorber: Absorber::settle_submissions / plan_read / read_wave /         *)
(* stage_chunk / commit, and the same task's dirty-index rescan            *)
(* (seed_from_dirty_index) and lane-mark prune.  The absorber is one task: *)
(* settling, the rescan and the prune never run while a gather is in       *)
(* progress.                                                               *)

\* Each pump tick settles answered receipts right before it gathers
\* (worker.rs), one gather per tick, so a plan never sees a receipt that was
\* answered before the tick's settle.  A receipt answered after the settle
\* commutes with the plan (the plan reads neither receipts nor the channel,
\* the committer reads neither the gather nor the lane), so requiring every
\* answered receipt to be settled before a plan loses no behaviour.
AbsorberPlan ==
    /\ ab.ph = "idle" /\ obs = NoObs
    /\ \A i \in 1..Len(subs) : subs[i].st = "pending"
    /\ LET from == Max(mark.from, P.abs)
           upto == PlanUpto(mark, from, P.next)
       IN
         /\ from < upto
         /\ \E ring \in (IF GatherRing THEN BOOLEAN ELSE {FALSE}) :
              ab' = [ph |-> "reading", from |-> from, upto |-> upto,
                     cur |-> from, chunk |-> {}, ring |-> ring, snap |-> NoSnap]
    /\ UNCHANGED <<A, pend, D, P, hc, hp, mark, subs, obs, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

\* read_frames_range: a ring hit returns the window densely (the ring keeps
\* durable copies of rows a trim has since deleted); otherwise one
\* Remote-durable scan, observed one row per step.  The scan reads the
\* snapshot of D taken when it is created (its first row), so it skips the
\* rows trimmed by then and none that a later trim deletes.
AbsorberRead ==
    /\ ab.ph = "reading"
    /\ ab.cur < ab.upto
    /\ Cardinality(ab.chunk) < Cap
    /\ LET sn == ScanView(ab)
       IN ab' = [ab EXCEPT !.cur = ab.cur + 1, !.snap = sn,
                           !.chunk = IF ab.ring \/ (ab.cur >= sn.trimmed /\ ab.cur < sn.next)
                                       THEN ab.chunk \cup {ab.cur} ELSE ab.chunk]
    /\ UNCHANGED <<A, pend, D, P, hc, hp, mark, subs, obs, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

AbsorberReadEnd == \* range exhausted, or the per-stream byte cap is reached
    /\ ab.ph = "reading"
    /\ ab.cur = ab.upto \/ Cardinality(ab.chunk) = Cap
    /\ ab' = [ab EXCEPT !.ph = "readdone"]
    /\ UNCHANGED <<A, pend, D, P, hc, hp, mark, subs, obs, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

AbsorberStage ==   \* stage_chunk: stage_rows + stage_postings into one WriteBatch
    /\ ab.ph = "readdone"
    /\ IF ab.chunk = {}
         THEN /\ ab' = IdleAb                       \* no_work
              /\ UNCHANGED <<hc, hp, pgMem>>
         ELSE /\ hc' = WriteRows(hc, ab.chunk)
              /\ hp' = PostingsWrite(hp, ab.chunk)
              /\ pgMem' = IF Pages THEN Overwrite(pgMem, ChunkPages(ab.chunk)) ELSE pgMem
              /\ ab' = [ab EXCEPT !.ph = "staged"]
    /\ UNCHANGED <<A, pend, D, P, mark, subs, obs, chan, rd, cacheVars, pgDur, crashes,
                   flushFails, rejects, drops, reads, ghostVars>>

AbsorberFlushOk == \* part.flush() returned Ok: the whole memtable is durable
    /\ ab.ph = "staged"
    /\ hc' = FlushAll(hc) /\ hp' = FlushAll(hp) /\ FlushPages
    /\ ab' = [ab EXCEPT !.ph = "flushed"]
    /\ UNCHANGED <<A, pend, D, P, mark, subs, obs, chan, rd, cacheVars, crashes, flushFails,
                   rejects, drops, reads, ghostVars>>

AbsorberFlushFail ==  \* part.flush() returned Err (outcome may be ambiguous)
    /\ ab.ph = "staged"
    /\ flushFails < MaxFlushFail
    /\ flushFails' = flushFails + 1
    /\ ab' = IdleAb
    /\ \/ UNCHANGED <<hc, hp, pageVars>>
       \/ /\ hc' = FlushAll(hc) /\ hp' = FlushAll(hp) /\ FlushPages
    /\ UNCHANGED <<A, pend, D, P, mark, subs, obs, chan, rd, cacheVars, crashes, rejects, drops,
                   reads, ghostVars>>

\* Absorber::commit after the flush: install_chunk (warm the slice cache
\* over [WarmInstallFrom, last + 1) with the staged runs), then
\* submit_absorbed_batch_v2 with (hash, plan.from, last + 1, chunk bytes),
\* which returns the batch's receipt, then raise_lane_marks: the mark rises
\* to the chunk's end, a replay end it has passed is cleared, and the
\* submission is recorded with its receipt.  No await separates the install
\* from the send, nor the send's return from the raise.
AbsorberSubmit ==
    /\ SubmitReady(ab)
    /\ Len(chan) < MaxChan
    /\ LET u  == SetMax(ab.chunk) + 1
           cf == WarmInstallFrom(ab)
           nf == Max(mark.from, u)
       IN
         /\ chan' = Append(chan, [f |-> ab.from, u |-> u, b |-> Cardinality(ab.chunk)])
         /\ mark' = [from |-> nf, replay |-> IF mark.replay > nf THEN mark.replay ELSE 0]
         /\ subs' = Append(subs, [f |-> ab.from, u |-> u, st |-> "pending"])
         /\ IF ModelCache THEN InstallChunk(cf, u, ab.chunk) ELSE UNCHANGED <<sl, wm>>
         /\ lateInstall' = (lateInstall \/ (ModelCache /\ cf > ab.from))
    /\ ab' = IdleAb
    /\ UNCHANGED <<A, pend, D, P, hc, hp, obs, rd, pageVars, crashes, flushFails, rejects,
                   drops, reads, evicts, crashLoss, rejectLoss, dupSeen, diverged,
                   prunedInFlight, misStart, missingRow, recountVars>>

\* Absorber::settle_submissions (gather.rs), atomic under the lane mutex and
\* never waiting on the committer: every answered submission leaves the
\* lane (landed ones already left when answered, see Answer); for each
\* refused chunk [f, u), in submission order, a mark that
\* still rests on u rolls back to [from |-> f, replay |-> u] (Lane::replay).
\* A mark a later chunk already raised stays: that chunk's advance recounts
\* the refused one (a late refusal).  Re-pending the stream is outside the
\* model (no roster).
RECURSIVE SettleRefused(_, _)
SettleRefused(r, s) ==
    IF s = <<>> THEN r
    ELSE LET c     == Head(s)
             rests == r.m.from = c.u
             back  == rests /\ SettleRollsBack
         IN SettleRefused([m      |-> IF back THEN [from |-> c.f, replay |-> c.u] ELSE r.m,
                           late   |-> r.late + (IF rests THEN 0 ELSE 1),
                           rolled |-> r.rolled \/ back], Tail(s))

AbsorberSettle ==
    /\ ab.ph = "idle" /\ obs = NoObs
    /\ \E i \in 1..Len(subs) : subs[i].st = "refused"
    /\ LET r == SettleRefused([m |-> mark, late |-> 0, rolled |-> FALSE],
                              SelectSeq(subs, IsRefused))
       IN /\ mark' = r.m
          /\ late' = late + r.late
          /\ rolledBack' = (rolledBack \/ r.rolled)
    /\ subs' = SelectSeq(subs, IsPending)
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, obs, chan, rd, cacheVars, pageVars, crashes,
                   flushFails, rejects, drops, reads, crashLoss, rejectLoss, dupSeen, diverged,
                   prunedInFlight, misStart, missingRow, lateInstall, lateRecount,
                   overInFlight>>

RescanObserve ==   \* scan_dirty_streams_page (Memory read): the dirty row's absorbed
    /\ Rescan
    /\ ab.ph = "idle" /\ obs = NoObs
    /\ A.abs < A.next \/ A.trimmed < A.safe          \* the row exists
    /\ obs' = A.abs
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, subs, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

RescanRollback ==  \* roll_back_stranded_mark(h, absorbed) with the row read above
    /\ obs # NoObs
    /\ mark' = IF mark.from > obs THEN NoMark ELSE mark
    /\ obs' = NoObs
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, subs, chan, rd, cacheVars, pageVars, crashes,
                   flushFails, rejects, drops, reads, ghostVars>>

\* prune_lane_marks (gather.rs, run every sweep tick) keeps a mark only while
\* the stream is pending or its RESIDENT handle's published absorbed trails
\* mark.from.  The model ignores `pending` (prunes more often) and lets an
\* unreferenced handle be evicted: a reloaded handle (stream_handle) reads
\* the Memory-level tail, which then equals D and P because no group is in
\* flight.
MarkPrune ==
    /\ Prune
    /\ ab.ph = "idle" /\ obs = NoObs
    /\ mark # NoMark
    /\ P.abs >= mark.from \/ Evictable
    /\ mark' = NoMark
    /\ prunedInFlight' = (prunedInFlight \/
                          (P.abs < mark.from /\ \E i \in 1..Len(chan) : chan[i].u = mark.from))
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, subs, obs, chan, rd, cacheVars, pageVars, crashes,
                   flushFails, rejects, drops, reads, crashLoss, rejectLoss, dupSeen, diverged,
                   misStart, missingRow, lateInstall, recountVars>>

-----------------------------------------------------------------------------
(* Committer: CommitTransaction::absorbed / ::trim, and the batch receipt  *)
(* (answer_landed on a written or no-write group; dropped by a refusal).   *)

CommitAbsorbed ==
    /\ chan # <<>>
    /\ chan' = Tail(chan)
    /\ LET m      == Head(chan)
           u      == m.u
           prev   == A.abs
           newAbs == Min(u, A.next)
           rb     == RetireBytes(m, prev, newAbs)
       IN IF u > prev
            THEN IF rb = Unreadable \/ rb > A.ub
                   THEN \* a retired row is missing, or
                        \* unabsorbed_bytes.checked_sub(bytes) is None: the
                        \* whole group is refused ("maintenance accounting
                        \* diverged"); nothing is written and the receipt
                        \* is dropped.
                        /\ diverged' = TRUE
                        /\ missingRow' = (missingRow \/ rb = Unreadable)
                        /\ Answer("refused")
                        /\ UNCHANGED <<A, pend, dupSeen, misStart, lateRecount, overInFlight>>
                   ELSE /\ \E allowed \in TrimBudgets :
                             LET safe   == Max(A.safe, prev)
                                 tt     == Min(AdvanceTrimTarget(safe, newAbs),
                                               A.trimmed + allowed)
                             IN NewGroup([A EXCEPT !.abs = newAbs, !.safe = safe,
                                                   !.trimmed = Max(A.trimmed, tt),
                                                   !.gprev = prev,
                                                   !.ub = A.ub - rb])
                        /\ Answer("landed")
                        /\ misStart' = (misStart \/ m.f # prev)
                        /\ overInFlight' = (overInFlight \/
                                            (m.f # prev /\ newAbs - prev > Cap * (MaxChan + 1)))
                        /\ lateRecount' = (lateRecount \/ (m.f > prev /\ late > 0 /\ drops = 0))
                        /\ UNCHANGED <<dupSeen, diverged, missingRow>>
            ELSE /\ dupSeen' = TRUE
                 /\ Answer("landed")
                 /\ UNCHANGED <<diverged, misStart, missingRow, lateRecount, overInFlight>>
                 /\ IF SafeRaisedOnDuplicate /\ A.safe < A.abs
                      THEN \E allowed \in TrimBudgets :
                             LET safe == A.abs
                             IN NewGroup([A EXCEPT !.safe = safe,
                                    !.trimmed = Max(A.trimmed,
                                                    Min(safe, A.trimmed + allowed))])
                      ELSE UNCHANGED <<A, pend>>   \* no-write group
    /\ UNCHANGED <<D, P, hc, hp, ab, mark, obs, rd, cacheVars, pageVars, crashes, flushFails,
                   rejects, drops, reads, crashLoss, rejectLoss, prunedInFlight, lateInstall,
                   late, rolledBack>>

\* The group carrying the advance is refused before it is written, and the
\* receipt is dropped unanswered, so the absorber is told: a closed engine
\* or a failed billing-row pre-read (CommitTransaction::run), another
\* operation's accounting divergence or a failed stored_frame_bytes read
\* (finish), stage_maintenance's divergence, or the test failpoint
\* fail_next_absorbed_group.  A failed group write retires the engine
\* (write_failed -> begin_close), which is the engine-close branch of Crash.
CommitRefuseGroup ==
    /\ chan # <<>>
    /\ rejects < MaxRejects
    /\ chan' = Tail(chan)
    /\ rejects' = rejects + 1
    /\ rejectLoss' = TRUE
    /\ Answer("refused")
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, obs, rd, cacheVars, pageVars, crashes,
                   flushFails, drops, reads, crashLoss, dupSeen, diverged, prunedInFlight,
                   misStart, missingRow, lateInstall, recountVars>>

\* stage() drops just this Absorbed op because stream_handle failed
\* (reject_op's `_ => {}`) while the rest of the group lands: the group's
\* receipt is answered, so the absorber is NOT told, and only the
\* dirty-index rescan (RescanRollback) heals the mark.
CommitDropOne ==
    /\ chan # <<>>
    /\ rejects < MaxRejects /\ drops < MaxDrops
    /\ chan' = Tail(chan)
    /\ rejects' = rejects + 1
    /\ drops' = drops + 1
    /\ rejectLoss' = TRUE
    /\ Answer("landed")
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, obs, rd, cacheVars, pageVars, crashes,
                   flushFails, reads, crashLoss, dupSeen, diverged, prunedInFlight,
                   misStart, missingRow, lateInstall, recountVars>>

TrimStep ==        \* TrimTick expansion: one budgeted CommitOp::TrimStep
    /\ TrimDebt(A)
    /\ \E allowed \in TrimBudgets \ {0} :
         NewGroup([A EXCEPT !.trimmed = Min(TickTrimTarget(A), A.trimmed + allowed)])
    /\ UNCHANGED <<D, P, hc, hp, ab, mark, subs, obs, chan, rd, cacheVars, pageVars, crashes,
                   flushFails, rejects, drops, reads, ghostVars>>

-----------------------------------------------------------------------------
(* Reader holding a published absorbed snapshot (execute_segment's first    *)
(* observation); the full merged read is TLA-018 (ReadCompose).             *)

ReaderSnap ==
    /\ rd.ph = "idle"
    /\ reads < MaxReads
    /\ rd' = [ph |-> "snap", B |-> P.abs]
    /\ reads' = reads + 1
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, subs, obs, chan, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, ghostVars>>

ReaderRelease ==
    /\ rd.ph = "snap"
    /\ rd' = IdleRd
    /\ UNCHANGED <<A, pend, D, P, hc, hp, ab, mark, subs, obs, chan, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

-----------------------------------------------------------------------------
(* Legitimate quiescence: everything appended, absorbed, durable,           *)
(* published, trimmed to the safe target, no volatile work outstanding.     *)
(* Without the prune (Prune = FALSE) a lane mark is never removed, so it   *)
(* may remain at quiescence.                                                *)
Settled ==
    /\ A.next = N /\ pend = <<>> /\ chan = <<>> /\ ab.ph = "idle" /\ obs = NoObs
    /\ A = D /\ P = D /\ D.abs = N /\ ~TrimDebt(D)
    /\ \A o \in Offs : hc[o] # "mem" /\ hp[o] # "mem"
    /\ pgMem = {}
    /\ rd.ph = "idle" /\ subs = <<>>
    /\ Prune => mark = NoMark

Terminated == Settled /\ UNCHANGED vars

Next ==
    \/ CustomerAppend \/ WalDurable \/ Dispatch \/ Crash \/ HistoryBackgroundFlush
    \/ AbsorberSettle \/ AbsorberPlan \/ AbsorberRead \/ AbsorberReadEnd \/ AbsorberStage
    \/ AbsorberFlushOk \/ AbsorberFlushFail \/ AbsorberSubmit
    \/ RescanObserve \/ RescanRollback \/ MarkPrune
    \/ CommitAbsorbed \/ CommitRefuseGroup \/ CommitDropOne \/ TrimStep
    \/ ReaderSnap \/ ReaderRelease
    \/ CacheEvict \/ CacheLoad
    \/ Terminated

Spec == Init /\ [][Next]_vars

(* Liveness ("absorption completes once faults cease").  Faults are bounded *)
(* (crashes, flush failures, refused groups, dropped ops, appends), so they *)
(* cease.  Fairness is on the actors' ATTEMPTS, never on a success outcome: *)
(* the flush attempt (Ok or Err), the committer's handling of a message     *)
(* (commit, refusal or drop), the WAL flusher, dispatch, and the absorber   *)
(* task.  The absorber's settle and gather are strongly fair because each  *)
(* tick settles and then gathers pending streams after at most one         *)
(* dirty-index page (worker.rs), so rescans, which disable both between     *)
(* their two steps, cannot starve them; the committer and trim tick are     *)
(* strongly fair because they are only intermittently enabled while the    *)
(* WAL buffer is full.  No fairness on appends, crashes, prunes, evictions  *)
(* or loads.                                                                *)
AbsorberFlush == AbsorberFlushOk \/ AbsorberFlushFail
CommitStep == CommitAbsorbed \/ CommitRefuseGroup \/ CommitDropOne
LiveSpec ==
    /\ Spec
    /\ WF_vars(WalDurable) /\ WF_vars(Dispatch)
    /\ SF_vars(AbsorberSettle)
    /\ SF_vars(AbsorberPlan) /\ WF_vars(AbsorberRead) /\ WF_vars(AbsorberReadEnd)
    /\ WF_vars(AbsorberStage) /\ WF_vars(AbsorberFlush) /\ SF_vars(AbsorberSubmit)
    /\ SF_vars(CommitStep) /\ SF_vars(TrimStep)
    /\ SF_vars(RescanObserve) /\ WF_vars(RescanRollback)
    /\ WF_vars(ReaderRelease)

-----------------------------------------------------------------------------
(* Safety properties                                                        *)

\* The lane's unanswered submissions are exactly the channel's batches, in
\* order: the committer answers each receipt when it handles its batch.
SubsMatchChan ==
    LET pending == SelectSeq(subs, IsPending)
    IN /\ Len(pending) = Len(chan)
       /\ \A i \in 1..Len(chan) : pending[i].f = chan[i].f /\ pending[i].u = chan[i].u

TypeOK ==
    /\ A \in TailT /\ D \in TailT /\ P \in TailT
    /\ \A i \in 1..Len(pend) : pend[i] \in TailT
    /\ Len(pend) <= MaxPend
    /\ hc \in [Offs -> Rows] /\ hp \in [Offs -> Rows]
    /\ ab.ph \in {"idle", "reading", "readdone", "staged", "flushed"}
    /\ ab.chunk \subseteq Offs /\ ab.ring \in BOOLEAN /\ ab.snap.set \in BOOLEAN
    /\ mark \in [from : 0..N, replay : 0..N] /\ obs \in 0..NoObs
    /\ mark.replay # 0 => mark.replay > mark.from
    /\ Len(chan) <= MaxChan
    /\ \A i \in 1..Len(chan) : chan[i].f \in 0..N /\ chan[i].u \in 1..N /\ chan[i].b \in 1..N
    /\ Len(subs) <= MaxChan + 1
    /\ \A i \in 1..Len(subs) : subs[i].f \in 0..N /\ subs[i].u \in 1..N /\ subs[i].st \in SubSt
    /\ SubsMatchChan
    /\ rd.ph \in {"idle", "snap"} /\ rd.B \in 0..N
    /\ \A k \in Keys : sl[k].cf \in 0..N /\ sl[k].it \in 0..N /\ sl[k].runs \subseteq Offs
    /\ \A pg \in pgMem \cup pgDur : pg[1] \in Keys /\ pg[2] \in Offs /\ pg[3] \subseteq Offs
    /\ drops <= rejects

\* stored_tail() refuses these shapes; every tail view must stay loadable.
StoredTailValid ==
    \A T \in TailStates : T.trimmed <= T.abs /\ T.abs <= T.next /\ T.safe <= T.abs

\* H3: the absorbed boundary (every view, including in-flight applied groups)
\* is backed by DURABLE canonical frames AND postings.
H3_AbsorbedBackedByDurableHistory ==
    \A T \in TailStates : \A o \in Offs :
        o < T.abs => (hc[o] = "dur" /\ hp[o] = "dur")

\* Physical trimming never removes the last recoverable copy: in the
\* Remote-durable view and in the applied (Memory) view, a record below next
\* is either still in the shard log or durable in history (canonical+postings).
LastRecoverableCopy ==
    \A T \in {A, D} : \A o \in Offs :
        o < T.next => (o >= T.trimmed \/ (hc[o] = "dur" /\ hp[o] = "dur"))

\* H4 GUARD CHECK: trim never passes the absorbed boundary as of the
\* PREVIOUS advance.  This restates the committer's guard (safe = max(safe,
\* prev), trims capped at safe); it checks that every path (duplicates,
\* trim ticks, budgets, crashes) keeps the guard, not a reader guarantee.
H4_TrimWithinPreviousBoundary ==
    \A T \in TailStates : T.trimmed <= T.safe /\ T.safe <= T.gprev

\* GUARD CHECK of the documented trim_safe_to contract (src/shard.rs
\* TailFields): a reader whose published snapshot lags the current boundary
\* by AT MOST ONE advance still finds its whole tail range in the shard log.
\* Its antecedent excludes snapshots two or more advances stale, which
\* Witness_StaleSnapshotLosesTail shows are NOT protected (TLA-016-F2); the
\* read path's revalidation (TLA-018) carries that case.
StaleReaderRangeIntact ==
    rd.ph = "snap" =>
        /\ (rd.B >= D.gprev => D.trimmed <= rd.B)
        /\ (rd.B >= A.gprev => A.trimmed <= rd.B)

\* A lane mark never claims more than durable history (it would otherwise
\* let plan_read skip an unabsorbed range forever).
MarkBackedByHistory ==
    \A o \in Offs : o < mark.from => hc[o] = "dur" /\ hp[o] = "dur"

\* The exact per-stream unabsorbed_bytes ledger equals the stored frame bytes
\* in [absorbed, next) (src/shard.rs TailFields doc) in every tail view.
\* Repeated/overlapping absorption must not retire bytes twice.
LedgerExact == \A T \in TailStates : T.ub = T.next - T.abs

\* The committer never finds a row of the range it retires missing: trims
\* stay below the boundary, so stored_frame_bytes refuses a group only on a
\* failed read (a transient error, which CommitRefuseGroup covers), never
\* forever.
NoMissingRetiredRow == ~missingRow

\* The postings-slice cache never proves the absence of a record of the
\* key that history holds below the durable absorbed boundary (a keyed read
\* served from such a slice would skip that record and report completion).
CacheNeverProvesFalseAbsence ==
    \A k \in Keys : sl[k].has =>
        \A o \in Offs :
            (/\ sl[k].cf <= o /\ o < EffTo(k)
             /\ o < D.abs /\ KeyOf[o] = k) => o \in sl[k].runs

\* What the refusal rollback establishes: once every submission is settled,
\* the lane mark rests at or below the applied boundary, so the next chunk
\* starts at the boundary and its advance recounts nothing, unless a
\* refusal was settled late (a later chunk of the stream had already
\* raised the mark) or the committer dropped an op without telling the
\* absorber.  Before the fix a refused chunk's mark stayed raised.
SettledMarkAtBoundary ==
    (subs = <<>> /\ late = 0 /\ drops = 0) => mark.from <= A.abs

\* The recount bound the fix's comments state: a mis-started advance
\* (stored_frame_bytes over [prev_absorbed, upto)) covers at most the chunks
\* in flight at a refusal, MaxChan queued batches and the gather in progress,
\* Cap records each.  Witness_RecountBeyondInFlight shows it does not hold
\* when refusals are settled late one after another (TLA-016-F4).
RecountWithinInFlight == ~overInFlight

\* Two pages of one key overlap: one starts inside the other's span (from
\* its first offset to its last offset + 1).  A chunk replayed from the same
\* row rewrites the same pages in place instead.
Straddled ==
    \E p, q \in EffPages : p[1] = q[1] /\ p[2] < q[2] /\ q[2] < SetMax(p[3]) + 1
\* The refusal rollback's replay never leaves overlapping pages: it
\* re-gathers exactly the refused chunk (the replay_to cap).
NoStraddledChunk == ~Straddled

\* The reader's cold index load (append_page_runs with keep_past, d16559b3)
\* over one key's pages in key order: a page that starts below the
\* accumulated end is admitted only if it lists exactly the accumulated
\* offsets over the common span [first, min(end, its end)), and only its
\* part past the end is kept; any disagreement refuses the whole index
\* (POSTINGS_CORRUPT, the envelope scan).
RECURSIVE Admit(_, _)
Admit(acc, S) ==
    IF S = {} THEN acc
    ELSE LET p      == CHOOSE q \in S : \A r \in S : q[2] <= r[2]
             last   == SetMax(p[3]) + 1
             common == Min(acc.end, last)
             agree  == {o \in acc.offs : p[2] <= o /\ o < common} = {o \in p[3] : o < common}
         IN Admit([ok   |-> acc.ok /\ agree,
                   offs |-> acc.offs \cup {o \in p[3] : o >= acc.end},
                   end  |-> Max(acc.end, last)], S \ {p})
IndexAdmits(k) == Admit([ok |-> TRUE, offs |-> {}, end |-> 0], {p \in EffPages : p[1] = k}).ok
\* Every key's pages admit as one index, overlapping ones included: pages
\* of different chunks describe the same immutable rows, and each chunk's
\* staged rows are dense, so overlapping pages agree over their common span.
PagesAdmit == \A k \in Keys : IndexAdmits(k)

FrontiersMonotone ==
    [][ /\ D'.next >= D.next /\ D'.abs >= D.abs /\ D'.trimmed >= D.trimmed
        /\ P'.next >= P.next /\ P'.abs >= P.abs /\ P'.trimmed >= P.trimmed ]_vars

\* Liveness: once faults cease, every appended record is absorbed, the
\* advance is durable and published.
AbsorptionCompletes == <>[](A.abs = A.next /\ A = D /\ P = D)

-----------------------------------------------------------------------------
(* Reachability witnesses: each is EXPECTED to be violated on the           *)
(* unmodified model (the named behaviour is reachable).                     *)

Witness_TrimDurable == ~(D.trimmed > 0)
Witness_FullyAbsorbedDurable == ~(D.abs = N)
Witness_LostPublicationRecovered == ~(rejectLoss /\ D.abs = N /\ D.trimmed > 0)
Witness_FlushFailRecovered == ~(flushFails > 0 /\ D.abs = N)
Witness_StaleDuplicateIgnored == ~(dupSeen /\ D.abs = N)
Witness_CrashRecovered == ~(crashLoss /\ D.abs = N)
Witness_GatherOverTrimmedPrefix ==
    ~(ab.ph = "readdone" /\ ab.chunk # {} /\ SetMin(ab.chunk) > ab.from)
Witness_StaleSnapshotLosesTail == ~(rd.ph = "snap" /\ D.trimmed > rd.B)
Witness_TwoAdvancesNotDurable ==
    ~(\E i, j \in 1..Len(pend) : i < j /\ pend[i].abs > D.abs /\ pend[j].abs > pend[i].abs)
Witness_MarkRolledBackInFlight ==
    ~(obs # NoObs /\ mark.from > obs /\ \E i \in 1..Len(chan) : chan[i].u = mark.from)
Witness_EvictedMarkPrunedInFlight == ~prunedInFlight
Witness_RingGatherBelowTrim ==
    ~(ab.ph = "readdone" /\ ab.ring /\ \E o \in ab.chunk : o < D.trimmed)
Witness_WarmBridgeCovers ==
    ~(\E k \in Keys : sl[k].has /\ EffTo(k) > sl[k].it /\ EffTo(k) <= D.abs)
\* An advance whose chunk did not start at the applied boundary retired the
\* stored bytes of the range it moved over, and absorption then completed.
Witness_MisStartedAdvanceCompletes == ~(misStart /\ D.abs = N)
\* A re-gather whose scan skipped a trimmed head warms the cache over the
\* rows it staged, a range starting above plan.from (the F3 fix path).
Witness_InstallStartsAbovePlan == ~lateInstall
\* A refused chunk is rolled back and replayed, every advance starts at the
\* boundary (no recount), and absorption completes.
Witness_RefusalReplayed == ~(rolledBack /\ ~misStart /\ D.abs = N)
\* A refusal settled after the stream's next chunk was planned leaves the
\* mark raised; that chunk's advance starts above the boundary and recounts
\* the refused chunk with its own.
Witness_LateRefusalRecount == ~lateRecount
\* A mis-started advance recounts more chunks than were in flight at any
\* one refusal: consecutive late refusals leave consecutive holes
\* (TLA-016-F4).
Witness_RecountBeyondInFlight == ~overInFlight
\* A re-gather from a stale boundary leaves pages of one key that overlap,
\* the reader admits them as one index, and absorption completes.
Witness_OverlapAdmitted == ~(Straddled /\ PagesAdmit /\ D.abs = N)
=============================================================================
