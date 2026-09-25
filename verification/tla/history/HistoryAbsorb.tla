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
(*   * the absorber's volatile state: the gather in progress, the stream's *)
(*     v2 lane mark, its entry in the pending roster (`due`), the          *)
(*     committer channel carrying AbsorbedBatch messages, and the          *)
(*     settlement receipts of the advances it submitted (Submissions).     *)
(*                                                                         *)
(* Receipts (src/shard/commit_plan.rs Submissions / SubmitReceipt).  Every *)
(* submitted advance is counted in its stream's bucket before it is sent   *)
(* and settles when its receipt drops.  The model tracks the receipts of   *)
(* this stream: one per channel entry; a `held` flag on each applied group *)
(* that retired an Exact advance (the group keeps the receipt in its       *)
(* DurableEffects); and `durHeld`, the receipts of groups that are remote- *)
(* durable but not yet dispatched.  Every other advance (Detached,         *)
(* Diverged, duplicate, dropped, or in a refused group) settles at once.   *)
(* `mateBusy` is an environment toggle standing for another stream of the  *)
(* same bucket with an advance in flight (BucketMate).                     *)
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
(* overlap an earlier chunk's is visible (Straddled), and so is whether a  *)
(* reader admits every key's pages (PagesAdmit: append_page_runs /         *)
(* keep_past admit an overlapping page only when it agrees over the common *)
(* span).                                                                  *)
(*                                                                         *)
(* Crash erases every volatile thing (applied groups, channel, mark,       *)
(* roster, receipts, partition memtable) and preserves the durable prefix  *)
(* D and durable history rows.  Faults: failed/ambiguous history flush, a  *)
(* refused commit group, a dropped Absorbed op in a group that lands,      *)
(* crash, a lane mark pruned for an evicted handle while its advance is    *)
(* queued (the sweep prune is not gated on settlement), trims between a    *)
(* plan and its scan, per-stream-cap gathers, gathers served by the        *)
(* durable tail ring, exhausted global trim budget.  Every record is one   *)
(* byte, so the tail's exact unabsorbed_bytes ledger `ub` must equal       *)
(* next - abs.                                                             *)
(*                                                                         *)
(* The action -> production-function mapping and the atomicity table are  *)
(* in README.md.  Mutation points used by the negative controls are the    *)
(* operators SubmitReady, PostingsWrite, AdvanceTrimTarget,                *)
(* TickTrimTarget, SafeRaisedOnDuplicate, Retires, WarmInstallFrom,        *)
(* RollbackGate, HeldPastWal and ScanView; the MC module overrides them    *)
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
    Keys,          \* routing keys (cache and page models)
    KeyOf,         \* [0..N-1 -> Keys] (cache and page models)
    MaxEvict,      \* cache weight evictions (cache model)
    Pages,         \* model the postings page ranges chunks write
    Prune,         \* the sweep prunes lane marks (worker.rs submitted.retain)
    BucketMate     \* another stream shares this stream's settlement bucket

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
       /\ Pages \in BOOLEAN /\ Prune \in BOOLEAN /\ BucketMate \in BOOLEAN

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
(* The v2 lane mark (history.rs Absorber::submitted, LaneMarks): the       *)
(* highest upto the lane submitted.  NoMark = 0 is "no entry": plan_read   *)
(* floors at max(mark, P.abs), so an absent entry and 0 plan alike, and    *)
(* roll_back_stranded_mark never removes a 0 because it is never above    *)
(* the boundary.                                                           *)
NoMark == 0

VARIABLES
    A,          \* applied tail: the committer's StreamOverlay base (state.applied)
    pend,       \* applied, not yet remotely durable groups (tail after each group)
    held,       \* receipts: held[i] iff pend[i] retired an Exact advance (keeps its receipt)
    durHeld,    \* receipts: Exact advances remote-durable but not yet dispatched
    D,          \* remotely durable tail (WAL durable prefix; Remote reads)
    P,          \* published tail: handle.state.durable (dispatch_durable)
    hc,         \* history canonical row per offset
    hp,         \* history postings coverage per offset
    ab,         \* absorber gather in progress
    mark,       \* Absorber lane: the stream's v2 lane mark (NoMark = none)
    due,        \* Absorber: the stream has an entry in the pending roster
    mateBusy,   \* a bucket-sharing stream has an unsettled advance (BucketMate)
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
    prunedInFlight, \* ghost: a mark above P.abs was pruned while an advance was in flight
    lateInstall, \* ghost: a warm install named a range starting above plan.from
    detached,   \* ghost: the committer dropped an advance not starting at its boundary
    rolledBack, \* ghost: a plan rolled a stranded mark back
    mateHeld    \* ghost: a plan kept a stranded mark only because the bucket mate was busy

vars == <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan, rd, sl, wm,
          pgMem, pgDur, crashes, flushFails, rejects, drops, reads, evicts,
          crashLoss, rejectLoss, dupSeen, diverged, prunedInFlight, lateInstall, detached,
          rolledBack, mateHeld>>
cacheVars == <<sl, wm, evicts>>
pageVars == <<pgMem, pgDur>>
ghostVars == <<crashLoss, rejectLoss, dupSeen, diverged, prunedInFlight, lateInstall, detached,
               rolledBack, mateHeld>>

-----------------------------------------------------------------------------
(* Receipts and the settlement gate.                                       *)

\* Some receipt of this stream is unsettled: a queued batch, an applied
\* group holding an Exact advance's receipt, or a durable group whose
\* dispatch has not yet published its tails.
HeldInPend == \E i \in 1..Len(held) : held[i]
Unsettled == chan # <<>> \/ HeldInPend \/ durHeld > 0
\* Submissions::settled(hash): the stream's bucket counter is zero.
SettledGate == ~Unsettled /\ ~mateBusy

-----------------------------------------------------------------------------
(* Mutation points (baseline definitions).                                 *)

\* The gather publishes (submit_absorbed_batch_v2) only after part.flush().
SubmitReady(a) == a.ph = "flushed"
\* stage_checked writes the postings pages into the SAME WriteBatch.
WriteRows(h, S) == [o \in Offs |-> IF o \in S /\ h[o] = "none" THEN "mem" ELSE h[o]]
PostingsWrite(h, S) == WriteRows(h, S)
\* advance_boundary trims toward trim_safe_to (previous boundary).
AdvanceTrimTarget(safe, newAbs) == safe
\* CommitTransaction::trim targets trim_safe_to.min(absorbed).
TickTrimTarget(t) == Min(t.safe, t.abs)
\* A non-advancing Absorbed op changes nothing (the `upto > prev_absorbed` guard).
SafeRaisedOnDuplicate == FALSE
\* retire_absorbed (commit_plan.rs): only a copy that starts exactly at the
\* stream's absorbed boundary retires; any other is Detached.
Retires(m, prev) == m.f = prev
\* The start of the range the gather names for PostingsCache::install_chunk
\* after its flush (TLA-016-F3 fix, re-applied over slate's gather):
\* note_frames returns the staged rows first..last + 1, so the install
\* claims only rows the scan read.  The staged rows are dense (one Remote
\* scan snapshot, or a ring window with a density proof) but start above
\* plan.from when the scan skipped a head a trim deleted after the plan.
WarmInstallFrom(a) == SetMin(a.chunk)
\* plan_reads rolls a stranded mark back only while the stream's bucket is
\* settled (b5751e75).
RollbackGate == SettledGate
\* A group that retired an Exact advance keeps its receipt past WAL
\* durability, until dispatch_durable has published its tails.
HeldPastWal(h) == h
\* The Remote scan's view for its next row: the snapshot of D taken when it
\* was created (its first row), ASM-SLATEDB-DURABLE (j).
ScanView(a) == IF a.snap.set THEN a.snap
               ELSE [set |-> TRUE, trimmed |-> D.trimmed, next |-> D.next]

-----------------------------------------------------------------------------
FlushAll(h) == [o \in Offs |-> IF h[o] = "mem" THEN "dur" ELSE h[o]]
LoseMem(h) == [o \in Offs |-> IF h[o] = "mem" THEN "none" ELSE h[o]]
\* The pages a staged chunk writes (stage_checked): one per routing key it
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

Init ==
    /\ A = InitTail /\ D = InitTail /\ P = InitTail
    /\ pend = <<>> /\ held = <<>> /\ durHeld = 0
    /\ hc = [o \in Offs |-> "none"] /\ hp = [o \in Offs |-> "none"]
    /\ ab = IdleAb /\ mark = NoMark /\ due = TRUE /\ mateBusy = FALSE
    /\ chan = <<>> /\ rd = IdleRd
    /\ sl = InitSl /\ wm = InitWm /\ pgMem = {} /\ pgDur = {}
    /\ crashes = 0 /\ flushFails = 0 /\ rejects = 0 /\ drops = 0 /\ reads = 0 /\ evicts = 0
    /\ crashLoss = FALSE /\ rejectLoss = FALSE /\ dupSeen = FALSE
    /\ diverged = FALSE /\ prunedInFlight = FALSE /\ lateInstall = FALSE
    /\ detached = FALSE /\ rolledBack = FALSE /\ mateHeld = FALSE

(* A new applied commit group: one WriteBatch applied to the memtable and   *)
(* queued for the WAL (CommitTransaction::write + publish).  `h` says       *)
(* whether its DurableEffects hold an Exact advance's receipt.              *)
NewGroupH(T, h) == /\ Len(pend) < MaxPend
                   /\ A' = T
                   /\ pend' = Append(pend, T)
                   /\ held' = Append(held, h)
NewGroup(T) == NewGroupH(T, FALSE)

-----------------------------------------------------------------------------
(* Postings-slice cache claims (ModelCache).                                *)

\* PostingsCache::install_chunk(inc, chunk_from, chunk_to, per_key): the
\* absorber hands over the runs it encoded from the frames it actually read
\* (check_postings), for the range [cf0, ct) it names.  A gap in the warm
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
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan, rd,
                   pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

CacheLoad ==       \* runs_for Lead: cold store load of the key's postings pages
    /\ ModelCache
    /\ \E k \in Keys, to \in {P.abs, D.abs} :
         /\ ~sl[k].has /\ to > 0
         /\ sl' = [sl EXCEPT ![k] = [has |-> TRUE, cf |-> 0, it |-> to,
                     runs |-> {o \in Offs : o < to /\ KeyOf[o] = k /\ hp[o] # "none"}]]
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan, rd, wm,
                   pageVars, crashes, flushFails, rejects, drops, reads, evicts, ghostVars>>

-----------------------------------------------------------------------------
(* Shard log / committer / durability pipeline                             *)

CustomerAppend ==
    /\ A.next < N
    /\ NewGroup([A EXCEPT !.next = A.next + 1, !.ub = A.ub + 1])
    /\ UNCHANGED <<durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan, rd, cacheVars,
                   pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

WalDurable ==      \* SlateDB remote WAL flush: durable_seq covers the oldest group
    /\ pend # <<>>
    /\ D' = Head(pend)
    /\ pend' = Tail(pend)
    /\ held' = Tail(held)
    /\ durHeld' = durHeld + (IF HeldPastWal(Head(held)) THEN 1 ELSE 0)
    /\ UNCHANGED <<A, P, hc, hp, ab, mark, due, mateBusy, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

\* ShardEngine::dispatch_durable publishes handle.state.durable group by
\* group; each group's receipts drop at the end of its iteration, after its
\* tails.  An append group's AbsorbSignal is sent there too (try_send; the
\* model never drops it, the rescan would cover a drop).  The signal is
\* handled by the absorber's select loop after any gather in progress, so
\* it re-pends the stream after that gather settles (see GatherDone).
Dispatch ==
    /\ P # D
    /\ P' = D
    /\ durHeld' = 0
    /\ due' = (due \/ D.next > P.next)
    /\ UNCHANGED <<A, pend, held, D, hc, hp, ab, mark, mateBusy, chan, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

Crash ==           \* process crash/restart or ownership move of the shard engine
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ A' = D /\ P' = D /\ pend' = <<>> /\ held' = <<>> /\ durHeld' = 0
    \* The absorber task ends with the engine: its lane marks, pending
    \* roster and Submissions (one per engine) go with it; the next owner's
    \* absorber seeds its roster from the dirty index (RescanSeed).
    /\ chan' = <<>> /\ ab' = IdleAb /\ mark' = NoMark /\ due' = FALSE /\ mateBusy' = FALSE
    /\ hc' = LoseMem(hc) /\ hp' = LoseMem(hp) /\ pgMem' = {}
    /\ rd' = IdleRd
    \* The postings cache is process-wide: a process crash wipes it, an
    \* engine close or ownership move of this shard leaves it.
    /\ \/ sl' = InitSl /\ wm' = InitWm
       \/ UNCHANGED <<sl, wm>>
    /\ crashLoss' = (crashLoss \/ chan # <<>> \/ A.abs > D.abs \/ ab.ph # "idle")
    /\ UNCHANGED <<D, pgDur, flushFails, rejects, drops, reads, evicts, rejectLoss, dupSeen,
                   diverged, prunedInFlight, lateInstall, detached, rolledBack, mateHeld>>

HistoryBackgroundFlush ==   \* memtable flush not requested by the gather (or close)
    /\ (\E o \in Offs : hc[o] = "mem" \/ hp[o] = "mem") \/ pgMem # {}
    /\ hc' = FlushAll(hc) /\ hp' = FlushAll(hp) /\ FlushPages
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, ab, mark, due, mateBusy, chan, rd, cacheVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

\* Another stream of the same settlement bucket submits an advance, or its
\* last receipt settles (Submissions::bucket; 1,024 buckets).
MateToggle ==
    /\ BucketMate
    /\ mateBusy' = ~mateBusy
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, chan, rd, cacheVars,
                   pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

-----------------------------------------------------------------------------
(* Absorber: Absorber::plan_reads / plan_read / read_wave / stage_chunk /   *)
(* commit, the same task's dirty-index rescan (seed_from_dirty_index) and   *)
(* lane-mark prune (worker.rs).  The absorber is one task: the rescan and   *)
(* the prune never run while a gather is in progress.                      *)

\* The pending entry leaves the roster when the gather settles it (advanced
\* to the plan's end, or no_work); a partial advance or a failed flush keeps
\* it.  A signal for records published during the gather (P.next beyond the
\* plan's end) was queued behind the gather and re-pends the stream.
GatherDone(partial) == due' = (partial \/ P.next > ab.upto)

\* plan_reads (gather.rs): for a pending stream, load the handle; if the
\* stream's bucket is settled, roll a mark above the published boundary
\* back (roll_back_stranded_mark with resident_absorbed); then plan_read
\* from max(mark, P.abs) to P.next.  An empty window is no_work, which
\* removes the stream from the roster.  The gate, the P read and the mark
\* read are one step: only this task submits, so a settled bucket stays
\* settled until it submits again, and the Release/Acquire pairing makes
\* every boundary published before the last receipt dropped visible.
AbsorberPlan ==
    /\ ab.ph = "idle" /\ due
    /\ LET stranded == mark > P.abs
           back     == stranded /\ RollbackGate
           m1       == IF back THEN NoMark ELSE mark
           from     == Max(m1, P.abs)
           upto     == P.next
       IN /\ mark' = m1
          /\ rolledBack' = (rolledBack \/ back)
          /\ mateHeld' = (mateHeld \/ (stranded /\ ~back /\ ~Unsettled))
          /\ IF from < upto
               THEN /\ \E ring \in (IF GatherRing THEN BOOLEAN ELSE {FALSE}) :
                         ab' = [ph |-> "reading", from |-> from, upto |-> upto,
                                cur |-> from, chunk |-> {}, ring |-> ring, snap |-> NoSnap]
                    /\ UNCHANGED due
               ELSE /\ due' = FALSE
                    /\ UNCHANGED ab
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, mateBusy, chan, rd, cacheVars,
                   pageVars, crashes, flushFails, rejects, drops, reads, crashLoss, rejectLoss,
                   dupSeen, diverged, prunedInFlight, lateInstall, detached>>

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
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, mark, due, mateBusy, chan, rd,
                   cacheVars, pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

AbsorberReadEnd == \* range exhausted, or the per-stream byte cap is reached
    /\ ab.ph = "reading"
    /\ ab.cur = ab.upto \/ Cardinality(ab.chunk) = Cap
    /\ ab' = [ab EXCEPT !.ph = "readdone"]
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, mark, due, mateBusy, chan, rd,
                   cacheVars, pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

AbsorberStage ==   \* stage_chunk: note_frames, check_postings, stage_checked
    /\ ab.ph = "readdone"
    /\ IF ab.chunk = {}
         THEN /\ ab' = IdleAb                       \* no_work
              /\ GatherDone(FALSE)
              /\ UNCHANGED <<hc, hp, pgMem>>
         ELSE /\ hc' = WriteRows(hc, ab.chunk)
              /\ hp' = PostingsWrite(hp, ab.chunk)
              /\ pgMem' = IF Pages THEN Overwrite(pgMem, ChunkPages(ab.chunk)) ELSE pgMem
              /\ ab' = [ab EXCEPT !.ph = "staged"]
              /\ UNCHANGED due
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, mark, mateBusy, chan, rd, cacheVars, pgDur,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

AbsorberFlushOk == \* part.flush() returned Ok: the whole memtable is durable
    /\ ab.ph = "staged"
    /\ hc' = FlushAll(hc) /\ hp' = FlushAll(hp) /\ FlushPages
    /\ ab' = [ab EXCEPT !.ph = "flushed"]
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, mark, due, mateBusy, chan, rd, cacheVars,
                   crashes, flushFails, rejects, drops, reads, ghostVars>>

AbsorberFlushFail ==  \* part.flush() returned Err (outcome may be ambiguous)
    /\ ab.ph = "staged"
    /\ flushFails < MaxFlushFail
    /\ flushFails' = flushFails + 1
    /\ ab' = IdleAb                                 \* settle_gather_error: stays pending
    /\ \/ UNCHANGED <<hc, hp, pageVars>>
       \/ /\ hc' = FlushAll(hc) /\ hp' = FlushAll(hp) /\ FlushPages
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, mark, due, mateBusy, chan, rd, cacheVars,
                   crashes, rejects, drops, reads, ghostVars>>

\* Absorber::commit after the flush: install_chunk (warm the slice cache
\* over [WarmInstallFrom, last + 1) with the staged runs), then count the
\* advance in its bucket (Submissions::submit, the receipt rides the copy),
\* submit_absorbed_batch_v2 with (hash, last + 1, CopiedBytes(plan.from,
\* chunk bytes)), then raise_lane_marks: the mark rises to the chunk's end.
\* Then settle_gather.  No await separates the install from the send, nor
\* the send's return from the raise; a crash while the send is blocked is
\* the kept-cache branch of Crash.
AbsorberSubmit ==
    /\ SubmitReady(ab)
    /\ Len(chan) < MaxChan
    /\ LET u  == SetMax(ab.chunk) + 1
           cf == WarmInstallFrom(ab)
       IN
         /\ chan' = Append(chan, [f |-> ab.from, u |-> u, b |-> Cardinality(ab.chunk)])
         /\ mark' = Max(mark, u)
         /\ GatherDone(u < ab.upto)
         /\ IF ModelCache THEN InstallChunk(cf, u, ab.chunk) ELSE UNCHANGED <<sl, wm>>
         /\ lateInstall' = (lateInstall \/ (ModelCache /\ cf > ab.from))
    /\ ab' = IdleAb
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, mateBusy, rd, pageVars, crashes,
                   flushFails, rejects, drops, reads, evicts, crashLoss, rejectLoss, dupSeen,
                   diverged, prunedInFlight, detached, rolledBack, mateHeld>>

\* seed_from_dirty_index (every RESCAN_EVERY ticks, and the first scan of a
\* new owner's absorber): the dirty row's tail, read at Memory level
\* (backlog_of -> tail_fields), shows unabsorbed records, so the stream is
\* merged into the roster (or_insert).  It no longer touches lane marks.
RescanSeed ==
    /\ ab.ph = "idle" /\ ~due
    /\ A.abs < A.next
    /\ due' = TRUE
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, mateBusy, chan, rd,
                   cacheVars, pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

\* The sweep's submitted.retain (worker.rs, every sweep_every ticks) keeps a
\* mark only while the stream is pending or its RESIDENT handle's published
\* absorbed trails the mark.  It is not gated on settlement.  An
\* unreferenced handle may be evicted (ASM-HISTORY-EVICTION): a reloaded
\* handle reads the Memory-level tail, which then equals D and P because no
\* group is in flight.
MarkPrune ==
    /\ Prune
    /\ ab.ph = "idle" /\ ~due
    /\ mark # NoMark
    /\ P.abs >= mark \/ Evictable
    /\ mark' = NoMark
    /\ prunedInFlight' = (prunedInFlight \/ (mark > P.abs /\ Unsettled))
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, due, mateBusy, chan, rd,
                   cacheVars, pageVars, crashes, flushFails, rejects, drops, reads, crashLoss,
                   rejectLoss, dupSeen, diverged, lateInstall, detached, rolledBack, mateHeld>>

-----------------------------------------------------------------------------
(* Committer: CommitTransaction::absorbed -> advance_boundary ->            *)
(* retire_absorbed, and ::trim.                                            *)

\* One Absorbed op, staged in a group of its own.  An advancing op that
\* starts at the applied boundary is Exact (the group keeps its receipt) or,
\* if it claims more than the ledger holds, Diverged (the whole group is
\* refused).  One that starts elsewhere is Detached: nothing moves.  A
\* Detached, Diverged or non-advancing op drops its receipt at staging.
CommitAbsorbed ==
    /\ chan # <<>>
    /\ chan' = Tail(chan)
    /\ LET m      == Head(chan)
           prev   == A.abs
           newAbs == Min(m.u, A.next)
       IN IF m.u > prev
            THEN IF Retires(m, prev)
                   THEN IF m.b > A.ub
                          THEN \* Diverged: accounting_diverged refuses the
                               \* whole group in finish(); nothing is written.
                               /\ diverged' = TRUE
                               /\ UNCHANGED <<A, pend, held, dupSeen, detached>>
                          ELSE \* Exact
                               /\ \E allowed \in TrimBudgets :
                                    LET safe   == Max(A.safe, prev)
                                        tt     == Min(AdvanceTrimTarget(safe, newAbs),
                                                      A.trimmed + allowed)
                                    IN NewGroupH([A EXCEPT !.abs = newAbs, !.safe = safe,
                                                          !.trimmed = Max(A.trimmed, tt),
                                                          !.gprev = prev,
                                                          !.ub = A.ub - m.b], TRUE)
                               /\ UNCHANGED <<dupSeen, diverged, detached>>
                   ELSE \* Detached (warned, dropped whole); a no-write group
                        /\ detached' = TRUE
                        /\ UNCHANGED <<A, pend, held, dupSeen, diverged>>
            ELSE /\ dupSeen' = TRUE
                 /\ UNCHANGED <<diverged, detached>>
                 /\ IF SafeRaisedOnDuplicate /\ A.safe < A.abs
                      THEN \E allowed \in TrimBudgets :
                             LET safe == A.abs
                             IN NewGroup([A EXCEPT !.safe = safe,
                                    !.trimmed = Max(A.trimmed,
                                                    Min(safe, A.trimmed + allowed))])
                      ELSE UNCHANGED <<A, pend, held>>   \* no-write group
    /\ UNCHANGED <<durHeld, D, P, hc, hp, ab, mark, due, mateBusy, rd, cacheVars, pageVars,
                   crashes, flushFails, rejects, drops, reads, crashLoss, rejectLoss,
                   prunedInFlight, lateInstall, rolledBack, mateHeld>>

\* The group carrying the advance is refused before it is written, and its
\* receipt drops with it: a closed engine or a failed billing-row pre-read
\* (CommitTransaction::run), another operation's accounting divergence
\* (finish), stage_maintenance's divergence, or the test failpoint
\* fail_next_absorbed_group.  A failed group write retires the engine
\* (write_failed -> begin_close), which is the engine-close branch of Crash.
CommitRefuseGroup ==
    /\ chan # <<>>
    /\ rejects < MaxRejects
    /\ chan' = Tail(chan)
    /\ rejects' = rejects + 1
    /\ rejectLoss' = TRUE
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, rd,
                   cacheVars, pageVars, crashes, flushFails, drops, reads, crashLoss, dupSeen,
                   diverged, prunedInFlight, lateInstall, detached, rolledBack, mateHeld>>

\* stage() drops just this Absorbed op because stream_handle failed
\* (reject_op's `_ => {}`) while the rest of the group lands; its receipt
\* drops at staging.
CommitDropOne ==
    /\ chan # <<>>
    /\ rejects < MaxRejects /\ drops < MaxDrops
    /\ chan' = Tail(chan)
    /\ rejects' = rejects + 1
    /\ drops' = drops + 1
    /\ rejectLoss' = TRUE
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, rd,
                   cacheVars, pageVars, crashes, flushFails, reads, crashLoss, dupSeen,
                   diverged, prunedInFlight, lateInstall, detached, rolledBack, mateHeld>>

TrimStep ==        \* TrimTick expansion: one budgeted CommitOp::TrimStep
    /\ TrimDebt(A)
    /\ \E allowed \in TrimBudgets \ {0} :
         NewGroup([A EXCEPT !.trimmed = Min(TickTrimTarget(A), A.trimmed + allowed)])
    /\ UNCHANGED <<durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan, rd, cacheVars,
                   pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

-----------------------------------------------------------------------------
(* Reader holding a published absorbed snapshot (execute_segment's first    *)
(* observation); the full merged read is TLA-018 (ReadCompose).             *)

ReaderSnap ==
    /\ rd.ph = "idle"
    /\ reads < MaxReads
    /\ rd' = [ph |-> "snap", B |-> P.abs]
    /\ reads' = reads + 1
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan,
                   cacheVars, pageVars, crashes, flushFails, rejects, drops, ghostVars>>

ReaderRelease ==
    /\ rd.ph = "snap"
    /\ rd' = IdleRd
    /\ UNCHANGED <<A, pend, held, durHeld, D, P, hc, hp, ab, mark, due, mateBusy, chan,
                   cacheVars, pageVars, crashes, flushFails, rejects, drops, reads, ghostVars>>

-----------------------------------------------------------------------------
(* Legitimate quiescence: everything appended, absorbed, durable,           *)
(* published, trimmed to the safe target, no volatile work outstanding.     *)
(* Without the prune (Prune = FALSE) a lane mark is never removed, so it   *)
(* may remain at quiescence.  With BucketMate the mate may still toggle.    *)
Settled ==
    /\ A.next = N /\ pend = <<>> /\ chan = <<>> /\ ab.ph = "idle" /\ ~due
    /\ A = D /\ P = D /\ D.abs = N /\ ~TrimDebt(D)
    /\ \A o \in Offs : hc[o] # "mem" /\ hp[o] # "mem"
    /\ pgMem = {}
    /\ rd.ph = "idle"
    /\ Prune => mark = NoMark

Terminated == Settled /\ UNCHANGED vars

Next ==
    \/ CustomerAppend \/ WalDurable \/ Dispatch \/ Crash \/ HistoryBackgroundFlush
    \/ MateToggle
    \/ AbsorberPlan \/ AbsorberRead \/ AbsorberReadEnd \/ AbsorberStage
    \/ AbsorberFlushOk \/ AbsorberFlushFail \/ AbsorberSubmit
    \/ RescanSeed \/ MarkPrune
    \/ CommitAbsorbed \/ CommitRefuseGroup \/ CommitDropOne \/ TrimStep
    \/ ReaderSnap \/ ReaderRelease
    \/ CacheEvict \/ CacheLoad
    \/ Terminated

Spec == Init /\ [][Next]_vars

(* Liveness ("absorption completes once faults cease").  Faults are bounded *)
(* (crashes, flush failures, refused groups, dropped ops, appends), so they *)
(* cease.  Fairness is on the actors' ATTEMPTS, never on a success outcome: *)
(* the flush attempt (Ok or Err), the committer's handling of a message     *)
(* (commit, refusal or drop), the WAL flusher, dispatch, the absorber's     *)
(* gather and its dirty-index rescan.  A refused advance leaves a quiet     *)
(* stream out of the roster with a stranded mark; only the rescan re-pends  *)
(* it, so the rescan is fair (it runs every RESCAN_EVERY ticks).  The plan  *)
(* and the committer are strongly fair because they are only              *)
(* intermittently enabled (the committer while the WAL buffer is full).    *)
(* No fairness on appends, crashes, prunes, the bucket mate, evictions or  *)
(* loads; the liveness shapes have no bucket mate.                         *)
AbsorberFlush == AbsorberFlushOk \/ AbsorberFlushFail
CommitStep == CommitAbsorbed \/ CommitRefuseGroup \/ CommitDropOne
LiveSpec ==
    /\ Spec
    /\ WF_vars(WalDurable) /\ WF_vars(Dispatch)
    /\ SF_vars(AbsorberPlan) /\ WF_vars(AbsorberRead) /\ WF_vars(AbsorberReadEnd)
    /\ WF_vars(AbsorberStage) /\ WF_vars(AbsorberFlush) /\ SF_vars(AbsorberSubmit)
    /\ SF_vars(CommitStep) /\ SF_vars(TrimStep)
    /\ WF_vars(RescanSeed)
    /\ WF_vars(ReaderRelease)

-----------------------------------------------------------------------------
(* Safety properties                                                        *)

\* The receipts are exactly the unsettled Exact advances: an applied group
\* holds one iff it moved the boundary, and a durable group still holds one
\* iff dispatch has not published the boundary it moved.  (The channel's
\* receipts are one per entry by construction.)
PrevAbs(i) == IF i = 1 THEN D.abs ELSE pend[i - 1].abs
ReceiptsMatchInFlight ==
    /\ Len(held) = Len(pend)
    /\ \A i \in 1..Len(pend) : held[i] = (pend[i].abs > PrevAbs(i))
    /\ (durHeld > 0) = (D.abs > P.abs)

TypeOK ==
    /\ A \in TailT /\ D \in TailT /\ P \in TailT
    /\ \A i \in 1..Len(pend) : pend[i] \in TailT
    /\ Len(pend) <= MaxPend
    /\ \A i \in 1..Len(held) : held[i] \in BOOLEAN
    /\ durHeld \in 0..N
    /\ hc \in [Offs -> Rows] /\ hp \in [Offs -> Rows]
    /\ ab.ph \in {"idle", "reading", "readdone", "staged", "flushed"}
    /\ ab.chunk \subseteq Offs /\ ab.ring \in BOOLEAN /\ ab.snap.set \in BOOLEAN
    /\ mark \in 0..N /\ due \in BOOLEAN /\ mateBusy \in BOOLEAN
    /\ Len(chan) <= MaxChan
    /\ \A i \in 1..Len(chan) : chan[i].f \in 0..N /\ chan[i].u \in 1..N /\ chan[i].b \in 1..N
    /\ ReceiptsMatchInFlight
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
    \A o \in Offs : o < mark => hc[o] = "dur" /\ hp[o] = "dur"

\* The exact per-stream unabsorbed_bytes ledger equals the stored frame bytes
\* in [absorbed, next) (src/shard.rs TailFields doc) in every tail view.
\* Repeated/overlapping absorption must not retire bytes twice.
LedgerExact == \A T \in TailStates : T.ub = T.next - T.abs

\* The settlement gate (b5751e75): a gather never starts below an advance
\* that can still move or publish the boundary, i.e. below a queued batch's
\* end or below the applied boundary.  The ungated sweep prune is the one
\* way around it (D6), so the property is checked until a prune removed a
\* mark while an advance was in flight.
NoRegatherUnderInFlight ==
    (ab.ph # "idle" /\ ~prunedInFlight) =>
        /\ A.abs <= ab.from
        /\ \A i \in 1..Len(chan) : chan[i].u <= ab.from

\* The committer drops an advance as Detached only after a fault that lost
\* an advance of the stream (a refused group or a dropped op, whose chained
\* successor no longer starts at the boundary) or after the ungated prune
\* regathered under an advance in flight.  Crashes, flush failures, stale
\* published snapshots and the bucket mate alone never produce one.
DetachedOnlyAfterFault == detached => (rejectLoss \/ prunedInFlight)

\* The postings-slice cache never proves the absence of a record of the
\* key that history holds below the durable absorbed boundary (a keyed read
\* served from such a slice would skip that record and report completion).
CacheNeverProvesFalseAbsence ==
    \A k \in Keys : sl[k].has =>
        \A o \in Offs :
            (/\ sl[k].cf <= o /\ o < EffTo(k)
             /\ o < D.abs /\ KeyOf[o] = k) => o \in sl[k].runs

\* Two pages of one key overlap: one starts inside the other's span (from
\* its first offset to its last offset + 1).  A chunk re-gathered from the
\* same row rewrites the same pages in place instead.  Reachable (a refused
\* chain, the prune, a new owner): Witness_OverlapAdmitted.
Straddled ==
    \E p, q \in EffPages : p[1] = q[1] /\ p[2] < q[2] /\ q[2] < SetMax(p[3]) + 1

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
Witness_EvictedMarkPrunedInFlight == ~prunedInFlight
Witness_RingGatherBelowTrim ==
    ~(ab.ph = "readdone" /\ ab.ring /\ \E o \in ab.chunk : o < D.trimmed)
Witness_WarmBridgeCovers ==
    ~(\E k \in Keys : sl[k].has /\ EffTo(k) > sl[k].it /\ EffTo(k) <= D.abs)
\* A re-gather whose scan skipped a trimmed head warms the cache over the
\* rows it staged, a range starting above plan.from (the F3 fix path).
Witness_InstallStartsAbovePlan == ~lateInstall
\* A refused advance strands the mark; the rescan re-pends the quiet
\* stream, the settled plan rolls the mark back, and absorption completes.
Witness_RefusalHealed == ~(rejectLoss /\ rolledBack /\ D.abs = N)
\* A stranded mark is kept by a plan only because a bucket-sharing stream
\* had an advance in flight, and absorption still completes.
Witness_MateDelaysRollback == ~(mateHeld /\ D.abs = N)
\* A re-gather from a stale boundary leaves pages of one key that overlap,
\* the reader admits them as one index, and absorption completes.
Witness_OverlapAdmitted == ~(Straddled /\ PagesAdmit /\ D.abs = N)
=============================================================================
