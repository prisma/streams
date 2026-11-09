------------------------------ MODULE SealSpill ------------------------------
(***************************************************************************)
(* TLA-016 (layout 6A successor) -- the seal spill: pending operands, the  *)
(* snapshot, the segment object, the directory and the remainder commit.   *)
(*                                                                         *)
(* One shard log (a SlateDB with a WAL) and its sealer.  Every commit      *)
(* group is one WriteBatch, applied in order (`wal`, a sequence of         *)
(* groups) and durable as a prefix (`dur`, ASM-SLATEDB-DURABLE (a)(b)).    *)
(* A group that appends to a stream puts its tail and appends its pages as *)
(* ONE merge operand on the stream's pending row (finalize.rs              *)
(* stage_pending_rows), and publishes them into the handle's in-memory     *)
(* pending pages at applied visibility (overlay.rs publish_into).  Durable *)
(* dispatch publishes the durable tails in group order (`pub`); a record   *)
(* is acknowledged when its group's durable tail is published.             *)
(*                                                                         *)
(* The sealer runs one seal at a time (seal/steps.rs seal_steps):          *)
(*   (2) snapshot each stream's durable unsealed pages                     *)
(*       [durable.absorbed, durable.next) from the handle's pending pages  *)
(*       (skipped when they do not tile it: snapshot_gaps);                *)
(*   (3)(4) build and PUT one object (PutMode::Create, never deleted here);*)
(*   (5) publish its chunks in the in-memory directory;                    *)
(*   (6) CommitOp::Seal: the committer checks each stream's LIVE (applied) *)
(*       tail, all or nothing (absorbed = from, upto <= next), moves the   *)
(*       boundary, PUTs the pending row's remainder = the handle's applied *)
(*       pages >= upto ++ this group's own pages of the stream (REVIEW-6A  *)
(*       F1; a delete when empty), and stages the 'K' (and 'R') rows in    *)
(*       the same batch; a refused or dropped group withdraws the entries; *)
(*   (7a) a wait until dispatch published the seal's group (no WAL flush   *)
(*       of its own; not modelled: 7b over-approximates it, below);        *)
(*       (7b) a forced memtable flush.                                     *)
(* A deadline flush (and every other forced flush) is one step under the   *)
(* flush lock, with no WAL flush of its own (REVIEW-6A F2).                *)
(* Step 1 (WAL flush + bounded wait) is not a separate step: WalFlush is   *)
(* free at all times and the snapshot reads whatever dispatch published    *)
(* (the wait times out after 1 s in production).                           *)
(*                                                                         *)
(* A crash keeps the durable prefix; the restart (seal/open.rs,            *)
(* handle_load.rs) rebuilds the directory from the durable 'K' rows and    *)
(* loads the tail and pending row of each stream the unsealed index 'W'    *)
(* names (layout 8, S3a §4.1): no tail scan.  Every group stages a        *)
(* stream's 'W' row from its base (applied) and staged tails alone         *)
(* (open_index::unsealed_op): a put when the stream gains unsealed pages,  *)
(* a delete when it loses its last; W_IndexMatchesPending is invariant W.  *)
(*                                                                         *)
(* F2 needs SlateDB's memtable-flush retention (flush.rs, uploader.rs):    *)
(* entries above the boundary are all kept; at or below it, merge operands *)
(* are kept down to the newest put, which shadows every older version.    *)
(* With the WAL enabled the boundary is the frozen memtable's last seq     *)
(* (slatedb/slatedb#2208: the L0 publishes only once the WAL is durable    *)
(* through it), so `FlushBoundary` is Len(wal) whatever `dur` is (no       *)
(* active snapshot or transaction holds an older seq: an assumption, see   *)
(* README.md).                                                             *)
(*                                                                         *)
(* Records are one-record pages [o, e]: offset o appended by incarnation e *)
(* (the crash count), so an offset re-used after a crash is a different    *)
(* record.  Mutation points (MC_SealSpill.tla): RemainderBase (F1),        *)
(* FlushBoundary (F2), RemainderIndexOp ('W').                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Streams,        \* the shard's streams (model values)
    N,              \* offsets 0..N-1 per stream
    MaxCrashes,     \* crash + restart budget
    MaxSeals,       \* seals the sealer starts (empty ones included)
    MaxRejects,     \* seal groups the committer drops whole (finish's reject)
    MaxAborts,      \* seals that fail at a step: the PUT, or after apply
    MaxDeadlines,   \* deadline memtable flushes
    PairAppend      \* a seal's group may also append to a stream it seals

ASSUME /\ Streams # {} /\ N \in Nat \ {0}
       /\ MaxCrashes \in Nat /\ MaxSeals \in Nat /\ MaxRejects \in Nat
       /\ MaxAborts \in Nat /\ MaxDeadlines \in Nat /\ PairAppend \in BOOLEAN

Offs == 0..(N-1)
Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
Range(f) == {f[i] : i \in DOMAIN f}

\* A tail in a group: `has` FALSE when the group did not touch the stream.
NoTail == [has |-> FALSE, a |-> 0, n |-> 0]
MkTail(a, n) == [has |-> TRUE, a |-> a, n |-> n]
\* A pending-row write in a group.
NoOp == [t |-> "none", pg |-> <<>>]
\* A group's 'W' op for a stream, from its base and staged tails
\* (open_index::unsealed_op).
WOp(b, st) == IF b.a = b.n /\ st.a < st.n THEN "put"
              ELSE IF b.a < b.n /\ st.a = st.n THEN "del"
              ELSE "none"
\* The pages of `pg` at or above offset u (PendingPages::from).
From(pg, u) == SelectSeq(pg, LAMBDA p : p.o >= u)
Below(pg, u) == SelectSeq(pg, LAMBDA p : p.o < u)
\* `pg` tiles [f, u) exactly, oldest first.
Tiles(pg, f, u) == /\ Len(pg) + f = u
                   /\ \A i \in 1..Len(pg) : pg[i].o = f + i - 1

VARIABLES
    wal,      \* applied groups, in order
    dur,      \* durable prefix length (WAL or L0 + manifest)
    pub,      \* groups whose durable effects dispatch published
    app,      \* [s -> [a, n]] applied tail (handle.state.applied)
    dtl,      \* [s -> [a, n]] durable tail (handle.state.durable)
    pend,     \* [s -> Seq(page)] the handle's pending pages (in memory)
    inc,      \* incarnation = crash count
    dir,      \* in-memory directory: set of [s, f, l, id]
    objs,     \* PUT objects: set of [id, s, f, pg] (one chunk per stream)
    sl,       \* the sealer
    mstart,   \* groups 1..mstart are no longer in the memtable
    acked,    \* acknowledged records: set of <<s, page>>
    f2,       \* pages a seal's own 7b wrote to an L0: <<s, o, upto>>
    dl0,      \* sealed pages a deadline flush wrote to an L0: <<s, o>>
    late,     \* a durable-bound seal put a remainder holding a page applied after its snapshot
    refused,  \* a seal was refused by the live-tail check
    cnt       \* budgets used

vars == <<wal, dur, pub, app, dtl, pend, inc, dir, objs, sl, mstart,
          acked, f2, dl0, late, refused, cnt>>

Idle == [ph |-> "idle", id |-> 0, w |-> {}, g |-> 0]

-----------------------------------------------------------------------------
(* The store as a fold of a group prefix: tail and pending row per stream. *)

RECURSIVE FoldS(_, _, _)
FoldS(w, s, i) ==
    IF i = 0 THEN [a |-> 0, n |-> 0, row |-> <<>>]
    ELSE LET p == FoldS(w, s, i - 1)
             g == w[i]
         IN [a   |-> IF g.tl[s].has THEN g.tl[s].a ELSE p.a,
             n   |-> IF g.tl[s].has THEN g.tl[s].n ELSE p.n,
             row |-> CASE g.pd[s].t = "put"   -> g.pd[s].pg
                       [] g.pd[s].t = "merge" -> p.row \o g.pd[s].pg
                       [] g.pd[s].t = "del"   -> <<>>
                       [] OTHER               -> p.row]

Fold(w) == [s \in Streams |-> FoldS(w, s, Len(w))]

\* The unsealed index of a group prefix: the stream's last 'W' op.
RECURSIVE FoldWS(_, _, _)
FoldWS(w, s, i) ==
    IF i = 0 THEN FALSE
    ELSE CASE w[i].wi[s] = "put" -> TRUE
           [] w[i].wi[s] = "del" -> FALSE
           [] OTHER              -> FoldWS(w, s, i - 1)
FoldW(w) == [s \in Streams |-> FoldWS(w, s, Len(w))]
KRows(w) == UNION {w[i].k : i \in 1..Len(w)}
Durable == SubSeq(wal, 1, dur)

ObjOf(c) == {x \in objs : x.id = c.id /\ x.s = c.s}
ObjPage(c, o) == LET x == CHOOSE x \in ObjOf(c) : TRUE IN x.pg[o - x.f + 1]
Covering(K, s, o) == {c \in K : c.s = s /\ c.f <= o /\ o <= c.l}

-----------------------------------------------------------------------------
(* Mutation points.                                                        *)

\* The pages a seal's remainder starts from (transaction/seal.rs ->
\* finalize.rs stage_remainder): the handle's applied pages at or above
\* the new boundary when the committer stages the group.
RemainderBase(s, w) == From(pend[s], w.u)

\* The retention boundary of a memtable flush: the frozen memtable's last
\* sequence (slatedb/slatedb#2208), not the WAL's durable progress.
FlushBoundary == Len(wal)

\* The 'W' op the seal's remainder commit stages for a sealed stream
\* (finalize.rs stage_index_rows on the seal's group).
RemainderIndexOp(b, st) == WOp(b, st)

-----------------------------------------------------------------------------
(* SlateDB's memtable flush: the pages of stream s it writes to the L0,     *)
(* with retention boundary B.                                              *)

L0Pages(s, B) ==
    LET V == {i \in (mstart + 1)..Len(wal) : wal[i].pd[s].t # "none"}
        Kept == {i \in V : i > B \/
                   ~\E j \in V : j > i /\ j <= B /\ wal[j].pd[s].t \in {"put", "del"}}
    IN UNION {Range(wal[i].pd[s].pg) : i \in Kept}

-----------------------------------------------------------------------------
Init ==
    /\ wal = <<>>
    /\ dur = 0
    /\ pub = 0
    /\ app = [s \in Streams |-> [a |-> 0, n |-> 0]]
    /\ dtl = [s \in Streams |-> [a |-> 0, n |-> 0]]
    /\ pend = [s \in Streams |-> <<>>]
    /\ inc = 0
    /\ dir = {}
    /\ objs = {}
    /\ sl = Idle
    /\ mstart = 0
    /\ acked = {}
    /\ f2 = {}
    /\ dl0 = {}
    /\ late = FALSE
    /\ refused = FALSE
    /\ cnt = [crash |-> 0, seal |-> 0, rej |-> 0, abort |-> 0, dead |-> 0]

(* A client group appending one record to s. *)
AppendRec(s) ==
    /\ app[s].n < N
    /\ LET p == [o |-> app[s].n, e |-> inc]
           g == [tl |-> [t \in Streams |-> IF t = s THEN MkTail(app[s].a, app[s].n + 1) ELSE NoTail],
                 pd |-> [t \in Streams |-> IF t = s THEN [t |-> "merge", pg |-> <<p>>] ELSE NoOp],
                 wi |-> [t \in Streams |-> IF t = s THEN WOp(app[s], [a |-> app[s].a, n |-> app[s].n + 1])
                                           ELSE "none"],
                 k  |-> {},
                 ap |-> [t \in Streams |-> IF t = s THEN <<p>> ELSE <<>>]]
       IN /\ wal' = Append(wal, g)
          /\ app' = [app EXCEPT ![s].n = @ + 1]
          /\ pend' = [pend EXCEPT ![s] = Append(@, p)]
    /\ UNCHANGED <<dur, pub, dtl, inc, dir, objs, sl, mstart, acked, f2, dl0,
                   late, refused, cnt>>

(* A WAL flush (the pump, step 1, or anyone's): a longer durable prefix. *)
WalFlush ==
    /\ dur < Len(wal)
    /\ \E d \in (dur + 1)..Len(wal) : dur' = d
    /\ UNCHANGED <<wal, pub, app, dtl, pend, inc, dir, objs, sl, mstart, acked,
                   f2, dl0, late, refused, cnt>>

(* Durable dispatch publishes the next durable group's tails; its records *)
(* are acknowledged.                                                       *)
Dispatch ==
    /\ pub < dur
    /\ LET g == wal[pub + 1] IN
       /\ dtl' = [s \in Streams |-> IF g.tl[s].has THEN [a |-> g.tl[s].a, n |-> g.tl[s].n]
                                    ELSE dtl[s]]
       /\ acked' = acked \cup UNION {{<<s, p>> : p \in Range(g.ap[s])} : s \in Streams}
    /\ pub' = pub + 1
    /\ UNCHANGED <<wal, dur, app, pend, inc, dir, objs, sl, mstart, f2, dl0,
                   late, refused, cnt>>

(* (2) The snapshot of every stream's durable unsealed pages, from memory. *)
Window(s) == [s |-> s, f |-> dtl[s].a, u |-> dtl[s].n,
              pg |-> Below(From(pend[s], dtl[s].a), dtl[s].n),
              rem |-> From(pend[s], dtl[s].n)]
Snapshot ==
    /\ sl.ph = "idle"
    /\ cnt.seal < MaxSeals
    /\ LET W == {Window(s) : s \in {t \in Streams :
                    /\ dtl[t].a < dtl[t].n
                    /\ Tiles(Below(From(pend[t], dtl[t].a), dtl[t].n), dtl[t].a, dtl[t].n)}}
       IN IF W = {}
          THEN \* An empty seal: 7b (seal_steps).
               sl' = [ph |-> "flushed", id |-> cnt.seal + 1, w |-> {}, g |-> 0]
          ELSE sl' = [ph |-> "snap", id |-> cnt.seal + 1, w |-> W, g |-> 0]
    /\ cnt' = [cnt EXCEPT !.seal = @ + 1]
    /\ UNCHANGED <<wal, dur, pub, app, dtl, pend, inc, dir, objs, mstart, acked, f2, dl0,
                   late, refused>>

(* (3)(4) The object's PUT. *)
Put ==
    /\ sl.ph = "snap"
    /\ objs' = objs \cup {[id |-> sl.id, s |-> w.s, f |-> w.f, pg |-> w.pg] : w \in sl.w}
    /\ sl' = [sl EXCEPT !.ph = "put"]
    /\ UNCHANGED <<wal, dur, pub, app, dtl, pend, inc, dir, mstart, acked, f2, dl0,
                   late, refused, cnt>>

PutFails ==
    /\ sl.ph = "snap"
    /\ cnt.abort < MaxAborts
    /\ sl' = Idle
    /\ cnt' = [cnt EXCEPT !.abort = @ + 1]
    /\ UNCHANGED <<wal, dur, pub, app, dtl, pend, inc, dir, objs, mstart, acked,
                   f2, dl0, late, refused>>

Entries(s0) == {[s |-> w.s, f |-> w.f, l |-> w.u - 1, id |-> s0.id] : w \in s0.w}

(* (5) The chunks published in the in-memory directory. *)
Publish ==
    /\ sl.ph = "put"
    /\ dir' = dir \cup Entries(sl)
    /\ sl' = [sl EXCEPT !.ph = "pub"]
    /\ UNCHANGED <<wal, dur, pub, app, dtl, pend, inc, objs, mstart, acked, f2, dl0,
                   late, refused, cnt>>

(* (6) The committer stages the seal against the live tails, in a group    *)
(* that may also append one record to each stream in A (staged before or  *)
(* after the seal: either way the remainder takes the group's own pages). *)
SealedSet == {w.s : w \in sl.w}
WinOf(s) == CHOOSE w \in sl.w : w.s = s
Fits(w) == app[w.s].a = w.f /\ w.u <= app[w.s].n

Commit(A) ==
    /\ sl.ph = "pub"
    /\ A \subseteq {s \in SealedSet : app[s].n < N}
    /\ (PairAppend \/ A = {})
    /\ LET own == [s \in Streams |-> IF s \in A THEN <<[o |-> app[s].n, e |-> inc]>> ELSE <<>>]
           ok == \A w \in sl.w : Fits(w)
       IN
       \/ \* The group is dropped whole: nothing applied, entries withdrawn.
          /\ cnt.rej < MaxRejects
          /\ dir' = dir \ Entries(sl)
          /\ sl' = Idle
          /\ cnt' = [cnt EXCEPT !.rej = @ + 1]
          /\ UNCHANGED <<wal, app, pend, late, refused>>
       \/ \* Refused: a stream moved since its snapshot (A = {}: the group
          \* then carries nothing of interest here).
          /\ ~ok /\ A = {}
          /\ dir' = dir \ Entries(sl)
          /\ sl' = Idle
          /\ refused' = TRUE
          /\ UNCHANGED <<wal, app, pend, late, cnt>>
       \/ /\ ok
          /\ LET rem == [s \in SealedSet |-> RemainderBase(s, WinOf(s)) \o own[s]]
                 g == [tl |-> [t \in Streams |->
                                 IF t \in SealedSet THEN MkTail(WinOf(t).u, app[t].n + Len(own[t]))
                                 ELSE NoTail],
                       pd |-> [t \in Streams |->
                                 IF t \in SealedSet
                                 THEN IF rem[t] = <<>> THEN [t |-> "del", pg |-> <<>>]
                                      ELSE [t |-> "put", pg |-> rem[t]]
                                 ELSE NoOp],
                       wi |-> [t \in Streams |->
                                 IF t \in SealedSet
                                 THEN RemainderIndexOp(app[t], [a |-> WinOf(t).u,
                                                                n |-> app[t].n + Len(own[t])])
                                 ELSE "none"],
                       k  |-> Entries(sl),
                       ap |-> own]
             IN /\ wal' = Append(wal, g)
                /\ app' = [t \in Streams |-> IF t \in SealedSet
                                             THEN [a |-> WinOf(t).u, n |-> app[t].n + Len(own[t])]
                                             ELSE app[t]]
                /\ pend' = [t \in Streams |-> IF t \in SealedSet
                                              THEN From(pend[t] \o own[t], WinOf(t).u)
                                              ELSE pend[t]]
                /\ late' = (late \/ \E w \in sl.w : Len(From(pend[w.s], w.u)) > Len(w.rem))
                /\ sl' = [sl EXCEPT !.ph = "applied", !.g = Len(wal) + 1]
          /\ UNCHANGED <<dir, refused, cnt>>
    /\ UNCHANGED <<dur, pub, dtl, inc, objs, mstart, acked, f2, dl0>>

(* The seal fails after its group applied (its memtable flush fails, or a  *)
(* test abort): the entries stay, the boundary moved in memory.            *)
AbortApplied ==
    /\ sl.ph = "applied"
    /\ cnt.abort < MaxAborts
    /\ sl' = Idle
    /\ cnt' = [cnt EXCEPT !.abort = @ + 1]
    /\ UNCHANGED <<wal, dur, pub, app, dtl, pend, inc, dir, objs, mstart, acked,
                   f2, dl0, late, refused>>

(* (7b) One forced memtable flush under the flush lock, as soon as the     *)
(* commit applied: the code waits for dispatch first (7a), a settlement   *)
(* contract F2 does not rest on, so the model lets the flush overtake it.  *)
(* The flush publishes once its WAL prefix is durable.                     *)
Seal7b ==
    /\ sl.ph \in {"applied", "flushed"}
    /\ f2' = f2 \cup UNION {{<<w.s, p.o, w.u>> : p \in L0Pages(w.s, FlushBoundary)} : w \in sl.w}
    /\ mstart' = Len(wal)
    /\ dur' = Len(wal)
    /\ sl' = Idle
    /\ UNCHANGED <<wal, pub, app, dtl, pend, inc, dir, objs, acked, dl0, late,
                   refused, cnt>>

(* A deadline flush: one forced memtable flush under the flush lock, at any *)
(* point of a seal (it may race the seal's commit).                        *)
Deadline ==
    /\ cnt.dead < MaxDeadlines
    /\ LET F == Fold(wal) IN
       dl0' = dl0 \cup UNION {{<<s, q.o>> : q \in {r \in L0Pages(s, FlushBoundary) : r.o < F[s].a}} : s \in Streams}
    /\ mstart' = Len(wal)
    /\ dur' = Len(wal)
    /\ cnt' = [cnt EXCEPT !.dead = @ + 1]
    /\ UNCHANGED <<wal, pub, app, dtl, pend, inc, dir, objs, sl, acked, f2, late,
                   refused>>

(* A crash and the restart: the durable prefix survives; the directory is  *)
(* rebuilt from its 'K' rows and each stream the durable 'W' rows name     *)
(* loads its tail and pending row: the held set comes from 'W' alone.      *)
CrashRestart ==
    /\ cnt.crash < MaxCrashes
    /\ LET w2 == Durable
           F == Fold(w2)
       IN /\ wal' = w2
          /\ pub' = dur
          /\ app' = [s \in Streams |-> [a |-> F[s].a, n |-> F[s].n]]
          /\ dtl' = [s \in Streams |-> [a |-> F[s].a, n |-> F[s].n]]
          /\ pend' = [s \in Streams |-> IF FoldW(w2)[s] THEN F[s].row ELSE <<>>]
          /\ dir' = KRows(w2)
    /\ inc' = inc + 1
    /\ sl' = Idle
    /\ mstart' = Min(mstart, dur)
    /\ cnt' = [cnt EXCEPT !.crash = @ + 1]
    /\ UNCHANGED <<dur, objs, acked, f2, dl0, late, refused>>

Quiet == sl.ph = "idle" /\ pub = dur
Terminated == Quiet /\ UNCHANGED vars

Next ==
    \/ \E s \in Streams : AppendRec(s)
    \/ WalFlush \/ Dispatch
    \/ Snapshot \/ Put \/ PutFails \/ Publish
    \/ \E A \in SUBSET Streams : Commit(A)
    \/ AbortApplied \/ Seal7b
    \/ Deadline
    \/ CrashRestart
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants.                                                             *)

TypeOK ==
    /\ dur \in 0..Len(wal) /\ pub \in 0..dur /\ mstart \in 0..Len(wal)
    /\ sl.ph \in {"idle", "snap", "put", "pub", "applied", "flushed"}
    /\ \A s \in Streams : /\ app[s].a <= app[s].n /\ app[s].n <= N
                          /\ dtl[s].a <= dtl[s].n /\ dtl[s].n <= app[s].n

\* (I1) Every acknowledged record is readable after a crash and restart,
\* exactly once and in its place: below the durable boundary from exactly
\* one durable directory chunk whose object holds it, at or above it from
\* its position in the durable pending row.
I1_AckedReadableAfterRestart ==
    LET F == Fold(Durable)
        K == KRows(Durable)
    IN \A x \in acked :
        LET s == x[1]
            p == x[2]
            o == p.o
        IN /\ o < F[s].n
           /\ IF o < F[s].a
              THEN LET C == Covering(K, s, o) IN
                   /\ Cardinality(C) = 1
                   /\ \A c \in C : Cardinality(ObjOf(c)) = 1 /\ ObjPage(c, o) = p
              ELSE LET i == o - F[s].a + 1 IN
                   /\ i <= Len(F[s].row)
                   /\ F[s].row[i] = p

\* (I2) The pending row tiles [absorbed, next) after every applied group
\* (every durable prefix was once the whole applied sequence, so this
\* covers every state a restart can load).
I2_PendingRowTiles ==
    LET F == Fold(wal) IN
    \A s \in Streams : Tiles(F[s].row, F[s].a, F[s].n)

\* (I3) The directory rows tile [0, absorbed) exactly, each naming a PUT
\* object, after every applied group.
I3_DirectoryTiles ==
    LET F == Fold(wal)
        K == KRows(wal)
    IN /\ \A s \in Streams, o \in Offs :
            o < F[s].a => Cardinality(Covering(K, s, o)) = 1
       /\ \A c \in K : c.l < F[c.s].a /\ Cardinality(ObjOf(c)) = 1

\* The handle's memory is the applied store: tail and pending pages.
MemoryMirrorsStore ==
    LET F == Fold(wal) IN
    \A s \in Streams : /\ app[s].a = F[s].a /\ app[s].n = F[s].n
                       /\ pend[s] = F[s].row

\* The in-memory directory covers [0, applied.absorbed) exactly; an entry
\* above the boundary is the in-flight seal's, published before its commit.
MemoryDirectoryCovers ==
    /\ \A s \in Streams, o \in Offs :
        o < app[s].a => Cardinality(Covering(dir, s, o)) = 1
    /\ \A c \in dir : c.l < app[c.s].a \/ (sl.ph = "pub" /\ c.id = sl.id)
    /\ \A c \in dir : Cardinality(ObjOf(c)) = 1

\* (W) The unsealed index names exactly the streams whose pending row holds
\* a page, after every applied group (so after every restart too: every
\* durable prefix was once the applied sequence).
W_IndexMatchesPending ==
    LET F == Fold(wal)
        W == FoldW(wal)
    IN \A s \in Streams : W[s] = (F[s].row # <<>>)

\* (F2) A seal's own forced flush writes no page that seal sealed to an L0.
F2_SealFlushLeavesNoSealedPage ==
    \A x \in f2 : x[2] >= x[3]

\* (F2) Nor does a deadline flush, even one that races a seal's commit: the
\* remainder put shadows the sealed pages below the flush's boundary.
F2_DeadlineFlushLeavesNoSealedPage == dl0 = {}

-----------------------------------------------------------------------------
(* Reachability witnesses (each must be violated on the baseline).         *)

\* A seal's commit became durable and dispatched.
Witness_SealDurable == ~\E s \in Streams : dtl[s].a > 0

\* A seal's live remainder carried a page applied after its snapshot (the
\* F1 case the prebuilt remainder loses).
Witness_RemainderCarriesLatePage == ~late

\* A durable seal left a non-empty pending row that a restart loaded.
Witness_CrashAfterSealRecovered ==
    ~(cnt.crash >= 1 /\ \E s \in Streams : 0 < app[s].a /\ app[s].a < app[s].n
                                           /\ \E x \in acked : x[1] = s /\ x[2].o >= app[s].a)

\* A seal's group also appended to the stream it sealed: the remainder put
\* carries the group's own page.
Witness_SealGroupCarriesAppend ==
    ~\E i \in 1..Len(wal) : wal[i].k # {} /\ \E s \in Streams : wal[i].ap[s] # <<>>

\* The committer refused a seal: unreachable in this scope (the refusal is a
\* defensive check, README.md), so no check uses this definition.
Witness_SealRefused == ~refused
=============================================================================
