------------------------------ MODULE SealRead ------------------------------
(***************************************************************************)
(* TLA-018 (layout 6A retarget) -- a stream read composed of the ring, the *)
(* seal directory's chunks and the handle's unsealed pages, under the      *)
(* durable and applied visibility rules.                                   *)
(*                                                                         *)
(* One stream of one shard; a client pages through it from offset 0 with   *)
(* the production read (application/read.rs execute_segment ->            *)
(* shard/seal/read.rs read_stream_window):                                 *)
(*   R1 (ONE state lock, TLA-018-F4): P = applied.absorbed, end =        *)
(*      durable.next (durable) or max(applied.next, durable.next)          *)
(*      (applied); to = min(from + window, end); and a capture of the      *)
(*      pending pages from max(from, P) below `to`, at most the budget     *)
(*      (unsealed_pages: max_bytes plus the page that reaches it);         *)
(*   sealed part [from, min(to, P)): the ring when it holds the whole      *)
(*      part (tail_ring.rs ring_read), else the directory's chunks from    *)
(*      the one holding `from` (plan_sealed + read_sealed): a chunk the    *)
(*      key summary excludes is consumed without a GET, a fetched chunk's  *)
(*      pages are filtered by key, and an offset no chunk holds ends the   *)
(*      read short (the honest partial: nothing served or claimed past     *)
(*      the gap); a part that did not reach its end, or spent the budget,  *)
(*      is the whole answer;                                               *)
(*   unsealed part [max(from, P), to): the pages R1 captured             *)
(*      (read_unsealed): a page that does not continue the offsets is      *)
(*      RecordCorruption::Misplaced, an end short of `to`                  *)
(*      RecordCorruption::Missing.                                         *)
(* Every step is atomic; writers interleave between them.                  *)
(*                                                                         *)
(* The writer: appends (applied, then durable in order through dispatch,   *)
(* which also fills the ring), ring eviction, and the sealer (snapshot of  *)
(* the durable unsealed pages, object PUT with per-chunk key summaries     *)
(* that may over-approximate, directory publication BEFORE the commit,     *)
(* the commit at applied visibility that moves applied.absorbed and drops  *)
(* the pages below it from memory, then durability).  No crash, refusal    *)
(* or GC: TLA-016 (SealSpill.tla) owns durability and the commit's         *)
(* tiling, and objects are immutable once PUT (PutMode::Create).           *)
(*                                                                         *)
(* Records are one-record pages; offset o carries routing key KeyOf[o].    *)
(* The budget: each read draws a page budget 1..MaxBudget at its start     *)
(* (the byte budget, the plan's GET budget and the chunk caps all stop a   *)
(* read early; every such stop is a budget stop here, `cut`).              *)
(*                                                                         *)
(* Mutation points (MC_SealRead.tla): PublishEarly (late directory),       *)
(* HoleConsumed (a missing chunk treated as consumed), Summary (an         *)
(* under-approximating key summary), UnsealedSource (the pre-F4 second    *)
(* lock acquisition, see README.md).                                       *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    N,              \* offsets 0..N-1
    Keys,           \* routing keys
    KeyOf,          \* [0..N-1 -> Keys]
    Filter,         \* "none" (unfiltered) or one routing key
    Mode,           \* "durable" | "applied"
    Window,         \* offsets one read scans at most (SCAN_WINDOW)
    MaxBudget,      \* largest page budget a read draws
    ChunkPages,     \* pages per chunk at most (max_pages_per_chunk)
    MaxSeals, MaxReads,
    AllowRing,      \* the durable tail ring serves windows it holds
    FalsePositive   \* a chunk summary may admit a key the chunk lacks (bloom)

ASSUME /\ N \in Nat \ {0} /\ Mode \in {"durable", "applied"}
       /\ Window \in Nat \ {0} /\ MaxBudget \in Nat \ {0} /\ ChunkPages \in Nat \ {0}
       /\ MaxSeals \in Nat /\ MaxReads \in Nat
       /\ AllowRing \in BOOLEAN /\ FalsePositive \in BOOLEAN

Offs == 0..(N-1)
Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y
Match(o) == Filter = "none" \/ KeyOf[o] = Filter
\* The matching offsets of [a, b], in order.
RECURSIVE Matching(_, _)
Matching(a, b) == IF a > b THEN <<>>
                  ELSE (IF Match(a) THEN <<a>> ELSE <<>>) \o Matching(a + 1, b)

VARIABLES
    nx,     \* applied.next
    aa,     \* applied.absorbed (P); the handle's pending pages are [aa, nx)
    q,      \* applied tails not yet durable: Seq([a, n])
    dtl,    \* durable tail [a, n]
    ring,   \* the ring holds the durable pages [f, c)
    dir,    \* the in-memory directory: set of chunks [f, l, keys]
    sl,     \* the sealer
    rd,     \* the reader's current read
    cursor, \* the client's next read position
    dlv,    \* every record delivered so far, in order
    reads,  \* reads finished
    seals   \* seals started

vars == <<nx, aa, q, dtl, ring, dir, sl, rd, cursor, dlv, reads, seals>>

RdIdle == [ph |-> "idle", c0 |-> 0, P |-> 0, end |-> 0, to |-> 0, bud |-> 1,
           due |-> 0, srv |-> <<>>, last |-> -1, insp |-> 0, cut |-> FALSE,
           err |-> "none", src |-> {}, sa |-> 0, sn |-> 0]
SlIdle == [ph |-> "idle", f |-> 0, u |-> 0, ch |-> {}]

-----------------------------------------------------------------------------
(* Mutation points.                                                        *)

\* Step 5 precedes step 6: the chunks are in the directory before the
\* commit can move any boundary.
PublishEarly == TRUE

\* A window offset no chunk holds below the boundary ends the read short.
HoleConsumed == FALSE

\* A chunk's key summary: its keys, or (a bloom) a superset of them.
ActualKeys(c) == {KeyOf[o] : o \in c.f..c.l}
Summary(c, fp) == IF fp THEN Keys ELSE ActualKeys(c)

\* The pages the unsealed part reads: the pages [sa, sn) R1 captured with
\* its boundary under the same lock (TLA-018-F4 fix).
UnsealedSource == [a |-> rd.sa, n |-> rd.sn]

-----------------------------------------------------------------------------
(* The read's walks.  A walk state is the reader record; `due` is the next *)
(* offset owed.                                                            *)

Take(r, o) == [r EXCEPT !.due = o + 1, !.last = o, !.insp = @ + 1,
                        !.srv = IF Match(o) THEN Append(@, o) ELSE @,
                        !.cut = (r.insp + 1 >= r.bud)]

\* Dense pages [due, b] from one source (a fetched chunk, or the ring),
\* stopping at the budget.
RECURSIVE TakeTo(_, _)
TakeTo(r, b) == IF r.cut \/ r.due > b THEN r ELSE TakeTo(Take(r, r.due), b)

\* The directory part of [due, upto) (plan_sealed + read_sealed).
RECURSIVE DirWalk(_, _, _)
DirWalk(r, upto, D) ==
    IF r.cut \/ r.due >= upto THEN r
    ELSE LET Cov == {c \in D : c.f <= r.due /\ r.due <= c.l} IN
         IF Cov = {}
         THEN IF HoleConsumed
              THEN LET nxt == SetMin({c.f : c \in {d \in D : d.f > r.due}} \cup {upto})
                   IN DirWalk([r EXCEPT !.due = nxt, !.last = nxt - 1], upto, D)
              ELSE r    \* the honest partial
         ELSE LET c == CHOOSE x \in Cov : TRUE
                  th == Min(c.l, upto - 1)
              IN IF Filter # "none" /\ Filter \notin c.keys
                 THEN DirWalk([r EXCEPT !.due = th + 1, !.last = th, !.src = @ \cup {"skip"}],
                              upto, D)
                 ELSE DirWalk(TakeTo([r EXCEPT !.src = @ \cup {"dir"}], th), upto, D)

\* The unsealed part (read_unsealed) over pending pages [A, Nn): the pages
\* at or above the part's start, oldest first.
RECURSIVE PendWalk(_, _, _, _)
PendWalk(r, o, Nn, to) ==
    IF o >= Nn \/ o >= to
    THEN IF r.due # to THEN [r EXCEPT !.err = "missing"] ELSE r
    ELSE IF o # r.due THEN [r EXCEPT !.err = "misplaced"]
    ELSE LET r2 == Take([r EXCEPT !.src = @ \cup {"pend"}], o) IN
         IF r2.cut THEN r2 ELSE PendWalk(r2, o + 1, Nn, to)

-----------------------------------------------------------------------------
Init ==
    /\ nx = 0 /\ aa = 0 /\ q = <<>>
    /\ dtl = [a |-> 0, n |-> 0]
    /\ ring = [f |-> 0, c |-> 0]
    /\ dir = {}
    /\ sl = SlIdle
    /\ rd = RdIdle
    /\ cursor = 0 /\ dlv = <<>> /\ reads = 0 /\ seals = 0

AppendW ==
    /\ nx < N
    /\ nx' = nx + 1
    /\ q' = Append(q, [a |-> aa, n |-> nx + 1])
    /\ UNCHANGED <<aa, dtl, ring, dir, sl, rd, cursor, dlv, reads, seals>>

(* Durable dispatch publishes the next durable tail; the ring takes the    *)
(* newly durable pages.                                                    *)
Dispatch ==
    /\ q # <<>>
    /\ dtl' = Head(q)
    /\ q' = Tail(q)
    /\ ring' = IF AllowRing THEN [ring EXCEPT !.c = Head(q).n] ELSE [f |-> Head(q).n, c |-> Head(q).n]
    /\ UNCHANGED <<nx, aa, dir, sl, rd, cursor, dlv, reads, seals>>

RingEvict ==
    /\ AllowRing /\ ring.f < ring.c
    /\ \E f \in (ring.f + 1)..ring.c : ring' = [ring EXCEPT !.f = f]
    /\ UNCHANGED <<nx, aa, q, dtl, dir, sl, rd, cursor, dlv, reads, seals>>

Chunks(f, u) == {[f |-> f + i * ChunkPages, l |-> Min(f + (i + 1) * ChunkPages, u) - 1]
                 : i \in 0..((u - f - 1) \div ChunkPages)}

(* (2) Snapshot of the durable unsealed pages, (3)(4) the object's PUT     *)
(* with each chunk's summary (one step: nothing reads in between).        *)
SnapshotPut ==
    /\ sl.ph = "idle" /\ seals < MaxSeals
    /\ dtl.a < dtl.n /\ aa = dtl.a
    /\ \E fp \in [Chunks(dtl.a, dtl.n) -> IF FalsePositive THEN BOOLEAN ELSE {FALSE}] :
         sl' = [ph |-> "put", f |-> dtl.a, u |-> dtl.n,
                ch |-> {[f |-> c.f, l |-> c.l, keys |-> Summary(c, fp[c])] : c \in Chunks(dtl.a, dtl.n)}]
    /\ seals' = seals + 1
    /\ UNCHANGED <<nx, aa, q, dtl, ring, dir, rd, cursor, dlv, reads>>

(* (5) Publication in the directory. *)
Publish ==
    /\ sl.ph = "put"
    /\ dir' = IF PublishEarly THEN dir \cup sl.ch ELSE dir
    /\ sl' = [sl EXCEPT !.ph = "pub"]
    /\ UNCHANGED <<nx, aa, q, dtl, ring, rd, cursor, dlv, reads, seals>>

(* (6) The commit, applied: the boundary moves, memory drops the pages     *)
(* below it.                                                               *)
Commit ==
    /\ sl.ph = "pub"
    /\ aa = sl.f /\ sl.u <= nx
    /\ aa' = sl.u
    /\ q' = Append(q, [a |-> sl.u, n |-> nx])
    /\ sl' = [sl EXCEPT !.ph = "applied"]
    /\ UNCHANGED <<nx, dtl, ring, dir, rd, cursor, dlv, reads, seals>>

(* (7a) The seal is durable and published (the late-directory control      *)
(* publishes its chunks only now).                                         *)
SealDurable ==
    /\ sl.ph = "applied" /\ dtl.a >= sl.u
    /\ dir' = IF PublishEarly THEN dir ELSE dir \cup sl.ch
    /\ sl' = SlIdle
    /\ UNCHANGED <<nx, aa, q, dtl, ring, rd, cursor, dlv, reads, seals>>

-----------------------------------------------------------------------------
(* The reader.                                                             *)

VisEnd == IF Mode = "durable" THEN dtl.n ELSE Max(nx, dtl.n)

ReadStart ==
    /\ rd.ph = "idle" /\ reads < MaxReads
    /\ \E b \in 1..MaxBudget :
       LET to == Min(cursor + Window, VisEnd)
           start == Max(cursor, aa)
           r == [RdIdle EXCEPT !.c0 = cursor, !.due = cursor, !.P = aa, !.end = VisEnd,
                               !.to = to, !.bud = b,
                               !.sa = start, !.sn = Max(start, Min(Min(to, nx), start + b))]
       IN rd' = [r EXCEPT !.ph = IF cursor >= to THEN "done"
                                  ELSE IF cursor < aa THEN "sealed" ELSE "unsealed"]
    /\ UNCHANGED <<nx, aa, q, dtl, ring, dir, sl, cursor, dlv, reads, seals>>

RingHolds(from, upto) == AllowRing /\ ring.f <= from /\ upto <= ring.c /\ upto <= dtl.n

ReadSealed ==
    /\ rd.ph = "sealed"
    /\ LET upto == Min(rd.to, rd.P)
           r == IF RingHolds(rd.due, upto)
                THEN TakeTo([rd EXCEPT !.src = @ \cup {"ring"}], upto - 1)
                ELSE DirWalk(rd, upto, dir)
       IN rd' = [r EXCEPT !.ph = IF r.due < upto \/ r.cut THEN "done" ELSE "unsealed"]
    /\ UNCHANGED <<nx, aa, q, dtl, ring, dir, sl, cursor, dlv, reads, seals>>

ReadUnsealed ==
    /\ rd.ph = "unsealed"
    /\ LET src == UnsealedSource
           r0 == IF aa > rd.P THEN [rd EXCEPT !.src = @ \cup {"raced"}] ELSE rd
           r == PendWalk(r0, Max(src.a, rd.due), src.n, rd.to)
       IN rd' = [r EXCEPT !.ph = "done"]
    /\ UNCHANGED <<nx, aa, q, dtl, ring, dir, sl, cursor, dlv, reads, seals>>

(* The client takes the page: on success it advances past the consumed     *)
(* offsets; on an error it retries from the same cursor.                   *)
ReadFinish ==
    /\ rd.ph = "done"
    /\ IF rd.err = "none"
       THEN /\ dlv' = dlv \o rd.srv
            /\ cursor' = IF rd.last >= 0 THEN rd.last + 1 ELSE cursor
       ELSE UNCHANGED <<dlv, cursor>>
    /\ rd' = RdIdle
    /\ reads' = reads + 1
    /\ UNCHANGED <<nx, aa, q, dtl, ring, dir, sl, seals>>

Quiet == rd.ph = "idle" /\ sl.ph = "idle"
Terminated == Quiet /\ UNCHANGED vars

Next ==
    \/ AppendW \/ Dispatch \/ RingEvict
    \/ SnapshotPut \/ Publish \/ Commit \/ SealDurable
    \/ ReadStart \/ ReadSealed \/ ReadUnsealed \/ ReadFinish
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants, judged on a finished read (rd.ph = "done") and across reads.*)

Done == rd.ph = "done"
OK == Done /\ rd.err = "none"

TypeOK ==
    /\ aa <= nx /\ nx <= N /\ dtl.a <= dtl.n /\ dtl.n <= nx
    /\ ring.f <= ring.c /\ ring.c <= dtl.n
    /\ rd.ph \in {"idle", "sealed", "unsealed", "done"}
    /\ rd.err \in {"none", "misplaced", "missing"}

\* A read never serves a record past its visibility end: below the end it
\* read under the lock, and, for a durable read, durable.
VisibleOnly ==
    Done => \A i \in DOMAIN rd.srv :
                /\ rd.srv[i] < rd.end
                /\ (Mode = "durable" => rd.srv[i] < dtl.n)

\* A read never skips a record: what it served is exactly the matching
\* records of [cursor, last], so every offset it claims as consumed it
\* served or legitimately passed over (another key); an incomplete read
\* claims nothing past what it inspected.
NoSkip ==
    OK => /\ rd.last < rd.to
          /\ rd.srv = Matching(rd.c0, rd.last)

\* A read that did not stop for its budget reaches its end: below the
\* boundary the directory never has a hole (the honest partial is never
\* needed while step 5 precedes step 6).
NoShortRead ==
    (OK /\ ~rd.cut) => (rd.c0 >= rd.to \/ rd.last + 1 = rd.to)

\* Across reads, the client has received exactly the matching records of
\* [0, cursor), once each, in order.
ExactDeliveredPrefix ==
    dlv = Matching(0, cursor - 1)

\* No read fails on a healthy store: a seal committing after R1 removes
\* only pages R1 already captured (TLA-018-F4).
NoReadError ==
    Done => rd.err = "none"

-----------------------------------------------------------------------------
(* Reachability witnesses (each must be violated on the baseline).         *)

\* One read served the sealed part from the directory and the rest from
\* the unsealed pages.
Witness_ReadSpansBoundary == ~(OK /\ {"dir", "pend"} \subseteq rd.src)

\* A sealed part served from the ring.
Witness_RingServed == ~(OK /\ "ring" \in rd.src)

\* A keyed read consumed a chunk its summary excludes, without a GET.
Witness_SummarySkip == ~(OK /\ "skip" \in rd.src)

\* A seal committed between a read's R1 and its unsealed part (the
\* TLA-018-F4 race), and the read still succeeded from its capture.
Witness_UnsealedReadRacesSeal == ~(OK /\ "raced" \in rd.src /\ "pend" \in rd.src)
=============================================================================
