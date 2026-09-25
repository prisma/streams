------------------------------- MODULE ReachGC -------------------------------
(***************************************************************************)
(* TLA-019 -- reachability-based garbage collection and reader protection  *)
(* for one shared history-v2 partition.                                    *)
(*                                                                         *)
(* PHYSICAL layer (integration protocol; upstream SlateDB GC is an ASSUMED *)
(* INTERFACE, not source verified here).  A tiny object graph: L1 (a       *)
(* committed L0 holding offset 0), L2 (a later L0 flush holding offset 1:  *)
(* upload, then manifest commit), C (a compaction output), O (an L0        *)
(* uploaded by a FENCED old writer whose manifest CAS fails).  The         *)
(* collector is the pinned upstream contract (garbage_collector/           *)
(* compacted_gc.rs at rev 0717cc1): read the compactions low watermark,    *)
(* THEN the manifest (+ checkpoint manifests), THEN LIST, then delete an   *)
(* SST iff it is unreferenced and its id time is below                     *)
(* cutoff = min(now - min_age, low watermark, newest L0 of the manifest).  *)
(* Discovery, reachability observation, eligibility and deletion are       *)
(* separate steps.  Before its manifest commit the compactor writes a      *)
(* checkpoint on the pre-compaction manifest with a fixed lifetime         *)
(* (compactor_state_protocols.rs write_manifest, 900 s); the collector     *)
(* drops expired checkpoints at the start of each task and treats every    *)
(* SST an unexpired checkpoint's manifest names as live.                   *)
(*                                                                         *)
(* REPOSITORY-OWNED decisions (each has a mutation point):                 *)
(*   * the absorbed boundary advances only after the covering flush is in  *)
(*     the manifest (Absorber::commit awaits part.flush()) -- AdvanceBacked;*)
(*   * history reads go through the open writer Db's IN-MEMORY manifest    *)
(*     view `wv` (no checkpoint, no DbReader), which the writer refreshes  *)
(*     only on its own manifest writes or every manifest_poll_interval     *)
(*     (300 s, src/history.rs history_settings), NOT when the embedded     *)
(*     compactor commits -- ReadBegin/WriterPoll.  The view is refreshed   *)
(*     within PollInterval of going stale, and a read ends within          *)
(*     ReadSpan of starting (ASM-SLATEDB-COMPACTION-CHECKPOINT);           *)
(*   * a read that fails on a deleted SST is propagated as an error        *)
(*     (decode_history_range `map_err(|e| e.to_string())?`) --             *)
(*     RepoOnDeletedRead.                                                   *)
(* The upstream read of a deleted SST (ASM-SLATEDB-GC (iii)) is the        *)
(* separate assumption point UpstreamDeletedRead.                          *)
(*                                                                         *)
(* The fork pin (registry-level retention) shares no state with this graph *)
(* and is checked in ForkPin.tla (decomposition by invariant ownership).   *)
(*                                                                         *)
(* Mutation points: GcEligible, GcRefs, GcInventory, CompactionCheckpoint  *)
(* (upstream contract); AdvanceBacked, RepoOnDeletedRead (repository);     *)
(* UpstreamDeletedRead (assumption probe).                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MinAge,        \* GarbageCollectorDirectoryOptions.min_age, in ticks
    Horizon,       \* last tick at which writers/pins may act
    MaxTime,       \* clock bound (>= Horizon + MinAge + 1 so objects can age)
    CkLife,        \* lifetime of the compactor's checkpoint, in ticks
    PollInterval,  \* the writer view is refreshed within this many ticks of going stale
    ReadSpan,      \* a history read ends within this many ticks of starting
    UseCheckpoint, \* model a user checkpoint pin (the repository creates none)
    AllowOrphan,   \* a fenced old writer uploads an L0 that is never committed
    AllowReader    \* a production read through the writer's in-memory view

ASSUME /\ MinAge \in Nat /\ Horizon \in Nat /\ MaxTime \in Nat
       /\ MaxTime >= Horizon + MinAge + 1
       /\ CkLife \in Nat /\ PollInterval \in Nat \ {0} /\ ReadSpan \in Nat \ {0}
       /\ UseCheckpoint \in BOOLEAN /\ AllowOrphan \in BOOLEAN /\ AllowReader \in BOOLEAN

Obj == {"L1", "L2", "C", "O"}
L0 == {"L1", "L2", "O"}
Never == MaxTime + 1
NoCk == [refs |-> {}, exp |-> Never]
Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x
SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y

VARIABLES
    now,
    st,            \* [Obj -> "none" | "present" | "deleted"]
    gen,           \* SST id time (ULID) = upload tick
    cov,           \* offsets whose history rows the object holds
    man,           \* objects referenced by the live (stored) manifest
    committed,     \* objects ever committed to the manifest
    lastCompL0,    \* id time of the newest compacted L0 (manifest fallback)
    job,           \* running compaction: [active, start, inputs]
    done,          \* [has, start]: most recent completed compaction
    abs,           \* history absorbed boundary of the one stream (0..2)
    ck,            \* checkpoint pin: [active, used, refs]
    cck,           \* the compactor's checkpoint: [refs, exp] (exp = Never: none)
    wv,            \* the writer Db's in-memory manifest view (what reads use)
    stale,         \* tick at which wv last became stale (Never: current)
    rv,            \* production read: [active, view, out, start]
    gc,            \* collector pass: [ph, age, cut, refs, inv]
    gcLast,        \* tick of the most recently started collector pass (Never)
    firstInv       \* ghost: the first inventory ever listed (stale-inventory NC)

vars == <<now, st, gen, cov, man, committed, lastCompL0, job, done, abs, ck, cck, wv, stale,
          rv, gc, gcLast, firstInv>>
storeVars == <<st, gen, cov, man, committed, lastCompL0, job, done>>

-----------------------------------------------------------------------------
LowWatermark ==      \* CompactedGcTask::compaction_low_watermark_dt
    LET S == (IF job.active THEN {job.start} ELSE {})
             \cup (IF done.has THEN {done.start} ELSE {})
    IN IF S = {} THEN 0 ELSE SetMin(S)
NewestL0 ==          \* newest_l0_dt over the live manifest
    IF man \cap L0 # {} THEN SetMax({gen[x] : x \in man \cap L0}) ELSE lastCompL0
AgeCut(t) == IF t >= MinAge THEN t - MinAge ELSE 0
\* cutoff_dt = min(now - min_age, compaction low watermark, newest L0); the
\* pass records min(age cut, low watermark) when it reads the compactions
\* store and folds in the newest L0 when it reads the manifest.
Cutoff == gc.cut

(* Mutation points (baseline definitions).                                  *)
\* Upstream contract (ASM-SLATEDB-GC (i)).
GcEligible(x) == gen[x] < Cutoff /\ x \notin gc.refs
\* The manifests of the unexpired checkpoints (a checkpoint expires when
\* its expiry tick is reached; checking it at GcReadManifest rather than at
\* the pass start only makes the collector delete earlier).
CheckpointRefs ==
    (IF UseCheckpoint /\ ck.active THEN ck.refs ELSE {})
    \cup (IF now < cck.exp THEN cck.refs ELSE {})
GcRefs == man \cup CheckpointRefs
\* Upstream (ASM-SLATEDB-COMPACTION-CHECKPOINT): the compactor checkpoints
\* the pre-compaction manifest before it commits the compaction.
CompactionCheckpoint(m) == m
GcInventory == {x \in Obj : st[x] = "present"}
\* Repository: the committer advances `absorbed` only after part.flush()
\* returned Ok, i.e. after the covering L0 is in the manifest.
AdvanceBacked(x) == x \in man
\* Upstream (ASM-SLATEDB-GC (iii)): a read that needs a deleted SST fails;
\* NotFound maps to SlateDBError::ObjectStoreError -> Error::data.
UpstreamDeletedRead == "error"
\* Repository: decode_history_range propagates the storage error.
RepoOnDeletedRead(r) == r

-----------------------------------------------------------------------------
Init ==
    /\ now = 0
    /\ st = [x \in Obj |-> IF x = "L1" THEN "present" ELSE "none"]
    /\ gen = [x \in Obj |-> 0]
    /\ cov = [x \in Obj |-> IF x = "L1" THEN {0} ELSE {}]
    /\ man = {"L1"} /\ committed = {"L1"}
    /\ lastCompL0 = 0
    /\ job = [active |-> FALSE, start |-> 0, inputs |-> {}]
    /\ done = [has |-> FALSE, start |-> 0]
    /\ abs = 0
    /\ ck = [active |-> FALSE, used |-> FALSE, refs |-> {}]
    /\ cck = NoCk
    /\ wv = {"L1"}
    /\ stale = Never
    /\ rv = [active |-> FALSE, view |-> {}, out |-> "none", start |-> 0]
    /\ gc = [ph |-> "idle", age |-> 0, cut |-> 0, refs |-> {}, inv |-> {}]
    /\ gcLast = Never
    /\ firstInv = [set |-> FALSE, inv |-> {}]

\* Time advances only as the timing assumptions allow: the writer view is
\* refreshed within PollInterval ticks of going stale, and a read ends
\* within ReadSpan ticks of starting.
Tick ==
    /\ now < MaxTime
    /\ stale = Never \/ now < stale + PollInterval
    /\ ~rv.active \/ now < rv.start + ReadSpan
    /\ now' = now + 1
    \* An expired compactor checkpoint protects nothing (CheckpointRefs);
    \* forgetting it only merges states.
    /\ cck' = IF cck.exp <= now + 1 THEN NoCk ELSE cck
    /\ UNCHANGED <<storeVars, abs, ck, wv, stale, rv, gc, gcLast, firstInv>>

-----------------------------------------------------------------------------
(* Writer of the history partition (flush / compaction) and fenced writer  *)

FlushUpload ==       \* memtable flush uploads the L0 SST (id time = now)
    /\ now <= Horizon /\ st["L2"] = "none"
    /\ st' = [st EXCEPT !["L2"] = "present"]
    /\ gen' = [gen EXCEPT !["L2"] = now]
    /\ cov' = [cov EXCEPT !["L2"] = {1}]
    /\ UNCHANGED <<now, man, committed, lastCompL0, job, done, abs, ck, cck, wv, stale, rv,
                   gc, gcLast, firstInv>>

\* The writer's manifest write for its own flush (no existence re-check).
\* A write conflict with the compactor's newer manifest makes the writer
\* load and merge the stored manifest first (manifest_writer.rs
\* write_manifest_update_safely), so its in-memory view is current after.
FlushCommit ==
    /\ now <= Horizon /\ st["L2"] # "none" /\ "L2" \notin committed
    /\ man' = man \cup {"L2"} /\ committed' = committed \cup {"L2"}
    /\ wv' = IF AllowReader THEN man \cup {"L2"} ELSE wv
    /\ stale' = Never
    /\ UNCHANGED <<now, st, gen, cov, lastCompL0, job, done, abs, ck, cck, rv,
                   gc, gcLast, firstInv>>

CompactStart ==      \* compactor records the job (low watermark) first
    /\ now <= Horizon /\ ~job.active /\ st["C"] = "none" /\ man \cap L0 # {}
    /\ job' = [active |-> TRUE, start |-> now, inputs |-> man \cap L0]
    /\ UNCHANGED <<now, st, gen, cov, man, committed, lastCompL0, done, abs, ck, cck, wv, stale, rv,
                   gc, gcLast, firstInv>>

CompactUpload ==
    /\ now <= Horizon /\ job.active /\ st["C"] = "none"
    /\ st' = [st EXCEPT !["C"] = "present"]
    /\ gen' = [gen EXCEPT !["C"] = now]
    /\ cov' = [cov EXCEPT !["C"] = UNION {cov[x] : x \in job.inputs}]
    /\ UNCHANGED <<now, man, committed, lastCompL0, job, done, abs, ck, cck, wv, stale, rv,
                   gc, gcLast, firstInv>>

\* The embedded compactor first checkpoints the stored (pre-compaction)
\* manifest with a CkLife lifetime, then swaps inputs for the output in the
\* STORED manifest (compactor_state_protocols.rs write_manifest).  Merging
\* the two writes into one step only removes states in which the live
\* manifest itself still protects the inputs.  The writer Db's in-memory
\* view is not told (no coupling in compactor.rs); it learns at its next
\* poll or manifest write.
CompactCommit ==
    /\ now <= Horizon /\ job.active /\ st["C"] # "none"
    /\ cck' = [refs |-> CompactionCheckpoint(man), exp |-> now + CkLife]
    /\ man' = (man \ job.inputs) \cup {"C"}
    /\ committed' = committed \cup {"C"}
    /\ lastCompL0' = Max(lastCompL0, SetMax({gen[x] : x \in job.inputs}))
    /\ done' = [has |-> TRUE, start |-> job.start]
    /\ job' = [job EXCEPT !.active = FALSE]
    /\ stale' = IF AllowReader /\ stale = Never THEN now ELSE stale
    /\ UNCHANGED <<now, st, gen, cov, abs, ck, wv, rv, gc, gcLast, firstInv>>

OrphanUpload ==      \* fenced old writer: SST uploaded, manifest CAS then fails
    /\ AllowOrphan /\ now <= Horizon /\ st["O"] = "none"
    /\ st' = [st EXCEPT !["O"] = "present"]
    /\ gen' = [gen EXCEPT !["O"] = now]
    /\ cov' = [cov EXCEPT !["O"] = {1}]
    /\ UNCHANGED <<now, man, committed, lastCompL0, job, done, abs, ck, cck, wv, stale, rv,
                   gc, gcLast, firstInv>>

\* ManifestWriterCommand::PollManifest every manifest_poll_interval: the
\* writer merges the stored manifest into its in-memory view.  It may
\* happen any time, and Tick forces it within PollInterval ticks of the
\* view going stale.
WriterPoll ==
    /\ AllowReader /\ wv # man
    /\ wv' = man
    /\ stale' = Never
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, rv, gc, gcLast, firstInv>>

Advance ==           \* committer advances `absorbed` after the covering flush
    /\ abs < 2
    /\ \E x \in Obj : AdvanceBacked(x) /\ abs \in cov[x]
    /\ abs' = abs + 1
    /\ UNCHANGED <<now, storeVars, ck, cck, wv, stale, rv, gc, gcLast, firstInv>>

-----------------------------------------------------------------------------
(* Pins: upstream checkpoint (contract only) and the production read view  *)

CkCreate ==
    /\ UseCheckpoint /\ now <= Horizon /\ ~ck.active /\ ~ck.used
    /\ ck' = [active |-> TRUE, used |-> TRUE, refs |-> man]
    /\ UNCHANGED <<now, storeVars, abs, cck, wv, stale, rv, gc, gcLast, firstInv>>

CkRelease ==
    /\ ck.active /\ now <= Horizon
    /\ ck' = [ck EXCEPT !.active = FALSE]
    /\ UNCHANGED <<now, storeVars, abs, cck, wv, stale, rv, gc, gcLast, firstInv>>

ReadBegin ==         \* a history read captures the writer's in-memory view
    /\ AllowReader /\ ~rv.active /\ rv.out = "none"
    /\ rv' = [active |-> TRUE, view |-> wv, out |-> "none", start |-> now]
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, wv, stale, gc, gcLast, firstInv>>

\* The read completes.  With every object of its view present it returns
\* the correct rows ("ok"); otherwise the outcome is what the upstream read
\* of a deleted SST yields, as the repository handles it.  "short" means a
\* completed read that silently omits rows.
ReadEnd ==
    /\ rv.active
    /\ rv' = [active |-> FALSE, view |-> {}, start |-> 0,
              out |-> IF \A x \in rv.view : st[x] = "present" THEN "ok"
                      ELSE RepoOnDeletedRead(UpstreamDeletedRead)]
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, wv, stale, gc, gcLast, firstInv>>

-----------------------------------------------------------------------------
(* Collector (upstream CompactedGcTask::collect, one pass per tick)        *)

GcReadCompactions == \* one pass per tick (the configured interval)
    /\ gc.ph = "idle" /\ (gcLast = Never \/ gcLast < now)
    /\ gc' = [gc EXCEPT !.ph = "lw", !.age = AgeCut(now),
                        !.cut = Min(AgeCut(now), LowWatermark)]
    /\ gcLast' = now
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, wv, stale, rv, firstInv>>

GcReadManifest ==
    /\ gc.ph = "lw"
    /\ gc' = [gc EXCEPT !.ph = "viewed", !.refs = GcRefs, !.cut = Min(gc.cut, NewestL0)]
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, wv, stale, rv, gcLast, firstInv>>

GcList ==
    /\ gc.ph = "viewed"
    /\ gc' = [gc EXCEPT !.ph = "listed", !.inv = GcInventory]
    /\ firstInv' = IF firstInv.set THEN firstInv
                   ELSE [set |-> TRUE, inv |-> {x \in Obj : st[x] = "present"}]
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, wv, stale, rv, gcLast>>

GcDelete ==
    /\ gc.ph = "listed"
    /\ \E x \in gc.inv :
         /\ GcEligible(x)
         /\ st' = [st EXCEPT ![x] = IF st[x] = "present" THEN "deleted" ELSE st[x]]
         /\ gc' = [gc EXCEPT !.inv = gc.inv \ {x}]
    /\ UNCHANGED <<now, gen, cov, man, committed, lastCompL0, job, done, abs, ck, cck, wv,
                   stale, rv, gcLast, firstInv>>

GcFinish ==
    /\ gc.ph = "listed"
    /\ ~\E x \in gc.inv : GcEligible(x)
    /\ gc' = [ph |-> "idle", age |-> 0, cut |-> 0, refs |-> {}, inv |-> {}]
    /\ UNCHANGED <<now, storeVars, abs, ck, cck, wv, stale, rv, gcLast, firstInv>>

-----------------------------------------------------------------------------
Settled ==
    /\ now = MaxTime /\ gc.ph = "idle" /\ gcLast = MaxTime
    /\ ~rv.active

Terminated == Settled /\ UNCHANGED vars

Writer == FlushUpload \/ FlushCommit \/ CompactStart \/ CompactUpload \/ CompactCommit
          \/ OrphanUpload \/ WriterPoll \/ Advance
Pins == CkCreate \/ CkRelease \/ ReadBegin \/ ReadEnd
Collector == GcReadCompactions \/ GcReadManifest \/ GcList \/ GcDelete \/ GcFinish

Next == Tick \/ Writer \/ Pins \/ Collector \/ Terminated

Spec == Init /\ [][Next]_vars

(* Liveness (UPSTREAM-CONTRACT check, not repository evidence): time        *)
(* advances to the clock bound, the collector keeps running and in-flight   *)
(* reads end.  No fairness on writers, pins, or any success step;           *)
(* writer/pin activity simply stops at Horizon (the faults cease).          *)
LiveSpec ==
    /\ Spec
    /\ WF_vars(Tick)
    /\ WF_vars(GcReadCompactions) /\ WF_vars(GcReadManifest) /\ WF_vars(GcList)
    /\ WF_vars(GcDelete) /\ WF_vars(GcFinish)
    /\ WF_vars(ReadEnd)

-----------------------------------------------------------------------------
(* Safety                                                                   *)

TypeOK ==
    /\ now \in 0..MaxTime
    /\ st \in [Obj -> {"none", "present", "deleted"}]
    /\ man \subseteq Obj /\ committed \subseteq Obj /\ man \subseteq committed
    /\ wv \subseteq committed
    /\ abs \in 0..2
    /\ gc.ph \in {"idle", "lw", "viewed", "listed"}
    /\ rv.out \in {"none", "ok", "error", "short"}
    /\ cck.refs \subseteq Obj /\ (stale # Never => AllowReader)

\* Nothing referenced by the live manifest is ever deleted (includes an
\* upload that is committed only after the collector observed an older view).
ManifestRefsPresent == \A x \in man : st[x] = "present"

\* Uncompleted-history-transition / last-copy composition: every offset below
\* the absorbed boundary is held by a present object of the live manifest.
HistoryBacked ==
    \A o \in 0..1 : o < abs => \E x \in man : o \in cov[x] /\ st[x] = "present"

\* Upstream checkpoint contract: a pinned manifest's objects survive.
CheckpointPinned == ck.active => \A x \in ck.refs : st[x] = "present"

\* The catalog's reader clause: nothing required by a live reader's view is
\* deleted while the read is in flight.
LiveReadViewProtected == rv.active => \A x \in rv.view : st[x] # "deleted"

\* A read never COMPLETES while silently missing rows of a deleted SST: it
\* either returns the correct rows or fails.
NoFalseCompleteRead == rv.out # "short"

-----------------------------------------------------------------------------
(* Liveness                                                                 *)

Pinned(x) == x \in CheckpointRefs
\* The upstream eligibility over the CURRENT state; every conjunct is stable
\* once true (man only loses x for good, cutoffs are non-decreasing).
EligibleNow(x) ==
    /\ st[x] = "present" /\ x \notin man /\ ~Pinned(x)
    /\ gen[x] < AgeCut(now) /\ gen[x] < LowWatermark /\ gen[x] < NewestL0
EligibleEventuallyReclaimed ==
    \A x \in Obj : EligibleNow(x) ~> (st[x] = "deleted")

-----------------------------------------------------------------------------
(* Reachability witnesses (expected violated on the unmodified model).      *)
Witness_CompactedInputReclaimed == ~(st["L1"] = "deleted")
Witness_OrphanReclaimed == ~(st["O"] = "deleted")
Witness_HistoryServedAfterReclaim == ~(abs = 2 /\ st["L1"] = "deleted")
Witness_ReaderViewErrors == ~(rv.out = "error")
Witness_QuietDeadZoneRetains ==
    ~(Settled /\ \E x \in {"L2", "O"} : st[x] = "present" /\ x \notin man)
Witness_LateCommitAfterGcView ==
    ~(gc.ph = "listed" /\ "L2" \in gc.inv /\ "L2" \notin gc.refs /\ "L2" \in man)
\* A read begins on a writer view that still lists a compacted-away input.
Witness_StaleWriterViewRead ==
    ~(rv.active /\ \E x \in rv.view : x \notin man)
\* A collector pass has listed a compacted-away input that a live read view
\* still references and that is past the age and generation cutoff; only
\* the compactor's checkpoint keeps it live.
Witness_CheckpointProtectsReadView ==
    ~(/\ rv.active /\ gc.ph = "listed"
      /\ \E x \in rv.view : /\ x \notin man /\ x \in gc.inv /\ gen[x] < gc.cut
                            /\ x \in gc.refs /\ x \in cck.refs)
=============================================================================
