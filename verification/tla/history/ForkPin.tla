------------------------------- MODULE ForkPin -------------------------------
(***************************************************************************)
(* TLA-019 (logical layer) -- the fork pin that keeps a source incarnation *)
(* (and therefore its shard-log and history rows, which the repository     *)
(* never deletes except by absorbed-boundary trimming) readable for a live *)
(* fork, and the release that eventually unpins it.                        *)
(*                                                                         *)
(* This layer shares no state with the physical graph in ReachGC.tla:      *)
(* registry retention decides whether a descriptor may be tombstoned, the  *)
(* SlateDB manifest decides which SSTs are reachable.  Decomposed by       *)
(* invariant ownership (roadmap 7.8).                                      *)
(*                                                                         *)
(* One source NAME whose incarnations are numbered 1..MaxEpoch (a          *)
(* tombstoned source may be recreated under the same name), and fork       *)
(* children in `Children`, each one child INCARNATION bound to the source  *)
(* incarnation it forked.  Prev[c] names the child incarnation whose       *)
(* tombstone c replaces when c recreates its name ("-" for none).  Every   *)
(* registry step below is ONE conditional write bound to an incarnation    *)
(* (Registry::mutate_incarnation, ASM-OBJSTORE-CAS), as in production:     *)
(*   anchor::install         -- install the child's reference on the       *)
(*                              incarnation it forked                      *)
(*   deletion::delete_transition -- soft vs hard decided inside the CAS    *)
(*   deletion::release_fork_ref  -- drop the reference and decide the      *)
(*                                 source's fate in the same CAS; an       *)
(*                                 incarnation mismatch is conclusive      *)
(* A child DELETE writes a fork-debt index marker for the child            *)
(* incarnation BEFORE its tombstone (write-ahead), then the tombstone CAS  *)
(* (recording the parent_ref_pending debt in the same write), then, in the *)
(* SAME request, release_fork_ref; a conclusive release clears the debt    *)
(* and (best effort) the marker.  A crash between them, or an inconclusive *)
(* release, leaves the debt and the marker.  The background reconciler     *)
(* (fork-debt-reconcile) pages the markers and runs what a repeated DELETE *)
(* runs, or releases from the marker when the child's name was recreated.  *)
(* A client may still repeat DELETE.  With Legacy, deletes before the      *)
(* rollout run the old binary (no markers), and the one-time backfill      *)
(* indexes the debt-bearing tombstones it finds after the rollout.         *)
(*                                                                         *)
(* Mutation points: DeleteDecision, InstallFence, WriteAhead, MarkerView,  *)
(* ReleaseId, SettleMarker, BackfillOn.                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Children,     \* fork child incarnations
    Prev,         \* [Children -> Children \cup {"-"}]: the incarnation c's name held before
    MaxEpoch,     \* source incarnations (1 = the original; > 1 = recreated)
    MaxCrashes,   \* request crashes/cancellations (creator or delete request)
    Legacy        \* deletes run the pre-index binary until the rollout

ASSUME /\ MaxEpoch \in Nat \ {0} /\ MaxCrashes \in Nat /\ Legacy \in BOOLEAN
       /\ Prev \in [Children -> Children \cup {"-"}]

VARIABLES
    srcEpoch,  \* the incarnation currently holding the source name
    src,       \* its lifecycle: "live" | "soft" | "deleted" (tombstone)
    refs,      \* its fork_children
    child,     \* [Children -> "none"|"creating"|"anchored"|"ready"|"gone"]
    cEpoch,    \* [Children -> source incarnation the child forked (0 = none)]
    creator,   \* [Children -> "idle"|"pending"|"installed"|"done"]
    debt,      \* [Children -> the child's tombstone owes the parent release]
    req,       \* [Children -> "none"|"releasing"]: a DELETE past its tombstone CAS
    marker,    \* [Children -> the fork-debt index holds this incarnation's marker]
    rolled,    \* the binary with the index and the reconciler is deployed
    bfDone,    \* the one-time backfill recorded its completion
    crashes,
    cascaded,  \* ghost: a source was tombstoned by the release cascade
    lateInst,  \* ghost: an install landed after its child was deleted
    declinedRecreated, \* ghost: an install was declined because the source was recreated
    conclusiveRecreated, \* ghost: a debt was cleared because the source was recreated
    okDel,     \* ghost: [Children -> a DELETE of the child returned success to its client]
    recLate,   \* ghost: the reconciler released a reference whose child's DELETE had succeeded
    recReplaced, \* ghost: the reconciler released from the marker of a recreated name
    bfSet,     \* ghost: the incarnations whose marker the backfill wrote
    recBackfilled, \* ghost: the reconciler released a reference the backfill indexed
    lostUnindexed  \* ghost: after the rollout, a recreation overwrote an unindexed debt

vars == <<srcEpoch, src, refs, child, cEpoch, creator, debt, req, marker, rolled, bfDone,
          crashes, cascaded, lateInst, declinedRecreated, conclusiveRecreated, okDel,
          recLate, recReplaced, bfSet, recBackfilled, lostUnindexed>>
ghosts == <<cascaded, lateInst, declinedRecreated, conclusiveRecreated, okDel,
            recLate, recReplaced, bfSet, recBackfilled, lostUnindexed>>
rghosts == <<recLate, recReplaced, bfSet, recBackfilled, lostUnindexed>>

\* delete_transition: soft-delete while live children exist, else tombstone.
DeleteDecision(hasRef) == IF hasRef THEN "soft" ELSE "deleted"
\* anchor::install's CAS is bound to the incarnation the child forked
\* (mutate_incarnation(&source, &source_desc.stream_epoch)).
InstallFence(c) == cEpoch[c] = srcEpoch
\* delete_lifecycle writes the marker before the tombstone write
\* (record_fork_debt, deletion.rs:290-294); a failed write fails the delete.
WriteAhead(c) == marker[c]
\* The reconciler's reading of a marker (reconcile.rs settle, :202-251):
\* the name holds another incarnation (or none) -> release from the marker;
\* this incarnation's tombstone -> repair it; anything alive -> defer.
Replaced(c) == \E d \in Children : Prev[d] = c /\ child[d] # "none"
MarkerView(c) ==
    IF Replaced(c) THEN "replaced"
    ELSE IF child[c] = "gone" THEN "tombstone" ELSE "deferred"
\* The replaced-name release names the fork id the marker recorded.
ReleaseId(c) == c
\* A marker is removed only after a conclusive release.
SettleMarker(conclusive) == conclusive
\* The one-time backfill (Registry::backfill_fork_debt) runs.
BackfillOn == TRUE

Alive(c) == child[c] \in {"creating", "anchored", "ready"}
NameFree(c) == IF Prev[c] = "-" THEN TRUE ELSE child[Prev[c]] = "gone"

Init ==
    /\ srcEpoch = 1 /\ src = "live" /\ refs = {}
    /\ child = [c \in Children |-> "none"]
    /\ cEpoch = [c \in Children |-> 0]
    /\ creator = [c \in Children |-> "idle"]
    /\ debt = [c \in Children |-> FALSE]
    /\ req = [c \in Children |-> "none"]
    /\ marker = [c \in Children |-> FALSE]
    /\ rolled = ~Legacy
    \* Without Legacy the index predates every delete: the backfill found nothing.
    /\ bfDone = ~Legacy
    /\ crashes = 0
    /\ cascaded = FALSE /\ lateInst = FALSE
    /\ declinedRecreated = FALSE /\ conclusiveRecreated = FALSE
    /\ okDel = [c \in Children |-> FALSE]
    /\ recLate = FALSE /\ recReplaced = FALSE /\ bfSet = {}
    /\ recBackfilled = FALSE /\ lostUnindexed = FALSE

(* release_fork_ref(source, id, sep) as one atomic step: the epoch check on *)
(* its snapshot and the CAS bound to that snapshot's incarnation agree (a   *)
(* recreate in between surfaces as IncarnationChanged, which is conclusive  *)
(* too).  Verdict is its return value (conclusive or not).                  *)
Verdict(id, sep) == sep # srcEpoch \/ src = "deleted" \/ id \in refs
DoRelease(id, sep) ==
    IF sep = srcEpoch /\ src # "deleted"
      THEN LET left == refs \ {id}
               tomb == left = {} /\ src = "soft"
           IN /\ refs' = left
              /\ src' = IF tomb THEN "deleted" ELSE src
              /\ cascaded' = (cascaded \/ tomb)
      ELSE UNCHANGED <<src, refs, cascaded>>
\* The child's own release; `clear` also clears its debt when conclusive
\* (clear_parent_debt, deletion.rs:37-71).
Release(c, clear) ==
    /\ DoRelease(c, cEpoch[c])
    /\ debt' = IF clear /\ Verdict(c, cEpoch[c]) THEN [debt EXCEPT ![c] = FALSE] ELSE debt
    /\ conclusiveRecreated' =
           (conclusiveRecreated \/ (clear /\ debt[c] /\ srcEpoch # cEpoch[c]))

-----------------------------------------------------------------------------
ForkBegin(c) ==     \* fork::prepare validated the live current incarnation
    /\ child[c] = "none" /\ src = "live" /\ NameFree(c)
    /\ child' = [child EXCEPT ![c] = "creating"]
    /\ cEpoch' = [cEpoch EXCEPT ![c] = srcEpoch]
    /\ creator' = [creator EXCEPT ![c] = "pending"]
    \* Recreating the name overwrites the previous incarnation's tombstone,
    \* and its parent_ref_pending debt with it; only its marker remains.
    /\ debt' = IF Prev[c] = "-" THEN debt ELSE [debt EXCEPT ![Prev[c]] = FALSE]
    /\ lostUnindexed' = (lostUnindexed \/ IF Prev[c] = "-" THEN FALSE
                                            ELSE rolled /\ debt[Prev[c]] /\ ~marker[Prev[c]])
    /\ UNCHANGED <<srcEpoch, src, refs, req, marker, rolled, bfDone, crashes, cascaded,
                   lateInst, declinedRecreated, conclusiveRecreated, okDel,
                   recLate, recReplaced, bfSet, recBackfilled>>

ForkInstall(c) ==   \* anchor::install CAS on the incarnation the child forked
    /\ creator[c] = "pending"
    /\ IF ~InstallFence(c)
         THEN \* IncarnationChanged: fork_source_changed
              /\ creator' = [creator EXCEPT ![c] = "done"]
              /\ declinedRecreated' = TRUE
              /\ UNCHANGED <<refs, lateInst>>
         ELSE IF c \in refs
           THEN \* already installed: Declined(!deleted) -- idempotent
                /\ creator' = [creator EXCEPT ![c] = IF src # "deleted" THEN "installed"
                                                     ELSE "done"]
                /\ UNCHANGED <<refs, lateInst, declinedRecreated>>
           ELSE IF src = "live"
             THEN /\ refs' = refs \cup {c}
                  /\ creator' = [creator EXCEPT ![c] = "installed"]
                  /\ lateInst' = (lateInst \/ child[c] = "gone")
                  /\ UNCHANGED declinedRecreated
             ELSE \* soft or tombstoned: declined (fork_source_changed)
                  /\ creator' = [creator EXCEPT ![c] = "done"]
                  /\ UNCHANGED <<refs, lateInst, declinedRecreated>>
    /\ UNCHANGED <<srcEpoch, src, child, cEpoch, debt, req, marker, rolled, bfDone, crashes,
                   cascaded, conclusiveRecreated, okDel, rghosts>>

ForkPostCheck(c) == \* anchor::install post-install child check
    /\ creator[c] = "installed"
    /\ creator' = [creator EXCEPT ![c] = "done"]
    /\ IF Alive(c)
         THEN \* anchored only if the source NAME's current descriptor still
              \* lists the fork id (anchor.rs post-install lookup by name, no
              \* epoch check); otherwise fork_source_gone and the child stays
              \* initializing
              /\ child' = [child EXCEPT ![c] =
                              IF @ = "creating" /\ c \in refs THEN "anchored" ELSE @]
              /\ UNCHANGED <<src, refs, debt, cascaded, conclusiveRecreated>>
         ELSE \* the child incarnation was deleted meanwhile (the lookup is
              \* bound to its epoch): release this request's fresh reference
              \* (the verdict does not clear the child's debt)
              /\ UNCHANGED child
              /\ Release(c, FALSE)
    /\ UNCHANGED <<srcEpoch, cEpoch, req, marker, rolled, bfDone, crashes, lateInst,
                   declinedRecreated, okDel, rghosts>>

ForkReady(c) ==     \* Ready published only after the anchor exists (C5)
    /\ child[c] = "anchored"
    /\ child' = [child EXCEPT ![c] = "ready"]
    /\ UNCHANGED <<srcEpoch, src, refs, cEpoch, creator, debt, req, marker, rolled, bfDone,
                   crashes, ghosts>>

CreatorCrash(c) ==  \* the create request dies before its post-check
    /\ crashes < MaxCrashes
    /\ creator[c] \in {"pending", "installed"}
    /\ creator' = [creator EXCEPT ![c] = "done"]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, debt, req, marker, rolled, bfDone, ghosts>>

SourceDelete ==     \* delete_lifecycle -> delete_transition inside the CAS
    /\ src = "live"
    /\ src' = DeleteDecision(refs # {})
    /\ UNCHANGED <<srcEpoch, refs, child, cEpoch, creator, debt, req, marker, rolled, bfDone,
                   crashes, ghosts>>

SourceRecreate ==   \* create under the same name after the tombstone (F5)
    /\ src = "deleted" /\ srcEpoch < MaxEpoch
    /\ srcEpoch' = srcEpoch + 1 /\ src' = "live" /\ refs' = {}
    /\ UNCHANGED <<child, cEpoch, creator, debt, req, marker, rolled, bfDone, crashes, ghosts>>

IndexDebt(c) ==     \* record_fork_debt, ahead of the tombstone (new binary only)
    /\ rolled /\ Alive(c) /\ ~marker[c]
    /\ marker' = [marker EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, req, rolled, bfDone,
                   crashes, ghosts>>

ChildDelete(c) ==   \* delete_transition on the child: tombstone + debt, one CAS
    /\ Alive(c)
    /\ ~rolled \/ WriteAhead(c)
    /\ child' = [child EXCEPT ![c] = "gone"]
    /\ debt' = [debt EXCEPT ![c] = TRUE]
    /\ req' = [req EXCEPT ![c] = "releasing"]
    /\ UNCHANGED <<srcEpoch, src, refs, cEpoch, creator, marker, rolled, bfDone, crashes, ghosts>>

InRequestRelease(c) == \* the same DELETE request continues: release_fork_ref
    /\ req[c] = "releasing"
    /\ req' = [req EXCEPT ![c] = "none"]
    /\ Release(c, TRUE)
    \* settle_marker after a conclusive release is best effort
    \* (deletion.rs:451-459): a failure leaves the marker for the reconciler
    /\ \E kept \in BOOLEAN :
         marker' = [marker EXCEPT ![c] =
                       IF SettleMarker(Verdict(c, cEpoch[c])) THEN @ /\ kept ELSE @]
    \* delete_lifecycle returns Ok(()) whether or not the release was
    \* conclusive: the client sees success either way
    /\ okDel' = [okDel EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<srcEpoch, child, cEpoch, creator, rolled, bfDone, crashes, lateInst,
                   declinedRecreated, rghosts>>

RequestAbandon(c) == \* crash or cancellation after the tombstone CAS
    /\ crashes < MaxCrashes
    /\ req[c] = "releasing"
    /\ req' = [req EXCEPT ![c] = "none"]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, marker, rolled, bfDone,
                   ghosts>>

RetryDelete(c) ==   \* the CLIENT re-issues DELETE: repair_tombstone
    /\ child[c] = "gone" /\ ~Replaced(c) /\ debt[c] /\ req[c] = "none"
    /\ req' = [req EXCEPT ![c] = "releasing"]
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, marker, rolled, bfDone,
                   crashes, ghosts>>

(* The background reconciler, one marker (reconcile.rs settle, :202-261).   *)
(* A tombstone with debt: repair_tombstone(desc, true), the repeated        *)
(* DELETE's release, then the marker goes once the debt is paid.  A         *)
(* tombstone without debt: the marker goes.  A recreated name: release from *)
(* the marker, fenced to the source incarnation, and settle if conclusive.  *)
(* Anything alive: deferred (no step).  One step per marker: every CAS in   *)
(* it is idempotent and a restart re-reads the marker.                      *)
Reconcile(c) ==
    /\ marker[c]
    /\ CASE MarkerView(c) = "tombstone" /\ debt[c] ->
              /\ Release(c, TRUE)
              /\ marker' = [marker EXCEPT ![c] = ~SettleMarker(Verdict(c, cEpoch[c]))]
              /\ recLate' = (recLate \/ (okDel[c] /\ c \in refs /\ cEpoch[c] = srcEpoch
                                        /\ src # "deleted"))
              /\ recBackfilled' = (recBackfilled \/ (c \in bfSet /\ c \in refs
                                        /\ cEpoch[c] = srcEpoch /\ src # "deleted"))
              /\ UNCHANGED recReplaced
         [] MarkerView(c) = "tombstone" /\ ~debt[c] ->
              /\ marker' = [marker EXCEPT ![c] = FALSE]
              /\ UNCHANGED <<src, refs, cascaded, debt, conclusiveRecreated,
                             recLate, recReplaced, recBackfilled>>
         [] MarkerView(c) = "replaced" ->
              LET id == ReleaseId(c) IN
              /\ DoRelease(id, cEpoch[c])
              /\ marker' = [marker EXCEPT ![c] = ~SettleMarker(Verdict(id, cEpoch[c]))]
              /\ recReplaced' = (recReplaced \/ (id \in refs /\ cEpoch[c] = srcEpoch
                                                /\ src # "deleted"))
              /\ UNCHANGED <<debt, conclusiveRecreated, recLate, recBackfilled>>
         [] OTHER -> FALSE      \* deferred: a live child's delete may not have tombstoned yet
    /\ UNCHANGED <<srcEpoch, child, cEpoch, creator, req, rolled, bfDone, crashes, lateInst,
                   declinedRecreated, okDel, bfSet, lostUnindexed>>

Rollout ==          \* the binary with the index replaces the old one; an old
                    \* request still in flight wrote no marker, so it finishes alike
    /\ ~rolled
    /\ rolled' = TRUE
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, req, marker,
                   bfDone, crashes, ghosts>>

BackfillDue(c) ==
    /\ BackfillOn /\ rolled /\ ~bfDone
    /\ child[c] = "gone" /\ ~Replaced(c) /\ debt[c] /\ ~marker[c]

Backfill(c) ==      \* backfill_fork_debt indexes a debt-bearing tombstone it walks
    /\ BackfillDue(c)
    /\ marker' = [marker EXCEPT ![c] = TRUE]
    /\ bfSet' = bfSet \cup {c}
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, req, rolled, bfDone,
                   crashes, cascaded, lateInst, declinedRecreated, conclusiveRecreated,
                   okDel, recLate, recReplaced, recBackfilled, lostUnindexed>>

\* The walk has passed every descriptor and records `complete`; it never
\* runs again in this deployment (fork_debt.rs:279-281, :298-303).  After
\* the rollout no delete writes debt without a marker, so the walk has seen
\* every unindexed debt-bearing tombstone when it completes.
BackfillFinish ==
    /\ BackfillOn /\ rolled /\ ~bfDone
    /\ \A c \in Children : ~BackfillDue(c)
    /\ bfDone' = TRUE
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, req, marker, rolled,
                   crashes, ghosts>>

-----------------------------------------------------------------------------
(* Settled: nothing in flight, no debt that a retry could still pay, no     *)
(* marker the reconciler would still act on, nothing left to backfill.  A   *)
(* debt or marker whose release is inconclusive (absent reference on the    *)
(* live incarnation it forked) may legitimately persist.                    *)
PayableDebt(c) ==
    debt[c] /\ ~Replaced(c) /\ Verdict(c, cEpoch[c])
ReconcileDue(c) ==
    /\ marker[c]
    /\ \/ MarkerView(c) = "tombstone" /\ (~debt[c] \/ Verdict(c, cEpoch[c]))
       \/ MarkerView(c) = "replaced" /\ Verdict(ReleaseId(c), cEpoch[c])
Settled ==
    /\ rolled /\ (bfDone \/ ~BackfillOn)
    /\ \A c \in Children : creator[c] \in {"idle", "done"} /\ req[c] = "none"
    /\ \A c \in Children : ~PayableDebt(c) /\ ~ReconcileDue(c) /\ ~BackfillDue(c)
Terminated == Settled /\ UNCHANGED vars

Next ==
    \/ \E c \in Children :
         \/ ForkBegin(c) \/ ForkInstall(c) \/ ForkPostCheck(c) \/ ForkReady(c)
         \/ CreatorCrash(c) \/ IndexDebt(c) \/ ChildDelete(c) \/ InRequestRelease(c)
         \/ RequestAbandon(c) \/ RetryDelete(c) \/ Reconcile(c) \/ Backfill(c)
    \/ SourceDelete \/ SourceRecreate \/ Rollout \/ BackfillFinish

\* Deadlock checking stays on; Terminated is the only stuttering step.
Spec == Init /\ [][Next \/ Terminated]_vars

(* Liveness.  The in-process steps of a live request are weakly fair.        *)
(* Crashes are bounded, so they cease.  No fairness on deletes, forks,       *)
(* recreation or any success outcome.                                        *)
(* LiveSpecReconciler: the supervised reconciler task (Critical policy,      *)
(* restarted by the supervisor, one circle every FORK_DEBT_SWEEP_SECS) and   *)
(* its backfill steps are weakly fair, and the rollout completes; no client  *)
(* ever repeats DELETE.  LiveSpecClientRetries: the pre-reconciler premise   *)
(* F8 relied on (the client retries while a payable debt remains).           *)
InProcess(c) == ForkInstall(c) \/ ForkPostCheck(c) \/ InRequestRelease(c)
InProcessFairness == \A c \in Children : WF_vars(InProcess(c))
ClientRetryFairness == \A c \in Children : WF_vars(RetryDelete(c))
ReconcilerFairness ==
    /\ \A c \in Children : WF_vars(Reconcile(c)) /\ WF_vars(Backfill(c))
    /\ WF_vars(BackfillFinish)
RolloutFairness == WF_vars(Rollout)
LiveSpecClientRetries == Spec /\ InProcessFairness /\ ClientRetryFairness
LiveSpecReconciler ==
    Spec /\ InProcessFairness /\ ReconcilerFairness /\ RolloutFairness

TypeOK ==
    /\ srcEpoch \in 1..MaxEpoch /\ src \in {"live", "soft", "deleted"}
    /\ refs \subseteq Children
    /\ child \in [Children -> {"none", "creating", "anchored", "ready", "gone"}]
    /\ cEpoch \in [Children -> 0..MaxEpoch]
    /\ creator \in [Children -> {"idle", "pending", "installed", "done"}]
    /\ debt \in [Children -> BOOLEAN] /\ req \in [Children -> {"none", "releasing"}]
    /\ marker \in [Children -> BOOLEAN] /\ rolled \in BOOLEAN /\ bfDone \in BOOLEAN

\* F6/F10: a child that is anchored or Ready points at the CURRENT, not
\* tombstoned incarnation it forked: its source was neither hard-deleted nor
\* replaced by a recreation while it lived.
ForkPinRespected ==
    \A c \in Children : child[c] \in {"anchored", "ready"} =>
        (cEpoch[c] = srcEpoch /\ src # "deleted")
\* The pin is exactly the reference: an anchored/Ready child holds it.
ReadyHoldsRef ==
    \A c \in Children : child[c] \in {"anchored", "ready"} =>
        (cEpoch[c] = srcEpoch /\ c \in refs)
\* (New binary.)  A deleted child's reference that still pins the source
\* incarnation it forked, with no creator left to release it, is indexed:
\* no crash and no settlement leaves debt the reconciler cannot find.
OwedRefIndexed ==
    \A c \in Children :
        (/\ child[c] = "gone" /\ c \in refs /\ cEpoch[c] = srcEpoch
         /\ creator[c] = "done")
            => marker[c]

\* No permanent pin: once a child is gone, its reference on the incarnation
\* it forked is eventually released, and a soft-deleted source whose
\* children are all gone is eventually tombstoned.
RefEventuallyReleased ==
    \A c \in Children :
        (child[c] = "gone" /\ cEpoch[c] = srcEpoch /\ c \in refs)
            ~> ~(cEpoch[c] = srcEpoch /\ c \in refs)
SoftSourceEventuallyTombstoned ==
    (src = "soft" /\ \A c \in Children : child[c] \in {"none", "gone"})
        ~> (src = "deleted")

Witness_SoftDeleteRetainedForFork ==
    ~(src = "soft" /\ \E c \in Children : child[c] = "ready")
Witness_ForkCascadeTombstone == ~cascaded
Witness_InstallAfterChildDeleted == ~lateInst
Witness_InstallDeclinedOnRecreatedSource == ~declinedRecreated
Witness_DebtClearedOnRecreatedSource == ~conclusiveRecreated
Witness_TwoChildrenPinSource ==
    ~(src = "soft" /\ \E c1, c2 \in Children :
         c1 # c2 /\ \A c \in {c1, c2} : child[c] = "ready" /\ c \in refs)
\* The late-install variant: a DELETE of the child already returned success
\* (its release found the reference absent on the live source, which is not
\* conclusive, so the tombstone kept the debt), the creator's install then
\* landed and the creator died before its post-check.  The client has no
\* signal to repeat the DELETE; the reconciler releases it.
Witness_PinAfterSuccessfulDelete ==
    ~(\E c \in Children :
        /\ child[c] = "gone" /\ okDel[c] /\ debt[c] /\ req[c] = "none"
        /\ creator[c] = "done" /\ c \in refs /\ cEpoch[c] = srcEpoch)
\* A child's DELETE crashed after its tombstone CAS and its reference still
\* pins a soft-deleted source; no in-process step can release it.  Before
\* the reconciler only a client retry could; now the marker is paged.
Witness_PermanentPinWithoutRetry ==
    ~(\E c \in Children :
        /\ child[c] = "gone" /\ debt[c] /\ req[c] = "none"
        /\ creator[c] = "done" /\ c \in refs /\ cEpoch[c] = srcEpoch
        /\ src = "soft")
\* The reconciler released a reference whose child's DELETE had answered
\* success (the TLA-019-F4 schedule), with no client retry.
Witness_ReconcilerReleasesLatePin == ~recLate
\* The reconciler released, from the marker, the reference of a child
\* incarnation whose name was recreated (its tombstone and debt overwritten).
Witness_ReconcilerReleasesReplacedName == ~recReplaced
\* Legacy: a pre-index debt was indexed by the backfill and released.
Witness_BackfillReleased == ~recBackfilled
\* Legacy residual: after the rollout, a recreation of a child's name
\* overwrote a debt-bearing tombstone the backfill had not yet indexed.
Witness_UnindexedDebtOverwritten == ~lostUnindexed
=============================================================================
