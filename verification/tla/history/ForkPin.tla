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
(* children in `Children`, each bound to the incarnation it forked.  Every *)
(* registry step below is ONE conditional write bound to an incarnation    *)
(* (Registry::mutate_incarnation, ASM-OBJSTORE-CAS), as in production:     *)
(*   anchor::install         -- install the child's reference on the       *)
(*                              incarnation it forked                      *)
(*   deletion::delete_transition -- soft vs hard decided inside the CAS    *)
(*   deletion::release_fork_ref  -- drop the reference and decide the      *)
(*                                 source's fate in the same CAS; an       *)
(*                                 incarnation mismatch is conclusive      *)
(* A child DELETE is two steps: the tombstone CAS (recording the           *)
(* parent_ref_pending debt in the same write), then, in the SAME request,  *)
(* release_fork_ref.  A crash or cancellation between them leaves the debt *)
(* on the tombstone; the only repair is the client re-issuing DELETE       *)
(* (delete_lifecycle -> repair_tombstone); there is no background sweeper. *)
(*                                                                         *)
(* Mutation points: DeleteDecision, InstallFence.                          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Children,     \* fork child ids
    MaxEpoch,     \* source incarnations (1 = the original; > 1 = recreated)
    MaxCrashes    \* request crashes/cancellations (creator or delete request)

ASSUME /\ MaxEpoch \in Nat \ {0} /\ MaxCrashes \in Nat

VARIABLES
    srcEpoch,  \* the incarnation currently holding the source name
    src,       \* its lifecycle: "live" | "soft" | "deleted" (tombstone)
    refs,      \* its fork_children
    child,     \* [Children -> "none"|"creating"|"anchored"|"ready"|"gone"]
    cEpoch,    \* [Children -> source incarnation the child forked (0 = none)]
    creator,   \* [Children -> "idle"|"pending"|"installed"|"done"]
    debt,      \* [Children -> the child's tombstone owes the parent release]
    req,       \* [Children -> "none"|"releasing"]: a DELETE past its tombstone CAS
    crashes,
    cascaded,  \* ghost: a source was tombstoned by the release cascade
    lateInst,  \* ghost: an install landed after its child was deleted
    declinedRecreated, \* ghost: an install was declined because the source was recreated
    conclusiveRecreated, \* ghost: a debt was cleared because the source was recreated
    okDel      \* ghost: [Children -> a DELETE of the child returned success to its client]

vars == <<srcEpoch, src, refs, child, cEpoch, creator, debt, req, crashes,
          cascaded, lateInst, declinedRecreated, conclusiveRecreated, okDel>>
ghosts == <<cascaded, lateInst, declinedRecreated, conclusiveRecreated, okDel>>

\* delete_transition: soft-delete while live children exist, else tombstone.
DeleteDecision(hasRef) == IF hasRef THEN "soft" ELSE "deleted"
\* anchor::install's CAS is bound to the incarnation the child forked
\* (mutate_incarnation(&source, &source_desc.stream_epoch)).
InstallFence(c) == cEpoch[c] = srcEpoch

Alive(c) == child[c] \in {"creating", "anchored", "ready"}

Init ==
    /\ srcEpoch = 1 /\ src = "live" /\ refs = {}
    /\ child = [c \in Children |-> "none"]
    /\ cEpoch = [c \in Children |-> 0]
    /\ creator = [c \in Children |-> "idle"]
    /\ debt = [c \in Children |-> FALSE]
    /\ req = [c \in Children |-> "none"]
    /\ crashes = 0
    /\ cascaded = FALSE /\ lateInst = FALSE
    /\ declinedRecreated = FALSE /\ conclusiveRecreated = FALSE
    /\ okDel = [c \in Children |-> FALSE]

(* release_fork_ref(source, c, cEpoch[c]) as one atomic step: the epoch     *)
(* check on its snapshot and the CAS bound to that snapshot's incarnation   *)
(* agree (a recreate in between surfaces as IncarnationChanged, which is    *)
(* conclusive too).  `ok'` receives the conclusiveness verdict.             *)
Release(c, clear) ==
    IF srcEpoch # cEpoch[c]
      THEN \* the incarnation this release was owed to is gone: conclusive
           /\ debt' = IF clear THEN [debt EXCEPT ![c] = FALSE] ELSE debt
           /\ conclusiveRecreated' = (conclusiveRecreated \/ (clear /\ debt[c]))
           /\ UNCHANGED <<src, refs, cascaded>>
    ELSE IF src = "deleted"
      THEN \* a hard-deleted source holds no live references: conclusive
           /\ debt' = IF clear THEN [debt EXCEPT ![c] = FALSE] ELSE debt
           /\ UNCHANGED <<src, refs, cascaded, conclusiveRecreated>>
    ELSE LET removed == c \in refs
             left    == refs \ {c}
             tomb    == left = {} /\ src = "soft"
         IN /\ refs' = left
            /\ src' = IF tomb THEN "deleted" ELSE src
            /\ cascaded' = (cascaded \/ tomb)
            \* conclusive iff the reference was removed (removed_ref)
            /\ debt' = IF clear /\ removed THEN [debt EXCEPT ![c] = FALSE] ELSE debt
            /\ UNCHANGED conclusiveRecreated

-----------------------------------------------------------------------------
ForkBegin(c) ==     \* fork::prepare validated the live current incarnation
    /\ child[c] = "none" /\ src = "live"
    /\ child' = [child EXCEPT ![c] = "creating"]
    /\ cEpoch' = [cEpoch EXCEPT ![c] = srcEpoch]
    /\ creator' = [creator EXCEPT ![c] = "pending"]
    /\ UNCHANGED <<srcEpoch, src, refs, debt, req, crashes, ghosts>>

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
    /\ UNCHANGED <<srcEpoch, src, child, cEpoch, debt, req, crashes, cascaded,
                   conclusiveRecreated, okDel>>

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
         ELSE \* the child was deleted meanwhile: release this request's
              \* fresh reference (the verdict does not clear the child's debt)
              /\ UNCHANGED child
              /\ Release(c, FALSE)
    /\ UNCHANGED <<srcEpoch, cEpoch, req, crashes, lateInst, declinedRecreated, okDel>>

ForkReady(c) ==     \* Ready published only after the anchor exists (C5)
    /\ child[c] = "anchored"
    /\ child' = [child EXCEPT ![c] = "ready"]
    /\ UNCHANGED <<srcEpoch, src, refs, cEpoch, creator, debt, req, crashes, ghosts>>

CreatorCrash(c) ==  \* the create request dies before its post-check
    /\ crashes < MaxCrashes
    /\ creator[c] \in {"pending", "installed"}
    /\ creator' = [creator EXCEPT ![c] = "done"]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, debt, req, ghosts>>

SourceDelete ==     \* delete_lifecycle -> delete_transition inside the CAS
    /\ src = "live"
    /\ src' = DeleteDecision(refs # {})
    /\ UNCHANGED <<srcEpoch, refs, child, cEpoch, creator, debt, req, crashes, ghosts>>

SourceRecreate ==   \* create under the same name after the tombstone (F5)
    /\ src = "deleted" /\ srcEpoch < MaxEpoch
    /\ srcEpoch' = srcEpoch + 1 /\ src' = "live" /\ refs' = {}
    /\ UNCHANGED <<child, cEpoch, creator, debt, req, crashes, ghosts>>

ChildDelete(c) ==   \* delete_transition on the child: tombstone + debt, one CAS
    /\ Alive(c)
    /\ child' = [child EXCEPT ![c] = "gone"]
    /\ debt' = [debt EXCEPT ![c] = TRUE]
    /\ req' = [req EXCEPT ![c] = "releasing"]
    /\ UNCHANGED <<srcEpoch, src, refs, cEpoch, creator, crashes, ghosts>>

InRequestRelease(c) == \* the same DELETE request continues: release_fork_ref
    /\ req[c] = "releasing"
    /\ req' = [req EXCEPT ![c] = "none"]
    /\ Release(c, TRUE)
    \* delete_lifecycle returns Ok(()) whether or not the release was
    \* conclusive: the client sees success either way
    /\ okDel' = [okDel EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<srcEpoch, child, cEpoch, creator, crashes, lateInst, declinedRecreated>>

RequestAbandon(c) == \* crash or cancellation after the tombstone CAS
    /\ crashes < MaxCrashes
    /\ req[c] = "releasing"
    /\ req' = [req EXCEPT ![c] = "none"]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, ghosts>>

RetryDelete(c) ==   \* the CLIENT re-issues DELETE: repair_tombstone
    /\ child[c] = "gone" /\ debt[c] /\ req[c] = "none"
    /\ req' = [req EXCEPT ![c] = "releasing"]
    /\ UNCHANGED <<srcEpoch, src, refs, child, cEpoch, creator, debt, crashes, ghosts>>

-----------------------------------------------------------------------------
(* Settled: nothing in flight and no debt that a retry could still pay.  A  *)
(* debt whose release is inconclusive (absent reference on the live         *)
(* incarnation it forked) may legitimately persist: that is not a stuck     *)
(* protocol, and a retry would not change it.                               *)
PayableDebt(c) ==
    debt[c] /\ (srcEpoch # cEpoch[c] \/ src # "live" \/ c \in refs)
Settled ==
    /\ \A c \in Children : creator[c] \in {"idle", "done"} /\ req[c] = "none"
    /\ \A c \in Children : ~PayableDebt(c)
Terminated == Settled /\ UNCHANGED vars

Next ==
    \/ \E c \in Children :
         \/ ForkBegin(c) \/ ForkInstall(c) \/ ForkPostCheck(c) \/ ForkReady(c)
         \/ CreatorCrash(c) \/ ChildDelete(c) \/ InRequestRelease(c)
         \/ RequestAbandon(c) \/ RetryDelete(c)
    \/ SourceDelete \/ SourceRecreate

\* Deadlock checking stays on; RetryDelete keeps a state with a payable debt
\* enabled, and Terminated is the only stuttering step.
Spec == Init /\ [][Next \/ Terminated]_vars

(* Liveness premise of F8 ("deletion debt is recoverable by retrying the    *)
(* original public operation"): the in-process steps of a live request are  *)
(* weakly fair and the CLIENT retries DELETE while a payable debt remains.  *)
(* Crashes are bounded, so they cease.  No fairness on deletes, forks,      *)
(* recreation or any success outcome.  ClientRetryFairness is the           *)
(* unestablished client premise (TLA-019-F4); the probe drops it.          *)
InProcess(c) == ForkInstall(c) \/ ForkPostCheck(c) \/ InRequestRelease(c)
InProcessFairness == \A c \in Children : WF_vars(InProcess(c))
ClientRetryFairness == \A c \in Children : WF_vars(RetryDelete(c))
LiveSpecClientRetries == Spec /\ InProcessFairness /\ ClientRetryFairness

TypeOK ==
    /\ srcEpoch \in 1..MaxEpoch /\ src \in {"live", "soft", "deleted"}
    /\ refs \subseteq Children
    /\ child \in [Children -> {"none", "creating", "anchored", "ready", "gone"}]
    /\ cEpoch \in [Children -> 0..MaxEpoch]
    /\ creator \in [Children -> {"idle", "pending", "installed", "done"}]
    /\ debt \in [Children -> BOOLEAN] /\ req \in [Children -> {"none", "releasing"}]

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
    ~(src = "soft" /\ \A c \in Children : child[c] = "ready" /\ c \in refs)
\* The late-install variant: a DELETE of the child already returned success
\* (its release found the reference absent on the live source, which is not
\* conclusive, so the tombstone kept the debt), the creator's install then
\* landed and the creator died before its post-check.  The client has no
\* signal to repeat the DELETE.
Witness_PinAfterSuccessfulDelete ==
    ~(\E c \in Children :
        /\ child[c] = "gone" /\ okDel[c] /\ debt[c] /\ req[c] = "none"
        /\ creator[c] = "done" /\ c \in refs /\ cEpoch[c] = srcEpoch)
\* The limitation: a child's DELETE crashed after its tombstone CAS, its
\* reference still pins a soft-deleted source, and no in-process step can
\* release it -- only a client retry (RetryDelete) is enabled for it.
Witness_PermanentPinWithoutRetry ==
    ~(\E c \in Children :
        /\ child[c] = "gone" /\ debt[c] /\ req[c] = "none"
        /\ creator[c] = "done" /\ c \in refs /\ cEpoch[c] = srcEpoch
        /\ src = "soft")
=============================================================================
