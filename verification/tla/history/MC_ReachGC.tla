----------------------------- MODULE MC_ReachGC -----------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-019 (physical layer).  Baseline cfgs     *)
(* load this module without overrides; each negative-control or probe cfg  *)
(* substitutes exactly one baseline operator (Op <- MutOp).                *)
(***************************************************************************)
EXTENDS ReachGC

\* UPSTREAM-CONTRACT NC (generation): delete from the observed (old)
\* reachability snapshot using only the min_age condition, without the
\* low-watermark / newest-L0 generation conditions.
MutEligibleAgeOnly(x) == gen[x] < gc.age /\ x \notin gc.refs

\* UPSTREAM-CONTRACT NC (checkpoint pin): the collector ignores
\* checkpoint-referenced manifests, user and compactor checkpoints alike.
\* The repository creates no user checkpoints; the compactor's own
\* checkpoint is modelled by CompactionCheckpoint.
MutRefsIgnoreCheckpoint == man

\* UPSTREAM-CONTRACT NC (stale inventory): after the first LIST the collector
\* keeps trusting that first inventory forever.
MutInventoryFrozen ==
    IF firstInv.set THEN firstInv.inv ELSE {x \in Obj : st[x] = "present"}

\* REPOSITORY NC (advance ordering): the absorbed boundary advances once the
\* covering SST is UPLOADED, before the manifest commit that references it
\* (e.g. submitting the AbsorbedBatch before part.flush() returns).
MutAdvanceOnUpload(x) == st[x] = "present"

\* REPOSITORY NC (read error handling): the history read path treats a
\* storage error on a deleted SST as "no rows" and completes the page.
MutRepoSwallowReadError(r) == IF r = "error" THEN "short" ELSE r

\* ASSUMPTION PROBE (ASM-SLATEDB-GC (iii), not production): the upstream
\* read of a deleted SST returns a short or empty success.
MutUpstreamShortRead == "short"

\* UPSTREAM-CONTRACT NC (the model before the compactor checkpoint was
\* added; TLA-019-F1): the compactor commits without checkpointing the
\* pre-compaction manifest.
MutNoCompactionCheckpoint(m) == {}
=============================================================================
