------------------------------ MODULE MC_RefGC ------------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-019 (physical layer, layout 6A package   *)
(* 6).  Baseline cfgs load this module without overrides; each negative    *)
(* control substitutes exactly one baseline operator (Op <- MutOp).        *)
(***************************************************************************)
EXTENDS RefGC

\* REPOSITORY NC (reader pin): the collector deletes a zeroed object once
\* its grace passed, whatever sealed reads of this process are running.
MutPinIgnored(o) == TRUE

\* REPOSITORY NC (lazy reader, Fable 5.1 S1-1): a read stops consulting
\* index sources once the chunks found so far tile its window, so it can
\* pick originals whose consolidated chunk sits in a later source.
MutLazyPicks(s) ==
    IF s = "A" /\ {"a1", "a2"} \subseteq indexed
    THEN {c \in indexed : Strm[c] = s /\ c # "ka"}
    ELSE Maximal(s)

\* REPOSITORY NC (consolidation after a purge, Fable 5.1 S1-3): the
\* consolidation's re-check ignores the stream's 'H' row.
MutConsUnchecked == Maximal("A") \cap {"a1", "a2"} = {"a1", "a2"}

\* REPOSITORY NC (sweep in flight): the orphan sweep deletes an unnamed
\* object of this opening whose operation still runs.
MutSweepAll(o) == TRUE

\* REPOSITORY NC (zombie seal, Fable 5.1 S2-1): the committer admits a
\* seal's chunk of a purged stream.
MutSealAlways(o) == TRUE

\* The segment cleaner's copy ca1 has a1's span; the controls below make one
\* of the three equal-span choices prefer the older object, or drop half of
\* the cleaner's re-check.
OlderFirst(set) ==
    IF {"a1", "ca1"} \subseteq indexed /\ "ca1" \in set
    THEN (set \ {"ca1"}) \cup {"a1"}
    ELSE set

\* REPOSITORY NC (tie read): a read picks the older of two equal spans.
MutTieReadPicks(s) == OlderFirst(Maximal(s))

\* REPOSITORY NC (tie reclaim): a reclaim decrements the older of two equal
\* spans.
MutTieReclaimPicks(s) == OlderFirst(Maximal(s))

\* REPOSITORY NC (tie keep): a fold or merge keeps the older of two equal
\* spans and drops the copy.
MutTieKeepCovered(c) ==
    IF c = "ca1" THEN "a1" \in indexed
    ELSE \E d \in indexed \ {"ca1"} : c \in Covers(d)

\* REPOSITORY NC (cleaner ignores a purge): the cleaner's re-check ignores
\* the stream's 'H' row.
MutCleanIgnoresDead == "a1" \in Maximal("A")

\* REPOSITORY NC (cleaner ignores the picks): the cleaner's re-check ignores
\* whether its chunk is still a pick (a consolidation superseded it).
MutCleanIgnoresPick == dead["A"] = "live"
=============================================================================
