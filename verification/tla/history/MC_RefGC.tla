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
=============================================================================
