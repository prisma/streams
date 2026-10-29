----------------------------- MODULE MC_SealRead -----------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-018 (layout 6A reads).  Baseline and     *)
(* witness cfgs load this module without overrides except the key map;     *)
(* each negative-control cfg substitutes baseline operators with the Mut*  *)
(* operators below (Op <- MutOp).                                          *)
(***************************************************************************)
EXTENDS SealRead

\* Records 0 and 2 belong to K1, record 1 to K2 (and record 3, in the
\* four-offset shape, to K2).
MCKeys == {"K1", "K2"}
MCKeyOf == [o \in 0..2 |-> IF o = 1 THEN "K2" ELSE "K1"]
MCKeyOf4 == [o \in 0..3 |-> IF o \in {1, 3} THEN "K2" ELSE "K1"]

\* NC late directory (the race DST's control, `SealHooks::late_directory`):
\* the chunks are published only after the commit is durable, so a reader
\* that sees the moved boundary finds no chunk below it.
MutPublishLate == FALSE

\* NC hole consumed: a window offset no chunk holds is treated as consumed
\* (the reader skips to the next chunk or the part's end), instead of the
\* honest partial.  Its cfg also substitutes MutPublishLate: while step 5
\* precedes step 6 the model (like production) never has a hole below
\* the boundary, so HoleConsumed alone changes no behaviour and could not
\* fail; and the late directory alone is masked for NoSkip by the honest
\* partial (it breaks NoShortRead instead).  Together they show that the
\* honest-partial rule is what keeps a hole from becoming a skip.
MutHoleConsumed == TRUE

\* NC summary under-approximates: a chunk's summary omits the read's own
\* key, so a keyed read consumes a chunk holding its records without a GET.
MutSummaryUnder(c, fp) == ActualKeys(c) \ {Filter}

\* NC second lock (pre-fix TLA-018-F4): the unsealed part reads the
\* handle's pending pages under a second acquisition of the state lock, so
\* a seal committing after R1 has dropped the pages [P, new boundary) and
\* the read fails as RecordCorruption::Missing or Misplaced.
MutSecondLock == [a |-> aa, n |-> nx]
=============================================================================
