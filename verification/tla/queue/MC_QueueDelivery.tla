------------------------- MODULE MC_QueueDelivery -------------------------
(***************************************************************************)
(* Model-checking wrapper for TLA-QD (QueueDelivery).  Baseline and        *)
(* witness cfgs load this module without overrides; each negative-control  *)
(* cfg substitutes exactly one baseline operator with a Mut* operator      *)
(* below (Op <- MutOp).  The numbers are S3b-DESIGN §11.1's.               *)
(***************************************************************************)
EXTENDS QueueDelivery

\* NC 1: a lane lease that does not open the range row's reference.
MutLaneOpen(r) == opn[r]

\* NC 2: the frontier passing an unleased immediate offset: a live page
\* counts as not live, so LanePass takes it into the gap untaken.
MutHeadLive(f) == FALSE

\* NC 3: a cancel that marks nothing, so the frontier leases it later.
MutCancelMarks(o) == cancel

\* NC 4: the scheduled claim clears the message's sbits bit: the frontier
\* then takes its page for a resolved one's while it is leased.
MutScheduledClaimKeepsSbits(o) == sbits \ {o}

\* NC 5: a settle without the generation check: a replayed receipt acts on
\* the current lease.
MutGenOK(o, g) == d[o].st = "L"

\* NC 6: an internal unlease that spends a release.
MutUnleaseReleases(row) == row.rel + 1

\* NC 8: an internal unlease with no row counter: a poison body is leased
\* without bound.
MutUnleaseCounts(row) == row.iu
=============================================================================
