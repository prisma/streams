# September 2026 hardening program: acceptance record

From 2026-09-21 a coding agent worked directly on `slate` through a numbered
list of review items and bugs. On 2026-09-24 an external reviewer reviewed
the snapshot at 24c4c77a. The verdict: keep the demonstrated fixes, but do not
approve production deployment. The next phase is release safety, not another
numbered batch. The repository owner adopted the reviewer's positions as their
decisions. This directory is the record the reviewer asked for, kept in the
repository rather than in a session scratchpad.

## Contents

| Path | What it is |
| --- | --- |
| `report/report.html` | The program report as written at the end of the batches (a self-contained page). |
| `report/ledger.json` | Every landed item: commit, what changed, how it was proven. |
| `report/decisions.json` | The decisions held for the owner, and the 52 edge changes made (`edge_changes_made`). |
| `report/remaining.json` | Items not done, open observations and follow-ups, as of 24c4c77a. |
| `report/process.json` | Method, gate traps, questions for the reviewer, risks. |
| `report/external-review-claims.json` | The reviewer's claims, each checked against the tree by independent read-only passes. |
| `edge-changes.md` | A before/after contract record for each of the 52 edge changes, grouped by risk (5 high, 11 medium, 36 low), each checked against its commit. One record (#45) did not match its recorded text: an infinite scaler cooldown disables a stream's first transition too, not only re-scaling. |
| `effective-config-diff.md` | The old-vs-new (v0.2.0-rc.4 vs HEAD) effective-configuration comparison for every deployment family: verdicts, changed fields, notices, and what the owner must decide before deploy (evidence in `evidence/effective-config/2026-09-24/`). |
| `plans/` | The implementation plans the program worked from (68 files, one directory per planning round). Each plan names the tree it was written against; line numbers and scratch paths are as of that tree. |
| `evidence/gate-b12-capacity-leg-500.log` | The failed local gate leg: one append answered 500 right after a split in `post_split_throughput_scales` (the release hold). |
| `evidence/gate-b12-gate-output.txt` | The whole gate run that leg belongs to. |

Related records elsewhere in the repository:

- `docs/quality/exception-audit-2026-09.md`: every exception reason the program
  edited, classified with the ratchet's own contracts, the adversarial review of
  the new gate, and the owner decisions D1-D9.
- `docs/RUST-QUALITY.md` and `AGENTS.md`: the exception-growth rule the
  reviewer asked for (an agent never authorizes growth of its own exception).
- `scripts/platform-e2e-negative.mjs`: the platform-e2e battery's negative
  control, which requires two refused fault injections to fail exactly their
  three checks.

## Work packages the reviewer set, and their state

The reviewer grouped the remaining work into four packages. Commits are on
`slate`; state as of 2026-09-25.

1. **A reliable acceptance record.** This directory (d255ad6d); the exception
   audit (6a36e406); the exception-growth gate (bcddd615); the platform-e2e
   negative control (1d079e01); the preserved failed gate log; the
   effective-configuration comparison (42b92d81..26c555dd).
2. **Correctness findings.** Each fix below has a red regression and passed an
   adversarial review; findings of those reviews were fixed before push unless
   listed under "Open" below.
   - Task-supervisor deferred-drop slot and the corrected F-G wording (95bfabc6).
   - Tracker race in `QuotaRegistry::admit` (d4d631df).
   - Product-append 413: stable code, its limit, no quota debit before it
     (9b3a3829, eac89829, eda79ccf).
   - `WWW-Authenticate` on every 401 (da635ec8).
   - Capability placement: `status_and_quotas` answers from `served_policy`
     (46f668b3).
   - Item 28 metering from the typed outcome and the incarnation it committed
     to (e6453674, 2ffff86c).
   - Item 73 `ProductOperation`: authenticated unknown operations are refused
     by route, 404/405 (7fb70f65).
   - Usage `?streamId=` read another stream's usage outside the prefix grant
     (found by item 73's review): now authorized by the name the rollup
     recorded for the id (7549e28a, e347fb1a; edge change #53).
   - The split-time 500 (release hold, below): 5a6f9f56, 9c6675d7, b5751e75,
     b2a8001f; and F3, a registry read failure in an append's retry
     re-preparation answered a non-retryable 500 at the split boundary
     (0f4cd8c1, edge change #54). Found with it: a raw close whose seal intent
     failed transiently answered a false 409 `sealed` (839135a4, edge change
     #55).
3. **Upgrade and recovery.**
   - Items 38/39: the binary's process root answers a critical loop's exit
     with its ordered stop, bounded at 30 s off the executor, and exits 1; the
     deploy wrapper exits with the child's code after a death once the child
     has been ready 60 s, so Compute replaces it (f657ecab, fdb6c3bf,
     e966863d, then two review rounds: 21fd165d, 80e29dab, 8f9d2b0d, da06acad,
     27ea208c, 6eca6787). Campaign deploys re-stage their wrapper from
     `deploy/` (`bench/stage-app.sh`).
   - Effective configuration, rc.4 vs HEAD, for every deployment family: no
     verdict changes; four changed fields; `effective-config-diff.md` lists the
     owner decisions that still block a deploy (a real per-project env export
     above all).
   - Not done: item 40 (heartbeat draining, health-aware ring eligibility),
     item 50 (tracker sizing), bug #7 migration tooling and its rehearsal on a
     consistent copy of a real rollup database, and the Compute validation of
     items 38/39 (startup, readiness, restart, memory pressure, rollback).
     Plans exist; the rehearsal and the Compute runs need the owner.
4. **Release-wide verification.** Not started: one mutation run over the whole
   hardening range, including billing, rollup, product and usage (owners not
   selected by the per-push plan today), on one pinned artifact.

## The split-time 500 (release hold HOLD-SPLIT-500)

Cause (probable, not established; see the elimination argument): the
absorber's dirty-index rescan rolled a lane mark back while the batch
absorbed from it was submitted but not yet applied. The next gather re-read
the in-flight range, the committer retired those bytes a second time, the
maintenance ledger's checked subtraction failed, and the whole commit group
was rejected ("maintenance accounting diverged"), so every append co-grouped
with it answered 500. Timing, as the investigation's skeptic corrected it:
across runs cap-01..cap-20 the split started at 8.598-8.742 s and the run had
16 rollbacks in the third pre-split window; the 500 fell before the split (no
post-split window can end before about 9.7 s, and the test joins every client
before `execute_split` and opens both children before restarting load, so no
client append overlaps the split). Elimination: with a fault-free store,
maintenance divergence is the only source of such a 500 reachable in the
capacity run. The initial commit message (5a6f9f56) carries the uncorrected
figures (8.64-8.92 s, 18 rollbacks); the figures here supersede them.

Fix: the committer retires an absorbed advance only from its own boundary
(9c6675d7), and a lane mark is rolled back only when no submitted advance of
its stream can still land, tracked by per-stream-bucket settlement receipts
(b5751e75). Reds assert exact ledger bytes, not only the absence of a 500.
Acceptance: 20 loaded runs of the capacity test with its absorber event
counter (b2a8001f): zero divergences, zero dropped advances, zero refused
appends; an A/B against the pre-fix tree under the same load diverged in 5 of
10 runs. Two of the 20 runs missed only the 1.8x throughput ratio (1.73,
1.77) with pre/post-split throughput equal to the pre-fix tree's, i.e. host
contention. C9: a single-instance split retires no engine, so the capacity
test never exercises the Moved/retirement settlement path; R5b covers a
refused group settling its receipt.

Residuals, recorded rather than fixed (low, adversarial review of the fix):
the rollback deletes no postings pages, so a refused group that carried a
stream advance with a later advance of the same stream chained behind it
heals into overlapping pages (ledger exact; that key's page bucket reads
through the envelope fallback). The same holds for a bucket-sharing stream's
dropped gather and for an engine retirement that drops two chained advances.
Pre-fix, the same overlap arose whenever a rescan rolled back first, and a
refused chain left a phantom ledger instead. See docs/HISTORY-V2.md.

Closure is the owner's call. The criterion (a causal fix plus a red
regression) is met on the evidence above; the attribution stays probable.

## Open for the owner

- Ratify or reverse edge changes #53 (usage `streamId`, high by the rubric),
  #54 (F3, low) and #55 (raw close 409 -> 503, medium), each recorded by its
  implementer in `edge-changes.md`.
- Close HOLD-SPLIT-500, or name what else closure needs.
- Project usage totals (`GET /v1/projects/{project}/usage`) answer any
  `streams.usage.read` credential whatever its prefix grant, so a
  prefix-limited credential can subtract its streams from the total
  (pre-existing, low): refuse it for prefix-limited credentials, or document
  that project totals are visible to any usage reader.
- `:seal` with a `final` body appends a record under
  `streams.lifecycle.manage` alone (pre-existing): whether it should also
  demand `streams.records.append`.
- The effective-configuration decisions (`effective-config-diff.md`), bug #7
  migration design (a boot-time copy is a startup rewrite), item 40's drain
  scope, item 50's sizing, F2 (a retiring engine's written group) and F1's
  cross-instance route (both have plans).

## Follow-ups found in review, not done

- Recreating a name over an incarnation that expired while idle never closes
  that incarnation's billing gauge, so month close keeps carrying its storage
  (pre-existing, medium; wider than the crash-only residual `tombstone_walk`
  accepts).
- A heal after a refused chain should delete the stream's stale postings
  pages in [durable, mark) (the residual above).
- `parse_month` accepts a `+` sign ("2026-+9"), answering a zero row instead
  of 400 `invalid_month` (pre-existing, low).
- An append's first registry read still answers 500 on a transient store
  failure (F3 plan D2); a close's failed renewal of an owed claim answers 503
  `internal` where its intent failure answers 503 `seal_incomplete`.
- The deploy wrapper does not forward SIGTERM/SIGINT to the binary
  (pre-existing).
- `dst::dst_tests::admission_maintenance::first_request_waits_for_restoration_then_sees_the_restored_ledger`
  orders its request with fixed sleeps and failed once under host load.
