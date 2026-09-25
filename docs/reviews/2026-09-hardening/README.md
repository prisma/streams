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
| `NEXT-WORK.md` | The handoff: every task not done after the second external review, in the owner's priority order, with the owner's decision text, current state, design, tests and gates for each. |
| `plans/` | The implementation plans the program worked from, one directory per planning round (`plans18`-`plans20` hold the item 40, item 50, 38/39, absorber, F1, F2, F3, bug #7 and effective-config plans). Each plan names the tree it was written against; line numbers and scratch paths are as of that tree. |
| `evidence/split-investigation.json` | The split-boundary investigation and its skeptic's corrections (the hold's timing figures, F1, F2, F3). |
| `evidence/refused-chain-probe.patch` | The review probe that builds a refused chain's overlapping postings pages (NEXT-WORK item 1). |
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
   - Second review (2026-09-25): project usage totals need an unrestricted
     grant and a final-record seal needs the append scope (1d4d8660,
     d58ebf15); the wrapper's held diagnostic is generic, it forwards the
     platform's stop, and staging refuses hidden files (b7f6dc3a); a
     recreation records the replaced incarnation's storage close as a
     durable debt (2ba4bc47); item 50 option (a) (45f8711c).
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

**Closed by the owner on 2026-09-25** (second external review of 9813d1cb):
"Closed: a causal absorber-accounting defect matching the observed failure
class is fixed and regression-covered. Attribution of the original incident
remains probable." Limits that stay explicit:

- This was not a demonstrated append-during-split failure: the corrected
  investigation places the 500 before the split, and the capacity test joins
  its clients before splitting, so this evidence does not certify concurrent
  append/split behaviour.
- The two throughput-ratio misses (1.73 and 1.77 against 1.8) remain misses:
  correctness acceptance and performance acceptance are separate, and
  "consistent with host contention" is not a proven explanation.
- The overlapping-postings residual is a separate issue, accepted for this
  closure because the reader has a bounded canonical fallback
  (`o4a_stored_overlapping_pages_cannot_skip_a_canonical_match` tests overlap
  handling). Still to do: a composed test that creates the refused/chained
  state and reads it through the public path; stale-page repair is scheduled.

## Owner decisions from the second external review (2026-09-25)

| Question | Decision | State on `slate` |
| --- | --- | --- |
| Edge #53 usage `?streamId=` | Ratified | 7549e28a, e347fb1a |
| Edge #54 F3 retry re-preparation | Ratified | 0f4cd8c1 |
| Edge #55 transient raw close | Ratified; normalize the owed-claim renewal failure | 839135a4; renewal failure now 503 `seal_incomplete` (1d4d8660) |
| HOLD-SPLIT-500 | Closed (narrow claim, above) | 5a6f9f56..b2a8001f, 882004d9 |
| Restricted credentials and project-wide usage | Denied: `usage.read` AND an unrestricted effective grant | 1d4d8660 (edge #56) |
| `:seal` with a final record | Needs `lifecycle.manage` AND `records.append` | 1d4d8660, d58ebf15 (edge #57) |
| Staging hidden files, stray entries | Refuse before install; fresh allowlisted directory | b7f6dc3a |
| Held wrapper diagnostic | Generic and unhealthy; details only in the log | b7f6dc3a |
| Wrapper signal forwarding | Bounded forwarding | b7f6dc3a (Compute lifecycle still to verify) |
| Idle-expiry recreation billing | Release blocker: durable, generation-fenced cleanup obligation | 2ba4bc47 (closure debts) |
| Item 50 | Option (a): preserve live bindings, cap 32,768, reject `HANDLE_IDLE_EVICT_SECS=0` | 45f8711c (holder rule added beside the counted pin; the pin's full retirement not done) |
| Effective configuration | Method and E3 transcription accepted; 120 s default accepted | Deployment gates below |
| Bug #7 | Option (b), explicit migration; activation gated on a real-DB rehearsal | Not started |
| Item 40 | Separate Critical heartbeat, progress and eligibility | Not started |
| F1 | Authenticated fleet-internal seal-fence operation | Not started |
| F2 | Keep the unknown-outcome model; public append/seal plus successor composition test | Not started |

### Deployment gates (block deployment sign-off, not merging)

- Compute validation of the wrapper -> binary -> platform lifecycle (startup,
  readiness, restart, signal delivery, memory pressure, rollback).
- The Compute deployment owner enumerates every target project and exports
  its redacted configuration; comparison and boot validation re-run on the
  final candidate with the actual persisted namespace constraints (the
  archived comparison's "HEAD" is an earlier revision).
- A release-posture Compute family (production fleet authentication, usage
  and audit configuration); the static fleet-auth bridge stays a benchmark
  exception.
- Upstream idle compatibility with the 120 s header timeout, with margin, or
  an explicit validated timeout.
- Bug #7 rehearsal on a consistent copy of a real rollup database before its
  format-changing activation.
- Release-wide verification on one final artifact: the mutation campaign over
  the whole hardening range, including billing, rollup, product and usage
  owners the per-push plan does not select. Per-push runs so far (for
  example 81 mutants over 26c555dd..9813d1cb) are not that campaign.

## Follow-ups found in review, not done

- Typed classification of an append's first registry read: a transient store
  failure still answers 500 there (F3 plan D2). Today the registry reports an
  injected or real store failure and a descriptor corruption with the same
  error shape, so the fix needs a typed registry read error first; corruption
  must stay a fail-closed 500.
- Stale-page repair after a refused chain: superseded by d16559b3 (readers
  admit agreeing overlaps); the composed public-read test is b059e4a2.
- Item 50's plan retires the counted admission pin in favour of the holder
  rule alone; the holder rule now sits beside the pin.
- Closure debts (2ba4bc47; the settlement pass's starvation fixed in
  40dbf0d3): tests for month crossing, owner movement mid-debt and a crash
  between the debt write and the replacing write.
- `parse_month` accepts a `+` sign ("2026-+9"), answering a zero row instead
  of 400 `invalid_month` (pre-existing, low).
- `dst::dst_tests::admission_maintenance::first_request_waits_for_restoration_then_sees_the_restored_ledger`
  orders its request with fixed sleeps and failed once under host load.
