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
`slate`.

1. **A reliable acceptance record.** This directory; the exception audit
   (6a36e406); the exception-growth gate (bcddd615); the platform-e2e negative
   control; the preserved failed gate log.
2. **Correctness findings.**
   - Done: the task-supervisor deferred-drop slot and the corrected F-G wording
     (95bfabc6); the tracker race in `QuotaRegistry::admit` (d4d631df).
   - In progress: the product-append 413 (stable code, its limit, no quota
     debit before it); the split-time 500 (a controlled-pause investigation at
     every split boundary).
   - Not started: item 28 metering (a typed outcome and metering identity from
     the application owner), capability placement (`status_and_quotas` aligned
     with `served_policy`), `WWW-Authenticate` on every 401, item 73
     (`ProductOperation`, authenticated unknown operations 404/405).
3. **Upgrade and recovery.** Plans in progress for items 38/39 (bounded process
   exit on a critical-task failure, with a watchdog off the executor), 40
   (heartbeat draining from readiness, health-aware ring eligibility) and 50
   (never evict live tracker state, with its sizing decision). Not started: bug
   #7 migration tooling and its rehearsal harness; an old-versus-new effective
   configuration comparison for every deployed configuration family. The
   rehearsal on a consistent copy of a real rollup database and the Compute
   validation need the owner.
4. **Release-wide verification.** Not started: a mutation run over the whole
   hardening range, including billing, rollup, product and usage, on one pinned
   artifact.

The split-time 500 remains a release hold until it has a causal fix with a red
regression, or a documented legitimate outcome with an approved protocol or
test change.
