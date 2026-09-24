# Readiness matrix (authoritative)

Updated 2026-08-14 (R30). Older campaign documents are historical
evidence; where a status here disagrees with an older doc, THIS table
wins.

## Launch-scope verdicts (R30 review)

| Scope | Verdict |
|---|---|
| Internal development | Go |
| Existing private preview | Go |
| Larger unbilled private preview, dedicated cells | Go after rc.1 exact-binary certification |
| Public preview, dedicated tenancy | Conditional: platform availability + external security review |
| Paid dedicated-tenant GA | No-go until invoice/reconciliation closes |
| Shared multitenant GA | No-go until per-request principals + tenant quotas |

## Engine (application) — DONE
- Durable maintenance backpressure, exact frame-byte ledger, typed shed:
  PASSED at 100.6% of cap (CAPACITY-R27.md).
- Fleet handoff at peak backlog with exact gauge restoration + exactly-
  once reconcile: PASSED (handoff-fh185257).
- Cold-shard scheduler: custody model, budget before opening, walk
  continuation cursor (R30), single-guard close (R30). DST gates cover
  peak, fairness, races, rotation.
- Compactor memory profile on EVERY DB family, boot-certified
  (MEMPROFILE_CERT) + structural test.
- Build identity end to end: git commit + build time + binary sha +
  boot id on /readyz and /v1/debug/load; campaign verifier compares all.
- Cost of the survival profile: +4.7% cost units (accepted).

## Tenancy — PLAN FROZEN, IMPLEMENTATION IN PROGRESS
**Many projects per cell is the chosen architecture** (decided
2026-08-14; implementation plan delivered and frozen 2026-08-15 as
`docs/MULTITENANCY.md` — that document is the contract; the
stage-by-stage conversion inventory is `docs/MULTITENANCY-MAP.md`).
Shared-cell GA blockers tracked there (§20): project-qualified
identity everywhere (layout 4), per-request verified principal with
exact ownership/grant version checks, project/workspace quotas,
transfer without data rewrite, separate customer/fleet/operator/watch
trust boundaries, shared-cell field battery + external security
review. Until Stage 8 exits, deployments remain
one-project-per-cell dedicated.

## Platform (Prisma Compute) — BLOCKING GA
1. Stale-build serving: deploys must carry an expected digest, VMs must
   report the running digest, readiness must fail on mismatch, and the
   edge must route only digest-matching-ready instances. Application
   detection exists; prevention must be platform-owned, with platform
   acceptance tests.
2. Silent zombie / crash-loop: a dead process must be removed and
   replaced; today the version stays "running" and the edge keeps
   routing infrastructure failures.
3. cgroup memory.peak not exposed to the sandbox (kernel-peak capture
   blocked).

## Billing — usage dashboards provisional; invoicing DISABLED
Remaining before paid GA (OBSERVABILITY-BILLING-STATUS.md): ledger <->
rollup <-> artifact reconciliation + drift alerts, invoice export with
acknowledged delivery, retention jobs, three-node fleet billing
acceptance, ops-history/SLO work, documented terminal-closure residual.

## Ops & security before GA
- Two+ ready instances per active cell; stable service/LB endpoint
  (never version-scoped preview URLs); documented SLO regions only.
- External security review; token rotation/revocation; dependency
  scanning; restore/corruption/region-loss drills on record.
- Alerts: maintenance backlog, bulk-gate waits, compaction working set,
  scheduler deferrals (walk_deferred_total), terminal billing debt,
  fork cleanup debt (fork_debt_stale: a pending marker or a stalled
  reconciler circle older than 3 × FORK_DEBT_SWEEP_SECS),
  digest mismatch, platform-origin 5xx/404, billing readiness.

## Service obligations from formal verification (open)
Recorded by the first formal-verification spike
(docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md §0). Each stays open
until its acceptance criteria are met. None of them is a model result.
- **Physical reclamation of hard-deleted incarnations (TLA-019-F3).** Hard
  deletion writes only a registry tombstone
  (`src/application/creation/deletion.rs`). No code deletes a deleted or
  expired incarnation's shard-log rows (`record_key`), tail and producer
  rows, or history-partition rows. The only row deletes are
  absorbed-boundary trims. The owner has to adopt a policy. Acceptance:
  (1) a written policy naming which rows are reclaimed, after what delay,
  and what fork references, checkpoints and billing closes must be
  released first; (2) after a hard delete, or a TTL expiry that reaches
  hard deletion, with no fork child left, every row keyed by that
  incarnation's hash is deleted within the stated delay, and SlateDB GC
  then reclaims the SSTs; (3) a same-name recreate, a live fork child and
  an unsettled billing close each block reclamation, with a DST scenario
  for each; (4) reclamation deletes run under the global trim or GC
  budget, never as one unbounded batch (H5, R8); (5) a gauge of
  unreclaimed deleted incarnations and their oldest age, with an alert.
- **GC convergence without further writes (H14, TLA-019-F2).** An
  unreferenced SST newer than a quiet partition's last compaction or newest
  L0 is never collected. Acceptance is in
  docs/dst/DST-EXPANSION-SPEC.md §9.12.3: the SST is deleted within a
  stated bound on a partition with no writes, with no new periodic LIST.
- **H11 completeness against lost durable postings or rows (TLA-018-F2).**
  This is an owner decision between a coverage mechanism and a revised
  contract (docs/dst/DST-EXPANSION-SPEC.md §9.12.2). Until it is made,
  H11 is not met.

## Deliberately NOT on the GA path
Generic Compute autoscaling, the full deterministic simulator (#108),
SlateDB ReadIoMetrics (#197), warm-scan optimization.
