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

## Tenancy — DECIDED, implementation pending
**Many projects per cell is the chosen architecture** (2026-08-14;
implementation plan in preparation, owner: Søren). Consequences this
table tracks as GA blockers for shared cells:
- per-request trusted principal (account, project, permissions);
- tenant-scoped quotas (streams, append/read bytes + requests,
  subscriptions, consumer ops, stored bytes, monthly spend).
Until that lands, deployments are one-project-per-cell dedicated.

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
  digest mismatch, platform-origin 5xx/404, billing readiness.

## Deliberately NOT on the GA path
Generic Compute autoscaling, the full deterministic simulator (#108),
SlateDB ReadIoMetrics (#197), warm-scan optimization.
