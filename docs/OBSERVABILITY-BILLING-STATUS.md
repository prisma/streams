# Billing / telemetry — implementation status

**Companion to** [OBSERVABILITY-BILLING.md](./OBSERVABILITY-BILLING.md)
(the normative design). This file is the truth about WHAT EXISTS TODAY:
the per-mechanism implementation matrix, the review-round history, and
the gates still open before invoice-grade operation. Where the design
document reads as if a mechanism exists and this matrix says otherwise,
this matrix wins.

Updated: 2026-08-07 (post round-22 boundary review).

## Implementation matrix

| mechanism | state | where |
|---|---|---|
| Committer-atomic segment billing state (`<seg>B` row, `U` dirty index, `V` month finals, one WriteBatch with customer data) | DONE | src/shard.rs committer billing block |
| Trusted billing time (server wall clock; `BILLING_CLOCK_OVERRIDE` for tests only) | DONE | src/billing.rs `billing_now_ms` |
| Storage clock with month splitting + idle multi-month rollover | DONE | `SegmentBillingMetaV1::advance_storage_clock` |
| One public-response read meter (bytes/records/ops/queue/append counted at response choke points) | DONE | src/http.rs + src/product.rs meter sites |
| Durable per-instance read spool (sealed batches durable BEFORE ledger; remainder-requeue on mid-loop fault; corrupt-row quarantine + persistent count + alert; open verifies readability) | DONE (round-22 items 2a/2c) | `ReadSpool`, `spool_sealed` |
| `_usage` ledger, fleet-safe delivery (one `Streams-Replay-To` hop, `FLEET_INTERNAL_TOKEN`) | DONE | `system_append` / `system_read` |
| Drain: HARD bounds (envelopes AND encoded bytes incl. finals/lifecycle, per-row atomic admission) | DONE (round-22 item 9) | `drain_once` |
| Financial scans fail CLOSED (dirty/finals scan error defers the engine; registry error defers the row) | DONE (round-22 item 6/7) | `drain_once` |
| Terminal closure as a durable saga: `logical_close_ms` stamped in the tombstone write; closes account to the persisted instant; awaited submission; tombstone-walk reconciler; durable `retained_by_forks` | DONE (round-22 item 7) | registry stamp, `CommitOp::BillingClose{close_ms}`, `tombstone_walk` |
| Rollup: exactly-once page application (source-seq dedupe, absolute→delta, cursor in one WriteBatch) | DONE | src/rollup.rs `apply_page` |
| Multi-segment idle carry merges (page-local caches; sum not last-put-wins) | DONE (round-22 item 3) | `close_month` pass A |
| Same-version final/live tie-break on the storage clock | DONE (round-22 item 4) | `apply_snapshot` |
| Exact integer month split for read batches (remainder-to-last; >2-month spans) | DONE (round-22 item 5) | `month_spans` + `apply_read_batch` |
| Two-phase month finalization: frozen invoice base + artifact staged in the same batch | DONE | `close_month` pass B |
| Ordered missed-month catch-up (persisted oldest-unclosed marker; oldest-data init) | DONE (round-22 item 8) | `close_months_due` |
| Month-close chunks bounded by rows AND bytes | DONE (round-22 item 9) | `close_month` |
| Corrections: full dimensions + `correction_id`/version/source event/created-at; applied exactly once; materialized sums on month rows AND name/project aggregates; immutable per-correction artifacts | DONE (round-22 item 8) | `UsageCorrection`, `push_correction` |
| Artifact publication verifies AlreadyExists CONTENT; mismatch = standing alarm, row stays pending | DONE (round-22 item 8) | `publish_artifacts`, `ARTIFACT_MISMATCHES` |
| Usage API: per-stream month/current with provisional storage, historical incarnations, effective totals + correction list | DONE | `product_usage` |
| `GET /v1/projects/{project}/usage` (aggregate + correction sums + effective) | DONE (round-22 D3) | `project_usage` |
| Tenancy: ONE PROJECT PER CELL is the deployment contract; required mode refuses placeholders; per-request principals named as the shared-cell precondition | DECIDED + documented (round-22 item 1) | design doc §artifacts/tenancy; src/main.rs gate |
| `BILLING_MODE=required` readiness: sync spool open (verified readable) + sync rollup-DB open on the rollup owner BEFORE serving; `/health` 503 until ready; no memory-only fallback | DONE (round-22 items 2b/10) | src/main.rs, `health_axum`, `drain_once` |
| Billing-readiness surface (`GET /operator/billing.json`: ready, spool depth/quarantine, drain recency, rollup cursor age, oldest-unclosed month, pending/mismatched artifacts, walk counters, open alerts) | DONE (round-22 item 10) | `billing_readiness_axum` |
| Ops event journal (`_ops_events`) + ops metrics (`_ops_metrics`) + m1 rollup + raw retention + alert evaluator | DONE (basic set) | src/ops.rs, rollup ops section |
| Legacy planes (`_billing`, `__metrics__`, metrics.rs) | DELETED (hard cutover) | — |

## Review-round history

- Round 20 (design approval + P0-P6 implementation) — v0.2.0-preview.6.
- Round 21 (11 invoice blockers: trusted time, idle carry, read
  durability, fleet delivery, closure, finalization, tenant keys,
  bounds, identities) — v0.2.0-preview.7.
- Round 22 (boundary review: 10 must-fix + 10 doc items) — commits
  R22-A..E on `slate`; judge-verified closure required before tagging.

## Still open before invoice-grade operation

1. **Reconciliation jobs** — ledger vs rollup vs artifacts sweeps
   (beyond the terminal-closure tombstone walk) with drift alerts.
2. **Invoice export** — the artifact → billing-system handoff (format,
   delivery, acknowledgement) is not built; artifacts + corrections
   are the inputs.
3. **Ops workstream 2** — comprehensive ops history / SLO telemetry
   (current set is the basic §11-§13 slice).
4. **Retention jobs** — `_usage`/`_ops_*` ledger trimming beyond the
   raw ops sweep; spool quarantine lifecycle policy.
5. **3-node fleet acceptance campaign** — the design's §16 battery
   (owner moves, crash/restart, month boundary under churn, WAN) on a
   real fleet; single-instance legs exist, the fleet run does not.
6. **Known residual (documented):** a closure lost to a crash while
   its row was clean AND the name recreated within one sweep interval
   leaves that incarnation's gauge reachable only via the dirty-path
   reconciler (`tombstone_walk` doc comment).

## Acceptance campaigns (design §16) — status

| campaign | status |
|---|---|
| Suite + DST (crash points, clock abuse, boundary races) | GREEN (326 tests at R22-E) |
| Single-instance telemetry E2E (ci-fanout leg: meter → spool → ledger → rollup → API) | GREEN (bench/fleet/ci-fanout.sh) |
| Fleet (3-node) billing acceptance | NOT RUN |
| Month-boundary soak (clock-driven, multi-day) | NOT RUN (simulated in tests only) |
| Invoice-export dry run | BLOCKED on export tooling |
