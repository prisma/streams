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

## OOM review (2026-08-07): preview.7 kill — mechanisms landed

The ab21 A/B campaign killed preview.7 under load (both conc 128 and
192, each after ≈1 GiB accepted payload). The review's diagnosis —
history-L0/absorber backpressure amplified by process-wide memory
budgeting failures — is supported by the recorded facts: the failed
run's preserved bucket shows **16 physical shards** (16 × 32 MiB
nominal gather exposure = 512 MiB), and both telemetry SlateDB DBs
(read spool; rollup under ROLLUP=1) opened with SlateDB per-DB
defaults instead of bounded caches.

### Claim correction (supersedes earlier wording)

`BILLING_METER=off` disables ONLY the committer-side
SegmentBillingMetaV1 mutation. It does NOT disable the read meter, the
read-spool DB, `_usage` ledger appends, the rollup consumer/DB,
`_ops_events`/`_ops_metrics`, or the owned-shard outbox sweep. The
billing-off arm therefore exonerates the same-WriteBatch billing
metadata — and NOT the round-20/21 telemetry subsystem. The full-off
discriminator is `USAGE_STREAM_KEY` unset + `ROLLUP=0` +
`BILLING_MODE=off` on a fresh process and namespace (experiment arm B).

### Mechanisms landed (this round)

| review item | mechanism |
|---|---|
| 1. process-wide absorber budget | `AbsorbBudget` (bytes semaphore + concurrent-gather cap, ONE process static); every gather reserves gather_max × build-multiplier BEFORE reading frames; oversized estimates clamp to the whole budget (serialize, never deadlock) |
| 2. absorber phase stagger | first tick seeded from the shard-prefix hash (interval_at), like the WAL stagger |
| 3. bounded telemetry DBs | `TELEMETRY_CACHE_BYTES` (16 MiB default) shared by spool + rollup; explicit small Settings (8 MiB unflushed, 2 MiB L0 target, 32 L0 cap, slow polls/GC) — SlateDB defaults unreachable |
| 4. rollup off ingest | deployment contract: multi-instance cells run ROLLUP=1 on a designated non-ingest instance; single-instance cells co-locate ONLY with the bounded posture + SLATEDB_RT_THREADS=4 (STAGING/RUNBOOK) |
| 5. batched spool flush | `persist_all`: one WriteBatch + one flush per drain round, all-or-nothing (whole-round requeue) |
| 6. sweep residency | sweep/walk-opened engines are MARKED and deliberately closed once billing-debt-free (remove + begin_close); probes fail toward keeping discovery alive; `sweep_resident_engines` gauge |
| P2 shed | admission pressure = sampled RSS + reserved absorber bytes (`memory_pressure_mb`), so the line moves before the allocation shows in a sample |

### Instrumentation landed

Ops snapshot (`_ops_metrics`, 15 s): `history_l0_ssts_max`,
`history_l0_bytes_total`, `history_compacted_runs_max`,
`history_partitions_open`, `history_flush_wait_ms_max`
(peak-since-scrape), `gather_last_{reserved,actual,read_ms,write_ms,flush_ms}`,
`absorb_reserved_bytes`, `absorb_gathers_inflight`,
`absorb_bytes_total` / `ingest_bytes_total` (rate pair),
`read_spool_pending_{rows,bytes}`, `telemetry_cache_capacity_bytes`,
`rollup_apply_duration_ms`, `rss_mb`, `rss_peak_since_scrape_mb`
(250 ms in-process sampler feeds the peak), cgroup
`memory.current/peak` + `oom_kill` when the platform exposes them, and
mimalloc current/peak commit. Per-partition detail:
`GET /v1/debug/absorb`. L0 facts come from each open partition's
IN-MEMORY manifest snapshot (`Db::manifest()`) — no store requests.
Telemetry DBs expose the same manifest-derived L0 posture
(`spool_l0_ssts`/`rollup_l0_ssts` + bytes in the snapshot; full detail
under `telemetry` in `/v1/debug/absorb`). Not available at the current
upstream pin: SlateDB-internal compaction-queue depth and
unflushed-bytes counters (no public stats API);
`history_flush_wait_ms_max` + the L0 gauges are the operative
substitutes, and a public stats surface is tracked as an upstream ask.

### Deterministic regression

`stalled_flush_keeps_gather_memory_bounded_then_recovers`: a gather
stalled in its history flush holds the budget → aggregate reserved
bytes can never exceed capacity (the 16×32 MiB shape is impossible by
construction), the next gather WAITS (backpressure) while the shed
expression trips on RSS+reserved, and healing hands the budget to the
waiter (recovery without restart). Plus `gather_concurrency_cap_holds`
and `absorber_phase_stagger_is_prefix_seeded`. Timing-independent; the
soak-scale injected-slowdown leg is part of the acceptance gate below.

### Acceptance gate before preview.7-class builds are restored

At least THREE consecutive full-telemetry soaks, each: ≥5 GiB accepted
payload; concurrency 128 and 192; zero process exits/restarts;
client-visible errors limited to deliberate 429/503 shedding; cgroup
peak comfortably below the platform kill line; flat post-warm-up RSS
trend; absorber lag bounded and recovering; history L0 bounded (no
monotonic climb); spool/rollup backlog bounded; exact acked-operation
reconciliation. PLUS one injected history-compactor slowdown run
proving: admission sheds, memory stays bounded, the absorber catches
up after healing, the process never dies.

### Separate platform blocker

The Compute zombie (process dead; version "running"; edge 500s with no
`Prisma-Streams-Origin`; no self-recovery until manual redeploy) is a
platform process-supervision/health-removal defect, tracked separately
from the app's memory behavior. A durability service cannot depend on
an operator noticing a silent zombie.

### Campaign state (matrix COMPLETE, 2026-08-07)

Recorded in the soak results (ab21 FINDINGS): failed-run topology facts
(16 shards from the preserved bucket), the exoneration correction, the
A/B fairness note, and the full experiment matrix at conc 192 with a
fresh namespace per arm:

| arm | binary | shards | telemetry | outcome |
|---|---|---|---|---|
| A2 ≡ B | preview.7 | 16 | verifiably off | DEAD @ 330,676 ok |
| D | preview.7 | 4 | off | DEAD @ 210,950 ok |
| F4 control | freeze4 (pre-billing) | 16 | — | DEAD @ 534,332 ok |
| X | FIXED build | 16 | ON (ROLLUP=1) | **SURVIVED** @ 671,623 ok (~2.15 GiB target) |

Conclusions: the cumulative-work kill PRE-EXISTS the telemetry window
(freeze4 dies too); preview.7 made it ~40% earlier (real partial
regression; bisect is follow-up, not a blocker for the fix); neither
telemetry nor shard count alone flips preview.7; the ONLY surviving
arm is the fixed build — with strictly more work (full telemetry) on
the killer topology — whose causal gauges showed the budget
serializing gathers at the 64 MiB cap while history flushes stalled at
~2.5 s. Survival margin at stock knobs is thin (RSS peak 702 MB
sampled): the survival posture stays the deploy stance until the
acceptance-gate soaks run.
