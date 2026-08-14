# Maintenance backpressure — the shipped system (R25/R26)

Status: current as of 2026-08-12. Supersedes `CHAOS-R24.md` (R24
snapshot) and the R23-1 sections of `CHAOS-R23.md`. The implementation
plan this executes is R25 ("durable maintenance backpressure"); the
R26 round closed the review findings on top of it.

## The rule

Never accept writes indefinitely faster than they can be absorbed or
safely refused. When the durable backlog passes a bound, new appends get
a retryable `503 maintenance_backpressure`; reads, consumers, the
control plane, close-only operations, and reserved system streams stay
admitted — shedding any of those would stop the drain or make the
overload unrepairable.

## State: engine-owned, durable, exact

- `ShardMaintenance` (src/shard.rs) is owned by the `ShardEngine`. It is
  loaded synchronously by `load_or_rebuild_maintenance` BEFORE the
  engine starts serving: no first request can race an async restore, no
  stale owner can delete a successor's state, and the state moves with
  the shard database on ownership handoff.
- The unit is **exact encoded frame bytes** in `[absorbed, next)` — the
  same quantity the stream tails carry, both directions. (The R24
  payload-unit accounting manufactured a 9.4% "absorption ratio" on
  compressible payloads; see the corrected-soak report.)
- The durable row (tag `M` under the dirty sentinel, versioned v2
  codec) is staged in the SAME WriteBatch as the commit group it
  accounts for, and the in-memory state publishes only after the write
  returns Ok. A failed group changes nothing: no phantom backlog, no
  phantom retirement.
- **Actual quantities, not net** (R26-2): a mixed append+absorb group
  counts both its ingested and retired frame bytes; a balanced group
  still writes the row and refreshes the progress clock. A per-stream
  invariant (net tail movement == appended − retired) fails the group
  on any untracked mutation.
- **Checked retirement** (R26-3): retiring more than the ledger holds is
  a divergence — the whole group fails before the write and the old
  durable boundary is preserved. Never a saturating clamp.
- **Legacy rows rebuild** (R26-4): the R24 16-byte payload-unit row is
  never converted (it can under- OR overstate); the opener rebuilds
  from durable tails, repairing pre-gauge tails by summing their actual
  stored frames.

## Admission: one gate, after ownership

`append_core` (src/http.rs) is the single admission point, placed AFTER
`engine_for` resolves ownership. Both public append surfaces converge
there — the raw wildcard route including hierarchical names, product
append/appendMany, every routing key, and split children on their own
shard routes — so there is no second copy of the route grammar to
drift. A non-owner receives `409 not_ring_owner` + `Streams-Replay-To`
before admission is ever evaluated: a stale local latch cannot answer
for a backlog that belongs to someone else.

Bounds (src/backpressure.rs), high/low hysteresis, release at
`MAINT_BACKPRESSURE_RELEASE_PCT` (75%):

| Bound | Env | Default | Scope |
|---|---|---|---|
| Instance bytes | `MAX_UNABSORBED_BYTES_PER_INSTANCE` | 512 MiB | instance latch |
| Shard bytes | `MAX_UNABSORBED_BYTES_PER_SHARD` | 256 MiB | per-engine latch |
| No-progress | `MAX_ABSORB_LAG_SECS` | 900 s | instance latch |

The stall clock is `no_progress_secs` — time since durable retirement
while backlog is outstanding — never oldest-record age, which stays
permanently old under continuous traffic.

**Semantics (R26-6): resident safety.** The instance aggregate covers
engines this process has OPEN — what consumes its memory and pipelines.
An owned-but-cold shard is protected individually the moment anything
opens it (its ledger loads before it serves). These are not
ownership-wide storage contracts; an owned-shard index for fleet-wide
accounting is pre-GA work. `MAX_REPLAY_BYTES` was deleted: it was the
same open-engine sum as the instance bound under a name implying
ownership-wide replay projection.

**No sparse deferral (R26-1).** Age absorption takes everything. The
interim `ABSORB_MIN_BYTES_FOR_AGE` mode was deleted: a sub-threshold
residual that never retires ages the no-progress clock into the
instance latch while shed appends are the only way it could ever grow
eligible — a permanent self-trap the 2026-08-11 soak came one evaluator
tick from demonstrating (154 KiB residual at 938 s of stall).

## Telemetry (R26-7)

`/v1/debug/load` carries: per-shard ledgers + latch flags + progress
clocks, the instance latch state with engage/shed counters, the exact
cumulative `ingest_frame_bytes_total` / `absorbed_frame_bytes_total`,
`rate_limit_refusals` by code (the ordinary per-stream limiter counts
separately from maintenance shed — the 2026-08-11 soak plateau was the
5,000 rec/s limiter, initially misattributed), and `binary_sha256`
(wrapper-hashed build identity).

## Proof

Deterministic gates (src/dst/dst_tests.rs), all through the real
committer or real HTTP routes:

- unit exactness: `maintenance_uses_frame_bytes_not_payload_bytes`
- atomicity: `maintenance_failed_append_group_is_noop`,
  `absorbed_boundary_and_maintenance_retire_atomically`
- mixed/balanced groups: `mixed_append_absorb_group_refreshes_the_progress_clock`,
  `balanced_append_absorb_group_still_writes_progress`
- checked retirement: `over_retirement_fails_the_group_and_preserves_the_boundary`
- legacy migration: `legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open`
- anti-deadlock: `tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch`
- routes (R26-5): `raw_hierarchical_append_sheds_typed_503_under_backlog`,
  `split_child_sheds_while_sibling_child_admits`,
  `ownership_replay_wins_over_a_latched_local_engine`,
  `first_request_waits_for_restoration_then_sees_the_restored_ledger`,
  `reserved_streams_append_through_a_latched_engine`
- ownership: `ownership_handoff_moves_backlog_without_aba`
- telemetry: `debug_load_reports_typed_limiter_and_frame_totals`

Field evidence: `docs/SOAK-R25H-REPORT.md` (corrected six-region soak);
`docs/CAPACITY-R27.md` — the decisive pause/catch-up capacity gate
PASSED (cap-20260813T085718Z-55853: peak ledger at 100.6% of the
instance cap, held, with typed shed and exact reconciliation) and the
fleet handoff at peak backlog PASSED (handoff-fh185257). No open items
remain in this mechanism; the authoritative status table is
`docs/READINESS.md`.
