# Maintenance backpressure

Status: current for layout 6 (2026-10-28). The mechanism was built in R25
("durable maintenance backpressure") and closed in R26; layout 6 kept its
admission rule and its bounds and moved its ledger into memory, measuring
the sealer's backlog (SPEC §3.6). `CHAOS-R24.md` and the R23-1 sections of
`CHAOS-R23.md` record the system before R25.

## The rule

Never accept writes indefinitely faster than they can be sealed or safely
refused. When the unsealed backlog passes a bound, new appends get a
retryable `503 maintenance_backpressure`; reads, consumers, the control
plane, close-only operations, and reserved system streams stay admitted —
shedding any of those would stop the drain or make the overload
unrepairable.

## State: engine-owned, in memory, exact

- `ShardMaintenance` (`src/shard.rs`, `src/shard/maintenance_row.rs`) is
  owned by the `ShardEngine`. `load_or_rebuild_maintenance` rebuilds it
  from the shard's stream tails BEFORE the engine starts serving: no first
  request can race the rebuild, and the state moves with the shard
  database on ownership handoff because every open rebuilds it from the
  tails the database holds. There is no durable ledger row.
- The unit is **exact stored page bytes** in `[absorbed, next)`, the
  stream's unsealed pages above its sealed boundary — the same quantity
  each tail carries (`unabsorbed_bytes`), in both directions.
- The committer applies a group's delta (bytes its appends added, bytes
  its seal commit retired) and publishes the new state only after the
  group's write returns Ok. A failed group changes nothing: no phantom
  backlog, no phantom retirement.
- **Actual quantities, not net**: a group that both appends and retires
  counts both, and a group that retires anything refreshes the progress
  clock.
- **Checked retirement**: retiring more than the ledger holds is a
  divergence — the group fails before the write and the old boundary is
  preserved. Never a saturating clamp (KANI-047 proves the delta).
- A tail that does not decode is left out of the rebuild and logged; its
  stream fails alone when loaded (edge change #210). A store error fails
  the open.

## Admission: one gate, after ownership

`append_core` is the single admission point, placed AFTER `engine_for`
resolves ownership. Both public append surfaces converge there — the raw
wildcard route including hierarchical names, product append/appendMany,
every routing key, and split children on their own shard routes — so
there is no second copy of the route grammar to drift. A non-owner
receives `409 not_ring_owner` + `Streams-Replay-To` before admission is
ever evaluated: a stale local latch cannot answer for a backlog that
belongs to someone else.

Bounds (`src/backpressure.rs`), high/low hysteresis, release at
`MAINT_BACKPRESSURE_RELEASE_PCT` (75%). The names predate layout 6; they
measure unsealed bytes and the sealer's stall:

| Bound | Env | Default | Scope |
|---|---|---|---|
| Instance bytes | `MAX_UNABSORBED_BYTES_PER_INSTANCE` | 512 MiB | instance latch |
| Shard bytes | `MAX_UNABSORBED_BYTES_PER_SHARD` | 256 MiB | per-engine latch |
| No-progress | `MAX_ABSORB_LAG_SECS` | 900 s | instance latch |

The stall clock is `no_progress_secs` — time since a seal last retired
unsealed bytes while some are outstanding — never oldest-record age, which
stays permanently old under continuous traffic. The fleet heartbeat's
`absorb_lag_max_secs` carries the longest stall across a process's shards,
and the rebalancer moves a shard when it stays above `REBALANCE_LAG_SECS`
(`docs/SCALING.md` §4).

The sealer's own triggers keep a healthy shard far below these bounds: it
seals at 24 MiB of unsealed bytes or a 45 s old page, and backs off at
most 16 s after a failed seal (SPEC §3.6). The latch engages when sealing
stops making progress, for example while the object store refuses the
seal's PUT.

**Semantics (R26-6): resident safety.** The instance aggregate covers
engines this process has OPEN — what consumes its memory and pipelines;
each open engine holds its unsealed pages in memory. An owned-but-cold
shard is protected individually the moment anything opens it (its ledger
is rebuilt before it serves). These are not ownership-wide storage
contracts.

**No sparse deferral (R26-1).** The age trigger seals every unsealed page,
however few bytes it holds: a residual that never retires would age the
no-progress clock into the instance latch while shed appends are the only
way it could grow.

## Telemetry

`/v1/debug/load` carries `maintenance_shards` (per-shard ledgers
`unabsorbed_frame_bytes`, `backlog_started_ms`, `last_progress_ms`,
`no_progress_secs` and the shard latch flag, plus the cumulative
`ingest_frame_bytes_total` / `absorbed_frame_bytes_total`) and
`maintenance_backpressure` (the instance latch with its cause, engage and
shed counters, `unabsorbed_bytes` and `absorb_lag_secs`), and
`rate_limit_refusals` by code (the per-stream limiter counts separately
from maintenance shed). The sealer's counters are under each shard's
`seal` in `/v1/debug/timings`.

## Proof

Deterministic gates, through the real committer or real HTTP routes:
`dst::dst_tests::billing_maintenance` (unit exactness, failed groups,
atomic retirement with the sealed boundary, checked retirement, handoff),
`dst::dst_tests::admission_maintenance` (typed 503 on the raw and product
routes, split children, ownership replay before the latch, restoration
before the first request, reserved streams, `/v1/debug/load`), and
`shard::seal::tests` (the ledger rebuilt from the tails at open, a corrupt
tail failing only its stream). KANI-047 checks the delta and the stall
clock.

Field evidence for the mechanism predates layout 6:
`docs/SOAK-R25H-REPORT.md` and `docs/CAPACITY-R27.md` (the pause/catch-up
capacity gate and the fleet handoff at peak backlog, both measured on the
absorber).
