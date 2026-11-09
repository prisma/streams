# Limits

**Storage is unlimited; throughput is capped; a project that needs more
throughput than a shared cell's share moves to a dedicated cell.**

That is the owner's decision of 2026-11-03, verbatim: "Make storage
unlimited. Document a hard cap on throughput, and specify dedicated cell as
the soluton." (edge change #244). This page is the product statement of the
caps. Every figure is taken from the shipped profiles
(`deploy/profiles/compute-1g.env`, `shared-cell.env`, `dedicated-cell.env`)
and the code named beside it. Every throughput figure on this page is
provisional: measured on the reference rig; to be confirmed on AMD Compute
(production Compute runs AMD x86_64; the owner, 2026-11-04). The storage and
memory statements are not. A figure labelled *measured* comes from a run
named in its source, not from configuration. The customer-facing reference
with every answer and its retry rule is
[BETA-REFERENCE.md](BETA-REFERENCE.md); SPEC §6 (C9) links here.

## Storage

- A project has no stored-bytes limit. The shipped profiles set
  `CONTAINER_STORED_BYTES_DEFAULT=0`, which `cell-admin` and the interim
  issuer publish as each project's `max_stored_bytes`, and the server reads
  0 as no limit (`auth::ceiling`, `quota::stored::StoredTable::admit`).
  Streams keep their data until they are deleted or expire (an expiry is
  set at creation, `expiry: {idle}` or `{at}`).
- Retained bytes are metered and billed; the stored-bytes gauge
  (`ownedStoredBytesNow`, `/operator/billing.json` `rollup.stored`) stays.
  The cell's guard is the operator's: RUNBOOK "What to alert on" rows
  `r23_stored_growth` and `r24_daily_spend`.
- An operator may still give one project a cap as a cost guard
  (`cell-admin quota --project P --quota max_stored_bytes=N`). That project
  is refused appends `429 stored_bytes_limit`, not retryable, once it
  stores N bytes (edge change #158).
- Native queues keep their own per-queue `maxQueuedBytes`, default 256 MiB
  (`src/native_queue/mod.rs`); a send past it is `429 queue_full`. It
  stays until the queue-storage redesign.
- A cell made from the profiles before 2026-11-03 keeps its recorded
  10 GiB default for its life: a cell's profile is fixed
  (deploy/cell-admin/README.md).

## Throughput per project

The rates below are provisional: measured on the reference rig; to be
confirmed on AMD Compute.

A cell is one 1 GiB Compute instance (`compute-1g.env`). Its measured
envelope is the shared bound every project's ceilings divide:

| Bound | Value | Profile key |
|---|---:|---|
| Requests a second | 1,411 (provisional) | `CELL_ENVELOPE_REQUESTS_PER_SEC` |
| Appended bytes a second | 2,060,000 (about 2.06 MB/s; provisional) | `CELL_ENVELOPE_APPEND_BYTES_PER_SEC` |
| Read bytes a second | 30,000,000 (30 MB/s; provisional) | `CELL_ENVELOPE_READ_BYTES_PER_SEC` |
| Requests in flight | 512 | `ADMIT_MAX_INFLIGHT` |
| Live subscriptions (SSE) | 1,200 | `SSE_MAX_CONNECTIONS` |
| Streams | 65,536 | the per-stream maps (`REGISTRY_CACHE_MAX`) |
| Appended bytes queued, not yet stored | 512 MiB | `MAX_UNABSORBED_BYTES_PER_INSTANCE` |
| Resident stream handles per shard (the hot working set) | 8,192 | `HANDLE_MAX_RESIDENT` |

Resident stream handles are the hot working set: a stream beyond them reloads
its tail from the shard on its next touch (one store GET of the tail row), and
the 65,536 is the per-stream maps' bound, not a hot-set guarantee. Footers are read
whole: streams per shard are comfortable to about 100k on a 1 GiB cell. Past about
200k streams in one object a footer is no longer cached, and sealed reads fetch it
one at a time, slowly. Measured 2026-11-06: 100,000 tiny streams touched at 300/s on a
1 GiB dedicated cell, peak 356 MiB, no shed.

The three envelope rates are *measured* on the 1 GiB class on the reference
rig, marked in `shared-cell.env` as pending re-measurement on the one-shard
shape, and are to be confirmed on AMD Compute. Every project rate below
derives from them, and is provisional with them.

On a cell shared k ways, the server holds a project on each bounded axis to
⌊bound × w ÷ k⌋, where w is its weight on that axis (`src/auth/ceiling.rs`,
edge change #196). cell-admin publishes the unit ceiling ⌊bound ÷ k⌋ × w,
never above it, so a project it admitted is held to that (the table's
figures). The request weight scales requests, in flight,
subscriptions and streams. The byte weight scales the append and read byte
rates. Queued bytes are not scaled. A weight is 1 unless the project was
admitted with one: 1 to k − 1, fixed for the project's life. cell-admin's
packing rule holds the cell's heavy projects to k − 1 units per axis
(`deploy/cell-admin/rules.ts`, `checkPacking`).

| Ceiling, per project | Shared cell, k = 8, weight 1 | Shared cell, k = 8, weight 7 (the most) | Dedicated cell, k = 1 |
|---|---:|---:|---:|
| Requests a second (provisional) | 176 | 1,232 (the server's ⌊bound × w ÷ k⌋: 1,234) | 1,411 |
| Appended bytes a second (provisional) | 257,500 | 1,802,500 | 2,060,000 |
| Read bytes a second (provisional) | 3,750,000 | 26,250,000 | 30,000,000 |
| Requests in flight | 64 | 448 | 512 |
| Live subscriptions | 150 | 1,050 | 1,200 |
| Streams | 8,192 | 57,344 | 65,536 |
| Appended bytes queued | 64 MiB | 64 MiB | 512 MiB |
| Memory its requests hold (memory line) | 32 MiB | 63,963,135 B (about 61 MiB) | 256 MiB |

**Reads and memory.** A read from a held
cursor costs no store round trip while its records are in the cell's ring or
log, and about 0.2 a read on average at the documented capacity (measured
2026-11-06). A read from offset 0 of a stream with n sealed chunks costs about
n + 2 store round trips (the descriptor, the run index, one per chunk) and holds
its page (1 MiB on a shared cell) and its request state in memory while it runs;
the cell bounds that memory by the project's read reservation against its memory
line, its read capacity (131 MB), and its 512 requests in flight, and past its
memory shed line refuses the heaviest project first and then every write. Measured
on a 1 GiB shared cell at documented capacity (8 tenants, 65,536 streams, 1,200
requests a second): cursor reads, 373 MiB, 0 refusals; every tenant replaying
six-chunk streams from offset 0 at full rate, 457 MiB, 8.4 round trips a read, 0
refusals, 43 MiB under the shed line.

- A project created through self-serve (the interim issuer) gets
  **10 requests a second**, unless its workspace's allow-list entry names
  another rate (`FREE_TIER_REQUESTS_PER_SEC`, `deploy/issuer`).
- Weight 7 is one project holding all k − 1 units of an axis; a weighted
  project's memory line is at most the read memory less two unit lines
  (`checkPacking`). The size class M is weights (request 2, bytes 6) on a
  shared cell.
- The request rate is a token bucket holding the larger of one second's
  requests and 50; the byte rates hold one second's worth
  (BETA-REFERENCE "Bursts", edge change #162).

## Throughput per stream and per queue

The measured figures below are provisional: measured on the reference rig;
to be confirmed on AMD Compute. The buckets and caps are configured values.

| Limit | Value | Source |
|---|---|---|
| Appends to one stream | 1,000 requests, 5,000 records and 5,000,000 bytes a second, each a token bucket holding 2 s | `LIMIT_REQS_PER_SEC`, `LIMIT_RECS_PER_SEC`, `LIMIT_BYTES_PER_SEC`, `LIMIT_BURST_SECS` (RUNBOOK) |
| Concurrent appends on one stream | 256 | `ADMIT_MAX_INFLIGHT_PER_STREAM` |
| One ordered stream, measured | 662 requests, 13,256 events and 27.1 MB a second sustained (median of 20 s windows), on a 1 GiB instance in hot-log mode (history absorption deferred) on 2026-07-23; about 300 requests and 15 MB a second with live absorption | *measured*, provisional: `bench/sinmax-report.md`, on an earlier storage layout, not re-measured since; to be confirmed on AMD Compute |
| One queue's drain, measured | at least 374 messages a second: the drain kept up with a 377 a second due rate at 13.1% of a core; its ceiling above that is not measured | *measured*, provisional: `docs/QUEUES-PRICING-BENCHMARK.md` §11 (2026-10-25); to be confirmed on AMD Compute |
| One queue message | 131,072 B envelope; a batch 1-1,000 | WIRE-MATRIX §2.23 |

On a 1 GiB cell the cell's own envelope binds before the per-stream
buckets do: 2.06 MB/s appended for the whole cell, below one stream's
5 MB/s bucket and below its measured 27.1 MB/s. A project's own ceiling
binds before either on a shared cell.

## What a client sees at a cap

| Cap | Answer | Retry |
|---|---|---|
| A project's request rate or byte rate | `429 container_rate_limit` | `Retry-After`: the seconds until the bucket has the request's tokens |
| A project's requests in flight, or its live subscriptions | `429 container_concurrency_limit` | retryable |
| A project's streams | `429 stream_limit` | not retryable until a stream is deleted |
| A project's queued appended bytes | `429 queued_bytes` | retryable, once in-flight appends decide |
| A project's memory line | `429 container_memory_pressure` | `Retry-After: 1` |
| A stream's buckets, or its concurrent appends | product `429 rate_limited` (raw `limit_requests_per_sec`, `limit_records_per_sec`, `limit_bytes_per_sec`, `stream_overloaded`) | `Retry-After` when the server gives one: the bucket's computed wait, or 1 |
| The cell's memory or in-flight shed | `429 rate_limited` | `Retry-After: 2` (memory) or `1` (in flight) |
| A queue past its `maxQueuedBytes` | `429 queue_full` | retryable |
| A stored-bytes cap an operator set | `429 stored_bytes_limit` | not retryable |

The SDK retries every retryable answer with its `Retry-After`. The full
table, with each `retryable` flag, is BETA-REFERENCE's.

## When a project needs more

A project that needs more throughput than its share of a shared cell moves
to a **dedicated cell** (k = 1, `deploy/profiles/dedicated-cell.env`):
cell-admin places one project there, at the bounds themselves (the right
column above). On compute-1g that is the full envelope: about 2.06 MB/s
appended, 30 MB/s read and 1,411 requests a second (provisional: measured on
the reference rig; to be confirmed on AMD Compute), 512 in flight and 1,200
live subscriptions.

What a dedicated 1 GiB cell does **not** give: anything past its envelope.
More than 2.06 MB/s appended, or 30 MB/s read, for one project needs a
larger instance class or more shards. That is the scaling design's job,
under the owner's review; this page does not promise it.
