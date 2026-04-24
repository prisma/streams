# Prisma Streams Metrics

Status: implemented

This document describes the **current shipped metrics system**.

Prisma Streams now treats metrics as a **profile on top of the durable stream
engine**, not as a second datastore.

That means:

- stream segments remain the durable source of truth
- metrics-specific serving artifacts are immutable companion objects in object
  storage
- query-time planning chooses the cheapest available path while preserving
  correctness
- local SQLite stores only bounded metadata and family progress, not an
  unbounded metrics head

The comparison that motivated this direction is captured in
[alternative-metrics-approach.md](./alternative-metrics-approach.md).

## Summary

The shipped metrics system has three layers:

1. canonical metric interval records in ordinary stream segments
2. `.agg` rollup companions for aligned time windows
3. `.mblk` per-segment metrics-block companions for non-aligned or
   non-rollup-eligible aggregate queries

The query planner for metrics streams is:

1. use `.agg` when the query aligns with a configured rollup window and its
   filters are rollup-eligible
2. otherwise use `.mblk` when coverage is present
3. otherwise fall back to raw segment scan and WAL-tail scan for correctness

This borrows the important part of the MetricsDB philosophy:

- object-store-native immutable serving artifacts
- no large local metrics index tables
- query-time fanout/compute instead of a second always-resident metrics store

It does **not** introduce a separate primary metrics engine beside streams.

## Built-In `metrics` Profile

The server now ships a built-in `metrics` stream profile.

It means:

- stream content type must be `application/json`
- JSON appends are normalized into a canonical metrics interval envelope
- the profile auto-installs a canonical schema registry
- the profile auto-installs default `search` fields and `search.rollups`
- the profile enables the `.mblk` metrics-block family in addition to the
  generic search families

The internal `__stream_metrics__` system stream is automatically created with
this profile at startup.

## Source Of Truth And Recovery

Metrics do **not** bypass the core durable stream model.

The durable source of truth is still:

- SQLite WAL for acknowledged but not yet published data
- sealed stream segments for published data
- manifests and companion objects in object storage for published search state

Published metrics search and aggregation state is recovered through the same
bootstrap model as the rest of the search system:

- schema registry object
- manifest `search_families`
- segment objects
- `.agg` companion objects
- `.mblk` companion objects

## Canonical Metrics Record

The shipped metrics profile currently stores **interval summary records**.

This is the canonical envelope written to `__stream_metrics__` and accepted by
user streams that install the `metrics` profile:

```json
{
  "apiVersion": "durable.streams/metrics/v1",
  "kind": "interval",
  "metric": "tieredstore.append.bytes",
  "unit": "bytes",
  "metricKind": "summary",
  "temporality": "delta",
  "windowStart": 1761396000000,
  "windowEnd": 1761396010000,
  "intervalMs": 10000,
  "instance": "12345-abcd12",
  "stream": "orders",
  "tags": { "env": "prod" },
  "attributes": { "env": "prod" },
  "dimensionPairs": ["env=prod"],
  "dimensionKey": "env=prod",
  "seriesKey": "summary|delta|tieredstore.append.bytes|bytes|orders|12345-abcd12|env=prod",
  "count": 4,
  "sum": 2048,
  "min": 128,
  "max": 1024,
  "avg": 512,
  "p50": 512,
  "p95": 1024,
  "p99": 1024,
  "buckets": { "128": 1, "512": 2, "1024": 1 },
  "summary": {
    "count": 4,
    "sum": 2048,
    "min": 128,
    "max": 1024,
    "histogram": { "128": 1, "512": 2, "1024": 1 }
  }
}
```

Notes:

- `tags` and `attributes` currently carry the same normalized dimension map
- `dimensionPairs` and `dimensionKey` are the flattened query/index shape
- `seriesKey` is the canonical routing key
- the record is still an ordinary JSON stream entry, not a separate metrics row
  store

## Search And Rollup Surface

The default metrics schema installs:

- exact keyword fields such as `metric`, `unit`, `stream`, `instance`,
  `metricKind`, `temporality`, `seriesKey`, `dimensionKey`, and
  `dimensionPairs`
- typed column fields such as `windowStart`, `windowEnd`, `intervalMs`,
  `count`, `sum`, `min`, `max`, `avg`, `p95`, and `p99`
- a default `metrics` rollup over `windowStart`

That means metrics streams can use:

- `GET /v1/stream/{name}?filter=...`
- `POST /v1/stream/{name}/_search`
- `POST /v1/stream/{name}/_aggregate`

Typical aggregate query:

```json
{
  "rollup": "metrics",
  "from": "2026-03-25T10:00:00.000Z",
  "to": "2026-03-25T11:00:00.000Z",
  "interval": "1m",
  "q": "metric:tieredstore.append.bytes stream:orders",
  "group_by": ["metric", "stream"]
}
```

## `.agg` Versus `.mblk`

### `.agg`

`.agg` remains the fast path for **aligned** windows.

Use it when:

- the time range lines up with a configured rollup interval
- the query only constrains rollup dimensions with exact filters
- published `.agg` coverage is available

This is ideal for charts, KPI tiles, and repeated dashboard queries.

### `.mblk`

`.mblk` is the metrics-specific fallback accelerator.

Current properties:

- immutable `mblk` sections inside bundled per-segment `.cix` companions
- binary `mblk2` section payloads loaded on demand from the bundled container
- the metrics-record payload is zstd-compressed when that reduces bytes, then
  inflated lazily on first fallback scan
- bundled companions are stored in object storage under
  `streams/<hash>/segments/...cix`
- local SQLite stores only bundled companion plan state and object keys
- `mblk` sections carry canonical metric interval summaries plus time-range
  metadata

Use it when:

- the query is not rollup-eligible
- the requested time range does not line up perfectly with a rollup window
- you still want to avoid decoding full JSON segments whenever possible

`.mblk` fits neatly beside `.agg`:

- `.agg` is for aligned precomputed windows
- `.mblk` is for ad hoc aggregate serving over canonical metrics records

## Cardinality Handling

The current design improves cardinality handling in two important ways:

1. query serving state is remote and immutable
2. non-rollup aggregate queries no longer need large local SQLite projections

This means high-cardinality metrics primarily show up as:

- more bytes appended to the metrics stream
- more `.mblk` and `.agg` companion bytes in object storage
- more query-time scan/fanout work

They do **not** require a separate resident TSDB head or large local mutable
series tables.

### Important Current Limitation

The internal emitter still maintains an in-memory per-series map for the flush
interval in [src/metrics.ts](../src/metrics.ts).

So the shipped system improves **storage and query-path cardinality behavior**
more than **ingest-path cardinality behavior**.

That is deliberate for now. It keeps one durable stream model while avoiding a
much larger rewrite of the internal metrics producer.

## Internal `__stream_metrics__` Stream

The server emits operational interval summaries to `__stream_metrics__`.

Current behavior:

- flush interval: `DS_METRICS_FLUSH_MS` (default `10000`; `0` disables)
- destination: `__stream_metrics__`
- content type: `application/json`
- profile: `metrics`
- routing key: canonical `seriesKey`
- installed registry: canonical schema only
- installed schema routing key: none
- installed search config: none
- background routing / lexicon / exact / bundled companion indexing: disabled

This is intentional. The internal metrics stream must not create its own heavy
search and aggregate backfill loop while the node is already under load.
Searchable and aggregatable metrics remain supported on normal user-created
`metrics` streams; the lean internal stream exists only for durable operational
event capture.

To correlate those interval records with the node's current configuration, use
`GET /v1/server/_details`.

## Metrics Currently Emitted

This implementation emits interval summaries for:

### Ingest and backpressure

- `tieredstore.ingest.flush.latency`
- `tieredstore.ingest.sqlite_busy.wait`
- `tieredstore.ingest.queue.bytes`
- `tieredstore.ingest.queue.requests`
- `tieredstore.ingest.queue.capacity.bytes`
- `tieredstore.ingest.queue.capacity.requests`
- `tieredstore.backpressure.over_limit`
- `tieredstore.backpressure.current.bytes`
- `tieredstore.backpressure.limit.bytes`
- `tieredstore.backpressure.pressure`

### Process memory

- `process.rss.bytes`
- `process.rss.over_limit`
- `process.rss.current.bytes`
- `process.rss.max_interval.bytes`
- `process.heap.total.bytes`
- `process.heap.used.bytes`
- `process.external.bytes`
- `process.array_buffers.bytes`
- `process.memory.rss.anon.bytes`
- `process.memory.rss.file.bytes`
- `process.memory.rss.shmem.bytes`
- `process.memory.js_managed.bytes`
- `process.memory.js_external_non_array_buffers.bytes`
- `process.memory.unattributed.bytes`
- `process.memory.unattributed_anon.bytes`
- `process.memory.limit.bytes`
- `process.memory.pressure`
- `process.gc.forced.count`
- `process.gc.reclaimed.bytes`
  - tags:
    - `kind=last|total`
- `process.gc.last_forced_at_ms`
- `process.heap.snapshot.count`
- `process.heap.snapshot.last_at_ms`
- `process.memory.high_water.bytes`
  - tags:
    - `metric=<name>`
- `tieredstore.sqlite.memory.used.bytes`
- `tieredstore.sqlite.memory.high_water.bytes`
- `tieredstore.sqlite.pagecache.used`
- `tieredstore.sqlite.pagecache.high_water`
- `tieredstore.sqlite.pagecache.overflow.bytes`
- `tieredstore.sqlite.pagecache.overflow.high_water.bytes`
- `tieredstore.sqlite.malloc.count`
- `tieredstore.sqlite.malloc.high_water.count`
- `tieredstore.sqlite.open_connections`
- `tieredstore.sqlite.prepared_statements`
- `tieredstore.sqlite.high_water`
  - tags:
    - `metric=<name>`
- `tieredstore.memory.subsystem.bytes`
  - tags:
    - `kind=heap_estimates|mapped_files|disk_caches|configured_budgets|pipeline_buffers|sqlite_runtime`
    - `subsystem=<name>`
- `tieredstore.memory.subsystem.count`
  - tags:
    - `subsystem=<name>`
- `tieredstore.memory.tracked.bytes`
  - tags:
    - `kind=heap_estimate|mapped_file|disk_cache|configured_budget|pipeline_buffer|sqlite_runtime`
- `tieredstore.memory.high_water.bytes`
  - tags:
    - `kind=runtime_total|runtime_subsystem`
    - `metric=<name>`
    - `subsystem_kind=<name>` for `kind=runtime_subsystem`

### Runtime concurrency and limits

- `tieredstore.concurrency.limit`
  - tags:
    - `gate=ingest|read|search|async_index`
    - `kind=configured|effective`
- `tieredstore.concurrency.active`
  - tags:
    - `gate=ingest|read|search|async_index`
- `tieredstore.concurrency.queued`
  - tags:
    - `gate=ingest|read|search|async_index`
- `tieredstore.upload.pending_segments`
- `tieredstore.upload.concurrency.limit`
- `tieredstore.auto_tune.preset_mb`
- `tieredstore.auto_tune.effective_memory_limit_mb`

### Object store

- `tieredstore.objectstore.put.latency`
- `tieredstore.objectstore.get.latency`
- `tieredstore.objectstore.head.latency`
- `tieredstore.objectstore.delete.latency`
- `tieredstore.objectstore.list.latency`
  - tags:
    - `artifact=manifest|schema_registry|routing_index|routing_key_lexicon|exact_index|segment|bundled_companion|stream_catalog|meta|unknown`
    - `outcome=ok|miss|error`

### Append and read throughput

- `tieredstore.append.bytes`
- `tieredstore.append.entries`
- `tieredstore.read.bytes`
- `tieredstore.read.entries`

### Indexing and compaction

- `tieredstore.index.lag.segments`
- `tieredstore.index.build.queue_len`
- `tieredstore.index.builds_inflight`
- `tieredstore.index.build.latency`
- `tieredstore.index.runs.built`
- `tieredstore.index.compact.latency`
- `tieredstore.index.runs.compacted`
- `tieredstore.index.bytes.read`
- `tieredstore.index.bytes.written`
- `tieredstore.index.active_runs`

### Index run caches

- `tieredstore.index.run_cache.used_bytes`
- `tieredstore.index.run_cache.entries`
- `tieredstore.index.run_cache.hits`
- `tieredstore.index.run_cache.misses`
- `tieredstore.index.run_cache.evictions`
- `tieredstore.index.run_cache.bytes_added`

## Inspection

Use these endpoints to inspect metrics stream state:

- `GET /metrics`
  lightweight process snapshot including current in-memory series count
- `GET /v1/server/_details`
  configured cache / concurrency limits plus live gate, queue, upload, and detailed runtime memory state
- `GET /v1/server/_mem`
  compact process-memory triage view including process breakdown, SQLite runtime
  counters, GC/high-water state, and bounded top-stream contributors
- `GET /v1/stream/__stream_metrics__/_profile`
  current profile resource
- `GET /v1/stream/__stream_metrics__/_schema`
  canonical metrics schema for the internal stream; this intentionally has no
  `routingKey` and no `search` section
- `GET /v1/stream/__stream_metrics__/_index_status`
  current index state; the internal stream should report no configured routing,
  lexicon, exact, or bundled companion families
- `GET /v1/stream/__stream_metrics__/_details`
  combined stream/profile/schema/index view

## Current Non-Goals

Not implemented today:

- a second dedicated metrics datastore beside streams
- a metrics-specific query language
- first-class OpenTelemetry metric-point ingest
- metrics-native exemplars or baggage/context semantics
- histogram/exponential-histogram canonical source records
- elimination of the in-memory flush-interval series map

## Follow-On Direction

The next natural expansion points are:

- broaden the `metrics` profile from interval summaries to first-class
  canonical metric points
- add profile-owned handling for `Sum`, `Gauge`, `Histogram`, and
  `ExponentialHistogram`
- use `.mblk` more aggressively for series discovery and non-rollup aggregate
  planning
- reduce ingest-path active-series memory pressure in the internal emitter

## Heap diagnostics

The runtime memory view is intentionally split into:

- `process.*`
  - direct process totals from `process.memoryUsage()`
- `runtime.memory.subsystems.heap_estimates`
  - bytes the server can currently attribute to in-process retained structures
    such as the ingest queue and in-memory index-run caches
  - index-run cache bytes are tracked against encoded run-object size so active
    routing runs can remain hot without the estimate expanding to full JS object
    overhead
- `runtime.memory.subsystems.mapped_files`
  - mmap-backed file bytes for cached segments, `.lex`, and bundled `.cix` caches
- `runtime.memory.subsystems.disk_caches`
  - on-disk cache occupancy for segment, run, lexicon, and companion caches
- `runtime.memory.subsystems.configured_budgets`
  - the configured caps those caches are meant to respect

Use these together when diagnosing high RSS:

- if `process.heap.used.bytes` is high and `heap_estimates` grows with it, the
  likely issue is retained JS-side state
- if `mapped_files` is large but `heap.used` stays modest, the process is
  likely file-backed rather than heap-heavy
- if RSS is high while both `heap_estimates` and `mapped_files` stay small, the
  remaining pressure is likely SQLite, Bun runtime, or other unattributed
  native allocations
