# Alternative Metrics Approach

Status: historical research note

This document compares Axiom's MetricsDB design, described in
["Metrics are generally available"](https://axiom.co/blog/metrics-mpl) on
March 25, 2026, with the current Prisma Streams implementation.

It is not the main metrics design doc anymore. The current shipped design lives
in [metrics.md](./metrics.md). This note remains useful as background on what
we chose to borrow and what we deliberately did not copy from MetricsDB.

Primary Axiom sources used here:

- [Metrics are generally available](https://axiom.co/blog/metrics-mpl)
- [Axiom Metrics overview](https://axiom.co/docs/query-data/metrics/overview)

Repository sources used for the current Prisma Streams behavior:

- [docs/metrics.md](/Users/sorenschmidt/code/streams/docs/metrics.md)
- [docs/aggregation-rollups.md](/Users/sorenschmidt/code/streams/docs/aggregation-rollups.md)
- [src/metrics.ts](/Users/sorenschmidt/code/streams/src/metrics.ts)
- [src/metrics_emitter.ts](/Users/sorenschmidt/code/streams/src/metrics_emitter.ts)

## Summary

Axiom's MetricsDB is a dedicated metrics datastore with:

- object-storage-backed compressed columnar metric storage
- ephemeral fan-out query compute
- ingest and pricing that do not depend on active time series count
- a query language built specifically for time-series operations

Prisma Streams today does **not** have a dedicated metrics datastore.

Instead, we currently have two separate pieces:

- the internal `__stream_metrics__` stream, which emits already-aggregated
  interval JSON events about server behavior
- the generic stream/search/rollup engine, which can now build schema-owned
  rollups (`.agg`) over JSON streams

That means Axiom is solving "metrics as a primary product datastore", while we
are currently solving "generic durable streams plus reusable indexing and
rollup primitives".

## How MetricsDB Differs From Prisma Streams Today

### 1. Dedicated metrics engine vs generic stream engine

Axiom's docs are explicit that MetricsDB is a purpose-built metrics datastore,
not just event storage with query-time aggregation layered on top. Their docs
call out storage format, query engine, and compression as metrics-specific
optimizations.

Prisma Streams today keeps one durable storage model:

- append-only streams
- sealed segments in object storage
- schema-owned search and rollup companions

Even after the new `.agg` work, our metrics story is still "metrics represented
as stream records", not a separate metrics-native storage engine.

### 2. Native metrics ingest vs internal interval events

Axiom ingests OpenTelemetry metrics into a dedicated metrics dataset and stores
them in a metrics-optimized representation.

Our current `__stream_metrics__` path is much simpler:

- the server builds an in-memory series map keyed by metric + unit + stream +
  tags
- every flush interval emits one JSON interval event per unique series
- those events are appended to `__stream_metrics__` like any other stream data

This is operational telemetry for Prisma Streams itself, not a general metrics
database.

### 3. Columnar metric storage vs JSON records plus companions

Axiom says metrics are written to object storage in a compressed columnar
format, then queried through ephemeral compute.

Our current system stores:

- JSON metric interval events in ordinary stream segments
- search/rollup metadata in separate exact / `.col` / `.fts` / `.agg`
  companion objects

So our current design is "object-store-native indexes over stream records",
while Axiom is closer to "object-store-native metric storage from the start".

### 4. Metrics-native query language vs search + aggregate endpoints

Axiom has MPL, a metrics-specific pipeline language with first-class time-series
operations like alignment and rate computation.

Prisma Streams currently exposes:

- `GET /v1/stream/{name}?filter=...`
- `POST /v1/stream/{name}/_search`
- `POST /v1/stream/{name}/_aggregate`

That is enough for structured search and time-window summaries, but it is not a
metrics-native query language.

### 5. Metrics-specific data model optimizations

The Axiom metrics docs describe several deliberate optimizations:

- truncating timestamps to second precision
- flattening resource, scope, and metric tags into one namespace
- normalizing `unit`
- assuming equal-width histograms and discarding some histogram metadata
- limiting supported data types

Prisma Streams does not currently impose those metrics-specific normalizations
at the storage engine level. Our generic stream/search/rollup model preserves
more of the original JSON shape unless a profile chooses to normalize it.

## How Axiom Handles Cardinality Explosion

The main theme in Axiom's March 25, 2026 post is that they avoid the traditional
"active time series tax".

Their stated approach is:

- store metrics in object storage in compressed columnar form
- query them with ephemeral compute that scales with the query
- price by ingest/storage/query usage rather than by active series count
- avoid resident indexer clusters or TSDB head structures that scale directly
  with active series count

The important point is not that high cardinality becomes free. It is that the
dominant cost and scaling pressure moves away from "how many distinct active
series exist right now?" and toward:

- bytes ingested
- bytes stored
- bytes scanned / compute used at query time

That is why they can say hyper-cardinality is "a design principle" rather than
an active-series billing penalty.

Their docs also help cardinality operationally by using a metrics-specific data
model:

- flattened tags simplify filtering
- compressed columnar storage makes repeated dimensional values cheaper to store
- ingesting OTel summaries/histograms avoids turning every raw measurement into
  a standalone event

## Where Cardinality Pressure Shows Up In Prisma Streams Today

Prisma Streams does **not** bill by active time series, but the current internal
metrics path still has active-series pressure in the runtime.

The sharpest example is [src/metrics.ts](/Users/sorenschmidt/code/streams/src/metrics.ts):

- every distinct metric + tag set becomes a `MetricSeries` in memory
- that map lives for the whole flush interval
- one interval record is emitted per series

So a cardinality explosion in today's `__stream_metrics__` path means:

- more in-memory active series during the flush window
- more emitted interval events
- more stream bytes
- more downstream indexing / rollup work if that stream is queried heavily

In other words, we do not currently have the Prometheus-style billing model,
but we do still have an in-process active-series structure for this internal
metrics stream.

The newer generic rollup system is better positioned than the current internal
metrics emitter because:

- rollups are object-store-native
- companion state is rebuildable
- query-time fallback preserves correctness without requiring large local SQLite
  projections

But the source data is still ordinary stream records, not a dedicated metric
column store.

## Would A Similar Approach Be Appropriate Here?

### Yes, in part

Several Axiom ideas fit our direction well:

- object-store-native metrics state instead of large local mutable indexes
- async background build, not request-path aggregation
- treating high-cardinality metrics as a storage/query problem, not a special
  in-memory local database problem
- preferring pre-aggregated metric summaries and histograms over raw
  per-measurement events
- flattening and normalizing metric dimensions inside a dedicated metrics
  profile

Those ideas fit the design we already chose for search and rollups.

### No, not as a wholesale replacement

A full MetricsDB-style subsystem would be a larger architectural jump than we
need right now.

Reasons:

- Prisma Streams is intentionally built around one durable stream engine
- we just added reusable `.agg` rollups that fit the current manifest/bootstrap
  model
- a second dedicated storage engine would complicate recovery, manifest
  semantics, and the current "streams are the source of truth" rule

So the right lesson is not "copy MetricsDB exactly". The better lesson is
"borrow its object-store-native, cardinality-tolerant principles while keeping
metrics as a profile on the existing durable stream core".

## Recommended Direction For Prisma Streams

If we want a stronger metrics story later, the most compatible path would be:

1. Add a dedicated metrics profile instead of a separate storage engine.
2. Normalize metric dimensions into a stable canonical shape at ingest time.
3. Prefer summary/histogram-style source records over raw sample events.
4. Use schema-owned rollups and possibly a metrics-specific column family for
   common windowed queries.
5. Keep the durable source of truth in stream segments and manifests.

That would preserve the current architecture while adopting the useful parts of
the MetricsDB philosophy.

## Specific Takeaways

### Ideas worth borrowing

- No active-series billing or design assumptions in the storage model.
- Object-store-native serving structures.
- Metrics-specific canonicalization in a dedicated profile.
- Strong support for summary/histogram mergeability.
- Query planning that prefers precomputed windows but can still fall back to raw
  source data.

### Ideas to avoid copying directly

- A separate metrics-only datastore that bypasses stream semantics.
- Global second-precision truncation as a system-wide rule.
- Histogram simplifications as a generic default for every profile.
- A metrics query language before we have enough profile-owned metrics semantics
  to justify it.

## Bottom Line

Axiom's MetricsDB is more specialized than Prisma Streams today.

Their answer to cardinality explosion is fundamentally:

- object-storage-native metric storage
- ephemeral query compute
- no active time series tax
- aggressive metrics-specific data-model optimization

Our current system is more general and less optimized for first-class metrics.
It already shares the object-store-native and async-background instincts, but it
still represents metrics as ordinary stream records and, for `__stream_metrics__`,
still keeps an in-memory active-series map per flush interval.

So a similar **philosophy** is appropriate for our design.
A full MetricsDB-style **subsystem** is probably not.
The best fit is a future metrics profile that keeps the durable stream engine
intact while adopting more metrics-specific canonicalization, compression, and
rollup behavior.
