# Indexing Architecture

Status: implemented baseline

This document describes the **current shipped search and indexing model**.
The long-term target still lives in
[aspirational-indexing-architecture.md](./aspirational-indexing-architecture.md).
The planned low-latency read model for heavy-ingest periods lives in
[low-latency-reads-under-ingest.md](./low-latency-reads-under-ingest.md).

## Summary

Prisma Streams now ships six indexing layers:

- the existing routing-key tiered index
- the existing exact-match secondary index family, now treated as an internal
  accelerator derived from schema `search.fields`
- a bundled per-segment companion container (`.cix`)
- a `col` section family inside `.cix` for typed equality, range, and
  existence
- an `fts` section family inside `.cix` for keyword exact/prefix and text
  search
- an `agg` section family inside `.cix` for time-window rollups and
  aggregation serving
- an `mblk` section family inside `.cix` for metrics-profile aggregate serving

The public schema model is **`search`**, not `indexes[]`.

The public query surfaces are:

- `GET /v1/stream/{name}?filter=...`
  - exact/range/existence filtering on JSON streams
  - cursor-friendly read semantics
  - exact family and `.col` may prune sealed history
  - the local unsealed tail is always scanned for correctness
- `POST /v1/stream/{name}/_search`
- `GET /v1/stream/{name}/_search?q=...`
  - fielded search over the same stream data
  - keyword exact/prefix, typed equality/range, bare text, and phrase queries
  - `_search` is the primary evlog/UI query surface
- `POST /v1/stream/{name}/_aggregate`
  - schema-owned rollup queries over JSON streams
  - uses `.agg` companions for aligned windows when coverage and query shape
    allow it
  - scans source segments and the WAL tail for partial edges and uncovered
    ranges

The source of truth remains the stream itself. Search families are accelerators
and remote serving structures, not durable record stores.

## Design Rules

1. WAL commit remains the only write acknowledgment point.
2. Heavy indexing stays off the request path.
3. Local SQLite stays bounded and rebuildable.
4. Published search state is recovered from manifests and object-store objects.
5. Missing or stale search coverage must fall back to source-segment or WAL-tail
   scan instead of returning false negatives.

## Schema Contract

Search configuration is schema-owned because field extraction belongs to the
payload contract, not the stream profile.

The registry now uses a top-level `search` section:

```json
{
  "apiVersion": "durable.streams/schema-registry/v1",
  "schema": "billing-evlog",
  "currentVersion": 1,
  "boundaries": [{ "offset": 0, "version": 1 }],
  "schemas": {
    "1": { "type": "object", "additionalProperties": true }
  },
  "lenses": {},
  "search": {
    "primaryTimestampField": "eventTime",
    "aliases": {
      "req": "requestId"
    },
    "defaultFields": [
      { "field": "message", "boost": 2.0 },
      { "field": "why", "boost": 1.5 }
    ],
    "fields": {
      "eventTime": {
        "kind": "date",
        "bindings": [{ "version": 1, "jsonPointer": "/eventTime" }],
        "column": true,
        "exists": true,
        "sortable": true
      },
      "service": {
        "kind": "keyword",
        "bindings": [{ "version": 1, "jsonPointer": "/service" }],
        "normalizer": "lowercase_v1",
        "exact": true,
        "prefix": true,
        "exists": true,
        "sortable": true
      },
      "status": {
        "kind": "integer",
        "bindings": [{ "version": 1, "jsonPointer": "/status" }],
        "exact": true,
        "column": true,
        "exists": true,
        "sortable": true
      },
      "message": {
        "kind": "text",
        "bindings": [{ "version": 1, "jsonPointer": "/message" }],
        "analyzer": "unicode_word_v1",
        "exists": true,
        "positions": true
      }
    }
  }
}
```

Supported field kinds:

- `keyword`
- `text`
- `integer`
- `float`
- `date`
- `bool`

Supported capability bits:

- `exact`
- `prefix`
- `column`
- `exists`
- `sortable`
- `aggregatable`
- `contains`
- `positions`

Current support notes:

- `contains` is reserved in schema, but `.sub` is not implemented yet
- `aggregatable` is used by `search.rollups` `summary` measures
- schema-owned `search.rollups` drive the shipped `_aggregate` API and `.agg`
  family
- `indexes[]` is rejected; the supported public model is `search`
- a `search`-only update requires an already-installed schema version
- if you are installing the first schema for a stream, install `schema` and
  `search` together in one `_schema` update

## Family Split

### Routing-key family

The routing-key family is unchanged. It remains the hot path for exact routing
key lookup and `/pk/<key>` reads.

### Exact secondary family

The old generic secondary index family is still present, but it is now an
**internal exact-match accelerator**.

It is derived automatically from `search.fields` entries that set `exact=true`.

Properties:

- asynchronous
- compacted tiered runs in object storage
- used for sealed-segment pruning on exact-equality clauses
- recovered from manifest `secondary_indexes`

It is no longer the public schema model.

Exact-index rebuild is now config-aware:

- each configured exact field has a stable config hash
- if the schema changes that exact field on an existing stream, the current
  exact state is treated as stale
- exact queries fall back to raw scans until background rebuild catches up

### `.col` family

The `.col` family is the typed range/equality family.

Current implementation:

- immutable per-segment sections inside bundled `.cix` companions
- no `.col` run compaction yet
- local SQLite stores only the bundled companion plan and per-segment companion
  object keys
- published companion objects live under `streams/<hash>/segments/...cix`
- bundled companion backfill is oldest-missing-first and batched, so `.col`
  coverage grows contiguously across the uploaded segment prefix
- bundled companion builds are single-pass and cooperative, so `.col`, `.fts`,
  `.agg`, and `.mblk` share one decoded segment walk and yield periodically
  while building

Current responsibilities:

- typed equality
- typed range filters
- `has:` on typed column fields
- typed sort extraction for `_search`

Current field coverage:

- `integer`
- `float`
- `date`
- `bool`

### `.fts` family

The `.fts` family is the keyword/text family.

Current implementation:

- immutable per-segment sections inside bundled `.cix` companions
- no `.fts` run compaction yet
- local SQLite stores only the bundled companion plan and per-segment companion
  object keys
- published companion objects live under `streams/<hash>/segments/...cix`
- bundled companion backfill is oldest-missing-first and batched, so `.fts`
  coverage grows contiguously across the uploaded segment prefix
- `.fts` uses null-prototype term dictionaries, so tokens like `constructor`
  and `push` behave like ordinary search terms instead of colliding with
  `Object.prototype`

Current responsibilities:

- keyword exact
- keyword prefix
- text term queries
- phrase queries on fields with `positions=true`
- `has:` on keyword/text fields

### `.agg` family

The `.agg` family is the shipped aggregation rollup family.

Current implementation:

- immutable per-segment sections inside bundled `.cix` companions
- no `.agg` compaction yet
- local SQLite stores only the bundled companion plan and per-segment companion
  object keys
- published companion objects live under `streams/<hash>/segments/...cix`
- bundled companion backfill is oldest-missing-first and batched, so `.agg`
  coverage grows contiguously across the uploaded segment prefix

Current responsibilities:

- rollup serving for `POST /v1/stream/{name}/_aggregate`
- precomputed aligned-window summaries for configured `search.rollups`
- metrics-style `count` / `summary` / `summary_parts` state

Current usage rules:

- only aligned middle windows use `.agg`
- partial edge windows still scan the source stream
- uncovered or stale ranges still scan the source stream
- WAL tail records are always evaluated directly

### `.mblk` family

The `.mblk` family is the metrics-specific aggregate-serving family.

Current implementation:

- immutable per-segment sections inside bundled `.cix` companions
- no `.mblk` compaction yet
- local SQLite stores only the bundled companion plan and per-segment companion
  object keys
- published companion objects live under `streams/<hash>/segments/...cix`

Current responsibilities:

- canonical metrics interval serving for non-rollup-eligible aggregate queries
- aligned-window edge serving when `.agg` cannot fully answer the query
- metrics-profile aggregate serving without decoding full JSON segments

## SQLite Catalog

Current local catalog tables:

- `secondary_index_state`
- `secondary_index_runs`
- `search_companion_plans`
- `search_segment_companions`

Interpretation:

- `secondary_*` tables catalog the compacted exact-match family
- `search_companion_plans` stores the current desired bundled companion plan
- `search_segment_companions` maps each covered segment to its current bundled
  companion object key, generation, and section inventory

These tables are rebuildable from manifest state and remote objects. They are
not durable source-of-truth data stores.

## Manifest And Bootstrap

Manifest state now includes:

- `secondary_indexes`
- `search_companions`

`search_companions` currently stores:

- the current bundled companion plan generation and hash
- the desired plan JSON summary
- per-segment current companion object keys and section inventories

Bootstrap restores:

- exact secondary index state and runs
- bundled companion plan state
- current per-segment bundled companions

This means a node can be deleted and cold-restored from published R2 state
without rebuilding the already-published search catalogs locally first.

## Runtime Model

All indexing is currently asynchronous **in-process** work. There is no
separate search worker service and no dedicated worker-thread pool for search.

The full server starts these background managers:

- `IndexManager` for routing-key runs
- `SecondaryIndexManager` for exact secondary runs
- `SearchCompanionManager` for bundled `.cix` companions and historical
  backfill

These wake on `DS_INDEX_CHECK_MS`.

Relevant concurrency knobs:

- `DS_INDEX_BUILD_CONCURRENCY`
- `DS_INDEX_COMPACT_CONCURRENCY`
- `DS_INDEX_CHECK_MS`

These are in-process async concurrency limits, not separate OS workers.

`SearchCompanionManager` also emits progress metrics so companion lag can be
observed independently of the exact family:

- `tieredstore.companion.build.queue_len`
- `tieredstore.companion.builds_inflight`
- `tieredstore.companion.lag.segments`
- `tieredstore.companion.build.latency`
- `tieredstore.companion.objects.built`

On startup, the full server enqueues all streams into the index controller so
existing streams can catch up automatically after bootstrap, schema changes, or
bundled companion plan changes.

## Bundled Companions And Backfill

Current bundled-companion rules:

- each uploaded segment may have one current `.cix`
- the `.cix` may contain any subset of `col`, `fts`, `agg`, and `mblk`
- the desired bundled companion plan is hashed and versioned per stream
- each bundled companion build loads one segment and builds enabled families
  sequentially, so `col`, `fts`, `agg`, and `mblk` do not keep their heaviest
  in-memory state live at the same time
- query-time companion reads cache raw `.cix` bytes plus the parsed TOC and
  decode only the requested section family on demand
- long-running bundled companion builds yield cooperatively every bounded number
  of segment blocks so the HTTP server stays responsive during backfill
- bundled companion backfill defers work when the process memory guard is over
  limit, preferring temporary mixed coverage over driving the main server
  deeper into memory pressure
- a plan change puts the stream into mixed coverage until historical companions
  are rebuilt
- queries use current bundled sections where present and raw-scan missing or
  stale ranges otherwise

See [bundled-companion-and-backfill.md](./bundled-companion-and-backfill.md)
for the dedicated architecture document.

## Query Surfaces

### `GET /v1/stream/{name}?filter=...`

Current contract:

- JSON streams only
- fields must come from `search.fields`
- supported operators:
  - exact match
  - `>`, `>=`, `<`, `<=`
  - `has:field`
  - boolean `AND`, `OR`, `NOT`, `-`, grouping
- exact equality may use the internal exact family to prune sealed segments
- typed equality/range may use `.col` companions to prune segment-local docs
- unsealed WAL tail is always scanned
- one filtered response stops after 100 MB of examined payload bytes and reports
  that through response headers

This path is optimized for stream-like cursor progression, not ranked search.

### `_search`

Current request shape:

- `q`
- `size`
- `search_after`
- `sort`
- `track_total_hits`
- `timeout_ms`

Current response shape:

- `stream`
- `snapshot_end_offset`
- `took_ms`
- `coverage`
- `total`
- `hits`
- `next_search_after`

Current search coverage fields:

- `mode`
- `complete`
- `visible_through_offset`
- `possible_missing_events_upper_bound`
- `possible_missing_uploaded_segments`
- `possible_missing_sealed_rows`
- `possible_missing_wal_rows`
- `indexed_segments`
- `scanned_segments`
- `scanned_tail_docs`
- `index_families_used`

Current query support:

- fielded exact keyword queries
- fielded keyword prefix queries
- typed equality and range queries
- `has:field`
- bare terms over `search.defaultFields`
- fielded text queries
- quoted phrase queries on text fields with `positions=true`
- alias resolution from `search.aliases`
- filter-only and score-based sorts

Current non-support:

- `contains:` / `.sub`
- snippets
- multi-stream search

Current newest-suffix behavior:

- while sealed segments are still unpublished or bundled companions are still
  catching up, `/_search` omits that newest suffix instead of raw-scanning it
- the omitted range is reported through the `possible_missing_*` coverage
  fields
- once publish and bundled-companion work are fully caught up, `/_search` may
  use the bounded WAL tail as a local overlay so a quiet sub-segment tail still
  shows up

### `_aggregate`

Current request shape:

- `rollup`
- `from`
- `to`
- `interval`
- `q`
- `group_by`
- `measures`

Current response shape:

- `stream`
- `rollup`
- `from`
- `to`
- `interval`
- `coverage`
- `buckets`

Current coverage fields:

- `mode`
- `complete`
- `visible_through_offset`
- `possible_missing_events_upper_bound`
- `possible_missing_uploaded_segments`
- `possible_missing_sealed_rows`
- `possible_missing_wal_rows`
- `used_rollups`
- `indexed_segments`
- `scanned_segments`
- `scanned_tail_docs`
- `index_families_used`

Current newest-suffix behavior:

- while sealed segments are still unpublished or bundled companions are still
  catching up, `/_aggregate` omits that newest suffix instead of raw-scanning it
- the omitted range is reported through the `possible_missing_*` coverage
  fields
- once publish and bundled-companion work are fully caught up, `/_aggregate`
  may use the bounded WAL tail as a local overlay

## Inspection Endpoints

The current shipped management surface includes two per-stream inspection
endpoints:

- `GET /v1/stream/{name}/_index_status`
- `GET /v1/stream/{name}/_details`

`/_index_status` reports:

- segment counts
- manifest generation/upload state
- routing-key index status
- internal exact-index status, including stale-config detection
- bundled companion object coverage
- `col`, `fts`, `agg`, and `mblk` family progress derived from bundled
  companion sections

Current exact-index scheduling:

- bundled companions are the first background priority for uploaded segments
- exact secondary-index build and compaction only run after bundled companions
  are caught up, the stream has no in-progress segment cut or pending upload
  segment, and the stream has been append-idle for about ten minutes so exact
  work does not re-enter the ingest hot path during long but temporary quiet
  gaps
- byte-at-rest and object-count accounting for index families
- lag in both segments and milliseconds for routing, exact, and bundled-family
  progress

`/_details` is the combined stream-management descriptor. It nests:

- the current stream summary
- the full `/_profile` resource
- the full `/_schema` registry
- the current `/_index_status` payload
- uploaded object-storage byte breakdown
- local retained byte breakdown
- node-local per-stream object-store request counters

This is the supported UI inspection path. Clients do not need to infer search
progress from low-level objects or manifest rows.

## Current Evlog Shape

The evlog profile now has a search-capable foundation without adding unbounded
local SQLite projections.

The built-in `evlog` profile auto-installs these `search.fields`:

- keyword exact/prefix:
  - `service`
  - `level`
  - `requestId`
  - `traceId`
  - `spanId`
  - `path`
  - `method`
  - `environment`
- typed column:
  - `timestamp`
  - `status`
  - `duration`
- text:
  - `message`
  - `why`
  - `fix`
  - `error.message`

It also auto-installs default `search.rollups` so UIs can use `_aggregate`
without a separate manual schema step.

## Current Metrics Shape

The `metrics` profile auto-installs:

- a canonical metrics schema
- default `search.fields`
- default `search.rollups`
- the `.mblk` family alongside `.agg`

The intended planner order for metrics streams is:

- `.agg` for aligned rollup-eligible windows
- `.mblk` for non-aligned or non-rollup-eligible aggregate serving
- raw source scan only when published coverage is missing

## Deliberate Gaps Versus The Aspirational Design

The long-term design doc is still directionally correct, but the current system
ships a smaller subset:

- `.col` and `.fts` are per-segment companions only; there are no compacted
  `.col`, `.fts`, or `.agg` runs yet
- `.sub` is not implemented
- `_search` does not ship snippets
- current text scoring is query-time text scoring over the source records; it is
  not a full global BM25 implementation yet
- primary timestamp fallback to append time for missing source fields is not
  implemented yet

These are intentional current-state limits, not compatibility shims.
