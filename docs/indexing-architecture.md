# Indexing Architecture

Status: implemented baseline

This document describes the **current shipped search and indexing model**.
The long-term target still lives in
[aspirational-indexing-architecture.md](./aspirational-indexing-architecture.md).

## Summary

Prisma Streams now ships four indexing layers:

- the existing routing-key tiered index
- the existing exact-match secondary index family, now treated as an internal
  accelerator derived from schema `search.fields`
- a per-segment `.col` family for typed equality, range, and existence
- a per-segment `.fts` family for keyword exact/prefix and text search

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
- `aggregatable` is accepted in schema, but aggregation APIs are not shipped yet
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

### `.col` family

The `.col` family is the typed range/equality family.

Current implementation:

- immutable **per-segment companion objects**
- no `.col` run compaction yet
- local SQLite stores only family progress and companion object keys
- companion objects are uploaded under `streams/<hash>/col/segments/...`

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

- immutable **per-segment companion objects**
- no `.fts` run compaction yet
- local SQLite stores only family progress and companion object keys
- companion objects are uploaded under `streams/<hash>/fts/segments/...`

Current responsibilities:

- keyword exact
- keyword prefix
- text term queries
- phrase queries on fields with `positions=true`
- `has:` on keyword/text fields

## SQLite Catalog

Current local catalog tables:

- `secondary_index_state`
- `secondary_index_runs`
- `search_family_state`
- `search_family_segments`

Interpretation:

- `secondary_*` tables catalog the compacted exact-match family
- `search_family_state` tracks per-family uploaded coverage (`col`, `fts`)
- `search_family_segments` maps each covered segment to its uploaded companion
  object key

These tables are rebuildable from manifest state and remote objects. They are
not durable source-of-truth data stores.

## Manifest And Bootstrap

Manifest state now includes:

- `secondary_indexes`
- `search_families`

`search_families` is keyed by family name and currently stores:

- `uploaded_through`
- per-segment companion object keys

Bootstrap restores:

- exact secondary index state and runs
- `.col` family state and segment companions
- `.fts` family state and segment companions

This means a node can be deleted and cold-restored from published R2 state
without rebuilding the already-published search catalogs locally first.

## Runtime Model

All indexing is currently asynchronous **in-process** work. There is no
separate search worker service and no dedicated worker-thread pool for search.

The full server starts these background managers:

- `IndexManager` for routing-key runs
- `SecondaryIndexManager` for exact secondary runs
- `SearchColManager` for `.col` per-segment companions
- `SearchFtsManager` for `.fts` per-segment companions

These wake on `DS_INDEX_CHECK_MS`.

Relevant concurrency knobs:

- `DS_INDEX_BUILD_CONCURRENCY`
- `DS_INDEX_COMPACT_CONCURRENCY`
- `DS_INDEX_CHECK_MS`

These are in-process async concurrency limits, not separate OS workers.

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
- aggregations
- multi-stream search

## Inspection Endpoints

The current shipped management surface includes two per-stream inspection
endpoints:

- `GET /v1/stream/{name}/_index_status`
- `GET /v1/stream/{name}/_details`

`/_index_status` reports:

- segment counts
- manifest generation/upload state
- routing-key index status
- internal exact-index status
- `.col` and `.fts` family progress

`/_details` is the combined stream-management descriptor. It nests:

- the current stream summary
- the full `/_profile` resource
- the full `/_schema` registry
- the current `/_index_status` payload

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

## Deliberate Gaps Versus The Aspirational Design

The long-term design doc is still directionally correct, but the current system
ships a smaller subset:

- `.col` and `.fts` are per-segment companions only; there are no compacted
  `.col` or `.fts` runs yet
- `.sub` is not implemented
- `_search` does not ship snippets or aggregations yet
- current text scoring is query-time text scoring over the source records; it is
  not a full global BM25 implementation yet
- primary timestamp fallback to append time for missing source fields is not
  implemented yet

These are intentional current-state limits, not compatibility shims.
