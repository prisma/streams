# Indexing Architecture

Status: implemented baseline

This document describes the **current shipped search and indexing model**.
The long-term target still lives in
[aspirational-indexing-architecture.md](./aspirational-indexing-architecture.md).
The planned low-latency read model for heavy-ingest periods lives in
[low-latency-reads-under-ingest.md](./low-latency-reads-under-ingest.md).

## Summary

Prisma Streams now ships these indexing layers:

- the existing routing-key tiered index
- a stream-level lexicographic lexicon run family, currently auto-enabled for
  schema routing keys
- the existing exact-match secondary index family, now treated as an internal
  accelerator derived from schema `search.fields`
- a bundled per-segment `PSCIX2` companion container (`.cix`)
- a plan-relative binary `exact` section family inside `.cix` for doc-level
  exact-value postings
- a plan-relative binary `col` section family inside `.cix` for typed equality, range, and
  existence
- a plan-relative binary `fts` section family inside `.cix` for keyword exact/prefix and text
  search
- a plan-relative binary `agg` section family inside `.cix` for time-window rollups and
  aggregation serving
- a binary `mblk` section family inside `.cix` for metrics-profile aggregate serving

Bundled companion reads now use a local immutable `.cix` cache plus
`Bun.mmap()` over the cached file. The object-store read unit is therefore the
full bundled companion on first access, while the runtime decode unit remains
the requested family section.

The public schema model is **`search`**, not `indexes[]`.

The public query surfaces are:

- `GET /v1/stream/{name}?filter=...`
  - exact/range/existence filtering on JSON streams
  - cursor-friendly read semantics
  - exact family and `.col` may prune sealed history
  - keyed reads use the routing index to build a sealed read plan, so they scan
    candidate indexed segments plus the uncovered tail instead of cursor-walking
    the entire indexed prefix
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
- `GET /v1/stream/{name}/_routing_keys?limit=...&after=...`
  - returns distinct routing keys in alphabetical order
  - pages via the exclusive `after` cursor
  - uses stream-level lexicon runs for the indexed uploaded prefix
  - falls back to direct routing-key extraction from uncovered segments and the
    WAL tail so results remain complete before the first run exists and while
    the lexicon catches up

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

### Routing-key lexicon family

The routing-key lexicon family is a separate immutable run family for
alphabetical distinct-key enumeration. It is enabled automatically for any
stream whose installed schema declares `routingKey`.

Current implementation:

- immutable stream-level `.lex` runs, not per-segment companions
- L0 build span matches the other tiered indexes: 16 uploaded segments
- asynchronous background build after upload
- higher-level immutable compaction using the same contiguous-run policy as the
  other tiered index families
- manifest-published state and run lists
- a local immutable `.lex` file cache under `${DS_ROOT}/cache/lexicon`
  - freshly built runs are seeded into the local cache immediately after upload
  - first read of an uncached run downloads the full `.lex` object once, stores
    it locally, and serves future requests from a local `Bun.mmap()` mapping
  - as with bundled companion files, a `.lex` file that has been mmapped by the
    current process is treated as pinned until restart because Bun does not
    expose an explicit unmap primitive
- best-effort alphabetical browse while lexicon lag exists
  - the indexed uploaded prefix comes from `.lex` runs
  - once any `.lex` coverage exists, uncovered uploaded sealed segments are not
    scanned in the request path
  - the request path may scan at most one uncovered local sealed segment plus
    the WAL tail
  - before the first `.lex` run exists, the request path may scan at most one
    uploaded sealed segment plus the local tail / WAL
  - if uncovered uploaded history remains, `_routing_keys` returns a partial
    page with `coverage.complete=false`
  - `next_after` may still be non-null for those partial pages; Studio must
    treat the cursor as best-effort and show that uploaded lexicon lag may
    still hide earlier keys

The object-store naming is intentionally generic so the same family can later
support lexicographic listing for other fields:

```text
streams/<hash>/lexicon/<source-kind>/<source-name>/<run-id>.lex
```

Current routing-key mapping:

- `source-kind = routing_key`
- `source-name = __default__` in the object path

Future field lexicons can reuse this prefix with a different `source-kind` and
field-specific `source-name`.

Each `.lex` run stores a sorted restart-coded string table. That gives:

- lower-bound seek for `after=...`
- compact immutable objects
- k-way merge pagination across active runs without a mutable global B-tree

The `_routing_keys` response now also exposes per-request timing so operators
and Studio can tell where time is going:

- `timing.lexicon_run_get_ms`
- `timing.lexicon_decode_ms`
- `timing.lexicon_enumerate_ms`
- `timing.lexicon_merge_ms`
- `timing.fallback_scan_ms`
- `timing.fallback_segment_get_ms`
- `timing.fallback_wal_scan_ms`
- `timing.lexicon_runs_loaded`

Studio / operator UI guidance:

- use `/_details.storage.local_storage.lexicon_index_cache_bytes` to show how
  much local `.lex` cache is resident on the node
- use `/_details.storage.local_storage.segment_cache_bytes`,
  `routing_index_cache_bytes`, `exact_index_cache_bytes`, and
  `companion_cache_bytes` to show how much local read-through cache has been
  seeded by routing-key reads, stream reads, and index backfill work
- use `/_routing_keys.coverage.complete`,
  `coverage.possible_missing_uploaded_segments`, and
  `coverage.possible_missing_local_segments` to explain whether the page is
  complete or still waiting on lexicon catch-up
- when `coverage.complete=false`, label the page as best-effort even if
  `next_after` is present
- use the `timing.*` breakdown to distinguish:
  - cached lexicon run load/decode cost
  - lexicon enumeration cost across active runs
  - indexed/fallback merge cost
  - fallback segment scan cost
  - WAL tail scan cost

Serving the first page does not require enumerating the full indexed prefix:

- the request loads all active runs, but only asks each run for a page-sized
  candidate set starting at `after`
- the indexed side is no longer expanded by the number of fallback keys found
  in the WAL or local tail
- lexicon merge yields are batched, so a large fallback set no longer forces
  thousands of event-loop round trips just to serve a small page

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
- binary `col2` field payloads keyed by plan-relative field ordinals
- presence docsets, typed value streams, min/max values, and optional page
  indexes
- no `.col` run compaction yet
- local SQLite stores only the bundled companion plan and per-segment companion
  object keys
- published companion objects live under `streams/<hash>/segments/...cix`
- bundled companion backfill is oldest-missing-first and batched, so `.col`
  coverage grows contiguously across the uploaded segment prefix
- query-time `.col` reads decode only the requested bundled section

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
- binary `fts2` field payloads keyed by plan-relative field ordinals
- restart-string term dictionaries, document-frequency arrays, and block-coded
  posting lists
- first read downloads the full `.cix` once, stores it under
  `${DS_ROOT}/cache/companions`, and mmaps the local cached file
- query-time field views and posting iterators instead of whole-section JSON
  materialization
- document-frequency and postings-offset tables stay as zero-copy views over
  the mapped file
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
- binary `agg2` rollup and interval directories keyed by plan-relative
  ordinals
- interval-local dimension dictionaries, ordinal columns, and measure columns
- per-interval zstd compression when a payload shrinks, with lazy inflate on
  first read of that interval
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
- binary `mblk2` payloads used only by metrics-profile aggregate paths
- zstd-compressed metrics-record payloads when the blob shrinks, with lazy
  inflate on first fallback scan
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
- the desired plan JSON, including plan-relative field and rollup ordinals
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
- `DS_ASYNC_INDEX_CONCURRENCY`

These are in-process async concurrency limits, not separate OS workers.

`DS_ASYNC_INDEX_CONCURRENCY` is the shared top-level permit pool across:

- routing-key L0 build / compaction
- exact-secondary L0 build / compaction
- bundled companion build / backfill

Each manager still has its own inner build/compaction fanout knobs, but no
manager can monopolize more top-level concurrent jobs than the shared async
gate allows.

`SearchCompanionManager` also emits progress metrics so companion lag can be
observed independently of the exact family:

- `tieredstore.companion.build.queue_len`
- `tieredstore.companion.builds_inflight`
- `tieredstore.companion.lag.segments`
- `tieredstore.companion.build.latency`
- `tieredstore.companion.objects.built`

The node also emits shared runtime gate metrics into `__stream_metrics__` so
operators can tell whether async indexing is being narrowed under
memory pressure:

- `tieredstore.concurrency.limit` with `gate=async_index` and
  `kind=configured|effective`
- `tieredstore.concurrency.active` with `gate=async_index`
- `tieredstore.concurrency.queued` with `gate=async_index`
- `process.memory.pressure`

For a point-in-time view of the same state plus the configured auto-tune
budget, use `GET /v1/server/_details`.

On startup, the full server enqueues all streams into the index controller so
existing streams can catch up automatically after bootstrap, schema changes, or
bundled companion plan changes.

## Bundled Companions And Backfill

Current bundled-companion rules:

- each uploaded segment may have one current `.cix`
- the `.cix` may contain any subset of `exact`, `col`, `fts`, `agg`, and `mblk`
- the desired bundled companion plan is hashed and versioned per stream
- bundled companions use the binary `PSCIX2` container with a fixed header and
  fixed section table
- each bundled companion build loads one segment and builds enabled families
  sequentially, so `exact`, `col`, `fts`, `agg`, and `mblk` do not keep their
  heaviest in-memory state live at the same time
- family payloads are plan-relative and do not repeat field or rollup names
- query-time companion reads cache raw `.cix` bytes plus the parsed section
  table and decode only the requested section family on demand
- long-running bundled companion builds yield cooperatively every bounded number
  of segment blocks so the HTTP server stays responsive during backfill
- bundled companion backfill no longer hard-pauses on a memory overload flag;
  instead it competes for the shared async-index gate like the other indexing
  managers
- a plan change puts the stream into mixed coverage until historical companions
  are rebuilt
- queries use current bundled sections where present and raw-scan missing or
  stale ranges otherwise

See [bundled-companion-and-backfill.md](./bundled-companion-and-backfill.md)
and [storage-layout-architecture.md](./storage-layout-architecture.md) for the
dedicated bundled-companion and binary storage documents.

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
- when a routing-key or exact-candidate set is available, the reader plans the
  sealed segment scan up front and visits only candidate indexed segments plus
  the uncovered uploaded tail; it does not cursor-walk the full indexed sealed
  prefix just to skip non-candidate history

This path is optimized for stream-like cursor progression, not ranked search.

### `_search`

Current request shape:

- `q`
- `size`
- `search_after`
- `sort`
- `timeout_ms`
  - optional lower per-request budget
  - the server clamps effective `/_search` timeout to `<= 3000 ms`
  - the deadline is enforced cooperatively between work units, so wall time may
    overshoot slightly before the partial response is returned

Current response shape:

- `stream`
- `snapshot_end_offset`
- `took_ms`
- `timed_out`
- `timeout_ms`
- `coverage`
- `total`
- `hits`
- `next_search_after`

Current timeout behavior:

- `/_search` returns `200` when it finishes within the effective timeout budget
- `/_search` returns `408` when it exhausts the effective timeout budget
- the `408` response still includes a normal search result body with:
  - partial `hits`
  - `timed_out=true`
  - `total.relation="gte"`
  - coverage and search timing counters
- if the outer generic `5000 ms` resolver timeout fires first while an
  in-flight search work unit is still running, `/_search` may instead return
  the generic `request_timeout` error body
- because timeout checks are cooperative, elapsed wall time can be slightly
  higher than `timeout_ms`
- response headers mirror the same timing counters for easier inspection in
  browser tooling
- `/_search` does not support request-time exact total-hit counting
- when exact clauses produce a candidate segment set, `_search` uses a planned
  sealed-segment scan instead of iterating the entire indexed sealed prefix one
  segment at a time

Current search coverage fields:

- `mode`
- `complete`
- `stream_head_offset`
- `visible_through_offset`
- `visible_through_primary_timestamp_max`
- `oldest_omitted_append_at`
- `possible_missing_events_upper_bound`
- `possible_missing_uploaded_segments`
- `possible_missing_sealed_rows`
- `possible_missing_wal_rows`
- `indexed_segments`
- `indexed_segment_time_ms`
- `scanned_segments`
- `scanned_segment_time_ms`
- `scanned_tail_docs`
- `scanned_tail_time_ms`
- `exact_candidate_time_ms`
- `candidate_doc_ids`
- `decoded_records`
- `json_parse_time_ms`
- `segment_payload_bytes_fetched`
- `sort_time_ms`
- `peak_hits_held`
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

Current candidate-planning behavior:

- exact-equality clauses use bundled `.exact` doc-id postings when available
  before intersecting with `.col` and `.fts` candidates
- for append-order reverse search, a non-empty per-segment candidate doc-id set
  is walked directly in offset order, and only blocks containing candidate hits
  are decoded
- remote candidate-doc searches range-read the segment footer and matching
  compressed blocks instead of fetching the full segment object when the DSB3
  footer is available
- fielded exact keyword clauses still use the internal exact family first for
  sealed-history segment pruning when that family is available
- if a keyword field is also present in bundled `.fts` because it enables
  `prefix=true`, `_search` also uses the `.fts` term dictionary/postings as a
  per-segment doc-id fallback for exact clauses
- positive `.fts` clauses are evaluated in estimated-selectivity order, and
  later clauses are checked against the current candidate doc-id set instead of
  materializing every clause against the whole segment
- quiet WAL-tail exact clauses use a per-reader in-memory exact postings cache
  for the visible tail range, so repeated exact tail lookups fetch only matching
  WAL rows instead of scanning the whole tail again

Current non-support:

- `contains:` / `.sub`
- snippets
- multi-stream search

Current newest-suffix behavior:

- while sealed segments are still unpublished or bundled companions are still
  catching up, `/_search` omits that newest suffix instead of raw-scanning it
- the omitted range is reported through the `possible_missing_*` coverage
  fields
- once publish and bundled-companion work are fully caught up, `/_search`
  still omits a fresh WAL tail during active ingest
- `/_search` only uses the bounded WAL tail as a local overlay after the tail
  is quiet for the configured overlay period and still fits within the overlay
  budget
- `visible_through_primary_timestamp_max` and `oldest_omitted_append_at` let
  Studio explain the freshness gap in time terms

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
- `stream_head_offset`
- `visible_through_offset`
- `visible_through_primary_timestamp_max`
- `oldest_omitted_append_at`
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
  still omits a fresh WAL tail during active ingest
- `/_aggregate` only uses the bounded WAL tail as a local overlay after the
  tail is quiet for the configured overlay period and still fits within the
  overlay budget

## Inspection Endpoints

The current shipped management surface includes two per-stream inspection
endpoints:

- `GET /v1/stream/{name}/_index_status`
- `GET /v1/stream/{name}/_details`
- `GET /v1/stream/{name}/_routing_keys`

`/_index_status` reports:

- segment counts
- manifest generation/upload state
- routing-key index status
- routing-key lexicon status
- internal exact-index status, including stale-config detection
- bundled companion object coverage
- `exact`, `col`, `fts`, `agg`, and `mblk` family progress derived from bundled
  companion sections

Current exact-index scheduling:

- bundled companions are the first background priority for uploaded segments
- all async index families share one bounded gate and yield cooperatively inside
  segment scans; when a foreground read or search is active, those background
  loops back off further instead of monopolizing the event loop
- routing, exact, and lexicon compactions also wait for a short quiet window
  after foreground traffic before resuming
- exact secondary-index build and compaction only run after bundled companions
  are caught up, the stream has no in-progress segment cut or pending upload
  segment, and the stream has been append-idle for about ten minutes so exact
  work does not re-enter the ingest hot path during long but temporary quiet
  gaps
- byte-at-rest and object-count accounting for index families
- lag in both segments and milliseconds for routing, routing-key lexicon,
  exact, and bundled-family
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

The internal `__stream_metrics__` system stream is the intentional exception:

- it keeps the `metrics` profile for canonical record normalization
- it installs only the canonical schema
- it does not install `routingKey`, `search.fields`, or `search.rollups`
- it therefore does not build routing, lexicon, exact, `.col`, `.fts`, `.agg`,
  or `.mblk` state for the internal stream

The intended planner order for metrics streams is:

- `.agg` for aligned rollup-eligible windows
- `.mblk` for non-aligned or non-rollup-eligible aggregate serving
- raw source scan only when published coverage is missing

## Deliberate Gaps Versus The Aspirational Design

The long-term design doc is still directionally correct, but the current system
ships a smaller subset:

- `.exact`, `.col`, and `.fts` are per-segment companions only; there are no
  compacted `.exact`, `.col`, `.fts`, or `.agg` runs yet
- `.sub` is not implemented
- `_search` does not ship snippets
- current text scoring is query-time text scoring over the source records; it is
  not a full global BM25 implementation yet
- primary timestamp fallback to append time for missing source fields is not
  implemented yet

These are intentional current-state limits, not compatibility shims.
