# Indexing architecture for evlog search on Durable Streams

Status: long-term target  
Audience: implementation engineers working on the Bun/TypeScript Durable Streams server  
Scope: object-store-native asynchronous search indexing for `application/json` evlog streams

Current shipped state lives in
[indexing-architecture.md](./indexing-architecture.md). This document remains
the broader target design; it intentionally describes features that are not yet
implemented, including `.sub`, snippets, and full BM25-style global scoring.

Current shipped rollups now live in a separate `.agg` family documented in
[aggregation-rollups.md](./aggregation-rollups.md). This aspirational document
still describes the broader evlog search/search-index target, not the shipped
rollup serving path.

---

## 1. Executive summary

The primary UI for evlog should be **structured, fielded search over wide events**, not raw substring search over anonymous log lines.

An evlog event is one structured wide event per request. That gives the system three distinct search jobs:

1. **Exact/typed filtering** for fields like `@timestamp`, `level`, `service`, `environment`, `method`, `path`, `route`, `status`, `duration_ms`, `requestId`, `traceId`, `user.id`, `user.plan`, and `tenantId`.
2. **Full-text search** for human-readable explanation fields like `message`, `why`, `fix`, and `error.message`.
3. **Explicit contains/grep mode** for escape-hatch substring searches such as partial IDs, stack trace fragments, or raw snippets.

The architecture in this document implements those three jobs with **three independent secondary index families** built on top of the existing immutable segment + manifest system:

- **FTS family (`.fts`)**: a general inverted index family used for both analyzed text fields and exact-token keyword fields.
- **Column family (`.col`)**: a fast-field / columnar family used for ranges, sorting, time pruning, and aggregations.
- **Substring family (`.sub`)**: a trigram-based family used only for explicit `contains:` queries.

These families are:

- **asynchronous with respect to appends**: writes still commit only to the SQLite WAL in the hot path;
- **object-store-native**: the source of truth for uploaded index state is the stream manifest plus immutable search objects in R2;
- **incremental**: new sealed segments get new companion index objects, and larger immutable runs are built later by compaction;
- **correct even when indexing lags**: queries always scan the unindexed or stale tail instead of returning incomplete results;
- **TTL-safe**: expired streams are hidden immediately before any index access; cleanup happens later.

The key design decision is that **search correctness never depends on index completeness**. The index is an accelerator and a first-class remote serving structure, but not the source of truth. The source of truth remains the WAL, sealed data segments, and the stream manifest.

---

## 2. Design goals

### 2.1 Goals

1. Provide a first-class search UI for evlog streams.
2. Support low-latency fielded search and full-text search without requiring the full index to be resident locally.
3. Preserve the current Durable Streams correctness model:
   - WAL commit remains the only write durability point.
   - Uploaded data becomes visible only after manifest publication.
   - Search must never expose data that the stream read path would not expose.
4. Reuse the existing architecture:
   - SQLite as local WAL + metadata catalog.
   - immutable sealed segments on local disk and R2.
   - manifest-published uploaded prefixes.
   - local cache + object store for remote reads.
5. Support schema evolution for JSON streams without making ordinary field moves or renames impossible.
6. Support stream TTL without stale search hits.
7. Make backfill, recovery, and cold bootstrap possible from object storage.

### 2.2 Non-goals

1. Raw substring search is **not** the primary search mechanism.
2. A local monolithic Lucene/Tantivy/SQLite-FTS database is **not** the primary persisted search store.
3. Multi-stream search is not required in the first implementation.
4. Row-level deletes and per-document TTL are out of scope for v1.
5. Regex, fuzzy search, synonym expansion, and arbitrary text-range semantics are out of scope for v1.

---

## 3. Existing platform assumptions

This design assumes the current Durable Streams server shape from the repository:

- appends go to SQLite WAL rows;
- a background segmenter builds immutable `segments/<n>.bin` objects;
- an uploader publishes `manifest.json` generations containing the contiguous uploaded data prefix;
- a reader merges sealed history from segments with tail rows from SQLite;
- routing-key indexing already exists as an object-store-native, rebuildable tier-2 index family;
- streams already have TTL/expiry state via `ttl_seconds`, `expires_at_ms`, and `stream_flags`;
- streams already have schema registries and versioned schema boundaries.

This search design follows the same principles as the existing routing-key index:

- SQLite stores only local catalog state and upload progress.
- Uploaded search objects in R2 plus manifest metadata are the durable remote state.
- Local caches are disposable.
- Unindexed data is still readable/searchable through a fallback path.

---

## 4. Search model for evlog

### 4.1 Event classes

evlog wide events naturally split fields into three classes.

**A. Core typed fields**

These should behave like first-class columns and exact filters:

- `@timestamp`
- `level`
- `service`
- `environment`
- `method`
- `path`
- `route`
- `status`
- `duration_ms`
- `requestId`
- `traceId`
- `link`
- `error.code`
- `error.type`
- `user.id`
- `user.plan`
- `tenantId`

**B. Human-readable text fields**

These should behave like full-text fields:

- `message`
- `why`
- `fix`
- `error.message`

**C. Flexible nested business/application attributes**

These should behave like flattened dotted attributes with exact-match and presence semantics:

- `cart.id`
- `shipping.country`
- `ai.model`
- `ai.toolCalls[*].name`
- any other nested scalar values not already claimed by core fields

### 4.2 Default evlog field behavior

The default evlog search profile is:

- `@timestamp`, `status`, `duration_ms` are numeric/date fast fields in `.col`.
- keyword-like fields (`level`, `service`, `environment`, `method`, `path`, `route`, `requestId`, `traceId`, `error.code`, `error.type`, `user.id`, `user.plan`, `tenantId`, `link`) are indexed in `.fts` with a **keyword analyzer** for exact/prefix search and optionally in `.col` if they must support sorting or aggregations.
- text explanation fields (`message`, `why`, `fix`, `error.message`) are indexed in `.fts` with a **text analyzer** and positions enabled.
- selected fields (`requestId`, `traceId`, `message`, `why`, `fix`, `error.message`, `path`, `link`) are also indexed in `.sub` for explicit `contains:` search.
- arbitrary nested scalar attributes are flattened into dotted-path exact terms and presence terms. They are queryable as exact/presence filters, but not sortable or aggregatable unless explicitly mapped as core fields.
- `search_text` and `contains_text` are **logical virtual fields**, not required physical fields. `search_text` rewrites to the configured default text fields; `contains_text` rewrites to the configured contains-enabled fields. This keeps the implementation aligned with evlog’s conceptual model without duplicating storage.

### 4.3 Default query behavior

The primary query surface is a human-friendly query string syntax.

Examples:

```txt
card declined issuer
status:>=500 path:/api/checkout
why:"insufficient funds"
fix:"try a different card"
req:req_8f2k
trace:9f2b4a1c
user.id:1842
shipping.country:US
@timestamp:>=now-15m
has:fix
-status:200
contains:req_8f2k
contains:"TypeError: fetch failed"
```

Semantics:

- bare terms search the configured default text fields;
- `field:value` means exact match for keyword fields, numeric/date compare for typed fields, text match for text fields;
- `contains:` is explicit and slower than the primary search path;
- dotted paths not declared as explicit fields are treated as flattened attribute filters;
- structured filters constrain the candidate set and usually do not contribute to score;
- text clauses contribute score via BM25.

---

## 5. Search schema extension

Search indexing must be schema-driven. It cannot depend on ad hoc field discovery alone because the query layer needs stable field identities, analyzers, and capabilities.

### 5.1 Registry extension

Extend the existing per-stream schema registry with a new top-level `search` section.

Example:

```json
{
  "apiVersion": "durable.streams/schema-registry/v1",
  "schema": "billing-evlog",
  "currentVersion": 2,
  "boundaries": [
    { "offset": 0, "version": 1 },
    { "offset": 150000, "version": 2 }
  ],
  "schemas": {
    "1": { "type": "object" },
    "2": { "type": "object" }
  },
  "lenses": {
    "1": { "...": "v1->v2 lens" }
  },
  "search": {
    "profile": "evlog/v1",
    "primaryTimestampField": "@timestamp",
    "defaultFields": [
      { "field": "message", "boost": 3.0 },
      { "field": "why", "boost": 2.0 },
      { "field": "fix", "boost": 1.5 },
      { "field": "error.message", "boost": 2.5 }
    ],
    "containsDefaultFields": [
      "requestId",
      "traceId",
      "message",
      "why",
      "fix",
      "error.message",
      "path",
      "link"
    ],
    "aliases": {
      "req": "requestId",
      "trace": "traceId",
      "ts": "@timestamp",
      "dur": "duration_ms"
    },
    "fields": {
      "@timestamp": {
        "kind": "date",
        "bindings": [
          { "version": 1, "jsonPointer": "/@timestamp" },
          { "version": 2, "jsonPointer": "/@timestamp" }
        ],
        "column": true,
        "sortable": true,
        "aggregatable": true,
        "exists": true
      },
      "level": {
        "kind": "keyword",
        "bindings": [
          { "version": 1, "jsonPointer": "/level" },
          { "version": 2, "jsonPointer": "/level" }
        ],
        "normalizer": "lowercase_v1",
        "inverted": true,
        "column": true,
        "sortable": true,
        "aggregatable": true,
        "exists": true
      },
      "path": {
        "kind": "keyword",
        "bindings": [
          { "version": 1, "jsonPointer": "/path" },
          { "version": 2, "jsonPointer": "/http/path" }
        ],
        "normalizer": "identity_v1",
        "inverted": true,
        "contains": true,
        "exists": true
      },
      "status": {
        "kind": "integer",
        "bindings": [
          { "version": 1, "jsonPointer": "/status" },
          { "version": 2, "jsonPointer": "/http/status" }
        ],
        "column": true,
        "sortable": true,
        "aggregatable": true,
        "exists": true
      },
      "message": {
        "kind": "text",
        "bindings": [
          { "version": 1, "jsonPointer": "/message" },
          { "version": 2, "jsonPointer": "/message" }
        ],
        "analyzer": "unicode_word_v1",
        "record": "position",
        "fieldnorms": true,
        "inverted": true,
        "contains": true,
        "exists": true
      }
    },
    "dynamic": {
      "flattenUnmappedScalars": true,
      "maxDepth": 8,
      "maxStringBytes": 4096,
      "indexMode": "exact_only"
    }
  }
}
```

### 5.2 Field identity

Every entry in `search.fields` has a **stable logical field ID** such as `message`, `requestId`, or `error.code`.

Queries target the logical field ID, not the source JSON pointer.

That is how the system survives schema evolution. Example:

- v1: `path` binds to `/path`
- v2: `path` binds to `/http/path`

Both versions still index into the same logical field ID: `path`.

### 5.3 Bindings

`bindings` are per-source-schema extraction rules.

For each record being indexed or scanned:

1. determine the schema version for the record offset using the stream’s boundary table;
2. select the field binding for that schema version;
3. extract the value(s) from the JSON document;
4. normalize/tokenize according to the field’s capabilities.

If a field has no binding for a record’s schema version, the field is treated as missing for that record.

### 5.4 Compatibility rules

Changes that **do not** require reindexing old segments:

- adding a binding for a new schema version to an existing field ID;
- adding a new field ID;
- adding/removing aliases;
- changing default field weights;
- changing UI-only metadata.

Changes that **do** require old segments that reference that field to be treated as stale for that field until rebuilt:

- changing `kind`;
- changing analyzer/tokenizer/normalizer;
- toggling `record` mode;
- toggling `fieldnorms`;
- enabling or disabling substring indexing for that field;
- changing field semantics while reusing the same field ID.

### 5.5 Field coverage and stale detection

Each companion `.hot` file must contain a **field directory entry for every field defined in the search mapping at build time**, even if the field has zero documents in that segment.

Each entry carries:

- `field_id`
- `field_semantics_epoch` (or hash)
- capability bits (`inverted`, `column`, `contains`, `positions`, `exists`, `sortable`, `aggregatable`)
- `docs_with_field`

This is critical for correctness.

It lets the planner distinguish these cases:

- the segment is authoritative for `fix`, and `docs_with_field=0` means no matching values exist;
- the segment companion predates the introduction of `fix`, so it is **not authoritative** for `fix` and must be scanned instead.

### 5.6 Primary timestamp field

Every searchable stream must declare one `primaryTimestampField`.

For the evlog profile this is `@timestamp`.

If extraction fails or the field is missing on a document, indexing falls back to the WAL ingest timestamp for that document. This preserves time pruning and recency sort even when source events are imperfect.

The original event JSON blob remains the source for the event detail view and for snippet verification/hydration. The search index stores only derived search structures, not a second canonical copy of the event.

---

## 6. Query API

### 6.1 Endpoint

Add:

```txt
POST /v1/stream/:name/_search
```

Optional convenience form:

```txt
GET /v1/stream/:name/_search?q=...
```

The POST form is the primary interface.

### 6.2 Request body

```json
{
  "q": "status:>=500 path:/api/checkout contains:\"TypeError: fetch failed\"",
  "size": 50,
  "track_total_hits": true,
  "search_after": [8.104, 1735778119000, "0:123355"],
  "sort": ["_score", "@timestamp:desc", "offset:desc"],
  "snippet": {
    "enabled": true,
    "fields": ["message", "why", "fix", "error.message"],
    "fragment_size": 160,
    "max_fragments": 1
  },
  "aggs": {
    "levels": { "terms": { "field": "level", "size": 10 } },
    "timeline": { "date_histogram": { "field": "@timestamp", "fixed_interval": "1m" } }
  },
  "timeout_ms": 5000
}
```

### 6.3 Response body

```json
{
  "stream": "billing/evlog",
  "snapshot_end_offset": "0:123456",
  "took_ms": 37,
  "coverage": {
    "indexed_segments": 120,
    "scanned_segments": 3,
    "scanned_tail_docs": 142,
    "index_families_used": ["fts", "col", "sub"]
  },
  "total": {
    "value": 182,
    "relation": "eq"
  },
  "hits": [
    {
      "offset": "0:123400",
      "score": 9.712,
      "sort": [9.712, 1735778123123, "0:123400"],
      "fields": {
        "@timestamp": "2026-03-25T10:15:23.123Z",
        "level": "error",
        "service": "billing-api",
        "path": "/api/checkout",
        "status": 402,
        "duration_ms": 1834,
        "requestId": "req_8f2k",
        "traceId": "trace-xyz-789"
      },
      "snippet": {
        "message": ["card declined by issuer"],
        "why": ["issuer reported insufficient funds"]
      },
      "source": {
        "...": "original event JSON"
      }
    }
  ],
  "next_search_after": [8.104, 1735778119000, "0:123355"],
  "aggs": {
    "levels": { "buckets": [{ "key": "error", "doc_count": 81 }] },
    "timeline": { "buckets": [{ "key": 1735778100000, "doc_count": 12 }] }
  }
}
```

### 6.4 Query string grammar

Supported grammar in v1:

- implicit `AND` between whitespace-separated clauses;
- explicit `OR`;
- `(` and `)` for grouping;
- unary negation with `-` or `NOT`;
- quoted phrases;
- field scoping via `field:value`;
- comparison operators for typed fields: `>`, `>=`, `<`, `<=`;
- existence checks with `has:field`;
- substring escape hatch with `contains:value`;
- trailing `*` for prefix queries on supported fields;
- optional clause boost using `^N`.

Examples:

```txt
level:error status:>=500
(message:"card declined" OR why:"insufficient funds") user.plan:pro
path:/api/checkout* -status:200
has:fix contains:req_8f2k
```

### 6.5 Type-specific semantics

- `keyword` fields: exact match, prefix, exists, terms aggregation, optional sort.
- `text` fields: analyzed match, phrase, phrase-prefix, exists, snippets.
- `integer`, `float`, `date`, `bool`: range/equality/existence, optional sort, histogram/range aggregations.
- `dynamic flattened attrs`: exact match and presence only in v1.

### 6.6 Field aliases

The parser resolves aliases before planning.

Default evlog aliases:

- `req` → `requestId`
- `trace` → `traceId`
- `ts` → `@timestamp`
- `dur` → `duration_ms`

### 6.7 Default sort

- If the query contains any scoring text clause, default sort is `_score desc, @timestamp desc, offset desc`.
- If the query is filter-only, default sort is `@timestamp desc, offset desc`.

### 6.8 Pagination

Support deep pagination with `search_after`, not scroll.

A `search_after` token is valid only when used with:

- the same query;
- the same sort order;
- the same `snapshot_end_offset`.

If the stream expires before the next page request, return `404 stream expired`.

---

## 7. Query feature support matrix

The table below maps the intended query surface to the backing structures.

### 7.1 Supported in the initial architecture

- bare terms → `.fts` over configured default text fields
- `term` / exact keyword match → `.fts` with keyword analyzer
- `bool` / `AND` / `OR` / `NOT` → query planner
- `query_string`-style field syntax → query parser + planner
- phrase / `match_phrase` → `.fts` positions
- phrase-prefix / `match_phrase_prefix` → `.fts` positions + prefix lexicon expansion
- prefix / `match_bool_prefix` → `.fts` lexicon prefix expansion
- `exists` → presence bitmaps or flattened-attr presence terms
- `match_all` / `match_none` → planner constants
- numeric/date equality and range → `.col`
- `@timestamp` time filters → `.col` + manifest segment time arrays
- field sorting → `.col`
- snippets → source hydration + retokenization
- `terms`, `range`, `histogram`, `date_histogram`, `count`, `min`, `max`, `avg`, `sum`, `stats` aggregations → `.col`
- explicit substring `contains:` → `.sub`, with scan fallback where unavailable

### 7.2 Deferred or optional

- cardinality → `.col` + HLL sketch or filtered exact set merge
- percentiles / extended_stats → `.col` + DDSketch/t-digest or exact filtered scan
- multi-stream federated search → later
- scroll → later, if needed
- regex / fuzzy / synonyms → not in v1
- lexicographic text ranges → not supported

### 7.3 Deliberate differences from Quickwit-like behavior

1. Prefix expansion must not silently drop hits by default. If expansion exceeds the configured limit, return a clear error unless the caller explicitly asks for approximate prefix semantics.
2. Flattened dynamic attrs are exact/presence only in v1. Range over arbitrary dynamic paths is not supported.
3. `contains:` is explicit. Substring search is not blended into the main full-text path.

---

## 8. Index families

## 8.1 FTS family (`.fts`)

### 8.1.1 Role

The FTS family is the primary inverted index family.

Despite the name, it indexes both:

- analyzed text fields; and
- exact-token keyword fields.

It answers:

- full-text match;
- fielded text match;
- exact keyword filters;
- phrase and phrase-prefix queries;
- prefix queries;
- `exists` via presence bitmaps;
- candidate generation for BM25 ranking.

### 8.1.2 Analyzer set

Define these analyzers/normalizers initially:

- `unicode_word_v1`
  - Unicode word tokenization
  - lowercase
  - no stemming by default
  - no stopword removal by default
- `keyword_v1`
  - entire value is one token
  - normalizer-controlled (`identity_v1`, `lowercase_v1`, `uppercase_v1`)
- `path_keyword_v1`
  - same as `keyword_v1` for now; reserved for future path-specific tokenization

Technical/log text is often harmed by stemming and aggressive stopword removal. Start conservative.

### 8.1.3 Record modes

Each field in `.fts` uses one of these record modes:

- `basic`: docids only; suitable for filter-only exact fields
- `freq`: docids + term frequency; minimum for BM25
- `position`: docids + term frequency + term positions; required for phrase queries and best snippets

Recommended defaults:

- text fields: `position`
- keyword filter fields: `basic`
- if a keyword field needs score contribution, use `freq`

### 8.1.4 Data model

Documents are segment-local and correspond 1:1 with stream offsets in that segment.

For a segment with `start_offset = X` and `doc_count = N`:

- local docid range is `[0, N)`
- stream offset = `start_offset + local_docid`

### 8.1.5 Companion object keys

Remote:

```txt
streams/<hash>/fts/segments/<segment-index>-g<object-gen>.fts
streams/<hash>/fts/segments/<segment-index>-g<object-gen>.hot
streams/<hash>/fts/runs/<run-id>.ftr
streams/<hash>/fts/runs/<run-id>.hot
```

Local:

```txt
DS_ROOT/local/streams/<hash>/fts/segments/<segment-index>-g<object-gen>.fts
DS_ROOT/local/streams/<hash>/fts/segments/<segment-index>-g<object-gen>.hot
DS_ROOT/cache/search/fts/...
```

`object-gen` is a monotonically increasing immutable version for that segment/family. It prevents mutable overwrites of object-store keys.

### 8.1.6 `.fts` file layout

Version 1 layout:

- fixed header
  - magic `FTS1`
  - format version
  - stream epoch
  - segment index
  - start offset
  - end offset exclusive
  - doc count
  - footer offset
- data sections
  - field lexicon blocks
  - postings blobs
  - positions blobs
  - fieldnorm arrays
  - existence bitmaps
- footer
  - field directory
  - section offsets/lengths
  - per-field statistics
  - checksum

Per-field footer entry:

- `field_id`
- `field_semantics_epoch`
- `kind`
- `analyzer_id`
- capability bits
- `docs_with_field`
- `sum_field_len`
- `term_count`
- offsets and lengths for:
  - lexicon sparse index
  - norms
  - presence bitmap

Lexicon blocks:

- sorted by term bytes
- block-local prefix compression
- each term record stores:
  - `doc_freq`
  - `total_term_freq`
  - `postings_offset`
  - `postings_length`
  - `positions_offset`
  - `positions_length`

Postings encoding in v1:

- delta-coded local docids as unsigned varints
- optional term frequencies as unsigned varints
- positions only for `record=position`

This encoding is intentionally simple for v1. The format reserves room for chunked postings and block-level skipping later.

### 8.1.7 `.hot` layout

The `.hot` file is the small planning object used first in the query path.

It contains:

- segment/run identity
- doc count
- start/end offsets
- timestamp min/max for the primary timestamp field
- field directory entries for every mapped field at build time
- per-field summary stats
- sparse lexicon block directory:
  - first term in block
  - block offset
  - block length

The planner must be able to answer these questions using `.hot` alone:

- is the provider authoritative for field `X`?
- does field `X` support positions / prefix / exists / sorting / aggs?
- what terms/blocks should be range-read from the `.fts` object?
- can this segment/run be pruned by time before reading deeper data?

### 8.1.7a Run layout

An `.fts` run covers a contiguous segment range.

A run reuses the same top-level layout as a segment companion, with these differences:

- the header stores `start_segment`, `end_segment`, and run level rather than one segment index;
- the `.hot` file includes a segment directory for the covered range;
- postings are encoded as segment-relative groups rather than a single segment’s local docids.

Recommended postings encoding for runs in v1:

- term metadata points to one postings blob;
- the postings blob is a sequence of segment groups;
- each group stores `segment_rel`, doc count for that segment, and delta-coded local docids within that segment;
- text fields additionally store per-doc term frequency and optional positions payload per segment group.

This avoids rewriting hits to absolute offsets inside the run while still allowing the searcher to recover the stream offset using `start_offset + local_docid` for the referenced segment.

### 8.1.8 Query behavior

- exact keyword filter → look up single term in keyword field
- text term query → look up analyzed tokens in text field
- phrase → fetch positions for intersected candidate docs only
- prefix → enumerate lexicon range; enforce `max_prefix_terms`
- `exists` → check field presence bitmap, not postings existence

### 8.1.9 Scoring

BM25 parameters are configurable but start with standard defaults.

Scoring rules:

- only text clauses and explicitly boosted keyword clauses contribute score;
- filter-only clauses constrain candidates but do not contribute score;
- collection stats are computed over all indexed providers participating in the query plus the scanned tail when needed.

## 8.2 Column family (`.col`)

### 8.2.1 Role

The column family is the fast-field layer.

It answers:

- numeric/date/bool equality and range filters
- primary timestamp pruning
- sort by field
- aggregations
- faceting/histograms

### 8.2.2 Fields in `.col`

Recommended evlog fields in `.col`:

- `@timestamp`
- `status`
- `duration_ms`
- `level`
- `service`
- `environment`
- `method`
- `route`
- `user.plan`

Optional high-cardinality keyword fields in `.col`:

- `requestId`
- `traceId`
- `user.id`
- `tenantId`

These are only needed if the UI requires sort or aggregations on them. Exact filtering on those fields should still use `.fts` keyword postings.

### 8.2.3 `.col` file layout

Remote:

```txt
streams/<hash>/col/segments/<segment-index>-g<object-gen>.col
streams/<hash>/col/segments/<segment-index>-g<object-gen>.hot
streams/<hash>/col/runs/<run-id>.cor
streams/<hash>/col/runs/<run-id>.hot
```

Per-field entry in the footer:

- `field_id`
- `field_semantics_epoch`
- `kind`
- `sortable`
- `aggregatable`
- `doc_count`
- `null_count`
- `min_value`
- `max_value`
- `encoding`
- `null_bitmap_offset` / `length`
- `values_offset` / `length`
- optional dictionary offsets for keyword ordinals

Supported encodings:

- `u64_delta_v1` for non-negative integers and timestamps
- `i64_delta_v1` for signed integers
- `f64_plain_v1` for floats
- `bool_bitmap_v1` for booleans
- `dict_u32_v1` for keyword ordinals

For v1, per-segment min/max statistics are sufficient. Block-level stats may be added later.

### 8.2.4 Aggregations

Supported over `.col` in the base architecture:

- `terms`
- `range`
- `histogram`
- `date_histogram`
- `count`
- `min`
- `max`
- `avg`
- `sum`
- `stats`

Terms aggregation over strings should generally run on keyword/ordinal columns, not analyzed text fields.

### 8.2.5 Timestamp pruning

For the declared `primaryTimestampField`, segment-level min/max timestamps are also materialized into manifest-aligned arrays. This allows the planner to prune most segments before opening any `.hot` file.

### 8.2.6 Run layout

A `.col` run covers a contiguous segment range and stores column chunks grouped by segment.

For each field, the run footer must include a segment directory:

- `segment_rel`
- covered doc count
- null bitmap offset/length
- values offset/length
- per-segment min/max

The searcher can then decode only the segment chunks it still needs after other clauses have reduced the candidate set.

## 8.3 Substring family (`.sub`)

### 8.3.1 Role

The substring family exists solely for explicit `contains:` queries.

It is not part of the main relevance path.

### 8.3.2 Approach

Use a trigram index over normalized UTF-8 bytes for contains-enabled fields.

Normalization:

- Unicode NFC
- lowercase
- preserve punctuation and whitespace bytes

For each selected field value:

- generate overlapping byte trigrams;
- index trigram → local docid postings.

### 8.3.3 Query behavior

For a query `contains:"TypeError: fetch failed"`:

1. normalize the query string;
2. if normalized length < 3 bytes, skip `.sub` and scan fallback directly;
3. generate trigrams;
4. fetch postings for each trigram;
5. intersect candidates;
6. verify candidates against hydrated field/source text.

Verification is mandatory. The trigram index is only a candidate generator.

### 8.3.4 Fields covered by `contains:`

Default evlog fields:

- `requestId`
- `traceId`
- `message`
- `why`
- `fix`
- `error.message`
- `path`
- `link`

Dynamic attrs are **not** substring-indexed in v1 by default, to keep storage growth bounded. Queries that need substring over unmapped or dynamic content fall back to raw scan.

### 8.3.5 `.sub` format

The `.sub` format can reuse the same lexicon + postings layout as `.fts`, with these simplifications:

- fixed analyzer: `trigram_bytes_v1`
- no fieldnorms
- no positions
- no scoring

Remote:

```txt
streams/<hash>/sub/segments/<segment-index>-g<object-gen>.sub
streams/<hash>/sub/segments/<segment-index>-g<object-gen>.hot
streams/<hash>/sub/runs/<run-id>.sbr
streams/<hash>/sub/runs/<run-id>.hot
```

Substring runs use the same segment-grouped postings strategy as `.fts` runs, but without norms, frequencies, or positions.

---

## 9. Manifest and local catalog

## 9.1 Manifest extension

Extend `manifest.json` with a new `search_index` object.

```json
{
  "name": "billing/evlog",
  "generation": 42,
  "uploaded_through": 128,
  "segment_count": 128,
  "search_index": {
    "version": 1,
    "primary_timestamp_field": "@timestamp",
    "default_fields": [
      { "field": "message", "boost": 3.0 },
      { "field": "why", "boost": 2.0 },
      { "field": "fix", "boost": 1.5 },
      { "field": "error.message", "boost": 2.5 }
    ],
    "segment_doc_counts_b64": "...",
    "segment_min_ts_ms_b64": "...",
    "segment_max_ts_ms_b64": "...",
    "families": {
      "fts": {
        "uploaded_segment_count": 120,
        "segment_object_gens_b64": "...",
        "active_runs": [
          {
            "run_id": "fts-l1-0000000000000000-0000000000000015-...",
            "level": 1,
            "start_segment": 0,
            "end_segment": 15,
            "data_object_key": "streams/<hash>/fts/runs/<run>.ftr",
            "hot_object_key": "streams/<hash>/fts/runs/<run>.hot",
            "field_semantics_digest": "sha256:..."
          }
        ],
        "retired_runs": []
      },
      "col": {
        "uploaded_segment_count": 120,
        "segment_object_gens_b64": "...",
        "active_runs": [],
        "retired_runs": []
      },
      "sub": {
        "uploaded_segment_count": 80,
        "segment_object_gens_b64": "...",
        "active_runs": [],
        "retired_runs": []
      }
    }
  }
}
```

Notes:

- `segment_doc_counts_b64`, `segment_min_ts_ms_b64`, and `segment_max_ts_ms_b64` are aligned with the **data uploaded prefix**, not with any family’s own uploaded prefix.
- each family keeps its own `uploaded_segment_count` because sidecars may lag data upload independently;
- `segment_object_gens_b64` is a compressed `u32le` array aligned with that family’s uploaded prefix. It lets cold nodes derive immutable object keys for per-segment companions.

### 9.2 SQLite tables

Add the following tables.

### 9.2.1 `search_state`

```sql
CREATE TABLE IF NOT EXISTS search_state (
  stream TEXT PRIMARY KEY,
  fts_uploaded_segment_count INTEGER NOT NULL DEFAULT 0,
  col_uploaded_segment_count INTEGER NOT NULL DEFAULT 0,
  sub_uploaded_segment_count INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL
);
```

This stores the locally known contiguous uploaded prefixes per family.

### 9.2.2 `search_segment_meta`

```sql
CREATE TABLE IF NOT EXISTS search_segment_meta (
  stream TEXT PRIMARY KEY,
  segment_count INTEGER NOT NULL,
  segment_doc_counts BLOB NOT NULL,
  segment_min_ts_ms BLOB NOT NULL,
  segment_max_ts_ms BLOB NOT NULL
);
```

This is append-only derived metadata parallel to `stream_segment_meta` and aligned by segment index.

### 9.2.3 `search_segments`

```sql
CREATE TABLE IF NOT EXISTS search_segments (
  stream TEXT NOT NULL,
  family TEXT NOT NULL,
  segment_index INTEGER NOT NULL,
  object_gen INTEGER NOT NULL,
  data_local_path TEXT NOT NULL,
  hot_local_path TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  hot_size_bytes INTEGER NOT NULL,
  ready INTEGER NOT NULL DEFAULT 1,
  uploaded_at_ms INTEGER NULL,
  data_etag TEXT NULL,
  hot_etag TEXT NULL,
  PRIMARY KEY (stream, family, segment_index, object_gen)
);

CREATE INDEX IF NOT EXISTS search_segments_pending_upload_idx
  ON search_segments(family, uploaded_at_ms);
```

`ready=0` means the companion build failed or is incomplete; the system must scan that segment for that family.

If multiple generations exist for the same `(stream, family, segment_index)`, the highest uploaded `object_gen` is the authoritative local version. Older generations may remain until a janitor removes them.

### 9.2.4 `search_runs`

```sql
CREATE TABLE IF NOT EXISTS search_runs (
  run_id TEXT PRIMARY KEY,
  stream TEXT NOT NULL,
  family TEXT NOT NULL,
  level INTEGER NOT NULL,
  start_segment INTEGER NOT NULL,
  end_segment INTEGER NOT NULL,
  data_object_key TEXT NOT NULL,
  hot_object_key TEXT NOT NULL,
  field_semantics_digest TEXT NOT NULL,
  retired_gen INTEGER NULL,
  retired_at_ms INTEGER NULL
);

CREATE INDEX IF NOT EXISTS search_runs_stream_idx
  ON search_runs(stream, family, level, start_segment);
```

### 9.2.5 Optional `search_backfills`

```sql
CREATE TABLE IF NOT EXISTS search_backfills (
  stream TEXT NOT NULL,
  family TEXT NOT NULL,
  segment_index INTEGER NOT NULL,
  reason TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  enqueued_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream, family, segment_index)
);
```

This is recommended for historical reindex after mapping changes.

---

## 10. Index build pipeline

## 10.1 High-level flow

1. append request commits to SQLite WAL
2. segmenter selects a stream and builds a sealed data segment
3. during the same background scan, the segmenter also extracts search documents and builds companion search objects
4. data segment and search companions are atomically renamed into place locally
5. local SQLite catalog is updated for the data segment and search companions
6. data uploader uploads the data segment and publishes a new manifest generation for the data prefix
7. search uploader uploads companion objects and publishes new manifest generations for search family prefixes
8. compactor later merges many per-segment companions into larger immutable runs

The append path never waits for any search work.

## 10.2 Search extraction during segmenting

The segmenter already streams WAL rows to build the `.bin` segment. Search extraction must happen in the same pass to avoid a second SQLite scan.

Pseudo-flow for one WAL row:

1. parse JSON payload
2. determine source schema version from stream boundary table
3. build a canonical `SearchDoc`:
   - `timestamp_ms`
   - explicit mapped fields
   - derived normalized fields such as numeric `duration_ms`
   - flattened dynamic attr exact terms
   - flattened dynamic attr presence terms
   - contains-enabled field strings
4. append the row to:
   - data segment builder
   - `.fts` builder
   - `.col` builder
   - `.sub` builder
5. update per-segment search metadata:
   - `doc_count`
   - `min_ts_ms`
   - `max_ts_ms`

### 10.2.1 Canonical `SearchDoc`

The builder should use a single canonical intermediate structure shared by:

- sidecar building
- raw segment scan fallback
- WAL tail scan fallback
- snippet generation support

Recommended TypeScript shape:

```ts
type SearchDoc = {
  localDocId: number;
  streamOffset: bigint;
  timestampMs: number;
  explicitFields: Map<string, SearchValue[]>;
  flattenedExactTerms: Array<{ path: string; canonicalValue: string }>;
  flattenedPresencePaths: string[];
  containsFields: Map<string, string[]>;
};
```

`SearchValue` is one of:

- string
- bigint / number
- boolean
- date millis

### 10.2.2 Dynamic flattening rules

For `dynamic.flattenUnmappedScalars=true`:

- recursively flatten objects to dotted paths;
- arrays of scalars become repeated values at the same path;
- arrays of objects flatten repeated leaf paths without array indices;
- skip nulls;
- skip paths already claimed by explicit mapped fields;
- stop at `maxDepth`;
- skip strings larger than `maxStringBytes` for dynamic exact indexing.

Dynamic exact term encoding in the inverted index:

```txt
<path-bytes> 0x00 <canonical-value-bytes>
```

Dynamic presence term encoding:

```txt
<presence-path-bytes>
```

Use dedicated hidden field IDs, for example:

- `_attrs_exact`
- `_attrs_present`

### 10.2.3 Canonical value encoding for dynamic exact terms

- string → UTF-8 exact bytes
- integer/float → canonical decimal string
- bool → `true` or `false`
- date → epoch milliseconds decimal string

### 10.2.4 Error handling

If search extraction fails for any row in a segment:

- the data segment must still be sealed and committed;
- the affected search companion(s) for that segment/family are marked `ready=0`;
- search falls back to scan for that segment/family;
- emit metrics and log the failure.

Search indexing failure must never block data durability.

## 10.3 Memory bounds and cut policy

The builders must remain memory-bounded.

Add these configuration limits:

- `DS_SEARCH_FTS_BUILD_MAX_BYTES`
- `DS_SEARCH_SUB_BUILD_MAX_BYTES`
- `DS_SEARCH_SEGMENT_TARGET_DOCS`

Rules:

- if the `.fts` builder estimate exceeds `DS_SEARCH_FTS_BUILD_MAX_BYTES`, the segmenter should cut the segment early and seal a smaller segment;
- if the `.sub` builder exceeds `DS_SEARCH_SUB_BUILD_MAX_BYTES`, mark that segment’s `.sub` companion unavailable and rely on scan fallback for `contains:` on that segment;
- `.col` memory is bounded by doc count and fixed-width/value-encoding decisions.

The full-text path is primary enough to justify earlier segment cuts. The substring path is secondary and may degrade to scan per segment.

## 10.4 Local commit sequence

Once the data segment and any search companions are fully written to temp files:

1. rename temp files atomically into final local paths;
2. in one SQLite transaction:
   - insert/update `segments`
   - append to `stream_segment_meta`
   - append to `search_segment_meta`
   - insert `search_segments` rows for ready companions
   - advance `sealed_through`
   - adjust pending counters
3. clear `segment_in_progress`

If a search family failed to build, do not insert a `ready=1` row for that family/version.

---

## 11. Upload and manifest publication

## 11.1 Data upload remains the visibility commit point

The data path is unchanged:

- upload segment bytes
- publish manifest generation referencing the new data prefix
- advance `uploaded_through`
- GC WAL rows below `uploaded_through`

Search upload must never weaken that rule.

## 11.2 Search companion upload

Search companion upload is separate from data upload.

For each ready `search_segments` row:

1. upload `.hot`
2. upload data object (`.fts`, `.col`, or `.sub`)
3. mark companion row uploaded locally
4. recompute the contiguous uploaded prefix for that family
5. publish a new manifest generation containing the updated `search_index` state

The family prefix may only advance up to the stream’s data `uploaded_segment_count`.

A cold node must never see search coverage for a segment that is not yet data-visible.

A local active node may still use locally built companions for sealed-but-not-yet-uploaded segments, because those segments are already visible through the normal local read path. A cold node rebuilt from object storage may use only manifest-visible search state plus raw scans of manifest-visible data segments.

## 11.3 Manifest generation locking

Use the same per-stream manifest publication lock already needed by data upload and routing-key index publication.

Any update that changes the manifest—data upload, search family prefix advance, search compaction, search run retirement—must serialize through that lock.

## 11.4 Search runs and compaction

Per-segment companions make new data searchable immediately. Compaction reduces GET fan-out for older history.

Recommended compaction scheme:

- level 0: per-segment companions (not listed individually as runs)
- level 1: merge 16 contiguous segments
- level 2: merge 16 contiguous level-1 runs (256 segments)
- level 3: merge 16 contiguous level-2 runs (4096 segments)

Configuration:

- `DS_SEARCH_COMPACTION_FANOUT` default `16`
- `DS_SEARCH_MAX_LEVEL` default `4`
- `DS_SEARCH_COMPACTION_CONCURRENCY` default `2`

Each family compacts independently.

### 11.4.1 Run selection

A run is eligible when:

- its input segments/runs are all uploaded and manifest-visible;
- their segment ranges are contiguous;
- they share the same family;
- they share compatible field semantics for the fields included in the run.

### 11.4.2 Run publication

For a compaction output:

1. build run object and `.hot`
2. upload both
3. insert `search_runs` row(s)
4. publish manifest with the new active run
5. mark input runs retired in the manifest and local catalog
6. delete retired run objects after the generation/time safety window

### 11.4.3 L0 per-segment companions

Keep per-segment companions as the authoritative base for v1 and v2.

They are not deleted immediately after compaction. This simplifies bootstrap and correctness. Later GC of L0 companions is possible, but it is not required for the first implementation.

---

## 12. Query planning and execution

## 12.1 Visibility gate

Every search request begins by loading the stream row.

If any of the following is true, return `404`/`gone` before touching search indexes:

- stream does not exist
- stream is deleted
- `expires_at_ms` is in the past

This applies to:

- hit queries
- count-only queries
- aggregations
- snippet generation
- paginated follow-up requests

## 12.2 Snapshot

Search executes over a stable snapshot defined by:

- current `epoch`
- `snapshot_end_offset = next_offset - 1`

Return that snapshot in the response. `search_after` must reuse it.

## 12.3 Family selection by query clause

The planner decomposes the AST into clause groups.

- text clauses → `.fts`
- keyword exact/prefix clauses → `.fts`
- typed range/sort/agg clauses → `.col`
- `contains:` clauses → `.sub`
- unknown dotted paths → `_attrs_exact` / `_attrs_present` in `.fts`

## 12.4 Segment pruning order

Prune in this order:

1. stream TTL/deletion gate
2. data-visible segment range (`uploaded_segment_count` + local sealed tail)
3. manifest-aligned timestamp min/max arrays
4. provider `.hot` field coverage / capability checks
5. family-specific candidate generation
6. raw scan for any uncovered or stale segments

## 12.5 Provider abstraction

The planner should work with a unified provider abstraction.

A provider is one of:

- a compacted run
- a single segment companion
- a raw sealed segment scan provider
- a WAL tail scan provider

Each provider exposes:

- covered segment range
- available families/capabilities
- field coverage metadata
- methods to produce candidate docids or scan docs

Runs are preferred over many segment companions, but both are semantically equivalent.

## 12.6 Authoritative coverage rule

A provider may answer a clause only if:

- the required family is present;
- the field exists in the provider field directory;
- the field semantics epoch matches the current query mapping;
- the required capability bit is present.

If any of those checks fails, the provider is **not authoritative** for that clause and the planner must scan the underlying data for that segment range.

This is the core correctness rule for mixed index coverage.

## 12.7 Candidate generation

### 12.7.1 `.fts`

- exact keyword term → postings for one term
- text term → postings for analyzed token(s)
- boolean combination → postings set algebra
- phrase → candidate intersection, then position verification
- prefix → lexicon range enumeration, capped by `max_prefix_terms`
- `exists` → field presence bitmap
- dynamic attr exact → lookup term in hidden `_attrs_exact` field
- dynamic attr presence → lookup term in hidden `_attrs_present` field

### 12.7.2 `.col`

- numeric/date equality/range → decode column and produce matching docids
- keyword column equality → allowed, but high-cardinality exact matching should prefer `.fts`
- sort → fetch sort values for top candidates or full candidate set as required
- aggs → scan filtered column values

### 12.7.3 `.sub`

- generate normalized trigrams
- intersect postings
- verify candidate docs against hydrated text

## 12.8 Scan fallback

The system must support scan fallback for four cases:

1. WAL tail rows not yet segmented
2. sealed segments without search companions
3. search companions that are stale for a referenced field/capability
4. search companion build/upload failures

The scan path must use the same `SearchDoc` extraction logic as the builder.

Sources for scan:

- WAL tail → SQLite iteration
- sealed local segment → local file scan
- sealed remote segment on cold node → existing segment object load/range-scan path

## 12.9 Merging indexed and scanned results

Merge all hits into one stream-ordered or sort-ordered result set.

Scoring and sorting must be consistent across:

- `.fts`-indexed segments
- `.col`-filtered segments
- `.sub`-verified segments
- scanned segments
- WAL tail

Collection stats for BM25 should include the scanned tail when text clauses touch it.

## 12.10 Snippets

Generate snippets only for the top returned hits.

Algorithm:

1. hydrate source event JSON
2. determine matched field(s)
3. re-tokenize the field text with the configured analyzer
4. extract the best fragment(s)

Do not store character offsets in the index for v1.

## 12.11 Aggregations

Aggregation execution model:

1. execute filter/text query to produce candidate docs
2. for each candidate provider, read the required `.col` fields
3. stream values into aggregator state
4. merge partial states across providers

Initial UI-facing aggs should focus on:

- `level`
- `service`
- `environment`
- `status`
- `method`
- `user.plan`
- `@timestamp` histogram
- `duration_ms` histogram / avg / max

---

## 13. TTL, retention, and stale-data rules

## 13.1 Current TTL model

The current platform TTL is stream-level via `ttl_seconds` / `expires_at_ms`.

That means search can implement:

- **hide immediately**
- **delete later**

Search does **not** need per-document delete bitmaps for current stream TTL semantics.

## 13.2 Visibility rule

If `now >= expires_at_ms` or the stream is deleted:

- return `404` before search planning;
- do not expose hits, counts, snippets, or aggs;
- do not allow `search_after` continuation.

## 13.3 Background cleanup

After expiry:

- local caches may still contain `.hot`, `.fts`, `.col`, `.sub`, and run objects;
- R2 may still contain search objects;
- a janitor later deletes them after a safety window.

Correctness is preserved because serving is gated before index use.

## 13.4 Search workers and expiry

Workers should skip expired streams where practical:

- search companion upload
- search compaction
- backfill

Cancellation is an optimization, not a correctness requirement.

## 13.5 Future row-level delete support

If the platform later adds row-level deletes or per-document retention, search will need delete masks or tombstone bitmaps per segment/run. That is explicitly out of scope for the initial implementation.

---

## 14. Recovery, bootstrap, and caching

## 14.1 Local state is rebuildable

If local SQLite search catalog rows and caches are lost, the system must be reconstructible from:

- `manifest.json`
- search run objects listed in the manifest
- per-segment companion object generations in the manifest
- the underlying data segments

## 14.2 Bootstrap

Extend bootstrap to restore:

- `search_state`
- `search_segment_meta`
- `search_runs`

For per-segment companions, bootstrap does not need individual rows for the full uploaded prefix. It can derive object keys from:

- stream hash
- family
- segment index
- `object_gen` from the manifest array

## 14.3 Caches

Maintain two cache layers for search objects.

1. memory cache
   - decoded `.hot` objects
   - decoded run metadata
2. disk cache
   - raw `.hot` bytes
   - raw run bytes
   - optionally raw per-segment companion bytes

Recommended config:

- `DS_SEARCH_HOT_MEM_CACHE_BYTES`
- `DS_SEARCH_RUN_MEM_CACHE_BYTES`
- `DS_SEARCH_OBJ_CACHE_MAX_BYTES`

Caches are safe to delete.

## 14.4 Object store concurrency

Search must bound concurrent remote GETs.

Recommended config:

- `DS_SEARCH_REMOTE_GET_CONCURRENCY`
- `DS_SEARCH_QUERY_MAX_REMOTE_BYTES`

Do not allow one query to fan out unboundedly across thousands of objects.

---

## 15. Operational guardrails

Add these limits.

- max query size: 1000 hits
- max clauses per query: 64
- max `OR` expansions: 256
- max prefix expansions: 256 by default
- `contains:` minimum normalized length: 3 bytes for indexed path
- track_total_hits default cap: 10,000
- timeout per query: configurable, default 5s

Behavior on over-limit conditions should be explicit `400` or `429`, not silent truncation.

---

## 16. Observability

Add metrics per family and per query path.

### 16.1 Build metrics

- `search.segment_build.latency_ms`
- `search.segment_build.bytes`
- `search.segment_build.failures`
- `search.segment_build.ready_segments`
- `search.backfill.queue_len`

### 16.2 Upload/compaction metrics

- `search.upload.pending_segments`
- `search.upload.prefix_lag_segments`
- `search.run.build.latency_ms`
- `search.run.active`
- `search.run.retired`
- `search.run.gc_deleted`

### 16.3 Query metrics

- `search.query.latency_ms`
- `search.query.remote_gets`
- `search.query.remote_bytes`
- `search.query.indexed_segments`
- `search.query.scanned_segments`
- `search.query.scanned_tail_docs`
- `search.query.candidate_docs`
- `search.query.time_pruned_segments`
- `search.query.contains_verifications`
- `search.query.timeout`

Emit per-stream tags sparingly to avoid cardinality blowups.

---

## 17. Implementation plan in this repository

### 17.1 New modules

Recommended module layout:

```txt
src/search/
  schema.ts
  aliases.ts
  doc_extract.ts
  query_parser.ts
  planner.ts
  searcher.ts
  snippet.ts
  upload_manager.ts
  compactor.ts
  provider.ts
  cache.ts
  families/
    fts_format.ts
    fts_builder.ts
    fts_reader.ts
    col_format.ts
    col_builder.ts
    col_reader.ts
    sub_format.ts
    sub_builder.ts
    sub_reader.ts
```

### 17.2 Existing files to change

- `src/db/schema.ts`
- `src/db/db.ts`
- `src/manifest.ts`
- `src/bootstrap.ts`
- `src/util/stream_paths.ts`
- `src/segment/segmenter.ts`
- `src/uploader.ts`
- `src/app.ts`
- `docs/SCHEMAS.md`
- `docs/INDEX.md` or new `docs/SEARCH_INDEX.md`

### 17.3 New path helpers

Add helpers for:

- `ftsSegmentObjectKey(streamHash, segmentIndex, objectGen)`
- `ftsHotObjectKey(...)`
- `colSegmentObjectKey(...)`
- `subSegmentObjectKey(...)`
- local path equivalents

### 17.4 New HTTP routes

Add:

- `POST /v1/stream/:name/_search`
- optional `GET /v1/stream/:name/_search`
- optional `GET /v1/stream/:name/_search/status`

`_search/status` should return:

- data uploaded segment count
- per-family uploaded segment count
- active runs per family
- lag segments
- last build/upload timestamps

---

## 18. Recommended delivery order

### Phase 1

Ship the minimum useful evlog search stack:

- search schema extension
- `.fts` per-segment companions
- `.col` per-segment companions for timestamp/status/duration/level/service/method
- query parser
- `_search` endpoint
- `search_after` pagination
- WAL tail + raw segment scan fallback
- snippets
- TTL-safe serving gate

This phase already delivers a good evlog UI.

### Phase 2

Add:

- `.sub` for explicit `contains:`
- search run compaction for `.fts` and `.col`
- terms/date histogram/range aggs
- status/debug endpoints

### Phase 3

Add:

- `.sub` compaction
- additional aggs (percentiles/cardinality)
- optional multi-stream federation
- optional L0 companion GC after compaction proves safe

---

## 19. Invariants

These invariants must hold.

1. Search correctness never depends on index coverage.
2. A stream that is expired or deleted must be rejected before any index access.
3. Search family uploaded prefixes must never exceed the data uploaded segment prefix.
4. Every `.hot` file must include field directory entries for every mapped field at build time.
5. A provider may answer a clause only when it is authoritative for that field/capability.
6. Per-segment search companions are immutable objects addressed by `(segment_index, object_gen)`.
7. Runs are immutable objects.
8. Manifest publication is the only remote visibility mechanism.
9. Local caches are disposable and rebuildable from object storage.
10. Search indexing failure must not block segment sealing or data upload.

---

## 20. Test plan

At minimum, add tests for:

1. exact keyword search on uploaded history
2. exact keyword search spanning uploaded history and WAL tail
3. text match + phrase + prefix
4. time range pruning
5. range filter on `status` and `duration_ms`
6. dynamic dotted attr exact filters and `has:`
7. explicit `contains:` with trigram verification
8. segment companion build failure → scan fallback correctness
9. schema evolution where a field path moves between versions
10. search mapping change introducing a new field → old segments scanned for that field until rebuilt
11. stream TTL hides hits immediately
12. crash after search object upload but before manifest publish
13. bootstrap from R2 with no local search catalog
14. compaction run correctness and retired run GC
15. search_after stability with append activity

---

## 21. Final recommendation

The cleanest implementation path is:

- keep writes simple and durable through SQLite WAL only;
- build search companions asynchronously during segment sealing;
- upload them independently of the data path but only advertise them through manifest generations;
- use `.fts` for both full-text and exact-token keyword search;
- use `.col` for range, sort, and aggregations;
- use `.sub` only for explicit `contains:`;
- preserve correctness with raw scan fallback for any range lacking authoritative index coverage.

That gives evlog a search model that matches the product:

- fast fielded search for request context and exact IDs;
- strong full-text on `message`, `why`, and `fix`;
- a deliberate grep-like escape hatch;
- and no requirement that the entire index be local.
