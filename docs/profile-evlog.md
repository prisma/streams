# Evlog Profile

This document defines the v1 `evlog` profile design.

The design is based on evlog's core model of one wide event per request,
structured errors with `why` and `fix`, request-scoped context, trace
correlation, and production-safe logging practices.

## Goals

The v1 profile:

- keep evlog data in ordinary durable streams
- store one canonical JSON event per appended record
- install a canonical schema registry, default search fields, and default
  rollups automatically
- avoid unbounded local SQLite indexing
- preserve exact append-only durability semantics
- support request-centric lookup through the existing routing-key path

The v1 profile does not introduce a separate observability storage engine.

## Stream Contract

`evlog` means:

- stream content type must be `application/json`
- the profile must be installed before the stream has appended data
- appended JSON records are normalized into an evlog canonical envelope
- installing the profile also installs the canonical evlog schema registry and
  default `search` and `search.rollups` config
- the profile provides a default routing key from `requestId`, with `traceId`
  fallback
- reads continue to use the normal durable stream APIs

## Canonical Envelope

Each stored event should use this stable top-level shape:

- `timestamp`
- `level`
- `service`
- `environment`
- `version`
- `region`
- `requestId`
- `traceId`
- `spanId`
- `method`
- `path`
- `status`
- `duration`
- `message`
- `why`
- `fix`
- `link`
- `sampling`
- `redaction`
- `context`

All non-reserved fields are moved into `context`.

`context` is where wide event data stays extensible without letting the profile
surface become open-ended.

## Write Path

V1 uses the existing stream append APIs:

- `PUT /v1/stream/{name}`
- `POST /v1/stream/{name}`

When the stream profile is `evlog`, the profile's JSON-ingest hook:

1. validates that each JSON record is an object
2. normalizes the event into the canonical envelope
3. applies redaction before durable append
4. suggests a routing key from `requestId` or `traceId`

The auto-installed canonical schema validates the normalized evlog record after
profile normalization.

In the supported v1 path, users do not need to install that schema manually.
`POST /v1/stream/{name}/_profile` with `{"profile":{"kind":"evlog"}}`
installs:

- the canonical evlog JSON schema as schema version `1`
- the default evlog `search` field registry
- the default evlog `search.rollups` registry
- the schema-registry object in object storage before manifest publication

## Redaction

V1 redaction is profile-owned and happens before durable append.

Built-in behavior:

- sensitive keys are matched case-insensitively
- matching fields are replaced with a redacted marker
- redaction metadata is stored in `redaction`

Default sensitive keys should include:

- `password`
- `token`
- `secret`
- `authorization`
- `cookie`
- `apiKey`

The profile may allow extending this list.

## Reads And Lookup

V1 does not add a local SQLite observability index.

Instead:

- normal reads use `GET /v1/stream/{name}`
- exact request lookup uses the existing routing-key filter with the derived
  `requestId`
- trace fallback still works for streams where `requestId` is absent

This keeps the durable source of truth in the stream and avoids unbounded local
projection tables.

## Secondary Indexing

Evlog indexing builds on the generic schema-owned `search` system, not on
unbounded local SQLite tables.

The auto-installed evlog `search.fields` are:

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

The auto-installed aliases are:

- `req` -> `requestId`
- `trace` -> `traceId`
- `span` -> `spanId`
- `time` / `ts` -> `timestamp`
- `msg` -> `message`

Those fields are declared in the schema registry and built asynchronously into:

- the internal exact-match family for exact keyword/typed equality pruning
- `.col` per-segment companions for typed equality/range
- `.fts` per-segment companions for keyword exact/prefix and text search
- `.agg` per-segment companions for time-window rollups

The required properties stay the same:

- asynchronous
- rebuildable from the durable stream
- stored as object-store-native index artifacts

This keeps local SQLite bounded and preserves the existing recovery model.

## Schema Relationship

`evlog` now auto-installs and owns a canonical schema registry.

That registry provides:

- schema version `1` for the canonical evlog envelope
- the default evlog `search` config
- the default evlog `search.rollups` config

The profile still owns:

- envelope normalization
- redaction
- routing-key defaults
- future evlog-specific query surfaces

Current evlog query surfaces:

- `GET /v1/stream/{name}?filter=...`
- `POST /v1/stream/{name}/_search`
- `GET /v1/stream/{name}/_search?q=...`
- `POST /v1/stream/{name}/_aggregate`

## UI Integration

An evlog UI should treat `/_search` as the primary list/query surface,
`/_aggregate` as the charting/KPI surface, and the stream read APIs as the
record/detail surface.

Recommended integration flow:

1. Create the stream with `application/json`.
2. Install the `evlog` profile with `POST /v1/stream/{name}/_profile`.
3. Read `GET /v1/stream/{name}/_details` when the UI needs the combined
   stream/profile/schema/index descriptor.
4. Read `GET /v1/stream/{name}/_index_status` for dedicated indexing progress
   and freshness state.
5. Start appending evlog JSON records through the normal stream write path.
6. Use `POST /v1/stream/{name}/_search` for the main event list.
7. Use `POST /v1/stream/{name}/_aggregate` for time-series charts and grouped
   summaries.

No manual `/_schema` call is required for the default evlog UI path.

### Charts And Rollups

Use `POST /v1/stream/{name}/_aggregate` with:

- `rollup` for the configured evlog rollup name
- `from` / `to` for the chart time range
- `interval` for the bucket size
- `q` for optional dimension-compatible filtering
- `group_by` for grouped breakdowns such as `service` or `level`

The response `coverage` tells the UI whether aligned middle windows came from
`.agg` (`used_rollups=true`) and whether the server also had to scan source
segments or the WAL tail for partial edges or uncovered ranges.

### Main List View

Use `POST /v1/stream/{name}/_search` with:

- `q` for fielded search and free text
- `size` for page size
- `sort` for stable ordering
- `search_after` for pagination

Recommended default sort:

```json
["timestamp:desc", "offset:desc"]
```

Recommended default query patterns:

- recent events: `q: "service:checkout"`
- errors: `q: "level:error"`
- slow requests: `q: "duration:>1000"`
- HTTP failures: `q: "status:>=500"`
- request lookup: `q: "req:req_123"`
- trace lookup: `q: "trace:trace_123"`
- free text: `q: "card declined"`
- structured text: `q: "service:checkout why:\"issuer declined\""`

Use `next_search_after` from the previous page for infinite scroll or cursor
pagination.

### Detail View

Use `hit.source` from `_search` for the full canonical evlog event. It already
contains:

- top-level correlation fields such as `requestId`, `traceId`, and `spanId`
- structured error fields such as `why`, `fix`, and `link`
- redaction metadata
- full wide-event spillover under `context`

If the UI wants a deterministic raw stream read instead of cached search hits,
it can also use:

- `GET /v1/stream/{name}?format=json&key=<requestId>`
- `GET /v1/stream/{name}?format=json&key=<traceId>`
- `GET /v1/stream/{name}/_details` for the full stream/profile/schema/index
  descriptor

### Export And Cursor Walks

Use `GET /v1/stream/{name}?filter=...` when the UI needs cursor-oriented stream
reads instead of ranked search results. This is a good fit for:

- export jobs
- deterministic background scans
- point-in-time stream walkers

Typical evlog filter examples:

- `service:checkout status:>=400`
- `requestId:req_123`
- `traceId:trace_123`
- `timestamp:>=\"2026-03-25T00:00:00.000Z\"`

### Coverage And Freshness

The UI should read these `_search` response fields:

- `snapshot_end_offset`: the consistent stream snapshot that search covered
- `coverage.index_families_used`: which families answered the query
- `coverage.scanned_segments`: sealed segments that had to fall back to source
  scans
- `coverage.scanned_tail_docs`: unsealed WAL-tail docs scanned for correctness

That lets the UI distinguish:

- fully indexed historical queries
- mixed indexed-plus-tail queries
- cold or rebuilding search coverage

For a persistent management view, `GET /v1/stream/{name}/_index_status`
reports:

- total and uploaded segment counts
- manifest generation and last uploaded manifest metadata
- routing-key index state
- exact secondary-index state per logical field
- `.col` and `.fts` family progress and covered fields

The combined `GET /v1/stream/{name}/_details` endpoint includes the same
`index_status` payload together with the stream summary, full profile resource,
and full schema registry. That is the easiest single-call entry point for an
evlog UI's stream settings or diagnostics panel.

## Deferred Work

Not in the v1 cut:

- native `POST /v1/evlog`
- OTLP `/v1/logs`
- richer profile-owned query endpoints

Those can be added later through the same profile hook model without changing
the durable stream core.
