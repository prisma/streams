# UI Search Integration

This document describes the supported way to build a stream event-list UI with:

- indexed filtering
- chronological ordering
- infinite scroll pagination
- conditional filter controls based on stream capabilities

## Use The Plain Stream Read For Unfiltered Browsing

For the default event list with no active search or filter, use:

- `GET /v1/stream/{name}`

This is the fast path for chronological browsing. It is a stream cursor read,
not a search query, so it can return the next page without evaluating or
sorting the whole candidate set.

Use `/_search` only after the user has actually applied a query or filter.

## Use `/_search` For Filtered Event Lists

For a user-facing filtered event list, use:

- `POST /v1/stream/{name}/_search`
- or `GET /v1/stream/{name}/_search?q=...`

Do **not** use `GET /v1/stream/{name}?filter=...` as the main event-list
surface.

Why:

- `/_search` is the full indexed query surface
- it supports exact, prefix, range, `has:field`, bare text, fielded text, and
  phrase queries
- it supports explicit sorting
- it supports cursor pagination with `search_after`

`GET /v1/stream/{name}?filter=...` is still useful for stream-like cursor walks
and export-style reads, but it is not the primary UI search surface.

Important:

- do not switch the unfiltered default event list onto `/_search`
- `/_search` is optimized for indexed filtering and search semantics, not for
  plain chronological browsing of the whole stream

## Chronological Ordering

For the most efficient filtered event list, sort by append order, not event
time:

- newest first: `["offset:desc"]`
- oldest first: `["offset:asc"]`

This keeps the search order aligned with how the stream is actually stored and
lets the server paginate much more efficiently with `search_after`.

If the UI explicitly wants event-time ordering instead, it may use a sortable
timestamp field plus `offset` as a tie-breaker, but that path is less efficient
for deep infinite-scroll pagination.

Example:

```json
{
  "q": "service:checkout status:>=500 why:\"issuer declined\"",
  "size": 100,
  "sort": ["offset:desc"],
  "track_total_hits": false
}
```

## Infinite Scroll

Use the `next_search_after` value returned by the previous `/_search` response.

Rules:

- keep the same `q`
- keep the same `sort`
- pass `search_after` exactly as returned
- request the next page with the same `size`
- keep `track_total_hits` disabled unless the UI really needs an exact total

For newest-first append-order search, there is no separate `search_before`
mechanism. Use:

- `sort: ["offset:desc"]`
- then pass `next_search_after` from the previous page

That walks backward through append order, which is the efficient infinite-scroll
pattern for a stream event list.

Example first page:

```json
{
  "q": "service:checkout status:>=500",
  "size": 100,
  "sort": ["offset:desc"],
  "track_total_hits": false
}
```

Example next page:

```json
{
  "q": "service:checkout status:>=500",
  "size": 100,
  "sort": ["offset:desc"],
  "track_total_hits": false,
  "search_after": ["0000000000000000000000007Z"]
}
```

Current performance note:

- `/_search` pagination is correct and stable for infinite scroll
- the server supports `search_after`, so the UI can keep scrolling without page
  numbers
- the most efficient path is append-order pagination with
  `sort=["offset:desc"]` or `sort=["offset:asc"]`
- that path can prune by `search_after` before scanning older/newer ranges
- event-time sorts are supported, but they are less efficient for deep
  infinite-scroll pagination
- `/_search` is still not the right mechanism for the unfiltered default event
  list

## Coverage And Freshness

Under active ingest, `/_search` and `/_aggregate` may intentionally omit the
newest suffix instead of scanning it on the request path.

Use the response `coverage` object to drive the UI:

- `complete`
  - `true` means the response includes everything visible at the current stream
    head
  - `false` means the newest suffix was intentionally omitted
- `stream_head_offset`
  - the current append-order head for the request snapshot
- `visible_through_offset`
  - the newest append-order offset included in the response
- `visible_through_primary_timestamp_max`
  - the newest included primary-timestamp value when the stream defines one
- `oldest_omitted_append_at`
  - the append-time watermark where the omitted suffix begins
- `possible_missing_events_upper_bound`
  - an upper bound on newest events that may be omitted
- `possible_missing_uploaded_segments`
  - newest published segments omitted because bundled companions are still
    catching up
- `possible_missing_sealed_rows`
  - newest sealed but not yet published rows omitted from the response
- `possible_missing_wal_rows`
  - newest unsealed WAL rows omitted from the response

Recommended UI treatment:

- render results immediately
- if `coverage.complete === false`, show a subtle freshness banner such as:
  - `Results may exclude up to 26,394 of the newest events while indexing catches up.`
- if `coverage.visible_through_primary_timestamp_max` is present, prefer
  describing freshness in time terms:
  - `Results include data through 2011-03-29T16:59:18Z.`
- if `coverage.oldest_omitted_append_at` is present, show when the omitted
  suffix began:
  - `Newest omitted events started arriving at 2026-04-01T12:57:15Z.`
- treat `total.relation === "gte"` on `/_search` as a lower bound, not an exact
  total

## Query Syntax

The current `q` syntax supports:

- fielded exact keyword queries:
  - `service:checkout`
- fielded keyword prefix queries:
  - `req:req_*`
- typed equality and range queries:
  - `status:>=500`
  - `duration:>1000`
- existence queries:
  - `has:why`
- bare terms over `search.defaultFields`:
  - `timeout`
- fielded text queries:
  - `message:timeout`
- quoted phrase queries on text fields with `positions=true`:
  - `why:"issuer declined"`
- boolean composition:
  - `AND`
  - `OR`
  - `NOT`
  - unary `-`
  - parentheses

Examples:

```text
service:billing-api status:>=500
req:req_*
timeout
why:"issuer declined"
(service:billing-api OR service:worker) NOT status:<500
```

Current non-support:

- `contains:`
- snippets/highlighting
- multi-stream search

## Use `/_details` To Drive The UI

`GET /v1/stream/{name}/_details` is the supported combined descriptor endpoint
for a stream-management or event-list UI.

It returns:

- `stream`
- `profile`
- `schema`
- `index_status`
- `storage`
- `object_store_requests`

That is enough for the UI to decide whether to show filter/search controls and
which controls to render.

For an active stream page, `/_details.stream` also includes the stream head
fields needed for live/tail state:

- `epoch`
- `next_offset`
- `created_at`
- `expires_at`
- `sealed_through`
- `uploaded_through`
- `total_size_bytes`

`/_details` also supports the cheap polling pattern a stream page usually
needs:

- first call `GET /v1/stream/{name}/_details`
- store the returned `ETag`
- then reissue `GET /v1/stream/{name}/_details?live=long-poll&timeout=30s`
  with `If-None-Match: <etag>`

The server responds:

- `200` with a fresh descriptor when new events arrive or descriptor-visible
  metadata changes
- `304` when the timeout expires with no visible change

This lets a stream page follow `next_offset`, `epoch`, `total_size_bytes`, and
indexing progress without polling the full `/v1/streams` list.

For a stream health or cost popover, the same `/_details` response is also the
supported source of truth:

- `storage.object_storage`
  Uploaded bytes and object counts for segments, indexes, and manifest/schema
  metadata.
- `storage.local_storage`
  Current retained bytes for WAL, pending sealed segments, caches, and the
  shared SQLite footprint. This now includes the local bundled-companion cache
  under `${DS_ROOT}/cache/companions`.
- `storage.companion_families`
  Bundled companion byte breakdown for `col`, `fts`, `agg`, and `mblk`.
- `index_status.routing_key_index`, `index_status.exact_indexes[*]`, and
  `index_status.search_families[*]`
  Per-family progress, lag, and bytes-at-rest for index surfaces.
- `object_store_requests`
  Node-local per-stream object-store request counters, including a per-artifact
  breakdown.

The current contract reports lag in `lag_ms`, so a UI can render seconds or
minutes directly. `sqlite_shared_total_bytes` is shared process-local state, so
it should be labeled as shared rather than attributed as fully stream-owned.

## When To Show The Filter UI

Show the full filter/search UI only if:

- `details.schema.search` exists

If `details.schema.search` is absent, treat the stream as not search-enabled
for end-user filtering.

## Which Controls To Show

Use `details.schema.search.fields` to drive the filter builder.

Suggested mapping:

- show exact-match controls for fields with `exact: true`
- show prefix-capable controls for fields with `prefix: true`
- show range controls for fields with `column: true`
- show exists toggles for fields with `exists: true`
- show free-text search if `defaultFields` is non-empty or there is at least
  one field with `kind: "text"`
- show phrase-search help for text fields with `positions: true`
- use `details.schema.search.aliases` to support short field names in advanced
  search UIs

Relevant fields from `details.schema.search`:

- `primaryTimestampField`
- `defaultFields`
- `aliases`
- `fields`

## Indexing Readiness

The stream can be search-capable before every uploaded segment is fully indexed.

Use `details.index_status` to decide whether to show:

- a normal ready state
- an indexing-in-progress banner
- a reduced-capability message

Relevant fields:

- `details.index_status.exact_indexes`
- `details.index_status.search_families`

Useful checks:

- exact filters are fully caught up when the relevant entry in
  `exact_indexes` has `fully_indexed_uploaded_segments: true`
- range queries are fully caught up when the `col` family entry has
  `fully_indexed_uploaded_segments: true`
- keyword/text queries are fully caught up when the `fts` family entry has
  `fully_indexed_uploaded_segments: true`

Even while indexing is still catching up, search remains correct. The server may
scan uncovered published ranges or the WAL tail to preserve correctness.

## Suggested UI Flow

1. Call `GET /v1/stream/{name}/_details`.
2. If `schema.search` is absent, hide the advanced filter/search UI.
3. Build search controls from `schema.search.fields`.
4. Choose the default chronological sort from `primaryTimestampField`, with
   `offset` as the tie-breaker.
5. Issue `POST /v1/stream/{name}/_search` for the event list.
6. Use `next_search_after` for infinite scroll.
7. Use `index_status` to show indexing progress or freshness indicators.

## Practical Recommendation

For a filtered, chronologically ordered, infinitely scrolling event list:

- use `/_search`
- sort by the primary timestamp field plus `offset`
- paginate with `search_after`
- inspect `/_details` to determine whether search is available and which query
  controls to render

That is the supported integration model for stream UIs.
