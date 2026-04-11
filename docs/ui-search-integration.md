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

## Indexed-Only Serving Contract

`/_search` now has two serving modes:

- indexed-only queries stay on indexed coverage only
- non-index-only queries may still use mixed indexed plus raw-scan serving

Indexed-only queries are positive conjunctions of leaves that are fully handled
by shipped search indexes:

- exact keyword equality through exact-secondary
- typed equality, range, and `has:` through `.col`
- keyword prefix, text, phrase, and text/keyword `has:` through `.fts`

Queries that use `OR`, `NOT`, unary `-`, or other non-index-covered shapes are
not indexed-only.

Important consequence:

- if an indexed-only query runs while exact-secondary or bundled-companion
  coverage is behind, the response simply omits the uncovered newest suffix
- the server does not raw-scan that suffix to fill in the gap
- totals become lower bounds and `coverage.complete` stays `false` until the
  required indexed coverage catches up

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
  "sort": ["offset:desc"]
}
```

## Timeout Handling

`/_search` uses a server-side timeout target of `3000 ms`.

Indexed-only queries default to a lower `200 ms` budget when the request does
not provide `timeout_ms`. This applies to search shapes that can be served
entirely from exact, `.col`, and/or `.fts` indexes.

For mixed exact-plus-typed filters such as `env:"staging" AND duration:1422.027`:

- the server keeps the query on indexed coverage only
- the typed predicate (`duration`) drives the companion-backed candidate set
- the exact predicate (`env`) is still enforced inside that indexed prefix
- the request does not wait on extra exact-secondary planning for the same page

- the request may set `timeout_ms` to a lower value
- values above `3000` are clamped to `3000`
- if `timeout_ms` is omitted:
  - index-capable `/_search` requests default to `200 ms`
  - other search shapes default to `3000 ms`
- the reader checks that deadline cooperatively between work units
- if the budget is exhausted, the server still returns a normal JSON search
  response body instead of hanging the request
  - the HTTP status stays `200`
  - `timed_out: true` and `search-timed-out: true` mark the body as partial
- because timeout checks are cooperative, observed wall time may overshoot the
  configured timeout slightly while an in-flight unit of work completes

Important UI rule:

- `/_search` has two timeout shapes:
  - the normal search timeout shape: `200` with a structured partial-result
    body and `search-timed-out: true`
  - the outer generic resolver timeout shape: `408` with
    `{ "error": { "code": "request_timeout", "message": "request timed out" } }`
- when `search-timed-out: true` is present, treat the response as a structured
  partial result, not as a transport failure
- still parse the JSON body
- still render returned hits
- show that the query timed out and totals are lower bounds
- when the body is the generic `request_timeout` error, show a retry prompt
  instead of trying to render hits from it

Timed-out search responses include:

- body fields:
  - `timed_out`
  - `timeout_ms`
  - `coverage`
  - `total`
  - `hits`
- headers:
  - `search-timed-out`
  - `search-timeout-ms`
  - `search-took-ms`
  - `search-total-relation`
  - `search-coverage-complete`
  - `search-indexed-segments`
  - `search-indexed-segment-time-ms`
  - `search-fts-section-get-ms`
  - `search-fts-decode-ms`
  - `search-fts-clause-estimate-ms`
  - `search-scanned-segments`
  - `search-scanned-segment-time-ms`
  - `search-scanned-tail-docs`
  - `search-scanned-tail-time-ms`
  - `search-exact-candidate-time-ms`
  - `search-index-families-used`

Recommended UI treatment on timeout:

- keep showing the returned hits
- if `timed_out === true` or `search-timed-out: true`, show a banner such as:
  - `Search hit its 3.0s budget. Showing the newest matches found so far.`
- if the body is the generic `request_timeout` error, show a banner such as:
  - `Search request timed out before the server produced a partial result. Try a narrower query and retry.`
- if `total.relation === "gte"`, label totals as a lower bound:
  - `50+ matches`
- expose a retry affordance if the UI wants to rerun with narrower filters
- `/_search` no longer supports request-time exact total-hit counting

Recommended integrator strategy:

- treat `200` plus `timed_out: true` as a successful partial result, not as an
  error response
- treat `408` with
  `{ "error": { "code": "request_timeout", "message": "request timed out" } }`
  as a real request failure because the server did not produce a structured
  search body
- keep one UI state for `complete` results and one for `partial` results; do
  not collapse partial results into the generic error state
- render partial hits immediately and attach inline explanation next to the
  result list rather than replacing the list with a toast or blocking modal
- prefer a narrow banner model:
  - `Showing partial results from the indexed prefix. Totals are lower bounds.`
  - if `coverage.visible_through_primary_timestamp_max` is present, append the
    freshness watermark
- keep the scroll/pagination controls enabled when `next_search_after` is
  present, even if `timed_out === true`
  - the next page request should reuse the same `q`, `sort`, and returned
    `search_after`
- do not auto-retry the same query in a tight loop when `timed_out === true`
  - that just burns the budget repeatedly against the same indexed prefix
- if the user wants more complete results, offer explicit actions instead:
  - narrow the query
  - reduce `size`
  - retry with a larger `timeout_ms`
- keep the generic retry button only for the outer `408 request_timeout` shape

Suggested UI flow:

1. Send `/_search`.
2. If the HTTP status is `200`, always parse and render the body.
3. If `timed_out === true`, mark the page as partial, show the timeout/freshness
   banner, and treat `total.relation === "gte"` as a lower bound.
4. If `next_search_after` is present, allow infinite scroll to continue from the
   returned page.
5. Only show a hard error state when the server returns the generic `408`
   timeout error body or some other non-search failure.

## Infinite Scroll

Use the `next_search_after` value returned by the previous `/_search` response.

Rules:

- keep the same `q`
- keep the same `sort`
- pass `search_after` exactly as returned
- request the next page with the same `size`

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
  "sort": ["offset:desc"]
}
```

Example next page:

```json
{
  "q": "service:checkout status:>=500",
  "size": 100,
  "sort": ["offset:desc"],
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
- for `sort=["offset:desc"]`, the server scans sealed segments from the tail
  and only decodes the blocks needed to fill the current page
- for exact-only `sort=["offset:desc"]` search, the server also resolves the
  exact-secondary candidate set lazily from newest runs backward, so the first
  page does not need to fetch every historical exact run before it can return
- event-time sorts are supported, but they are less efficient for deep
  infinite-scroll pagination
- `/_search` is still not the right mechanism for the unfiltered default event
  list

## Coverage And Freshness

Under active ingest, `/_search` and `/_aggregate` may intentionally omit the
newest suffix instead of scanning it on the request path.

For `/_search`, this is now deliberate for any indexed-only query shape: if the
query can be answered by the shipped indexes, the server keeps it on the
indexed path only and reports the freshness gap through `coverage` instead of
scanning uncovered data.

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
  - newest published segments omitted because the required indexed visibility
    is still catching up
  - for text / column indexed-only search that usually means bundled companions
    are behind
  - for exact-only indexed search that can instead mean the exact-secondary
    family has not indexed the newest published suffix yet
  - for mixed exact plus text or exact plus column search, visibility is still
    limited by the companion families the query requires
  - exact clauses are still enforced inside that visible indexed prefix, but
    they do not trigger a separate exact-secondary visibility expansion
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
- if `timed_out === true` or `search-timed-out: true`, combine the freshness
  banner with a timeout note instead of treating the response as an error page
- do not present missing newest rows for indexed-only queries as an error or a
  retryable fallback case; it is the intended freshness/latency tradeoff

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
- then reissue `GET /v1/stream/{name}/_details?live=long-poll&timeout=5s`
  with `If-None-Match: <etag>`

The server responds:

- `200` with a fresh descriptor when new events arrive or descriptor-visible
  metadata changes
- `304` when the timeout expires with no visible change
- `408` when the generic server-side resolver timeout fires first

Current timeout rule:

- all HTTP resolvers use a cooperative server-side timeout target of `5000 ms`
- keep `/_details` long-poll requests at `<= 5s`
- if the UI gets `408` with
  `{ "error": { "code": "request_timeout", "message": "request timed out" } }`,
  immediately reconnect using the latest `ETag`

This lets a stream page follow `next_offset`, `epoch`, `total_size_bytes`, and
indexing progress without polling the full `/v1/streams` list.

For a stream health or cost popover, the same `/_details` response is also the
supported source of truth:

- `storage.object_storage`
  Uploaded bytes and object counts for segments, indexes, and manifest/schema
  metadata.
- `storage.local_storage`
  Current retained bytes for WAL, pending sealed segments, caches, and the
  shared SQLite footprint. This includes:
  - the local segment read-through cache under `${DS_ROOT}/cache/`
  - the local routing/exact run caches
  - the local lexicon cache under `${DS_ROOT}/cache/lexicon`
  - the local bundled-companion cache under `${DS_ROOT}/cache/companions`
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
