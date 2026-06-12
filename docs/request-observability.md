# Request Observability API

This document describes the backend API that correlates `evlog` request events
with `otel-traces` spans. It is a query layer over streams, not a stream
profile and not a custom UI.

The API is designed for request detail views, trace waterfall renderers, and
debugging tools that need one response containing:

- the best evlog request event
- matching trace spans
- a parent/child trace tree
- service edges
- errors
- a combined timeline
- search coverage and partial-result warnings

## Endpoint

```http
POST /v1/observe/request
Content-Type: application/json
```

Request:

```json
{
  "streams": {
    "events": "app-events",
    "traces": "app-traces"
  },
  "lookup": {
    "requestId": "req_123"
  },
  "time": {
    "from": "2026-03-27T00:00:00.000Z",
    "to": "2026-03-28T00:00:00.000Z",
    "paddingMs": 5000
  },
  "include": {
    "events": true,
    "trace": true,
    "timeline": true,
    "raw": false
  },
  "limits": {
    "events": 100,
    "spans": 5000
  }
}
```

`lookup` must contain exactly one of:

- `requestId`
- `traceId`
- `spanId`

`streams.events` is required when `include.events` is true. `streams.traces` is
required when `include.trace` is true.

Limits:

- `limits.events`: 1 to 500, default 100
- `limits.spans`: 1 to 10000, default 5000

The implementation pages internally through `_search` because `_search` pages
are capped at 500 hits.

## Pairing Descriptor

Clients should discover request-observability pairs from stream metadata before
calling this endpoint. `GET /v1/streams` and
`GET /v1/stream/{name}/_details` expose `observability.request` when a stream
profile declares its counterpart:

```json
{
  "name": "app-events",
  "profile": "evlog",
  "observability": {
    "request": {
      "events_stream": "app-events",
      "traces_stream": "app-traces"
    }
  }
}
```

For an `evlog` stream, the descriptor comes from
`profile.observability.request.tracesStream`.
For an `otel-traces` stream, it comes from
`profile.observability.request.eventsStream`.

The descriptor is the supported way to choose the counterpart stream. Clients
must not pick the first stream with the opposite profile. If a descriptor is
absent, clients may still call this endpoint with only the active stream and
set the missing side's include flag to false.

## Lookup Behavior

### Request ID

For `{ "lookup": { "requestId": "req_123" } }`:

1. Search the evlog stream with `req:"req_123"`.
2. Extract candidate `traceId`s from matching evlog events.
3. If trace IDs exist, search the trace stream by each `trace:"..."`.
4. If no trace ID is found from evlog, search the trace stream with
   `req:"req_123"`.
5. Build the trace tree and combined timeline.

### Trace ID

For `{ "lookup": { "traceId": "..." } }`:

1. Search the trace stream with `trace:"..."`.
2. Search the evlog stream with `trace:"..."`.
3. Build the same response shape.

### Span ID

For `{ "lookup": { "spanId": "..." } }`:

1. Search the trace stream with `span:"..."`.
2. Extract the trace ID from matching span records.
3. Search the full trace by `trace:"..."`.
4. Search the evlog stream by the resolved trace ID.

If a span lookup cannot resolve a trace ID, the API falls back to searching the
event stream by `span:"..."`.

## Response Shape

The response has this top-level shape:

```json
{
  "lookup": {
    "requestId": "req_123",
    "traceId": "5b8efff798038103d269b633813fc60c",
    "spanId": null
  },
  "summary": {},
  "evlog": {},
  "trace": {},
  "timeline": [],
  "coverage": {
    "events": {},
    "traces": {},
    "warnings": []
  }
}
```

`summary` is a best-effort request header synthesized from evlog first and
root/server span data second. It includes method/path/route/status, service,
environment, duration, start/end time, level, and error fields.

`evlog` is null when `include.events=false`. Otherwise it contains:

- `stream`
- `primary`
- `matches`

The primary event prefers a match with the selected trace ID, otherwise the
first event result.

`trace` is null when `include.trace=false`. Otherwise it contains:

- `stream`
- `traceId`
- `rootSpanId`
- `spans`
- `tree`
- `serviceMap`
- `criticalPath`
- `errors`
- `partial`
- `missingParents`
- `duplicateSpans`

Spans are deduplicated by `traceId:spanId` for the trace view. The underlying
stream remains append-only and keeps duplicate deliveries.

`rootSpanId` is selected from the returned root candidates by scoring likely
request roots first: no parent, server kind, HTTP fields, request ID, and then
duration. Other root spans remain in `trace.tree`; the selected root only
drives summary fields and the highlighted path.

`criticalPath` is an interval-aware highlighted span path that starts at the
selected root when one exists. Child selection uses each subtree's exclusive
time plus its longest descendant contribution, so overlapping sibling spans do
not simply add together.

## Trace Tree

Tree nodes contain:

```json
{
  "spanId": "086e83747d0e381e",
  "parentSpanId": null,
  "children": [],
  "depth": 0,
  "service": "checkout",
  "name": "GET /checkout",
  "kind": "server",
  "startTime": "2026-03-27T10:00:00.000Z",
  "endTime": "2026-03-27T10:00:00.260Z",
  "duration": 260,
  "statusCode": "error"
}
```

Parents are linked by `parentSpanId`. Spans without a parent, or whose parent
was not found in the returned span set, become roots. Missing parent span IDs
are reported in `trace.missingParents`, and `trace.partial` becomes true.

Children are sorted by start time, then duration descending, then name.

## Timeline

The timeline merges profile-owned timeline items:

- `evlog.event`
- `otel.span.start`
- `otel.span.end`
- `otel.span.event`
- `otel.exception`

Each item includes time, title, service, severity, IDs, source stream/profile,
and source data.

This response is intended for custom UI rendering, but no custom UI is shipped
with this feature.

## Coverage

`coverage.events` and `coverage.traces` summarize the `_search` calls used by
the request:

- `searched`
- `complete`
- `timed_out`
- `limit_reached`
- `hits`
- `unique_hits`
- `query_count`
- `batch_count`
- `total`
- `index_families_used`
- `scanned_tail_docs`
- `scanned_segments`
- `possible_missing_events_upper_bound`

`hits`, `unique_hits`, and `total.value` are de-duplicated by stream and offset
across overlapping lookup searches. `query_count` / `batch_count` show how many
underlying `_search` batches were used. `total.relation` is `gte` whenever a
limit, timeout, incomplete coverage, or any underlying lower-bound total means
the exact unique total is not known.

Warnings are emitted for missing evlog events, missing trace spans, hit limits,
incomplete search coverage, and missing parent spans. A UI should surface these
warnings instead of presenting an incomplete response as authoritative.

## Examples

Lookup by request ID:

```json
{
  "streams": {
    "events": "app-events",
    "traces": "app-traces"
  },
  "lookup": {
    "requestId": "req_123"
  }
}
```

Lookup by trace ID:

```json
{
  "streams": {
    "events": "app-events",
    "traces": "app-traces"
  },
  "lookup": {
    "traceId": "5b8efff798038103d269b633813fc60c"
  },
  "limits": {
    "spans": 5000
  }
}
```

Lookup by span ID without event data:

```json
{
  "streams": {
    "traces": "app-traces"
  },
  "lookup": {
    "spanId": "086e83747d0e381e"
  },
  "include": {
    "events": false,
    "trace": true,
    "timeline": true
  }
}
```
