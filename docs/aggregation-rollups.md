# Aggregation Rollups Architecture

Status: implemented baseline

This document defines the object-store-native aggregation and rollup model for
Durable Streams JSON search schemas.

It is designed to fit the existing indexing families:

- exact secondary runs for exact equality pruning
- `.col` companions for typed equality/range
- `.fts` companions for keyword/text search
- `.agg` companions for time-window rollups

The source of truth remains the stream itself. Rollups are accelerators and
remote serving structures, not the durable record store.

## Goals

The rollup system should:

- be schema-owned and generic, not profile-specific
- remain asynchronous with respect to appends
- keep local SQLite bounded
- store durable rollup artifacts in object storage
- support query-time composition when requested time ranges do not align to
  rollup windows
- fit naturally with existing filtered reads and `_search`
- support metric-style summaries like the records emitted to
  `__stream_metrics__`

## Non-goals

The current cut does not try to implement:

- arbitrary cube-style precomputation for every possible dimension set
- multi-stream aggregation
- exact quantile sketches with mergeable t-digest or HDR histogram state
- `.sub` / substring-aware aggregation planning

## Public Model

Rollups are declared under schema `search.rollups`.

Search fields still declare the searchable/filterable field catalog. Rollups
reference those fields and add precomputed time-window aggregation behavior.

Example:

```json
{
  "search": {
    "primaryTimestampField": "timestamp",
    "fields": {
      "timestamp": {
        "kind": "date",
        "bindings": [{ "version": 1, "jsonPointer": "/timestamp" }],
        "exact": true,
        "column": true,
        "sortable": true
      },
      "service": {
        "kind": "keyword",
        "bindings": [{ "version": 1, "jsonPointer": "/service" }],
        "normalizer": "lowercase_v1",
        "exact": true,
        "prefix": true
      },
      "duration": {
        "kind": "float",
        "bindings": [{ "version": 1, "jsonPointer": "/duration" }],
        "exact": true,
        "column": true,
        "aggregatable": true
      }
    },
    "rollups": {
      "latency": {
        "timestampField": "timestamp",
        "dimensions": ["service"],
        "intervals": ["1m", "5m", "1h"],
        "measures": {
          "events": { "kind": "count" },
          "duration": {
            "kind": "summary",
            "field": "duration",
            "histogram": "log2_v1"
          }
        }
      }
    }
  }
}
```

For streams that already carry interval summaries, such as
`__stream_metrics__`, a rollup can merge existing summary parts instead of
recomputing them from a raw scalar field:

```json
{
  "search": {
    "rollups": {
      "metric_windows": {
        "timestampField": "windowStart",
        "dimensions": ["metric", "unit", "stream"],
        "intervals": ["1m", "5m", "1h"],
        "measures": {
          "samples": {
            "kind": "summary_parts",
            "countJsonPointer": "/count",
            "sumJsonPointer": "/sum",
            "minJsonPointer": "/min",
            "maxJsonPointer": "/max",
            "histogramJsonPointer": "/buckets"
          }
        }
      }
    }
  }
}
```

## Measure Semantics

Current measure kinds:

- `count`
  - counts matching records
- `summary`
  - builds a metric-style summary from one numeric field
  - stores `count`, `sum`, `min`, `max`
  - may also store a mergeable histogram using `log2_v1`
- `summary_parts`
  - merges already-aggregated summary fields from each record
  - intended for metric-style interval records such as `__stream_metrics__`
  - stores the same logical state as `summary`

Derived values such as `avg`, `p50`, `p95`, and `p99` are computed at query
time from the stored summary state.

`p50` / `p95` / `p99` are approximate in the current cut. They are derived
from the merged histogram buckets when histogram state is present.

## `.agg` Family

Rollups are stored in a new `.agg` search family.

The first implementation uses **per-segment companions**:

- one immutable bundled `.cix` per uploaded segment
- each current `.cix` may include an `agg` section with all configured rollups
  for that segment
- the `agg` section is a binary `agg2` payload keyed by plan-relative rollup
  and interval ordinals
- each rollup contains one or more configured intervals
- each interval stores sparse time-window buckets in interval-local columnar
  payloads
- each bucket contains one or more dimension groups and measure states

SQLite stores only:

- `search_companion_plans`
- `search_segment_companions` rows whose `sections_json` includes `agg`

This means:

- object store is the durable rollup store
- SQLite only tracks local catalog state
- bootstrap-from-R2 restores bundled companion catalog state from manifests
- query reads load only the requested rollup/interval view instead of decoding
  the whole bundled companion

## Query Surface

Rollups use a dedicated endpoint:

- `POST /v1/stream/{name}/_aggregate`

Current request shape:

- `rollup`: rollup name
- `from`: inclusive start time
- `to`: exclusive end time
- `interval`: one configured rollup interval
- `q`: optional search query string used as a filter
- `group_by`: optional subset of rollup dimensions
- `measures`: optional subset of configured measure names

Current response shape:

- `stream`
- `rollup`
- `from`
- `to`
- `interval`
- `coverage`
- `buckets`

Current coverage fields:

- `used_rollups`
- `indexed_segments`
- `scanned_segments`
- `scanned_tail_docs`
- `index_families_used`

Each response bucket contains:

- `start`
- `end`
- `groups`

Each group contains:

- `key`
- `measures`

## Query-Time Composition

Rollups are only exact when the query can be answered from rollup dimensions
and full rollup windows.

The system therefore splits a query into three parts:

1. **full aligned windows**
   - these can be answered from `.agg` companions
2. **partial edge windows**
   - these are answered by scanning source records
3. **uncovered or stale ranges**
   - these are answered by scanning source records

For example, a `5m` rollup query over:

- `from = 10:03`
- `to = 10:27`

becomes:

- raw scan for `10:03-10:05`
- rollup windows for `10:05-10:25`
- raw scan for `10:25-10:27`

This is the key correctness rule: rollups accelerate aligned middle ranges,
but partial edges and lagging coverage still come from the durable source
stream.

## Eligibility Rules

The current cut uses `.agg` companions only when:

- the requested interval is configured on the rollup
- `group_by` is a subset of the rollup dimensions
- the filter query is either empty or reducible to exact equality filters on
  rollup dimensions

If the filter includes text, prefix, OR, NOT, non-dimension comparisons, or
other unsupported clauses, the server falls back to raw record scans for
correctness.

This keeps the first implementation simple and predictable.

## Metrics-Style Summaries

The metrics stream in this repository is the reference shape for summary
outputs. Rollup summary responses should expose the same high-value fields:

- `count`
- `sum`
- `min`
- `max`
- `avg`
- `p50`
- `p95`
- `p99`

## Current UI Integration

For a GUI, the recommended flow is:

- use `GET /v1/stream/{name}/_details` to discover the current `search.rollups`
  registry and current `.agg` family status
- use `GET /v1/stream/{name}/_index_status` when you want a narrower polling
  endpoint for rollup freshness
- use `POST /v1/stream/{name}/_aggregate` for charts, KPI tiles, and grouped
  summaries
- use `POST /v1/stream/{name}/_search` for the event list and detail drilldown

Interpretation rules:

- if `coverage.used_rollups=true`, the aligned middle portion of the requested
  time range was answered from `.agg`
- if `coverage.scanned_segments > 0` or `coverage.scanned_tail_docs > 0`, the
  server also consulted raw source data for partial edges or uncovered ranges
- `p99`
- `histogram`

That makes rollups suitable for:

- evlog latency panels
- service/status counters over time
- internal operational metrics
- future profile-owned dashboards

## Current Constraints

The current cut intentionally keeps scope narrow:

- only one requested rollup per query
- only one requested interval per query
- no aggregations embedded inside `_search`
- no arbitrary nested group-by expressions
- no server-side rate or derivative calculations

Those can be added later without changing the `.agg` family contract.

## Fit With The Existing Search Architecture

The family split is now:

- exact family: exact equality segment pruning
- `.col`: typed equality/range and sort
- `.fts`: keyword/text search
- `.agg`: time-window aggregations and rollups

This keeps each family narrow and composable instead of creating one oversized
generic index format.
