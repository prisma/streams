# Low-Latency Reads Under Heavy Ingest

Status: proposed future architecture

This document describes the planned architecture for keeping `/_details`,
`/_search`, and `/_aggregate` responsive while heavy ingest and background
indexing are active.

It intentionally describes a **future supported model**, not the current
runtime. The current shipped behavior is documented in
[indexing-architecture.md](./indexing-architecture.md) and
[bundled-companion-and-backfill.md](./bundled-companion-and-backfill.md).

## Problem

Today, Prisma Streams preserves correctness by falling back to raw scans when
bundled search coverage is missing or stale.

That is a good correctness baseline, but it creates two user-visible problems
under heavy ingest:

1. high write volume can increase `/_search` and `/_aggregate` latency because
   the request path may still need to consider the newest unindexed suffix
2. the UI cannot cleanly distinguish between:
   - no matching data
   - no data in the selected time range
   - a partially indexed suffix that might contain additional matching events

For UI workloads, the better trade is usually:

- serve a stable indexed prefix quickly
- omit a small newest suffix when needed
- report exactly how much data might be missing

## Goals

1. High ingest rate should affect indexed reads only minimally.
2. `/_details` must remain cheap even while search companions are being built.
3. `/_search` and `/_aggregate` should use immutable published search state by
   default, not raw-tail recovery scans.
4. The response must report how many events may be missing from the newest
   unindexed window.
5. The UI must be able to show freshness and coverage clearly.
6. The system must remain object-store-native and rebuildable.

## Non-Goals

1. Do not change WAL commit as the write acknowledgment point.
2. Do not make local SQLite a second source of truth.
3. Do not require zero-lag indexing before data becomes queryable.
4. Do not force exact completeness for default UI search requests.

## Core Idea

Future indexed reads should operate on a **published search-visible prefix**
instead of the full stream head.

The system should track two separate frontiers:

- `stream_head_offset`
  The newest durably appended stream offset.
- `search_visible_through_offset`
  The newest offset for which the relevant search families are published and
  queryable.

The gap between them is the **coverage gap**. Default UI reads may omit that
gap in exchange for bounded latency.

This makes the architecture explicit:

- ingest publishes raw durable history
- search builders publish immutable derived state later
- read requests choose between:
  - a fast published-search view
  - a slower completeness-first view

## Query Modes

Future request handling should support two distinct consistency modes.

### 1. `coverage=published`

This should be the default for UI-oriented `/_search` and `/_aggregate`.

Behavior:

- search only the published indexed prefix
- do not raw-scan the newest missing suffix
- return coverage metadata describing the omitted suffix

This mode keeps latency stable under heavy ingest because request work is
bounded by the already-published search snapshot.

### 2. `coverage=complete`

This should remain available for correctness-first tooling and debugging.

Behavior:

- search the published indexed prefix
- fall back to the missing suffix by scanning source segments and/or WAL when
  needed
- return `complete=true`

This mode can stay expensive. It is not the default UI path.

## Search Visibility Model

The system should maintain a **query-family visibility watermark** per stream.

At minimum:

- `col_visible_through_segment`
- `fts_visible_through_segment`
- `agg_visible_through_segment`
- `search_visible_through_segment`
  The minimum usable segment across the families required for the query

For offset-aware reporting, the system should also track:

- `visible_through_offset`
- `visible_through_row_count`
- `visible_through_primary_timestamp_max`

These values must be cheap to read from SQLite and cheap to rebuild from
manifest plus segment metadata.

## Quiet Tail Handling

The published-prefix model must not hide a quiet WAL tail forever.

This is the key edge case:

- new events are appended to WAL
- there are not enough rows or bytes to cut a normal segment
- ingest then goes quiet for minutes or hours

In that situation, those events must still become searchable without waiting for
future traffic.

The preferred future architecture should use exactly one mechanism:

- if there are outstanding sealed segments to upload, bundled companions to
  build, or exact-index runs to build, the request path does **not** search WAL
- if there is **no** outstanding segment or companion work, the request path may
  search the current WAL tail locally as a bounded overlay

This preserves the key product rule:

- active high-rate ingest should not drag request latency upward with repeated
  tail scans
- a quiet sub-segment tail should still become searchable without manifest
  pollution from tiny partial segments

### Why this is the preferred single mechanism

Compared with idle partial sealing, bounded WAL search during quiet periods:

- avoids publishing tiny temporary segments and companions
- avoids fragmenting the manifest with small objects
- keeps one clear durability model: only normal sealed segments are published
- still makes quiet tails searchable once indexing has caught up

### Eligibility rule

The request path may search WAL only when all of the following are true:

- there is no uploaded segment waiting for bundled companion generation
- there is no sealed segment waiting for upload
- there is no pending exact-index build for the query-relevant fields
- the query-required indexed prefix is otherwise caught up

If any of those are false, the request path should search only the published
indexed prefix and report omission metadata.

### Cost envelope

With a `16 MiB` segment size, the quiet-tail WAL overlay is viable if it stays
bounded by that same order of magnitude.

On the live GH Archive node used for this design work, a real WAL-tail scan of
`34,707` rows and `13,995,545` bytes took:

- about `51 ms` to parse every row
- about `60 ms` to parse every row and run a simple text-style match across
  `title`, `message`, and `body`

That is comfortably below the “couple seconds at most” target. The exact number
will vary by hardware and predicate complexity, but the current evidence says a
quiet WAL tail capped near segment size is cheap enough to use as a local
overlay.

## Cheap Omission Accounting

The UI requirement is not “tell me how many matches are missing.”
That would still require reading the suffix.

The requirement is:

- tell me how many events **might** be missing

So the response should return an **upper bound** on omitted events.

That upper bound can be computed cheaply from control-plane state:

- rows in uploaded-but-uncompanioned segments
- rows in sealed-but-unuploaded segments
- rows in the unsealed WAL tail

The system already tracks much of this at the stream level:

- `uploaded_through`
- `sealed_through`
- `pending_rows`
- `uploaded_segment_count`
- `segment_count`

To make query-specific omission accounting cheap, each sealed segment should
also publish a tiny synopsis:

- `segment_index`
- `row_count`
- `min_offset`
- `max_offset`
- `primary_timestamp_min`
- `primary_timestamp_max`

This synopsis belongs in the raw segment manifest metadata and/or the bundled
companion TOC so it can be recovered without scanning payloads.

## Request-Path Behavior

### `GET /v1/stream/{name}/_details`

Future `/_details` should remain a control-plane lookup only.

It should never wait for active indexing to finish.

It should expose:

- `stream_head_offset`
- `search_visible_through_offset`
- per-family visibility
- `possible_missing_events_upper_bound`
- `possible_missing_uploaded_segments`
- `possible_missing_sealed_rows`
- `possible_missing_wal_rows`

### `POST /v1/stream/{name}/_search`

Default path:

1. parse query
2. determine required families
3. choose the published visible prefix for those families
4. if there is no outstanding segment or companion work, optionally search the
   WAL tail locally as a bounded overlay
5. otherwise search only the published prefix
6. return coverage metadata for any omitted suffix

The request path should not wait for the newest segment companion to finish.

### `POST /v1/stream/{name}/_aggregate`

Default path:

1. determine the rollup family and aligned interval requirements
2. use only the published `.agg` prefix
3. if there is no outstanding segment or companion work and the WAL tail is
   still within the bounded overlay budget, optionally include a local WAL
   overlay
4. otherwise omit the missing suffix instead of scanning it
5. return omitted-event metadata and visible time watermark

For UI charts, this is the right trade: slightly stale but fast.

## Response Contract

Future `/_search` and `/_aggregate` responses should expose explicit coverage.

Example shape:

```json
{
  "stream": "gharchive-demo-all",
  "snapshot_end_offset": "000000000000000BTDC0000000",
  "coverage": {
    "mode": "published",
    "complete": false,
    "visible_through_offset": "000000000000000BT9R0000000",
    "visible_through_primary_timestamp_max": "2011-03-10T21:09:59.000Z",
    "stream_head_offset": "000000000000000BTDC0000000",
    "possible_missing_events_upper_bound": 26394,
    "possible_missing_uploaded_segments": 1,
    "possible_missing_sealed_rows": 0,
    "possible_missing_wal_rows": 7952,
    "required_families": ["fts"],
    "index_families_used": ["fts"]
  },
  "hits": []
}
```

Important semantics:

- `possible_missing_events_upper_bound` is an upper bound for omitted events,
  not omitted matches
- `complete=false` means the result excludes the newest suffix on purpose
- `visible_through_primary_timestamp_max` lets the UI explain the freshness of
  the returned data in time terms, not only offset terms

## UI Behavior

This model gives the Studio UI a clean contract:

- render results immediately from the published indexed prefix
- show a subtle freshness banner such as:
  - `Results may exclude up to 26,394 of the newest events while indexing catches up.`
- optionally offer a slower completeness-first retry
- distinguish between:
  - no results in the published prefix
  - no data in the selected time range
  - partial coverage of the newest suffix

For charts:

- the chart can render immediately from the available rollup buckets
- the banner can explain how many newest events are still outside rollup
  coverage

## Isolating Background Work From Reads

Coverage-only semantics are not enough on their own. The indexing system also
needs stronger runtime isolation.

Future implementation should do all of the following:

1. move bundled companion builds out of the HTTP request event loop into a
   dedicated worker/process pool
2. cap concurrent build CPU and memory independently from the request loop
3. publish search-visible state only after a companion batch is durable and
   manifest-visible
4. let the coverage gap grow temporarily under heavy ingest instead of forcing
   reads to pay the catch-up cost

The desired outcome is:

- ingest pressure increases the reported coverage gap
- ingest pressure does not dramatically increase p95 read latency

## Exact Family

The exact secondary family should remain an internal accelerator.

It should not gate the published search-visible prefix for UI queries.

If exact runs lag:

- `/_search` still serves from the bundled `.cix` prefix
- `/_details` reports exact lag separately
- exact rebuild remains opportunistic and asynchronous

## Rollup-Specific Notes

For `.agg`, the published prefix must be tracked per rollup interval family.

At minimum:

- `1m_visible_through_offset`
- `5m_visible_through_offset`
- `1h_visible_through_offset`

This avoids conflating:

- raw searchable coverage
- rollup-ready coverage

For example, `.fts` may be current while `1m` rollups lag by one segment.

## Rollout Plan

### Phase 1: Coverage metadata

- add `coverage.mode`
- add `complete`
- add `possible_missing_events_upper_bound`
- add `visible_through_offset`
- add `visible_through_primary_timestamp_max`

Keep current fallback behavior available behind `coverage=complete`.

### Phase 2: Published-prefix default for UI search

- make `coverage=published` the Studio default
- remove request-path raw-tail scans from the default `/_search` UI path
- remove request-path raw-tail scans from the default `/_aggregate` UI path

### Phase 3: Worker/process isolation

- move bundled companion builds into a dedicated worker pool
- keep the main request loop limited to routing, validation, and immutable
  object reads

### Phase 4: Per-interval `.agg` visibility

- track rollup visibility separately from general search visibility
- let Studio render accurate “rollup freshness” per chart interval

## Bottom Line

The future model should prefer:

- fast reads from a published indexed prefix
- explicit coverage metadata
- clear UI freshness messaging

over:

- slow correctness-first reads that try to recover the newest suffix on every
  request

Under this model, heavy ingest increases the **reported coverage gap**, not the
latency of ordinary UI reads.

The important exception is the quiet-tail case: once ingest pauses long enough,
the tail should converge into published searchable state even if it never grew
large enough for a normal segment cut.
