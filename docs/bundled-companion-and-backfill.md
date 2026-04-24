# Bundled Segment Companions And Async Backfill

Status: implemented

This document defines the current bundled companion model for Prisma Streams.

The supported shape is:

- one immutable raw segment object per sealed segment
- one immutable bundled companion `.cix` per covered segment
- one desired companion plan per stream
- async oldest-missing-first backfill when the desired plan changes

The storage layout for the `.cix` object itself is defined in
[storage-layout-architecture.md](./storage-layout-architecture.md).

## Summary

For a sealed uploaded segment, the steady-state published objects are:

- raw segment object: `streams/<hash>/segments/<segment>.bin`
- bundled companion object: `streams/<hash>/segments/<segment>-<id>.cix`

The `.cix` may contain any subset of:

- `exact`
- `col`
- `fts`
- `agg`
- `mblk`

The exact secondary index family remains separate from `.exact`: secondary
exact runs are compacted cross-segment accelerators, while `.exact` is the
per-segment doc-level postings section.

Decoded section views are cached in memory by companion object key, plan
generation, and section kind. The cache is bounded by
`DS_SEARCH_COMPANION_SECTION_CACHE_BYTES`; raw immutable `.cix` objects remain
managed by the local companion file cache.

## Why Bundle Companions

Bundling keeps the object model small while still allowing family-specific
section codecs.

Benefits:

- one companion PUT per segment instead of one PUT per family
- one per-segment catalog row in SQLite
- one remote object to account for in manifests and `/_details`
- one lazy container that can still decode only the requested family at query
  time

Bundling does not change the source-of-truth rule. Raw segments remain
authoritative and every bundled family is rebuildable from the stream history.

## Desired Companion Plan

Each stream persists one desired bundled companion plan in
`search_companion_plans`.

The desired plan now includes:

- enabled family bits
- stable field ordinals
- stable rollup ordinals
- stable interval ordinals
- stable measure ordinals

The plan is versioned by:

- `generation`
- `plan_hash`
- `plan_json`

When schema or profile changes alter the desired plan, the stream enters mixed
coverage until historical bundled companions are rebuilt for the new
generation.

## Local Catalog

SQLite stores only rebuildable catalog state:

- `search_companion_plans`
- `search_segment_companions`

`search_segment_companions` records:

- `stream`
- `segment_index`
- `object_key`
- `plan_generation`
- `sections_json`
- `section_sizes_json`
- `size_bytes`
- `updated_at_ms`

This is an object catalog, not a local search projection.

## Container

The bundled companion object is now a binary `PSCIX2` container.

Key properties:

- fixed binary header
- fixed section table
- no JSON TOC
- no legacy `PSCIX1` support
- plan-relative family payloads

Query-time reads no longer range-read companion sections directly from remote
object storage.

Current read flow:

1. download the full remote `PSCIX2` object once
2. store it atomically under `${DS_ROOT}/cache/companions`
3. mmap that local immutable file
4. resolve the requested section from the mapped section table
5. decode only the requested family view

This means:

- one remote GET per cold bundled companion, not one GET for the TOC plus
  another GET per requested section
- repeated section reads reuse the local cached `.cix`
- `.fts` field metadata stays as zero-copy views over the mapped file instead
  of copied heap arrays
- stale local companion files are retired during startup pruning and before new
  cache admissions

Examples:

- an FTS query decodes only the `fts` section
- a typed filter decodes only the `col` section
- an aggregate query loads only the target `agg` interval view

## Build And Publish Flow

For a newly sealed uploaded segment:

1. build the raw `.bin` segment
2. load that segment’s bytes for companion generation
3. parse the segment once
4. extract the union of required search fields once per record
5. feed the enabled family builders from that shared record pass
6. encode each family directly into its binary companion section payload
7. wrap the sections into one `PSCIX2` `.cix`
8. upload the raw segment
9. upload the bundled companion
10. seed the local companion cache with the same immutable `.cix`
11. publish the manifest generation that references both

No uploaded historical object becomes visible until manifest publication.

## Async Backfill

Bundled companion backfill runs when:

- a bundled family is newly enabled
- schema-owned field configuration changes
- rollup definitions change
- the metrics profile toggles `mblk`
- companion generation metadata is missing or stale

Current runtime behavior:

- in-process and timer-driven
- oldest-missing-first across the uploaded prefix
- bounded by `DS_SEARCH_COMPANION_BATCH_SEGMENTS`
- cooperative via `DS_SEARCH_COMPANION_YIELD_BLOCKS`
- local companion cache admissions bounded by
  `DS_SEARCH_COMPANION_FILE_CACHE_MAX_BYTES`
- shares the top-level async-index concurrency gate with routing and exact
  background work
- may run with fewer async-index permits while the memory-pressure threshold is
  exceeded, but never below one permit
- one replacement `.cix` per rebuilt segment
- one manifest publish after each successful rebuild batch

Queries remain correct during backfill because uncovered or stale historical
ranges fall back to raw segment and WAL-tail scans.

## Mixed Coverage Rules

Queries treat bundled sections as optional accelerators.

Planning rules:

1. use a current bundled section when it is present for the desired plan
2. otherwise raw-scan the sealed segment
3. read the unsealed WAL tail from SQLite directly only when the published
   prefix is otherwise caught up and the tail passes the quiet-overlay gate

That means:

- new search fields become queryable immediately
- new rollups become queryable immediately
- exactness comes from fallback, not from waiting for historical rebuild to
  finish

The management endpoints that surface this state are:

- `GET /v1/stream/{name}/_index_status`
- `GET /v1/stream/{name}/_details`

Relevant fields include:

- `desired_index_plan_generation`
- `bundled_companions`
- `search_families`

## Exact Secondary Indexes

The exact secondary family is intentionally not part of `.cix`.

It remains:

- schema-driven
- cross-segment
- compacted into separate run objects

Exact build is lower priority than bundled companions:

- bundled companions must catch up first
- the stream must not have an in-progress cut or pending upload segment
- the stream must be append-idle before exact build or compaction is allowed to
  resume

This keeps active ingest focused on raw publish plus bundled-family coverage.

## Current Limits

The current implementation does not yet provide:

- cross-segment `.col` compaction
- cross-segment `.fts` compaction
- cross-segment `.agg` compaction
- background GC for orphaned old companion generations

Those are future optimizations. They are not required for correctness of the
current bundled companion model.

## Bottom Line

The supported model is now:

- `PSCIX2` bundled companions only
- one current `.cix` per covered segment
- one desired plan with plan-relative ordinals per stream
- query-time lazy family decode over a local mmap-backed companion cache
- async oldest-missing-first backfill
- raw fallback whenever bundled coverage is missing or stale
