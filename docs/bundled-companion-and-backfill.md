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

- `col`
- `fts`
- `agg`
- `mblk`

The exact secondary index family remains separate because it is a compacted
cross-segment accelerator, not a per-segment section family.

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

Query-time reads cache raw `.cix` bytes plus the parsed section table and then
decode only the requested family.

Examples:

- an FTS query decodes only the `fts` section
- a typed filter decodes only the `col` section
- an aggregate query loads only the target `agg` interval view

## Build And Publish Flow

For a newly sealed uploaded segment:

1. build the raw `.bin` segment
2. load that segment’s bytes for companion generation
3. build each enabled family in a separate family-specific pass
4. encode each family directly into its binary companion section payload
5. wrap the sections into one `PSCIX2` `.cix`
6. upload the raw segment
7. upload the bundled companion
8. publish the manifest generation that references both

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
- deferred when the memory guard is already over limit
- one replacement `.cix` per rebuilt segment
- one manifest publish after each successful rebuild batch

Queries remain correct during backfill because uncovered or stale historical
ranges fall back to raw segment and WAL-tail scans.

## Mixed Coverage Rules

Queries treat bundled sections as optional accelerators.

Planning rules:

1. use a current bundled section when it is present for the desired plan
2. otherwise raw-scan the sealed segment
3. always read the unsealed WAL tail from SQLite directly

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
- bundled companion range reads from object storage
- background GC for orphaned old companion generations

Those are future optimizations. They are not required for correctness of the
current bundled companion model.

## Bottom Line

The supported model is now:

- `PSCIX2` bundled companions only
- one current `.cix` per covered segment
- one desired plan with plan-relative ordinals per stream
- query-time lazy family decode
- async oldest-missing-first backfill
- raw fallback whenever bundled coverage is missing or stale
