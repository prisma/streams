# Bundled Segment Companions And Async Backfill

Status: implemented

This document defines the current per-segment search companion architecture.

Prisma Streams now uses one immutable bundled companion object per sealed
segment instead of separate per-family `.col`, `.fts`, `.agg`, and `.mblk`
objects.

It also treats companion rebuild for existing streams as a normal background
operation:

- schema or profile changes can change the desired companion plan
- historical segments can temporarily have mixed coverage
- query correctness is preserved by falling back to raw segment and WAL scans

This design is aligned with the current stream/profile/schema model:

- the stream is the durable source of truth
- the profile owns semantics and profile-specific behavior
- the schema owns payload shape, field extraction, and schema-owned search
  configuration

## Summary

For a sealed historical segment, the normal published object shape is now:

- one raw segment object: `streams/<hash>/segments/<segment>.bin`
- one bundled companion object: `streams/<hash>/segments/<segment>-<id>.cix`

The bundled companion may contain any combination of:

- `col`
- `fts`
- `agg`
- `mblk`

The exact secondary index family remains separate because it is a compacted
cross-segment accelerator, not a per-segment section family.

When the desired companion plan changes for an existing stream, the server:

1. records the new desired plan generation
2. marks older companions as stale for planning purposes
3. backfills the affected segments asynchronously
4. keeps queries correct by using raw fallback where fresh companion coverage is
   missing

## Why Bundle Companions

Without bundling, every new per-segment family adds another object-store PUT and
another per-segment catalog surface.

Bundling keeps the steady-state physical layout small:

- fewer object-store writes per sealed segment
- simpler manifest bookkeeping
- one per-segment object to fetch for search-family planning
- room for future profile-owned section families without multiplying PUT count

This change does not alter the source-of-truth rule. Raw segments remain
authoritative.

## Design Rules

1. Raw segments remain authoritative for historical records.
2. Heavy per-segment derived state is bundled into one immutable `.cix`.
3. Cross-segment accelerators remain separate objects.
4. Companion rebuild is asynchronous and additive.
5. Queries must tolerate mixed coverage.
6. Backfill applies only to derived state that can be recomputed from durable
   stream history.

## Bundled Companion Container

The bundled companion format lives in
[src/search/companion_format.ts](/Users/sorenschmidt/code/streams/src/search/companion_format.ts).

Current structure:

- magic: `PSCIX1`
- TOC length
- JSON TOC near the start of the object
- concatenated section payloads

Current TOC fields:

- `version`
- `stream`
- `segment_index`
- `plan_generation`
- `sections[]`

Each section entry currently carries:

- `kind`
- `offset`
- `length`

Section payloads reuse the existing per-family codecs:

- `.col`
- `.fts`
- `.agg`
- `.mblk`

This keeps family-specific encoding independent while still giving the system
one per-segment companion object.

At query time, the runtime caches the raw `.cix` bytes plus the parsed TOC and
decodes only the requested section family on demand. An FTS read therefore does
not inflate unrelated `col`, `agg`, or `mblk` sections unless that query asks
for them.

## Desired Companion Plan

The server persists one desired companion plan per stream in SQLite and publishes
it in the manifest.

The current plan captures:

- which bundled families are enabled
- the schema-owned field and rollup configuration that affects those families
- profile-specific inclusion such as metrics `mblk`

The current persisted fields are:

- `generation`
- `plan_hash`
- `plan_json`

Whenever the computed desired plan hash changes, the stream enters a mixed
coverage state until historical companions are rebuilt for the new generation.

## Local Catalog

SQLite keeps only small rebuildable catalog state:

- `search_companion_plans`
- `search_segment_companions`

`search_segment_companions` stores:

- `stream`
- `segment_index`
- `object_key`
- `plan_generation`
- `sections_json`
- `updated_at_ms`

This is an object catalog, not a local search projection.

The old per-family companion catalog tables are no longer part of the supported
runtime.

## Manifest And Bootstrap

Published manifest state now includes:

- `search_companions.generation`
- `search_companions.plan_hash`
- `search_companions.plan_json`
- `search_companions.segments[]`

Each segment entry currently includes:

- `segment_index`
- `object_key`
- `plan_generation`
- `sections`

Bootstrap restores:

- the desired companion plan
- the current per-segment companion object catalog

That means a cold-restored node can immediately reuse already-published bundled
companions without rebuilding them locally first.

## Build And Publish Flow

For a newly sealed uploaded segment:

1. build the raw segment
2. build any enabled bundled companion sections from that segment
3. upload the raw segment
4. upload the `.cix`
5. publish the manifest generation that references both

No uploaded historical object becomes visible until manifest publication.

## Async Backfill

Bundled companion backfill runs when:

- a bundled family is newly enabled
- bundled-family field configuration changes
- rollup definitions change
- the metrics profile toggles `mblk`
- companion generation metadata is missing or stale

The current runtime implementation:

- is in-process and timer-driven
- batches a bounded number of uploaded stale segments per tick
- backfills the oldest missing uploaded segments first, so coverage grows
  contiguously instead of sparsely
- loads each segment once, then builds enabled bundled families sequentially so
  `.col`, `.fts`, `.agg`, and `.mblk` do not all retain their heaviest
  in-memory state at the same time
- narrows raw-value extraction per family to the fields that family actually
  needs, and reuses precomputed rollup field values inside aggregate builds
- yields cooperatively every bounded number of segment blocks while building,
  so large `.fts` sections do not monopolize the main event loop
- defers background companion work when the process memory guard is already over
  limit, preferring temporary mixed coverage over pushing the server deeper
  into memory pressure
- rebuilds the full desired section set for each affected segment
- writes one replacement `.cix` per stale segment
- publishes one manifest after each successful batch instead of once per
  replacement object
- periodically sweeps streams that already have companion plans, so backfill
  continues even if a specific enqueue edge is missed

No request path performs synchronous historical rebuild.

## Query Planning With Mixed Coverage

Queries treat bundled sections as optional accelerators.

Planning rules:

1. use a compatible section from the current `.cix` when present
2. otherwise raw-scan the sealed segment
3. always scan the unsealed WAL tail directly

This means a stream can adopt new search fields or rollups immediately, before
historical backfill is finished.

The current management surface exposes this state through:

- `GET /v1/stream/{name}/_index_status`
- `GET /v1/stream/{name}/_details`

Relevant fields include:

- `desired_index_plan_generation`
- `bundled_companions`
- `search_families`

## Exact Secondary Indexes

The exact secondary index family is intentionally not bundled into `.cix`.

It remains:

- schema-driven
- compacted across segments
- stored as separate run objects

It now follows the same catch-up principle as bundled companions:

- exact index config is hashed
- a config mismatch marks the current exact state stale
- exact queries fall back to raw scans until the exact index is rebuilt
- schema updates enqueue background exact-index rebuild automatically
- exact build and compaction also wait until bundled companions are caught up,
  there is no in-progress segment cut or pending upload segment, and the
  stream has been append-idle for about ten minutes, so active ingest keeps its
  memory and CPU budget instead of letting exact work jump back in during long
  but temporary quiet gaps between uploads

## Current Limits

The current implementation does not yet provide:

- cross-segment `.col` compaction
- cross-segment `.fts` compaction
- patch overlays for partial companion rewrites
- bundled-companion range reads from object storage
- background GC of orphaned old `.cix` generations

Operational tuning:

- `DS_SEARCH_COMPANION_BATCH_SEGMENTS` controls how many stale uploaded
  segments one companion-manager pass will rebuild for a stream before it yields
  back to the main loop
- `DS_SEARCH_COMPANION_YIELD_BLOCKS` controls how many decoded segment blocks a
  bundled companion build processes before it yields cooperatively back to the
  event loop (default 4)

Those are future optimizations, not required for correctness.

## Bottom Line

The current supported shape is:

- one raw segment object per sealed segment
- one bundled `.cix` per sealed segment
- separate exact secondary runs for exact-match cross-segment pruning
- async oldest-missing-first backfill for existing streams
- single-pass cooperative bundled companion builds for background fairness
- raw fallback whenever companion or exact coverage is stale or missing

This gives Prisma Streams richer per-segment indexing without turning local
SQLite into a large search store or multiplying object-store PUTs per segment
forever.
