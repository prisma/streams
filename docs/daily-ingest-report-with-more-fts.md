# Daily Ingest Report With More FTS

Date: March 30, 2026

## Goal

Re-enable full-text indexing for the larger GH Archive text fields:

- `title`
- `message`
- `body`

Then rerun the `day` GH Archive demo against the R2-backed full server and
verify that ingest, bundled search backfill, and server responsiveness remain
acceptable.

## Issue Uncovered

With `message` and `body` added back to the demo schema's `.fts` surface, the
first live rerun exposed a real performance problem:

- the server could still append and upload
- but cheap HTTP endpoints like `/v1/streams` and `/_details` could time out
  during heavy bundled companion builds

The underlying issue was architectural. Bundled `.cix` builds were doing too
much work per segment in one uninterrupted turn:

- each enabled section family re-scanned the same segment separately
- each family re-decoded and re-parsed the same JSON records again
- large `.fts` fields made that repeated work expensive enough to monopolize the
  main event loop

## Fix

The bundled companion builder in
[src/search/companion_manager.ts](../src/search/companion_manager.ts)
was changed in two important ways:

1. Single-pass section building

- `.col`, `.fts`, `.agg`, and `.mblk` now share one segment walk
- each record is decoded and parsed once
- all enabled section builders consume that shared parsed record

2. Cooperative yielding

- bundled companion builds now yield every
  `DS_SEARCH_COMPANION_YIELD_BLOCKS` decoded segment blocks
- default: `4`
- this keeps background `.cix` backfill from monopolizing the main process

Additional observability was also added:

- `tieredstore.companion.build.queue_len`
- `tieredstore.companion.builds_inflight`
- `tieredstore.companion.lag.segments`
- `tieredstore.companion.build.latency`
- `tieredstore.companion.objects.built`

## Targeted Regression Coverage

The new targeted regression test is in
[test/companion_backfill.test.ts](../test/companion_backfill.test.ts).

It exercises a GH Archive-like large-text schema and verifies that a bundled
companion build yields back to the event loop before it finishes:

- `title`, `message`, and `body` all indexed as `text`
- small block size to force many decoded blocks
- `searchCompanionYieldBlocks: 1`
- a `setTimeout(0)` sentinel must fire before the build completes

That test would fail on the old uninterrupted build path.

## Validation Before Live Rerun

Verified locally with:

- `bun run typecheck`
- `bun run check:result-policy`
- `bun test`
- `bun test test/conformance.test.ts`

## Live Run

Server config:

- R2-backed full server
- `--auto-tune=4096`
- `DS_ROOT=/tmp/lib/prisma-streams`
- `PORT=8787`

Demo command:

```bash
bun run demo:gharchive day --debug-progress
```

Final ingester output:

```text
GH Archive demo ready
stream: gharchive-demo-day
range: day (2026-03-29-16 -> 2026-03-30-15, requested=24, downloaded=14, missing=10)
rows: 2268757
raw_source_bytes: 2598123271
normalized_bytes: 1137900527
stream_total_size_bytes: 1137900527
avg_ingest_mib_per_s: 1.102
elapsed_ms: 984814
ready: true (uploaded=true, bundled=true, exact=false, search=true)
```

## Final Stream State

From `GET /v1/stream/gharchive-demo-day/_details` at completion:

- `next_offset`: `2268757`
- `segment_count`: `67`
- `uploaded_segment_count`: `67`
- `total_size_bytes`: `1137900527`
- bundled companions: `67/67`
- bundled `.col`: `67/67`
- bundled `.fts`: `67/67`
- bundled `.agg`: `67/67`

The trailing unsealed WAL tail remained:

- `pending_rows`: `29865`
- `pending_bytes`: `13797989`

That is expected. The demo's readiness condition is based on:

- uploaded sealed history
- bundled companion coverage for uploaded segments

It does not wait for the final small tail to be sealed into one more segment.

## Exact Secondary Index Status

Exact secondary indexes were still behind at completion:

- most exact fields: `64/67`

That is expected with the current exact-family design:

- exact runs build in fixed L0 spans
- default span is `16` uploaded segments
- a trailing partial span remains uncovered until enough additional uploaded
  segments exist

This does not block the demo because readiness is intentionally defined around
uploaded data plus bundled search coverage, not exact-family completion.

## What Improved

Compared with the earlier FTS-heavy attempts:

- the day ingest completed successfully with `message` and `body` indexed again
- bundled companion backfill progressed continuously instead of stalling
- `/_details` remained usable during the steady-state ingest/backfill phase
- CPU settled down after startup instead of staying pinned in a pathological
  loop

Most importantly, the bundled search path now behaves like the rest of the
system architecture intends:

- background-only
- bounded
- object-store-native
- correctness-preserving
- fair to foreground HTTP traffic

## Remaining Shortcomings

Two follow-ups remain worth doing:

1. Exact-family ergonomics on recreated/demo streams

- the exact family still shows partial-span lag at the end of demo runs
- that is expected, but it makes exact readiness look worse than bundled search
  readiness

2. Startup warm-up on reused data directories

- when the server starts with an existing `DS_ROOT` that already contains large
  historical streams, there can still be a brief warm-up period while startup
  catch-up overlaps with new ingest
- the system recovered quickly in this run, but there is still room to improve
  cold-start responsiveness under accumulated historical state

## Bottom Line

Re-enabling full-text indexing for the larger GH Archive fields is now viable in
the demo:

- `title`, `message`, and `body` are all indexed
- the day corpus ingested successfully
- bundled search and aggregation coverage reached `67/67`
- the original event-loop starvation issue was fixed by the new single-pass,
  cooperative bundled companion builder
