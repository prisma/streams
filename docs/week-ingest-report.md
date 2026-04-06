# GH Archive Week Ingest Report

Date: 2026-03-30

This report records a full `week` run of the GH Archive demo against a live
Prisma Streams server using R2-backed tiered storage.

## Setup

Server command:

```bash
DS_ROOT=/tmp/lib/prisma-streams \
PORT=8787 \
DURABLE_STREAMS_R2_ACCOUNT_ID=238119c12adfa204286eff750135c58a \
DURABLE_STREAMS_R2_BUCKET=stream-asia \
DURABLE_STREAMS_R2_ACCESS_KEY_ID=ee7a982d008725082e848bc5b047b7a0 \
DURABLE_STREAMS_R2_SECRET_ACCESS_KEY=1741cb2577a9f400c7a3ccbfdff3e46071ddd029ee06150359827373439fab87 \
DS_LOCAL_BACKLOG_MAX_BYTES=7516192768 \
DS_SEGMENT_CACHE_MAX_BYTES=2147483648 \
DS_INDEX_RUN_CACHE_MAX_BYTES=536870912 \
bun run src/server.ts --object-store r2 --auto-tune=4096
```

Ingest command:

```bash
bun run demo:gharchive week --debug-progress
```

## Final Demo Output

The demo completed with this final summary:

```text
GH Archive demo ready
stream: gharchive-demo-week
range: week (2026-03-23-14 -> 2026-03-30-13, requested=168, downloaded=98, missing=70)
rows: 15839087
raw_source_bytes: 18474228951
normalized_bytes: 8133182054
stream_total_size_bytes: 8133182054
avg_ingest_mib_per_s: 1.466
elapsed_ms: 5292088
ready: true (uploaded=true, bundled=true, exact=false, search=true)
```

## Final Stream State

Collected immediately after completion:

- `next_offset`: `15839087`
- `sealed_through`: `15810678`
- `uploaded_through`: `15810678`
- `uploaded_segment_count`: `484`
- `segment_count`: `484`
- `bundled_companion_count`: `484`
- `pending_rows`: `28408`
- `pending_bytes`: `12794490` bytes (`12.20 MiB`)
- `logical_size_bytes`: `8133182054` bytes (`7756.41 MiB`, `7.57 GiB`)

Interpretation:

- All sealed segments were uploaded.
- All bundled search families (`.col`, `.fts`, `.agg`) caught up fully to the
  uploaded segment prefix.
- The stream still had a small unsealed WAL tail at completion because the demo
  does not force a final seal/upload of the last partial segment.

## Index State

Bundled companions and search families:

- `bundled_companions.object_count = 484`
- `.col`: `covered_segment_count=484`, `stale_segment_count=0`
- `.fts`: `covered_segment_count=484`, `stale_segment_count=0`
- `.agg`: `covered_segment_count=484`, `stale_segment_count=0`

Exact secondary indexes had not fully caught up at demo completion, which is
expected because the demo only waits for uploaded + bundled + search readiness:

- `action`: `464`
- `actorLogin`: `432`
- `eventType`: `416`
- `ghArchiveId`: `416`
- `isBot`: `400`
- `orgLogin`: `464`
- `public`: `384`
- `refType`: `368`
- `repoName`: `448`
- `repoOwner`: `464`

## Observations

- The week ingest completed successfully on the patched indexing runtime.
- Bundled companion coverage stayed healthy throughout the run and no longer
  stalled on sparse segment indexes.
- The previous bundled `.fts` failure mode caused by prototype-like text tokens
  (for example `push` and `constructor`) did not recur.
- `/_details` and `/_index_status` could be slow during heavy ingest, but the
  underlying stream and index state remained healthy and queryable.
- GH Archive availability was incomplete for the requested wall-clock week:
  `98` hourly files were available and `70` returned `404`, which the demo now
  treats as non-fatal missing hours.

## Takeaways

- The current bundled companion system is good enough to sustain a large,
  multi-hour ingest against R2-backed storage without the previous companion
  backfill stall.
- The demo’s `ready` state is a good benchmark for Studio-oriented search
  usability because it waits for uploaded history plus bundled search coverage,
  not just raw append completion.
- If a fully durable end-of-run benchmark is needed, the next improvement should
  be an explicit final seal/upload step for the last partial WAL tail so the
  demo ends with `pending_rows=0`.
