# Prisma Streams Routing-Key Performance

Status: **informational status note**. Dedicated routing-key perf tooling is not
currently maintained in this repo.

Current implementation status:

- Per-block bloom filters + segment footer index are implemented.
- Tier‑2 routing-key index runs (L0 + compaction to higher levels) are implemented.
- Routing-key L0 build compute now runs through the shared generic
  `DS_INDEX_BUILDERS` worker pool; the main process still owns scheduling,
  SQLite state, and manifest publication.
- Routing-key compaction build compute now also runs through the same generic
  worker pool; the main process still owns run retirement, SQLite state, and
  manifest publication.
- Routing-key lexicon L0 build compute now also runs through the same generic
  worker pool; the main process still owns scheduling, SQLite state, and
  manifest publication for the lexicon family.
- Routing-key lexicon compaction compute also runs through that same worker
  pool.
- Each routing-key L0 or routing-key lexicon L0 build depends on the next `16`
  uploaded segment files being available locally. Those cached segment entries are marked
  `required for indexing` until the build finishes, then released.
- Required-for-indexing segment files do not bypass the segment-cache cap. If
  routing or routing-key lexicon backfill falls far enough behind that the next
  16-segment window cannot be retained locally, append admission may return
  `429` with `error.code = "index_building_behind"` while the index backlog
  drains.
- Keyed reads now populate the local segment cache on first touch and scan that
  local file, rather than issuing remote block-range reads against uploaded
  segments.
- Once cached locally, keyed reads use `Bun.mmap()` and a single forward pass
  over block headers / matching blocks instead of reopening the cached segment
  file for many tiny footer and block reads.
- If mmap is unavailable, keyed reads fall back to a single full-file byte
  buffer for the segment scan, not repeated small local file reads.
- When a routing-key tiered index candidate set exists, keyed reads no longer
  walk the full indexed sealed prefix segment-by-segment. They build a sealed
  read plan up front and scan only:
  - the candidate segments from the indexed uploaded prefix
  - the uncovered uploaded tail after `indexed_through`
- Timestamp seeks with `key=...` use the same sealed read plan, so routing-key
  cursor seeks also scale with candidate-set size instead of total sealed
  history.

That means routing-key reads should scale with the size of the candidate set and
the uncovered tail, not with the total number of sealed uploaded segments in the
stream.

For details, see:

- `tiered-index.md`
- `overview.md` (repository overview and benchmark entry points)
- `segment-performance.md`

If you need custom routing-key workload benchmarks, use `experiments/bench/synth.ts`
or build a dedicated driver for your query pattern.

## Local Memory Overhead Test

There is an opt-in local test that compares peak RSS while building:

- one stream with routing key disabled
- one stream with routing key enabled (`routingKey` configured)

Default workload:

- `100 MiB` appended payload per scenario
- `8 MiB` segment target (expected `>=10` segments)
- the test runs `3` repeat pairs by default and compares medians
- variant `build-only`:
  - async routing/lexicon index loops are intentionally deferred so the
    comparison isolates stream build/upload memory
  - default workload is `100 MiB` total with `8 MiB` segment target
  - memory budget assertion: keyed median peak RSS delta can be at most
    `+10 MiB` above the unkeyed median peak RSS delta
- variant `with-indexing`:
  - the stream is built/uploaded first, then routing and lexicon indexing loops
    run; the test requires indexed runs to be built
  - default workload is `100 MiB` total with `1 MiB` segment target (`>=10`
    segments)
  - default indexed segment cache budget is `32 MiB` so the routing worker can
    lease required local windows while building runs
  - memory budget assertion: keyed median **settled RSS delta** (after forced GC)
    can be at most `+50 MiB` above the unkeyed median settled RSS delta

Run:

```bash
bun run test:routing-key-memory
```

Tuning knobs:

- `DS_ROUTING_KEY_MEMORY_TOTAL_BYTES`
- `DS_ROUTING_KEY_MEMORY_SEGMENT_BYTES`
- `DS_ROUTING_KEY_MEMORY_MIN_SEGMENTS`
- `DS_ROUTING_KEY_MEMORY_ALLOWED_EXTRA_BYTES`
- `DS_ROUTING_KEY_MEMORY_INDEXED_TOTAL_BYTES`
- `DS_ROUTING_KEY_MEMORY_INDEXED_SEGMENT_BYTES`
- `DS_ROUTING_KEY_MEMORY_INDEXED_SEGMENT_CACHE_BYTES`
- `DS_ROUTING_KEY_MEMORY_INDEXED_MIN_SEGMENTS`
- `DS_ROUTING_KEY_MEMORY_ALLOWED_EXTRA_BYTES_WITH_INDEXING`
- `DS_ROUTING_KEY_MEMORY_INDEXED_INDEX_SPAN`
- `DS_ROUTING_KEY_MEMORY_REPEATS`
- `DS_ROUTING_KEY_MEMORY_TIMEOUT_MS`
