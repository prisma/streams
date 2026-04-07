# Prisma Streams Segment Performance

Status: **informational**. This repo does not ship a dedicated segment perf tool yet.

Current behavior:

- Segment sealing uses a fixed `16 MiB` / `100,000`-row geometry across
  auto-tune presets.
- DSB3 block size is also fixed across auto-tune presets at `1 MiB`, so build
  and read behavior does not vary by memory class.
- The hot block-encoding path now assembles each DSB3 block in a single
  preallocated uncompressed buffer and a single final output buffer. It does
  not allocate one intermediate `Uint8Array` per record and then concatenate
  them before compression.
- Per-block bloom construction now caches the 32-byte probe mask for each
  distinct routing key within the block. Repeated low-cardinality keys therefore
  do not pay the bloom hash again for every record.
- When segment sealing runs inside `DS_SEGMENTER_WORKERS`, the worker-thread
  build loop no longer pays timer-based cooperative yields after every flushed
  block or every `512` rows. The main-thread segmenter path still keeps those
  yields for foreground safety.
- Worker-thread segmenters now treat SQLite as read-mostly. They perform the
  heavy WAL scan, block encode, and local file write themselves, but route
  `tryClaimSegment`, `commitSealedSegment`, and claim release back through the
  main-thread SQLite connection to avoid cross-connection writer lock contention
  with ingest.
- Every committed segment build now records one local SQLite row in
  `segment_build_actions`, including duration, input row count, input payload
  bytes, output segment bytes, start/end offsets, and a build-specific
  `detail_json`.
- The segment-build `detail_json` now breaks out:
  - WAL fetch time
  - row-materialization / per-row loop time
  - block encode time
  - write / fsync / rename time
  - commit time and any explicit busy-retry wait
- The segmenter keeps a trailing window over the latest `8` sealed segments for
  the same stream and computes a cheap compressed/logical ratio from stored
  metadata (`size_bytes` / `payload_bytes`).
- If recent segments compressed below `50%` of the logical target, the next
  segment's logical byte target is raised so the expected compressed segment
  reaches at least `50%` of `DS_SEGMENT_MAX_BYTES`.
- This heuristic is best-effort:
  - it never reduces the logical byte target below `DS_SEGMENT_MAX_BYTES`
  - cut eligibility uses the same raised logical-byte target, so the segmenter
    does not begin cutting early just because backlog crossed the base byte
    threshold
  - it does not decode old segments
  - it still seals immediately when `DS_SEGMENT_TARGET_ROWS` is reached
- The read path uses per-block bloom filters plus a **single forward pass** over
  cached segment bytes for keyed scans.
- For unkeyed offset reads inside sealed segments, the reader now uses the
  segment footer's block index to jump directly to the first block whose
  `firstOffset` can satisfy the request. It does not decode earlier blocks just
  to walk forward to the requested offset.
- For remote segments, the reader now downloads the **full segment object once**, stores it in the local segment cache, and serves block/footer reads from that local file instead of issuing remote range reads.
- Cold remote segment reads issue a direct object GET and treat a missing-object
  GET failure as `null`; they do not perform an existence-probe round trip
  before fetching the segment body.
- A bounded **disk cache** (`DS_SEGMENT_CACHE_MAX_BYTES`) stores recently used segment objects, and keyed reads now populate that cache on first touch instead of walking the remote object block-by-block.
- Hot cached segment files are served from `Bun.mmap()` views, so keyed reads do
  not repeatedly `open()` / `read()` / `close()` tiny footer and block slices.
- If mmap is unavailable for a cached or local segment file, the reader falls
  back to a single full-file byte buffer for that segment scan. It does not
  fall back to repeated tiny local file reads.
- For uploaded keyed reads, the hot path is now: download the segment once into
  the local cache, map the cached file, read the footer trailer once to find the
  data limit, then walk block headers and matching blocks from the mapped bytes.
  The request path no longer performs remote range reads or repeated local
  syscall-heavy range reads.
- For keyed reads with routing-index coverage, the reader now plans the sealed
  segment scan before it starts reading bytes:
  - only indexed candidate segments are scanned from the indexed uploaded prefix
  - the uncovered uploaded tail is scanned sequentially for correctness
  - the request path no longer does one SQLite `findSegmentForOffset()` lookup
    per sealed segment just to skip non-candidate history
- For unversioned JSON streams, HTTP `format=json` responses now reuse stored
  payload bytes directly and assemble the response body with native buffer
  concatenation. The request path no longer decodes and re-serializes each JSON
  record just to emit an array response.
- `since + key` cursor seeks use the same planned sealed-segment scan, so they
  also avoid full indexed-prefix walks when the routing index is selective.
- `_search` uses the same planning idea when exact clauses provide a candidate
  segment set: it scans only candidate indexed segments plus any uncovered tail,
  instead of iterating the full indexed sealed prefix one segment at a time.
- Background routing, lexicon, exact, and bundled-companion builders now yield
  at bounded per-record or per-block intervals and slow down further while a
  foreground read or search is active. That keeps hot keyed reads fast even
  while async indexing is catching up.
- Routing, exact, and lexicon compactions also defer briefly after recent
  foreground traffic, so back-to-back keyed reads do not bounce between fast
  request work and a newly resumed large compaction pass.

Benchmarking:

- `bun run experiments/bench/segment_cached_scan_perf.ts` runs a repeatable
  before/after benchmark against a copied real segment fixture from the remote
  `golden-stream-2` cache.
- The benchmark reports:
  - a legacy syscall-heavy keyed scan baseline over the copied segment
  - the new mapped one-pass keyed scan over the same bytes

If you need a repeatable perf benchmark, use the synthetic ingest benchmark and
measure read latency at the HTTP layer, or build a small custom driver that:

1) Appends a known number of records
2) Forces a segment seal
3) Issues keyed and unkeyed reads
4) Measures response latency and bytes read

For segment-build tuning on a live node, start with:

```sql
SELECT seq, stream, duration_ms, input_count, input_size_bytes, output_size_bytes, detail_json
FROM segment_build_actions
ORDER BY seq DESC
LIMIT 50;
```

For a local reproduction of the worker-thread hot path, run:

```sh
bun test test/segment_build_perf.test.ts --timeout 30000
```

That test benchmarks a representative `simple-1`-shaped segment build against
an embedded legacy baseline and asserts at least a `25%` speedup.
