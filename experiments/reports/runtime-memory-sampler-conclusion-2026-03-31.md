# Runtime Memory Sampler Conclusion

Raw timeline and point-in-time samples are in:

- `/Users/sorenschmidt/code/streams/experiments/reports/runtime-memory-sampler-experiments-2026-03-31.md`

## Setup

Three experiments were run with the new runtime sampler enabled:

1. macOS indexed workload
   - stream: `gharchive-sampler-indexed-all`
   - object store: R2
   - sampler: `sampler-indexed*.jsonl`
2. macOS no-index control
   - stream: `gharchive-sampler-noindex-all`
   - same ingest workload, but `--noindex`
   - sampler: `sampler-noindex*.jsonl`
3. Linux indexed workload in Docker
   - stream: `gharchive-sampler-linux-all`
   - same indexed ingest workload
   - sampler: `sampler-linux*.jsonl`

The sampler recorded:

- `process.memoryUsage()`
- `bun:jsc.heapStats()`
- `bun:jsc.memoryUsage()`
- phase labels around `append`, `cut`, `upload`, `routing_l0`, `exact_l0`, and `companion`

## Result

### 1. Plain ingest is not the main problem

The no-index control reached the same order of magnitude of uploaded data as the indexed run and stayed small:

- macOS no-index peak RSS: `417,857,536` bytes
- macOS no-index peak JSC heap size: `53,295,407` bytes
- macOS no-index state after stopping ingest:
  - process RSS stayed around `407,872 KB` in `ps`
  - `19` uploaded segments
  - `0` bundled companions
  - `0` exact indexes
  - `0` search families

That rules out the basic append + segment cut + segment upload path as the cause of the 4-5 GB behavior.

### 2. Indexed/search work is the trigger

The indexed macOS run behaved very differently:

- macOS indexed peak RSS: `5,137,563,648` bytes
- macOS indexed peak JSC heap size: `2,197,030,300` bytes
- after stopping ingest and letting index catch-up finish:
  - stream reached `17` uploaded segments
  - all `10` exact indexes reached `16` segments
  - routing reached `16` segments
  - bundled companions reached `17` objects
  - sampler showed only about `69 MB` live JS heap while RSS stayed at about `5.14 GB`

That is not consistent with "the process still has 5 GB of live JS objects". It is consistent with large transient indexed/search allocations that leave Bun/JSC on macOS holding on to a much larger resident footprint afterward.

### 3. The bundled companion path is the primary amplifier

The sampler showed large growth before the first exact/routing L0 span even existed:

- macOS indexed was already about `4.0 GB` RSS by `11` uploaded segments
- exact/routing L0 work did not start until `16` uploaded segments

This means exact secondary indexing contributes later, but it is not the original source of the blow-up.

The strongest code-side match is bundled companion building in:

- `/Users/sorenschmidt/code/streams/src/search/companion_manager.ts`

Why this path is expensive:

- it builds `col`, `fts`, `agg`, and `mblk` companion state in the same segment pass
- the FTS builder stores large JS maps of strings to postings arrays
- aggregate rollups accumulate nested maps of windows and groups
- section encoding duplicates data again

The concrete duplication points are:

- `/Users/sorenschmidt/code/streams/src/search/companion_manager.ts`
  - builds full in-memory companion objects
- `/Users/sorenschmidt/code/streams/src/search/companion_format.ts`
  - encodes each section separately, then concatenates them into a final bundled payload
- `/Users/sorenschmidt/code/streams/src/search/fts_format.ts`
- `/Users/sorenschmidt/code/streams/src/search/agg_format.ts`
- `/Users/sorenschmidt/code/streams/src/search/col_format.ts`
- `/Users/sorenschmidt/code/streams/src/profiles/metrics/block_format.ts`
  - each section currently goes through `JSON.stringify(...)`, `TextEncoder`, and zstd compression

So the peak can include:

- the live builder objects
- the JSON string
- the encoded section buffer
- the final concatenated `.cix` payload

That is a real memory amplifier even if it is not a permanent logical leak.

### 4. Linux is materially better than macOS

The Linux indexed run was still heavy, but it did not show the same retained-RSS behavior:

- Linux indexed peak sampler RSS: `3,061,227,520` bytes
- Linux indexed peak JSC heap size: `1,952,328,197` bytes
- Linux reached `16` uploaded segments and the first `routing_l0` and `exact_l0` samples
- Linux `ps` RSS before stopping ingest at that point: about `2,025,436 KB`
- Linux `ps` RSS immediately after stopping ingest: about `1,727,116 KB`
- Linux `ps` RSS a bit later in recovery: about `1,264,540 KB`

So Linux still pays a real transient cost for indexed/search work, but it recovers far better than macOS.

## Interpretation

The current evidence supports this model:

1. Streams companion building and, secondarily, exact L0 indexing create very large transient JS object graphs.
2. On macOS, Bun/JavaScriptCore appears to retain a much larger resident footprint after those graphs are freed.
3. On Linux, the same workload is still expensive but the process gives memory back much more readily.

In short:

- this is not primarily a plain ingest leak
- this is not primarily segmenter-worker memory
- this is not primarily "indexes are still working on many segments"
- this is mostly indexed/search transient allocation pressure plus poor RSS recovery on Bun/macOS

## Secondary Finding

Routing index work still occurs even when the stream reports `routing_key_index.configured = false`. That showed up in both indexed and no-index runs. It is not the main memory problem here, but it is wasted work and should be cleaned up.

## Recommendation

Short-term operational recommendation:

- prefer Linux for production workloads that backfill bundled companions and exact indexes
- reduce pressure on macOS with:
  - `DS_SEARCH_COMPANION_BATCH_SEGMENTS=1`
  - `DS_SEARCH_COMPANION_YIELD_BLOCKS=1`
  - `DS_INDEX_BUILD_CONCURRENCY=1` or `2`

Short-term code recommendation:

- stop building routing L0 runs when `routingKey` is not configured
- make background companion/index builders respect memory pressure and pause sooner

Likely structural fix:

- reduce bundled companion peak memory by avoiding "all families in memory plus all encoded payloads at once"
- the most promising direction is to build and encode companion sections more incrementally instead of materializing the entire multi-family companion object, then section JSON, then compressed section buffers, then the final concatenated bundle

## Follow-up Tooling

A targeted Bun API loop harness is available at:

- `/Users/sorenschmidt/code/streams/experiments/bench/bun_api_loop.ts`

It covers:

- `Bun.file`
- `Bun.serve`
- `Bun.spawn`
- `bun:sqlite`
- `Bun.S3Client`
- `Bun.hash.*`
- `Bun.sleep`
- `bun:jsc`

I did not run the full API matrix yet because the three experiments above already isolated the problem enough to prioritize Streams companion/index memory amplification plus Bun/macOS retention behavior.
