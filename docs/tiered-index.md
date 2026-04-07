# Prisma Streams Tiered Routing-Key Index

Status: **implemented (L0 + L1+ compaction + binary fuse filters)**.

This document describes the tiered routing-key index used by the Prisma
Streams Bun + TypeScript implementation. The index is a secondary structure
that accelerates key-filtered reads without changing the single-log write path.

This document covers the fingerprint-based exact routing-key index. Alphabetic
routing-key enumeration now uses a separate immutable lexicon run family,
documented in [indexing-architecture.md](./indexing-architecture.md). The two
families are complementary:

- the tiered routing-key index answers exact key -> candidate segments
- the routing-key lexicon answers sorted distinct key listing

## Goals

- Make key-filtered reads fast by narrowing the segment scan set.
- Store index runs as immutable objects in R2.
- Keep local state as a cache: if local SQLite and caches are deleted, the
  index can be rebuilt from R2 manifests and run objects.

## High-Level Flow (Read Path)

Reads by routing key are a two-step operation:

1. Consult local SQLite (`index_runs`) to decide which index run files might
   contain the key.
2. Load run files (memory cache -> disk cache -> R2), then compute the set of
   candidate segments for the key and scan only those segments, plus any
   unindexed tail.

This matches the object-storage architecture: SQLite is only a catalog; the
source of truth remains the manifest and run objects in R2.

Unindexed tail segments (segment index >= `indexed_through`) are still scanned
linearly to preserve correctness while indexing catches up.

## Index Runs (L0)

### L0 runs

An L0 run covers 16 consecutive segments by default and stores:

- a sorted list of 64-bit fingerprints (`SipHash-2-4` of routing keys)
- a 16-bit mask per fingerprint, indicating which of the 16 segments contain
  the key

### Run object layout

Runs are stored as immutable objects in R2:

```text
streams/<hash>/index/<run-id>.idx
```

Binary layout (big-endian, compatible with the original Go format):

- Header (36 bytes)
  - magic `"IRN1"`
  - version `1`
  - run_type (`0` = mask16, `1` = postings)
  - level (L0 = `0`)
  - start_segment (u64)
  - end_segment (u64)
  - record_count (u32)
- filter_len (u32) -> binary fuse filter bytes
- data_len (u32)
- Filter bytes (`filter_len`) -> binary fuse filter for fast negative lookup
- Records
  - L0 `mask16`: `(fingerprint u64, mask u16)` repeated `record_count` times
  - L1+ `postings`: `(fingerprint u64, uvarint count, uvarint postings...)`

### Manifest fields

The stream manifest tracks index state:

- `index_secret`: base64-encoded 16-byte SipHash key
- `indexed_through`: highest segment index, exclusive, covered by runs
- `active_runs[]`: list of run metadata (`run_id`, `level`, `start_segment`,
  `end_segment`, `object_key`, `filter_len`, `record_count`)
- `retired_runs[]`: retired runs pending deletion, with `retired_gen` and
  `retired_at_unix`

### Build trigger

After segments are uploaded, the indexer checks whether:

```text
uploaded_segments >= indexed_through + span
```

If true, it builds the next L0 run over that 16-segment window, uploads the
run object, updates `index_state`, and publishes a new manifest generation.

### L1+ compaction

When `compaction_fanout` runs exist at the same level with contiguous segment
ranges, they are merged into a higher-level run:

- L0 -> L1: 16 L0 runs -> 1 L1 run (covers 256 segments by default)
- L1 -> L2: 16 L1 runs -> 1 L2 run (covers 4,096 segments by default)

Higher-level runs use postings lists (relative segment ids) instead of 16-bit
masks. Each run includes a binary fuse filter.

### Retired runs and GC

Compaction retires its input runs:

- they are removed from `active_runs`
- copied into `retired_runs` with `retired_gen` and `retired_at_unix`
- GC deletes retired run objects after the safety window

### Caching

Index runs are cached in two layers:

- Memory cache (decoded runs, shared LRU)
  - `DS_INDEX_RUN_MEM_CACHE_BYTES`
- Disk cache (encoded runs)
  - `DS_INDEX_RUN_CACHE_MAX_BYTES`

The caches are optional and bounded. They are safe to delete.

### Configuration

- `DS_INDEX_L0_SPAN` (default 16)
- `DS_INDEX_BUILDERS` (default 1; auto-tuned by memory preset)
- `DS_INDEX_CHECK_MS` (default 1000)
- `DS_INDEX_RUN_CACHE_MAX_BYTES` (disk cache)
- `DS_INDEX_RUN_MEM_CACHE_BYTES` (memory cache)
- `DS_INDEX_COMPACTION_FANOUT` (default 16)
- `DS_INDEX_MAX_LEVEL` (default 4)
- `DS_INDEX_RETIRE_GEN_WINDOW` (default 2)
- `DS_INDEX_RETIRE_MIN_MS` (default 300000)

### Rebuild from R2

Local index state is a cache. If local SQLite and caches are deleted, the
system can be reconstructed from:

- `streams/<hash>/manifest.json` for index state and run lists
- `streams/<hash>/index/<run-id>.idx` for run objects
