# Prisma Streams Operational Notes

This document describes operational knobs, SQLite settings, and how to diagnose stalls.

## Configuration knobs

Most commonly tuned environment variables are listed below. For the broader
runtime overview and command surface, see `overview.md`.

- `DS_ROOT`: data directory (default `./ds-data`)
- `DS_DB_PATH`: SQLite file path (default `${DS_ROOT}/wal.sqlite`)
- `DS_SEGMENT_MAX_BYTES`: segment seal threshold (default 16 MiB)
- `DS_BLOCK_MAX_BYTES`: max uncompressed bytes per DSB3 block (default 256 KiB)
- `DS_SEGMENT_TARGET_ROWS`: segment seal threshold by row count (default 50k)
- `DS_SEGMENT_MAX_INTERVAL_MS`: max time between segment cuts (default 0; 0 disables time-based sealing)
- `DS_SEGMENT_CHECK_MS`: segmenter tick interval (default 250ms)
- `DS_SEGMENTER_WORKERS`: background segmenter worker threads (default 0)
- `DS_UPLOAD_CHECK_MS`: uploader tick interval (default 250ms)
- `DS_UPLOAD_CONCURRENCY`: max concurrent uploads (default 4)
- `DS_SEGMENT_CACHE_MAX_BYTES`: on-disk segment cache cap (default 256 MiB)
- `DS_INDEX_L0_SPAN`: segments per L0 index run (default 16)
- `DS_INDEX_BUILD_CONCURRENCY`: max parallel async segment-processing tasks inside one exact-family run build (default 4; in-process, not worker threads)
- `DS_INDEX_CHECK_MS`: in-process tick interval for the routing-key, exact secondary, `.col`, `.fts`, and `.agg` index managers (default 1000ms)
- `DS_INDEX_RUN_CACHE_MAX_BYTES`: on-disk index-run cache cap (default 256 MiB)
- `DS_INDEX_RUN_MEM_CACHE_BYTES`: in-memory index-run cache cap (default 64 MiB, auto-tuned when memory limit is set)
- `DS_INDEX_COMPACTION_FANOUT`: compaction fanout (default 16)
- `DS_INDEX_COMPACT_CONCURRENCY`: max parallel async run-loading tasks inside one exact-family compaction job (default 4; in-process, not worker threads)
- `DS_READ_MAX_BYTES`: read response byte cap (default 1 MiB)
- `DS_READ_MAX_RECORDS`: read response record cap (default 1000)
- `DS_APPEND_MAX_BODY_BYTES`: max append body size (default 10 MiB)
- `DS_INGEST_FLUSH_MS`: group-commit flush interval (default 10ms)
- `DS_INGEST_MAX_BATCH_REQS`: max requests per batch (default 200)
- `DS_INGEST_MAX_BATCH_BYTES`: max payload bytes per batch (default 8 MiB)
- `DS_INGEST_MAX_QUEUE_REQS`: max queued append requests (default 50k)
- `DS_INGEST_MAX_QUEUE_BYTES`: max queued append bytes (default 64 MiB)
- `DS_LOCAL_BACKLOG_MAX_BYTES`: backpressure when unuploaded backlog exceeds this (default 10 GiB; 0 disables).
- `DS_SQLITE_CACHE_BYTES` / `DS_SQLITE_CACHE_MB`: SQLite page cache budget (defaults to 25% of `DS_MEMORY_LIMIT_*` when set)
- `DS_MEMORY_LIMIT_MB` / `DS_MEMORY_LIMIT_BYTES`: RSS guard for backpressure (default disabled)
- `DS_OBJECTSTORE_TIMEOUT_MS`: object store request timeout (default 5s)
- `DS_OBJECTSTORE_RETRIES`: object store retry count (default 3)
- `DS_OBJECTSTORE_RETRY_BASE_MS`: base backoff for retries (default 50ms)
- `DS_OBJECTSTORE_RETRY_MAX_MS`: max backoff for retries (default 2s)
- `DS_EXPIRY_SWEEP_MS`: expired stream sweep interval (default 60s; 0 disables)
- `DS_EXPIRY_SWEEP_LIMIT`: expired streams per sweep tick (default 100)
- `DS_METRICS_FLUSH_MS`: metrics flush interval (default 10s; 0 disables)
- `DS_STATS_INTERVAL_MS`: stats log interval when using `--stats` (default 60s)
- `DS_BACKPRESSURE_BUDGET_MS`: per-request queue-wait budget used by stats (default `DS_INGEST_FLUSH_MS + 1`)
- `PORT`: HTTP listen port (default 8080)

MockR2 env vars (only when using `--object-store local`):
- `DS_MOCK_R2_MAX_INMEM_BYTES` / `DS_MOCK_R2_MAX_INMEM_MB`
- `DS_MOCK_R2_SPILL_DIR`

Indexing note:
- Full mode runs indexing in the server process via background timer loops.
- There is no separate indexing daemon or worker-thread pool today.

## SQLite PRAGMAs

Applied on open (see `src/db/schema.ts`):
- `journal_mode=WAL`
- `synchronous=FULL` (default; use `NORMAL` for benchmarks only)
- `foreign_keys=ON`
- `busy_timeout=5000`
- `temp_store=MEMORY`

If you need to cap memory, set SQLite `cache_size` manually at startup.

## Recommended profiles

### Low memory (0.5–1 GB RAM)
- `DS_SEGMENT_MAX_BYTES=8MiB`
- `DS_BLOCK_MAX_BYTES=128KiB`
- `DS_UPLOAD_CONCURRENCY=2`
- `DS_INGEST_MAX_BATCH_BYTES=4MiB`
- `DS_READ_MAX_BYTES=512KiB`
- SQLite `cache_size` around 32–64 MiB

### Medium (1–4 GB RAM)
- `DS_SEGMENT_MAX_BYTES=16MiB`
- `DS_BLOCK_MAX_BYTES=256KiB`
- `DS_UPLOAD_CONCURRENCY=4–8`
- `DS_INGEST_MAX_BATCH_BYTES=8–16MiB`
- `DS_READ_MAX_BYTES=1–4MiB`
- SQLite `cache_size` around 128–256 MiB

## Diagnosing stalls

When throughput drops, check in this order:

1) Ingest queue backlog (append latency spikes)
- Reduce `DS_INGEST_MAX_BATCH_REQS` or increase `DS_INGEST_FLUSH_MS`.

2) Segmenter backlog (pending_bytes high)
- Reduce `DS_SEGMENT_MAX_BYTES` or decrease `DS_SEGMENT_CHECK_MS`.

3) Upload backlog (segments stuck locally)
- Increase `DS_UPLOAD_CONCURRENCY` if network allows.
- Check object store latency and error rates.

4) SQLite write stalls
- Ensure the DB is on fast local SSD.
- Keep `synchronous=FULL` for correctness; `NORMAL` only for benchmarks.

## Recovery tips

- On restart, the server resumes pending segments and uploads from SQLite state.
- Temporary segment files (`*.tmp`) are cleaned up by the segmenter on next run.

## Debug scripts

There are no bundled debug tools yet. Use the SQLite DB (`wal.sqlite`) and
stats logs to inspect state.
