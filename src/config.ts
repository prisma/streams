import { dsError } from "./util/ds_error.ts";
export type Config = {
  autoTuneRequestedMemoryMb: number | null;
  autoTunePresetMb: number | null;
  autoTuneEffectiveMemoryLimitMb: number | null;
  host: string;
  rootDir: string;
  dbPath: string;
  segmentMaxBytes: number;
  blockMaxBytes: number;
  segmentTargetRows: number;
  segmentMaxIntervalMs: number;
  segmentCheckIntervalMs: number;
  segmenterWorkers: number;
  uploadIntervalMs: number;
  uploadConcurrency: number;
  segmentCacheMaxBytes: number;
  segmentFooterCacheEntries: number;
  indexRunCacheMaxBytes: number;
  indexRunMemoryCacheBytes: number;
  lexiconIndexCacheMaxBytes: number;
  lexiconMappedCacheEntries: number;
  indexL0SpanSegments: number;
  indexBuildConcurrency: number;
  indexCheckIntervalMs: number;
  searchCompanionBuildBatchSegments: number;
  searchCompanionYieldBlocks: number;
  searchCompanionFileCacheMaxBytes: number;
  searchCompanionFileCacheMaxAgeMs: number;
  searchCompanionMappedCacheEntries: number;
  searchCompanionTocCacheBytes: number;
  searchCompanionSectionCacheBytes: number;
  searchWalOverlayQuietPeriodMs: number;
  searchWalOverlayMaxBytes: number;
  indexCompactionFanout: number;
  indexMaxLevel: number;
  indexCompactionConcurrency: number;
  indexRetireGenWindow: number;
  indexRetireMinMs: number;
  readMaxBytes: number;
  readMaxRecords: number;
  appendMaxBodyBytes: number;
  ingestFlushIntervalMs: number;
  ingestMaxBatchRequests: number;
  ingestMaxBatchBytes: number;
  ingestMaxQueueRequests: number;
  ingestMaxQueueBytes: number;
  ingestConcurrency: number;
  ingestBusyTimeoutMs: number;
  localBacklogMaxBytes: number;
  memoryLimitBytes: number;
  sqliteCacheBytes: number;
  workerSqliteCacheBytes: number;
  readConcurrency: number;
  searchConcurrency: number;
  asyncIndexConcurrency: number;
  heapSnapshotPath: string | null;
  memorySamplerPath: string | null;
  memorySamplerIntervalMs: number;
  objectStoreTimeoutMs: number;
  objectStoreRetries: number;
  objectStoreBaseDelayMs: number;
  objectStoreMaxDelayMs: number;
  expirySweepIntervalMs: number;
  expirySweepBatchLimit: number;
  metricsFlushIntervalMs: number;
  touchWorkers: number;
  touchCheckIntervalMs: number;
  touchMaxBatchRows: number;
  touchMaxBatchBytes: number;
  port: number;
};

const KNOWN_DS_ENVS = new Set<string>([
  "DS_ROOT",
  "DS_HOST",
  "DS_DB_PATH",
  "DS_SEGMENT_MAX_BYTES",
  "DS_BLOCK_MAX_BYTES",
  "DS_SEGMENT_TARGET_ROWS",
  "DS_SEGMENT_MAX_INTERVAL_MS",
  "DS_SEGMENT_CHECK_MS",
  "DS_SEGMENTER_WORKERS",
  "DS_UPLOAD_CHECK_MS",
  "DS_UPLOAD_CONCURRENCY",
  "DS_BASE_WAL_GC_CHUNK_OFFSETS",
  "DS_BASE_WAL_GC_INTERVAL_MS",
  "DS_SEGMENT_CACHE_MAX_BYTES",
  "DS_SEGMENT_FOOTER_CACHE_ENTRIES",
  "DS_INDEX_RUN_CACHE_MAX_BYTES",
  "DS_INDEX_RUN_MEM_CACHE_BYTES",
  "DS_LEXICON_INDEX_CACHE_MAX_BYTES",
  "DS_LEXICON_MMAP_CACHE_ENTRIES",
  "DS_INDEX_L0_SPAN",
  "DS_INDEX_BUILD_CONCURRENCY",
  "DS_INDEX_CHECK_MS",
  "DS_SEARCH_COMPANION_BATCH_SEGMENTS",
  "DS_SEARCH_COMPANION_YIELD_BLOCKS",
  "DS_SEARCH_COMPANION_FILE_CACHE_MAX_BYTES",
  "DS_SEARCH_COMPANION_FILE_CACHE_MAX_AGE_MS",
  "DS_SEARCH_COMPANION_MMAP_CACHE_ENTRIES",
  "DS_SEARCH_COMPANION_TOC_CACHE_BYTES",
  "DS_SEARCH_COMPANION_SECTION_CACHE_BYTES",
  "DS_SEARCH_WAL_OVERLAY_QUIET_MS",
  "DS_SEARCH_WAL_OVERLAY_MAX_BYTES",
  "DS_INDEX_COMPACTION_FANOUT",
  "DS_INDEX_MAX_LEVEL",
  "DS_INDEX_COMPACT_CONCURRENCY",
  "DS_INDEX_RETIRE_GEN_WINDOW",
  "DS_INDEX_RETIRE_MIN_MS",
  "DS_READ_MAX_BYTES",
  "DS_READ_MAX_RECORDS",
  "DS_APPEND_MAX_BODY_BYTES",
  "DS_INGEST_FLUSH_MS",
  "DS_INGEST_MAX_BATCH_REQS",
  "DS_INGEST_MAX_BATCH_BYTES",
  "DS_INGEST_MAX_QUEUE_REQS",
  "DS_INGEST_MAX_QUEUE_BYTES",
  "DS_INGEST_CONCURRENCY",
  "DS_INGEST_BUSY_MS",
  "DS_LOCAL_BACKLOG_MAX_BYTES",
  "DS_MEMORY_LIMIT_BYTES",
  "DS_MEMORY_LIMIT_MB",
  "DS_SQLITE_CACHE_BYTES",
  "DS_SQLITE_CACHE_MB",
  "DS_WORKER_SQLITE_CACHE_BYTES",
  "DS_WORKER_SQLITE_CACHE_MB",
  "DS_READ_CONCURRENCY",
  "DS_SEARCH_CONCURRENCY",
  "DS_ASYNC_INDEX_CONCURRENCY",
  "DS_HEAP_SNAPSHOT_PATH",
  "DS_MEMORY_SAMPLER_PATH",
  "DS_MEMORY_SAMPLER_INTERVAL_MS",
  "DS_OBJECTSTORE_TIMEOUT_MS",
  "DS_OBJECTSTORE_RETRIES",
  "DS_OBJECTSTORE_RETRY_BASE_MS",
  "DS_OBJECTSTORE_RETRY_MAX_MS",
  "DS_LOCAL_DATA_ROOT",
  "DS_EXPIRY_SWEEP_MS",
  "DS_EXPIRY_SWEEP_LIMIT",
  "DS_METRICS_FLUSH_MS",
  "DS_TOUCH_WORKERS",
  "DS_TOUCH_CHECK_MS",
  "DS_TOUCH_MAX_BATCH_ROWS",
  "DS_TOUCH_MAX_BATCH_BYTES",
  "DS_AUTO_TUNE_REQUESTED_MB",
  "DS_AUTO_TUNE_PRESET_MB",
  "DS_AUTO_TUNE_EFFECTIVE_MEMORY_LIMIT_MB",
  "DS_STATS_INTERVAL_MS",
  "DS_BACKPRESSURE_BUDGET_MS",
  "DS_MOCK_R2_MAX_INMEM_BYTES",
  "DS_MOCK_R2_MAX_INMEM_MB",
  "DS_MOCK_R2_SPILL_DIR",
  "DS_BENCH_URL",
  "DS_BENCH_DURATION_MS",
  "DS_BENCH_INTERVAL_MS",
  "DS_BENCH_PAYLOAD_BYTES",
  "DS_BENCH_CONCURRENCY",
  "DS_BENCH_REQUEST_TIMEOUT_MS",
  "DS_BENCH_DRAIN_TIMEOUT_MS",
  "DS_BENCH_PAUSE_BACKGROUND",
  "DS_BENCH_YIELD_EVERY",
  "DS_BENCH_DEBUG",
  "DS_BENCH_SCENARIOS",
  "DS_MEMORY_STRESS_LIMITS_MB",
  "DS_MEMORY_STRESS_STATS_MS",
  "DS_MEMORY_STRESS_PORT_BASE",
  "DS_RK_EVENTS_MAX",
  "DS_RK_EVENTS_STEP",
  "DS_RK_PAYLOAD_BYTES",
  "DS_RK_APPEND_BATCH",
  "DS_RK_KEYS",
  "DS_RK_HOT_KEYS",
  "DS_RK_HOT_PCT",
  "DS_RK_PAYLOAD_POOL",
  "DS_RK_READ_ENTRIES",
  "DS_RK_WARM_READS",
  "DS_RK_SEGMENT_BYTES",
  "DS_RK_BLOCK_BYTES",
  "DS_RK_SEED",
  "DS_RK_R2_GET_DELAY_MS",
  "DS_LARGE_INDEX_FILTER",
  "DS_LARGE_INDEX_FILTER_TOTAL_BYTES",
  "DS_LARGE_INDEX_FILTER_PAYLOAD_BYTES",
  "DS_LARGE_INDEX_FILTER_BATCH_ROWS",
  "DS_LARGE_INDEX_FILTER_SEGMENT_BYTES",
  "DS_LARGE_INDEX_FILTER_INDEX_SPAN",
  "DS_LARGE_INDEX_FILTER_TIMEOUT_MS",
  "DS_LARGE_INDEX_FILTER_R2_MAX_INMEM_BYTES",
]);

let warnedUnknownEnv = false;

function warnUnknownEnv(): void {
  if (warnedUnknownEnv) return;
  warnedUnknownEnv = true;
  const unknown: string[] = [];
  for (const key of Object.keys(process.env)) {
    if (!key.startsWith("DS_")) continue;
    if (KNOWN_DS_ENVS.has(key)) continue;
    unknown.push(key);
  }
  if (unknown.length > 0) {
    unknown.sort();
    console.warn(`[config] unknown DS_* environment variables: ${unknown.join(", ")}`);
  }
}

function envNum(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = Number(v);
  if (!Number.isFinite(n)) throw dsError(`invalid ${name}: ${v}`);
  return n;
}

function envBytes(name: string): number | null {
  const v = process.env[name];
  if (!v) return null;
  const n = Number(v);
  if (!Number.isFinite(n)) throw dsError(`invalid ${name}: ${v}`);
  return Math.max(0, Math.floor(n));
}

function clampBytes(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  if (value < min) return min;
  if (value > max) return max;
  return value;
}

export function loadConfig(): Config {
  warnUnknownEnv();
  const rootDir = process.env.DS_ROOT ?? "./ds-data";
  const host = process.env.DS_HOST?.trim() || "127.0.0.1";
  const autoTuneRequestedMemoryMb = envBytes("DS_AUTO_TUNE_REQUESTED_MB");
  const autoTunePresetMb = envBytes("DS_AUTO_TUNE_PRESET_MB");
  const autoTuneEffectiveMemoryLimitMb = envBytes("DS_AUTO_TUNE_EFFECTIVE_MEMORY_LIMIT_MB");
  const bytesOverride = envBytes("DS_MEMORY_LIMIT_BYTES");
  const mbOverride = envBytes("DS_MEMORY_LIMIT_MB");
  const memoryLimitBytes = bytesOverride ?? (mbOverride != null ? mbOverride * 1024 * 1024 : 0);
  const backlogOverride = envBytes("DS_LOCAL_BACKLOG_MAX_BYTES");
  const sqliteCacheBytesOverride = envBytes("DS_SQLITE_CACHE_BYTES");
  const sqliteCacheMbOverride = envBytes("DS_SQLITE_CACHE_MB");
  const workerSqliteCacheBytesOverride = envBytes("DS_WORKER_SQLITE_CACHE_BYTES");
  const workerSqliteCacheMbOverride = envBytes("DS_WORKER_SQLITE_CACHE_MB");
  const indexMemOverride = envBytes("DS_INDEX_RUN_MEM_CACHE_BYTES");
  const indexDiskOverride = envBytes("DS_INDEX_RUN_CACHE_MAX_BYTES");
  const lexiconDiskOverride = envBytes("DS_LEXICON_INDEX_CACHE_MAX_BYTES");
  const localBacklogMaxBytes = backlogOverride ?? 10 * 1024 * 1024 * 1024;
  const sqliteCacheBytes =
    sqliteCacheBytesOverride ??
    (sqliteCacheMbOverride != null
      ? sqliteCacheMbOverride * 1024 * 1024
      : memoryLimitBytes > 0
        ? Math.floor(memoryLimitBytes * 0.25)
        : 0);
  const workerSqliteCacheBytes =
    workerSqliteCacheBytesOverride ??
    (workerSqliteCacheMbOverride != null
      ? workerSqliteCacheMbOverride * 1024 * 1024
      : sqliteCacheBytes > 0
        ? clampBytes(Math.floor(sqliteCacheBytes / 8), 8 * 1024 * 1024, 32 * 1024 * 1024)
        : 0);
  const tunedIndexMem =
    indexMemOverride ??
    (memoryLimitBytes > 0
      ? clampBytes(Math.floor(memoryLimitBytes * 0.05), 8 * 1024 * 1024, 128 * 1024 * 1024)
      : 64 * 1024 * 1024);
  const companionSectionCacheBytes =
    envBytes("DS_SEARCH_COMPANION_SECTION_CACHE_BYTES") ??
    (memoryLimitBytes > 0
      ? clampBytes(Math.floor(memoryLimitBytes * 0.02), 8 * 1024 * 1024, 128 * 1024 * 1024)
      : 32 * 1024 * 1024);
  const companionFileCacheBytes =
    envBytes("DS_SEARCH_COMPANION_FILE_CACHE_MAX_BYTES") ??
    clampBytes(Math.max(512 * 1024 * 1024, Math.floor(localBacklogMaxBytes * 0.1)), 256 * 1024 * 1024, 4 * 1024 * 1024 * 1024);
  const segmentMaxBytes = envNum("DS_SEGMENT_MAX_BYTES", 16 * 1024 * 1024);
  const searchWalOverlayMaxBytes = envBytes("DS_SEARCH_WAL_OVERLAY_MAX_BYTES") ?? segmentMaxBytes;
  return {
    autoTuneRequestedMemoryMb,
    autoTunePresetMb,
    autoTuneEffectiveMemoryLimitMb,
    host,
    rootDir,
    dbPath: process.env.DS_DB_PATH ?? `${rootDir}/wal.sqlite`,
    segmentMaxBytes,
    blockMaxBytes: envNum("DS_BLOCK_MAX_BYTES", 256 * 1024),
    segmentTargetRows: envNum("DS_SEGMENT_TARGET_ROWS", 100_000),
    segmentMaxIntervalMs: envNum("DS_SEGMENT_MAX_INTERVAL_MS", 0),
    segmentCheckIntervalMs: envNum("DS_SEGMENT_CHECK_MS", 250),
    segmenterWorkers: envNum("DS_SEGMENTER_WORKERS", 0),
    uploadIntervalMs: envNum("DS_UPLOAD_CHECK_MS", 250),
    uploadConcurrency: envNum("DS_UPLOAD_CONCURRENCY", 4),
    segmentCacheMaxBytes: envNum("DS_SEGMENT_CACHE_MAX_BYTES", 256 * 1024 * 1024),
    segmentFooterCacheEntries: envNum("DS_SEGMENT_FOOTER_CACHE_ENTRIES", 2048),
    indexRunCacheMaxBytes: indexDiskOverride ?? 256 * 1024 * 1024,
    indexRunMemoryCacheBytes: tunedIndexMem,
    lexiconIndexCacheMaxBytes:
      lexiconDiskOverride ??
      (memoryLimitBytes > 0
        ? clampBytes(Math.floor(memoryLimitBytes * 0.03), 8 * 1024 * 1024, 256 * 1024 * 1024)
        : 64 * 1024 * 1024),
    lexiconMappedCacheEntries: envNum("DS_LEXICON_MMAP_CACHE_ENTRIES", 64),
    indexL0SpanSegments: envNum("DS_INDEX_L0_SPAN", 16),
    indexBuildConcurrency: envNum("DS_INDEX_BUILD_CONCURRENCY", 4),
    indexCheckIntervalMs: envNum("DS_INDEX_CHECK_MS", 1000),
    searchCompanionBuildBatchSegments: envNum("DS_SEARCH_COMPANION_BATCH_SEGMENTS", 4),
    searchCompanionYieldBlocks: envNum("DS_SEARCH_COMPANION_YIELD_BLOCKS", 4),
    searchCompanionFileCacheMaxBytes: companionFileCacheBytes,
    searchCompanionFileCacheMaxAgeMs: envNum("DS_SEARCH_COMPANION_FILE_CACHE_MAX_AGE_MS", 24 * 60 * 60 * 1000),
    searchCompanionMappedCacheEntries: envNum("DS_SEARCH_COMPANION_MMAP_CACHE_ENTRIES", 64),
    searchCompanionTocCacheBytes: envNum("DS_SEARCH_COMPANION_TOC_CACHE_BYTES", 1 * 1024 * 1024),
    searchCompanionSectionCacheBytes: companionSectionCacheBytes,
    searchWalOverlayQuietPeriodMs: envNum("DS_SEARCH_WAL_OVERLAY_QUIET_MS", 5_000),
    searchWalOverlayMaxBytes,
    indexCompactionFanout: envNum("DS_INDEX_COMPACTION_FANOUT", 16),
    indexMaxLevel: envNum("DS_INDEX_MAX_LEVEL", 4),
    indexCompactionConcurrency: envNum("DS_INDEX_COMPACT_CONCURRENCY", 4),
    indexRetireGenWindow: envNum("DS_INDEX_RETIRE_GEN_WINDOW", 2),
    indexRetireMinMs: envNum("DS_INDEX_RETIRE_MIN_MS", 5 * 60 * 1000),
    readMaxBytes: envNum("DS_READ_MAX_BYTES", 1 * 1024 * 1024),
    readMaxRecords: envNum("DS_READ_MAX_RECORDS", 1000),
    appendMaxBodyBytes: envNum("DS_APPEND_MAX_BODY_BYTES", 10 * 1024 * 1024),
    ingestFlushIntervalMs: envNum("DS_INGEST_FLUSH_MS", 10),
    ingestMaxBatchRequests: envNum("DS_INGEST_MAX_BATCH_REQS", 200),
    ingestMaxBatchBytes: envNum("DS_INGEST_MAX_BATCH_BYTES", 8 * 1024 * 1024),
    ingestMaxQueueRequests: envNum("DS_INGEST_MAX_QUEUE_REQS", 50_000),
    ingestMaxQueueBytes: envNum("DS_INGEST_MAX_QUEUE_BYTES", 64 * 1024 * 1024),
    ingestConcurrency: envNum("DS_INGEST_CONCURRENCY", 2),
    ingestBusyTimeoutMs: envNum("DS_INGEST_BUSY_MS", 5000),
    localBacklogMaxBytes,
    memoryLimitBytes,
    sqliteCacheBytes,
    workerSqliteCacheBytes,
    readConcurrency: envNum("DS_READ_CONCURRENCY", 4),
    searchConcurrency: envNum("DS_SEARCH_CONCURRENCY", 2),
    asyncIndexConcurrency: envNum("DS_ASYNC_INDEX_CONCURRENCY", 1),
    heapSnapshotPath: process.env.DS_HEAP_SNAPSHOT_PATH?.trim() || null,
    memorySamplerPath: process.env.DS_MEMORY_SAMPLER_PATH?.trim() || null,
    memorySamplerIntervalMs: envNum("DS_MEMORY_SAMPLER_INTERVAL_MS", 1_000),
    objectStoreTimeoutMs: envNum("DS_OBJECTSTORE_TIMEOUT_MS", 5000),
    objectStoreRetries: envNum("DS_OBJECTSTORE_RETRIES", 3),
    objectStoreBaseDelayMs: envNum("DS_OBJECTSTORE_RETRY_BASE_MS", 50),
    objectStoreMaxDelayMs: envNum("DS_OBJECTSTORE_RETRY_MAX_MS", 2000),
    expirySweepIntervalMs: envNum("DS_EXPIRY_SWEEP_MS", 60_000),
    expirySweepBatchLimit: envNum("DS_EXPIRY_SWEEP_LIMIT", 100),
    metricsFlushIntervalMs: envNum("DS_METRICS_FLUSH_MS", 10_000),
    touchWorkers: envNum("DS_TOUCH_WORKERS", 1),
    touchCheckIntervalMs: envNum("DS_TOUCH_CHECK_MS", 250),
    touchMaxBatchRows: envNum("DS_TOUCH_MAX_BATCH_ROWS", 500),
    touchMaxBatchBytes: envNum("DS_TOUCH_MAX_BATCH_BYTES", 4 * 1024 * 1024),
    port: envNum("PORT", 8080),
  };
}
