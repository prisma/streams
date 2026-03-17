import { dsError } from "./util/ds_error.ts";
export type Config = {
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
  indexL0SpanSegments: number;
  indexBuildConcurrency: number;
  indexCheckIntervalMs: number;
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
  ingestBusyTimeoutMs: number;
  localBacklogMaxBytes: number;
  memoryLimitBytes: number;
  sqliteCacheBytes: number;
  objectStoreTimeoutMs: number;
  objectStoreRetries: number;
  objectStoreBaseDelayMs: number;
  objectStoreMaxDelayMs: number;
  expirySweepIntervalMs: number;
  expirySweepBatchLimit: number;
  metricsFlushIntervalMs: number;
  interpreterWorkers: number;
  interpreterCheckIntervalMs: number;
  interpreterMaxBatchRows: number;
  interpreterMaxBatchBytes: number;
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
  "DS_SEGMENT_CACHE_MAX_BYTES",
  "DS_SEGMENT_FOOTER_CACHE_ENTRIES",
  "DS_INDEX_RUN_CACHE_MAX_BYTES",
  "DS_INDEX_RUN_MEM_CACHE_BYTES",
  "DS_INDEX_L0_SPAN",
  "DS_INDEX_BUILD_CONCURRENCY",
  "DS_INDEX_CHECK_MS",
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
  "DS_INGEST_BUSY_MS",
  "DS_LOCAL_BACKLOG_MAX_BYTES",
  "DS_MEMORY_LIMIT_BYTES",
  "DS_MEMORY_LIMIT_MB",
  "DS_SQLITE_CACHE_BYTES",
  "DS_SQLITE_CACHE_MB",
  "DS_OBJECTSTORE_TIMEOUT_MS",
  "DS_OBJECTSTORE_RETRIES",
  "DS_OBJECTSTORE_RETRY_BASE_MS",
  "DS_OBJECTSTORE_RETRY_MAX_MS",
  "DS_LOCAL_DATA_ROOT",
  "DS_EXPIRY_SWEEP_MS",
  "DS_EXPIRY_SWEEP_LIMIT",
  "DS_METRICS_FLUSH_MS",
  "DS_INTERPRETER_WORKERS",
  "DS_INTERPRETER_CHECK_MS",
  "DS_INTERPRETER_MAX_BATCH_ROWS",
  "DS_INTERPRETER_MAX_BATCH_BYTES",
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
  const bytesOverride = envBytes("DS_MEMORY_LIMIT_BYTES");
  const mbOverride = envBytes("DS_MEMORY_LIMIT_MB");
  const memoryLimitBytes = bytesOverride ?? (mbOverride != null ? mbOverride * 1024 * 1024 : 0);
  const backlogOverride = envBytes("DS_LOCAL_BACKLOG_MAX_BYTES");
  const sqliteCacheBytesOverride = envBytes("DS_SQLITE_CACHE_BYTES");
  const sqliteCacheMbOverride = envBytes("DS_SQLITE_CACHE_MB");
  const indexMemOverride = envBytes("DS_INDEX_RUN_MEM_CACHE_BYTES");
  const indexDiskOverride = envBytes("DS_INDEX_RUN_CACHE_MAX_BYTES");
  const localBacklogMaxBytes = backlogOverride ?? 10 * 1024 * 1024 * 1024;
  const sqliteCacheBytes =
    sqliteCacheBytesOverride ??
    (sqliteCacheMbOverride != null
      ? sqliteCacheMbOverride * 1024 * 1024
      : memoryLimitBytes > 0
        ? Math.floor(memoryLimitBytes * 0.25)
        : 0);
  const tunedIndexMem =
    indexMemOverride ??
    (memoryLimitBytes > 0
      ? clampBytes(Math.floor(memoryLimitBytes * 0.05), 8 * 1024 * 1024, 128 * 1024 * 1024)
      : 64 * 1024 * 1024);
  return {
    host,
    rootDir,
    dbPath: process.env.DS_DB_PATH ?? `${rootDir}/wal.sqlite`,
    segmentMaxBytes: envNum("DS_SEGMENT_MAX_BYTES", 16 * 1024 * 1024),
    blockMaxBytes: envNum("DS_BLOCK_MAX_BYTES", 256 * 1024),
    segmentTargetRows: envNum("DS_SEGMENT_TARGET_ROWS", 50_000),
    segmentMaxIntervalMs: envNum("DS_SEGMENT_MAX_INTERVAL_MS", 0),
    segmentCheckIntervalMs: envNum("DS_SEGMENT_CHECK_MS", 250),
    segmenterWorkers: envNum("DS_SEGMENTER_WORKERS", 0),
    uploadIntervalMs: envNum("DS_UPLOAD_CHECK_MS", 250),
    uploadConcurrency: envNum("DS_UPLOAD_CONCURRENCY", 4),
    segmentCacheMaxBytes: envNum("DS_SEGMENT_CACHE_MAX_BYTES", 256 * 1024 * 1024),
    segmentFooterCacheEntries: envNum("DS_SEGMENT_FOOTER_CACHE_ENTRIES", 2048),
    indexRunCacheMaxBytes: indexDiskOverride ?? 256 * 1024 * 1024,
    indexRunMemoryCacheBytes: tunedIndexMem,
    indexL0SpanSegments: envNum("DS_INDEX_L0_SPAN", 16),
    indexBuildConcurrency: envNum("DS_INDEX_BUILD_CONCURRENCY", 4),
    indexCheckIntervalMs: envNum("DS_INDEX_CHECK_MS", 1000),
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
    ingestBusyTimeoutMs: envNum("DS_INGEST_BUSY_MS", 5000),
    localBacklogMaxBytes,
    memoryLimitBytes,
    sqliteCacheBytes,
    objectStoreTimeoutMs: envNum("DS_OBJECTSTORE_TIMEOUT_MS", 5000),
    objectStoreRetries: envNum("DS_OBJECTSTORE_RETRIES", 3),
    objectStoreBaseDelayMs: envNum("DS_OBJECTSTORE_RETRY_BASE_MS", 50),
    objectStoreMaxDelayMs: envNum("DS_OBJECTSTORE_RETRY_MAX_MS", 2000),
    expirySweepIntervalMs: envNum("DS_EXPIRY_SWEEP_MS", 60_000),
    expirySweepBatchLimit: envNum("DS_EXPIRY_SWEEP_LIMIT", 100),
    metricsFlushIntervalMs: envNum("DS_METRICS_FLUSH_MS", 10_000),
    interpreterWorkers: envNum("DS_INTERPRETER_WORKERS", 1),
    interpreterCheckIntervalMs: envNum("DS_INTERPRETER_CHECK_MS", 250),
    interpreterMaxBatchRows: envNum("DS_INTERPRETER_MAX_BATCH_ROWS", 500),
    interpreterMaxBatchBytes: envNum("DS_INTERPRETER_MAX_BATCH_BYTES", 4 * 1024 * 1024),
    port: envNum("PORT", 8080),
  };
}
