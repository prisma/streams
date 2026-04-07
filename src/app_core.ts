import { mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import type { Config } from "./config";
import { SqliteDurableStore, type StreamRow, type SearchCompanionPlanRow, type SearchSegmentCompanionRow } from "./db/db";
import { IngestQueue, type ProducerInfo, type AppendRow, type AppendResult } from "./ingest";
import type { ObjectStore } from "./objectstore/interface";
import type { StreamReader, ReadBatch, ReaderError, SearchResultBatch } from "./reader";
import { StreamNotifier } from "./notifier";
import { encodeOffset, parseOffsetResult, offsetToSeqOrNeg1, canonicalizeOffset, type ParsedOffset } from "./offset";
import { parseDurationMsResult } from "./util/duration";
import { Metrics } from "./metrics";
import { parseTimestampMsResult } from "./util/time";
import { cleanupTempSegments } from "./util/cleanup";
import { MetricsEmitter } from "./metrics_emitter";
import {
  SchemaRegistryStore,
  parseSchemaUpdateResult,
  type SchemaRegistry,
  type SearchConfig,
  type SchemaRegistryMutationError,
  type SchemaRegistryReadError,
} from "./schema/registry";
import { decodeJsonPayloadResult } from "./schema/read_json";
import { resolvePointerResult } from "./util/json_pointer";
import { ExpirySweeper } from "./expiry_sweeper";
import type { StatsCollector } from "./stats";
import { BackpressureGate } from "./backpressure";
import { MemoryPressureMonitor } from "./memory";
import { RuntimeMemorySampler } from "./runtime_memory_sampler";
import { TouchProcessorManager } from "./touch/manager";
import type { SegmentDiskCache } from "./segment/cache";
import { StreamSizeReconciler } from "./stream_size_reconciler";
import { ConcurrencyGate } from "./concurrency_gate";
import {
  buildProcessMemoryBreakdown,
  type RuntimeHighWaterMark,
  type RuntimeMemoryHighWaterSnapshot,
  type RuntimeMemorySubsystemSnapshot,
  type RuntimeMemorySnapshot,
} from "./runtime_memory";
import type { SegmenterController } from "./segment/segmenter_workers";
import type { UploaderController } from "./uploader";
import type { StreamIndexLookup } from "./index/indexer";
import { ForegroundActivityTracker } from "./foreground_activity";
import { Result } from "better-result";
import { parseReadFilterResult } from "./read_filter";
import { hashSecondaryIndexField } from "./index/secondary_schema";
import { buildDesiredSearchCompanionPlan, hashSearchCompanionPlan } from "./search/companion_plan";
import { parseSearchRequestBodyResult, parseSearchRequestQueryResult } from "./search/query";
import { parseAggregateRequestBodyResult } from "./search/aggregate";
import {
  StreamProfileStore,
  parseProfileUpdateResult,
  resolveJsonIngestCapability,
  resolveTouchCapability,
  type StreamTouchRoute,
} from "./profiles";
import { dsError } from "./util/ds_error.ts";
import { streamHash16Hex } from "./util/stream_paths";

function withNosniff(headers: HeadersInit = {}): HeadersInit {
  return {
    "x-content-type-options": "nosniff",
    ...headers,
  };
}

function json(status: number, body: any, headers: HeadersInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...withNosniff(headers),
    },
  });
}

const OVERLOAD_RETRY_AFTER_SECONDS = "1";
const UNAVAILABLE_RETRY_AFTER_SECONDS = "5";
const APPEND_REQUEST_TIMEOUT_MS = 3_000;
const HTTP_RESOLVER_TIMEOUT_MS = 5_000;
const SEARCH_REQUEST_TIMEOUT_MS = 3_000;
const TIMEOUT_SENTINEL = Symbol("request-timeout");
const DEFAULT_TOUCH_JOURNAL_FILTER_BYTES = 4 * (1 << 22);

type TimeoutSentinel = typeof TIMEOUT_SENTINEL;

function retryAfterHeaders(seconds: string, headers: HeadersInit = {}): HeadersInit {
  return {
    "retry-after": seconds,
    ...headers,
  };
}

function clampSearchRequestTimeoutMs(timeoutMs: number | null): number {
  return timeoutMs == null ? SEARCH_REQUEST_TIMEOUT_MS : Math.min(timeoutMs, SEARCH_REQUEST_TIMEOUT_MS);
}

async function awaitWithCooperativeTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T | TimeoutSentinel> {
  let timer: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      promise,
      new Promise<TimeoutSentinel>((resolve) => {
        timer = setTimeout(() => resolve(TIMEOUT_SENTINEL), timeoutMs);
      }),
    ]);
  } finally {
    if (timer != null) clearTimeout(timer);
  }
}

function isAbortLikeError(error: unknown): boolean {
  return typeof error === "object" && error != null && "name" in error && (error as { name?: unknown }).name === "AbortError";
}

function searchResponseHeaders(search: SearchResultBatch): HeadersInit {
  return {
    "search-timed-out": search.timedOut ? "true" : "false",
    "search-timeout-ms": String(search.timeoutMs ?? SEARCH_REQUEST_TIMEOUT_MS),
    "search-took-ms": String(search.tookMs),
    "search-total-relation": search.total.relation,
    "search-coverage-complete": search.coverage.complete ? "true" : "false",
    "search-indexed-segments": String(search.coverage.indexedSegments),
    "search-indexed-segment-time-ms": String(search.coverage.indexedSegmentTimeMs),
    "search-fts-section-get-ms": String(search.coverage.ftsSectionGetMs),
    "search-fts-decode-ms": String(search.coverage.ftsDecodeMs),
    "search-fts-clause-estimate-ms": String(search.coverage.ftsClauseEstimateMs),
    "search-scanned-segments": String(search.coverage.scannedSegments),
    "search-scanned-segment-time-ms": String(search.coverage.scannedSegmentTimeMs),
    "search-scanned-tail-docs": String(search.coverage.scannedTailDocs),
    "search-scanned-tail-time-ms": String(search.coverage.scannedTailTimeMs),
    "search-exact-candidate-time-ms": String(search.coverage.exactCandidateTimeMs),
    "search-index-families-used": search.coverage.indexFamiliesUsed.join(","),
  };
}

function internalError(message = "internal server error"): Response {
  return json(500, { error: { code: "internal", message } });
}

function badRequest(msg: string): Response {
  return json(400, { error: { code: "bad_request", message: msg } });
}

function notFound(msg = "not_found"): Response {
  return json(404, { error: { code: "not_found", message: msg } });
}

function readerErrorResponse(err: ReaderError): Response {
  if (err.kind === "not_found") return notFound();
  if (err.kind === "gone") return notFound("stream expired");
  if (err.kind === "internal") return internalError();
  return badRequest(err.message);
}

function schemaMutationErrorResponse(err: SchemaRegistryMutationError): Response {
  if (err.kind === "version_mismatch") return conflict(err.message);
  return badRequest(err.message);
}

function schemaReadErrorResponse(_err: SchemaRegistryReadError): Response {
  return internalError();
}

function conflict(msg: string, headers: HeadersInit = {}): Response {
  return json(409, { error: { code: "conflict", message: msg } }, headers);
}

function tooLarge(msg: string): Response {
  return json(413, { error: { code: "payload_too_large", message: msg } });
}

function unavailable(msg = "server shutting down"): Response {
  return json(503, { error: { code: "unavailable", message: msg } }, retryAfterHeaders(UNAVAILABLE_RETRY_AFTER_SECONDS));
}

function overloaded(msg = "ingest queue full", code = "overloaded"): Response {
  return json(429, { error: { code, message: msg } }, retryAfterHeaders(OVERLOAD_RETRY_AFTER_SECONDS));
}

function requestTimeout(msg = "request timed out"): Response {
  return json(408, { error: { code: "request_timeout", message: msg } });
}

function appendTimeout(): Response {
  return json(408, {
    error: {
      code: "append_timeout",
      message: "append timed out; append outcome is unknown, check Stream-Next-Offset before retrying",
    },
  });
}

async function cancelRequestBody(req: Request): Promise<void> {
  const body = req.body;
  if (!body) return;
  try {
    await body.cancel("request rejected");
    return;
  } catch {
    // ignore and try a reader-based cancel below
  }
  try {
    const reader = body.getReader();
    await reader.cancel("request rejected");
  } catch {
    // ignore
  }
}

function normalizeContentType(value: string | null): string | null {
  if (!value) return null;
  const base = value.split(";")[0]?.trim().toLowerCase();
  return base ? base : null;
}

function isJsonContentType(value: string | null): boolean {
  return normalizeContentType(value) === "application/json";
}

function isTextContentType(value: string | null): boolean {
  const norm = normalizeContentType(value);
  return norm === "application/json" || (norm != null && norm.startsWith("text/"));
}

function parseStreamClosedHeader(value: string | null): boolean {
  return value != null && value.trim().toLowerCase() === "true";
}

function parseStreamSeqHeader(value: string | null): Result<string | null, { message: string }> {
  if (value == null) return Result.ok(null);
  const v = value.trim();
  if (v.length === 0) return Result.err({ message: "invalid Stream-Seq" });
  return Result.ok(v);
}

function parseStreamTtlSeconds(value: string): Result<number, { message: string }> {
  const s = value.trim();
  if (/^(0|[1-9][0-9]*)$/.test(s)) return Result.ok(Number(s));
  if (/^(0|[1-9][0-9]*)(ms|s|m|h|d)$/.test(s)) {
    const msRes = parseDurationMsResult(s);
    if (Result.isError(msRes)) return Result.err({ message: msRes.error.message });
    const ms = msRes.value;
    if (ms % 1000 !== 0) return Result.err({ message: "invalid Stream-TTL" });
    return Result.ok(Math.floor(ms / 1000));
  }
  return Result.err({ message: "invalid Stream-TTL" });
}

function parseNonNegativeInt(value: string): number | null {
  if (!/^[0-9]+$/.test(value)) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n;
}

function splitSseLines(data: string): string[] {
  if (data === "") return [""];
  return data.split(/\r\n|\r|\n/);
}

function encodeSseEvent(eventType: string, data: string): string {
  const lines = splitSseLines(data);
  let out = `event: ${eventType}\n`;
  for (const line of lines) {
    out += `data:${line}\n`;
  }
  out += `\n`;
  return out;
}

const INTERNAL_METRICS_STREAM = "__stream_metrics__";

function clearInternalMetricsAccelerationState(db: SqliteDurableStore): void {
  db.deleteAccelerationState(INTERNAL_METRICS_STREAM);
}

function reconcileDeletedStreamAccelerationState(db: SqliteDurableStore): void {
  let offset = 0;
  const pageSize = 1000;
  for (;;) {
    const streams = db.listDeletedStreams(pageSize, offset);
    for (const stream of streams) {
      db.deleteAccelerationState(stream);
    }
    if (streams.length < pageSize) break;
    offset += streams.length;
  }
}

function computeCursor(nowMs: number, provided: string | null): string {
  let cursor = Math.floor(nowMs / 1000);
  if (provided && /^[0-9]+$/.test(provided)) {
    const n = Number(provided);
    if (Number.isFinite(n) && n >= cursor) cursor = n + 1;
  }
  return String(cursor);
}

function concatPayloads(parts: Uint8Array[]): Buffer {
  return Buffer.concat(parts.map((part) => Buffer.from(part.buffer, part.byteOffset, part.byteLength)));
}

function bodyBufferFromBytes(bytes: Uint8Array): ArrayBuffer {
  const buffer = bytes.buffer;
  if (bytes.byteOffset === 0 && bytes.byteLength === buffer.byteLength) {
    return buffer as ArrayBuffer;
  }
  return buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

function keyBytesFromString(s: string | null): Uint8Array | null {
  if (s == null) return null;
  return new TextEncoder().encode(s);
}

function extractRoutingKey(reg: SchemaRegistry, value: any): Result<Uint8Array | null, { message: string }> {
  if (!reg.routingKey) return Result.ok(null);
  const { jsonPointer, required } = reg.routingKey;
  const resolvedRes = resolvePointerResult(value, jsonPointer);
  if (Result.isError(resolvedRes)) return Result.err({ message: resolvedRes.error.message });
  const resolved = resolvedRes.value;
  if (!resolved.exists) {
    if (required) return Result.err({ message: "routing key missing" });
    return Result.ok(null);
  }
  if (typeof resolved.value !== "string") return Result.err({ message: "routing key must be string" });
  return Result.ok(keyBytesFromString(resolved.value));
}

function timestampToIsoString(value: bigint | null): string | null {
  return value == null ? null : new Date(Number(value)).toISOString();
}

function weakEtag(namespace: string, body: string): string {
  const hash = createHash("sha1").update(body).digest("hex");
  return `W/"${namespace}:${hash}"`;
}

function configuredExactIndexes(search: SearchConfig | undefined): Array<{ name: string; kind: string; configHash: string }> {
  if (!search) return [];
  return Object.entries(search.fields)
    .filter(([, field]) => field.exact === true && field.kind !== "text")
    .map(([name, field]) => ({
      name,
      kind: field.kind,
      configHash: hashSecondaryIndexField({ name, config: field }),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

function configuredSearchFamilies(search: SearchConfig | undefined): Array<{ family: "col" | "fts" | "agg" | "mblk"; fields: string[] }> {
  if (!search) return [];
  const out: Array<{ family: "col" | "fts" | "agg" | "mblk"; fields: string[] }> = [];
  const colFields = Object.entries(search.fields)
    .filter(([, field]) => field.column === true)
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  if (colFields.length > 0) out.push({ family: "col", fields: colFields });
  const ftsFields = Object.entries(search.fields)
    .filter(([, field]) => field.kind === "text" || (field.kind === "keyword" && field.prefix === true))
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  if (ftsFields.length > 0) out.push({ family: "fts", fields: ftsFields });
  const aggRollups = Object.keys(search.rollups ?? {}).sort((a, b) => a.localeCompare(b));
  if (aggRollups.length > 0) out.push({ family: "agg", fields: aggRollups });
  if (search.profile === "metrics") out.push({ family: "mblk", fields: ["metrics"] });
  return out;
}

function parseCompanionSections(value: string): Set<string> {
  try {
    const parsed = JSON.parse(value);
    return new Set(Array.isArray(parsed) ? parsed.filter((entry) => typeof entry === "string") : []);
  } catch {
    return new Set();
  }
}

function parseCompanionSectionSizes(value: string): Record<string, number> {
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object") return {};
    const out: Record<string, number> = {};
    for (const [key, raw] of Object.entries(parsed)) {
      if (typeof raw === "number" && Number.isFinite(raw) && raw >= 0) out[key] = raw;
    }
    return out;
  } catch {
    return {};
  }
}

function contiguousCoveredSegmentCount(rows: Array<{ segment_index: number; sections_json: string }>, family: string): number {
  let expected = 0;
  for (const row of rows) {
    if (row.segment_index < expected) continue;
    if (row.segment_index > expected) break;
    if (!parseCompanionSections(row.sections_json).has(family)) break;
    expected += 1;
  }
  return expected;
}

export type App = {
  fetch: (req: Request) => Promise<Response>;
  close: () => void;
  deps: {
    config: Config;
    db: SqliteDurableStore;
    os: ObjectStore;
    ingest: IngestQueue;
    notifier: StreamNotifier;
    reader: StreamReader;
    segmenter: SegmenterController;
    uploader: UploaderController;
    indexer?: StreamIndexLookup;
    segmentDiskCache?: SegmentDiskCache;
    metrics: Metrics;
    registry: SchemaRegistryStore;
    profiles: StreamProfileStore;
    touch: TouchProcessorManager;
    stats?: StatsCollector;
    backpressure?: BackpressureGate;
    memory?: MemoryPressureMonitor;
    concurrency?: {
      ingest: ConcurrencyGate;
      read: ConcurrencyGate;
      search: ConcurrencyGate;
      asyncIndex: ConcurrencyGate;
    };
    memorySampler?: RuntimeMemorySampler;
  };
};

export type CreateAppRuntimeArgs = {
  config: Config;
  db: SqliteDurableStore;
  ingest: IngestQueue;
  notifier: StreamNotifier;
  registry: SchemaRegistryStore;
  profiles: StreamProfileStore;
  touch: TouchProcessorManager;
  stats?: StatsCollector;
  backpressure?: BackpressureGate;
  memory: MemoryPressureMonitor;
  asyncIndexGate: ConcurrencyGate;
  foregroundActivity: ForegroundActivityTracker;
  metrics: Metrics;
  memorySampler?: RuntimeMemorySampler;
};

type AppRuntimeDeps = {
  store: ObjectStore;
  reader: StreamReader;
  segmenter: SegmenterController;
  uploader: UploaderController;
  indexer?: StreamIndexLookup;
  segmentDiskCache?: SegmentDiskCache;
  uploadSchemaRegistry: (stream: string, registry: SchemaRegistry) => Promise<void>;
  getRuntimeMemorySnapshot?: () => RuntimeMemorySubsystemSnapshot;
  getLocalStorageUsage?: (stream: string) => {
    segment_cache_bytes: number;
    routing_index_cache_bytes: number;
    exact_index_cache_bytes: number;
    lexicon_index_cache_bytes: number;
    companion_cache_bytes: number;
  };
  start(): void;
};

export type CreateAppCoreOptions = {
  stats?: StatsCollector;
  createRuntime(args: CreateAppRuntimeArgs): AppRuntimeDeps;
};

function reduceConcurrencyLimit(limit: number): number {
  return Math.max(1, Math.ceil(Math.max(1, limit) / 2));
}

function gateSnapshot(configuredLimit: number, gate: ConcurrencyGate) {
  return {
    configured_limit: configuredLimit,
    current_limit: gate.getLimit(),
    active: gate.getActive(),
    queued: gate.getQueued(),
  };
}

export function createAppCore(cfg: Config, opts: CreateAppCoreOptions): App {
  mkdirSync(cfg.rootDir, { recursive: true });
  cleanupTempSegments(cfg.rootDir);

  const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.sqliteCacheBytes });
  db.resetSegmentInProgress();
  reconcileDeletedStreamAccelerationState(db);
  const stats = opts.stats;
  const metrics = new Metrics();
  const backpressure =
    cfg.localBacklogMaxBytes > 0
      ? new BackpressureGate(cfg.localBacklogMaxBytes, db.sumPendingBytes() + db.sumPendingSegmentBytes())
      : undefined;
  const memorySampler =
    cfg.memorySamplerPath != null
      ? new RuntimeMemorySampler(cfg.memorySamplerPath, {
          intervalMs: cfg.memorySamplerIntervalMs,
          scope: "main",
        })
      : undefined;
  memorySampler?.start();
  const ingestGate = new ConcurrencyGate(cfg.ingestConcurrency);
  const readGate = new ConcurrencyGate(cfg.readConcurrency);
  const searchGate = new ConcurrencyGate(cfg.searchConcurrency);
  const asyncIndexGate = new ConcurrencyGate(cfg.asyncIndexConcurrency);
  const foregroundActivity = new ForegroundActivityTracker();
  const memory = new MemoryPressureMonitor(cfg.memoryLimitBytes, {
    onSample: (rss, overLimit) => {
      metrics.record("process.rss.bytes", rss, "bytes");
      if (overLimit) metrics.record("process.rss.over_limit", 1, "count");
      searchGate.setLimit(overLimit ? reduceConcurrencyLimit(cfg.searchConcurrency) : cfg.searchConcurrency);
      asyncIndexGate.setLimit(overLimit ? reduceConcurrencyLimit(cfg.asyncIndexConcurrency) : cfg.asyncIndexConcurrency);
    },
    heapSnapshotPath: cfg.heapSnapshotPath ?? undefined,
  });
  memory.start();
  const ingest = new IngestQueue(cfg, db, stats, backpressure, metrics);
  const notifier = new StreamNotifier();
  const registry = new SchemaRegistryStore(db);
  const profiles = new StreamProfileStore(db, registry);
  const touch = new TouchProcessorManager(cfg, db, ingest, notifier, profiles, backpressure);
  const runtime = opts.createRuntime({
    config: cfg,
    db,
    ingest,
    notifier,
    registry,
    profiles,
    touch,
    stats,
    backpressure,
    memory,
    asyncIndexGate,
    foregroundActivity,
    metrics,
    memorySampler,
  });
  const { store, reader, segmenter, uploader, indexer, uploadSchemaRegistry, getRuntimeMemorySnapshot, getLocalStorageUsage } = runtime;
  const runtimeHighWater: RuntimeMemoryHighWaterSnapshot = {
    process: {},
    process_breakdown: {},
    sqlite: {},
    runtime_bytes: {},
    runtime_totals: {},
  };

  const observeHighWaterValue = (target: Record<string, RuntimeHighWaterMark>, key: string, value: number, at: string): void => {
    const next = Math.max(0, Math.floor(value));
    const existing = target[key];
    if (!existing || next > existing.value) {
      target[key] = {
        value: next,
        at,
      };
    }
  };

  const buildRuntimeBytes = (runtimeMemory: RuntimeMemorySnapshot): Record<string, Record<string, number>> => {
    const groups: Record<string, Record<string, number>> = {};
    for (const [kind, values] of Object.entries(runtimeMemory.subsystems)) {
      if (kind === "counts") continue;
      groups[kind] = values;
    }
    return groups;
  };

  const buildTopStreamContributors = (limit = 5) => {
    const safeLimit = Math.max(1, Math.min(limit, 20));
    const localStorageRows: Array<{
      stream: string;
      bytes: number;
      wal_retained_bytes: number;
      segment_cache_bytes: number;
      index_cache_bytes: number;
    }> = [];
    const pendingWalRows: Array<{ stream: string; pending_wal_bytes: number; pending_rows: number }> = [];
    let offset = 0;
    const pageSize = 1000;
    for (;;) {
      const rows = db.listStreams(pageSize, offset);
      if (rows.length === 0) break;
      for (const row of rows) {
        if (db.isDeleted(row)) continue;
        const usage = getLocalStorageUsage?.(row.stream) ?? { segment_cache_bytes: 0 };
        const walRetainedBytes = Number(row.pending_bytes);
        const segmentCacheBytes = Math.max(0, Math.floor(Number((usage as Record<string, number>).segment_cache_bytes ?? 0)));
        const indexCacheBytes = Math.max(
          0,
          Math.floor(
            Number((usage as Record<string, number>).routing_index_cache_bytes ?? 0) +
              Number((usage as Record<string, number>).exact_index_cache_bytes ?? 0) +
              Number((usage as Record<string, number>).lexicon_index_cache_bytes ?? 0) +
              Number((usage as Record<string, number>).companion_cache_bytes ?? 0)
          )
        );
        localStorageRows.push({
          stream: row.stream,
          bytes: Math.max(0, walRetainedBytes + segmentCacheBytes + indexCacheBytes),
          wal_retained_bytes: Math.max(0, walRetainedBytes),
          segment_cache_bytes: segmentCacheBytes,
          index_cache_bytes: indexCacheBytes,
        });
        pendingWalRows.push({
          stream: row.stream,
          pending_wal_bytes: Math.max(0, walRetainedBytes),
          pending_rows: Math.max(0, Number(row.pending_rows)),
        });
      }
      if (rows.length < pageSize) break;
      offset += rows.length;
    }
    localStorageRows.sort((a, b) => b.bytes - a.bytes || a.stream.localeCompare(b.stream));
    pendingWalRows.sort((a, b) => b.pending_wal_bytes - a.pending_wal_bytes || a.stream.localeCompare(b.stream));
    return {
      local_storage_bytes: localStorageRows.slice(0, safeLimit),
      pending_wal_bytes: pendingWalRows.slice(0, safeLimit),
      touch_journal_filter_bytes: touch.getTopStreams(safeLimit),
      notifier_waiters: notifier.getTopStreams(safeLimit),
    };
  };

  const buildRuntimeMemorySnapshot = (): RuntimeMemorySnapshot => {
    const processUsage = process.memoryUsage();
    const subsystemSnapshot = getRuntimeMemorySnapshot?.() ?? {
      subsystems: {
        heap_estimates: {},
        mapped_files: {},
        disk_caches: {},
        configured_budgets: {},
        pipeline_buffers: {},
        sqlite_runtime: {},
        counts: {},
      },
      totals: {
        heap_estimate_bytes: 0,
        mapped_file_bytes: 0,
        disk_cache_bytes: 0,
        configured_budget_bytes: 0,
        pipeline_buffer_bytes: 0,
        sqlite_runtime_bytes: 0,
      },
    };
    const sqliteRuntimeBytes = subsystemSnapshot.subsystems.sqlite_runtime ?? {};
    const runtimeCounts = subsystemSnapshot.subsystems.counts ?? {};
    const snapshot: RuntimeMemorySnapshot = {
      process: {
        rss_bytes: processUsage.rss,
        heap_total_bytes: processUsage.heapTotal,
        heap_used_bytes: processUsage.heapUsed,
        external_bytes: processUsage.external,
        array_buffers_bytes: processUsage.arrayBuffers,
      },
      process_breakdown: buildProcessMemoryBreakdown({
        process: {
          rss_bytes: processUsage.rss,
          heap_total_bytes: processUsage.heapTotal,
          heap_used_bytes: processUsage.heapUsed,
          external_bytes: processUsage.external,
          array_buffers_bytes: processUsage.arrayBuffers,
        },
        mappedFileBytes: subsystemSnapshot.totals.mapped_file_bytes,
        sqliteRuntimeBytes: Number(sqliteRuntimeBytes["sqlite_memory_used_bytes"] ?? 0),
      }),
      sqlite: {
        available: Number(runtimeCounts["sqlite_open_connections"] ?? 0) > 0 || Number(sqliteRuntimeBytes["sqlite_memory_used_bytes"] ?? 0) > 0,
        source:
          Number(runtimeCounts["sqlite_open_connections"] ?? 0) > 0 || Number(sqliteRuntimeBytes["sqlite_memory_used_bytes"] ?? 0) > 0
            ? "sqlite3_status64"
            : "unavailable",
        memory_used_bytes: Math.max(0, Math.floor(Number(sqliteRuntimeBytes["sqlite_memory_used_bytes"] ?? 0))),
        memory_highwater_bytes: Math.max(
          0,
          Math.floor(Number(sqliteRuntimeBytes["sqlite_memory_highwater_bytes"] ?? 0))
        ),
        pagecache_used_slots: Math.max(0, Math.floor(Number(runtimeCounts["sqlite_pagecache_used_slots"] ?? 0))),
        pagecache_used_slots_highwater: Math.max(
          0,
          Math.floor(Number(runtimeCounts["sqlite_pagecache_used_slots_highwater"] ?? 0))
        ),
        pagecache_overflow_bytes: Math.max(
          0,
          Math.floor(Number(sqliteRuntimeBytes["sqlite_pagecache_overflow_bytes"] ?? 0))
        ),
        pagecache_overflow_highwater_bytes: Math.max(
          0,
          Math.floor(Number(sqliteRuntimeBytes["sqlite_pagecache_overflow_highwater_bytes"] ?? 0))
        ),
        malloc_count: Math.max(0, Math.floor(Number(runtimeCounts["sqlite_malloc_count"] ?? 0))),
        malloc_count_highwater: Math.max(
          0,
          Math.floor(Number(runtimeCounts["sqlite_malloc_count_highwater"] ?? 0))
        ),
        open_connections: Math.max(0, Math.floor(Number(runtimeCounts["sqlite_open_connections"] ?? 0))),
        prepared_statements: Math.max(
          0,
          Math.floor(Number(runtimeCounts["sqlite_prepared_statements"] ?? 0))
        ),
      },
      gc: memory.getGcStats(),
      subsystems: subsystemSnapshot.subsystems,
      totals: subsystemSnapshot.totals,
    };
    const ts = new Date().toISOString();
    for (const [key, value] of Object.entries(snapshot.process)) observeHighWaterValue(runtimeHighWater.process, key, value, ts);
    for (const [key, value] of Object.entries(snapshot.process_breakdown)) {
      if (typeof value === "number") observeHighWaterValue(runtimeHighWater.process_breakdown, key, value, ts);
    }
    for (const [key, value] of Object.entries(snapshot.sqlite)) {
      if (typeof value === "number") observeHighWaterValue(runtimeHighWater.sqlite, key, value, ts);
    }
    for (const [kind, values] of Object.entries(buildRuntimeBytes(snapshot))) {
      const bucket = (runtimeHighWater.runtime_bytes[kind] ??= {});
      for (const [key, value] of Object.entries(values)) observeHighWaterValue(bucket, key, value, ts);
    }
    for (const [key, value] of Object.entries(snapshot.totals)) observeHighWaterValue(runtimeHighWater.runtime_totals, key, value, ts);
    if (snapshot.sqlite.memory_used_bytes > 0) observeHighWaterValue(runtimeHighWater.sqlite, "memory_used_bytes", snapshot.sqlite.memory_used_bytes, ts);
    if (snapshot.sqlite.pagecache_overflow_bytes > 0)
      observeHighWaterValue(runtimeHighWater.sqlite, "pagecache_overflow_bytes", snapshot.sqlite.pagecache_overflow_bytes, ts);
    if (snapshot.sqlite.pagecache_used_slots > 0)
      observeHighWaterValue(runtimeHighWater.sqlite, "pagecache_used_slots", snapshot.sqlite.pagecache_used_slots, ts);
    if (snapshot.sqlite.malloc_count > 0) observeHighWaterValue(runtimeHighWater.sqlite, "malloc_count", snapshot.sqlite.malloc_count, ts);
    return snapshot;
  };

  const buildLeakCandidateCounters = (): Record<string, number> => {
    const runtimeMemory = buildRuntimeMemorySnapshot();
    const runtimeCounts = runtimeMemory.subsystems.counts ?? {};
    const countValue = (name: string): number => {
      const raw = Number(runtimeCounts[name] ?? 0);
      if (!Number.isFinite(raw)) return 0;
      return Math.max(0, Math.floor(raw));
    };
    const touchMemory = touch.getMemoryStats();
    const notifierMemory = notifier.getMemoryStats();
    const metricsMemory = metrics.getMemoryStats();
    return {
      "tieredstore.mem.leak_candidate.segment_cache.pinned_entries": countValue("segment_pinned_files"),
      "tieredstore.mem.leak_candidate.lexicon_file_cache.pinned_entries": countValue("lexicon_pinned_files"),
      "tieredstore.mem.leak_candidate.companion_file_cache.pinned_entries": countValue("companion_pinned_files"),
      "tieredstore.mem.leak_candidate.routing_run_disk_cache.pinned_entries": countValue("routing_run_disk_cache_pinned_entries"),
      "tieredstore.mem.leak_candidate.exact_run_disk_cache.pinned_entries": countValue("exact_run_disk_cache_pinned_entries"),
      "tieredstore.mem.leak_candidate.touch.journals.active_count": touchMemory.journals,
      "tieredstore.mem.leak_candidate.touch.journals.created_total": touchMemory.journalsCreatedTotal,
      "tieredstore.mem.leak_candidate.touch.journals.filter_bytes_total": touchMemory.journalFilterBytesTotal,
      "tieredstore.mem.leak_candidate.touch.journal.default_filter_bytes": DEFAULT_TOUCH_JOURNAL_FILTER_BYTES,
      "tieredstore.mem.leak_candidate.touch.maps.fine_lag_coarse_only_streams": touchMemory.fineLagCoarseOnlyStreams,
      "tieredstore.mem.leak_candidate.touch.maps.touch_mode_streams": touchMemory.touchModeStreams,
      "tieredstore.mem.leak_candidate.touch.maps.fine_token_bucket_streams": touchMemory.fineTokenBucketStreams,
      "tieredstore.mem.leak_candidate.touch.maps.hot_fine_streams": touchMemory.hotFineStreams,
      "tieredstore.mem.leak_candidate.touch.maps.lag_source_offset_streams": touchMemory.lagSourceOffsetStreams,
      "tieredstore.mem.leak_candidate.touch.maps.restricted_template_bucket_streams": touchMemory.restrictedTemplateBucketStreams,
      "tieredstore.mem.leak_candidate.touch.maps.runtime_totals_streams": touchMemory.runtimeTotalsStreams,
      "tieredstore.mem.leak_candidate.touch.maps.zero_row_backlog_streams": touchMemory.zeroRowBacklogStreakStreams,
      "tieredstore.mem.leak_candidate.live_template.last_seen_entries": touchMemory.templateLastSeenEntries,
      "tieredstore.mem.leak_candidate.live_template.dirty_last_seen_entries": touchMemory.templateDirtyLastSeenEntries,
      "tieredstore.mem.leak_candidate.live_template.rate_state_streams": touchMemory.templateRateStateStreams,
      "tieredstore.mem.leak_candidate.live_metrics.counter_streams": touchMemory.liveMetricsCounterStreams,
      "tieredstore.mem.leak_candidate.notifier.latest_seq_streams": notifierMemory.latestSeqStreams,
      "tieredstore.mem.leak_candidate.notifier.details_version_streams": notifierMemory.detailsVersionStreams,
      "tieredstore.mem.leak_candidate.metrics.series": metricsMemory.seriesCount,
      "tieredstore.mem.leak_candidate.mock_r2.in_memory_bytes": countValue("mock_r2_in_memory_bytes"),
      "tieredstore.mem.leak_candidate.mock_r2.object_count": countValue("mock_r2_object_count"),
    };
  };

  const buildServerMem = () => {
    const runtimeMemory = buildRuntimeMemorySnapshot();
    return {
      ts: new Date().toISOString(),
      process: runtimeMemory.process,
      process_breakdown: runtimeMemory.process_breakdown,
      sqlite: runtimeMemory.sqlite,
      gc: runtimeMemory.gc,
      high_water: runtimeHighWater,
      counters: buildLeakCandidateCounters(),
      runtime_counts: runtimeMemory.subsystems.counts,
      runtime_bytes: buildRuntimeBytes(runtimeMemory),
      runtime_totals: runtimeMemory.totals,
      top_streams: buildTopStreamContributors(),
    };
  };
  memorySampler?.setSubsystemProvider(() => buildRuntimeMemorySnapshot().subsystems);
  const buildServerDetails = () => {
    const runtimeMemory = buildRuntimeMemorySnapshot();
    const uploaderMemory = uploader.getMemoryStats?.() ?? {
      inflight_segments: 0,
      inflight_segment_bytes: 0,
      manifest_inflight_streams: 0,
    };
    const uploaderRuntime = uploader.getRuntimeStats?.();
    return {
      auto_tune: {
        enabled: cfg.autoTunePresetMb != null,
        requested_memory_mb: cfg.autoTuneRequestedMemoryMb,
        preset_mb: cfg.autoTunePresetMb,
        effective_memory_limit_mb: cfg.autoTuneEffectiveMemoryLimitMb,
      },
      configured_limits: {
        caches: {
          sqlite_cache_bytes: cfg.sqliteCacheBytes,
          worker_sqlite_cache_bytes: cfg.workerSqliteCacheBytes,
          index_run_memory_cache_bytes: cfg.indexRunMemoryCacheBytes,
          index_run_disk_cache_bytes: cfg.indexRunCacheMaxBytes,
          lexicon_index_cache_bytes: cfg.lexiconIndexCacheMaxBytes,
          segment_cache_bytes: cfg.segmentCacheMaxBytes,
          companion_toc_cache_bytes: cfg.searchCompanionTocCacheBytes,
          companion_section_cache_bytes: cfg.searchCompanionSectionCacheBytes,
          companion_file_cache_bytes: cfg.searchCompanionFileCacheMaxBytes,
        },
        concurrency: {
          ingest: cfg.ingestConcurrency,
          read: cfg.readConcurrency,
          search: cfg.searchConcurrency,
          async_index: cfg.asyncIndexConcurrency,
          upload: cfg.uploadConcurrency,
          index_builders: cfg.indexBuilders,
        },
        ingest: {
          max_batch_requests: cfg.ingestMaxBatchRequests,
          max_batch_bytes: cfg.ingestMaxBatchBytes,
          max_queue_requests: cfg.ingestMaxQueueRequests,
          max_queue_bytes: cfg.ingestMaxQueueBytes,
          busy_timeout_ms: cfg.ingestBusyTimeoutMs,
          local_backlog_max_bytes: cfg.localBacklogMaxBytes,
        },
        search: {
          companion_batch_segments: cfg.searchCompanionBuildBatchSegments,
          companion_yield_blocks: cfg.searchCompanionYieldBlocks,
          wal_overlay_quiet_period_ms: cfg.searchWalOverlayQuietPeriodMs,
          wal_overlay_max_bytes: cfg.searchWalOverlayMaxBytes,
        },
        segmenting: {
          segment_max_bytes: cfg.segmentMaxBytes,
          segment_target_rows: cfg.segmentTargetRows,
          segmenter_workers: cfg.segmenterWorkers,
        },
        timeouts: {
          append_request_timeout_ms: APPEND_REQUEST_TIMEOUT_MS,
          search_request_timeout_ms: SEARCH_REQUEST_TIMEOUT_MS,
          resolver_timeout_ms: HTTP_RESOLVER_TIMEOUT_MS,
          object_store_timeout_ms: cfg.objectStoreTimeoutMs,
        },
        memory: {
          pressure_limit_bytes: cfg.memoryLimitBytes,
        },
      },
      runtime: {
        memory: {
          pressure_active: memory.isOverLimit(),
          pressure_limit_bytes: memory.getLimitBytes(),
          last_rss_bytes: memory.getLastRssBytes(),
          max_rss_bytes: memory.getMaxRssBytes(),
          process: runtimeMemory.process,
          process_breakdown: runtimeMemory.process_breakdown,
          sqlite: runtimeMemory.sqlite,
          gc: runtimeMemory.gc,
          subsystems: runtimeMemory.subsystems,
          totals: runtimeMemory.totals,
          high_water: runtimeHighWater,
        },
        ingest_queue: {
          requests: ingest.getQueueStats().requests,
          bytes: ingest.getQueueStats().bytes,
          full: ingest.isQueueFull(),
        },
        local_backpressure: {
          enabled: backpressure?.enabled() ?? false,
          current_bytes: backpressure?.getCurrentBytes() ?? 0,
          max_bytes: backpressure?.getMaxBytes() ?? 0,
          over_limit: backpressure?.isOverLimit() ?? false,
        },
        uploads: {
          pending_segments: uploader.countSegmentsWaiting(),
          inflight_segments: uploaderMemory.inflight_segments,
          inflight_segment_bytes: uploaderMemory.inflight_segment_bytes,
          manifest_inflight_streams: uploaderMemory.manifest_inflight_streams,
          path: uploaderRuntime,
        },
        concurrency: {
          ingest: gateSnapshot(cfg.ingestConcurrency, ingestGate),
          read: gateSnapshot(cfg.readConcurrency, readGate),
          search: gateSnapshot(cfg.searchConcurrency, searchGate),
          async_index: gateSnapshot(cfg.asyncIndexConcurrency, asyncIndexGate),
        },
        top_streams: buildTopStreamContributors(),
      },
    };
  };
  const collectRuntimeMetrics = () => {
    const queue = ingest.getQueueStats();
    const emitGate = (name: string, configuredLimit: number, gate: ConcurrencyGate) => {
      metrics.record("tieredstore.concurrency.limit", configuredLimit, "count", { gate: name, kind: "configured" });
      metrics.record("tieredstore.concurrency.limit", gate.getLimit(), "count", { gate: name, kind: "effective" });
      metrics.record("tieredstore.concurrency.active", gate.getActive(), "count", { gate: name });
      metrics.record("tieredstore.concurrency.queued", gate.getQueued(), "count", { gate: name });
    };
    emitGate("ingest", cfg.ingestConcurrency, ingestGate);
    emitGate("read", cfg.readConcurrency, readGate);
    emitGate("search", cfg.searchConcurrency, searchGate);
    emitGate("async_index", cfg.asyncIndexConcurrency, asyncIndexGate);
    metrics.record("tieredstore.ingest.queue.capacity.requests", cfg.ingestMaxQueueRequests, "count");
    metrics.record("tieredstore.ingest.queue.capacity.bytes", cfg.ingestMaxQueueBytes, "bytes");
    metrics.record("tieredstore.upload.pending_segments", uploader.countSegmentsWaiting(), "count");
    metrics.record("tieredstore.upload.concurrency.limit", cfg.uploadConcurrency, "count");
    if (cfg.memoryLimitBytes > 0) metrics.record("process.memory.limit.bytes", cfg.memoryLimitBytes, "bytes");
    const lastRss = memory.getLastRssBytes();
    if (lastRss > 0) metrics.record("process.rss.current.bytes", lastRss, "bytes");
    const maxRss = memory.snapshotMaxRssBytes();
    if (maxRss > 0) metrics.record("process.rss.max_interval.bytes", maxRss, "bytes");
    const runtimeMemory = buildRuntimeMemorySnapshot();
    metrics.record("process.heap.total.bytes", runtimeMemory.process.heap_total_bytes, "bytes");
    metrics.record("process.heap.used.bytes", runtimeMemory.process.heap_used_bytes, "bytes");
    metrics.record("process.external.bytes", runtimeMemory.process.external_bytes, "bytes");
    metrics.record("process.array_buffers.bytes", runtimeMemory.process.array_buffers_bytes, "bytes");
    if (runtimeMemory.process_breakdown.rss_anon_bytes != null) {
      metrics.record("process.memory.rss.anon.bytes", runtimeMemory.process_breakdown.rss_anon_bytes, "bytes");
    }
    if (runtimeMemory.process_breakdown.rss_file_bytes != null) {
      metrics.record("process.memory.rss.file.bytes", runtimeMemory.process_breakdown.rss_file_bytes, "bytes");
    }
    if (runtimeMemory.process_breakdown.rss_shmem_bytes != null) {
      metrics.record("process.memory.rss.shmem.bytes", runtimeMemory.process_breakdown.rss_shmem_bytes, "bytes");
    }
    if (runtimeMemory.process_breakdown.unattributed_anon_bytes != null) {
      metrics.record("process.memory.unattributed_anon.bytes", runtimeMemory.process_breakdown.unattributed_anon_bytes, "bytes");
    }
    metrics.record("process.memory.js_managed.bytes", runtimeMemory.process_breakdown.js_managed_bytes, "bytes");
    metrics.record(
      "process.memory.js_external_non_array_buffers.bytes",
      runtimeMemory.process_breakdown.js_external_non_array_buffers_bytes,
      "bytes"
    );
    metrics.record("process.memory.unattributed.bytes", runtimeMemory.process_breakdown.unattributed_rss_bytes, "bytes");
    metrics.record("tieredstore.sqlite.memory.used.bytes", runtimeMemory.sqlite.memory_used_bytes, "bytes");
    metrics.record("tieredstore.sqlite.memory.high_water.bytes", runtimeMemory.sqlite.memory_highwater_bytes, "bytes");
    metrics.record("tieredstore.sqlite.pagecache.used", runtimeMemory.sqlite.pagecache_used_slots, "count");
    metrics.record("tieredstore.sqlite.pagecache.high_water", runtimeMemory.sqlite.pagecache_used_slots_highwater, "count");
    metrics.record("tieredstore.sqlite.pagecache.overflow.bytes", runtimeMemory.sqlite.pagecache_overflow_bytes, "bytes");
    metrics.record(
      "tieredstore.sqlite.pagecache.overflow.high_water.bytes",
      runtimeMemory.sqlite.pagecache_overflow_highwater_bytes,
      "bytes"
    );
    metrics.record("tieredstore.sqlite.malloc.count", runtimeMemory.sqlite.malloc_count, "count");
    metrics.record("tieredstore.sqlite.malloc.high_water.count", runtimeMemory.sqlite.malloc_count_highwater, "count");
    metrics.record("tieredstore.sqlite.open_connections", runtimeMemory.sqlite.open_connections, "count");
    metrics.record("tieredstore.sqlite.prepared_statements", runtimeMemory.sqlite.prepared_statements, "count");
    for (const [kind, values] of Object.entries(runtimeMemory.subsystems)) {
      if (kind === "counts") continue;
      for (const [subsystem, value] of Object.entries(values)) {
        metrics.record("tieredstore.memory.subsystem.bytes", value, "bytes", { kind, subsystem });
      }
    }
    for (const [subsystem, value] of Object.entries(runtimeMemory.subsystems.counts)) {
      metrics.record("tieredstore.memory.subsystem.count", value, "count", { subsystem });
    }
    metrics.record("tieredstore.memory.tracked.bytes", runtimeMemory.totals.heap_estimate_bytes, "bytes", { kind: "heap_estimate" });
    metrics.record("tieredstore.memory.tracked.bytes", runtimeMemory.totals.mapped_file_bytes, "bytes", { kind: "mapped_file" });
    metrics.record("tieredstore.memory.tracked.bytes", runtimeMemory.totals.disk_cache_bytes, "bytes", { kind: "disk_cache" });
    metrics.record("tieredstore.memory.tracked.bytes", runtimeMemory.totals.configured_budget_bytes, "bytes", { kind: "configured_budget" });
    metrics.record("tieredstore.memory.tracked.bytes", runtimeMemory.totals.pipeline_buffer_bytes, "bytes", { kind: "pipeline_buffer" });
    metrics.record("tieredstore.memory.tracked.bytes", runtimeMemory.totals.sqlite_runtime_bytes, "bytes", { kind: "sqlite_runtime" });
    const memLeakCounters = buildLeakCandidateCounters();
    for (const [metricName, value] of Object.entries(memLeakCounters)) {
      const unit = metricName.endsWith("_bytes") ? "bytes" : "count";
      metrics.record(metricName, value, unit);
    }
    metrics.record("process.gc.forced.count", runtimeMemory.gc.forced_gc_count, "count");
    metrics.record("process.gc.reclaimed.bytes", runtimeMemory.gc.forced_gc_reclaimed_bytes_total, "bytes", { kind: "total" });
    if (runtimeMemory.gc.last_forced_gc_reclaimed_bytes != null) {
      metrics.record("process.gc.reclaimed.bytes", runtimeMemory.gc.last_forced_gc_reclaimed_bytes, "bytes", { kind: "last" });
    }
    if (runtimeMemory.gc.last_forced_gc_at_ms != null) {
      metrics.record("process.gc.last_forced_at_ms", runtimeMemory.gc.last_forced_gc_at_ms, "count");
    }
    metrics.record("process.heap.snapshot.count", runtimeMemory.gc.heap_snapshots_written, "count");
    if (runtimeMemory.gc.last_heap_snapshot_at_ms != null) {
      metrics.record("process.heap.snapshot.last_at_ms", runtimeMemory.gc.last_heap_snapshot_at_ms, "count");
    }
    for (const [metricName, entry] of Object.entries(runtimeHighWater.process)) {
      metrics.record("process.memory.high_water.bytes", entry.value, "bytes", { metric: metricName });
    }
    for (const [metricName, entry] of Object.entries(runtimeHighWater.process_breakdown)) {
      metrics.record("process.memory.high_water.bytes", entry.value, "bytes", { metric: metricName });
    }
    for (const [metricName, entry] of Object.entries(runtimeHighWater.runtime_totals)) {
      metrics.record("tieredstore.memory.high_water.bytes", entry.value, "bytes", { kind: "runtime_total", metric: metricName });
    }
    for (const [kind, entries] of Object.entries(runtimeHighWater.runtime_bytes)) {
      for (const [metricName, entry] of Object.entries(entries)) {
        metrics.record("tieredstore.memory.high_water.bytes", entry.value, "bytes", {
          kind: "runtime_subsystem",
          subsystem_kind: kind,
          metric: metricName,
        });
      }
    }
    for (const [metricName, entry] of Object.entries(runtimeHighWater.sqlite)) {
      const unit =
        metricName.includes("bytes") || metricName.includes("memory") || metricName.includes("overflow") ? "bytes" : "count";
      metrics.record("tieredstore.sqlite.high_water", entry.value, unit, { metric: metricName });
    }
    metrics.record("process.memory.pressure", memory.isOverLimit() ? 1 : 0, "count");
    if (backpressure) {
      metrics.record("tieredstore.backpressure.current.bytes", backpressure.getCurrentBytes(), "bytes");
      metrics.record("tieredstore.backpressure.limit.bytes", backpressure.getMaxBytes(), "bytes");
      metrics.record("tieredstore.backpressure.pressure", backpressure.isOverLimit() ? 1 : 0, "count");
    }
    if (cfg.autoTunePresetMb != null) {
      metrics.record("tieredstore.auto_tune.preset_mb", cfg.autoTunePresetMb, "count");
    }
    if (cfg.autoTuneEffectiveMemoryLimitMb != null) {
      metrics.record("tieredstore.auto_tune.effective_memory_limit_mb", cfg.autoTuneEffectiveMemoryLimitMb, "count");
    }
  };
  const metricsEmitter = new MetricsEmitter(metrics, ingest, cfg.metricsFlushIntervalMs, {
    onAppended: ({ lastOffset, stream }) => {
      notifier.notify(stream, lastOffset);
      notifier.notifyDetailsChanged(stream);
    },
    collectRuntimeMetrics,
  });
  const expirySweeper = new ExpirySweeper(cfg, db);
  const streamSizeReconciler = new StreamSizeReconciler(
    db,
    store,
    runtime.segmentDiskCache,
    (stream) => notifier.notifyDetailsChanged(stream)
  );

  const metricsStreamRow = db.ensureStream(INTERNAL_METRICS_STREAM, { contentType: "application/json", profile: "metrics" });
  const metricsProfileRes = profiles.updateProfileResult(INTERNAL_METRICS_STREAM, metricsStreamRow, { kind: "metrics" });
  if (Result.isError(metricsProfileRes)) {
    throw dsError(`failed to initialize ${INTERNAL_METRICS_STREAM} profile: ${metricsProfileRes.error.message}`);
  }
  clearInternalMetricsAccelerationState(db);
  runtime.start();
  if (metricsProfileRes.value.schemaRegistry) {
    void (async () => {
      try {
        await uploadSchemaRegistry(INTERNAL_METRICS_STREAM, metricsProfileRes.value.schemaRegistry!);
        await uploader.publishManifest(INTERNAL_METRICS_STREAM);
      } catch {
        // background best-effort; next manifest publication will reconcile
      }
    })();
  }
  metricsEmitter.start();
  expirySweeper.start();
  touch.start();
  streamSizeReconciler.start();

  const buildJsonRows = (
    stream: string,
    bodyBytes: Uint8Array,
    routingKeyHeader: string | null,
    allowEmptyArray: boolean
  ): Result<{ rows: AppendRow[] }, { status: 400 | 500; message: string }> => {
    const regRes = registry.getRegistryResult(stream);
    if (Result.isError(regRes)) {
      return Result.err({ status: 500, message: regRes.error.message });
    }
    const profileRes = profiles.getProfileResult(stream);
    if (Result.isError(profileRes)) {
      return Result.err({ status: 500, message: profileRes.error.message });
    }
    const reg = regRes.value;
    const jsonIngest = resolveJsonIngestCapability(profileRes.value);
    const text = new TextDecoder().decode(bodyBytes);
    let arr: any;
    try {
      arr = JSON.parse(text);
    } catch {
      return Result.err({ status: 400, message: "invalid JSON" });
    }
    if (!Array.isArray(arr)) arr = [arr];
    if (arr.length === 0 && !allowEmptyArray) return Result.err({ status: 400, message: "empty JSON array" });
    if (reg.routingKey && routingKeyHeader) {
      return Result.err({ status: 400, message: "Stream-Key not allowed when routingKey is configured" });
    }

    const validator = reg.currentVersion > 0 ? registry.getValidatorForVersion(reg, reg.currentVersion) : null;
    if (reg.currentVersion > 0 && !validator) {
      return Result.err({ status: 500, message: "schema validator missing" });
    }

    const rows: AppendRow[] = [];
    for (const v of arr) {
      let value = v;
      let profileRoutingKey: Uint8Array | null = null;
      if (jsonIngest) {
        const preparedRes = jsonIngest.prepareRecordResult({ stream, profile: profileRes.value, value: v });
        if (Result.isError(preparedRes)) return Result.err({ status: 400, message: preparedRes.error.message });
        value = preparedRes.value.value;
        profileRoutingKey = keyBytesFromString(preparedRes.value.routingKey);
      }
      if (validator && !validator(value)) {
        const msg = validator.errors ? validator.errors.map((e) => e.message).join("; ") : "schema validation failed";
        return Result.err({ status: 400, message: msg });
      }
      const rkRes = reg.routingKey
        ? extractRoutingKey(reg, value)
        : Result.ok(routingKeyHeader != null ? keyBytesFromString(routingKeyHeader) : profileRoutingKey);
      if (Result.isError(rkRes)) return Result.err({ status: 400, message: rkRes.error.message });
      rows.push({
        routingKey: rkRes.value,
        contentType: "application/json",
        payload: new TextEncoder().encode(JSON.stringify(value)),
      });
    }
    return Result.ok({ rows });
  };

  const buildAppendRowsResult = (
    stream: string,
    bodyBytes: Uint8Array,
    contentType: string,
    routingKeyHeader: string | null,
    allowEmptyJsonArray: boolean
  ): Result<{ rows: AppendRow[] }, { status: 400 | 500; message: string }> => {
    if (isJsonContentType(contentType)) {
      return buildJsonRows(stream, bodyBytes, routingKeyHeader, allowEmptyJsonArray);
    }
    const regRes = registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ status: 500, message: regRes.error.message });
    const reg = regRes.value;
    if (reg.currentVersion > 0) return Result.err({ status: 400, message: "stream requires JSON" });
    return Result.ok({
      rows: [
        {
          routingKey: keyBytesFromString(routingKeyHeader),
          contentType,
          payload: bodyBytes,
        },
      ],
    });
  };

  const enqueueAppend = (args: {
    stream: string;
    baseAppendMs: bigint;
    rows: AppendRow[];
    contentType: string | null;
    close: boolean;
    streamSeq?: string | null;
    producer?: ProducerInfo | null;
  }) =>
    ingest.append({
      stream: args.stream,
      baseAppendMs: args.baseAppendMs,
      rows: args.rows,
      contentType: args.contentType,
      streamSeq: args.streamSeq,
      producer: args.producer,
      close: args.close,
    });

  const awaitAppendWithTimeout = async (appendPromise: Promise<AppendResult>): Promise<AppendResult | Response> => {
    const appendResult = await awaitWithCooperativeTimeout(appendPromise, APPEND_REQUEST_TIMEOUT_MS);
    return appendResult === TIMEOUT_SENTINEL ? appendTimeout() : appendResult;
  };

  const recordAppendOutcome = (args: {
    stream: string;
    lastOffset: bigint;
    appendedRows: number;
    metricsBytes: number;
    ingestedBytes: number;
    touched: boolean;
    closed: boolean;
  }): void => {
    if (args.appendedRows > 0) {
      metrics.recordAppend(args.metricsBytes, args.appendedRows);
      notifier.notify(args.stream, args.lastOffset);
      notifier.notifyDetailsChanged(args.stream);
      touch.notify(args.stream);
    }
    if (stats) {
      if (args.touched) stats.recordStreamTouched(args.stream);
      if (args.appendedRows > 0) stats.recordIngested(args.ingestedBytes);
    }
    if (args.closed) {
      notifier.notifyDetailsChanged(args.stream);
      notifier.notifyClose(args.stream);
    }
  };

  const decodeJsonRecords = (
    stream: string,
    records: Array<{ offset: bigint; payload: Uint8Array }>
  ): Result<{ values: any[] }, { status: 400 | 500; message: string }> => {
    const values: any[] = [];
    for (const r of records) {
      const valueRes = decodeJsonPayloadResult(registry, stream, r.offset, r.payload);
      if (Result.isError(valueRes)) return valueRes;
      values.push(valueRes.value);
    }
    return Result.ok({ values });
  };

  const encodeStoredJsonArrayResult = (
    stream: string,
    records: Array<{ payload: Uint8Array }>
  ): Result<Buffer | null, { status: 400 | 500; message: string }> => {
    const regRes = registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ status: 500, message: regRes.error.message });
    if (regRes.value.currentVersion !== 0) return Result.ok(null);
    const parts: Buffer[] = [Buffer.from("[")];
    for (let i = 0; i < records.length; i++) {
      if (i > 0) parts.push(Buffer.from(","));
      const payload = records[i]!.payload;
      parts.push(Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength));
    }
    parts.push(Buffer.from("]"));
    return Result.ok(Buffer.concat(parts));
  };

  const buildStreamSummary = (
    stream: string,
    row: StreamRow,
    profileKind: string,
    segmentCount: number,
    uploadedSegmentCount: number
  ) => ({
    name: stream,
    content_type: normalizeContentType(row.content_type) ?? row.content_type,
    profile: profileKind,
    created_at: timestampToIsoString(row.created_at_ms),
    updated_at: timestampToIsoString(row.updated_at_ms),
    expires_at: timestampToIsoString(row.expires_at_ms),
    ttl_seconds: row.ttl_seconds,
    stream_seq: row.stream_seq,
    closed: row.closed !== 0,
    epoch: row.epoch,
    next_offset: row.next_offset.toString(),
    sealed_through: row.sealed_through.toString(),
    uploaded_through: row.uploaded_through.toString(),
    segment_count: segmentCount,
    uploaded_segment_count: uploadedSegmentCount,
    pending_rows: row.pending_rows.toString(),
    pending_bytes: row.pending_bytes.toString(),
    total_size_bytes: row.logical_size_bytes.toString(),
    wal_rows: row.wal_rows.toString(),
    wal_bytes: row.wal_bytes.toString(),
    last_append_at: timestampToIsoString(row.last_append_ms),
    last_segment_cut_at: timestampToIsoString(row.last_segment_cut_ms),
  });

  const buildIndexLagMs = (stream: string, headRow: StreamRow, coveredSegmentCount: number): string | null => {
    if (coveredSegmentCount <= 0) return null;
    const coveredLastAppendMs = db.getSegmentLastAppendMsFromMeta(stream, coveredSegmentCount - 1);
    if (coveredLastAppendMs == null) return null;
    const lagMs = headRow.last_append_ms > coveredLastAppendMs ? headRow.last_append_ms - coveredLastAppendMs : 0n;
    return lagMs.toString();
  };

  const buildStorageBreakdown = (
    stream: string,
    row: StreamRow,
    currentCompanionRows: Array<{
      sections_json: string;
      section_sizes_json: string;
      size_bytes: number;
    }>,
    indexStatus: any,
    uploadedSegmentCount: number,
    uploadedSegmentBytes: bigint,
    pendingSealedSegmentBytes: bigint,
    manifest: {
      generation: number;
      uploaded_generation: number;
      last_uploaded_at_ms: bigint | null;
      last_uploaded_etag: string | null;
      last_uploaded_size_bytes: bigint | null;
    },
    routingIndexStorage: { object_count: number; bytes: bigint },
    routingLexiconStorage: { object_count: number; bytes: bigint },
    secondaryIndexStorage: Map<string, { index_name: string; object_count: number; bytes: bigint }>,
    companionStorage: { object_count: number; bytes: bigint }
  ) => {
    const schemaRow = db.getSchemaRegistry(stream);
    const localStorageUsage = {
      segment_cache_bytes: 0,
      routing_index_cache_bytes: 0,
      exact_index_cache_bytes: 0,
      lexicon_index_cache_bytes: 0,
      companion_cache_bytes: 0,
      ...(getLocalStorageUsage?.(stream) ?? {}),
    };
    const sqliteSharedBytes = BigInt(db.getSqliteSharedTotalBytes());
    const exactIndexBytes = indexStatus.exact_indexes.reduce((sum: bigint, entry: any) => sum + BigInt(entry.bytes_at_rest ?? 0), 0n);
    const familyBytes = new Map<string, bigint>();
    for (const row of currentCompanionRows) {
      const sizes = parseCompanionSectionSizes(row.section_sizes_json);
      for (const [kind, size] of Object.entries(sizes)) {
        familyBytes.set(kind, (familyBytes.get(kind) ?? 0n) + BigInt(size));
      }
    }
    return {
      object_storage: {
        total_bytes: (
          uploadedSegmentBytes +
          routingIndexStorage.bytes +
          routingLexiconStorage.bytes +
          exactIndexBytes +
          companionStorage.bytes +
          (manifest.last_uploaded_size_bytes ?? 0n) +
          (schemaRow?.uploaded_size_bytes ?? 0n)
        ).toString(),
        segments_bytes: uploadedSegmentBytes.toString(),
        indexes_bytes: (routingIndexStorage.bytes + routingLexiconStorage.bytes + exactIndexBytes + companionStorage.bytes).toString(),
        manifest_and_meta_bytes: ((manifest.last_uploaded_size_bytes ?? 0n) + (schemaRow?.uploaded_size_bytes ?? 0n)).toString(),
        manifest_bytes: (manifest.last_uploaded_size_bytes ?? 0n).toString(),
        schema_registry_bytes: (schemaRow?.uploaded_size_bytes ?? 0n).toString(),
        segment_object_count: uploadedSegmentCount,
        routing_index_object_count: routingIndexStorage.object_count,
        routing_lexicon_object_count: routingLexiconStorage.object_count,
        exact_index_object_count: indexStatus.exact_indexes.reduce((sum: number, entry: any) => sum + Number(entry.object_count ?? 0), 0),
        bundled_companion_object_count: companionStorage.object_count,
      },
      local_storage: {
        total_bytes: (
          row.wal_bytes +
          pendingSealedSegmentBytes +
          BigInt(localStorageUsage.segment_cache_bytes) +
          BigInt(localStorageUsage.routing_index_cache_bytes) +
          BigInt(localStorageUsage.exact_index_cache_bytes) +
          BigInt(localStorageUsage.lexicon_index_cache_bytes) +
          BigInt(localStorageUsage.companion_cache_bytes)
        ).toString(),
        wal_retained_bytes: row.wal_bytes.toString(),
        pending_tail_bytes: row.pending_bytes.toString(),
        pending_sealed_segment_bytes: pendingSealedSegmentBytes.toString(),
        segment_cache_bytes: String(localStorageUsage.segment_cache_bytes),
        routing_index_cache_bytes: String(localStorageUsage.routing_index_cache_bytes),
        exact_index_cache_bytes: String(localStorageUsage.exact_index_cache_bytes),
        lexicon_index_cache_bytes: String(localStorageUsage.lexicon_index_cache_bytes),
        companion_cache_bytes: String(localStorageUsage.companion_cache_bytes),
        sqlite_shared_total_bytes: sqliteSharedBytes.toString(),
      },
      companion_families: {
        col_bytes: String(familyBytes.get("col") ?? 0n),
        fts_bytes: String(familyBytes.get("fts") ?? 0n),
        agg_bytes: String(familyBytes.get("agg") ?? 0n),
        mblk_bytes: String(familyBytes.get("mblk") ?? 0n),
      },
    };
  };

  const buildObjectStoreRequestSummary = (stream: string) => {
    const summary = db.getObjectStoreRequestSummaryByHash(streamHash16Hex(stream));
    return {
      puts: summary.puts.toString(),
      reads: summary.reads.toString(),
      gets: summary.gets.toString(),
      heads: summary.heads.toString(),
      lists: summary.lists.toString(),
      deletes: summary.deletes.toString(),
      by_artifact: summary.by_artifact.map((entry) => ({
        artifact: entry.artifact,
        puts: entry.puts.toString(),
        gets: entry.gets.toString(),
        heads: entry.heads.toString(),
        lists: entry.lists.toString(),
        deletes: entry.deletes.toString(),
        reads: entry.reads.toString(),
      })),
    };
  };

  const buildIndexStatus = (stream: string, row: StreamRow, reg: SchemaRegistry, profileKind: string) => {
    const segmentSummary = db.getStreamSegmentStorageSummary(stream);
    const segmentCount = segmentSummary.segmentCount;
    const uploadedSegmentCount = row.uploaded_segment_count;
    const manifest = db.getManifestRow(stream);

    const routingState = db.getIndexState(stream);
    const routingStorage = db.getRoutingIndexStorage(stream);
    const routingLexiconState = db.getLexiconIndexState(stream, "routing_key", "");
    const routingLexiconStorage =
      db
        .getLexiconIndexStorage(stream)
        .find((entry) => entry.source_kind === "routing_key" && entry.source_name === "") ?? { object_count: 0, bytes: 0n };
    const secondaryIndexStorage = new Map(db.getSecondaryIndexStorage(stream).map((entry) => [entry.index_name, entry]));

    const exactIndexes = configuredExactIndexes(reg.search).map(({ name, kind, configHash }) => {
      const state = db.getSecondaryIndexState(stream, name);
      const configMatches = state?.config_hash === configHash;
      const indexedSegmentCount = configMatches ? (state?.indexed_through ?? 0) : 0;
      const storage = secondaryIndexStorage.get(name);
      return {
        name,
        kind,
        indexed_segment_count: indexedSegmentCount,
        lag_segments: Math.max(0, uploadedSegmentCount - indexedSegmentCount),
        lag_ms: buildIndexLagMs(stream, row, indexedSegmentCount),
        bytes_at_rest: String(storage?.bytes ?? 0n),
        object_count: storage?.object_count ?? 0,
        active_run_count: db.countActiveSecondaryIndexRuns(stream, name),
        retired_run_count: db.countRetiredSecondaryIndexRuns(stream, name),
        fully_indexed_uploaded_segments: configMatches && indexedSegmentCount >= uploadedSegmentCount,
        stale_configuration: !configMatches,
        updated_at: timestampToIsoString(state?.updated_at_ms ?? null),
      };
    });

    const desiredCompanionPlan = buildDesiredSearchCompanionPlan(reg);
    const configuredFamilies = configuredSearchFamilies(reg.search);
    let desiredIndexPlanGeneration = 0;
    let currentCompanionRows: SearchSegmentCompanionRow[] = [];
    let currentCompanionBytes = 0n;
    let companionPlanRow: SearchCompanionPlanRow | null = null;
    if (configuredFamilies.length > 0) {
      const desiredCompanionHash = hashSearchCompanionPlan(desiredCompanionPlan);
      companionPlanRow = db.getSearchCompanionPlan(stream);
      desiredIndexPlanGeneration = companionPlanRow
        ? companionPlanRow.plan_hash === desiredCompanionHash
          ? companionPlanRow.generation
          : companionPlanRow.generation + 1
        : 1;
      const companionRows = db.listSearchSegmentCompanions(stream);
      currentCompanionRows = companionRows.filter((entry) => entry.plan_generation === desiredIndexPlanGeneration);
      currentCompanionBytes = currentCompanionRows.reduce((sum, entry) => sum + BigInt(entry.size_bytes), 0n);
    }
    const searchFamilies = configuredFamilies.map(({ family, fields }) => {
      const coveredSegmentCount = currentCompanionRows.filter((row) => parseCompanionSections(row.sections_json).has(family)).length;
      const contiguousCoveredCount = contiguousCoveredSegmentCount(currentCompanionRows, family);
      let familyBytes = 0n;
      let familyObjectCount = 0;
      for (const row of currentCompanionRows) {
        const size = parseCompanionSectionSizes(row.section_sizes_json)[family];
        if (size == null) continue;
        familyBytes += BigInt(size);
        familyObjectCount += 1;
      }
      return {
        family,
        fields,
        plan_generation: desiredIndexPlanGeneration,
        covered_segment_count: coveredSegmentCount,
        contiguous_covered_segment_count: contiguousCoveredCount,
        lag_segments: Math.max(0, uploadedSegmentCount - contiguousCoveredCount),
        lag_ms: buildIndexLagMs(stream, row, contiguousCoveredCount),
        bytes_at_rest: familyBytes.toString(),
        object_count: familyObjectCount,
        stale_segment_count: Math.max(0, uploadedSegmentCount - coveredSegmentCount),
        fully_indexed_uploaded_segments: coveredSegmentCount >= uploadedSegmentCount,
        updated_at: timestampToIsoString(companionPlanRow?.updated_at_ms ?? null),
      };
    });

    return {
      stream,
      profile: profileKind,
      desired_index_plan_generation: desiredIndexPlanGeneration,
      segments: {
        total_count: segmentCount,
        uploaded_count: uploadedSegmentCount,
      },
      manifest: {
        generation: manifest.generation,
        uploaded_generation: manifest.uploaded_generation,
        last_uploaded_at: timestampToIsoString(manifest.last_uploaded_at_ms),
        last_uploaded_etag: manifest.last_uploaded_etag,
        last_uploaded_size_bytes: manifest.last_uploaded_size_bytes?.toString() ?? null,
      },
      routing_key_index: {
        configured: reg.routingKey != null,
        indexed_segment_count: routingState?.indexed_through ?? 0,
        lag_segments: Math.max(0, uploadedSegmentCount - (routingState?.indexed_through ?? 0)),
        lag_ms: buildIndexLagMs(stream, row, routingState?.indexed_through ?? 0),
        bytes_at_rest: routingStorage.bytes.toString(),
        object_count: routingStorage.object_count,
        active_run_count: db.countActiveIndexRuns(stream),
        retired_run_count: db.countRetiredIndexRuns(stream),
        fully_indexed_uploaded_segments: reg.routingKey == null ? true : (routingState?.indexed_through ?? 0) >= uploadedSegmentCount,
        updated_at: timestampToIsoString(routingState?.updated_at_ms ?? null),
      },
      routing_key_lexicon: {
        configured: reg.routingKey != null,
        indexed_segment_count: routingLexiconState?.indexed_through ?? 0,
        lag_segments: Math.max(0, uploadedSegmentCount - (routingLexiconState?.indexed_through ?? 0)),
        lag_ms: buildIndexLagMs(stream, row, routingLexiconState?.indexed_through ?? 0),
        bytes_at_rest: routingLexiconStorage.bytes.toString(),
        object_count: routingLexiconStorage.object_count,
        active_run_count: db.countActiveLexiconIndexRuns(stream, "routing_key", ""),
        retired_run_count: db.countRetiredLexiconIndexRuns(stream, "routing_key", ""),
        fully_indexed_uploaded_segments: reg.routingKey == null ? true : (routingLexiconState?.indexed_through ?? 0) >= uploadedSegmentCount,
        updated_at: timestampToIsoString(routingLexiconState?.updated_at_ms ?? null),
      },
      exact_indexes: exactIndexes,
      bundled_companions: {
        object_count: currentCompanionRows.length,
        bytes_at_rest: currentCompanionBytes.toString(),
        fully_indexed_uploaded_segments: currentCompanionRows.length >= uploadedSegmentCount,
      },
      search_families: searchFamilies,
      current_companion_rows: currentCompanionRows,
      _segment_count: segmentCount,
      _uploaded_segment_count: uploadedSegmentCount,
      _uploaded_segment_bytes: segmentSummary.uploadedSegmentBytes,
      _pending_sealed_segment_bytes: segmentSummary.pendingSealedSegmentBytes,
      _manifest_row: manifest,
      _routing_storage: routingStorage,
      _routing_lexicon_storage: routingLexiconStorage,
      _secondary_index_storage: secondaryIndexStorage,
      _companion_storage: db.getBundledCompanionStorage(stream),
    };
  };

  type DetailsSnapshot = { etag: string; body: string; version: bigint };

  const buildDetailsSnapshotResult = (
    stream: string,
    mode: "details" | "index_status"
  ): Result<DetailsSnapshot, { status: 404 | 500; message: string }> => {
    const srow = db.getStream(stream);
    if (!srow || db.isDeleted(srow)) return Result.err({ status: 404, message: "not_found" });
    if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return Result.err({ status: 404, message: "stream expired" });

    const regRes = registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ status: 500, message: regRes.error.message });
    const profileRes = profiles.getProfileResourceResult(stream, srow);
    if (Result.isError(profileRes)) return Result.err({ status: 500, message: profileRes.error.message });

    const profileKind = profileRes.value.profile.kind;
    const indexStatus = buildIndexStatus(stream, srow, regRes.value, profileKind);
    const storage = buildStorageBreakdown(
      stream,
      srow,
      indexStatus.current_companion_rows,
      indexStatus,
      indexStatus._uploaded_segment_count,
      indexStatus._uploaded_segment_bytes,
      indexStatus._pending_sealed_segment_bytes,
      indexStatus._manifest_row,
      indexStatus._routing_storage,
      indexStatus._routing_lexicon_storage,
      indexStatus._secondary_index_storage,
      indexStatus._companion_storage
    );
    const objectStoreRequests = buildObjectStoreRequestSummary(stream);
    delete (indexStatus as any).current_companion_rows;
    delete (indexStatus as any)._segment_count;
    delete (indexStatus as any)._uploaded_segment_count;
    delete (indexStatus as any)._uploaded_segment_bytes;
    delete (indexStatus as any)._pending_sealed_segment_bytes;
    delete (indexStatus as any)._manifest_row;
    delete (indexStatus as any)._routing_storage;
    delete (indexStatus as any)._routing_lexicon_storage;
    delete (indexStatus as any)._secondary_index_storage;
    delete (indexStatus as any)._companion_storage;
    const payload =
      mode === "index_status"
        ? indexStatus
        : {
            stream: buildStreamSummary(stream, srow, profileKind, indexStatus.segments.total_count, indexStatus.segments.uploaded_count),
            profile: profileRes.value,
            schema: regRes.value,
            index_status: indexStatus,
            storage,
            object_store_requests: objectStoreRequests,
          };
    const body = JSON.stringify(payload);
    const version = notifier.currentDetailsVersion(stream);
    return Result.ok({
      etag: weakEtag(mode, body),
      body,
      version,
    });
  };

  let closing = false;
  const fetch = async (req: Request): Promise<Response> => {
    if (closing) {
      return unavailable();
    }
    const requestAbortController = new AbortController();
    const abortFromClient = () => requestAbortController.abort(req.signal.reason);
    let timedOut = false;
    if (req.signal.aborted) requestAbortController.abort(req.signal.reason);
    else req.signal.addEventListener("abort", abortFromClient, { once: true });
    try {
      const runWithGate = async <T>(gate: ConcurrencyGate, fn: () => Promise<T>): Promise<T> =>
        gate.run(fn, requestAbortController.signal);
      const runForeground = async <T>(fn: () => Promise<T>): Promise<T> => {
        const leaveForeground = foregroundActivity.enter();
        try {
          return await fn();
        } finally {
          leaveForeground();
        }
      };
      const runForegroundWithGate = async <T>(gate: ConcurrencyGate, fn: () => Promise<T>): Promise<T> =>
        runForeground(() => runWithGate(gate, fn));
      const requestPromise = (async (): Promise<Response> => {
      let url: URL;
      try {
        url = new URL(req.url, "http://localhost");
      } catch {
        return badRequest("invalid url");
      }
      const path = url.pathname;

      if (path === "/health") {
        return json(200, { ok: true });
      }
      if (path === "/metrics") {
        return json(200, metrics.snapshot());
      }
      if (req.method === "GET" && path === "/v1/server/_details") {
        return json(200, buildServerDetails());
      }
      if (req.method === "GET" && path === "/v1/server/_mem") {
        return json(200, buildServerMem());
      }

      // /v1/streams
      if (req.method === "GET" && path === "/v1/streams") {
        const limit = Number(url.searchParams.get("limit") ?? "100");
        const offset = Number(url.searchParams.get("offset") ?? "0");
        const rows = db.listStreams(Math.max(0, Math.min(limit, 1000)), Math.max(0, offset));
        const out = [];
        for (const r of rows) {
          const profileRes = profiles.getProfileResult(r.stream, r);
          if (Result.isError(profileRes)) return internalError("invalid stream profile");
          const profile = profileRes.value;
          out.push({
            name: r.stream,
            created_at: new Date(Number(r.created_at_ms)).toISOString(),
            expires_at: r.expires_at_ms == null ? null : new Date(Number(r.expires_at_ms)).toISOString(),
            epoch: r.epoch,
            next_offset: r.next_offset.toString(),
            sealed_through: r.sealed_through.toString(),
            uploaded_through: r.uploaded_through.toString(),
            profile: profile.kind,
          });
        }
        return json(200, out);
      }

      // /v1/stream/:name[/_schema|/_profile|/_details|/_index_status] (accept encoded or raw slashes in name)
      const streamPrefix = "/v1/stream/";
      if (path.startsWith(streamPrefix)) {
        const rawRest = path.slice(streamPrefix.length);
        const rest = rawRest.replace(/\/+$/, "");
        if (rest.length === 0) return badRequest("missing stream name");
        const segments = rest.split("/");
        let isSchema = false;
        let isProfile = false;
        let isSearch = false;
        let isAggregate = false;
        let isDetails = false;
        let isIndexStatus = false;
        let isRoutingKeys = false;
        let pathKeyParam: string | null = null;
        let touchMode: StreamTouchRoute | null = null;
        if (segments[segments.length - 1] === "_schema") {
          isSchema = true;
          segments.pop();
        } else if (segments[segments.length - 1] === "_profile") {
          isProfile = true;
          segments.pop();
        } else if (segments[segments.length - 1] === "_search") {
          isSearch = true;
          segments.pop();
        } else if (segments[segments.length - 1] === "_aggregate") {
          isAggregate = true;
          segments.pop();
        } else if (segments[segments.length - 1] === "_details") {
          isDetails = true;
          segments.pop();
        } else if (segments[segments.length - 1] === "_index_status") {
          isIndexStatus = true;
          segments.pop();
        } else if (segments[segments.length - 1] === "_routing_keys") {
          isRoutingKeys = true;
          segments.pop();
        } else if (
          segments.length >= 3 &&
          segments[segments.length - 3] === "touch" &&
          segments[segments.length - 2] === "templates" &&
          segments[segments.length - 1] === "activate"
        ) {
          touchMode = { kind: "templates_activate" };
          segments.splice(segments.length - 3, 3);
        } else if (segments.length >= 2 && segments[segments.length - 2] === "touch" && segments[segments.length - 1] === "meta") {
          touchMode = { kind: "meta" };
          segments.splice(segments.length - 2, 2);
        } else if (segments.length >= 2 && segments[segments.length - 2] === "touch" && segments[segments.length - 1] === "wait") {
          touchMode = { kind: "wait" };
          segments.splice(segments.length - 2, 2);
        } else if (segments.length >= 2 && segments[segments.length - 2] === "pk") {
          pathKeyParam = decodeURIComponent(segments[segments.length - 1]);
          segments.splice(segments.length - 2, 2);
        }
        const streamPart = segments.join("/");
        if (streamPart.length === 0) return badRequest("missing stream name");
        const stream = decodeURIComponent(streamPart);

        if (isSchema) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          if (req.method === "GET") {
            const regRes = registry.getRegistryResult(stream);
            if (Result.isError(regRes)) return schemaReadErrorResponse(regRes.error);
            return json(200, regRes.value);
          }
          if (req.method === "POST") {
            let body: unknown;
            try {
              body = await req.json();
            } catch {
              return badRequest("schema update must be valid JSON");
            }
            const updateRes = parseSchemaUpdateResult(body);
            if (Result.isError(updateRes)) return badRequest(updateRes.error.message);
            const update = updateRes.value;
            if (update.schema === undefined && update.routingKey !== undefined && update.search === undefined) {
              const regRes = registry.updateRoutingKeyResult(stream, update.routingKey ?? null);
              if (Result.isError(regRes)) return schemaMutationErrorResponse(regRes.error);
              try {
                await uploadSchemaRegistry(stream, regRes.value);
              } catch {
                return json(500, { error: { code: "internal", message: "schema upload failed" } });
              }
              indexer?.enqueue(stream);
              notifier.notifyDetailsChanged(stream);
              return json(200, regRes.value);
            }
            if (update.schema === undefined && update.search !== undefined && update.routingKey === undefined) {
              const regRes = registry.updateSearchResult(stream, update.search ?? null);
              if (Result.isError(regRes)) return schemaMutationErrorResponse(regRes.error);
              try {
                await uploadSchemaRegistry(stream, regRes.value);
              } catch {
                return json(500, { error: { code: "internal", message: "schema upload failed" } });
              }
              indexer?.enqueue(stream);
              notifier.notifyDetailsChanged(stream);
              return json(200, regRes.value);
            }
            const regRes = registry.updateRegistryResult(stream, srow, {
              schema: update.schema,
              lens: update.lens,
              routingKey: update.routingKey ?? undefined,
              search: update.search,
            });
            if (Result.isError(regRes)) return schemaMutationErrorResponse(regRes.error);
            try {
              await uploadSchemaRegistry(stream, regRes.value);
            } catch {
              return json(500, { error: { code: "internal", message: "schema upload failed" } });
            }
            indexer?.enqueue(stream);
            notifier.notifyDetailsChanged(stream);
            return json(200, regRes.value);
          }
          return badRequest("unsupported method");
        }

        if (isProfile) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          if (req.method === "GET") {
            const profileRes = profiles.getProfileResourceResult(stream, srow);
            if (Result.isError(profileRes)) return internalError("invalid stream profile");
            return json(200, profileRes.value);
          }

          if (req.method === "POST") {
            let body: any;
            try {
              body = await req.json();
            } catch {
              return badRequest("profile update must be valid JSON");
            }
            const nextProfileRes = parseProfileUpdateResult(body);
            if (Result.isError(nextProfileRes)) return badRequest(nextProfileRes.error.message);
            const profileRes = profiles.updateProfileResult(stream, srow, nextProfileRes.value);
            if (Result.isError(profileRes)) return badRequest(profileRes.error.message);
            try {
              if (profileRes.value.schemaRegistry) {
                await uploadSchemaRegistry(stream, profileRes.value.schemaRegistry);
              }
              await uploader.publishManifest(stream);
            } catch {
              return json(500, { error: { code: "internal", message: "profile upload failed" } });
            }
            indexer?.enqueue(stream);
            notifier.notifyDetailsChanged(stream);
            return json(200, profileRes.value.resource);
          }

          return badRequest("unsupported method");
        }

        if (isDetails || isIndexStatus) {
          if (req.method !== "GET") return badRequest("unsupported method");
          const liveParam = url.searchParams.get("live") ?? "";
          let longPoll = false;
          if (liveParam === "" || liveParam === "false" || liveParam === "0") longPoll = false;
          else if (liveParam === "long-poll" || liveParam === "true" || liveParam === "1") longPoll = true;
          else return badRequest("invalid live mode");

          const timeout = url.searchParams.get("timeout") ?? url.searchParams.get("timeout_ms");
          let timeoutMs: number | null = null;
          if (timeout) {
            if (/^[0-9]+$/.test(timeout)) {
              timeoutMs = Number(timeout);
            } else {
              const timeoutRes = parseDurationMsResult(timeout);
              if (Result.isError(timeoutRes)) return badRequest("invalid timeout");
              timeoutMs = timeoutRes.value;
            }
          }

          const loadSnapshot = (): Response | DetailsSnapshot => {
            const snapshotRes = buildDetailsSnapshotResult(stream, isIndexStatus ? "index_status" : "details");
            if (Result.isError(snapshotRes)) {
              if (snapshotRes.error.status === 404) {
                return snapshotRes.error.message === "stream expired" ? notFound("stream expired") : notFound();
              }
              return internalError(snapshotRes.error.message);
            }
            return snapshotRes.value;
          };

          let snapshotOrResponse = loadSnapshot();
          if (snapshotOrResponse instanceof Response) return snapshotOrResponse;
          let snapshot = snapshotOrResponse;
          const ifNoneMatch = req.headers.get("if-none-match");

          if (!longPoll) {
            if (ifNoneMatch && ifNoneMatch === snapshot.etag) {
              return new Response(null, {
                status: 304,
                headers: withNosniff({ "cache-control": "no-store", etag: snapshot.etag }),
              });
            }
            return new Response(snapshot.body, {
              status: 200,
              headers: withNosniff({
                "content-type": "application/json; charset=utf-8",
                "cache-control": "no-store",
                etag: snapshot.etag,
              }),
            });
          }

          if (!ifNoneMatch || ifNoneMatch !== snapshot.etag) {
            return new Response(snapshot.body, {
              status: 200,
              headers: withNosniff({
                "content-type": "application/json; charset=utf-8",
                "cache-control": "no-store",
                etag: snapshot.etag,
              }),
            });
          }

          const deadline = Date.now() + (timeoutMs ?? 3000);
          while (ifNoneMatch === snapshot.etag) {
            const remaining = deadline - Date.now();
            if (remaining <= 0) {
              return new Response(null, {
                status: 304,
                headers: withNosniff({ "cache-control": "no-store", etag: snapshot.etag }),
              });
            }
            await notifier.waitForDetailsChange(stream, snapshot.version, remaining, req.signal);
            if (req.signal.aborted) return new Response(null, { status: 204 });
            snapshotOrResponse = loadSnapshot();
            if (snapshotOrResponse instanceof Response) return snapshotOrResponse;
            snapshot = snapshotOrResponse;
          }

          return new Response(snapshot.body, {
            status: 200,
            headers: withNosniff({
              "content-type": "application/json; charset=utf-8",
              "cache-control": "no-store",
              etag: snapshot.etag,
            }),
          });
        }

        if (isRoutingKeys) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");
          if (req.method !== "GET") return badRequest("unsupported method");
          const regRes = registry.getRegistryResult(stream);
          if (Result.isError(regRes)) return internalError();
          if (regRes.value.routingKey == null) return badRequest("routing key not configured");
          const limitRaw = url.searchParams.get("limit");
          const limit = limitRaw == null ? 100 : Number(limitRaw);
          if (!Number.isFinite(limit) || limit <= 0 || !Number.isInteger(limit)) return badRequest("invalid limit");
          const after = url.searchParams.get("after");
          const listRes = indexer?.listRoutingKeysResult
            ? await runForeground(() => indexer.listRoutingKeysResult!(stream, after, limit))
            : Result.err({ kind: "invalid_lexicon_index", message: "routing key lexicon unavailable" });
          if (Result.isError(listRes)) return internalError(listRes.error.message);
          return json(200, {
            stream,
            source: {
              kind: "routing_key",
              name: "",
            },
            took_ms: listRes.value.tookMs,
            coverage: {
              complete: listRes.value.coverage.complete,
              indexed_segments: listRes.value.coverage.indexedSegments,
              scanned_uploaded_segments: listRes.value.coverage.scannedUploadedSegments,
              scanned_local_segments: listRes.value.coverage.scannedLocalSegments,
              scanned_wal_rows: listRes.value.coverage.scannedWalRows,
              possible_missing_uploaded_segments: listRes.value.coverage.possibleMissingUploadedSegments,
              possible_missing_local_segments: listRes.value.coverage.possibleMissingLocalSegments,
            },
            timing: {
              lexicon_run_get_ms: listRes.value.timing.lexiconRunGetMs,
              lexicon_decode_ms: listRes.value.timing.lexiconDecodeMs,
              lexicon_enumerate_ms: listRes.value.timing.lexiconEnumerateMs,
              lexicon_merge_ms: listRes.value.timing.lexiconMergeMs,
              fallback_scan_ms: listRes.value.timing.fallbackScanMs,
              fallback_segment_get_ms: listRes.value.timing.fallbackSegmentGetMs,
              fallback_wal_scan_ms: listRes.value.timing.fallbackWalScanMs,
              lexicon_runs_loaded: listRes.value.timing.lexiconRunsLoaded,
            },
            keys: listRes.value.keys,
            next_after: listRes.value.nextAfter,
          });
        }

        if (isSearch) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          const regRes = registry.getRegistryResult(stream);
          if (Result.isError(regRes)) return internalError();

          const respondSearch = async (requestBody: unknown, fromQuery: boolean): Promise<Response> => {
            const requestRes = fromQuery
              ? parseSearchRequestQueryResult(regRes.value, url.searchParams)
              : parseSearchRequestBodyResult(regRes.value, requestBody);
            if (Result.isError(requestRes)) return badRequest(requestRes.error.message);
            const request = {
              ...requestRes.value,
              timeoutMs: clampSearchRequestTimeoutMs(requestRes.value.timeoutMs),
            };
            const searchRes = await runForegroundWithGate(searchGate, () => reader.searchResult({ stream, request }));
            if (Result.isError(searchRes)) return readerErrorResponse(searchRes.error);
            const status = searchRes.value.timedOut ? 408 : 200;
            return json(status, {
              stream,
              snapshot_end_offset: searchRes.value.snapshotEndOffset,
              took_ms: searchRes.value.tookMs,
              timed_out: searchRes.value.timedOut,
              timeout_ms: searchRes.value.timeoutMs,
              coverage: {
                mode: searchRes.value.coverage.mode,
                complete: searchRes.value.coverage.complete,
                stream_head_offset: searchRes.value.coverage.streamHeadOffset,
                visible_through_offset: searchRes.value.coverage.visibleThroughOffset,
                visible_through_primary_timestamp_max: searchRes.value.coverage.visibleThroughPrimaryTimestampMax,
                oldest_omitted_append_at: searchRes.value.coverage.oldestOmittedAppendAt,
                possible_missing_events_upper_bound: searchRes.value.coverage.possibleMissingEventsUpperBound,
                possible_missing_uploaded_segments: searchRes.value.coverage.possibleMissingUploadedSegments,
                possible_missing_sealed_rows: searchRes.value.coverage.possibleMissingSealedRows,
                possible_missing_wal_rows: searchRes.value.coverage.possibleMissingWalRows,
                indexed_segments: searchRes.value.coverage.indexedSegments,
                indexed_segment_time_ms: searchRes.value.coverage.indexedSegmentTimeMs,
                fts_section_get_ms: searchRes.value.coverage.ftsSectionGetMs,
                fts_decode_ms: searchRes.value.coverage.ftsDecodeMs,
                fts_clause_estimate_ms: searchRes.value.coverage.ftsClauseEstimateMs,
                scanned_segments: searchRes.value.coverage.scannedSegments,
                scanned_segment_time_ms: searchRes.value.coverage.scannedSegmentTimeMs,
                scanned_tail_docs: searchRes.value.coverage.scannedTailDocs,
                scanned_tail_time_ms: searchRes.value.coverage.scannedTailTimeMs,
                exact_candidate_time_ms: searchRes.value.coverage.exactCandidateTimeMs,
                index_families_used: searchRes.value.coverage.indexFamiliesUsed,
              },
              total: searchRes.value.total,
              hits: searchRes.value.hits,
              next_search_after: searchRes.value.nextSearchAfter,
            }, searchResponseHeaders(searchRes.value));
          };

          if (req.method === "GET") {
            return respondSearch(null, true);
          }

          if (req.method === "POST") {
            let body: unknown;
            try {
              body = await req.json();
            } catch {
              return badRequest("search request must be valid JSON");
            }
            return respondSearch(body, false);
          }

          return badRequest("unsupported method");
        }

        if (isAggregate) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");
          if (req.method !== "POST") return badRequest("unsupported method");

          const regRes = registry.getRegistryResult(stream);
          if (Result.isError(regRes)) return internalError();

          let body: unknown;
          try {
            body = await req.json();
          } catch {
            return badRequest("aggregate request must be valid JSON");
          }

          const requestRes = parseAggregateRequestBodyResult(regRes.value, body);
          if (Result.isError(requestRes)) return badRequest(requestRes.error.message);
          const aggregateRes = await runForegroundWithGate(searchGate, () => reader.aggregateResult({ stream, request: requestRes.value }));
          if (Result.isError(aggregateRes)) return readerErrorResponse(aggregateRes.error);
          return json(200, {
            stream,
            rollup: aggregateRes.value.rollup,
            from: aggregateRes.value.from,
            to: aggregateRes.value.to,
            interval: aggregateRes.value.interval,
              coverage: {
                mode: aggregateRes.value.coverage.mode,
                complete: aggregateRes.value.coverage.complete,
                stream_head_offset: aggregateRes.value.coverage.streamHeadOffset,
                visible_through_offset: aggregateRes.value.coverage.visibleThroughOffset,
                visible_through_primary_timestamp_max: aggregateRes.value.coverage.visibleThroughPrimaryTimestampMax,
                oldest_omitted_append_at: aggregateRes.value.coverage.oldestOmittedAppendAt,
                possible_missing_events_upper_bound: aggregateRes.value.coverage.possibleMissingEventsUpperBound,
                possible_missing_uploaded_segments: aggregateRes.value.coverage.possibleMissingUploadedSegments,
                possible_missing_sealed_rows: aggregateRes.value.coverage.possibleMissingSealedRows,
              possible_missing_wal_rows: aggregateRes.value.coverage.possibleMissingWalRows,
              used_rollups: aggregateRes.value.coverage.usedRollups,
              indexed_segments: aggregateRes.value.coverage.indexedSegments,
              scanned_segments: aggregateRes.value.coverage.scannedSegments,
              scanned_tail_docs: aggregateRes.value.coverage.scannedTailDocs,
              index_families_used: aggregateRes.value.coverage.indexFamiliesUsed,
            },
            buckets: aggregateRes.value.buckets,
          });
        }

        if (touchMode) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          const profileRes = profiles.getProfileResult(stream, srow);
          if (Result.isError(profileRes)) return internalError("invalid stream profile");
          const touchCapability = resolveTouchCapability(profileRes.value);
          if (!touchCapability?.handleRoute) return notFound("touch not enabled");
          return touchCapability.handleRoute({
            route: touchMode,
            req,
            stream,
            streamRow: srow,
            profile: profileRes.value,
            db,
            touchManager: touch,
            respond: { json, badRequest, internalError, notFound },
          });
        }

        // Stream lifecycle.
        if (req.method === "PUT") {
          const streamClosed = parseStreamClosedHeader(req.headers.get("stream-closed"));
          const ttlHeader = req.headers.get("stream-ttl");
          const expiresHeader = req.headers.get("stream-expires-at");
          if (ttlHeader && expiresHeader) return badRequest("only one of Stream-TTL or Stream-Expires-At is allowed");

          let ttlSeconds: number | null = null;
          let expiresAtMs: bigint | null = null;
          if (ttlHeader) {
            const ttlRes = parseStreamTtlSeconds(ttlHeader);
            if (Result.isError(ttlRes)) return badRequest(ttlRes.error.message);
            ttlSeconds = ttlRes.value;
            expiresAtMs = db.nowMs() + BigInt(ttlSeconds) * 1000n;
          } else if (expiresHeader) {
            const expiresRes = parseTimestampMsResult(expiresHeader);
            if (Result.isError(expiresRes)) return badRequest(expiresRes.error.message);
            expiresAtMs = expiresRes.value;
          }

          const contentType = normalizeContentType(req.headers.get("content-type")) ?? "application/octet-stream";
          const routingKeyHeader = req.headers.get("stream-key");
          const leaveAppendPhase = memorySampler?.enter("append", {
            route: "put",
            stream,
            content_type: contentType,
          });
          try {
            return await runWithGate(ingestGate, async () => {
              const ab = await req.arrayBuffer();
              if (ab.byteLength > cfg.appendMaxBodyBytes) return tooLarge(`body too large (max ${cfg.appendMaxBodyBytes})`);
              const bodyBytes = new Uint8Array(ab);

              let srow = db.getStream(stream);
              if (srow && db.isDeleted(srow)) {
                db.hardDeleteStream(stream);
                srow = null;
              }
              if (srow && srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) {
                db.hardDeleteStream(stream);
                srow = null;
              }

              if (srow) {
                const existingClosed = srow.closed !== 0;
                const existingContentType = normalizeContentType(srow.content_type) ?? srow.content_type;
                const ttlMatch =
                  ttlSeconds != null
                    ? srow.ttl_seconds != null && srow.ttl_seconds === ttlSeconds
                    : expiresAtMs != null
                      ? srow.ttl_seconds == null && srow.expires_at_ms != null && srow.expires_at_ms === expiresAtMs
                      : srow.ttl_seconds == null && srow.expires_at_ms == null;
                if (existingContentType !== contentType || existingClosed !== streamClosed || !ttlMatch) {
                  return conflict("stream config mismatch");
                }

                const tailOffset = encodeOffset(srow.epoch, srow.next_offset - 1n);
                const headers: Record<string, string> = {
                  "content-type": existingContentType,
                  "stream-next-offset": tailOffset,
                };
                if (existingClosed) headers["stream-closed"] = "true";
                if (srow.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(srow.expires_at_ms)).toISOString();
                return new Response(null, { status: 200, headers: withNosniff(headers) });
              }

              db.ensureStream(stream, { contentType, expiresAtMs, ttlSeconds, closed: false });
              notifier.notifyDetailsChanged(stream);
              let lastOffset = -1n;
              let appendedRows = 0;
              let closedNow = false;

              if (bodyBytes.byteLength > 0) {
                const rowsRes = buildAppendRowsResult(stream, bodyBytes, contentType, routingKeyHeader, true);
                if (Result.isError(rowsRes)) {
                  if (rowsRes.error.status === 500) return internalError();
                  return badRequest(rowsRes.error.message);
                }
                const rows = rowsRes.value.rows;
                appendedRows = rows.length;
                if (rows.length > 0 || streamClosed) {
                  const appendResOrResponse = await awaitAppendWithTimeout(enqueueAppend({
                    stream,
                    baseAppendMs: db.nowMs(),
                    rows,
                    contentType,
                    close: streamClosed,
                  }));
                  if (appendResOrResponse instanceof Response) return appendResOrResponse;
                  const appendRes = appendResOrResponse;
                  if (Result.isError(appendRes)) {
                    if (appendRes.error.kind === "overloaded") {
                      return overloaded(appendRes.error.message ?? "ingest queue full", appendRes.error.code ?? "overloaded");
                    }
                    return json(500, { error: { code: "internal", message: "append failed" } });
                  }
                  lastOffset = appendRes.value.lastOffset;
                  closedNow = appendRes.value.closed;
                }
              } else if (streamClosed) {
                const appendResOrResponse = await awaitAppendWithTimeout(enqueueAppend({
                  stream,
                  baseAppendMs: db.nowMs(),
                  rows: [],
                  contentType,
                  close: true,
                }));
                if (appendResOrResponse instanceof Response) return appendResOrResponse;
                const appendRes = appendResOrResponse;
                if (Result.isError(appendRes)) {
                  if (appendRes.error.kind === "overloaded") {
                    return overloaded(appendRes.error.message ?? "ingest queue full", appendRes.error.code ?? "overloaded");
                  }
                  return json(500, { error: { code: "internal", message: "close failed" } });
                }
                lastOffset = appendRes.value.lastOffset;
                closedNow = appendRes.value.closed;
              }

              recordAppendOutcome({
                stream,
                lastOffset,
                appendedRows,
                metricsBytes: bodyBytes.byteLength,
                ingestedBytes: bodyBytes.byteLength,
                touched: bodyBytes.byteLength > 0 || streamClosed,
                closed: closedNow,
              });

              const createdRow = db.getStream(stream)!;
              const tailOffset = encodeOffset(createdRow.epoch, createdRow.next_offset - 1n);
              const headers: Record<string, string> = {
                "content-type": contentType,
                "stream-next-offset": appendedRows > 0 || streamClosed ? encodeOffset(createdRow.epoch, lastOffset) : tailOffset,
                location: req.url,
              };
              if (streamClosed || closedNow) headers["stream-closed"] = "true";
              if (createdRow.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(createdRow.expires_at_ms)).toISOString();
              return new Response(null, { status: 201, headers: withNosniff(headers) });
            });
          } finally {
            leaveAppendPhase?.();
          }
        }

        if (req.method === "DELETE") {
          const deleted = db.deleteStream(stream);
          if (!deleted) return notFound();
          notifier.notifyDetailsChanged(stream);
          notifier.notifyClose(stream);
          await uploader.publishManifest(stream);
          db.clearObjectStoreRequestCounts(stream);
          notifier.notifyDetailsChanged(stream);
          return new Response(null, { status: 204, headers: withNosniff() });
        }

        if (req.method === "HEAD") {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");
          const tailOffset = encodeOffset(srow.epoch, srow.next_offset - 1n);
          const headers: Record<string, string> = {
            "content-type": normalizeContentType(srow.content_type) ?? srow.content_type,
            "stream-next-offset": tailOffset,
            "stream-end-offset": tailOffset,
            "cache-control": "no-store",
          };
          if (srow.closed !== 0) headers["stream-closed"] = "true";
          if (srow.ttl_seconds != null && srow.expires_at_ms != null) {
            const remainingMs = Number(srow.expires_at_ms - db.nowMs());
            const remaining = Math.max(0, Math.ceil(remainingMs / 1000));
            headers["stream-ttl"] = String(remaining);
          }
          if (srow.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(srow.expires_at_ms)).toISOString();
          return new Response(null, { status: 200, headers: withNosniff(headers) });
        }

        if (req.method === "POST") {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          const streamClosed = parseStreamClosedHeader(req.headers.get("stream-closed"));
          const streamContentType = normalizeContentType(srow.content_type) ?? srow.content_type;

          const producerId = req.headers.get("producer-id");
          const producerEpochHeader = req.headers.get("producer-epoch");
          const producerSeqHeader = req.headers.get("producer-seq");
          let producer: ProducerInfo | null = null;
          if (producerId != null || producerEpochHeader != null || producerSeqHeader != null) {
            if (!producerId || producerId.trim() === "") return badRequest("invalid Producer-Id");
            if (!producerEpochHeader || !producerSeqHeader) return badRequest("missing producer headers");
            const epoch = parseNonNegativeInt(producerEpochHeader);
            const seq = parseNonNegativeInt(producerSeqHeader);
            if (epoch == null || seq == null) return badRequest("invalid producer headers");
            producer = { id: producerId, epoch, seq };
          }

          let streamSeq: string | null = null;
          const streamSeqRes = parseStreamSeqHeader(req.headers.get("stream-seq"));
          if (Result.isError(streamSeqRes)) return badRequest(streamSeqRes.error.message);
          streamSeq = streamSeqRes.value;

          const tsHdr = req.headers.get("stream-timestamp");
          let baseAppendMs = db.nowMs();
          if (tsHdr) {
            const tsRes = parseTimestampMsResult(tsHdr);
            if (Result.isError(tsRes)) return badRequest(tsRes.error.message);
            baseAppendMs = tsRes.value;
          }

          const leaveAppendPhase = memorySampler?.enter("append", {
            route: "post",
            stream,
            stream_content_type: streamContentType,
          });
          try {
            return await runWithGate(ingestGate, async () => {
              const ab = await req.arrayBuffer();
              if (ab.byteLength > cfg.appendMaxBodyBytes) return tooLarge(`body too large (max ${cfg.appendMaxBodyBytes})`);
              const bodyBytes = new Uint8Array(ab);

              const isCloseOnly = streamClosed && bodyBytes.byteLength === 0;
              if (bodyBytes.byteLength === 0 && !streamClosed) return badRequest("empty body");

              let reqContentType = normalizeContentType(req.headers.get("content-type"));
              if (!isCloseOnly && !reqContentType) return badRequest("missing content-type");

              const routingKeyHeader = req.headers.get("stream-key");
              let rows: AppendRow[] = [];
              if (!isCloseOnly) {
                const rowsRes = buildAppendRowsResult(stream, bodyBytes, reqContentType!, routingKeyHeader, false);
                if (Result.isError(rowsRes)) {
                  if (rowsRes.error.status === 500) return internalError();
                  return badRequest(rowsRes.error.message);
                }
                rows = rowsRes.value.rows;
              }

              const appendResOrResponse = await awaitAppendWithTimeout(enqueueAppend({
                stream,
                baseAppendMs,
                rows,
                contentType: reqContentType ?? streamContentType,
                streamSeq,
                producer,
                close: streamClosed,
              }));
              if (appendResOrResponse instanceof Response) return appendResOrResponse;
              const appendRes = appendResOrResponse;

              if (Result.isError(appendRes)) {
                const err = appendRes.error;
                if (err.kind === "overloaded") {
                  return overloaded(err.message ?? "ingest queue full", err.code ?? "overloaded");
                }
                if (err.kind === "gone") return notFound("stream expired");
                if (err.kind === "not_found") return notFound();
                if (err.kind === "content_type_mismatch") return conflict("content-type mismatch");
                if (err.kind === "stream_seq") {
                  return conflict("sequence mismatch", {
                    "stream-expected-seq": err.expected,
                    "stream-received-seq": err.received,
                  });
                }
                if (err.kind === "closed") {
                  const headers: Record<string, string> = {
                    "stream-next-offset": encodeOffset(srow.epoch, err.lastOffset),
                    "stream-closed": "true",
                  };
                  return new Response(null, { status: 409, headers: withNosniff(headers) });
                }
                if (err.kind === "producer_stale_epoch") {
                  return new Response(null, {
                    status: 403,
                    headers: withNosniff({ "producer-epoch": String(err.producerEpoch) }),
                  });
                }
                if (err.kind === "producer_gap") {
                  return new Response(null, {
                    status: 409,
                    headers: withNosniff({
                      "producer-expected-seq": String(err.expected),
                      "producer-received-seq": String(err.received),
                    }),
                  });
                }
                if (err.kind === "producer_epoch_seq") return badRequest("invalid producer sequence");
                return json(500, { error: { code: "internal", message: "append failed" } });
              }
              const res = appendRes.value;

              const appendBytes = rows.reduce((acc, r) => acc + r.payload.byteLength, 0);
              recordAppendOutcome({
                stream,
                lastOffset: res.lastOffset,
                appendedRows: res.appendedRows,
                metricsBytes: appendBytes,
                ingestedBytes: bodyBytes.byteLength,
                touched: true,
                closed: res.closed,
              });

              const headers: Record<string, string> = {
                "stream-next-offset": encodeOffset(srow.epoch, res.lastOffset),
              };
              if (res.closed) headers["stream-closed"] = "true";
              if (producer && res.producer) {
                headers["producer-epoch"] = String(res.producer.epoch);
                headers["producer-seq"] = String(res.producer.seq);
              }

              const status = producer && res.appendedRows > 0 ? 200 : 204;
              return new Response(null, { status, headers: withNosniff(headers) });
            });
          } finally {
            leaveAppendPhase?.();
          }
        }

        if (req.method === "GET") {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          const streamContentType = normalizeContentType(srow.content_type) ?? srow.content_type;
          const isJsonStream = streamContentType === "application/json";

          const fmtParam = url.searchParams.get("format");
          let format: "raw" | "json" = isJsonStream ? "json" : "raw";
          if (fmtParam) {
            if (fmtParam !== "raw" && fmtParam !== "json") return badRequest("invalid format");
            format = fmtParam as "raw" | "json";
          }
          if (format === "json" && !isJsonStream) return badRequest("invalid format");

          const pathKey = pathKeyParam ?? null;
          const key = pathKey ?? url.searchParams.get("key");
          const rawFilter = url.searchParams.get("filter");
          let filterInput: string | null = null;
          let filter = null;
          if (rawFilter != null) {
            if (!isJsonStream) return badRequest("filter requires application/json stream content-type");
            filterInput = rawFilter.trim();
            const regRes = registry.getRegistryResult(stream);
            if (Result.isError(regRes)) return internalError();
            const filterRes = parseReadFilterResult(regRes.value, filterInput);
            if (Result.isError(filterRes)) return badRequest(filterRes.error.message);
            filter = filterRes.value;
          }

          const liveParam = url.searchParams.get("live") ?? "";
          const cursorParam = url.searchParams.get("cursor");
          let mode: "catchup" | "long-poll" | "sse";
          if (liveParam === "" || liveParam === "false" || liveParam === "0") mode = "catchup";
          else if (liveParam === "long-poll" || liveParam === "true" || liveParam === "1") mode = "long-poll";
          else if (liveParam === "sse") mode = "sse";
          else return badRequest("invalid live mode");
          if (filter && mode === "sse") return badRequest("filter does not support live=sse");

          const timeout = url.searchParams.get("timeout") ?? url.searchParams.get("timeout_ms");
          let timeoutMs: number | null = null;
          if (timeout) {
            if (/^[0-9]+$/.test(timeout)) {
              timeoutMs = Number(timeout);
            } else {
              const timeoutRes = parseDurationMsResult(timeout);
              if (Result.isError(timeoutRes)) return badRequest("invalid timeout");
              timeoutMs = timeoutRes.value;
            }
          }

          const hasOffsetParam = url.searchParams.has("offset");
          let offset = url.searchParams.get("offset");
          if (hasOffsetParam && (!offset || offset.trim() === "")) return badRequest("missing offset");
          const sinceParam = url.searchParams.get("since");
          if (!offset && sinceParam) {
            const sinceRes = parseTimestampMsResult(sinceParam);
            if (Result.isError(sinceRes)) return badRequest(sinceRes.error.message);
            const seekRes = await reader.seekOffsetByTimestampResult(stream, sinceRes.value, key ?? null);
            if (Result.isError(seekRes)) return readerErrorResponse(seekRes.error);
            offset = seekRes.value;
          }

          if (!offset) {
            if (mode === "catchup") offset = "-1";
            else return badRequest("missing offset");
          }

          let parsedOffset: ParsedOffset | null = null;
          if (offset !== "now") {
            const offsetRes = parseOffsetResult(offset);
            if (Result.isError(offsetRes)) return badRequest(offsetRes.error.message);
            parsedOffset = offsetRes.value;
          }

          const ifNoneMatch = req.headers.get("if-none-match");

          const sendBatch = async (batch: ReadBatch, cacheControl: string | null, includeEtag: boolean): Promise<Response> => {
            const upToDate = batch.nextOffsetSeq === batch.endOffsetSeq;
            const closedAtTail = srow.closed !== 0 && upToDate;
            const etag = includeEtag
              ? `W/\"slice:${canonicalizeOffset(offset!)}:${batch.nextOffset}:key=${key ?? ""}:fmt=${format}:filter=${filterInput ? encodeURIComponent(filterInput) : ""}\"`
              : null;
            const baseHeaders: Record<string, string> = {
              "stream-next-offset": batch.nextOffset,
              "stream-end-offset": batch.endOffset,
              "cross-origin-resource-policy": "cross-origin",
            };
            if (upToDate) baseHeaders["stream-up-to-date"] = "true";
            if (closedAtTail) baseHeaders["stream-closed"] = "true";
            if (cacheControl) baseHeaders["cache-control"] = cacheControl;
            if (etag) baseHeaders["etag"] = etag;
            if (srow.expires_at_ms != null) baseHeaders["stream-expires-at"] = new Date(Number(srow.expires_at_ms)).toISOString();
            if (batch.filterScanLimitReached) {
              baseHeaders["stream-filter-scan-limit-reached"] = "true";
              baseHeaders["stream-filter-scan-limit-bytes"] = String(batch.filterScanLimitBytes ?? 0);
              baseHeaders["stream-filter-scanned-bytes"] = String(batch.filterScannedBytes ?? 0);
            }

            if (etag && ifNoneMatch && ifNoneMatch === etag) {
              return new Response(null, { status: 304, headers: withNosniff(baseHeaders) });
            }

            if (format === "json") {
              const encodedRes = encodeStoredJsonArrayResult(stream, batch.records);
              if (Result.isError(encodedRes)) {
                if (encodedRes.error.status === 500) return internalError();
                return badRequest(encodedRes.error.message);
              }
              if (encodedRes.value) {
                metrics.recordRead(encodedRes.value.byteLength, batch.records.length);
                const headers: Record<string, string> = {
                  "content-type": "application/json",
                  ...baseHeaders,
                };
                return new Response(bodyBufferFromBytes(encodedRes.value), { status: 200, headers: withNosniff(headers) });
              }

              const decoded = decodeJsonRecords(stream, batch.records);
              if (Result.isError(decoded)) {
                if (decoded.error.status === 500) return internalError();
                return badRequest(decoded.error.message);
              }
              const body = JSON.stringify(decoded.value.values);
              metrics.recordRead(body.length, decoded.value.values.length);
              const headers: Record<string, string> = {
                "content-type": "application/json",
                ...baseHeaders,
              };
              return new Response(body, { status: 200, headers: withNosniff(headers) });
            }

            const outBytes = concatPayloads(batch.records.map((r) => r.payload));
            metrics.recordRead(outBytes.byteLength, batch.records.length);
            const headers: Record<string, string> = {
              "content-type": streamContentType,
              ...baseHeaders,
            };
            return new Response(bodyBufferFromBytes(outBytes), { status: 200, headers: withNosniff(headers) });
          };

          if (mode === "sse") {
            const baseCursor = srow.closed !== 0 ? null : computeCursor(Date.now(), cursorParam);
            const dataEncoding = isTextContentType(streamContentType) ? "text" : "base64";
            const startOffsetSeq = offset === "now" ? srow.next_offset - 1n : offsetToSeqOrNeg1(parsedOffset!);
            const startOffset = offset === "now" ? encodeOffset(srow.epoch, startOffsetSeq) : canonicalizeOffset(offset);

            const encoder = new TextEncoder();
            let aborted = false;
            const abortController = new AbortController();
            const streamBody = new ReadableStream({
              start(controller) {
                (async () => {
                  const fail = (message: string): void => {
                    if (aborted) return;
                    aborted = true;
                    abortController.abort();
                    controller.error(new Error(message));
                  };
                  let currentOffset = startOffset;
                  let currentSeq = startOffsetSeq;
                  let first = true;
                  while (!aborted) {
                    let batch: ReadBatch;
                    if (offset === "now" && first) {
                      batch = {
                        stream,
                        format,
                        key: key ?? null,
                        requestOffset: startOffset,
                        endOffset: startOffset,
                        nextOffset: startOffset,
                        endOffsetSeq: currentSeq,
                        nextOffsetSeq: currentSeq,
                        records: [],
                      };
                    } else {
                      const batchRes = await runForegroundWithGate(readGate, () =>
                        reader.readResult({ stream, offset: currentOffset, key: key ?? null, format, filter })
                      );
                      if (Result.isError(batchRes)) {
                        fail(batchRes.error.message);
                        return;
                      }
                      batch = batchRes.value;
                    }
                    first = false;

                    let ssePayload = "";

                    if (batch.records.length > 0) {
                      let dataPayload = "";
                      if (format === "json") {
                        const encodedRes = encodeStoredJsonArrayResult(stream, batch.records);
                        if (Result.isError(encodedRes)) {
                          fail(encodedRes.error.message);
                          return;
                        }
                        if (encodedRes.value) {
                          dataPayload = new TextDecoder().decode(encodedRes.value);
                        } else {
                          const decoded = decodeJsonRecords(stream, batch.records);
                          if (Result.isError(decoded)) {
                            fail(decoded.error.message);
                            return;
                          }
                          dataPayload = JSON.stringify(decoded.value.values);
                        }
                      } else {
                        const outBytes = concatPayloads(batch.records.map((r) => r.payload));
                        dataPayload =
                          dataEncoding === "base64"
                            ? Buffer.from(outBytes).toString("base64")
                            : new TextDecoder().decode(outBytes);
                      }
                      ssePayload += encodeSseEvent("data", dataPayload);
                    }

                    const upToDate = batch.nextOffsetSeq === batch.endOffsetSeq;
                    const latest = db.getStream(stream);
                    const closedNow = !!latest && latest.closed !== 0 && upToDate;

                    const control: Record<string, any> = { streamNextOffset: batch.nextOffset };
                    if (upToDate) control.upToDate = true;
                    if (closedNow) control.streamClosed = true;
                    if (!closedNow && baseCursor) control.streamCursor = baseCursor;
                    ssePayload += encodeSseEvent("control", JSON.stringify(control));
                    controller.enqueue(encoder.encode(ssePayload));

                    if (closedNow) break;
                    currentOffset = batch.nextOffset;
                    currentSeq = batch.nextOffsetSeq;
                    if (!upToDate) continue;

                    const sseWaitMs = timeoutMs == null ? 30_000 : timeoutMs;
                    await notifier.waitFor(stream, currentSeq, sseWaitMs, abortController.signal);
                  }
                  if (!aborted) controller.close();
                })().catch((err) => {
                  if (!aborted) controller.error(err);
                });
              },
              cancel() {
                aborted = true;
                abortController.abort();
              },
            });

            const headers: Record<string, string> = {
              "content-type": "text/event-stream",
              "cache-control": "no-cache",
              "cross-origin-resource-policy": "cross-origin",
              "stream-next-offset": startOffset,
              "stream-end-offset": encodeOffset(srow.epoch, srow.next_offset - 1n),
            };
            if (dataEncoding === "base64") headers["stream-sse-data-encoding"] = "base64";
            return new Response(streamBody, { status: 200, headers: withNosniff(headers) });
          }

          const defaultLongPollTimeoutMs = 3000;

          if (offset === "now") {
            const tailOffset = encodeOffset(srow.epoch, srow.next_offset - 1n);
            if (srow.closed !== 0) {
              if (mode === "long-poll") {
                const headers: Record<string, string> = {
                "stream-next-offset": tailOffset,
                "stream-end-offset": tailOffset,
                "stream-up-to-date": "true",
                "stream-closed": "true",
                "cache-control": "no-store",
              };
              if (srow.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(srow.expires_at_ms)).toISOString();
              return new Response(null, { status: 204, headers: withNosniff(headers) });
            }
            const headers: Record<string, string> = {
              "content-type": streamContentType,
                "stream-next-offset": tailOffset,
              "stream-end-offset": tailOffset,
              "stream-up-to-date": "true",
              "stream-closed": "true",
              "cache-control": "no-store",
              "cross-origin-resource-policy": "cross-origin",
            };
            if (srow.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(srow.expires_at_ms)).toISOString();
            const body = format === "json" ? "[]" : "";
            return new Response(body, { status: 200, headers: withNosniff(headers) });
          }

            if (mode === "long-poll") {
              const deadline = Date.now() + (timeoutMs ?? defaultLongPollTimeoutMs);
              let currentOffset = tailOffset;
              while (true) {
                const batchRes = await runForegroundWithGate(readGate, () =>
                  reader.readResult({ stream, offset: currentOffset, key: key ?? null, format, filter })
                );
                if (Result.isError(batchRes)) return readerErrorResponse(batchRes.error);
                const batch = batchRes.value;
                if (batch.records.length > 0 || batch.filterScanLimitReached) {
                  const cursor = computeCursor(Date.now(), cursorParam);
                  const resp = await sendBatch(batch, "no-store", false);
                  const headers = new Headers(resp.headers);
                  headers.set("stream-cursor", cursor);
                  return new Response(resp.body, { status: resp.status, headers });
                }
                const latest = db.getStream(stream);
                if (latest && latest.closed !== 0 && batch.nextOffsetSeq === batch.endOffsetSeq) {
                  const latestTail = encodeOffset(latest.epoch, latest.next_offset - 1n);
                  const headers: Record<string, string> = {
                    "stream-next-offset": latestTail,
                    "stream-end-offset": latestTail,
                    "stream-up-to-date": "true",
                    "stream-closed": "true",
                    "cache-control": "no-store",
                  };
                  if (latest.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(latest.expires_at_ms)).toISOString();
                  return new Response(null, { status: 204, headers: withNosniff(headers) });
                }
                const remaining = deadline - Date.now();
                if (remaining <= 0) break;
                currentOffset = batch.nextOffset;
                await notifier.waitFor(stream, batch.endOffsetSeq, remaining, req.signal);
                if (req.signal.aborted) return new Response(null, { status: 204 });
              }
              const latest = db.getStream(stream);
              const latestTail = latest ? encodeOffset(latest.epoch, latest.next_offset - 1n) : tailOffset;
              const headers: Record<string, string> = {
                "stream-next-offset": latestTail,
                "stream-end-offset": latestTail,
                "stream-up-to-date": "true",
                "cache-control": "no-store",
              };
              if (latest && latest.closed !== 0) headers["stream-closed"] = "true";
              else headers["stream-cursor"] = computeCursor(Date.now(), cursorParam);
              if (latest && latest.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(latest.expires_at_ms)).toISOString();
              return new Response(null, { status: 204, headers: withNosniff(headers) });
            }

            const headers: Record<string, string> = {
              "content-type": streamContentType,
              "stream-next-offset": tailOffset,
              "stream-end-offset": tailOffset,
              "stream-up-to-date": "true",
              "cache-control": "no-store",
              "cross-origin-resource-policy": "cross-origin",
            };
            const body = format === "json" ? "[]" : "";
            return new Response(body, { status: 200, headers: withNosniff(headers) });
          }

          if (mode === "long-poll") {
            const deadline = Date.now() + (timeoutMs ?? defaultLongPollTimeoutMs);
            let currentOffset = offset;
            while (true) {
              const batchRes = await runForegroundWithGate(readGate, () =>
                reader.readResult({ stream, offset: currentOffset, key: key ?? null, format, filter })
              );
              if (Result.isError(batchRes)) return readerErrorResponse(batchRes.error);
              const batch = batchRes.value;
              if (batch.records.length > 0 || batch.filterScanLimitReached) {
                const cursor = computeCursor(Date.now(), cursorParam);
                const resp = await sendBatch(batch, "no-store", false);
                const headers = new Headers(resp.headers);
                headers.set("stream-cursor", cursor);
                return new Response(resp.body, { status: resp.status, headers });
              }
              const latest = db.getStream(stream);
              if (latest && latest.closed !== 0 && batch.nextOffsetSeq === batch.endOffsetSeq) {
                const latestTail = encodeOffset(latest.epoch, latest.next_offset - 1n);
                const headers: Record<string, string> = {
                  "stream-next-offset": latestTail,
                  "stream-end-offset": latestTail,
                  "stream-up-to-date": "true",
                  "stream-closed": "true",
                  "cache-control": "no-store",
                };
                if (latest.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(latest.expires_at_ms)).toISOString();
                return new Response(null, { status: 204, headers: withNosniff(headers) });
              }
              const remaining = deadline - Date.now();
              if (remaining <= 0) break;
              currentOffset = batch.nextOffset;
              await notifier.waitFor(stream, batch.endOffsetSeq, remaining, req.signal);
              if (req.signal.aborted) return new Response(null, { status: 204 });
            }
            const latest = db.getStream(stream);
            const latestTail = latest ? encodeOffset(latest.epoch, latest.next_offset - 1n) : currentOffset;
            const headers: Record<string, string> = {
              "stream-next-offset": latestTail,
              "stream-end-offset": latestTail,
              "stream-up-to-date": "true",
              "cache-control": "no-store",
            };
            if (latest && latest.closed !== 0) headers["stream-closed"] = "true";
            else headers["stream-cursor"] = computeCursor(Date.now(), cursorParam);
            if (latest && latest.expires_at_ms != null) headers["stream-expires-at"] = new Date(Number(latest.expires_at_ms)).toISOString();
            return new Response(null, { status: 204, headers: withNosniff(headers) });
          }

          const batchRes = await runForegroundWithGate(readGate, () =>
            reader.readResult({ stream, offset, key: key ?? null, format, filter })
          );
          if (Result.isError(batchRes)) return readerErrorResponse(batchRes.error);
          const batch = batchRes.value;
          const cacheControl = "immutable, max-age=31536000";
          return sendBatch(batch, cacheControl, true);
        }

        return badRequest("unsupported method");
      }

      return notFound();
        })();
      const resolved = await awaitWithCooperativeTimeout(requestPromise, HTTP_RESOLVER_TIMEOUT_MS);
      if (resolved === TIMEOUT_SENTINEL) {
        timedOut = true;
        requestAbortController.abort(new Error("request timed out"));
        void requestPromise.catch(() => {});
        await cancelRequestBody(req);
        return requestTimeout();
      }
      return resolved;
    } catch (e: any) {
      if (isAbortLikeError(e)) {
        if (timedOut) return requestTimeout();
        return new Response(null, { status: 204 });
      }
      const msg = String(e?.message ?? e);
      if (!closing && !msg.includes("Statement has finalized")) {
        // eslint-disable-next-line no-console
        console.error("request failed", e);
      }
      return internalError();
    } finally {
      req.signal.removeEventListener("abort", abortFromClient);
    }
  };

  const close = () => {
    closing = true;
    touch.stop();
    uploader.stop(true);
    indexer?.stop();
    segmenter.stop(true);
    metricsEmitter.stop();
    expirySweeper.stop();
    streamSizeReconciler.stop();
    ingest.stop();
    memorySampler?.stop();
    memory.stop();
    db.close();
  };

  return {
    fetch,
    close,
    deps: {
      config: cfg,
      db,
      os: store,
      ingest,
      notifier,
      reader,
      segmenter,
      uploader,
      indexer,
      segmentDiskCache: runtime.segmentDiskCache,
      metrics,
      registry,
      profiles,
      touch,
      stats,
      backpressure,
      memory,
      concurrency: {
        ingest: ingestGate,
        read: readGate,
        search: searchGate,
        asyncIndex: asyncIndexGate,
      },
      memorySampler,
    },
  };
}
