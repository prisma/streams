import { mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import type { Config } from "./config";
import { SqliteDurableStore, type StreamRow } from "./db/db";
import { IngestQueue, type ProducerInfo, type AppendRow } from "./ingest";
import type { ObjectStore } from "./objectstore/interface";
import type { StreamReader, ReadBatch, ReaderError } from "./reader";
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
import { MemoryGuard } from "./memory";
import { RuntimeMemorySampler } from "./runtime_memory_sampler";
import { TouchProcessorManager } from "./touch/manager";
import { StreamSizeReconciler } from "./stream_size_reconciler";
import type { SegmenterController } from "./segment/segmenter_workers";
import type { UploaderController } from "./uploader";
import type { StreamIndexLookup } from "./index/indexer";
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

function retryAfterHeaders(seconds: string, headers: HeadersInit = {}): HeadersInit {
  return {
    "retry-after": seconds,
    ...headers,
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

function overloaded(msg = "ingest queue full"): Response {
  return json(429, { error: { code: "overloaded", message: msg } }, retryAfterHeaders(OVERLOAD_RETRY_AFTER_SECONDS));
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

function computeCursor(nowMs: number, provided: string | null): string {
  let cursor = Math.floor(nowMs / 1000);
  if (provided && /^[0-9]+$/.test(provided)) {
    const n = Number(provided);
    if (Number.isFinite(n) && n >= cursor) cursor = n + 1;
  }
  return String(cursor);
}

function concatPayloads(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.byteLength;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
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
    metrics: Metrics;
    registry: SchemaRegistryStore;
    profiles: StreamProfileStore;
    touch: TouchProcessorManager;
    stats?: StatsCollector;
    backpressure?: BackpressureGate;
    memory?: MemoryGuard;
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
  memory: MemoryGuard;
  metrics: Metrics;
  memorySampler?: RuntimeMemorySampler;
};

type AppRuntimeDeps = {
  store: ObjectStore;
  reader: StreamReader;
  segmenter: SegmenterController;
  uploader: UploaderController;
  indexer?: StreamIndexLookup;
  uploadSchemaRegistry: (stream: string, registry: SchemaRegistry) => Promise<void>;
  getLocalStorageUsage?: (stream: string) => {
    segment_cache_bytes: number;
    routing_index_cache_bytes: number;
    exact_index_cache_bytes: number;
  };
  start(): void;
};

export type CreateAppCoreOptions = {
  stats?: StatsCollector;
  createRuntime(args: CreateAppRuntimeArgs): AppRuntimeDeps;
};

export function createAppCore(cfg: Config, opts: CreateAppCoreOptions): App {
  mkdirSync(cfg.rootDir, { recursive: true });
  cleanupTempSegments(cfg.rootDir);

  const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.sqliteCacheBytes });
  db.resetSegmentInProgress();
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
  const memory = new MemoryGuard(cfg.memoryLimitBytes, {
    onSample: (rss, overLimit) => {
      metrics.record("process.rss.bytes", rss, "bytes");
      if (overLimit) metrics.record("process.rss.over_limit", 1, "count");
    },
    heapSnapshotPath: cfg.heapSnapshotPath ?? undefined,
  });
  memory.start();
  const ingest = new IngestQueue(cfg, db, stats, backpressure, memory, metrics);
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
    metrics,
    memorySampler,
  });
  const { store, reader, segmenter, uploader, indexer, uploadSchemaRegistry, getLocalStorageUsage } = runtime;
  const metricsEmitter = new MetricsEmitter(metrics, ingest, cfg.metricsFlushIntervalMs, {
    onAppended: ({ lastOffset, stream }) => {
      notifier.notify(stream, lastOffset);
      notifier.notifyDetailsChanged(stream);
    },
  });
  const expirySweeper = new ExpirySweeper(cfg, db);
  const streamSizeReconciler = new StreamSizeReconciler(db, store, (stream) => notifier.notifyDetailsChanged(stream));

  const metricsStreamRow = db.ensureStream("__stream_metrics__", { contentType: "application/json", profile: "metrics" });
  const metricsProfileRes = profiles.updateProfileResult("__stream_metrics__", metricsStreamRow, { kind: "metrics" });
  if (Result.isError(metricsProfileRes)) {
    throw dsError(`failed to initialize __stream_metrics__ profile: ${metricsProfileRes.error.message}`);
  }
  runtime.start();
  if (metricsProfileRes.value.schemaRegistry) {
    void (async () => {
      try {
        await uploadSchemaRegistry("__stream_metrics__", metricsProfileRes.value.schemaRegistry!);
        await uploader.publishManifest("__stream_metrics__");
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

  const buildStreamSummary = (stream: string, row: StreamRow, profileKind: string) => ({
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
    segment_count: db.countSegmentsForStream(stream),
    uploaded_segment_count: db.countUploadedSegments(stream),
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
    indexStatus: any
  ) => {
    const manifest = db.getManifestRow(stream);
    const schemaRow = db.getSchemaRegistry(stream);
    const uploadedSegmentBytes = db.getUploadedSegmentBytes(stream);
    const pendingSealedSegmentBytes = db.getPendingSealedSegmentBytes(stream);
    const routingIndexStorage = db.getRoutingIndexStorage(stream);
    const secondaryIndexStorage = new Map(db.getSecondaryIndexStorage(stream).map((entry) => [entry.index_name, entry]));
    const companionStorage = db.getBundledCompanionStorage(stream);
    const localStorageUsage = {
      segment_cache_bytes: 0,
      routing_index_cache_bytes: 0,
      exact_index_cache_bytes: 0,
      companion_cache_bytes: 0,
      ...(getLocalStorageUsage?.(stream) ?? {}),
    };
    const sqliteSharedBytes = BigInt(db.getWalDbSizeBytes() + db.getMetaDbSizeBytes());
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
          exactIndexBytes +
          companionStorage.bytes +
          (manifest.last_uploaded_size_bytes ?? 0n) +
          (schemaRow?.uploaded_size_bytes ?? 0n)
        ).toString(),
        segments_bytes: uploadedSegmentBytes.toString(),
        indexes_bytes: (routingIndexStorage.bytes + exactIndexBytes + companionStorage.bytes).toString(),
        manifest_and_meta_bytes: ((manifest.last_uploaded_size_bytes ?? 0n) + (schemaRow?.uploaded_size_bytes ?? 0n)).toString(),
        manifest_bytes: (manifest.last_uploaded_size_bytes ?? 0n).toString(),
        schema_registry_bytes: (schemaRow?.uploaded_size_bytes ?? 0n).toString(),
        segment_object_count: indexStatus.segments.uploaded_count,
        routing_index_object_count: routingIndexStorage.object_count,
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
          BigInt(localStorageUsage.companion_cache_bytes)
        ).toString(),
        wal_retained_bytes: row.wal_bytes.toString(),
        pending_tail_bytes: row.pending_bytes.toString(),
        pending_sealed_segment_bytes: pendingSealedSegmentBytes.toString(),
        segment_cache_bytes: String(localStorageUsage.segment_cache_bytes),
        routing_index_cache_bytes: String(localStorageUsage.routing_index_cache_bytes),
        exact_index_cache_bytes: String(localStorageUsage.exact_index_cache_bytes),
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
    const segmentCount = db.countSegmentsForStream(stream);
    const uploadedSegmentCount = db.countUploadedSegments(stream);
    const manifest = db.getManifestRow(stream);

    const routingState = db.getIndexState(stream);
    const routingRuns = db.listIndexRuns(stream);
    const retiredRoutingRuns = db.listRetiredIndexRuns(stream);
    const routingStorage = db.getRoutingIndexStorage(stream);
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
        active_run_count: db.listSecondaryIndexRuns(stream, name).length,
        retired_run_count: db.listRetiredSecondaryIndexRuns(stream, name).length,
        fully_indexed_uploaded_segments: configMatches && indexedSegmentCount >= uploadedSegmentCount,
        stale_configuration: !configMatches,
        updated_at: timestampToIsoString(state?.updated_at_ms ?? null),
      };
    });

    const desiredCompanionPlan = buildDesiredSearchCompanionPlan(reg);
    const desiredCompanionHash = hashSearchCompanionPlan(desiredCompanionPlan);
    const companionPlanRow = db.getSearchCompanionPlan(stream);
    const desiredIndexPlanGeneration =
      Object.values(desiredCompanionPlan.families).some(Boolean)
        ? companionPlanRow
          ? companionPlanRow.plan_hash === desiredCompanionHash
            ? companionPlanRow.generation
            : companionPlanRow.generation + 1
          : 1
        : 0;
    const companionRows = db.listSearchSegmentCompanions(stream);
    const currentCompanionRows = companionRows.filter((row) => row.plan_generation === desiredIndexPlanGeneration);
    const currentCompanionBytes = currentCompanionRows.reduce((sum, entry) => sum + BigInt(entry.size_bytes), 0n);
    const searchFamilies = configuredSearchFamilies(reg.search).map(({ family, fields }) => {
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
        active_run_count: routingRuns.length,
        retired_run_count: retiredRoutingRuns.length,
        fully_indexed_uploaded_segments: reg.routingKey == null ? true : (routingState?.indexed_through ?? 0) >= uploadedSegmentCount,
        updated_at: timestampToIsoString(routingState?.updated_at_ms ?? null),
      },
      exact_indexes: exactIndexes,
      bundled_companions: {
        object_count: currentCompanionRows.length,
        bytes_at_rest: currentCompanionBytes.toString(),
        fully_indexed_uploaded_segments: currentCompanionRows.length >= uploadedSegmentCount,
      },
      search_families: searchFamilies,
      current_companion_rows: currentCompanionRows,
    };
  };

  type DetailsSnapshot = { etag: string; body: string; version: bigint };

  const buildDetailsSnapshotResult = (
    stream: string,
    mode: "details" | "index_status"
  ): Result<DetailsSnapshot, { status: 404 | 500; message: string }> => {
    for (let attempt = 0; attempt < 3; attempt++) {
      const beforeVersion = notifier.currentDetailsVersion(stream);
      const srow = db.getStream(stream);
      if (!srow || db.isDeleted(srow)) return Result.err({ status: 404, message: "not_found" });
      if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return Result.err({ status: 404, message: "stream expired" });

      const regRes = registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return Result.err({ status: 500, message: regRes.error.message });
      const profileRes = profiles.getProfileResourceResult(stream, srow);
      if (Result.isError(profileRes)) return Result.err({ status: 500, message: profileRes.error.message });

      const profileKind = profileRes.value.profile.kind;
      const indexStatus = buildIndexStatus(stream, srow, regRes.value, profileKind);
      const storage = buildStorageBreakdown(stream, srow, indexStatus.current_companion_rows, indexStatus);
      const objectStoreRequests = buildObjectStoreRequestSummary(stream);
      delete (indexStatus as any).current_companion_rows;
      const payload =
        mode === "index_status"
          ? indexStatus
          : {
              stream: buildStreamSummary(stream, srow, profileKind),
              profile: profileRes.value,
              schema: regRes.value,
              index_status: indexStatus,
              storage,
              object_store_requests: objectStoreRequests,
            };
      const body = JSON.stringify(payload);
      const afterVersion = notifier.currentDetailsVersion(stream);
      if (beforeVersion === afterVersion) {
        return Result.ok({
          etag: weakEtag(mode, body),
          body,
          version: afterVersion,
        });
      }
    }

    return Result.err({ status: 500, message: "details changed too quickly" });
  };

  let closing = false;
  const fetch = async (req: Request): Promise<Response> => {
    if (closing) {
      return unavailable();
    }
    try {
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

      const rejectIfMemoryLimited = (): Response | null => {
        if (!memory || memory.shouldAllow()) return null;
        memory.maybeGc("memory limit");
        memory.maybeHeapSnapshot("memory limit");
        metrics.record("tieredstore.backpressure.over_limit", 1, "count", { reason: "memory" });
        return overloaded();
      };

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
            const searchRes = await reader.searchResult({ stream, request: requestRes.value });
            if (Result.isError(searchRes)) return readerErrorResponse(searchRes.error);
            return json(200, {
              stream,
              snapshot_end_offset: searchRes.value.snapshotEndOffset,
              took_ms: searchRes.value.tookMs,
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
            });
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
          const aggregateRes = await reader.aggregateResult({ stream, request: requestRes.value });
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
            const memReject = rejectIfMemoryLimited();
            if (memReject) return memReject;
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
              const appendRes = await enqueueAppend({
                stream,
                baseAppendMs: db.nowMs(),
                rows,
                contentType,
                close: streamClosed,
              });
              if (Result.isError(appendRes)) {
                if (appendRes.error.kind === "overloaded") return overloaded();
                return json(500, { error: { code: "internal", message: "append failed" } });
              }
              lastOffset = appendRes.value.lastOffset;
              closedNow = appendRes.value.closed;
            }
          } else if (streamClosed) {
            const appendRes = await enqueueAppend({
              stream,
              baseAppendMs: db.nowMs(),
              rows: [],
              contentType,
              close: true,
            });
            if (Result.isError(appendRes)) {
              if (appendRes.error.kind === "overloaded") return overloaded();
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
            const memReject = rejectIfMemoryLimited();
            if (memReject) return memReject;
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

            const appendRes = await enqueueAppend({
              stream,
              baseAppendMs,
              rows,
              contentType: reqContentType ?? streamContentType,
              streamSeq,
              producer,
              close: streamClosed,
            });

            if (Result.isError(appendRes)) {
              const err = appendRes.error;
              if (err.kind === "overloaded") return overloaded();
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
            const outBody = new Uint8Array(outBytes.byteLength);
            outBody.set(outBytes);
            return new Response(outBody, { status: 200, headers: withNosniff(headers) });
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
                      const batchRes = await reader.readResult({ stream, offset: currentOffset, key: key ?? null, format, filter });
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
                        const decoded = decodeJsonRecords(stream, batch.records);
                        if (Result.isError(decoded)) {
                          fail(decoded.error.message);
                          return;
                        }
                        dataPayload = JSON.stringify(decoded.value.values);
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
                const batchRes = await reader.readResult({ stream, offset: currentOffset, key: key ?? null, format, filter });
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
              const batchRes = await reader.readResult({ stream, offset: currentOffset, key: key ?? null, format, filter });
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

          const batchRes = await reader.readResult({ stream, offset, key: key ?? null, format, filter });
          if (Result.isError(batchRes)) return readerErrorResponse(batchRes.error);
          const batch = batchRes.value;
          const cacheControl = "immutable, max-age=31536000";
          return sendBatch(batch, cacheControl, true);
        }

        return badRequest("unsupported method");
      }

      return notFound();
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (!closing && !msg.includes("Statement has finalized")) {
        // eslint-disable-next-line no-console
        console.error("request failed", e);
      }
      return internalError();
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
      metrics,
      registry,
      profiles,
      touch,
      stats,
      backpressure,
      memory,
      memorySampler,
    },
  };
}
