import { mkdirSync } from "node:fs";
import type { Config } from "./config";
import { SqliteDurableStore, STREAM_FLAG_TOUCH } from "./db/db";
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
import { SchemaRegistryStore, type SchemaRegistry, type SchemaRegistryMutationError, type SchemaRegistryReadError } from "./schema/registry";
import { resolvePointerResult } from "./util/json_pointer";
import { applyLensChainResult } from "./lens/lens";
import { ExpirySweeper } from "./expiry_sweeper";
import type { StatsCollector } from "./stats";
import { BackpressureGate } from "./backpressure";
import { MemoryGuard } from "./memory";
import { TouchInterpreterManager } from "./touch/manager";
import { isTouchEnabled } from "./touch/spec";
import { resolveTouchStreamName } from "./touch/naming";
import { RoutingKeyNotifier } from "./touch/routing_key_notifier";
import { parseTouchCursor } from "./touch/touch_journal";
import { touchKeyIdFromRoutingKeyResult } from "./touch/touch_key_id";
import { tableKeyIdFor, templateKeyIdFor } from "./touch/live_keys";
import type { SegmenterController } from "./segment/segmenter_workers";
import type { UploaderController } from "./uploader";
import type { IndexManager } from "./index/indexer";
import { Result } from "better-result";

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

function schemaVersionForOffset(reg: SchemaRegistry, offset: bigint): number {
  if (!reg.boundaries || reg.boundaries.length === 0) return 0;
  const off = Number(offset);
  let version = 0;
  for (const b of reg.boundaries) {
    if (b.offset <= off) version = b.version;
    else break;
  }
  return version;
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
    touchRoutingKeyNotifier: RoutingKeyNotifier;
    reader: StreamReader;
    segmenter: SegmenterController;
    uploader: UploaderController;
    indexer?: IndexManager;
    metrics: Metrics;
    registry: SchemaRegistryStore;
    touch: TouchInterpreterManager;
    stats?: StatsCollector;
    backpressure?: BackpressureGate;
    memory?: MemoryGuard;
  };
};

export type CreateAppRuntimeArgs = {
  config: Config;
  db: SqliteDurableStore;
  ingest: IngestQueue;
  notifier: StreamNotifier;
  touchRoutingKeyNotifier: RoutingKeyNotifier;
  registry: SchemaRegistryStore;
  touch: TouchInterpreterManager;
  stats?: StatsCollector;
  backpressure?: BackpressureGate;
  memory: MemoryGuard;
  metrics: Metrics;
};

type AppRuntimeDeps = {
  store: ObjectStore;
  reader: StreamReader;
  segmenter: SegmenterController;
  uploader: UploaderController;
  indexer?: IndexManager;
  uploadSchemaRegistry: (stream: string, registry: SchemaRegistry) => Promise<void>;
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
  const backpressure =
    cfg.localBacklogMaxBytes > 0
      ? new BackpressureGate(cfg.localBacklogMaxBytes, db.sumPendingBytes() + db.sumPendingSegmentBytes())
      : undefined;
  const memory = new MemoryGuard(cfg.memoryLimitBytes, {
    onSample: (rss, overLimit) => {
      metrics.record("process.rss.bytes", rss, "bytes");
      if (overLimit) metrics.record("process.rss.over_limit", 1, "count");
    },
    heapSnapshotPath: `${cfg.rootDir}/heap.heapsnapshot`,
  });
  memory.start();
  const metrics = new Metrics();
  const ingest = new IngestQueue(cfg, db, stats, backpressure, memory, metrics);
  const notifier = new StreamNotifier();
  const touchRoutingKeyNotifier = new RoutingKeyNotifier();
  const registry = new SchemaRegistryStore(db);
  const touch = new TouchInterpreterManager(cfg, db, ingest, notifier, registry, backpressure, touchRoutingKeyNotifier);
  const runtime = opts.createRuntime({
    config: cfg,
    db,
    ingest,
    notifier,
    touchRoutingKeyNotifier,
    registry,
    touch,
    stats,
    backpressure,
    memory,
    metrics,
  });
  const { store, reader, segmenter, uploader, indexer, uploadSchemaRegistry } = runtime;
  const metricsEmitter = new MetricsEmitter(metrics, ingest, cfg.metricsFlushIntervalMs);
  const expirySweeper = new ExpirySweeper(cfg, db);

  db.ensureStream("__stream_metrics__", { contentType: "application/json" });
  runtime.start();
  metricsEmitter.start();
  expirySweeper.start();
  touch.start();

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
    const reg = regRes.value;
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
      if (validator && !validator(v)) {
        const msg = validator.errors ? validator.errors.map((e) => e.message).join("; ") : "schema validation failed";
        return Result.err({ status: 400, message: msg });
      }
      const rkRes = reg.routingKey ? extractRoutingKey(reg, v) : Result.ok(keyBytesFromString(routingKeyHeader));
      if (Result.isError(rkRes)) return Result.err({ status: 400, message: rkRes.error.message });
      rows.push({
        routingKey: rkRes.value,
        contentType: "application/json",
        payload: new TextEncoder().encode(JSON.stringify(v)),
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
      touch.notify(args.stream);
    }
    if (stats) {
      if (args.touched) stats.recordStreamTouched(args.stream);
      if (args.appendedRows > 0) stats.recordIngested(args.ingestedBytes);
    }
    if (args.closed) notifier.notifyClose(args.stream);
  };

  const decodeJsonRecords = (
    stream: string,
    records: Array<{ offset: bigint; payload: Uint8Array }>
  ): Result<{ values: any[] }, { status: 400 | 500; message: string }> => {
    const regRes = registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ status: 500, message: regRes.error.message });
    const reg = regRes.value;
    const values: any[] = [];
    for (const r of records) {
      try {
        const s = new TextDecoder().decode(r.payload);
        let value: any = JSON.parse(s);
        if (reg.currentVersion > 0) {
          const version = schemaVersionForOffset(reg, r.offset);
          if (version < reg.currentVersion) {
            const chainRes = registry.getLensChainResult(reg, version, reg.currentVersion);
            if (Result.isError(chainRes)) return Result.err({ status: 500, message: chainRes.error.message });
            const chain = chainRes.value;
            const transformedRes = applyLensChainResult(chain, value);
            if (Result.isError(transformedRes)) return Result.err({ status: 400, message: transformedRes.error.message });
            value = transformedRes.value;
          }
        }
        values.push(value);
      } catch (e: any) {
        return Result.err({ status: 400, message: String(e?.message ?? e) });
      }
    }
    return Result.ok({ values });
  };

  let closing = false;
  const fetch = async (req: Request): Promise<Response> => {
    if (closing) {
      return json(503, { error: { code: "unavailable", message: "server shutting down" } });
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
        return json(429, { error: { code: "overloaded", message: "ingest queue full" } });
      };

      // /v1/streams
      if (req.method === "GET" && path === "/v1/streams") {
        const limit = Number(url.searchParams.get("limit") ?? "100");
        const offset = Number(url.searchParams.get("offset") ?? "0");
        const rows = db.listStreams(Math.max(0, Math.min(limit, 1000)), Math.max(0, offset));
        const out = rows.map((r) => ({
          name: r.stream,
          created_at: new Date(Number(r.created_at_ms)).toISOString(),
          expires_at: r.expires_at_ms == null ? null : new Date(Number(r.expires_at_ms)).toISOString(),
          epoch: r.epoch,
          next_offset: r.next_offset.toString(),
          sealed_through: r.sealed_through.toString(),
          uploaded_through: r.uploaded_through.toString(),
        }));
        return json(200, out);
      }

      // /v1/stream/:name[/_schema] (accept encoded or raw slashes in name)
      const streamPrefix = "/v1/stream/";
      if (path.startsWith(streamPrefix)) {
        const rawRest = path.slice(streamPrefix.length);
        const rest = rawRest.replace(/\/+$/, "");
        if (rest.length === 0) return badRequest("missing stream name");
        const segments = rest.split("/");
        let isSchema = false;
        let pathKeyParam: string | null = null;
        let touchMode:
          | null
          | { kind: "read"; key: string | null }
          | { kind: "meta" }
          | { kind: "wait" }
          | { kind: "templates_activate" } = null;
        if (segments[segments.length - 1] === "_schema") {
          isSchema = true;
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
        } else if (segments.length >= 3 && segments[segments.length - 3] === "touch" && segments[segments.length - 2] === "pk") {
          touchMode = { kind: "read", key: decodeURIComponent(segments[segments.length - 1]) };
          segments.splice(segments.length - 3, 3);
        } else if (segments[segments.length - 1] === "touch") {
          touchMode = { kind: "read", key: null };
          segments.pop();
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
            let body: any;
            try {
              body = await req.json();
            } catch {
              return badRequest("schema update must be valid JSON");
            }
            // Accept incremental update shape ({schema, lens, routingKey}),
            // full registry payload ({schemas, lenses, currentVersion, ...}),
            // and routingKey-only updates (used by the Bluesky demo).
            let update = body;
            const isSchemaObject =
              update &&
              (update.schema === true ||
                update.schema === false ||
                (typeof update.schema === "object" && update.schema !== null && !Array.isArray(update.schema)));
            if (!isSchemaObject && update && typeof update === "object" && update.schemas && typeof update.schemas === "object") {
              const versions = Object.keys(update.schemas)
                .map((v) => Number(v))
                .filter((v) => Number.isFinite(v) && v >= 0);
              const currentVersion =
                typeof update.currentVersion === "number" && Number.isFinite(update.currentVersion)
                  ? update.currentVersion
                  : versions.length > 0
                    ? Math.max(...versions)
                    : null;
              if (currentVersion != null) {
                const schema = update.schemas[String(currentVersion)];
                const lens =
                  update.lens ??
                  (update.lenses && typeof update.lenses === "object" ? update.lenses[String(currentVersion - 1)] : undefined);
                update = {
                  schema,
                  lens,
                  routingKey: update.routingKey,
                  interpreter: (update as any).interpreter,
                };
              }
            }
            if (update && typeof update === "object") {
              if (update.schema === null) {
                delete update.schema;
              }
              if (update.routingKey === undefined) {
                const raw = update as any;
                const candidate =
                  raw.routing_key ?? raw.routingKeyPointer ?? raw.routing_key_pointer ?? raw.routingKey;
                if (typeof candidate === "string") {
                  update.routingKey = { jsonPointer: candidate, required: true };
                } else if (candidate && typeof candidate === "object") {
                  const jsonPointer = candidate.jsonPointer ?? candidate.json_pointer;
                  if (typeof jsonPointer === "string") {
                    update.routingKey = {
                      jsonPointer,
                      required: typeof candidate.required === "boolean" ? candidate.required : true,
                    };
                  }
                }
              } else if (update.routingKey && typeof update.routingKey === "object") {
                const rk = update.routingKey as any;
                if (rk.jsonPointer === undefined && typeof rk.json_pointer === "string") {
                  update.routingKey = {
                    jsonPointer: rk.json_pointer,
                    required: typeof rk.required === "boolean" ? rk.required : true,
                  };
                }
              }
            }
            if (update.schema === undefined && update.routingKey !== undefined && update.interpreter === undefined) {
              const regRes = registry.updateRoutingKeyResult(stream, update.routingKey ?? null);
              if (Result.isError(regRes)) return schemaMutationErrorResponse(regRes.error);
              try {
                await uploadSchemaRegistry(stream, regRes.value);
              } catch {
                return json(500, { error: { code: "internal", message: "schema upload failed" } });
              }
              return json(200, regRes.value);
            }
            if (update.schema === undefined && update.interpreter !== undefined && update.routingKey === undefined) {
              const regRes = registry.updateInterpreterResult(stream, update.interpreter ?? null);
              if (Result.isError(regRes)) return schemaMutationErrorResponse(regRes.error);
              try {
                await uploadSchemaRegistry(stream, regRes.value);
              } catch {
                return json(500, { error: { code: "internal", message: "schema upload failed" } });
              }
              return json(200, regRes.value);
            }
            if (update.schema === undefined && update.routingKey !== undefined && update.interpreter !== undefined) {
              // Apply both updates, reusing the same endpoint semantics.
              const routingRes = registry.updateRoutingKeyResult(stream, update.routingKey ?? null);
              if (Result.isError(routingRes)) return schemaMutationErrorResponse(routingRes.error);
              const interpreterRes = registry.updateInterpreterResult(stream, update.interpreter ?? null);
              if (Result.isError(interpreterRes)) return schemaMutationErrorResponse(interpreterRes.error);
              try {
                await uploadSchemaRegistry(stream, interpreterRes.value);
              } catch {
                return json(500, { error: { code: "internal", message: "schema upload failed" } });
              }
              return json(200, interpreterRes.value);
            }
            const regRes = registry.updateRegistryResult(stream, srow, update);
            if (Result.isError(regRes)) return schemaMutationErrorResponse(regRes.error);
            try {
              await uploadSchemaRegistry(stream, regRes.value);
            } catch {
              return json(500, { error: { code: "internal", message: "schema upload failed" } });
            }
            return json(200, regRes.value);
          }
          return badRequest("unsupported method");
        }

        if (touchMode) {
          const srow = db.getStream(stream);
          if (!srow || db.isDeleted(srow)) return notFound();
          if (srow.expires_at_ms != null && db.nowMs() > srow.expires_at_ms) return notFound("stream expired");

          const regRes = registry.getRegistryResult(stream);
          if (Result.isError(regRes)) return schemaReadErrorResponse(regRes.error);
          const reg = regRes.value;
          if (!isTouchEnabled(reg.interpreter)) return notFound("touch not enabled");

          const touchCfg = reg.interpreter.touch;
          const touchStorage = touchCfg.storage ?? "memory";
          const derived = resolveTouchStreamName(stream, touchCfg);

          const ensureTouchStream = (): Result<void, { kind: "touch_stream_content_type_mismatch"; message: string }> => {
            const existing = db.getStream(derived);
            if (existing) {
              if (String(existing.content_type) !== "application/json") {
                return Result.err({
                  kind: "touch_stream_content_type_mismatch",
                  message: `touch stream content-type mismatch: ${existing.content_type}`,
                });
              }
              if ((existing.stream_flags & STREAM_FLAG_TOUCH) === 0) db.addStreamFlags(derived, STREAM_FLAG_TOUCH);
              return Result.ok(undefined);
            }
            db.ensureStream(derived, { contentType: "application/json", streamFlags: STREAM_FLAG_TOUCH });
            return Result.ok(undefined);
          };

          if (touchMode.kind === "templates_activate") {
            if (req.method !== "POST") return badRequest("unsupported method");
            let body: any;
            try {
              body = await req.json();
            } catch {
              return badRequest("activate body must be valid JSON");
            }
            const templatesRaw = body?.templates;
            if (!Array.isArray(templatesRaw) || templatesRaw.length === 0) {
              return badRequest("activate.templates must be a non-empty array");
            }
            if (templatesRaw.length > 256) return badRequest("activate.templates too large (max 256)");

            const ttlRaw = body?.inactivityTtlMs;
            const inactivityTtlMs =
              ttlRaw === undefined
                ? touchCfg.templates?.defaultInactivityTtlMs ?? 60 * 60 * 1000
                : typeof ttlRaw === "number" && Number.isFinite(ttlRaw) && ttlRaw >= 0
                  ? Math.floor(ttlRaw)
                  : null;
            if (inactivityTtlMs == null) return badRequest("activate.inactivityTtlMs must be a non-negative number (ms)");

            const templates: Array<{ entity: string; fields: Array<{ name: string; encoding: any }> }> = [];
            for (const t of templatesRaw) {
              const entity = typeof t?.entity === "string" ? t.entity.trim() : "";
              const fieldsRaw = t?.fields;
              if (entity === "" || !Array.isArray(fieldsRaw) || fieldsRaw.length === 0 || fieldsRaw.length > 3) continue;
              const fields: Array<{ name: string; encoding: any }> = [];
              for (const f of fieldsRaw) {
                const name = typeof f?.name === "string" ? f.name.trim() : "";
                const encoding = f?.encoding;
                if (name === "") continue;
                fields.push({ name, encoding });
              }
              if (fields.length !== fieldsRaw.length) continue;
              templates.push({ entity, fields });
            }
            if (templates.length !== templatesRaw.length) return badRequest("activate.templates contains invalid template definitions");

            const limits = {
              maxActiveTemplatesPerStream: touchCfg.templates?.maxActiveTemplatesPerStream ?? 2048,
              maxActiveTemplatesPerEntity: touchCfg.templates?.maxActiveTemplatesPerEntity ?? 256,
            };

            let activeFromTouchOffset: string;
            if (touchStorage === "sqlite") {
              const touchRes = ensureTouchStream();
              if (Result.isError(touchRes)) return conflict(touchRes.error.message);
              const trow = db.getStream(derived)!;
              const tailSeq = trow.next_offset - 1n;
              activeFromTouchOffset = encodeOffset(trow.epoch, tailSeq);
            } else {
              activeFromTouchOffset = touch.getOrCreateJournal(derived, touchCfg).getCursor();
            }

            const res = touch.activateTemplates({
              stream,
              touchCfg,
              baseStreamNextOffset: srow.next_offset,
              activeFromTouchOffset,
              templates,
              inactivityTtlMs,
            });

            return json(200, { activated: res.activated, denied: res.denied, limits });
          }

          if (touchMode.kind === "meta") {
            if (req.method !== "GET") return badRequest("unsupported method");
            let activeTemplates = 0;
            try {
              const row = db.db.query(`SELECT COUNT(*) as cnt FROM live_templates WHERE stream=? AND state='active';`).get(stream) as any;
              activeTemplates = Number(row?.cnt ?? 0);
            } catch {
              activeTemplates = 0;
            }
            if (touchStorage === "sqlite") {
              const touchRes = ensureTouchStream();
              if (Result.isError(touchRes)) return conflict(touchRes.error.message);
              const trow = db.getStream(derived)!;
              const tailSeq = trow.next_offset - 1n;
              const currentTouchOffset = encodeOffset(trow.epoch, tailSeq);
              const oldestSeq = db.getWalOldestOffset(derived);
              const oldestCursorSeq = oldestSeq == null ? -1n : oldestSeq - 1n;
              const oldestAvailableTouchOffset = encodeOffset(trow.epoch, oldestCursorSeq);
              const clampBigInt = (v: bigint): number => {
                if (v <= 0n) return 0;
                const max = BigInt(Number.MAX_SAFE_INTEGER);
                return v > max ? Number.MAX_SAFE_INTEGER : Number(v);
              };
              return json(200, {
                mode: "sqlite",
                currentTouchOffset,
                oldestAvailableTouchOffset,
                coarseIntervalMs: touchCfg.coarseIntervalMs ?? 100,
                touchCoalesceWindowMs: touchCfg.touchCoalesceWindowMs ?? 100,
                touchRetentionMs: touchCfg.retention?.maxAgeMs ?? null,
                activeTemplates,
                touchWalRetainedRows: clampBigInt(trow.wal_rows),
                touchWalRetainedBytes: clampBigInt(trow.wal_bytes),
              });
            }
            const meta = touch.getOrCreateJournal(derived, touchCfg).getMeta();
            const runtime = touch.getTouchRuntimeSnapshot({ stream, touchCfg });
            const interp = db.getStreamInterpreter(stream);
            return json(200, {
              ...meta,
              coarseIntervalMs: touchCfg.coarseIntervalMs ?? 100,
              touchCoalesceWindowMs: touchCfg.touchCoalesceWindowMs ?? 100,
              touchRetentionMs: null,
              activeTemplates,
              lagSourceOffsets: runtime.lagSourceOffsets,
              touchMode: runtime.touchMode,
              walScannedThrough: interp ? encodeOffset(srow.epoch, interp.interpreted_through) : null,
              bucketMaxSourceOffsetSeq: meta.bucketMaxSourceOffsetSeq,
              hotFineKeys: runtime.hotFineKeys,
              hotTemplates: runtime.hotTemplates,
              hotFineKeysActive: runtime.hotFineKeysActive,
              hotFineKeysGrace: runtime.hotFineKeysGrace,
              hotTemplatesActive: runtime.hotTemplatesActive,
              hotTemplatesGrace: runtime.hotTemplatesGrace,
              fineWaitersActive: runtime.fineWaitersActive,
              coarseWaitersActive: runtime.coarseWaitersActive,
              broadFineWaitersActive: runtime.broadFineWaitersActive,
              hotKeyFilteringEnabled: runtime.hotKeyFilteringEnabled,
              hotTemplateFilteringEnabled: runtime.hotTemplateFilteringEnabled,
              scanRowsTotal: runtime.scanRowsTotal,
              scanBatchesTotal: runtime.scanBatchesTotal,
              scannedButEmitted0BatchesTotal: runtime.scannedButEmitted0BatchesTotal,
              interpretedThroughDeltaTotal: runtime.interpretedThroughDeltaTotal,
              touchesEmittedTotal: runtime.touchesEmittedTotal,
              touchesTableTotal: runtime.touchesTableTotal,
              touchesTemplateTotal: runtime.touchesTemplateTotal,
              fineTouchesDroppedDueToBudgetTotal: runtime.fineTouchesDroppedDueToBudgetTotal,
              fineTouchesSkippedColdTemplateTotal: runtime.fineTouchesSkippedColdTemplateTotal,
              fineTouchesSkippedColdKeyTotal: runtime.fineTouchesSkippedColdKeyTotal,
              fineTouchesSkippedTemplateBucketTotal: runtime.fineTouchesSkippedTemplateBucketTotal,
              waitTouchedTotal: runtime.waitTouchedTotal,
              waitTimeoutTotal: runtime.waitTimeoutTotal,
              waitStaleTotal: runtime.waitStaleTotal,
              journalFlushesTotal: runtime.journalFlushesTotal,
              journalNotifyWakeupsTotal: runtime.journalNotifyWakeupsTotal,
              journalNotifyWakeMsTotal: runtime.journalNotifyWakeMsTotal,
              journalNotifyWakeMsMax: runtime.journalNotifyWakeMsMax,
              journalTimeoutsFiredTotal: runtime.journalTimeoutsFiredTotal,
              journalTimeoutSweepMsTotal: runtime.journalTimeoutSweepMsTotal,
            });
          }

          if (touchMode.kind === "wait") {
            if (req.method !== "POST") return badRequest("unsupported method");
            const waitStartMs = Date.now();
            let body: any;
            try {
              body = await req.json();
            } catch {
              return badRequest("wait body must be valid JSON");
            }
            const keysRaw = body?.keys;
            const cursorRaw = body?.cursor;
            const sinceRaw = body?.sinceTouchOffset;
            const timeoutMsRaw = body?.timeoutMs;
            if (keysRaw !== undefined && (!Array.isArray(keysRaw) || !keysRaw.every((k: any) => typeof k === "string" && k.trim() !== ""))) {
              return badRequest("wait.keys must be a non-empty string array when provided");
            }
            const keys = Array.isArray(keysRaw) ? Array.from(new Set(keysRaw.map((k: string) => k.trim()))) : [];
            if (keys.length > 1024) return badRequest("wait.keys too large (max 1024)");
            const keyIdsRaw = body?.keyIds;
            const keyIds =
              Array.isArray(keyIdsRaw) && keyIdsRaw.length > 0
                ? Array.from(
                    new Set(
                      keyIdsRaw.map((x: any) => Number(x)).filter((n: number) => Number.isFinite(n) && Number.isInteger(n) && n >= 0 && n <= 0xffffffff)
                    )
                  ).map((n) => n >>> 0)
                : [];
            if (Array.isArray(keyIdsRaw) && keyIds.length !== keyIdsRaw.length) {
              return badRequest("wait.keyIds must be a non-empty uint32 array when provided");
            }
            if (keys.length === 0 && keyIds.length === 0) return badRequest("wait requires keys or keyIds");
            if (keyIds.length > 1024) return badRequest("wait.keyIds too large (max 1024)");
            if (touchStorage === "sqlite" && keys.length === 0) {
              return badRequest("wait.keys must be a non-empty string array in sqlite touch storage mode");
            }
            const cursorOrSince = typeof cursorRaw === "string" && cursorRaw.trim() !== "" ? cursorRaw : sinceRaw;
            if (typeof cursorOrSince !== "string" || cursorOrSince.trim() === "") {
              return badRequest(touchStorage === "memory" ? "wait.cursor must be a non-empty string" : "wait.sinceTouchOffset must be a non-empty string");
            }

            const timeoutMs =
              timeoutMsRaw === undefined ? 30_000 : typeof timeoutMsRaw === "number" && Number.isFinite(timeoutMsRaw) ? Math.max(0, Math.min(120_000, timeoutMsRaw)) : null;
            if (timeoutMs == null) return badRequest("wait.timeoutMs must be a number (ms)");

            const templateIdsUsedRaw = body?.templateIdsUsed;
            if (Array.isArray(templateIdsUsedRaw) && !templateIdsUsedRaw.every((x: any) => typeof x === "string" && x.trim() !== "")) {
              return badRequest("wait.templateIdsUsed must be a string array");
            }
            const templateIdsUsed =
              Array.isArray(templateIdsUsedRaw) && templateIdsUsedRaw.length > 0
                ? Array.from(new Set(templateIdsUsedRaw.map((s: any) => (typeof s === "string" ? s.trim() : "")).filter((s: string) => s !== "")))
                : [];
            const interestModeRaw = body?.interestMode;
            if (interestModeRaw !== undefined && interestModeRaw !== "fine" && interestModeRaw !== "coarse") {
              return badRequest("wait.interestMode must be 'fine' or 'coarse'");
            }
            const interestMode: "fine" | "coarse" = interestModeRaw === "coarse" ? "coarse" : "fine";

            if (interestMode === "fine" && templateIdsUsed.length > 0) {
              touch.heartbeatTemplates({ stream, touchCfg, templateIdsUsed });
            }

            const declareTemplatesRaw = body?.declareTemplates;
            if (Array.isArray(declareTemplatesRaw) && declareTemplatesRaw.length > 0) {
              if (declareTemplatesRaw.length > 256) return badRequest("wait.declareTemplates too large (max 256)");
              const ttlRaw = body?.inactivityTtlMs;
              const inactivityTtlMs =
                ttlRaw === undefined
                  ? touchCfg.templates?.defaultInactivityTtlMs ?? 60 * 60 * 1000
                  : typeof ttlRaw === "number" && Number.isFinite(ttlRaw) && ttlRaw >= 0
                    ? Math.floor(ttlRaw)
                    : null;
              if (inactivityTtlMs == null) return badRequest("wait.inactivityTtlMs must be a non-negative number (ms)");

              const templates: Array<{ entity: string; fields: Array<{ name: string; encoding: any }> }> = [];
              for (const t of declareTemplatesRaw) {
                const entity = typeof t?.entity === "string" ? t.entity.trim() : "";
                const fieldsRaw = t?.fields;
                if (entity === "" || !Array.isArray(fieldsRaw) || fieldsRaw.length === 0 || fieldsRaw.length > 3) continue;
                const fields: Array<{ name: string; encoding: any }> = [];
                for (const f of fieldsRaw) {
                  const name = typeof f?.name === "string" ? f.name.trim() : "";
                  const encoding = f?.encoding;
                  if (name === "") continue;
                  fields.push({ name, encoding });
                }
                if (fields.length !== fieldsRaw.length) continue;
                templates.push({ entity, fields });
              }
              if (templates.length !== declareTemplatesRaw.length) return badRequest("wait.declareTemplates contains invalid template definitions");
              let activeFromTouchOffset: string;
              if (touchStorage === "sqlite") {
                const touchRes = ensureTouchStream();
                if (Result.isError(touchRes)) return conflict(touchRes.error.message);
                const trow = db.getStream(derived)!;
                const tailSeq = trow.next_offset - 1n;
                activeFromTouchOffset = encodeOffset(trow.epoch, tailSeq);
              } else {
                activeFromTouchOffset = touch.getOrCreateJournal(derived, touchCfg).getCursor();
              }
              touch.activateTemplates({
                stream,
                touchCfg,
                baseStreamNextOffset: srow.next_offset,
                activeFromTouchOffset,
                templates,
                inactivityTtlMs,
              });
            }

            if (touchStorage === "memory") {
              const j = touch.getOrCreateJournal(derived, touchCfg);
              const runtime = touch.getTouchRuntimeSnapshot({ stream, touchCfg });
              let rawFineKeyIds = keyIds;
              if (keyIds.length === 0) {
                const parsedKeyIds: number[] = [];
                for (const key of keys) {
                  const keyIdRes = touchKeyIdFromRoutingKeyResult(key);
                  if (Result.isError(keyIdRes)) return internalError();
                  parsedKeyIds.push(keyIdRes.value);
                }
                rawFineKeyIds = parsedKeyIds;
              }
              const templateWaitKeyIds =
                templateIdsUsed.length > 0
                  ? Array.from(new Set(templateIdsUsed.map((templateId) => templateKeyIdFor(templateId) >>> 0)))
                  : [];
              let waitKeyIds = rawFineKeyIds;
              let effectiveWaitKind: "fineKey" | "templateKey" | "tableKey" = "fineKey";

              if (interestMode === "coarse") {
                effectiveWaitKind = "tableKey";
              } else if (runtime.touchMode === "restricted" && templateIdsUsed.length > 0) {
                effectiveWaitKind = "templateKey";
              } else if (runtime.touchMode === "coarseOnly" && templateIdsUsed.length > 0) {
                effectiveWaitKind = "tableKey";
              }

              if (effectiveWaitKind === "templateKey") {
                waitKeyIds = templateWaitKeyIds;
              } else if (effectiveWaitKind === "tableKey") {
                if (templateIdsUsed.length > 0) {
                  const entities = touch.resolveTemplateEntitiesForWait({ stream, templateIdsUsed });
                  waitKeyIds = Array.from(new Set(entities.map((entity) => tableKeyIdFor(entity) >>> 0)));
                }
              }

              // Keep fine waits resilient to runtime mode flips: include template-key
              // fallbacks even when the current mode is fine. This avoids starvation
              // when a long-poll starts in fine mode but DS degrades to restricted
              // before that waiter naturally re-issues.
              if (interestMode === "fine" && effectiveWaitKind === "fineKey" && templateWaitKeyIds.length > 0) {
                const merged = new Set<number>();
                for (const keyId of waitKeyIds) merged.add(keyId >>> 0);
                for (const keyId of templateWaitKeyIds) merged.add(keyId >>> 0);
                waitKeyIds = Array.from(merged);
              }

              if (waitKeyIds.length === 0) {
                waitKeyIds = rawFineKeyIds;
                effectiveWaitKind = "fineKey";
              }
              const hotInterestKeyIds = interestMode === "fine" ? rawFineKeyIds : waitKeyIds;
              const releaseHotInterest = touch.beginHotWaitInterest({
                stream,
                touchCfg,
                keyIds: hotInterestKeyIds,
                templateIdsUsed,
                interestMode,
              });
              try {
                let sinceGen: number;
                if (cursorOrSince === "now") {
                  sinceGen = j.getGeneration();
                } else {
                  const parsed = parseTouchCursor(cursorOrSince);
                  if (!parsed) return badRequest("wait.cursor must be in the form <epochHex>:<generation> or 'now'");
                  if (parsed.epoch !== j.getEpoch()) {
                    const latencyMs = Date.now() - waitStartMs;
                    touch.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "stale", latencyMs });
                    return json(200, {
                      stale: true,
                      cursor: j.getCursor(),
                      epoch: j.getEpoch(),
                      generation: j.getGeneration(),
                      effectiveWaitKind,
                      bucketMaxSourceOffsetSeq: j.getLastFlushedSourceOffsetSeq().toString(),
                      flushAtMs: j.getLastFlushAtMs(),
                      bucketStartMs: j.getLastBucketStartMs(),
                      error: { code: "stale", message: "cursor epoch mismatch; rerun/re-subscribe and start from cursor" },
                    });
                  }
                  sinceGen = parsed.generation;
                }

                // Clamp bogus future cursors (defensive).
                const nowGen = j.getGeneration();
                if (sinceGen > nowGen) sinceGen = nowGen;

                // Fast path: already touched since cursor.
                if (j.maybeTouchedSinceAny(waitKeyIds, sinceGen)) {
                  const latencyMs = Date.now() - waitStartMs;
                  touch.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
                  return json(200, {
                    touched: true,
                    cursor: j.getCursor(),
                    effectiveWaitKind,
                    bucketMaxSourceOffsetSeq: j.getLastFlushedSourceOffsetSeq().toString(),
                    flushAtMs: j.getLastFlushAtMs(),
                    bucketStartMs: j.getLastBucketStartMs(),
                  });
                }

                const deadline = Date.now() + timeoutMs;
                const remaining = deadline - Date.now();
                if (remaining <= 0) {
                  const latencyMs = Date.now() - waitStartMs;
                  touch.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "timeout", latencyMs });
                  return json(200, {
                    touched: false,
                    cursor: j.getCursor(),
                    effectiveWaitKind,
                    bucketMaxSourceOffsetSeq: j.getLastFlushedSourceOffsetSeq().toString(),
                    flushAtMs: j.getLastFlushAtMs(),
                    bucketStartMs: j.getLastBucketStartMs(),
                  });
                }

                // Avoid lost-wakeup races by capturing the current generation before waiting.
                const afterGen = j.getGeneration();
                const hit = await j.waitForAny({ keys: waitKeyIds, afterGeneration: afterGen, timeoutMs: remaining, signal: req.signal });
                if (req.signal.aborted) return new Response(null, { status: 204 });

                if (hit == null) {
                  const latencyMs = Date.now() - waitStartMs;
                  touch.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "timeout", latencyMs });
                  return json(200, {
                    touched: false,
                    cursor: j.getCursor(),
                    effectiveWaitKind,
                    bucketMaxSourceOffsetSeq: j.getLastFlushedSourceOffsetSeq().toString(),
                    flushAtMs: j.getLastFlushAtMs(),
                    bucketStartMs: j.getLastBucketStartMs(),
                  });
                }

                const latencyMs = Date.now() - waitStartMs;
                touch.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
                return json(200, {
                  touched: true,
                  cursor: j.getCursor(),
                  effectiveWaitKind,
                  bucketMaxSourceOffsetSeq: hit.bucketMaxSourceOffsetSeq.toString(),
                  flushAtMs: hit.flushAtMs,
                  bucketStartMs: hit.bucketStartMs,
                });
              } finally {
                releaseHotInterest();
              }
            }

            // touchStorage === "sqlite"
            const touchRes = ensureTouchStream();
            if (Result.isError(touchRes)) return conflict(touchRes.error.message);
            const trow = db.getStream(derived)!;
            const tailSeq = trow.next_offset - 1n;
            const currentTouchOffset = encodeOffset(trow.epoch, tailSeq);
            const oldestSeq = db.getWalOldestOffset(derived);
            const oldestCursorSeq = oldestSeq == null ? -1n : oldestSeq - 1n;
            const oldestAvailableTouchOffset = encodeOffset(trow.epoch, oldestCursorSeq);

            const staleBody = () => ({
              stale: true,
              currentTouchOffset,
              oldestAvailableTouchOffset,
              error: {
                code: "stale",
                message:
                  "offset is older than oldestAvailableTouchOffset; rerun/re-subscribe and start from currentTouchOffset",
              },
            });

            let sinceSeq: bigint;
            if (cursorOrSince === "now") {
              sinceSeq = tailSeq;
            } else {
              const sinceRes = parseOffsetResult(cursorOrSince);
              if (Result.isError(sinceRes)) return badRequest(sinceRes.error.message);
              sinceSeq = offsetToSeqOrNeg1(sinceRes.value);
            }

            if (sinceSeq < oldestCursorSeq) {
              const latencyMs = Date.now() - waitStartMs;
              touch.recordWaitMetrics({ stream, touchCfg, keysCount: keys.length, outcome: "stale", latencyMs });
              return json(200, staleBody());
            }

            const encoder = new TextEncoder();
            let keyBytes: Uint8Array[] | null = null;
            const ensureKeyBytes = (): Uint8Array[] => {
              if (keyBytes) return keyBytes;
              keyBytes = keys.map((k) => encoder.encode(k));
              return keyBytes;
            };

            // Only use in-memory key notifications for small key sets; for huge key
            // sets this would cause O(keysPerWait) register/unregister overhead.
            const KEY_NOTIFIER_MAX_KEYS = 32;
            const useKeyNotifier = keys.length <= KEY_NOTIFIER_MAX_KEYS;

            let cursorSeq = sinceSeq;
            const deadline = Date.now() + timeoutMs;
            for (;;) {
              if (req.signal.aborted) return new Response(null, { status: 204 });

              const latest = db.getStream(derived);
              if (!latest || db.isDeleted(latest)) return notFound();

              const endSeq = latest.next_offset - 1n;
              const endTouchOffset = encodeOffset(latest.epoch, endSeq);

              const match = cursorSeq < endSeq ? db.findFirstWalOffsetForRoutingKeys(derived, cursorSeq, endSeq, ensureKeyBytes()) : null;
              if (match != null) {
                let touchedKey: string | null = null;
                try {
                  const row = db.db.query(`SELECT routing_key FROM wal WHERE stream=? AND offset=? LIMIT 1;`).get(derived, match) as any;
                  if (row && row.routing_key) {
                    touchedKey = new TextDecoder().decode(row.routing_key as Uint8Array);
                  }
                } catch {
                  touchedKey = null;
                }
                const latencyMs = Date.now() - waitStartMs;
                touch.recordWaitMetrics({ stream, touchCfg, keysCount: keys.length, outcome: "touched", latencyMs });
                return json(200, {
                  touched: true,
                  touchOffset: encodeOffset(latest.epoch, match),
                  currentTouchOffset: endTouchOffset,
                  touchedKeys: touchedKey ? [touchedKey] : [],
                });
              }

              const remaining = deadline - Date.now();
              if (remaining <= 0) {
                // Return the tail as-of the timeout moment, not as-of the last scan.
                const latest2 = db.getStream(derived);
                if (!latest2 || db.isDeleted(latest2)) return notFound();
                const endSeq2 = latest2.next_offset - 1n;
                const endTouchOffset2 = encodeOffset(latest2.epoch, endSeq2);
                const latencyMs = Date.now() - waitStartMs;
                touch.recordWaitMetrics({ stream, touchCfg, keysCount: keys.length, outcome: "timeout", latencyMs });
                return json(200, { touched: false, currentTouchOffset: endTouchOffset2 });
              }

              if (useKeyNotifier) {
                const hit = await touchRoutingKeyNotifier.waitForAny({
                  stream: derived,
                  keys,
                  afterSeq: endSeq,
                  timeoutMs: remaining,
                  signal: req.signal,
                });
                if (req.signal.aborted) return new Response(null, { status: 204 });

                const latest2 = db.getStream(derived);
                if (!latest2 || db.isDeleted(latest2)) return notFound();
                const endSeq2 = latest2.next_offset - 1n;
                const endTouchOffset2 = encodeOffset(latest2.epoch, endSeq2);

                if (hit == null) {
                  const latencyMs = Date.now() - waitStartMs;
                  touch.recordWaitMetrics({ stream, touchCfg, keysCount: keys.length, outcome: "timeout", latencyMs });
                  return json(200, { touched: false, currentTouchOffset: endTouchOffset2 });
                }

                const latencyMs = Date.now() - waitStartMs;
                touch.recordWaitMetrics({ stream, touchCfg, keysCount: keys.length, outcome: "touched", latencyMs });
                return json(200, {
                  touched: true,
                  touchOffset: encodeOffset(latest2.epoch, hit.seq),
                  currentTouchOffset: endTouchOffset2,
                  touchedKeys: [hit.key],
                });
              }

              // Fallback: wait for any new touch rows. Cursor advances to the tail we already scanned.
              await notifier.waitFor(derived, endSeq, remaining, req.signal);
              if (req.signal.aborted) return new Response(null, { status: 204 });
              cursorSeq = endSeq;
            }
          }

          // touchMode.kind === "read"
          if (req.method !== "GET") return badRequest("unsupported method");
          if (touchStorage === "memory") {
            return notFound("touch stream read not supported in memory mode; use /touch/wait");
          }
          const touchRes = ensureTouchStream();
          if (Result.isError(touchRes)) return conflict(touchRes.error.message);
          const trow = db.getStream(derived)!;
          const tailSeq = trow.next_offset - 1n;
          const currentTouchOffset = encodeOffset(trow.epoch, tailSeq);
          const oldestSeq = db.getWalOldestOffset(derived);
          const oldestCursorSeq = oldestSeq == null ? -1n : oldestSeq - 1n;
          const oldestAvailableTouchOffset = encodeOffset(trow.epoch, oldestCursorSeq);

          const staleBody = () => ({
            stale: true,
            currentTouchOffset,
            oldestAvailableTouchOffset,
            error: {
              code: "stale",
              message:
                "offset is older than oldestAvailableTouchOffset; rerun/re-subscribe and start from currentTouchOffset",
            },
          });

          // Default to "subscribe from now" for companion touches.
          const nextUrl = new URL(req.url, "http://localhost");
          if (!nextUrl.searchParams.has("offset")) {
            const sinceParam = nextUrl.searchParams.get("since");
            if (sinceParam) {
              const sinceRes = parseTimestampMsResult(sinceParam);
              if (Result.isError(sinceRes)) return badRequest(sinceRes.error.message);
              const key = touchMode.key ?? nextUrl.searchParams.get("key");
              const computedRes = await reader.seekOffsetByTimestampResult(derived, sinceRes.value, key ?? null);
              if (Result.isError(computedRes)) return readerErrorResponse(computedRes.error);
              nextUrl.searchParams.set("offset", computedRes.value);
              nextUrl.searchParams.delete("since");
            } else {
              nextUrl.searchParams.set("offset", "now");
            }
          }

          const requestedOffset = nextUrl.searchParams.get("offset") ?? "";
          if (requestedOffset !== "now") {
            const requestedRes = parseOffsetResult(requestedOffset);
            if (Result.isError(requestedRes)) return badRequest(requestedRes.error.message);
            const seq = offsetToSeqOrNeg1(requestedRes.value);
            if (seq < oldestCursorSeq) return json(409, staleBody());
          }

          // Delegate to the standard stream read path for the internal derived stream.
          const touchPath = touchMode.key
            ? `${streamPrefix}${encodeURIComponent(derived)}/pk/${encodeURIComponent(touchMode.key)}`
            : `${streamPrefix}${encodeURIComponent(derived)}`;
          nextUrl.pathname = touchPath;
          return fetch(new Request(nextUrl.toString(), req));
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
                if (appendRes.error.kind === "overloaded") return json(429, { error: { code: "overloaded", message: "ingest queue full" } });
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
              if (appendRes.error.kind === "overloaded") return json(429, { error: { code: "overloaded", message: "ingest queue full" } });
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
        }

        if (req.method === "DELETE") {
          const deleted = db.deleteStream(stream);
          if (!deleted) return notFound();
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
            if (err.kind === "overloaded") return json(429, { error: { code: "overloaded", message: "ingest queue full" } });
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

          const liveParam = url.searchParams.get("live") ?? "";
          const cursorParam = url.searchParams.get("cursor");
          let mode: "catchup" | "long-poll" | "sse";
          if (liveParam === "" || liveParam === "false" || liveParam === "0") mode = "catchup";
          else if (liveParam === "long-poll" || liveParam === "true" || liveParam === "1") mode = "long-poll";
          else if (liveParam === "sse") mode = "sse";
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
              ? `W/\"slice:${canonicalizeOffset(offset!)}:${batch.nextOffset}:key=${key ?? ""}:fmt=${format}\"`
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
                      const batchRes = await reader.readResult({ stream, offset: currentOffset, key: key ?? null, format });
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
                const batchRes = await reader.readResult({ stream, offset: currentOffset, key: key ?? null, format });
                if (Result.isError(batchRes)) return readerErrorResponse(batchRes.error);
                const batch = batchRes.value;
                if (batch.records.length > 0) {
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
              const batchRes = await reader.readResult({ stream, offset: currentOffset, key: key ?? null, format });
              if (Result.isError(batchRes)) return readerErrorResponse(batchRes.error);
              const batch = batchRes.value;
              if (batch.records.length > 0) {
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

          const batchRes = await reader.readResult({ stream, offset, key: key ?? null, format });
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
    ingest.stop();
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
      touchRoutingKeyNotifier,
      reader,
      segmenter,
      uploader,
      indexer,
      metrics,
      registry,
      touch,
      stats,
      backpressure,
      memory,
    },
  };
}
