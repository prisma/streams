import { randomBytes } from "node:crypto";
import { Result } from "better-result";
import type { Config } from "../config";
import type { IndexRunRow, SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SegmentDiskCache } from "../segment/cache";
import { retry, retryAbortable } from "../util/retry";
import { indexRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { siphash24 } from "../util/siphash";
import { binaryFuseContains, buildBinaryFuseResult } from "./binary_fuse";
import { decodeIndexRunResult, encodeIndexRunResult, RUN_TYPE_MASK16, RUN_TYPE_POSTINGS, type IndexRun } from "./run_format";
import { IndexRunCache } from "./run_cache";
import type { Metrics } from "../metrics";
import { dsError } from "../util/ds_error.ts";
import { yieldToEventLoop } from "../util/yield";
import { RuntimeMemorySampler } from "../runtime_memory_sampler";
import { ConcurrencyGate } from "../concurrency_gate";
import type { ForegroundActivityTracker } from "../foreground_activity";
import type { AggSectionView } from "../search/agg_format";
import type { ColSectionView } from "../search/col_format";
import type { FtsSectionView } from "../search/fts_format";
import type { MetricsBlockSectionView } from "../profiles/metrics/block_format";
import type { SchemaRegistryStore } from "../schema/registry";
import type { RoutingKeyLexiconListResult } from "./lexicon_indexer";
import { IndexSegmentLocalityManager } from "./segment_locality";
import { IndexBuildWorkerPool } from "./index_build_worker_pool";
import type { RoutingCompactionRunSource } from "./routing_compaction_build";
import { asyncIndexActionMemoryDetail, beginAsyncIndexAction } from "./async_index_actions";
import { RoutingLexiconL0BuildCoordinator } from "./routing_lexicon_l0_build_coordinator";

export type IndexCandidate = { segments: Set<number>; indexedThrough: number };
type IndexBuildError = { kind: "invalid_index_build"; message: string };
export type CompanionSectionLookupStats = {
  sectionGetMs: number;
  decodeMs: number;
};

export type StreamIndexLookup = {
  start(): void;
  stop(): void;
  enqueue(stream: string): void;
  candidateSegmentsForRoutingKey(stream: string, keyBytes: Uint8Array): Promise<IndexCandidate | null>;
  candidateSegmentsForSecondaryIndex(stream: string, indexName: string, keyBytes: Uint8Array): Promise<IndexCandidate | null>;
  getAggSegmentCompanion(stream: string, segmentIndex: number): Promise<AggSectionView | null>;
  getColSegmentCompanion(stream: string, segmentIndex: number): Promise<ColSectionView | null>;
  getFtsSegmentCompanion(stream: string, segmentIndex: number): Promise<FtsSectionView | null>;
  getFtsSegmentCompanionWithStats?(
    stream: string,
    segmentIndex: number
  ): Promise<{ companion: FtsSectionView | null; stats: CompanionSectionLookupStats }>;
  getMetricsBlockSegmentCompanion(stream: string, segmentIndex: number): Promise<MetricsBlockSectionView | null>;
  listRoutingKeysResult?(stream: string, after: string | null, limit: number): Promise<Result<RoutingKeyLexiconListResult, { kind: string; message: string }>>;
  getLocalStorageUsage?(stream: string): {
    routing_index_cache_bytes: number;
    exact_index_cache_bytes: number;
    companion_cache_bytes: number;
    lexicon_index_cache_bytes: number;
  };
};

function invalidIndexBuild<T = never>(message: string): Result<T, IndexBuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function errorMessage(e: unknown): string {
  return String((e as any)?.message ?? e);
}

export class IndexManager {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly segmentCache?: SegmentDiskCache;
  private readonly runDiskCache?: SegmentDiskCache;
  private readonly runCache: IndexRunCache;
  private readonly span: number;
  private readonly compactionFanout: number;
  private readonly maxLevel: number;
  private readonly retireGenWindow: number;
  private readonly retireMinMs: number;
  private readonly buildQueue = new Set<string>();
  private readonly compactionQueue = new Set<string>();
  private readonly building = new Set<string>();
  private readonly compacting = new Set<string>();
  private readonly metrics?: Metrics;
  private lastRunCacheHits = 0;
  private lastRunCacheMisses = 0;
  private lastRunCacheEvictions = 0;
  private lastDiskHits = 0;
  private lastDiskMisses = 0;
  private lastDiskEvictions = 0;
  private lastDiskBytesAdded = 0;
  private runningBuildTick = false;
  private runningCompactionTick = false;
  private readonly publishManifest?: (stream: string) => Promise<void>;
  private readonly onMetadataChanged?: (stream: string) => void;
  private readonly memorySampler?: RuntimeMemorySampler;
  private readonly registry?: SchemaRegistryStore;
  private readonly asyncGate: ConcurrencyGate;
  private readonly foregroundActivity?: ForegroundActivityTracker;
  private readonly buildWorkers: IndexBuildWorkerPool;
  private readonly ownsBuildWorkers: boolean;
  private readonly segmentLocality?: IndexSegmentLocalityManager;
  private readonly routingLexiconBuilds: RoutingLexiconL0BuildCoordinator;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    segmentCache: SegmentDiskCache | undefined,
    publishManifest?: (stream: string) => Promise<void>,
    metrics?: Metrics,
    onMetadataChanged?: (stream: string) => void,
    memorySampler?: RuntimeMemorySampler,
    registry?: SchemaRegistryStore,
    asyncGate?: ConcurrencyGate,
    foregroundActivity?: ForegroundActivityTracker,
    segmentLocality?: IndexSegmentLocalityManager,
    buildWorkers?: IndexBuildWorkerPool,
    routingLexiconBuilds?: RoutingLexiconL0BuildCoordinator
  ) {
    this.cfg = cfg;
    this.db = db;
    this.os = os;
    this.segmentCache = segmentCache;
    this.publishManifest = publishManifest;
    this.span = cfg.indexL0SpanSegments;
    this.compactionFanout = cfg.indexCompactionFanout;
    this.maxLevel = cfg.indexMaxLevel;
    this.retireGenWindow = Math.max(0, cfg.indexRetireGenWindow);
    this.retireMinMs = Math.max(0, cfg.indexRetireMinMs);
    this.metrics = metrics;
    this.onMetadataChanged = onMetadataChanged;
    this.memorySampler = memorySampler;
    this.registry = registry;
    this.asyncGate = asyncGate ?? new ConcurrencyGate(1);
    this.foregroundActivity = foregroundActivity;
    this.segmentLocality = segmentLocality;
    this.ownsBuildWorkers = !buildWorkers;
    this.buildWorkers = buildWorkers ?? new IndexBuildWorkerPool(Math.max(1, cfg.indexBuilders));
    if (this.ownsBuildWorkers) this.buildWorkers.start();
    this.routingLexiconBuilds = routingLexiconBuilds ?? new RoutingLexiconL0BuildCoordinator(this.buildWorkers);
    this.runCache = new IndexRunCache(cfg.indexRunMemoryCacheBytes);
    this.runDiskCache = cfg.indexRunCacheMaxBytes > 0 ? new SegmentDiskCache(`${cfg.rootDir}/cache/index`, cfg.indexRunCacheMaxBytes) : undefined;
  }

  private async yieldBackgroundWork(): Promise<void> {
    if (this.foregroundActivity) {
      await this.foregroundActivity.yieldBackgroundWork();
      return;
    }
    await yieldToEventLoop();
  }

  start(): void {
    // Scheduling is owned by the global index manager.
  }

  stop(): void {
    this.runningBuildTick = false;
    this.runningCompactionTick = false;
    if (this.ownsBuildWorkers) this.buildWorkers.stop();
  }

  enqueue(stream: string): void {
    if (this.span <= 0) return;
    this.buildQueue.add(stream);
    this.compactionQueue.add(stream);
  }

  async candidateSegmentsForRoutingKey(stream: string, keyBytes: Uint8Array): Promise<IndexCandidate | null> {
    if (this.span <= 0) return null;
    if (!this.isRoutingConfigured(stream)) return null;
    const state = this.db.getIndexState(stream);
    if (!state) return null;
    const runs = this.db.listIndexRuns(stream);
    if (runs.length === 0 && state.indexed_through === 0) return null;

    const fp = siphash24(state.index_secret, keyBytes);
    const segments = new Set<number>();
    for (const meta of runs) {
      const runRes = await this.loadRunResult(meta);
      if (Result.isError(runRes)) continue;
      const run = runRes.value;
      if (!run) continue;
      if (run.filter && !binaryFuseContains(run.filter, fp)) continue;
      if (run.runType === RUN_TYPE_MASK16 && run.masks) {
        const idx = binarySearch(run.fingerprints, fp);
        if (idx >= 0) {
          const mask = run.masks[idx];
          for (let bit = 0; bit < 16; bit++) {
            if ((mask & (1 << bit)) !== 0) segments.add(run.meta.startSegment + bit);
          }
        }
      } else if (run.postings) {
        const idx = binarySearch(run.fingerprints, fp);
        if (idx >= 0) {
          for (const seg of run.postings[idx]) segments.add(seg);
        }
      }
    }
    return { segments, indexedThrough: state.indexed_through };
  }

  async candidateSegmentsForSecondaryIndex(_stream: string, _indexName: string, _keyBytes: Uint8Array): Promise<IndexCandidate | null> {
    return null;
  }

  async getColSegmentCompanion(_stream: string, _segmentIndex: number): Promise<ColSectionView | null> {
    return null;
  }

  async getAggSegmentCompanion(_stream: string, _segmentIndex: number): Promise<AggSectionView | null> {
    return null;
  }

  async getFtsSegmentCompanion(_stream: string, _segmentIndex: number): Promise<FtsSectionView | null> {
    return null;
  }

  async getMetricsBlockSegmentCompanion(_stream: string, _segmentIndex: number): Promise<MetricsBlockSectionView | null> {
    return null;
  }

  getLocalCacheBytes(stream: string): number {
    if (!this.runDiskCache) return 0;
    return this.runDiskCache.bytesForObjectKeyPrefix(`streams/${streamHash16Hex(stream)}/index/`);
  }

  getMemoryStats(): {
    runCacheBytes: number;
    runCacheEntries: number;
    runDiskCacheBytes: number;
    runDiskCacheEntries: number;
    runDiskMappedBytes: number;
    runDiskMappedEntries: number;
    runDiskPinnedEntries: number;
  } {
    const mem = this.runCache.stats();
    const disk = this.runDiskCache?.stats();
    return {
      runCacheBytes: mem.usedBytes,
      runCacheEntries: mem.entries,
      runDiskCacheBytes: disk?.usedBytes ?? 0,
      runDiskCacheEntries: disk?.entryCount ?? 0,
      runDiskMappedBytes: disk?.mappedBytes ?? 0,
      runDiskMappedEntries: disk?.mappedEntryCount ?? 0,
      runDiskPinnedEntries: disk?.pinnedEntryCount ?? 0,
    };
  }

  async tick(): Promise<void> {
    if (this.metrics) {
      this.metrics.record(
        "tieredstore.index.build.queue_len",
        this.buildQueue.size + this.compactionQueue.size,
        "count"
      );
      this.metrics.record("tieredstore.index.builds_inflight", this.building.size + this.compacting.size, "count");
    }
    await Promise.all([this.runOneBuildTask(), this.runOneCompactionTask()]);
    this.recordCacheStats();
  }

  async runOneBuildTask(): Promise<boolean> {
    if (this.runningBuildTick) return false;
    const stream = this.takeNextStream(this.buildQueue);
    if (!stream) return false;
    this.runningBuildTick = true;
    try {
      if (!this.isRoutingConfigured(stream)) {
        const hadRoutingState = !!this.db.getIndexState(stream) || this.db.listIndexRunsAll(stream).length > 0;
        if (hadRoutingState) {
          this.db.deleteIndex(stream);
          this.onMetadataChanged?.(stream);
          if (this.publishManifest) {
            try {
              await this.publishManifest(stream);
            } catch {
              // ignore and retry on next enqueue
            }
          }
        }
        this.compactionQueue.delete(stream);
        return true;
      }
      try {
        const buildRes = await this.maybeBuildRuns(stream);
        if (Result.isError(buildRes)) {
          console.error("index build failed", stream, buildRes.error.message);
          this.buildQueue.add(stream);
        }
      } catch (e) {
        const msg = String((e as any)?.message ?? e);
        const lower = msg.toLowerCase();
        if (!lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
          console.error("index build failed", stream, e);
        }
        this.buildQueue.add(stream);
      }
      return true;
    } finally {
      this.runningBuildTick = false;
    }
  }

  async runOneCompactionTask(): Promise<boolean> {
    if (this.runningCompactionTick) return false;
    const stream = this.takeNextStream(this.compactionQueue);
    if (!stream) return false;
    this.runningCompactionTick = true;
    try {
      if (!this.isRoutingConfigured(stream)) {
        this.buildQueue.delete(stream);
        return true;
      }
      try {
        const compactRes = await this.maybeCompactRuns(stream);
        if (Result.isError(compactRes)) {
          console.error("index compaction failed", stream, compactRes.error.message);
          this.compactionQueue.add(stream);
        }
      } catch (e) {
        const msg = String((e as any)?.message ?? e);
        const lower = msg.toLowerCase();
        if (!lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
          console.error("index compaction failed", stream, e);
        }
        this.compactionQueue.add(stream);
      }
      return true;
    } finally {
      this.runningCompactionTick = false;
    }
  }

  private takeNextStream(queue: Set<string>): string | null {
    const next = queue.values().next();
    if (next.done) return null;
    const stream = next.value as string;
    queue.delete(stream);
    return stream;
  }

  private async maybeBuildRuns(stream: string): Promise<Result<void, IndexBuildError>> {
    if (this.span <= 0) return Result.ok(undefined);
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      return await this.asyncGate.run(async () => {
        let state = this.db.getIndexState(stream);
        if (!state) {
          const secret = randomBytes(16);
          this.db.upsertIndexState(stream, secret, 0);
          state = this.db.getIndexState(stream);
        }
        if (!state) return Result.ok(undefined);
        if (this.metrics) {
          const lag = Math.max(0, this.db.countUploadedSegments(stream) - state.indexed_through);
          this.metrics.record("tieredstore.index.lag.segments", lag, "count", undefined, stream);
        }
        const indexedThrough = state.indexed_through;
        const uploadedCount = this.db.countUploadedSegments(stream);
        if (uploadedCount < indexedThrough + this.span) return Result.ok(undefined);
        const start = indexedThrough;
        const end = start + this.span - 1;
        const segments: SegmentRow[] = [];
        for (let i = start; i <= end; i++) {
          const seg = this.db.getSegmentByIndex(stream, i);
          if (!seg || !seg.r2_etag) return Result.ok(undefined);
          segments.push(seg);
        }
        const t0 = Date.now();
        const segmentLeaseRes = this.segmentLocality
          ? await this.segmentLocality.acquireRoutingWindowResult(stream, segments)
          : Result.err({ kind: "missing_segment" as const, message: "routing indexing requires a segment disk cache" });
        if (Result.isError(segmentLeaseRes)) {
          if (segmentLeaseRes.error.kind === "index_cache_overloaded") {
            this.buildQueue.add(stream);
            this.onMetadataChanged?.(stream);
            return Result.ok(undefined);
          }
          return invalidIndexBuild(segmentLeaseRes.error.message);
        }
        const segmentLease = segmentLeaseRes.value;
        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "routing_l0_build",
          targetKind: "routing_index",
          inputKind: "segment",
          inputCount: segments.length,
          inputSizeBytes: segments.reduce((sum, segment) => sum + BigInt(segment.size_bytes), 0n),
          startSegment: start,
          endSegment: end,
          detail: {
            level: 0,
            input_payload_bytes: segments.reduce((sum, segment) => sum + Number(segment.payload_bytes), 0),
          },
        });
        let runPayloadRes;
        try {
          runPayloadRes = this.memorySampler
            ? await this.memorySampler.track(
                "routing_lexicon_l0",
                { stream, start_segment: start, end_segment: end },
                () =>
                  this.routingLexiconBuilds.buildWindowResult({
                    stream,
                    sourceKind: "routing_key",
                    sourceName: "",
                    cacheToken: Buffer.from(state.index_secret).toString("hex"),
                    startSegment: start,
                    span: this.span,
                    secret: state.index_secret,
                    segments: segmentLease.localSegments.map((segment) => ({
                      segmentIndex: segment.segmentIndex,
                      localPath: segment.localPath,
                    })),
                  })
              )
            : await this.routingLexiconBuilds.buildWindowResult({
              stream,
              sourceKind: "routing_key",
              sourceName: "",
              cacheToken: Buffer.from(state.index_secret).toString("hex"),
              startSegment: start,
                span: this.span,
                secret: state.index_secret,
                segments: segmentLease.localSegments.map((segment) => ({
                  segmentIndex: segment.segmentIndex,
                  localPath: segment.localPath,
                })),
              });
        } finally {
          segmentLease.release();
        }
        if (Result.isError(runPayloadRes)) {
          action.fail(runPayloadRes.error.message);
          return invalidIndexBuild(runPayloadRes.error.message);
        }
        const buildTelemetryDetail = asyncIndexActionMemoryDetail(runPayloadRes.value.jobTelemetry);
        const run = runPayloadRes.value.output.routing;
        const elapsedNs = BigInt(Date.now() - t0) * 1_000_000n;
        const persistRes = await this.persistRunPayloadResult(run.meta, run.payload, stream);
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              output_record_count: run.meta.recordCount,
              output_filter_len: run.meta.filterLen,
              shared_build_cache_status: runPayloadRes.value.cacheStatus,
              ...buildTelemetryDetail,
            },
          });
          return persistRes;
        }
        const sizeBytes = persistRes.value;
        this.db.insertIndexRun({
          run_id: run.meta.runId,
          stream,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          size_bytes: sizeBytes,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        if (this.metrics) {
          this.metrics.record("tieredstore.index.build.latency", Number(elapsedNs), "ns", { level: String(run.meta.level) }, stream);
          this.metrics.record("tieredstore.index.runs.built", 1, "count", { level: String(run.meta.level) }, stream);
          this.recordActiveRuns(stream);
        }
        const nextIndexedThrough = end + 1;
        this.db.updateIndexedThrough(stream, nextIndexedThrough);
        state.indexed_through = nextIndexedThrough;
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore manifest publish errors; will be retried by uploader/indexer
          }
        }
        if (this.db.countUploadedSegments(stream) >= nextIndexedThrough + this.span) this.buildQueue.add(stream);
        this.compactionQueue.add(stream);
        action.succeed({
          outputCount: 1,
          outputSizeBytes: BigInt(sizeBytes),
          detail: {
            output_record_count: run.meta.recordCount,
            output_filter_len: run.meta.filterLen,
            shared_build_cache_status: runPayloadRes.value.cacheStatus,
            ...buildTelemetryDetail,
          },
        });
        return Result.ok(undefined);
      });
    } finally {
      this.building.delete(stream);
    }
  }

  private async maybeCompactRuns(stream: string): Promise<Result<void, IndexBuildError>> {
    if (this.span <= 0) return Result.ok(undefined);
    if (this.compactionFanout <= 1) return Result.ok(undefined);
    if (this.compacting.has(stream)) return Result.ok(undefined);
    this.compacting.add(stream);
    try {
      return await this.asyncGate.run(async () => {
        const group = this.findCompactionGroup(stream);
        if (!group) {
          await this.gcRetiredRuns(stream);
          return Result.ok(undefined);
        }
        const t0 = Date.now();
        const { level, runs } = group;
        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "routing_compaction_build",
          targetKind: "routing_index",
          inputKind: "run",
          inputCount: runs.length,
          inputSizeBytes: runs.reduce((sum, run) => sum + BigInt(run.size_bytes), 0n),
          startSegment: runs[0]?.start_segment ?? null,
          endSegment: runs[runs.length - 1]?.end_segment ?? null,
          detail: {
            level_from: level,
            level_to: level + 1,
          },
        });
        const runRes = await this.buildCompactedRunResult(stream, level + 1, runs);
        if (Result.isError(runRes)) {
          action.fail(runRes.error.message);
          return runRes;
        }
        const buildTelemetryDetail = asyncIndexActionMemoryDetail(runRes.value.jobTelemetry);
        const run = runRes.value.run;
        const elapsedNs = BigInt(Date.now() - t0) * 1_000_000n;
        const persistRes = await this.persistRunResult(run, stream);
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              output_record_count: run.meta.recordCount,
              output_filter_len: run.meta.filterLen,
              ...buildTelemetryDetail,
            },
          });
          return persistRes;
        }
        const sizeBytes = persistRes.value;
        this.db.insertIndexRun({
          run_id: run.meta.runId,
          stream,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          size_bytes: sizeBytes,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        const state = this.db.getIndexState(stream);
        if (state && run.meta.endSegment + 1 > state.indexed_through) {
          this.db.updateIndexedThrough(stream, run.meta.endSegment + 1);
          state.indexed_through = run.meta.endSegment + 1;
        }
        const manifestRow = this.db.getManifestRow(stream);
        const retiredGen = manifestRow.generation + 1;
        const nowMs = this.db.nowMs();
        this.db.retireIndexRuns(
          runs.map((r) => r.run_id),
          retiredGen,
          nowMs
        );
        this.onMetadataChanged?.(stream);
        if (this.metrics) {
          this.metrics.record("tieredstore.index.compact.latency", Number(elapsedNs), "ns", { level: String(run.meta.level) }, stream);
          this.metrics.record("tieredstore.index.runs.compacted", 1, "count", { level: String(run.meta.level) }, stream);
          this.recordActiveRuns(stream);
        }
        for (const r of runs) {
          this.runCache.remove(r.object_key);
          this.runDiskCache?.remove(r.object_key);
        }
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore manifest publish errors; will be retried
          }
        }
        await this.gcRetiredRuns(stream);
        this.compactionQueue.add(stream);
        action.succeed({
          outputCount: 1,
          outputSizeBytes: BigInt(sizeBytes),
          detail: {
            output_record_count: run.meta.recordCount,
            output_filter_len: run.meta.filterLen,
            ...buildTelemetryDetail,
          },
        });
        return Result.ok(undefined);
      });
    } finally {
      this.compacting.delete(stream);
    }
  }

  private findCompactionGroup(stream: string): { level: number; runs: IndexRunRow[] } | null {
    const runs = this.db.listIndexRuns(stream);
    if (runs.length < this.compactionFanout) return null;
    const byLevel = new Map<number, IndexRunRow[]>();
    for (const r of runs) {
      const arr = byLevel.get(r.level) ?? [];
      arr.push(r);
      byLevel.set(r.level, arr);
    }
    for (let level = 0; level <= this.maxLevel; level++) {
      const levelRuns = byLevel.get(level);
      if (!levelRuns || levelRuns.length < this.compactionFanout) continue;
      const span = this.levelSpan(level);
      for (let i = 0; i + this.compactionFanout <= levelRuns.length; i++) {
        const base = levelRuns[i].start_segment;
        let ok = true;
        for (let j = 0; j < this.compactionFanout; j++) {
          const r = levelRuns[i + j];
          const expectStart = base + j * span;
          if (r.level !== level || r.start_segment !== expectStart || r.end_segment !== expectStart + span - 1) {
            ok = false;
            break;
          }
        }
        if (ok) return { level, runs: levelRuns.slice(i, i + this.compactionFanout) };
      }
    }
    return null;
  }

  private levelSpan(level: number): number {
    let span = this.span;
    for (let i = 0; i < level; i++) span *= this.compactionFanout;
    return span;
  }

  private async buildCompactedRunResult(
    stream: string,
    level: number,
    inputs: IndexRunRow[]
  ): Promise<Result<{ run: IndexRun; jobTelemetry: import("./index_build_telemetry").IndexBuildJobTelemetry | null }, IndexBuildError>> {
    const sourcesRes = await this.prepareCompactionSourcesResult(inputs);
    if (Result.isError(sourcesRes)) return sourcesRes;
    const buildRes = await this.buildWorkers.buildResult({
      kind: "routing_compaction_build",
      input: {
        stream,
        level,
        inputs: sourcesRes.value,
      },
    });
    if (Result.isError(buildRes)) return invalidIndexBuild(buildRes.error.message);
    if (buildRes.value.output.kind !== "routing_compaction_build") return invalidIndexBuild("unexpected worker result kind");
    const runRes = decodeIndexRunResult(buildRes.value.output.output.payload);
    if (Result.isError(runRes)) return invalidIndexBuild(runRes.error.message);
    const run = runRes.value;
    run.meta.runId = buildRes.value.output.output.meta.runId;
    run.meta.level = buildRes.value.output.output.meta.level;
    run.meta.startSegment = buildRes.value.output.output.meta.startSegment;
    run.meta.endSegment = buildRes.value.output.output.meta.endSegment;
    run.meta.objectKey = buildRes.value.output.output.meta.objectKey;
    run.meta.filterLen = buildRes.value.output.output.meta.filterLen;
    run.meta.recordCount = buildRes.value.output.output.meta.recordCount;
    return Result.ok({ run, jobTelemetry: buildRes.value.telemetry });
  }

  private async prepareCompactionSourcesResult(
    inputs: IndexRunRow[]
  ): Promise<Result<RoutingCompactionRunSource[], IndexBuildError>> {
    const sources: RoutingCompactionRunSource[] = [];
    for (const meta of inputs) {
      const sourceRes = await this.prepareCompactionSourceResult(meta);
      if (Result.isError(sourceRes)) return sourceRes;
      sources.push(sourceRes.value);
    }
    return Result.ok(sources);
  }

  private async prepareCompactionSourceResult(meta: IndexRunRow): Promise<Result<RoutingCompactionRunSource, IndexBuildError>> {
    if (this.runDiskCache?.has(meta.object_key)) {
      return Result.ok({
        runId: meta.run_id,
        startSegment: meta.start_segment,
        endSegment: meta.end_segment,
        localPath: this.runDiskCache.getPath(meta.object_key),
      });
    }
    try {
      const bytes = await retry(
        async () => {
          const data = await this.os.get(meta.object_key);
          if (!data) throw dsError(`missing index run ${meta.object_key}`);
          return data;
        },
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
      if (this.runDiskCache?.put(meta.object_key, bytes)) {
        return Result.ok({
          runId: meta.run_id,
          startSegment: meta.start_segment,
          endSegment: meta.end_segment,
          localPath: this.runDiskCache.getPath(meta.object_key),
        });
      }
      return Result.ok({
        runId: meta.run_id,
        startSegment: meta.start_segment,
        endSegment: meta.end_segment,
        bytes,
      });
    } catch (e: unknown) {
      return invalidIndexBuild(errorMessage(e));
    }
  }

  private async gcRetiredRuns(stream: string): Promise<void> {
    const retired = this.db.listRetiredIndexRuns(stream);
    if (retired.length === 0) return;
    const manifest = this.db.getManifestRow(stream);
    const nowMs = this.db.nowMs();
    const cutoffGen = this.retireGenWindow > 0 && manifest.generation > this.retireGenWindow ? manifest.generation - this.retireGenWindow : 0;
    const toDelete: IndexRunRow[] = [];
    for (const r of retired) {
      const expiredByGen = r.retired_gen != null && r.retired_gen > 0 && r.retired_gen <= cutoffGen;
      const expiredByTTL = r.retired_at_ms != null && r.retired_at_ms + BigInt(this.retireMinMs) <= nowMs;
      if (expiredByGen || expiredByTTL) toDelete.push(r);
    }
    if (toDelete.length === 0) return;
    for (const r of toDelete) {
      try {
        await this.os.delete(r.object_key);
      } catch {
        // ignore deletion errors
      }
      this.runCache.remove(r.object_key);
      this.runDiskCache?.remove(r.object_key);
    }
    this.db.deleteIndexRuns(toDelete.map((r) => r.run_id));
  }

  private isRoutingConfigured(stream: string): boolean {
    const streamRow = this.db.getStream(stream);
    const contentType = streamRow?.content_type.split(";")[0]?.trim().toLowerCase() ?? null;
    if (contentType != null && contentType !== "application/json") return true;
    if (!this.registry) return false;
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return false;
    return !!regRes.value.routingKey;
  }

  private async persistRunResult(run: IndexRun, stream?: string): Promise<Result<number, IndexBuildError>> {
    const payloadRes = encodeIndexRunResult(run);
    if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
    return await this.persistRunPayloadResult(run.meta, payloadRes.value, stream, run);
  }

  private async persistRunPayloadResult(
    meta: IndexRun["meta"],
    payload: Uint8Array,
    stream?: string,
    decodedRun?: IndexRun
  ): Promise<Result<number, IndexBuildError>> {
    if (this.metrics) {
      this.metrics.record("tieredstore.index.bytes.written", payload.byteLength, "bytes", { level: String(meta.level) }, stream);
    }
    try {
      await retryAbortable(
        (signal) => this.os.put(meta.objectKey, payload, { contentLength: payload.byteLength, signal }),
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
    } catch (e: any) {
      return invalidIndexBuild(String(e?.message ?? e));
    }
    this.runDiskCache?.put(meta.objectKey, payload);
    if (decodedRun) {
      this.runCache.put(meta.objectKey, decodedRun, payload.byteLength);
    }
    return Result.ok(payload.byteLength);
  }

  private async loadRunResult(meta: IndexRunRow): Promise<Result<IndexRun | null, IndexBuildError>> {
    const cached = this.runCache.get(meta.object_key);
    if (cached) return Result.ok(cached);
    let bytes: Uint8Array | null = null;
    if (this.runDiskCache) {
      try {
        bytes = this.runDiskCache.get(meta.object_key);
      } catch {
        this.runDiskCache.remove(meta.object_key);
      }
    }
    if (!bytes) {
      try {
        bytes = await retry(
          async () => {
            const data = await this.os.get(meta.object_key);
            if (!data) throw dsError(`missing index run ${meta.object_key}`);
            return data;
          },
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        );
      } catch (e: unknown) {
        return invalidIndexBuild(errorMessage(e));
      }
      if (this.metrics) {
        this.metrics.record("tieredstore.index.bytes.read", bytes.byteLength, "bytes", { level: String(meta.level) }, meta.stream);
      }
      this.runDiskCache?.put(meta.object_key, bytes);
    }
    const runRes = decodeIndexRunResult(bytes);
    if (Result.isError(runRes)) {
      this.runDiskCache?.remove(meta.object_key);
      return Result.ok(null);
    }
    const run = runRes.value;
    run.meta.runId = meta.run_id;
    run.meta.objectKey = meta.object_key;
    run.meta.level = meta.level;
    run.meta.startSegment = meta.start_segment;
    run.meta.endSegment = meta.end_segment;
    run.meta.filterLen = meta.filter_len;
    run.meta.recordCount = meta.record_count;
    this.runCache.put(meta.object_key, run, meta.size_bytes);
    return Result.ok(run);
  }

  private recordCacheStats(): void {
    if (!this.metrics) return;
    const mem = this.runCache.stats();
    this.metrics.record("tieredstore.index.run_cache.used_bytes", mem.usedBytes, "bytes", { cache: "mem" });
    this.metrics.record("tieredstore.index.run_cache.entries", mem.entries, "count", { cache: "mem" });
    const deltaHits = mem.hits - this.lastRunCacheHits;
    const deltaMisses = mem.misses - this.lastRunCacheMisses;
    const deltaEvict = mem.evictions - this.lastRunCacheEvictions;
    if (deltaHits > 0) this.metrics.record("tieredstore.index.run_cache.hits", deltaHits, "count", { cache: "mem" });
    if (deltaMisses > 0) this.metrics.record("tieredstore.index.run_cache.misses", deltaMisses, "count", { cache: "mem" });
    if (deltaEvict > 0) this.metrics.record("tieredstore.index.run_cache.evictions", deltaEvict, "count", { cache: "mem" });
    this.lastRunCacheHits = mem.hits;
    this.lastRunCacheMisses = mem.misses;
    this.lastRunCacheEvictions = mem.evictions;

    if (this.runDiskCache) {
      const disk = this.runDiskCache.stats();
      this.metrics.record("tieredstore.index.run_cache.used_bytes", disk.usedBytes, "bytes", { cache: "disk" });
      this.metrics.record("tieredstore.index.run_cache.entries", disk.entryCount, "count", { cache: "disk" });
      const dh = disk.hits - this.lastDiskHits;
      const dm = disk.misses - this.lastDiskMisses;
      const de = disk.evictions - this.lastDiskEvictions;
      const db = disk.bytesAdded - this.lastDiskBytesAdded;
      if (dh > 0) this.metrics.record("tieredstore.index.run_cache.hits", dh, "count", { cache: "disk" });
      if (dm > 0) this.metrics.record("tieredstore.index.run_cache.misses", dm, "count", { cache: "disk" });
      if (de > 0) this.metrics.record("tieredstore.index.run_cache.evictions", de, "count", { cache: "disk" });
      if (db > 0) this.metrics.record("tieredstore.index.run_cache.bytes_added", db, "bytes", { cache: "disk" });
      this.lastDiskHits = disk.hits;
      this.lastDiskMisses = disk.misses;
      this.lastDiskEvictions = disk.evictions;
      this.lastDiskBytesAdded = disk.bytesAdded;
    }
  }

  private recordActiveRuns(stream: string): void {
    if (!this.metrics) return;
    const runs = this.db.listIndexRuns(stream);
    this.metrics.record("tieredstore.index.active_runs", runs.length, "count", undefined, stream);
    const byLevel = new Map<number, number>();
    for (const r of runs) byLevel.set(r.level, (byLevel.get(r.level) ?? 0) + 1);
    for (const [level, count] of byLevel.entries()) {
      this.metrics.record("tieredstore.index.active_runs", count, "count", { level: String(level) }, stream);
    }
  }
}

function binarySearch(arr: bigint[], target: bigint): number {
  let lo = 0;
  let hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const v = arr[mid];
    if (v === target) return mid;
    if (v < target) lo = mid + 1;
    else hi = mid - 1;
  }
  return -1;
}
