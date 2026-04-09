import { Result } from "better-result";
import { randomBytes } from "node:crypto";
import type { Config } from "../config";
import type { LexiconIndexRunRow, LexiconIndexStateRow, SegmentRow, SqliteDurableStore } from "../db/db";
import type { Metrics } from "../metrics";
import type { ObjectStore } from "../objectstore/interface";
import type { SchemaRegistryStore } from "../schema/registry";
import { iterateBlockRecordsResult } from "../segment/format";
import { SegmentDiskCache } from "../segment/cache";
import { loadSegmentBytesCached } from "../segment/cached_segment";
import { RestartStringTableView } from "../search/binary/restart_strings";
import { retry, retryAbortable } from "../util/retry";
import { dsError } from "../util/ds_error.ts";
import { streamHash16Hex, lexiconRunObjectKey } from "../util/stream_paths";
import { yieldToEventLoop } from "../util/yield";
import { ConcurrencyGate } from "../concurrency_gate";
import type { ForegroundActivityTracker } from "../foreground_activity";
import { LexiconFileCache } from "./lexicon_file_cache";
import type { IndexSegmentLocalityManager } from "./segment_locality";
import { IndexBuildWorkerPool } from "./index_build_worker_pool";
import type { LexiconCompactionRunSource } from "./lexicon_compaction_build";
import { asyncIndexActionMemoryDetail, beginAsyncIndexAction } from "./async_index_actions";
import { RoutingLexiconL0BuildCoordinator } from "./routing_lexicon_l0_build_coordinator";
import {
  buildLexiconRunPayload,
  decodeLexiconRunResult,
  encodeLexiconRunResult,
  type LexiconRun,
} from "./lexicon_format";

const TEXT_DECODER = new TextDecoder();
const ROUTING_KEY_SOURCE_KIND = "routing_key";
const ROUTING_KEY_SOURCE_NAME = "";

export type RoutingKeyLexiconListResult = {
  keys: string[];
  nextAfter: string | null;
  tookMs: number;
  coverage: {
    complete: boolean;
    indexedSegments: number;
    scannedUploadedSegments: number;
    scannedLocalSegments: number;
    scannedWalRows: number;
    possibleMissingUploadedSegments: number;
    possibleMissingLocalSegments: number;
  };
  timing: {
    lexiconRunGetMs: number;
    lexiconDecodeMs: number;
    lexiconEnumerateMs: number;
    lexiconMergeMs: number;
    fallbackScanMs: number;
    fallbackSegmentGetMs: number;
    fallbackWalScanMs: number;
    lexiconRunsLoaded: number;
  };
};

type LexiconIndexError = {
  kind: "invalid_lexicon_index";
  message: string;
};

function invalidLexiconIndex<T = never>(message: string): Result<T, LexiconIndexError> {
  return Result.err({ kind: "invalid_lexicon_index", message });
}

function errorMessage(error: unknown): string {
  return String((error as any)?.message ?? error);
}

function compareKeys(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function nextLexiconTerm(view: RestartStringTableView, after: string | null): { ordinal: number; term: string | null } {
  let ordinal = after == null ? 0 : view.lowerBoundOrdinal(after);
  while (ordinal < view.count()) {
    const term = view.termAt(ordinal);
    if (term == null) break;
    if (after == null || compareKeys(term, after) > 0) return { ordinal, term };
    ordinal += 1;
  }
  return { ordinal: view.count(), term: null };
}

export class LexiconIndexManager {
  private readonly span: number;
  private readonly compactionFanout: number;
  private readonly maxLevel: number;
  private readonly retireGenWindow: number;
  private readonly retireMinMs: number;
  private readonly fileCache?: LexiconFileCache;
  private readonly foregroundActivity?: ForegroundActivityTracker;
  private readonly buildWorkers: IndexBuildWorkerPool;
  private readonly ownsBuildWorkers: boolean;
  private readonly segmentLocality?: IndexSegmentLocalityManager;
  private readonly routingLexiconBuilds: RoutingLexiconL0BuildCoordinator;
  private readonly buildQueue = new Set<string>();
  private readonly compactionQueue = new Set<string>();
  private readonly building = new Set<string>();
  private readonly compacting = new Set<string>();
  private runningBuildTick = false;
  private runningCompactionTick = false;

  constructor(
    private readonly cfg: Config,
    private readonly db: SqliteDurableStore,
    private readonly os: ObjectStore,
    private readonly segmentCache: SegmentDiskCache | undefined,
    private readonly publishManifest: ((stream: string) => Promise<void>) | undefined,
    private readonly onMetadataChanged: ((stream: string) => void) | undefined,
    private readonly metrics: Metrics | undefined,
    private readonly registry: SchemaRegistryStore | undefined,
    private readonly asyncGate: ConcurrencyGate,
    foregroundActivity?: ForegroundActivityTracker,
    segmentLocality?: IndexSegmentLocalityManager,
    buildWorkers?: IndexBuildWorkerPool,
    routingLexiconBuilds?: RoutingLexiconL0BuildCoordinator
  ) {
    this.span = cfg.indexL0SpanSegments;
    this.compactionFanout = cfg.indexCompactionFanout;
    this.maxLevel = cfg.indexMaxLevel;
    this.retireGenWindow = Math.max(0, cfg.indexRetireGenWindow);
    this.retireMinMs = Math.max(0, cfg.indexRetireMinMs);
    this.foregroundActivity = foregroundActivity;
    this.segmentLocality = segmentLocality;
    this.ownsBuildWorkers = !buildWorkers;
    this.buildWorkers = buildWorkers ?? new IndexBuildWorkerPool(Math.max(1, cfg.indexBuilders));
    if (this.ownsBuildWorkers) this.buildWorkers.start();
    this.routingLexiconBuilds = routingLexiconBuilds ?? new RoutingLexiconL0BuildCoordinator(this.buildWorkers);
    this.fileCache =
      cfg.lexiconIndexCacheMaxBytes > 0
        ? new LexiconFileCache(`${cfg.rootDir}/cache/lexicon`, cfg.lexiconIndexCacheMaxBytes, cfg.lexiconMappedCacheEntries)
        : undefined;
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
    this.fileCache?.clearMapped();
  }

  enqueue(stream: string): void {
    if (this.span <= 0) return;
    this.buildQueue.add(stream);
    this.compactionQueue.add(stream);
  }

  getLocalCacheBytes(stream: string): number {
    return this.fileCache?.bytesForObjectKeyPrefix(`streams/${streamHash16Hex(stream)}/lexicon/`) ?? 0;
  }

  getMemoryStats(): {
    fileCacheBytes: number;
    fileCacheEntries: number;
    mappedFileBytes: number;
    mappedFileEntries: number;
    pinnedFileEntries: number;
  } {
    const stats = this.fileCache?.stats();
    return {
      fileCacheBytes: stats?.usedBytes ?? 0,
      fileCacheEntries: stats?.entryCount ?? 0,
      mappedFileBytes: stats?.mappedBytes ?? 0,
      mappedFileEntries: stats?.mappedEntryCount ?? 0,
      pinnedFileEntries: stats?.pinnedEntryCount ?? 0,
    };
  }

  async listRoutingKeysResult(stream: string, after: string | null, limit: number): Promise<Result<RoutingKeyLexiconListResult, LexiconIndexError>> {
    const safeLimit = Math.max(1, Math.min(limit, 500));
    const startedAt = Date.now();
    const timing = {
      lexiconRunGetMs: 0,
      lexiconDecodeMs: 0,
      lexiconEnumerateMs: 0,
      lexiconMergeMs: 0,
      fallbackScanMs: 0,
      fallbackSegmentGetMs: 0,
      fallbackWalScanMs: 0,
      lexiconRunsLoaded: 0,
    };
    const sourceState = this.db.getLexiconIndexState(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME);
    const uploadedSegmentCount = this.db.countUploadedSegments(stream);
    const indexedThrough = Math.max(0, Math.min(sourceState?.indexed_through ?? 0, uploadedSegmentCount));
    const fallbackScan = await this.scanFallbackKeysResult(stream, indexedThrough, uploadedSegmentCount, after, timing);
    if (Result.isError(fallbackScan)) return fallbackScan;

    const indexedRuns = this.db.listLexiconIndexRuns(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME);
    const indexedPage = await this.listKeysFromRunsResult(indexedRuns, after, safeLimit + 1, timing);
    if (Result.isError(indexedPage)) return indexedPage;

    const mergeStartedAt = Date.now();
    const merged = mergeSortedUnique(indexedPage.value, fallbackScan.value.keys, safeLimit + 1);
    timing.lexiconMergeMs += Date.now() - mergeStartedAt;
    const keys = merged.length > safeLimit ? merged.slice(0, safeLimit) : merged;
    const complete =
      fallbackScan.value.possibleMissingUploadedSegments === 0 && fallbackScan.value.possibleMissingLocalSegments === 0;
    const nextAfter = keys.length === 0 ? null : merged.length > safeLimit || !complete ? keys[keys.length - 1] ?? null : null;
    return Result.ok({
      keys,
      nextAfter,
      tookMs: Date.now() - startedAt,
      coverage: {
        complete,
        indexedSegments: indexedThrough,
        scannedUploadedSegments: fallbackScan.value.scannedUploadedSegments,
        scannedLocalSegments: fallbackScan.value.scannedLocalSegments,
        scannedWalRows: fallbackScan.value.scannedWalRows,
        possibleMissingUploadedSegments: fallbackScan.value.possibleMissingUploadedSegments,
        possibleMissingLocalSegments: fallbackScan.value.possibleMissingLocalSegments,
      },
      timing,
    });
  }

  async tick(): Promise<void> {
    await Promise.all([this.runOneBuildTask(), this.runOneCompactionTask()]);
  }

  async runOneBuildTask(): Promise<boolean> {
    if (this.runningBuildTick) return false;
    const stream = this.takeNextStream(this.buildQueue);
    if (!stream) return false;
    this.runningBuildTick = true;
    try {
      if (!this.isRoutingLexiconConfigured(stream)) {
        const hadState =
          this.db.getLexiconIndexState(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME) != null ||
          this.db.listLexiconIndexRunsAll(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME).length > 0;
        if (hadState) {
          this.db.deleteLexiconIndexSource(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME);
          this.onMetadataChanged?.(stream);
          if (this.publishManifest) {
            try {
              await this.publishManifest(stream);
            } catch {
              // retry on next enqueue
            }
          }
        }
        this.compactionQueue.delete(stream);
        return true;
      }
      const buildRes = await this.maybeBuildRuns(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME);
      if (Result.isError(buildRes)) {
        console.error("lexicon build failed", stream, buildRes.error.message);
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
      if (!this.isRoutingLexiconConfigured(stream)) {
        this.buildQueue.delete(stream);
        return true;
      }
      const compactRes = await this.maybeCompactRuns(stream, ROUTING_KEY_SOURCE_KIND, ROUTING_KEY_SOURCE_NAME);
      if (Result.isError(compactRes)) {
        console.error("lexicon compaction failed", stream, compactRes.error.message);
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

  private async maybeBuildRuns(
    stream: string,
    sourceKind: string,
    sourceName: string
  ): Promise<Result<void, LexiconIndexError>> {
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      return await this.asyncGate.run(async () => {
        let state = this.db.getLexiconIndexState(stream, sourceKind, sourceName);
        if (!state) {
          this.db.upsertLexiconIndexState(stream, sourceKind, sourceName, 0);
          state = this.db.getLexiconIndexState(stream, sourceKind, sourceName);
        }
        if (!state) return Result.ok(undefined);
        let routingState = this.db.getIndexState(stream);
        if (!routingState) {
          const secret = randomBytes(16);
          this.db.upsertIndexState(stream, secret, 0);
          routingState = this.db.getIndexState(stream);
        }
        if (!routingState) {
          this.buildQueue.add(stream);
          return Result.ok(undefined);
        }
        const uploadedCount = this.db.countUploadedSegments(stream);
        if (uploadedCount < state.indexed_through + this.span) return Result.ok(undefined);
        const startSegment = state.indexed_through;
        const endSegment = startSegment + this.span - 1;
        const segments: SegmentRow[] = [];
        for (let segmentIndex = startSegment; segmentIndex <= endSegment; segmentIndex += 1) {
          const segment = this.db.getSegmentByIndex(stream, segmentIndex);
          if (!segment || !segment.r2_etag) return Result.ok(undefined);
          segments.push(segment);
        }
        const segmentLeaseRes = this.segmentLocality
          ? await this.segmentLocality.acquireLexiconWindowResult(stream, segments)
          : Result.err({ kind: "missing_segment" as const, message: "routing-key lexicon indexing requires a segment disk cache" });
        if (Result.isError(segmentLeaseRes)) {
          if (segmentLeaseRes.error.kind === "index_cache_overloaded") {
            this.buildQueue.add(stream);
            this.onMetadataChanged?.(stream);
            return Result.ok(undefined);
          }
          return invalidLexiconIndex(segmentLeaseRes.error.message);
        }
        const segmentLease = segmentLeaseRes.value;
        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "lexicon_l0_build",
          targetKind: sourceKind,
          targetName: sourceName,
          inputKind: "segment",
          inputCount: segments.length,
          inputSizeBytes: segments.reduce((sum, segment) => sum + BigInt(segment.size_bytes), 0n),
          startSegment,
          endSegment,
          detail: {
            input_payload_bytes: segments.reduce((sum, segment) => sum + Number(segment.payload_bytes), 0),
            level: 0,
          },
        });
        let runRes;
        try {
          runRes = await this.routingLexiconBuilds.buildWindowResult({
            stream,
            sourceKind,
            sourceName,
            cacheToken: Buffer.from(routingState.index_secret).toString("hex"),
            startSegment,
            span: this.span,
            secret: routingState.index_secret,
            segments: segmentLease.localSegments.map((segment) => ({
              segmentIndex: segment.segmentIndex,
              localPath: segment.localPath,
            })),
          });
        } finally {
          segmentLease.release();
        }
        if (Result.isError(runRes)) {
          action.fail(runRes.error.message);
          return invalidLexiconIndex(runRes.error.message);
        }
        const buildTelemetryDetail = asyncIndexActionMemoryDetail(runRes.value.jobTelemetry);
        const lexiconRun = runRes.value.output.lexicon;
        const persistRes = await this.persistRunPayloadResult(lexiconRun.meta, lexiconRun.payload, stream);
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              output_record_count: lexiconRun.meta.recordCount,
              shared_build_cache_status: runRes.value.cacheStatus,
              ...buildTelemetryDetail,
            },
          });
          return persistRes;
        }
        this.db.insertLexiconIndexRun({
          run_id: lexiconRun.meta.runId,
          stream,
          source_kind: sourceKind,
          source_name: sourceName,
          level: lexiconRun.meta.level,
          start_segment: lexiconRun.meta.startSegment,
          end_segment: lexiconRun.meta.endSegment,
          object_key: lexiconRun.meta.objectKey,
          size_bytes: persistRes.value,
          record_count: lexiconRun.meta.recordCount,
        });
        this.db.updateLexiconIndexedThrough(stream, sourceKind, sourceName, endSegment + 1);
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // retry on next publish
          }
        }
        if (this.db.countUploadedSegments(stream) >= endSegment + 1 + this.span) {
          this.buildQueue.add(stream);
        }
        this.compactionQueue.add(stream);
        action.succeed({
          outputCount: 1,
          outputSizeBytes: BigInt(persistRes.value),
          detail: {
            output_record_count: lexiconRun.meta.recordCount,
            shared_build_cache_status: runRes.value.cacheStatus,
            ...buildTelemetryDetail,
          },
        });
        return Result.ok(undefined);
      });
    } catch (error) {
      return invalidLexiconIndex(errorMessage(error));
    } finally {
      this.building.delete(stream);
    }
  }

  private async maybeCompactRuns(
    stream: string,
    sourceKind: string,
    sourceName: string
  ): Promise<Result<void, LexiconIndexError>> {
    if (this.compactionFanout <= 1) return Result.ok(undefined);
    if (this.compacting.has(stream)) return Result.ok(undefined);
    this.compacting.add(stream);
    try {
      return await this.asyncGate.run(async () => {
        const group = this.findCompactionGroup(stream, sourceKind, sourceName);
        if (!group) {
          await this.gcRetiredRuns(stream, sourceKind, sourceName);
          return Result.ok(undefined);
        }
        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "lexicon_compaction_build",
          targetKind: sourceKind,
          targetName: sourceName,
          inputKind: "run",
          inputCount: group.runs.length,
          inputSizeBytes: group.runs.reduce((sum, run) => sum + BigInt(run.size_bytes), 0n),
          startSegment: group.runs[0]?.start_segment ?? null,
          endSegment: group.runs[group.runs.length - 1]?.end_segment ?? null,
          detail: {
            level_from: group.level,
            level_to: group.level + 1,
          },
        });
        const runRes = await this.buildCompactedRunResult(stream, sourceKind, sourceName, group.level + 1, group.runs);
        if (Result.isError(runRes)) {
          action.fail(runRes.error.message);
          return runRes;
        }
        const buildTelemetryDetail = asyncIndexActionMemoryDetail(runRes.value.jobTelemetry);
        const persistRes = await this.persistRunResult(runRes.value.run, stream);
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              output_record_count: runRes.value.run.meta.recordCount,
              ...buildTelemetryDetail,
            },
          });
          return persistRes;
        }
        this.db.insertLexiconIndexRun({
          run_id: runRes.value.run.meta.runId,
          stream,
          source_kind: sourceKind,
          source_name: sourceName,
          level: runRes.value.run.meta.level,
          start_segment: runRes.value.run.meta.startSegment,
          end_segment: runRes.value.run.meta.endSegment,
          object_key: runRes.value.run.meta.objectKey,
          size_bytes: persistRes.value,
          record_count: runRes.value.run.meta.recordCount,
        });
        const state = this.db.getLexiconIndexState(stream, sourceKind, sourceName);
        if (state && runRes.value.run.meta.endSegment + 1 > state.indexed_through) {
          this.db.updateLexiconIndexedThrough(stream, sourceKind, sourceName, runRes.value.run.meta.endSegment + 1);
        }
        const manifestRow = this.db.getManifestRow(stream);
        this.db.retireLexiconIndexRuns(group.runs.map((run) => run.run_id), manifestRow.generation + 1, this.db.nowMs());
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // retry on next publish
          }
        }
        await this.gcRetiredRuns(stream, sourceKind, sourceName);
        this.compactionQueue.add(stream);
        action.succeed({
          outputCount: 1,
          outputSizeBytes: BigInt(persistRes.value),
          detail: {
            output_record_count: runRes.value.run.meta.recordCount,
            ...buildTelemetryDetail,
          },
        });
        return Result.ok(undefined);
      });
    } catch (error) {
      return invalidLexiconIndex(errorMessage(error));
    } finally {
      this.compacting.delete(stream);
    }
  }

  private findCompactionGroup(stream: string, sourceKind: string, sourceName: string): { level: number; runs: LexiconIndexRunRow[] } | null {
    const runs = this.db.listLexiconIndexRuns(stream, sourceKind, sourceName);
    if (runs.length < this.compactionFanout) return null;
    const byLevel = new Map<number, LexiconIndexRunRow[]>();
    for (const run of runs) {
      const entries = byLevel.get(run.level) ?? [];
      entries.push(run);
      byLevel.set(run.level, entries);
    }
    for (let level = 0; level <= this.maxLevel; level += 1) {
      const levelRuns = byLevel.get(level);
      if (!levelRuns || levelRuns.length < this.compactionFanout) continue;
      const span = this.levelSpan(level);
      for (let offset = 0; offset + this.compactionFanout <= levelRuns.length; offset += 1) {
        const baseStart = levelRuns[offset]!.start_segment;
        let matches = true;
        for (let i = 0; i < this.compactionFanout; i += 1) {
          const run = levelRuns[offset + i]!;
          const expectedStart = baseStart + i * span;
          if (run.level !== level || run.start_segment !== expectedStart || run.end_segment !== expectedStart + span - 1) {
            matches = false;
            break;
          }
        }
        if (matches) return { level, runs: levelRuns.slice(offset, offset + this.compactionFanout) };
      }
    }
    return null;
  }

  private levelSpan(level: number): number {
    let span = this.span;
    for (let i = 0; i < level; i += 1) span *= this.compactionFanout;
    return span;
  }

  private async buildCompactedRunResult(
    stream: string,
    sourceKind: string,
    sourceName: string,
    level: number,
    runs: LexiconIndexRunRow[]
  ): Promise<Result<{ run: LexiconRun; jobTelemetry: import("./index_build_telemetry").IndexBuildJobTelemetry | null }, LexiconIndexError>> {
    const sourcesRes = await this.prepareCompactionSourcesResult(runs);
    if (Result.isError(sourcesRes)) return sourcesRes;
    const buildRes = await this.buildWorkers.buildResult({
      kind: "lexicon_compaction_build",
      input: {
        stream,
        sourceKind,
        sourceName,
        level,
        inputs: sourcesRes.value,
      },
    });
    if (Result.isError(buildRes)) return invalidLexiconIndex(buildRes.error.message);
    if (buildRes.value.output.kind !== "lexicon_compaction_build") return invalidLexiconIndex("unexpected worker result kind");
    const runRes = decodeLexiconRunResult(buildRes.value.output.output.payload);
    if (Result.isError(runRes)) return invalidLexiconIndex(runRes.error.message);
    const run = runRes.value;
    run.meta.runId = buildRes.value.output.output.meta.runId;
    run.meta.level = buildRes.value.output.output.meta.level;
    run.meta.startSegment = buildRes.value.output.output.meta.startSegment;
    run.meta.endSegment = buildRes.value.output.output.meta.endSegment;
    run.meta.objectKey = buildRes.value.output.output.meta.objectKey;
    run.meta.recordCount = buildRes.value.output.output.meta.recordCount;
    return Result.ok({ run, jobTelemetry: buildRes.value.telemetry });
  }

  private async prepareCompactionSourcesResult(
    runs: LexiconIndexRunRow[]
  ): Promise<Result<LexiconCompactionRunSource[], LexiconIndexError>> {
    const sources: LexiconCompactionRunSource[] = [];
    for (const meta of runs) {
      const sourceRes = await this.prepareCompactionSourceResult(meta);
      if (Result.isError(sourceRes)) return sourceRes;
      sources.push(sourceRes.value);
    }
    return Result.ok(sources);
  }

  private async prepareCompactionSourceResult(meta: LexiconIndexRunRow): Promise<Result<LexiconCompactionRunSource, LexiconIndexError>> {
    if (this.fileCache) {
      const localPathRes = await this.fileCache.ensureLocalFileResult({
        objectKey: meta.object_key,
        expectedSize: meta.size_bytes,
        loadBytes: () =>
          retry(
            async () => {
              const data = await this.os.get(meta.object_key);
              if (!data) throw dsError(`missing lexicon run ${meta.object_key}`);
              return data;
            },
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          ),
      });
      if (Result.isError(localPathRes)) return invalidLexiconIndex(localPathRes.error.message);
      return Result.ok({
        runId: meta.run_id,
        startSegment: meta.start_segment,
        endSegment: meta.end_segment,
        localPath: localPathRes.value,
      });
    }
    try {
      const bytes = await retry(
        async () => {
          const data = await this.os.get(meta.object_key);
          if (!data) throw dsError(`missing lexicon run ${meta.object_key}`);
          return data;
        },
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
      return Result.ok({
        runId: meta.run_id,
        startSegment: meta.start_segment,
        endSegment: meta.end_segment,
        bytes,
      });
    } catch (error) {
      return invalidLexiconIndex(errorMessage(error));
    }
  }

  private async persistRunResult(run: LexiconRun, stream: string): Promise<Result<number, LexiconIndexError>> {
    const payloadRes = encodeLexiconRunResult(run);
    if (Result.isError(payloadRes)) return invalidLexiconIndex(payloadRes.error.message);
    return await this.persistRunPayloadResult(run.meta, payloadRes.value, stream);
  }

  private async persistRunPayloadResult(
    meta: LexiconRun["meta"],
    payload: Uint8Array,
    stream: string
  ): Promise<Result<number, LexiconIndexError>> {
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
      this.fileCache?.storeBytesResult(meta.objectKey, payload);
      this.metrics?.record("tieredstore.lexicon.bytes.written", payload.byteLength, "bytes", { source: ROUTING_KEY_SOURCE_KIND }, stream);
      return Result.ok(payload.byteLength);
    } catch (error) {
      return invalidLexiconIndex(errorMessage(error));
    }
  }

  private async listKeysFromRunsResult(
    runs: LexiconIndexRunRow[],
    after: string | null,
    limit: number,
    timing: RoutingKeyLexiconListResult["timing"]
  ): Promise<Result<string[], LexiconIndexError>> {
    const enumerateStartedAt = Date.now();
    const cursors: Array<{ run: LexiconRun; ordinal: number; current: string | null }> = [];
    for (const meta of runs) {
      const runRes = await this.loadRunResult(meta, timing);
      if (Result.isError(runRes)) return runRes;
      if (!runRes.value) continue;
      const next = nextLexiconTerm(runRes.value.terms, after);
      cursors.push({ run: runRes.value, ordinal: next.ordinal, current: next.term });
    }
    const results: string[] = [];
    let lastValue: string | null = null;
    let emittedSinceYield = 0;
    while (results.length < limit) {
      let smallest: string | null = null;
      for (const cursor of cursors) {
        if (cursor.current == null) continue;
        if (smallest == null || compareKeys(cursor.current, smallest) < 0) smallest = cursor.current;
      }
      if (smallest == null) break;
      if (smallest !== lastValue) {
        results.push(smallest);
        lastValue = smallest;
      }
      for (const cursor of cursors) {
        while (cursor.current != null && cursor.current === smallest) {
          cursor.ordinal += 1;
          cursor.current = cursor.ordinal < cursor.run.terms.count() ? cursor.run.terms.termAt(cursor.ordinal) : null;
        }
      }
      emittedSinceYield += 1;
      if (emittedSinceYield >= 256) {
        emittedSinceYield = 0;
        await this.yieldBackgroundWork();
      }
    }
    timing.lexiconEnumerateMs += Date.now() - enumerateStartedAt;
    return Result.ok(results);
  }

  private async loadRunResult(
    meta: LexiconIndexRunRow,
    timing: RoutingKeyLexiconListResult["timing"]
  ): Promise<Result<LexiconRun | null, LexiconIndexError>> {
    try {
      let bytes: Uint8Array;
      const runGetStartedAt = Date.now();
      if (this.fileCache) {
        const mappedRes = await this.fileCache.loadMappedFileResult({
          objectKey: meta.object_key,
          expectedSize: meta.size_bytes,
          loadBytes: () =>
            retry(
              async () => {
                const data = await this.os.get(meta.object_key);
                if (!data) throw dsError(`missing lexicon run ${meta.object_key}`);
                return data;
              },
              {
                retries: this.cfg.objectStoreRetries,
                baseDelayMs: this.cfg.objectStoreBaseDelayMs,
                maxDelayMs: this.cfg.objectStoreMaxDelayMs,
                timeoutMs: this.cfg.objectStoreTimeoutMs,
              }
            ),
        });
        if (Result.isError(mappedRes)) return invalidLexiconIndex(mappedRes.error.message);
        bytes = mappedRes.value.bytes;
      } else {
        bytes = await retry(
          async () => {
            const data = await this.os.get(meta.object_key);
            if (!data) throw dsError(`missing lexicon run ${meta.object_key}`);
            return data;
          },
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        );
      }
      timing.lexiconRunGetMs += Date.now() - runGetStartedAt;
      const decodeStartedAt = Date.now();
      const runRes = decodeLexiconRunResult(bytes);
      if (Result.isError(runRes)) return invalidLexiconIndex(runRes.error.message);
      timing.lexiconDecodeMs += Date.now() - decodeStartedAt;
      timing.lexiconRunsLoaded += 1;
      const run = runRes.value;
      run.meta.runId = meta.run_id;
      run.meta.level = meta.level;
      run.meta.startSegment = meta.start_segment;
      run.meta.endSegment = meta.end_segment;
      run.meta.objectKey = meta.object_key;
      run.meta.recordCount = meta.record_count;
      this.metrics?.record("tieredstore.lexicon.bytes.read", bytes.byteLength, "bytes", { source: ROUTING_KEY_SOURCE_KIND }, meta.stream);
      return Result.ok(run);
    } catch (error) {
      return invalidLexiconIndex(errorMessage(error));
    }
  }

  private async scanFallbackKeysResult(
    stream: string,
    indexedThrough: number,
    uploadedSegmentCount: number,
    after: string | null,
    timing: RoutingKeyLexiconListResult["timing"]
  ): Promise<
    Result<
      {
        keys: string[];
        scannedUploadedSegments: number;
        scannedLocalSegments: number;
        scannedWalRows: number;
        possibleMissingUploadedSegments: number;
        possibleMissingLocalSegments: number;
      },
      LexiconIndexError
    >
  > {
    const startedAt = Date.now();
    const streamRow = this.db.getStream(stream);
    if (!streamRow) return invalidLexiconIndex(`missing stream ${stream}`);
    const segmentCount = this.db.countSegmentsForStream(stream);
    const fallbackKeys = new Set<string>();
    let scannedUploadedSegments = 0;
    let scannedLocalSegments = 0;
    const shouldScanUploadedSegments = indexedThrough === 0;
    const segmentScanLimit = 1;
    let scannedSegments = 0;
    const fallbackStartSegment = shouldScanUploadedSegments ? indexedThrough : uploadedSegmentCount;
    for (let segmentIndex = fallbackStartSegment; segmentIndex < segmentCount; segmentIndex += 1) {
      if (scannedSegments >= segmentScanLimit) break;
      const segment = this.db.getSegmentByIndex(stream, segmentIndex);
      if (!segment) continue;
      const segmentGetStartedAt = Date.now();
      const bytesRes = await this.loadSegmentBytesResult(segment);
      if (Result.isError(bytesRes)) return bytesRes;
      timing.fallbackSegmentGetMs += Date.now() - segmentGetStartedAt;
      for (const recordRes of iterateBlockRecordsResult(bytesRes.value)) {
        if (Result.isError(recordRes)) return invalidLexiconIndex(recordRes.error.message);
        if (recordRes.value.routingKey.byteLength === 0) continue;
        const key = TEXT_DECODER.decode(recordRes.value.routingKey);
        if (after != null && compareKeys(key, after) <= 0) continue;
        fallbackKeys.add(key);
      }
      if (segmentIndex < uploadedSegmentCount) scannedUploadedSegments += 1;
      else scannedLocalSegments += 1;
      scannedSegments += 1;
      await this.yieldBackgroundWork();
    }

    let scannedWalRows = 0;
    const walStart = streamRow.sealed_through + 1n;
    const walEnd = streamRow.next_offset - 1n;
    if (walStart <= walEnd) {
      const walStartedAt = Date.now();
      for (const row of this.db.iterWalRange(stream, walStart, walEnd)) {
        scannedWalRows += 1;
        const routingKey = row.routing_key == null ? null : row.routing_key instanceof Uint8Array ? row.routing_key : new Uint8Array(row.routing_key);
        if (!routingKey || routingKey.byteLength === 0) continue;
        const key = TEXT_DECODER.decode(routingKey);
        if (after != null && compareKeys(key, after) <= 0) continue;
        fallbackKeys.add(key);
      }
      timing.fallbackWalScanMs += Date.now() - walStartedAt;
    }

    const totalUncoveredUploadedSegments = Math.max(0, uploadedSegmentCount - indexedThrough);
    const totalUncoveredLocalSegments = Math.max(0, segmentCount - uploadedSegmentCount);
    timing.fallbackScanMs += Date.now() - startedAt;

    return Result.ok({
      keys: Array.from(fallbackKeys).sort(compareKeys),
      scannedUploadedSegments,
      scannedLocalSegments,
      scannedWalRows,
      possibleMissingUploadedSegments: Math.max(0, totalUncoveredUploadedSegments - scannedUploadedSegments),
      possibleMissingLocalSegments: Math.max(0, totalUncoveredLocalSegments - scannedLocalSegments),
    });
  }

  private async gcRetiredRuns(stream: string, sourceKind: string, sourceName: string): Promise<void> {
    const retiredRuns = this.db.listRetiredLexiconIndexRuns(stream, sourceKind, sourceName);
    if (retiredRuns.length === 0) return;
    const manifest = this.db.getManifestRow(stream);
    const nowMs = this.db.nowMs();
    const cutoffGen =
      this.retireGenWindow > 0 && manifest.generation > this.retireGenWindow ? manifest.generation - this.retireGenWindow : 0;
    const deletions = retiredRuns.filter((run) => {
      const expiredByGen = run.retired_gen != null && run.retired_gen > 0 && run.retired_gen <= cutoffGen;
      const expiredByTtl = run.retired_at_ms != null && run.retired_at_ms + BigInt(this.retireMinMs) <= nowMs;
      return expiredByGen || expiredByTtl;
    });
    if (deletions.length === 0) return;
    for (const run of deletions) {
      try {
        await this.os.delete(run.object_key);
      } catch {
        // best effort
      }
    }
    this.db.deleteLexiconIndexRuns(deletions.map((run) => run.run_id));
  }

  private isRoutingLexiconConfigured(stream: string): boolean {
    if (!this.registry) return false;
    const registryRes = this.registry.getRegistryResult(stream);
    if (Result.isError(registryRes)) return false;
    return registryRes.value.routingKey != null;
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, LexiconIndexError>> {
    try {
      const bytes = await loadSegmentBytesCached(
        this.os,
        seg,
        this.segmentCache,
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
      return Result.ok(bytes);
    } catch (error) {
      return invalidLexiconIndex(errorMessage(error));
    }
  }
}

function mergeSortedUnique(left: string[], right: string[], limit: number): string[] {
  const merged: string[] = [];
  let li = 0;
  let ri = 0;
  let last: string | null = null;
  while (merged.length < limit && (li < left.length || ri < right.length)) {
    let next: string;
    if (li >= left.length) {
      next = right[ri++]!;
    } else if (ri >= right.length) {
      next = left[li++]!;
    } else {
      const cmp = compareKeys(left[li]!, right[ri]!);
      if (cmp <= 0) {
        next = left[li++]!;
        if (cmp === 0) ri += 1;
      } else {
        next = right[ri++]!;
      }
    }
    if (next === last) continue;
    merged.push(next);
    last = next;
  }
  return merged;
}
