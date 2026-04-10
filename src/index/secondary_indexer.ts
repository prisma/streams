import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, unlinkSync } from "node:fs";
import { Result } from "better-result";
import type { Config } from "../config";
import type { SecondaryIndexRunRow, SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SchemaRegistryStore, type SchemaRegistry } from "../schema/registry";
import { SegmentDiskCache } from "../segment/cache";
import { loadSegmentBytesCached } from "../segment/cached_segment";
import { retry, retryAbortable } from "../util/retry";
import { dsError } from "../util/ds_error.ts";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { siphash24 } from "../util/siphash";
import { yieldToEventLoop } from "../util/yield";
import { RuntimeMemorySampler } from "../runtime_memory_sampler";
import { ConcurrencyGate } from "../concurrency_gate";
import type { ForegroundActivityTracker } from "../foreground_activity";
import { binaryFuseContains, buildBinaryFuseResult } from "./binary_fuse";
import { IndexRunCache } from "./run_cache";
import {
  decodeIndexRunResult,
  encodeIndexRunResult,
  RUN_TYPE_MASK16,
  RUN_TYPE_POSTINGS,
  RUN_TYPE_SINGLE_SEGMENT,
  type IndexRun,
} from "./run_format";
import {
  extractSecondaryIndexValuesResult,
  getConfiguredSecondaryIndexes,
  hashSecondaryIndexField,
  type SecondaryIndexField,
} from "./secondary_schema";
import type { IndexSegmentLocalityManager } from "./segment_locality";
import { IndexBuildWorkerPool } from "./index_build_worker_pool";
import type { SecondaryCompactionBuildOutput, SecondaryCompactionRunSource } from "./secondary_compaction_build";
import { asyncIndexActionMemoryDetail, beginAsyncIndexAction } from "./async_index_actions";
import { uploadRunPayloadsBoundedResult } from "./run_payload_upload";
import { writeIndexRunOutputFileResult } from "./index_run_output_file";
import { SearchSegmentBuildCoordinator } from "../search/search_segment_build_coordinator";
import { collectUnifiedSearchBuildExactIndexes, type SearchSegmentBuildOutput } from "../search/search_segment_build";
import type { CompanionSectionKind } from "../search/companion_format";
import type { IndexTailMatcher } from "./indexer";

type SecondaryIndexBuildError = { kind: "invalid_index_build"; message: string };
const SECONDARY_L0_BATCH_MAX_FIELDS = 1;
const SECONDARY_BUILD_DISPATCH_CONCURRENCY_EVLOG = 4;
const SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_HIGH_CARDINALITY = 2;
const SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_LOW_CARDINALITY = 16;
const SECONDARY_COMPACTION_SOURCE_INLINE_FETCH_MAX_BYTES = 256 * 1024;
const SECONDARY_RETIRED_GC_DELETE_CONCURRENCY = 4;
const SECONDARY_COMPACTION_FANOUT_HIGH_CARDINALITY = 2;
const SECONDARY_COMPACTION_MAX_LEVEL_HIGH_CARDINALITY = 0;
const EVLOG_SECONDARY_COMPACTION_DEFER_LAG_SEGMENTS = 256;
const EVLOG_HIGH_CARDINALITY_EXACT_FIELDS = new Set(["requestId", "traceId", "spanId", "path"]);
const EVLOG_HIGH_CARDINALITY_EXACT_SPAN_LIMIT = 2;
const EVLOG_EXACT_BATCH_SPAN_LIMITS = new Map<string, number>([
  ["timestamp", 2],
  ["duration,method,status", 2],
  ["environment,level,service", 4],
]);
const EVLOG_EXACT_BATCH_GROUPS = [
  ["timestamp"],
  ["level", "service", "environment"],
  ["method", "status", "duration"],
  ["requestId"],
  ["traceId"],
  ["spanId"],
  ["path"],
] as const;
type ExactBuiltRun = SearchSegmentBuildOutput["exactRuns"][number];
type UploadedExactRun = {
  run: ExactBuiltRun;
  sizeBytes: number;
};

export function secondaryExactBatchSpan(
  profile: string | null | undefined,
  configuredSpan: number,
  indexNames: readonly string[]
): number {
  const effectiveConfiguredSpan = Math.max(0, configuredSpan);
  if (effectiveConfiguredSpan <= 0) return 0;
  if ((profile ?? null) !== "evlog") return effectiveConfiguredSpan;
  if (indexNames.length === 1 && EVLOG_HIGH_CARDINALITY_EXACT_FIELDS.has(indexNames[0]!)) {
    return Math.min(effectiveConfiguredSpan, EVLOG_HIGH_CARDINALITY_EXACT_SPAN_LIMIT);
  }
  for (const indexName of indexNames) {
    if (EVLOG_HIGH_CARDINALITY_EXACT_FIELDS.has(indexName)) return 1;
  }
  const batchKey = Array.from(indexNames).sort((left, right) => left.localeCompare(right)).join(",");
  const spanLimit = EVLOG_EXACT_BATCH_SPAN_LIMITS.get(batchKey);
  return Math.min(effectiveConfiguredSpan, spanLimit ?? effectiveConfiguredSpan);
}

function invalidIndexBuild<T = never>(message: string): Result<T, SecondaryIndexBuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

export function secondaryCompactionSourceFetchConcurrency(indexName: string, inputCount: number): number {
  const cap = EVLOG_HIGH_CARDINALITY_EXACT_FIELDS.has(indexName)
    ? SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_HIGH_CARDINALITY
    : SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_LOW_CARDINALITY;
  return Math.max(1, Math.min(cap, inputCount || 1));
}

export function secondaryCompactionFanout(indexName: string, configuredFanout: number): number {
  if (configuredFanout <= 1) return configuredFanout;
  if (!EVLOG_HIGH_CARDINALITY_EXACT_FIELDS.has(indexName)) return configuredFanout;
  return Math.max(2, Math.min(configuredFanout, SECONDARY_COMPACTION_FANOUT_HIGH_CARDINALITY));
}

export function secondaryCompactionMaxLevel(indexName: string, configuredMaxLevel: number): number {
  if (configuredMaxLevel <= 0) return configuredMaxLevel;
  if (!EVLOG_HIGH_CARDINALITY_EXACT_FIELDS.has(indexName)) return configuredMaxLevel;
  return Math.min(configuredMaxLevel, SECONDARY_COMPACTION_MAX_LEVEL_HIGH_CARDINALITY);
}

export function secondaryBuildDispatchConcurrency(profile: string | null | undefined, batchCount: number): number {
  if ((profile ?? null) !== "evlog") return 1;
  return Math.max(1, Math.min(SECONDARY_BUILD_DISPATCH_CONCURRENCY_EVLOG, batchCount));
}

export function shouldDeferSecondaryCompaction(
  profile: string | null | undefined,
  uploadedCount: number,
  indexedThrough: number
): boolean {
  if ((profile ?? null) !== "evlog") return false;
  return Math.max(0, uploadedCount - indexedThrough) >= EVLOG_SECONDARY_COMPACTION_DEFER_LAG_SEGMENTS;
}

function binarySearch(values: bigint[], needle: bigint): number {
  let lo = 0;
  let hi = values.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const cur = values[mid];
    if (cur === needle) return mid;
    if (cur < needle) lo = mid + 1;
    else hi = mid - 1;
  }
  return -1;
}

export class SecondaryIndexManager {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly registry: SchemaRegistryStore;
  private readonly segmentCache?: SegmentDiskCache;
  private readonly runDiskCache?: SegmentDiskCache;
  private readonly runCache: IndexRunCache;
  private readonly configuredSpan: number;
  private readonly compactionFanout: number;
  private readonly maxLevel: number;
  private readonly retireGenWindow: number;
  private readonly retireMinMs: number;
  private readonly buildQueue = new Set<string>();
  private readonly compactionQueue = new Set<string>();
  private readonly deferredManifestPublishes = new Set<string>();
  private readonly building = new Set<string>();
  private readonly compacting = new Set<string>();
  private runningBuildTick = false;
  private runningCompactionTick = false;
  private readonly publishManifest?: (stream: string) => Promise<void>;
  private readonly onMetadataChanged?: (stream: string) => void;
  private readonly memorySampler?: RuntimeMemorySampler;
  private readonly asyncGate: ConcurrencyGate;
  private readonly foregroundActivity?: ForegroundActivityTracker;
  private readonly segmentLocality?: IndexSegmentLocalityManager;
  private readonly buildWorkers: IndexBuildWorkerPool;
  private readonly ownsBuildWorkers: boolean;
  private readonly searchSegmentBuilds?: SearchSegmentBuildCoordinator;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    registry: SchemaRegistryStore,
    segmentCache?: SegmentDiskCache,
    publishManifest?: (stream: string) => Promise<void>,
    onMetadataChanged?: (stream: string) => void,
    memorySampler?: RuntimeMemorySampler,
    asyncGate?: ConcurrencyGate,
    foregroundActivity?: ForegroundActivityTracker,
    segmentLocality?: IndexSegmentLocalityManager,
    buildWorkers?: IndexBuildWorkerPool,
    searchSegmentBuilds?: SearchSegmentBuildCoordinator
  ) {
    this.cfg = cfg;
    this.db = db;
    this.os = os;
    this.registry = registry;
    this.segmentCache = segmentCache;
    this.publishManifest = publishManifest;
    this.onMetadataChanged = onMetadataChanged;
    this.memorySampler = memorySampler;
    this.asyncGate = asyncGate ?? new ConcurrencyGate(1);
    this.foregroundActivity = foregroundActivity;
    this.segmentLocality = segmentLocality;
    this.ownsBuildWorkers = !buildWorkers;
    this.buildWorkers = buildWorkers ?? new IndexBuildWorkerPool(Math.max(1, cfg.indexBuilders));
    if (this.ownsBuildWorkers) this.buildWorkers.start();
    this.searchSegmentBuilds = searchSegmentBuilds;
    this.configuredSpan = Math.max(0, cfg.indexL0SpanSegments);
    this.compactionFanout = cfg.indexCompactionFanout;
    this.maxLevel = cfg.indexMaxLevel;
    this.retireGenWindow = Math.max(0, cfg.indexRetireGenWindow);
    this.retireMinMs = Math.max(0, cfg.indexRetireMinMs);
    this.runCache = new IndexRunCache(cfg.indexRunMemoryCacheBytes);
    this.runDiskCache =
      cfg.indexRunCacheMaxBytes > 0
        ? new SegmentDiskCache(`${cfg.rootDir}/cache/secondary-index`, cfg.indexRunCacheMaxBytes)
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
  }

  private requestDeferredManifestPublish(stream: string): void {
    this.deferredManifestPublishes.add(stream);
  }

  private async flushDeferredManifestPublish(stream: string): Promise<void> {
    if (!this.deferredManifestPublishes.has(stream)) return;
    this.deferredManifestPublishes.delete(stream);
    if (!this.publishManifest) return;
    try {
      await this.publishManifest(stream);
    } catch (error) {
      this.deferredManifestPublishes.add(stream);
      throw error;
    }
  }

  enqueue(stream: string): void {
    if (this.configuredSpan <= 0) return;
    this.buildQueue.add(stream);
    this.compactionQueue.add(stream);
  }

  async candidateSegmentsForSecondaryIndex(
    stream: string,
    indexName: string,
    keyBytes: Uint8Array
  ): Promise<{ segments: Set<number>; indexedThrough: number } | null> {
    if (this.configuredSpan <= 0) return null;
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return null;
    const configured = getConfiguredSecondaryIndexes(regRes.value).find((entry) => entry.name === indexName);
    if (!configured) return null;
    const state = this.db.getSecondaryIndexState(stream, indexName);
    if (!state) return null;
    if (state.config_hash !== hashSecondaryIndexField(configured)) return null;
    const runs = this.db.listSecondaryIndexRuns(stream, indexName);
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
      } else if (run.runType === RUN_TYPE_SINGLE_SEGMENT) {
        if (binarySearch(run.fingerprints, fp) >= 0) segments.add(run.meta.startSegment);
      } else if (run.postings) {
        const idx = binarySearch(run.fingerprints, fp);
        if (idx >= 0) {
          for (const seg of run.postings[idx]) segments.add(seg);
        }
      }
    }
    return { segments, indexedThrough: state.indexed_through };
  }

  async createSecondaryIndexTailMatcher(stream: string, indexName: string, keyBytes: Uint8Array): Promise<IndexTailMatcher | null> {
    if (this.configuredSpan <= 0) return null;
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return null;
    const configured = getConfiguredSecondaryIndexes(regRes.value).find((entry) => entry.name === indexName);
    if (!configured) return null;
    const state = this.db.getSecondaryIndexState(stream, indexName);
    if (!state) return null;
    if (state.config_hash !== hashSecondaryIndexField(configured)) return null;
    if (state.indexed_through <= 0) return null;
    const runs = this.db
      .listSecondaryIndexRuns(stream, indexName)
      .filter((meta) => meta.start_segment < state.indexed_through);
    if (runs.length === 0) return null;

    const fp = siphash24(state.index_secret, keyBytes);
    let cursor = runs.length - 1;
    let resolvedDownTo = state.indexed_through;
    const matchedSegments = new Set<number>();
    const unknownSegments = new Set<number>();

    return {
      indexedThrough: state.indexed_through,
      matchesSegment: async (segmentIndex: number) => {
        if (segmentIndex < 0 || segmentIndex >= state.indexed_through) return false;
        while (segmentIndex < resolvedDownTo && cursor >= 0) {
          const meta = runs[cursor--]!;
          resolvedDownTo = Math.min(resolvedDownTo, meta.start_segment);
          const runRes = await this.loadRunResult(meta);
          if (Result.isError(runRes) || !runRes.value) {
            for (let seg = meta.start_segment; seg <= meta.end_segment; seg++) {
              unknownSegments.add(seg);
            }
            continue;
          }
          const run = runRes.value;
          if (run.filter && !binaryFuseContains(run.filter, fp)) continue;
          if (run.runType === RUN_TYPE_MASK16 && run.masks) {
            const idx = binarySearch(run.fingerprints, fp);
            if (idx >= 0) {
              const mask = run.masks[idx];
              for (let bit = 0; bit < 16; bit++) {
                if ((mask & (1 << bit)) !== 0) matchedSegments.add(run.meta.startSegment + bit);
              }
            }
            continue;
          }
          if (run.runType === RUN_TYPE_SINGLE_SEGMENT) {
            if (binarySearch(run.fingerprints, fp) >= 0) matchedSegments.add(run.meta.startSegment);
            continue;
          }
          if (!run.postings) continue;
          const idx = binarySearch(run.fingerprints, fp);
          if (idx < 0) continue;
          for (const seg of run.postings[idx]) matchedSegments.add(seg);
        }
        if (segmentIndex < resolvedDownTo) return true;
        return matchedSegments.has(segmentIndex) || unknownSegments.has(segmentIndex);
      },
    };
  }

  getLocalCacheBytes(stream: string): number {
    if (!this.runDiskCache) return 0;
    return this.runDiskCache.bytesForObjectKeyPrefix(`streams/${streamHash16Hex(stream)}/secondary-index/`);
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
    await Promise.all([this.runOneBuildTask(), this.runOneCompactionTask()]);
  }

  async persistUnifiedExactRunsFromSearchBuild(
    stream: string,
    registry: SchemaRegistry,
    segmentIndex: number,
    runs: SearchSegmentBuildOutput["exactRuns"]
  ): Promise<
    Result<
      | null
      | {
          persistedIndexNames: string[];
          outputSizeBytes: bigint;
          outputRecordCounts: Record<string, number>;
          outputFilterLens: Record<string, number>;
        },
      SecondaryIndexBuildError
    >
  > {
    if (this.configuredSpan <= 0 || runs.length === 0) return Result.ok(null);
    const configured = getConfiguredSecondaryIndexes(registry);
    if (configured.length === 0) return Result.ok(null);
    const stateByName = new Map(this.db.listSecondaryIndexStates(stream).map((state) => [state.index_name, state]));
    const targetIndexes = configured.filter((configuredIndex) => {
      const state = stateByName.get(configuredIndex.name);
      if (!state) return false;
      if (state.config_hash !== hashSecondaryIndexField(configuredIndex)) return false;
      return state.indexed_through === segmentIndex;
    });
    if (targetIndexes.length === 0) return Result.ok(null);
    const targetIndexNames = new Set(targetIndexes.map((entry) => entry.name));
    const outputRuns = runs.filter((entry) => targetIndexNames.has(entry.indexName));
    if (outputRuns.length === 0) return Result.ok(null);

    const persistRes = await this.persistExactRuns(stream, outputRuns, targetIndexes, stateByName, segmentIndex + 1, 1);
    if (Result.isError(persistRes)) return persistRes;
    return Result.ok({
      persistedIndexNames: targetIndexes.map((entry) => entry.name),
      ...persistRes.value,
    });
  }

  async runOneBuildTask(): Promise<boolean> {
    if (this.runningBuildTick) return false;
    const stream = this.takeNextStream(this.buildQueue);
    if (!stream) return false;
    this.runningBuildTick = true;
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return true;
      const registry = regRes.value;
      const configured = getConfiguredSecondaryIndexes(registry);
      const configuredNames = new Set(configured.map((entry) => entry.name));
      const existing = this.db.listSecondaryIndexStates(stream);
      let removedAny = false;
      for (const state of existing) {
        if (configuredNames.has(state.index_name)) continue;
        this.db.deleteSecondaryIndex(stream, state.index_name);
        removedAny = true;
      }
      if (removedAny) {
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore and retry on next enqueue
          }
        }
      }
      const batchLeaders = this.selectBuildBatchLeaders(stream, configured, registry.search?.profile ?? null);
      let failed = false;
      const gate = new ConcurrencyGate(secondaryBuildDispatchConcurrency(registry.search?.profile ?? null, batchLeaders.length));
      await Promise.all(
        batchLeaders.map((index) =>
          gate.run(async () => {
            if (failed) return;
            try {
              const buildRes = await this.maybeBuildRuns(stream, index);
              if (Result.isError(buildRes)) failed = true;
            } catch (e) {
              const msg = String((e as any)?.message ?? e).toLowerCase();
              if (
                !msg.includes("database has closed") &&
                !msg.includes("closed database") &&
                !msg.includes("statement has finalized")
              ) {
                console.error("secondary index build failed", stream, index.name, e);
              }
              failed = true;
            }
          })
        )
      );
      if (!failed) {
        try {
          await this.flushDeferredManifestPublish(stream);
        } catch {
          failed = true;
        }
      }
      if (failed) {
        this.buildQueue.add(stream);
      }
      return true;
    } finally {
      this.runningBuildTick = false;
    }
  }

  private selectBuildBatchLeaders(
    stream: string,
    configured: SecondaryIndexField[],
    profile: string | null
  ): SecondaryIndexField[] {
    const states = new Map(this.db.listSecondaryIndexStates(stream).map((state) => [state.index_name, state.indexed_through]));
    if (profile === "evlog") {
      const leaders: SecondaryIndexField[] = [];
      const covered = new Set<string>();
      const configuredOrder = new Map(configured.map((entry, index) => [entry.name, index]));
      for (const group of EVLOG_EXACT_BATCH_GROUPS) {
        const groupIndexes = configured.filter((entry) => (group as readonly string[]).includes(entry.name));
        if (groupIndexes.length === 0) continue;
        let minIndexedThrough = Number.POSITIVE_INFINITY;
        for (const configuredIndex of groupIndexes) {
          minIndexedThrough = Math.min(minIndexedThrough, states.get(configuredIndex.name) ?? 0);
        }
        const laggingBatch = groupIndexes.filter((configuredIndex) => (states.get(configuredIndex.name) ?? 0) === minIndexedThrough);
        if (laggingBatch.length > 0) leaders.push(laggingBatch[0]!);
        for (const configuredIndex of groupIndexes) covered.add(configuredIndex.name);
      }
      for (const configuredIndex of configured) {
        if (covered.has(configuredIndex.name)) continue;
        leaders.push(configuredIndex);
      }
      return leaders.sort((a, b) => {
        const indexedThroughA = states.get(a.name) ?? 0;
        const indexedThroughB = states.get(b.name) ?? 0;
        if (indexedThroughA !== indexedThroughB) return indexedThroughA - indexedThroughB;
        return (configuredOrder.get(a.name) ?? 0) - (configuredOrder.get(b.name) ?? 0);
      });
    }
    const leaders: SecondaryIndexField[] = [];
    const seen = new Set<string>();
    for (const configuredIndex of configured) {
      const indexedThrough = states.get(configuredIndex.name) ?? 0;
      const peerIndexes = configured.filter((entry) => (states.get(entry.name) ?? 0) === indexedThrough);
      const batchIndexes =
        peerIndexes.slice(0, SECONDARY_L0_BATCH_MAX_FIELDS);
      const batchKey = batchIndexes
        .map((entry) => entry.name)
        .sort((a, b) => a.localeCompare(b))
        .join(",");
      if (seen.has(batchKey)) continue;
      seen.add(batchKey);
      leaders.push(configuredIndex);
    }
    return leaders;
  }

  async runOneCompactionTask(): Promise<boolean> {
    if (this.runningCompactionTick) return false;
    const stream = this.takeNextStream(this.compactionQueue);
    if (!stream) return false;
    this.runningCompactionTick = true;
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return true;
      const configured = getConfiguredSecondaryIndexes(regRes.value);
      const profile = regRes.value.search?.profile ?? null;
      const uploadedCount = this.db.countUploadedSegments(stream);
      let failed = false;
      let attemptedAny = false;
      for (const index of configured) {
        const state = this.db.getSecondaryIndexState(stream, index.name);
        if (state && shouldDeferSecondaryCompaction(profile, uploadedCount, state.indexed_through)) continue;
        attemptedAny = true;
        try {
          const compactRes = await this.maybeCompactRuns(stream, index.name);
          if (Result.isError(compactRes)) {
            this.compactionQueue.add(stream);
            failed = true;
            break;
          }
        } catch (e) {
          const msg = String((e as any)?.message ?? e).toLowerCase();
          if (!msg.includes("database has closed") && !msg.includes("closed database") && !msg.includes("statement has finalized")) {
            console.error("secondary index compaction failed", stream, index.name, e);
          }
          this.compactionQueue.add(stream);
          failed = true;
          break;
        }
      }
      if (!attemptedAny) return false;
      if (!failed) {
        try {
          await this.flushDeferredManifestPublish(stream);
        } catch {
          this.compactionQueue.add(stream);
        }
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

  private getEvlogPeerBatch(indexName: string, peerIndexes: SecondaryIndexField[]): SecondaryIndexField[] {
    for (const group of EVLOG_EXACT_BATCH_GROUPS) {
      const batch = peerIndexes.filter((configuredIndex) => (group as readonly string[]).includes(configuredIndex.name));
      if (batch.some((configuredIndex) => configuredIndex.name === indexName)) return batch;
    }
    return peerIndexes.filter((configuredIndex) => configuredIndex.name === indexName);
  }

  private shouldPublishManifestAfterL0Batch(
    configured: SecondaryIndexField[],
    batchIndexes: SecondaryIndexField[],
    stateByName: Map<string, NonNullable<ReturnType<SqliteDurableStore["getSecondaryIndexState"]>>>,
    nextIndexedThrough: number
  ): boolean {
    let minBefore = Number.POSITIVE_INFINITY;
    let minAfter = Number.POSITIVE_INFINITY;
    const batchNames = new Set(batchIndexes.map((entry) => entry.name));
    for (const configuredIndex of configured) {
      const state = stateByName.get(configuredIndex.name);
      const indexedThrough = state?.indexed_through ?? 0;
      minBefore = Math.min(minBefore, indexedThrough);
      minAfter = Math.min(minAfter, batchNames.has(configuredIndex.name) ? nextIndexedThrough : indexedThrough);
    }
    return minAfter > minBefore;
  }

  private async maybeBuildRuns(stream: string, index: SecondaryIndexField): Promise<Result<void, SecondaryIndexBuildError>> {
    if (this.configuredSpan <= 0) return Result.ok(undefined);
    const key = `${stream}:${index.name}`;
    if (this.building.has(key)) return Result.ok(undefined);
    this.building.add(key);
    try {
      const registryRes = this.registry.getRegistryResult(stream);
      if (Result.isError(registryRes)) return invalidIndexBuild(registryRes.error.message);
      const registry = registryRes.value;
      return await this.asyncGate.run(async () => {
        const configured = getConfiguredSecondaryIndexes(registry);
        let manifestChanged = false;
        const stateByName = new Map<string, NonNullable<ReturnType<SqliteDurableStore["getSecondaryIndexState"]>>>();
        for (const configuredIndex of configured) {
          const configHash = hashSecondaryIndexField(configuredIndex);
          let state = this.db.getSecondaryIndexState(stream, configuredIndex.name);
          if (!state) {
            this.db.upsertSecondaryIndexState(stream, configuredIndex.name, randomBytes(16), configHash, 0);
            state = this.db.getSecondaryIndexState(stream, configuredIndex.name);
          } else if (state.config_hash !== configHash) {
            this.db.deleteSecondaryIndex(stream, configuredIndex.name);
            this.db.upsertSecondaryIndexState(stream, configuredIndex.name, randomBytes(16), configHash, 0);
            state = this.db.getSecondaryIndexState(stream, configuredIndex.name);
            manifestChanged = true;
          }
          if (state) stateByName.set(configuredIndex.name, state);
        }
        if (manifestChanged) {
          this.onMetadataChanged?.(stream);
          if (this.publishManifest) {
            try {
              await this.publishManifest(stream);
            } catch {
              // ignore and retry later
            }
          }
        }

        const state = stateByName.get(index.name);
        if (!state) return Result.ok(undefined);
        const indexedThrough = state.indexed_through;
        const uploadedCount = this.db.countUploadedSegments(stream);
        const peerIndexes = configured.filter((configuredIndex) => stateByName.get(configuredIndex.name)?.indexed_through === indexedThrough);
        const batchIndexes =
          registry.search?.profile === "evlog"
            ? this.getEvlogPeerBatch(index.name, peerIndexes)
            : peerIndexes.slice(0, SECONDARY_L0_BATCH_MAX_FIELDS);
        if (batchIndexes.length === 0) return Result.ok(undefined);
        const batchSpan = secondaryExactBatchSpan(
          registry.search?.profile,
          this.configuredSpan,
          batchIndexes.map((entry) => entry.name)
        );
        if (batchSpan <= 0) return Result.ok(undefined);
        const start = indexedThrough;
        if (uploadedCount < indexedThrough + batchSpan) return Result.ok(undefined);
        const end = start + batchSpan - 1;
        const segments: SegmentRow[] = [];
        for (let i = start; i <= end; i++) {
          const seg = this.db.getSegmentByIndex(stream, i);
          if (!seg || !seg.r2_etag) return Result.ok(undefined);
          segments.push(seg);
        }
        const segmentLeaseRes = this.segmentLocality
          ? await this.segmentLocality.acquireWindowResult(
              stream,
              segments,
              `exact indexing (${batchIndexes.map((entry) => entry.name).join(", ")})`
            )
          : Result.err({ kind: "missing_segment" as const, message: "exact indexing requires a segment disk cache" });
        if (Result.isError(segmentLeaseRes)) {
          if (segmentLeaseRes.error.kind === "index_cache_overloaded") {
            this.buildQueue.add(stream);
            this.onMetadataChanged?.(stream);
            return Result.ok(undefined);
          }
          return invalidIndexBuild(segmentLeaseRes.error.message);
        }

        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "secondary_l0_build",
          targetKind: "secondary_index_batch",
          targetName: batchIndexes.map((entry) => entry.name).join(","),
          inputKind: "segment",
          inputCount: segments.length,
          inputSizeBytes: segments.reduce((sum, segment) => sum + BigInt(segment.size_bytes), 0n),
          startSegment: start,
          endSegment: end,
          detail: {
            index_count: batchIndexes.length,
            index_names: batchIndexes.map((entry) => entry.name),
            level: 0,
            input_payload_bytes: segments.reduce((sum, segment) => sum + Number(segment.payload_bytes), 0),
          },
        });
        const useUnifiedSearchBuild =
          registry.search?.profile === "evlog" &&
          batchSpan === 1 &&
          !!this.searchSegmentBuilds &&
          batchIndexes.length > 1;
        const batchIndexNames = new Set(batchIndexes.map((entry) => entry.name));
        const unifiedExactIndexes = useUnifiedSearchBuild
          ? collectUnifiedSearchBuildExactIndexes(registry, (indexName) => {
              const state = stateByName.get(indexName);
              if (!state) return null;
              return {
                configHash: state.config_hash,
                secret: state.index_secret,
              };
            }, (indexName) => batchIndexNames.has(indexName))
          : [];
        let runRes;
        let sharedBuildCacheStatus: "miss" | "shared_inflight" | null = null;
        let piggybackCompanionSectionKinds: CompanionSectionKind[] = [];
        let buildMs = 0;
        let piggybackCompanionPersistMs = 0;
        try {
          const buildStartedAt = Date.now();
          runRes = useUnifiedSearchBuild
            ? await (this.memorySampler
                ? this.memorySampler.track(
                    "exact_l0",
                    {
                      stream,
                      index_name: batchIndexes.map((entry) => entry.name).join(","),
                      start_segment: start,
                      end_segment: end,
                    },
                    async () => {
                      const sharedRes = await this.searchSegmentBuilds!.buildSegmentResult({
                        stream,
                        registry,
                        exactIndexes: unifiedExactIndexes,
                        plan: null,
                        planGeneration: null,
                        includeCompanion: false,
                        outputDir: `${this.cfg.rootDir}/tmp/search-segment-builds`,
                        segment: {
                          segmentIndex: segmentLeaseRes.value.localSegments[0]!.segmentIndex,
                          startOffset: segments[0]!.start_offset,
                          localPath: segmentLeaseRes.value.localSegments[0]!.localPath,
                        },
                      });
                      if (Result.isError(sharedRes)) return Result.err(sharedRes.error);
                      sharedBuildCacheStatus = sharedRes.value.cacheStatus;
                      return Result.ok({
                        output: {
                          kind: "secondary_l0_build" as const,
                          output: {
                            runs: sharedRes.value.output.exactRuns.filter((entry) => batchIndexNames.has(entry.indexName)),
                          },
                        },
                        telemetry: sharedRes.value.jobTelemetry,
                      });
                    }
                  )
                : (async () => {
                    const sharedRes = await this.searchSegmentBuilds!.buildSegmentResult({
                      stream,
                      registry,
                      exactIndexes: unifiedExactIndexes,
                      plan: null,
                      planGeneration: null,
                      includeCompanion: false,
                      outputDir: `${this.cfg.rootDir}/tmp/search-segment-builds`,
                      segment: {
                        segmentIndex: segmentLeaseRes.value.localSegments[0]!.segmentIndex,
                        startOffset: segments[0]!.start_offset,
                        localPath: segmentLeaseRes.value.localSegments[0]!.localPath,
                      },
                    });
                    if (Result.isError(sharedRes)) return Result.err(sharedRes.error);
                    sharedBuildCacheStatus = sharedRes.value.cacheStatus;
                    return Result.ok({
                      output: {
                        kind: "secondary_l0_build" as const,
                        output: {
                          runs: sharedRes.value.output.exactRuns.filter((entry) => batchIndexNames.has(entry.indexName)),
                        },
                      },
                      telemetry: sharedRes.value.jobTelemetry,
                    });
                  })())
            : this.memorySampler
              ? await this.memorySampler.track(
                  "exact_l0",
                  {
                    stream,
                    index_name: batchIndexes.map((entry) => entry.name).join(","),
                    start_segment: start,
                    end_segment: end,
                  },
                  () =>
                    this.buildWorkers.buildResult({
                      kind: "secondary_l0_build",
                      input: {
                        stream,
                        registry,
                        startSegment: start,
                        span: batchSpan,
                        outputDir: `${this.cfg.rootDir}/tmp/secondary-index-builds`,
                        indexes: batchIndexes.map((configuredIndex) => ({
                          index: configuredIndex,
                          secret: stateByName.get(configuredIndex.name)!.index_secret,
                        })),
                        segments: segmentLeaseRes.value.localSegments.map((segment) => ({
                          segmentIndex: segment.segmentIndex,
                          startOffset: segments.find((row) => row.segment_index === segment.segmentIndex)!.start_offset,
                          localPath: segment.localPath,
                        })),
                      },
                    })
                )
              : await this.buildWorkers.buildResult({
                  kind: "secondary_l0_build",
                  input: {
                    stream,
                    registry,
                    startSegment: start,
                    span: batchSpan,
                    outputDir: `${this.cfg.rootDir}/tmp/secondary-index-builds`,
                    indexes: batchIndexes.map((configuredIndex) => ({
                      index: configuredIndex,
                      secret: stateByName.get(configuredIndex.name)!.index_secret,
                    })),
                    segments: segmentLeaseRes.value.localSegments.map((segment) => ({
                      segmentIndex: segment.segmentIndex,
                      startOffset: segments.find((row) => row.segment_index === segment.segmentIndex)!.start_offset,
                      localPath: segment.localPath,
                    })),
                  },
                });
          buildMs = Date.now() - buildStartedAt;
        } finally {
          segmentLeaseRes.value.release();
        }
        if (Result.isError(runRes)) {
          action.fail(runRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
            },
          });
          return invalidIndexBuild(runRes.error.message);
        }
        if (runRes.value.output.kind !== "secondary_l0_build") {
          action.fail("unexpected worker result kind");
          return invalidIndexBuild("unexpected worker result kind");
        }
        const buildTelemetryDetail = asyncIndexActionMemoryDetail(runRes.value.telemetry);
        const outputRuns = runRes.value.output.output.runs;
        const nextIndexedThrough = end + 1;
        const shouldPublishManifest = this.shouldPublishManifestAfterL0Batch(
          configured,
          batchIndexes,
          stateByName,
          nextIndexedThrough
        );
        const exactPersistStartedAt = Date.now();
        const uploadExactRes = await this.uploadExactRunsResult(outputRuns);
        if (Result.isError(uploadExactRes)) {
          const exactPersistMs = Date.now() - exactPersistStartedAt;
          action.fail(uploadExactRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
              exact_persist_ms: exactPersistMs,
              ...buildTelemetryDetail,
            },
          });
          return invalidIndexBuild(uploadExactRes.error.message);
        }
        const persistRes = await this.finalizeExactRunsPersistResult(
          stream,
          uploadExactRes.value,
          batchIndexes,
          stateByName,
          nextIndexedThrough,
          false,
          batchSpan
        );
        const exactPersistMs = Date.now() - exactPersistStartedAt;
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
              exact_persist_ms: exactPersistMs,
              ...buildTelemetryDetail,
            },
          });
          return invalidIndexBuild(persistRes.error.message);
        }
        if (shouldPublishManifest) this.requestDeferredManifestPublish(stream);
        const deferPiggybackCompanion = false;
        piggybackCompanionSectionKinds = [];
        piggybackCompanionPersistMs = 0;
        action.succeed({
          outputCount: uploadExactRes.value.outputRuns.length,
          outputSizeBytes: uploadExactRes.value.outputSizeBytes,
          detail: {
            index_names: batchIndexes.map((entry) => entry.name),
            output_record_counts: uploadExactRes.value.outputRecordCounts,
            output_filter_lens: uploadExactRes.value.outputFilterLens,
            shared_build_cache_status: sharedBuildCacheStatus,
            piggyback_companion_section_kinds: piggybackCompanionSectionKinds,
            piggyback_companion_deferred: deferPiggybackCompanion,
            manifest_published: false,
            manifest_publish_requested: shouldPublishManifest,
            build_ms: buildMs,
            piggyback_companion_persist_ms: piggybackCompanionPersistMs,
            exact_persist_ms: exactPersistMs,
            ...buildTelemetryDetail,
          },
        });
        return Result.ok(undefined);
      });
    } catch (e: unknown) {
      return invalidIndexBuild(String((e as any)?.message ?? e));
    } finally {
      this.building.delete(key);
    }
  }

  private async maybeCompactRuns(stream: string, indexName: string): Promise<Result<void, SecondaryIndexBuildError>> {
    if (this.configuredSpan <= 0) return Result.ok(undefined);
    const compactionFanout = secondaryCompactionFanout(indexName, this.compactionFanout);
    if (compactionFanout <= 1) return Result.ok(undefined);
    const key = `${stream}:${indexName}`;
    if (this.compacting.has(key)) return Result.ok(undefined);
    this.compacting.add(key);
    try {
      return await this.asyncGate.run(async () => {
        const group = this.findCompactionGroup(stream, indexName);
        if (!group) {
          await this.gcRetiredRuns(stream, indexName);
          return Result.ok(undefined);
        }
        const { level, runs } = group;
        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "secondary_compaction_build",
          targetKind: "secondary_index",
          targetName: indexName,
          inputKind: "run",
          inputCount: runs.length,
          inputSizeBytes: runs.reduce((sum, run) => sum + BigInt(run.size_bytes), 0n),
          startSegment: runs[0]?.start_segment ?? null,
          endSegment: runs[runs.length - 1]?.end_segment ?? null,
          detail: {
            level_from: level,
            level_to: level + 1,
            compaction_fanout: compactionFanout,
          },
        });
        const runRes = await this.buildCompactedRunResult(stream, indexName, level + 1, runs);
        if (Result.isError(runRes)) {
          action.fail(runRes.error.message);
          return runRes;
        }
        const buildTelemetryDetail = asyncIndexActionMemoryDetail(runRes.value.jobTelemetry);
        const run = runRes.value.run;
        const artifactPersistStartedAt = Date.now();
        const persistRes = await this.persistRunArtifactResult(run);
        const artifactPersistMs = Date.now() - artifactPersistStartedAt;
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              output_record_count: run.meta.recordCount,
              output_filter_len: run.meta.filterLen,
              source_prepare_ms: runRes.value.sourcePrepareMs,
              worker_build_ms: runRes.value.workerBuildMs,
              artifact_persist_ms: artifactPersistMs,
              source_cache_hits: runRes.value.sourceCacheHitCount,
              source_cache_fills: runRes.value.sourceCacheFillCount,
              source_temp_spills: runRes.value.sourceTempSpillCount,
              source_fetch_concurrency: runRes.value.sourceFetchConcurrency,
              compaction_fanout: compactionFanout,
              ...buildTelemetryDetail,
            },
          });
          return persistRes;
        }
        const sizeBytes = persistRes.value;
        const metadataFinalizeStartedAt = Date.now();
        this.db.insertSecondaryIndexRun({
          run_id: run.meta.runId,
          stream,
          index_name: indexName,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          size_bytes: sizeBytes,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        const state = this.db.getSecondaryIndexState(stream, indexName);
        if (state && run.meta.endSegment + 1 > state.indexed_through) {
          this.db.updateSecondaryIndexedThrough(stream, indexName, run.meta.endSegment + 1);
        }
        const manifestRow = this.db.getManifestRow(stream);
        this.db.retireSecondaryIndexRuns(
          runs.map((r) => r.run_id),
          manifestRow.generation + 1,
          this.db.nowMs()
        );
        this.onMetadataChanged?.(stream);
        const metadataFinalizeMs = Date.now() - metadataFinalizeStartedAt;
        this.requestDeferredManifestPublish(stream);
        const retiredGcStartedAt = Date.now();
        await this.gcRetiredRuns(stream, indexName);
        const retiredGcMs = Date.now() - retiredGcStartedAt;
        this.compactionQueue.add(stream);
        const finalizeMs = Date.now() - metadataFinalizeStartedAt;
        action.succeed({
          outputCount: 1,
          outputSizeBytes: BigInt(sizeBytes),
          detail: {
            output_record_count: run.meta.recordCount,
            output_filter_len: run.meta.filterLen,
            source_prepare_ms: runRes.value.sourcePrepareMs,
            worker_build_ms: runRes.value.workerBuildMs,
            artifact_persist_ms: artifactPersistMs,
            finalize_ms: finalizeMs,
            source_cache_hits: runRes.value.sourceCacheHitCount,
            source_cache_fills: runRes.value.sourceCacheFillCount,
            source_temp_spills: runRes.value.sourceTempSpillCount,
            source_fetch_concurrency: runRes.value.sourceFetchConcurrency,
            compaction_fanout: compactionFanout,
            metadata_finalize_ms: metadataFinalizeMs,
            publish_manifest_ms: 0,
            manifest_published: false,
            manifest_publish_requested: true,
            retired_gc_ms: retiredGcMs,
            ...buildTelemetryDetail,
          },
        });
        return Result.ok(undefined);
      });
    } finally {
      this.compacting.delete(key);
    }
  }

  private async persistExactRuns(
    stream: string,
    outputRuns: ExactBuiltRun[],
    targetIndexes: SecondaryIndexField[],
    stateByName: Map<string, ReturnType<SqliteDurableStore["listSecondaryIndexStates"]>[number]>,
    nextIndexedThrough: number,
    batchSpan: number
  ): Promise<
    Result<
      {
        outputSizeBytes: bigint;
        outputRecordCounts: Record<string, number>;
        outputFilterLens: Record<string, number>;
      },
      SecondaryIndexBuildError
    >
  > {
    const uploadRes = await this.uploadExactRunsResult(outputRuns);
    if (Result.isError(uploadRes)) return uploadRes;
    const finalizeRes = await this.finalizeExactRunsPersistResult(
      stream,
      uploadRes.value,
      targetIndexes,
      stateByName,
      nextIndexedThrough,
      true,
      batchSpan
    );
    if (Result.isError(finalizeRes)) return finalizeRes;
    return Result.ok({
      outputSizeBytes: uploadRes.value.outputSizeBytes,
      outputRecordCounts: uploadRes.value.outputRecordCounts,
      outputFilterLens: uploadRes.value.outputFilterLens,
    });
  }

  private async uploadExactRunsResult(
    outputRuns: ExactBuiltRun[]
  ): Promise<
    Result<
      {
        outputRuns: UploadedExactRun[];
        outputSizeBytes: bigint;
        outputRecordCounts: Record<string, number>;
        outputFilterLens: Record<string, number>;
      },
      SecondaryIndexBuildError
    >
  > {
    const persistRes = await uploadRunPayloadsBoundedResult({
      tasks: outputRuns.map((run) =>
        run.storage === "file"
          ? { objectKey: run.meta.objectKey, localPath: run.localPath, sizeBytes: run.sizeBytes }
          : { objectKey: run.meta.objectKey, payload: run.payload }
      ),
      os: this.os,
      concurrency: outputRuns.length,
      retries: this.cfg.objectStoreRetries,
      baseDelayMs: this.cfg.objectStoreBaseDelayMs,
      maxDelayMs: this.cfg.objectStoreMaxDelayMs,
      timeoutMs: this.cfg.objectStoreTimeoutMs,
    });
    if (Result.isError(persistRes)) return invalidIndexBuild(persistRes.error.message);

    let outputSizeBytes = 0n;
    const outputRecordCounts: Record<string, number> = {};
    const outputFilterLens: Record<string, number> = {};
    const uploadedRuns: UploadedExactRun[] = [];
    for (let index = 0; index < outputRuns.length; index += 1) {
      const run = outputRuns[index]!;
      const sizeBytes = persistRes.value[index] ?? (run.storage === "file" ? run.sizeBytes : run.payload.byteLength);
      outputSizeBytes += BigInt(sizeBytes);
      outputRecordCounts[run.indexName] = run.meta.recordCount;
      outputFilterLens[run.indexName] = run.meta.filterLen;
      uploadedRuns.push({ run, sizeBytes });
      if (run.storage === "file") {
        const cached = this.runDiskCache?.putFromLocal(run.meta.objectKey, run.localPath, run.sizeBytes) ?? false;
        if (!cached) {
          try {
            unlinkSync(run.localPath);
          } catch {
            // ignore temp cleanup failures
          }
        }
      } else {
        this.runDiskCache?.put(run.meta.objectKey, run.payload);
      }
    }
    return Result.ok({ outputRuns: uploadedRuns, outputSizeBytes, outputRecordCounts, outputFilterLens });
  }

  private async finalizeExactRunsPersistResult(
    stream: string,
    uploaded: {
      outputRuns: UploadedExactRun[];
      outputSizeBytes: bigint;
      outputRecordCounts: Record<string, number>;
      outputFilterLens: Record<string, number>;
    },
    targetIndexes: SecondaryIndexField[],
    stateByName: Map<string, ReturnType<SqliteDurableStore["listSecondaryIndexStates"]>[number]>,
    nextIndexedThrough: number,
    shouldPublishManifest: boolean,
    batchSpan: number
  ): Promise<Result<void, SecondaryIndexBuildError>> {
    for (const { run, sizeBytes } of uploaded.outputRuns) {
      this.db.insertSecondaryIndexRun({
        run_id: run.meta.runId,
        stream,
        index_name: run.indexName,
        level: run.meta.level,
        start_segment: run.meta.startSegment,
        end_segment: run.meta.endSegment,
        object_key: run.meta.objectKey,
        size_bytes: sizeBytes,
        filter_len: run.meta.filterLen,
        record_count: run.meta.recordCount,
      });
    }
    for (const configuredIndex of targetIndexes) {
      this.db.updateSecondaryIndexedThrough(stream, configuredIndex.name, nextIndexedThrough);
      const currentState = stateByName.get(configuredIndex.name);
      if (currentState) currentState.indexed_through = nextIndexedThrough;
    }
    this.onMetadataChanged?.(stream);
    if (shouldPublishManifest && this.publishManifest) {
      try {
        await this.publishManifest(stream);
      } catch {
        // ignore and retry later
      }
    }
    if (this.db.countUploadedSegments(stream) >= nextIndexedThrough + batchSpan) this.buildQueue.add(stream);
    this.compactionQueue.add(stream);
    return Result.ok(undefined);
  }

  private findCompactionGroup(stream: string, indexName: string): { level: number; runs: SecondaryIndexRunRow[] } | null {
    const runs = this.db.listSecondaryIndexRuns(stream, indexName);
    const compactionFanout = secondaryCompactionFanout(indexName, this.compactionFanout);
    if (runs.length < compactionFanout) return null;
    const maxLevel = secondaryCompactionMaxLevel(indexName, this.maxLevel);
    const byLevel = new Map<number, SecondaryIndexRunRow[]>();
    for (const run of runs) {
      const arr = byLevel.get(run.level) ?? [];
      arr.push(run);
      byLevel.set(run.level, arr);
    }
    for (let level = 0; level <= maxLevel; level++) {
      const levelRuns = byLevel.get(level);
      if (!levelRuns || levelRuns.length < compactionFanout) continue;
      const span = this.levelSpan(stream, indexName, level);
      for (let i = 0; i + compactionFanout <= levelRuns.length; i++) {
        const base = levelRuns[i].start_segment;
        let ok = true;
        for (let j = 0; j < compactionFanout; j++) {
          const run = levelRuns[i + j];
          const expectStart = base + j * span;
          if (run.start_segment !== expectStart || run.end_segment !== expectStart + span - 1) {
            ok = false;
            break;
          }
        }
        if (ok) return { level, runs: levelRuns.slice(i, i + compactionFanout) };
      }
    }
    return null;
  }

  private levelSpan(stream: string, indexName: string, level: number): number {
    const compactionFanout = secondaryCompactionFanout(indexName, this.compactionFanout);
    let span = this.baseL0Span(stream, [indexName]);
    for (let i = 0; i < level; i++) span *= compactionFanout;
    return span;
  }

  private baseL0Span(stream: string, indexNames: readonly string[]): number {
    const registryRes = this.registry.getRegistryResult(stream);
    const profile = Result.isOk(registryRes) ? registryRes.value.search?.profile : null;
    return secondaryExactBatchSpan(profile, this.configuredSpan, indexNames);
  }

  private async buildCompactedRunResult(
    stream: string,
    indexName: string,
    level: number,
    inputs: SecondaryIndexRunRow[]
  ): Promise<
    Result<
      {
        run: SecondaryCompactionBuildOutput;
        jobTelemetry: import("./index_build_telemetry").IndexBuildJobTelemetry | null;
        sourcePrepareMs: number;
        workerBuildMs: number;
        sourceCacheHitCount: number;
        sourceCacheFillCount: number;
        sourceTempSpillCount: number;
        sourceFetchConcurrency: number;
      },
      SecondaryIndexBuildError
    >
  > {
    const sourcePrepareStartedAt = Date.now();
    const sourcesRes = await this.prepareCompactionSourcesResult(inputs);
    const sourcePrepareMs = Date.now() - sourcePrepareStartedAt;
    if (Result.isError(sourcesRes)) return sourcesRes;
    try {
      const workerBuildStartedAt = Date.now();
      const buildRes = await this.buildWorkers.buildResult({
        kind: "secondary_compaction_build",
        input: {
          stream,
          indexName,
          level,
          outputDir: `${this.cfg.rootDir}/tmp/secondary-index-builds`,
          inputs: sourcesRes.value.sources,
        },
      });
      const workerBuildMs = Date.now() - workerBuildStartedAt;
      if (Result.isError(buildRes)) return invalidIndexBuild(buildRes.error.message);
      if (buildRes.value.output.kind !== "secondary_compaction_build") return invalidIndexBuild("unexpected worker result kind");
      return Result.ok({
        run: buildRes.value.output.output,
        jobTelemetry: buildRes.value.telemetry,
        sourcePrepareMs,
        workerBuildMs,
        sourceCacheHitCount: sourcesRes.value.cacheHitCount,
        sourceCacheFillCount: sourcesRes.value.cacheFillCount,
        sourceTempSpillCount: sourcesRes.value.tempSpillCount,
        sourceFetchConcurrency: sourcesRes.value.fetchConcurrency,
      });
    } finally {
      sourcesRes.value.cleanup();
    }
  }

  private async prepareCompactionSourcesResult(
    inputs: SecondaryIndexRunRow[]
  ): Promise<
    Result<
      {
        sources: SecondaryCompactionRunSource[];
        cacheHitCount: number;
        cacheFillCount: number;
        tempSpillCount: number;
        fetchConcurrency: number;
        cleanup: () => void;
      },
      SecondaryIndexBuildError
    >
  > {
    const sources = new Array<SecondaryCompactionRunSource>(inputs.length);
    const tempPaths: string[] = [];
    let cacheHitCount = 0;
    let cacheFillCount = 0;
    let tempSpillCount = 0;
    let firstError: SecondaryIndexBuildError | null = null;
    const fetchConcurrency = secondaryCompactionSourceFetchConcurrency(inputs[0]?.index_name ?? "", inputs.length);
    const gate = new ConcurrencyGate(fetchConcurrency);

    await Promise.all(
      inputs.map((meta, index) =>
        gate.run(async () => {
          if (firstError) return;
          const sourceRes = await this.prepareCompactionSourceResult(meta);
          if (Result.isError(sourceRes)) {
            firstError = sourceRes.error;
            return;
          }
          sources[index] = sourceRes.value.source;
          if (sourceRes.value.location === "cache_hit") cacheHitCount += 1;
          if (sourceRes.value.location === "cache_fill") cacheFillCount += 1;
          if (sourceRes.value.location === "temp_spill") {
            tempSpillCount += 1;
            tempPaths.push(sourceRes.value.source.localPath!);
          }
        })
      )
    );

    const cleanup = () => {
      for (const tempPath of tempPaths) {
        try {
          unlinkSync(tempPath);
        } catch {
          // ignore temp cleanup failures
        }
      }
    };

    if (firstError) {
      cleanup();
      return Result.err(firstError);
    }

    return Result.ok({
      sources,
      cacheHitCount,
      cacheFillCount,
      tempSpillCount,
      fetchConcurrency,
      cleanup,
    });
  }

  private async prepareCompactionSourceResult(
    meta: SecondaryIndexRunRow
  ): Promise<
    Result<
      {
        source: SecondaryCompactionRunSource;
        location: "cache_hit" | "cache_fill" | "temp_spill" | "inline_fetch";
      },
      SecondaryIndexBuildError
    >
  > {
    if (this.runDiskCache?.has(meta.object_key)) {
      return Result.ok({
        source: {
          runId: meta.run_id,
          startSegment: meta.start_segment,
          endSegment: meta.end_segment,
          localPath: this.runDiskCache.getPath(meta.object_key),
        },
        location: "cache_hit",
      });
    }
    const tempOutputDir = `${this.cfg.rootDir}/tmp/secondary-index-sources`;
    const tempLocalPath = `${tempOutputDir}/secondary-source-${meta.run_id}-${Date.now()}-${randomBytes(4).toString("hex")}.irn`;
    try {
      if (meta.size_bytes <= SECONDARY_COMPACTION_SOURCE_INLINE_FETCH_MAX_BYTES) {
        const bytes = await retry(
          async () => {
            const data = await this.os.get(meta.object_key);
            if (!data) throw dsError(`missing secondary index run ${meta.object_key}`);
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
          source: {
            runId: meta.run_id,
            startSegment: meta.start_segment,
            endSegment: meta.end_segment,
            bytes,
          },
          location: "inline_fetch",
        });
      }
      if (this.os.getFile) {
        mkdirSync(tempOutputDir, { recursive: true });
        const fileRes = await retry(
          async () => {
            const result = await this.os.getFile!(meta.object_key, tempLocalPath);
            if (!result) throw dsError(`missing secondary index run ${meta.object_key}`);
            return result;
          },
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        );
        const sizeBytes = fileRes.size > 0 ? fileRes.size : meta.size_bytes;
        if (this.runDiskCache?.putFromLocal(meta.object_key, tempLocalPath, sizeBytes)) {
          return Result.ok({
            source: {
              runId: meta.run_id,
              startSegment: meta.start_segment,
              endSegment: meta.end_segment,
              localPath: this.runDiskCache.getPath(meta.object_key),
            },
            location: "cache_fill",
          });
        }
        return Result.ok({
          source: {
            runId: meta.run_id,
            startSegment: meta.start_segment,
            endSegment: meta.end_segment,
            localPath: tempLocalPath,
            deleteAfterUse: true,
          },
          location: "temp_spill",
        });
      }
      const bytes = await retry(
        async () => {
          const data = await this.os.get(meta.object_key);
          if (!data) throw dsError(`missing secondary index run ${meta.object_key}`);
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
          source: {
            runId: meta.run_id,
            startSegment: meta.start_segment,
            endSegment: meta.end_segment,
            localPath: this.runDiskCache.getPath(meta.object_key),
          },
          location: "cache_fill",
        });
      }
      const spillRes = writeIndexRunOutputFileResult(
        `${this.cfg.rootDir}/tmp/secondary-index-sources`,
        `secondary-source-${meta.run_id}`,
        bytes
      );
      if (Result.isError(spillRes)) return invalidIndexBuild(spillRes.error.message);
      return Result.ok({
        source: {
          runId: meta.run_id,
          startSegment: meta.start_segment,
          endSegment: meta.end_segment,
          localPath: spillRes.value.localPath,
          deleteAfterUse: true,
        },
        location: "temp_spill",
      });
    } catch (e: unknown) {
      try {
        unlinkSync(tempLocalPath);
      } catch {
        // ignore temp cleanup failures
      }
      return invalidIndexBuild(String((e as any)?.message ?? e));
    }
  }

  private async gcRetiredRuns(stream: string, indexName: string): Promise<void> {
    const retired = this.db.listRetiredSecondaryIndexRuns(stream, indexName);
    if (retired.length === 0) return;
    const manifest = this.db.getManifestRow(stream);
    const nowMs = this.db.nowMs();
    const cutoffGen =
      this.retireGenWindow > 0 && manifest.generation > this.retireGenWindow
        ? manifest.generation - this.retireGenWindow
        : 0;
    const toDelete: SecondaryIndexRunRow[] = [];
    for (const run of retired) {
      const expiredByGen = run.retired_gen != null && run.retired_gen > 0 && run.retired_gen <= cutoffGen;
      const expiredByTTL = run.retired_at_ms != null && run.retired_at_ms + BigInt(this.retireMinMs) <= nowMs;
      if (expiredByGen || expiredByTTL) toDelete.push(run);
    }
    if (toDelete.length === 0) return;
    const gate = new ConcurrencyGate(Math.max(1, SECONDARY_RETIRED_GC_DELETE_CONCURRENCY));
    await Promise.all(
      toDelete.map((run) =>
        gate.run(async () => {
          try {
            await this.os.delete(run.object_key);
          } catch {
            // ignore deletion errors
          }
          this.runCache.remove(run.object_key);
          this.runDiskCache?.remove(run.object_key);
        })
      )
    );
    this.db.deleteSecondaryIndexRuns(toDelete.map((run) => run.run_id));
  }

  private async persistRunResult(run: IndexRun): Promise<Result<number, SecondaryIndexBuildError>> {
    const payloadRes = encodeIndexRunResult(run);
    if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
    return await this.persistRunPayloadResult(run.meta, payloadRes.value, run);
  }

  private async persistRunArtifactResult(run: SecondaryCompactionBuildOutput): Promise<Result<number, SecondaryIndexBuildError>> {
    if (run.storage === "file") {
      try {
        if (this.os.putFileNoEtag) {
          await retry(
            () => this.os.putFileNoEtag!(run.meta.objectKey, run.localPath, run.sizeBytes),
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
        } else if (this.os.putFile) {
          await retryAbortable(
            (signal) => this.os.putFile!(run.meta.objectKey, run.localPath, run.sizeBytes, { signal }),
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
        } else {
          const payload = await Bun.file(run.localPath).bytes();
          await retryAbortable(
            (signal) => this.os.put(run.meta.objectKey, payload, { contentLength: payload.byteLength, signal }),
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
        }
      } catch (e: unknown) {
        try {
          unlinkSync(run.localPath);
        } catch {
          // ignore temp cleanup failures
        }
        return invalidIndexBuild(String((e as any)?.message ?? e));
      }
      const cached = this.runDiskCache?.putFromLocal(run.meta.objectKey, run.localPath, run.sizeBytes) ?? false;
      if (!cached) {
        try {
          unlinkSync(run.localPath);
        } catch {
          // ignore temp cleanup failures
        }
      }
      return Result.ok(run.sizeBytes);
    }
    return await this.persistRunPayloadResult(run.meta, run.payload);
  }

  private async persistRunPayloadResult(
    meta: IndexRun["meta"],
    payload: Uint8Array,
    decodedRun?: IndexRun
  ): Promise<Result<number, SecondaryIndexBuildError>> {
    try {
      if (this.os.putNoEtag) {
        await retry(
          () => this.os.putNoEtag!(meta.objectKey, payload, { contentLength: payload.byteLength }),
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        );
      } else {
        await retryAbortable(
          (signal) => this.os.put(meta.objectKey, payload, { contentLength: payload.byteLength, signal }),
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        );
      }
    } catch (e: unknown) {
      return invalidIndexBuild(String((e as any)?.message ?? e));
    }
    this.runDiskCache?.put(meta.objectKey, payload);
    if (decodedRun) this.runCache.put(meta.objectKey, decodedRun, payload.byteLength);
    return Result.ok(payload.byteLength);
  }

  private async loadRunResult(meta: SecondaryIndexRunRow): Promise<Result<IndexRun | null, SecondaryIndexBuildError>> {
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
      if (this.runDiskCache && this.os.getFile) {
        const tempOutputDir = `${this.cfg.rootDir}/tmp/secondary-index-runs`;
        const tempLocalPath = `${tempOutputDir}/secondary-run-${meta.run_id}-${Date.now()}-${randomBytes(4).toString("hex")}.irn`;
        try {
          mkdirSync(tempOutputDir, { recursive: true });
          const fileRes = await retry(
            async () => {
              const result = await this.os.getFile!(meta.object_key, tempLocalPath);
              if (!result) throw dsError(`missing secondary index run ${meta.object_key}`);
              return result;
            },
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
          const sizeBytes = fileRes.size > 0 ? fileRes.size : meta.size_bytes;
          if (this.runDiskCache.putFromLocal(meta.object_key, tempLocalPath, sizeBytes)) {
            bytes = this.runDiskCache.get(meta.object_key);
            if (!bytes) bytes = readFileSync(this.runDiskCache.getPath(meta.object_key));
          } else {
            bytes = readFileSync(tempLocalPath);
            rmSync(tempLocalPath, { force: true });
          }
        } catch (e: unknown) {
          rmSync(tempLocalPath, { force: true });
          return invalidIndexBuild(String((e as any)?.message ?? e));
        }
      } else {
        try {
          bytes = await retry(
            async () => {
              const data = await this.os.get(meta.object_key);
              if (!data) throw dsError(`missing secondary index run ${meta.object_key}`);
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
          return invalidIndexBuild(String((e as any)?.message ?? e));
        }
      }
      if (!this.runDiskCache?.has(meta.object_key)) this.runDiskCache?.put(meta.object_key, bytes);
    }
    const decodeRes = decodeIndexRunResult(bytes);
    if (Result.isError(decodeRes)) return invalidIndexBuild(decodeRes.error.message);
    this.runCache.put(meta.object_key, decodeRes.value, meta.size_bytes);
    return Result.ok(decodeRes.value);
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, SecondaryIndexBuildError>> {
    try {
      const data = await loadSegmentBytesCached(
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
      return Result.ok(data);
    } catch (e: unknown) {
      return invalidIndexBuild(String((e as any)?.message ?? e));
    }
  }
}
