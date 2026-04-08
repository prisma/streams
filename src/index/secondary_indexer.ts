import { randomBytes } from "node:crypto";
import { unlinkSync } from "node:fs";
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
import { beginAsyncIndexAction } from "./async_index_actions";
import { uploadRunPayloadsBoundedResult } from "./run_payload_upload";
import { writeIndexRunOutputFileResult } from "./index_run_output_file";
import { buildDesiredSearchCompanionPlan, hashSearchCompanionPlan, type SearchCompanionPlan } from "../search/companion_plan";
import { SearchSegmentBuildCoordinator } from "../search/search_segment_build_coordinator";
import { collectUnifiedSearchBuildExactIndexes, type SearchSegmentBuildOutput } from "../search/search_segment_build";
import type { CompanionSectionKind } from "../search/companion_format";

type SecondaryIndexBuildError = { kind: "invalid_index_build"; message: string };
const SECONDARY_L0_BATCH_MAX_FIELDS = 1;
const SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_HIGH_CARDINALITY = 2;
const SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_LOW_CARDINALITY = 4;
const SECONDARY_RETIRED_GC_DELETE_CONCURRENCY = 4;
const EVLOG_EXACT_COMPANION_OWNER_GROUP = ["method", "status", "duration"] as const;
const EVLOG_HIGH_CARDINALITY_EXACT_FIELDS = new Set(["requestId", "traceId", "spanId", "path"]);
const EVLOG_EXACT_BATCH_GROUPS = [
  ["level", "service", "environment"],
  ["timestamp"],
  ["requestId"],
  ["traceId"],
  ["spanId"],
  ["path"],
  ["method", "status", "duration"],
] as const;
type UnifiedCompanionSink = (
  stream: string,
  segmentIndex: number,
  planGeneration: number,
  output: NonNullable<SearchSegmentBuildOutput["companion"]>
) => Promise<
  Result<
    | null
    | {
        sectionKinds: CompanionSectionKind[];
        finalize: () => Result<{ sectionKinds: CompanionSectionKind[] }, { kind: string; message: string }>;
      },
    { kind: string; message: string }
  >
>;
type ExactBuiltRun = SearchSegmentBuildOutput["exactRuns"][number];
type UploadedExactRun = {
  run: ExactBuiltRun;
  sizeBytes: number;
};

function invalidIndexBuild<T = never>(message: string): Result<T, SecondaryIndexBuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

export function secondaryCompactionSourceFetchConcurrency(indexName: string, inputCount: number): number {
  const cap = EVLOG_HIGH_CARDINALITY_EXACT_FIELDS.has(indexName)
    ? SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_HIGH_CARDINALITY
    : SECONDARY_COMPACTION_SOURCE_FETCH_CONCURRENCY_LOW_CARDINALITY;
  return Math.max(1, Math.min(cap, inputCount || 1));
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
  private readonly span: number;
  private readonly compactionFanout: number;
  private readonly maxLevel: number;
  private readonly retireGenWindow: number;
  private readonly retireMinMs: number;
  private readonly buildQueue = new Set<string>();
  private readonly compactionQueue = new Set<string>();
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
  private unifiedCompanionSink?: UnifiedCompanionSink;

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
    this.span = cfg.indexL0SpanSegments > 0 ? 1 : 0;
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

  enqueue(stream: string): void {
    if (this.span <= 0) return;
    this.buildQueue.add(stream);
    this.compactionQueue.add(stream);
  }

  async candidateSegmentsForSecondaryIndex(
    stream: string,
    indexName: string,
    keyBytes: Uint8Array
  ): Promise<{ segments: Set<number>; indexedThrough: number } | null> {
    if (this.span <= 0) return null;
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
      } else if (run.postings) {
        const idx = binarySearch(run.fingerprints, fp);
        if (idx >= 0) {
          for (const seg of run.postings[idx]) segments.add(seg);
        }
      }
    }
    return { segments, indexedThrough: state.indexed_through };
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

  setUnifiedCompanionSink(sink: UnifiedCompanionSink): void {
    this.unifiedCompanionSink = sink;
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
    if (this.span <= 0 || runs.length === 0) return Result.ok(null);
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

    const persistRes = await this.persistExactRuns(stream, outputRuns, targetIndexes, stateByName, segmentIndex + 1);
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
      for (const index of configured) {
        try {
          const buildRes = await this.maybeBuildRuns(stream, index);
          if (Result.isError(buildRes)) {
            this.buildQueue.add(stream);
            break;
          }
        } catch (e) {
          const msg = String((e as any)?.message ?? e).toLowerCase();
          if (!msg.includes("database has closed") && !msg.includes("closed database") && !msg.includes("statement has finalized")) {
            console.error("secondary index build failed", stream, index.name, e);
          }
          this.buildQueue.add(stream);
          break;
        }
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
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return true;
      const configured = getConfiguredSecondaryIndexes(regRes.value);
      for (const index of configured) {
        try {
          const compactRes = await this.maybeCompactRuns(stream, index.name);
          if (Result.isError(compactRes)) {
            this.compactionQueue.add(stream);
            break;
          }
        } catch (e) {
          const msg = String((e as any)?.message ?? e).toLowerCase();
          if (!msg.includes("database has closed") && !msg.includes("closed database") && !msg.includes("statement has finalized")) {
            console.error("secondary index compaction failed", stream, index.name, e);
          }
          this.compactionQueue.add(stream);
          break;
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

  private getCurrentCompanionPlan(stream: string, registry: SchemaRegistry): {
    generation: number;
    plan: SearchCompanionPlan;
  } | null {
    const desiredPlan = buildDesiredSearchCompanionPlan(registry);
    const desiredHash = hashSearchCompanionPlan(desiredPlan);
    const current = this.db.getSearchCompanionPlan(stream);
    if (!current || current.plan_hash !== desiredHash) return null;
    return { generation: current.generation, plan: desiredPlan };
  }

  private getEvlogPeerBatch(indexName: string, peerIndexes: SecondaryIndexField[]): SecondaryIndexField[] {
    for (const group of EVLOG_EXACT_BATCH_GROUPS) {
      const batch = peerIndexes.filter((configuredIndex) => (group as readonly string[]).includes(configuredIndex.name));
      if (batch.some((configuredIndex) => configuredIndex.name === indexName)) return batch;
    }
    return peerIndexes.filter((configuredIndex) => configuredIndex.name === indexName);
  }

  private shouldBuildCompanionForSegment(
    stream: string,
    segmentIndex: number,
    companionPlanGeneration: number | null,
    configuredIndexes: SecondaryIndexField[],
    peerIndexes: SecondaryIndexField[],
    batchIndexes: SecondaryIndexField[]
  ): boolean {
    if (!this.unifiedCompanionSink || companionPlanGeneration == null) return false;
    const currentCompanion = this.db.getSearchSegmentCompanion(stream, segmentIndex);
    if (currentCompanion && currentCompanion.plan_generation === companionPlanGeneration) return false;
    const configuredOwnerBatch = configuredIndexes.filter((configuredIndex) =>
      (EVLOG_EXACT_COMPANION_OWNER_GROUP as readonly string[]).includes(configuredIndex.name)
    );
    if (configuredOwnerBatch.length > 0) {
      return configuredOwnerBatch.some((configuredIndex) => batchIndexes.some((entry) => entry.name === configuredIndex.name));
    }
    const fallbackOwnerName = peerIndexes.map((configuredIndex) => configuredIndex.name).sort((a, b) => a.localeCompare(b))[0];
    return batchIndexes.some((entry) => entry.name === fallbackOwnerName);
  }

  private shouldPublishManifestAfterL0Batch(
    configured: SecondaryIndexField[],
    batchIndexes: SecondaryIndexField[],
    stateByName: Map<string, NonNullable<ReturnType<SqliteDurableStore["getSecondaryIndexState"]>>>,
    nextIndexedThrough: number,
    shouldBuildCompanion: boolean
  ): boolean {
    if (shouldBuildCompanion) return true;
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
    if (this.span <= 0) return Result.ok(undefined);
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
        if (uploadedCount < indexedThrough + this.span) return Result.ok(undefined);
        const peerIndexes = configured.filter((configuredIndex) => stateByName.get(configuredIndex.name)?.indexed_through === indexedThrough);
        const batchIndexes =
          registry.search?.profile === "evlog"
            ? this.getEvlogPeerBatch(index.name, peerIndexes)
            : peerIndexes.slice(0, SECONDARY_L0_BATCH_MAX_FIELDS);
        if (batchIndexes.length === 0) return Result.ok(undefined);
        const start = indexedThrough;
        const end = start + this.span - 1;
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
        const currentCompanionPlan = this.getCurrentCompanionPlan(stream, registry);
        const shouldBuildCompanion = this.shouldBuildCompanionForSegment(
          stream,
          start,
          currentCompanionPlan?.generation ?? null,
          configured,
          peerIndexes,
          batchIndexes
        );
        const useUnifiedSearchBuild =
          this.searchSegmentBuilds != null && this.span === 1 && registry.search?.profile === "evlog";
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
                        plan: shouldBuildCompanion ? (currentCompanionPlan?.plan ?? null) : null,
                        planGeneration: shouldBuildCompanion ? (currentCompanionPlan?.generation ?? null) : null,
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
                        kind: "secondary_l0_build" as const,
                        output: {
                          runs: sharedRes.value.output.exactRuns.filter((entry) => batchIndexNames.has(entry.indexName)),
                          companion: sharedRes.value.output.companion,
                        },
                      });
                    }
                  )
                : (async () => {
                    const sharedRes = await this.searchSegmentBuilds!.buildSegmentResult({
                      stream,
                      registry,
                      exactIndexes: unifiedExactIndexes,
                      plan: shouldBuildCompanion ? (currentCompanionPlan?.plan ?? null) : null,
                      planGeneration: shouldBuildCompanion ? (currentCompanionPlan?.generation ?? null) : null,
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
                      kind: "secondary_l0_build" as const,
                      output: {
                        runs: sharedRes.value.output.exactRuns.filter((entry) => batchIndexNames.has(entry.indexName)),
                        companion: sharedRes.value.output.companion,
                      },
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
                        span: this.span,
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
                    span: this.span,
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
        if (runRes.value.kind !== "secondary_l0_build") {
          action.fail("unexpected worker result kind");
          return invalidIndexBuild("unexpected worker result kind");
        }
        const outputRuns = runRes.value.output.runs;
        const nextIndexedThrough = end + 1;
        const shouldPublishManifest = this.shouldPublishManifestAfterL0Batch(
          configured,
          batchIndexes,
          stateByName,
          nextIndexedThrough,
          shouldBuildCompanion
        );
        const unifiedCompanionOutput: NonNullable<SearchSegmentBuildOutput["companion"]> | null =
          useUnifiedSearchBuild && "companion" in runRes.value.output
            ? ((runRes.value.output.companion as SearchSegmentBuildOutput["companion"]) ?? null)
            : null;
        const companionStageStartedAt = Date.now();
        const companionStagePromise =
          useUnifiedSearchBuild &&
          shouldBuildCompanion &&
          this.unifiedCompanionSink &&
          currentCompanionPlan?.generation != null &&
          unifiedCompanionOutput
            ? this.unifiedCompanionSink(
                stream,
                segmentLeaseRes.value.localSegments[0]!.segmentIndex,
                currentCompanionPlan.generation,
                unifiedCompanionOutput
              )
            : Promise.resolve(Result.ok(null));
        const exactPersistStartedAt = Date.now();
        const [companionStageRes, uploadExactRes] = await Promise.all([
          companionStagePromise,
          this.uploadExactRunsResult(outputRuns),
        ]);
        piggybackCompanionPersistMs = Date.now() - companionStageStartedAt;
        if (Result.isError(companionStageRes)) {
          action.fail(companionStageRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
            },
          });
          return invalidIndexBuild(companionStageRes.error.message);
        }
        piggybackCompanionSectionKinds = companionStageRes.value?.sectionKinds ?? [];
        if (Result.isError(uploadExactRes)) {
          const exactPersistMs = Date.now() - exactPersistStartedAt;
          action.fail(uploadExactRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
              exact_persist_ms: exactPersistMs,
            },
          });
          return invalidIndexBuild(uploadExactRes.error.message);
        }
        const finalizeCompanionRes = companionStageRes.value?.finalize();
        if (finalizeCompanionRes && Result.isError(finalizeCompanionRes)) {
          const exactPersistMs = Date.now() - exactPersistStartedAt;
          action.fail(finalizeCompanionRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
              exact_persist_ms: exactPersistMs,
            },
          });
          return invalidIndexBuild(finalizeCompanionRes.error.message);
        }
        const persistRes = await this.finalizeExactRunsPersistResult(
          stream,
          uploadExactRes.value,
          batchIndexes,
          stateByName,
          nextIndexedThrough,
          shouldPublishManifest
        );
        const exactPersistMs = Date.now() - exactPersistStartedAt;
        if (Result.isError(persistRes)) {
          action.fail(persistRes.error.message, {
            detail: {
              build_ms: buildMs,
              piggyback_companion_persist_ms: piggybackCompanionPersistMs,
              exact_persist_ms: exactPersistMs,
            },
          });
          return invalidIndexBuild(persistRes.error.message);
        }
        action.succeed({
          outputCount: uploadExactRes.value.outputRuns.length,
          outputSizeBytes: uploadExactRes.value.outputSizeBytes,
          detail: {
            index_names: batchIndexes.map((entry) => entry.name),
            output_record_counts: uploadExactRes.value.outputRecordCounts,
            output_filter_lens: uploadExactRes.value.outputFilterLens,
            shared_build_cache_status: sharedBuildCacheStatus,
            piggyback_companion_section_kinds: piggybackCompanionSectionKinds,
            manifest_published: shouldPublishManifest,
            build_ms: buildMs,
            piggyback_companion_persist_ms: piggybackCompanionPersistMs,
            exact_persist_ms: exactPersistMs,
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
    if (this.span <= 0) return Result.ok(undefined);
    if (this.compactionFanout <= 1) return Result.ok(undefined);
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
          },
        });
        const runRes = await this.buildCompactedRunResult(stream, indexName, level + 1, runs);
        if (Result.isError(runRes)) {
          action.fail(runRes.error.message);
          return runRes;
        }
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
        const publishManifestStartedAt = Date.now();
        let publishManifestMs = 0;
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore and retry later
          }
          publishManifestMs = Date.now() - publishManifestStartedAt;
        }
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
            metadata_finalize_ms: metadataFinalizeMs,
            publish_manifest_ms: publishManifestMs,
            retired_gc_ms: retiredGcMs,
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
    nextIndexedThrough: number
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
      true
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
    shouldPublishManifest: boolean
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
    if (this.db.countUploadedSegments(stream) >= nextIndexedThrough + this.span) this.buildQueue.add(stream);
    this.compactionQueue.add(stream);
    return Result.ok(undefined);
  }

  private findCompactionGroup(stream: string, indexName: string): { level: number; runs: SecondaryIndexRunRow[] } | null {
    const runs = this.db.listSecondaryIndexRuns(stream, indexName);
    if (runs.length < this.compactionFanout) return null;
    const byLevel = new Map<number, SecondaryIndexRunRow[]>();
    for (const run of runs) {
      const arr = byLevel.get(run.level) ?? [];
      arr.push(run);
      byLevel.set(run.level, arr);
    }
    for (let level = 0; level <= this.maxLevel; level++) {
      const levelRuns = byLevel.get(level);
      if (!levelRuns || levelRuns.length < this.compactionFanout) continue;
      const span = this.levelSpan(level);
      for (let i = 0; i + this.compactionFanout <= levelRuns.length; i++) {
        const base = levelRuns[i].start_segment;
        let ok = true;
        for (let j = 0; j < this.compactionFanout; j++) {
          const run = levelRuns[i + j];
          const expectStart = base + j * span;
          if (run.start_segment !== expectStart || run.end_segment !== expectStart + span - 1) {
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
    indexName: string,
    level: number,
    inputs: SecondaryIndexRunRow[]
  ): Promise<
    Result<
      {
        run: SecondaryCompactionBuildOutput;
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
      if (buildRes.value.kind !== "secondary_compaction_build") return invalidIndexBuild("unexpected worker result kind");
      return Result.ok({
        run: buildRes.value.output,
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
        location: "cache_hit" | "cache_fill" | "temp_spill";
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
    try {
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
        if (this.os.putFile) {
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
      await retryAbortable(
        (signal) => this.os.put(meta.objectKey, payload, { contentLength: payload.byteLength, signal }),
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
      this.runDiskCache?.put(meta.object_key, bytes);
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
