import { randomBytes } from "node:crypto";
import { Result } from "better-result";
import type { Config } from "../config";
import type {
  SearchCompanionPlanRow,
  SearchSegmentCompanionRow,
  SegmentRow,
  SqliteDurableStore,
} from "../db/db";
import type { Metrics } from "../metrics";
import type { ObjectStore } from "../objectstore/interface";
import { SchemaRegistryStore, type SchemaRegistry, type SearchFieldConfig } from "../schema/registry";
import { SegmentDiskCache } from "../segment/cache";
import { loadSegmentBytesCached } from "../segment/cached_segment";
import { iterateBlockRecordsResult } from "../segment/format";
import { dsError } from "../util/ds_error.ts";
import { RuntimeMemorySampler } from "../runtime_memory_sampler";
import { ConcurrencyGate } from "../concurrency_gate";
import type { ForegroundActivityTracker } from "../foreground_activity";
import { retry, retryAbortable } from "../util/retry";
import { yieldToEventLoop } from "../util/yield";
import { searchCompanionObjectKey, streamHash16Hex } from "../util/stream_paths";
import { beginAsyncIndexAction } from "../index/async_index_actions";
import { buildDesiredSearchCompanionPlan, hashSearchCompanionPlan, type SearchCompanionPlan } from "./companion_plan";
import {
  PSCIX2_MAX_TOC_BYTES,
  decodeCompanionSectionPayloadResult,
  decodeBundledSegmentCompanionResult,
  decodeBundledSegmentCompanionTocResult,
  encodeBundledSegmentCompanionFromPayloads,
  encodeCompanionSectionPayload,
  type BundledSegmentCompanion,
  type CompanionSectionKind,
  type CompanionSectionInputMap,
  type CompanionSectionMap,
  type CompanionToc,
  type EncodedCompanionSectionPayload,
} from "./companion_format";
import { CompanionFileCache } from "./companion_file_cache";
import type { ColFieldInput, ColScalar, ColSectionInput, ColSectionView } from "./col_format";
import {
  analyzeTextValue,
  canonicalizeColumnValue,
  extractRawSearchValuesForFieldsResult,
  normalizeKeywordValue,
} from "./schema";
import type { FtsFieldInput, FtsSectionInput, FtsSectionView, FtsTermInput } from "./fts_format";
import { buildMetricsBlockRecord } from "../profiles/metrics/normalize";
import type { MetricsBlockSectionInput, MetricsBlockSectionView } from "../profiles/metrics/block_format";
import { parseDurationMsResult } from "../util/duration";
import {
  cloneAggMeasureState,
  extractRollupContributionResult,
  mergeAggMeasureState,
  rollupRequiredFieldNames,
} from "./aggregate";
import type { AggMeasureState, AggSectionInput, AggWindowGroup, AggSectionView } from "./agg_format";
import type { SearchRollupConfig } from "../schema/registry";
import type { CompanionSectionLookupStats } from "../index/indexer";
import type { IndexSegmentLocalityManager } from "../index/segment_locality";
import { IndexBuildWorkerPool } from "../index/index_build_worker_pool";

type CompanionBuildError = { kind: "invalid_companion_build"; message: string };

function invalidCompanionBuild<T = never>(message: string): Result<T, CompanionBuildError> {
  return Result.err({ kind: "invalid_companion_build", message });
}

type ColumnFieldBuilder = {
  config: SearchFieldConfig;
  kind: ColFieldInput["kind"];
  docIds: number[];
  values: ColScalar[];
  invalid: boolean;
};

type FtsFieldBuilder = {
  config: SearchFieldConfig;
  companion: FtsFieldInput;
};

type GroupBuilder = {
  key: string;
  measures: Record<string, AggMeasureState>;
};

type MetricsBlockBuilder = {
  records: MetricsBlockSectionInput["records"];
  minWindowStartMs: number | undefined;
  maxWindowEndMs: number | undefined;
};

type AggRollupBuilder = {
  rollup: SearchRollupConfig;
  intervalsMs: number[];
  intervalMap: Map<number, Map<number, Map<string, GroupBuilder>>>;
  dimensionNames: string[];
  fieldNames: string[];
};

type CompanionBuildProgress = {
  docCount: number;
  colFields: number;
  colValues: number;
  ftsFields: number;
  ftsTerms: number;
  ftsPostings: number;
  ftsPositions: number;
  aggRollups: number;
  aggWindows: number;
  aggGroups: number;
  metricRecords: number;
};

const PAYLOAD_DECODER = new TextDecoder();

function compareValues(left: bigint | number | boolean, right: bigint | number | boolean): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  return String(left).localeCompare(String(right));
}

const AGG_DIMENSION_SEPARATOR = "\u001f";
const AGG_DIMENSION_NULL = "\u0000";

function encodeAggDimensionPart(value: string | null): string {
  if (value == null) return AGG_DIMENSION_NULL;
  return value.replaceAll(AGG_DIMENSION_SEPARATOR, `${AGG_DIMENSION_SEPARATOR}${AGG_DIMENSION_SEPARATOR}`);
}

function decodeAggDimensionPart(value: string): string | null {
  if (value === AGG_DIMENSION_NULL) return null;
  return value.replaceAll(`${AGG_DIMENSION_SEPARATOR}${AGG_DIMENSION_SEPARATOR}`, AGG_DIMENSION_SEPARATOR);
}

function encodeAggGroupKey(dimensions: Record<string, string | null>, dimensionNames: string[]): string {
  return dimensionNames.map((name) => encodeAggDimensionPart(dimensions[name] ?? null)).join(AGG_DIMENSION_SEPARATOR);
}

function decodeAggGroupKey(groupKey: string, dimensionNames: string[]): Record<string, string | null> {
  const parts: string[] = [];
  let current = "";
  for (let index = 0; index < groupKey.length; index++) {
    const char = groupKey[index]!;
    if (char !== AGG_DIMENSION_SEPARATOR) {
      current += char;
      continue;
    }
    const next = groupKey[index + 1];
    if (next === AGG_DIMENSION_SEPARATOR) {
      current += AGG_DIMENSION_SEPARATOR;
      index += 1;
      continue;
    }
    parts.push(current);
    current = "";
  }
  parts.push(current);
  const decoded: Record<string, string | null> = {};
  for (let index = 0; index < dimensionNames.length; index++) {
    decoded[dimensionNames[index]!] = decodeAggDimensionPart(parts[index] ?? AGG_DIMENSION_NULL);
  }
  return decoded;
}

function parseSectionKinds(row: SearchSegmentCompanionRow): Set<CompanionSectionKind> {
  try {
    const parsed = JSON.parse(row.sections_json);
    if (!Array.isArray(parsed)) return new Set();
    return new Set(
      parsed.filter((value): value is CompanionSectionKind => value === "col" || value === "fts" || value === "agg" || value === "mblk")
    );
  } catch {
    return new Set();
  }
}

export class SearchCompanionManager {
  private readonly queue = new Set<string>();
  private readonly building = new Set<string>();
  private readonly fileCache: CompanionFileCache;
  private readonly segmentCache?: SegmentDiskCache;
  private readonly yieldBlocks: number;
  private readonly memorySampler?: RuntimeMemorySampler;
  private readonly asyncGate: ConcurrencyGate;
  private readonly foregroundActivity?: ForegroundActivityTracker;
  private runningBuildTick = false;
  private readonly segmentLocality?: IndexSegmentLocalityManager;
  private readonly buildWorkers: IndexBuildWorkerPool;
  private readonly ownsBuildWorkers: boolean;

  constructor(
    private readonly cfg: Config,
    private readonly db: SqliteDurableStore,
    private readonly os: ObjectStore,
    private readonly registry: SchemaRegistryStore,
    segmentCache?: SegmentDiskCache,
    private readonly publishManifest?: (stream: string) => Promise<void>,
    private readonly onMetadataChanged?: (stream: string) => void,
    private readonly metrics?: Metrics,
    memorySampler?: RuntimeMemorySampler,
    asyncGate?: ConcurrencyGate,
    foregroundActivity?: ForegroundActivityTracker,
    segmentLocality?: IndexSegmentLocalityManager,
    buildWorkers?: IndexBuildWorkerPool
  ) {
    this.yieldBlocks = Math.max(1, cfg.searchCompanionYieldBlocks);
    this.segmentCache = segmentCache;
    this.memorySampler = memorySampler;
    this.asyncGate = asyncGate ?? new ConcurrencyGate(1);
    this.foregroundActivity = foregroundActivity;
    this.segmentLocality = segmentLocality;
    this.ownsBuildWorkers = !buildWorkers;
    this.buildWorkers = buildWorkers ?? new IndexBuildWorkerPool(Math.max(1, cfg.indexBuilders));
    if (this.ownsBuildWorkers) this.buildWorkers.start();
    this.fileCache = new CompanionFileCache(
      `${cfg.rootDir}/cache/companions`,
      cfg.searchCompanionFileCacheMaxBytes,
      cfg.searchCompanionFileCacheMaxAgeMs,
      cfg.searchCompanionMappedCacheEntries
    );
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
    if (this.ownsBuildWorkers) this.buildWorkers.stop();
    this.fileCache.clearMapped();
  }

  enqueue(stream: string): void {
    this.queue.add(stream);
  }

  async getColSegmentCompanion(stream: string, segmentIndex: number): Promise<ColSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "col")) ?? null;
  }

  async getFtsSegmentCompanion(stream: string, segmentIndex: number): Promise<FtsSectionView | null> {
    return (await this.getFtsSegmentCompanionWithStats(stream, segmentIndex)).companion;
  }

  async getFtsSegmentCompanionWithStats(
    stream: string,
    segmentIndex: number
  ): Promise<{ companion: FtsSectionView | null; stats: CompanionSectionLookupStats }> {
    const result = await this.getSectionCompanionWithStats(stream, segmentIndex, "fts");
    return { companion: result.companion ?? null, stats: result.stats };
  }

  async getAggSegmentCompanion(stream: string, segmentIndex: number): Promise<AggSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "agg")) ?? null;
  }

  async getMetricsBlockSegmentCompanion(stream: string, segmentIndex: number): Promise<MetricsBlockSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "mblk")) ?? null;
  }

  getLocalCacheBytes(stream: string): number {
    return this.fileCache.bytesForObjectKeyPrefix(`streams/${streamHash16Hex(stream)}/segments/`);
  }

  getMemoryStats(): {
    fileCacheBytes: number;
    fileCacheEntries: number;
    mappedFileBytes: number;
    mappedFileEntries: number;
    pinnedFileEntries: number;
  } {
    const stats = this.fileCache.stats();
    return {
      fileCacheBytes: stats.usedBytes,
      fileCacheEntries: stats.entryCount,
      mappedFileBytes: stats.mappedBytes,
      mappedFileEntries: stats.mappedEntryCount,
      pinnedFileEntries: stats.pinnedEntryCount,
    };
  }

  private async getSectionCompanion<K extends CompanionSectionKind>(
    stream: string,
    segmentIndex: number,
    kind: K
  ): Promise<CompanionSectionMap[K] | null> {
    return (await this.getSectionCompanionWithStats(stream, segmentIndex, kind)).companion;
  }

  private async getSectionCompanionWithStats<K extends CompanionSectionKind>(
    stream: string,
    segmentIndex: number,
    kind: K
  ): Promise<{ companion: CompanionSectionMap[K] | null; stats: CompanionSectionLookupStats }> {
    const leave = this.memorySampler?.enter("companion_read", { stream, segment_index: segmentIndex, kind });
    try {
      let sectionGetMs = 0;
      let decodeMs = 0;
      const planRow = this.getCurrentPlanRow(stream);
      if (!planRow) return { companion: null, stats: { sectionGetMs, decodeMs } };
      const row = this.db.getSearchSegmentCompanion(stream, segmentIndex);
      if (!row || row.plan_generation !== planRow.generation) return { companion: null, stats: { sectionGetMs, decodeMs } };
      if (!parseSectionKinds(row).has(kind)) return { companion: null, stats: { sectionGetMs, decodeMs } };
      const sectionStartedAt = Date.now();
      const bundle = await this.loadBundleResult(row);
      if (Result.isError(bundle)) throw dsError(bundle.error.message);
      const plan = this.parsePlanRowResult(planRow);
      if (Result.isError(plan)) throw dsError(plan.error.message);
      const sectionBytes = this.sectionPayloadResult(bundle.value.bytes, bundle.value.toc, row.object_key, kind);
      if (Result.isError(sectionBytes)) throw dsError(sectionBytes.error.message);
      sectionGetMs = Date.now() - sectionStartedAt;
      const decodeStartedAt = Date.now();
      const decoded = decodeCompanionSectionPayloadResult(kind, sectionBytes.value, plan.value);
      if (Result.isError(decoded)) throw dsError(decoded.error.message);
      decodeMs = Date.now() - decodeStartedAt;
      return { companion: decoded.value ?? null, stats: { sectionGetMs, decodeMs } };
    } finally {
      leave?.();
    }
  }

  private getCurrentPlanRow(stream: string): SearchCompanionPlanRow | null {
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return null;
    const desiredPlan = buildDesiredSearchCompanionPlan(regRes.value);
    const desiredHash = hashSearchCompanionPlan(desiredPlan);
    const current = this.db.getSearchCompanionPlan(stream);
    if (current && current.plan_hash === desiredHash) return current;
    return null;
  }

  private parsePlanRowResult(planRow: SearchCompanionPlanRow): Result<SearchCompanionPlan, CompanionBuildError> {
    try {
      const parsed = JSON.parse(planRow.plan_json) as SearchCompanionPlan;
      if (!parsed || !parsed.families || !Array.isArray(parsed.fields) || !Array.isArray(parsed.rollups)) {
        return invalidCompanionBuild("invalid bundled companion plan json");
      }
      return Result.ok(parsed);
    } catch (e: unknown) {
      return invalidCompanionBuild(String((e as any)?.message ?? e));
    }
  }

  private async loadBundleResult(
    row: SearchSegmentCompanionRow
  ): Promise<Result<{ bytes: Uint8Array; toc: CompanionToc }, CompanionBuildError>> {
    if (row.size_bytes <= 0) return invalidCompanionBuild(`invalid .cix size for ${row.object_key}`);
    const bundleRes = await this.fileCache.loadMappedBundleResult({
      objectKey: row.object_key,
      expectedSize: row.size_bytes,
      loadBytes: async () =>
        retry(
          async () => {
            const data = await this.os.get(row.object_key);
            if (!data) throw dsError(`missing .cix object ${row.object_key}`);
            return data;
          },
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        ),
      decodeToc: (bytes) => {
        const tocRes = decodeBundledSegmentCompanionTocResult(bytes.subarray(0, Math.min(bytes.byteLength, PSCIX2_MAX_TOC_BYTES)));
        if (Result.isError(tocRes)) return Result.err({ message: tocRes.error.message });
        return Result.ok(tocRes.value);
      },
    });
    if (Result.isError(bundleRes)) return invalidCompanionBuild(bundleRes.error.message);
    return Result.ok({ bytes: bundleRes.value.bytes, toc: bundleRes.value.toc });
  }

  private sectionPayloadResult(
    bytes: Uint8Array,
    toc: CompanionToc,
    objectKey: string,
    kind: CompanionSectionKind
  ): Result<Uint8Array, CompanionBuildError> {
    const section = toc.sections.find((entry) => entry.kind === kind);
    if (!section) return invalidCompanionBuild(`missing ${kind} section in ${objectKey}`);
    if (section.offset < 0 || section.length < 0 || section.offset + section.length > bytes.byteLength) {
      return invalidCompanionBuild(`invalid ${kind} section bounds in ${objectKey}`);
    }
    return Result.ok(bytes.subarray(section.offset, section.offset + section.length));
  }

  async tick(): Promise<void> {
    if (this.metrics) {
      this.metrics.record("tieredstore.companion.build.queue_len", this.queue.size, "count");
      this.metrics.record("tieredstore.companion.builds_inflight", this.building.size, "count");
    }
    await this.runOneBuildTask();
  }

  async runOneBuildTask(): Promise<boolean> {
    if (this.runningBuildTick) return false;
    const stream = this.takeNextStream();
    if (!stream) return false;
    this.runningBuildTick = true;
    try {
      try {
        const buildRes = await this.buildPendingSegmentsResult(stream);
        if (Result.isError(buildRes)) {
          console.error("bundled companion build failed", stream, buildRes.error.message);
          this.queue.add(stream);
        }
      } catch (e: unknown) {
        console.error("bundled companion tick failed", stream, e);
        this.queue.add(stream);
      }
      return true;
    } finally {
      this.runningBuildTick = false;
    }
  }

  private takeNextStream(): string | null {
    const streams = new Set([...this.db.listSearchCompanionPlanStreams(), ...this.queue]);
    const next = streams.values().next();
    if (next.done) return null;
    const stream = next.value as string;
    this.queue.delete(stream);
    return stream;
  }

  private async buildPendingSegmentsResult(stream: string): Promise<Result<void, CompanionBuildError>> {
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return invalidCompanionBuild(regRes.error.message);
      const desiredPlan = buildDesiredSearchCompanionPlan(regRes.value);
      const desiredHash = hashSearchCompanionPlan(desiredPlan);
      const wantedFamilies = Object.values(desiredPlan.families).some(Boolean);
      let planRow = this.db.getSearchCompanionPlan(stream);
      if (!wantedFamilies) {
        if (planRow) {
          this.db.deleteSearchSegmentCompanions(stream);
          this.db.deleteSearchCompanionPlan(stream);
          this.onMetadataChanged?.(stream);
          if (this.publishManifest) {
            try {
              await this.publishManifest(stream);
            } catch {
              // background loop will retry
            }
          }
        }
        return Result.ok(undefined);
      }
      if (!planRow) {
        this.db.upsertSearchCompanionPlan(stream, 1, desiredHash, JSON.stringify(desiredPlan));
        planRow = this.db.getSearchCompanionPlan(stream);
      } else if (planRow.plan_hash !== desiredHash) {
        this.db.upsertSearchCompanionPlan(stream, planRow.generation + 1, desiredHash, JSON.stringify(desiredPlan));
        planRow = this.db.getSearchCompanionPlan(stream);
      }
      if (!planRow) return Result.ok(undefined);

      const uploadedSegments = this.db.countUploadedSegments(stream);
      const stale: number[] = [];
      for (let segmentIndex = 0; segmentIndex < uploadedSegments; segmentIndex++) {
        const current = this.db.getSearchSegmentCompanion(stream, segmentIndex);
        if (!current || current.plan_generation !== planRow.generation) stale.push(segmentIndex);
      }
      if (this.metrics) {
        this.metrics.record("tieredstore.companion.lag.segments", stale.length, "count", undefined, stream);
      }
      if (stale.length === 0) return Result.ok(undefined);

      const batchLimit = Math.max(1, this.cfg.searchCompanionBuildBatchSegments);
      const batch = stale.slice(0, batchLimit);
      let builtCount = 0;
      for (const nextSegmentIndex of batch) {
        const seg = this.db.getSegmentByIndex(stream, nextSegmentIndex);
        if (!seg || !seg.r2_etag) continue;
        const startedAt = Date.now();
        const segmentLeaseRes = this.segmentLocality
          ? await this.segmentLocality.acquireWindowResult(stream, [seg], "search companion indexing")
          : Result.err({ kind: "missing_segment" as const, message: "search companion indexing requires a segment disk cache" });
        if (Result.isError(segmentLeaseRes)) {
          if (segmentLeaseRes.error.kind === "index_cache_overloaded") {
            this.queue.add(stream);
            this.onMetadataChanged?.(stream);
            return Result.ok(undefined);
          }
          return invalidCompanionBuild(segmentLeaseRes.error.message);
        }
        const action = beginAsyncIndexAction(this.db, {
          stream,
          actionKind: "companion_build",
          targetKind: "bundled_companion",
          inputKind: "segment",
          inputCount: 1,
          inputSizeBytes: BigInt(seg.size_bytes),
          startSegment: seg.segment_index,
          endSegment: seg.segment_index,
          detail: {
            input_payload_bytes: Number(seg.payload_bytes),
            plan_generation: planRow.generation,
          },
        });
        let companionRes;
        try {
          companionRes = await this.asyncGate.run(async () =>
            this.memorySampler
              ? await this.memorySampler.track(
                  "companion",
                  { stream, segment_index: seg.segment_index, plan_generation: planRow.generation },
                  () =>
                    this.buildWorkers.buildResult({
                      kind: "companion_build",
                      input: {
                        registry: regRes.value,
                        plan: desiredPlan,
                        planGeneration: planRow.generation,
                        segment: {
                          stream,
                          segmentIndex: seg.segment_index,
                          startOffset: seg.start_offset,
                          localPath: segmentLeaseRes.value.localSegments[0]!.localPath,
                        },
                      },
                    })
                )
              : await this.buildWorkers.buildResult({
                  kind: "companion_build",
                  input: {
                    registry: regRes.value,
                    plan: desiredPlan,
                    planGeneration: planRow.generation,
                    segment: {
                      stream,
                      segmentIndex: seg.segment_index,
                      startOffset: seg.start_offset,
                      localPath: segmentLeaseRes.value.localSegments[0]!.localPath,
                    },
                  },
                })
          );
        } finally {
          segmentLeaseRes.value.release();
        }
        if (Result.isError(companionRes)) {
          action.fail(companionRes.error.message);
          return invalidCompanionBuild(companionRes.error.message);
        }
        if (companionRes.value.kind !== "companion_build") {
          action.fail("unexpected worker result kind");
          return invalidCompanionBuild("unexpected worker result kind");
        }
        const objectId = Buffer.from(randomBytes(8)).toString("hex");
        const objectKey = searchCompanionObjectKey(streamHash16Hex(stream), seg.segment_index, objectId);
        const payload = companionRes.value.output.payload;
        const sectionSizes = companionRes.value.output.sectionSizes;
        try {
          await retryAbortable(
            (signal) => this.os.put(objectKey, payload, { contentLength: payload.byteLength, signal }),
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
        } catch (e: unknown) {
          action.fail(String((e as any)?.message ?? e), {
            detail: {
              section_kinds: companionRes.value.output.sectionKinds,
            },
          });
          return invalidCompanionBuild(String((e as any)?.message ?? e));
        }
        const cacheRes = this.fileCache.storeBytesResult(objectKey, payload);
        if (Result.isError(cacheRes)) {
          console.warn("bundled companion local cache populate failed", objectKey, cacheRes.error.message);
        }
        const sectionKinds = companionRes.value.output.sectionKinds;
        this.db.upsertSearchSegmentCompanion(
          stream,
          seg.segment_index,
          objectKey,
          planRow.generation,
          JSON.stringify(sectionKinds),
          JSON.stringify(sectionSizes),
          payload.byteLength,
          companionRes.value.output.primaryTimestampMinMs,
          companionRes.value.output.primaryTimestampMaxMs
        );
        builtCount += 1;
        if (this.metrics) {
          const elapsedNs = BigInt(Date.now() - startedAt) * 1_000_000n;
          this.metrics.record("tieredstore.companion.build.latency", Number(elapsedNs), "ns", undefined, stream);
          this.metrics.record("tieredstore.companion.objects.built", 1, "count", undefined, stream);
        }
        action.succeed({
          outputCount: 1,
          outputSizeBytes: BigInt(payload.byteLength),
          detail: {
            section_kinds: sectionKinds,
          },
        });
      }

      if (stale.length > builtCount) this.queue.add(stream);
      if (builtCount === 0) return Result.ok(undefined);

      this.onMetadataChanged?.(stream);
      if (this.publishManifest) {
        try {
          await this.publishManifest(stream);
        } catch (e: unknown) {
          console.error("bundled companion manifest publish failed", stream, e);
          // background loop will retry
        }
      }
      return Result.ok(undefined);
    } finally {
      this.building.delete(stream);
    }
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, CompanionBuildError>> {
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
    } catch (e: unknown) {
      return invalidCompanionBuild(String((e as any)?.message ?? e));
    }
  }

  private async visitParsedSegmentRecordsResult(
    segmentBytes: Uint8Array,
    seg: SegmentRow,
    visit: (args: {
      docCount: number;
      offset: bigint;
      parsed: unknown | null;
      parsedOk: boolean;
    }) => Promise<Result<void, CompanionBuildError>>
  ): Promise<Result<number, CompanionBuildError>> {
    let docCount = 0;
    let offset = seg.start_offset;
    let processedBlocks = 0;
    let lastBlockOffset = -1;
    for (const recRes of iterateBlockRecordsResult(segmentBytes)) {
      if (Result.isError(recRes)) return invalidCompanionBuild(recRes.error.message);
      const rec = recRes.value;
      if (rec.blockOffset !== lastBlockOffset) {
        processedBlocks += 1;
        lastBlockOffset = rec.blockOffset;
        if (processedBlocks % this.yieldBlocks === 0) await this.yieldBackgroundWork();
      }
      let parsed: unknown = null;
      let parsedOk = false;
      try {
        parsed = JSON.parse(PAYLOAD_DECODER.decode(rec.payload));
        parsedOk = true;
      } catch {
        parsed = null;
      }
      const visitRes = await visit({ docCount, offset, parsed, parsedOk });
      if (Result.isError(visitRes)) return visitRes;
      offset += 1n;
      docCount += 1;
    }
    return Result.ok(docCount);
  }

  private async buildEncodedBundledCompanionResult(
    registry: SchemaRegistry,
    plan: SearchCompanionPlan,
    planGeneration: number,
    seg: SegmentRow
  ): Promise<
    Result<
      {
        payload: Uint8Array;
        sectionKinds: CompanionSectionKind[];
        sectionSizes: Record<string, number>;
        primaryTimestampMinMs: bigint | null;
        primaryTimestampMaxMs: bigint | null;
      },
      CompanionBuildError
    >
  > {
    const leaveLoad = this.memorySampler?.enter("companion_load_segment", {
      stream: seg.stream,
      segment_index: seg.segment_index,
    });
    const bytesRes = await this.loadSegmentBytesResult(seg);
    leaveLoad?.();
    if (Result.isError(bytesRes)) return bytesRes;
    const segmentBytes = bytesRes.value;
    const colBuilders = plan.families.col ? this.createColBuilders(registry) : new Map<string, ColumnFieldBuilder>();
    const ftsBuilders = plan.families.fts ? this.createFtsBuilders(registry) : new Map<string, FtsFieldBuilder>();
    const aggBuildersRes = plan.families.agg ? this.createAggRollupBuildersResult(registry) : Result.ok(new Map<string, AggRollupBuilder>());
    if (Result.isError(aggBuildersRes)) return aggBuildersRes;
    const aggBuilders = aggBuildersRes.value;
    const metricsBuilder: MetricsBlockBuilder | null = plan.families.mblk
      ? { records: [], minWindowStartMs: undefined, maxWindowEndMs: undefined }
      : null;
    const requiredFieldNames = new Set<string>();
    for (const fieldName of colBuilders.keys()) requiredFieldNames.add(fieldName);
    for (const fieldName of ftsBuilders.keys()) requiredFieldNames.add(fieldName);
    for (const builder of aggBuilders.values()) {
      for (const fieldName of builder.fieldNames) requiredFieldNames.add(fieldName);
    }
    const fieldNameList = Array.from(requiredFieldNames).sort((a, b) => a.localeCompare(b));
    const leaveScan = this.memorySampler?.enter("companion_scan_records", {
      stream: seg.stream,
      segment_index: seg.segment_index,
    });
    const docCountRes = await this.visitParsedSegmentRecordsResult(segmentBytes, seg, async ({ docCount, offset, parsed, parsedOk }) => {
      let rawSearchValues: Map<string, unknown[]> | null = null;
      if (parsedOk && fieldNameList.length > 0) {
        const leaveExtract = this.memorySampler?.enter("companion_extract_raw", { doc_count: docCount });
        const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, offset, parsed, fieldNameList);
        leaveExtract?.();
        if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
        rawSearchValues = rawValuesRes.value;
      }
      if (rawSearchValues) {
        const leaveCol = this.memorySampler?.enter("companion_record_col", { doc_count: docCount });
        this.recordColBuilders(colBuilders, rawSearchValues, docCount);
        leaveCol?.();
        const leaveFts = this.memorySampler?.enter("companion_record_fts", { doc_count: docCount });
        this.recordFtsBuilders(ftsBuilders, rawSearchValues, docCount);
        leaveFts?.();
      }
      if (parsedOk && rawSearchValues) {
        const leaveAgg = this.memorySampler?.enter("companion_record_agg", { doc_count: docCount });
        for (const builder of aggBuilders.values()) {
          const contributionRes = extractRollupContributionResult(registry, builder.rollup, offset, parsed, rawSearchValues);
          if (Result.isError(contributionRes)) {
            leaveAgg?.();
            return invalidCompanionBuild(contributionRes.error.message);
          }
          if (!contributionRes.value) continue;
          const recordRes = this.recordAggContributionResult(builder, contributionRes.value);
          if (Result.isError(recordRes)) {
            leaveAgg?.();
            return recordRes;
          }
        }
        leaveAgg?.();
      }
      if (metricsBuilder && parsedOk) {
        const leaveMetrics = this.memorySampler?.enter("companion_record_mblk", { doc_count: docCount });
        this.recordMetricsBlockBuilder(metricsBuilder, parsed, docCount);
        leaveMetrics?.();
      }
      if (this.memorySampler && (docCount + 1) % 1024 === 0) {
        this.memorySampler.capture("companion_progress", {
          stream: seg.stream,
          segment_index: seg.segment_index,
          ...this.summarizeCompanionBuildProgress(colBuilders, ftsBuilders, aggBuilders, metricsBuilder, docCount + 1),
        });
      }
      return Result.ok(undefined);
    });
    leaveScan?.();
    if (Result.isError(docCountRes)) return docCountRes;

    const sectionPayloads: EncodedCompanionSectionPayload[] = [];
    const sectionKinds: CompanionSectionKind[] = [];
    const sectionSizes: Record<string, number> = {};
    let primaryTimestampMinMs: bigint | null = null;
    let primaryTimestampMaxMs: bigint | null = null;
    const addSection = (payload: EncodedCompanionSectionPayload): void => {
      sectionPayloads.push(payload);
      const kind = payload.kind;
      sectionKinds.push(kind);
      sectionSizes[kind] = payload.payload.byteLength;
    };

    if (plan.families.col) {
      const leaveColEncode = this.memorySampler?.enter("companion_encode_col", {
        stream: seg.stream,
        segment_index: seg.segment_index,
        doc_count: docCountRes.value,
      });
      const colSection = this.finalizeColSection(registry, colBuilders, docCountRes.value);
      const primaryTimestampField = colSection.primary_timestamp_field;
      const primaryTimestampColumn = primaryTimestampField ? colSection.fields[primaryTimestampField] : undefined;
      primaryTimestampMinMs = typeof primaryTimestampColumn?.min === "bigint" ? primaryTimestampColumn.min : null;
      primaryTimestampMaxMs = typeof primaryTimestampColumn?.max === "bigint" ? primaryTimestampColumn.max : null;
      addSection(encodeCompanionSectionPayload("col", colSection, plan));
      colBuilders.clear();
      leaveColEncode?.();
    }
    if (plan.families.fts) {
      const leaveFtsEncode = this.memorySampler?.enter("companion_encode_fts", {
        stream: seg.stream,
        segment_index: seg.segment_index,
        doc_count: docCountRes.value,
      });
      addSection(encodeCompanionSectionPayload("fts", this.finalizeFtsSection(ftsBuilders, docCountRes.value), plan));
      ftsBuilders.clear();
      leaveFtsEncode?.();
    }
    if (plan.families.agg) {
      const leaveAggEncode = this.memorySampler?.enter("companion_encode_agg", {
        stream: seg.stream,
        segment_index: seg.segment_index,
      });
      addSection(encodeCompanionSectionPayload("agg", this.finalizeAggSection(aggBuilders), plan));
      aggBuilders.clear();
      leaveAggEncode?.();
    }
    if (plan.families.mblk && metricsBuilder) {
      const leaveMetricsEncode = this.memorySampler?.enter("companion_encode_mblk", {
        stream: seg.stream,
        segment_index: seg.segment_index,
      });
      addSection(encodeCompanionSectionPayload("mblk", this.finalizeMetricsBlockSection(metricsBuilder), plan));
      metricsBuilder.records.length = 0;
      leaveMetricsEncode?.();
    }

    return Result.ok({
      payload: encodeBundledSegmentCompanionFromPayloads({
        stream: seg.stream,
        segment_index: seg.segment_index,
        plan_generation: planGeneration,
        sections: sectionPayloads,
      }),
      sectionKinds,
      sectionSizes,
      primaryTimestampMinMs,
      primaryTimestampMaxMs,
    });
  }

  private async buildBundledCompanionResult(
    registry: SchemaRegistry,
    plan: SearchCompanionPlan,
    planGeneration: number,
    seg: SegmentRow
  ): Promise<Result<BundledSegmentCompanion, CompanionBuildError>> {
    const encodedRes = await this.buildEncodedBundledCompanionResult(registry, plan, planGeneration, seg);
    if (Result.isError(encodedRes)) return encodedRes;
    const decodedRes = decodeBundledSegmentCompanionResult(encodedRes.value.payload, plan);
    if (Result.isError(decodedRes)) return invalidCompanionBuild(decodedRes.error.message);
    return Result.ok(decodedRes.value);
  }

  private createColBuilders(registry: SchemaRegistry): Map<string, ColumnFieldBuilder> {
    const columnFields = Object.entries(registry.search?.fields ?? {}).filter(([, field]) => field.column === true);
    const builders = new Map<string, ColumnFieldBuilder>();
    for (const [fieldName, field] of columnFields) {
      builders.set(fieldName, { config: field, kind: field.kind, docIds: [], values: [], invalid: false });
    }
    return builders;
  }

  private recordColBuilders(builders: Map<string, ColumnFieldBuilder>, rawSearchValues: Map<string, unknown[]>, docCount: number): void {
    for (const [fieldName, builder] of builders) {
      if (builder.invalid) continue;
      const rawValues = rawSearchValues.get(fieldName) ?? [];
      const colValues: Array<bigint | number | boolean> = [];
      for (const rawValue of rawValues) {
        const normalized = canonicalizeColumnValue(builder.config, rawValue);
        if (normalized != null) colValues.push(normalized);
      }
      if (colValues.length > 1) {
        builder.invalid = true;
        continue;
      }
      if (colValues.length === 1) {
        builder.docIds.push(docCount);
        builder.values.push(colValues[0]!);
      }
    }
  }

  private finalizeColSection(
    registry: SchemaRegistry,
    builders: Map<string, ColumnFieldBuilder>,
    docCount: number
  ): ColSectionInput {
    const fields: Record<string, ColFieldInput> = {};
    const primaryTimestampField = registry.search?.primaryTimestampField;
    for (const [fieldName, builder] of builders) {
      if (builder.invalid) continue;
      let minValue: bigint | number | boolean | null = null;
      let maxValue: bigint | number | boolean | null = null;
      for (const value of builder.values) {
        if (minValue == null || compareValues(value, minValue) < 0) minValue = value;
        if (maxValue == null || compareValues(value, maxValue) > 0) maxValue = value;
      }
      if (builder.values.length === 0) continue;
      fields[fieldName] = {
        kind: builder.kind,
        doc_ids: builder.docIds,
        values: builder.values,
        min: minValue,
        max: maxValue,
      };
    }

    return {
      doc_count: docCount,
      primary_timestamp_field: primaryTimestampField ?? undefined,
      fields,
    };
  }

  private createFtsFieldBuilder(field: SearchFieldConfig): FtsFieldBuilder {
    return {
      config: field,
      companion: {
        kind: field.kind,
        exact: field.exact === true ? true : undefined,
        prefix: field.prefix === true ? true : undefined,
        positions: field.positions === true ? true : undefined,
        exists_docs: [],
        terms: Object.create(null) as Record<string, FtsTermInput>,
      },
    };
  }

  private createFtsBuilders(registry: SchemaRegistry): Map<string, FtsFieldBuilder> {
    const builders = new Map<string, FtsFieldBuilder>();
    for (const [fieldName, field] of Object.entries(registry.search?.fields ?? {}).sort((a, b) => a[0].localeCompare(b[0]))) {
      if (field.kind !== "text" && !(field.kind === "keyword" && field.prefix === true)) continue;
      builders.set(fieldName, this.createFtsFieldBuilder(field));
    }
    return builders;
  }

  private recordFtsBuilders(builders: Map<string, FtsFieldBuilder>, rawSearchValues: Map<string, unknown[]>, docCount: number): void {
    for (const [fieldName, builder] of builders) {
      const fieldCompanion = builder.companion;
      const textValues: string[] = [];
      for (const rawValue of rawSearchValues.get(fieldName) ?? []) {
        if (builder.config.kind === "keyword") {
          const normalized = normalizeKeywordValue(rawValue, builder.config.normalizer);
          if (normalized != null) textValues.push(normalized);
        } else if (builder.config.kind === "text" && typeof rawValue === "string") {
          textValues.push(rawValue);
        }
      }
      if (textValues.length === 0) continue;
      fieldCompanion.exists_docs.push(docCount);
      if (builder.config.kind === "keyword") {
        for (const value of textValues) {
          const postings = fieldCompanion.terms[value] ?? { doc_ids: [] };
          const docIds = postings.doc_ids;
          if (docIds.length === 0 || docIds[docIds.length - 1] !== docCount) docIds.push(docCount);
          fieldCompanion.terms[value] = postings;
        }
        continue;
      }
      let position = 0;
      for (const value of textValues) {
        const tokens = analyzeTextValue(value, builder.config.analyzer);
        for (const token of tokens) {
          const postings = fieldCompanion.terms[token] ?? {
            doc_ids: [],
            freqs: fieldCompanion.positions ? [] : undefined,
            positions: fieldCompanion.positions ? [] : undefined,
          };
          const docIds = postings.doc_ids;
          const lastIndex = docIds.length - 1;
          if (lastIndex < 0 || docIds[lastIndex] !== docCount) {
            docIds.push(docCount);
            if (fieldCompanion.positions) {
              postings.freqs!.push(1);
              postings.positions!.push(position);
            }
          } else if (fieldCompanion.positions) {
            postings.freqs![lastIndex] = (postings.freqs![lastIndex] ?? 0) + 1;
            postings.positions!.push(position);
          }
          fieldCompanion.terms[token] = postings;
          position += 1;
        }
      }
    }
  }

  private finalizeFtsSection(
    builders: Map<string, FtsFieldBuilder>,
    docCount: number
  ): FtsSectionInput {
    const orderedFields = Object.create(null) as Record<string, FtsFieldInput>;
    for (const [fieldName, builder] of Array.from(builders.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
      orderedFields[fieldName] = builder.companion;
    }
    return {
      doc_count: docCount,
      fields: orderedFields,
    };
  }

  private createAggRollupBuildersResult(registry: SchemaRegistry): Result<Map<string, AggRollupBuilder>, CompanionBuildError> {
    const builders = new Map<string, AggRollupBuilder>();
    for (const [rollupName, rollup] of Object.entries(registry.search?.rollups ?? {}).sort((a, b) => a[0].localeCompare(b[0]))) {
      const parsedIntervalsRes = this.parseRollupIntervalsResult(rollup);
      if (Result.isError(parsedIntervalsRes)) return parsedIntervalsRes;
      const intervalMap = new Map<number, Map<number, Map<string, GroupBuilder>>>();
      for (const intervalMs of parsedIntervalsRes.value) intervalMap.set(intervalMs, new Map());
      builders.set(rollupName, {
        rollup,
        intervalsMs: parsedIntervalsRes.value,
        intervalMap,
        dimensionNames: [...(rollup.dimensions ?? [])],
        fieldNames: rollupRequiredFieldNames(registry, rollup),
      });
    }
    return Result.ok(builders);
  }

  private finalizeAggSection(builders: Map<string, AggRollupBuilder>): AggSectionInput {
    const encodedRollups: AggSectionInput["rollups"] = {};
    for (const [rollupName, builder] of builders) {
      encodedRollups[rollupName] = { intervals: this.finalizeAggIntervals(builder.intervalMap, builder.dimensionNames) };
    }

    return { rollups: encodedRollups };
  }

  private summarizeCompanionBuildProgress(
    colBuilders: Map<string, ColumnFieldBuilder>,
    ftsBuilders: Map<string, FtsFieldBuilder>,
    aggBuilders: Map<string, AggRollupBuilder>,
    metricsBuilder: MetricsBlockBuilder | null,
    docCount: number
  ): CompanionBuildProgress {
    let colValues = 0;
    for (const builder of colBuilders.values()) colValues += builder.values.length;

    let ftsTerms = 0;
    let ftsPostings = 0;
    let ftsPositions = 0;
    for (const builder of ftsBuilders.values()) {
      for (const postings of Object.values(builder.companion.terms)) {
        ftsTerms += 1;
        ftsPostings += postings.doc_ids.length;
        ftsPositions += postings.positions?.length ?? 0;
      }
    }

    let aggWindows = 0;
    let aggGroups = 0;
    for (const builder of aggBuilders.values()) {
      for (const windowMap of builder.intervalMap.values()) {
        aggWindows += windowMap.size;
        for (const groups of windowMap.values()) aggGroups += groups.size;
      }
    }

    return {
      docCount,
      colFields: colBuilders.size,
      colValues,
      ftsFields: ftsBuilders.size,
      ftsTerms,
      ftsPostings,
      ftsPositions,
      aggRollups: aggBuilders.size,
      aggWindows,
      aggGroups,
      metricRecords: metricsBuilder?.records.length ?? 0,
    };
  }

  private parseRollupIntervalsResult(rollup: SearchRollupConfig): Result<number[], CompanionBuildError> {
    const parsed: number[] = [];
    for (const interval of rollup.intervals) {
      const intervalMsRes = parseDurationMsResult(interval);
      if (Result.isError(intervalMsRes)) return invalidCompanionBuild(intervalMsRes.error.message);
      parsed.push(intervalMsRes.value);
    }
    return Result.ok(parsed);
  }

  private recordAggContributionResult(
    builder: AggRollupBuilder,
    contribution: {
      timestampMs: number;
      dimensions: Record<string, string | null>;
      measures: Record<string, AggMeasureState>;
    }
  ): Result<void, CompanionBuildError> {
    const groupKey = encodeAggGroupKey(contribution.dimensions, builder.dimensionNames);
    for (const intervalMs of builder.intervalsMs) {
      if (!Number.isFinite(intervalMs) || intervalMs <= 0) return invalidCompanionBuild(`invalid rollup interval ${intervalMs}`);
      const startMs = Math.floor(contribution.timestampMs / intervalMs) * intervalMs;
      const windowMap = builder.intervalMap.get(intervalMs) ?? new Map<number, Map<string, GroupBuilder>>();
      builder.intervalMap.set(intervalMs, windowMap);
      const groups = windowMap.get(startMs) ?? new Map<string, GroupBuilder>();
      windowMap.set(startMs, groups);
      let group = groups.get(groupKey);
      if (!group) {
        const measures: Record<string, AggMeasureState> = {};
        for (const [measureName, state] of Object.entries(contribution.measures)) {
          measures[measureName] = cloneAggMeasureState(state);
        }
        group = {
          key: groupKey,
          measures,
        };
        groups.set(groupKey, group);
        continue;
      }
      for (const [measureName, state] of Object.entries(contribution.measures)) {
        const existing = group.measures[measureName];
        group.measures[measureName] = existing ? mergeAggMeasureState(existing, state) : cloneAggMeasureState(state);
      }
    }
    return Result.ok(undefined);
  }

  private finalizeAggIntervals(
    intervalMap: Map<number, Map<number, Map<string, GroupBuilder>>>,
    dimensionNames: string[]
  ): AggSectionInput["rollups"][string]["intervals"] {
    const intervals: AggSectionInput["rollups"][string]["intervals"] = {};
    for (const [intervalMs, windowMap] of Array.from(intervalMap.entries()).sort((a, b) => a[0] - b[0])) {
      intervals[String(intervalMs)] = {
        interval_ms: intervalMs,
        windows: Array.from(windowMap.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([startMs, groups]) => ({
            start_ms: startMs,
            groups: Array.from(groups.values()).map((group) => ({
              dimensions: decodeAggGroupKey(group.key, dimensionNames),
              measures: group.measures,
            })),
          })),
      };
    }
    return intervals;
  }

  private finalizeMetricsBlockSection(builder: MetricsBlockBuilder): MetricsBlockSectionInput {
    return {
      record_count: builder.records.length,
      min_window_start_ms: builder.minWindowStartMs,
      max_window_end_ms: builder.maxWindowEndMs,
      records: builder.records,
    };
  }

  private recordMetricsBlockBuilder(builder: MetricsBlockBuilder, parsed: unknown, docCount: number): void {
    const normalizedRes = buildMetricsBlockRecord(docCount, parsed);
    if (Result.isError(normalizedRes)) return;
    builder.records.push(normalizedRes.value);
    builder.minWindowStartMs =
      builder.minWindowStartMs == null
        ? normalizedRes.value.windowStartMs
        : Math.min(builder.minWindowStartMs, normalizedRes.value.windowStartMs);
    builder.maxWindowEndMs =
      builder.maxWindowEndMs == null
        ? normalizedRes.value.windowEndMs
        : Math.max(builder.maxWindowEndMs, normalizedRes.value.windowEndMs);
  }
}
