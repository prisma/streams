import { randomBytes } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
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
import { iterateBlockRecordsResult } from "../segment/format";
import { dsError } from "../util/ds_error.ts";
import { RuntimeMemorySampler } from "../runtime_memory_sampler";
import type { MemoryGuard } from "../memory";
import { retry } from "../util/retry";
import { searchCompanionObjectKey, segmentObjectKey, streamHash16Hex } from "../util/stream_paths";
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
import type { FtsFieldInput, FtsPosting, FtsSectionInput, FtsSectionView } from "./fts_format";
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
  dimensions: Record<string, string | null>;
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
  fieldNames: string[];
};

const PAYLOAD_DECODER = new TextDecoder();

async function yieldToEventLoop(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

function compareValues(left: bigint | number | boolean, right: bigint | number | boolean): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  return String(left).localeCompare(String(right));
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
  private readonly yieldBlocks: number;
  private readonly memorySampler?: RuntimeMemorySampler;
  private timer: any | null = null;
  private running = false;

  constructor(
    private readonly cfg: Config,
    private readonly db: SqliteDurableStore,
    private readonly os: ObjectStore,
    private readonly registry: SchemaRegistryStore,
    private readonly publishManifest?: (stream: string) => Promise<void>,
    private readonly onMetadataChanged?: (stream: string) => void,
    private readonly metrics?: Metrics,
    memorySampler?: RuntimeMemorySampler,
    private readonly memory?: MemoryGuard
  ) {
    this.yieldBlocks = Math.max(1, cfg.searchCompanionYieldBlocks);
    this.memorySampler = memorySampler;
    this.fileCache = new CompanionFileCache(
      `${cfg.rootDir}/cache/companions`,
      cfg.searchCompanionFileCacheMaxBytes,
      cfg.searchCompanionFileCacheMaxAgeMs,
      cfg.searchCompanionMappedCacheEntries
    );
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.cfg.indexCheckIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
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
      if (this.memory?.isOverLimit()) this.fileCache.clearMapped();
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

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      if (this.metrics) {
        this.metrics.record("tieredstore.companion.build.queue_len", this.queue.size, "count");
        this.metrics.record("tieredstore.companion.builds_inflight", this.building.size, "count");
      }
      const streams = Array.from(new Set([...this.db.listSearchCompanionPlanStreams(), ...this.queue]));
      this.queue.clear();
      for (const stream of streams) {
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
      }
    } finally {
      this.running = false;
    }
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
      if (this.memory && !this.memory.shouldAllow()) {
        this.queue.add(stream);
        return Result.ok(undefined);
      }

      const batchLimit = Math.max(1, this.cfg.searchCompanionBuildBatchSegments);
      const batch = stale.slice(0, batchLimit);
      let builtCount = 0;
      for (const nextSegmentIndex of batch) {
        if (this.memory && !this.memory.shouldAllow()) {
          this.queue.add(stream);
          return Result.ok(undefined);
        }
        const seg = this.db.getSegmentByIndex(stream, nextSegmentIndex);
        if (!seg || !seg.r2_etag) continue;
        const startedAt = Date.now();
        const companionRes = this.memorySampler
          ? await this.memorySampler.track(
              "companion",
              { stream, segment_index: seg.segment_index, plan_generation: planRow.generation },
              () => this.buildEncodedBundledCompanionResult(regRes.value, desiredPlan, planRow.generation, seg)
            )
          : await this.buildEncodedBundledCompanionResult(regRes.value, desiredPlan, planRow.generation, seg);
        if (Result.isError(companionRes)) return companionRes;
        const objectId = Buffer.from(randomBytes(8)).toString("hex");
        const objectKey = searchCompanionObjectKey(streamHash16Hex(stream), seg.segment_index, objectId);
        const payload = companionRes.value.payload;
        const sectionSizes = companionRes.value.sectionSizes;
        try {
          await retry(
            () => this.os.put(objectKey, payload, { contentLength: payload.byteLength }),
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
        } catch (e: unknown) {
          return invalidCompanionBuild(String((e as any)?.message ?? e));
        }
        const cacheRes = this.fileCache.storeBytesResult(objectKey, payload);
        if (Result.isError(cacheRes)) {
          console.warn("bundled companion local cache populate failed", objectKey, cacheRes.error.message);
        }
        const sectionKinds = companionRes.value.sectionKinds;
        this.db.upsertSearchSegmentCompanion(
          stream,
          seg.segment_index,
          objectKey,
          planRow.generation,
          JSON.stringify(sectionKinds),
          JSON.stringify(sectionSizes),
          payload.byteLength,
          companionRes.value.primaryTimestampMinMs,
          companionRes.value.primaryTimestampMaxMs
        );
        builtCount += 1;
        if (this.metrics) {
          const elapsedNs = BigInt(Date.now() - startedAt) * 1_000_000n;
          this.metrics.record("tieredstore.companion.build.latency", Number(elapsedNs), "ns", undefined, stream);
          this.metrics.record("tieredstore.companion.objects.built", 1, "count", undefined, stream);
        }
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
      if (existsSync(seg.local_path)) return Result.ok(readFileSync(seg.local_path));
      const bytes = await retry(
        async () => {
          const data = await this.os.get(segmentObjectKey(streamHash16Hex(seg.stream), seg.segment_index));
          if (!data) throw dsError(`missing segment ${seg.segment_index}`);
          return data;
        },
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
        if (processedBlocks % this.yieldBlocks === 0) await yieldToEventLoop();
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
    const bytesRes = await this.loadSegmentBytesResult(seg);
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
    const docCountRes = await this.visitParsedSegmentRecordsResult(segmentBytes, seg, async ({ docCount, offset, parsed, parsedOk }) => {
      let rawSearchValues: Map<string, unknown[]> | null = null;
      if (parsedOk && fieldNameList.length > 0) {
        const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, offset, parsed, fieldNameList);
        if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
        rawSearchValues = rawValuesRes.value;
      }
      if (rawSearchValues) {
        this.recordColBuilders(colBuilders, rawSearchValues, docCount);
        this.recordFtsBuilders(ftsBuilders, rawSearchValues, docCount);
      }
      if (parsedOk && rawSearchValues) {
        for (const builder of aggBuilders.values()) {
          const contributionRes = extractRollupContributionResult(registry, builder.rollup, offset, parsed, rawSearchValues);
          if (Result.isError(contributionRes)) return invalidCompanionBuild(contributionRes.error.message);
          if (!contributionRes.value) continue;
          const recordRes = this.recordAggContributionResult(builder.intervalMap, builder.intervalsMs, contributionRes.value);
          if (Result.isError(recordRes)) return recordRes;
        }
      }
      if (metricsBuilder && parsedOk) this.recordMetricsBlockBuilder(metricsBuilder, parsed, docCount);
      return Result.ok(undefined);
    });
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
      const colSection = this.finalizeColSection(registry, colBuilders, docCountRes.value);
      const primaryTimestampField = colSection.primary_timestamp_field;
      const primaryTimestampColumn = primaryTimestampField ? colSection.fields[primaryTimestampField] : undefined;
      primaryTimestampMinMs = typeof primaryTimestampColumn?.min === "bigint" ? primaryTimestampColumn.min : null;
      primaryTimestampMaxMs = typeof primaryTimestampColumn?.max === "bigint" ? primaryTimestampColumn.max : null;
      addSection(encodeCompanionSectionPayload("col", colSection, plan));
    }
    if (plan.families.fts) {
      addSection(encodeCompanionSectionPayload("fts", this.finalizeFtsSection(ftsBuilders, docCountRes.value), plan));
    }
    if (plan.families.agg) {
      addSection(encodeCompanionSectionPayload("agg", this.finalizeAggSection(aggBuilders), plan));
    }
    if (plan.families.mblk && metricsBuilder) {
      addSection(encodeCompanionSectionPayload("mblk", this.finalizeMetricsBlockSection(metricsBuilder), plan));
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
        doc_ids: [...builder.docIds],
        values: [...builder.values],
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
        terms: Object.create(null) as Record<string, FtsPosting[]>,
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
          const postings = fieldCompanion.terms[value] ?? [];
          if (postings.length === 0 || postings[postings.length - 1]!.d !== docCount) postings.push({ d: docCount });
          fieldCompanion.terms[value] = postings;
        }
        continue;
      }
      let position = 0;
      for (const value of textValues) {
        const tokens = analyzeTextValue(value, builder.config.analyzer);
        for (const token of tokens) {
          const postings = fieldCompanion.terms[token] ?? [];
          const last = postings[postings.length - 1];
          if (!last || last.d !== docCount) {
            postings.push({ d: docCount, p: fieldCompanion.positions ? [position] : undefined });
          } else if (fieldCompanion.positions && last.p) {
            last.p.push(position);
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
        fieldNames: rollupRequiredFieldNames(registry, rollup),
      });
    }
    return Result.ok(builders);
  }

  private finalizeAggSection(builders: Map<string, AggRollupBuilder>): AggSectionInput {
    const encodedRollups: AggSectionInput["rollups"] = {};
    for (const [rollupName, builder] of builders) {
      encodedRollups[rollupName] = { intervals: this.finalizeAggIntervals(builder.intervalMap) };
    }

    return { rollups: encodedRollups };
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
    intervalMap: Map<number, Map<number, Map<string, GroupBuilder>>>,
    intervalDurationsMs: number[],
    contribution: {
      timestampMs: number;
      dimensions: Record<string, string | null>;
      measures: Record<string, AggMeasureState>;
    }
  ): Result<void, CompanionBuildError> {
    for (const intervalMs of intervalDurationsMs) {
      if (!Number.isFinite(intervalMs) || intervalMs <= 0) return invalidCompanionBuild(`invalid rollup interval ${intervalMs}`);
      const startMs = Math.floor(contribution.timestampMs / intervalMs) * intervalMs;
      const windowMap = intervalMap.get(intervalMs) ?? new Map<number, Map<string, GroupBuilder>>();
      intervalMap.set(intervalMs, windowMap);
      const groups = windowMap.get(startMs) ?? new Map<string, GroupBuilder>();
      windowMap.set(startMs, groups);
      const groupKey = JSON.stringify(contribution.dimensions);
      let group = groups.get(groupKey);
      if (!group) {
        const measures: Record<string, AggMeasureState> = {};
        for (const [measureName, state] of Object.entries(contribution.measures)) {
          measures[measureName] = cloneAggMeasureState(state);
        }
        group = {
          dimensions: { ...contribution.dimensions },
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
    intervalMap: Map<number, Map<number, Map<string, GroupBuilder>>>
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
              dimensions: group.dimensions,
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
