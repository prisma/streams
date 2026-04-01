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
import { LruCache } from "../util/lru";
import { RuntimeMemorySampler } from "../runtime_memory_sampler";
import type { MemoryGuard } from "../memory";
import { retry } from "../util/retry";
import { searchCompanionObjectKey, segmentObjectKey, streamHash16Hex } from "../util/stream_paths";
import { buildDesiredSearchCompanionPlan, hashSearchCompanionPlan, type SearchCompanionPlan } from "./companion_plan";
import {
  decodeBundledSegmentCompanionSectionFromTocResult,
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

type CachedCompanionBundle = {
  bytes: Uint8Array;
  toc: CompanionToc;
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
  // Keep only a tiny hot cache of compressed bundle bytes plus the parsed TOC.
  // Query reads decode the requested section on demand instead of inflating the
  // full bundle into memory.
  private readonly cache = new LruCache<string, CachedCompanionBundle>(4);
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
  }

  enqueue(stream: string): void {
    this.queue.add(stream);
  }

  async getColSegmentCompanion(stream: string, segmentIndex: number): Promise<ColSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "col")) ?? null;
  }

  async getFtsSegmentCompanion(stream: string, segmentIndex: number): Promise<FtsSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "fts")) ?? null;
  }

  async getAggSegmentCompanion(stream: string, segmentIndex: number): Promise<AggSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "agg")) ?? null;
  }

  async getMetricsBlockSegmentCompanion(stream: string, segmentIndex: number): Promise<MetricsBlockSectionView | null> {
    return (await this.getSectionCompanion(stream, segmentIndex, "mblk")) ?? null;
  }

  private async getSectionCompanion<K extends CompanionSectionKind>(
    stream: string,
    segmentIndex: number,
    kind: K
  ): Promise<CompanionSectionMap[K] | null> {
    const leave = this.memorySampler?.enter("companion_read", { stream, segment_index: segmentIndex, kind });
    try {
      const planRow = this.getCurrentPlanRow(stream);
      if (!planRow) return null;
      const row = this.db.getSearchSegmentCompanion(stream, segmentIndex);
      if (!row || row.plan_generation !== planRow.generation) return null;
      if (!parseSectionKinds(row).has(kind)) return null;
      const bundle = await this.loadBundleBytesResult(row);
      if (Result.isError(bundle)) throw dsError(bundle.error.message);
      const plan = this.parsePlanRowResult(planRow);
      if (Result.isError(plan)) throw dsError(plan.error.message);
      const decoded = decodeBundledSegmentCompanionSectionFromTocResult(bundle.value.bytes, bundle.value.toc, kind, plan.value);
      if (Result.isError(decoded)) throw dsError(decoded.error.message);
      return decoded.value ?? null;
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

  private async loadBundleBytesResult(row: SearchSegmentCompanionRow): Promise<Result<CachedCompanionBundle, CompanionBuildError>> {
    const cached = this.cache.get(row.object_key);
    if (cached) return Result.ok(cached);
    try {
      const bytes = await retry(
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
      );
      const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
      if (Result.isError(tocRes)) return invalidCompanionBuild(tocRes.error.message);
      const entry = { bytes, toc: tocRes.value };
      this.cache.set(row.object_key, entry);
      return Result.ok(entry);
    } catch (e: unknown) {
      return invalidCompanionBuild(String((e as any)?.message ?? e));
    }
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
        const sectionKinds = companionRes.value.sectionKinds;
        this.db.upsertSearchSegmentCompanion(
          stream,
          seg.segment_index,
          objectKey,
          planRow.generation,
          JSON.stringify(sectionKinds),
          JSON.stringify(sectionSizes),
          payload.byteLength
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
  ): Promise<Result<{ payload: Uint8Array; sectionKinds: CompanionSectionKind[]; sectionSizes: Record<string, number> }, CompanionBuildError>> {
    const bytesRes = await this.loadSegmentBytesResult(seg);
    if (Result.isError(bytesRes)) return bytesRes;
    const segmentBytes = bytesRes.value;
    const sectionPayloads: EncodedCompanionSectionPayload[] = [];
    const sectionKinds: CompanionSectionKind[] = [];
    const sectionSizes: Record<string, number> = {};
    const addSection = (payload: EncodedCompanionSectionPayload): void => {
      sectionPayloads.push(payload);
      const kind = payload.kind;
      sectionKinds.push(kind);
      sectionSizes[kind] = payload.payload.byteLength;
    };

    if (plan.families.col) {
      const colRes = await this.buildColSectionResult(registry, seg, segmentBytes);
      if (Result.isError(colRes)) return colRes;
      addSection(encodeCompanionSectionPayload("col", colRes.value, plan));
    }
    if (plan.families.fts) {
      const ftsRes = await this.buildFtsSectionResult(registry, seg, segmentBytes);
      if (Result.isError(ftsRes)) return ftsRes;
      addSection(encodeCompanionSectionPayload("fts", ftsRes.value, plan));
    }
    if (plan.families.agg) {
      const aggRes = await this.buildAggSectionResult(registry, seg, segmentBytes);
      if (Result.isError(aggRes)) return aggRes;
      addSection(encodeCompanionSectionPayload("agg", aggRes.value, plan));
    }
    if (plan.families.mblk) {
      const metricsRes = await this.buildMetricsBlockSectionResult(seg, segmentBytes);
      if (Result.isError(metricsRes)) return metricsRes;
      addSection(encodeCompanionSectionPayload("mblk", metricsRes.value, plan));
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

  private async buildColSectionResult(
    registry: SchemaRegistry,
    seg: SegmentRow,
    segmentBytes: Uint8Array
  ): Promise<Result<ColSectionInput, CompanionBuildError>> {
    const colBuilders = this.createColBuilders(registry);
    const fieldNames = Array.from(colBuilders.keys());
    const docCountRes = await this.visitParsedSegmentRecordsResult(segmentBytes, seg, async ({ docCount, offset, parsed, parsedOk }) => {
      let rawSearchValues: Map<string, unknown[]> | null = null;
      if (parsedOk) {
        const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, offset, parsed, fieldNames);
        if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
        rawSearchValues = rawValuesRes.value;
      }
      for (const [fieldName, builder] of colBuilders) {
        if (builder.invalid) continue;
        const rawValues = rawSearchValues?.get(fieldName) ?? [];
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
      return Result.ok(undefined);
    });
    if (Result.isError(docCountRes)) return docCountRes;
    return Result.ok(this.finalizeColSection(registry, colBuilders, docCountRes.value));
  }

  private async buildFtsSectionResult(
    registry: SchemaRegistry,
    seg: SegmentRow,
    segmentBytes: Uint8Array
  ): Promise<Result<FtsSectionInput, CompanionBuildError>> {
    const ftsFieldEntries = Object.entries(registry.search?.fields ?? {})
      .filter(([, field]) => field.kind === "text" || (field.kind === "keyword" && field.prefix === true))
      .sort((a, b) => a[0].localeCompare(b[0]));
    const builders = new Map<string, FtsFieldBuilder>();
    let docCount = 0;
    for (const [fieldName, field] of ftsFieldEntries) {
      const fieldRes = await this.buildFtsFieldResult(registry, seg, segmentBytes, fieldName, field);
      if (Result.isError(fieldRes)) return fieldRes;
      builders.set(fieldName, fieldRes.value.builder);
      docCount = fieldRes.value.docCount;
    }
    return Result.ok(this.finalizeFtsSection(builders, docCount));
  }

  private async buildFtsFieldResult(
    registry: SchemaRegistry,
    seg: SegmentRow,
    segmentBytes: Uint8Array,
    fieldName: string,
    field: SearchFieldConfig
  ): Promise<Result<{ docCount: number; builder: FtsFieldBuilder }, CompanionBuildError>> {
    const builder = this.createFtsFieldBuilder(field);
    const docCountRes = await this.visitParsedSegmentRecordsResult(segmentBytes, seg, async ({ docCount, offset, parsed, parsedOk }) => {
      const fieldCompanion = builder.companion;
      const textValues: string[] = [];
      if (parsedOk) {
        const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, offset, parsed, [fieldName]);
        if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
        for (const rawValue of rawValuesRes.value.get(fieldName) ?? []) {
          if (builder.config.kind === "keyword") {
            const normalized = normalizeKeywordValue(rawValue, builder.config.normalizer);
            if (normalized != null) textValues.push(normalized);
          } else if (builder.config.kind === "text" && typeof rawValue === "string") {
            textValues.push(rawValue);
          }
        }
      }
      if (textValues.length === 0) {
        return Result.ok(undefined);
      }
      fieldCompanion.exists_docs.push(docCount);
      if (builder.config.kind === "keyword") {
        for (const value of textValues) {
          const postings = fieldCompanion.terms[value] ?? [];
          if (postings.length === 0 || postings[postings.length - 1]!.d !== docCount) {
            postings.push({ d: docCount });
          }
          fieldCompanion.terms[value] = postings;
        }
        return Result.ok(undefined);
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
      return Result.ok(undefined);
    });
    if (Result.isError(docCountRes)) return docCountRes;
    return Result.ok({ docCount: docCountRes.value, builder });
  }

  private async buildAggSectionResult(
    registry: SchemaRegistry,
    seg: SegmentRow,
    segmentBytes: Uint8Array
  ): Promise<Result<AggSectionInput, CompanionBuildError>> {
    const encodedRollups: AggSectionInput["rollups"] = {};
    for (const [rollupName, rollup] of Object.entries(registry.search?.rollups ?? {}).sort((a, b) => a[0].localeCompare(b[0]))) {
      const parsedIntervalsRes = this.parseRollupIntervalsResult(rollup);
      if (Result.isError(parsedIntervalsRes)) return parsedIntervalsRes;
      const intervalMap = new Map<number, Map<number, Map<string, GroupBuilder>>>();
      for (const intervalMs of parsedIntervalsRes.value) intervalMap.set(intervalMs, new Map());
      const fieldNames = rollupRequiredFieldNames(registry, rollup);
      const buildRes = await this.visitParsedSegmentRecordsResult(segmentBytes, seg, async ({ offset, parsed, parsedOk }) => {
        if (!parsedOk) return Result.ok(undefined);
        const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, offset, parsed, fieldNames);
        if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
        const contributionRes = extractRollupContributionResult(registry, rollup, offset, parsed, rawValuesRes.value);
        if (Result.isError(contributionRes)) return invalidCompanionBuild(contributionRes.error.message);
        if (!contributionRes.value) return Result.ok(undefined);
        const recordRes = this.recordAggContributionResult(intervalMap, parsedIntervalsRes.value, contributionRes.value);
        if (Result.isError(recordRes)) return recordRes;
        return Result.ok(undefined);
      });
      if (Result.isError(buildRes)) return buildRes;
      encodedRollups[rollupName] = { intervals: this.finalizeAggIntervals(intervalMap) };
    }
    return Result.ok({ rollups: encodedRollups });
  }

  private async buildMetricsBlockSectionResult(
    seg: SegmentRow,
    segmentBytes: Uint8Array
  ): Promise<Result<MetricsBlockSectionInput, CompanionBuildError>> {
    const builder: MetricsBlockBuilder = { records: [], minWindowStartMs: undefined, maxWindowEndMs: undefined };
    const buildRes = await this.visitParsedSegmentRecordsResult(segmentBytes, seg, async ({ docCount, parsed, parsedOk }) => {
      if (!parsedOk) return Result.ok(undefined);
      const normalizedRes = buildMetricsBlockRecord(docCount, parsed);
      if (!Result.isError(normalizedRes)) {
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
      return Result.ok(undefined);
    });
    if (Result.isError(buildRes)) return buildRes;
    return Result.ok(this.finalizeMetricsBlockSection(builder));
  }

  private createColBuilders(registry: SchemaRegistry): Map<string, ColumnFieldBuilder> {
    const columnFields = Object.entries(registry.search?.fields ?? {}).filter(([, field]) => field.column === true);
    const builders = new Map<string, ColumnFieldBuilder>();
    for (const [fieldName, field] of columnFields) {
      builders.set(fieldName, { config: field, kind: field.kind, docIds: [], values: [], invalid: false });
    }
    return builders;
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

  private finalizeAggSection(
    builders: Map<string, Map<number, Map<number, Map<string, GroupBuilder>>>>
  ): AggSectionInput {
    const encodedRollups: AggSectionInput["rollups"] = {};
    for (const [rollupName, intervalMap] of builders) {
      encodedRollups[rollupName] = { intervals: this.finalizeAggIntervals(intervalMap) };
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
}
