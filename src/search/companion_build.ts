import { closeSync, mkdirSync, mkdtempSync, openSync, renameSync, rmSync, unlinkSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import type { SchemaRegistry, SearchFieldConfig, SearchRollupConfig } from "../schema/registry";
import {
  DSB3_HEADER_BYTES,
  encodeBlockResult,
  encodeFooter,
  iterateBlockRecordsFromFileResult,
  iterateBlocksFromFileResult,
  type BlockIndexEntry,
  type SegmentRecord,
} from "../segment/format";
import {
  encodeBundledSegmentCompanionChunksFromPayloads,
  encodeBundledSegmentCompanionFromPayloads,
  encodeCompanionSectionChunkPayload,
  encodeCompanionSectionPayload,
  type CompanionSectionKind,
  type EncodedCompanionSectionChunkPayload,
  type EncodedCompanionSectionPayload,
} from "./companion_format";
import type { SearchCompanionPlan, SearchCompanionPlanField } from "./companion_plan";
import type { ColFieldInput, ColScalar, ColSectionInput } from "./col_format";
import {
  appendKeywordPostingDoc,
  appendTextPostingPositions,
  type FtsFieldInput,
  type FtsSectionInput,
  type FtsTermInput,
} from "./fts_format";
import {
  analyzeTextValueCached,
  buildFastScalarAccessorTrieResult,
  canonicalizeColumnValue,
  compareSearchStrings,
  compileSearchFieldAccessorsResult,
  extractRawSearchValuesWithCompiledAccessorsResult,
  normalizeKeywordValue,
  type FastScalarAccessorTrie,
  visitFastScalarJsonValuesFromBytesWithTrieResult,
  visitRawSearchValuesWithCompiledAccessorsResult,
  type CompiledSearchFieldAccessor,
} from "./schema";
import { buildMetricsBlockRecord } from "../profiles/metrics/normalize";
import type { MetricsBlockSectionInput } from "../profiles/metrics/block_format";
import { parseDurationMsResult } from "../util/duration";
import {
  cloneAggMeasureState,
  extractRollupContributionResult,
  mergeAggMeasureState,
  rollupRequiredFieldNames,
} from "./aggregate";
import type { AggMeasureState, AggSectionInput } from "./agg_format";
import { schemaVersionForOffset } from "../schema/read_json";
import { mergePartialCompanionFilesResult, type PartialCompanionOutput } from "./companion_merge";
import { writeCompanionOutputChunksResult } from "./companion_output_file";

const PAYLOAD_DECODER = new TextDecoder();
const PAYLOAD_ENCODER = new TextEncoder();
const AGG_DIMENSION_SEPARATOR = "\u001f";
const AGG_DIMENSION_NULL = "\u0000";
const EMPTY_JSON_BYTES = PAYLOAD_ENCODER.encode("{}");
const EMPTY_ROUTING_KEY = new Uint8Array(0);

type CompanionBuildError = { kind: "invalid_companion_build"; message: string };
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
  tokenCache: Map<string, string[]>;
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

type FastRecordState = {
  colRecordValues: Map<string, ColumnRecordValue>;
  ftsSeenFields: Set<string>;
  ftsPositions: Map<string, number>;
};
type AggRollupBuilder = {
  rollup: SearchRollupConfig;
  intervalsMs: number[];
  intervalMap: Map<number, Map<number, Map<string, GroupBuilder>>>;
  dimensionNames: string[];
  fieldNames: string[];
};

export type CompanionBuildInput = {
  registry: SchemaRegistry;
  plan: SearchCompanionPlan;
  planGeneration: number;
  keywordTermShard?: {
    count: number;
    index: number;
  };
  docIdShard?: {
    count: number;
    index: number;
  };
  outputDir?: string;
  segment: {
    stream: string;
    segmentIndex: number;
    startOffset: bigint;
    localPath: string;
  };
};

export type CompanionBuildOutput =
  | {
      storage: "bytes";
      payload: Uint8Array;
      sectionKinds: CompanionSectionKind[];
      sectionSizes: Record<string, number>;
      primaryTimestampMinMs: bigint | null;
      primaryTimestampMaxMs: bigint | null;
    }
  | {
      storage: "file";
      localPath: string;
      sizeBytes: number;
      sectionKinds: CompanionSectionKind[];
      sectionSizes: Record<string, number>;
      primaryTimestampMinMs: bigint | null;
      primaryTimestampMaxMs: bigint | null;
    };

type PartialPlanSpec = {
  name: string;
  fields: string[];
  col?: boolean;
  fts?: boolean;
  termShards?: number;
  docShards?: number;
};

export type CompanionBuildPartialPlan = {
  name: string;
  plan: SearchCompanionPlan;
  keywordTermShard?: {
    count: number;
    index: number;
  };
  docIdShard?: {
    count: number;
    index: number;
  };
};

export type CompanionScratchSegment = {
  name: string;
  plan: SearchCompanionPlan;
  localPath: string;
};

type ScratchSegmentWriter = {
  finalPath: string;
  tmpPath: string;
  fd: number;
  blockOffset: number;
  footerEntries: BlockIndexEntry[];
};

type ScratchPlanState = {
  name: string;
  plan: SearchCompanionPlan;
  writer: ScratchSegmentWriter;
  localPath: string;
};

const EVLOG_PARTIAL_PLAN_SPECS: PartialPlanSpec[] = [
  { name: "col-timestamp", fields: ["timestamp"], col: true, docShards: 2 },
  { name: "col-status", fields: ["status"], col: true, docShards: 2 },
  { name: "col-duration", fields: ["duration"], col: true, docShards: 2 },
  { name: "keyword-level", fields: ["level"], fts: true },
  { name: "keyword-service", fields: ["service"], fts: true },
  { name: "keyword-environment", fields: ["environment"], fts: true },
  { name: "keyword-method", fields: ["method"], fts: true },
  { name: "keyword-requestId", fields: ["requestId"], fts: true, termShards: 4 },
  { name: "keyword-traceId", fields: ["traceId"], fts: true, termShards: 4 },
  { name: "keyword-spanId", fields: ["spanId"], fts: true, termShards: 4 },
  { name: "keyword-path", fields: ["path"], fts: true, termShards: 4 },
  { name: "text-message", fields: ["message"], fts: true, termShards: 4 },
  { name: "text-error-message", fields: ["error.message"], fts: true, termShards: 4 },
  { name: "text-why", fields: ["why"], fts: true, termShards: 4 },
  { name: "text-fix", fields: ["fix"], fts: true, termShards: 4 },
];

function invalidCompanionBuild(message: string): Result<never, CompanionBuildError> {
  return Result.err({ kind: "invalid_companion_build", message });
}

function termBelongsToShard(
  term: string,
  shard: CompanionBuildInput["keywordTermShard"] | CompanionBuildPartialPlan["keywordTermShard"] | undefined
): boolean {
  if (!shard || shard.count <= 1) return true;
  let hash = 2166136261;
  for (let index = 0; index < term.length; index += 1) {
    hash ^= term.charCodeAt(index)!;
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % shard.count === shard.index;
}

function docBelongsToShard(
  docId: number,
  shard: CompanionBuildInput["docIdShard"] | CompanionBuildPartialPlan["docIdShard"] | undefined
): boolean {
  if (!shard || shard.count <= 1) return true;
  return docId % shard.count === shard.index;
}

function createSubsetCompanionPlan(
  plan: SearchCompanionPlan,
  fields: SearchCompanionPlanField[],
  families: Pick<SearchCompanionPlan["families"], "col" | "fts">
): SearchCompanionPlan {
  return {
    families: {
      col: families.col,
      fts: families.fts,
      agg: false,
      mblk: false,
    },
    fields,
    rollups: [],
    summary: plan.summary,
  };
}

export function buildCompanionPartialPlans(
  profile: string | undefined,
  plan: SearchCompanionPlan
): CompanionBuildPartialPlan[] {
  if (profile !== "evlog") return [];
  if (plan.families.agg || plan.families.mblk) return [];
  const planFieldsByName = new Map(plan.fields.map((field) => [field.name, field]));
  const partials: CompanionBuildPartialPlan[] = [];
  const coveredFieldNames = new Set<string>();
  for (const spec of EVLOG_PARTIAL_PLAN_SPECS) {
    const fields = spec.fields
      .map((fieldName) => planFieldsByName.get(fieldName))
      .filter((field): field is SearchCompanionPlanField => field != null);
    if (fields.length === 0) continue;
    for (const field of fields) coveredFieldNames.add(field.name);
    const subsetPlan = createSubsetCompanionPlan(plan, fields, {
      col: spec.col === true && fields.some((field) => field.column),
      fts: spec.fts === true && fields.some((field) => field.kind === "text" || (field.kind === "keyword" && field.prefix)),
    });
    const termShardCount = Math.max(1, spec.termShards ?? 1);
    const docShardCount = Math.max(1, spec.docShards ?? 1);
    for (let termShardIndex = 0; termShardIndex < termShardCount; termShardIndex += 1) {
      for (let docShardIndex = 0; docShardIndex < docShardCount; docShardIndex += 1) {
        const suffixParts: string[] = [];
        if (termShardCount > 1) suffixParts.push(`shard-${termShardIndex + 1}-of-${termShardCount}`);
        if (docShardCount > 1) suffixParts.push(`doc-${docShardIndex + 1}-of-${docShardCount}`);
        partials.push({
          name: suffixParts.length === 0 ? spec.name : `${spec.name}-${suffixParts.join("-")}`,
          plan: subsetPlan,
          keywordTermShard: termShardCount === 1 ? undefined : { count: termShardCount, index: termShardIndex },
          docIdShard: docShardCount === 1 ? undefined : { count: docShardCount, index: docShardIndex },
        });
      }
    }
  }
  const remainingFields = plan.fields.filter((field) => !coveredFieldNames.has(field.name));
  if (remainingFields.length > 0) {
    partials.push({
      name: "remaining",
      plan: createSubsetCompanionPlan(plan, remainingFields, {
        col: remainingFields.some((field) => field.column),
        fts: remainingFields.some((field) => field.kind === "text" || (field.kind === "keyword" && field.prefix)),
      }),
    });
  }
  if (partials.length <= 1) return [];
  const pureShardSplit =
    partials.length > 1 &&
    partials.every(
      (partial) =>
        partial.plan.families.col === plan.families.col &&
        partial.plan.families.fts === plan.families.fts &&
        partial.plan.fields.length === plan.fields.length &&
        partial.plan.fields.every((field, index) => field.name === plan.fields[index]?.name)
    );
  if (pureShardSplit) return [];
  return partials.filter((partial) => partial.plan.families.col || partial.plan.families.fts);
}

function cleanupPartialCompanionFiles(partials: PartialCompanionOutput[]): void {
  for (const partial of partials) {
    try {
      rmSync(partial.localPath, { force: true });
    } catch {
      // ignore cleanup failures
    }
  }
}

function cleanupScratchPlanStates(states: ScratchPlanState[]): void {
  for (const state of states) {
    try {
      closeSync(state.writer.fd);
    } catch {
      // ignore close failures
    }
    try {
      unlinkSync(state.writer.tmpPath);
    } catch {
      // ignore temp cleanup failures
    }
    try {
      rmSync(state.localPath, { force: true });
    } catch {
      // ignore cleanup failures
    }
  }
}

function openScratchSegmentWriterResult(
  outputDir: string,
  filePrefix: string
): Result<ScratchSegmentWriter, CompanionBuildError> {
  mkdirSync(outputDir, { recursive: true });
  const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
  const finalPath = join(outputDir, `${filePrefix}-${nonce}.bin`);
  const tmpPath = `${finalPath}.tmp`;
  try {
    return Result.ok({
      finalPath,
      tmpPath,
      fd: openSync(tmpPath, "w"),
      blockOffset: 0,
      footerEntries: [],
    });
  } catch (error: unknown) {
    return invalidCompanionBuild(String((error as Error)?.message ?? error));
  }
}

function appendScratchSegmentBlockResult(
  writer: ScratchSegmentWriter,
  firstOffset: bigint,
  records: SegmentRecord[]
): Result<void, CompanionBuildError> {
  const blockRes = encodeBlockResult(records);
  if (Result.isError(blockRes)) return invalidCompanionBuild(blockRes.error.message);
  const block = blockRes.value;
  let written = 0;
  try {
    while (written < block.byteLength) {
      written += writeSync(writer.fd, block, written, block.byteLength - written);
    }
    writer.footerEntries.push({
      blockOffset: writer.blockOffset,
      firstOffset,
      recordCount: records.length,
      compressedLen: block.byteLength - DSB3_HEADER_BYTES,
      firstAppendNs: records[0]!.appendNs,
      lastAppendNs: records[records.length - 1]!.appendNs,
    });
    writer.blockOffset += block.byteLength;
    return Result.ok(undefined);
  } catch (error: unknown) {
    return invalidCompanionBuild(String((error as Error)?.message ?? error));
  }
}

function finalizeScratchSegmentWriterResult(writer: ScratchSegmentWriter): Result<string, CompanionBuildError> {
  const footer = encodeFooter(writer.footerEntries);
  let written = 0;
  try {
    while (written < footer.byteLength) {
      written += writeSync(writer.fd, footer, written, footer.byteLength - written);
    }
    closeSync(writer.fd);
    renameSync(writer.tmpPath, writer.finalPath);
    return Result.ok(writer.finalPath);
  } catch (error: unknown) {
    try {
      closeSync(writer.fd);
    } catch {
      // ignore close failures
    }
    try {
      unlinkSync(writer.tmpPath);
    } catch {
      // ignore temp cleanup failures
    }
    return invalidCompanionBuild(String((error as Error)?.message ?? error));
  }
}

function setProjectedPathValue(target: Record<string, unknown>, path: string[], rawValue: unknown): void {
  if (path.length === 0) return;
  let current: Record<string, unknown> = target;
  for (let index = 0; index < path.length - 1; index += 1) {
    const segment = path[index]!;
    const existing = current[segment];
    if (existing && typeof existing === "object" && !Array.isArray(existing)) {
      current = existing as Record<string, unknown>;
      continue;
    }
    const next: Record<string, unknown> = {};
    current[segment] = next;
    current = next;
  }
  const leaf = path[path.length - 1]!;
  const existingLeaf = current[leaf];
  if (existingLeaf === undefined) {
    current[leaf] = rawValue;
    return;
  }
  if (Array.isArray(existingLeaf)) {
    existingLeaf.push(rawValue);
    return;
  }
  current[leaf] = [existingLeaf, rawValue];
}

export function buildEvlogScratchSegmentsResult(
  input: CompanionBuildInput,
  partialPlans: Array<{ name: string; plan: SearchCompanionPlan }>
): Result<CompanionScratchSegment[], CompanionBuildError> {
  const scratchRoot = mkdtempSync(join(tmpdir(), `ds-companion-scratch-${input.segment.segmentIndex}-`));
  const states: ScratchPlanState[] = [];
  const fieldToGroupIndex = new Map<string, number>();
  for (const [index, partialPlan] of partialPlans.entries()) {
    for (const field of partialPlan.plan.fields) fieldToGroupIndex.set(field.name, index);
    const writerRes = openScratchSegmentWriterResult(scratchRoot, `${partialPlan.name}-${input.segment.segmentIndex}`);
    if (Result.isError(writerRes)) {
      cleanupScratchPlanStates(states);
      return writerRes;
    }
    states.push({
      name: partialPlan.name,
      plan: partialPlan.plan,
      writer: writerRes.value,
      localPath: writerRes.value.finalPath,
    });
  }

  const combinedFieldNames = Array.from(fieldToGroupIndex.keys()).sort(compareSearchStrings);
  const fastByteScanPlanByVersion = new Map<number, FastByteScanPlan>();
  let offset = input.segment.startOffset;

  for (const blockRes of iterateBlocksFromFileResult(input.segment.localPath)) {
    if (Result.isError(blockRes)) {
      cleanupScratchPlanStates(states);
      return invalidCompanionBuild(blockRes.error.message);
    }
    const blockFirstOffset = offset;
    const scratchBlockRecords = states.map(() => [] as SegmentRecord[]);
    for (const record of blockRes.value.decoded.records) {
      const version = schemaVersionForOffset(input.registry, offset);
      let fastPlan = fastByteScanPlanByVersion.get(version);
      if (!fastPlan) {
        const compiledRes = compileSearchFieldAccessorsResult(input.registry, combinedFieldNames, version);
        if (Result.isError(compiledRes)) {
          cleanupScratchPlanStates(states);
          return invalidCompanionBuild(compiledRes.error.message);
        }
        const trieRes = buildFastScalarAccessorTrieResult(compiledRes.value);
        if (Result.isError(trieRes)) {
          cleanupScratchPlanStates(states);
          return invalidCompanionBuild(trieRes.error.message);
        }
        fastPlan = { trie: trieRes.value };
        fastByteScanPlanByVersion.set(version, fastPlan);
      }
      const projectedRecords = states.map(() => ({} as Record<string, unknown>));
      const visitCounts = states.map(() => 0);
      const visitRes = visitFastScalarJsonValuesFromBytesWithTrieResult(record.payload, fastPlan.trie, (accessor, rawValue) => {
        const groupIndex = fieldToGroupIndex.get(accessor.fieldName);
        if (groupIndex == null) return;
        setProjectedPathValue(projectedRecords[groupIndex]!, accessor.path, rawValue);
        visitCounts[groupIndex] += 1;
      });
      if (Result.isError(visitRes)) {
        cleanupScratchPlanStates(states);
        return invalidCompanionBuild(visitRes.error.message);
      }
      for (let groupIndex = 0; groupIndex < states.length; groupIndex += 1) {
        const payload =
          visitCounts[groupIndex] === 0 ? EMPTY_JSON_BYTES : PAYLOAD_ENCODER.encode(JSON.stringify(projectedRecords[groupIndex]!));
        scratchBlockRecords[groupIndex]!.push({
          appendNs: record.appendNs,
          routingKey: EMPTY_ROUTING_KEY,
          payload,
        });
      }
      offset += 1n;
    }
    for (let groupIndex = 0; groupIndex < states.length; groupIndex += 1) {
      const writeRes = appendScratchSegmentBlockResult(states[groupIndex]!.writer, blockFirstOffset, scratchBlockRecords[groupIndex]!);
      if (Result.isError(writeRes)) {
        cleanupScratchPlanStates(states);
        return writeRes;
      }
    }
  }

  for (const state of states) {
    const finalizeRes = finalizeScratchSegmentWriterResult(state.writer);
    if (Result.isError(finalizeRes)) {
      cleanupScratchPlanStates(states);
      return finalizeRes;
    }
    state.localPath = finalizeRes.value;
  }
  return Result.ok(states.map((state) => ({ name: state.name, plan: state.plan, localPath: state.localPath })));
}

function maybeBuildSplitEvlogCompanionResult(
  input: CompanionBuildInput
): Result<CompanionBuildOutput, CompanionBuildError> | null {
  if (!input.outputDir) return null;
  const partialPlans = buildCompanionPartialPlans(input.registry.search?.profile, input.plan);
  if (partialPlans.length <= 1) return null;
  const partials: PartialCompanionOutput[] = [];
  for (const partialPlan of partialPlans) {
    const partialRes = buildEncodedBundledCompanionPayloadResult({
      ...input,
      plan: partialPlan.plan,
      keywordTermShard: partialPlan.keywordTermShard,
      docIdShard: partialPlan.docIdShard,
    });
    if (Result.isError(partialRes)) {
      cleanupPartialCompanionFiles(partials);
      return partialRes;
    }
    if (partialRes.value.storage !== "file") {
      cleanupPartialCompanionFiles(partials);
      return invalidCompanionBuild("split evlog companion build requires file-backed partial outputs");
    }
    partials.push({
      targetName: partialPlan.name,
      localPath: partialRes.value.localPath,
      sizeBytes: partialRes.value.sizeBytes,
      sectionKinds: partialRes.value.sectionKinds,
      primaryTimestampMinMs: partialRes.value.primaryTimestampMinMs,
      primaryTimestampMaxMs: partialRes.value.primaryTimestampMaxMs,
    });
  }

  const mergeRes = mergePartialCompanionFilesResult({
    stream: input.segment.stream,
    segmentIndex: input.segment.segmentIndex,
    planGeneration: input.planGeneration,
    outputDir: input.outputDir,
    filePrefix: `segment-${input.segment.segmentIndex}`,
    partials,
  });
  if (Result.isError(mergeRes)) return invalidCompanionBuild(mergeRes.error.message);
  return Result.ok({
    storage: "file",
    localPath: mergeRes.value.localPath,
    sizeBytes: mergeRes.value.sizeBytes,
    sectionKinds: mergeRes.value.sectionKinds,
    sectionSizes: mergeRes.value.sectionSizes,
    primaryTimestampMinMs: mergeRes.value.primaryTimestampMinMs,
    primaryTimestampMaxMs: mergeRes.value.primaryTimestampMaxMs,
  });
}

function compareValues(left: bigint | number | boolean, right: bigint | number | boolean): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  return compareSearchStrings(String(left), String(right));
}

type ColumnRecordValue = {
  count: number;
  value: ColScalar | null;
};

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
  for (let index = 0; index < groupKey.length; index += 1) {
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
  for (let index = 0; index < dimensionNames.length; index += 1) {
    decoded[dimensionNames[index]!] = decodeAggDimensionPart(parts[index] ?? AGG_DIMENSION_NULL);
  }
  return decoded;
}

function createColBuilders(registry: SchemaRegistry): Map<string, ColumnFieldBuilder> {
  const columnFields = Object.entries(registry.search?.fields ?? {}).filter(([, field]) => field.column === true);
  const builders = new Map<string, ColumnFieldBuilder>();
  for (const [fieldName, field] of columnFields) {
    builders.set(fieldName, { config: field, kind: field.kind, docIds: [], values: [], invalid: false });
  }
  return builders;
}

function recordColBuilders(builders: Map<string, ColumnFieldBuilder>, rawSearchValues: Map<string, unknown[]>, docCount: number): void {
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

function finalizeColSection(registry: SchemaRegistry, builders: Map<string, ColumnFieldBuilder>, docCount: number): ColSectionInput {
  const fields: Record<string, ColFieldInput> = {};
  const primaryTimestampField = registry.search?.primaryTimestampField;
  for (const [fieldName, builder] of builders) {
    if (builder.invalid || builder.values.length === 0) continue;
    let minValue: bigint | number | boolean | null = null;
    let maxValue: bigint | number | boolean | null = null;
    for (const value of builder.values) {
      if (minValue == null || compareValues(value, minValue) < 0) minValue = value;
      if (maxValue == null || compareValues(value, maxValue) > 0) maxValue = value;
    }
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

function createFtsFieldBuilder(field: SearchFieldConfig): FtsFieldBuilder {
  return {
    config: field,
    companion: {
      kind: field.kind,
      exact: field.exact === true ? true : undefined,
      prefix: field.prefix === true ? true : undefined,
      positions: field.positions === true ? true : undefined,
      exists_docs: [],
      _exists_prefix_count: 0,
      terms: Object.create(null) as Record<string, FtsTermInput>,
    },
    tokenCache: new Map<string, string[]>(),
  };
}

function recordFtsFieldExists(fieldCompanion: FtsFieldInput, docCount: number): void {
  const prefixCount = fieldCompanion._exists_prefix_count;
  if (typeof prefixCount === "number") {
    if (prefixCount === docCount) {
      fieldCompanion._exists_prefix_count = prefixCount + 1;
      return;
    }
    fieldCompanion.exists_docs = Array.from({ length: prefixCount }, (_, index) => index);
    delete fieldCompanion._exists_prefix_count;
  }
  fieldCompanion.exists_docs.push(docCount);
}

function createFtsBuilders(registry: SchemaRegistry): Map<string, FtsFieldBuilder> {
  const builders = new Map<string, FtsFieldBuilder>();
  for (const [fieldName, field] of Object.entries(registry.search?.fields ?? {}).sort((a, b) => compareSearchStrings(a[0], b[0]))) {
    if (field.kind !== "text" && !(field.kind === "keyword" && field.prefix === true)) continue;
    builders.set(fieldName, createFtsFieldBuilder(field));
  }
  return builders;
}

function recordFtsBuilders(
  builders: Map<string, FtsFieldBuilder>,
  rawSearchValues: Map<string, unknown[]>,
  docCount: number,
  keywordTermShard: CompanionBuildInput["keywordTermShard"]
): void {
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
    recordFtsFieldExists(fieldCompanion, docCount);
    if (builder.config.kind === "keyword") {
      for (const value of textValues) {
        if (!termBelongsToShard(value, keywordTermShard)) continue;
        fieldCompanion.terms[value] = appendKeywordPostingDoc(fieldCompanion.terms[value], docCount);
      }
      continue;
    }
    let recordedExists = false;
    let position = 0;
    for (const value of textValues) {
      const tokenPositions = new Map<string, number[]>();
      const tokens = analyzeTextValueCached(value, builder.config.analyzer, builder.tokenCache);
      for (const token of tokens) {
        const includeToken = termBelongsToShard(token, keywordTermShard);
        const positions = tokenPositions.get(token);
        if (includeToken) {
          if (positions) positions.push(position);
          else tokenPositions.set(token, [position]);
        }
        position += 1;
      }
      if (tokenPositions.size > 0 && !recordedExists) {
        recordFtsFieldExists(fieldCompanion, docCount);
        recordedExists = true;
      }
      for (const [token, positions] of tokenPositions) {
        fieldCompanion.terms[token] = appendTextPostingPositions(fieldCompanion.terms[token], docCount, positions);
      }
    }
  }
}

function finalizeFtsSection(builders: Map<string, FtsFieldBuilder>, docCount: number): FtsSectionInput {
  const orderedFields = Object.create(null) as Record<string, FtsFieldInput>;
  for (const [fieldName, builder] of Array.from(builders.entries()).sort((a, b) => compareSearchStrings(a[0], b[0]))) {
    orderedFields[fieldName] = builder.companion;
  }
  return {
    doc_count: docCount,
    fields: orderedFields,
  };
}

function parseRollupIntervalsResult(rollup: SearchRollupConfig): Result<number[], CompanionBuildError> {
  const parsed: number[] = [];
  for (const interval of rollup.intervals) {
    const intervalMsRes = parseDurationMsResult(interval);
    if (Result.isError(intervalMsRes)) return invalidCompanionBuild(intervalMsRes.error.message);
    parsed.push(intervalMsRes.value);
  }
  return Result.ok(parsed);
}

function createAggRollupBuildersResult(registry: SchemaRegistry): Result<Map<string, AggRollupBuilder>, CompanionBuildError> {
  const builders = new Map<string, AggRollupBuilder>();
  for (const [rollupName, rollup] of Object.entries(registry.search?.rollups ?? {}).sort((a, b) => compareSearchStrings(a[0], b[0]))) {
    const parsedIntervalsRes = parseRollupIntervalsResult(rollup);
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

function recordAggContributionResult(
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
      groups.set(groupKey, { key: groupKey, measures });
      continue;
    }
    for (const [measureName, state] of Object.entries(contribution.measures)) {
      const existing = group.measures[measureName];
      group.measures[measureName] = existing ? mergeAggMeasureState(existing, state) : cloneAggMeasureState(state);
    }
  }
  return Result.ok(undefined);
}

function finalizeAggIntervals(
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

function finalizeAggSection(builders: Map<string, AggRollupBuilder>): AggSectionInput {
  const encodedRollups: AggSectionInput["rollups"] = {};
  for (const [rollupName, builder] of builders) {
    encodedRollups[rollupName] = { intervals: finalizeAggIntervals(builder.intervalMap, builder.dimensionNames) };
  }
  return { rollups: encodedRollups };
}

function finalizeMetricsBlockSection(builder: MetricsBlockBuilder): MetricsBlockSectionInput {
  return {
    record_count: builder.records.length,
    min_window_start_ms: builder.minWindowStartMs,
    max_window_end_ms: builder.maxWindowEndMs,
    records: builder.records,
  };
}

function recordMetricsBlockBuilder(builder: MetricsBlockBuilder, parsed: unknown, docCount: number): void {
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

function createFastRecordState(): FastRecordState {
  return {
    colRecordValues: new Map<string, ColumnRecordValue>(),
    ftsSeenFields: new Set<string>(),
    ftsPositions: new Map<string, number>(),
  };
}

function recordFastColumnAndFtsValue(
  state: FastRecordState,
  fieldName: string,
  rawValue: unknown,
  colBuilders: Map<string, ColumnFieldBuilder>,
  ftsBuilders: Map<string, FtsFieldBuilder>,
  docCount: number,
  keywordTermShard: CompanionBuildInput["keywordTermShard"]
): void {
  const colBuilder = colBuilders.get(fieldName);
  if (colBuilder && !colBuilder.invalid) {
    const normalized = canonicalizeColumnValue(colBuilder.config, rawValue);
    if (normalized != null) {
      const current = state.colRecordValues.get(fieldName);
      if (!current) {
        state.colRecordValues.set(fieldName, { count: 1, value: normalized });
      } else {
        current.count += 1;
      }
    }
  }

  const ftsBuilder = ftsBuilders.get(fieldName);
  if (!ftsBuilder) return;
  const fieldCompanion = ftsBuilder.companion;
  if (ftsBuilder.config.kind === "keyword") {
    const normalized = normalizeKeywordValue(rawValue, ftsBuilder.config.normalizer);
    if (normalized == null) return;
    if (!termBelongsToShard(normalized, keywordTermShard)) return;
    if (!state.ftsSeenFields.has(fieldName)) {
      recordFtsFieldExists(fieldCompanion, docCount);
      state.ftsSeenFields.add(fieldName);
    }
    fieldCompanion.terms[normalized] = appendKeywordPostingDoc(fieldCompanion.terms[normalized], docCount);
    return;
  }
  if (typeof rawValue !== "string") return;
  if (!state.ftsSeenFields.has(fieldName)) {
    recordFtsFieldExists(fieldCompanion, docCount);
    state.ftsSeenFields.add(fieldName);
  }
  const tokenPositions = new Map<string, number[]>();
  let position = state.ftsPositions.get(fieldName) ?? 0;
  const tokens = analyzeTextValueCached(rawValue, ftsBuilder.config.analyzer, ftsBuilder.tokenCache);
  for (const token of tokens) {
    if (termBelongsToShard(token, keywordTermShard)) {
      const positions = tokenPositions.get(token);
      if (positions) positions.push(position);
      else tokenPositions.set(token, [position]);
    }
    position += 1;
  }
  if (tokenPositions.size === 0) {
    state.ftsPositions.set(fieldName, position);
    return;
  }
  for (const [token, positions] of tokenPositions) {
    fieldCompanion.terms[token] = appendTextPostingPositions(fieldCompanion.terms[token], docCount, positions);
  }
  state.ftsPositions.set(fieldName, position);
}

function finalizeFastRecordState(
  state: FastRecordState,
  colBuilders: Map<string, ColumnFieldBuilder>,
  docCount: number
): void {
  for (const [fieldName, recordValue] of state.colRecordValues) {
    const builder = colBuilders.get(fieldName);
    if (!builder || builder.invalid) continue;
    if (recordValue.count > 1) {
      builder.invalid = true;
      continue;
    }
    if (recordValue.value != null) {
      builder.docIds.push(docCount);
      builder.values.push(recordValue.value);
    }
  }
}

function recordFastPathColumnAndFtsBuildersResult(
  parsed: unknown,
  accessors: ReadonlyArray<CompiledSearchFieldAccessor>,
  colBuilders: Map<string, ColumnFieldBuilder>,
  ftsBuilders: Map<string, FtsFieldBuilder>,
  docCount: number,
  keywordTermShard: CompanionBuildInput["keywordTermShard"]
): Result<void, CompanionBuildError> {
  const state = createFastRecordState();
  const visitRes = visitRawSearchValuesWithCompiledAccessorsResult(parsed, accessors, (accessor, rawValue) => {
    recordFastColumnAndFtsValue(state, accessor.fieldName, rawValue, colBuilders, ftsBuilders, docCount, keywordTermShard);
  });
  if (Result.isError(visitRes)) return invalidCompanionBuild(visitRes.error.message);
  finalizeFastRecordState(state, colBuilders, docCount);
  return Result.ok(undefined);
}

type FastByteScanPlan = {
  trie: FastScalarAccessorTrie;
};

function appendEvlogColumnValue(builder: ColumnFieldBuilder | undefined, rawValue: unknown, docCount: number): void {
  if (!builder || builder.invalid) return;
  const normalized = canonicalizeColumnValue(builder.config, rawValue);
  if (normalized == null) return;
  builder.docIds.push(docCount);
  builder.values.push(normalized);
}

function appendEvlogKeywordFtsValue(
  builder: FtsFieldBuilder | undefined,
  rawValue: unknown,
  docCount: number,
  keywordTermShard: CompanionBuildInput["keywordTermShard"]
): void {
  if (!builder) return;
  const normalized = normalizeKeywordValue(rawValue, builder.config.normalizer);
  if (normalized == null) return;
  if (!termBelongsToShard(normalized, keywordTermShard)) return;
  builder.companion.exists_docs.push(docCount);
  builder.companion.terms[normalized] = appendKeywordPostingDoc(builder.companion.terms[normalized], docCount);
}

function appendEvlogTextFtsValue(
  builder: FtsFieldBuilder | undefined,
  rawValue: unknown,
  docCount: number,
  keywordTermShard: CompanionBuildInput["keywordTermShard"]
): void {
  if (!builder || typeof rawValue !== "string") return;
  const fieldCompanion = builder.companion;
  const tokenPositions = new Map<string, number[]>();
  const tokens = analyzeTextValueCached(rawValue, builder.config.analyzer, builder.tokenCache);
  for (let position = 0; position < tokens.length; position += 1) {
    const token = tokens[position]!;
    if (termBelongsToShard(token, keywordTermShard)) {
      const positions = tokenPositions.get(token);
      if (positions) positions.push(position);
      else tokenPositions.set(token, [position]);
    }
  }
  if (tokenPositions.size === 0) return;
  fieldCompanion.exists_docs.push(docCount);
  for (const [token, positions] of tokenPositions) {
    fieldCompanion.terms[token] = appendTextPostingPositions(fieldCompanion.terms[token], docCount, positions);
  }
}

function recordEvlogByteScanColumnAndFtsBuildersResult(
  payload: Uint8Array,
  plan: FastByteScanPlan,
  colBuilders: Map<string, ColumnFieldBuilder>,
  ftsBuilders: Map<string, FtsFieldBuilder>,
  docCount: number,
  keywordTermShard: CompanionBuildInput["keywordTermShard"]
): Result<void, CompanionBuildError> {
  const visitRes = visitFastScalarJsonValuesFromBytesWithTrieResult(payload, plan.trie, (accessor, rawValue) => {
    switch (accessor.fieldName) {
      case "timestamp":
      case "status":
      case "duration":
        appendEvlogColumnValue(colBuilders.get(accessor.fieldName), rawValue, docCount);
        return;
      case "level":
      case "service":
      case "environment":
      case "requestId":
      case "traceId":
      case "spanId":
      case "path":
      case "method":
        appendEvlogKeywordFtsValue(ftsBuilders.get(accessor.fieldName), rawValue, docCount, keywordTermShard);
        return;
      case "message":
      case "why":
      case "fix":
      case "error.message":
        appendEvlogTextFtsValue(ftsBuilders.get(accessor.fieldName), rawValue, docCount, keywordTermShard);
        return;
      default:
        return;
    }
  });
  if (Result.isError(visitRes)) return invalidCompanionBuild(visitRes.error.message);
  return Result.ok(undefined);
}

export function buildEncodedBundledCompanionPayloadResult(input: CompanionBuildInput): Result<CompanionBuildOutput, CompanionBuildError> {
  const splitRes = maybeBuildSplitEvlogCompanionResult(input);
  if (splitRes) return splitRes;
  const colBuilders = input.plan.families.col ? createColBuilders(input.registry) : new Map<string, ColumnFieldBuilder>();
  const ftsBuilders = input.plan.families.fts ? createFtsBuilders(input.registry) : new Map<string, FtsFieldBuilder>();
  const aggBuildersRes = input.plan.families.agg ? createAggRollupBuildersResult(input.registry) : Result.ok(new Map<string, AggRollupBuilder>());
  if (Result.isError(aggBuildersRes)) return aggBuildersRes;
  const aggBuilders = aggBuildersRes.value;
  const metricsBuilder: MetricsBlockBuilder | null = input.plan.families.mblk
    ? { records: [], minWindowStartMs: undefined, maxWindowEndMs: undefined }
    : null;
  const requiredFieldNames = new Set<string>();
  for (const fieldName of colBuilders.keys()) requiredFieldNames.add(fieldName);
  for (const fieldName of ftsBuilders.keys()) requiredFieldNames.add(fieldName);
  for (const builder of aggBuilders.values()) {
    for (const fieldName of builder.fieldNames) requiredFieldNames.add(fieldName);
  }
  const fieldNameList = Array.from(requiredFieldNames).sort(compareSearchStrings);
  const compiledByVersion = new Map<number, CompiledSearchFieldAccessor[]>();
  const fastByteScanPlanByVersion = new Map<number, FastByteScanPlan>();
  const canUseFastPath = aggBuilders.size === 0;
  const canUseEvlogByteFastPath = canUseFastPath && !metricsBuilder && input.registry.search?.profile === "evlog";
  let docCount = 0;
  let offset = input.segment.startOffset;
  for (const recRes of iterateBlockRecordsFromFileResult(input.segment.localPath)) {
    if (Result.isError(recRes)) return invalidCompanionBuild(recRes.error.message);
    let parsed: unknown = null;
    let parsedOk = false;
    let rawSearchValues: Map<string, unknown[]> | null = null;
    if (canUseEvlogByteFastPath && fieldNameList.length > 0) {
      if (!docBelongsToShard(docCount, input.docIdShard)) {
        offset += 1n;
        docCount += 1;
        continue;
      }
      const version = schemaVersionForOffset(input.registry, offset);
      let plan = fastByteScanPlanByVersion.get(version);
      if (!plan) {
        const compiledRes = compileSearchFieldAccessorsResult(input.registry, fieldNameList, version);
        if (Result.isError(compiledRes)) return invalidCompanionBuild(compiledRes.error.message);
        const trieRes = buildFastScalarAccessorTrieResult(compiledRes.value);
        if (Result.isError(trieRes)) return invalidCompanionBuild(trieRes.error.message);
        plan = { trie: trieRes.value };
        compiledByVersion.set(version, compiledRes.value);
        fastByteScanPlanByVersion.set(version, plan);
      }
      const fastScanRes = recordEvlogByteScanColumnAndFtsBuildersResult(
        recRes.value.payload,
        plan,
        colBuilders,
        ftsBuilders,
        docCount,
        input.keywordTermShard
      );
      if (Result.isError(fastScanRes)) return fastScanRes;
    } else {
      if (!docBelongsToShard(docCount, input.docIdShard)) {
        offset += 1n;
        docCount += 1;
        continue;
      }
      try {
        parsed = JSON.parse(PAYLOAD_DECODER.decode(recRes.value.payload));
        parsedOk = true;
      } catch {
        parsed = null;
      }
      if (parsedOk && fieldNameList.length > 0) {
        const version = schemaVersionForOffset(input.registry, offset);
        let accessors = compiledByVersion.get(version);
        if (!accessors) {
          const compiledRes = compileSearchFieldAccessorsResult(input.registry, fieldNameList, version);
          if (Result.isError(compiledRes)) return invalidCompanionBuild(compiledRes.error.message);
          accessors = compiledRes.value;
          compiledByVersion.set(version, accessors);
        }
        if (canUseFastPath) {
          const fastPathRes = recordFastPathColumnAndFtsBuildersResult(
            parsed,
            accessors,
            colBuilders,
            ftsBuilders,
            docCount,
            input.keywordTermShard
          );
          if (Result.isError(fastPathRes)) return fastPathRes;
        } else {
          const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, accessors);
          if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
          rawSearchValues = rawValuesRes.value;
        }
      }
      if (!canUseFastPath && rawSearchValues) {
        recordColBuilders(colBuilders, rawSearchValues, docCount);
        recordFtsBuilders(ftsBuilders, rawSearchValues, docCount, input.keywordTermShard);
      }
      if (parsedOk && rawSearchValues) {
        for (const builder of aggBuilders.values()) {
          const contributionRes = extractRollupContributionResult(input.registry, builder.rollup, offset, parsed, rawSearchValues);
          if (Result.isError(contributionRes)) return invalidCompanionBuild(contributionRes.error.message);
          if (!contributionRes.value) continue;
          const recordRes = recordAggContributionResult(builder, contributionRes.value);
          if (Result.isError(recordRes)) return recordRes;
        }
      }
      if (metricsBuilder && parsedOk) {
        recordMetricsBlockBuilder(metricsBuilder, parsed, docCount);
      }
    }
    offset += 1n;
    docCount += 1;
  }

  const sectionPayloads: EncodedCompanionSectionPayload[] = [];
  const sectionChunkPayloads: EncodedCompanionSectionChunkPayload[] = [];
  const sectionKinds: CompanionSectionKind[] = [];
  const sectionSizes: Record<string, number> = {};
  let primaryTimestampMinMs: bigint | null = null;
  let primaryTimestampMaxMs: bigint | null = null;
  const addSection = (payload: EncodedCompanionSectionPayload): void => {
    sectionPayloads.push(payload);
    sectionKinds.push(payload.kind);
    sectionSizes[payload.kind] = payload.payload.byteLength;
  };
  const addChunkSection = (payload: EncodedCompanionSectionChunkPayload): void => {
    sectionChunkPayloads.push(payload);
    sectionKinds.push(payload.kind);
    sectionSizes[payload.kind] = payload.sizeBytes;
  };

  if (input.plan.families.col) {
    const colSection = finalizeColSection(input.registry, colBuilders, docCount);
    const primaryTimestampField = colSection.primary_timestamp_field;
    const primaryTimestampColumn = primaryTimestampField ? colSection.fields[primaryTimestampField] : undefined;
    primaryTimestampMinMs = typeof primaryTimestampColumn?.min === "bigint" ? primaryTimestampColumn.min : null;
    primaryTimestampMaxMs = typeof primaryTimestampColumn?.max === "bigint" ? primaryTimestampColumn.max : null;
    if (input.outputDir) addChunkSection(encodeCompanionSectionChunkPayload("col", colSection, input.plan));
    else addSection(encodeCompanionSectionPayload("col", colSection, input.plan));
  }
  if (input.plan.families.fts) {
    const ftsSection = finalizeFtsSection(ftsBuilders, docCount);
    if (input.outputDir) addChunkSection(encodeCompanionSectionChunkPayload("fts", ftsSection, input.plan));
    else addSection(encodeCompanionSectionPayload("fts", ftsSection, input.plan));
  }
  if (input.plan.families.agg) {
    const aggSection = finalizeAggSection(aggBuilders);
    if (input.outputDir) addChunkSection(encodeCompanionSectionChunkPayload("agg", aggSection, input.plan));
    else addSection(encodeCompanionSectionPayload("agg", aggSection, input.plan));
  }
  if (input.plan.families.mblk && metricsBuilder) {
    const metricsSection = finalizeMetricsBlockSection(metricsBuilder);
    if (input.outputDir) addChunkSection(encodeCompanionSectionChunkPayload("mblk", metricsSection, input.plan));
    else addSection(encodeCompanionSectionPayload("mblk", metricsSection, input.plan));
  }
  if (input.outputDir) {
    const chunkSet = encodeBundledSegmentCompanionChunksFromPayloads({
      stream: input.segment.stream,
      segment_index: input.segment.segmentIndex,
      plan_generation: input.planGeneration,
      sections: sectionChunkPayloads,
    });
    const writeRes = writeCompanionOutputChunksResult(input.outputDir, `segment-${input.segment.segmentIndex}`, chunkSet.chunks, chunkSet.sizeBytes);
    if (Result.isError(writeRes)) return invalidCompanionBuild(writeRes.error.message);
    return Result.ok({
      storage: "file",
      localPath: writeRes.value.localPath,
      sizeBytes: writeRes.value.sizeBytes,
      sectionKinds,
      sectionSizes,
      primaryTimestampMinMs,
      primaryTimestampMaxMs,
    });
  }
  const payload = encodeBundledSegmentCompanionFromPayloads({
    stream: input.segment.stream,
    segment_index: input.segment.segmentIndex,
    plan_generation: input.planGeneration,
    sections: sectionPayloads,
  });
  return Result.ok({
    storage: "bytes",
    payload,
    sectionKinds,
    sectionSizes,
    primaryTimestampMinMs,
    primaryTimestampMaxMs,
  });
}
