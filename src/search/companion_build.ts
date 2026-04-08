import { mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Result } from "better-result";
import type { SchemaRegistry, SearchFieldConfig, SearchRollupConfig } from "../schema/registry";
import { iterateBlockRecordsResult } from "../segment/format";
import {
  encodeBundledSegmentCompanionFromPayloads,
  encodeCompanionSectionPayload,
  type CompanionSectionKind,
  type EncodedCompanionSectionPayload,
} from "./companion_format";
import type { SearchCompanionPlan } from "./companion_plan";
import type { ColFieldInput, ColScalar, ColSectionInput } from "./col_format";
import { appendKeywordPostingDoc, type FtsFieldInput, type FtsSectionInput, type FtsTermInput } from "./fts_format";
import {
  buildFastScalarAccessorTrieResult,
  canonicalizeColumnValue,
  compareSearchStrings,
  compileSearchFieldAccessorsResult,
  extractRawSearchValuesWithCompiledAccessorsResult,
  normalizeKeywordValue,
  type FastScalarAccessorTrie,
  visitAnalyzedTextValue,
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

const PAYLOAD_DECODER = new TextDecoder();
const AGG_DIMENSION_SEPARATOR = "\u001f";
const AGG_DIMENSION_NULL = "\u0000";

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

function invalidCompanionBuild(message: string): Result<never, CompanionBuildError> {
  return Result.err({ kind: "invalid_companion_build", message });
}

function buildCompanionOutputFilePath(outputDir: string, segmentIndex: number): string {
  const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
  return join(outputDir, `segment-${segmentIndex}-${nonce}.cix`);
}

function writeCompanionOutputFileResult(
  outputDir: string,
  segmentIndex: number,
  payload: Uint8Array
): Result<{ localPath: string; sizeBytes: number }, CompanionBuildError> {
  mkdirSync(outputDir, { recursive: true });
  const finalPath = buildCompanionOutputFilePath(outputDir, segmentIndex);
  const tmpPath = `${finalPath}.tmp`;
  try {
    writeFileSync(tmpPath, payload);
    renameSync(tmpPath, finalPath);
    return Result.ok({ localPath: finalPath, sizeBytes: payload.byteLength });
  } catch (error: unknown) {
    try {
      unlinkSync(tmpPath);
    } catch {
      // ignore temp cleanup failures
    }
    return invalidCompanionBuild(String((error as any)?.message ?? error));
  }
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
      terms: Object.create(null) as Record<string, FtsTermInput>,
    },
  };
}

function createFtsBuilders(registry: SchemaRegistry): Map<string, FtsFieldBuilder> {
  const builders = new Map<string, FtsFieldBuilder>();
  for (const [fieldName, field] of Object.entries(registry.search?.fields ?? {}).sort((a, b) => compareSearchStrings(a[0], b[0]))) {
    if (field.kind !== "text" && !(field.kind === "keyword" && field.prefix === true)) continue;
    builders.set(fieldName, createFtsFieldBuilder(field));
  }
  return builders;
}

function recordFtsBuilders(builders: Map<string, FtsFieldBuilder>, rawSearchValues: Map<string, unknown[]>, docCount: number): void {
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
        fieldCompanion.terms[value] = appendKeywordPostingDoc(fieldCompanion.terms[value], docCount);
      }
      continue;
    }
    let position = 0;
    for (const value of textValues) {
      visitAnalyzedTextValue(value, builder.config.analyzer, (token) => {
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
      });
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
  docCount: number
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
    if (!state.ftsSeenFields.has(fieldName)) {
      fieldCompanion.exists_docs.push(docCount);
      state.ftsSeenFields.add(fieldName);
    }
    fieldCompanion.terms[normalized] = appendKeywordPostingDoc(fieldCompanion.terms[normalized], docCount);
    return;
  }
  if (typeof rawValue !== "string") return;
  if (!state.ftsSeenFields.has(fieldName)) {
    fieldCompanion.exists_docs.push(docCount);
    state.ftsSeenFields.add(fieldName);
  }
  let position = state.ftsPositions.get(fieldName) ?? 0;
  visitAnalyzedTextValue(rawValue, ftsBuilder.config.analyzer, (token) => {
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
  });
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
  docCount: number
): Result<void, CompanionBuildError> {
  const state = createFastRecordState();
  const visitRes = visitRawSearchValuesWithCompiledAccessorsResult(parsed, accessors, (accessor, rawValue) => {
    recordFastColumnAndFtsValue(state, accessor.fieldName, rawValue, colBuilders, ftsBuilders, docCount);
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

function appendEvlogKeywordFtsValue(builder: FtsFieldBuilder | undefined, rawValue: unknown, docCount: number): void {
  if (!builder) return;
  const normalized = normalizeKeywordValue(rawValue, builder.config.normalizer);
  if (normalized == null) return;
  builder.companion.exists_docs.push(docCount);
  builder.companion.terms[normalized] = appendKeywordPostingDoc(builder.companion.terms[normalized], docCount);
}

function appendEvlogTextFtsValue(builder: FtsFieldBuilder | undefined, rawValue: unknown, docCount: number): void {
  if (!builder || typeof rawValue !== "string") return;
  const fieldCompanion = builder.companion;
  fieldCompanion.exists_docs.push(docCount);
  let position = 0;
  visitAnalyzedTextValue(rawValue, builder.config.analyzer, (token) => {
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
  });
}

function recordEvlogByteScanColumnAndFtsBuildersResult(
  payload: Uint8Array,
  plan: FastByteScanPlan,
  colBuilders: Map<string, ColumnFieldBuilder>,
  ftsBuilders: Map<string, FtsFieldBuilder>,
  docCount: number
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
        appendEvlogKeywordFtsValue(ftsBuilders.get(accessor.fieldName), rawValue, docCount);
        return;
      case "message":
      case "why":
      case "fix":
      case "error.message":
        appendEvlogTextFtsValue(ftsBuilders.get(accessor.fieldName), rawValue, docCount);
        return;
      default:
        return;
    }
  });
  if (Result.isError(visitRes)) return invalidCompanionBuild(visitRes.error.message);
  return Result.ok(undefined);
}

export function buildEncodedBundledCompanionPayloadResult(input: CompanionBuildInput): Result<CompanionBuildOutput, CompanionBuildError> {
  const segmentBytes = readFileSync(input.segment.localPath);
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
  for (const recRes of iterateBlockRecordsResult(segmentBytes)) {
    if (Result.isError(recRes)) return invalidCompanionBuild(recRes.error.message);
    let parsed: unknown = null;
    let parsedOk = false;
    let rawSearchValues: Map<string, unknown[]> | null = null;
    if (canUseEvlogByteFastPath && fieldNameList.length > 0) {
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
      const fastScanRes = recordEvlogByteScanColumnAndFtsBuildersResult(recRes.value.payload, plan, colBuilders, ftsBuilders, docCount);
      if (Result.isError(fastScanRes)) return fastScanRes;
    } else {
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
          const fastPathRes = recordFastPathColumnAndFtsBuildersResult(parsed, accessors, colBuilders, ftsBuilders, docCount);
          if (Result.isError(fastPathRes)) return fastPathRes;
        } else {
          const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, accessors);
          if (Result.isError(rawValuesRes)) return invalidCompanionBuild(rawValuesRes.error.message);
          rawSearchValues = rawValuesRes.value;
        }
      }
      if (!canUseFastPath && rawSearchValues) {
        recordColBuilders(colBuilders, rawSearchValues, docCount);
        recordFtsBuilders(ftsBuilders, rawSearchValues, docCount);
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
  const sectionKinds: CompanionSectionKind[] = [];
  const sectionSizes: Record<string, number> = {};
  let primaryTimestampMinMs: bigint | null = null;
  let primaryTimestampMaxMs: bigint | null = null;
  const addSection = (payload: EncodedCompanionSectionPayload): void => {
    sectionPayloads.push(payload);
    sectionKinds.push(payload.kind);
    sectionSizes[payload.kind] = payload.payload.byteLength;
  };

  if (input.plan.families.col) {
    const colSection = finalizeColSection(input.registry, colBuilders, docCount);
    const primaryTimestampField = colSection.primary_timestamp_field;
    const primaryTimestampColumn = primaryTimestampField ? colSection.fields[primaryTimestampField] : undefined;
    primaryTimestampMinMs = typeof primaryTimestampColumn?.min === "bigint" ? primaryTimestampColumn.min : null;
    primaryTimestampMaxMs = typeof primaryTimestampColumn?.max === "bigint" ? primaryTimestampColumn.max : null;
    addSection(encodeCompanionSectionPayload("col", colSection, input.plan));
  }
  if (input.plan.families.fts) {
    addSection(encodeCompanionSectionPayload("fts", finalizeFtsSection(ftsBuilders, docCount), input.plan));
  }
  if (input.plan.families.agg) {
    addSection(encodeCompanionSectionPayload("agg", finalizeAggSection(aggBuilders), input.plan));
  }
  if (input.plan.families.mblk && metricsBuilder) {
    addSection(encodeCompanionSectionPayload("mblk", finalizeMetricsBlockSection(metricsBuilder), input.plan));
  }

  const payload = encodeBundledSegmentCompanionFromPayloads({
    stream: input.segment.stream,
    segment_index: input.segment.segmentIndex,
    plan_generation: input.planGeneration,
    sections: sectionPayloads,
  });
  if (input.outputDir) {
    const writeRes = writeCompanionOutputFileResult(input.outputDir, input.segment.segmentIndex, payload);
    if (Result.isError(writeRes)) return writeRes;
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
  return Result.ok({
    storage: "bytes",
    payload,
    sectionKinds,
    sectionSizes,
    primaryTimestampMinMs,
    primaryTimestampMaxMs,
  });
}
