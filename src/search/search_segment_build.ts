import { readFileSync } from "node:fs";
import { Result } from "better-result";
import type { SchemaRegistry, SearchFieldConfig } from "../schema/registry";
import { schemaVersionForOffset } from "../schema/read_json";
import { iterateBlockRecordsResult } from "../segment/format";
import type { SearchCompanionPlan } from "./companion_plan";
import type { ColFieldInput, ColScalar, ColSectionInput } from "./col_format";
import {
  encodeBundledSegmentCompanionFromPayloads,
  encodeCompanionSectionPayload,
  type CompanionSectionKind,
  type EncodedCompanionSectionPayload,
} from "./companion_format";
import type { FtsFieldInput, FtsSectionInput, FtsTermInput } from "./fts_format";
import { appendKeywordPostingDoc } from "./fts_format";
import {
  buildFastScalarAccessorTrieResult,
  canonicalizeColumnValue,
  canonicalizeExactValue,
  compareSearchStrings,
  compileSearchFieldAccessorsResult,
  normalizeKeywordValue,
  visitAnalyzedTextValue,
  visitFastScalarJsonValuesFromBytesWithTrieResult,
  type FastScalarAccessorTrie,
} from "./schema";
import { siphash24 } from "../util/siphash";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { encodeIndexRunResult, RUN_TYPE_MASK16 } from "../index/run_format";
import { getConfiguredSecondaryIndexes, hashSecondaryIndexField, type SecondaryIndexField } from "../index/secondary_schema";

type SearchSegmentBuildError = {
  kind: "invalid_search_segment_build";
  message: string;
};

export type SearchSegmentExactBuildInput = {
  index: SecondaryIndexField;
  secret: Uint8Array;
};

type ExactFieldState = {
  index: SecondaryIndexField;
  secret: Uint8Array;
  canonicalTerms: Set<string>;
};

type ColumnFieldBuilder = {
  config: SearchFieldConfig;
  kind: ColFieldInput["kind"];
  docIds: number[];
  values: ColScalar[];
};

type FtsFieldBuilder = {
  config: SearchFieldConfig;
  companion: FtsFieldInput;
};

type FastPlan = {
  trie: FastScalarAccessorTrie;
};

export type SearchSegmentBuildInput = {
  stream: string;
  registry: SchemaRegistry;
  exactIndexes: SearchSegmentExactBuildInput[];
  plan: SearchCompanionPlan | null;
  planGeneration: number | null;
  segment: {
    segmentIndex: number;
    startOffset: bigint;
    localPath: string;
  };
};

export function collectUnifiedSearchBuildExactIndexes(
  registry: SchemaRegistry,
  resolveState: (indexName: string) => { configHash: string; secret: Uint8Array } | null
): SearchSegmentExactBuildInput[] {
  if (registry.search?.profile !== "evlog") return [];
  return getConfiguredSecondaryIndexes(registry)
    .map((configuredIndex) => {
      const state = resolveState(configuredIndex.name);
      if (!state) return null;
      if (state.configHash !== hashSecondaryIndexField(configuredIndex)) return null;
      return {
        index: configuredIndex,
        secret: state.secret,
      };
    })
    .filter((entry): entry is SearchSegmentExactBuildInput => entry != null);
}

export type SearchSegmentBuildOutput = {
  exactRuns: Array<{
    indexName: string;
    meta: {
      runId: string;
      level: number;
      startSegment: number;
      endSegment: number;
      objectKey: string;
      filterLen: number;
      recordCount: number;
    };
    storage: "bytes";
    payload: Uint8Array;
  }>;
  companion:
    | null
    | {
        storage: "bytes";
        payload: Uint8Array;
        sectionKinds: CompanionSectionKind[];
        sectionSizes: Record<string, number>;
        primaryTimestampMinMs: bigint | null;
        primaryTimestampMaxMs: bigint | null;
      };
};

function invalidSearchSegmentBuild<T = never>(message: string): Result<T, SearchSegmentBuildError> {
  return Result.err({ kind: "invalid_search_segment_build", message });
}

function compareValues(left: bigint | number | boolean, right: bigint | number | boolean): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  return String(left).localeCompare(String(right));
}

function createColBuilders(registry: SchemaRegistry, plan: SearchCompanionPlan | null): Map<string, ColumnFieldBuilder> {
  const builders = new Map<string, ColumnFieldBuilder>();
  if (!plan?.families.col) return builders;
  for (const field of plan.fields.filter((entry) => entry.column).sort((a, b) => compareSearchStrings(a.name, b.name))) {
    const config = registry.search?.fields[field.name];
    if (!config) continue;
    builders.set(field.name, { config, kind: config.kind, docIds: [], values: [] });
  }
  return builders;
}

function createFtsBuilders(registry: SchemaRegistry, plan: SearchCompanionPlan | null): Map<string, FtsFieldBuilder> {
  const builders = new Map<string, FtsFieldBuilder>();
  if (!plan?.families.fts) return builders;
  for (const field of plan.fields
    .filter((entry) => entry.kind === "text" || (entry.kind === "keyword" && entry.prefix))
    .sort((a, b) => compareSearchStrings(a.name, b.name))) {
    const config = registry.search?.fields[field.name];
    if (!config) continue;
    builders.set(field.name, {
      config,
      companion: {
        kind: config.kind,
        exact: config.exact === true ? true : undefined,
        prefix: config.prefix === true ? true : undefined,
        positions: config.positions === true ? true : undefined,
        exists_docs: [],
        terms: Object.create(null) as Record<string, FtsTermInput>,
      },
    });
  }
  return builders;
}

function finalizeColSection(registry: SchemaRegistry, builders: Map<string, ColumnFieldBuilder>, docCount: number): ColSectionInput {
  const fields: Record<string, ColFieldInput> = {};
  const primaryTimestampField = registry.search?.primaryTimestampField;
  for (const [fieldName, builder] of builders) {
    if (builder.values.length === 0) continue;
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

function appendNormalizedKeywordFtsValue(builder: FtsFieldBuilder | undefined, normalized: string, docCount: number): void {
  if (!builder) return;
  const fieldCompanion = builder.companion;
  fieldCompanion.exists_docs.push(docCount);
  fieldCompanion.terms[normalized] = appendKeywordPostingDoc(fieldCompanion.terms[normalized], docCount);
}

function addExactCanonical(state: ExactFieldState | undefined, canonical: string | null): void {
  if (!state || canonical == null) return;
  state.canonicalTerms.add(canonical);
}

function addExactRawValue(state: ExactFieldState | undefined, rawValue: unknown): void {
  if (!state) return;
  addExactCanonical(state, canonicalizeExactValue(state.index.config, rawValue));
}

function appendColumnAndExactValue(
  builder: ColumnFieldBuilder | undefined,
  state: ExactFieldState | undefined,
  rawValue: unknown,
  docCount: number
): void {
  addExactRawValue(state, rawValue);
  if (!builder) return;
  const normalized = canonicalizeColumnValue(builder.config, rawValue);
  if (normalized == null) return;
  builder.docIds.push(docCount);
  builder.values.push(normalized);
}

function appendTextFtsValue(builder: FtsFieldBuilder | undefined, rawValue: unknown, docCount: number): void {
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

export function supportsUnifiedSearchSegmentBuild(input: SearchSegmentBuildInput): boolean {
  if (input.registry.search?.profile !== "evlog") return false;
  if (input.plan && (input.plan.families.agg || input.plan.families.mblk)) return false;
  return true;
}

export function buildSearchSegmentResult(
  input: SearchSegmentBuildInput
): Result<SearchSegmentBuildOutput, SearchSegmentBuildError> {
  if (!supportsUnifiedSearchSegmentBuild(input)) {
    return invalidSearchSegmentBuild("unified search segment build only supports evlog col/fts search plans");
  }

  const segmentBytes = readFileSync(input.segment.localPath);
  const exactStates = new Map<string, ExactFieldState>(
    input.exactIndexes.map((entry) => [
      entry.index.name,
      {
        index: entry.index,
        secret: entry.secret,
        canonicalTerms: new Set<string>(),
      },
    ])
  );
  const colBuilders = createColBuilders(input.registry, input.plan);
  const ftsBuilders = createFtsBuilders(input.registry, input.plan);
  const requiredFieldNames = new Set<string>();
  for (const fieldName of exactStates.keys()) requiredFieldNames.add(fieldName);
  for (const fieldName of colBuilders.keys()) requiredFieldNames.add(fieldName);
  for (const fieldName of ftsBuilders.keys()) requiredFieldNames.add(fieldName);
  const fieldNameList = Array.from(requiredFieldNames).sort(compareSearchStrings);
  const planByVersion = new Map<number, FastPlan>();

  let docCount = 0;
  let offset = input.segment.startOffset;
  for (const recRes of iterateBlockRecordsResult(segmentBytes)) {
    if (Result.isError(recRes)) return invalidSearchSegmentBuild(recRes.error.message);
    const version = schemaVersionForOffset(input.registry, offset);
    let fastPlan = planByVersion.get(version);
    if (!fastPlan) {
      const compiledRes = compileSearchFieldAccessorsResult(input.registry, fieldNameList, version);
      if (Result.isError(compiledRes)) return invalidSearchSegmentBuild(compiledRes.error.message);
      const trieRes = buildFastScalarAccessorTrieResult(compiledRes.value);
      if (Result.isError(trieRes)) return invalidSearchSegmentBuild(trieRes.error.message);
      fastPlan = { trie: trieRes.value };
      planByVersion.set(version, fastPlan);
    }
    const visitRes = visitFastScalarJsonValuesFromBytesWithTrieResult(recRes.value.payload, fastPlan.trie, (accessor, rawValue) => {
      switch (accessor.fieldName) {
      case "timestamp":
      case "status":
      case "duration":
          appendColumnAndExactValue(
            colBuilders.get(accessor.fieldName),
            exactStates.get(accessor.fieldName),
            rawValue,
            docCount
          );
          return;
      case "level":
      case "service":
      case "environment":
        case "requestId":
        case "traceId":
      case "spanId":
        case "path":
      case "method": {
          const ftsBuilder = ftsBuilders.get(accessor.fieldName);
          const exact = exactStates.get(accessor.fieldName);
          const normalized = normalizeKeywordValue(rawValue, ftsBuilder?.config.normalizer ?? exact?.index.config.normalizer ?? undefined);
          addExactCanonical(exact, normalized);
          if (normalized != null) appendNormalizedKeywordFtsValue(ftsBuilder, normalized, docCount);
          return;
        }
      case "message":
      case "why":
      case "fix":
      case "error.message":
          addExactRawValue(exactStates.get(accessor.fieldName), rawValue);
          appendTextFtsValue(ftsBuilders.get(accessor.fieldName), rawValue, docCount);
          return;
      }
    });
    if (Result.isError(visitRes)) return invalidSearchSegmentBuild(visitRes.error.message);
    offset += 1n;
    docCount += 1;
  }

  const exactRuns: SearchSegmentBuildOutput["exactRuns"] = [];
  const streamHash = streamHash16Hex(input.stream);
  for (const state of exactStates.values()) {
    const fingerprints = Array.from(state.canonicalTerms, (canonical) => siphash24(state.secret, new TextEncoder().encode(canonical))).sort((a, b) =>
      a < b ? -1 : a > b ? 1 : 0
    );
    const runId = `${state.index.name}-l0-${input.segment.segmentIndex.toString().padStart(16, "0")}-${input.segment.segmentIndex
      .toString()
      .padStart(16, "0")}-${Date.now()}`;
    const meta = {
      runId,
      level: 0,
      startSegment: input.segment.segmentIndex,
      endSegment: input.segment.segmentIndex,
      objectKey: secondaryIndexRunObjectKey(streamHash, state.index.name, runId),
      filterLen: 0,
      recordCount: fingerprints.length,
    };
    const payloadRes = encodeIndexRunResult({
      meta,
      runType: RUN_TYPE_MASK16,
      filterBytes: new Uint8Array(0),
      fingerprints,
      masks: new Array(fingerprints.length).fill(1),
    });
    if (Result.isError(payloadRes)) return invalidSearchSegmentBuild(payloadRes.error.message);
    exactRuns.push({
      indexName: state.index.name,
      meta,
      storage: "bytes",
      payload: payloadRes.value,
    });
  }

  let companion: SearchSegmentBuildOutput["companion"] = null;
  if (input.plan) {
    const sectionPayloads: EncodedCompanionSectionPayload[] = [];
    const sectionKinds: CompanionSectionKind[] = [];
    const sectionSizes: Record<string, number> = {};
    const addSection = (payload: EncodedCompanionSectionPayload): void => {
      sectionPayloads.push(payload);
      sectionKinds.push(payload.kind);
      sectionSizes[payload.kind] = payload.payload.byteLength;
    };
    let primaryTimestampMinMs: bigint | null = null;
    let primaryTimestampMaxMs: bigint | null = null;
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
    companion = {
      storage: "bytes",
      payload: encodeBundledSegmentCompanionFromPayloads({
        stream: input.stream,
        segment_index: input.segment.segmentIndex,
        plan_generation: input.planGeneration ?? 0,
        sections: sectionPayloads,
      }),
      sectionKinds,
      sectionSizes,
      primaryTimestampMinMs,
      primaryTimestampMaxMs,
    };
  }

  return Result.ok({
    exactRuns,
    companion,
  });
}
