import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildEvlogEvent } from "../experiments/demo/evlog_ingester";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { buildEvlogDefaultRegistry } from "../src/profiles/evlog/schema";
import { buildEncodedBundledCompanionPayloadResult } from "../src/search/companion_build";
import { CompanionFileCache } from "../src/search/companion_file_cache";
import { buildDesiredSearchCompanionPlan } from "../src/search/companion_plan";
import { decodeBundledSegmentCompanionResult, encodeBundledSegmentCompanionFromPayloads, encodeCompanionSectionPayload } from "../src/search/companion_format";
import type { ColFieldInput, ColScalar, ColSectionInput } from "../src/search/col_format";
import type { FtsFieldInput, FtsSectionInput } from "../src/search/fts_format";
import {
  ANALYZED_TEXT_CACHE_MAX_ENTRIES,
  ANALYZED_TEXT_CACHE_MAX_VALUE_CHARS,
  analyzeTextValueCached,
  buildFastScalarAccessorTrieResult,
  analyzeTextValue,
  canonicalizeColumnValue,
  compileSearchFieldAccessorsResult,
  extractRawSearchValuesWithCompiledAccessorsResult,
  normalizeKeywordValue,
  visitAnalyzedTextValue,
  visitFastScalarJsonValuesFromBytesWithTrieResult,
} from "../src/search/schema";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter, iterateBlockRecordsResult } from "../src/segment/format";
import type { SchemaRegistry, SearchFieldConfig, SearchFieldKind } from "../src/schema/registry";
import { schemaVersionForOffset } from "../src/schema/read_json";
import { BinaryWriter, concatBytes } from "../src/search/binary/codec";
import { encodeDocSet } from "../src/search/binary/docset";
import { encodeRestartStringTable } from "../src/search/binary/restart_strings";
import { writeUVarint } from "../src/search/binary/varint";

type LocalSegment = { stream: string; segmentIndex: number; startOffset: bigint; localPath: string };
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

const LEGACY_KIND_CODE: Record<SearchFieldKind, number> = {
  keyword: 0,
  text: 1,
  integer: 2,
  float: 3,
  date: 4,
  bool: 5,
};

const LEGACY_FLAG_EXACT = 1 << 0;
const LEGACY_FLAG_PREFIX = 1 << 1;
const LEGACY_FLAG_POSITIONS = 1 << 2;
const LEGACY_BLOCK_FLAG_POSITIONS = 1 << 0;
const LEGACY_FIELD_DIR_ENTRY_BYTES = 52;
const LEGACY_BLOCK_POSTING_LIMIT = 128;

function writeEvlogSegment(root: string, rowCount: number): LocalSegment {
  const encoder = new TextEncoder();
  const blocks: Uint8Array[] = [];
  const footerEntries: Array<{
    blockOffset: number;
    firstOffset: bigint;
    recordCount: number;
    compressedLen: number;
    firstAppendNs: bigint;
    lastAppendNs: bigint;
  }> = [];
  const blocksPerSegment = 24;
  const rowsPerBlock = Math.floor(rowCount / blocksPerSegment);
  let nextId = 0;
  let blockOffset = 0;
  let recordBase = 0;

  for (let blockIndex = 0; blockIndex < blocksPerSegment; blockIndex += 1) {
    const records = [];
    const rowLimit = blockIndex === blocksPerSegment - 1 ? rowCount - recordBase : rowsPerBlock;
    for (let recordIndex = 0; recordIndex < rowLimit; recordIndex += 1) {
      const id = nextId++;
      const timestamp = new Date(Date.UTC(2026, 3, 7, 0, 0, id % 60, id % 1000)).toISOString();
      records.push({
        appendNs: BigInt(id + 1),
        routingKey: new Uint8Array(0),
        payload: encoder.encode(JSON.stringify(buildEvlogEvent(id, timestamp))),
      });
    }
    const block = encodeBlock(records);
    blocks.push(block);
    footerEntries.push({
      blockOffset,
      firstOffset: BigInt(recordBase),
      recordCount: records.length,
      compressedLen: block.byteLength - DSB3_HEADER_BYTES,
      firstAppendNs: records[0]!.appendNs,
      lastAppendNs: records[records.length - 1]!.appendNs,
    });
    blockOffset += block.byteLength;
    recordBase += records.length;
  }

  const footer = encodeFooter(footerEntries);
  const bytes = new Uint8Array(blockOffset + footer.byteLength);
  let cursor = 0;
  for (const block of blocks) {
    bytes.set(block, cursor);
    cursor += block.byteLength;
  }
  bytes.set(footer, cursor);

  const localPath = join(root, "segment-0.bin");
  writeFileSync(localPath, bytes);
  return {
    stream: "evlog-1",
    segmentIndex: 0,
    startOffset: 0n,
    localPath,
  };
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function compareValues(left: bigint | number | boolean, right: bigint | number | boolean): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  return String(left).localeCompare(String(right));
}

function createLegacyColBuilders(registry: SchemaRegistry): Map<string, ColumnFieldBuilder> {
  const builders = new Map<string, ColumnFieldBuilder>();
  for (const [fieldName, field] of Object.entries(registry.search?.fields ?? {})) {
    if (field.column !== true) continue;
    builders.set(fieldName, { config: field, kind: field.kind, docIds: [], values: [], invalid: false });
  }
  return builders;
}

function recordLegacyColBuilders(builders: Map<string, ColumnFieldBuilder>, rawSearchValues: Map<string, unknown[]>, docCount: number): void {
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

function finalizeLegacyColSection(registry: SchemaRegistry, builders: Map<string, ColumnFieldBuilder>, docCount: number): ColSectionInput {
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

function createLegacyFtsBuilders(registry: SchemaRegistry): Map<string, FtsFieldBuilder> {
  const builders = new Map<string, FtsFieldBuilder>();
  for (const [fieldName, field] of Object.entries(registry.search?.fields ?? {}).sort((a, b) => a[0].localeCompare(b[0]))) {
    if (field.kind !== "text" && !(field.kind === "keyword" && field.prefix === true)) continue;
    builders.set(fieldName, {
      config: field,
      companion: {
        kind: field.kind,
        exact: field.exact === true ? true : undefined,
        prefix: field.prefix === true ? true : undefined,
        positions: field.positions === true ? true : undefined,
        exists_docs: [],
        terms: Object.create(null) as Record<string, { doc_ids: number[]; freqs?: number[]; positions?: number[] }>,
      },
    });
  }
  return builders;
}

function recordLegacyFtsBuilders(builders: Map<string, FtsFieldBuilder>, rawSearchValues: Map<string, unknown[]>, docCount: number): void {
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

function finalizeLegacyFtsSection(builders: Map<string, FtsFieldBuilder>, docCount: number): FtsSectionInput {
  const orderedFields = Object.create(null) as Record<string, FtsFieldInput>;
  for (const [fieldName, builder] of Array.from(builders.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
    orderedFields[fieldName] = builder.companion;
  }
  return {
    doc_count: docCount,
    fields: orderedFields,
  };
}

function encodeLegacyPostingList(postings: { doc_ids: number[]; freqs?: number[]; positions?: number[] }, withPositions: boolean): Uint8Array {
  const writer = new BinaryWriter();
  const docIds = postings.doc_ids;
  const freqs = postings.freqs ?? [];
  const positions = postings.positions ?? [];
  let positionOffset = 0;
  for (let start = 0; start < docIds.length; start += LEGACY_BLOCK_POSTING_LIMIT) {
    const end = Math.min(docIds.length, start + LEGACY_BLOCK_POSTING_LIMIT);
    const docDeltaWriter = new BinaryWriter();
    const freqWriter = new BinaryWriter();
    const posWriter = new BinaryWriter();
    for (let index = start + 1; index < end; index += 1) {
      writeUVarint(docDeltaWriter, docIds[index]! - docIds[index - 1]!);
    }
    for (let index = start; index < end; index += 1) {
      const freq = withPositions ? freqs[index] ?? 0 : 1;
      writeUVarint(freqWriter, freq);
      if (!withPositions) continue;
      let previous = 0;
      for (let posIndex = 0; posIndex < freq; posIndex += 1) {
        const position = positions[positionOffset++] ?? 0;
        writeUVarint(posWriter, position - previous);
        previous = position;
      }
    }
    writer.writeU16(end - start);
    writer.writeU8(withPositions ? LEGACY_BLOCK_FLAG_POSITIONS : 0);
    writer.writeU8(0);
    writer.writeU32(docIds[start] ?? 0);
    writer.writeU32(docDeltaWriter.length);
    writer.writeU32(freqWriter.length);
    writer.writeU32(posWriter.length);
    writer.writeBytes(docDeltaWriter.finish());
    writer.writeBytes(freqWriter.finish());
    writer.writeBytes(posWriter.finish());
  }
  return writer.finish();
}

function encodeLegacyFtsSectionPayload(input: FtsSectionInput, plan: ReturnType<typeof buildDesiredSearchCompanionPlan>): Uint8Array {
  const orderedFields = plan.fields
    .filter((field) => input.fields[field.name] && (field.kind === "text" || (field.kind === "keyword" && field.prefix)))
    .sort((a, b) => a.ordinal - b.ordinal);
  const fieldPayloads: Array<{
    fieldOrdinal: number;
    kind: SearchFieldKind;
    exact: boolean;
    prefix: boolean;
    positions: boolean;
    termCount: number;
    existsCodec: number;
    exists: Uint8Array;
    dict: Uint8Array;
    dfs: Uint8Array;
    postingOffsets: Uint8Array;
    postings: Uint8Array;
    existsOffset: number;
    dictOffset: number;
    dfOffset: number;
    postingsOffsetTableOffset: number;
    postingsDataOffset: number;
  }> = [];

  for (const planField of orderedFields) {
    const field = input.fields[planField.name]!;
    const terms = Object.keys(field.terms).sort((a, b) => a.localeCompare(b));
    const dict = encodeRestartStringTable(terms);
    const encodedDocSet = encodeDocSet(input.doc_count, field.exists_docs);
    const dfWriter = new BinaryWriter();
    const postingOffsetWriter = new BinaryWriter();
    const postingsWriter = new BinaryWriter();
    let postingOffset = 0;
    for (const term of terms) {
      const postings = field.terms[term] ?? { doc_ids: [] };
      dfWriter.writeU32(postings.doc_ids.length);
      postingOffsetWriter.writeU32(postingOffset);
      const payload = encodeLegacyPostingList(postings, field.positions === true);
      postingsWriter.writeBytes(payload);
      postingOffset += payload.byteLength;
    }
    postingOffsetWriter.writeU32(postingOffset);
    fieldPayloads.push({
      fieldOrdinal: planField.ordinal,
      kind: field.kind,
      exact: field.exact === true,
      prefix: field.prefix === true,
      positions: field.positions === true,
      termCount: terms.length,
      existsCodec: encodedDocSet.codec,
      exists: encodedDocSet.payload,
      dict,
      dfs: dfWriter.finish(),
      postingOffsets: postingOffsetWriter.finish(),
      postings: postingsWriter.finish(),
      existsOffset: 0,
      dictOffset: 0,
      dfOffset: 0,
      postingsOffsetTableOffset: 0,
      postingsDataOffset: 0,
    });
  }

  const header = new BinaryWriter();
  header.writeU32(input.doc_count);
  header.writeU16(fieldPayloads.length);
  header.writeU16(0);

  let payloadOffset = header.length + LEGACY_FIELD_DIR_ENTRY_BYTES * fieldPayloads.length;
  for (const payload of fieldPayloads) {
    payload.existsOffset = payloadOffset;
    payloadOffset += payload.exists.byteLength;
    payload.dictOffset = payloadOffset;
    payloadOffset += payload.dict.byteLength;
    payload.dfOffset = payloadOffset;
    payloadOffset += payload.dfs.byteLength;
    payload.postingsOffsetTableOffset = payloadOffset;
    payloadOffset += payload.postingOffsets.byteLength;
    payload.postingsDataOffset = payloadOffset;
    payloadOffset += payload.postings.byteLength;
  }

  const directory = new BinaryWriter();
  for (const payload of fieldPayloads) {
    const flags =
      (payload.exact ? LEGACY_FLAG_EXACT : 0) |
      (payload.prefix ? LEGACY_FLAG_PREFIX : 0) |
      (payload.positions ? LEGACY_FLAG_POSITIONS : 0);
    directory.writeU16(payload.fieldOrdinal);
    directory.writeU8(LEGACY_KIND_CODE[payload.kind] ?? 0);
    directory.writeU8(flags);
    directory.writeU32(payload.termCount);
    directory.writeU32(payload.existsOffset);
    directory.writeU32(payload.exists.byteLength);
    directory.writeU32((payload.existsCodec << 24) | 0);
    directory.writeU32(payload.dictOffset);
    directory.writeU32(payload.dict.byteLength);
    directory.writeU32(payload.dfOffset);
    directory.writeU32(payload.dfs.byteLength);
    directory.writeU32(payload.postingsOffsetTableOffset);
    directory.writeU32(payload.postingOffsets.byteLength);
    directory.writeU32(payload.postingsDataOffset);
    directory.writeU32(payload.postings.byteLength);
  }

  return concatBytes([
    header.finish(),
    directory.finish(),
    ...fieldPayloads.flatMap((payload) => [payload.exists, payload.dict, payload.dfs, payload.postingOffsets, payload.postings]),
  ]);
}

function buildLegacyEvlogBundledCompanionPayloadResult(input: {
  registry: SchemaRegistry;
  segment: LocalSegment;
  planGeneration: number;
}) {
  const plan = buildDesiredSearchCompanionPlan(input.registry);
  expect(plan.families.agg).toBe(false);
  expect(plan.families.mblk).toBe(false);

  const payloadDecoder = new TextDecoder();
  const segmentFileBytes = readFileSync(input.segment.localPath);
  const colBuilders = plan.families.col ? createLegacyColBuilders(input.registry) : new Map<string, ColumnFieldBuilder>();
  const ftsBuilders = plan.families.fts ? createLegacyFtsBuilders(input.registry) : new Map<string, FtsFieldBuilder>();
  const requiredFieldNames = new Set<string>();
  for (const fieldName of colBuilders.keys()) requiredFieldNames.add(fieldName);
  for (const fieldName of ftsBuilders.keys()) requiredFieldNames.add(fieldName);
  const fieldNameList = Array.from(requiredFieldNames).sort(compareStrings);
  const compiledByVersion = new Map<number, ReturnType<typeof compileSearchFieldAccessorsResult> extends Result<infer T, any> ? T : never>();
  let docCount = 0;
  let offset = input.segment.startOffset;
  for (const recRes of iterateBlockRecordsResult(segmentFileBytes)) {
    if (Result.isError(recRes)) throw new Error(recRes.error.message);
    let parsed: unknown = null;
    let parsedOk = false;
    try {
      parsed = JSON.parse(payloadDecoder.decode(recRes.value.payload));
      parsedOk = true;
    } catch {
      parsed = null;
    }
    if (parsedOk && fieldNameList.length > 0) {
      const version = schemaVersionForOffset(input.registry, offset);
      let accessors = compiledByVersion.get(version);
      if (!accessors) {
        const compiledRes = compileSearchFieldAccessorsResult(input.registry, fieldNameList, version);
        if (Result.isError(compiledRes)) throw new Error(compiledRes.error.message);
        accessors = compiledRes.value;
        compiledByVersion.set(version, accessors);
      }
      const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, accessors);
      if (Result.isError(rawValuesRes)) throw new Error(rawValuesRes.error.message);
      recordLegacyColBuilders(colBuilders, rawValuesRes.value, docCount);
      recordLegacyFtsBuilders(ftsBuilders, rawValuesRes.value, docCount);
    }
    offset += 1n;
    docCount += 1;
  }

  const sections = [];
  if (plan.families.col) sections.push(encodeCompanionSectionPayload("col", finalizeLegacyColSection(input.registry, colBuilders, docCount), plan));
  if (plan.families.fts) {
    sections.push({
      kind: "fts" as const,
      payload: encodeLegacyFtsSectionPayload(finalizeLegacyFtsSection(ftsBuilders, docCount), plan),
    });
  }
  return encodeBundledSegmentCompanionFromPayloads({
    stream: input.segment.stream,
    segment_index: input.segment.segmentIndex,
    plan_generation: input.planGeneration,
    sections,
  });
}

function recordLegacyEvlogByteScanColumnAndFtsBuildersResult(
  payload: Uint8Array,
  trie: FastScalarAccessorTrie,
  colBuilders: Map<string, ColumnFieldBuilder>,
  ftsBuilders: Map<string, FtsFieldBuilder>,
  docCount: number
): Result<void, { kind: "invalid_companion_build"; message: string }> {
  const visitRes = visitFastScalarJsonValuesFromBytesWithTrieResult(payload, trie, (accessor, rawValue) => {
    switch (accessor.fieldName) {
      case "timestamp":
      case "status":
      case "duration": {
        const builder = colBuilders.get(accessor.fieldName);
        if (!builder || builder.invalid) return;
        const normalized = canonicalizeColumnValue(builder.config, rawValue);
        if (normalized == null) return;
        builder.docIds.push(docCount);
        builder.values.push(normalized);
        return;
      }
      case "level":
      case "service":
      case "environment":
      case "requestId":
      case "traceId":
      case "spanId":
      case "path":
      case "method": {
        const builder = ftsBuilders.get(accessor.fieldName);
        if (!builder) return;
        const normalized = normalizeKeywordValue(rawValue, builder.config.normalizer);
        if (normalized == null) return;
        builder.companion.exists_docs.push(docCount);
        const postings = builder.companion.terms[normalized] ?? { doc_ids: [] };
        const docIds = postings.doc_ids;
        if (docIds.length === 0 || docIds[docIds.length - 1] !== docCount) docIds.push(docCount);
        builder.companion.terms[normalized] = postings;
        return;
      }
      case "message":
      case "why":
      case "fix":
      case "error.message": {
        const builder = ftsBuilders.get(accessor.fieldName);
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
        return;
      }
    }
  });
  if (Result.isError(visitRes)) return Result.err({ kind: "invalid_companion_build", message: visitRes.error.message });
  return Result.ok(undefined);
}

function buildLegacyFastEvlogBundledCompanionPayloadResult(input: {
  registry: SchemaRegistry;
  segment: LocalSegment;
  planGeneration: number;
}) {
  const plan = buildDesiredSearchCompanionPlan(input.registry);
  const segmentBytes = readFileSync(input.segment.localPath);
  const colBuilders = createLegacyColBuilders(input.registry);
  const ftsBuilders = createLegacyFtsBuilders(input.registry);
  const requiredFieldNames = new Set<string>();
  for (const fieldName of colBuilders.keys()) requiredFieldNames.add(fieldName);
  for (const fieldName of ftsBuilders.keys()) requiredFieldNames.add(fieldName);
  const fieldNameList = Array.from(requiredFieldNames).sort(compareStrings);
  const fastByteScanPlanByVersion = new Map<number, FastScalarAccessorTrie>();
  let docCount = 0;
  let offset = input.segment.startOffset;
  for (const recRes of iterateBlockRecordsResult(segmentBytes)) {
    if (Result.isError(recRes)) throw new Error(recRes.error.message);
    const version = schemaVersionForOffset(input.registry, offset);
    let trie = fastByteScanPlanByVersion.get(version);
    if (!trie) {
      const compiledRes = compileSearchFieldAccessorsResult(input.registry, fieldNameList, version);
      if (Result.isError(compiledRes)) throw new Error(compiledRes.error.message);
      const trieRes = buildFastScalarAccessorTrieResult(compiledRes.value);
      if (Result.isError(trieRes)) throw new Error(trieRes.error.message);
      trie = trieRes.value;
      fastByteScanPlanByVersion.set(version, trie);
    }
    const recordRes = recordLegacyEvlogByteScanColumnAndFtsBuildersResult(recRes.value.payload, trie, colBuilders, ftsBuilders, docCount);
    if (Result.isError(recordRes)) throw new Error(recordRes.error.message);
    offset += 1n;
    docCount += 1;
  }
  return encodeBundledSegmentCompanionFromPayloads({
    stream: input.segment.stream,
    segment_index: input.segment.segmentIndex,
    plan_generation: input.planGeneration,
    sections: [
      encodeCompanionSectionPayload("col", finalizeLegacyColSection(input.registry, colBuilders, docCount), plan),
      { kind: "fts" as const, payload: encodeLegacyFtsSectionPayload(finalizeLegacyFtsSection(ftsBuilders, docCount), plan) },
    ],
  });
}

function collectSegmentPayloads(segment: LocalSegment): Uint8Array[] {
  const payloads: Uint8Array[] = [];
  const segmentFileBytes = readFileSync(segment.localPath);
  for (const recRes of iterateBlockRecordsResult(segmentFileBytes)) {
    if (Result.isError(recRes)) throw new Error(recRes.error.message);
    payloads.push(recRes.value.payload);
  }
  return payloads;
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function collectEvlogTextCorpus(segment: LocalSegment): string[] {
  const payloadDecoder = new TextDecoder();
  const values: string[] = [];
  const segmentFileBytes = readFileSync(segment.localPath);
  for (const recRes of iterateBlockRecordsResult(segmentFileBytes)) {
    if (Result.isError(recRes)) throw new Error(recRes.error.message);
    const parsed = JSON.parse(payloadDecoder.decode(recRes.value.payload)) as {
      message?: string;
      why?: string;
      fix?: string;
      context?: { error?: { message?: string } };
    };
    if (typeof parsed.message === "string") values.push(parsed.message);
    if (typeof parsed.why === "string") values.push(parsed.why);
    if (typeof parsed.fix === "string") values.push(parsed.fix);
    if (typeof parsed.context?.error?.message === "string") values.push(parsed.context.error.message);
  }
  return values;
}

async function runCompanionStagingElapsedMs(args: {
  cache: CompanionFileCache;
  store: MockR2Store;
  objectKey: string;
  payload: Uint8Array;
  tempPath: string;
  mode: "bytes" | "file";
}): Promise<number> {
  const started = performance.now();
  if (args.mode === "file") {
    writeFileSync(args.tempPath, args.payload);
    await args.store.putFile(args.objectKey, args.tempPath, args.payload.byteLength);
    const cacheRes = args.cache.storeFileResult(args.objectKey, args.tempPath, args.payload.byteLength);
    expect(Result.isOk(cacheRes)).toBe(true);
  } else {
    const transferred = new Uint8Array(args.payload);
    await args.store.put(args.objectKey, transferred, { contentLength: transferred.byteLength });
    const cacheRes = args.cache.storeBytesResult(args.objectKey, transferred);
    expect(Result.isOk(cacheRes)).toBe(true);
  }
  return performance.now() - started;
}

describe("bundled companion build performance", () => {
  test("evlog companion raw-byte extraction matches the compiled parsed extraction semantics", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-build-perf-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 96_000);
      const plan = buildDesiredSearchCompanionPlan(registry);
      const fieldNames = plan.fields.map((field) => field.name);
      const compiledRes = compileSearchFieldAccessorsResult(registry, fieldNames, 1);
      expect(Result.isOk(compiledRes)).toBe(true);
      if (Result.isError(compiledRes)) return;
      const payloads = collectSegmentPayloads(segment);
      const trieRes = buildFastScalarAccessorTrieResult(compiledRes.value);
      expect(Result.isOk(trieRes)).toBe(true);
      if (Result.isError(trieRes)) return;

      let parsedCount = 0;
      let byteCount = 0;
      const decoder = new TextDecoder();
      for (const payload of payloads.slice(0, 512)) {
        const parsed = JSON.parse(decoder.decode(payload));
        const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, compiledRes.value);
        expect(Result.isOk(rawValuesRes)).toBe(true);
        if (Result.isOk(rawValuesRes)) {
          for (const values of rawValuesRes.value.values()) parsedCount += values.length;
        }
        const visitRes = visitFastScalarJsonValuesFromBytesWithTrieResult(payload, trieRes.value, () => {
          byteCount += 1;
        });
        expect(Result.isOk(visitRes)).toBe(true);
      }
      expect(byteCount).toBe(parsedCount);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog companion builds are at least 25% faster than the legacy parsed path", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-build-perf-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 24_000);
      const input = {
        registry,
        plan: buildDesiredSearchCompanionPlan(registry),
        planGeneration: 1,
        segment,
      } as const;

      const warmLegacy = buildLegacyEvlogBundledCompanionPayloadResult({ registry, segment, planGeneration: 1 });
      const warmCurrent = buildEncodedBundledCompanionPayloadResult(input);
      expect(Result.isOk(warmCurrent)).toBe(true);
      if (Result.isError(warmCurrent)) return;
      expect(warmCurrent.value.storage).toBe("bytes");
      if (warmCurrent.value.storage !== "bytes") return;

      const legacyPayload = buildLegacyEvlogBundledCompanionPayloadResult({ registry, segment, planGeneration: 1 });
      const decodedLegacy = decodeBundledSegmentCompanionResult(legacyPayload, input.plan);
      const decodedCurrent = decodeBundledSegmentCompanionResult(warmCurrent.value.payload, input.plan);
      expect(Result.isOk(decodedLegacy)).toBe(true);
      expect(Result.isOk(decodedCurrent)).toBe(true);
      if (Result.isOk(decodedLegacy) && Result.isOk(decodedCurrent)) {
        expect(decodedCurrent.value.toc.sections.map((section) => [section.kind, section.length])).toEqual(
          decodedLegacy.value.toc.sections.map((section) => [section.kind, section.kind === "fts" ? expect.any(Number) : section.length])
        );
        const legacyFtsSection = decodedLegacy.value.toc.sections.find((section) => section.kind === "fts");
        const currentFtsSection = decodedCurrent.value.toc.sections.find((section) => section.kind === "fts");
        expect(Math.abs((currentFtsSection?.length ?? 0) - (legacyFtsSection?.length ?? 0))).toBeLessThanOrEqual(1);
        const legacyCol = decodedLegacy.value.sections.col!;
        const currentCol = decodedCurrent.value.sections.col!;
        expect(Object.keys(currentCol.fields).sort(compareStrings)).toEqual(Object.keys(legacyCol.fields).sort(compareStrings));
        for (const fieldName of ["timestamp", "status", "duration"]) {
          expect(currentCol.fields[fieldName]?.doc_ids.length).toBe(legacyCol.fields[fieldName]?.doc_ids.length);
          expect(currentCol.fields[fieldName]?.values.length).toBe(legacyCol.fields[fieldName]?.values.length);
        }
        const legacyFts = decodedLegacy.value.sections.fts!;
        const currentFts = decodedCurrent.value.sections.fts!;
        for (const fieldName of ["requestId", "message"]) {
          const legacyField = legacyFts.getField(fieldName);
          const currentField = currentFts.getField(fieldName);
          const currentTerms = currentField?.terms() ?? [];
          const legacyTerms = legacyField?.terms() ?? [];
          expect(currentTerms.length).toBe(legacyTerms.length);
          expect([...currentTerms].sort(compareStrings).slice(0, 256)).toEqual([...legacyTerms].sort(compareStrings).slice(0, 256));
          expect(currentField?.existsDocIds().length).toBe(legacyField?.existsDocIds().length);
        }
      }
      const legacySamples = Array.from({ length: 3 }, () => {
        const started = performance.now();
        buildLegacyEvlogBundledCompanionPayloadResult({ registry, segment, planGeneration: 1 });
        return performance.now() - started;
      });
      const currentSamples = Array.from({ length: 3 }, () => {
        const started = performance.now();
        const res = buildEncodedBundledCompanionPayloadResult(input);
        expect(Result.isOk(res)).toBe(true);
        return performance.now() - started;
      });
      const legacyElapsedMs = average(legacySamples);
      const currentElapsedMs = average(currentSamples);

      expect(currentElapsedMs).toBeLessThan(legacyElapsedMs * 0.75);
      expect(warmLegacy.byteLength).toBeGreaterThan(0);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog fast-byte companion builds are at least 25% faster than the legacy singleton-array postings path", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-fast-singleton-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 64_000);
      const input = {
        registry,
        plan: buildDesiredSearchCompanionPlan(registry),
        planGeneration: 1,
        segment,
      } as const;

      const warmCurrent = buildEncodedBundledCompanionPayloadResult(input);
      expect(Result.isOk(warmCurrent)).toBe(true);
      buildLegacyFastEvlogBundledCompanionPayloadResult({ registry, segment, planGeneration: 1 });

      const currentSamples = Array.from({ length: 3 }, () => {
        const started = performance.now();
        const res = buildEncodedBundledCompanionPayloadResult(input);
        expect(Result.isOk(res)).toBe(true);
        return performance.now() - started;
      });
      const legacySamples = Array.from({ length: 3 }, () => {
        const started = performance.now();
        buildLegacyFastEvlogBundledCompanionPayloadResult({ registry, segment, planGeneration: 1 });
        return performance.now() - started;
      });

      expect(average(currentSamples)).toBeLessThan(average(legacySamples) * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("cached evlog text tokenization is at least 25% faster than uncached tokenization on a real segment corpus", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-token-cache-"));
    try {
      const segment = writeEvlogSegment(root, 96_000);
      const corpus = collectEvlogTextCorpus(segment);
      expect(corpus.length).toBeGreaterThan(0);

      const uncached = (): number => {
        const started = performance.now();
        let tokenCount = 0;
        for (const value of corpus) tokenCount += analyzeTextValue(value, "unicode_word_v1").length;
        expect(tokenCount).toBeGreaterThan(0);
        return performance.now() - started;
      };

      const cached = (): number => {
        const started = performance.now();
        const cache = new Map<string, string[]>();
        let tokenCount = 0;
        for (const value of corpus) tokenCount += analyzeTextValueCached(value, "unicode_word_v1", cache).length;
        expect(tokenCount).toBeGreaterThan(0);
        return performance.now() - started;
      };

      uncached();
      cached();

      const uncachedSamples = Array.from({ length: 4 }, () => uncached());
      const cachedSamples = Array.from({ length: 4 }, () => cached());
      expect(average(cachedSamples)).toBeLessThan(average(uncachedSamples) * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("text token cache stays bounded on unique long values", () => {
    const cache = new Map<string, string[]>();
    for (let index = 0; index < ANALYZED_TEXT_CACHE_MAX_ENTRIES * 3; index += 1) {
      const unique = `${index}-${"x".repeat(ANALYZED_TEXT_CACHE_MAX_VALUE_CHARS + 64)}`;
      const tokens = analyzeTextValueCached(unique, "unicode_word_v1", cache);
      expect(tokens.length).toBeGreaterThan(0);
    }
    expect(cache.size).toBe(0);

    for (let index = 0; index < ANALYZED_TEXT_CACHE_MAX_ENTRIES * 3; index += 1) {
      const unique = `short-${index}`;
      const tokens = analyzeTextValueCached(unique, "unicode_word_v1", cache);
      expect(tokens.length).toBeGreaterThan(0);
    }
    expect(cache.size).toBeLessThanOrEqual(ANALYZED_TEXT_CACHE_MAX_ENTRIES);
  });

  test("file-backed companion staging uploads and caches the same artifact", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-build-worker-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 48_000);
      const buildRes = buildEncodedBundledCompanionPayloadResult({
        registry,
        plan: buildDesiredSearchCompanionPlan(registry),
        planGeneration: 1,
        segment,
      });
      expect(Result.isOk(buildRes)).toBe(true);
      if (Result.isError(buildRes)) return;
      expect(buildRes.value.storage).toBe("bytes");
      if (buildRes.value.storage !== "bytes") return;
      const payload = concatBytes([
        buildRes.value.payload,
        buildRes.value.payload,
        buildRes.value.payload,
        buildRes.value.payload,
      ]);
      const fileCache = new CompanionFileCache(join(root, "cache-file"), 512 * 1024 * 1024, 60_000, 4);
      const fileStore = new MockR2Store({ maxInMemoryBytes: 0, spillDir: join(root, "spill-file") });

      const elapsedMs = await runCompanionStagingElapsedMs({
        cache: fileCache,
        store: fileStore,
        objectKey: "streams/a/segments/file-backed.cix",
        payload,
        tempPath: join(root, "file-backed.cix"),
        mode: "file",
      });
      expect(elapsedMs).toBeGreaterThan(0);
      expect(readFileSync(join(root, "cache-file", "streams/a/segments/file-backed.cix"))).toEqual(payload);
      expect(await fileStore.get("streams/a/segments/file-backed.cix")).toEqual(payload);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
