import { Result } from "better-result";
import { createBitset, bitsetSet } from "./bitset";
import type { SearchFieldKind } from "../schema/registry";
import { decodeDocIds, encodeDocSet } from "./binary/docset";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, concatBytes, readF64, readI64, readU16, readU32 } from "./binary/codec";
import type { SearchCompanionPlan } from "./companion_plan";
import { readUVarint, writeUVarint } from "./binary/varint";

export type ColScalar = bigint | number | boolean;

export type ColFieldInput = {
  kind: SearchFieldKind;
  doc_ids: number[];
  values: ColScalar[];
  min: ColScalar | null;
  max: ColScalar | null;
};

export type ColSectionInput = {
  doc_count: number;
  primary_timestamp_field?: string;
  fields: Record<string, ColFieldInput>;
};

type ColFieldDirectoryEntry = {
  fieldOrdinal: number;
  kind: SearchFieldKind;
  presentCount: number;
  existsCodec: number;
  existsOffset: number;
  existsLength: number;
  valuesOffset: number;
  valuesLength: number;
  pageIndexOffset: number;
  pageIndexLength: number;
  minValue: ColScalar | null;
  maxValue: ColScalar | null;
};

type EncodedColFieldPayload = {
  entry: ColFieldDirectoryEntry;
  exists: Uint8Array;
  values: Uint8Array;
  pages: Uint8Array;
};

type ColPageEntry = {
  firstDocId: number;
  valueStartIndex: number;
  min: ColScalar;
  max: ColScalar;
};

type DocIdCursorState =
  | {
      codec: "all";
      nextDocId: number;
      docCount: number;
    }
  | {
      codec: "bitset";
      nextDocId: number;
      docCount: number;
      payload: Uint8Array;
    }
  | {
      codec: "delta";
      cursor: BinaryCursor;
      previousDocId: number;
      emittedCount: number;
    };

const PAGE_SIZE = 256;

const KIND_CODE: Record<SearchFieldKind, number> = {
  keyword: 0,
  text: 1,
  integer: 2,
  float: 3,
  date: 4,
  bool: 5,
};

const CODE_KIND: Record<number, SearchFieldKind> = {
  0: "keyword",
  1: "text",
  2: "integer",
  3: "float",
  4: "date",
  5: "bool",
};

const FLAG_HAS_MINMAX = 1 << 0;
const FLAG_HAS_PAGE_INDEX = 1 << 1;

const DIRECTORY_ENTRY_BYTES = 52;
const PAGE_ENTRY_BYTES = 24;

export type ColFormatError = { kind: "invalid_col_segment"; message: string };

function invalidCol<T = never>(message: string): Result<T, ColFormatError> {
  return Result.err({ kind: "invalid_col_segment", message });
}

export class ColFieldView {
  private pageCache: ColPageEntry[] | null = null;
  private docIdsCache: number[] | null = null;

  constructor(
    readonly name: string,
    readonly kind: SearchFieldKind,
    private readonly docCount: number,
    private readonly presentCount: number,
    private readonly existsCodec: number,
    private readonly existsPayload: Uint8Array,
    private readonly valuesPayload: Uint8Array,
    private readonly pageIndexPayload: Uint8Array,
    private readonly minValueInternal: ColScalar | null,
    private readonly maxValueInternal: ColScalar | null
  ) {}

  minValue(): ColScalar | null {
    return this.minValueInternal;
  }

  maxValue(): ColScalar | null {
    return this.maxValueInternal;
  }

  hasPageIndex(): boolean {
    return this.pageIndexPayload.byteLength > 0;
  }

  docIds(): number[] {
    if (!this.docIdsCache) {
      this.docIdsCache = decodeDocIds(this.docCount, this.existsCodec, this.existsPayload);
    }
    return this.docIdsCache;
  }

  forEachValue(fn: (docId: number, value: ColScalar) => void): void {
    if (this.presentCount === 0) return;
    const docIds = this.docIds();
    const width = valueWidth(this.kind);
    for (let index = 0; index < docIds.length; index++) {
      fn(docIds[index]!, decodeValue(this.kind, this.valuesPayload, index * width));
    }
  }

  forEachValueRange(startValueIndex: number, endValueIndex: number, fn: (docId: number, value: ColScalar) => void): void {
    const docIds = this.docIds();
    const width = valueWidth(this.kind);
    for (let index = startValueIndex; index < endValueIndex && index < docIds.length; index++) {
      fn(docIds[index]!, decodeValue(this.kind, this.valuesPayload, index * width));
    }
  }

  pageEntries(): ColPageEntry[] {
    if (this.pageCache) return this.pageCache;
    const entries: ColPageEntry[] = [];
    for (let offset = 0; offset + PAGE_ENTRY_BYTES <= this.pageIndexPayload.byteLength; offset += PAGE_ENTRY_BYTES) {
      entries.push({
        firstDocId: readU32(this.pageIndexPayload, offset),
        valueStartIndex: readU32(this.pageIndexPayload, offset + 4),
        min: decodeScalarInline(this.kind, this.pageIndexPayload.subarray(offset + 8, offset + 16))!,
        max: decodeScalarInline(this.kind, this.pageIndexPayload.subarray(offset + 16, offset + 24))!,
      });
    }
    this.pageCache = entries;
    return entries;
  }
}

export class ColSectionView {
  private readonly fieldByName = new Map<string, ColFieldView>();

  constructor(
    readonly docCount: number,
    readonly primaryTimestampField: string | null,
    readonly fields: ColFieldView[]
  ) {
    for (const field of fields) this.fieldByName.set(field.name, field);
  }

  getField(fieldName: string): ColFieldView | null {
    return this.fieldByName.get(fieldName) ?? null;
  }

  minTimestampMs(): bigint | null {
    const field = this.primaryTimestampField ? this.getField(this.primaryTimestampField) : null;
    const value = field?.minValue();
    return typeof value === "bigint" ? value : null;
  }

  maxTimestampMs(): bigint | null {
    const field = this.primaryTimestampField ? this.getField(this.primaryTimestampField) : null;
    const value = field?.maxValue();
    return typeof value === "bigint" ? value : null;
  }
}

export function compareColScalars(left: ColScalar, right: ColScalar): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  return String(left).localeCompare(String(right));
}

export function encodeColSegmentCompanion(input: ColSectionInput, plan: SearchCompanionPlan): Uint8Array {
  return concatBytes(encodeColSegmentCompanionChunks(input, plan).chunks);
}

export function encodeColSegmentCompanionChunks(
  input: ColSectionInput,
  plan: SearchCompanionPlan
): { chunks: Uint8Array[]; sizeBytes: number } {
  const orderedFields = plan.fields
    .filter((field) => input.fields[field.name] && (field.kind === "integer" || field.kind === "float" || field.kind === "date" || field.kind === "bool"))
    .sort((a, b) => a.ordinal - b.ordinal);
  const fieldPayloads: EncodedColFieldPayload[] = [];
  for (const planField of orderedFields) {
    const field = input.fields[planField.name]!;
    const encodedDocSet = encodeDocSet(input.doc_count, field.doc_ids);
    const values = encodeFieldValues(field.kind, field.values);
    const pages = encodePageIndex(field.kind, field.doc_ids, field.values);
    fieldPayloads.push({
      entry: {
        fieldOrdinal: planField.ordinal,
        kind: field.kind,
        presentCount: field.doc_ids.length,
        existsCodec: encodedDocSet.codec,
        existsOffset: 0,
        existsLength: encodedDocSet.payload.byteLength,
        valuesOffset: 0,
        valuesLength: values.byteLength,
        pageIndexOffset: 0,
        pageIndexLength: pages.byteLength,
        minValue: field.min,
        maxValue: field.max,
      },
      exists: encodedDocSet.payload,
      values,
      pages,
    });
  }

  const primaryTimestampOrdinal =
    input.primary_timestamp_field == null ? 0xffff : (plan.fields.find((field) => field.name === input.primary_timestamp_field)?.ordinal ?? 0xffff);
  return encodeColSectionChunksFromFieldPayloads(input.doc_count, primaryTimestampOrdinal, fieldPayloads);
}

function encodeColSectionChunksFromFieldPayloads(
  docCount: number,
  primaryTimestampOrdinal: number,
  fieldPayloads: EncodedColFieldPayload[]
): { chunks: Uint8Array[]; sizeBytes: number } {
  const header = new BinaryWriter();
  header.writeU32(docCount);
  header.writeU16(fieldPayloads.length);
  header.writeU16(primaryTimestampOrdinal);

  const directoryBase = header.length + DIRECTORY_ENTRY_BYTES * fieldPayloads.length;
  let payloadOffset = directoryBase;
  for (const payload of fieldPayloads) {
    payload.entry.existsOffset = payloadOffset;
    payloadOffset += payload.exists.byteLength;
    payload.entry.valuesOffset = payloadOffset;
    payloadOffset += payload.values.byteLength;
    payload.entry.pageIndexOffset = payload.pages.byteLength > 0 ? payloadOffset : 0;
    payloadOffset += payload.pages.byteLength;
  }

  const directory = new BinaryWriter();
  for (const payload of fieldPayloads) {
    const flags =
      (payload.entry.minValue != null && payload.entry.maxValue != null ? FLAG_HAS_MINMAX : 0) |
      (payload.pages.byteLength > 0 ? FLAG_HAS_PAGE_INDEX : 0);
    directory.writeU16(payload.entry.fieldOrdinal);
    directory.writeU8(KIND_CODE[payload.entry.kind] ?? 0);
    directory.writeU8(flags);
    directory.writeU32(payload.entry.presentCount);
    directory.writeU8(payload.entry.existsCodec);
    directory.writeU8(0);
    directory.writeU16(0);
    directory.writeU32(payload.entry.existsOffset);
    directory.writeU32(payload.entry.existsLength);
    directory.writeU32(payload.entry.valuesOffset);
    directory.writeU32(payload.entry.valuesLength);
    directory.writeU32(payload.entry.pageIndexOffset);
    directory.writeU32(payload.entry.pageIndexLength);
    directory.writeBytes(encodeScalarInline(payload.entry.kind, payload.entry.minValue));
    directory.writeBytes(encodeScalarInline(payload.entry.kind, payload.entry.maxValue));
  }

  const chunks = [header.finish(), directory.finish(), ...fieldPayloads.flatMap((payload) => [payload.exists, payload.values, payload.pages])];
  return { chunks, sizeBytes: payloadOffset };
}

export function mergeEncodedColSectionChunksResult(
  bytesParts: Uint8Array[]
): Result<{ chunks: Uint8Array[]; sizeBytes: number; primaryTimestampOrdinal: number }, ColFormatError> {
  if (bytesParts.length === 0) return Result.ok({ chunks: [new Uint8Array(0)], sizeBytes: 0, primaryTimestampOrdinal: 0xffff });
  const fieldPayloads: EncodedColFieldPayload[] = [];
  let docCount: number | null = null;
  let primaryTimestampOrdinal = 0xffff;
  for (const bytes of bytesParts) {
    const partDocCount = readU32(bytes, 0);
    if (docCount == null) docCount = partDocCount;
    else if (docCount !== partDocCount) return invalidCol("mismatched .col2 doc_count while merging partial companions");
    const partPrimaryTimestampOrdinal = readU16(bytes, 6);
    if (partPrimaryTimestampOrdinal !== 0xffff) {
      if (primaryTimestampOrdinal !== 0xffff && primaryTimestampOrdinal !== partPrimaryTimestampOrdinal) {
        return invalidCol("mismatched .col2 primary timestamp field while merging partial companions");
      }
      primaryTimestampOrdinal = partPrimaryTimestampOrdinal;
    }
    const fieldCount = readU16(bytes, 4);
    const directoryOffset = 8;
    for (let index = 0; index < fieldCount; index += 1) {
      const entryOffset = directoryOffset + index * DIRECTORY_ENTRY_BYTES;
      if (entryOffset + DIRECTORY_ENTRY_BYTES > bytes.byteLength) return invalidCol("invalid .col2 directory");
      const flags = bytes[entryOffset + 3]!;
      const entry: ColFieldDirectoryEntry = {
        fieldOrdinal: readU16(bytes, entryOffset),
        kind: CODE_KIND[bytes[entryOffset + 2]!] ?? "keyword",
        presentCount: readU32(bytes, entryOffset + 4),
        existsCodec: bytes[entryOffset + 8]!,
        existsOffset: 0,
        existsLength: readU32(bytes, entryOffset + 16),
        valuesOffset: 0,
        valuesLength: readU32(bytes, entryOffset + 24),
        pageIndexOffset: 0,
        pageIndexLength: readU32(bytes, entryOffset + 32),
        minValue: (flags & FLAG_HAS_MINMAX) !== 0 ? decodeScalarInline(CODE_KIND[bytes[entryOffset + 2]!] ?? "keyword", bytes.subarray(entryOffset + 36, entryOffset + 44)) : null,
        maxValue: (flags & FLAG_HAS_MINMAX) !== 0 ? decodeScalarInline(CODE_KIND[bytes[entryOffset + 2]!] ?? "keyword", bytes.subarray(entryOffset + 44, entryOffset + 52)) : null,
      };
      if (entry.kind !== "integer" && entry.kind !== "float" && entry.kind !== "date" && entry.kind !== "bool") {
        return invalidCol("invalid .col2 field kind while merging partial companions");
      }
      fieldPayloads.push({
        entry,
        exists: slicePayload(bytes, readU32(bytes, entryOffset + 12), entry.existsLength, "invalid .col2 exists payload"),
        values: slicePayload(bytes, readU32(bytes, entryOffset + 20), entry.valuesLength, "invalid .col2 values payload"),
        pages:
          entry.pageIndexLength > 0
            ? slicePayload(bytes, readU32(bytes, entryOffset + 28), entry.pageIndexLength, "invalid .col2 page index")
            : new Uint8Array(),
      });
    }
  }
  fieldPayloads.sort((left, right) => left.entry.fieldOrdinal - right.entry.fieldOrdinal);
  const mergedFieldPayloads: EncodedColFieldPayload[] = [];
  for (const payload of fieldPayloads) {
    const previous = mergedFieldPayloads[mergedFieldPayloads.length - 1];
    if (!previous || previous.entry.fieldOrdinal !== payload.entry.fieldOrdinal) {
      mergedFieldPayloads.push(payload);
      continue;
    }
    const mergedPayloadRes = mergeColFieldPayloadsResult(docCount ?? 0, previous, payload);
    if (Result.isError(mergedPayloadRes)) return mergedPayloadRes;
    mergedFieldPayloads[mergedFieldPayloads.length - 1] = mergedPayloadRes.value;
  }
  const encoded = encodeColSectionChunksFromFieldPayloads(docCount ?? 0, primaryTimestampOrdinal, mergedFieldPayloads);
  return Result.ok({ ...encoded, primaryTimestampOrdinal });
}

function mergeColFieldPayloadsResult(
  docCount: number,
  left: EncodedColFieldPayload,
  right: EncodedColFieldPayload
): Result<EncodedColFieldPayload, ColFormatError> {
  if (left.entry.fieldOrdinal !== right.entry.fieldOrdinal) return invalidCol("mismatched .col2 field ordinal while merging partial companions");
  if (left.entry.kind !== right.entry.kind) return invalidCol("mismatched .col2 field kind while merging partial companions");
  const kind = left.entry.kind;
  const width = valueWidth(kind);
  const estimatedPresentCount = left.entry.presentCount + right.entry.presentCount;
  const leftDocs = createDocIdCursorState(docCount, left.entry.existsCodec, left.exists);
  const rightDocs = createDocIdCursorState(docCount, right.entry.existsCodec, right.exists);
  let leftDocId = nextDocId(leftDocs);
  let rightDocId = nextDocId(rightDocs);
  let leftValueOffset = 0;
  let rightValueOffset = 0;
  let mergedCount = 0;
  let previousDocId = -1;
  const deltaBytes = new Uint8Array(Math.max(1, estimatedPresentCount * 5));
  let deltaLength = 0;
  const values = new Uint8Array(Math.max(1, estimatedPresentCount * width));
  const valuesView = new DataView(values.buffer, values.byteOffset, values.byteLength);
  let valuesLength = 0;
  const bitset = createBitset(docCount);
  const pageBytes = estimatedPresentCount > PAGE_SIZE ? new Uint8Array(Math.ceil(estimatedPresentCount / PAGE_SIZE) * PAGE_ENTRY_BYTES) : new Uint8Array(0);
  const pageView = pageBytes.byteLength > 0 ? new DataView(pageBytes.buffer, pageBytes.byteOffset, pageBytes.byteLength) : null;
  let pageLength = 0;
  let pageStartDocId = -1;
  let pageStartIndex = 0;
  let pageMin: ColScalar | null = null;
  let pageMax: ColScalar | null = null;
  let pageCount = 0;
  const flushPage = (): void => {
    if (pageCount === 0) return;
    if (!pageView) return;
    pageView.setUint32(pageLength, pageStartDocId, true);
    pageView.setUint32(pageLength + 4, pageStartIndex, true);
    writeScalarInlineToBuffer(pageBytes, pageLength + 8, kind, pageMin);
    writeScalarInlineToBuffer(pageBytes, pageLength + 16, kind, pageMax);
    pageLength += PAGE_ENTRY_BYTES;
    pageCount = 0;
    pageMin = null;
    pageMax = null;
  };
  while (leftDocId != null || rightDocId != null) {
    const takeLeft = rightDocId == null || (leftDocId != null && leftDocId < rightDocId);
    const docId = takeLeft ? leftDocId! : rightDocId!;
    if (docId === previousDocId) return invalidCol("duplicate .col2 doc_id while merging partial companions");
    const value = takeLeft ? decodeValue(kind, left.values, leftValueOffset) : decodeValue(kind, right.values, rightValueOffset);
    if (takeLeft) {
      leftValueOffset += width;
      leftDocId = nextDocId(leftDocs);
    } else {
      rightValueOffset += width;
      rightDocId = nextDocId(rightDocs);
    }
    bitsetSet(bitset, docId);
    if (mergedCount === 0) {
      deltaLength = writeUVarintToBytes(deltaBytes, deltaLength, docId);
      pageStartDocId = docId;
      pageStartIndex = 0;
      pageMin = value;
      pageMax = value;
      pageCount = 1;
    } else {
      deltaLength = writeUVarintToBytes(deltaBytes, deltaLength, docId - previousDocId);
      if (pageCount === 0) {
        pageStartDocId = docId;
        pageStartIndex = mergedCount;
        pageMin = value;
        pageMax = value;
        pageCount = 1;
      } else {
        if (compareColScalars(value, pageMin!) < 0) pageMin = value;
        if (compareColScalars(value, pageMax!) > 0) pageMax = value;
        pageCount += 1;
      }
    }
    valuesLength = writeScalarValueToBuffer(values, valuesView, valuesLength, kind, value);
    mergedCount += 1;
    if (pageCount === PAGE_SIZE) flushPage();
    previousDocId = docId;
  }
  if (mergedCount > PAGE_SIZE) flushPage();
  let minValue = left.entry.minValue;
  if (right.entry.minValue != null && (minValue == null || compareColScalars(right.entry.minValue, minValue) < 0)) {
    minValue = right.entry.minValue;
  }
  let maxValue = left.entry.maxValue;
  if (right.entry.maxValue != null && (maxValue == null || compareColScalars(right.entry.maxValue, maxValue) > 0)) {
    maxValue = right.entry.maxValue;
  }
  const deltaPayload = deltaBytes.slice(0, deltaLength);
  const bitsetPayload = new Uint8Array(bitset);
  const useAllDocSet = mergedCount === docCount;
  const useDelta = !useAllDocSet && mergedCount > 0 && deltaPayload.byteLength < bitsetPayload.byteLength;
  const valuesPayload = values.slice(0, valuesLength);
  const pages = mergedCount > PAGE_SIZE ? pageBytes.slice(0, pageLength) : new Uint8Array();
  return Result.ok({
    entry: {
      fieldOrdinal: left.entry.fieldOrdinal,
      kind,
      presentCount: mergedCount,
      existsCodec: useAllDocSet ? 0 : useDelta ? 2 : 1,
      existsOffset: 0,
      existsLength: useAllDocSet ? 0 : useDelta ? deltaPayload.byteLength : bitsetPayload.byteLength,
      valuesOffset: 0,
      valuesLength: valuesPayload.byteLength,
      pageIndexOffset: 0,
      pageIndexLength: pages.byteLength,
      minValue,
      maxValue,
    },
    exists: useAllDocSet ? new Uint8Array() : useDelta ? deltaPayload : bitsetPayload,
    values: valuesPayload,
    pages,
  });
}

export function decodeColSegmentCompanionResult(bytes: Uint8Array, plan: SearchCompanionPlan): Result<ColSectionView, ColFormatError> {
  try {
    const cursor = new BinaryCursor(bytes);
    const docCount = cursor.readU32();
    const fieldCount = cursor.readU16();
    const primaryTimestampOrdinal = cursor.readU16();
    const directoryOffset = cursor.offset;
    const fields: ColFieldView[] = [];
    for (let index = 0; index < fieldCount; index++) {
      const entryOffset = directoryOffset + index * DIRECTORY_ENTRY_BYTES;
      if (entryOffset + DIRECTORY_ENTRY_BYTES > bytes.byteLength) return invalidCol("invalid .col2 directory");
      const fieldOrdinal = readU16(bytes, entryOffset);
      const kindCode = bytes[entryOffset + 2]!;
      const flags = bytes[entryOffset + 3]!;
      const presentCount = readU32(bytes, entryOffset + 4);
      const existsCodec = bytes[entryOffset + 8]!;
      const existsOffset = readU32(bytes, entryOffset + 12);
      const existsLength = readU32(bytes, entryOffset + 16);
      const valuesOffset = readU32(bytes, entryOffset + 20);
      const valuesLength = readU32(bytes, entryOffset + 24);
      const pageIndexOffset = readU32(bytes, entryOffset + 28);
      const pageIndexLength = readU32(bytes, entryOffset + 32);
      const kind = CODE_KIND[kindCode];
      if (!kind || (kind !== "integer" && kind !== "float" && kind !== "date" && kind !== "bool")) {
        return invalidCol("invalid .col2 field kind");
      }
      const planField = plan.fields.find((field) => field.ordinal === fieldOrdinal);
      if (!planField) return invalidCol(`missing .col2 plan field ordinal ${fieldOrdinal}`);
      const minValue = (flags & FLAG_HAS_MINMAX) !== 0 ? decodeScalarInline(kind, bytes.subarray(entryOffset + 36, entryOffset + 44)) : null;
      const maxValue = (flags & FLAG_HAS_MINMAX) !== 0 ? decodeScalarInline(kind, bytes.subarray(entryOffset + 44, entryOffset + 52)) : null;
      fields.push(
        new ColFieldView(
          planField.name,
          kind,
          docCount,
          presentCount,
          existsCodec,
          slicePayload(bytes, existsOffset, existsLength, "invalid .col2 exists payload"),
          slicePayload(bytes, valuesOffset, valuesLength, "invalid .col2 values payload"),
          pageIndexLength > 0 ? slicePayload(bytes, pageIndexOffset, pageIndexLength, "invalid .col2 page index") : new Uint8Array(),
          minValue,
          maxValue
        )
      );
    }
    const primaryTimestampField =
      primaryTimestampOrdinal === 0xffff ? null : (plan.fields.find((field) => field.ordinal === primaryTimestampOrdinal)?.name ?? null);
    return Result.ok(new ColSectionView(docCount, primaryTimestampField, fields));
  } catch (e: unknown) {
    return invalidCol(String((e as any)?.message ?? e));
  }
}

function encodeFieldValues(kind: SearchFieldKind, values: ColScalar[]): Uint8Array {
  const width = valueWidth(kind);
  const payload = new Uint8Array(Math.max(1, values.length * width));
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  let offset = 0;
  for (const value of values) offset = writeScalarValueToBuffer(payload, view, offset, kind, value);
  return payload.subarray(0, offset);
}

function writeScalarValueToBuffer(
  bytes: Uint8Array,
  view: DataView,
  offset: number,
  kind: SearchFieldKind,
  value: ColScalar
): number {
  if (kind === "integer" || kind === "date") {
    view.setBigInt64(offset, BigInt.asIntN(64, value as bigint), true);
    return offset + 8;
  }
  if (kind === "float") {
    view.setFloat64(offset, value as number, true);
    return offset + 8;
  }
  bytes[offset] = (value as boolean) ? 1 : 0;
  return offset + 1;
}

function encodePageIndex(kind: SearchFieldKind, docIds: number[], values: ColScalar[]): Uint8Array {
  if (docIds.length <= PAGE_SIZE) return new Uint8Array();
  const writer = new BinaryWriter();
  for (let start = 0; start < docIds.length; start += PAGE_SIZE) {
    const end = Math.min(docIds.length, start + PAGE_SIZE);
    let min = values[start]!;
    let max = values[start]!;
    for (let index = start + 1; index < end; index++) {
      const value = values[index]!;
      if (compareColScalars(value, min) < 0) min = value;
      if (compareColScalars(value, max) > 0) max = value;
    }
    writer.writeU32(docIds[start]!);
    writer.writeU32(start);
    writer.writeBytes(encodeScalarInline(kind, min));
    writer.writeBytes(encodeScalarInline(kind, max));
  }
  return writer.finish();
}

function encodeScalarInline(kind: SearchFieldKind, value: ColScalar | null): Uint8Array {
  const writer = new BinaryWriter();
  if (kind === "integer" || kind === "date") writer.writeI64(value == null ? 0n : (value as bigint));
  else if (kind === "float") writer.writeF64(value == null ? Number.NaN : (value as number));
  else if (kind === "bool") {
    writer.writeU8(value == null ? 0xff : (value as boolean) ? 1 : 0);
    writer.writeBytes(new Uint8Array(7));
  } else {
    writer.writeBytes(new Uint8Array(8));
  }
  return writer.finish();
}

function decodeScalarInline(kind: SearchFieldKind, bytes: Uint8Array): ColScalar | null {
  if (kind === "integer" || kind === "date") return readI64(bytes, 0);
  if (kind === "float") {
    const value = readF64(bytes, 0);
    return Number.isNaN(value) ? null : value;
  }
  if (kind === "bool") {
    const value = bytes[0];
    if (value === 0xff) return null;
    return value === 1;
  }
  return null;
}

function decodeValue(kind: SearchFieldKind, bytes: Uint8Array, offset: number): ColScalar {
  if (kind === "integer" || kind === "date") return readI64(bytes, offset);
  if (kind === "float") return readF64(bytes, offset);
  return bytes[offset] === 1;
}

function writeScalarInlineToBuffer(bytes: Uint8Array, offset: number, kind: SearchFieldKind, value: ColScalar | null): void {
  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, 8);
  if (kind === "integer" || kind === "date") {
    view.setBigInt64(0, BigInt.asIntN(64, value == null ? 0n : (value as bigint)), true);
    return;
  }
  if (kind === "float") {
    view.setFloat64(0, value == null ? Number.NaN : (value as number), true);
    return;
  }
  if (kind === "bool") {
    bytes[offset] = value == null ? 0xff : (value as boolean) ? 1 : 0;
    bytes.fill(0, offset + 1, offset + 8);
    return;
  }
  bytes.fill(0, offset, offset + 8);
}

function writeUVarintToBytes(bytes: Uint8Array, offset: number, value: number): number {
  let remaining = value >>> 0;
  while (remaining >= 0x80) {
    bytes[offset++] = (remaining & 0x7f) | 0x80;
    remaining >>>= 7;
  }
  bytes[offset++] = remaining;
  return offset;
}

function createDocIdCursorState(docCount: number, codec: number, payload: Uint8Array): DocIdCursorState {
  if (codec === 0) return { codec: "all", nextDocId: 0, docCount };
  if (codec === 1) return { codec: "bitset", nextDocId: 0, docCount, payload };
  if (codec === 2) return { codec: "delta", cursor: new BinaryCursor(payload), previousDocId: 0, emittedCount: 0 };
  throw new BinaryPayloadError(`unknown docset codec ${codec}`);
}

function nextDocId(state: DocIdCursorState): number | null {
  if (state.codec === "all") {
    if (state.nextDocId >= state.docCount) return null;
    return state.nextDocId++;
  }
  if (state.codec === "bitset") {
    while (state.nextDocId < state.docCount) {
      const candidate = state.nextDocId++;
      const byteIndex = candidate >>> 3;
      const bitMask = 1 << (candidate & 7);
      if ((state.payload[byteIndex] ?? 0) & bitMask) return candidate;
    }
    return null;
  }
  if (state.cursor.remaining() <= 0) return null;
  const delta = Number(readUVarint(state.cursor));
  const next = state.emittedCount === 0 ? delta : state.previousDocId + delta;
  state.previousDocId = next;
  state.emittedCount += 1;
  return next;
}

function valueWidth(kind: SearchFieldKind): number {
  if (kind === "bool") return 1;
  return 8;
}

function slicePayload(bytes: Uint8Array, offset: number, length: number, message: string): Uint8Array {
  if (offset < 0 || length < 0 || offset + length > bytes.byteLength) {
    throw new BinaryPayloadError(message);
  }
  if (length === 0) return new Uint8Array();
  return bytes.subarray(offset, offset + length);
}
