import { Result } from "better-result";
import type { SearchFieldKind } from "../schema/registry";
import { decodeDocIds, encodeDocSet } from "./binary/docset";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, concatBytes, readF64, readI64, readU16, readU32 } from "./binary/codec";
import type { SearchCompanionPlan } from "./companion_plan";

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

type ColPageEntry = {
  firstDocId: number;
  valueStartIndex: number;
  min: ColScalar;
  max: ColScalar;
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
  const orderedFields = plan.fields
    .filter((field) => input.fields[field.name] && (field.kind === "integer" || field.kind === "float" || field.kind === "date" || field.kind === "bool"))
    .sort((a, b) => a.ordinal - b.ordinal);
  const fieldPayloads: Array<{ entry: ColFieldDirectoryEntry; exists: Uint8Array; values: Uint8Array; pages: Uint8Array }> = [];
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

  const header = new BinaryWriter();
  header.writeU32(input.doc_count);
  header.writeU16(fieldPayloads.length);
  header.writeU16(
    input.primary_timestamp_field == null ? 0xffff : (plan.fields.find((field) => field.name === input.primary_timestamp_field)?.ordinal ?? 0xffff)
  );

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

  return concatBytes([header.finish(), directory.finish(), ...fieldPayloads.flatMap((payload) => [payload.exists, payload.values, payload.pages])]);
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
  const writer = new BinaryWriter();
  for (const value of values) {
    if (kind === "integer" || kind === "date") writer.writeI64(value as bigint);
    else if (kind === "float") writer.writeF64(value as number);
    else if (kind === "bool") writer.writeU8((value as boolean) ? 1 : 0);
  }
  return writer.finish();
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
