import { Result } from "better-result";
import type { SearchFieldKind } from "../schema/registry";
import { decodeDocIds, encodeDocSet } from "./binary/docset";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, concatBytes, readU16, readU32 } from "./binary/codec";
import { RestartStringTableView, encodeRestartStringTable } from "./binary/restart_strings";
import type { SearchCompanionPlan } from "./companion_plan";

export type ExactFieldInput = {
  kind: SearchFieldKind;
  exists_docs: number[];
  terms: Record<string, number[]>;
};

export type ExactSectionInput = {
  doc_count: number;
  fields: Record<string, ExactFieldInput>;
};

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

const FIELD_DIR_ENTRY_BYTES = 52;

export type ExactFormatError = { kind: "invalid_exact_segment"; message: string };

function invalidExact<T = never>(message: string): Result<T, ExactFormatError> {
  return Result.err({ kind: "invalid_exact_segment", message });
}

class U32LeView {
  private readonly view: DataView;
  readonly length: number;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.length = Math.floor(bytes.byteLength / 4);
  }

  get(index: number): number {
    if (index < 0 || index >= this.length) return 0;
    return this.view.getUint32(index * 4, true);
  }
}

export class ExactFieldView {
  private readonly termsView: RestartStringTableView;
  private readonly docFreqs: U32LeView;
  private readonly postingOffsets: U32LeView;
  private existsDocIdsCache: number[] | null = null;
  private readonly termDocIdsCache = new Map<number, number[]>();

  constructor(
    readonly name: string,
    readonly kind: SearchFieldKind,
    private readonly docCount: number,
    private readonly existsCodec: number,
    private readonly existsPayload: Uint8Array,
    dictPayload: Uint8Array,
    docFreqPayload: Uint8Array,
    postingOffsetsPayload: Uint8Array,
    private readonly postingsPayload: Uint8Array
  ) {
    this.termsView = new RestartStringTableView(dictPayload);
    this.docFreqs = new U32LeView(docFreqPayload);
    this.postingOffsets = new U32LeView(postingOffsetsPayload);
  }

  existsDocIds(): number[] {
    if (!this.existsDocIdsCache) {
      this.existsDocIdsCache = decodeDocIds(this.docCount, this.existsCodec, this.existsPayload);
    }
    return this.existsDocIdsCache;
  }

  lookupTerm(term: string): number | null {
    return this.termsView.lookup(term);
  }

  docFreq(termOrdinal: number): number {
    return this.docFreqs.get(termOrdinal);
  }

  docIds(termOrdinal: number): number[] {
    const cached = this.termDocIdsCache.get(termOrdinal);
    if (cached) return cached;
    const start = this.postingOffsets.get(termOrdinal);
    const end = this.postingOffsets.get(termOrdinal + 1) || start;
    const bytes = this.postingsPayload.subarray(start, end);
    const docIds = decodeDocIds(this.docCount, 0xff & (bytes[0] ?? 0), bytes.subarray(1));
    this.termDocIdsCache.set(termOrdinal, docIds);
    return docIds;
  }
}

export class ExactSectionView {
  private readonly fieldByName = new Map<string, ExactFieldView>();

  constructor(readonly docCount: number, readonly fields: ExactFieldView[]) {
    for (const field of fields) this.fieldByName.set(field.name, field);
  }

  getField(fieldName: string): ExactFieldView | null {
    return this.fieldByName.get(fieldName) ?? null;
  }
}

export function encodeExactSegmentCompanion(input: ExactSectionInput, plan: SearchCompanionPlan): Uint8Array {
  const orderedFields = plan.fields
    .filter((field) => input.fields[field.name] && field.exact && field.kind !== "text")
    .sort((a, b) => a.ordinal - b.ordinal);
  const fieldPayloads: Array<{
    entry: {
      fieldOrdinal: number;
      kind: SearchFieldKind;
      termCount: number;
      existsOffset: number;
      existsLength: number;
      existsCodec: number;
      dictOffset: number;
      dictLength: number;
      dfOffset: number;
      dfLength: number;
      postingsOffsetTableOffset: number;
      postingsOffsetTableLength: number;
      postingsDataOffset: number;
      postingsDataLength: number;
    };
    exists: Uint8Array;
    dict: Uint8Array;
    dfs: Uint8Array;
    postingOffsets: Uint8Array;
    postings: Uint8Array;
  }> = [];

  for (const planField of orderedFields) {
    const field = input.fields[planField.name]!;
    const terms = Object.keys(field.terms).sort((a, b) => a.localeCompare(b));
    const dict = encodeRestartStringTable(terms);
    const encodedExists = encodeDocSet(input.doc_count, field.exists_docs);
    const dfWriter = new BinaryWriter();
    const postingOffsetWriter = new BinaryWriter();
    const postingsWriter = new BinaryWriter();
    let postingOffset = 0;
    for (const term of terms) {
      const encodedPostings = encodeDocSet(input.doc_count, field.terms[term] ?? []);
      dfWriter.writeU32(encodedPostings.docIds.length);
      postingOffsetWriter.writeU32(postingOffset);
      postingsWriter.writeU8(encodedPostings.codec);
      postingsWriter.writeBytes(encodedPostings.payload);
      postingOffset += 1 + encodedPostings.payload.byteLength;
    }
    postingOffsetWriter.writeU32(postingOffset);
    fieldPayloads.push({
      entry: {
        fieldOrdinal: planField.ordinal,
        kind: field.kind,
        termCount: terms.length,
        existsOffset: 0,
        existsLength: encodedExists.payload.byteLength,
        existsCodec: encodedExists.codec,
        dictOffset: 0,
        dictLength: dict.byteLength,
        dfOffset: 0,
        dfLength: dfWriter.length,
        postingsOffsetTableOffset: 0,
        postingsOffsetTableLength: postingOffsetWriter.length,
        postingsDataOffset: 0,
        postingsDataLength: postingsWriter.length,
      },
      exists: encodedExists.payload,
      dict,
      dfs: dfWriter.finish(),
      postingOffsets: postingOffsetWriter.finish(),
      postings: postingsWriter.finish(),
    });
  }

  const header = new BinaryWriter();
  header.writeU32(input.doc_count);
  header.writeU16(fieldPayloads.length);
  header.writeU16(0);

  let payloadOffset = header.length + FIELD_DIR_ENTRY_BYTES * fieldPayloads.length;
  for (const payload of fieldPayloads) {
    payload.entry.existsOffset = payloadOffset;
    payloadOffset += payload.exists.byteLength;
    payload.entry.dictOffset = payloadOffset;
    payloadOffset += payload.dict.byteLength;
    payload.entry.dfOffset = payloadOffset;
    payloadOffset += payload.dfs.byteLength;
    payload.entry.postingsOffsetTableOffset = payloadOffset;
    payloadOffset += payload.postingOffsets.byteLength;
    payload.entry.postingsDataOffset = payloadOffset;
    payloadOffset += payload.postings.byteLength;
  }

  const directory = new BinaryWriter();
  for (const payload of fieldPayloads) {
    directory.writeU16(payload.entry.fieldOrdinal);
    directory.writeU8(KIND_CODE[payload.entry.kind] ?? 0);
    directory.writeU8(0);
    directory.writeU32(payload.entry.termCount);
    directory.writeU32(payload.entry.existsOffset);
    directory.writeU32(payload.entry.existsLength);
    directory.writeU32((payload.entry.existsCodec << 24) | 0);
    directory.writeU32(payload.entry.dictOffset);
    directory.writeU32(payload.entry.dictLength);
    directory.writeU32(payload.entry.dfOffset);
    directory.writeU32(payload.entry.dfLength);
    directory.writeU32(payload.entry.postingsOffsetTableOffset);
    directory.writeU32(payload.entry.postingsOffsetTableLength);
    directory.writeU32(payload.entry.postingsDataOffset);
    directory.writeU32(payload.entry.postingsDataLength);
  }

  return concatBytes([
    header.finish(),
    directory.finish(),
    ...fieldPayloads.flatMap((payload) => [payload.exists, payload.dict, payload.dfs, payload.postingOffsets, payload.postings]),
  ]);
}

export function decodeExactSegmentCompanionResult(bytes: Uint8Array, plan: SearchCompanionPlan): Result<ExactSectionView, ExactFormatError> {
  try {
    const cursor = new BinaryCursor(bytes);
    const docCount = cursor.readU32();
    const fieldCount = cursor.readU16();
    cursor.readU16();
    const directoryOffset = cursor.offset;
    const fields: ExactFieldView[] = [];
    for (let index = 0; index < fieldCount; index++) {
      const entryOffset = directoryOffset + index * FIELD_DIR_ENTRY_BYTES;
      if (entryOffset + FIELD_DIR_ENTRY_BYTES > bytes.byteLength) return invalidExact("invalid .exact2 directory");
      const fieldOrdinal = readU16(bytes, entryOffset);
      const kindCode = bytes[entryOffset + 2]!;
      const existsCodec = readU32(bytes, entryOffset + 16) >>> 24;
      const planField = plan.fields.find((field) => field.ordinal === fieldOrdinal);
      if (!planField) return invalidExact(`missing .exact2 plan field ordinal ${fieldOrdinal}`);
      const kind = CODE_KIND[kindCode];
      if (!kind) return invalidExact("invalid .exact2 field kind");
      fields.push(
        new ExactFieldView(
          planField.name,
          kind,
          docCount,
          existsCodec,
          slicePayload(bytes, readU32(bytes, entryOffset + 8), readU32(bytes, entryOffset + 12), "invalid .exact2 exists payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 20), readU32(bytes, entryOffset + 24), "invalid .exact2 dict payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 28), readU32(bytes, entryOffset + 32), "invalid .exact2 docfreq payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 36), readU32(bytes, entryOffset + 40), "invalid .exact2 posting-offset payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 44), readU32(bytes, entryOffset + 48), "invalid .exact2 postings payload")
        )
      );
    }
    return Result.ok(new ExactSectionView(docCount, fields));
  } catch (e: unknown) {
    return invalidExact(String((e as any)?.message ?? e));
  }
}

function slicePayload(bytes: Uint8Array, offset: number, length: number, message: string): Uint8Array {
  if (offset < 0 || length < 0 || offset + length > bytes.byteLength) {
    throw new BinaryPayloadError(message);
  }
  return bytes.subarray(offset, offset + length);
}
