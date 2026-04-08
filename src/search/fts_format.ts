import { Result } from "better-result";
import type { SearchFieldKind } from "../schema/registry";
import { compareSearchStrings } from "./schema";
import { decodeDocIds, encodeDocSet } from "./binary/docset";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, concatBytes, readU16, readU32 } from "./binary/codec";
import { RestartStringTableView, encodeRestartStringTable } from "./binary/restart_strings";
import { readUVarint, writeUVarint } from "./binary/varint";
import type { SearchCompanionPlan } from "./companion_plan";

export type FtsTermInput = {
  doc_ids: number[];
  freqs?: number[];
  positions?: number[];
};

export type FtsFieldInput = {
  kind: SearchFieldKind;
  exact?: boolean;
  prefix?: boolean;
  positions?: boolean;
  exists_docs: number[];
  terms: Record<string, FtsTermInput>;
};

export type FtsSectionInput = {
  doc_count: number;
  fields: Record<string, FtsFieldInput>;
};

export type FtsPostingBlock = {
  docIds: Uint32Array;
  freqs: Uint32Array;
  positions?: Uint32Array;
  posOffsets?: Uint32Array;
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

const FLAG_EXACT = 1 << 0;
const FLAG_PREFIX = 1 << 1;
const FLAG_POSITIONS = 1 << 2;
const BLOCK_FLAG_POSITIONS = 1 << 0;

const FIELD_DIR_ENTRY_BYTES = 52;
const BLOCK_POSTING_LIMIT = 128;

export type FtsFormatError = { kind: "invalid_fts_segment"; message: string };

function invalidFts<T = never>(message: string): Result<T, FtsFormatError> {
  return Result.err({ kind: "invalid_fts_segment", message });
}

type FieldDirectoryEntry = {
  fieldOrdinal: number;
  kind: SearchFieldKind;
  exact: boolean;
  prefix: boolean;
  positions: boolean;
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

export function appendKeywordPostingDoc(postings: FtsTermInput | undefined, docId: number): FtsTermInput {
  if (!postings) return { doc_ids: [docId] };
  const docIds = postings.doc_ids;
  if (docIds.length === 0 || docIds[docIds.length - 1] !== docId) docIds.push(docId);
  return postings;
}

export function ftsTermDocCount(postings: FtsTermInput): number {
  return postings.doc_ids.length;
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

export class PostingIterator {
  private cursor: BinaryCursor;
  private readonly endOffset: number;

  constructor(private readonly bytes: Uint8Array) {
    this.cursor = new BinaryCursor(bytes);
    this.endOffset = bytes.byteLength;
  }

  nextBlock(): FtsPostingBlock | null {
    if (this.cursor.offset >= this.endOffset) return null;
    const docsInBlock = this.cursor.readU16();
    const flags = this.cursor.readU8();
    this.cursor.skip(1);
    const firstDocId = this.cursor.readU32();
    const docDeltaLength = this.cursor.readU32();
    const freqLength = this.cursor.readU32();
    const posLength = this.cursor.readU32();
    const docDeltaBytes = this.cursor.readBytes(docDeltaLength);
    const freqBytes = this.cursor.readBytes(freqLength);
    const posBytes = this.cursor.readBytes(posLength);

    const docIds = new Uint32Array(docsInBlock);
    docIds[0] = firstDocId;
    if (docsInBlock > 1) {
      const deltaCursor = new BinaryCursor(docDeltaBytes);
      let previous = firstDocId;
      for (let index = 1; index < docsInBlock; index++) {
        previous += Number(readUVarint(deltaCursor));
        docIds[index] = previous;
      }
    }

    const freqs = new Uint32Array(docsInBlock);
    const freqCursor = new BinaryCursor(freqBytes);
    let totalPositions = 0;
    for (let index = 0; index < docsInBlock; index++) {
      freqs[index] = Number(readUVarint(freqCursor));
      totalPositions += freqs[index]!;
    }

    if ((flags & BLOCK_FLAG_POSITIONS) === 0) {
      return { docIds, freqs };
    }

    const positions = new Uint32Array(totalPositions);
    const posOffsets = new Uint32Array(docsInBlock + 1);
    const posCursor = new BinaryCursor(posBytes);
    let writeOffset = 0;
    for (let docIndex = 0; docIndex < docsInBlock; docIndex++) {
      posOffsets[docIndex] = writeOffset;
      let previous = 0;
      for (let freqIndex = 0; freqIndex < freqs[docIndex]!; freqIndex++) {
        previous += Number(readUVarint(posCursor));
        positions[writeOffset++] = previous;
      }
    }
    posOffsets[docsInBlock] = writeOffset;
    return { docIds, freqs, positions, posOffsets };
  }
}

export class FtsFieldView {
  private readonly termsView: RestartStringTableView;
  private readonly docFreqs: U32LeView;
  private readonly postingOffsets: U32LeView;
  private existsDocIdsCache: number[] | null = null;

  constructor(
    readonly name: string,
    readonly kind: SearchFieldKind,
    readonly exact: boolean,
    readonly prefix: boolean,
    readonly positions: boolean,
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

  terms(): string[] {
    return this.termsView.terms();
  }

  lookupTerm(term: string): number | null {
    return this.termsView.lookup(term);
  }

  expandPrefixResult(prefix: string, limit: number): Result<number[], { message: string }> {
    return this.termsView.expandPrefixResult(prefix, limit);
  }

  docFreq(termOrdinal: number): number {
    return this.docFreqs.get(termOrdinal);
  }

  postings(termOrdinal: number): PostingIterator {
    const start = this.postingOffsets.get(termOrdinal);
    const end = this.postingOffsets.get(termOrdinal + 1) || start;
    return new PostingIterator(this.postingsPayload.subarray(start, end));
  }
}

export class FtsSectionView {
  private readonly fieldByName = new Map<string, FtsFieldView>();

  constructor(readonly docCount: number, readonly fields: FtsFieldView[]) {
    for (const field of fields) this.fieldByName.set(field.name, field);
  }

  getField(fieldName: string): FtsFieldView | null {
    return this.fieldByName.get(fieldName) ?? null;
  }
}

export function encodeFtsSegmentCompanion(input: FtsSectionInput, plan: SearchCompanionPlan): Uint8Array {
  const orderedFields = plan.fields
    .filter((field) => input.fields[field.name] && (field.kind === "text" || (field.kind === "keyword" && field.prefix)))
    .sort((a, b) => a.ordinal - b.ordinal);
  const fieldPayloads: Array<{
    entry: FieldDirectoryEntry;
    exists: Uint8Array;
    dict: Uint8Array;
    dfs: Uint8Array;
    postingOffsets: Uint8Array;
    postings: Uint8Array;
  }> = [];

  for (const planField of orderedFields) {
    const field = input.fields[planField.name]!;
    const terms = Object.keys(field.terms).sort(compareSearchStrings);
    const dict = encodeRestartStringTable(terms);
    const encodedDocSet = encodeDocSet(input.doc_count, field.exists_docs);
    const singletonPayload = encodeSingletonFieldPayloadResult(field, terms);
    const dfWriter = singletonPayload ? null : new BinaryWriter();
    const postingOffsetWriter = singletonPayload ? null : new BinaryWriter();
    const postingsWriter = singletonPayload ? null : new BinaryWriter();
    let postingOffset = 0;
    if (!singletonPayload) {
      for (const term of terms) {
        const postings = field.terms[term] ?? { doc_ids: [] };
        dfWriter!.writeU32(ftsTermDocCount(postings));
        postingOffsetWriter!.writeU32(postingOffset);
        const payload = encodePostingList(postings, field.positions === true);
        postingsWriter!.writeBytes(payload);
        postingOffset += payload.byteLength;
      }
      postingOffsetWriter!.writeU32(postingOffset);
    }
    fieldPayloads.push({
      entry: {
        fieldOrdinal: planField.ordinal,
        kind: field.kind,
        exact: field.exact === true,
        prefix: field.prefix === true,
        positions: field.positions === true,
        termCount: terms.length,
        existsOffset: 0,
        existsLength: encodedDocSet.payload.byteLength,
        existsCodec: encodedDocSet.codec,
        dictOffset: 0,
        dictLength: dict.byteLength,
        dfOffset: 0,
        dfLength: singletonPayload ? singletonPayload.dfs.byteLength : dfWriter!.length,
        postingsOffsetTableOffset: 0,
        postingsOffsetTableLength: singletonPayload ? singletonPayload.postingOffsets.byteLength : postingOffsetWriter!.length,
        postingsDataOffset: 0,
        postingsDataLength: singletonPayload ? singletonPayload.postings.byteLength : postingsWriter!.length,
      },
      exists: encodedDocSet.payload,
      dict,
      dfs: singletonPayload ? singletonPayload.dfs : dfWriter!.finish(),
      postingOffsets: singletonPayload ? singletonPayload.postingOffsets : postingOffsetWriter!.finish(),
      postings: singletonPayload ? singletonPayload.postings : postingsWriter!.finish(),
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
    const flags = (payload.entry.exact ? FLAG_EXACT : 0) | (payload.entry.prefix ? FLAG_PREFIX : 0) | (payload.entry.positions ? FLAG_POSITIONS : 0);
    directory.writeU16(payload.entry.fieldOrdinal);
    directory.writeU8(KIND_CODE[payload.entry.kind] ?? 0);
    directory.writeU8(flags);
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

export function decodeFtsSegmentCompanionResult(bytes: Uint8Array, plan: SearchCompanionPlan): Result<FtsSectionView, FtsFormatError> {
  try {
    const cursor = new BinaryCursor(bytes);
    const docCount = cursor.readU32();
    const fieldCount = cursor.readU16();
    cursor.readU16();
    const directoryOffset = cursor.offset;
    const fields: FtsFieldView[] = [];
    for (let index = 0; index < fieldCount; index++) {
      const entryOffset = directoryOffset + index * FIELD_DIR_ENTRY_BYTES;
      if (entryOffset + FIELD_DIR_ENTRY_BYTES > bytes.byteLength) return invalidFts("invalid .fts2 directory");
      const fieldOrdinal = readU16(bytes, entryOffset);
      const kindCode = bytes[entryOffset + 2]!;
      const flags = bytes[entryOffset + 3]!;
      const existsCodec = readU32(bytes, entryOffset + 16) >>> 24;
      const planField = plan.fields.find((field) => field.ordinal === fieldOrdinal);
      if (!planField) return invalidFts(`missing .fts2 plan field ordinal ${fieldOrdinal}`);
      const kind = CODE_KIND[kindCode];
      if (!kind) return invalidFts("invalid .fts2 field kind");
      fields.push(
        new FtsFieldView(
          planField.name,
          kind,
          (flags & FLAG_EXACT) !== 0,
          (flags & FLAG_PREFIX) !== 0,
          (flags & FLAG_POSITIONS) !== 0,
          docCount,
          existsCodec,
          slicePayload(bytes, readU32(bytes, entryOffset + 8), readU32(bytes, entryOffset + 12), "invalid .fts2 exists payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 20), readU32(bytes, entryOffset + 24), "invalid .fts2 dict payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 28), readU32(bytes, entryOffset + 32), "invalid .fts2 docfreq payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 36), readU32(bytes, entryOffset + 40), "invalid .fts2 posting-offset payload"),
          slicePayload(bytes, readU32(bytes, entryOffset + 44), readU32(bytes, entryOffset + 48), "invalid .fts2 postings payload")
        )
      );
    }
    return Result.ok(new FtsSectionView(docCount, fields));
  } catch (e: unknown) {
    return invalidFts(String((e as any)?.message ?? e));
  }
}

function encodePostingList(postings: FtsTermInput, withPositions: boolean): Uint8Array {
  if (!withPositions && postings.doc_ids.length === 1) {
    return encodeSingletonPostingList(postings.doc_ids[0] ?? 0);
  }
  const writer = new BinaryWriter();
  const docIds = postings.doc_ids;
  const freqs = postings.freqs ?? [];
  const positions = postings.positions ?? [];
  let positionOffset = 0;
  for (let start = 0; start < docIds.length; start += BLOCK_POSTING_LIMIT) {
    const end = Math.min(docIds.length, start + BLOCK_POSTING_LIMIT);
    const docDeltaWriter = new BinaryWriter();
    const freqWriter = new BinaryWriter();
    const posWriter = new BinaryWriter();
    for (let index = start + 1; index < end; index++) {
      writeUVarint(docDeltaWriter, docIds[index]! - docIds[index - 1]!);
    }
    for (let index = start; index < end; index++) {
      const freq = withPositions ? freqs[index] ?? 0 : 1;
      writeUVarint(freqWriter, freq);
      if (!withPositions) continue;
      let previous = 0;
      for (let posIndex = 0; posIndex < freq; posIndex++) {
        const position = positions[positionOffset++] ?? 0;
        writeUVarint(posWriter, position - previous);
        previous = position;
      }
    }
    writer.writeU16(end - start);
    writer.writeU8(withPositions ? BLOCK_FLAG_POSITIONS : 0);
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

function encodeSingletonPostingList(docId: number): Uint8Array {
  const bytes = new Uint8Array(21);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  view.setUint16(0, 1, true);
  bytes[2] = 0;
  bytes[3] = 0;
  view.setUint32(4, docId, true);
  view.setUint32(8, 0, true);
  view.setUint32(12, 1, true);
  view.setUint32(16, 0, true);
  bytes[20] = 1;
  return bytes;
}

function encodeSingletonFieldPayloadResult(
  field: FtsFieldInput,
  terms: string[]
): { dfs: Uint8Array; postingOffsets: Uint8Array; postings: Uint8Array } | null {
  if (field.positions === true || terms.length === 0) return null;
  const dfs = new Uint8Array(terms.length * 4);
  const postingOffsets = new Uint8Array((terms.length + 1) * 4);
  const postings = new Uint8Array(terms.length * 21);
  const dfsView = new DataView(dfs.buffer, dfs.byteOffset, dfs.byteLength);
  const offsetView = new DataView(postingOffsets.buffer, postingOffsets.byteOffset, postingOffsets.byteLength);
  const postingsView = new DataView(postings.buffer, postings.byteOffset, postings.byteLength);
  for (let index = 0; index < terms.length; index += 1) {
    const postingsEntry = field.terms[terms[index]!] ?? { doc_ids: [] };
    if (postingsEntry.doc_ids.length !== 1) return null;
    dfsView.setUint32(index * 4, 1, true);
    const baseOffset = index * 21;
    offsetView.setUint32(index * 4, baseOffset, true);
    postingsView.setUint16(baseOffset, 1, true);
    postings[baseOffset + 2] = 0;
    postings[baseOffset + 3] = 0;
    postingsView.setUint32(baseOffset + 4, postingsEntry.doc_ids[0] ?? 0, true);
    postingsView.setUint32(baseOffset + 8, 0, true);
    postingsView.setUint32(baseOffset + 12, 1, true);
    postingsView.setUint32(baseOffset + 16, 0, true);
    postings[baseOffset + 20] = 1;
  }
  offsetView.setUint32(terms.length * 4, terms.length * 21, true);
  return { dfs, postingOffsets, postings };
}

function slicePayload(bytes: Uint8Array, offset: number, length: number, message: string): Uint8Array {
  if (offset < 0 || length < 0 || offset + length > bytes.byteLength) {
    throw new BinaryPayloadError(message);
  }
  return bytes.subarray(offset, offset + length);
}
