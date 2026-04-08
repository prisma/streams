import { closeSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeSync } from "node:fs";
import { join } from "node:path";
import { Result } from "better-result";
import { concatBytes, readU32BE, readU64BE, writeU32BE, writeU64BE } from "../util/endian";
import { INDEX_RUN_MAGIC, INDEX_RUN_VERSION, RUN_TYPE_MASK16, RUN_TYPE_POSTINGS } from "./run_format";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../util/stream_paths";

export type SecondaryCompactionRunSource = {
  runId: string;
  startSegment: number;
  endSegment: number;
} & ({ localPath: string; deleteAfterUse?: boolean; bytes?: undefined } | { localPath?: undefined; bytes: Uint8Array });

export type SecondaryCompactionBuildInput = {
  stream: string;
  indexName: string;
  level: number;
  outputDir?: string;
  inputs: SecondaryCompactionRunSource[];
};

export type SecondaryCompactionBuildOutput = {
  meta: {
    runId: string;
    level: number;
    startSegment: number;
    endSegment: number;
    objectKey: string;
    filterLen: number;
    recordCount: number;
  };
} & (
  | {
      storage: "bytes";
      payload: Uint8Array;
    }
  | {
      storage: "file";
      localPath: string;
      sizeBytes: number;
    }
);

type BuildError = {
  kind: "invalid_index_build";
  message: string;
};

function invalidIndexBuild(message: string): Result<never, BuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function readSourceBytes(source: SecondaryCompactionRunSource): Uint8Array {
  return source.localPath ? readFileSync(source.localPath) : source.bytes!;
}

type InputCursor = {
  startSegment: number;
  runType: number;
  recordCount: number;
  body: Uint8Array;
  pos: number;
  nextIndex: number;
  currentFp: bigint | null;
  currentMask: number;
  currentPostings: number[] | null;
};

interface DataWriter {
  readonly dataLength: number;
  writeU64(value: bigint): Result<void, BuildError>;
  writeUVarint(value: number): Result<void, BuildError>;
  finish(meta: SecondaryCompactionBuildOutput["meta"]): Result<SecondaryCompactionBuildOutput, BuildError>;
  abort(): void;
}

class GrowableBytesWriter implements DataWriter {
  private bytes: Uint8Array;
  private offset = 0;

  constructor(initialCapacity: number) {
    this.bytes = new Uint8Array(Math.max(64, initialCapacity));
  }

  get dataLength(): number {
    return this.offset;
  }

  writeU64(value: bigint): Result<void, BuildError> {
    this.ensureCapacity(8);
    writeU64BE(this.bytes, this.offset, value);
    this.offset += 8;
    return Result.ok(undefined);
  }

  writeUVarint(value: number): Result<void, BuildError> {
    let v = value >>> 0;
    while (v >= 0x80) {
      this.ensureCapacity(1);
      this.bytes[this.offset++] = (v & 0x7f) | 0x80;
      v >>>= 7;
    }
    this.ensureCapacity(1);
    this.bytes[this.offset++] = v;
    return Result.ok(undefined);
  }

  finish(meta: SecondaryCompactionBuildOutput["meta"]): Result<SecondaryCompactionBuildOutput, BuildError> {
    const dataBytes = this.bytes.slice(0, this.offset);
    const header = buildRunHeader(meta, dataBytes.byteLength);
    return Result.ok({
      meta,
      storage: "bytes",
      payload: concatBytes([header, dataBytes]),
    });
  }

  abort(): void {}

  private ensureCapacity(needed: number): void {
    if (this.offset + needed <= this.bytes.byteLength) return;
    let next = this.bytes.byteLength;
    while (next < this.offset + needed) next *= 2;
    const grown = new Uint8Array(next);
    grown.set(this.bytes, 0);
    this.bytes = grown;
  }
}

class FileBackedDataWriter implements DataWriter {
  private readonly finalPath: string;
  private readonly tmpPath: string;
  private readonly scratch = new Uint8Array(10);
  private readonly buffer = new Uint8Array(256 * 1024);
  private fd: number | null;
  private offset = 0;
  private bufferedBytes = 0;

  constructor(outputDir: string, filePrefix: string) {
    mkdirSync(outputDir, { recursive: true });
    const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
    this.finalPath = join(outputDir, `${filePrefix}-${nonce}.irn`);
    this.tmpPath = `${this.finalPath}.tmp`;
    this.fd = openSync(this.tmpPath, "w");
    const placeholderHeader = new Uint8Array(36);
    let written = 0;
    while (written < placeholderHeader.byteLength) {
      written += writeSync(this.fd, placeholderHeader, written, placeholderHeader.byteLength - written);
    }
  }

  get dataLength(): number {
    return this.offset;
  }

  writeU64(value: bigint): Result<void, BuildError> {
    writeU64BE(this.scratch, 0, value);
    return this.writeChunk(this.scratch.subarray(0, 8));
  }

  writeUVarint(value: number): Result<void, BuildError> {
    let v = value >>> 0;
    let len = 0;
    while (v >= 0x80) {
      this.scratch[len++] = (v & 0x7f) | 0x80;
      v >>>= 7;
    }
    this.scratch[len++] = v;
    return this.writeChunk(this.scratch.subarray(0, len));
  }

  finish(meta: SecondaryCompactionBuildOutput["meta"]): Result<SecondaryCompactionBuildOutput, BuildError> {
    try {
      const flushRes = this.flushBuffer();
      if (Result.isError(flushRes)) return flushRes;
      const header = buildRunHeader(meta, this.offset);
      if (this.fd == null) return invalidIndexBuild("secondary compaction output already closed");
      let written = 0;
      while (written < header.byteLength) {
        written += writeSync(this.fd, header, written, header.byteLength - written, written);
      }
      closeSync(this.fd);
      this.fd = null;
      renameSync(this.tmpPath, this.finalPath);
      return Result.ok({
        meta,
        storage: "file",
        localPath: this.finalPath,
        sizeBytes: 36 + this.offset,
      });
    } catch (error: unknown) {
      this.abort();
      return invalidIndexBuild(String((error as any)?.message ?? error));
    }
  }

  abort(): void {
    if (this.fd != null) {
      try {
        closeSync(this.fd);
      } catch {
        // ignore close failures
      }
      this.fd = null;
    }
    try {
      unlinkSync(this.tmpPath);
    } catch {
      // ignore temp cleanup failures
    }
  }

  private writeChunk(chunk: Uint8Array): Result<void, BuildError> {
    try {
      if (this.fd == null) return invalidIndexBuild("secondary compaction output already closed");
      if (chunk.byteLength >= this.buffer.byteLength) {
        const flushRes = this.flushBuffer();
        if (Result.isError(flushRes)) return flushRes;
        let written = 0;
        while (written < chunk.byteLength) {
          written += writeSync(this.fd, chunk, written, chunk.byteLength - written);
        }
        this.offset += chunk.byteLength;
        return Result.ok(undefined);
      }
      if (this.bufferedBytes + chunk.byteLength > this.buffer.byteLength) {
        const flushRes = this.flushBuffer();
        if (Result.isError(flushRes)) return flushRes;
      }
      this.buffer.set(chunk, this.bufferedBytes);
      this.bufferedBytes += chunk.byteLength;
      this.offset += chunk.byteLength;
      return Result.ok(undefined);
    } catch (error: unknown) {
      return invalidIndexBuild(String((error as any)?.message ?? error));
    }
  }

  private flushBuffer(): Result<void, BuildError> {
    try {
      if (this.fd == null) return invalidIndexBuild("secondary compaction output already closed");
      let written = 0;
      while (written < this.bufferedBytes) {
        written += writeSync(this.fd, this.buffer, written, this.bufferedBytes - written);
      }
      this.bufferedBytes = 0;
      return Result.ok(undefined);
    } catch (error: unknown) {
      return invalidIndexBuild(String((error as any)?.message ?? error));
    }
  }
}

function buildRunHeader(meta: SecondaryCompactionBuildOutput["meta"], dataLength: number): Uint8Array {
  const payloadHeader = new Uint8Array(36);
  payloadHeader[0] = INDEX_RUN_MAGIC.charCodeAt(0);
  payloadHeader[1] = INDEX_RUN_MAGIC.charCodeAt(1);
  payloadHeader[2] = INDEX_RUN_MAGIC.charCodeAt(2);
  payloadHeader[3] = INDEX_RUN_MAGIC.charCodeAt(3);
  payloadHeader[4] = INDEX_RUN_VERSION;
  payloadHeader[5] = RUN_TYPE_POSTINGS & 0xff;
  payloadHeader[6] = meta.level & 0xff;
  payloadHeader[7] = 0;
  writeU64BE(payloadHeader, 8, BigInt(meta.startSegment));
  writeU64BE(payloadHeader, 16, BigInt(meta.endSegment));
  writeU32BE(payloadHeader, 24, meta.recordCount);
  writeU32BE(payloadHeader, 28, 0);
  writeU32BE(payloadHeader, 32, dataLength);
  return payloadHeader;
}

function decodeUVarint(bytes: Uint8Array, offset: number): { value: number; next: number } | null {
  let x = 0;
  let s = 0;
  for (let i = 0; i < 10 && offset + i < bytes.byteLength; i += 1) {
    const b = bytes[offset + i]!;
    if (b < 0x80) {
      if (i === 9 && b > 1) return null;
      return { value: x | (b << s), next: offset + i + 1 };
    }
    x |= (b & 0x7f) << s;
    s += 7;
  }
  return null;
}

function buildInputCursorResult(meta: SecondaryCompactionRunSource): Result<InputCursor, BuildError> {
  const data = readSourceBytes(meta);
  if (data.byteLength < 36) return invalidIndexBuild(`secondary compact run ${meta.runId} too short`);
  const magic = String.fromCharCode(data[0]!, data[1]!, data[2]!, data[3]!);
  if (magic !== INDEX_RUN_MAGIC) return invalidIndexBuild(`secondary compact run ${meta.runId} invalid magic`);
  if (data[4] !== INDEX_RUN_VERSION) return invalidIndexBuild(`secondary compact run ${meta.runId} unsupported version`);
  const runType = data[5]!;
  if (runType !== RUN_TYPE_MASK16 && runType !== RUN_TYPE_POSTINGS) {
    return invalidIndexBuild(`unknown run type ${runType}`);
  }
  const startSegment = Number(readU64BE(data, 8));
  const endSegment = Number(readU64BE(data, 16));
  if (startSegment !== meta.startSegment || endSegment !== meta.endSegment) {
    return invalidIndexBuild(`secondary compact run ${meta.runId} metadata mismatch`);
  }
  const recordCount = readU32BE(data, 24);
  const filterLen = readU32BE(data, 28);
  const dataLen = readU32BE(data, 32);
  const totalLen = 36 + filterLen + dataLen;
  if (totalLen > data.byteLength) return invalidIndexBuild(`secondary compact run ${meta.runId} truncated`);
  const cursor: InputCursor = {
    startSegment: meta.startSegment,
    runType,
    recordCount,
    body: data.subarray(36 + filterLen, totalLen),
    pos: 0,
    nextIndex: 0,
    currentFp: null,
    currentMask: 0,
    currentPostings: null,
  };
  const advanceRes = advanceInputCursorResult(cursor);
  if (Result.isError(advanceRes)) return advanceRes;
  return Result.ok(cursor);
}

function advanceInputCursorResult(cursor: InputCursor): Result<void, BuildError> {
  if (cursor.nextIndex >= cursor.recordCount) {
    cursor.currentFp = null;
    cursor.currentMask = 0;
    cursor.currentPostings = null;
    return Result.ok(undefined);
  }
  if (cursor.runType === RUN_TYPE_MASK16) {
    if (cursor.pos + 10 > cursor.body.byteLength) return invalidIndexBuild("secondary compact mask run truncated");
    cursor.currentFp = readU64BE(cursor.body, cursor.pos);
    cursor.currentMask = ((cursor.body[cursor.pos + 8] ?? 0) << 8) | (cursor.body[cursor.pos + 9] ?? 0);
    cursor.currentPostings = null;
    cursor.pos += 10;
    cursor.nextIndex += 1;
    return Result.ok(undefined);
  }

  if (cursor.pos + 8 > cursor.body.byteLength) return invalidIndexBuild("secondary compact postings run truncated (fp)");
  cursor.currentFp = readU64BE(cursor.body, cursor.pos);
  cursor.pos += 8;
  const countRes = decodeUVarint(cursor.body, cursor.pos);
  if (!countRes) return invalidIndexBuild("secondary compact postings run truncated (count)");
  cursor.pos = countRes.next;
  const postings = new Array<number>(countRes.value);
  for (let i = 0; i < countRes.value; i += 1) {
    const postingRes = decodeUVarint(cursor.body, cursor.pos);
    if (!postingRes) return invalidIndexBuild("secondary compact postings run truncated (value)");
    cursor.pos = postingRes.next;
    postings[i] = postingRes.value;
  }
  cursor.currentMask = 0;
  cursor.currentPostings = postings;
  cursor.nextIndex += 1;
  return Result.ok(undefined);
}

function countMaskBits(mask: number): number {
  let count = 0;
  let value = mask >>> 0;
  while (value !== 0) {
    count += value & 1;
    value >>>= 1;
  }
  return count;
}

function encodeSingleCursorPostingsResult(writer: DataWriter, cursor: InputCursor, startSegment: number): Result<number, BuildError> {
  if (cursor.runType === RUN_TYPE_MASK16) {
    const mask = cursor.currentMask;
    const count = countMaskBits(mask);
    const countRes = writer.writeUVarint(count);
    if (Result.isError(countRes)) return countRes;
    for (let bit = 0; bit < 16; bit += 1) {
      if ((mask & (1 << bit)) === 0) continue;
      const writeRes = writer.writeUVarint(cursor.startSegment + bit - startSegment);
      if (Result.isError(writeRes)) return writeRes;
    }
    return Result.ok(count);
  }
  const postings = cursor.currentPostings ?? [];
  const countRes = writer.writeUVarint(postings.length);
  if (Result.isError(countRes)) return countRes;
  for (const rel of postings) {
    const writeRes = writer.writeUVarint(cursor.startSegment + rel - startSegment);
    if (Result.isError(writeRes)) return writeRes;
  }
  return Result.ok(postings.length);
}

function mergeRelativeSegments(cursors: InputCursor[], startSegment: number): number[] {
  if (cursors.length === 1) {
    const cursor = cursors[0]!;
    if (cursor.runType === RUN_TYPE_MASK16) {
      const rel: number[] = [];
      const mask = cursor.currentMask;
      for (let bit = 0; bit < 16; bit += 1) {
        if ((mask & (1 << bit)) !== 0) rel.push(cursor.startSegment + bit - startSegment);
      }
      return rel;
    }
    const postings = cursor.currentPostings ?? [];
    return postings.map((rel) => cursor.startSegment + rel - startSegment);
  }
  const merged: number[] = [];
  let last = Number.NaN;
  const positions = cursors.map(() => 0);
  while (true) {
    let minSeg = Number.POSITIVE_INFINITY;
    let found = false;
    for (let i = 0; i < cursors.length; i += 1) {
      const cursor = cursors[i]!;
      if (cursor.runType === RUN_TYPE_MASK16) {
        let pos = positions[i]!;
        while (pos < 16 && (cursor.currentMask & (1 << pos)) === 0) pos += 1;
        positions[i] = pos;
        if (pos >= 16) continue;
        const seg = cursor.startSegment + pos;
        if (seg < minSeg) minSeg = seg;
        found = true;
        continue;
      }
      const postings = cursor.currentPostings ?? [];
      const pos = positions[i]!;
      if (pos >= postings.length) continue;
      const seg = cursor.startSegment + postings[pos]!;
      if (seg < minSeg) minSeg = seg;
      found = true;
    }
    if (!found) break;
    for (let i = 0; i < cursors.length; i += 1) {
      const cursor = cursors[i]!;
      if (cursor.runType === RUN_TYPE_MASK16) {
        if (positions[i]! < 16 && cursor.startSegment + positions[i]! === minSeg) positions[i]! += 1;
        continue;
      }
      const postings = cursor.currentPostings ?? [];
      while (positions[i]! < postings.length && cursor.startSegment + postings[positions[i]!] === minSeg) positions[i]! += 1;
    }
    if (minSeg === last) continue;
    merged.push(minSeg - startSegment);
    last = minSeg;
  }
  return merged;
}

function writeMergedPostingsResult(writer: DataWriter, cursors: InputCursor[], startSegment: number): Result<void, BuildError> {
  const merged = mergeRelativeSegments(cursors, startSegment);
  const countRes = writer.writeUVarint(merged.length);
  if (Result.isError(countRes)) return countRes;
  for (const rel of merged) {
    const writeRes = writer.writeUVarint(rel);
    if (Result.isError(writeRes)) return writeRes;
  }
  return Result.ok(undefined);
}

export function buildSecondaryCompactionPayloadResult(
  input: SecondaryCompactionBuildInput
): Result<SecondaryCompactionBuildOutput, BuildError> {
  if (input.inputs.length === 0) return invalidIndexBuild("compact: missing inputs");
  const cursors: InputCursor[] = [];
  for (const meta of input.inputs) {
    const cursorRes = buildInputCursorResult(meta);
    if (Result.isError(cursorRes)) return cursorRes;
    if (cursorRes.value.currentFp != null) cursors.push(cursorRes.value);
  }

  const startSegment = input.inputs[0]!.startSegment;
  const endSegment = input.inputs[input.inputs.length - 1]!.endSegment;
  const runId = `${input.indexName}-l${input.level}-${startSegment.toString().padStart(16, "0")}-${endSegment
    .toString()
    .padStart(16, "0")}-${Date.now()}`;
  const meta = {
    runId,
    level: input.level,
    startSegment,
    endSegment,
    objectKey: secondaryIndexRunObjectKey(streamHash16Hex(input.stream), input.indexName, runId),
    filterLen: 0,
    recordCount: 0,
  };

  const writer: DataWriter = input.outputDir
    ? new FileBackedDataWriter(input.outputDir, `${input.indexName}-l${input.level}-${startSegment}-${endSegment}`)
    : new GrowableBytesWriter(1024 * 1024);
  let succeeded = false;

  try {
    while (true) {
      let nextFp: bigint | null = null;
      for (const cursor of cursors) {
        if (cursor.currentFp == null) continue;
        if (nextFp == null || cursor.currentFp < nextFp) nextFp = cursor.currentFp;
      }
      if (nextFp == null) break;

      const fpRes = writer.writeU64(nextFp);
      if (Result.isError(fpRes)) return fpRes;
      const matchingCursors: InputCursor[] = [];
      for (const cursor of cursors) {
        if (cursor.currentFp !== nextFp) continue;
        matchingCursors.push(cursor);
      }
      if (matchingCursors.length === 1) {
        const singleRes = encodeSingleCursorPostingsResult(writer, matchingCursors[0]!, startSegment);
        if (Result.isError(singleRes)) return singleRes;
      } else {
        const mergedRes = writeMergedPostingsResult(writer, matchingCursors, startSegment);
        if (Result.isError(mergedRes)) return mergedRes;
      }
      for (const cursor of matchingCursors) {
        const advanceRes = advanceInputCursorResult(cursor);
        if (Result.isError(advanceRes)) return advanceRes;
      }
      meta.recordCount += 1;
    }
    const finishRes = writer.finish(meta);
    if (Result.isError(finishRes)) return finishRes;
    succeeded = true;
    return finishRes;
  } finally {
    if (!succeeded) writer.abort();
  }
}
