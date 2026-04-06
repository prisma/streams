import { Result } from "better-result";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, concatBytes } from "./codec";
import { readUVarint, writeUVarint } from "./varint";

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

export class RestartStringTableView {
  private readonly termCount: number;
  private readonly restartInterval: number;
  private readonly restartOffsets: number[];
  private readonly entriesOffset: number;
  private readonly blockFirstTerms = new Map<number, string>();
  private termsCache: string[] | null = null;

  constructor(private readonly bytes: Uint8Array) {
    try {
      const cursor = new BinaryCursor(bytes);
      this.termCount = cursor.readU32();
      this.restartInterval = Math.max(1, cursor.readU16());
      const restartCount = cursor.readU16();
      this.restartOffsets = [];
      for (let i = 0; i < restartCount; i++) this.restartOffsets.push(cursor.readU32());
      this.entriesOffset = cursor.offset;
    } catch {
      this.termCount = 0;
      this.restartInterval = 1;
      this.restartOffsets = [];
      this.entriesOffset = bytes.byteLength;
    }
  }

  count(): number {
    return this.termCount;
  }

  terms(): string[] {
    if (this.termsCache) return this.termsCache;
    try {
      const cursor = new BinaryCursor(this.bytes.subarray(this.entriesOffset));
      const terms: string[] = [];
      let previous = "";
      for (let index = 0; index < this.termCount; index++) {
        if (index % this.restartInterval === 0) previous = "";
        const prefixLength = Number(readUVarint(cursor));
        const suffixLength = Number(readUVarint(cursor));
        const suffix = TEXT_DECODER.decode(cursor.readBytes(suffixLength));
        previous = previous.slice(0, prefixLength) + suffix;
        terms.push(previous);
      }
      this.termsCache = terms;
      return terms;
    } catch {
      this.termsCache = [];
      return this.termsCache;
    }
  }

  lookup(term: string): number | null {
    let low = 0;
    let high = this.termCount - 1;
    while (low <= high) {
      const mid = (low + high) >> 1;
      const current = this.decodeTermAt(mid);
      if (current == null) return null;
      const cmp = current.localeCompare(term);
      if (cmp === 0) return mid;
      if (cmp < 0) low = mid + 1;
      else high = mid - 1;
    }
    return null;
  }

  expandPrefixResult(prefix: string, limit: number): Result<number[], { message: string }> {
    const start = this.lowerBoundOrdinal(prefix);
    const matches: number[] = [];
    for (let index = start; index < this.termCount; index++) {
      const term = this.decodeTermAt(index);
      if (term == null) break;
      if (!term.startsWith(prefix)) break;
      matches.push(index);
      if (matches.length > limit) {
        return Result.err({ message: `prefix expansion exceeds limit (${limit})` });
      }
    }
    return Result.ok(matches);
  }

  lowerBoundOrdinal(target: string): number {
    let low = 0;
    let high = this.termCount;
    while (low < high) {
      const mid = (low + high) >> 1;
      const current = this.decodeTermAt(mid);
      if (current != null && current.localeCompare(target) < 0) low = mid + 1;
      else high = mid;
    }
    return low;
  }

  termAt(termOrdinal: number): string | null {
    return this.decodeTermAt(termOrdinal);
  }

  private decodeTermAt(termOrdinal: number): string | null {
    if (termOrdinal < 0 || termOrdinal >= this.termCount) return null;
    const blockIndex = Math.floor(termOrdinal / this.restartInterval);
    const blockStartOrdinal = blockIndex * this.restartInterval;
    const blockOffset = this.restartOffsets[blockIndex];
    if (blockOffset == null) return null;
    if (termOrdinal === blockStartOrdinal) return this.decodeBlockFirstTerm(blockIndex);
    try {
      const cursor = new BinaryCursor(this.bytes.subarray(this.entriesOffset + blockOffset));
      let previous = "";
      let current = "";
      for (let ordinal = blockStartOrdinal; ordinal <= termOrdinal; ordinal++) {
        const prefixLength = Number(readUVarint(cursor));
        const suffixLength = Number(readUVarint(cursor));
        const suffix = TEXT_DECODER.decode(cursor.readBytes(suffixLength));
        current = previous.slice(0, prefixLength) + suffix;
        previous = current;
      }
      return current;
    } catch {
      return null;
    }
  }

  private decodeBlockFirstTerm(blockIndex: number): string | null {
    const cached = this.blockFirstTerms.get(blockIndex);
    if (cached != null) return cached;
    const blockOffset = this.restartOffsets[blockIndex];
    if (blockOffset == null) return null;
    try {
      const cursor = new BinaryCursor(this.bytes.subarray(this.entriesOffset + blockOffset));
      const prefixLength = Number(readUVarint(cursor));
      const suffixLength = Number(readUVarint(cursor));
      if (prefixLength !== 0) {
        throw new BinaryPayloadError("restart block must begin with zero prefix length");
      }
      const term = TEXT_DECODER.decode(cursor.readBytes(suffixLength));
      this.blockFirstTerms.set(blockIndex, term);
      return term;
    } catch {
      return null;
    }
  }
}

export function encodeRestartStringTable(values: string[], restartInterval = 16): Uint8Array {
  const sorted = [...values];
  const entryWriter = new BinaryWriter();
  const restartOffsets: number[] = [];
  let previous = "";
  for (let index = 0; index < sorted.length; index++) {
    if (restartInterval > 0 && index % restartInterval === 0) {
      restartOffsets.push(entryWriter.length);
      previous = "";
    }
    const value = sorted[index]!;
    const prefixLength = sharedPrefixLength(previous, value);
    const suffixBytes = TEXT_ENCODER.encode(value.slice(prefixLength));
    writeUVarint(entryWriter, prefixLength);
    writeUVarint(entryWriter, suffixBytes.byteLength);
    entryWriter.writeBytes(suffixBytes);
    previous = value;
  }
  const header = new BinaryWriter();
  header.writeU32(sorted.length);
  header.writeU16(Math.max(1, restartInterval));
  header.writeU16(restartOffsets.length);
  for (const offset of restartOffsets) header.writeU32(offset);
  return concatBytes([header.finish(), entryWriter.finish()]);
}

function sharedPrefixLength(left: string, right: string): number {
  const max = Math.min(left.length, right.length);
  let index = 0;
  while (index < max && left.charCodeAt(index) === right.charCodeAt(index)) index += 1;
  return index;
}
