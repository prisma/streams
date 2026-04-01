import { Result } from "better-result";
import { BinaryCursor, BinaryWriter, concatBytes } from "./codec";
import { readUVarint, writeUVarint } from "./varint";

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

export class RestartStringTableView {
  private termsCache: string[] | null = null;

  constructor(private readonly bytes: Uint8Array) {}

  terms(): string[] {
    if (this.termsCache) return this.termsCache;
    try {
      const cursor = new BinaryCursor(this.bytes);
      const termCount = cursor.readU32();
      cursor.readU16();
      const restartCount = cursor.readU16();
      for (let i = 0; i < restartCount; i++) cursor.readU32();
      const terms: string[] = [];
      let previous = "";
      for (let index = 0; index < termCount; index++) {
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
    const terms = this.terms();
    let low = 0;
    let high = terms.length - 1;
    while (low <= high) {
      const mid = (low + high) >> 1;
      const cmp = terms[mid]!.localeCompare(term);
      if (cmp === 0) return mid;
      if (cmp < 0) low = mid + 1;
      else high = mid - 1;
    }
    return null;
  }

  expandPrefixResult(prefix: string, limit: number): Result<number[], { message: string }> {
    const terms = this.terms();
    const start = lowerBound(terms, prefix);
    const matches: number[] = [];
    for (let index = start; index < terms.length; index++) {
      const term = terms[index]!;
      if (!term.startsWith(prefix)) break;
      matches.push(index);
      if (matches.length > limit) {
        return Result.err({ message: `prefix expansion exceeds limit (${limit})` });
      }
    }
    return Result.ok(matches);
  }
}

function lowerBound(values: string[], target: string): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (values[mid]!.localeCompare(target) < 0) low = mid + 1;
    else high = mid;
  }
  return low;
}

export function encodeRestartStringTable(values: string[], restartInterval = 16): Uint8Array {
  const sorted = [...values];
  const entryWriter = new BinaryWriter();
  const restartOffsets: number[] = [];
  let previous = "";
  for (let index = 0; index < sorted.length; index++) {
    if (restartInterval > 0 && index % restartInterval === 0) {
      restartOffsets.push(entryWriter.length);
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
  header.writeU16(restartInterval);
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
