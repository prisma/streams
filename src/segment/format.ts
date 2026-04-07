import { Result } from "better-result";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { Bloom256, bloom256MaskForKey } from "../util/bloom256";
import { crc32c } from "../util/crc32c";
import { readU32BE, readU64BE, writeU32BE, writeU64BE } from "../util/endian";
import { dsError } from "../util/ds_error.ts";

export type SegmentRecord = {
  appendNs: bigint;
  routingKey: Uint8Array; // UTF8 bytes (may be empty)
  payload: Uint8Array;
};

export type DecodedBlock = {
  recordCount: number;
  firstAppendNs: bigint;
  lastAppendNs: bigint;
  bloom: Uint8Array; // 32 bytes
  records: SegmentRecord[];
};

export type BlockIndexEntry = {
  blockOffset: number;
  firstOffset: bigint;
  recordCount: number;
  compressedLen: number;
  firstAppendNs: bigint;
  lastAppendNs: bigint;
};

export type SegmentFooter = {
  version: number;
  blocks: BlockIndexEntry[];
};

export type ParsedFooter = {
  footer: SegmentFooter | null;
  footerStart: number;
};

export type BlockHeader = {
  uncompressedLen: number;
  compressedLen: number;
  recordCount: number;
  bloom: Uint8Array;
  firstAppendNs: bigint;
  lastAppendNs: bigint;
  crc32c: number;
};

export type SegmentFormatError = {
  kind: "invalid_segment_format";
  message: string;
};

export type IterateBlockEntry = {
  blockOffset: number;
  blockBytes: Uint8Array;
  decoded: DecodedBlock;
};

export type IterateBlockRecordEntry = {
  blockOffset: number;
  recordIndex: number;
  appendNs: bigint;
  routingKey: Uint8Array;
  payload: Uint8Array;
};

function invalidSegment<T = never>(message: string): Result<T, SegmentFormatError> {
  return Result.err({ kind: "invalid_segment_format", message });
}

export const DSB3_HEADER_BYTES = 68;

const FOOTER_MAGIC = "DSF1";
const FOOTER_VERSION = 1;
const FOOTER_ENTRY_BYTES = 40; // 8+8+4+4+8+8
const FOOTER_TRAILER_BYTES = 8; // u32 len + 4-byte magic

type BloomMaskCacheEntry = {
  key: Uint8Array;
  mask: Uint8Array;
};

function hashRoutingKey32(keyUtf8: Uint8Array): number {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < keyUtf8.byteLength; i++) {
    h ^= keyUtf8[i]!;
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h >>> 0;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

export function encodeRecord(rec: SegmentRecord): Uint8Array {
  const keyLen = rec.routingKey.byteLength;
  const dataLen = rec.payload.byteLength;
  const out = new Uint8Array(8 + 4 + keyLen + 4 + dataLen);
  writeU64BE(out, 0, rec.appendNs);
  writeU32BE(out, 8, keyLen);
  out.set(rec.routingKey, 12);
  writeU32BE(out, 12 + keyLen, dataLen);
  out.set(rec.payload, 16 + keyLen);
  return out;
}

export function encodeBlock(records: SegmentRecord[]): Uint8Array {
  const res = encodeBlockResult(records);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function encodeBlockResult(records: SegmentRecord[]): Result<Uint8Array, SegmentFormatError> {
  if (records.length === 0) return invalidSegment("empty block");

  const bloom = new Bloom256();
  const bloomMaskCache = new Map<number, BloomMaskCacheEntry[]>();
  let uncompressedLen = 0;
  for (const r of records) {
    if (r.routingKey.byteLength > 0) {
      const hash = hashRoutingKey32(r.routingKey);
      let bucket = bloomMaskCache.get(hash);
      let mask: Uint8Array | null = null;
      if (bucket) {
        for (const entry of bucket) {
          if (sameBytes(entry.key, r.routingKey)) {
            mask = entry.mask;
            break;
          }
        }
      } else {
        bucket = [];
        bloomMaskCache.set(hash, bucket);
      }
      if (!mask) {
        mask = bloom256MaskForKey(r.routingKey);
        bucket.push({ key: r.routingKey, mask });
      }
      bloom.orMask(mask);
    }
    uncompressedLen += 8 + 4 + r.routingKey.byteLength + 4 + r.payload.byteLength;
  }

  const uncompressed = new Uint8Array(uncompressedLen);
  const uncompressedView = new DataView(uncompressed.buffer, uncompressed.byteOffset, uncompressed.byteLength);
  let off = 0;
  for (const r of records) {
    uncompressedView.setBigUint64(off, r.appendNs, false);
    off += 8;
    uncompressedView.setUint32(off, r.routingKey.byteLength >>> 0, false);
    off += 4;
    uncompressed.set(r.routingKey, off);
    off += r.routingKey.byteLength;
    uncompressedView.setUint32(off, r.payload.byteLength >>> 0, false);
    off += 4;
    uncompressed.set(r.payload, off);
    off += r.payload.byteLength;
  }
  const compressed = new Uint8Array(zstdCompressSync(uncompressed));
  const crc = crc32c(compressed);

  const firstTs = records[0].appendNs;
  const lastTs = records[records.length - 1].appendNs;
  const out = new Uint8Array(DSB3_HEADER_BYTES + compressed.byteLength);
  out[0] = "D".charCodeAt(0);
  out[1] = "S".charCodeAt(0);
  out[2] = "B".charCodeAt(0);
  out[3] = "3".charCodeAt(0);
  const outView = new DataView(out.buffer, out.byteOffset, out.byteLength);
  outView.setUint32(4, uncompressed.byteLength >>> 0, false);
  outView.setUint32(8, compressed.byteLength >>> 0, false);
  outView.setUint32(12, records.length >>> 0, false);
  out.set(bloom.toBytes(), 16);
  outView.setBigUint64(48, firstTs, false);
  outView.setBigUint64(56, lastTs, false);
  outView.setUint32(64, crc >>> 0, false);
  out.set(compressed, DSB3_HEADER_BYTES);

  return Result.ok(out);
}

export function decodeBlock(blockBytes: Uint8Array): DecodedBlock {
  const res = decodeBlockResult(blockBytes);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function decodeBlockResult(blockBytes: Uint8Array): Result<DecodedBlock, SegmentFormatError> {
  const headerRes = parseBlockHeaderResult(blockBytes);
  if (Result.isError(headerRes)) return headerRes;
  const header = headerRes.value;
  const uncompressedRes = decompressBlockPayloadResult(blockBytes, header);
  if (Result.isError(uncompressedRes)) return uncompressedRes;
  const uncompressed = uncompressedRes.value;

  const records: SegmentRecord[] = [];
  let off = 0;
  for (let i = 0; i < header.recordCount; i++) {
    if (off + 8 + 4 > uncompressed.byteLength) return invalidSegment("truncated record");
    const appendNs = readU64BE(uncompressed, off);
    off += 8;
    const keyLen = readU32BE(uncompressed, off);
    off += 4;
    if (off + keyLen + 4 > uncompressed.byteLength) return invalidSegment("truncated key");
    const routingKey = uncompressed.subarray(off, off + keyLen);
    off += keyLen;
    const dataLen = readU32BE(uncompressed, off);
    off += 4;
    if (off + dataLen > uncompressed.byteLength) return invalidSegment("truncated payload");
    const payload = uncompressed.subarray(off, off + dataLen);
    off += dataLen;
    records.push({ appendNs, routingKey, payload });
  }

  return Result.ok({
    recordCount: header.recordCount,
    firstAppendNs: header.firstAppendNs,
    lastAppendNs: header.lastAppendNs,
    bloom: header.bloom.slice(),
    records,
  });
}

export function encodeFooter(entries: BlockIndexEntry[]): Uint8Array {
  const footerLen = 12 + entries.length * FOOTER_ENTRY_BYTES;
  const footer = new Uint8Array(footerLen + FOOTER_TRAILER_BYTES);
  footer[0] = FOOTER_MAGIC.charCodeAt(0);
  footer[1] = FOOTER_MAGIC.charCodeAt(1);
  footer[2] = FOOTER_MAGIC.charCodeAt(2);
  footer[3] = FOOTER_MAGIC.charCodeAt(3);
  writeU32BE(footer, 4, FOOTER_VERSION);
  writeU32BE(footer, 8, entries.length);

  let off = 12;
  for (const e of entries) {
    writeU64BE(footer, off, BigInt(e.blockOffset));
    off += 8;
    writeU64BE(footer, off, e.firstOffset);
    off += 8;
    writeU32BE(footer, off, e.recordCount);
    off += 4;
    writeU32BE(footer, off, e.compressedLen);
    off += 4;
    writeU64BE(footer, off, e.firstAppendNs);
    off += 8;
    writeU64BE(footer, off, e.lastAppendNs);
    off += 8;
  }

  // Trailer: footer length + magic
  writeU32BE(footer, footerLen, footerLen);
  footer[footerLen + 4] = FOOTER_MAGIC.charCodeAt(0);
  footer[footerLen + 5] = FOOTER_MAGIC.charCodeAt(1);
  footer[footerLen + 6] = FOOTER_MAGIC.charCodeAt(2);
  footer[footerLen + 7] = FOOTER_MAGIC.charCodeAt(3);

  return footer;
}

export function parseFooter(segmentBytes: Uint8Array): ParsedFooter | null {
  if (segmentBytes.byteLength < FOOTER_TRAILER_BYTES) return null;
  const tail = segmentBytes.slice(segmentBytes.byteLength - 4);
  const tailMagic = String.fromCharCode(tail[0], tail[1], tail[2], tail[3]);
  if (tailMagic !== FOOTER_MAGIC) return null;

  const footerLen = readU32BE(segmentBytes, segmentBytes.byteLength - 8);
  if (footerLen <= 0 || footerLen + FOOTER_TRAILER_BYTES > segmentBytes.byteLength) return null;

  const footerStart = segmentBytes.byteLength - FOOTER_TRAILER_BYTES - footerLen;
  if (footerStart < 0) return null;
  const footer = segmentBytes.slice(footerStart, footerStart + footerLen);
  const parsed = parseFooterBytes(footer);
  return { footer: parsed, footerStart };
}

export function* iterateBlocksResult(
  segmentBytes: Uint8Array
): Generator<Result<IterateBlockEntry, SegmentFormatError>, void, void> {
  const parsed = parseFooter(segmentBytes);
  const limit = parsed ? parsed.footerStart : segmentBytes.byteLength;
  let off = 0;
  while (off < limit) {
    if (off + DSB3_HEADER_BYTES > limit) {
      yield invalidSegment("truncated segment (block header)");
      return;
    }
    const header = segmentBytes.slice(off, off + DSB3_HEADER_BYTES);
    const compressedLen = readU32BE(header, 8);
    const totalLen = DSB3_HEADER_BYTES + compressedLen;
    if (off + totalLen > limit) {
      yield invalidSegment("truncated segment (block payload)");
      return;
    }
    const blockBytes = segmentBytes.slice(off, off + totalLen);
    const decodedRes = decodeBlockResult(blockBytes);
    if (Result.isError(decodedRes)) {
      yield decodedRes;
      return;
    }
    yield Result.ok({ blockOffset: off, blockBytes, decoded: decodedRes.value });
    off += totalLen;
  }
}

export function* iterateBlockRecordsResult(
  segmentBytes: Uint8Array
): Generator<Result<IterateBlockRecordEntry, SegmentFormatError>, void, void> {
  const parsed = parseFooter(segmentBytes);
  const limit = parsed ? parsed.footerStart : segmentBytes.byteLength;
  let off = 0;
  while (off < limit) {
    if (off + DSB3_HEADER_BYTES > limit) {
      yield invalidSegment("truncated segment (block header)");
      return;
    }
    const headerRes = parseBlockHeaderResult(segmentBytes.subarray(off, off + DSB3_HEADER_BYTES));
    if (Result.isError(headerRes)) {
      yield headerRes;
      return;
    }
    const header = headerRes.value;
    const totalLen = DSB3_HEADER_BYTES + header.compressedLen;
    if (off + totalLen > limit) {
      yield invalidSegment("truncated segment (block payload)");
      return;
    }
    const blockBytes = segmentBytes.subarray(off, off + totalLen);
    const uncompressedRes = decompressBlockPayloadResult(blockBytes, header);
    if (Result.isError(uncompressedRes)) {
      yield uncompressedRes;
      return;
    }
    const uncompressed = uncompressedRes.value;
    let recOff = 0;
    for (let recordIndex = 0; recordIndex < header.recordCount; recordIndex++) {
      if (recOff + 8 + 4 > uncompressed.byteLength) {
        yield invalidSegment("truncated record");
        return;
      }
      const appendNs = readU64BE(uncompressed, recOff);
      recOff += 8;
      const keyLen = readU32BE(uncompressed, recOff);
      recOff += 4;
      if (recOff + keyLen + 4 > uncompressed.byteLength) {
        yield invalidSegment("truncated key");
        return;
      }
      const routingKey = uncompressed.subarray(recOff, recOff + keyLen);
      recOff += keyLen;
      const dataLen = readU32BE(uncompressed, recOff);
      recOff += 4;
      if (recOff + dataLen > uncompressed.byteLength) {
        yield invalidSegment("truncated payload");
        return;
      }
      const payload = uncompressed.subarray(recOff, recOff + dataLen);
      recOff += dataLen;
      yield Result.ok({ blockOffset: off, recordIndex, appendNs, routingKey, payload });
    }
    off += totalLen;
  }
}

export function forEachRoutingKeyResult(
  segmentBytes: Uint8Array,
  visit: (routingKey: Uint8Array) => void
): Result<void, SegmentFormatError> {
  const parsed = parseFooter(segmentBytes);
  const limit = parsed ? parsed.footerStart : segmentBytes.byteLength;
  let off = 0;
  while (off < limit) {
    if (off + DSB3_HEADER_BYTES > limit) {
      return invalidSegment("truncated segment (block header)");
    }
    const headerRes = parseBlockHeaderResult(segmentBytes.subarray(off, off + DSB3_HEADER_BYTES));
    if (Result.isError(headerRes)) return headerRes;
    const header = headerRes.value;
    const totalLen = DSB3_HEADER_BYTES + header.compressedLen;
    if (off + totalLen > limit) {
      return invalidSegment("truncated segment (block payload)");
    }
    const blockBytes = segmentBytes.subarray(off, off + totalLen);
    const uncompressedRes = decompressBlockPayloadResult(blockBytes, header);
    if (Result.isError(uncompressedRes)) return uncompressedRes;
    const uncompressed = uncompressedRes.value;
    let recOff = 0;
    for (let recordIndex = 0; recordIndex < header.recordCount; recordIndex++) {
      if (recOff + 8 + 4 > uncompressed.byteLength) {
        return invalidSegment("truncated record");
      }
      recOff += 8; // appendNs
      const keyLen = readU32BE(uncompressed, recOff);
      recOff += 4;
      if (recOff + keyLen + 4 > uncompressed.byteLength) {
        return invalidSegment("truncated key");
      }
      const routingKey = uncompressed.subarray(recOff, recOff + keyLen);
      recOff += keyLen;
      const dataLen = readU32BE(uncompressed, recOff);
      recOff += 4;
      if (recOff + dataLen > uncompressed.byteLength) {
        return invalidSegment("truncated payload");
      }
      recOff += dataLen;
      visit(routingKey);
    }
    off += totalLen;
  }
  return Result.ok(undefined);
}

export function* iterateBlocks(segmentBytes: Uint8Array): Generator<IterateBlockEntry, void, void> {
  for (const itemRes of iterateBlocksResult(segmentBytes)) {
    if (Result.isError(itemRes)) throw dsError(itemRes.error.message);
    yield itemRes.value;
  }
}

export function* iterateBlockRecords(segmentBytes: Uint8Array): Generator<IterateBlockRecordEntry, void, void> {
  for (const itemRes of iterateBlockRecordsResult(segmentBytes)) {
    if (Result.isError(itemRes)) throw dsError(itemRes.error.message);
    yield itemRes.value;
  }
}

export function parseFooterBytes(footer: Uint8Array): SegmentFooter | null {
  if (footer.byteLength < 12) return null;
  const magic = String.fromCharCode(footer[0], footer[1], footer[2], footer[3]);
  if (magic !== FOOTER_MAGIC) return null;
  const version = readU32BE(footer, 4);
  const blockCount = readU32BE(footer, 8);
  const expectedLen = 12 + blockCount * FOOTER_ENTRY_BYTES;
  if (footer.byteLength !== expectedLen) return null;
  const blocks: BlockIndexEntry[] = [];
  let off = 12;
  for (let i = 0; i < blockCount; i++) {
    const blockOffset = Number(readU64BE(footer, off));
    off += 8;
    const firstOffset = readU64BE(footer, off);
    off += 8;
    const recordCount = readU32BE(footer, off);
    off += 4;
    const compressedLen = readU32BE(footer, off);
    off += 4;
    const firstAppendNs = readU64BE(footer, off);
    off += 8;
    const lastAppendNs = readU64BE(footer, off);
    off += 8;
    blocks.push({ blockOffset, firstOffset, recordCount, compressedLen, firstAppendNs, lastAppendNs });
  }
  return { version, blocks };
}

export function parseBlockHeader(header: Uint8Array): BlockHeader {
  const res = parseBlockHeaderResult(header);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function parseBlockHeaderResult(header: Uint8Array): Result<BlockHeader, SegmentFormatError> {
  if (header.byteLength < DSB3_HEADER_BYTES) return invalidSegment("block header too small");
  if (
    header[0] !== "D".charCodeAt(0) ||
    header[1] !== "S".charCodeAt(0) ||
    header[2] !== "B".charCodeAt(0) ||
    header[3] !== "3".charCodeAt(0)
  ) {
    return invalidSegment("bad block magic");
  }
  const uncompressedLen = readU32BE(header, 4);
  const compressedLen = readU32BE(header, 8);
  const recordCount = readU32BE(header, 12);
  const bloom = header.slice(16, 48);
  const firstAppendNs = readU64BE(header, 48);
  const lastAppendNs = readU64BE(header, 56);
  const crc32cVal = readU32BE(header, 64);
  return Result.ok({
    uncompressedLen,
    compressedLen,
    recordCount,
    bloom,
    firstAppendNs,
    lastAppendNs,
    crc32c: crc32cVal,
  });
}

function decompressBlockPayloadResult(blockBytes: Uint8Array, header: BlockHeader): Result<Uint8Array, SegmentFormatError> {
  const payload = blockBytes.subarray(DSB3_HEADER_BYTES, DSB3_HEADER_BYTES + header.compressedLen);
  if (payload.byteLength !== header.compressedLen) return invalidSegment("truncated block");
  const actualCrc = crc32c(payload);
  if (actualCrc !== header.crc32c) return invalidSegment("crc mismatch");

  let uncompressed: Uint8Array;
  try {
    uncompressed = new Uint8Array(zstdDecompressSync(payload));
  } catch (e: any) {
    return invalidSegment(String(e?.message ?? e));
  }
  if (uncompressed.byteLength !== header.uncompressedLen) {
    return invalidSegment(`bad uncompressed len: got=${uncompressed.byteLength} expected=${header.uncompressedLen}`);
  }
  return Result.ok(uncompressed);
}
