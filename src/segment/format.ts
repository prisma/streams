import { Result } from "better-result";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { Bloom256 } from "../util/bloom256";
import { crc32c } from "../util/crc32c";
import { concatBytes, readU32BE, readU64BE, writeU32BE, writeU64BE } from "../util/endian";
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

function invalidSegment<T = never>(message: string): Result<T, SegmentFormatError> {
  return Result.err({ kind: "invalid_segment_format", message });
}

export const DSB3_HEADER_BYTES = 68;

const FOOTER_MAGIC = "DSF1";
const FOOTER_VERSION = 1;
const FOOTER_ENTRY_BYTES = 40; // 8+8+4+4+8+8
const FOOTER_TRAILER_BYTES = 8; // u32 len + 4-byte magic

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
  for (const r of records) bloom.add(r.routingKey);

  const recBytes = records.map(encodeRecord);
  const uncompressed = concatBytes(recBytes);
  const compressed = new Uint8Array(zstdCompressSync(uncompressed));
  const crc = crc32c(compressed);

  const header = new Uint8Array(DSB3_HEADER_BYTES);
  header[0] = "D".charCodeAt(0);
  header[1] = "S".charCodeAt(0);
  header[2] = "B".charCodeAt(0);
  header[3] = "3".charCodeAt(0);
  writeU32BE(header, 4, uncompressed.byteLength);
  writeU32BE(header, 8, compressed.byteLength);
  writeU32BE(header, 12, records.length);
  header.set(bloom.toBytes(), 16);

  const firstTs = records[0].appendNs;
  const lastTs = records[records.length - 1].appendNs;
  writeU64BE(header, 48, firstTs);
  writeU64BE(header, 56, lastTs);
  writeU32BE(header, 64, crc);

  return Result.ok(concatBytes([header, compressed]));
}

export function decodeBlock(blockBytes: Uint8Array): DecodedBlock {
  const res = decodeBlockResult(blockBytes);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function decodeBlockResult(blockBytes: Uint8Array): Result<DecodedBlock, SegmentFormatError> {
  if (blockBytes.byteLength < DSB3_HEADER_BYTES) return invalidSegment("block too small");
  if (
    blockBytes[0] !== "D".charCodeAt(0) ||
    blockBytes[1] !== "S".charCodeAt(0) ||
    blockBytes[2] !== "B".charCodeAt(0) ||
    blockBytes[3] !== "3".charCodeAt(0)
  ) {
    return invalidSegment("bad block magic");
  }

  const uncompressedLen = readU32BE(blockBytes, 4);
  const compressedLen = readU32BE(blockBytes, 8);
  const recordCount = readU32BE(blockBytes, 12);
  const bloom = blockBytes.slice(16, 48);
  const firstAppendNs = readU64BE(blockBytes, 48);
  const lastAppendNs = readU64BE(blockBytes, 56);
  const expectedCrc = readU32BE(blockBytes, 64);

  const payload = blockBytes.slice(DSB3_HEADER_BYTES, DSB3_HEADER_BYTES + compressedLen);
  if (payload.byteLength !== compressedLen) return invalidSegment("truncated block");
  const actualCrc = crc32c(payload);
  if (actualCrc !== expectedCrc) return invalidSegment("crc mismatch");

  let uncompressed: Uint8Array;
  try {
    uncompressed = new Uint8Array(zstdDecompressSync(payload));
  } catch (e: any) {
    return invalidSegment(String(e?.message ?? e));
  }
  if (uncompressed.byteLength !== uncompressedLen) {
    return invalidSegment(`bad uncompressed len: got=${uncompressed.byteLength} expected=${uncompressedLen}`);
  }

  const records: SegmentRecord[] = [];
  let off = 0;
  for (let i = 0; i < recordCount; i++) {
    if (off + 8 + 4 > uncompressed.byteLength) return invalidSegment("truncated record");
    const appendNs = readU64BE(uncompressed, off);
    off += 8;
    const keyLen = readU32BE(uncompressed, off);
    off += 4;
    if (off + keyLen + 4 > uncompressed.byteLength) return invalidSegment("truncated key");
    const routingKey = uncompressed.slice(off, off + keyLen);
    off += keyLen;
    const dataLen = readU32BE(uncompressed, off);
    off += 4;
    if (off + dataLen > uncompressed.byteLength) return invalidSegment("truncated payload");
    const payload = uncompressed.slice(off, off + dataLen);
    off += dataLen;
    records.push({ appendNs, routingKey, payload });
  }

  return Result.ok({ recordCount, firstAppendNs, lastAppendNs, bloom, records });
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

export function* iterateBlocks(segmentBytes: Uint8Array): Generator<IterateBlockEntry, void, void> {
  for (const itemRes of iterateBlocksResult(segmentBytes)) {
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
