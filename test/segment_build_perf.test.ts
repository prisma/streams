import { describe, expect, test } from "bun:test";
import { performance } from "node:perf_hooks";
import { zstdCompressSync } from "node:zlib";
import { crc32c } from "../src/util/crc32c";
import {
  DSB3_HEADER_BYTES,
  encodeBlock,
  encodeFooter,
  encodeRecord,
  type BlockIndexEntry,
  type SegmentRecord,
} from "../src/segment/format";
import { concatBytes, writeU32BE, writeU64BE } from "../src/util/endian";
const CURRENT_BLOCK_MAX_BYTES = 1_048_576;
const TOTAL_ROWS = 70_500;
const ITERATIONS = 3;
const BLOOM_BITS = 256n;
const BLOOM_MASK64 = (1n << 64n) - 1n;

function mulberry32Step(state: number): [number, number] {
  const next = (state + 0x6d2b79f5) >>> 0;
  let t = Math.imul(next ^ (next >>> 15), next | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return [((t ^ (t >>> 14)) >>> 0) >>> 0, next];
}

function randomBase36String(seed: number, len: number): string {
  let out = "";
  let state = seed >>> 0;
  while (out.length < len) {
    const [value, nextState] = mulberry32Step(state);
    state = nextState;
    out += value.toString(36).padStart(7, "0");
  }
  return out.slice(0, len);
}

function buildRecords(totalRows: number): SegmentRecord[] {
  const encoder = new TextEncoder();
  const cardinality100 = Array.from({ length: 100 }, (_, index) =>
    randomBase36String(10_000 + index, 50)
  );
  const cardinality1000 = Array.from({ length: 1_000 }, (_, index) =>
    randomBase36String(20_000 + index, 50)
  );
  const records: SegmentRecord[] = [];
  for (let id = 1; id <= totalRows; id += 1) {
    const payload = encoder.encode(
      JSON.stringify({
        randomString: randomBase36String(30_000 + id, 100),
        randomNumber: (id * 2654435761) >>> 0,
        id,
        cardinality100: cardinality100[id % cardinality100.length],
        cardinality1000: cardinality1000[id % cardinality1000.length],
        time: `2026-04-07T${String(id % 24).padStart(2, "0")}:00:00.000Z`,
      })
    );
    records.push({
      appendNs: BigInt(id) * 1_000_000n,
      routingKey: encoder.encode(cardinality100[id % cardinality100.length]!),
      payload,
    });
  }
  return records;
}

function fnv1a64(data: Uint8Array): bigint {
  let h = 14695981039346656037n;
  for (const b of data) {
    h ^= BigInt(b);
    h = (h * 1099511628211n) & BLOOM_MASK64;
  }
  return h;
}

function mix64(x: bigint): bigint {
  x = (x + 0x9e3779b97f4a7c15n) & BLOOM_MASK64;
  x = (x ^ (x >> 30n)) * 0xbf58476d1ce4e5b9n & BLOOM_MASK64;
  x = (x ^ (x >> 27n)) * 0x94d049bb133111ebn & BLOOM_MASK64;
  x = x ^ (x >> 31n);
  return x & BLOOM_MASK64;
}

function encodeBlockLegacyBigIntBloom(records: SegmentRecord[]): Uint8Array {
  if (records.length === 0) throw new Error("empty block");
  const bloom = new Uint8Array(32);
  for (const record of records) {
    if (record.routingKey.byteLength === 0) continue;
    const h1 = fnv1a64(record.routingKey);
    const h2 = mix64(h1 ^ 0xa0761d6478bd642fn);
    for (let i = 0; i < 3; i++) {
      const idx = Number((h1 + BigInt(i) * h2) % BLOOM_BITS);
      bloom[idx >> 3] |= 1 << (idx & 7);
    }
  }
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
  header.set(bloom, 16);
  writeU64BE(header, 48, records[0]!.appendNs);
  writeU64BE(header, 56, records[records.length - 1]!.appendNs);
  writeU32BE(header, 64, crc);
  return concatBytes([header, compressed]);
}

async function buildSegmentBytes(
  records: SegmentRecord[],
  encodeBlockFn: (records: SegmentRecord[]) => Uint8Array
): Promise<Uint8Array> {
  const blocks: Uint8Array[] = [];
  const footerEntries: BlockIndexEntry[] = [];
  let blockRecords: SegmentRecord[] = [];
  let blockBytesApprox = 0;
  let fileBytes = 0;
  let blockFirstOffset = 0n;

  const flush = async () => {
    const block = encodeBlockFn(blockRecords);
    blocks.push(block);
    footerEntries.push({
      blockOffset: fileBytes,
      firstOffset: blockFirstOffset,
      recordCount: blockRecords.length,
      compressedLen: block.byteLength - DSB3_HEADER_BYTES,
      firstAppendNs: blockRecords[0]!.appendNs,
      lastAppendNs: blockRecords[blockRecords.length - 1]!.appendNs,
    });
    fileBytes += block.byteLength;
    blockRecords = [];
    blockBytesApprox = 0;
  };

  for (let index = 0; index < records.length; index += 1) {
    const record = records[index]!;
    const recSize = 8 + 4 + record.routingKey.byteLength + 4 + record.payload.byteLength;
    if (blockRecords.length > 0 && blockBytesApprox + recSize > CURRENT_BLOCK_MAX_BYTES) {
      await flush();
    }
    if (blockRecords.length === 0) blockFirstOffset = BigInt(index);
    blockRecords.push(record);
    blockBytesApprox += recSize;
  }

  if (blockRecords.length > 0) await flush();

  const footer = encodeFooter(footerEntries);
  const out = new Uint8Array(fileBytes + footer.byteLength);
  let cursor = 0;
  for (const block of blocks) {
    out.set(block, cursor);
    cursor += block.byteLength;
  }
  out.set(footer, cursor);
  return out;
}

async function measureAverageMs(
  records: SegmentRecord[],
  encodeBlockFn: (records: SegmentRecord[]) => Uint8Array
): Promise<{ avgMs: number; bytes: Uint8Array }> {
  let totalMs = 0;
  let last = new Uint8Array(0);
  for (let iteration = 0; iteration < ITERATIONS; iteration += 1) {
    const started = performance.now();
    last = await buildSegmentBytes(records, encodeBlockFn);
    totalMs += performance.now() - started;
  }
  return { avgMs: totalMs / ITERATIONS, bytes: last };
}

describe("segment build performance", () => {
  test("builds representative current-geometry segments at least 25% faster than the legacy BigInt bloom path", async () => {
    const records = buildRecords(TOTAL_ROWS);

    const legacyWarm = await buildSegmentBytes(records, encodeBlockLegacyBigIntBloom);
    const currentWarm = await buildSegmentBytes(records, encodeBlock);
    expect(currentWarm.byteLength).toBeLessThanOrEqual(legacyWarm.byteLength);

    const legacy = await measureAverageMs(records, encodeBlockLegacyBigIntBloom);
    const current = await measureAverageMs(records, encodeBlock);

    expect(current.bytes.byteLength).toBeLessThanOrEqual(legacy.bytes.byteLength);
    // eslint-disable-next-line no-console
    console.log(
      `[segment-build-perf] legacyAvgMs=${legacy.avgMs.toFixed(1)} currentAvgMs=${current.avgMs.toFixed(1)} rows=${TOTAL_ROWS}`
    );
    expect(current.avgMs).toBeLessThan(legacy.avgMs * 0.75);
  }, 30_000);
});
