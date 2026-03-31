import { describe, test, expect } from "bun:test";
import {
  encodeBlock,
  decodeBlock,
  encodeFooter,
  parseFooter,
  parseFooterBytes,
  iterateBlockRecords,
  DSB3_HEADER_BYTES,
} from "../src/segment/format";

describe("segment format", () => {
  test("encode/decode block roundtrip", () => {
    const records = [
      { appendNs: 1n, routingKey: new TextEncoder().encode("a"), payload: new Uint8Array([1, 2]) },
      { appendNs: 2n, routingKey: new TextEncoder().encode("b"), payload: new Uint8Array([3]) },
    ];
    const block = encodeBlock(records);
    expect(block.byteLength).toBeGreaterThan(DSB3_HEADER_BYTES);
    const decoded = decodeBlock(block);
    expect(decoded.recordCount).toBe(2);
    expect(decoded.records[0].payload).toEqual(records[0].payload);
    expect(decoded.records[1].payload).toEqual(records[1].payload);
  });

  test("footer roundtrip", () => {
    const entries = [
      {
        blockOffset: 0,
        firstOffset: 0n,
        recordCount: 2,
        compressedLen: 123,
        firstAppendNs: 1n,
        lastAppendNs: 2n,
      },
    ];
    const footer = encodeFooter(entries);
    const segmentBytes = new Uint8Array(10 + footer.byteLength);
    segmentBytes.set(footer, 10);
    const parsed = parseFooter(segmentBytes);
    expect(parsed?.footer?.blocks.length).toBe(1);
    const parsedFooter = parseFooterBytes(footer.slice(0, footer.byteLength - 8));
    expect(parsedFooter?.blocks.length).toBe(1);
  });

  test("decodeBlock rejects corrupted payloads", () => {
    const records = [{ appendNs: 1n, routingKey: new TextEncoder().encode("k"), payload: new Uint8Array([9, 9, 9]) }];
    const block = encodeBlock(records);
    const corrupted = block.slice();
    corrupted[corrupted.length - 1] ^= 0xff;
    expect(() => decodeBlock(corrupted)).toThrow();
  });

  test("iterateBlockRecords streams records across multiple blocks", () => {
    const recordsA = [
      { appendNs: 1n, routingKey: new TextEncoder().encode("a"), payload: new Uint8Array([1, 2]) },
      { appendNs: 2n, routingKey: new TextEncoder().encode("b"), payload: new Uint8Array([3]) },
    ];
    const recordsB = [{ appendNs: 3n, routingKey: new TextEncoder().encode("c"), payload: new Uint8Array([4, 5, 6]) }];
    const blockA = encodeBlock(recordsA);
    const blockB = encodeBlock(recordsB);
    const footer = encodeFooter([
      {
        blockOffset: 0,
        firstOffset: 0n,
        recordCount: recordsA.length,
        compressedLen: blockA.byteLength - DSB3_HEADER_BYTES,
        firstAppendNs: recordsA[0]!.appendNs,
        lastAppendNs: recordsA[recordsA.length - 1]!.appendNs,
      },
      {
        blockOffset: blockA.byteLength,
        firstOffset: 2n,
        recordCount: recordsB.length,
        compressedLen: blockB.byteLength - DSB3_HEADER_BYTES,
        firstAppendNs: recordsB[0]!.appendNs,
        lastAppendNs: recordsB[recordsB.length - 1]!.appendNs,
      },
    ]);
    const segment = new Uint8Array(blockA.byteLength + blockB.byteLength + footer.byteLength);
    segment.set(blockA, 0);
    segment.set(blockB, blockA.byteLength);
    segment.set(footer, blockA.byteLength + blockB.byteLength);

    const decoded = Array.from(iterateBlockRecords(segment));
    expect(decoded).toHaveLength(3);
    expect(Array.from(decoded[0]!.routingKey)).toEqual(Array.from(recordsA[0]!.routingKey));
    expect(Array.from(decoded[1]!.payload)).toEqual(Array.from(recordsA[1]!.payload));
    expect(Array.from(decoded[2]!.payload)).toEqual(Array.from(recordsB[0]!.payload));
  });

  test("parseFooter returns null on truncated footer", () => {
    const entries = [
      {
        blockOffset: 0,
        firstOffset: 0n,
        recordCount: 1,
        compressedLen: 12,
        firstAppendNs: 1n,
        lastAppendNs: 1n,
      },
    ];
    const footer = encodeFooter(entries);
    const segmentBytes = new Uint8Array(4 + footer.byteLength);
    segmentBytes.set(footer, 4);
    const truncated = segmentBytes.slice(0, segmentBytes.byteLength - 2);
    const parsed = parseFooter(truncated);
    expect(parsed).toBeNull();
  });
});
