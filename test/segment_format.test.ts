import { describe, test, expect } from "bun:test";
import { encodeBlock, decodeBlock, encodeFooter, parseFooter, parseFooterBytes, DSB3_HEADER_BYTES } from "../src/segment/format";

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
