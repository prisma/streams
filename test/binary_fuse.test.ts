import { describe, expect, test } from "bun:test";
import { binaryFuseContains, buildBinaryFuse, decodeBinaryFuse } from "../src/index/binary_fuse";

describe("binary fuse filter", () => {
  test("roundtrip encode/decode contains keys", () => {
    const keys = [1n, 2n, 3n, 4n, 10n, 42n];
    const { filter, bytes } = buildBinaryFuse(keys);
    expect(filter).not.toBeNull();
    expect(bytes.byteLength).toBeGreaterThan(28);
    const decoded = decodeBinaryFuse(bytes);
    expect(decoded).not.toBeNull();
    const f = decoded!;
    for (const k of keys) {
      expect(binaryFuseContains(f, k)).toBe(true);
    }
  });
});
