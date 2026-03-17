import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentDiskCache } from "../src/segment/cache";

describe("segment disk cache", () => {
  test("persists across restart", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const cache = new SegmentDiskCache(root, 1024 * 1024);
      const key = "streams/a/segments/0000000000000000.bin";
      const payload = new Uint8Array([1, 2, 3, 4]);
      expect(cache.put(key, payload)).toBe(true);

      const cache2 = new SegmentDiskCache(root, 1024 * 1024);
      const got = cache2.get(key);
      expect(got).not.toBeNull();
      expect(got && Array.from(got)).toEqual(Array.from(payload));
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("lru eviction keeps most recently used", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const key1 = "streams/a/segments/0000000000000000.bin";
      const key2 = "streams/b/segments/0000000000000000.bin";
      const key3 = "streams/c/segments/0000000000000000.bin";
      const payload = new Uint8Array([1, 2, 3, 4]);
      const size = payload.byteLength;
      const cache = new SegmentDiskCache(root, size * 2);

      expect(cache.put(key1, payload)).toBe(true);
      expect(cache.put(key2, payload)).toBe(true);
      expect(cache.get(key1)).not.toBeNull();
      expect(cache.put(key3, payload)).toBe(true);

      expect(cache.has(key2)).toBe(false);
      expect(cache.has(key1)).toBe(true);
      expect(cache.has(key3)).toBe(true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("putFromLocal moves file into cache", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const cache = new SegmentDiskCache(root, 1024 * 1024);
      const key = "streams/a/segments/0000000000000000.bin";
      const local = join(root, "local.bin");
      writeFileSync(local, new Uint8Array([9, 9, 9]));
      expect(existsSync(local)).toBe(true);

      expect(cache.putFromLocal(key, local, 3)).toBe(true);
      expect(existsSync(local)).toBe(false);
      expect(cache.has(key)).toBe(true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("stats track hits/misses/evictions", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const cache = new SegmentDiskCache(root, 4);
      const key1 = "streams/a/segments/0000000000000000.bin";
      const key2 = "streams/b/segments/0000000000000000.bin";
      const payload = new Uint8Array([1, 2, 3, 4]);
      cache.put(key1, payload);
      cache.get(key1); // hit
      cache.get("missing"); // miss
      cache.put(key2, payload); // eviction

      const stats = cache.stats();
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(1);
      expect(stats.evictions).toBeGreaterThanOrEqual(1);
      expect(stats.entryCount).toBe(1);
      expect(stats.maxBytes).toBe(4);
      expect(stats.usedBytes).toBeGreaterThan(0);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
