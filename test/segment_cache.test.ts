import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentDiskCache } from "../src/segment/cache";
import { loadSegmentSource } from "../src/segment/cached_segment";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { segmentObjectKey, streamHash16Hex } from "../src/util/stream_paths";

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

  test("required indexing entries reject overflow instead of exceeding budget", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const cache = new SegmentDiskCache(root, 4);
      const key1 = "streams/a/segments/0000000000000000.bin";
      const key2 = "streams/b/segments/0000000000000000.bin";
      const payload = new Uint8Array([1, 2, 3, 4]);
      cache.put(key1, payload);
      expect(cache.markRequiredForIndexing(key1)).toBe(true);
      cache.get(key1); // hit
      cache.get("missing"); // miss
      expect(cache.put(key2, payload)).toBe(false);

      const stats = cache.stats();
      expect(stats.hits).toBe(1);
      expect(stats.misses).toBe(1);
      expect(stats.evictions).toBe(0);
      expect(stats.entryCount).toBe(1);
      expect(stats.maxBytes).toBe(4);
      expect(stats.usedBytes).toBeLessThanOrEqual(stats.maxBytes);
      expect(stats.mappedEntryCount).toBe(1);
      expect(stats.pinnedEntryCount).toBe(1);
      expect(stats.requiredEntryCount).toBe(1);
      expect(stats.requiredBytes).toBe(4);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("mapped files stay readable after on-disk eviction pressure", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const cache = new SegmentDiskCache(root, 4, 4);
      const key1 = "streams/a/segments/0000000000000000.bin";
      const key2 = "streams/b/segments/0000000000000000.bin";
      const payload1 = new Uint8Array([1, 2, 3, 4]);
      const payload2 = new Uint8Array([5, 6, 7, 8]);
      expect(cache.put(key1, payload1)).toBe(true);
      const mapped = cache.getMapped(key1);
      expect(mapped).not.toBeNull();
      expect(mapped && Array.from(mapped.bytes)).toEqual(Array.from(payload1));

      expect(cache.put(key2, payload2)).toBe(true);
      expect(cache.has(key1)).toBe(false);
      expect(cache.has(key2)).toBe(true);
      expect(Array.from(cache.get(key1) ?? new Uint8Array())).toEqual(Array.from(payload1));

      const stats = cache.stats();
      expect(stats.mappedEntryCount).toBe(1);
      expect(stats.pinnedEntryCount).toBe(1);
      expect(stats.usedBytes).toBeLessThanOrEqual(stats.maxBytes);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("segment sources are mmap-backed or in-memory bytes, never path fallbacks", async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-cache-"));
    try {
      const cache = new SegmentDiskCache(root, 1024 * 1024, 4);
      const key = segmentObjectKey(streamHash16Hex("a"), 0);
      const payload = new Uint8Array([1, 2, 3, 4]);
      expect(cache.put(key, payload)).toBe(true);

      const cachedSeg = {
        stream: "a",
        segment_index: 0,
        local_path: "",
      } as any;
      const cachedSource = await loadSegmentSource(new MockR2Store(), cachedSeg, cache);
      expect(cachedSource.kind === "mapped" || cachedSource.kind === "bytes").toBe(true);

      const localPath = join(root, "local-seg.bin");
      writeFileSync(localPath, payload);
      const localSeg = {
        stream: "a",
        segment_index: 1,
        local_path: localPath,
      } as any;
      const localSource = await loadSegmentSource(new MockR2Store(), localSeg, cache);
      expect(localSource.kind === "mapped" || localSource.kind === "bytes").toBe(true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
