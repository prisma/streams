import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CompanionFileCache } from "../src/search/companion_file_cache";

const EMPTY_TOC = {
  version: 2 as const,
  plan_generation: 1,
  segment_index: 0,
  stream_hash16: "0".repeat(32),
  sections: [],
};

describe("CompanionFileCache", () => {
  test("prunes oldest companion files on startup to fit the configured budget", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-file-cache-"));
    try {
      const firstKey = "streams/a/companions/first.cix";
      const secondKey = "streams/a/companions/second.cix";
      const thirdKey = "streams/a/companions/third.cix";
      const firstPath = join(root, firstKey);
      const secondPath = join(root, secondKey);
      const thirdPath = join(root, thirdKey);
      mkdirSync(join(root, "streams/a/companions"), { recursive: true });
      writeFileSync(firstPath, new Uint8Array([1, 2, 3, 4]));
      writeFileSync(secondPath, new Uint8Array([5, 6, 7, 8]));
      writeFileSync(thirdPath, new Uint8Array([9, 10, 11, 12]));
      utimesSync(firstPath, new Date(1_000), new Date(1_000));
      utimesSync(secondPath, new Date(2_000), new Date(2_000));
      utimesSync(thirdPath, new Date(3_000), new Date(3_000));

      new CompanionFileCache(root, 8, 0, 4);

      expect(existsSync(firstPath)).toBeFalse();
      expect(existsSync(secondPath)).toBeTrue();
      expect(existsSync(thirdPath)).toBeTrue();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("prunes stale companion files on startup before reuse", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-file-cache-"));
    try {
      const staleKey = "streams/a/companions/stale.cix";
      const freshKey = "streams/a/companions/fresh.cix";
      const stalePath = join(root, staleKey);
      const freshPath = join(root, freshKey);
      mkdirSync(join(root, "streams/a/companions"), { recursive: true });
      writeFileSync(stalePath, new Uint8Array([1, 2, 3, 4]));
      writeFileSync(freshPath, new Uint8Array([5, 6, 7, 8]));
      const now = Date.now();
      utimesSync(stalePath, new Date(now - 10_000), new Date(now - 10_000));
      utimesSync(freshPath, new Date(now - 1_000), new Date(now - 1_000));

      new CompanionFileCache(root, 1024, 5_000, 4);

      expect(existsSync(stalePath)).toBeFalse();
      expect(existsSync(freshPath)).toBeTrue();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("downloads a companion bundle once and reuses the local mmap-backed cache", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-companion-file-cache-"));
    try {
      const cache = new CompanionFileCache(root, 1024, 60_000, 4);
      let loadCount = 0;
      const loadBytes = async () => {
        loadCount += 1;
        return new Uint8Array([1, 2, 3, 4]);
      };

      const firstRes = await cache.loadMappedBundleResult({
        objectKey: "streams/a/companions/one.cix",
        expectedSize: 4,
        loadBytes,
        decodeToc: () => Result.ok(EMPTY_TOC),
      });
      expect(Result.isError(firstRes)).toBeFalse();
      if (Result.isError(firstRes)) return;
      expect(loadCount).toBe(1);
      expect(firstRes.value.bytes.byteLength).toBe(4);

      const secondRes = await cache.loadMappedBundleResult({
        objectKey: "streams/a/companions/one.cix",
        expectedSize: 4,
        loadBytes,
        decodeToc: () => Result.ok(EMPTY_TOC),
      });
      expect(Result.isError(secondRes)).toBeFalse();
      if (Result.isError(secondRes)) return;
      expect(loadCount).toBe(1);
      expect(secondRes.value.path).toBe(firstRes.value.path);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
