import { afterEach, describe, expect, test } from "bun:test";
import { loadConfig } from "../src/config";
import { deriveMemoryGuardLimitBytes } from "../src/memory";

const KEYS = [
  "DS_MEMORY_LIMIT_MB",
  "DS_SQLITE_CACHE_MB",
  "DS_SQLITE_CACHE_BYTES",
  "DS_WORKER_SQLITE_CACHE_MB",
  "DS_WORKER_SQLITE_CACHE_BYTES",
  "DS_HEAP_SNAPSHOT_PATH",
] as const;

const originalEnv = new Map<string, string | undefined>(KEYS.map((key) => [key, process.env[key]]));

afterEach(() => {
  for (const key of KEYS) {
    const value = originalEnv.get(key);
    if (value == null) delete process.env[key];
    else process.env[key] = value;
  }
});

describe("config memory tuning", () => {
  test("derives a smaller worker sqlite cache than the main process cache", () => {
    process.env.DS_MEMORY_LIMIT_MB = "4096";
    process.env.DS_SQLITE_CACHE_MB = "256";
    delete process.env.DS_WORKER_SQLITE_CACHE_MB;
    delete process.env.DS_WORKER_SQLITE_CACHE_BYTES;

    const cfg = loadConfig();
    expect(cfg.sqliteCacheBytes).toBe(256 * 1024 * 1024);
    expect(cfg.workerSqliteCacheBytes).toBe(32 * 1024 * 1024);
  });

  test("accepts an explicit worker sqlite cache override", () => {
    process.env.DS_SQLITE_CACHE_MB = "256";
    process.env.DS_WORKER_SQLITE_CACHE_MB = "12";

    const cfg = loadConfig();
    expect(cfg.workerSqliteCacheBytes).toBe(12 * 1024 * 1024);
  });

  test("disables heap snapshots by default and enables them only when configured", () => {
    delete process.env.DS_HEAP_SNAPSHOT_PATH;
    let cfg = loadConfig();
    expect(cfg.heapSnapshotPath).toBeNull();

    process.env.DS_HEAP_SNAPSHOT_PATH = "/tmp/streams.heapsnapshot";
    cfg = loadConfig();
    expect(cfg.heapSnapshotPath).toBe("/tmp/streams.heapsnapshot");
  });

  test("clamps the memory guard to a safe fraction of host memory", () => {
    const hostTotalBytes = 4 * 1024 * 1024 * 1024;
    expect(deriveMemoryGuardLimitBytes(0, hostTotalBytes)).toBe(0);
    expect(deriveMemoryGuardLimitBytes(2 * 1024 * 1024 * 1024, hostTotalBytes)).toBe(2 * 1024 * 1024 * 1024);
    expect(deriveMemoryGuardLimitBytes(5 * 1024 * 1024 * 1024, hostTotalBytes)).toBe(
      Math.floor(hostTotalBytes * 0.7)
    );
  });
});
