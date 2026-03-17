import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { Result } from "better-result";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ingestFlushIntervalMs: 100_000,
    segmentCheckIntervalMs: 100_000,
    uploadIntervalMs: 100_000,
    indexCheckIntervalMs: 100_000,
    interpreterCheckIntervalMs: 100_000,
    interpreterWorkers: 0,
    segmenterWorkers: 0,
    ...overrides,
  };
}

describe("ingest queue drain", () => {
  test("flush does not depend on Array.shift for dequeueing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-ingest-drain-"));
    try {
      const app = createApp(makeConfig(root), new MockR2Store());
      app.deps.db.ensureStream("queue_stream", { contentType: "application/octet-stream" });

      const appendPromise = app.deps.ingest.append({
        stream: "queue_stream",
        baseAppendMs: 1n,
        rows: [{ payload: new Uint8Array([1]), routingKey: null, contentType: null }],
        contentType: "application/octet-stream",
      });

      const ingestAny = app.deps.ingest as any;
      const originalShift = ingestAny.q.shift;
      ingestAny.q.shift = () => {
        throw new Error("queue shift should not be called");
      };

      try {
        await app.deps.ingest.flush();
      } finally {
        ingestAny.q.shift = originalShift;
      }

      const res = await appendPromise;
      expect(Result.isOk(res)).toBe(true);
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
