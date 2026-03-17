import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
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
    ingestBusyTimeoutMs: 1000,
    segmentCheckIntervalMs: 100_000,
    uploadIntervalMs: 100_000,
    indexCheckIntervalMs: 100_000,
    segmenterWorkers: 0,
    ...overrides,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("ingest busy retry", () => {
  test("flush retries SQLITE_BUSY and succeeds once the lock is released", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-busy-"));
    try {
      const cfg = makeConfig(root);
      const app = createApp(cfg, new MockR2Store());
      app.deps.db.ensureStream("busy_stream", { contentType: "application/octet-stream" });

      // Force immediate SQLITE_BUSY so the retry loop is exercised.
      app.deps.db.db.exec("PRAGMA busy_timeout = 0;");

      const lockDb = new Database(cfg.dbPath);
      lockDb.exec("PRAGMA busy_timeout = 0;");
      lockDb.exec("BEGIN IMMEDIATE;");

      const release = (async () => {
        await sleep(150);
        lockDb.exec("COMMIT;");
        lockDb.close();
      })();

      const appendPromise = app.deps.ingest.append({
        stream: "busy_stream",
        baseAppendMs: 1n,
        rows: [{ payload: new Uint8Array([1, 2, 3]), routingKey: null, contentType: null }],
        contentType: "application/octet-stream",
      });

      await app.deps.ingest.flush();
      await release;
      const res = await appendPromise;
      expect(Result.isOk(res)).toBe(true);

      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
