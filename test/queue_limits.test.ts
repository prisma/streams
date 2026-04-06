import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ingestFlushIntervalMs: 100_000,
    ingestMaxQueueRequests: 2,
    ingestMaxBatchRequests: 100,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("queue limits", () => {
  test("append queue returns overload instead of growing unbounded", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-queue-"));
    try {
      const cfg = makeConfig(root);
      const app = createApp(cfg, new MockR2Store());
      const server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/queue_test`, {
        method: "PUT",
        headers: { "content-type": "application/octet-stream" },
      });

      const payload = new Uint8Array([1, 2, 3]);
      const p1 = fetch(`${baseUrl}/v1/stream/queue_test`, {
        method: "POST",
        headers: { "content-type": "application/octet-stream" },
        body: payload,
      });
      const p2 = fetch(`${baseUrl}/v1/stream/queue_test`, {
        method: "POST",
        headers: { "content-type": "application/octet-stream" },
        body: payload,
      });

      await sleep(20);

      const r3 = await fetch(`${baseUrl}/v1/stream/queue_test`, {
        method: "POST",
        headers: { "content-type": "application/octet-stream" },
        body: payload,
      });
      expect(r3.status).toBe(429);

      await app.deps.ingest.flush();
      const r1 = await p1;
      const r2 = await p2;
      expect([200, 204, 408]).toContain(r1.status);
      expect([200, 204, 408]).toContain(r2.status);

      server.stop();
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
