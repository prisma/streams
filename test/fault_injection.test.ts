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
    segmentMaxBytes: 1024,
    blockMaxBytes: 512,
    segmentCheckIntervalMs: 50,
    uploadIntervalMs: 50,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("fault injection", () => {
  test("segment upload retries succeed under transient failures", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-fault-"));
    try {
      const cfg = makeConfig(root);
      const os = new MockR2Store({ failPutEvery: 2 });
      const app = createApp(cfg, os);
      const server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/fault_test`, { method: "PUT" });

      const payload = new Uint8Array(600);
      payload.fill(7);
      for (let i = 0; i < 6; i++) {
        const r = await fetch(`${baseUrl}/v1/stream/fault_test`, {
          method: "POST",
          headers: { "content-type": "application/octet-stream" },
          body: payload,
        });
        expect(r.status).toBe(204);
      }

      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        const keys = await os.list("streams/");
        const hasManifest = keys.some((k) => k.endsWith("/manifest.json"));
        const hasSegment = keys.some((k) => k.includes("/segments/") && k.endsWith(".bin"));
        if (hasManifest && hasSegment) break;
        await sleep(50);
      }

      const keys = await os.list("streams/");
      expect(keys.some((k) => k.endsWith("/manifest.json"))).toBe(true);
      expect(keys.some((k) => k.includes("/segments/") && k.endsWith(".bin"))).toBe(true);

      server.stop();
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
