import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { streamHash16Hex } from "../src/util/stream_paths";

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
    uploadConcurrency: 2,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("poison stream isolation", () => {
  test("one failing stream does not block other streams from uploading", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-poison-"));
    const poisonStream = "poison_stream";
    const okStream = "ok_stream";
    const origError = console.error;
    try {
      console.error = () => {};
      const poisonHash = streamHash16Hex(poisonStream);
      const os = new MockR2Store({ faults: { failPutPrefix: `streams/${poisonHash}/` } });
      const cfg = makeConfig(root);
      const app = createApp(cfg, os);
      // Stop background timers; drive ticks manually for deterministic behavior.
      app.deps.segmenter.stop();
      app.deps.uploader.stop();
      const server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(poisonStream)}`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
      });
      await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(okStream)}`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
      });

      const payload = new Uint8Array(600);
      payload.fill(7);
      for (let i = 0; i < 4; i++) {
        await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(poisonStream)}`, {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: payload,
        });
        await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(okStream)}`, {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: payload,
        });
      }

      const okHash = streamHash16Hex(okStream);
      const okManifest = `streams/${okHash}/manifest.json`;
      const poisonPrefix = `streams/${poisonHash}/`;
      const segmenter = app.deps.segmenter as any;
      const uploader = app.deps.uploader as any;
      const deadline = Date.now() + 5000;
      let okManifestSeen = false;
      while (Date.now() < deadline) {
        await segmenter.tick();
        await uploader.tick();
        const keys = await os.list("streams/");
        if (keys.includes(okManifest)) {
          okManifestSeen = true;
          break;
        }
        await sleep(20);
      }

      expect(okManifestSeen).toBe(true);
      const poisonKeys = await os.list(poisonPrefix);
      expect(poisonKeys.length).toBe(0);

      server.stop();
      app.close();
    } finally {
      console.error = origError;
      rmSync(root, { recursive: true, force: true });
    }
  });
});
