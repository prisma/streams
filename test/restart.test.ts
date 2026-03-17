import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

function makeConfig(rootDir: string): Config {
  const base = loadConfig();
  return { ...base, rootDir, dbPath: `${rootDir}/wal.sqlite`, port: 0 };
}

describe("restart recovery", () => {
  test("data remains after restart", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-restart-"));
    try {
      const cfg = makeConfig(root);
      const os = new MockR2Store();
      let app = createApp(cfg, os);
      let server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/restart`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/restart`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: new TextEncoder().encode("hello"),
      });

      server.stop();
      app.close();

      app = createApp(cfg, os);
      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl2 = `http://localhost:${server.port}`;
      const r = await fetch(`${baseUrl2}/v1/stream/restart?offset=-1`);
      const bytes = new Uint8Array(await r.arrayBuffer());
      expect(new TextDecoder().decode(bytes)).toBe("hello");

      server.stop();
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
