import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { localSegmentPath, streamHash16Hex } from "../src/util/stream_paths";
import { SqliteDurableStore } from "../src/db/db";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 60_000,
    ...overrides,
  };
}

describe("segment recovery", () => {
  test("startup cleans temp segments and resets in-progress flag", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-seg-recover-"));
    const stream = "seg_recover";
    try {
      const cfg = makeConfig(root);
      const app1 = createApp(cfg, new MockR2Store());
      app1.deps.segmenter.stop();
      app1.deps.uploader.stop();
      const server1 = Bun.serve({ port: 0, fetch: app1.fetch });
      const baseUrl1 = `http://localhost:${server1.port}`;

      await fetch(`${baseUrl1}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
      });
      await fetch(`${baseUrl1}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "hello",
      });

      server1.stop();
      app1.close();

      const db = new SqliteDurableStore(cfg.dbPath);
      db.db.query(`UPDATE streams SET segment_in_progress=1 WHERE stream=?;`).run(stream);
      db.close();

      const shash = streamHash16Hex(stream);
      const tmpPath = `${localSegmentPath(cfg.rootDir, shash, 0)}.tmp`;
      mkdirSync(dirname(tmpPath), { recursive: true });
      writeFileSync(tmpPath, new Uint8Array([1, 2, 3]));
      expect(existsSync(tmpPath)).toBe(true);

      const app2 = createApp(cfg, new MockR2Store());
      app2.deps.segmenter.stop();
      app2.deps.uploader.stop();
      const srow = app2.deps.db.getStream(stream);
      expect(srow).not.toBeNull();
      if (srow) expect(srow.segment_in_progress).toBe(0);
      expect(existsSync(tmpPath)).toBe(false);

      const server2 = Bun.serve({ port: 0, fetch: app2.fetch });
      const baseUrl2 = `http://localhost:${server2.port}`;
      const r = await fetch(`${baseUrl2}/v1/stream/${encodeURIComponent(stream)}?offset=-1`);
      expect(r.status).toBe(200);
      const text = await r.text();
      expect(text).toBe("hello");

      server2.stop();
      app2.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
