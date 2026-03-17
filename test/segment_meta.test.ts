import { describe, expect, test } from "bun:test";
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
    segmentMaxBytes: 256,
    segmentCheckIntervalMs: 25,
    uploadIntervalMs: 25,
    uploadConcurrency: 2,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("segment meta", () => {
  test("segment meta arrays track uploaded segments for manifests", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-segmeta-"));
    const stream = "segmeta";
    const payload = new Uint8Array(128);
    const store = new MockR2Store();
    const cfg = makeConfig(root);
    const app = createApp(cfg, store);
    try {
      await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/octet-stream" },
        })
      );

      for (let i = 0; i < 8; i++) {
        const r = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/octet-stream" },
            body: payload,
          })
        );
        expect(r.status).toBe(204);
      }

      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        const pending = app.deps.db.countPendingSegments();
        const srow = app.deps.db.getStream(stream);
        const uploadedOk = srow ? srow.uploaded_through >= srow.sealed_through : false;
        if (pending === 0 && uploadedOk) break;
        await sleep(50);
      }

      const segs = app.deps.db.listSegmentsForStream(stream);
      const meta = app.deps.db.getSegmentMeta(stream);
      expect(meta).not.toBeNull();
      expect(meta!.segment_count).toBe(segs.length);
      expect(meta!.segment_offsets.byteLength).toBe(segs.length * 8);
      expect(meta!.segment_blocks.byteLength).toBe(segs.length * 4);
      expect(meta!.segment_last_ts.byteLength).toBe(segs.length * 8);

      const srow = app.deps.db.getStream(stream);
      expect(srow).not.toBeNull();
      expect(srow!.uploaded_segment_count).toBe(segs.length);

      const shash = streamHash16Hex(stream);
      const manifestKey = `streams/${shash}/manifest.json`;
      const manifestBytes = await store.get(manifestKey);
      expect(manifestBytes).not.toBeNull();
      const manifest = JSON.parse(new TextDecoder().decode(manifestBytes!));
      expect(manifest.segment_count).toBe(segs.length);
      expect(manifest.uploaded_through).toBe(segs.length);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
