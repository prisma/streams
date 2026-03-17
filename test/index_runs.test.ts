import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { decodeIndexRun, encodeIndexRun, RUN_TYPE_MASK16, type IndexRun } from "../src/index/run_format";

function makeConfig(rootDir: string, overrides: Partial<Config>): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

describe("index runs", () => {
  test("encode/decode roundtrip (mask16)", () => {
    const run: IndexRun = {
      meta: {
        runId: "l0-0000000000000000-0000000000000001-123",
        level: 0,
        startSegment: 0,
        endSegment: 1,
        objectKey: "streams/abc/index/run.idx",
        filterLen: 0,
        recordCount: 2,
      },
      runType: RUN_TYPE_MASK16,
      filterBytes: new Uint8Array(0),
      fingerprints: [1n, 2n],
      masks: [1, 2],
    };
    const enc = encodeIndexRun(run);
    const dec = decodeIndexRun(enc);
    expect(dec.runType).toBe(RUN_TYPE_MASK16);
    expect(dec.fingerprints.length).toBe(2);
    expect(dec.fingerprints[0]).toBe(1n);
    expect(dec.masks?.[1]).toBe(2);
  });

  test("index manager builds L0 run and selects candidate segments", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-index-"));
    const stream = "idx";
    const payload = new Uint8Array(1024);
    const rowsPerSegment = 3;
    const segments = 2;
    const hotKey = "hot";

    const cfg = makeConfig(root, {
      segmentMaxBytes: payload.byteLength * rowsPerSegment,
      segmentCheckIntervalMs: 25,
      uploadIntervalMs: 25,
      uploadConcurrency: 2,
      indexL0SpanSegments: 2,
      indexCheckIntervalMs: 50,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
    });
    const os = new MockR2Store();
    const app = createApp(cfg, os);
    try {
      expect(app.deps.config.indexL0SpanSegments).toBe(2);
      await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/octet-stream" },
        })
      );

      for (let seg = 0; seg < segments; seg++) {
        const key = seg === segments - 1 ? hotKey : `k${seg}`;
        for (let i = 0; i < rowsPerSegment; i++) {
          const r = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
              method: "POST",
              headers: { "content-type": "application/octet-stream", "stream-key": key },
              body: payload,
            })
          );
          expect(r.status).toBe(204);
        }
      }

      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        const segs = app.deps.db.listSegmentsForStream(stream);
        const pending = app.deps.db.countPendingSegments();
        const srow = app.deps.db.getStream(stream);
        const uploadedOk = srow ? srow.uploaded_through >= srow.sealed_through : false;
        if (segs.length >= segments && pending === 0 && uploadedOk) break;
        await sleep(50);
      }

      app.deps.indexer?.enqueue(stream);
      await (app.deps.indexer as any)?.tick?.();

      const runDeadline = Date.now() + 10_000;
      while (Date.now() < runDeadline) {
        const runs = app.deps.db.listIndexRuns(stream);
        if (runs.length >= 1) break;
        await sleep(50);
      }

      const indexer = app.deps.indexer;
      expect(indexer).toBeTruthy();
      const res = await indexer!.candidateSegments(stream, new TextEncoder().encode(hotKey));
      expect(res).not.toBeNull();
      expect(res!.indexedThrough).toBe(2);
      expect(res!.segments.has(1)).toBe(true);
      expect(res!.segments.has(0)).toBe(false);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);
});
