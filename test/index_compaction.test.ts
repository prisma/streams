import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

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

describe("index compaction", () => {
  test(
    "compacts L0 runs into L1 postings",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-compact-"));
      const stream = "compact";
      const payload = new Uint8Array(512);
      const rowsPerSegment = 2;
      const segments = 4;

      const cfg = makeConfig(root, {
        segmentMaxBytes: payload.byteLength * rowsPerSegment,
        segmentCheckIntervalMs: 25,
        uploadIntervalMs: 25,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCompactionFanout: 2,
        indexMaxLevel: 1,
        indexCompactionConcurrency: 1,
        indexCheckIntervalMs: 25,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });

      const os = new MockR2Store();
      const app = createApp(cfg, os);
      try {
        await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "PUT",
            headers: { "content-type": "application/octet-stream" },
          })
        );

        for (let seg = 0; seg < segments; seg++) {
          const key = `k${seg}`;
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

        const indexer = app.deps.indexer;
        expect(indexer).toBeTruthy();
        indexer!.enqueue(stream);
        await (indexer as any)?.tick?.();

        const runDeadline = Date.now() + 10_000;
        while (Date.now() < runDeadline) {
          const active = app.deps.db.listIndexRuns(stream);
          const retired = app.deps.db.listRetiredIndexRuns(stream);
          if (active.length === 1 && active[0].level === 1 && retired.length === 2) break;
          indexer!.enqueue(stream);
          await (indexer as any)?.tick?.();
          await sleep(50);
        }

        const active = app.deps.db.listIndexRuns(stream);
        const retired = app.deps.db.listRetiredIndexRuns(stream);
        expect(active.length).toBe(1);
        expect(active[0].level).toBe(1);
        expect(retired.length).toBe(2);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );
});
