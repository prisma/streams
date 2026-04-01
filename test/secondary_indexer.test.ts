import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { SecondaryIndexManager } from "../src/index/secondary_indexer";
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

function waitForExactIdleGate(manager: SecondaryIndexManager, stream: string, attempts = 3000): boolean {
  for (let attempt = 0; attempt < attempts; attempt++) {
    if (!(manager as any).shouldPauseExactBackgroundWork(stream)) return true;
  }
  return false;
}

describe("secondary indexer", () => {
  test("pauses exact background work while a stream still has local backlog", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-pause-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 60,
      segmentCheckIntervalMs: 25,
      uploadIntervalMs: 25,
      uploadConcurrency: 2,
      indexL0SpanSegments: 2,
      indexCheckIntervalMs: 25,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([201, 204]).toContain(createRes.status);

      const schemaRes = await app.fetch(
        new Request("http://local/v1/stream/evlog/_schema", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            schema: {
              type: "object",
              properties: {
                eventTime: { type: "string" },
                service: { type: "string" },
              },
              required: ["eventTime", "service"],
            },
            search: {
              primaryTimestampField: "eventTime",
              fields: {
                eventTime: {
                  kind: "date",
                  bindings: [{ version: 1, jsonPointer: "/eventTime" }],
                  exact: true,
                  column: true,
                  exists: true,
                  sortable: true,
                },
                service: {
                  kind: "keyword",
                  bindings: [{ version: 1, jsonPointer: "/service" }],
                  normalizer: "lowercase_v1",
                  exact: true,
                },
              },
            },
          }),
        })
      );
      expect(schemaRes.status).toBe(200);

      const manager = new SecondaryIndexManager(cfg, app.deps.db, store, app.deps.registry);
      app.deps.db.db.query(`UPDATE streams SET last_append_ms=? WHERE stream=?;`).run(app.deps.db.nowMs() - 600_000n, "evlog");
      expect(waitForExactIdleGate(manager, "evlog")).toBe(true);

      app.deps.db.db.query(`UPDATE streams SET logical_size_bytes=logical_size_bytes+1 WHERE stream=?;`).run("evlog");
      expect((manager as any).shouldPauseExactBackgroundWork("evlog")).toBe(true);

      app.deps.db.db
        .query(`UPDATE streams SET pending_rows=1, pending_bytes=1, last_append_ms=? WHERE stream=?;`)
        .run(app.deps.db.nowMs(), "evlog");
      expect((manager as any).shouldPauseExactBackgroundWork("evlog")).toBe(true);

      app.deps.db.db
        .query(`UPDATE streams SET pending_rows=1, pending_bytes=1, last_append_ms=?, segment_in_progress=0 WHERE stream=?;`)
        .run(app.deps.db.nowMs() - 600_000n, "evlog");
      expect((manager as any).shouldPauseExactBackgroundWork("evlog")).toBe(true);

      app.deps.db.db.query(`UPDATE streams SET pending_rows=0, pending_bytes=0, segment_in_progress=1 WHERE stream=?;`).run("evlog");
      expect((manager as any).shouldPauseExactBackgroundWork("evlog")).toBe(true);

      app.deps.db.db.query(`UPDATE streams SET segment_in_progress=0 WHERE stream=?;`).run("evlog");
      app.deps.db.createSegmentRow({
        segmentId: "seg-0",
        stream: "evlog",
        segmentIndex: 0,
        startOffset: 0n,
        endOffset: 0n,
        blockCount: 1,
        lastAppendMs: app.deps.db.nowMs(),
        sizeBytes: 1,
        localPath: `${root}/seg-0.bin`,
      });
      expect((manager as any).shouldPauseExactBackgroundWork("evlog")).toBe(true);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("builds exact-match runs for schema-owned indexes", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 60,
      segmentCheckIntervalMs: 25,
      uploadIntervalMs: 25,
      uploadConcurrency: 2,
      indexL0SpanSegments: 2,
      indexCheckIntervalMs: 25,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([201, 204]).toContain(createRes.status);

      const schemaRes = await app.fetch(
        new Request("http://local/v1/stream/evlog/_schema", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            schema: {
              type: "object",
              properties: {
                eventTime: { type: "string" },
                service: { type: "string" },
                level: { type: "string" },
              },
              required: ["service", "level"],
            },
            search: {
              primaryTimestampField: "eventTime",
              fields: {
                eventTime: {
                  kind: "date",
                  bindings: [{ version: 1, jsonPointer: "/eventTime" }],
                  exact: true,
                  column: true,
                  exists: true,
                  sortable: true,
                },
                service: {
                  kind: "keyword",
                  bindings: [{ version: 1, jsonPointer: "/service" }],
                  normalizer: "lowercase_v1",
                  exact: true,
                  prefix: true,
                  exists: true,
                },
              },
            },
          }),
        })
      );
      expect(schemaRes.status).toBe(200);

      for (const event of [
        { service: "api", level: "info" },
        { service: "api", level: "error" },
        { service: "worker", level: "info" },
        { service: "worker", level: "error" },
      ]) {
        const appendRes = await app.fetch(
          new Request("http://local/v1/stream/evlog", {
            method: "POST",
            headers: { "content-type": "application/json" },
          body: JSON.stringify(event),
          })
        );
        expect(appendRes.status).toBe(204);
      }

      const readyDeadline = Date.now() + 10_000;
      while (Date.now() < readyDeadline) {
        const srow = app.deps.db.getStream("evlog");
        const uploadedOk = !!srow && srow.uploaded_segment_count >= 2;
        const companionPlan = app.deps.db.getSearchCompanionPlan("evlog");
        const companions = app.deps.db.listSearchSegmentCompanions("evlog");
        if (uploadedOk && companionPlan && companions.length >= 2) break;
        await sleep(50);
      }

      const manager = new SecondaryIndexManager(cfg, app.deps.db, store, app.deps.registry);
      app.deps.db.db.query(`UPDATE streams SET last_append_ms=? WHERE stream=?;`).run(app.deps.db.nowMs() - 600_000n, "evlog");
      expect(waitForExactIdleGate(manager, "evlog")).toBe(true);
      manager.enqueue("evlog");
      await (manager as any).tick?.();
      const deadline = Date.now() + 10_000;
      let stateCount = 0;
      let runCount = 0;
      while (Date.now() < deadline) {
        const segs = app.deps.db.listSegmentsForStream("evlog");
        const srow = app.deps.db.getStream("evlog");
        stateCount = app.deps.db.listSecondaryIndexStates("evlog").length;
        runCount = app.deps.db.listSecondaryIndexRuns("evlog", "service").length;
        const uploadedOk = !!srow && srow.uploaded_segment_count >= 2;
        if (segs.length >= 2 && uploadedOk && stateCount > 0 && runCount > 0) break;
        manager.enqueue("evlog");
        await (manager as any).tick?.();
        await sleep(50);
      }
      expect(stateCount).toBeGreaterThan(0);
      expect(runCount).toBeGreaterThan(0);

      const apiSegments = await manager.candidateSegmentsForSecondaryIndex("evlog", "service", new TextEncoder().encode("api"));
      const workerSegments = await manager.candidateSegmentsForSecondaryIndex("evlog", "service", new TextEncoder().encode("worker"));

      expect(apiSegments).not.toBeNull();
      expect(workerSegments).not.toBeNull();
      expect(Array.from(apiSegments!.segments).sort((a, b) => a - b)).toEqual([0]);
      expect(Array.from(workerSegments!.segments).sort((a, b) => a - b)).toEqual([1]);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
