import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { SecondaryIndexManager, secondaryBuildDispatchConcurrency } from "../src/index/secondary_indexer";
import { IndexSegmentLocalityManager } from "../src/index/segment_locality";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { getConfiguredSecondaryIndexes } from "../src/index/secondary_schema";

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

describe("secondary indexer", () => {
  test("builds exact-match runs for schema-owned indexes", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 60,
      segmentCheckIntervalMs: 25,
      uploadIntervalMs: 25,
      uploadConcurrency: 2,
      indexL0SpanSegments: 2,
      indexCheckIntervalMs: 25,
      segmentCacheMaxBytes: 64 * 1024 * 1024,
      segmentFooterCacheEntries: 128,
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

      const locality = new IndexSegmentLocalityManager(app.deps.segmentDiskCache!, store);
      const manager = new SecondaryIndexManager(
        cfg,
        app.deps.db,
        store,
        app.deps.registry,
        app.deps.segmentDiskCache,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        locality
      );
      app.deps.db.db.query(`UPDATE streams SET last_append_ms=? WHERE stream=?;`).run(app.deps.db.nowMs() - 600_000n, "evlog");
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

  test("evlog secondary build dispatch uses unique batch leaders and bounded parallelism", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-dispatch-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 60,
      segmentCheckIntervalMs: 25,
      uploadIntervalMs: 25,
      uploadConcurrency: 2,
      indexL0SpanSegments: 1,
      indexCheckIntervalMs: 25,
      segmentCacheMaxBytes: 64 * 1024 * 1024,
      segmentFooterCacheEntries: 128,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-dispatch", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([201, 204]).toContain(createRes.status);

      const profileRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-dispatch/_profile", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            profile: { kind: "evlog" },
          }),
        })
      );
      expect(profileRes.status).toBe(200);

      const locality = new IndexSegmentLocalityManager(app.deps.segmentDiskCache!, store);
      const manager = new SecondaryIndexManager(
        cfg,
        app.deps.db,
        store,
        app.deps.registry,
        app.deps.segmentDiskCache,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        locality
      );

      expect(secondaryBuildDispatchConcurrency("evlog", 6)).toBe(2);

      const registryRes = app.deps.registry.getRegistryResult("evlog-dispatch");
      expect(Result.isOk(registryRes)).toBe(true);
      if (Result.isError(registryRes)) throw new Error(registryRes.error.message);
      const configured = getConfiguredSecondaryIndexes(registryRes.value);
      const leaders = ((manager as any).selectBuildBatchLeaders("evlog-dispatch", configured, "evlog") as Array<{ name: string }>).map(
        (entry) => entry.name
      );
      expect(leaders).toEqual(["timestamp", "level", "requestId", "spanId", "path", "method"]);

      const started: string[] = [];
      const originalMaybeBuildRuns = (manager as any).maybeBuildRuns.bind(manager);
      (manager as any).maybeBuildRuns = async (_stream: string, index: { name: string }) => {
        started.push(index.name);
        await sleep(100);
        return Result.ok(undefined);
      };
      manager.enqueue("evlog-dispatch");

      const startedAt = performance.now();
      await (manager as any).runOneBuildTask();
      const elapsedMs = performance.now() - startedAt;

      (manager as any).maybeBuildRuns = originalMaybeBuildRuns;

      expect(started).toEqual(leaders);
      expect(elapsedMs).toBeLessThan(600 * 0.75);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
