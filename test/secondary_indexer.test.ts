import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import {
  SecondaryIndexManager,
  secondaryBuildDispatchConcurrency,
  secondaryExactBatchSpan,
  shouldDeferSecondaryCompaction,
} from "../src/index/secondary_indexer";
import { IndexSegmentLocalityManager } from "../src/index/segment_locality";
import { MockR2Store } from "../src/objectstore/mock_r2";
import type { GetOptions, ObjectStore, PutFileNoEtagOptions, PutFileOptions, PutNoEtagOptions, PutOptions, PutResult } from "../src/objectstore/interface";
import { getConfiguredSecondaryIndexes, hashSecondaryIndexField } from "../src/index/secondary_schema";
import { encodeIndexRunResult, RUN_TYPE_MASK16 } from "../src/index/run_format";

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
  test("evlog exact batches keep high-cardinality jobs single-field and use bounded multi-segment spans", () => {
    expect(secondaryExactBatchSpan("evlog", 16, ["requestId"])).toBe(2);
    expect(secondaryExactBatchSpan("evlog", 16, ["traceId"])).toBe(2);
    expect(secondaryExactBatchSpan("evlog", 16, ["spanId"])).toBe(2);
    expect(secondaryExactBatchSpan("evlog", 16, ["path"])).toBe(2);
    expect(secondaryExactBatchSpan("evlog", 1, ["requestId"])).toBe(1);
    expect(secondaryExactBatchSpan("evlog", 16, ["timestamp"])).toBe(2);
    expect(secondaryExactBatchSpan("evlog", 16, ["level", "service", "environment"])).toBe(4);
    expect(secondaryExactBatchSpan("evlog", 16, ["method", "status", "duration"])).toBe(2);
    expect(secondaryExactBatchSpan("generic", 8, ["field"])).toBe(8);
    expect(secondaryExactBatchSpan("evlog", 0, ["timestamp"])).toBe(0);
  });

  test("evlog defers secondary compaction only while lag stays large", () => {
    expect(shouldDeferSecondaryCompaction("evlog", 512, 0)).toBe(true);
    expect(shouldDeferSecondaryCompaction("evlog", 255, 0)).toBe(false);
    expect(shouldDeferSecondaryCompaction("evlog", 400, 200)).toBe(false);
    expect(shouldDeferSecondaryCompaction("generic", 512, 0)).toBe(false);
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
      let serviceIndexedThrough = 0;
      while (Date.now() < deadline) {
        const segs = app.deps.db.listSegmentsForStream("evlog");
        const srow = app.deps.db.getStream("evlog");
        stateCount = app.deps.db.listSecondaryIndexStates("evlog").length;
        runCount = app.deps.db.listSecondaryIndexRuns("evlog", "service").length;
        serviceIndexedThrough = app.deps.db.getSecondaryIndexState("evlog", "service")?.indexed_through ?? 0;
        const uploadedOk = !!srow && srow.uploaded_segment_count >= 2;
        if (segs.length >= 2 && uploadedOk && stateCount > 0 && runCount > 0 && serviceIndexedThrough >= 2) break;
        manager.enqueue("evlog");
        await (manager as any).tick?.();
        await sleep(50);
      }
      expect(stateCount).toBeGreaterThan(0);
      expect(runCount).toBeGreaterThan(0);
      expect(serviceIndexedThrough).toBeGreaterThanOrEqual(2);

      const apiSegments = await manager.candidateSegmentsForSecondaryIndex("evlog", "service", new TextEncoder().encode("api"));
      const workerSegments = await manager.candidateSegmentsForSecondaryIndex("evlog", "service", new TextEncoder().encode("worker"));

      expect(apiSegments).not.toBeNull();
      expect(workerSegments).not.toBeNull();
      expect(Array.from(apiSegments!.segments).sort((a, b) => a - b)).toEqual([0]);
      expect(Array.from(workerSegments!.segments).sort((a, b) => a - b)).toEqual([1]);
    } finally {
      app.close();
      await sleep(150);
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

      expect(secondaryBuildDispatchConcurrency("evlog", 6)).toBe(4);

      const registryRes = app.deps.registry.getRegistryResult("evlog-dispatch");
      expect(Result.isOk(registryRes)).toBe(true);
      if (Result.isError(registryRes)) throw new Error(registryRes.error.message);
      const configured = getConfiguredSecondaryIndexes(registryRes.value);
      const leaders = ((manager as any).selectBuildBatchLeaders("evlog-dispatch", configured, "evlog") as Array<{ name: string }>).map(
        (entry) => entry.name
      );
      expect(leaders).toEqual(["timestamp", "level", "requestId", "traceId", "spanId", "path", "method"]);

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
      expect(elapsedMs).toBeLessThan(leaders.length * 100 * 0.75);
    } finally {
      app.close();
      await sleep(150);
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog secondary build leaders prioritize the lagging members first", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-dispatch-skew-"));
    const cfg = makeConfig(root, {
      indexL0SpanSegments: 0,
      segmentCacheMaxBytes: 64 * 1024 * 1024,
      segmentFooterCacheEntries: 128,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-dispatch-skew", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([201, 204]).toContain(createRes.status);

      const profileRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-dispatch-skew/_profile", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            profile: { kind: "evlog" },
          }),
        })
      );
      expect(profileRes.status).toBe(200);

      const registryRes = app.deps.registry.getRegistryResult("evlog-dispatch-skew");
      expect(Result.isOk(registryRes)).toBe(true);
      if (Result.isError(registryRes)) throw new Error(registryRes.error.message);
      const configured = getConfiguredSecondaryIndexes(registryRes.value);
      for (const configuredIndex of configured) {
        app.deps.db.upsertSecondaryIndexState(
          "evlog-dispatch-skew",
          configuredIndex.name,
          new Uint8Array(16).fill(1),
          hashSecondaryIndexField(configuredIndex),
          0
        );
      }
      for (const [name, indexedThrough] of [
        ["timestamp", 10],
        ["level", 14],
        ["service", 14],
        ["environment", 14],
        ["method", 13],
        ["status", 12],
        ["duration", 11],
        ["requestId", 20],
        ["traceId", 18],
        ["spanId", 19],
        ["path", 17],
      ] as const) {
        const state = app.deps.db.getSecondaryIndexState("evlog-dispatch-skew", name);
        expect(state).not.toBeNull();
        app.deps.db.updateSecondaryIndexedThrough("evlog-dispatch-skew", name, indexedThrough);
      }

      const manager = new SecondaryIndexManager(
        cfg,
        app.deps.db,
        store,
        app.deps.registry,
        app.deps.segmentDiskCache
      );
      const leaders = ((manager as any).selectBuildBatchLeaders(
        "evlog-dispatch-skew",
        configured,
        "evlog"
      ) as Array<{ name: string }>).map((entry) => entry.name);
      expect(leaders).toEqual(["timestamp", "duration", "level", "path", "traceId", "spanId", "requestId"]);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog secondary build batches publish the manifest once per tick", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-manifest-build-"));
    const cfg = makeConfig(root, {
      indexL0SpanSegments: 1,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-build-manifest", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const profileRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-build-manifest/_profile", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            profile: { kind: "evlog" },
          }),
        })
      );
      expect(profileRes.status).toBe(200);

      let publishCalls = 0;
      const manager = new SecondaryIndexManager(
        cfg,
        app.deps.db,
        store,
        app.deps.registry,
        app.deps.segmentDiskCache,
        async () => {
          publishCalls += 1;
        }
      );

      const registryRes = app.deps.registry.getRegistryResult("evlog-build-manifest");
      expect(Result.isOk(registryRes)).toBe(true);
      if (Result.isError(registryRes)) throw new Error(registryRes.error.message);
      const configured = getConfiguredSecondaryIndexes(registryRes.value);
      const leaders = ((manager as any).selectBuildBatchLeaders("evlog-build-manifest", configured, "evlog") as Array<{ name: string }>).map(
        (entry) => entry.name
      );
      (manager as any).maybeBuildRuns = async (stream: string) => {
        (manager as any).requestDeferredManifestPublish(stream);
        await sleep(10);
        return Result.ok(undefined);
      };
      manager.enqueue("evlog-build-manifest");

      await (manager as any).runOneBuildTask();

      expect(leaders.length).toBeGreaterThan(1);
      expect(publishCalls).toBe(1);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog secondary compactions publish the manifest once per tick", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-manifest-compact-"));
    const cfg = makeConfig(root, {
      indexL0SpanSegments: 1,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-compact-manifest", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const profileRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-compact-manifest/_profile", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            profile: { kind: "evlog" },
          }),
        })
      );
      expect(profileRes.status).toBe(200);

      let publishCalls = 0;
      const manager = new SecondaryIndexManager(
        cfg,
        app.deps.db,
        store,
        app.deps.registry,
        app.deps.segmentDiskCache,
        async () => {
          publishCalls += 1;
        }
      );

      (manager as any).maybeCompactRuns = async (stream: string) => {
        (manager as any).requestDeferredManifestPublish(stream);
        await sleep(5);
        return Result.ok(undefined);
      };
      manager.enqueue("evlog-compact-manifest");

      await (manager as any).runOneCompactionTask();

      expect(publishCalls).toBe(1);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog secondary compactions defer while exact lag stays far behind uploads", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-compact-defer-"));
    const cfg = makeConfig(root, {
      indexL0SpanSegments: 0,
    });
    const store = new MockR2Store();
    const app = createApp(cfg, store);
    try {
      const createRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-compact-defer", {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const profileRes = await app.fetch(
        new Request("http://local/v1/stream/evlog-compact-defer/_profile", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            profile: { kind: "evlog" },
          }),
        })
      );
      expect(profileRes.status).toBe(200);

      const registryRes = app.deps.registry.getRegistryResult("evlog-compact-defer");
      expect(Result.isOk(registryRes)).toBe(true);
      if (Result.isError(registryRes)) throw new Error(registryRes.error.message);
      const configured = getConfiguredSecondaryIndexes(registryRes.value);
      for (const configuredIndex of configured) {
        app.deps.db.upsertSecondaryIndexState(
          "evlog-compact-defer",
          configuredIndex.name,
          new Uint8Array(16).fill(1),
          hashSecondaryIndexField(configuredIndex),
          0
        );
      }

      const manager = new SecondaryIndexManager(
        {
          ...cfg,
          indexL0SpanSegments: 1,
        },
        app.deps.db,
        store,
        app.deps.registry,
        app.deps.segmentDiskCache
      );

      let compactCalls = 0;
      (manager as any).maybeCompactRuns = async () => {
        compactCalls += 1;
        return Result.ok(undefined);
      };
      const originalCountUploadedSegments = app.deps.db.countUploadedSegments.bind(app.deps.db);
      (app.deps.db as any).countUploadedSegments = () => 512;
      manager.enqueue("evlog-compact-defer");

      const progressed = await (manager as any).runOneCompactionTask();

      (app.deps.db as any).countUploadedSegments = originalCountUploadedSegments;

      expect(progressed).toBe(false);
      expect(compactCalls).toBe(0);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("inline-fetches tiny compaction sources instead of using getFile", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-inline-source-"));
    const cfg = makeConfig(root, {
      indexL0SpanSegments: 1,
    });
    const payloadRes = encodeIndexRunResult({
      meta: {
        runId: "tiny-run",
        level: 0,
        startSegment: 0,
        endSegment: 0,
        objectKey: "streams/test/tiny-run.irn",
        filterLen: 0,
        recordCount: 1,
      },
      runType: RUN_TYPE_MASK16,
      filterBytes: new Uint8Array(0),
      fingerprints: [1n],
      masks: [1],
    });
    expect(Result.isOk(payloadRes)).toBe(true);
    if (Result.isError(payloadRes)) throw new Error(payloadRes.error.message);

    class InlineFetchProbeStore implements ObjectStore {
      getCalls = 0;
      getFileCalls = 0;

      async put(_key: string, _data: Uint8Array, _opts?: PutOptions): Promise<PutResult> {
        return { etag: "etag" };
      }

      async putFile(_key: string, _path: string, _size: number, _opts?: PutFileOptions): Promise<PutResult> {
        return { etag: "etag" };
      }

      async putNoEtag(_key: string, data: Uint8Array, _opts?: PutNoEtagOptions): Promise<number> {
        return data.byteLength;
      }

      async putFileNoEtag(_key: string, _path: string, size: number, _opts?: PutFileNoEtagOptions): Promise<number> {
        return size;
      }

      async get(_key: string, _opts?: GetOptions): Promise<Uint8Array | null> {
        this.getCalls += 1;
        return payloadRes.value;
      }

      async getFile(_key: string, _path: string): Promise<{ size: number } | null> {
        this.getFileCalls += 1;
        return { size: payloadRes.value.byteLength };
      }

      async head(_key: string): Promise<{ etag: string; size: number } | null> {
        return null;
      }

      async delete(_key: string): Promise<void> {}

      async list(_prefix: string): Promise<string[]> {
        return [];
      }
    }

    const store = new InlineFetchProbeStore();
    const app = createApp(cfg, new MockR2Store());
    try {
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
      const sourceRes = await (manager as any).prepareCompactionSourceResult({
        run_id: "tiny-run",
        stream: "evlog-1",
        index_name: "level",
        level: 0,
        start_segment: 0,
        end_segment: 0,
        object_key: "streams/test/tiny-run.irn",
        size_bytes: payloadRes.value.byteLength,
        filter_len: 0,
        record_count: 1,
      });
      expect(Result.isOk(sourceRes)).toBe(true);
      if (Result.isError(sourceRes)) throw new Error(sourceRes.error.message);
      expect(sourceRes.value.location).toBe("inline_fetch");
      expect(sourceRes.value.source.bytes).toBeInstanceOf(Uint8Array);
      expect(store.getCalls).toBe(1);
      expect(store.getFileCalls).toBe(0);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("run loads prefer getFile when the object store and run disk cache support it", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-index-run-file-"));
    const cfg = makeConfig(root, {
      indexRunCacheMaxBytes: 16 * 1024 * 1024,
      segmentCacheMaxBytes: 16 * 1024 * 1024,
      segmentFooterCacheEntries: 32,
    });
    const payloadRes = encodeIndexRunResult({
      meta: {
        runId: "run-file",
        level: 0,
        startSegment: 0,
        endSegment: 0,
        objectKey: "streams/test/run-file.irn",
        filterLen: 0,
        recordCount: 1,
      },
      runType: RUN_TYPE_MASK16,
      filterBytes: new Uint8Array(0),
      fingerprints: [1n],
      masks: [1],
    });
    expect(Result.isOk(payloadRes)).toBe(true);
    if (Result.isError(payloadRes)) throw new Error(payloadRes.error.message);

    class RunGetFileStore implements ObjectStore {
      getCalls = 0;
      getFileCalls = 0;

      async put(_key: string, _data: Uint8Array, _opts?: PutOptions): Promise<PutResult> {
        return { etag: "etag" };
      }

      async putFile(_key: string, _path: string, _size: number, _opts?: PutFileOptions): Promise<PutResult> {
        return { etag: "etag" };
      }

      async putNoEtag(_key: string, data: Uint8Array, _opts?: PutNoEtagOptions): Promise<number> {
        return data.byteLength;
      }

      async putFileNoEtag(_key: string, _path: string, size: number, _opts?: PutFileNoEtagOptions): Promise<number> {
        return size;
      }

      async get(_key: string, _opts?: GetOptions): Promise<Uint8Array | null> {
        this.getCalls += 1;
        return payloadRes.value;
      }

      async getFile(_key: string, path: string): Promise<{ size: number } | null> {
        this.getFileCalls += 1;
        await Bun.write(path, payloadRes.value);
        return { size: payloadRes.value.byteLength };
      }

      async head(_key: string): Promise<{ etag: string; size: number } | null> {
        return null;
      }

      async delete(_key: string): Promise<void> {}

      async list(_prefix: string): Promise<string[]> {
        return [];
      }
    }

    const store = new RunGetFileStore();
    const app = createApp(cfg, new MockR2Store());
    try {
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
      const runRes = await (manager as any).loadRunResult({
        run_id: "run-file",
        stream: "evlog-1",
        index_name: "service",
        level: 0,
        start_segment: 0,
        end_segment: 0,
        object_key: "streams/test/run-file.irn",
        size_bytes: payloadRes.value.byteLength,
        filter_len: 0,
        record_count: 1,
      });
      expect(Result.isOk(runRes)).toBe(true);
      if (Result.isError(runRes)) throw new Error(runRes.error.message);
      expect(store.getFileCalls).toBe(1);
      expect(store.getCalls).toBe(0);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
