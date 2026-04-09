import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { runEvlogIngester } from "../experiments/demo/evlog_ingester";

class SlowDeleteR2Store extends MockR2Store {
  constructor(private readonly deleteDelayMs: number) {
    super();
  }

  override async delete(key: string): Promise<void> {
    await sleep(this.deleteDelayMs);
    await super.delete(key);
  }
}

class SlowCompanionPutR2Store extends MockR2Store {
  constructor(private readonly putDelayMs: number) {
    super();
  }

  override async put(key: string, data: Uint8Array, opts = {}): Promise<{ etag: string }> {
    if (key.endsWith(".cix")) await sleep(this.putDelayMs);
    return super.put(key, data, opts);
  }

  override async putFile(key: string, path: string, size: number, opts = {}): Promise<{ etag: string }> {
    if (key.endsWith(".cix")) await sleep(this.putDelayMs);
    return super.putFile(key, path, size, opts);
  }
}

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 10,
    uploadIntervalMs: 10,
    uploadConcurrency: 2,
    indexL0SpanSegments: 1,
    indexCompactionFanout: 2,
    indexCheckIntervalMs: 10,
    segmentCacheMaxBytes: 64 * 1024 * 1024,
    segmentFooterCacheEntries: 64,
    ...overrides,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeDemoFetch(app: ReturnType<typeof createApp>, baseUrl: string): typeof fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    if (!url.startsWith(baseUrl)) {
      throw new Error(`unexpected fetch url: ${url}`);
    }
    if (input instanceof Request) return app.fetch(new Request(input, init));
    return app.fetch(new Request(url, init));
  }) as typeof fetch;
}

async function waitForActionKinds(
  app: ReturnType<typeof createApp>,
  stream: string,
  kinds: string[],
  timeoutMs = 15_000
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    app.deps.indexer?.enqueue(stream);
    await (app.deps.indexer as any)?.tick?.();
    const actions = app.deps.db.listAsyncIndexActions(stream, 200);
    const completed = new Set(
      actions.filter((row) => row.status === "succeeded" && row.end_time_ms != null).map((row) => row.action_kind)
    );
    if (kinds.every((kind) => completed.has(kind))) return actions;
    await sleep(50);
  }
  throw new Error(`timeout waiting for async index actions: ${kinds.join(", ")}`);
}

async function waitForCondition(
  app: ReturnType<typeof createApp>,
  stream: string,
  predicate: () => boolean,
  timeoutMs = 15_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    app.deps.indexer?.enqueue(stream);
    await (app.deps.indexer as any)?.tick?.();
    if (predicate()) return;
    await sleep(50);
  }
  throw new Error("timeout waiting for condition");
}

function expectActionShape(action: any): void {
  expect(action.seq).toBeGreaterThan(0n);
  expect(action.begin_time_ms).toBeGreaterThan(0n);
  expect(action.end_time_ms).not.toBeNull();
  expect(action.duration_ms).not.toBeNull();
  expect(action.duration_ms).toBeGreaterThanOrEqual(0n);
  expect(action.status).toBe("succeeded");
  expect(action.input_count).toBeGreaterThan(0);
  expect(action.input_size_bytes).toBeGreaterThan(0n);
  expect(action.output_count).toBeGreaterThan(0);
  expect(action.output_size_bytes).toBeGreaterThan(0n);
}

function expectJobRssDetail(detail: Record<string, unknown>): void {
  expect(typeof detail.job_worker_pid).toBe("number");
  expect(typeof detail.job_rss_baseline_bytes).toBe("number");
  expect(typeof detail.job_rss_peak_bytes).toBe("number");
  expect(typeof detail.job_rss_peak_contributed_bytes).toBe("number");
  expect((detail.job_rss_peak_bytes as number) >= (detail.job_rss_baseline_bytes as number)).toBe(true);
  expect((detail.job_rss_peak_contributed_bytes as number) >= 0).toBe(true);
}

describe("async index action observability", () => {
  test("logs routing and lexicon builds plus compactions", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-async-index-routing-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 96,
      segmentTargetRows: 1,
    });
    const app = createApp(cfg, new MockR2Store());
    const stream = "routing-observe";
    try {
      const createRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const schemaRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            schema: {
              type: "object",
              additionalProperties: true,
            },
            routingKey: {
              jsonPointer: "/bucket",
              required: true,
            },
          }),
        })
      );
      expect(schemaRes.status).toBe(200);

      for (let i = 0; i < 4; i += 1) {
        const appendRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              bucket: `routing-key-${i % 2}`,
              pad: "x".repeat(160),
            }),
          })
        );
        expect(appendRes.status).toBe(204);
      }

      const actions = await waitForActionKinds(app, stream, [
        "routing_l0_build",
        "routing_compaction_build",
        "lexicon_l0_build",
        "lexicon_compaction_build",
      ]);

      for (const kind of ["routing_l0_build", "routing_compaction_build", "lexicon_l0_build", "lexicon_compaction_build"]) {
        const action = actions.find((row) => row.action_kind === kind && row.status === "succeeded");
        expect(action).toBeTruthy();
        expectActionShape(action);
        expectJobRssDetail(JSON.parse(action!.detail_json));
      }
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  test("logs secondary and companion async index actions", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-async-index-search-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 220,
      segmentTargetRows: 1,
      searchCompanionBuildBatchSegments: 1,
    });
    const app = createApp(cfg, new MockR2Store());
    const stream = "search-observe";
    try {
      const createRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const schemaRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            schema: {
              type: "object",
              additionalProperties: true,
            },
            search: {
              primaryTimestampField: "time",
              fields: {
                time: {
                  kind: "date",
                  bindings: [{ version: 1, jsonPointer: "/time" }],
                  column: true,
                  exists: true,
                  sortable: true,
                },
                cardinality100: {
                  kind: "keyword",
                  bindings: [{ version: 1, jsonPointer: "/cardinality100" }],
                  normalizer: "lowercase_v1",
                  exact: true,
                  prefix: true,
                  exists: true,
                },
                randomString: {
                  kind: "text",
                  bindings: [{ version: 1, jsonPointer: "/randomString" }],
                  analyzer: "unicode_word_v1",
                  exists: true,
                  positions: true,
                },
              },
            },
          }),
        })
      );
      expect(schemaRes.status).toBe(200);

      for (let i = 0; i < 4; i += 1) {
        const appendRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              time: `2026-04-07T10:00:0${i}.000Z`,
              cardinality100: `bucket-${i % 2}`,
              randomString: `message ${i} ${"x".repeat(200)}`,
            }),
          })
        );
        expect(appendRes.status).toBe(204);
      }

      const actions = await waitForActionKinds(app, stream, [
        "secondary_l0_build",
        "secondary_compaction_build",
        "companion_build",
      ]);

      for (const kind of ["secondary_l0_build", "secondary_compaction_build", "companion_build"]) {
        const action = actions.find((row) => row.action_kind === kind && row.status === "succeeded");
        expect(action).toBeTruthy();
        expectActionShape(action);
        if (action?.action_kind === "secondary_l0_build") {
          const detail = JSON.parse(action.detail_json);
          expectJobRssDetail(detail);
          expect(typeof detail.build_ms).toBe("number");
          expect(typeof detail.exact_persist_ms).toBe("number");
          expect(typeof detail.piggyback_companion_persist_ms).toBe("number");
          expect(typeof detail.piggyback_companion_deferred).toBe("boolean");
          expect(typeof detail.manifest_published).toBe("boolean");
        }
        if (action?.action_kind === "secondary_compaction_build") {
          const detail = JSON.parse(action.detail_json);
          expectJobRssDetail(detail);
          expect(typeof detail.source_prepare_ms).toBe("number");
          expect(typeof detail.worker_build_ms).toBe("number");
          expect(typeof detail.artifact_persist_ms).toBe("number");
          expect(typeof detail.source_fetch_concurrency).toBe("number");
        }
        if (action?.action_kind === "companion_build") {
          const detail = JSON.parse(action.detail_json);
          expectJobRssDetail(detail);
        }
      }
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("secondary compaction deletes retired runs with bounded parallelism", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-async-index-secondary-gc-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 220,
      segmentTargetRows: 1,
      searchCompanionBuildBatchSegments: 1,
    });
    const app = createApp(cfg, new SlowDeleteR2Store(600));
    const stream = "search-observe-gc";
    try {
      const createRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      const schemaRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            schema: {
              type: "object",
              additionalProperties: true,
            },
            search: {
              primaryTimestampField: "time",
              fields: {
                time: {
                  kind: "date",
                  bindings: [{ version: 1, jsonPointer: "/time" }],
                  column: true,
                  exists: true,
                  sortable: true,
                },
                cardinality100: {
                  kind: "keyword",
                  bindings: [{ version: 1, jsonPointer: "/cardinality100" }],
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

      for (let i = 0; i < 4; i += 1) {
        const appendRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              time: `2026-04-07T10:00:0${i}.000Z`,
              cardinality100: `bucket-${i % 2}`,
              randomString: `message ${i} ${"x".repeat(200)}`,
            }),
          })
        );
        expect(appendRes.status).toBe(204);
      }

      const actions = await waitForActionKinds(app, stream, ["secondary_compaction_build"]);
      const action = actions.find((row) => row.action_kind === "secondary_compaction_build" && row.status === "succeeded");
      expect(action).toBeTruthy();
      expectActionShape(action);
      const detail = JSON.parse(action!.detail_json);
      expectJobRssDetail(detail);
      expect(typeof detail.retired_gc_ms).toBe("number");
      expect(detail.retired_gc_ms).toBeLessThan(1000);
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog fresh streams build companion coverage through split companion jobs", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-async-index-evlog-shared-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 2_048,
      segmentTargetRows: 1,
      searchCompanionBuildBatchSegments: 1,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      indexCheckIntervalMs: 10,
      uploadConcurrency: 2,
      indexL0SpanSegments: 1,
      segmentCacheMaxBytes: 32 * 1024 * 1024,
      segmentFooterCacheEntries: 64,
    });
    const app = createApp(cfg, new MockR2Store());
    const baseUrl = "http://127.0.0.1:8787";
    try {
      const summary = await runEvlogIngester(
        ["--url", baseUrl, "--stream", "evlog-telemetry", "--batch-size", "10", "--max-batches", "1", "--reset"],
        { fetchImpl: makeDemoFetch(app, baseUrl) }
      );
      expect(summary.appendedEvents).toBe(10);

      await waitForCondition(
        app,
        "evlog-telemetry",
        () => {
          const actions = app.deps.db.listAsyncIndexActions("evlog-telemetry", 200);
          const companionRows = app.deps.db.listSearchSegmentCompanions("evlog-telemetry");
          const hasSecondary = actions.some((row) => row.action_kind === "secondary_l0_build" && row.status === "succeeded");
          const hasCompanionMerge = actions.some((row) => row.action_kind === "companion_merge_build" && row.status === "succeeded");
          return hasSecondary && hasCompanionMerge && companionRows.length > 0;
        },
        60_000
      );

      const actions = app.deps.db.listAsyncIndexActions("evlog-telemetry", 200);
      const detailPayloads = actions
        .filter((row) => row.status === "succeeded" && row.action_kind === "secondary_l0_build")
        .map((row) => {
          try {
            return JSON.parse(row.detail_json);
          } catch {
            return {};
          }
        });
      const piggybackedCompanionSections = detailPayloads.flatMap((detail) =>
        Array.isArray(detail.piggyback_companion_section_kinds) ? detail.piggyback_companion_section_kinds : []
      );
      const manifestPublishedFlags = detailPayloads.map((detail) => detail.manifest_published).filter((value) => typeof value === "boolean");
      const deferredFlags = detailPayloads.map((detail) => detail.piggyback_companion_deferred).filter((value) => typeof value === "boolean");
      const companionBuilds = actions.filter((row) => row.status === "succeeded" && row.action_kind === "companion_build");
      const companionMerges = actions.filter((row) => row.status === "succeeded" && row.action_kind === "companion_merge_build");

      expect(app.deps.db.listSearchSegmentCompanions("evlog-telemetry").length).toBeGreaterThan(0);
      expect(piggybackedCompanionSections).toHaveLength(0);
      expect(deferredFlags).toContain(false);
      expect(manifestPublishedFlags).toContain(false);
      expect(companionBuilds.length).toBeGreaterThan(0);
      expect(companionMerges.length).toBeGreaterThan(0);
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  test("evlog exact batch can succeed before slow standalone companion upload finalizes", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-async-index-evlog-deferred-companion-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 2_048,
      segmentTargetRows: 1,
      searchCompanionBuildBatchSegments: 1,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      indexCheckIntervalMs: 10,
      uploadConcurrency: 2,
      indexL0SpanSegments: 1,
      segmentCacheMaxBytes: 32 * 1024 * 1024,
      segmentFooterCacheEntries: 64,
    });
    const app = createApp(cfg, new SlowCompanionPutR2Store(500));
    const baseUrl = "http://127.0.0.1:8787";
    try {
      const summary = await runEvlogIngester(
        ["--url", baseUrl, "--stream", "evlog-deferred-companion", "--batch-size", "1", "--max-batches", "1", "--reset"],
        { fetchImpl: makeDemoFetch(app, baseUrl) }
      );
      expect(summary.appendedEvents).toBe(1);

      await waitForCondition(
        app,
        "evlog-deferred-companion",
        () =>
          app.deps.db
            .listAsyncIndexActions("evlog-deferred-companion", 200)
            .some(
              (row) =>
                row.action_kind === "secondary_l0_build" && row.status === "succeeded"
            ),
        30_000
      );

      const ownerAction = app.deps.db
        .listAsyncIndexActions("evlog-deferred-companion", 200)
        .find((row) => row.action_kind === "secondary_l0_build" && row.status === "succeeded");
      expect(ownerAction).toBeTruthy();
      const ownerDetail = JSON.parse(ownerAction!.detail_json);
      expectJobRssDetail(ownerDetail);
      expect(ownerDetail.piggyback_companion_deferred).toBe(false);
      expect(app.deps.db.listSearchSegmentCompanions("evlog-deferred-companion")).toHaveLength(0);

      await waitForCondition(
        app,
        "evlog-deferred-companion",
        () => app.deps.db.listSearchSegmentCompanions("evlog-deferred-companion").length > 0,
        30_000
      );
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
