import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { runEvlogIngester } from "../experiments/demo/evlog_ingester";

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
      }
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

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
        if (action?.action_kind === "secondary_compaction_build") {
          const detail = JSON.parse(action.detail_json);
          expect(typeof detail.source_prepare_ms).toBe("number");
          expect(typeof detail.worker_build_ms).toBe("number");
          expect(typeof detail.artifact_persist_ms).toBe("number");
          expect(typeof detail.source_fetch_concurrency).toBe("number");
        }
      }
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("evlog unified search build piggybacks companion coverage from exact actions on fresh streams", async () => {
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
        ["--url", baseUrl, "--stream", "evlog-telemetry", "--batch-size", "20", "--max-batches", "2", "--reset"],
        { fetchImpl: makeDemoFetch(app, baseUrl) }
      );
      expect(summary.appendedEvents).toBe(40);

      await waitForCondition(
        app,
        "evlog-telemetry",
        () => {
          const actions = app.deps.db.listAsyncIndexActions("evlog-telemetry", 200);
          const companionRows = app.deps.db.listSearchSegmentCompanions("evlog-telemetry");
          const hasSecondary = actions.some((row) => row.action_kind === "secondary_l0_build" && row.status === "succeeded");
          return hasSecondary && companionRows.length > 0;
        },
        30_000
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
      const companionBuilds = actions.filter((row) => row.status === "succeeded" && row.action_kind === "companion_build");

      expect(app.deps.db.listSearchSegmentCompanions("evlog-telemetry").length).toBeGreaterThan(0);
      expect(piggybackedCompanionSections.length).toBeGreaterThan(0);
      expect(companionBuilds.length).toBe(0);
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
