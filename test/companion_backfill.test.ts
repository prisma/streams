import { describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { buildDesiredSearchCompanionPlan } from "../src/search/companion_plan";
import { decodeBundledSegmentCompanionTocResult } from "../src/search/companion_format";
import { streamHash16Hex } from "../src/util/stream_paths";

const STREAM = "backfill";

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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const SCHEMA_V1 = {
  schema: {
    type: "object",
    additionalProperties: true,
  },
  search: {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: {
        kind: "date",
        bindings: [{ version: 1, jsonPointer: "/eventTime" }],
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
};

const SEARCH_V2 = {
  primaryTimestampField: "eventTime",
  fields: {
    eventTime: {
      kind: "date",
      bindings: [{ version: 1, jsonPointer: "/eventTime" }],
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
    status: {
      kind: "integer",
      bindings: [{ version: 1, jsonPointer: "/status" }],
      exact: true,
      column: true,
      exists: true,
      sortable: true,
    },
    why: {
      kind: "text",
      bindings: [{ version: 1, jsonPointer: "/why" }],
      analyzer: "unicode_word_v1",
      exists: true,
      positions: true,
    },
  },
};

async function waitForCompanionGeneration(
  app: ReturnType<typeof createApp>,
  generation: number,
  opts: { manualKick?: boolean } = {},
  timeoutMs = 10_000
): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const uploadedSegments = app.deps.db.countUploadedSegments(STREAM);
    const plan = app.deps.db.getSearchCompanionPlan(STREAM);
    const companions = app.deps.db.listSearchSegmentCompanions(STREAM).filter((row) => row.plan_generation === generation);
    const row = app.deps.db.getStream(STREAM);
    const fullyUploaded = !!row && row.uploaded_through >= row.sealed_through && uploadedSegments > 0;
    if (fullyUploaded && plan?.generation === generation && companions.length >= uploadedSegments) {
      return uploadedSegments;
    }
    if (opts.manualKick !== false) app.deps.indexer?.enqueue(STREAM);
    await sleep(50);
  }
  throw new Error(`timeout waiting for companion generation ${generation}`);
}

async function waitForSegment(
  app: ReturnType<typeof createApp>,
  segmentIndex: number,
  timeoutMs = 10_000
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const seg = app.deps.db.getSegmentByIndex(STREAM, segmentIndex);
    if (seg) return seg;
    await sleep(25);
  }
  throw new Error(`timeout waiting for segment ${segmentIndex}`);
}

async function waitForMappedCompanionCount(
  app: ReturnType<typeof createApp>,
  minimum: number,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const mapped = (app.deps.indexer as any).companionIndex.getMemoryStats().mappedFileEntries;
    if (mapped >= minimum) return;
    await sleep(25);
  }
  throw new Error(`timeout waiting for mapped companion count >= ${minimum}`);
}

async function waitForSectionCacheEntries(
  app: ReturnType<typeof createApp>,
  minimum: number,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const entries = (app.deps.indexer as any).companionIndex.getMemoryStats().sectionCacheEntries;
    if (entries >= minimum) return;
    await sleep(25);
  }
  throw new Error(`timeout waiting for section cache entries >= ${minimum}`);
}

describe("bundled companions and backfill", () => {
  test(
    "stores one .cix per sealed segment and backfills existing streams after search config changes",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-backfill-"));
      const store = new MockR2Store();
      const buildCfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      let app = createApp(buildCfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(SCHEMA_V1),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 6; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:0${i}:00.000Z`,
                service: i % 2 === 0 ? "api" : "worker",
                status: 500 + i,
                why: i % 2 === 0 ? "retry later" : "issuer timeout",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        const uploadedSegments = await waitForCompanionGeneration(app, 1);
        const streamHash = streamHash16Hex(STREAM);
        const segmentKeys = (await store.list(`streams/${streamHash}/segments/`)).filter((key) => key.endsWith(".bin"));
        const companionKeys = (await store.list(`streams/${streamHash}/segments/`)).filter((key) => key.endsWith(".cix"));
        expect(segmentKeys.length).toBe(uploadedSegments);
        expect(companionKeys.length).toBe(uploadedSegments);
        expect((await store.list(`streams/${streamHash}/col/segments/`)).length).toBe(0);
        expect((await store.list(`streams/${streamHash}/fts/segments/`)).length).toBe(0);
        expect((await store.list(`streams/${streamHash}/agg/segments/`)).length).toBe(0);
        expect((await store.list(`streams/${streamHash}/mblk/segments/`)).length).toBe(0);
      } finally {
        app.close();
      }

      const pausedCfg = makeConfig(root, {
        ...buildCfg,
        indexCheckIntervalMs: 60_000,
        searchCompanionBuildBatchSegments: 1,
      });
      app = createApp(pausedCfg, store);
      try {
        const updateRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ search: SEARCH_V2 }),
          })
        );
        expect(updateRes.status).toBe(200);

        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.index_status.desired_index_plan_generation).toBe(2);
        expect(details.index_status.bundled_companions.object_count).toBe(0);
        expect(details.index_status.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ family: "col", fully_indexed_uploaded_segments: false }),
            expect.objectContaining({ family: "fts", fully_indexed_uploaded_segments: false }),
          ])
        );

        app.deps.indexer?.enqueue(STREAM);
        await (app.deps.indexer as any).companionIndex.tick();
        const afterOneTickRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, { method: "GET" })
        );
        expect(afterOneTickRes.status).toBe(200);
        const afterOneTick = await afterOneTickRes.json();
        expect(afterOneTick.index_status.bundled_companions.object_count).toBe(1);
        expect(afterOneTick.index_status.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ family: "col", covered_segment_count: 1 }),
            expect.objectContaining({ family: "fts", covered_segment_count: 1 }),
          ])
        );

        await (app.deps.indexer as any).companionIndex.tick();
        const afterSecondTickRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, { method: "GET" })
        );
        expect(afterSecondTickRes.status).toBe(200);
        const afterSecondTick = await afterSecondTickRes.json();
        expect(afterSecondTick.index_status.bundled_companions.object_count).toBe(2);
        expect(afterSecondTick.index_status.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ family: "col", covered_segment_count: 2 }),
            expect.objectContaining({ family: "fts", covered_segment_count: 2 }),
          ])
        );

        const searchRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'why:"retry later"',
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(searchRes.status).toBe(200);
        const searchBody = await searchRes.json();
        expect(searchBody.hits.length).toBeGreaterThan(0);
        expect(searchBody.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));

        const filterRes = await app.fetch(
          new Request(
            `http://local/v1/stream/${encodeURIComponent(STREAM)}?offset=-1&format=json&filter=${encodeURIComponent("status:>=505")}`,
            { method: "GET" }
          )
        );
        expect(filterRes.status).toBe(200);
        const filterBody = await filterRes.json();
        expect(filterBody).toHaveLength(1);
        expect(filterBody[0].status).toBe(505);
      } finally {
        app.close();
      }

      app = createApp(buildCfg, store);
      try {
        const uploadedSegments = await waitForCompanionGeneration(app, 2, { manualKick: false });
        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.index_status.desired_index_plan_generation).toBe(2);
        expect(details.index_status.bundled_companions.object_count).toBe(uploadedSegments);
        expect(details.index_status.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ family: "col", fully_indexed_uploaded_segments: true }),
            expect.objectContaining({ family: "fts", fully_indexed_uploaded_segments: true }),
          ])
        );

        const searchRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'why:"retry later"',
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(searchRes.status).toBe(200);
        const searchBody = await searchRes.json();
        expect(searchBody.hits.length).toBeGreaterThan(0);
        expect(searchBody.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "batched companion backfill fills the oldest missing uploaded segments contiguously",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-batch-"));
      const store = new MockR2Store();
      const buildCfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionBuildBatchSegments: 2,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      let app = createApp(buildCfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(SCHEMA_V1),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 6; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:1${i}:00.000Z`,
                service: i % 2 === 0 ? "api" : "worker",
                status: 500 + i,
                why: i % 2 === 0 ? "retry later" : "issuer timeout",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForCompanionGeneration(app, 1);
      } finally {
        app.close();
      }

      const pausedCfg = makeConfig(root, {
        ...buildCfg,
        indexCheckIntervalMs: 60_000,
        searchCompanionBuildBatchSegments: 2,
      });
      app = createApp(pausedCfg, store);
      try {
        const updateRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ search: SEARCH_V2 }),
          })
        );
        expect(updateRes.status).toBe(200);

        await (app.deps.indexer as any).companionIndex.tick();
        const generationTwoRowsAfterOneTick = app.deps.db
          .listSearchSegmentCompanions(STREAM)
          .filter((row) => row.plan_generation === 2)
          .map((row) => row.segment_index)
          .sort((a, b) => a - b);
        expect(generationTwoRowsAfterOneTick).toEqual([0, 1]);

        await (app.deps.indexer as any).companionIndex.tick();
        const generationTwoRowsAfterTwoTicks = app.deps.db
          .listSearchSegmentCompanions(STREAM)
          .filter((row) => row.plan_generation === 2)
          .map((row) => row.segment_index)
          .sort((a, b) => a - b);
        expect(generationTwoRowsAfterTwoTicks).toEqual([0, 1, 2, 3]);

        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.index_status.bundled_companions.object_count).toBe(4);
        expect(details.index_status.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ family: "col", covered_segment_count: 4 }),
            expect.objectContaining({ family: "fts", covered_segment_count: 4 }),
          ])
        );
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "bundled fts companions handle prototype-like tokens such as constructor and push",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-fts-proto-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionBuildBatchSegments: 2,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      const app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: {
                type: "object",
                additionalProperties: true,
              },
              search: {
                primaryTimestampField: "eventTime",
                fields: {
                  eventTime: {
                    kind: "date",
                    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
                    column: true,
                    exists: true,
                    sortable: true,
                  },
                  title: {
                    kind: "text",
                    bindings: [{ version: 1, jsonPointer: "/title" }],
                    analyzer: "unicode_word_v1",
                    exists: true,
                    positions: true,
                  },
                },
                defaultFields: [{ field: "title", boost: 1 }],
              },
            }),
          })
        );
        expect(res.status).toBe(200);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              eventTime: "2026-03-25T11:00:00.000Z",
              title: "constructor push keeps building",
              pad: "x".repeat(256),
            }),
          })
        );
        expect(res.status).toBe(204);

        const uploadedSegments = await waitForCompanionGeneration(app, 1, { manualKick: false });
        expect(uploadedSegments).toBeGreaterThan(0);

        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.index_status.bundled_companions.object_count).toBe(uploadedSegments);
        expect(details.index_status.search_families).toEqual(
          expect.arrayContaining([expect.objectContaining({ family: "fts", fully_indexed_uploaded_segments: true })])
        );

        const searchRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "constructor",
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(searchRes.status).toBe(200);
        const searchBody = await searchRes.json();
        expect(searchBody.hits).toHaveLength(1);
        expect(searchBody.hits[0]?.source?.title).toBe("constructor push keeps building");
        expect(searchBody.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "query reads decode only the requested companion section",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-section-read-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionBuildBatchSegments: 2,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      const app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: {
                type: "object",
                additionalProperties: true,
              },
              search: {
                primaryTimestampField: "eventTime",
                fields: {
                  eventTime: {
                    kind: "date",
                    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
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
                  message: {
                    kind: "text",
                    bindings: [{ version: 1, jsonPointer: "/message" }],
                    analyzer: "unicode_word_v1",
                    exists: true,
                    positions: true,
                  },
                },
                defaultFields: [{ field: "message", boost: 1 }],
                rollups: {
                  events: {
                    dimensions: ["service"],
                    intervals: ["1m"],
                    measures: {
                      events: { kind: "count" },
                    },
                  },
                },
              },
            }),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 4; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T11:0${i}:00.000Z`,
                service: i % 2 === 0 ? "api" : "worker",
                message: i % 2 === 0 ? "constructor keeps working" : "push retries later",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        const uploadedSegments = await waitForCompanionGeneration(app, 1, { manualKick: false });
        expect(uploadedSegments).toBeGreaterThan(0);

        const row = app.deps.db.getSearchSegmentCompanion(STREAM, 0);
        expect(row).not.toBeNull();
        if (!row) return;

        const bytes = await store.get(row.object_key);
        expect(bytes).not.toBeNull();
        if (!bytes) return;

        const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
        expect(Result.isError(tocRes)).toBeFalse();
        if (Result.isError(tocRes)) return;

        const aggSection = tocRes.value.sections.find((section) => section.kind === "agg");
        expect(aggSection).toBeDefined();
        if (!aggSection) return;

        const corrupted = bytes.slice();
        for (let index = 0; index < Math.min(8, aggSection.length); index++) {
          corrupted[aggSection.offset + index] = 0xff;
        }
        await store.put(row.object_key, corrupted);

        const localCachePath = join(root, "cache/companions", row.object_key);
        rmSync(localCachePath, { force: true });

        store.resetStats();
        const companionIndex = (app.deps.indexer as any).companionIndex;
        const ftsCompanion = await companionIndex.getFtsSegmentCompanion(STREAM, 0);
        expect(ftsCompanion).not.toBeNull();
        expect(ftsCompanion?.getField("message")).not.toBeNull();
        expect(store.stats().gets).toBeLessThanOrEqual(1);

        const secondFtsCompanion = await companionIndex.getFtsSegmentCompanion(STREAM, 0);
        expect(secondFtsCompanion?.getField("message")).not.toBeNull();
        expect(store.stats().gets).toBeLessThanOrEqual(1);

        let aggError: unknown = null;
        let aggCompanion: unknown = null;
        try {
          aggCompanion = await companionIndex.getAggSegmentCompanion(STREAM, 0);
        } catch (error) {
          aggError = error;
        }
        if (aggError == null) {
          expect(aggCompanion).not.toBeNull();
        } else {
          expect(String((aggError as { message?: string } | null)?.message ?? aggError).length).toBeGreaterThan(0);
        }
        expect(store.stats().gets).toBeLessThanOrEqual(1);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "reuses decoded companion sections from the in-process section cache",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-section-cache-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionSectionCacheBytes: 8 * 1024 * 1024,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      const app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: {
                type: "object",
                additionalProperties: true,
              },
              search: SEARCH_V2,
            }),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 4; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:0${i}:00.000Z`,
                service: "api",
                status: 500 + i,
                why: "timeout retry",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForCompanionGeneration(app, 1);

        const companionIndex = (app.deps.indexer as any).companionIndex as {
          getFtsSegmentCompanionWithStats(
            stream: string,
            segmentIndex: number
          ): Promise<{ companion: { getField(name: string): unknown } | null; stats: { sectionGetMs: number; decodeMs: number } }>;
        };

        const first = await companionIndex.getFtsSegmentCompanionWithStats(STREAM, 0);
        expect(first.companion?.getField("why")).not.toBeNull();

        const second = await companionIndex.getFtsSegmentCompanionWithStats(STREAM, 0);
        expect(second.companion?.getField("why")).not.toBeNull();
        expect(second.stats).toEqual({ sectionGetMs: 0, decodeMs: 0 });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "primes freshly published companions into the mapped bundle cache",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-mmap-prime-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionMappedCacheEntries: 8,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      const app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: { type: "object", additionalProperties: true },
              search: SEARCH_V2,
            }),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 4; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T11:0${i}:00.000Z`,
                service: "api",
                status: 500 + i,
                why: "timeout retry",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForCompanionGeneration(app, 1);
        await waitForMappedCompanionCount(app, 1);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "cold restart warming fetches newest companions into mapped and section caches",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-mmap-restart-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionMappedCacheEntries: 8,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      let app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: { type: "object", additionalProperties: true },
              search: SEARCH_V2,
            }),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 4; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T12:0${i}:00.000Z`,
                service: "api",
                status: 500 + i,
                why: "warm after restart",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForCompanionGeneration(app, 1);
        await waitForMappedCompanionCount(app, 1);
      } finally {
        app.close();
      }

      rmSync(join(root, "cache", "companions"), { recursive: true, force: true });
      store.resetStats();

      app = createApp(cfg, store);
      try {
        await waitForMappedCompanionCount(app, 1);
        await waitForSectionCacheEntries(app, 1);
        expect(store.stats().gets).toBeGreaterThan(0);

        const companionIndex = (app.deps.indexer as any).companionIndex as {
          getFtsSegmentCompanionWithStats(
            stream: string,
            segmentIndex: number
          ): Promise<{ companion: { getField(name: string): unknown } | null; stats: { sectionGetMs: number; decodeMs: number } }>;
        };
        const warmRead = await companionIndex.getFtsSegmentCompanionWithStats(STREAM, 0);
        expect(warmRead.companion?.getField("why")).not.toBeNull();
        const hotRead = await companionIndex.getFtsSegmentCompanionWithStats(STREAM, 0);
        expect(hotRead.companion?.getField("why")).not.toBeNull();
        expect(hotRead.stats).toEqual({ sectionGetMs: 0, decodeMs: 0 });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "query reads reuse the persisted local companion cache after restart",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-cache-restart-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        searchCompanionBuildBatchSegments: 2,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      let app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: {
                type: "object",
                additionalProperties: true,
              },
              search: {
                primaryTimestampField: "eventTime",
                fields: {
                  eventTime: {
                    kind: "date",
                    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
                    column: true,
                    exists: true,
                    sortable: true,
                  },
                  message: {
                    kind: "text",
                    bindings: [{ version: 1, jsonPointer: "/message" }],
                    analyzer: "unicode_word_v1",
                    exists: true,
                    positions: true,
                  },
                },
                defaultFields: [{ field: "message", boost: 1 }],
              },
            }),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 4; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T11:0${i}:00.000Z`,
                message: i % 2 === 0 ? "constructor keeps working" : "push retries later",
                pad: "x".repeat(256),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForCompanionGeneration(app, 1, { manualKick: false });
        const row = app.deps.db.getSearchSegmentCompanion(STREAM, 0);
        expect(row).not.toBeNull();
        if (!row) return;

        const localCachePath = join(root, "cache/companions", row.object_key);
        expect(existsSync(localCachePath)).toBeTrue();

        app.close();
        app = createApp(cfg, store);
        store.resetStats();

        const companionIndex = (app.deps.indexer as any).companionIndex;
        const ftsCompanion = await companionIndex.getFtsSegmentCompanion(STREAM, 0);
        expect(ftsCompanion?.getField("message")).not.toBeNull();
        expect(store.stats().gets).toBe(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "bundled companion builds yield to the event loop for large FTS-heavy segments",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-companion-yield-"));
      const store = new MockR2Store();
      const cfg = makeConfig(root, {
        segmentMaxBytes: 256 * 1024,
        blockMaxBytes: 4 * 1024,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 60_000,
        uploadConcurrency: 1,
        indexCheckIntervalMs: 60_000,
        searchCompanionBuildBatchSegments: 1,
        searchCompanionYieldBlocks: 1,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });

      const app = createApp(cfg, store);
      try {
        let res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
          })
        );
        expect([200, 201]).toContain(res.status);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              schema: {
                type: "object",
                additionalProperties: true,
              },
              search: {
                primaryTimestampField: "eventTime",
                fields: {
                  eventTime: {
                    kind: "date",
                    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
                    column: true,
                    exists: true,
                    sortable: true,
                  },
                  title: {
                    kind: "text",
                    bindings: [{ version: 1, jsonPointer: "/title" }],
                    analyzer: "unicode_word_v1",
                    exists: true,
                    positions: true,
                  },
                  message: {
                    kind: "text",
                    bindings: [{ version: 1, jsonPointer: "/message" }],
                    analyzer: "unicode_word_v1",
                    exists: true,
                    positions: true,
                  },
                  body: {
                    kind: "text",
                    bindings: [{ version: 1, jsonPointer: "/body" }],
                    analyzer: "unicode_word_v1",
                    exists: true,
                    positions: true,
                  },
                },
                defaultFields: [
                  { field: "title", boost: 1.5 },
                  { field: "message", boost: 1.25 },
                  { field: "body", boost: 0.9 },
                ],
              },
            }),
          })
        );
        expect(res.status).toBe(200);

        const largeMessage = "message ".repeat(1024);
        const largeBody = "body ".repeat(4096);
        const rows = Array.from({ length: 24 }, (_, i) => ({
          eventTime: `2026-03-25T12:${String(i).padStart(2, "0")}:00.000Z`,
          title: `Large issue ${i}`,
          message: `${largeMessage}${i}`,
          body: `${largeBody}${i}`,
        }));
        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(rows),
          })
        );
        expect(res.status).toBe(204);

        const segment = await waitForSegment(app, 0);
        const registryRes = app.deps.registry.getRegistryResult(STREAM);
        expect(Result.isError(registryRes)).toBeFalse();
        const registry = registryRes.value;
        const plan = buildDesiredSearchCompanionPlan(registry);
        const companionIndex = (app.deps.indexer as any).companionIndex;

        let timerFiredAt = 0;
        const startedAt = Date.now();
        setTimeout(() => {
          timerFiredAt = Date.now() - startedAt;
        }, 0);

        const buildRes = await companionIndex.buildBundledCompanionResult(registry, plan, 1, segment);
        const finishedAt = Date.now() - startedAt;

        expect(Result.isError(buildRes)).toBeFalse();
        expect(timerFiredAt).toBeGreaterThan(0);
        expect(timerFiredAt).toBeLessThan(finishedAt);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );
});
