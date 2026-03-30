import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
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
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
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

        const searchRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'why:"retry later"',
              sort: ["offset:desc"],
              size: 10,
              track_total_hits: false,
            }),
          })
        );
        expect(searchRes.status).toBe(200);
        const searchBody = await searchRes.json();
        expect(searchBody.hits.length).toBeGreaterThan(0);
        expect(searchBody.coverage.index_families_used).toEqual([]);
        expect(searchBody.coverage.scanned_segments).toBeGreaterThan(0);

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
              track_total_hits: false,
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
});
