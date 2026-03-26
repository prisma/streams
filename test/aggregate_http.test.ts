import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";

const STREAM = "aggregates";

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

const AGG_SCHEMA = {
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
        sortable: true,
      },
      duration: {
        kind: "float",
        bindings: [{ version: 1, jsonPointer: "/duration" }],
        exact: true,
        column: true,
        exists: true,
        sortable: true,
        aggregatable: true,
      },
      message: {
        kind: "text",
        bindings: [{ version: 1, jsonPointer: "/message" }],
        analyzer: "unicode_word_v1",
        exists: true,
        positions: true,
      },
    },
    rollups: {
      requests: {
        dimensions: ["service"],
        intervals: ["1m"],
        measures: {
          requests: { kind: "count" },
          latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
        },
      },
      metrics: {
        dimensions: ["service"],
        intervals: ["1m"],
        measures: {
          latency_parts: {
            kind: "summary_parts",
            countJsonPointer: "/metric/count",
            sumJsonPointer: "/metric/sum",
            minJsonPointer: "/metric/min",
            maxJsonPointer: "/metric/max",
            histogramJsonPointer: "/metric/histogram",
          },
        },
      },
    },
  },
};

async function waitForAggFamily(app: ReturnType<typeof createApp>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const srow = app.deps.db.getStream(STREAM);
    const aggState = app.deps.db.getSearchFamilyState(STREAM, "agg");
    const aggSegments = app.deps.db.listSearchFamilySegments(STREAM, "agg");
    if (
      srow &&
      srow.uploaded_through >= srow.sealed_through &&
      aggState &&
      aggState.uploaded_through >= srow.uploaded_segment_count &&
      aggSegments.length >= srow.uploaded_segment_count
    ) {
      return;
    }
    app.deps.indexer?.enqueue(STREAM);
    await sleep(50);
  }
  throw new Error("timeout waiting for agg family");
}

describe("_aggregate http", () => {
  test(
    "uses rollups for aligned windows and raw scans for partial edges",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-aggregate-http-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 220,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const app = createApp(cfg);
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
            body: JSON.stringify(AGG_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        const events = [
          { eventTime: "2026-03-25T10:00:40.000Z", service: "API", duration: 120, message: "checkout accepted" },
          { eventTime: "2026-03-25T10:01:10.000Z", service: "api", duration: 150, message: "checkout retry timeout" },
          { eventTime: "2026-03-25T10:01:50.000Z", service: "api", duration: 200, message: "checkout recovered" },
          { eventTime: "2026-03-25T10:02:10.000Z", service: "api", duration: 80, message: "checkout settled" },
        ];

        for (const event of events) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(event),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForAggFamily(app);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_index_status`, {
            method: "GET",
          })
        );
        expect(res.status).toBe(200);
        let indexStatus = await res.json();
        expect(indexStatus.search_families.map((family: any) => family.family).sort()).toEqual(["agg", "col", "fts"]);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_aggregate`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              rollup: "requests",
              from: "2026-03-25T10:00:30.000Z",
              to: "2026-03-25T10:02:30.000Z",
              interval: "1m",
              q: "service:api",
              group_by: ["service"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.coverage.used_rollups).toBe(true);
        expect(body.coverage.index_families_used).toEqual(["agg"]);
        expect(body.buckets).toHaveLength(3);
        expect(body.buckets[0]).toEqual({
          start: "2026-03-25T10:00:00.000Z",
          end: "2026-03-25T10:01:00.000Z",
          groups: [
            {
              key: { service: "api" },
              measures: {
                latency: expect.objectContaining({
                  count: 1,
                  sum: 120,
                  min: 120,
                  max: 120,
                  avg: 120,
                }),
                requests: { count: 1 },
              },
            },
          ],
        });
        expect(body.buckets[1].groups[0].measures.requests).toEqual({ count: 2 });
        expect(body.buckets[1].groups[0].measures.latency).toEqual(
          expect.objectContaining({
            count: 2,
            sum: 350,
            min: 150,
            max: 200,
            avg: 175,
          })
        );
        expect(body.buckets[2].groups[0].measures.requests).toEqual({ count: 1 });

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_aggregate`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              rollup: "requests",
              from: "2026-03-25T10:01:00.000Z",
              to: "2026-03-25T10:02:00.000Z",
              interval: "1m",
              q: "timeout",
              group_by: ["service"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const fallbackBody = await res.json();
        expect(fallbackBody.coverage.used_rollups).toBe(false);
        expect(fallbackBody.coverage.scanned_segments).toBeGreaterThan(0);
        expect(fallbackBody.buckets).toHaveLength(1);
        expect(fallbackBody.buckets[0].groups[0].measures.requests).toEqual({ count: 1 });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "supports summary_parts rollups",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-aggregate-parts-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 220,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const app = createApp(cfg);
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
            body: JSON.stringify(AGG_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        const metricRows = [
          {
            eventTime: "2026-03-25T11:00:10.000Z",
            service: "api",
            duration: 1,
            message: "metric sample",
            metric: { count: 2, sum: 30, min: 10, max: 20, histogram: { "16": 2 } },
          },
          {
            eventTime: "2026-03-25T11:00:20.000Z",
            service: "api",
            duration: 1,
            message: "metric sample",
            metric: { count: 1, sum: 40, min: 40, max: 40, histogram: { "32": 1 } },
          },
        ];

        for (const row of metricRows) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(row),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForAggFamily(app);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_details`, {
            method: "GET",
          })
        );
        expect(res.status).toBe(200);
        const details = await res.json();
        expect(details.index_status.search_families.map((family: any) => family.family).sort()).toEqual(["agg", "col", "fts"]);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_aggregate`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              rollup: "metrics",
              from: "2026-03-25T11:00:00.000Z",
              to: "2026-03-25T11:01:00.000Z",
              interval: "1m",
              group_by: ["service"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.coverage.used_rollups).toBe(true);
        expect(body.buckets).toHaveLength(1);
        expect(body.buckets[0].groups).toEqual([
          {
            key: { service: "api" },
            measures: {
              latency_parts: expect.objectContaining({
                count: 3,
                sum: 70,
                min: 10,
                max: 40,
              }),
            },
          },
        ]);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );
});
