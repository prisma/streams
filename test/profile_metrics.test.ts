import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createProfileTestApp, fetchJsonApp } from "./profile_test_utils";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForMetricsEvent(app: ReturnType<typeof createProfileTestApp>["app"], timeoutMs = 5_000): Promise<any[]> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await fetchJsonApp(app, "http://local/v1/stream/__stream_metrics__?offset=-1&format=json", { method: "GET" });
    if (Array.isArray(res.body) && res.body.length > 0) return res.body;
    await sleep(50);
  }
  throw new Error("timeout waiting for metrics event");
}

async function waitForMetricsFamilies(
  app: ReturnType<typeof createProfileTestApp>["app"],
  stream: string,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const srow = app.deps.db.getStream(stream);
    const uploadedSegments = app.deps.db.countUploadedSegments(stream);
    const fullySealed =
      !!srow &&
      srow.next_offset > 0n &&
      srow.sealed_through === srow.next_offset - 1n &&
      srow.uploaded_through >= srow.sealed_through &&
      srow.pending_bytes === 0n &&
      srow.pending_rows === 0n;
    const aggState = app.deps.db.getSearchFamilyState(stream, "agg");
    const mblkState = app.deps.db.getSearchFamilyState(stream, "mblk");
    const ready =
      fullySealed &&
      uploadedSegments > 0 &&
      (aggState?.uploaded_through ?? 0) >= uploadedSegments &&
      (mblkState?.uploaded_through ?? 0) >= uploadedSegments &&
      app.deps.db.listSearchFamilySegments(stream, "agg").length >= uploadedSegments &&
      app.deps.db.listSearchFamilySegments(stream, "mblk").length >= uploadedSegments;
    if (ready) return;
    app.deps.indexer?.enqueue(stream);
    await sleep(50);
  }
  throw new Error("timeout waiting for metrics search families");
}

describe("metrics profile", () => {
  test("auto-installs on __stream_metrics__ and emits canonical metrics records", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-metrics-system-"));
    const { app } = createProfileTestApp(root, {
      metricsFlushIntervalMs: 50,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      indexCheckIntervalMs: 10,
      segmentMaxBytes: 512,
      indexL0SpanSegments: 1,
    });
    try {
      const profileRes = await fetchJsonApp(app, "http://local/v1/stream/__stream_metrics__/_profile", { method: "GET" });
      expect(profileRes.status).toBe(200);
      expect(profileRes.body).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "metrics" },
      });

      const schemaRes = await fetchJsonApp(app, "http://local/v1/stream/__stream_metrics__/_schema", { method: "GET" });
      expect(schemaRes.status).toBe(200);
      expect(schemaRes.body?.currentVersion).toBe(1);
      expect(schemaRes.body?.search?.profile).toBe("metrics");
      expect(schemaRes.body?.routingKey).toEqual({ jsonPointer: "/seriesKey", required: true });
      expect(schemaRes.body?.search?.rollups?.metrics).toBeDefined();

      await app.fetch(
        new Request("http://local/v1/stream/metric-source", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );
      await app.fetch(
        new Request("http://local/v1/stream/metric-source", {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: "x",
        })
      );

      const events = await waitForMetricsEvent(app);
      expect(events[0]).toEqual(
        expect.objectContaining({
          apiVersion: "durable.streams/metrics/v1",
          kind: "interval",
          metricKind: "summary",
          temporality: "delta",
          seriesKey: expect.any(String),
          dimensionPairs: expect.any(Array),
          summary: expect.objectContaining({
            count: expect.any(Number),
            sum: expect.any(Number),
          }),
        })
      );

      const detailsRes = await fetchJsonApp(app, "http://local/v1/stream/__stream_metrics__/_details", { method: "GET" });
      expect(detailsRes.status).toBe(200);
      expect(detailsRes.body?.stream?.profile).toBe("metrics");
      expect(detailsRes.body?.schema?.search?.profile).toBe("metrics");
      expect(detailsRes.body?.index_status?.search_families.map((family: any) => family.family).sort()).toEqual(["agg", "col", "fts", "mblk"]);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("installs on user streams and uses agg plus mblk for metrics aggregates", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-profile-metrics-agg-"));
    const { app } = createProfileTestApp(root, {
      segmentMaxBytes: 350,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      uploadConcurrency: 2,
      indexL0SpanSegments: 1,
      indexCheckIntervalMs: 10,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      metricsFlushIntervalMs: 0,
    });
    try {
      const stream = "metrics-user";
      await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );

      const profileRes = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "metrics" },
        }),
      });
      expect(profileRes.status).toBe(200);
      expect(profileRes.body?.profile).toEqual({ kind: "metrics" });

      const intervals = [
        {
          kind: "interval",
          metric: "app.requests",
          unit: "count",
          windowStart: Date.parse("2026-03-25T10:00:00.000Z"),
          windowEnd: Date.parse("2026-03-25T10:01:00.000Z"),
          intervalMs: 60_000,
          instance: "api-1",
          stream: "checkout",
          tags: { env: "prod", region: "use1" },
          count: 10,
          sum: 100,
          min: 5,
          max: 20,
          avg: 10,
          p50: 8,
          p95: 16,
          p99: 16,
          buckets: { "8": 4, "16": 6 },
        },
        {
          kind: "interval",
          metric: "app.requests",
          unit: "count",
          windowStart: Date.parse("2026-03-25T10:01:00.000Z"),
          windowEnd: Date.parse("2026-03-25T10:02:00.000Z"),
          intervalMs: 60_000,
          instance: "api-1",
          stream: "checkout",
          tags: { env: "prod", region: "use1" },
          count: 8,
          sum: 120,
          min: 10,
          max: 25,
          avg: 15,
          p50: 16,
          p95: 16,
          p99: 32,
          buckets: { "16": 6, "32": 2 },
        },
        {
          kind: "interval",
          metric: "app.requests",
          unit: "count",
          windowStart: Date.parse("2026-03-25T10:02:00.000Z"),
          windowEnd: Date.parse("2026-03-25T10:03:00.000Z"),
          intervalMs: 60_000,
          instance: "api-1",
          stream: "checkout",
          tags: { env: "prod", region: "use1" },
          count: 4,
          sum: 44,
          min: 9,
          max: 13,
          avg: 11,
          p50: 8,
          p95: 16,
          p99: 16,
          buckets: { "8": 1, "16": 3 },
        },
      ];

      for (const interval of intervals) {
        const appendRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(interval),
          })
        );
        expect(appendRes.status).toBe(204);
      }

      await waitForMetricsFamilies(app, stream);

      const indexStatusRes = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_index_status`, { method: "GET" });
      expect(indexStatusRes.status).toBe(200);
      expect(indexStatusRes.body?.search_families.map((family: any) => family.family).sort()).toEqual(["agg", "col", "fts", "mblk"]);
      expect(indexStatusRes.body?.search_families.find((family: any) => family.family === "mblk")?.fully_indexed_uploaded_segments).toBe(true);

      const alignedRes = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_aggregate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          rollup: "metrics",
          from: "2026-03-25T10:00:00.000Z",
          to: "2026-03-25T10:03:00.000Z",
          interval: "1m",
          q: "metric:app.requests stream:checkout",
          group_by: ["metric", "stream"],
        }),
      });
      expect(alignedRes.status).toBe(200);
      expect(alignedRes.body?.coverage?.used_rollups).toBe(true);
      expect(alignedRes.body?.coverage?.index_families_used).toEqual(["agg"]);
      expect(alignedRes.body?.coverage?.scanned_segments).toBe(0);
      expect(alignedRes.body?.buckets).toHaveLength(3);
      expect(alignedRes.body?.buckets[0]?.groups?.[0]?.measures?.value).toEqual(
        expect.objectContaining({
          count: 10,
          sum: 100,
          min: 5,
          max: 20,
          avg: 10,
        })
      );

      const mblkRes = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(stream)}/_aggregate`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          rollup: "metrics",
          from: "2026-03-25T10:00:00.000Z",
          to: "2026-03-25T10:03:00.000Z",
          interval: "1m",
          q: "metric:app.requests avg:>=10",
          group_by: ["metric", "stream"],
        }),
      });
      expect(mblkRes.status).toBe(200);
      expect(mblkRes.body?.coverage?.used_rollups).toBe(false);
      expect(mblkRes.body?.coverage?.index_families_used).toEqual(["mblk"]);
      expect(mblkRes.body?.coverage?.indexed_segments).toBeGreaterThan(0);
      expect(mblkRes.body?.coverage?.scanned_segments).toBe(0);
      expect(mblkRes.body?.buckets).toHaveLength(3);
      expect(mblkRes.body?.buckets[1]?.groups?.[0]?.measures?.value).toEqual(
        expect.objectContaining({
          count: 8,
          sum: 120,
          min: 10,
          max: 25,
          avg: 15,
        })
      );
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
