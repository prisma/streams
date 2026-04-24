import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";

const STREAM = "searchable";

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

const SEARCH_SCHEMA = {
  schema: {
    type: "object",
    additionalProperties: true,
  },
  search: {
    primaryTimestampField: "eventTime",
    aliases: {
      req: "requestId",
    },
    defaultFields: [
      { field: "message", boost: 2 },
      { field: "why", boost: 1.5 },
    ],
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
        sortable: true,
      },
      status: {
        kind: "integer",
        bindings: [{ version: 1, jsonPointer: "/status" }],
        exact: true,
        column: true,
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
      },
      requestId: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/requestId" }],
        exact: true,
        prefix: true,
        exists: true,
        sortable: true,
      },
      region: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/region" }],
        exact: true,
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
      why: {
        kind: "text",
        bindings: [{ version: 1, jsonPointer: "/why" }],
        analyzer: "unicode_word_v1",
        exists: true,
        positions: true,
      },
    },
  },
};

async function waitForSearchFamilies(app: ReturnType<typeof createApp>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const srow = app.deps.db.getStream(STREAM);
    const companionPlan = app.deps.db.getSearchCompanionPlan(STREAM);
    const companionSegments = app.deps.db.listSearchSegmentCompanions(STREAM);
    const publishedSegmentCount =
      srow && srow.uploaded_through >= 0n
        ? ((app.deps.db.findSegmentForOffset(STREAM, srow.uploaded_through)?.segment_index ?? -1) + 1)
        : 0;
    if (
      srow &&
      publishedSegmentCount > 0 &&
      srow.uploaded_through >= srow.sealed_through &&
      companionPlan &&
      companionSegments.length >= publishedSegmentCount
    ) {
      return;
    }
    app.deps.indexer?.enqueue(STREAM);
    await sleep(50);
  }
  throw new Error("timeout waiting for search families");
}

async function waitForUploadedWithoutCompanions(
  app: ReturnType<typeof createApp>,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const srow = app.deps.db.getStream(STREAM);
    if (srow && srow.uploaded_through >= 0n) {
      app.deps.db.deleteSearchSegmentCompanions(STREAM);
      if (app.deps.db.listSearchSegmentCompanions(STREAM).length === 0) return;
    }
    await sleep(50);
  }
  throw new Error("timeout waiting for uploaded uncompanioned prefix");
}

describe("_search http", () => {
  test("processes explicitly queued search companion work before the next periodic sweep", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-search-http-enqueue-wake-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 200,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      indexL0SpanSegments: 2,
      indexCheckIntervalMs: 60_000,
      segmentCacheMaxBytes: 0,
      segmentFooterCacheEntries: 0,
      searchWalOverlayQuietPeriodMs: 0,
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
          body: JSON.stringify(SEARCH_SCHEMA),
        })
      );
      expect(res.status).toBe(200);

      const events = Array.from({ length: 24 }, (_, index) => ({
        eventTime: new Date(Date.UTC(2026, 2, 25, 10, 0, index)).toISOString(),
        service: index % 2 === 0 ? "billing-api" : "identity",
        status: index % 5 === 0 ? 503 : 200,
        duration: index + 1,
        requestId: `req_${index}`,
        region: index % 2 === 0 ? "ap-southeast-1" : "eu-west-1",
        message: index % 2 === 0 ? "queued companion wake match" : "other event",
        why: "background indexing",
      }));

      res = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(events),
        })
      );
      expect([201, 204]).toContain(res.status);

      await waitForSearchFamilies(app, 5_000);

      res = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            q: 'region:"ap-southeast-1" message:"queued companion wake"',
            size: 5,
            sort: ["offset:asc"],
          }),
        })
      );
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.coverage.indexed_segments).toBeGreaterThan(0);
      expect(body.hits.length).toBeGreaterThan(0);
    } finally {
      app.close();
      rmSync(root, { force: true, recursive: true });
    }
  });

  test(
    "supports exact, range, prefix, bare text, phrase, and search_after pagination",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-http-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 200,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
        searchWalOverlayQuietPeriodMs: 0,
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
            body: JSON.stringify(SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        const events = [
          {
            eventTime: "2026-03-25T10:15:23.123Z",
            service: "billing-api",
            status: 402,
            duration: 1834,
            requestId: "req_1",
            region: "ap-southeast-1",
            message: "card declined",
            why: "issuer reported insufficient funds",
          },
          {
            eventTime: "2026-03-25T10:16:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2400,
            requestId: "req_2",
            region: "us-east-1",
            message: "payment retry failed",
            why: "downstream timeout",
          },
          {
            eventTime: "2026-03-25T10:17:23.123Z",
            service: "billing-worker",
            status: 200,
            duration: 100,
            requestId: "job_1",
            region: "ap-southeast-1",
            message: "retry scheduled",
            why: "background job",
          },
          {
            eventTime: "2026-03-25T10:18:23.123Z",
            service: "billing-api",
            status: 402,
            duration: 2100,
            requestId: "req_3",
            region: "eu-west-1",
            message: "card declined again",
            why: "issuer declined card",
          },
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

        await waitForSearchFamilies(app);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "service:billing-api status:>=500",
              sort: ["eventTime:desc", "offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        expect(Number(res.headers.get("search-candidate-doc-ids"))).toBeGreaterThan(0);
        expect(Number(res.headers.get("search-decoded-records"))).toBeGreaterThan(0);
        expect(Number(res.headers.get("search-segment-payload-bytes-fetched"))).toBeGreaterThan(0);
        let body = await res.json();
        expect(body.total).toEqual({ value: 1, relation: "eq" });
        expect(body.coverage.index_families_used).toEqual(expect.arrayContaining(["col"]));
        expect(body.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
        expect(body.coverage.candidate_doc_ids).toBeGreaterThan(0);
        expect(body.coverage.decoded_records).toBeGreaterThan(0);
        expect(body.coverage.json_parse_time_ms).toEqual(expect.any(Number));
        expect(body.coverage.segment_payload_bytes_fetched).toBeGreaterThan(0);
        expect(body.coverage.sort_time_ms).toEqual(expect.any(Number));
        expect(body.coverage.peak_hits_held).toBeGreaterThan(0);
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_2");

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "region:ap-southeast-1",
              sort: ["eventTime:desc", "offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.total).toEqual({ value: 2, relation: "eq" });
        expect(body.coverage.index_families_used).toEqual(expect.not.arrayContaining(["fts"]));
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["job_1", "req_1"]);

        res = await app.fetch(
          new Request(
            `http://local/v1/stream/${encodeURIComponent(STREAM)}/_search?q=${encodeURIComponent("req:req_*")}&size=10&sort=eventTime:desc,offset:desc`,
            { method: "GET" }
          )
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.total.value).toBe(3);
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_3", "req_2", "req_1"]);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ q: "timeout" }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_2");
        expect(body.hits[0].score).toBeGreaterThan(0);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ q: 'why:"issuer declined"' }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_3");

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "service:billing-api",
              size: 1,
              sort: ["eventTime:desc", "offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_3");
        expect(body.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
        expect(body.coverage.index_families_used).toEqual(expect.not.arrayContaining(["col"]));
        expect(body.next_search_after).not.toBeNull();

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "service:billing-api",
              size: 1,
              sort: ["eventTime:desc", "offset:desc"],
              search_after: body.next_search_after,
            }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_2");
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "returns timed-out partial search responses with metrics headers and clamps timeout_ms to 3s",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-timeout-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 200,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
        searchWalOverlayQuietPeriodMs: 0,
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
            body: JSON.stringify(SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        for (const event of [
          {
            eventTime: "2026-03-25T10:15:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2400,
            requestId: "req_1",
            region: "us-east-1",
            message: "payment retry failed",
            why: "downstream timeout",
          },
          {
            eventTime: "2026-03-25T10:16:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2500,
            requestId: "req_2",
            region: "us-east-1",
            message: "payment retry failed again",
            why: "downstream timeout",
          },
        ]) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(event),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSearchFamilies(app);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              size: 10,
              sort: ["offset:desc"],
              timeout_ms: 10_000,
            }),
          })
        );
        expect(res.status).toBe(200);
        expect(res.headers.get("search-timed-out")).toBe("false");
        expect(res.headers.get("search-timeout-ms")).toBe("3000");
        expect(res.headers.get("search-took-ms")).toEqual(expect.any(String));
        expect(res.headers.get("search-indexed-segments")).toEqual(expect.any(String));

        let body = await res.json();
        expect(body.timed_out).toBe(false);
        expect(body.timeout_ms).toBe(3000);
        expect(body.total).toEqual({ value: 2, relation: "eq" });

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              size: 10,
              sort: ["offset:desc"],
              timeout_ms: 0,
            }),
          })
        );
        expect(res.status).toBe(408);
        expect(res.headers.get("search-timed-out")).toBe("true");
        expect(res.headers.get("search-timeout-ms")).toBe("0");
        expect(res.headers.get("search-total-relation")).toBe("gte");
        expect(res.headers.get("search-coverage-complete")).toBe("false");
        expect(res.headers.get("search-indexed-segments")).toBe("0");
        expect(res.headers.get("search-scanned-tail-docs")).toBe("0");

        body = await res.json();
        expect(body.timed_out).toBe(true);
        expect(body.timeout_ms).toBe(0);
        expect(body.coverage.complete).toBe(false);
        expect(body.total.relation).toBe("gte");
        expect(body.hits).toEqual([]);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              track_total_hits: true,
            }),
          })
        );
        expect(res.status).toBe(400);
        body = await res.json();
        expect(body).toEqual({
          error: {
            code: "bad_request",
            message: "track_total_hits is no longer supported",
          },
        });

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search?q=timeout&track_total_hits=true`, {
            method: "GET",
          })
        );
        expect(res.status).toBe(400);
        body = await res.json();
        expect(body).toEqual({
          error: {
            code: "bad_request",
            message: "track_total_hits is no longer supported",
          },
        });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "uses offset-desc search_after for efficient append-order pagination",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-offset-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 120,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
        searchWalOverlayQuietPeriodMs: 0,
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
            body: JSON.stringify(SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 8; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(10 + i).padStart(2, "0")}:23.123Z`,
                service: "billing-api",
                status: 500 + i,
                duration: 100 + i,
                requestId: `req_${i}`,
                message: `event ${i}`,
                why: "all docs match",
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSearchFamilies(app);
        expect(app.deps.db.countSegmentsForStream(STREAM)).toBeGreaterThan(1);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "has:message",
              size: 1,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        let body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_7");
        expect(body.coverage.indexed_segments + body.coverage.scanned_segments + Math.min(body.coverage.scanned_tail_docs, 1)).toBe(1);
        expect(body.next_search_after).not.toBeNull();

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "has:message",
              size: 1,
              sort: ["offset:desc"],
              search_after: body.next_search_after,
            }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_6");
        expect(body.coverage.indexed_segments + body.coverage.scanned_segments + Math.min(body.coverage.scanned_tail_docs, 1)).toBe(1);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "searches the quiet WAL tail once upload and companion work are caught up",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-quiet-tail-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 1_000_000,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
        searchWalOverlayQuietPeriodMs: 0,
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
            body: JSON.stringify(SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        // Keep this test focused on search behavior while companions are not
        // caught up. Enqueued work normally wakes the managers promptly.
        app.deps.indexer?.stop();

        for (const event of [
          {
            eventTime: "2026-03-25T10:15:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2400,
            requestId: "req_1",
            region: "us-east-1",
            message: "payment retry failed",
            why: "downstream timeout",
          },
          {
            eventTime: "2026-03-25T10:16:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2500,
            requestId: "req_2",
            region: "us-east-1",
            message: "another timeout",
            why: "quiet tail match",
          },
        ]) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(event),
            })
          );
          expect(res.status).toBe(204);
        }

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_2", "req_1"]);
        expect(body.coverage.complete).toBe(true);
        expect(body.coverage.mode).toBe("complete");
        expect(body.coverage.scanned_tail_docs).toBeGreaterThan(0);
        expect(body.coverage.possible_missing_events_upper_bound).toBe(0);
        expect(body.coverage.possible_missing_wal_rows).toBe(0);
        expect(body.total).toEqual({ value: 2, relation: "eq" });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "omits a fresh WAL tail under active ingest even after published coverage catches up",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-active-tail-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 200,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
        searchWalOverlayQuietPeriodMs: 60_000,
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
            body: JSON.stringify(SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        for (const event of [
          {
            eventTime: "2026-03-25T10:15:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2400,
            requestId: "req_1",
            region: "us-east-1",
            message: "uploaded timeout",
            why: "uploaded timeout",
          },
          {
            eventTime: "2026-03-25T10:16:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2500,
            requestId: "req_2",
            region: "us-east-1",
            message: "uploaded timeout two",
            why: "uploaded timeout two",
          },
        ]) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(event),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSearchFamilies(app);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              eventTime: "2026-03-25T10:17:23.123Z",
              service: "billing-api",
              status: 503,
              duration: 2600,
              requestId: "req_tail",
              region: "us-east-1",
              message: "fresh wal timeout",
              why: "fresh wal timeout",
            }),
          })
        );
        expect(res.status).toBe(204);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_2", "req_1"]);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.mode).toBe("published");
        expect(body.coverage.scanned_tail_docs).toBe(0);
        expect(body.coverage.possible_missing_wal_rows).toBeGreaterThan(0);
        expect(body.coverage.oldest_omitted_append_at).toEqual(expect.any(String));
        expect(body.coverage.visible_through_primary_timestamp_max).toEqual(expect.any(String));
        expect(body.total).toEqual({ value: 2, relation: "gte" });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "reports incomplete WAL coverage while returning visible uploaded matches",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-omit-suffix-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 140,
        segmentTargetRows: 1,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 60_000,
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
            body: JSON.stringify(SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        for (const event of [
          {
            eventTime: "2026-03-25T10:15:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2400,
            requestId: "req_1",
            region: "us-east-1",
            message: "segment timeout",
            why: "uploaded suffix match",
          },
          {
            eventTime: "2026-03-25T10:16:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2500,
            requestId: "req_2",
            region: "us-east-1",
            message: "tail timeout",
            why: "uploaded suffix match",
          },
        ]) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(event),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForUploadedWithoutCompanions(app);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              eventTime: "2026-03-25T10:17:23.123Z",
              service: "billing-api",
              status: 503,
              duration: 2600,
              requestId: "req_tail",
              region: "us-east-1",
              message: "fresh wal timeout",
              why: "wal suffix match",
            }),
          })
        );
        expect(res.status).toBe(204);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.length).toBeGreaterThan(0);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.mode).toBe("published");
        expect(body.coverage.indexed_segments + body.coverage.scanned_segments).toBeGreaterThan(0);
        expect(body.coverage.scanned_tail_docs).toBe(0);
        expect(body.coverage.possible_missing_wal_rows).toBeGreaterThan(0);
        expect(body.coverage.possible_missing_events_upper_bound).toBeGreaterThan(0);
        expect(body.total.value).toBeGreaterThan(0);
        expect(["eq", "gte"]).toContain(body.total.relation);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );
});
