import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import type {
  GetFileResult,
  GetOptions,
  ObjectStore,
  PutFileNoEtagOptions,
  PutFileOptions,
  PutNoEtagOptions,
  PutOptions,
  PutResult,
} from "../src/objectstore/interface";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { DSB3_HEADER_BYTES, parseFooter } from "../src/segment/format";
import { segmentObjectKey, streamHash16Hex } from "../src/util/stream_paths";

const STREAM = "searchable";

class CountingObjectStore implements ObjectStore {
  secondaryIndexGets = 0;

  constructor(private readonly inner: ObjectStore) {}

  private recordSecondaryIndexGet(key: string): void {
    if (key.includes("/secondary-index/")) this.secondaryIndexGets += 1;
  }

  put(key: string, data: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    return this.inner.put(key, data, opts);
  }

  putFile?(key: string, path: string, size: number, opts?: PutFileOptions): Promise<PutResult> {
    return this.inner.putFile!(key, path, size, opts);
  }

  putNoEtag?(key: string, data: Uint8Array, opts?: PutNoEtagOptions): Promise<number> {
    return this.inner.putNoEtag!(key, data, opts);
  }

  putFileNoEtag?(key: string, path: string, size: number, opts?: PutFileNoEtagOptions): Promise<number> {
    return this.inner.putFileNoEtag!(key, path, size, opts);
  }

  async get(key: string, opts?: GetOptions): Promise<Uint8Array | null> {
    this.recordSecondaryIndexGet(key);
    return this.inner.get(key, opts);
  }

  async getFile(key: string, path: string, opts?: GetOptions): Promise<GetFileResult | null> {
    this.recordSecondaryIndexGet(key);
    return this.inner.getFile!(key, path, opts);
  }

  head(key: string): Promise<{ etag: string; size: number } | null> {
    return this.inner.head(key);
  }

  delete(key: string): Promise<void> {
    return this.inner.delete(key);
  }

  list(prefix: string): Promise<string[]> {
    return this.inner.list(prefix);
  }
}

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

const EXACT_ONLY_SEARCH_SCHEMA = {
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
        sortable: true,
      },
      service: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/service" }],
        normalizer: "lowercase_v1",
        exact: true,
        sortable: true,
      },
      requestId: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/requestId" }],
        exact: true,
        sortable: true,
      },
    },
  },
};

function publishedSegmentCount(app: ReturnType<typeof createApp>): number {
  const srow = app.deps.db.getStream(STREAM);
  return srow && srow.uploaded_through >= 0n
    ? ((app.deps.db.findSegmentForOffset(STREAM, srow.uploaded_through)?.segment_index ?? -1) + 1)
    : 0;
}

async function waitForSearchFamilies(app: ReturnType<typeof createApp>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const srow = app.deps.db.getStream(STREAM);
    const companionPlan = app.deps.db.getSearchCompanionPlan(STREAM);
    const companionSegments = app.deps.db.listSearchSegmentCompanions(STREAM);
    const uploadedSegments = publishedSegmentCount(app);
    if (
      srow &&
      uploadedSegments > 0 &&
      srow.uploaded_through >= srow.sealed_through &&
      companionPlan &&
      companionSegments.length >= uploadedSegments
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
    const companionSegments = app.deps.db.listSearchSegmentCompanions(STREAM);
    if (srow && srow.uploaded_through >= 0n && companionSegments.length === 0) return;
    await sleep(50);
  }
  throw new Error("timeout waiting for uploaded uncompanioned prefix");
}

async function waitForSecondaryIndexPublished(
  app: ReturnType<typeof createApp>,
  indexName: string,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const uploadedSegments = publishedSegmentCount(app);
    const state = app.deps.db.getSecondaryIndexState(STREAM, indexName);
    if (uploadedSegments > 0 && state && state.indexed_through >= uploadedSegments) return;
    app.deps.indexer?.enqueue(STREAM);
    await sleep(50);
  }
  throw new Error(`timeout waiting for ${indexName} exact index coverage`);
}

async function waitForPublishedSegmentCountAtLeast(
  app: ReturnType<typeof createApp>,
  expectedCount: number,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (publishedSegmentCount(app) >= expectedCount) return;
    await sleep(50);
  }
  throw new Error(`timeout waiting for published segment count >= ${expectedCount}`);
}

async function waitForSearchGateIdle(app: ReturnType<typeof createApp>, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await app.fetch(new Request("http://local/v1/server/_details", { method: "GET" }));
    if (res.status === 200) {
      const body = await res.json();
      if ((body.runtime?.concurrency?.search?.active ?? 0) === 0) return;
    }
    await sleep(25);
  }
  throw new Error("timeout waiting for idle search gate");
}

describe("_search http", () => {
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
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
          {
            eventTime: "2026-03-25T10:19:23.123Z",
            service: "audit-daemon",
            status: 200,
            duration: 25,
            requestId: "other_1",
            region: "us-west-2",
            message: "background heartbeat",
            why: "health check",
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
        let body = await res.json();
        expect(body.total.value).toBe(1);
        expect(["eq", "gte"]).toContain(body.total.relation);
        expect(body.coverage.index_families_used).toEqual(expect.arrayContaining(["col"]));
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
        expect(body.total.value).toBe(2);
        expect(["eq", "gte"]).toContain(body.total.relation);
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
              q: "req:req_*",
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
              q: "req:req_*",
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
    "returns timed-out partial search responses, clamps explicit timeout_ms to 3s, and defaults indexed-only searches to 2000ms",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-timeout-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 200,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
          {
            eventTime: "2026-03-25T10:17:23.123Z",
            service: "audit-daemon",
            status: 200,
            duration: 20,
            requestId: "other_1",
            region: "us-west-2",
            message: "background heartbeat",
            why: "health check",
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
        expect(body.total.value).toBe(2);
        expect(["eq", "gte"]).toContain(body.total.relation);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout",
              size: 10,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        expect(res.headers.get("search-timeout-ms")).toBe("2000");
        body = await res.json();
        expect(body.timeout_ms).toBe(2000);
        expect(body.timed_out).toBe(false);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: "timeout OR health",
              size: 10,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        expect(res.headers.get("search-timeout-ms")).toBe("3000");
        body = await res.json();
        expect(body.timeout_ms).toBe(3000);
        expect(body.timed_out).toBe(false);

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
        await waitForSearchGateIdle(app);

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
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
    "uses exact index visibility for exact-only search even while companions lag",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-exact-visibility-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 150,
        blockMaxBytes: 96,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
          const late = i >= 4;
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(10 + i).padStart(2, "0")}:23.123Z`,
                service: late ? "staging-api" : "billing-api",
                status: 500 + i,
                duration: 1000 + i,
                requestId: `req_${i}`,
                region: late ? "us-west-2" : "us-east-1",
                message: late ? `late exact match ${i}` : `early noise ${i}`,
                why: late ? "latest exact-only segment" : "older exact-only segment",
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSearchFamilies(app);
        await waitForSecondaryIndexPublished(app, "service");
        expect(app.deps.db.countSegmentsForStream(STREAM)).toBeGreaterThan(2);

        app.deps.indexer?.stop();
        app.deps.db.deleteSearchSegmentCompanionsFrom(STREAM, 1);
        expect(app.deps.db.listSearchSegmentCompanions(STREAM).length).toBeLessThan(publishedSegmentCount(app));

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api"',
              size: 2,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_7", "req_6"]);
        expect(body.coverage.complete).toBe(true);
        expect(body.coverage.mode).toBe("complete");
        expect(body.coverage.possible_missing_uploaded_segments).toBe(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "keeps mixed exact and text search on companion visibility even when exact coverage is farther ahead",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-mixed-indexed-visibility-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 160,
        blockMaxBytes: 96,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
                service: "staging-api",
                status: 500 + i,
                duration: 1000 + i,
                requestId: `req_${i}`,
                region: "us-west-2",
                payload: "x".repeat(160),
                message: `timeout signal ${i}`,
                why: "mixed indexed visibility",
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSearchFamilies(app);
        await waitForSecondaryIndexPublished(app, "service");
        expect(app.deps.db.countSegmentsForStream(STREAM)).toBeGreaterThan(2);

        app.deps.indexer?.stop();
        app.deps.db.deleteSearchSegmentCompanionsFrom(STREAM, 1);
        expect(app.deps.db.listSearchSegmentCompanions(STREAM).length).toBeLessThan(publishedSegmentCount(app));

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api" timeout',
              size: 10,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_0"]);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.possible_missing_uploaded_segments).toBeGreaterThan(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "scans offset-desc from the tail without decoding older corrupted blocks",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-tail-lazy-"));
      const spillDir = join(root, "spill");
      const cfg = makeConfig(root, {
        segmentMaxBytes: 2_048,
        blockMaxBytes: 128,
        segmentMaxIntervalMs: 60_000,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });
      const app = createApp(cfg, new MockR2Store({ maxInMemoryBytes: 0, spillDir }));
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
            body: JSON.stringify(EXACT_ONLY_SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 10; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(10 + i).padStart(2, "0")}:23.123Z`,
                service: "billing-api",
                requestId: `req_${i}`,
                payload: Array.from({ length: 256 }, (_, j) => String.fromCharCode(33 + ((i * 17 + j) % 90))).join(""),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForUploadedWithoutCompanions(app);
        app.deps.segmenter.stop();
        app.deps.indexer?.stop();

        const seg = app.deps.db.getSegmentByIndex(STREAM, 0);
        expect(seg).not.toBeNull();
        const expectedRequestId = `req_${Number(seg!.end_offset)}`;
        const key = segmentObjectKey(streamHash16Hex(STREAM), 0);
        const spilledPath = join(spillDir, `${createHash("sha256").update(key).digest("hex")}.bin`);
        const segBytes = readFileSync(spilledPath);
        const footer = parseFooter(segBytes);
        expect(footer?.footer?.blocks.length ?? 0).toBeGreaterThan(1);
        segBytes[DSB3_HEADER_BYTES + 1] = segBytes[DSB3_HEADER_BYTES + 1]! ^ 0xff;
        writeFileSync(spilledPath, segBytes);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"billing-api"',
              size: 1,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe(expectedRequestId);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.possible_missing_wal_rows).toBeGreaterThan(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "loads only the newest exact runs for cold offset-desc exact search, including search_after pagination",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-exact-tail-runs-"));
      const spillDir = join(root, "spill");
      const store = new CountingObjectStore(new MockR2Store({ maxInMemoryBytes: 0, spillDir }));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 160,
        blockMaxBytes: 96,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 1,
        indexCheckIntervalMs: 10,
        indexCompactionFanout: 1,
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
            body: JSON.stringify(EXACT_ONLY_SEARCH_SCHEMA),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 24; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(Math.floor(i / 60)).padStart(2, "0")}:${String(i % 60).padStart(2, "0")}.123Z`,
                service: "staging-api",
                requestId: `req_${i}`,
                payload: "x".repeat(160),
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSecondaryIndexPublished(app, "service", 30_000);
        expect(app.deps.db.listSecondaryIndexRuns(STREAM, "service").length).toBeGreaterThan(10);

        app.close();
        rmSync(join(root, "cache", "secondary-index"), { recursive: true, force: true });

        app = createApp(cfg, store);
        store.secondaryIndexGets = 0;

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api"',
              size: 1,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        let body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_23");
        expect(body.next_search_after).not.toBeNull();
        expect(store.secondaryIndexGets).toBeLessThan(8);

        store.secondaryIndexGets = 0;
        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api"',
              size: 1,
              sort: ["offset:desc"],
              search_after: body.next_search_after,
            }),
          })
        );
        expect(res.status).toBe(200);
        body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_22");
        expect(store.secondaryIndexGets).toBeLessThan(8);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "keeps mixed keyword exact and typed equality search on the indexed column path",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-exact-tail-column-"));
      const spillDir = join(root, "spill");
      const store = new CountingObjectStore(new MockR2Store({ maxInMemoryBytes: 0, spillDir }));
      const exactColumnSchema = {
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
            },
            requestId: {
              kind: "keyword",
              bindings: [{ version: 1, jsonPointer: "/requestId" }],
              exact: true,
              prefix: true,
              exists: true,
              sortable: true,
            },
          },
        },
      } as const;
      const cfg = makeConfig(root, {
        segmentMaxBytes: 160,
        blockMaxBytes: 96,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 1,
        indexCheckIntervalMs: 10,
        indexCompactionFanout: 1,
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
            body: JSON.stringify(exactColumnSchema),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 24; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(Math.floor(i / 60)).padStart(2, "0")}:${String(i % 60).padStart(2, "0")}.123Z`,
                service: "staging-api",
                status: 200 + i,
                duration: i === 22 || i === 23 ? 1422.027 : 1000 + i,
                requestId: `req_${i}`,
                region: "us-west-2",
                payload: "x".repeat(160),
                message: `event ${i}`,
                why: "typed equality exact-tail regression",
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSecondaryIndexPublished(app, "service", 30_000);
        await waitForSecondaryIndexPublished(app, "duration", 30_000);
        expect(app.deps.db.listSecondaryIndexRuns(STREAM, "service").length).toBeGreaterThan(10);
        expect(app.deps.db.listSecondaryIndexRuns(STREAM, "duration").length).toBeGreaterThan(10);

        app.close();
        rmSync(join(root, "cache", "secondary-index"), { recursive: true, force: true });

        app = createApp(cfg, store);
        store.secondaryIndexGets = 0;

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api" AND duration:1422.027',
              size: 1,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_23");
        expect(body.coverage.index_families_used).toEqual(["col"]);
        expect(store.secondaryIndexGets).toBe(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "uses companion visibility for typed equality when companions lag",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-typed-exact-visibility-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 150,
        blockMaxBytes: 96,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
      });
      const app = createApp(cfg);
      const exactColumnSchema = {
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
            },
            requestId: {
              kind: "keyword",
              bindings: [{ version: 1, jsonPointer: "/requestId" }],
              exact: true,
              prefix: true,
              exists: true,
              sortable: true,
            },
          },
        },
      } as const;
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
            body: JSON.stringify(exactColumnSchema),
          })
        );
        expect(res.status).toBe(200);

        for (let i = 0; i < 8; i++) {
          const late = i >= 6;
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(10 + i).padStart(2, "0")}:23.123Z`,
                service: late ? "staging-api" : "billing-api",
                duration: late ? 1422.027 : 1000 + i,
                requestId: `req_${i}`,
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSecondaryIndexPublished(app, "service");
        await waitForSecondaryIndexPublished(app, "duration");
        expect(app.deps.db.countSegmentsForStream(STREAM)).toBeGreaterThan(2);

        app.deps.indexer?.stop();
        app.deps.db.deleteSearchSegmentCompanionsFrom(STREAM, 1);
        expect(app.deps.db.listSearchSegmentCompanions(STREAM).length).toBeLessThan(publishedSegmentCount(app));

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api" AND duration:1422.027',
              size: 2,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits).toEqual([]);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.mode).toBe("published");
        expect(body.coverage.possible_missing_uploaded_segments).toBeGreaterThan(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "omits newest uploaded exact matches when exact-secondary coverage lags",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-exact-lag-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 150,
        blockMaxBytes: 96,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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

        for (let i = 0; i < 4; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(10 + i).padStart(2, "0")}:23.123Z`,
                service: "staging-api",
                status: 500 + i,
                duration: 1000 + i,
                requestId: `req_${i}`,
                region: "us-west-2",
                message: `indexed exact match ${i}`,
                why: "indexed exact segment",
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForSecondaryIndexPublished(app, "service", 20_000);
        const indexedThrough = app.deps.db.getSecondaryIndexState(STREAM, "service")!.indexed_through;
        app.deps.indexer?.stop();

        for (let i = 4; i < 6; i++) {
          res = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                eventTime: `2026-03-25T10:${String(10 + i).padStart(2, "0")}:23.123Z`,
                service: "staging-api",
                status: 500 + i,
                duration: 1000 + i,
                requestId: `req_${i}`,
                region: "us-west-2",
                message: `late exact match ${i}`,
                why: "unindexed exact segment",
              }),
            })
          );
          expect(res.status).toBe(204);
        }

        await waitForPublishedSegmentCountAtLeast(app, indexedThrough + 2, 20_000);

        res = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              q: 'service:"staging-api"',
              size: 10,
              sort: ["offset:desc"],
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_3", "req_2", "req_1", "req_0"]);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.possible_missing_uploaded_segments).toBeGreaterThan(0);
        expect(body.coverage.scanned_tail_docs).toBe(0);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "keeps indexed text search on published indexed coverage even when the WAL tail is quiet",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-quiet-tail-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 1_000_000,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
        expect(body.hits).toHaveLength(0);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.mode).toBe("published");
        expect(body.coverage.scanned_tail_docs).toBe(0);
        expect(body.coverage.possible_missing_wal_rows).toBe(2);
        expect(body.total).toEqual({ value: 0, relation: "gte" });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );

  test(
    "still searches the quiet WAL tail for non-index-only queries",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-quiet-tail-fallback-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 1_000_000,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
              q: '-service:"other-api" timeout',
              sort: ["offset:desc"],
              size: 10,
            }),
          })
        );
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.hits.map((hit: any) => hit.fields.requestId)).toEqual(["req_2", "req_1"]);
        expect(body.coverage.complete).toBe(true);
        expect(body.coverage.scanned_tail_docs).toBeGreaterThan(0);
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
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
    "omits the newest uploaded and WAL suffix while companions are still catching up",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-omit-suffix-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: 140,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 2,
        indexCheckIntervalMs: 60_000,
        segmentCacheMaxBytes: 64 * 1024 * 1024,
        segmentFooterCacheEntries: 128,
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
        app.deps.indexer?.stop();

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
        expect(body.hits).toEqual([]);
        expect(body.coverage.complete).toBe(false);
        expect(body.coverage.mode).toBe("published");
        expect(body.coverage.scanned_segments).toBe(0);
        expect(body.coverage.scanned_tail_docs).toBe(0);
        expect(body.coverage.possible_missing_uploaded_segments).toBeGreaterThan(0);
        expect(body.coverage.possible_missing_wal_rows).toBeGreaterThan(0);
        expect(body.coverage.possible_missing_events_upper_bound).toBeGreaterThan(0);
        expect(body.total).toEqual({ value: 0, relation: "gte" });
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    30_000
  );
});
