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
    const secondaryStates = app.deps.db.listSecondaryIndexStates(STREAM);
    const colState = app.deps.db.getSearchFamilyState(STREAM, "col");
    const ftsState = app.deps.db.getSearchFamilyState(STREAM, "fts");
    const colSegments = app.deps.db.listSearchFamilySegments(STREAM, "col");
    const ftsSegments = app.deps.db.listSearchFamilySegments(STREAM, "fts");
    if (
      srow &&
      srow.uploaded_through >= srow.sealed_through &&
      secondaryStates.length >= 4 &&
      colState &&
      ftsState &&
      colSegments.length > 0 &&
      ftsSegments.length > 0
    ) {
      return;
    }
    app.deps.indexer?.enqueue(STREAM);
    await sleep(50);
  }
  throw new Error("timeout waiting for search families");
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

        const events = [
          {
            eventTime: "2026-03-25T10:15:23.123Z",
            service: "billing-api",
            status: 402,
            duration: 1834,
            requestId: "req_1",
            message: "card declined",
            why: "issuer reported insufficient funds",
          },
          {
            eventTime: "2026-03-25T10:16:23.123Z",
            service: "billing-api",
            status: 503,
            duration: 2400,
            requestId: "req_2",
            message: "payment retry failed",
            why: "downstream timeout",
          },
          {
            eventTime: "2026-03-25T10:17:23.123Z",
            service: "billing-worker",
            status: 200,
            duration: 100,
            requestId: "job_1",
            message: "retry scheduled",
            why: "background job",
          },
          {
            eventTime: "2026-03-25T10:18:23.123Z",
            service: "billing-api",
            status: 402,
            duration: 2100,
            requestId: "req_3",
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
        let body = await res.json();
        expect(body.total).toEqual({ value: 1, relation: "eq" });
        expect(body.coverage.index_families_used).toEqual(expect.arrayContaining(["col", "fts"]));
        expect(body.hits).toHaveLength(1);
        expect(body.hits[0].fields.requestId).toBe("req_2");

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
});
