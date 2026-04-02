import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { encodeOffset, parseOffset } from "../src/offset";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 60_000,
    uploadIntervalMs: 60_000,
    ...overrides,
  };
}

async function withServer<T>(
  overrides: Partial<Config>,
  fn: (ctx: { baseUrl: string }) => Promise<T>
): Promise<T> {
  const root = mkdtempSync(join(tmpdir(), "ds-http-"));
  const cfg = makeConfig(root, overrides);
  const app = createApp(cfg, new MockR2Store());
  const server = Bun.serve({ port: 0, fetch: app.fetch });
  const baseUrl = `http://localhost:${server.port}`;
  try {
    return await fn({ baseUrl });
  } finally {
    server.stop();
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

function nextOffset(resp: Response): bigint {
  const h = resp.headers.get("stream-next-offset");
  expect(h).not.toBeNull();
  const p = parseOffset(h!);
  return p.kind === "start" ? -1n : p.seq;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

const DETAILS_SEARCH_SCHEMA = {
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
    },
  },
};

describe("http behavior", () => {
  test("create/append/read raw and end offset", async () => {
    await withServer({}, async ({ baseUrl }) => {
      let r = await fetch(`${baseUrl}/v1/stream/foo`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect([200, 201]).toContain(r.status);
      expect(nextOffset(r)).toBe(-1n);

      r = await fetch(`${baseUrl}/v1/stream/foo`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      expect(r.status).toBe(204);
      expect(nextOffset(r)).toBe(0n);

      r = await fetch(`${baseUrl}/v1/stream/foo`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      expect(r.status).toBe(204);
      expect(nextOffset(r)).toBe(1n);

      r = await fetch(`${baseUrl}/v1/stream/foo?offset=-1`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("ab");
      expect(nextOffset(r)).toBe(1n);

      const end = encodeOffset(0, 1n);
      r = await fetch(`${baseUrl}/v1/stream/foo?offset=${end}`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("");
      expect(nextOffset(r)).toBe(1n);
    });
  });

  test("read empty stream returns empty body", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/empty`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/stream/empty?offset=-1`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("");
      expect(nextOffset(r)).toBe(-1n);
    });
  });

  test("read beyond end returns unchanged offset", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/end`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/end`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      await fetch(`${baseUrl}/v1/stream/end`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      const off = encodeOffset(0, 999n);
      const r = await fetch(`${baseUrl}/v1/stream/end?offset=${off}`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("");
      expect(nextOffset(r)).toBe(999n);
    });
  });

  test("list streams returns all streams", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/a`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/b`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/streams`);
      expect(r.status).toBe(200);
      const arr = await r.json();
      expect(Array.isArray(arr)).toBe(true);
      const names = arr.map((r: any) => r.name).sort();
      expect(names).toContain("a");
      expect(names).toContain("b");
      const streamA = arr.find((row: any) => row.name === "a");
      expect(streamA?.profile).toBe("generic");
    });
  });

  test("profile subresource defaults to generic and supports explicit declaration", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/profiled`, { method: "PUT", headers: { "content-type": "text/plain" } });

      let r = await fetch(`${baseUrl}/v1/stream/profiled/_profile`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });

      r = await fetch(`${baseUrl}/v1/stream/profiled/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "generic" },
        }),
      });
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });
    });
  });

  test("details endpoint combines stream, profile, schema, and index status", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/details`, { method: "PUT", headers: { "content-type": "text/plain" } });

      const r = await fetch(`${baseUrl}/v1/stream/details/_details`);
      expect(r.status).toBe(200);
      const body = await r.json();
      expect(body.stream).toMatchObject({
        name: "details",
        content_type: "text/plain",
        profile: "generic",
        created_at: expect.any(String),
        updated_at: expect.any(String),
        expires_at: null,
        epoch: 0,
        next_offset: "0",
        sealed_through: "-1",
        uploaded_through: "-1",
        segment_count: 0,
        uploaded_segment_count: 0,
        total_size_bytes: "0",
      });
      expect(body.profile).toEqual({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      });
      expect(body.schema).toMatchObject({
        apiVersion: "durable.streams/schema-registry/v1",
        schema: "details",
        currentVersion: 0,
        boundaries: [],
      });
      expect(body.index_status).toMatchObject({
        stream: "details",
        profile: "generic",
        segments: {
          total_count: 0,
          uploaded_count: 0,
        },
      });
      expect(body.index_status.routing_key_index?.configured).toBe(false);
      expect(body.index_status.exact_indexes).toEqual([]);
      expect(body.index_status.search_families).toEqual([]);
    });
  });

  test("details endpoint reports total_size_bytes with simple lookup", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/size`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/size`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "abc",
      });

      const r = await fetch(`${baseUrl}/v1/stream/size/_details`);
      expect(r.status).toBe(200);
      const body = await r.json();
      expect(body.stream).toMatchObject({
        name: "size",
        next_offset: "1",
        total_size_bytes: "3",
        pending_bytes: "3",
        wal_bytes: "3",
      });
    });
  });

  test("details endpoint reports storage usage and object-store request accounting", async () => {
    await withServer(
      {
        segmentMaxBytes: 180,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        indexCheckIntervalMs: 10,
        indexL0SpanSegments: 2,
        searchCompanionBuildBatchSegments: 2,
      },
      async ({ baseUrl }) => {
        let r = await fetch(`${baseUrl}/v1/stream/details-storage`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        });
        expect([200, 201]).toContain(r.status);

        r = await fetch(`${baseUrl}/v1/stream/details-storage/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(DETAILS_SEARCH_SCHEMA),
        });
        expect(r.status).toBe(200);

        for (let i = 0; i < 12; i++) {
          const event = {
            eventTime: `2026-03-31T12:${String(i).padStart(2, "0")}:00.000Z`,
            service: i % 2 === 0 ? "billing-api" : "worker-api",
            message: `record ${i} constructor push`,
          };
          const appendRes = await fetch(`${baseUrl}/v1/stream/details-storage`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(event),
          });
          expect(appendRes.status).toBe(204);
        }

        let body: any = null;
        const deadline = Date.now() + 10_000;
        let ready = false;
        while (Date.now() < deadline) {
          const detailsRes = await fetch(`${baseUrl}/v1/stream/details-storage/_details`);
          expect(detailsRes.status).toBe(200);
          body = await detailsRes.json();
          if (
            Number(body.stream?.uploaded_segment_count ?? 0) > 0 &&
            Number(body.index_status?.bundled_companions?.object_count ?? 0) > 0
          ) {
            ready = true;
            break;
          }
          await sleep(50);
        }

        expect(body).not.toBeNull();
        expect(ready).toBe(true);
        expect(Number(body.stream.uploaded_segment_count)).toBeGreaterThan(0);
        expect(Number(body.storage.object_storage.total_bytes)).toBeGreaterThan(0);
        expect(Number(body.storage.object_storage.segments_bytes)).toBeGreaterThan(0);
        expect(Number(body.storage.object_storage.indexes_bytes)).toBeGreaterThan(0);
        expect(Number(body.storage.object_storage.manifest_bytes)).toBeGreaterThan(0);
        expect(Number(body.storage.object_storage.schema_registry_bytes)).toBeGreaterThan(0);
        expect(body.storage.object_storage.segment_object_count).toBe(Number(body.stream.uploaded_segment_count));
        expect(body.storage.object_storage.bundled_companion_object_count).toBeGreaterThan(0);

        expect(Number(body.storage.local_storage.wal_retained_bytes)).toBeGreaterThanOrEqual(0);
        expect(Number(body.storage.local_storage.sqlite_shared_total_bytes)).toBeGreaterThan(0);

        expect(Number(body.object_store_requests.puts)).toBeGreaterThan(0);
        expect(Number(body.object_store_requests.reads)).toBeGreaterThanOrEqual(0);
        expect(body.object_store_requests.by_artifact).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              artifact: "segment",
            }),
            expect.objectContaining({
              artifact: "manifest",
            }),
            expect.objectContaining({
              artifact: "schema_registry",
            }),
          ])
        );

        const routingIndex = body.index_status.routing_key_index;
        expect(routingIndex).toMatchObject({
          configured: false,
        });
        expect(Number(routingIndex.bytes_at_rest)).toBeGreaterThanOrEqual(0);
        expect(routingIndex.object_count).toBe(0);
        expect(routingIndex.indexed_segment_count).toBe(0);

        const exactIndex = body.index_status.exact_indexes.find((entry: any) => entry.name === "service");
        expect(exactIndex).toBeDefined();
        expect(Number(exactIndex.bytes_at_rest)).toBeGreaterThanOrEqual(0);
        expect(exactIndex.object_count).toBeGreaterThanOrEqual(0);
        expect(exactIndex.lag_segments).toBeGreaterThanOrEqual(0);
        expect(typeof exactIndex.stale_configuration).toBe("boolean");

        const ftsFamily = body.index_status.search_families.find((entry: any) => entry.family === "fts");
        expect(ftsFamily).toBeDefined();
        expect(Number(ftsFamily.bytes_at_rest)).toBeGreaterThan(0);
        expect(ftsFamily.object_count).toBeGreaterThan(0);
      }
    );
  });

  test("details endpoint returns etag and supports conditional get", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/details-etag`, { method: "PUT", headers: { "content-type": "text/plain" } });

      const first = await fetch(`${baseUrl}/v1/stream/details-etag/_details`);
      expect(first.status).toBe(200);
      const etag = first.headers.get("etag");
      expect(etag).not.toBeNull();

      const second = await fetch(`${baseUrl}/v1/stream/details-etag/_details`, {
        headers: { "if-none-match": etag! },
      });
      expect(second.status).toBe(304);
      expect(second.headers.get("etag")).toBe(etag);
    });
  });

  test("details long-poll wakes on append", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/details-live`, { method: "PUT", headers: { "content-type": "text/plain" } });

      const first = await fetch(`${baseUrl}/v1/stream/details-live/_details`);
      const etag = first.headers.get("etag");
      expect(etag).not.toBeNull();

      const detailsPromise = fetch(`${baseUrl}/v1/stream/details-live/_details?live=long-poll&timeout=2s`, {
        headers: { "if-none-match": etag! },
      });
      await sleep(100);
      await fetch(`${baseUrl}/v1/stream/details-live`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "a",
      });

      const next = await detailsPromise;
      expect(next.status).toBe(200);
      expect(next.headers.get("etag")).not.toBe(etag);
      const body = await next.json();
      expect(body.stream).toMatchObject({
        name: "details-live",
        next_offset: "1",
        total_size_bytes: "1",
      });
    });
  });

  test("__stream_metrics__ details long-poll wakes on internal metrics emission", async () => {
    await withServer({ metricsFlushIntervalMs: 100 }, async ({ baseUrl }) => {
      const first = await fetch(`${baseUrl}/v1/stream/__stream_metrics__/_details`);
      expect(first.status).toBe(200);
      const etag = first.headers.get("etag");
      expect(etag).not.toBeNull();

      const start = Date.now();
      const next = await fetch(
        `${baseUrl}/v1/stream/__stream_metrics__/_details?live=long-poll&timeout=2s`,
        {
          headers: { "if-none-match": etag! },
        },
      );

      expect(next.status).toBe(200);
      expect(Date.now() - start).toBeLessThan(1800);
      expect(next.headers.get("etag")).not.toBe(etag);

      const body = await next.json();
      expect(body.stream?.name).toBe("__stream_metrics__");
      expect(Number(body.stream?.next_offset ?? "0")).toBeGreaterThan(0);
    });
  });

  test("details long-poll wakes on metadata changes", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/details-meta`, { method: "PUT", headers: { "content-type": "application/json" } });

      const first = await fetch(`${baseUrl}/v1/stream/details-meta/_details`);
      const etag = first.headers.get("etag");
      expect(etag).not.toBeNull();

      const detailsPromise = fetch(`${baseUrl}/v1/stream/details-meta/_details?live=long-poll&timeout=2s`, {
        headers: { "if-none-match": etag! },
      });
      await sleep(100);
      const update = await fetch(`${baseUrl}/v1/stream/details-meta/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "evlog" },
        }),
      });
      expect(update.status).toBe(200);

      const next = await detailsPromise;
      expect(next.status).toBe(200);
      expect(next.headers.get("etag")).not.toBe(etag);
      const body = await next.json();
      expect(body.stream?.profile).toBe("evlog");
      expect(body.profile?.profile).toEqual({ kind: "evlog" });
    });
  });

  test("details long-poll times out with 304 when unchanged", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/details-timeout`, { method: "PUT", headers: { "content-type": "text/plain" } });

      const first = await fetch(`${baseUrl}/v1/stream/details-timeout/_details`);
      const etag = first.headers.get("etag");
      expect(etag).not.toBeNull();

      const start = Date.now();
      const timedOut = await fetch(`${baseUrl}/v1/stream/details-timeout/_details?live=long-poll&timeout=200ms`, {
        headers: { "if-none-match": etag! },
      });
      expect(timedOut.status).toBe(304);
      expect(Date.now() - start).toBeGreaterThan(150);
      expect(timedOut.headers.get("etag")).toBe(etag);
    });
  });

  test("profile subresource rejects unsupported profiles", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/profiled`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/stream/profiled/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "queue" },
        }),
      });
      expect(r.status).toBe(400);
    });
  });

  test("state-protocol profile requires json streams and enables touch routes", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/text-profiled`, { method: "PUT", headers: { "content-type": "text/plain" } });

      let r = await fetch(`${baseUrl}/v1/stream/text-profiled/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: { enabled: true },
          },
        }),
      });
      expect(r.status).toBe(400);
      expect(await r.text()).toContain("application/json");

      await fetch(`${baseUrl}/v1/stream/json-profiled`, { method: "PUT", headers: { "content-type": "application/json" } });

      r = await fetch(`${baseUrl}/v1/stream/json-profiled/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: {
            kind: "state-protocol",
            touch: { enabled: true, onMissingBefore: "coarse" },
          },
        }),
      });
      expect(r.status).toBe(200);
      const profileJson = await r.json();
      expect(profileJson.apiVersion).toBe("durable.streams/profile/v1");
      expect(profileJson.profile?.kind).toBe("state-protocol");
      expect(profileJson.profile?.touch?.enabled).toBe(true);
      expect(profileJson.profile?.touch?.onMissingBefore).toBe("coarse");

      r = await fetch(`${baseUrl}/v1/stream/json-profiled/touch/meta`);
      expect(r.status).toBe(200);
    });
  });

  test("create is idempotent for existing stream", async () => {
    await withServer({}, async ({ baseUrl }) => {
      let r = await fetch(`${baseUrl}/v1/stream/dup`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect([200, 201]).toContain(r.status);
      expect(nextOffset(r)).toBe(-1n);

      r = await fetch(`${baseUrl}/v1/stream/dup`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect(r.status).toBe(200);
      expect(nextOffset(r)).toBe(-1n);
    });
  });

  test("delete returns 404 for missing stream and 204 for existing", async () => {
    await withServer({}, async ({ baseUrl }) => {
      let r = await fetch(`${baseUrl}/v1/stream/missing`, { method: "DELETE" });
      expect(r.status).toBe(404);

      await fetch(`${baseUrl}/v1/stream/delete-me`, { method: "PUT", headers: { "content-type": "text/plain" } });
      r = await fetch(`${baseUrl}/v1/stream/delete-me`, { method: "DELETE" });
      expect(r.status).toBe(204);

      r = await fetch(`${baseUrl}/v1/stream/delete-me?offset=-1`);
      expect(r.status).toBe(404);
    });
  });

  test("overload responses include retry-after", async () => {
    await withServer({ ingestMaxQueueBytes: 1, ingestMaxQueueRequests: 1 }, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/overloaded`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
      });

      const r = await fetch(`${baseUrl}/v1/stream/overloaded`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "ab",
      });

      expect(r.status).toBe(429);
      expect(r.headers.get("retry-after")).toBe("1");
      expect(await r.json()).toEqual({
        error: { code: "overloaded", message: "ingest queue full" },
      });
    });
  });

  test("memory backpressure rejects streaming request bodies promptly", { timeout: 5_000 }, async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-http-memory-backpressure-"));
    const app = createApp(makeConfig(root), new MockR2Store());
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    const baseUrl = `http://localhost:${server.port}`;
    try {
      let res = await fetch(`${baseUrl}/v1/stream/memory-backpressure`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
      });
      expect(res.status).toBe(201);

      const originalShouldAllow = app.deps.memory.shouldAllow.bind(app.deps.memory);
      (app.deps.memory as any).shouldAllow = () => false;

      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("hello"));
        },
      });

      const start = Date.now();
      res = await Promise.race([
        fetch(`${baseUrl}/v1/stream/memory-backpressure`, {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body,
          duplex: "half",
        }),
        sleep(1_500).then(() => {
          throw new Error("memory backpressure response hung");
        }),
      ]);
      const elapsed = Date.now() - start;

      expect(res.status).toBe(429);
      expect(elapsed).toBeLessThan(1_500);
      expect(res.headers.get("retry-after")).toBe("1");
      expect(await res.json()).toEqual({
        error: { code: "memory_backpressure", message: "server memory backpressure" },
      });

      (app.deps.memory as any).shouldAllow = originalShouldAllow;
    } finally {
      server.stop();
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("append requests time out after 3s with an append-specific error", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-http-append-timeout-"));
    const app = createApp(makeConfig(root), new MockR2Store());
    try {
      let res = await app.fetch(
        new Request("http://local/v1/stream/slow-append", {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        })
      );
      expect(res.status).toBe(201);

      app.deps.ingest.append = (() =>
        new Promise((resolve) => {
          setTimeout(() => resolve(Result.err({ kind: "internal" })), 3_200);
        })) as any;

      const start = Date.now();
      res = await app.fetch(
        new Request("http://local/v1/stream/slow-append", {
          method: "POST",
          headers: { "content-type": "text/plain" },
          body: "hello",
        })
      );
      const elapsed = Date.now() - start;

      expect(res.status).toBe(408);
      expect(elapsed).toBeGreaterThanOrEqual(2_900);
      expect(elapsed).toBeLessThan(4_500);
      expect(await res.json()).toEqual({
        error: {
          code: "append_timeout",
          message: "append timed out; append outcome is unknown, check Stream-Next-Offset before retrying",
        },
      });
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("generic resolver timeout returns 408 for handlers that exceed 5s", { timeout: 8_000 }, async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/details-resolver-timeout`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
      });

      const first = await fetch(`${baseUrl}/v1/stream/details-resolver-timeout/_details`);
      expect(first.status).toBe(200);
      const etag = first.headers.get("etag");
      expect(etag).not.toBeNull();

      const start = Date.now();
      const timedOut = await fetch(`${baseUrl}/v1/stream/details-resolver-timeout/_details?live=long-poll&timeout=6s`, {
        headers: { "if-none-match": etag! },
      });
      const elapsed = Date.now() - start;

      expect(timedOut.status).toBe(408);
      expect(elapsed).toBeGreaterThanOrEqual(4_900);
      expect(elapsed).toBeLessThan(7_500);
      expect(await timedOut.json()).toEqual({
        error: { code: "request_timeout", message: "request timed out" },
      });
    });
  });

  test("shutdown responses include retry-after", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-http-close-"));
    const app = createApp(makeConfig(root), new MockR2Store());
    try {
      app.close();
      const r = await app.fetch(new Request("http://local/health"));
      expect(r.status).toBe(503);
      expect(r.headers.get("retry-after")).toBe("5");
      expect(await r.json()).toEqual({
        error: { code: "unavailable", message: "server shutting down" },
      });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("append max body bytes enforced", async () => {
    await withServer({ appendMaxBodyBytes: 4 }, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/limit`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const r = await fetch(`${baseUrl}/v1/stream/limit`, {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "hello",
      });
      expect(r.status).toBe(413);
    });
  });

  test("json batch append and read json", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/json`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/json`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ x: 1 }, { y: 2 }]),
      });
      expect(r.status).toBe(204);
      r = await fetch(`${baseUrl}/v1/stream/json?offset=-1&format=json`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ x: 1 }, { y: 2 }]);
    });
  });

  test("schema routing key batch append and read by key", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/keys`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/keys/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ routingKey: { jsonPointer: "/k", required: true } }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/keys`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ k: "k1", x: 1 }, { k: "k2", y: 2 }]),
      });
      expect(r.status).toBe(204);

      r = await fetch(`${baseUrl}/v1/stream/keys/pk/k2?offset=-1&format=json`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ y: 2, k: "k2" }]);

      r = await fetch(`${baseUrl}/v1/stream/keys?offset=-1&format=json&key=k2`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ y: 2, k: "k2" }]);

      r = await fetch(`${baseUrl}/v1/stream/keys`, {
        method: "POST",
        headers: { "content-type": "application/json", "stream-key": "ignored" },
        body: JSON.stringify({ k: "k3", z: 3 }),
      });
      expect(r.status).toBe(400);
    });
  });

  test("json schema date-time format is enforced on append", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/date-format`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });

      let r = await fetch(`${baseUrl}/v1/stream/date-format/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          schema: {
            type: "object",
            properties: {
              eventTime: { type: "string", format: "date-time" },
            },
            required: ["eventTime"],
            additionalProperties: false,
          },
        }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/date-format`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ eventTime: "not-a-timestamp" }]),
      });
      expect(r.status).toBe(400);
      expect(await r.text()).toContain("must match format");

      r = await fetch(`${baseUrl}/v1/stream/date-format`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ eventTime: "2026-03-30T12:59:05.983Z" }]),
      });
      expect(r.status).toBe(204);
    });
  });

  test("read filter applies indexed predicates on the main read path", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/filterable`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/filterable/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          schema: {
            type: "object",
            properties: {
              eventTime: { type: "string" },
              service: { type: "string" },
              status: { type: "integer" },
              message: { type: "string" },
            },
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
              },
              status: {
                kind: "integer",
                bindings: [{ version: 1, jsonPointer: "/status" }],
                exact: true,
                column: true,
                exists: true,
              },
            },
          },
        }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/filterable`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([
          { service: "api", status: 200, message: "ok" },
          { service: "worker", status: 503, message: "retry" },
          { service: "api", status: 503, message: "boom" },
        ]),
      });
      expect(r.status).toBe(204);

      const params = new URLSearchParams({
        offset: "-1",
        format: "json",
        filter: "service:api status:>=500",
      });
      r = await fetch(`${baseUrl}/v1/stream/filterable?${params.toString()}`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([{ service: "api", status: 503, message: "boom" }]);
      expect(nextOffset(r)).toBe(2n);
    });
  });

  test("read filter advances the stream cursor when no entries match", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/filter-empty`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/filter-empty/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          schema: {
            type: "object",
            properties: {
              eventTime: { type: "string" },
              service: { type: "string" },
            },
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
              },
            },
          },
        }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/filter-empty`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ service: "api" }, { service: "api" }]),
      });
      expect(r.status).toBe(204);

      const params = new URLSearchParams({
        offset: "-1",
        format: "json",
        filter: "service:worker",
      });
      r = await fetch(`${baseUrl}/v1/stream/filter-empty?${params.toString()}`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([]);
      expect(nextOffset(r)).toBe(1n);
    });
  });

  test("read filter rejects non-json streams and non-indexed fields", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/filter-raw`, { method: "PUT", headers: { "content-type": "text/plain" } });
      let r = await fetch(`${baseUrl}/v1/stream/filter-raw?offset=-1&filter=service:api`);
      expect(r.status).toBe(400);
      expect(await r.text()).toContain("application/json");

      await fetch(`${baseUrl}/v1/stream/filter-bad-field`, { method: "PUT", headers: { "content-type": "application/json" } });
      r = await fetch(`${baseUrl}/v1/stream/filter-bad-field/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          schema: {
            type: "object",
            properties: {
              eventTime: { type: "string" },
              service: { type: "string" },
            },
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
              },
            },
          },
        }),
      });
      expect(r.status).toBe(200);

      r = await fetch(`${baseUrl}/v1/stream/filter-bad-field?offset=-1&format=json&filter=status:500`);
      expect(r.status).toBe(400);
      expect(await r.text()).toContain("not indexed");
    });
  });

  test("read filter reports the 100MB scan limit on unsealed tail scans", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/filter-limit`, { method: "PUT", headers: { "content-type": "application/json" } });
      let r = await fetch(`${baseUrl}/v1/stream/filter-limit/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          schema: {
            type: "object",
            properties: {
              eventTime: { type: "string" },
              status: { type: "integer" },
              message: { type: "string" },
              seq: { type: "integer" },
            },
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
              status: {
                kind: "integer",
                bindings: [{ version: 1, jsonPointer: "/status" }],
                exact: true,
                column: true,
                exists: true,
              },
            },
          },
        }),
      });
      expect(r.status).toBe(200);

      const largeMessage = "x".repeat(4 * 1024 * 1024);
      for (let i = 0; i < 30; i++) {
        r = await fetch(`${baseUrl}/v1/stream/filter-limit`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify([{ status: 200, message: largeMessage, seq: i }]),
        });
        expect(r.status).toBe(204);
      }

      const params = new URLSearchParams({
        offset: "-1",
        format: "json",
        filter: "status:>=500",
      });
      r = await fetch(`${baseUrl}/v1/stream/filter-limit?${params.toString()}`);
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual([]);
      expect(r.headers.get("stream-filter-scan-limit-reached")).toBe("true");
      expect(r.headers.get("stream-filter-scan-limit-bytes")).toBe(String(100 * 1024 * 1024));
      expect(Number(r.headers.get("stream-filter-scanned-bytes"))).toBeGreaterThanOrEqual(100 * 1024 * 1024);
      expect(nextOffset(r)).toBeLessThan(29n);
    });
  });

  test("live read timeout", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/live`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/live`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });

      const start = Date.now();
      const r = await fetch(`${baseUrl}/v1/stream/live?offset=${encodeOffset(0, 0n)}&live=long-poll&timeout=200ms`);
      expect(r.status).toBe(204);
      expect(Date.now() - start).toBeGreaterThan(150);
    });
  });

  test("live read wakes on append", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/live2`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/live2`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });

      const livePromise = fetch(`${baseUrl}/v1/stream/live2?offset=${encodeOffset(0, 0n)}&live=long-poll&timeout=2s`);
      await sleep(100);
      await fetch(`${baseUrl}/v1/stream/live2`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      const r = await livePromise;
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("b");
    });
  });

  test("live read by key only returns on match", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/keylive`, { method: "PUT", headers: { "content-type": "text/plain" } });
      const post = (key: string, body: string) =>
        fetch(`${baseUrl}/v1/stream/keylive`, {
          method: "POST",
          headers: { "content-type": "text/plain", "stream-key": key },
          body,
        });
      await post("a", "1");
      await post("b", "2");
      await post("a", "3");

      const livePromise = fetch(
        `${baseUrl}/v1/stream/keylive/pk/a?offset=${encodeOffset(0, 2n)}&live=long-poll&timeout=1s`
      );

      await sleep(100);
      await post("b", "4");

      let resolved = false;
      await Promise.race([
        livePromise.then(() => {
          resolved = true;
        }),
        sleep(150),
      ]);
      expect(resolved).toBe(false);

      await post("a", "5");
      const r = await livePromise;
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("5");
    });
  });

  test("ttl expiry returns 404", async () => {
    await withServer({}, async ({ baseUrl }) => {
      const expires = new Date(Date.now() + 200).toISOString();
      const r = await fetch(`${baseUrl}/v1/stream/ttl`, {
        method: "PUT",
        headers: { "content-type": "text/plain", "stream-expires-at": expires },
      });
      expect(r.status).toBe(201);
      await sleep(250);
      const r2 = await fetch(`${baseUrl}/v1/stream/ttl?offset=-1`);
      expect(r2.status).toBe(404);
    });
  });

  test("etag and cache-control headers on catch-up reads", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/etag`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/etag`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      const r = await fetch(`${baseUrl}/v1/stream/etag?offset=-1`);
      expect(r.status).toBe(200);
      expect(r.headers.get("cache-control")).not.toBeNull();
      expect(r.headers.get("etag")).not.toBeNull();
    });
  });

  test("etag match returns 304 with empty body", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/etag2`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/etag2`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      const r1 = await fetch(`${baseUrl}/v1/stream/etag2?offset=-1`);
      const etag = r1.headers.get("etag");
      expect(etag).not.toBeNull();
      await r1.text();
      const r2 = await fetch(`${baseUrl}/v1/stream/etag2?offset=-1`, { headers: { "if-none-match": etag! } });
      expect(r2.status).toBe(304);
      const body = await r2.text();
      expect(body).toBe("");
    });
  });

  test("cache-control no-store for live reads", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/nolive`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/nolive`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      const r = await fetch(`${baseUrl}/v1/stream/nolive?offset=${encodeOffset(0, 0n)}&live=true&timeout=10ms`);
      expect(r.status).toBe(204);
      expect(r.headers.get("cache-control")).toBe("no-store");
      expect(r.headers.get("etag")).toBeNull();
    });
  });

  test("stream seq header enforces monotonicity", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/seq`, { method: "PUT", headers: { "content-type": "text/plain" } });
      let r = await fetch(`${baseUrl}/v1/stream/seq`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-seq": "001" },
        body: "a",
      });
      expect(r.status).toBe(204);

      r = await fetch(`${baseUrl}/v1/stream/seq`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-seq": "001" },
        body: "b",
      });
      expect(r.status).toBe(409);

      r = await fetch(`${baseUrl}/v1/stream/seq`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-seq": "002" },
        body: "b",
      });
      expect(r.status).toBe(204);
    });
  });

  test("stream expires header is echoed on HEAD/GET", async () => {
    await withServer({}, async ({ baseUrl }) => {
      const r = await fetch(`${baseUrl}/v1/stream/expires`, {
        method: "PUT",
        headers: { "content-type": "text/plain", "stream-ttl": "3600" },
      });
      expect(r.status).toBe(201);
      const exp = r.headers.get("stream-expires-at");
      expect(exp).not.toBeNull();

      const head = await fetch(`${baseUrl}/v1/stream/expires`, { method: "HEAD" });
      expect(head.headers.get("stream-expires-at")).toBe(exp);

      const get = await fetch(`${baseUrl}/v1/stream/expires?offset=-1`);
      expect(get.headers.get("stream-expires-at")).toBe(exp);
    });
  });

  test("since param invalid and offset overrides since", async () => {
    await withServer({}, async ({ baseUrl }) => {
      await fetch(`${baseUrl}/v1/stream/since`, { method: "PUT", headers: { "content-type": "text/plain" } });
      await fetch(`${baseUrl}/v1/stream/since`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      await fetch(`${baseUrl}/v1/stream/since`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });

      let r = await fetch(`${baseUrl}/v1/stream/since?since=not-a-time`);
      expect(r.status).toBe(400);

      const future = new Date(Date.now() + 60_000).toISOString();
      r = await fetch(`${baseUrl}/v1/stream/since?offset=-1&since=${encodeURIComponent(future)}`);
      expect(r.status).toBe(200);
      expect(await r.text()).toBe("ab");
    });
  });
});
