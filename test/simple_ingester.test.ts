import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { parseSchemaUpdateResult } from "../src/schema/registry";
import {
  buildSimpleCardinalityValues,
  buildSimpleEvent,
  buildSimpleRoutingOnlyUpdate,
  runSimpleIngester,
} from "../experiments/demo/simple_ingester";

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

describe("simple ingester", () => {
  test("builds a routing-key-only schema update", () => {
    const parsed = parseSchemaUpdateResult(buildSimpleRoutingOnlyUpdate());
    if (Result.isError(parsed)) throw new Error(parsed.error.message);
    expect(parsed.value.routingKey).toEqual({ jsonPointer: "/cardinality100", required: true });
    expect(parsed.value.search).toBeUndefined();
    expect(parsed.value.schema).toBeUndefined();
  });

  test("builds deterministic event payloads", () => {
    const cardinality100 = buildSimpleCardinalityValues(100, 101);
    const cardinality1000 = buildSimpleCardinalityValues(1000, 1001);
    const eventA = buildSimpleEvent(42, "2026-04-07T00:00:00.000Z", cardinality100, cardinality1000);
    const eventB = buildSimpleEvent(42, "2026-04-07T00:00:00.000Z", cardinality100, cardinality1000);
    expect(eventA).toEqual(eventB);
    expect(eventA.randomString).toHaveLength(100);
    expect(eventA.cardinality100).toHaveLength(50);
    expect(eventA.cardinality1000).toHaveLength(50);
  });

  test(
    "runs end to end against a real app with routing key only",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-simple-ingester-"));
      const baseUrl = "http://127.0.0.1:8787";
      const cfg = makeConfig(root, {
        segmentMaxBytes: 1024,
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
        const summary = await runSimpleIngester(
          [
            "--url",
            baseUrl,
            "--stream",
            "simple-1",
            "--batch-size",
            "25",
            "--max-batches",
            "2",
            "--reset",
          ],
          { fetchImpl: makeDemoFetch(app, baseUrl) }
        );
        expect(summary.stream).toBe("simple-1");
        expect(summary.appendedEvents).toBe(50);
        expect(summary.nextId).toBe(50);

        const detailsRes = await app.fetch(new Request("http://local/v1/stream/simple-1/_details", { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.stream.next_offset).toBe("50");
        expect(details.schema.routingKey).toEqual({ jsonPointer: "/cardinality100", required: true });
        expect(details.schema.search).toBeUndefined();
        expect(details.index_status.routing_key_index.configured).toBe(true);
        expect(details.index_status.routing_key_lexicon.configured).toBe(true);
        expect(details.index_status.search_families).toEqual([]);

        const readRes = await app.fetch(
          new Request("http://local/v1/stream/simple-1?format=json&offset=-1", { method: "GET" })
        );
        expect(readRes.status).toBe(200);
        const rows = await readRes.json();
        expect(Array.isArray(rows)).toBe(true);
        expect(rows.length).toBeGreaterThan(0);
        expect(rows[0].cardinality100).toBeTypeOf("string");
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "resume-stream continues incrementing ids",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-simple-ingester-resume-"));
      const baseUrl = "http://127.0.0.1:8787";
      const cfg = makeConfig(root, {
        segmentMaxBytes: 1024,
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
        await runSimpleIngester(
          ["--url", baseUrl, "--stream", "simple-1", "--batch-size", "10", "--max-batches", "1", "--reset"],
          { fetchImpl: makeDemoFetch(app, baseUrl) }
        );
        const second = await runSimpleIngester(
          ["--url", baseUrl, "--stream", "simple-1", "--batch-size", "10", "--max-batches", "1", "--resume-stream"],
          { fetchImpl: makeDemoFetch(app, baseUrl) }
        );
        expect(second.startId).toBe(10);
        expect(second.nextId).toBe(20);
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );
});
