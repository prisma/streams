import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { buildEvlogEvent, runEvlogIngester } from "../experiments/demo/evlog_ingester";

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

describe("evlog ingester", () => {
  test("builds deterministic canonical-ish evlog payloads", () => {
    const eventA = buildEvlogEvent(42, "2026-04-07T12:00:00.000Z");
    const eventB = buildEvlogEvent(42, "2026-04-07T12:00:00.000Z");
    expect(eventA).toEqual(eventB);
    expect(eventA.requestId).toContain("req_");
    expect(eventA.traceId).toContain("trace_");
    expect(eventA.spanId).toContain("span_");
    expect(eventA.context.userId).toContain("user_");
  });

  test(
    "runs end to end against a real app with the evlog profile",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-evlog-ingester-"));
      const baseUrl = "http://127.0.0.1:8787";
      const cfg = makeConfig(root, {
        segmentMaxBytes: 2_048,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 1,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const app = createApp(cfg);
      try {
        const summary = await runEvlogIngester(
          [
            "--url",
            baseUrl,
            "--stream",
            "evlog-1",
            "--batch-size",
            "20",
            "--max-batches",
            "2",
            "--reset",
          ],
          { fetchImpl: makeDemoFetch(app, baseUrl) }
        );
        expect(summary.stream).toBe("evlog-1");
        expect(summary.appendedEvents).toBe(40);
        expect(summary.nextId).toBe(40);

        const detailsRes = await app.fetch(new Request("http://local/v1/stream/evlog-1/_details", { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.stream.next_offset).toBe("40");
        expect(details.stream.profile).toBe("evlog");
        expect(details.profile.profile.kind).toBe("evlog");
        expect(details.schema.search.profile).toBe("evlog");
        expect(details.schema.search.primaryTimestampField).toBe("timestamp");
        expect(details.index_status.routing_key_index.configured).toBe(false);
        expect(details.index_status.routing_key_lexicon.configured).toBe(false);
        expect(details.index_status.exact_indexes.map((entry: any) => entry.name)).toEqual(
          expect.arrayContaining(["timestamp", "service", "requestId", "traceId", "status", "duration"])
        );
        expect(details.index_status.search_families).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ family: "col" }),
            expect.objectContaining({ family: "fts" }),
          ])
        );
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
      const root = mkdtempSync(join(tmpdir(), "ds-evlog-ingester-resume-"));
      const baseUrl = "http://127.0.0.1:8787";
      const cfg = makeConfig(root, {
        segmentMaxBytes: 2_048,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 2,
        indexL0SpanSegments: 1,
        indexCheckIntervalMs: 10,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const app = createApp(cfg);
      try {
        await runEvlogIngester(
          ["--url", baseUrl, "--stream", "evlog-1", "--batch-size", "10", "--max-batches", "1", "--reset"],
          { fetchImpl: makeDemoFetch(app, baseUrl) }
        );
        const second = await runEvlogIngester(
          ["--url", baseUrl, "--stream", "evlog-1", "--batch-size", "10", "--max-batches", "1", "--resume-stream"],
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
