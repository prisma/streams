import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { gzipSync } from "node:zlib";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { parseSchemaUpdateResult } from "../src/schema/registry";
import {
  GH_ARCHIVE_ONLY_INDEX_SELECTORS,
  buildGhArchiveArchiveUrl,
  buildGhArchiveIndexedStreamName,
  buildGhArchiveSchemaUpdate,
  buildGhArchiveStreamName,
  normalizeGhArchiveEvent,
  resolveGhArchiveBatchDefaults,
  resolveGhArchiveRangeHours,
  resolveGhArchiveStreamTargets,
  runGhArchiveDemo,
} from "../experiments/demo/gharchive_demo";

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

function mockArchiveResponse(hourLabel: string): Response {
  const createdAt = `${hourLabel.slice(0, 10)}T${hourLabel.slice(11, 13)}:00:00Z`;
  const line = JSON.stringify({
    id: `gh-${hourLabel}`,
    type: hourLabel.endsWith("-00") ? "PushEvent" : "IssuesEvent",
    public: true,
    created_at: createdAt,
    actor: { login: hourLabel.endsWith("-00") ? "renovate[bot]" : "octocat" },
    repo: { name: hourLabel.endsWith("-00") ? "prisma/streams" : "openai/docs" },
    org: { login: hourLabel.endsWith("-00") ? "prisma" : "openai" },
    payload: hourLabel.endsWith("-00")
      ? {
          size: 2,
          commits: [{ message: "ship feature" }, { message: "fix regression" }],
          ref_type: "branch",
        }
      : {
          action: "opened",
          issue: {
            title: "Docs refresh",
            body: "Improve search and aggregation examples",
          },
        },
  });
  return new Response(gzipSync(`${line}\n`), {
    status: 200,
    headers: { "content-type": "application/gzip" },
  });
}

describe("gharchive demo", () => {
  test("builds base and per-index stream names", () => {
    expect(buildGhArchiveStreamName("gharchive-demo", "day")).toBe("gharchive-demo-day");
    expect(buildGhArchiveStreamName("lab", "year")).toBe("lab-year");
    expect(buildGhArchiveIndexedStreamName("gharchive-demo", "day", "fts:message")).toBe(
      "gharchive-demo-day-fts-message"
    );
  });

  test("uses smaller default append batches for the all-range demo", () => {
    expect(resolveGhArchiveBatchDefaults("day")).toEqual({
      batchMaxBytes: 8 * 1024 * 1024,
      batchMaxRecords: 1_000,
    });
    expect(resolveGhArchiveBatchDefaults("all")).toEqual({
      batchMaxBytes: 2 * 1024 * 1024,
      batchMaxRecords: 250,
    });
  });

  test("resolves supported range windows from a fixed clock", () => {
    const now = new Date("2026-03-30T10:45:12.000Z");
    const day = resolveGhArchiveRangeHours("day", now);
    expect(day.hours).toBe(24);
    expect(day.start.toISOString()).toBe("2026-03-29T10:00:00.000Z");
    expect(day.endExclusive.toISOString()).toBe("2026-03-30T10:00:00.000Z");
    expect(buildGhArchiveArchiveUrl(day.start)).toBe("https://data.gharchive.org/2026-03-29-10.json.gz");

    const week = resolveGhArchiveRangeHours("week", now);
    expect(week.hours).toBe(24 * 7);
    expect(week.start.toISOString()).toBe("2026-03-23T10:00:00.000Z");
  });

  test("normalizes push-style gh archive events into the demo envelope", () => {
    const event = normalizeGhArchiveEvent(
      {
        id: 123,
        type: "PushEvent",
        public: true,
        created_at: "2026-03-29T09:00:00Z",
        actor: { login: "renovate[bot]" },
        repo: { name: "prisma/streams" },
        org: { login: "prisma" },
        payload: {
          size: 2,
          ref_type: "branch",
          commits: [{ message: "ship feature" }, { message: "fix regression" }],
        },
      },
      new Date("2026-03-29T09:00:00.000Z"),
      2048
    );
    expect(event).toEqual(
      expect.objectContaining({
        ghArchiveId: "123",
        eventType: "PushEvent",
        repoOwner: "prisma",
        repoName: "prisma/streams",
        orgLogin: "prisma",
        commitCount: 2,
        isBot: true,
        public: true,
        refType: "branch",
        payloadBytes: 2048,
        payloadKb: 2,
      })
    );
    expect(event?.message).toContain("PushEvent");
    expect(event?.body).toContain("ship feature");
  });

  test("installs a schema that is valid and Studio aggregate friendly", () => {
    const parsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate());
    expect(Result.isOk(parsed)).toBe(true);
    if (Result.isError(parsed)) return;
    expect(parsed.value.routingKey).toEqual({ jsonPointer: "/repoName", required: false });
    expect(parsed.value.search?.primaryTimestampField).toBe("eventTime");
    expect(parsed.value.search?.fields.eventTime.kind).toBe("date");
    expect(parsed.value.search?.fields.eventType.exact).toBe(true);
    expect(parsed.value.search?.fields.repoName.prefix).toBeUndefined();
    expect(parsed.value.search?.fields.repoOwner.prefix).toBe(true);
    expect(parsed.value.search?.fields.payloadBytes.aggregatable).toBe(true);
    expect(parsed.value.search?.fields.title.kind).toBe("text");
    expect(parsed.value.search?.fields.message.kind).toBe("text");
    expect(parsed.value.search?.fields.body.kind).toBe("text");
    expect(parsed.value.search?.defaultFields).toEqual([
      { field: "title", boost: 1.5 },
      { field: "message", boost: 1.25 },
      { field: "body", boost: 0.9 },
    ]);
    expect(parsed.value.search?.rollups?.events.dimensions).toEqual(["eventType", "repoOwner", "public", "isBot"]);
    expect(parsed.value.search?.rollups?.events.intervals).toEqual(["1m", "5m", "15m", "1h", "6h", "1d", "7d"]);
    expect(parsed.value.search?.rollups?.events.measures).toEqual({
      events: { kind: "count" },
      payloadBytes: { kind: "summary", field: "payloadBytes", histogram: "log2_v1" },
      commitCount: { kind: "summary", field: "commitCount", histogram: "log2_v1" },
    });
  });

  test("supports a no-index schema mode for raw ingest experiments", () => {
    const parsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ noIndex: true }));
    expect(Result.isOk(parsed)).toBe(true);
    if (Result.isError(parsed)) return;
    expect(parsed.value.routingKey).toEqual({ jsonPointer: "/repoName", required: false });
    expect(parsed.value.search).toBeUndefined();
  });

  test("supports isolated exact, column, fts, and rollup search configs", () => {
    expect(GH_ARCHIVE_ONLY_INDEX_SELECTORS).toContain("exact:actorLogin");
    expect(GH_ARCHIVE_ONLY_INDEX_SELECTORS).toContain("col:payloadBytes");
    expect(GH_ARCHIVE_ONLY_INDEX_SELECTORS).toContain("fts:message");
    expect(GH_ARCHIVE_ONLY_INDEX_SELECTORS).toContain("agg:events");

    const exactParsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ onlyIndex: "exact:actorLogin" }));
    expect(Result.isOk(exactParsed)).toBe(true);
    if (Result.isError(exactParsed)) return;
    expect(Object.keys(exactParsed.value.search?.fields ?? {}).sort()).toEqual(["actorLogin", "eventTime"]);
    expect(exactParsed.value.search?.fields.actorLogin.exact).toBe(true);
    expect(exactParsed.value.search?.rollups).toBeUndefined();
    expect(exactParsed.value.search?.defaultFields).toBeUndefined();

    const columnParsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ onlyIndex: "col:payloadBytes" }));
    expect(Result.isOk(columnParsed)).toBe(true);
    if (Result.isError(columnParsed)) return;
    expect(Object.keys(columnParsed.value.search?.fields ?? {}).sort()).toEqual(["eventTime", "payloadBytes"]);
    expect(columnParsed.value.search?.fields.payloadBytes.column).toBe(true);
    expect(columnParsed.value.search?.fields.eventTime.column).toBeUndefined();

    const ftsParsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ onlyIndex: "fts:eventType" }));
    expect(Result.isOk(ftsParsed)).toBe(true);
    if (Result.isError(ftsParsed)) return;
    expect(Object.keys(ftsParsed.value.search?.fields ?? {}).sort()).toEqual(["eventTime", "eventType"]);
    expect(ftsParsed.value.search?.fields.eventType.prefix).toBe(true);
    expect(ftsParsed.value.search?.fields.eventType.exact).toBeUndefined();

    const aggParsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ onlyIndex: "agg:events" }));
    expect(Result.isOk(aggParsed)).toBe(true);
    if (Result.isError(aggParsed)) return;
    expect(Object.keys(aggParsed.value.search?.fields ?? {}).sort()).toEqual(["commitCount", "eventTime", "payloadBytes"]);
    expect(aggParsed.value.search?.rollups?.events.dimensions).toBeUndefined();
    expect(aggParsed.value.search?.rollups?.events.measures.payloadBytes).toEqual({
      kind: "summary",
      field: "payloadBytes",
      histogram: "log2_v1",
    });
  });

  test("resolves one target stream per selector by default and for repeated onlyindex flags", () => {
    const allTargets = resolveGhArchiveStreamTargets("gharchive-demo", "day");
    expect(allTargets).toHaveLength(GH_ARCHIVE_ONLY_INDEX_SELECTORS.length);
    expect(allTargets[0]?.schema.onlyIndex).toBe(GH_ARCHIVE_ONLY_INDEX_SELECTORS[0]);
    expect(new Set(allTargets.map((target) => target.stream)).size).toBe(allTargets.length);

    const selectedTargets = resolveGhArchiveStreamTargets("gharchive-lab", "week", {
      onlyIndexes: ["exact:ghArchiveId", "fts:message"],
    });
    expect(selectedTargets.map((target) => target.stream)).toEqual([
      "gharchive-lab-week-exact-ghArchiveId",
      "gharchive-lab-week-fts-message",
    ]);

    const noIndexTargets = resolveGhArchiveStreamTargets("gharchive-raw", "month", { noIndex: true });
    expect(noIndexTargets).toEqual([
      {
        stream: "gharchive-raw-month",
        selector: null,
        schema: { noIndex: true },
      },
    ]);
  });

  test(
    "fans out each batch sequentially across per-selector streams, retries the current stream, and never polls details",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      let detailsRequests = 0;
      let sawOverloadAppend = false;
      const appendOrder: string[] = [];
      const exactStream = buildGhArchiveIndexedStreamName("gharchive-e2e", "day", "exact:actorLogin");
      const ftsStream = buildGhArchiveIndexedStreamName("gharchive-e2e", "day", "fts:message");
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            const requestUrl = new URL(url);
            if (requestUrl.pathname.endsWith("/_details")) detailsRequests += 1;
            const method = init?.method ?? (input instanceof Request ? input.method : "GET");
            if (method.toUpperCase() === "POST" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(exactStream)}`) {
              if (!sawOverloadAppend) {
                sawOverloadAppend = true;
                appendOrder.push("exact:backoff");
                return new Response(JSON.stringify({ error: { code: "overloaded", message: "ingest queue full" } }), {
                  status: 429,
                  headers: {
                    "content-type": "application/json",
                    "retry-after": "0",
                  },
                });
              }
              appendOrder.push("exact:ok");
            }
            if (method.toUpperCase() === "POST" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(ftsStream)}`) {
              appendOrder.push("fts:ok");
            }
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            const match = /(\d{4}-\d{2}-\d{2}-\d{2})\.json\.gz$/.exec(url);
            if (!match) return new Response("not found", { status: 404 });
            return mockArchiveResponse(match[1]);
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        const summary = await runGhArchiveDemo([
          "day",
          "--url",
          baseUrl,
          "--stream-prefix",
          "gharchive-e2e",
          "--onlyindex",
          "exact:actorLogin",
          "--onlyindex",
          "fts:message",
        ]);

        expect(summary.streamCount).toBe(2);
        expect(summary.streams).toEqual([exactStream, ftsStream]);
        expect(summary.hours).toBe(24);
        expect(summary.downloadedHours).toBe(24);
        expect(summary.missingHours).toBe(0);
        expect(summary.normalizedRows).toBe(24);
        expect(summary.avgIngestMiBPerSec).toBeGreaterThan(0);
        expect(summary.appendAckMiBPerSec).toBeGreaterThanOrEqual(0);
        expect(summary.appendBackoffCount).toBe(1);
        expect(summary.appendBackoffWaitMs).toBe(0);
        expect(detailsRequests).toBe(0);
        expect(appendOrder.slice(0, 3)).toEqual(["exact:backoff", "exact:ok", "fts:ok"]);

        const exactDetailsRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(exactStream)}/_details`, { method: "GET" })
        );
        expect(exactDetailsRes.status).toBe(200);
        const exactDetails = await exactDetailsRes.json();
        expect(exactDetails.schema.routingKey).toEqual({ jsonPointer: "/repoName", required: false });
        expect(Object.keys(exactDetails.schema.search.fields).sort()).toEqual(["actorLogin", "eventTime"]);
        expect(exactDetails.index_status.exact_indexes).toHaveLength(1);
        expect(exactDetails.index_status.exact_indexes[0].name).toBe("actorLogin");
        expect(exactDetails.index_status.search_families).toEqual([]);

        const ftsDetailsRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(ftsStream)}/_details`, { method: "GET" })
        );
        expect(ftsDetailsRes.status).toBe(200);
        const ftsDetails = await ftsDetailsRes.json();
        expect(ftsDetails.schema.routingKey).toEqual({ jsonPointer: "/repoName", required: false });
        expect(Object.keys(ftsDetails.schema.search.fields).sort()).toEqual(["eventTime", "message"]);
        expect(ftsDetails.index_status.exact_indexes).toEqual([]);
        expect(ftsDetails.index_status.search_families.map((entry: { family: string }) => entry.family)).toEqual(["fts"]);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "retries timed-out appends on the current stream before moving to the next target",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-timeout-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      const appendOrder: string[] = [];
      let sawTimedOutAppend = false;
      const exactStream = buildGhArchiveIndexedStreamName("gharchive-timeout", "day", "exact:actorLogin");
      const ftsStream = buildGhArchiveIndexedStreamName("gharchive-timeout", "day", "fts:message");
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            const requestUrl = new URL(url);
            const method = init?.method ?? (input instanceof Request ? input.method : "GET");
            if (method.toUpperCase() === "POST" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(exactStream)}`) {
              if (!sawTimedOutAppend) {
                sawTimedOutAppend = true;
                appendOrder.push("exact:timeout");
                const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
                return new Promise<Response>((_resolve, reject) => {
                  const rejectAbort = () => reject(new DOMException("Aborted", "AbortError"));
                  if (!signal) {
                    reject(new Error("missing timeout signal"));
                    return;
                  }
                  if (signal.aborted) {
                    rejectAbort();
                    return;
                  }
                  signal.addEventListener("abort", rejectAbort, { once: true });
                });
              }
              appendOrder.push("exact:ok");
            }
            if (method.toUpperCase() === "POST" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(ftsStream)}`) {
              appendOrder.push("fts:ok");
            }
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            const match = /(\d{4}-\d{2}-\d{2}-\d{2})\.json\.gz$/.exec(url);
            if (!match) return new Response("not found", { status: 404 });
            return mockArchiveResponse(match[1]);
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        const summary = await runGhArchiveDemo(
          [
            "day",
            "--url",
            baseUrl,
            "--stream-prefix",
            "gharchive-timeout",
            "--onlyindex",
            "exact:actorLogin",
            "--onlyindex",
            "fts:message",
          ],
          {
            requestTimeoutMs: 20,
            timeoutRetryDelayMs: 0,
          }
        );

        expect(sawTimedOutAppend).toBe(true);
        expect(summary.appendBackoffCount).toBe(1);
        expect(summary.appendBackoffWaitMs).toBe(0);
        expect(appendOrder.slice(0, 3)).toEqual(["exact:timeout", "exact:ok", "fts:ok"]);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "retries server timeout responses on the current stream before moving to the next target",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-server-timeout-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      const appendOrder: string[] = [];
      let sawServerTimeout = false;
      const exactStream = buildGhArchiveIndexedStreamName("gharchive-server-timeout", "day", "exact:actorLogin");
      const ftsStream = buildGhArchiveIndexedStreamName("gharchive-server-timeout", "day", "fts:message");
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            const requestUrl = new URL(url);
            const method = init?.method ?? (input instanceof Request ? input.method : "GET");
            if (method.toUpperCase() === "POST" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(exactStream)}`) {
              if (!sawServerTimeout) {
                sawServerTimeout = true;
                appendOrder.push("exact:server-timeout");
                return new Response(JSON.stringify({ error: { code: "request_timeout", message: "request timed out" } }), {
                  status: 408,
                  headers: { "content-type": "application/json" },
                });
              }
              appendOrder.push("exact:ok");
            }
            if (method.toUpperCase() === "POST" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(ftsStream)}`) {
              appendOrder.push("fts:ok");
            }
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            const match = /(\d{4}-\d{2}-\d{2}-\d{2})\.json\.gz$/.exec(url);
            if (!match) return new Response("not found", { status: 404 });
            return mockArchiveResponse(match[1]);
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        const summary = await runGhArchiveDemo(
          [
            "day",
            "--url",
            baseUrl,
            "--stream-prefix",
            "gharchive-server-timeout",
            "--onlyindex",
            "exact:actorLogin",
            "--onlyindex",
            "fts:message",
          ],
          {
            timeoutRetryDelayMs: 0,
          }
        );

        expect(sawServerTimeout).toBe(true);
        expect(summary.appendBackoffCount).toBe(1);
        expect(summary.appendBackoffWaitMs).toBe(0);
        expect(appendOrder.slice(0, 3)).toEqual(["exact:server-timeout", "exact:ok", "fts:ok"]);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "retries overloaded stream creation on the current stream before moving to the next target",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-create-backoff-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      const createOrder: string[] = [];
      let sawCreateBackoff = false;
      const exactStream = buildGhArchiveIndexedStreamName("gharchive-create-backoff", "day", "exact:actorLogin");
      const ftsStream = buildGhArchiveIndexedStreamName("gharchive-create-backoff", "day", "fts:message");
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            const requestUrl = new URL(url);
            const method = init?.method ?? (input instanceof Request ? input.method : "GET");
            if (method.toUpperCase() === "PUT" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(exactStream)}`) {
              if (!sawCreateBackoff) {
                sawCreateBackoff = true;
                createOrder.push("exact:create-backoff");
                return new Response(JSON.stringify({ error: { code: "overloaded", message: "ingest queue full" } }), {
                  status: 429,
                  headers: {
                    "content-type": "application/json",
                    "retry-after": "0",
                  },
                });
              }
              createOrder.push("exact:create-ok");
            }
            if (method.toUpperCase() === "PUT" && requestUrl.pathname === `/v1/stream/${encodeURIComponent(ftsStream)}`) {
              createOrder.push("fts:create-ok");
            }
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            const match = /(\d{4}-\d{2}-\d{2}-\d{2})\.json\.gz$/.exec(url);
            if (!match) return new Response("not found", { status: 404 });
            return mockArchiveResponse(match[1]);
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        const summary = await runGhArchiveDemo([
          "day",
          "--url",
          baseUrl,
          "--stream-prefix",
          "gharchive-create-backoff",
          "--onlyindex",
          "exact:actorLogin",
          "--onlyindex",
          "fts:message",
        ]);

        expect(summary.streamCount).toBe(2);
        expect(sawCreateBackoff).toBe(true);
        expect(createOrder.slice(0, 3)).toEqual(["exact:create-backoff", "exact:create-ok", "fts:create-ok"]);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "runs end to end without indexes when --noindex is set",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-noindex-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            const match = /(\d{4}-\d{2}-\d{2}-\d{2})\.json\.gz$/.exec(url);
            if (!match) return new Response("not found", { status: 404 });
            return mockArchiveResponse(match[1]);
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        const summary = await runGhArchiveDemo([
          "day",
          "--url",
          baseUrl,
          "--stream-prefix",
          "gharchive-noindex",
          "--noindex",
        ]);

        expect(summary.streamCount).toBe(1);
        expect(summary.streams).toEqual(["gharchive-noindex-day"]);

        const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(summary.streams[0]!)}/_details`, { method: "GET" }));
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.schema.routingKey).toEqual({ jsonPointer: "/repoName", required: false });
        expect(details.schema.search).toBeUndefined();
        expect(details.index_status.exact_indexes).toEqual([]);
        expect(details.index_status.search_families).toEqual([]);
        expect(details.index_status.bundled_companions.object_count).toBe(0);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "skips missing gh archive hours and completes when other hours are available",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-missing-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      const realStderrWrite = process.stderr.write.bind(process.stderr);
      const stderrLines: string[] = [];
      let missingInjected = false;
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        process.stderr.write = ((chunk: string | Uint8Array) => {
          stderrLines.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"));
          return true;
        }) as typeof process.stderr.write;
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            const match = /(\d{4}-\d{2}-\d{2}-\d{2})\.json\.gz$/.exec(url);
            if (!match) return new Response("not found", { status: 404 });
            if (!missingInjected) {
              missingInjected = true;
              return new Response("not found", { status: 404 });
            }
            return mockArchiveResponse(match[1]);
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        const summary = await runGhArchiveDemo([
          "day",
          "--url",
          baseUrl,
          "--stream-prefix",
          "gharchive-missing-hour",
          "--onlyindex",
          "fts:message",
        ]);

        expect(missingInjected).toBe(true);
        expect(summary.hours).toBe(24);
        expect(summary.downloadedHours).toBe(23);
        expect(summary.missingHours).toBe(1);
        expect(summary.normalizedRows).toBe(23);
        expect(stderrLines.join("")).toContain("[gharchive-demo] missing hour");
      } finally {
        process.stderr.write = realStderrWrite;
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "fails clearly when no gh archive hours are available for the requested range",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-none-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      const cfg = makeConfig(root, {
        segmentMaxBytes: 512,
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
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
          if (url.startsWith(baseUrl)) {
            if (input instanceof Request) return app.fetch(new Request(input, init));
            return app.fetch(new Request(url, init));
          }
          if (url.startsWith("https://data.gharchive.org/")) {
            return new Response("not found", { status: 404 });
          }
          throw new Error(`unexpected fetch url: ${url}`);
        }) as typeof fetch;

        await expect(
          runGhArchiveDemo([
            "day",
            "--url",
            baseUrl,
            "--stream-prefix",
            "gharchive-no-hours",
          ])
        ).rejects.toThrow("no GH Archive hours were available");
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );
});
