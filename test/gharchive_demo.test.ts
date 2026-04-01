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
  buildGhArchiveSchemaUpdate,
  buildGhArchiveStreamName,
  buildGhArchiveArchiveUrl,
    normalizeGhArchiveEvent,
    computeDemoReadiness,
    resolveGhArchiveRangeHours,
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
  test("builds range suffix stream names", () => {
    expect(buildGhArchiveStreamName("gharchive-demo", "day")).toBe("gharchive-demo-day");
    expect(buildGhArchiveStreamName("lab", "year")).toBe("lab-year");
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

    const exactPrefixParsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ onlyIndex: "exact:eventType" }));
    expect(Result.isOk(exactPrefixParsed)).toBe(true);
    if (Result.isError(exactPrefixParsed)) return;
    expect(exactPrefixParsed.value.search?.fields.eventType.exact).toBe(true);
    expect(exactPrefixParsed.value.search?.fields.eventType.prefix).toBeUndefined();

    const columnExactParsed = parseSchemaUpdateResult(buildGhArchiveSchemaUpdate({ onlyIndex: "col:public" }));
    expect(Result.isOk(columnExactParsed)).toBe(true);
    if (Result.isError(columnExactParsed)) return;
    expect(columnExactParsed.value.search?.fields.public.column).toBe(true);
    expect(columnExactParsed.value.search?.fields.public.exact).toBeUndefined();

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

  test("supports combining multiple --onlyindex selectors into one minimal schema", () => {
    const parsed = parseSchemaUpdateResult(
      buildGhArchiveSchemaUpdate({
        onlyIndexes: ["exact:ghArchiveId", "fts:message", "agg:events"],
      })
    );
    expect(Result.isOk(parsed)).toBe(true);
    if (Result.isError(parsed)) return;
    expect(Object.keys(parsed.value.search?.fields ?? {}).sort()).toEqual([
      "commitCount",
      "eventTime",
      "ghArchiveId",
      "message",
      "payloadBytes",
    ]);
    expect(parsed.value.search?.fields.ghArchiveId.exact).toBe(true);
    expect(parsed.value.search?.fields.message.kind).toBe("text");
    expect(parsed.value.search?.rollups?.events.measures.commitCount).toEqual({
      kind: "summary",
      field: "commitCount",
      histogram: "log2_v1",
    });
  });

  test(
    "runs end to end against a local server and waits for details readiness",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-"));
      const baseUrl = "http://127.0.0.1:8787";
      const realFetch = globalThis.fetch;
      let sawOverloadAppend = false;
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
            const method =
              init?.method ??
              (input instanceof Request ? input.method : "GET");
            const appendPath = `/v1/stream/${encodeURIComponent("gharchive-e2e-day")}`;
            if (!sawOverloadAppend && method.toUpperCase() === "POST" && new URL(url).pathname === appendPath) {
              sawOverloadAppend = true;
              return new Response(JSON.stringify({ error: { code: "overloaded", message: "ingest queue full" } }), {
                status: 429,
                headers: {
                  "content-type": "application/json",
                  "retry-after": "0",
                },
              });
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
          "--ready-timeout-ms",
          "10000",
        ]);

        expect(summary.stream).toBe("gharchive-e2e-day");
        expect(summary.hours).toBe(24);
        expect(summary.downloadedHours).toBe(24);
        expect(summary.missingHours).toBe(0);
        expect(summary.normalizedRows).toBe(24);
        expect(sawOverloadAppend).toBe(true);
        expect(summary.ready).toBe(true);
        expect(summary.uploadedReady).toBe(true);
        expect(summary.exactIndexesReady).toBe(false);
        expect(summary.searchFamiliesReady).toBe(true);
        expect(summary.avgIngestMiBPerSec).toBeGreaterThan(0);
        expect(summary.downloadMiBPerSec).toBeGreaterThanOrEqual(0);
        expect(summary.normalizeMiBPerSec).toBeGreaterThanOrEqual(0);
        expect(summary.appendAckMiBPerSec).toBeGreaterThanOrEqual(0);
        expect(summary.timeToSearchReadyMs).toBeGreaterThanOrEqual(0);
        expect(BigInt(summary.totalSizeBytes)).toBeGreaterThan(0n);

        const detailsRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(summary.stream)}/_details`, {
            method: "GET",
          })
        );
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.stream.name).toBe(summary.stream);
        expect(details.profile.profile.kind).toBe("generic");
        expect(details.schema.search.primaryTimestampField).toBe("eventTime");
        expect(details.schema.search.rollups.events.measures.events.kind).toBe("count");
        expect(details.schema.search.rollups.events.measures.payloadBytes.kind).toBe("summary");
        expect(details.index_status.bundled_companions.fully_indexed_uploaded_segments).toBe(true);
        expect(
          details.index_status.search_families.every(
            (family: { fully_indexed_uploaded_segments?: boolean }) => family.fully_indexed_uploaded_segments === true
          )
        ).toBe(true);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test("treats bundled search readiness as sufficient even when exact indexes are still catching up", () => {
    const readiness = computeDemoReadiness({
      stream: {
        sealed_through: "9",
        total_size_bytes: "100",
        uploaded_through: "9",
      },
      profile: { apiVersion: "durable.streams/profile/v1", profile: { kind: "generic" } },
      schema: { apiVersion: "durable.streams/schema-registry/v1", schema: "gharchive-demo-day", currentVersion: 1 },
      index_status: {
        bundled_companions: { fully_indexed_uploaded_segments: true },
        exact_indexes: [{ name: "actorLogin", indexed_segment_count: 8, fully_indexed_uploaded_segments: false }],
        search_families: [{ family: "fts", covered_segment_count: 10, stale_segment_count: 0, fully_indexed_uploaded_segments: true }],
      },
    } as any);

    expect(readiness.uploadedReady).toBe(true);
    expect(readiness.bundledReady).toBe(true);
    expect(readiness.searchReady).toBe(true);
    expect(readiness.exactReady).toBe(false);
    expect(readiness.ready).toBe(true);
  });

  test("treats uploaded readiness as sufficient when search indexing is disabled", () => {
    const readiness = computeDemoReadiness({
      stream: {
        sealed_through: "9",
        total_size_bytes: "100",
        uploaded_through: "9",
      },
      profile: { apiVersion: "durable.streams/profile/v1", profile: { kind: "generic" } },
      schema: { apiVersion: "durable.streams/schema-registry/v1", schema: "gharchive-demo-day", currentVersion: 1 },
      index_status: {
        exact_indexes: [],
        search_families: [],
      },
    } as any);

    expect(readiness.uploadedReady).toBe(true);
    expect(readiness.bundledReady).toBe(true);
    expect(readiness.searchReady).toBe(true);
    expect(readiness.exactReady).toBe(true);
    expect(readiness.ready).toBe(true);
  });

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
          "--ready-timeout-ms",
          "10000",
          "--noindex",
        ]);

        expect(summary.stream).toBe("gharchive-noindex-day");
        expect(summary.ready).toBe(true);
        expect(summary.uploadedReady).toBe(true);
        expect(summary.bundledCompanionsReady).toBe(true);
        expect(summary.searchFamiliesReady).toBe(true);
        expect(summary.exactIndexesReady).toBe(true);

        const detailsRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(summary.stream)}/_details`, {
            method: "GET",
          })
        );
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(details.schema.search).toBeUndefined();
        expect(details.index_status.exact_indexes).toEqual([]);
        expect(details.index_status.search_families).toEqual([]);
        expect(details.index_status.bundled_companions.object_count).toBe(0);
        expect(details.index_status.bundled_companions.fully_indexed_uploaded_segments).toBe(true);
      } finally {
        globalThis.fetch = realFetch;
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    20_000
  );

  test(
    "runs end to end with a single exact index when --onlyindex is set",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-onlyindex-"));
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
          "gharchive-onlyindex",
          "--ready-timeout-ms",
          "10000",
          "--onlyindex",
          "exact:actorLogin",
        ]);

        expect(summary.stream).toBe("gharchive-onlyindex-day");
        expect(summary.ready).toBe(true);
        expect(summary.uploadedReady).toBe(true);
        expect(summary.bundledCompanionsReady).toBe(true);
        expect(summary.searchFamiliesReady).toBe(true);

        const detailsRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(summary.stream)}/_details`, {
            method: "GET",
          })
        );
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(Object.keys(details.schema.search.fields).sort()).toEqual(["actorLogin", "eventTime"]);
        expect(details.index_status.exact_indexes).toHaveLength(1);
        expect(details.index_status.exact_indexes[0].name).toBe("actorLogin");
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
    "runs end to end with repeated --onlyindex flags for a combined schema",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-gharchive-demo-multi-onlyindex-"));
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
          "gharchive-multi-onlyindex",
          "--ready-timeout-ms",
          "10000",
          "--onlyindex",
          "exact:ghArchiveId",
          "--onlyindex",
          "fts:message,agg:events",
        ]);

        expect(summary.ready).toBe(true);

        const detailsRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(summary.stream)}/_details`, {
            method: "GET",
          })
        );
        expect(detailsRes.status).toBe(200);
        const details = await detailsRes.json();
        expect(Object.keys(details.schema.search.fields).sort()).toEqual([
          "commitCount",
          "eventTime",
          "ghArchiveId",
          "message",
          "payloadBytes",
        ]);
        expect(details.index_status.exact_indexes).toHaveLength(1);
        expect(details.index_status.exact_indexes[0].name).toBe("ghArchiveId");
        expect(details.index_status.search_families.map((entry: { family: string }) => entry.family).sort()).toEqual([
          "agg",
          "fts",
        ]);
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
          "--ready-timeout-ms",
          "10000",
        ]);

        expect(missingInjected).toBe(true);
        expect(summary.stream).toBe("gharchive-missing-hour-day");
        expect(summary.hours).toBe(24);
        expect(summary.downloadedHours).toBe(23);
        expect(summary.missingHours).toBe(1);
        expect(summary.normalizedRows).toBe(23);
        expect(summary.ready).toBe(true);
        expect(summary.uploadedReady).toBe(true);
      } finally {
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
            "--ready-timeout-ms",
            "10000",
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
