import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

const RUN = process.env.SEARCH_PERF_REPRO === "1";
const t = RUN ? test : test.skip;
const EXPECT_SLOW = process.env.SEARCH_PERF_EXPECT_SLOW === "1";

const TIMEOUT_MS = envNumber("SEARCH_PERF_TIMEOUT_MS", 900_000);
const MIN_CASE_MS = envNumber("SEARCH_PERF_MIN_CASE_MS", 2_000, { allowZero: true });
const DEFAULT_SORT_SEGMENTS = envNumber("SEARCH_PERF_DEFAULT_SORT_SEGMENTS", 16);
const DEFAULT_SORT_ROWS_PER_SEGMENT = envNumber("SEARCH_PERF_DEFAULT_SORT_ROWS_PER_SEGMENT", 2_048);
const DEFAULT_SORT_PAYLOAD_BYTES = envNumber("SEARCH_PERF_DEFAULT_SORT_PAYLOAD_BYTES", 4 * 1024);
const REVERSE_ROWS = envNumber("SEARCH_PERF_REVERSE_ROWS", 32_768);
const REVERSE_PAYLOAD_BYTES = envNumber("SEARCH_PERF_REVERSE_PAYLOAD_BYTES", 8 * 1024);
const SMALL_STREAM_ROWS_PER_SEGMENT = envNumber("SEARCH_PERF_SMALL_ROWS_PER_SEGMENT", 32_768);
const SMALL_STREAM_PAYLOAD_BYTES = envNumber("SEARCH_PERF_SMALL_PAYLOAD_BYTES", 8 * 1024);
const WAL_TAIL_ROWS = envNumber("SEARCH_PERF_WAL_TAIL_ROWS", 32_768);
const WAL_TAIL_PAYLOAD_BYTES = envNumber("SEARCH_PERF_WAL_TAIL_PAYLOAD_BYTES", 4 * 1024);
const EXACT_ONLY_ROWS = envNumber("SEARCH_PERF_EXACT_ONLY_ROWS", 32_768);
const EXACT_ONLY_PAYLOAD_BYTES = envNumber("SEARCH_PERF_EXACT_ONLY_PAYLOAD_BYTES", 4 * 1024);
const APPEND_BATCH_ROWS = envNumber("SEARCH_PERF_APPEND_BATCH_ROWS", 512);
const BLOCK_MAX_BYTES = envNumber("SEARCH_PERF_BLOCK_MAX_BYTES", 64 * 1024);

function envNumber(name: string, fallback: number, options: { allowZero?: boolean } = {}): number {
  const raw = process.env[name];
  if (raw == null || raw.trim() === "") return fallback;
  const parsed = Number(raw);
  const valid = Number.isFinite(parsed) && (options.allowZero ? parsed >= 0 : parsed > 0);
  if (!valid) throw new Error(`${name} must be ${options.allowZero ? "a non-negative" : "a positive"} number`);
  return parsed;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

type PerfFixture = {
  app: ReturnType<typeof createApp>;
  root: string;
  store: MockR2Store;
  stream: string;
  rows: number;
  segments: number;
  payloadBytes: number;
};

function padFor(index: number, targetChars: number): string {
  const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._-";
  let state = (0x9e3779b9 ^ index) >>> 0;
  let out = "";
  while (out.length < targetChars) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    out += alphabet[state % alphabet.length]!;
  }
  return out.slice(0, targetChars);
}

function buildEvlogPayload(index: number, targetBytes: number, environment = "staging"): Uint8Array {
  const encoder = new TextEncoder();
  const timestamp = new Date(Date.UTC(2026, 0, 1, 0, 0, 0) + index * 1000).toISOString();
  const base = {
    timestamp,
    level: index % 11 === 0 ? "error" : "info",
    service: `svc-${String(index % 12).padStart(2, "0")}`,
    environment,
    version: "2026.04.24",
    region: index % 2 === 0 ? "us-east-1" : "eu-west-1",
    requestId: `req-${String(index).padStart(10, "0")}`,
    traceId: `trace-${String(Math.floor(index / 4)).padStart(10, "0")}`,
    spanId: `span-${String(index).padStart(10, "0")}`,
    method: index % 3 === 0 ? "POST" : "GET",
    path: `/api/resource/${index % 64}`,
    status: index % 17 === 0 ? 500 : 200,
    duration: 10 + (index % 500) / 10,
    message: `seed event ${index} staging request list candidate`,
    why: null,
    fix: null,
    link: null,
    sampling: null,
    redaction: { keys: [] },
    context: { seed: index, pad: "" },
  };
  const baseBytes = encoder.encode(JSON.stringify(base)).byteLength;
  const padChars = targetBytes - baseBytes;
  if (padChars < 0) throw new Error(`target payload ${targetBytes} is too small for evlog seed row (${baseBytes})`);
  const bytes = encoder.encode(JSON.stringify({ ...base, context: { seed: index, pad: padFor(index, padChars) } }));
  if (bytes.byteLength !== targetBytes) {
    throw new Error(`expected ${targetBytes} bytes, got ${bytes.byteLength}`);
  }
  return bytes;
}

function buildExactOnlyPayload(index: number, targetBytes: number): Uint8Array {
  const encoder = new TextEncoder();
  const timestamp = new Date(Date.UTC(2026, 0, 1, 0, 0, 0) + index * 1000).toISOString();
  const base = {
    eventTime: timestamp,
    customerId: `cust-${String(index).padStart(10, "0")}`,
    message: `exact-only seed event ${index}`,
    context: { seed: index, pad: "" },
  };
  const baseBytes = encoder.encode(JSON.stringify(base)).byteLength;
  const padChars = targetBytes - baseBytes;
  if (padChars < 0) throw new Error(`target payload ${targetBytes} is too small for exact-only seed row (${baseBytes})`);
  const bytes = encoder.encode(JSON.stringify({ ...base, context: { seed: index, pad: padFor(index, padChars) } }));
  if (bytes.byteLength !== targetBytes) {
    throw new Error(`expected ${targetBytes} bytes, got ${bytes.byteLength}`);
  }
  return bytes;
}

async function createEvlogStream(app: ReturnType<typeof createApp>, stream: string): Promise<void> {
  let res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  expect([200, 201]).toContain(res.status);

  res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_profile`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "evlog" },
      }),
    })
  );
  expect(res.status).toBe(200);
}

async function createExactOnlyStream(app: ReturnType<typeof createApp>, stream: string): Promise<void> {
  let res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  expect([200, 201]).toContain(res.status);

  res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        schema: { type: "object", additionalProperties: true },
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
            customerId: {
              kind: "keyword",
              bindings: [{ version: 1, jsonPointer: "/customerId" }],
              exact: true,
              exists: true,
              sortable: true,
            },
          },
        },
      }),
    })
  );
  expect(res.status).toBe(200);
}

function appendSeedRows(
  app: ReturnType<typeof createApp>,
  stream: string,
  rows: number,
  payloadBytes: number,
  batchRows: number
): void {
  let nextOffset = app.deps.db.getStream(stream)?.next_offset ?? 0n;
  let appendBaseMs = Date.parse("2026-01-01T00:00:00.000Z");
  for (let start = 0; start < rows; start += batchRows) {
    const count = Math.min(batchRows, rows - start);
    const batch = Array.from({ length: count }, (_, localIndex) => {
      const index = start + localIndex;
      return {
        routingKey: null,
        contentType: "application/json",
        payload: buildEvlogPayload(index, payloadBytes),
        appendMs: BigInt(appendBaseMs + index),
      };
    });
    const append = app.deps.db.appendWalRows({
      stream,
      startOffset: nextOffset,
      expectedOffset: nextOffset,
      baseAppendMs: BigInt(appendBaseMs + start),
      rows: batch,
    });
    expect(Result.isOk(append)).toBe(true);
    if (Result.isError(append)) throw new Error(append.error.kind);
    nextOffset = append.value.lastOffset + 1n;
  }
}

function appendExactOnlyRows(
  app: ReturnType<typeof createApp>,
  stream: string,
  rows: number,
  payloadBytes: number,
  batchRows: number
): void {
  let nextOffset = app.deps.db.getStream(stream)?.next_offset ?? 0n;
  const appendBaseMs = Date.parse("2026-01-01T00:00:00.000Z");
  for (let start = 0; start < rows; start += batchRows) {
    const count = Math.min(batchRows, rows - start);
    const batch = Array.from({ length: count }, (_, localIndex) => {
      const index = start + localIndex;
      return {
        routingKey: null,
        contentType: "application/json",
        payload: buildExactOnlyPayload(index, payloadBytes),
        appendMs: BigInt(appendBaseMs + index),
      };
    });
    const append = app.deps.db.appendWalRows({
      stream,
      startOffset: nextOffset,
      expectedOffset: nextOffset,
      baseAppendMs: BigInt(appendBaseMs + start),
      rows: batch,
    });
    expect(Result.isOk(append)).toBe(true);
    if (Result.isError(append)) throw new Error(append.error.kind);
    nextOffset = append.value.lastOffset + 1n;
  }
}

async function waitForUploadedCompanions(
  app: ReturnType<typeof createApp>,
  stream: string,
  expectedSegments: number,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row = app.deps.db.getStream(stream);
    const companions = app.deps.db.listSearchSegmentCompanions(stream);
    const fullySealed =
      !!row &&
      row.next_offset > 0n &&
      row.sealed_through === row.next_offset - 1n &&
      row.pending_bytes === 0n &&
      row.pending_rows === 0n;
    if (
      row &&
      fullySealed &&
      row.uploaded_segment_count === expectedSegments &&
      app.deps.db.countUploadedSegments(stream) === expectedSegments &&
      row.uploaded_through === row.next_offset - 1n &&
      app.deps.db.getSearchCompanionPlan(stream) &&
      companions.length === expectedSegments
    ) {
      return;
    }
    app.deps.indexer?.enqueue(stream);
    await sleep(50);
  }
  const row = app.deps.db.getStream(stream);
  throw new Error(
    `timeout waiting for fixture: expectedSegments=${expectedSegments} uploaded=${row?.uploaded_segment_count ?? 0} ` +
      `sealed=${row?.sealed_through?.toString() ?? "missing"} next=${row?.next_offset?.toString() ?? "missing"} ` +
      `companions=${app.deps.db.listSearchSegmentCompanions(stream).length}`
  );
}

async function buildFixture(args: {
  stream: string;
  segments: number;
  rowsPerSegment: number;
  payloadBytes: number;
  indexL0SpanSegments: number;
}): Promise<PerfFixture> {
  const root = mkdtempSync(join(tmpdir(), `ds-search-perf-${args.stream}-`));
  const store = new MockR2Store({
    maxInMemoryBytes: 1 * 1024 * 1024,
    spillDir: `${root}/mock-r2`,
  });
  const totalRows = args.segments * args.rowsPerSegment;
  const commonConfig = {
    segmentTargetRows: args.rowsPerSegment,
    segmentMaxBytes: args.rowsPerSegment * args.payloadBytes * 4,
    blockMaxBytes: BLOCK_MAX_BYTES,
    indexL0SpanSegments: args.indexL0SpanSegments,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    searchWalOverlayQuietPeriodMs: 0,
  } satisfies Partial<Config>;

  let buildApp: ReturnType<typeof createApp> | null = createApp(
    makeConfig(root, {
      ...commonConfig,
      segmentCheckIntervalMs: 5,
      uploadIntervalMs: 5,
      uploadConcurrency: 4,
      indexCheckIntervalMs: 5,
    }),
    store
  );
  try {
    await createEvlogStream(buildApp, args.stream);
    appendSeedRows(buildApp, args.stream, totalRows, args.payloadBytes, APPEND_BATCH_ROWS);
    await waitForUploadedCompanions(buildApp, args.stream, args.segments, TIMEOUT_MS);
  } finally {
    buildApp?.close();
    buildApp = null;
  }

  const app = createApp(
    makeConfig(root, {
      ...commonConfig,
      segmentCheckIntervalMs: 60_000,
      uploadIntervalMs: 60_000,
      indexCheckIntervalMs: 60_000,
    }),
    store
  );
  return {
    app,
    root,
    store,
    stream: args.stream,
    rows: totalRows,
    segments: args.segments,
    payloadBytes: args.payloadBytes,
  };
}

async function buildWalTailFixture(args: { stream: string; rows: number; payloadBytes: number }): Promise<PerfFixture> {
  const root = mkdtempSync(join(tmpdir(), `ds-search-perf-${args.stream}-`));
  const store = new MockR2Store({
    maxInMemoryBytes: 1 * 1024 * 1024,
    spillDir: `${root}/mock-r2`,
  });
  const app = createApp(
    makeConfig(root, {
      segmentTargetRows: args.rows,
      segmentMaxBytes: args.rows * args.payloadBytes * 4,
      blockMaxBytes: BLOCK_MAX_BYTES,
      searchWalOverlayQuietPeriodMs: 0,
      searchWalOverlayMaxBytes: args.rows * args.payloadBytes * 4,
      segmentCheckIntervalMs: 60_000,
      uploadIntervalMs: 60_000,
      indexCheckIntervalMs: 60_000,
    }),
    store
  );
  await createEvlogStream(app, args.stream);
  appendSeedRows(app, args.stream, args.rows, args.payloadBytes, APPEND_BATCH_ROWS);
  return {
    app,
    root,
    store,
    stream: args.stream,
    rows: args.rows,
    segments: 0,
    payloadBytes: args.payloadBytes,
  };
}

async function buildExactOnlyFixture(args: { stream: string; rows: number; payloadBytes: number }): Promise<PerfFixture> {
  const root = mkdtempSync(join(tmpdir(), `ds-search-perf-${args.stream}-`));
  const store = new MockR2Store({
    maxInMemoryBytes: 1 * 1024 * 1024,
    spillDir: `${root}/mock-r2`,
  });
  const commonConfig = {
    segmentTargetRows: args.rows,
    segmentMaxBytes: args.rows * args.payloadBytes * 4,
    blockMaxBytes: BLOCK_MAX_BYTES,
    indexL0SpanSegments: 16,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    searchWalOverlayQuietPeriodMs: 0,
  } satisfies Partial<Config>;
  let buildApp: ReturnType<typeof createApp> | null = createApp(
    makeConfig(root, {
      ...commonConfig,
      segmentCheckIntervalMs: 5,
      uploadIntervalMs: 5,
      indexCheckIntervalMs: 5,
    }),
    store
  );
  try {
    await createExactOnlyStream(buildApp, args.stream);
    appendExactOnlyRows(buildApp, args.stream, args.rows, args.payloadBytes, APPEND_BATCH_ROWS);
    await waitForUploadedCompanions(buildApp, args.stream, 1, TIMEOUT_MS);
  } finally {
    buildApp?.close();
    buildApp = null;
  }

  const app = createApp(
    makeConfig(root, {
      ...commonConfig,
      segmentCheckIntervalMs: 60_000,
      uploadIntervalMs: 60_000,
      indexCheckIntervalMs: 60_000,
    }),
    store
  );
  return {
    app,
    root,
    store,
    stream: args.stream,
    rows: args.rows,
    segments: 1,
    payloadBytes: args.payloadBytes,
  };
}

async function measuredSearch(
  app: ReturnType<typeof createApp>,
  stream: string,
  requestBody: Record<string, unknown>
): Promise<{ elapsedMs: number; parseCalls: number; body: any }> {
  const originalParse = JSON.parse;
  let parseCalls = 0;
  JSON.parse = ((text: string, reviver?: (this: unknown, key: string, value: unknown) => unknown) => {
    parseCalls += 1;
    return originalParse(text, reviver);
  }) as typeof JSON.parse;

  const started = performance.now();
  let res: Response;
  let text: string;
  try {
    res = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_search`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(requestBody),
      })
    );
    text = await res.text();
  } finally {
    JSON.parse = originalParse;
  }
  const elapsedMs = performance.now() - started;
  expect(res!.status).toBe(200);
  return { elapsedMs, parseCalls, body: originalParse(text!) };
}

function expectMultiSecondRuntime(label: string, elapsedMs: number): void {
  if (!EXPECT_SLOW || MIN_CASE_MS <= 0) return;
  expect(
    elapsedMs,
    `${label} completed in ${elapsedMs.toFixed(2)}ms; increase fixture size or lower SEARCH_PERF_MIN_CASE_MS for this machine`
  ).toBeGreaterThanOrEqual(MIN_CASE_MS);
}

function logPerfCase(label: string, fixture: PerfFixture, result: { elapsedMs: number; parseCalls: number; body: any }): void {
  const coverage = result.body.coverage ?? {};
  // eslint-disable-next-line no-console
  console.log(
    `[search-perf-repro] ${label} rows=${fixture.rows} segments=${fixture.segments} payloadBytes=${fixture.payloadBytes} ` +
      `elapsedMs=${result.elapsedMs.toFixed(2)} tookMs=${result.body.took_ms ?? result.body.tookMs ?? "n/a"} ` +
      `parseCalls=${result.parseCalls} indexedSegments=${coverage.indexed_segments ?? coverage.indexedSegments ?? "n/a"} ` +
      `indexedSegmentTimeMs=${coverage.indexed_segment_time_ms ?? coverage.indexedSegmentTimeMs ?? "n/a"} ` +
      `ftsDecodeMs=${coverage.fts_decode_ms ?? coverage.ftsDecodeMs ?? "n/a"} ` +
      `candidateDocIds=${coverage.candidate_doc_ids ?? "n/a"} decodedRecords=${coverage.decoded_records ?? "n/a"} ` +
      `jsonParseTimeMs=${coverage.json_parse_time_ms ?? "n/a"} segmentBytesFetched=${coverage.segment_payload_bytes_fetched ?? "n/a"} ` +
      `sortTimeMs=${coverage.sort_time_ms ?? "n/a"} peakHitsHeld=${coverage.peak_hits_held ?? "n/a"} ` +
      `families=${JSON.stringify(coverage.index_families_used ?? [])}`
  );
}

describe("search performance repro cases", () => {
  t(
    "default non-scoring evlog filter measures broad event-list query cost",
    async () => {
      const fixture = await buildFixture({
        stream: "perf-default-sort",
        segments: DEFAULT_SORT_SEGMENTS,
        rowsPerSegment: DEFAULT_SORT_ROWS_PER_SEGMENT,
        payloadBytes: DEFAULT_SORT_PAYLOAD_BYTES,
        indexL0SpanSegments: 2,
      });
      try {
        const result = await measuredSearch(fixture.app, fixture.stream, {
          q: 'environment:"staging"',
          size: 100,
        });
        logPerfCase("default-non-scoring-sort", fixture, result);

        expect(result.body.hits).toHaveLength(100);
        expect(result.body.coverage.index_families_used).toContain("fts");
        expectMultiSecondRuntime("default timestamp sort broad filter", result.elapsedMs);
      } finally {
        fixture.app.close();
        rmSync(fixture.root, { recursive: true, force: true });
      }
    },
    TIMEOUT_MS
  );

  t(
    "offset-desc broad filter measures newest-segment first-page cost",
    async () => {
      const fixture = await buildFixture({
        stream: "perf-reverse-offset",
        segments: 1,
        rowsPerSegment: REVERSE_ROWS,
        payloadBytes: REVERSE_PAYLOAD_BYTES,
        indexL0SpanSegments: 16,
      });
      try {
        const result = await measuredSearch(fixture.app, fixture.stream, {
          q: 'environment:"staging"',
          size: 1,
          sort: ["offset:desc"],
        });
        logPerfCase("offset-desc-full-segment-decode", fixture, result);

        expect(result.body.hits).toHaveLength(1);
        expect(result.parseCalls).toBeLessThanOrEqual(8);
        expect(result.body.coverage.index_families_used).toContain("fts");
        expectMultiSecondRuntime("offset-desc newest segment decode", result.elapsedMs);
      } finally {
        fixture.app.close();
        rmSync(fixture.root, { recursive: true, force: true });
      }
    },
    TIMEOUT_MS
  );

  t(
    "two uploaded evlog segments below the secondary exact L0 span use companion candidates plus source scan",
    async () => {
      const fixture = await buildFixture({
        stream: "perf-small-no-l0",
        segments: 2,
        rowsPerSegment: SMALL_STREAM_ROWS_PER_SEGMENT,
        payloadBytes: SMALL_STREAM_PAYLOAD_BYTES,
        indexL0SpanSegments: 16,
      });
      try {
        expect(fixture.app.deps.db.listSecondaryIndexRuns(fixture.stream, "environment")).toHaveLength(0);

        const result = await measuredSearch(fixture.app, fixture.stream, {
          q: 'environment:"staging"',
          size: 100,
          sort: ["offset:desc"],
        });
        logPerfCase("small-stream-no-exact-l0", fixture, result);

        expect(result.body.hits).toHaveLength(100);
        expect(result.body.coverage.index_families_used).toContain("fts");
        expect(result.body.coverage.indexed_segments).toBeGreaterThan(0);
        expect(fixture.app.deps.db.listSecondaryIndexRuns(fixture.stream, "environment")).toHaveLength(0);
        expectMultiSecondRuntime("small stream below exact L0 span", result.elapsedMs);
      } finally {
        fixture.app.close();
        rmSync(fixture.root, { recursive: true, force: true });
      }
    },
    TIMEOUT_MS
  );

  t(
    "quiet WAL-tail rare exact filters use the hot exact cache after the first lookup",
    async () => {
      const fixture = await buildWalTailFixture({
        stream: "perf-wal-tail-exact",
        rows: WAL_TAIL_ROWS,
        payloadBytes: WAL_TAIL_PAYLOAD_BYTES,
      });
      try {
        const requestBody = {
          q: 'requestId:"req-0000000000"',
          size: 1,
          sort: ["offset:desc"],
        };
        const cold = await measuredSearch(fixture.app, fixture.stream, requestBody);
        const warm = await measuredSearch(fixture.app, fixture.stream, requestBody);
        logPerfCase("wal-tail-rare-exact-cold", fixture, cold);
        logPerfCase("wal-tail-rare-exact-warm", fixture, warm);

        expect(cold.body.hits).toHaveLength(1);
        expect(warm.body.hits).toHaveLength(1);
        expect(warm.body.coverage.candidate_doc_ids).toBe(1);
        expect(warm.body.coverage.scanned_tail_docs).toBe(1);
        expect(warm.elapsedMs).toBeLessThan(cold.elapsedMs);
      } finally {
        fixture.app.close();
        rmSync(fixture.root, { recursive: true, force: true });
      }
    },
    TIMEOUT_MS
  );

  t(
    "sealed exact-only rare filters use .exact doc-id postings instead of parsing every candidate record",
    async () => {
      const fixture = await buildExactOnlyFixture({
        stream: "perf-exact-only-postings",
        rows: EXACT_ONLY_ROWS,
        payloadBytes: EXACT_ONLY_PAYLOAD_BYTES,
      });
      try {
        const result = await measuredSearch(fixture.app, fixture.stream, {
          q: 'customerId:"cust-0000000000"',
          size: 1,
          sort: ["offset:desc"],
        });
        logPerfCase("sealed-exact-only-postings", fixture, result);

        expect(result.body.hits).toHaveLength(1);
        expect(result.body.coverage.index_families_used).toContain("exact");
        expect(result.body.coverage.candidate_doc_ids).toBe(1);
        expect(result.parseCalls).toBeLessThanOrEqual(8);
      } finally {
        fixture.app.close();
        rmSync(fixture.root, { recursive: true, force: true });
      }
    },
    TIMEOUT_MS
  );
});
