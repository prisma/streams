import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { parseOffset } from "../src/offset";
import { streamHash16Hex } from "../src/util/stream_paths";
import { Result } from "better-result";

const RUN = process.env.DS_LARGE_INDEX_FILTER === "1";
const t = RUN ? test : test.skip;

const TOTAL_BYTES = Number(process.env.DS_LARGE_INDEX_FILTER_TOTAL_BYTES ?? 1024 * 1024 * 1024);
const PAYLOAD_BYTES = Number(process.env.DS_LARGE_INDEX_FILTER_PAYLOAD_BYTES ?? 64 * 1024);
const BATCH_ROWS = Number(process.env.DS_LARGE_INDEX_FILTER_BATCH_ROWS ?? 64);
const SEGMENT_MAX_BYTES = Number(process.env.DS_LARGE_INDEX_FILTER_SEGMENT_BYTES ?? 16 * 1024 * 1024);
const INDEX_SPAN_SEGMENTS = Number(process.env.DS_LARGE_INDEX_FILTER_INDEX_SPAN ?? 16);
const TIMEOUT_MS = Number(process.env.DS_LARGE_INDEX_FILTER_TIMEOUT_MS ?? 900_000);
const R2_MAX_IN_MEMORY_BYTES = Number(process.env.DS_LARGE_INDEX_FILTER_R2_MAX_INMEM_BYTES ?? 1 * 1024 * 1024);

const STREAM = "large-index-filter";
const SENTINEL_EVENT_TIME = "2026-03-25T00:00:00.000Z";

const SEARCH_CONFIG = {
  primaryTimestampField: "eventTime",
  defaultFields: [{ field: "message", boost: 1 }],
  fields: {
    eventTime: {
      kind: "date" as const,
      bindings: [{ version: 1, jsonPointer: "/eventTime" }],
      exact: true,
      column: true,
      exists: true,
      sortable: true,
      aggregatable: true,
    },
    service: {
      kind: "keyword" as const,
      bindings: [{ version: 1, jsonPointer: "/service" }],
      normalizer: "lowercase_v1" as const,
      exact: true,
      prefix: true,
      exists: true,
      sortable: true,
      aggregatable: true,
    },
    status: {
      kind: "integer" as const,
      bindings: [{ version: 1, jsonPointer: "/status" }],
      exact: true,
      column: true,
      exists: true,
      sortable: true,
      aggregatable: true,
    },
    duration: {
      kind: "float" as const,
      bindings: [{ version: 1, jsonPointer: "/duration" }],
      exact: true,
      column: true,
      exists: true,
      sortable: true,
      aggregatable: true,
    },
    ok: {
      kind: "bool" as const,
      bindings: [{ version: 1, jsonPointer: "/ok" }],
      exact: true,
      column: true,
      exists: true,
      sortable: true,
      aggregatable: true,
    },
    message: {
      kind: "text" as const,
      bindings: [{ version: 1, jsonPointer: "/message" }],
      analyzer: "unicode_word_v1" as const,
      exists: true,
      positions: true,
    },
  },
};
const EXACT_INDEX_NAMES = ["service", "status", "duration", "ok", "eventTime"] as const;

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
  return new Promise((res) => setTimeout(res, ms));
}

function parseOffsetSeq(value: string | null): bigint {
  expect(value).not.toBeNull();
  const parsed = parseOffset(value!);
  return parsed.kind === "start" ? -1n : parsed.seq;
}

function encodeSizedPayload(value: Record<string, unknown>, targetBytes: number): Uint8Array {
  const encoder = new TextEncoder();
  const baseBytes = encoder.encode(JSON.stringify({ ...value, pad: "" })).byteLength;
  const padLen = targetBytes - baseBytes;
  if (padLen < 0) throw new Error(`payload target ${targetBytes} too small for base object (${baseBytes})`);
  const payload = encoder.encode(JSON.stringify({ ...value, pad: "x".repeat(padLen) }));
  if (payload.byteLength !== targetBytes) {
    throw new Error(`expected payload of ${targetBytes} bytes, got ${payload.byteLength}`);
  }
  return payload;
}

function buildNormalPayloads(): Uint8Array[] {
  return [
    encodeSizedPayload(
      {
        kind: "background",
        service: "svc-a",
        status: 200,
        duration: 1.25,
        ok: true,
        eventTime: "2026-01-01T00:00:00.000Z",
        message: "background-a",
      },
      PAYLOAD_BYTES
    ),
    encodeSizedPayload(
      {
        kind: "background",
        service: "svc-b",
        status: 201,
        duration: 2.5,
        ok: true,
        eventTime: "2026-01-02T00:00:00.000Z",
        message: "background-b",
      },
      PAYLOAD_BYTES
    ),
    encodeSizedPayload(
      {
        kind: "background",
        service: "svc-c",
        status: 202,
        duration: 3.75,
        ok: true,
        eventTime: "2026-01-03T00:00:00.000Z",
        message: "background-c",
      },
      PAYLOAD_BYTES
    ),
    encodeSizedPayload(
      {
        kind: "background",
        service: "svc-d",
        status: 500,
        duration: 4.5,
        ok: true,
        eventTime: "2026-01-04T00:00:00.000Z",
        message: "background-d",
      },
      PAYLOAD_BYTES
    ),
  ];
}

function buildSentinelPayload(): Uint8Array {
  return encodeSizedPayload(
    {
      kind: "needle",
      service: "needle-service",
      status: 777,
      duration: 987.654,
      ok: false,
      eventTime: SENTINEL_EVENT_TIME,
      message: "needle record",
    },
    PAYLOAD_BYTES
  );
}

async function createJsonStreamWithSchema(app: ReturnType<typeof createApp>): Promise<void> {
  const createRes = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  expect([200, 201]).toContain(createRes.status);

  const schemaRes = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        schema: {
          type: "object",
          properties: {
            kind: { type: "string" },
            service: { type: "string" },
            status: { type: "integer" },
            duration: { type: "number" },
            ok: { type: "boolean" },
            eventTime: { type: "string" },
            message: { type: "string" },
            pad: { type: "string" },
          },
          required: ["kind", "service", "status", "duration", "ok", "eventTime", "message", "pad"],
        },
        search: SEARCH_CONFIG,
      }),
    })
  );
  expect(schemaRes.status).toBe(200);
}

function appendPayloadBatch(app: ReturnType<typeof createApp>, startOffset: bigint, rows: Uint8Array[], appendBaseMs: bigint): bigint {
  const res = app.deps.db.appendWalRows({
    stream: STREAM,
    startOffset,
    expectedOffset: startOffset,
    baseAppendMs: appendBaseMs,
    rows: rows.map((payload, index) => ({
      routingKey: null,
      contentType: "application/json",
      payload,
      appendMs: appendBaseMs + BigInt(index),
    })),
  });
  expect(Result.isOk(res)).toBe(true);
  if (Result.isError(res)) throw new Error(res.error.kind);
  return res.value.lastOffset + 1n;
}

async function ingestLargeDataset(app: ReturnType<typeof createApp>): Promise<{ totalPayloadBytes: number; rowCount: number }> {
  const normalPayloads = buildNormalPayloads();
  const sentinelPayload = buildSentinelPayload();
  const rowsNeeded = Math.floor(TOTAL_BYTES / PAYLOAD_BYTES);
  if (rowsNeeded < 2) throw new Error("large index filter test requires at least two rows");

  let nextOffset = 0n;
  let totalPayloadBytes = 0;
  let appendBaseMs = app.deps.db.nowMs();

  nextOffset = appendPayloadBatch(app, nextOffset, [sentinelPayload], appendBaseMs);
  totalPayloadBytes += sentinelPayload.byteLength;
  appendBaseMs += 10n;

  let remaining = rowsNeeded - 1;
  let variant = 0;
  while (remaining > 0) {
    const rows: Uint8Array[] = [];
    const batchCount = Math.min(BATCH_ROWS, remaining);
    for (let i = 0; i < batchCount; i++) {
      const payload = normalPayloads[variant % normalPayloads.length];
      rows.push(payload);
      totalPayloadBytes += payload.byteLength;
      variant += 1;
    }
    nextOffset = appendPayloadBatch(app, nextOffset, rows, appendBaseMs);
    appendBaseMs += BigInt(batchCount + 10);
    remaining -= batchCount;
  }

  return { totalPayloadBytes, rowCount: rowsNeeded };
}

async function waitForUploadAndIndexing(
  app: ReturnType<typeof createApp>,
  expectedIndexNames: string[],
  expectedSegments: number,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const streamRow = app.deps.db.getStream(STREAM);
    const segments = app.deps.db.listSegmentsForStream(STREAM);
    const uploadedSegments = app.deps.db.countUploadedSegments(STREAM);
    const fullySealed =
      !!streamRow &&
      streamRow.next_offset > 0n &&
      streamRow.sealed_through === streamRow.next_offset - 1n &&
      streamRow.pending_bytes === 0n &&
      streamRow.pending_rows === 0n;
    const uploadedOk =
      !!streamRow &&
      fullySealed &&
      segments.length === expectedSegments &&
      streamRow.uploaded_segment_count === expectedSegments &&
      uploadedSegments === expectedSegments &&
      streamRow.uploaded_through === streamRow.next_offset - 1n;
    const states = app.deps.db.listSecondaryIndexStates(STREAM);
    const statesOk =
      states.length === expectedIndexNames.length &&
      states.every((state) => state.indexed_through >= expectedSegments);
    const runsOk =
      statesOk &&
      expectedIndexNames.every((name) => app.deps.db.listSecondaryIndexRuns(STREAM, name).length > 0);
    const companionPlan = app.deps.db.getSearchCompanionPlan(STREAM);
    const companionSegments = app.deps.db.listSearchSegmentCompanions(STREAM);
    const searchFamiliesOk =
      !!companionPlan &&
      companionSegments.length === expectedSegments;
    if (uploadedOk && statesOk && runsOk && searchFamiliesOk) return;
    app.deps.indexer?.enqueue(STREAM);
    await sleep(250);
  }
  throw new Error("timeout waiting for uploaded segments and bundled search companions");
}

async function fetchFilteredJson(
  app: ReturnType<typeof createApp>,
  filter: string
): Promise<{ durationMs: number; body: any[]; nextOffset: bigint; endOffset: bigint }> {
  const params = new URLSearchParams({
    offset: "-1",
    format: "json",
    filter,
  });
  const started = performance.now();
  const res = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}?${params.toString()}`, { method: "GET" }));
  const durationMs = performance.now() - started;
  expect(res.status).toBe(200);
  const body = (await res.json()) as any[];
  return {
    durationMs,
    body,
    nextOffset: parseOffsetSeq(res.headers.get("stream-next-offset")),
    endOffset: parseOffsetSeq(res.headers.get("stream-end-offset")),
  };
}

async function fetchSearchJson(
  app: ReturnType<typeof createApp>,
  args:
    | {
        method: "GET";
        q: string;
        size?: number;
        sort?: string[];
      }
    | {
        method: "POST";
        q: string;
        size?: number;
        sort?: string[];
      }
): Promise<{ durationMs: number; body: any }> {
  const started = performance.now();
  let res: Response;
  if (args.method === "GET") {
    const params = new URLSearchParams({ q: args.q });
    if (args.size != null) params.set("size", String(args.size));
    for (const sort of args.sort ?? []) params.append("sort", sort);
    res = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search?${params.toString()}`, {
        method: "GET",
      })
    );
  } else {
    res = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          q: args.q,
          size: args.size,
          sort: args.sort,
        }),
      })
    );
  }
  const durationMs = performance.now() - started;
  expect(res.status).toBe(200);
  return { durationMs, body: await res.json() };
}

async function measureFullFilteredScan(app: ReturnType<typeof createApp>, filter: string): Promise<{
  durationMs: number;
  batches: number;
  limitHitBatches: number;
}> {
  let offset = "-1";
  let batches = 0;
  let limitHitBatches = 0;
  const started = performance.now();

  for (;;) {
    const params = new URLSearchParams({
      offset,
      format: "json",
      filter,
    });
    const res = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}?${params.toString()}`, { method: "GET" }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as any[];
    expect(body).toEqual([]);
    batches += 1;

    if (res.headers.get("stream-filter-scan-limit-reached") === "true") {
      limitHitBatches += 1;
      expect(res.headers.get("stream-filter-scan-limit-bytes")).toBe(String(100 * 1024 * 1024));
    }

    const nextOffset = res.headers.get("stream-next-offset");
    const endOffset = res.headers.get("stream-end-offset");
    expect(nextOffset).not.toBeNull();
    expect(endOffset).not.toBeNull();
    if (nextOffset === endOffset) break;
    offset = nextOffset!;
  }

  return {
    durationMs: performance.now() - started,
    batches,
    limitHitBatches,
  };
}

describe("large indexed filter integration", () => {
  t(
    "indexes 1GB of JSON data, verifies exact, .col, and .fts families, and measures filtered scan time",
    async () => {
      const root = mkdtempSync(join(tmpdir(), "ds-large-index-filter-"));
      const cfg = makeConfig(root, {
        segmentMaxBytes: SEGMENT_MAX_BYTES,
        segmentCheckIntervalMs: 10,
        uploadIntervalMs: 10,
        uploadConcurrency: 4,
        indexL0SpanSegments: INDEX_SPAN_SEGMENTS,
        indexCheckIntervalMs: 25,
        indexCompactionFanout: 1,
        segmentCacheMaxBytes: 0,
        segmentFooterCacheEntries: 0,
      });
      const store = new MockR2Store({
        maxInMemoryBytes: R2_MAX_IN_MEMORY_BYTES,
        spillDir: `${root}/mock-r2`,
      });
      const app = createApp(cfg, store);

      try {
        await createJsonStreamWithSchema(app);
        const { totalPayloadBytes, rowCount } = await ingestLargeDataset(app);
        const expectedSegments = Math.ceil(totalPayloadBytes / SEGMENT_MAX_BYTES);
        await waitForUploadAndIndexing(app, [...EXACT_INDEX_NAMES], expectedSegments, TIMEOUT_MS);

        const streamHash = streamHash16Hex(STREAM);
        const segmentKeys = (await store.list(`streams/${streamHash}/segments/`)).filter((key) => key.endsWith(".bin"));
        const exactIndexKeys = (await store.list(`streams/${streamHash}/secondary-index/`)).filter((key) => key.endsWith(".idx"));
        const companionKeys = (await store.list(`streams/${streamHash}/segments/`)).filter((key) => key.endsWith(".cix"));

        const segmentRows = app.deps.db.listSegmentsForStream(STREAM);
        const uploadedSegments = app.deps.db.countUploadedSegments(STREAM);
        const streamRow = app.deps.db.getStream(STREAM);
        expect(streamRow).not.toBeNull();
        expect(uploadedSegments).toBe(segmentRows.length);
        expect(streamRow!.uploaded_segment_count).toBe(segmentRows.length);
        expect(segmentKeys.length).toBe(segmentRows.length);
        expect(segmentRows.length).toBe(expectedSegments);

        const expectedIndexFileCount = EXACT_INDEX_NAMES.reduce(
          (sum, indexName) => sum + app.deps.db.listSecondaryIndexRuns(STREAM, indexName).length,
          0
        );
        expect(app.deps.db.listSecondaryIndexStates(STREAM).length).toBe(EXACT_INDEX_NAMES.length);
        for (const indexName of EXACT_INDEX_NAMES) {
          expect(app.deps.db.listSecondaryIndexRuns(STREAM, indexName).length).toBeGreaterThan(0);
        }
        expect(exactIndexKeys.length).toBe(expectedIndexFileCount);

        const companionPlan = app.deps.db.getSearchCompanionPlan(STREAM);
        const companionSegments = app.deps.db.listSearchSegmentCompanions(STREAM);
        expect(companionPlan).not.toBeNull();
        expect(companionSegments.length).toBe(expectedSegments);
        expect(companionKeys.length).toBe(expectedSegments);

        const serviceQuery = await fetchFilteredJson(app, "service:needle-service");
        const statusQuery = await fetchFilteredJson(app, "status:>=700");
        const durationQuery = await fetchFilteredJson(app, "duration:>900");
        const okQuery = await fetchFilteredJson(app, "ok:false");
        const eventTimeQuery = await fetchFilteredJson(app, 'eventTime:>="2026-03-25T00:00:00.000Z"');
        const combinedQuery = await fetchFilteredJson(
          app,
          'service:needle-service status:>=700 duration:>900 ok:false eventTime:>="2026-03-25T00:00:00.000Z"'
        );

        const readQueries = [
          ["service", serviceQuery],
          ["statusRange", statusQuery],
          ["durationRange", durationQuery],
          ["ok", okQuery],
          ["eventTimeRange", eventTimeQuery],
          ["combined", combinedQuery],
        ] as const;
        for (const [label, query] of readQueries) {
          expect(query.body, `${label} read filter result`).toHaveLength(1);
          expect(query.body[0], `${label} read filter payload`).toMatchObject({
            kind: "needle",
            service: "needle-service",
            status: 777,
            duration: 987.654,
            ok: false,
            eventTime: SENTINEL_EVENT_TIME,
            message: "needle record",
          });
        }

        const keywordExactSearch = await fetchSearchJson(app, {
          method: "POST",
          q: "service:needle-service",
          sort: ["eventTime:desc", "offset:desc"],
        });
        const keywordPrefixSearch = await fetchSearchJson(app, {
          method: "GET",
          q: "service:needle-*",
          sort: ["eventTime:desc", "offset:desc"],
        });
        const textSearch = await fetchSearchJson(app, {
          method: "POST",
          q: "needle",
          sort: ["_score:desc", "eventTime:desc", "offset:desc"],
        });
        const phraseSearch = await fetchSearchJson(app, {
          method: "POST",
          q: 'message:"needle record"',
          sort: ["_score:desc", "eventTime:desc", "offset:desc"],
        });
        const searchCombined = await fetchSearchJson(app, {
          method: "POST",
          q: 'service:needle-* status:>=700 duration:>900 ok:false eventTime:>="2026-03-25T00:00:00.000Z" message:"needle record"',
          sort: ["eventTime:desc", "offset:desc"],
        });

        const searchQueries = [
          ["keywordExact", keywordExactSearch],
          ["keywordPrefix", keywordPrefixSearch],
          ["text", textSearch],
          ["phrase", phraseSearch],
          ["combined", searchCombined],
        ] as const;
        for (const [label, search] of searchQueries) {
          expect(search.body.total, `${label} search total`).toEqual({ value: 1, relation: "eq" });
          expect(search.body.hits, `${label} search hits`).toHaveLength(1);
          expect(search.body.hits[0].fields, `${label} search hit`).toMatchObject({
            service: "needle-service",
            status: 777,
            duration: 987.654,
            ok: false,
            eventTime: SENTINEL_EVENT_TIME,
            message: "needle record",
          });
          expect(search.body.hits[0].source.kind, `${label} search source kind`).toBe("needle");
        }
        expect(keywordExactSearch.body.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
        expect(keywordPrefixSearch.body.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
        expect(textSearch.body.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
        expect(phraseSearch.body.coverage.index_families_used).toEqual(expect.arrayContaining(["fts"]));
        expect(searchCombined.body.coverage.index_families_used).toEqual(expect.arrayContaining(["col", "fts"]));

        const fullScan = await measureFullFilteredScan(app, "-has:service");
        if (totalPayloadBytes > 100 * 1024 * 1024) {
          expect(fullScan.batches).toBeGreaterThan(1);
          expect(fullScan.limitHitBatches).toBeGreaterThan(0);
        } else {
          expect(fullScan.batches).toBeGreaterThanOrEqual(1);
          expect(fullScan.limitHitBatches).toBe(0);
        }

        const exactQueryTimings = {
          serviceMs: serviceQuery.durationMs,
          statusMs: statusQuery.durationMs,
          durationMs: durationQuery.durationMs,
          okMs: okQuery.durationMs,
          eventTimeMs: eventTimeQuery.durationMs,
          combinedMs: combinedQuery.durationMs,
        };
        const searchQueryTimings = {
          keywordExactMs: keywordExactSearch.durationMs,
          keywordPrefixMs: keywordPrefixSearch.durationMs,
          textMs: textSearch.durationMs,
          phraseMs: phraseSearch.durationMs,
          combinedMs: searchCombined.durationMs,
        };
        const scanThroughputMBps = (totalPayloadBytes / (1024 * 1024)) / (fullScan.durationMs / 1000);

        // eslint-disable-next-line no-console
        console.log(
          `[large-index-filter] bytes=${totalPayloadBytes} rows=${rowCount} segmentBytes=${SEGMENT_MAX_BYTES} expectedSegments=${expectedSegments} ` +
            `segments=${segmentKeys.length} exactIndexFiles=${exactIndexKeys.length} companionFiles=${companionKeys.length} ` +
            `filterQueryMs=${JSON.stringify(exactQueryTimings)} searchQueryMs=${JSON.stringify(searchQueryTimings)} fullScanMs=${fullScan.durationMs.toFixed(2)} ` +
            `fullScanMBps=${scanThroughputMBps.toFixed(2)} batches=${fullScan.batches} limitHits=${fullScan.limitHitBatches}`
        );
      } finally {
        app.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
    TIMEOUT_MS
  );
});
