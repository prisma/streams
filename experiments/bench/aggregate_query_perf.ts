import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { dsError } from "../../src/util/ds_error.ts";

const STREAM = "aggregate-perf";

function argValue(name: string, def: number): number {
  const prefix = `--${name}=`;
  for (const arg of Bun.argv.slice(2)) {
    if (arg.startsWith(prefix)) return Number(arg.slice(prefix.length));
  }
  return def;
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

const AGG_SCHEMA = {
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
        sortable: true,
      },
      duration: {
        kind: "float",
        bindings: [{ version: 1, jsonPointer: "/duration" }],
        exact: true,
        column: true,
        exists: true,
        sortable: true,
        aggregatable: true,
      },
    },
    rollups: {
      requests: {
        dimensions: ["service"],
        intervals: ["1h"],
        measures: {
          requests: { kind: "count" },
          latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
        },
      },
    },
  },
};

async function waitForAggFamily(app: ReturnType<typeof createApp>, targetSegments: number, timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const srow = app.deps.db.getStream(STREAM);
    const companionPlan = app.deps.db.getSearchCompanionPlan(STREAM);
    const companionSegments = app.deps.db.listSearchSegmentCompanions(STREAM);
    if (
      srow &&
      srow.uploaded_segment_count >= targetSegments &&
      srow.uploaded_through >= srow.sealed_through &&
      companionPlan &&
      companionSegments.length >= srow.uploaded_segment_count
    ) {
      return;
    }
    app.deps.indexer?.enqueue(STREAM);
    await sleep(25);
  }
  throw dsError("timeout waiting for aggregate companions");
}

async function createStreamAndSchema(app: ReturnType<typeof createApp>): Promise<void> {
  let res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  if (![200, 201].includes(res.status)) {
    throw dsError(`stream create failed: ${res.status} ${await res.text()}`);
  }

  res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(AGG_SCHEMA),
    })
  );
  if (res.status !== 200) {
    throw dsError(`schema install failed: ${res.status} ${await res.text()}`);
  }
}

async function appendDataset(
  app: ReturnType<typeof createApp>,
  segments: number,
  rowsPerSegment: number,
  groupCardinality: number
): Promise<void> {
  const baseMs = Date.parse("2026-01-01T00:00:00.000Z");
  for (let segmentIndex = 0; segmentIndex < segments; segmentIndex++) {
    const segmentBaseMs = baseMs + segmentIndex * 3_600_000;
    const rows = Array.from({ length: rowsPerSegment }, (_, rowIndex) => ({
      eventTime: new Date(segmentBaseMs + rowIndex * 1000).toISOString(),
      service: `svc-${rowIndex % groupCardinality}`,
      duration: 10 + (rowIndex % 200),
    }));
    const res = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(rows),
      })
    );
    if (res.status !== 204) {
      throw dsError(`append failed: ${res.status} ${await res.text()}`);
    }
  }
}

type QueryCounters = {
  aggCalls: number;
  colCalls: number;
};

function instrumentQueryCounters(app: ReturnType<typeof createApp>): QueryCounters {
  const counters: QueryCounters = { aggCalls: 0, colCalls: 0 };
  const indexer = app.deps.indexer as {
    getAggSegmentCompanion(stream: string, segmentIndex: number): Promise<unknown>;
    getColSegmentCompanion(stream: string, segmentIndex: number): Promise<unknown>;
  };
  const originalAgg = indexer.getAggSegmentCompanion.bind(indexer);
  const originalCol = indexer.getColSegmentCompanion.bind(indexer);
  indexer.getAggSegmentCompanion = async (stream: string, segmentIndex: number) => {
    counters.aggCalls += 1;
    return originalAgg(stream, segmentIndex);
  };
  indexer.getColSegmentCompanion = async (stream: string, segmentIndex: number) => {
    counters.colCalls += 1;
    return originalCol(stream, segmentIndex);
  };
  return counters;
}

async function runAggregateQuery(app: ReturnType<typeof createApp>) {
  const startedAt = performance.now();
  const res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_aggregate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        rollup: "requests",
        from: "2026-01-01T00:00:00.000Z",
        to: "2026-01-01T01:00:00.000Z",
        interval: "1h",
        group_by: ["service"],
      }),
    })
  );
  const elapsedMs = performance.now() - startedAt;
  const text = await res.text();
  if (res.status !== 200) {
    throw dsError(`aggregate query failed: ${res.status} ${text}`);
  }
  return {
    elapsedMs,
    body: JSON.parse(text) as {
      coverage: Record<string, unknown>;
      buckets: Array<{ groups: unknown[] }>;
    },
  };
}

async function main(): Promise<void> {
  const segments = argValue("segments", 96);
  const rowsPerSegment = argValue("rows-per-segment", 512);
  const groupCardinality = argValue("group-cardinality", 256);

  const root = mkdtempSync(join(tmpdir(), "ds-aggregate-query-perf-"));
  const store = new MockR2Store();
  const buildCfg = makeConfig(root, {
    segmentMaxBytes: 16 * 1024 * 1024,
    segmentTargetRows: rowsPerSegment,
    segmentCheckIntervalMs: 10,
    uploadIntervalMs: 10,
    uploadConcurrency: 4,
    indexCheckIntervalMs: 10,
    indexL0SpanSegments: 2,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
  });
  const queryCfg = makeConfig(root, {
    segmentMaxBytes: 16 * 1024 * 1024,
    segmentTargetRows: rowsPerSegment,
    segmentCheckIntervalMs: 60_000,
    uploadIntervalMs: 60_000,
    indexCheckIntervalMs: 60_000,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
  });

  let buildApp: ReturnType<typeof createApp> | null = null;
  let queryApp: ReturnType<typeof createApp> | null = null;
  try {
    buildApp = createApp(buildCfg, store);
    console.error("aggregate_query_perf: created build app");
    await createStreamAndSchema(buildApp);
    console.error("aggregate_query_perf: created stream and schema");
    await appendDataset(buildApp, segments, rowsPerSegment, groupCardinality);
    console.error("aggregate_query_perf: appended dataset");
    await waitForAggFamily(buildApp, segments);
    console.error("aggregate_query_perf: aggregate companions ready");
    buildApp.close();
    buildApp = null;

    store.resetStats();
    queryApp = createApp(queryCfg, store);
    console.error("aggregate_query_perf: created query app");
    const counters = instrumentQueryCounters(queryApp);
    const beforeStoreStats = store.stats();
    const rssBefore = process.memoryUsage().rss;
    const queryRes = await runAggregateQuery(queryApp);
    console.error("aggregate_query_perf: aggregate query completed");
    const afterStoreStats = store.stats();
    const rssAfter = process.memoryUsage().rss;

    const result = {
      segments,
      rowsPerSegment,
      groupCardinality,
      uploadedSegments: queryApp.deps.db.getStream(STREAM)?.uploaded_segment_count ?? 0,
      elapsedMs: Number(queryRes.elapsedMs.toFixed(2)),
      aggCalls: counters.aggCalls,
      colCalls: counters.colCalls,
      storeGetsDelta: afterStoreStats.gets - beforeStoreStats.gets,
      rssBefore,
      rssAfter,
      coverage: queryRes.body.coverage,
      bucketCount: queryRes.body.buckets.length,
      firstBucketGroupCount: queryRes.body.buckets[0]?.groups.length ?? 0,
    };
    console.log(JSON.stringify(result, null, 2));
  } finally {
    queryApp?.close();
    buildApp?.close();
    rmSync(root, { recursive: true, force: true });
  }
}

try {
  await main();
  process.exit(0);
} catch (error: unknown) {
  console.error(error);
  process.exit(1);
}
