import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { PostingIterator } from "../../src/search/fts_format";
import { dsError } from "../../src/util/ds_error.ts";

const STREAM = "search-browse-perf";

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

const SEARCH_SCHEMA = {
  schema: {
    type: "object",
    additionalProperties: true,
  },
  search: {
    primaryTimestampField: "eventTime",
    aliases: {
      owner: "repoOwner",
      type: "eventType",
    },
    fields: {
      eventTime: {
        kind: "date",
        bindings: [{ version: 1, jsonPointer: "/eventTime" }],
        column: true,
        exists: true,
        sortable: true,
      },
      eventType: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/eventType" }],
        normalizer: "lowercase_v1",
        exact: true,
        prefix: true,
        exists: true,
        sortable: true,
      },
      repoOwner: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/repoOwner" }],
        normalizer: "lowercase_v1",
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
      },
    },
  },
};

async function waitForSearchFamilies(
  app: ReturnType<typeof createApp>,
  targetSegments: number,
  timeoutMs = 60_000
): Promise<void> {
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
  throw dsError("timeout waiting for search companions");
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
      body: JSON.stringify(SEARCH_SCHEMA),
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
  prefixVariants: number,
  prefixEvery: number,
  matchEverySegments: number,
  tailNonMatchDocs: number
): Promise<void> {
  const baseMs = Date.parse("2026-01-01T00:00:00.000Z");
  const prismaOwners = Array.from({ length: prefixVariants }, (_, index) =>
    index === 0 ? "prisma" : `prisma-${String(index + 1).padStart(2, "0")}`
  );
  for (let segmentIndex = 0; segmentIndex < segments; segmentIndex++) {
    const segmentBaseMs = baseMs + segmentIndex * rowsPerSegment * 1000;
    const rows = Array.from({ length: rowsPerSegment }, (_, rowIndex) => {
      const globalIndex = segmentIndex * rowsPerSegment + rowIndex;
      const allowMatches =
        matchEverySegments > 0 && (segmentIndex % matchEverySegments === matchEverySegments - 1 || segmentIndex === segments - 1);
      const tailDistance = rowsPerSegment - 1 - rowIndex;
      const isTailNonMatch = allowMatches && tailDistance >= 0 && tailDistance < tailNonMatchDocs;
      const isPrefixMatch = (allowMatches && globalIndex % prefixEvery === 0) || isTailNonMatch;
      const owner = isPrefixMatch
        ? prismaOwners[(globalIndex / prefixEvery) % prismaOwners.length]!
        : `other-${String(globalIndex % 4096).padStart(4, "0")}`;
      const isPush = isTailNonMatch ? false : globalIndex % 6 !== 0;
      const eventType = isPush ? "PushEvent" : "IssuesEvent";
      return {
        eventTime: new Date(segmentBaseMs + rowIndex * 1000).toISOString(),
        eventType,
        repoOwner: owner,
        message: isPrefixMatch ? "latest prisma push candidate" : "background event",
      };
    });
    if (matchEverySegments > 0 && (segmentIndex % matchEverySegments === matchEverySegments - 1 || segmentIndex === segments - 1)) {
      const targetIndex = Math.max(0, rowsPerSegment - tailNonMatchDocs - 1);
      rows[targetIndex] = {
        eventTime: new Date(segmentBaseMs + targetIndex * 1000).toISOString(),
        eventType: "PushEvent",
        repoOwner: prismaOwners[segmentIndex % prismaOwners.length]!,
        message: "latest prisma push candidate",
      };
    }
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
  ftsCalls: number;
  postingBlocks: number;
};

function instrumentQueryCounters(app: ReturnType<typeof createApp>): QueryCounters {
  const counters: QueryCounters = { ftsCalls: 0, postingBlocks: 0 };
  const indexer = app.deps.indexer as {
    getFtsSegmentCompanion(stream: string, segmentIndex: number): Promise<unknown>;
  };
  const originalGetFts = indexer.getFtsSegmentCompanion.bind(indexer);
  indexer.getFtsSegmentCompanion = async (stream: string, segmentIndex: number) => {
    counters.ftsCalls += 1;
    return originalGetFts(stream, segmentIndex);
  };

  const originalNextBlock = PostingIterator.prototype.nextBlock;
  PostingIterator.prototype.nextBlock = function instrumentedNextBlock(this: PostingIterator) {
    const block = originalNextBlock.call(this);
    if (block) counters.postingBlocks += 1;
    return block;
  };

  return counters;
}

async function runBrowseQuery(app: ReturnType<typeof createApp>) {
  const startedAt = performance.now();
  const res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        q: "type:pushevent owner:prisma*",
        size: 1,
        sort: ["offset:desc"],
        track_total_hits: false,
      }),
    })
  );
  const elapsedMs = performance.now() - startedAt;
  const text = await res.text();
  if (res.status !== 200) {
    throw dsError(`browse query failed: ${res.status} ${text}`);
  }
  return {
    elapsedMs,
    body: JSON.parse(text) as {
      coverage: Record<string, unknown>;
      hits: Array<{ fields?: Record<string, unknown>; offset: string }>;
      total: { value: number; relation: string };
    },
  };
}

async function main(): Promise<void> {
  const segments = argValue("segments", 96);
  const rowsPerSegment = argValue("rows-per-segment", 1024);
  const prefixVariants = argValue("prefix-variants", 32);
  const prefixEvery = argValue("prefix-every", 64);
  const iterations = argValue("iterations", 25);
  const matchEverySegments = argValue("match-every-segments", 1);
  const tailNonMatchDocs = argValue("tail-nonmatch-docs", 0);

  const root = mkdtempSync(join(tmpdir(), "ds-search-browse-perf-"));
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
  const originalNextBlock = PostingIterator.prototype.nextBlock;
  try {
    buildApp = createApp(buildCfg, store);
    await createStreamAndSchema(buildApp);
    await appendDataset(buildApp, segments, rowsPerSegment, prefixVariants, prefixEvery, matchEverySegments, tailNonMatchDocs);
    await waitForSearchFamilies(buildApp, segments);
    buildApp.close();
    buildApp = null;

    store.resetStats();
    queryApp = createApp(queryCfg, store);
    const counters = instrumentQueryCounters(queryApp);
    const beforeStoreStats = store.stats();
    const rssBefore = process.memoryUsage().rss;
    let parseCalls = 0;
    const originalJsonParse = JSON.parse;
    JSON.parse = ((text: string, reviver?: (this: unknown, key: string, value: unknown) => unknown) => {
      parseCalls += 1;
      return originalJsonParse(text, reviver);
    }) as typeof JSON.parse;
    const startedAt = performance.now();
    let queryRes: Awaited<ReturnType<typeof runBrowseQuery>> | null = null;
    try {
      for (let index = 0; index < iterations; index++) {
        queryRes = await runBrowseQuery(queryApp);
      }
    } finally {
      JSON.parse = originalJsonParse;
    }
    const elapsedMs = performance.now() - startedAt;
    const afterStoreStats = store.stats();
    const rssAfter = process.memoryUsage().rss;

    const result = {
      segments,
      rowsPerSegment,
      prefixVariants,
      prefixEvery,
      matchEverySegments,
      tailNonMatchDocs,
      iterations,
      uploadedSegments: queryApp.deps.db.getStream(STREAM)?.uploaded_segment_count ?? 0,
      elapsedMs: Number(elapsedMs.toFixed(2)),
      avgElapsedMs: Number((elapsedMs / iterations).toFixed(2)),
      lastElapsedMs: Number((queryRes?.elapsedMs ?? 0).toFixed(2)),
      ftsCalls: counters.ftsCalls,
      postingBlocks: counters.postingBlocks,
      parseCalls,
      storeGetsDelta: afterStoreStats.gets - beforeStoreStats.gets,
      rssBefore,
      rssAfter,
      coverage: queryRes?.body.coverage ?? null,
      total: queryRes?.body.total ?? null,
      topHitOffset: queryRes?.body.hits[0]?.offset ?? null,
    };
    console.log(JSON.stringify(result, null, 2));
  } finally {
    PostingIterator.prototype.nextBlock = originalNextBlock;
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
