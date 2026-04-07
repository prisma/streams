import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

const CHILD_MODE = process.env.DS_ROUTING_KEY_MEMORY_CHILD_MODE;
const CHILD_VARIANT = process.env.DS_ROUTING_KEY_MEMORY_CHILD_VARIANT;
const IS_CHILD_MODE = CHILD_MODE === "plain" || CHILD_MODE === "keyed";
const IS_CHILD_VARIANT = CHILD_VARIANT === "build-only" || CHILD_VARIANT === "with-indexing";
const IS_CHILD = IS_CHILD_MODE && IS_CHILD_VARIANT;
const RUN = !IS_CHILD && process.env.DS_ROUTING_KEY_MEMORY === "1";
const t = RUN ? test : test.skip;

const MIB = 1024 * 1024;
const BUILD_ONLY_TOTAL_PAYLOAD_BYTES = envInt("DS_ROUTING_KEY_MEMORY_TOTAL_BYTES", 100 * MIB);
const INDEXED_TOTAL_PAYLOAD_BYTES = envInt("DS_ROUTING_KEY_MEMORY_INDEXED_TOTAL_BYTES", BUILD_ONLY_TOTAL_PAYLOAD_BYTES);
const PAYLOAD_BYTES = envInt("DS_ROUTING_KEY_MEMORY_PAYLOAD_BYTES", 8 * 1024);
const BATCH_ROWS = envInt("DS_ROUTING_KEY_MEMORY_BATCH_ROWS", 256);
const BUILD_ONLY_SEGMENT_MAX_BYTES = envInt("DS_ROUTING_KEY_MEMORY_SEGMENT_BYTES", 8 * MIB);
const INDEXED_SEGMENT_MAX_BYTES = envInt("DS_ROUTING_KEY_MEMORY_INDEXED_SEGMENT_BYTES", 1 * MIB);
const INDEXED_SEGMENT_CACHE_BYTES = envInt(
  "DS_ROUTING_KEY_MEMORY_INDEXED_SEGMENT_CACHE_BYTES",
  32 * MIB
);
const BUILD_ONLY_MIN_SEGMENTS = envInt("DS_ROUTING_KEY_MEMORY_MIN_SEGMENTS", 10);
const INDEXED_MIN_SEGMENTS = envInt("DS_ROUTING_KEY_MEMORY_INDEXED_MIN_SEGMENTS", 10);
const BUILD_ONLY_INDEX_L0_SPAN_SEGMENTS = envInt("DS_ROUTING_KEY_MEMORY_INDEX_SPAN", 1024);
const INDEXED_INDEX_L0_SPAN_SEGMENTS = envInt(
  "DS_ROUTING_KEY_MEMORY_INDEXED_INDEX_SPAN",
  Math.max(1, INDEXED_MIN_SEGMENTS)
);
const KEY_CARDINALITY = envInt("DS_ROUTING_KEY_MEMORY_KEY_CARDINALITY", 1024);
const SCENARIO_REPEATS = envInt("DS_ROUTING_KEY_MEMORY_REPEATS", 3);
const SAMPLE_INTERVAL_MS = envInt("DS_ROUTING_KEY_MEMORY_SAMPLE_INTERVAL_MS", 20);
const SCENARIO_TIMEOUT_MS = envInt("DS_ROUTING_KEY_MEMORY_TIMEOUT_MS", 300_000);
const ALLOWED_EXTRA_BYTES_BUILD_ONLY = envIntNonNegative("DS_ROUTING_KEY_MEMORY_ALLOWED_EXTRA_BYTES", 10 * MIB);
const ALLOWED_EXTRA_BYTES_WITH_INDEXING = envIntNonNegative(
  "DS_ROUTING_KEY_MEMORY_ALLOWED_EXTRA_BYTES_WITH_INDEXING",
  50 * MIB
);
const TEST_TIMEOUT_MS = SCENARIO_TIMEOUT_MS * SCENARIO_REPEATS * 2 + 60_000;

type ScenarioSummary = {
  variant: ScenarioVariant;
  withRoutingKey: boolean;
  targetPayloadBytes: number;
  requiredMinSegments: number;
  rows: number;
  payloadBytes: number;
  segmentCount: number;
  uploadedSegmentCount: number;
  routingRuns: number;
  lexiconRuns: number;
  routingIndexedThrough: number | null;
  lexiconIndexedThrough: number | null;
  baselineRssBytes: number;
  peakRssBytes: number;
  peakRssDeltaBytes: number;
  settledRssBytes: number;
  settledRssDeltaBytes: number;
};

type ScenarioVariant = "build-only" | "with-indexing";

function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw == null || raw.trim() === "") return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`invalid ${name}: ${raw}`);
  }
  return Math.floor(parsed);
}

function envIntNonNegative(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw == null || raw.trim() === "") return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`invalid ${name}: ${raw}`);
  }
  return Math.floor(parsed);
}

function fmtMiB(bytes: number): string {
  return `${(bytes / MIB).toFixed(2)} MiB`;
}

function median(values: number[]): number {
  if (values.length === 0) throw new Error("median requires at least one value");
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid]!;
  return Math.floor((sorted[mid - 1]! + sorted[mid]!) / 2);
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

function maybeForceGc(): void {
  const maybeGc = (Bun as unknown as { gc?: (full?: boolean) => void }).gc;
  if (typeof maybeGc !== "function") return;
  try {
    maybeGc(true);
  } catch {
    // best effort only
  }
}

function makePayload(rowIndex: number, size: number): Uint8Array {
  const bytes = new Uint8Array(size);
  let x = (rowIndex + 1) >>> 0;
  for (let i = 0; i < size; i += 1) {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    bytes[i] = x & 0xff;
  }
  return bytes;
}

function buildRoutingKeys(): Uint8Array[] {
  const encoder = new TextEncoder();
  return Array.from({ length: KEY_CARDINALITY }, (_, i) => encoder.encode(`rk-${i.toString(36).padStart(6, "0")}`));
}

async function createStream(app: ReturnType<typeof createApp>, stream: string, withRoutingKey: boolean): Promise<void> {
  const createRes = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  expect([200, 201]).toContain(createRes.status);

  if (!withRoutingKey) return;

  const schemaRes = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        routingKey: {
          jsonPointer: "/rk",
          required: false,
        },
      }),
    })
  );
  expect(schemaRes.status).toBe(200);
}

async function appendDataset(
  app: ReturnType<typeof createApp>,
  stream: string,
  withRoutingKey: boolean,
  targetPayloadBytes: number
): Promise<{ rows: number; payloadBytes: number }> {
  const routingKeys = withRoutingKey ? buildRoutingKeys() : [];
  let nextOffset = 0n;
  let rows = 0;
  let payloadBytes = 0;
  let baseAppendMs = app.deps.db.nowMs();

  while (payloadBytes < targetPayloadBytes) {
    const batch: Array<{ routingKey: Uint8Array | null; contentType: string | null; payload: Uint8Array; appendMs: bigint }> = [];
    for (let i = 0; i < BATCH_ROWS && payloadBytes < targetPayloadBytes; i += 1) {
      const size = Math.min(PAYLOAD_BYTES, targetPayloadBytes - payloadBytes);
      const payload = makePayload(rows, size);
      batch.push({
        routingKey: withRoutingKey ? routingKeys[rows % routingKeys.length]! : null,
        contentType: "application/octet-stream",
        payload,
        appendMs: baseAppendMs + BigInt(i),
      });
      rows += 1;
      payloadBytes += size;
    }
    const appendRes = app.deps.db.appendWalRows({
      stream,
      startOffset: nextOffset,
      expectedOffset: nextOffset,
      baseAppendMs,
      rows: batch,
    });
    expect(Result.isOk(appendRes)).toBe(true);
    if (Result.isError(appendRes)) {
      throw new Error(`append failed: ${appendRes.error.kind}`);
    }
    nextOffset = appendRes.value.lastOffset + 1n;
    baseAppendMs += BigInt(batch.length + 10);
    await sleep(0);
  }

  return { rows, payloadBytes };
}

async function waitForSettled(
  app: ReturnType<typeof createApp>,
  stream: string,
  withRoutingKey: boolean,
  variant: ScenarioVariant,
  minSegments: number
): Promise<{
  segmentCount: number;
  uploadedSegmentCount: number;
  routingRuns: number;
  lexiconRuns: number;
  routingIndexedThrough: number | null;
  lexiconIndexedThrough: number | null;
}> {
  const expectIndexing = variant === "with-indexing" && withRoutingKey;
  const deadline = Date.now() + SCENARIO_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const streamRow = app.deps.db.getStream(stream);
    const segmentCount = app.deps.db.listSegmentsForStream(stream).length;
    const uploadedSegmentCount = app.deps.db.countUploadedSegments(stream);
    const routingRuns = app.deps.db.listIndexRuns(stream).length;
    const lexiconRuns = app.deps.db.listLexiconIndexRuns(stream, "routing_key", "").length;
    const routingState = app.deps.db.getIndexState(stream);
    const lexiconState = app.deps.db.getLexiconIndexState(stream, "routing_key", "");

    const streamStable =
      !!streamRow &&
      streamRow.next_offset > 0n &&
      streamRow.sealed_through === streamRow.next_offset - 1n &&
      streamRow.pending_rows === 0n &&
      streamRow.pending_bytes === 0n &&
      streamRow.uploaded_through === streamRow.sealed_through &&
      streamRow.uploaded_segment_count === segmentCount &&
      uploadedSegmentCount === segmentCount &&
      segmentCount >= minSegments;

    const indexingStable = expectIndexing
      ? routingRuns > 0 &&
        lexiconRuns > 0 &&
        (routingState?.indexed_through ?? 0) >= uploadedSegmentCount &&
        (lexiconState?.indexed_through ?? 0) >= uploadedSegmentCount
      : true;

    if (streamStable && indexingStable) {
      return {
        segmentCount,
        uploadedSegmentCount,
        routingRuns,
        lexiconRuns,
        routingIndexedThrough: routingState?.indexed_through ?? null,
        lexiconIndexedThrough: lexiconState?.indexed_through ?? null,
      };
    }

    if (expectIndexing) {
      app.deps.indexer?.enqueue(stream);
    }

    await sleep(50);
  }

  const streamRow = app.deps.db.getStream(stream);
  throw new Error(
    `timeout waiting for settled state stream=${stream} withRoutingKey=${withRoutingKey} variant=${variant} ` +
      `nextOffset=${streamRow?.next_offset?.toString() ?? "null"} ` +
      `sealedThrough=${streamRow?.sealed_through?.toString() ?? "null"} ` +
      `uploadedThrough=${streamRow?.uploaded_through?.toString() ?? "null"} ` +
      `segmentCount=${app.deps.db.listSegmentsForStream(stream).length} ` +
      `uploadedSegmentCount=${app.deps.db.countUploadedSegments(stream)} ` +
      `routingRuns=${app.deps.db.listIndexRuns(stream).length} ` +
      `lexiconRuns=${app.deps.db.listLexiconIndexRuns(stream, "routing_key", "").length}`
  );
}

async function runScenario(withRoutingKey: boolean, variant: ScenarioVariant): Promise<ScenarioSummary> {
  maybeForceGc();
  const root = mkdtempSync(
    join(tmpdir(), withRoutingKey ? `ds-routing-memory-keyed-${variant}-` : `ds-routing-memory-plain-${variant}-`)
  );
  const stream = withRoutingKey ? `routing-memory-keyed-${variant}` : `routing-memory-plain-${variant}`;
  const targetPayloadBytes = variant === "with-indexing" ? INDEXED_TOTAL_PAYLOAD_BYTES : BUILD_ONLY_TOTAL_PAYLOAD_BYTES;
  const segmentMaxBytes = variant === "with-indexing" ? INDEXED_SEGMENT_MAX_BYTES : BUILD_ONLY_SEGMENT_MAX_BYTES;
  const minSegments = variant === "with-indexing" ? INDEXED_MIN_SEGMENTS : BUILD_ONLY_MIN_SEGMENTS;
  const indexL0SpanSegments = variant === "with-indexing" ? INDEXED_INDEX_L0_SPAN_SEGMENTS : BUILD_ONLY_INDEX_L0_SPAN_SEGMENTS;
  const indexCheckIntervalMs = variant === "with-indexing" ? 10 : 60_000_000;
  const segmentCacheMaxBytes = variant === "with-indexing" ? INDEXED_SEGMENT_CACHE_BYTES : 0;
  const cfg = makeConfig(root, {
    segmentMaxBytes,
    segmentTargetRows: 250_000,
    segmentMaxIntervalMs: 100,
    segmentCheckIntervalMs: 10,
    uploadIntervalMs: 10,
    uploadConcurrency: 2,
    indexL0SpanSegments,
    indexBuilders: 1,
    indexCheckIntervalMs,
    indexCompactionFanout: 1,
    segmentCacheMaxBytes,
    segmentFooterCacheEntries: 0,
    indexRunCacheMaxBytes: 0,
    indexRunMemoryCacheBytes: 0,
    lexiconIndexCacheMaxBytes: 0,
    lexiconMappedCacheEntries: 0,
    searchCompanionBuildBatchSegments: 1,
    searchCompanionYieldBlocks: 1,
    searchCompanionFileCacheMaxBytes: 0,
    searchCompanionMappedCacheEntries: 0,
    searchCompanionTocCacheBytes: 0,
    searchCompanionSectionCacheBytes: 0,
  });
  const store = new MockR2Store({
    maxInMemoryBytes: 0,
    spillDir: `${root}/mock-r2`,
  });
  const app = createApp(cfg, store);

  try {
    await createStream(app, stream, withRoutingKey);
    if (variant === "with-indexing") {
      app.deps.indexer?.stop();
    }
    let baselineRssBytes = process.memoryUsage().rss;
    let peakRssBytes = baselineRssBytes;
    let sampler: ReturnType<typeof setInterval> | null = null;
    const startSampler = () => {
      if (sampler) clearInterval(sampler);
      sampler = setInterval(() => {
        const rss = process.memoryUsage().rss;
        if (rss > peakRssBytes) peakRssBytes = rss;
      }, SAMPLE_INTERVAL_MS);
    };
    const stopSampler = () => {
      if (!sampler) return;
      clearInterval(sampler);
      sampler = null;
    };

    try {
      if (variant === "build-only") {
        startSampler();
      }
      const appendSummary = await appendDataset(app, stream, withRoutingKey, targetPayloadBytes);
      const settled =
        variant === "with-indexing"
          ? await (async () => {
              await waitForSettled(app, stream, withRoutingKey, "build-only", minSegments);
              maybeForceGc();
              await sleep(50);
              baselineRssBytes = process.memoryUsage().rss;
              peakRssBytes = baselineRssBytes;
              startSampler();
              app.deps.indexer?.start();
              app.deps.indexer?.enqueue(stream);
              return await waitForSettled(app, stream, withRoutingKey, "with-indexing", minSegments);
            })()
          : await waitForSettled(app, stream, withRoutingKey, "build-only", minSegments);
      await sleep(200);
      stopSampler();
      maybeForceGc();
      await sleep(50);
      const settledRssBytes = process.memoryUsage().rss;
      peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
      return {
        variant,
        withRoutingKey,
        targetPayloadBytes,
        requiredMinSegments: minSegments,
        rows: appendSummary.rows,
        payloadBytes: appendSummary.payloadBytes,
        segmentCount: settled.segmentCount,
        uploadedSegmentCount: settled.uploadedSegmentCount,
        routingRuns: settled.routingRuns,
        lexiconRuns: settled.lexiconRuns,
        routingIndexedThrough: settled.routingIndexedThrough,
        lexiconIndexedThrough: settled.lexiconIndexedThrough,
        baselineRssBytes,
        peakRssBytes,
        peakRssDeltaBytes: Math.max(0, peakRssBytes - baselineRssBytes),
        settledRssBytes,
        settledRssDeltaBytes: Math.max(0, settledRssBytes - baselineRssBytes),
      };
    } finally {
      stopSampler();
      if (variant === "with-indexing") {
        app.deps.indexer?.stop();
      }
    }
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
    maybeForceGc();
  }
}

async function runScenarioInSubprocess(withRoutingKey: boolean, variant: ScenarioVariant): Promise<ScenarioSummary> {
  const childEnv: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value != null) childEnv[name] = value;
  }
  childEnv.DS_ROUTING_KEY_MEMORY = "0";
  childEnv.DS_ROUTING_KEY_MEMORY_CHILD_MODE = withRoutingKey ? "keyed" : "plain";
  childEnv.DS_ROUTING_KEY_MEMORY_CHILD_VARIANT = variant;

  const proc = Bun.spawn({
    cmd: ["bun", "run", import.meta.path],
    cwd: process.cwd(),
    env: childEnv,
    stdout: "pipe",
    stderr: "pipe",
  });

  const [stdout, stderr, exitCode] = await Promise.all([
    proc.stdout ? new Response(proc.stdout).text() : Promise.resolve(""),
    proc.stderr ? new Response(proc.stderr).text() : Promise.resolve(""),
    proc.exited,
  ]);

  if (exitCode !== 0) {
    throw new Error(
      `routing key memory child failed mode=${childEnv.DS_ROUTING_KEY_MEMORY_CHILD_MODE} exit=${exitCode}\n` +
        `stdout:\n${stdout}\n` +
        `stderr:\n${stderr}`
    );
  }

  const marker = "ROUTING_KEY_MEMORY_SUMMARY ";
  const summaryLine = stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .reverse()
    .find((line) => line.startsWith(marker));
  if (!summaryLine) {
    throw new Error(
      `routing key memory child missing summary mode=${childEnv.DS_ROUTING_KEY_MEMORY_CHILD_MODE}\n` +
        `stdout:\n${stdout}\n` +
        `stderr:\n${stderr}`
    );
  }

  return JSON.parse(summaryLine.slice(marker.length)) as ScenarioSummary;
}

if (IS_CHILD) {
  try {
    const childVariant = CHILD_VARIANT as ScenarioVariant;
    const summary = await runScenario(CHILD_MODE === "keyed", childVariant);
    // eslint-disable-next-line no-console
    console.log(`ROUTING_KEY_MEMORY_SUMMARY ${JSON.stringify(summary)}`);
    process.exit(0);
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exit(1);
  }
}

async function assertVariantBudget(args: {
  variant: ScenarioVariant;
  budgetBytes: number;
  requireIndexedRuns: boolean;
}): Promise<void> {
  const plainRuns: ScenarioSummary[] = [];
  const keyedRuns: ScenarioSummary[] = [];
  for (let i = 0; i < SCENARIO_REPEATS; i += 1) {
    plainRuns.push(await runScenarioInSubprocess(false, args.variant));
    keyedRuns.push(await runScenarioInSubprocess(true, args.variant));
  }

  for (const run of [...plainRuns, ...keyedRuns]) {
    expect(run.payloadBytes).toBe(run.targetPayloadBytes);
    expect(run.segmentCount).toBeGreaterThanOrEqual(run.requiredMinSegments);
  }
  if (args.requireIndexedRuns) {
    for (const run of keyedRuns) {
      expect(run.routingRuns).toBeGreaterThan(0);
      expect(run.lexiconRuns).toBeGreaterThan(0);
      expect(run.routingIndexedThrough ?? 0).toBeGreaterThanOrEqual(run.uploadedSegmentCount);
      expect(run.lexiconIndexedThrough ?? 0).toBeGreaterThanOrEqual(run.uploadedSegmentCount);
    }
  }

  const useSettledDeltas = args.variant === "with-indexing";
  const plainDeltas = plainRuns.map((run) => (useSettledDeltas ? run.settledRssDeltaBytes : run.peakRssDeltaBytes));
  const keyedDeltas = keyedRuns.map((run) => (useSettledDeltas ? run.settledRssDeltaBytes : run.peakRssDeltaBytes));
  const plainMedian = median(plainDeltas);
  const keyedMedian = median(keyedDeltas);
  const extraBytes = keyedMedian - plainMedian;
  expect(extraBytes).toBeLessThanOrEqual(args.budgetBytes);

  // eslint-disable-next-line no-console
  console.log(
    `[routing-key-memory:${args.variant}] total=${fmtMiB(plainRuns[0]!.targetPayloadBytes)} repeats=${SCENARIO_REPEATS} ` +
      `metric=${useSettledDeltas ? "settled-delta" : "peak-delta"} ` +
      `plain.deltas=${plainDeltas.map(fmtMiB).join(",")} keyed.deltas=${keyedDeltas.map(fmtMiB).join(",")} ` +
      `plain.median=${fmtMiB(plainMedian)} keyed.median=${fmtMiB(keyedMedian)} ` +
      `extra=${fmtMiB(extraBytes)} budget=${fmtMiB(args.budgetBytes)} ` +
      `segments=${plainRuns[0]!.segmentCount}/${keyedRuns[0]!.segmentCount}`
  );
}

describe("routing key memory overhead", () => {
  t(
    "keeps keyed stream build RSS overhead within configured budget for a 100MB class dataset",
    async () => {
      await assertVariantBudget({
        variant: "build-only",
        budgetBytes: ALLOWED_EXTRA_BYTES_BUILD_ONLY,
        requireIndexedRuns: false,
      });
    },
    TEST_TIMEOUT_MS
  );

  t(
    "keeps keyed stream build plus routing+lexicon indexing RSS overhead within configured budget",
    async () => {
      await assertVariantBudget({
        variant: "with-indexing",
        budgetBytes: ALLOWED_EXTRA_BYTES_WITH_INDEXING,
        requireIndexedRuns: true,
      });
    },
    TEST_TIMEOUT_MS
  );
});
