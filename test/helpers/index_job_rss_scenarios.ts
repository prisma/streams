import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildEvlogEvent } from "../../experiments/demo/evlog_ingester";
import type { IndexBuildJobInput } from "../../src/index/index_build_job";
import { buildRoutingCompactionPayloadResult } from "../../src/index/routing_compaction_build";
import { buildRoutingLexiconL0RunPayloadResult } from "../../src/index/routing_lexicon_l0_build";
import { getConfiguredSecondaryIndexes } from "../../src/index/secondary_schema";
import { buildSecondaryCompactionPayloadResult } from "../../src/index/secondary_compaction_build";
import { buildSecondaryL0RunPayloadResult } from "../../src/index/secondary_l0_build";
import { buildEvlogDefaultRegistry } from "../../src/profiles/evlog/schema";
import {
  buildCompanionPartialPlans,
  buildEncodedBundledCompanionPayloadResult,
  buildEvlogScratchSegmentsResult,
} from "../../src/search/companion_build";
import { buildDesiredSearchCompanionPlan } from "../../src/search/companion_plan";
import { buildCompanionMergeResult } from "../../src/search/companion_merge";
import { buildSearchSegmentResult } from "../../src/search/search_segment_build";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter } from "../../src/segment/format";
import { buildLexiconCompactionPayloadResult } from "../../src/index/lexicon_compaction_build";

export const INDEX_JOB_RSS_SCENARIOS = [
  "routing_lexicon_l0_build",
  "routing_compaction_build",
  "lexicon_compaction_build",
  "secondary_l0_build",
  "secondary_l0_build_grouped_span4",
  "secondary_compaction_build",
  "companion_build_col",
  "companion_build_keyword_core",
  "companion_build_keyword_request_id",
  "companion_build_keyword_trace_id_shard_1",
  "companion_build_keyword_trace_id_shard_2",
  "companion_build_keyword_span_id",
  "companion_build_keyword_path",
  "companion_build_text_message",
  "companion_build_text_context",
  "companion_merge_build",
  "search_segment_build_exact_only",
] as const;

export type IndexJobRssScenario = (typeof INDEX_JOB_RSS_SCENARIOS)[number];

export type IndexJobRssMeasurement = {
  scenario: IndexJobRssScenario;
  durationMs: number;
  baselineRssBytes: number;
  peakRssBytes: number;
  settledRssBytes: number;
  peakContributedRssBytes: number;
  settledContributedRssBytes: number;
  baselineHeapUsedBytes: number;
  peakHeapUsedBytes: number;
  settledHeapUsedBytes: number;
  baselineExternalBytes: number;
  peakExternalBytes: number;
  settledExternalBytes: number;
  baselineArrayBuffersBytes: number;
  peakArrayBuffersBytes: number;
  settledArrayBuffersBytes: number;
};

type LocalSegment = {
  stream?: string;
  segmentIndex: number;
  startOffset?: bigint;
  localPath: string;
};

function currentRssBytes(): number {
  return process.memoryUsage().rss;
}

type MemorySnapshot = ReturnType<typeof process.memoryUsage>;

function captureMemorySnapshot(): MemorySnapshot {
  return process.memoryUsage();
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

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function measureScenario(
  scenario: IndexJobRssScenario,
  run: () => void | Promise<void>
): Promise<IndexJobRssMeasurement> {
  maybeForceGc();
  await sleep(25);
  const baseline = captureMemorySnapshot();
  const samplerRoot = mkdtempSync(join(tmpdir(), `ds-index-job-rss-sampler-${scenario}-`));
  const stopPath = join(samplerRoot, "stop");
  const outputPath = join(samplerRoot, "result.json");
  const env: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value != null) env[name] = value;
  }
  const sampler = Bun.spawn({
    cmd: ["bun", "run", "test/helpers/process_tree_rss_sampler.ts", String(process.pid), stopPath, outputPath],
    cwd: process.cwd(),
    env,
    stdout: "ignore",
    stderr: "pipe",
  });
  const startedAt = performance.now();
  try {
    await run();
  } finally {
    writeFileSync(stopPath, "");
  }
  const durationMs = performance.now() - startedAt;
  const samplerExitCode = await sampler.exited;
  if (samplerExitCode !== 0) {
    const stderr = sampler.stderr ? await new Response(sampler.stderr).text() : "";
    throw new Error(`rss sampler failed scenario=${scenario} exit=${samplerExitCode}\n${stderr}`);
  }
  const peakRssBytes =
    (JSON.parse(readFileSync(outputPath, "utf8")) as { peakRssBytes: number | null }).peakRssBytes ?? baseline.rss;
  const afterRun = captureMemorySnapshot();
  maybeForceGc();
  await sleep(250);
  const settled = captureMemorySnapshot();
  rmSync(samplerRoot, { recursive: true, force: true });
  return {
    scenario,
    durationMs,
    baselineRssBytes: baseline.rss,
    peakRssBytes: Math.max(peakRssBytes, afterRun.rss, settled.rss),
    settledRssBytes: settled.rss,
    peakContributedRssBytes: Math.max(0, Math.max(peakRssBytes, afterRun.rss, settled.rss) - baseline.rss),
    settledContributedRssBytes: Math.max(0, settled.rss - baseline.rss),
    baselineHeapUsedBytes: baseline.heapUsed,
    peakHeapUsedBytes: afterRun.heapUsed,
    settledHeapUsedBytes: settled.heapUsed,
    baselineExternalBytes: baseline.external,
    peakExternalBytes: afterRun.external,
    settledExternalBytes: settled.external,
    baselineArrayBuffersBytes: baseline.arrayBuffers,
    peakArrayBuffersBytes: afterRun.arrayBuffers,
    settledArrayBuffersBytes: settled.arrayBuffers,
  };
}

function writeEvlogSegment(root: string, rowCount: number, segmentIndex = 0): LocalSegment {
  const encoder = new TextEncoder();
  const blocks: Uint8Array[] = [];
  const footerEntries: Array<{
    blockOffset: number;
    firstOffset: bigint;
    recordCount: number;
    compressedLen: number;
    firstAppendNs: bigint;
    lastAppendNs: bigint;
  }> = [];
  const blocksPerSegment = 24;
  const rowsPerBlock = Math.floor(rowCount / blocksPerSegment);
  let nextId = 0;
  let blockOffset = 0;
  let recordBase = 0;

  for (let blockIndex = 0; blockIndex < blocksPerSegment; blockIndex += 1) {
    const records = [];
    const rowLimit = blockIndex === blocksPerSegment - 1 ? rowCount - recordBase : rowsPerBlock;
    for (let recordIndex = 0; recordIndex < rowLimit; recordIndex += 1) {
      const id = nextId++;
      const timestamp = new Date(Date.UTC(2026, 3, 8, 0, 0, id % 60, id % 1000)).toISOString();
      records.push({
        appendNs: BigInt(id + 1),
        routingKey: new Uint8Array(0),
        payload: encoder.encode(JSON.stringify(buildEvlogEvent(id, timestamp))),
      });
    }
    const block = encodeBlock(records);
    blocks.push(block);
    footerEntries.push({
      blockOffset,
      firstOffset: BigInt(recordBase),
      recordCount: records.length,
      compressedLen: block.byteLength - DSB3_HEADER_BYTES,
      firstAppendNs: records[0]!.appendNs,
      lastAppendNs: records[records.length - 1]!.appendNs,
    });
    blockOffset += block.byteLength;
    recordBase += records.length;
  }

  const footer = encodeFooter(footerEntries);
  const bytes = new Uint8Array(blockOffset + footer.byteLength);
  let cursor = 0;
  for (const block of blocks) {
    bytes.set(block, cursor);
    cursor += block.byteLength;
  }
  bytes.set(footer, cursor);

  const localPath = join(root, `segment-${segmentIndex}.bin`);
  writeFileSync(localPath, bytes);
  return {
    stream: "evlog-1",
    segmentIndex,
    startOffset: BigInt(segmentIndex * rowCount),
    localPath,
  };
}

function writeEvlogSegments(root: string, rowsPerSegment: number, segmentCount: number): LocalSegment[] {
  return Array.from({ length: segmentCount }, (_, segmentIndex) => writeEvlogSegment(root, rowsPerSegment, segmentIndex));
}

function writeRepeatedKeySegments(root: string, rowsPerSegment: number, segmentCount: number): LocalSegment[] {
  const encoder = new TextEncoder();
  const keys = Array.from({ length: 100 }, (_, i) =>
    encoder.encode(`cardinality-${String(i).padStart(3, "0")}-${"x".repeat(30)}`)
  );
  const payload = encoder.encode(JSON.stringify({ randomString: "y".repeat(96), randomNumber: 42 }));
  const blocksPerSegment = 12;
  const rowsPerBlock = Math.floor(rowsPerSegment / blocksPerSegment);
  const segments: LocalSegment[] = [];

  for (let segmentIndex = 0; segmentIndex < segmentCount; segmentIndex += 1) {
    const blocks: Uint8Array[] = [];
    const footerEntries: Array<{
      blockOffset: number;
      firstOffset: bigint;
      recordCount: number;
      compressedLen: number;
      firstAppendNs: bigint;
      lastAppendNs: bigint;
    }> = [];
    let blockOffset = 0;
    let recordBase = 0;
    for (let blockIndex = 0; blockIndex < blocksPerSegment; blockIndex += 1) {
      const records = [];
      for (let recordIndex = 0; recordIndex < rowsPerBlock; recordIndex += 1) {
        const key = keys[(recordIndex + blockIndex + segmentIndex) % keys.length]!;
        records.push({
          appendNs: BigInt(recordBase + recordIndex + 1),
          routingKey: key,
          payload,
        });
      }
      const block = encodeBlock(records);
      blocks.push(block);
      footerEntries.push({
        blockOffset,
        firstOffset: BigInt(segmentIndex * rowsPerSegment + recordBase),
        recordCount: records.length,
        compressedLen: block.byteLength - DSB3_HEADER_BYTES,
        firstAppendNs: records[0]!.appendNs,
        lastAppendNs: records[records.length - 1]!.appendNs,
      });
      blockOffset += block.byteLength;
      recordBase += records.length;
    }

    const footer = encodeFooter(footerEntries);
    const bytes = new Uint8Array(blockOffset + footer.byteLength);
    let cursor = 0;
    for (const block of blocks) {
      bytes.set(block, cursor);
      cursor += block.byteLength;
    }
    bytes.set(footer, cursor);

    const localPath = join(root, `routing-segment-${segmentIndex}.bin`);
    writeFileSync(localPath, bytes);
    segments.push({ segmentIndex, localPath });
  }

  return segments;
}

function writePayloadToFile(root: string, name: string, payload: Uint8Array): string {
  const localPath = join(root, name);
  writeFileSync(localPath, payload);
  return localPath;
}

function buildRoutingLexiconL0Sources(root: string): {
  routingInputs: Array<{ runId: string; startSegment: number; endSegment: number; localPath: string }>;
  lexiconInputs: Array<{ runId: string; startSegment: number; endSegment: number; localPath: string }>;
} {
  const secret = new Uint8Array(16).fill(7);
  const segments = writeRepeatedKeySegments(root, 6_000, 32);
  const routingInputs: Array<{ runId: string; startSegment: number; endSegment: number; localPath: string }> = [];
  const lexiconInputs: Array<{ runId: string; startSegment: number; endSegment: number; localPath: string }> = [];

  for (const startSegment of [0, 16]) {
    const buildRes = buildRoutingLexiconL0RunPayloadResult({
      stream: "perf",
      sourceKind: "routing_key",
      sourceName: "",
      startSegment,
      span: 16,
      secret,
      segments: segments.slice(startSegment, startSegment + 16).map((segment) => ({
        segmentIndex: segment.segmentIndex,
        localPath: segment.localPath,
      })),
    });
    if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
    routingInputs.push({
      runId: buildRes.value.routing.meta.runId,
      startSegment,
      endSegment: startSegment + 15,
      localPath: writePayloadToFile(root, `routing-run-${startSegment}.bin`, buildRes.value.routing.payload),
    });
    lexiconInputs.push({
      runId: buildRes.value.lexicon.meta.runId,
      startSegment,
      endSegment: startSegment + 15,
      localPath: writePayloadToFile(root, `lexicon-run-${startSegment}.bin`, buildRes.value.lexicon.payload),
    });
  }

  return { routingInputs, lexiconInputs };
}

function buildSecondaryCompactionSources(root: string): {
  registry: ReturnType<typeof buildEvlogDefaultRegistry>;
  inputs: Array<{ runId: string; startSegment: number; endSegment: number; localPath: string }>;
} {
  const registry = buildEvlogDefaultRegistry("evlog-1");
  const requestId = getConfiguredSecondaryIndexes(registry).find((entry) => entry.name === "requestId");
  if (!requestId) throw new Error("missing requestId exact index");
  const segments = writeEvlogSegments(root, 8_000, 4);
  const inputs: Array<{ runId: string; startSegment: number; endSegment: number; localPath: string }> = [];
  for (const segment of segments) {
    const buildRes = buildSecondaryL0RunPayloadResult({
      stream: "evlog-1",
      registry,
      startSegment: segment.segmentIndex,
      span: 1,
      outputDir: join(root, "secondary-l0"),
      indexes: [{ index: requestId, secret: new Uint8Array(16).fill(9) }],
      segments: [
        {
          segmentIndex: segment.segmentIndex,
          startOffset: segment.startOffset!,
          localPath: segment.localPath,
        },
      ],
    });
    if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
    const run = buildRes.value.runs[0];
    if (!run || run.storage !== "file") throw new Error("expected file-backed secondary L0 run");
    inputs.push({
      runId: run.meta.runId,
      startSegment: segment.segmentIndex,
      endSegment: segment.segmentIndex,
      localPath: run.localPath,
    });
  }
  return { registry, inputs };
}

async function runRoutingLexiconL0Scenario(root: string): Promise<void> {
  const segments = writeRepeatedKeySegments(root, 6_000, 16);
  const buildRes = buildRoutingLexiconL0RunPayloadResult({
    stream: "perf",
    sourceKind: "routing_key",
    sourceName: "",
    startSegment: 0,
    span: 16,
    secret: new Uint8Array(16).fill(7),
    segments: segments.map((segment) => ({ segmentIndex: segment.segmentIndex, localPath: segment.localPath })),
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

async function runRoutingCompactionScenario(root: string): Promise<void> {
  const { routingInputs } = buildRoutingLexiconL0Sources(root);
  const buildRes = buildRoutingCompactionPayloadResult({
    stream: "perf",
    level: 1,
    inputs: routingInputs,
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

async function runLexiconCompactionScenario(root: string): Promise<void> {
  const { lexiconInputs } = buildRoutingLexiconL0Sources(root);
  const buildRes = buildLexiconCompactionPayloadResult({
    stream: "perf",
    sourceKind: "routing_key",
    sourceName: "",
    level: 1,
    inputs: lexiconInputs,
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

async function runSecondaryL0Scenario(root: string): Promise<void> {
  const registry = buildEvlogDefaultRegistry("evlog-1");
  const batchIndexes = getConfiguredSecondaryIndexes(registry)
    .filter((entry) => ["method", "status", "duration"].includes(entry.name))
    .map((index, ordinal) => ({
      index,
      secret: new Uint8Array(16).fill(ordinal + 1),
    }));
  const segment = writeEvlogSegment(root, 48_000, 0);
  const buildRes = buildSecondaryL0RunPayloadResult({
    stream: "evlog-1",
    registry,
    startSegment: 0,
    span: 1,
    outputDir: join(root, "secondary-l0"),
    indexes: batchIndexes,
    segments: [{ segmentIndex: 0, startOffset: segment.startOffset!, localPath: segment.localPath }],
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

async function runSecondaryCompactionScenario(root: string): Promise<void> {
  const { inputs } = buildSecondaryCompactionSources(root);
  const buildRes = buildSecondaryCompactionPayloadResult({
    stream: "evlog-1",
    indexName: "requestId",
    level: 1,
    outputDir: join(root, "secondary-compaction"),
    inputs,
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

async function runCompanionBuildScenario(root: string): Promise<void> {
  const registry = buildEvlogDefaultRegistry("evlog-1");
  const desiredPlan = buildDesiredSearchCompanionPlan(registry);
  const partialPlan = buildCompanionPartialPlans(registry.search?.profile, desiredPlan)[0];
  if (!partialPlan) throw new Error("expected evlog partial companion plan");
  const segment = writeEvlogSegment(root, 48_000, 0);
  const buildRes = buildEncodedBundledCompanionPayloadResult({
    registry,
    plan: partialPlan.plan,
    planGeneration: 1,
    outputDir: join(root, "companion"),
    segment: {
      stream: "evlog-1",
      segmentIndex: 0,
      startOffset: 0n,
      localPath: segment.localPath,
    },
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

function buildEvlogCompanionScenarioInput(
  root: string,
  partialName: string
): Extract<IndexBuildJobInput, { kind: "companion_build" }> {
  const registry = buildEvlogDefaultRegistry("evlog-1");
  const desiredPlan = buildDesiredSearchCompanionPlan(registry);
  const partialPlans = buildCompanionPartialPlans(registry.search?.profile, desiredPlan);
  const partialPlan = partialPlans.find((entry) => entry.name === partialName);
  if (!partialPlan) throw new Error(`missing evlog partial companion plan ${partialName}`);
  const segment = writeEvlogSegment(root, 48_000, 0);
  const uniqueScratchPlans = Array.from(
    new Map(partialPlans.map((entry) => [entry.plan.fields.map((field) => field.name).sort().join(","), entry])).values()
  ).map((entry) => ({ name: entry.name, plan: entry.plan }));
  const scratchRes = buildEvlogScratchSegmentsResult(
    {
      registry,
      plan: desiredPlan,
      planGeneration: 1,
      outputDir: join(root, "companion-scratch"),
      segment: {
        stream: "evlog-1",
        segmentIndex: 0,
        startOffset: 0n,
        localPath: segment.localPath,
      },
    },
    uniqueScratchPlans
  );
  if (Result.isError(scratchRes)) throw new Error(scratchRes.error.message);
  const scratchLocalPath =
    scratchRes.value.find((entry) => entry.plan.fields.map((field) => field.name).sort().join(",") === partialPlan.plan.fields.map((field) => field.name).sort().join(","))?.localPath ??
    segment.localPath;
  return {
    kind: "companion_build",
    input: {
      registry,
      plan: partialPlan.plan,
      planGeneration: 1,
      keywordTermShard: partialPlan.keywordTermShard,
      docIdShard: partialPlan.docIdShard,
      outputDir: join(root, "companion"),
      segment: {
        stream: "evlog-1",
        segmentIndex: 0,
        startOffset: 0n,
        localPath: scratchLocalPath,
      },
    },
  };
}

function buildEvlogCompanionMergeScenarioInput(
  root: string
): Extract<IndexBuildJobInput, { kind: "companion_merge_build" }> {
  const registry = buildEvlogDefaultRegistry("evlog-1");
  const desiredPlan = buildDesiredSearchCompanionPlan(registry);
  const partialPlans = buildCompanionPartialPlans(registry.search?.profile, desiredPlan);
  const segment = writeEvlogSegment(root, 48_000, 0);
  const uniqueScratchPlans = Array.from(
    new Map(partialPlans.map((entry) => [entry.plan.fields.map((field) => field.name).sort().join(","), entry])).values()
  ).map((entry) => ({ name: entry.name, plan: entry.plan }));
  const scratchRes = buildEvlogScratchSegmentsResult(
    {
      registry,
      plan: desiredPlan,
      planGeneration: 1,
      outputDir: join(root, "companion-scratch"),
      segment: {
        stream: "evlog-1",
        segmentIndex: 0,
        startOffset: 0n,
        localPath: segment.localPath,
      },
    },
    uniqueScratchPlans
  );
  if (Result.isError(scratchRes)) throw new Error(scratchRes.error.message);
  const scratchByFields = new Map(
    scratchRes.value.map((entry) => [entry.plan.fields.map((field) => field.name).sort().join(","), entry.localPath])
  );
  const partials = partialPlans.map((partialPlan) => {
    const buildRes = buildEncodedBundledCompanionPayloadResult({
      registry,
      plan: partialPlan.plan,
      planGeneration: 1,
      keywordTermShard: partialPlan.keywordTermShard,
      docIdShard: partialPlan.docIdShard,
      outputDir: join(root, "companion-merge-inputs"),
      segment: {
        stream: "evlog-1",
        segmentIndex: 0,
        startOffset: 0n,
        localPath: scratchByFields.get(partialPlan.plan.fields.map((field) => field.name).sort().join(",")) ?? segment.localPath,
      },
    });
    if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
    if (buildRes.value.storage !== "file") throw new Error("expected file-backed partial companion output");
    return {
      targetName: partialPlan.name,
      localPath: buildRes.value.localPath,
      sizeBytes: buildRes.value.sizeBytes,
      sectionKinds: buildRes.value.sectionKinds,
      primaryTimestampMinMs: buildRes.value.primaryTimestampMinMs,
      primaryTimestampMaxMs: buildRes.value.primaryTimestampMaxMs,
    };
  });
  return {
    kind: "companion_merge_build",
    input: {
      stream: "evlog-1",
      segmentIndex: 0,
      planGeneration: 1,
      outputDir: join(root, "companion-merge"),
      filePrefix: "segment-0",
      partials,
    },
  };
}

async function runSearchSegmentBuildScenario(root: string): Promise<void> {
  const registry = buildEvlogDefaultRegistry("evlog-1");
  const segment = writeEvlogSegment(root, 48_000, 0);
  const exactIndexes = getConfiguredSecondaryIndexes(registry)
    .filter((entry) => ["method", "status", "duration"].includes(entry.name))
    .map((index, ordinal) => ({
      index,
      secret: new Uint8Array(16).fill(ordinal + 1),
    }));
  const buildRes = buildSearchSegmentResult({
    stream: "evlog-1",
    registry,
    exactIndexes,
    plan: null,
    planGeneration: null,
    includeCompanion: false,
    outputDir: join(root, "search-segment"),
    segment: {
      stream: "evlog-1",
      segmentIndex: 0,
      startOffset: 0n,
      localPath: segment.localPath,
    },
  });
  if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
}

export async function runIndexJobRssScenario(scenario: IndexJobRssScenario): Promise<IndexJobRssMeasurement> {
  const root = mkdtempSync(join(tmpdir(), `ds-index-job-rss-${scenario}-`));
  try {
    const run = buildIndexJobRssScenarioRunner(scenario, root);
    return await measureScenario(scenario, run);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export function buildIndexJobInputForScenario(root: string, scenario: IndexJobRssScenario): IndexBuildJobInput {
  switch (scenario) {
    case "routing_lexicon_l0_build": {
      const segments = writeRepeatedKeySegments(root, 6_000, 16);
      return {
        kind: "routing_lexicon_l0_build",
        input: {
          stream: "perf",
          sourceKind: "routing_key",
          sourceName: "",
          startSegment: 0,
          span: 16,
          secret: new Uint8Array(16).fill(7),
          segments: segments.map((segment) => ({ segmentIndex: segment.segmentIndex, localPath: segment.localPath })),
        },
      };
    }
    case "routing_compaction_build": {
      const { routingInputs } = buildRoutingLexiconL0Sources(root);
      return {
        kind: "routing_compaction_build",
        input: {
          stream: "perf",
          level: 1,
          inputs: routingInputs,
        },
      };
    }
    case "lexicon_compaction_build": {
      const { lexiconInputs } = buildRoutingLexiconL0Sources(root);
      return {
        kind: "lexicon_compaction_build",
        input: {
          stream: "perf",
          sourceKind: "routing_key",
          sourceName: "",
          level: 1,
          inputs: lexiconInputs,
        },
      };
    }
    case "secondary_l0_build": {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const batchIndexes = getConfiguredSecondaryIndexes(registry)
        .filter((entry) => ["method", "status", "duration"].includes(entry.name))
        .map((index, ordinal) => ({
          index,
          secret: new Uint8Array(16).fill(ordinal + 1),
        }));
      const segment = writeEvlogSegment(root, 48_000, 0);
      return {
        kind: "secondary_l0_build",
        input: {
          stream: "evlog-1",
          registry,
          startSegment: 0,
          span: 1,
          outputDir: join(root, "secondary-l0"),
          indexes: batchIndexes,
          segments: [{ segmentIndex: 0, startOffset: segment.startOffset!, localPath: segment.localPath }],
        },
      };
    }
    case "secondary_l0_build_grouped_span4": {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const batchIndexes = getConfiguredSecondaryIndexes(registry)
        .filter((entry) => ["level", "service", "environment"].includes(entry.name))
        .map((index, ordinal) => ({
          index,
          secret: new Uint8Array(16).fill(ordinal + 1),
        }));
      const segments = writeEvlogSegments(root, 48_000, 4);
      return {
        kind: "secondary_l0_build",
        input: {
          stream: "evlog-1",
          registry,
          startSegment: 0,
          span: 4,
          outputDir: join(root, "secondary-l0-grouped-span4"),
          indexes: batchIndexes,
          segments: segments.map((segment) => ({
            segmentIndex: segment.segmentIndex,
            startOffset: segment.startOffset!,
            localPath: segment.localPath,
          })),
        },
      };
    }
    case "secondary_compaction_build": {
      const { inputs } = buildSecondaryCompactionSources(root);
      return {
        kind: "secondary_compaction_build",
        input: {
          stream: "evlog-1",
          indexName: "requestId",
          level: 1,
          outputDir: join(root, "secondary-compaction"),
          inputs,
        },
      };
    }
    case "companion_build_col":
      return buildEvlogCompanionScenarioInput(root, "col-duration-doc-1-of-2");
    case "companion_build_keyword_core":
      return buildEvlogCompanionScenarioInput(root, "keyword-service");
    case "companion_build_keyword_request_id":
      return buildEvlogCompanionScenarioInput(root, "keyword-requestId-shard-1-of-4");
    case "companion_build_keyword_trace_id_shard_1":
      return buildEvlogCompanionScenarioInput(root, "keyword-traceId-shard-1-of-4");
    case "companion_build_keyword_trace_id_shard_2":
      return buildEvlogCompanionScenarioInput(root, "keyword-traceId-shard-2-of-4");
    case "companion_build_keyword_span_id":
      return buildEvlogCompanionScenarioInput(root, "keyword-spanId-shard-1-of-4");
    case "companion_build_keyword_path":
      return buildEvlogCompanionScenarioInput(root, "keyword-path-shard-1-of-4");
    case "companion_build_text_message":
      return buildEvlogCompanionScenarioInput(root, "text-message-shard-1-of-4");
    case "companion_build_text_context":
      return buildEvlogCompanionScenarioInput(root, "text-error-message-shard-1-of-4");
    case "companion_merge_build":
      return buildEvlogCompanionMergeScenarioInput(root);
    case "search_segment_build_exact_only": {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 48_000, 0);
      const exactIndexes = getConfiguredSecondaryIndexes(registry)
        .filter((entry) => ["method", "status", "duration"].includes(entry.name))
        .map((index, ordinal) => ({
          index,
          secret: new Uint8Array(16).fill(ordinal + 1),
        }));
      return {
        kind: "search_segment_build",
        input: {
          stream: "evlog-1",
          registry,
          exactIndexes,
          plan: null,
          planGeneration: null,
          includeCompanion: false,
          outputDir: join(root, "search-segment"),
          segment: {
            stream: "evlog-1",
            segmentIndex: 0,
            startOffset: 0n,
            localPath: segment.localPath,
          },
        },
      };
    }
  }
}

function buildIndexJobRssScenarioRunner(scenario: IndexJobRssScenario, root: string): () => Promise<void> {
  const job = buildIndexJobInputForScenario(root, scenario);
  switch (job.kind) {
    case "routing_lexicon_l0_build":
      return async () => {
        const buildRes = buildRoutingLexiconL0RunPayloadResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "routing_compaction_build":
      return async () => {
        const buildRes = buildRoutingCompactionPayloadResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "lexicon_compaction_build":
      return async () => {
        const buildRes = buildLexiconCompactionPayloadResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "secondary_l0_build":
      return async () => {
        const buildRes = buildSecondaryL0RunPayloadResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "secondary_compaction_build":
      return async () => {
        const buildRes = buildSecondaryCompactionPayloadResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "companion_build":
      return async () => {
        const buildRes = buildEncodedBundledCompanionPayloadResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "companion_merge_build":
      return async () => {
        const buildRes = buildCompanionMergeResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
    case "search_segment_build":
      return async () => {
        const buildRes = buildSearchSegmentResult(job.input);
        if (Result.isError(buildRes)) throw new Error(buildRes.error.message);
      };
  }
}
