import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildEvlogEvent } from "../experiments/demo/evlog_ingester";
import { getConfiguredSecondaryIndexes } from "../src/index/secondary_schema";
import { buildEvlogDefaultRegistry } from "../src/profiles/evlog/schema";
import { buildDesiredSearchCompanionPlan } from "../src/search/companion_plan";
import { buildSearchSegmentResult } from "../src/search/search_segment_build";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter } from "../src/segment/format";
import { decodeIndexRunResult, encodeIndexRunResult, RUN_TYPE_MASK16, RUN_TYPE_SINGLE_SEGMENT } from "../src/index/run_format";
import { siphash24 } from "../src/util/siphash";

type LocalSegment = { stream: string; segmentIndex: number; startOffset: bigint; localPath: string };

function writeEvlogSegment(root: string, rowCount: number): LocalSegment {
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

  const localPath = join(root, "segment-0.bin");
  writeFileSync(localPath, bytes);
  return {
    stream: "evlog-1",
    segmentIndex: 0,
    startOffset: 0n,
    localPath,
  };
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function buildLegacySingleSegmentExactRunPayload(pathSecret: Uint8Array, segmentIndex: number): Uint8Array {
  const unique = new Set<bigint>();
  const termEncoder = new TextEncoder();
  for (let id = 0; id < 48_000; id += 1) {
    const value = `/service/${id % 23}/route/${id}`;
    unique.add(siphash24(pathSecret, termEncoder.encode(value)));
  }
  const fingerprints = Array.from(unique).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const payloadRes = encodeIndexRunResult({
    meta: {
      runId: `path-l0-${segmentIndex}-${segmentIndex}`,
      level: 0,
      startSegment: segmentIndex,
      endSegment: segmentIndex,
      objectKey: `legacy/path-${segmentIndex}.irn`,
      filterLen: 0,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_MASK16,
    filterBytes: new Uint8Array(0),
    fingerprints,
    masks: new Array(fingerprints.length).fill(1),
  });
  if (Result.isError(payloadRes)) throw new Error(payloadRes.error.message);
  return payloadRes.value;
}

describe("search segment build perf", () => {
  test(
    "building only the frontier exact batch is at least 25% faster than building the full exact set",
    () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-segment-exact-"));
      try {
        const registry = buildEvlogDefaultRegistry("evlog-1");
        const segment = writeEvlogSegment(root, 48_000);
        const plan = buildDesiredSearchCompanionPlan(registry);
        const exactIndexes = getConfiguredSecondaryIndexes(registry).map((index, indexOrdinal) => ({
          index,
          secret: new Uint8Array(16).fill(indexOrdinal + 1),
        }));
        const frontierBatch = exactIndexes.filter((entry) => ["method", "status", "duration"].includes(entry.index.name));

        const buildAllExact = (): void => {
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
        };

        const buildFrontierBatch = (): void => {
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: frontierBatch,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
        };

        buildAllExact();
        buildFrontierBatch();

        const allExactSamples: number[] = [];
        const frontierBatchSamples: number[] = [];
        for (let i = 0; i < 4; i += 1) {
          let startedAt = performance.now();
          buildAllExact();
          allExactSamples.push(performance.now() - startedAt);

          startedAt = performance.now();
          buildFrontierBatch();
          frontierBatchSamples.push(performance.now() - startedAt);
        }

        expect(average(frontierBatchSamples)).toBeLessThan(average(allExactSamples) * 0.75);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    120_000
  );

  test(
    "skipping companion rebuild on a lagging exact batch is at least 25% faster than rebuilding it",
    () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-segment-skip-companion-"));
      try {
        const registry = buildEvlogDefaultRegistry("evlog-1");
        const segment = writeEvlogSegment(root, 48_000);
        const plan = buildDesiredSearchCompanionPlan(registry);
        const exactIndexes = getConfiguredSecondaryIndexes(registry).map((index, indexOrdinal) => ({
          index,
          secret: new Uint8Array(16).fill(indexOrdinal + 1),
        }));
        const laggingBatch = exactIndexes.filter((entry) => ["method", "status", "duration"].includes(entry.index.name));

        const buildWithCompanion = (): void => {
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: laggingBatch,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
        };

        const buildWithoutCompanion = (): void => {
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: laggingBatch,
            plan: null,
            planGeneration: null,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
        };

        buildWithCompanion();
        buildWithoutCompanion();

        const withCompanionSamples: number[] = [];
        const withoutCompanionSamples: number[] = [];
        for (let i = 0; i < 4; i += 1) {
          let startedAt = performance.now();
          buildWithCompanion();
          withCompanionSamples.push(performance.now() - startedAt);

          startedAt = performance.now();
          buildWithoutCompanion();
          withoutCompanionSamples.push(performance.now() - startedAt);
        }

        expect(average(withoutCompanionSamples)).toBeLessThan(average(withCompanionSamples) * 0.75);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    120_000
  );

  test(
    "skipping companion rebuild on the lagging level,service,environment batch is at least 25% faster than rebuilding it",
    () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-segment-skip-owner-companion-"));
      try {
        const registry = buildEvlogDefaultRegistry("evlog-1");
        const segment = writeEvlogSegment(root, 48_000);
        const plan = buildDesiredSearchCompanionPlan(registry);
        const exactIndexes = getConfiguredSecondaryIndexes(registry).map((index, indexOrdinal) => ({
          index,
          secret: new Uint8Array(16).fill(indexOrdinal + 1),
        }));
        const laggingBatch = exactIndexes.filter((entry) => ["level", "service", "environment"].includes(entry.index.name));

        const buildWithCompanion = (): void => {
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: laggingBatch,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
        };

        const buildWithoutCompanion = (): void => {
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: laggingBatch,
            plan: null,
            planGeneration: null,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
        };

        buildWithCompanion();
        buildWithoutCompanion();

        const withCompanionSamples: number[] = [];
        const withoutCompanionSamples: number[] = [];
        for (let i = 0; i < 4; i += 1) {
          let startedAt = performance.now();
          buildWithCompanion();
          withCompanionSamples.push(performance.now() - startedAt);

          startedAt = performance.now();
          buildWithoutCompanion();
          withoutCompanionSamples.push(performance.now() - startedAt);
        }

        expect(average(withoutCompanionSamples)).toBeLessThan(average(withCompanionSamples) * 0.75);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    120_000
  );

  test(
    "splitting the fresh high-cardinality evlog exact batch into deterministic sub-batches cuts peak job time by at least 25%",
    () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-segment-split-evlog-batch-"));
      try {
        const registry = buildEvlogDefaultRegistry("evlog-1");
        const segment = writeEvlogSegment(root, 48_000);
        const plan = buildDesiredSearchCompanionPlan(registry);
        const exactIndexes = getConfiguredSecondaryIndexes(registry).map((index, indexOrdinal) => ({
          index,
          secret: new Uint8Array(16).fill(indexOrdinal + 1),
        }));
        const largeBatch = exactIndexes.filter((entry) =>
          ["timestamp", "level", "service", "environment", "requestId", "traceId", "spanId", "path"].includes(entry.index.name)
        );
        const splitBatches = [
          ["level", "service", "environment"],
          ["timestamp"],
          ["requestId"],
          ["traceId"],
          ["spanId"],
          ["path"],
        ].map((names) => exactIndexes.filter((entry) => names.includes(entry.index.name)));

        const buildMonolithic = (): number => {
          const startedAt = performance.now();
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: largeBatch,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
          return performance.now() - startedAt;
        };

        const buildSplitPeak = (): number => {
          let maxDuration = 0;
          for (const [index, batch] of splitBatches.entries()) {
            const startedAt = performance.now();
            const res = buildSearchSegmentResult({
              stream: segment.stream,
              registry,
              exactIndexes: batch,
              plan: index === 0 ? plan : null,
              planGeneration: index === 0 ? 1 : null,
              segment,
            });
            if (Result.isError(res)) throw new Error(res.error.message);
            maxDuration = Math.max(maxDuration, performance.now() - startedAt);
          }
          return maxDuration;
        };

        buildMonolithic();
        buildSplitPeak();

        const monolithicSamples: number[] = [];
        const splitPeakSamples: number[] = [];
        for (let i = 0; i < 4; i += 1) {
          monolithicSamples.push(buildMonolithic());
          splitPeakSamples.push(buildSplitPeak());
        }

        expect(average(splitPeakSamples)).toBeLessThan(average(monolithicSamples) * 0.75);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    120_000
  );

  test(
    "splitting evlog exact work into low-cardinality sub-batches keeps peak job time at least 25% below one all-exact pass",
    () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-segment-grouped-exact-only-"));
      try {
        const registry = buildEvlogDefaultRegistry("evlog-1");
        const segment = writeEvlogSegment(root, 48_000);
        const exactIndexes = getConfiguredSecondaryIndexes(registry).map((index, indexOrdinal) => ({
          index,
          secret: new Uint8Array(16).fill(indexOrdinal + 1),
        }));
        const monolithicBatch = exactIndexes;
        const groupedBatches = [
          ["timestamp"],
          ["level", "service", "environment"],
          ["method", "status", "duration"],
          ["requestId"],
          ["traceId"],
          ["spanId"],
          ["path"],
        ].map((names) => exactIndexes.filter((entry) => names.includes(entry.index.name)));

        const buildMonolithic = (): number => {
          const startedAt = performance.now();
          const res = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes: monolithicBatch,
            plan: null,
            planGeneration: null,
            segment,
          });
          if (Result.isError(res)) throw new Error(res.error.message);
          return performance.now() - startedAt;
        };

        const buildGroupedPeak = (): number => {
          let maxDuration = 0;
          for (const batch of groupedBatches) {
            const startedAt = performance.now();
            const res = buildSearchSegmentResult({
              stream: segment.stream,
              registry,
              exactIndexes: batch,
              plan: null,
              planGeneration: null,
              segment,
            });
            if (Result.isError(res)) throw new Error(res.error.message);
            maxDuration = Math.max(maxDuration, performance.now() - startedAt);
          }
          return maxDuration;
        };

        buildMonolithic();
        buildGroupedPeak();

        const monolithicSamples: number[] = [];
        const groupedPeakSamples: number[] = [];
        for (let i = 0; i < 4; i += 1) {
          monolithicSamples.push(buildMonolithic());
          groupedPeakSamples.push(buildGroupedPeak());
        }

        expect(average(groupedPeakSamples)).toBeLessThan(average(monolithicSamples) * 0.75);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    120_000
  );

  test("search exact runs use the compact singleton run format", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-search-segment-singleton-format-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 48_000);
      const pathIndex = getConfiguredSecondaryIndexes(registry).find((index) => index.name === "path");
      expect(pathIndex).toBeTruthy();
      if (!pathIndex) return;
      const secret = new Uint8Array(16).fill(9);
      const buildRes = buildSearchSegmentResult({
        stream: segment.stream,
        registry,
        exactIndexes: [{ index: pathIndex, secret }],
        plan: null,
        planGeneration: null,
        segment,
      });
      expect(Result.isOk(buildRes)).toBe(true);
      if (Result.isError(buildRes)) return;
      const run = buildRes.value.exactRuns[0]!;
      expect(run.storage).toBe("bytes");
      if (run.storage !== "bytes") return;

      const decodedRes = decodeIndexRunResult(run.payload);
      expect(Result.isOk(decodedRes)).toBe(true);
      if (Result.isError(decodedRes)) return;
      expect(decodedRes.value.runType).toBe(RUN_TYPE_SINGLE_SEGMENT);

      const legacyPayload = buildLegacySingleSegmentExactRunPayload(secret, segment.segmentIndex);
      expect(run.payload.byteLength).toBeLessThan(legacyPayload.byteLength * 0.85);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 120_000);

});
