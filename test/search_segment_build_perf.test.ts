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
import { buildEncodedBundledCompanionPayloadResult } from "../src/search/companion_build";
import { buildSearchSegmentResult } from "../src/search/search_segment_build";
import { buildSecondaryL0RunPayloadResult } from "../src/index/secondary_l0_build";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter } from "../src/segment/format";

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

describe("search segment build perf", () => {
  test(
    "unified evlog search build is at least 25% faster than split exact + companion builds",
    () => {
      const root = mkdtempSync(join(tmpdir(), "ds-search-segment-build-"));
      try {
        const registry = buildEvlogDefaultRegistry("evlog-1");
        const plan = buildDesiredSearchCompanionPlan(registry);
        const segment = writeEvlogSegment(root, 36_000);
        const exactIndexes = getConfiguredSecondaryIndexes(registry).map((index, indexOrdinal) => ({
          index,
          secret: new Uint8Array(16).fill(indexOrdinal + 1),
        }));

        const splitBuild = (): void => {
          for (const exact of exactIndexes) {
            const exactRes = buildSecondaryL0RunPayloadResult({
              stream: segment.stream,
              registry,
              startSegment: segment.segmentIndex,
              span: 1,
              indexes: [exact],
              segments: [segment],
            });
            if (Result.isError(exactRes)) throw new Error(exactRes.error.message);
          }
          const companionRes = buildEncodedBundledCompanionPayloadResult({
            registry,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(companionRes)) throw new Error(companionRes.error.message);
        };

        const unifiedBuild = (): void => {
          const unifiedRes = buildSearchSegmentResult({
            stream: segment.stream,
            registry,
            exactIndexes,
            plan,
            planGeneration: 1,
            segment,
          });
          if (Result.isError(unifiedRes)) throw new Error(unifiedRes.error.message);
          expect(unifiedRes.value.exactRuns).toHaveLength(exactIndexes.length);
          expect(unifiedRes.value.companion).not.toBeNull();
        };

        splitBuild();
        unifiedBuild();

        const splitSamples: number[] = [];
        const unifiedSamples: number[] = [];
        for (let i = 0; i < 4; i += 1) {
          const splitStart = performance.now();
          splitBuild();
          splitSamples.push(performance.now() - splitStart);

          const unifiedStart = performance.now();
          unifiedBuild();
          unifiedSamples.push(performance.now() - unifiedStart);
        }

        const splitAvg = average(splitSamples);
        const unifiedAvg = average(unifiedSamples);
        expect(unifiedAvg).toBeLessThan(splitAvg * 0.75);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
    120_000
  );

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

});
