import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildEvlogEvent } from "../experiments/demo/evlog_ingester";
import { getConfiguredSecondaryIndexes } from "../src/index/secondary_schema";
import { buildSecondaryL0RunPayloadResult } from "../src/index/secondary_l0_build";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter, iterateBlockRecordsResult } from "../src/segment/format";
import { buildEvlogDefaultRegistry } from "../src/profiles/evlog/schema";
import { canonicalizeExactValue } from "../src/search/schema";
import { buildSearchSegmentResult } from "../src/search/search_segment_build";
import { schemaVersionForOffset } from "../src/schema/read_json";
import { getSearchFieldBinding } from "../src/search/schema";
import { resolvePointerResult } from "../src/util/json_pointer";
import { siphash24 } from "../src/util/siphash";
import { buildBinaryFuseResult } from "../src/index/binary_fuse";
import { encodeIndexRunResult, RUN_TYPE_MASK16 } from "../src/index/run_format";
import type { SecondaryIndexField } from "../src/index/secondary_schema";
import { readFileSync } from "node:fs";

type LocalSegment = { segmentIndex: number; startOffset: bigint; localPath: string };

function writeEvlogSegments(root: string, rowsPerSegment: number): LocalSegment[] {
  const encoder = new TextEncoder();
  const segments: LocalSegment[] = [];
  const blocksPerSegment = 8;
  const rowsPerBlock = Math.floor(rowsPerSegment / blocksPerSegment);
  let nextId = 0;

  for (let segmentIndex = 0; segmentIndex < 16; segmentIndex += 1) {
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
        const id = nextId++;
        const timestamp = new Date(Date.UTC(2026, 3, 7, 0, 0, id % 60, id % 1000)).toISOString();
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

    const localPath = join(root, `segment-${segmentIndex}.bin`);
    writeFileSync(localPath, bytes);
    segments.push({
      segmentIndex,
      startOffset: BigInt(segmentIndex * rowsPerSegment),
      localPath,
    });
  }

  return segments;
}

function addRawValues(out: unknown[], value: unknown): void {
  if (Array.isArray(value)) {
    for (const item of value) addRawValues(out, item);
    return;
  }
  out.push(value);
}

function buildLegacySecondaryL0RunPayloadResult(input: {
  stream: string;
  index: SecondaryIndexField;
  registry: ReturnType<typeof buildEvlogDefaultRegistry>;
  startSegment: number;
  span: number;
  secret: Uint8Array;
  segments: LocalSegment[];
}) {
  const PAYLOAD_DECODER = new TextDecoder();
  const TERM_ENCODER = new TextEncoder();
  const maskByFp = new Map<bigint, number>();
  for (const segment of input.segments) {
    const bit = segment.segmentIndex - input.startSegment;
    const maskBit = 1 << bit;
    const local = new Map<bigint, number>();
    const segBytes = readFileSync(segment.localPath);
    let offset = segment.startOffset;
    for (const recordRes of iterateBlockRecordsResult(segBytes)) {
      if (Result.isError(recordRes)) throw new Error(recordRes.error.message);
      let parsed: unknown;
      try {
        parsed = JSON.parse(PAYLOAD_DECODER.decode(recordRes.value.payload));
      } catch {
        offset += 1n;
        continue;
      }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        offset += 1n;
        continue;
      }
      const version = schemaVersionForOffset(input.registry, offset);
      const binding = getSearchFieldBinding(input.index.config, version);
      if (!binding) {
        offset += 1n;
        continue;
      }
      const resolvedRes = resolvePointerResult(parsed, binding.jsonPointer);
      if (Result.isError(resolvedRes)) throw new Error(resolvedRes.error.message);
      if (!resolvedRes.value.exists) {
        offset += 1n;
        continue;
      }
      const rawValues: unknown[] = [];
      addRawValues(rawValues, resolvedRes.value.value);
      const seen = new Set<string>();
      for (const rawValue of rawValues) {
        const canonical = canonicalizeExactValue(input.index.config, rawValue);
        if (canonical == null || seen.has(canonical)) continue;
        seen.add(canonical);
        const fp = siphash24(input.secret, TERM_ENCODER.encode(canonical));
        local.set(fp, (local.get(fp) ?? 0) | maskBit);
      }
      offset += 1n;
    }
    for (const [fp, mask] of local.entries()) {
      maskByFp.set(fp, (maskByFp.get(fp) ?? 0) | mask);
    }
  }
  const fingerprints = Array.from(maskByFp.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const masks = fingerprints.map((fp) => maskByFp.get(fp) ?? 0);
  const fuseRes = buildBinaryFuseResult(fingerprints);
  if (Result.isError(fuseRes)) throw new Error(fuseRes.error.message);
  const endSegment = input.startSegment + input.span - 1;
  const runId = `${input.index.name}-legacy-${input.startSegment}-${endSegment}`;
  const payloadRes = encodeIndexRunResult({
    meta: {
      runId,
      level: 0,
      startSegment: input.startSegment,
      endSegment,
      objectKey: `legacy/${runId}`,
      filterLen: fuseRes.value.bytes.byteLength,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_MASK16,
    filterBytes: fuseRes.value.bytes,
    filter: fuseRes.value.filter,
    fingerprints,
    masks,
  });
  if (Result.isError(payloadRes)) throw new Error(payloadRes.error.message);
  return payloadRes.value;
}

describe("secondary L0 build performance", () => {
  test("batches evlog exact indexes in one segment scan with at least 25% speedup", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-l0-perf-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const indexes = getConfiguredSecondaryIndexes(registry);
      const segments = writeEvlogSegments(root, 2_000);
      const batchedInput = {
        stream: "evlog-1",
        registry,
        startSegment: 0,
        span: 16,
        indexes: indexes.map((index, idx) => ({
          index,
          secret: new Uint8Array(16).fill(idx + 1),
        })),
        segments,
      } as const;

      const warmBatch = buildSecondaryL0RunPayloadResult(batchedInput);
      expect(Result.isOk(warmBatch)).toBe(true);
      const warmLegacyStarted = performance.now();
      for (let idx = 0; idx < indexes.length; idx += 1) {
        void buildLegacySecondaryL0RunPayloadResult({
          stream: "evlog-1",
          index: indexes[idx]!,
          registry,
          startSegment: 0,
          span: 16,
          secret: new Uint8Array(16).fill(idx + 1),
          segments,
        });
      }
      const legacyWarmElapsed = performance.now() - warmLegacyStarted;
      expect(legacyWarmElapsed).toBeGreaterThan(0);

      const legacyStarted = performance.now();
      for (let idx = 0; idx < indexes.length; idx += 1) {
        void buildLegacySecondaryL0RunPayloadResult({
          stream: "evlog-1",
          index: indexes[idx]!,
          registry,
          startSegment: 0,
          span: 16,
          secret: new Uint8Array(16).fill(idx + 1),
          segments,
        });
      }
      const legacyElapsedMs = performance.now() - legacyStarted;

      const batchedStarted = performance.now();
      const batchedRes = buildSecondaryL0RunPayloadResult(batchedInput);
      const batchedElapsedMs = performance.now() - batchedStarted;

      expect(Result.isOk(batchedRes)).toBe(true);
      if (Result.isOk(batchedRes)) {
        expect(batchedRes.value.runs).toHaveLength(indexes.length);
      }
      expect(batchedElapsedMs).toBeLessThan(legacyElapsedMs * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("multi-segment low-cardinality evlog batches beat repeated single-segment exact-only builds by at least 50%", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-l0-group-span-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const exactIndexes = getConfiguredSecondaryIndexes(registry)
        .filter((entry) => ["method", "status", "duration"].includes(entry.name))
        .map((index, ordinal) => ({
          index,
          secret: new Uint8Array(16).fill(ordinal + 1),
        }));
      const segments = writeEvlogSegments(root, 2_000).slice(0, 2);

      const warmCurrentStarted = performance.now();
      let warmCurrentBytes = 0;
      for (const segment of segments) {
        const res = buildSearchSegmentResult({
          stream: "evlog-1",
          registry,
          exactIndexes,
          plan: null,
          planGeneration: null,
          includeCompanion: false,
          outputDir: join(root, "current"),
          segment,
        });
        if (Result.isError(res)) throw new Error(res.error.message);
        for (const run of res.value.exactRuns) warmCurrentBytes += run.storage === "bytes" ? run.payload.byteLength : run.sizeBytes;
      }
      const warmCurrentElapsedMs = performance.now() - warmCurrentStarted;
      expect(warmCurrentElapsedMs).toBeGreaterThan(0);
      expect(warmCurrentBytes).toBeGreaterThan(0);

      const warmProposedStarted = performance.now();
      const warmProposedRes = buildSecondaryL0RunPayloadResult({
        stream: "evlog-1",
        registry,
        startSegment: 0,
        span: 2,
        outputDir: join(root, "proposed"),
        indexes: exactIndexes,
        segments,
      });
      const warmProposedElapsedMs = performance.now() - warmProposedStarted;
      expect(Result.isOk(warmProposedRes)).toBe(true);
      expect(warmProposedElapsedMs).toBeGreaterThan(0);

      const currentStarted = performance.now();
      let currentBytes = 0;
      for (const segment of segments) {
        const res = buildSearchSegmentResult({
          stream: "evlog-1",
          registry,
          exactIndexes,
          plan: null,
          planGeneration: null,
          includeCompanion: false,
          outputDir: join(root, "current-2"),
          segment,
        });
        if (Result.isError(res)) throw new Error(res.error.message);
        for (const run of res.value.exactRuns) currentBytes += run.storage === "bytes" ? run.payload.byteLength : run.sizeBytes;
      }
      const currentElapsedMs = performance.now() - currentStarted;

      const proposedStarted = performance.now();
      const proposedRes = buildSecondaryL0RunPayloadResult({
        stream: "evlog-1",
        registry,
        startSegment: 0,
        span: 2,
        outputDir: join(root, "proposed-2"),
        indexes: exactIndexes,
        segments,
      });
      const proposedElapsedMs = performance.now() - proposedStarted;
      expect(Result.isOk(proposedRes)).toBe(true);
      if (Result.isError(proposedRes)) throw new Error(proposedRes.error.message);

      const proposedBytes = proposedRes.value.runs.reduce(
        (sum, run) => sum + (run.storage === "bytes" ? run.payload.byteLength : run.sizeBytes),
        0
      );
      expect(proposedElapsedMs).toBeLessThan(currentElapsedMs * 0.5 + 2);
      expect(proposedBytes).toBeGreaterThan(0);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
