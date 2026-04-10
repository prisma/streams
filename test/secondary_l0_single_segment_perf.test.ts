import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildEvlogEvent } from "../experiments/demo/evlog_ingester";
import { buildSecondaryL0RunPayloadResult } from "../src/index/secondary_l0_build";
import { getConfiguredSecondaryIndexes, type SecondaryIndexField } from "../src/index/secondary_schema";
import { buildEvlogDefaultRegistry } from "../src/profiles/evlog/schema";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter, iterateBlockRecordsResult } from "../src/segment/format";
import {
  canonicalizeExactValue,
  compileSearchFieldAccessorsResult,
  extractFastScalarJsonValueFromBytesResult,
  extractRawSearchValuesWithCompiledAccessorsResult,
  type CompiledSearchFieldAccessor,
} from "../src/search/schema";
import { schemaVersionForOffset } from "../src/schema/read_json";
import { siphash24 } from "../src/util/siphash";
import { buildBinaryFuseResult } from "../src/index/binary_fuse";
import { decodeIndexRunResult, encodeIndexRunResult, RUN_TYPE_MASK16, RUN_TYPE_SINGLE_SEGMENT } from "../src/index/run_format";

type LocalSegment = { segmentIndex: number; startOffset: bigint; localPath: string };

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
    segmentIndex: 0,
    startOffset: 0n,
    localPath,
  };
}

type LegacyBatchState = {
  index: SecondaryIndexField;
  secret: Uint8Array;
  maskByFp: Map<bigint, number>;
  fpByCanonical: Map<string, bigint>;
};

function buildCompiledAccessorsForVersionResult(
  registry: ReturnType<typeof buildEvlogDefaultRegistry>,
  indexes: LegacyBatchState[],
  version: number
): Result<CompiledSearchFieldAccessor[], { kind: "invalid_index_build"; message: string }> {
  const compiledRes = compileSearchFieldAccessorsResult(
    registry,
    indexes.map((entry) => entry.index.name),
    version
  );
  if (Result.isError(compiledRes)) return Result.err({ kind: "invalid_index_build", message: compiledRes.error.message });
  return Result.ok(compiledRes.value);
}

function buildLegacySingleSegmentSecondaryL0Result(input: {
  stream: string;
  registry: ReturnType<typeof buildEvlogDefaultRegistry>;
  indexes: Array<{ index: SecondaryIndexField; secret: Uint8Array }>;
  segment: LocalSegment;
}): Result<Uint8Array[], { kind: "invalid_index_build"; message: string }> {
  const states = input.indexes.map((entry) => ({
    index: entry.index,
    secret: entry.secret,
    maskByFp: new Map<bigint, number>(),
    fpByCanonical: new Map<string, bigint>(),
  })) satisfies LegacyBatchState[];
  const stateByName = new Map(states.map((entry) => [entry.index.name, entry]));
  const compiledByVersion = new Map<number, CompiledSearchFieldAccessor[]>();
  const segBytes = readFileSync(input.segment.localPath);
  const localSeen = new Map<string, Set<string>>();
  let offset = input.segment.startOffset;
  const maskBit = 1;
  const termEncoder = new TextEncoder();
  const decoder = new TextDecoder();

  for (const recordRes of iterateBlockRecordsResult(segBytes)) {
    if (Result.isError(recordRes)) return Result.err({ kind: "invalid_index_build", message: recordRes.error.message });
    let parsed: unknown;
    try {
      parsed = JSON.parse(decoder.decode(recordRes.value.payload));
    } catch {
      offset += 1n;
      continue;
    }
    const version = schemaVersionForOffset(input.registry, offset);
    let accessors = compiledByVersion.get(version);
    if (!accessors) {
      const compiledRes = buildCompiledAccessorsForVersionResult(input.registry, states, version);
      if (Result.isError(compiledRes)) return compiledRes;
      accessors = compiledRes.value;
      compiledByVersion.set(version, accessors);
    }
    const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, accessors);
    if (Result.isError(rawValuesRes)) return Result.err({ kind: "invalid_index_build", message: rawValuesRes.error.message });
    for (const [fieldName, rawValues] of rawValuesRes.value) {
      const state = stateByName.get(fieldName);
      if (!state) continue;
      let seenForField = localSeen.get(fieldName);
      if (!seenForField) {
        seenForField = new Set<string>();
        localSeen.set(fieldName, seenForField);
      }
      for (const rawValue of rawValues) {
        const canonical = canonicalizeExactValue(state.index.config, rawValue);
        if (canonical == null || seenForField.has(canonical)) continue;
        seenForField.add(canonical);
        let fp = state.fpByCanonical.get(canonical);
        if (fp == null) {
          fp = siphash24(state.secret, termEncoder.encode(canonical));
          state.fpByCanonical.set(canonical, fp);
        }
        state.maskByFp.set(fp, (state.maskByFp.get(fp) ?? 0) | maskBit);
      }
    }
    offset += 1n;
  }

  const payloads: Uint8Array[] = [];
  for (const state of states) {
    const fingerprints = Array.from(state.maskByFp.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const masks = fingerprints.map((fp) => state.maskByFp.get(fp) ?? 0);
    const fuseRes = buildBinaryFuseResult(fingerprints);
    if (Result.isError(fuseRes)) return Result.err({ kind: "invalid_index_build", message: fuseRes.error.message });
    const runId = `${state.index.name}-legacy-0-0`;
    const payloadRes = encodeIndexRunResult({
      meta: {
        runId,
        level: 0,
        startSegment: 0,
        endSegment: 0,
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
    if (Result.isError(payloadRes)) return Result.err({ kind: "invalid_index_build", message: payloadRes.error.message });
    payloads.push(payloadRes.value);
  }
  return Result.ok(payloads);
}

function buildLegacySingleFieldSingleSegmentSecondaryL0Result(input: {
  stream: string;
  registry: ReturnType<typeof buildEvlogDefaultRegistry>;
  index: { index: SecondaryIndexField; secret: Uint8Array };
  segment: LocalSegment;
}): Result<Uint8Array, { kind: "invalid_index_build"; message: string }> {
  const fpByCanonical = new Map<string, bigint>();
  const compiledByVersion = new Map<number, CompiledSearchFieldAccessor | null>();
  const decoder = new TextDecoder();
  const termEncoder = new TextEncoder();
  const segBytes = readFileSync(input.segment.localPath);
  let offset = input.segment.startOffset;

  for (const recordRes of iterateBlockRecordsResult(segBytes)) {
    if (Result.isError(recordRes)) return Result.err({ kind: "invalid_index_build", message: recordRes.error.message });
    let parsed: unknown;
    try {
      parsed = JSON.parse(decoder.decode(recordRes.value.payload));
    } catch {
      offset += 1n;
      continue;
    }
    const version = schemaVersionForOffset(input.registry, offset);
    let accessor = compiledByVersion.get(version);
    if (accessor === undefined) {
      const compiledRes = compileSearchFieldAccessorsResult(input.registry, [input.index.index.name], version);
      if (Result.isError(compiledRes)) return Result.err({ kind: "invalid_index_build", message: compiledRes.error.message });
      accessor = compiledRes.value[0] ?? null;
      compiledByVersion.set(version, accessor);
    }
    if (accessor) {
      const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, [accessor]);
      if (Result.isError(rawValuesRes)) return Result.err({ kind: "invalid_index_build", message: rawValuesRes.error.message });
      const values = rawValuesRes.value.get(accessor.fieldName) ?? [];
      for (const rawValue of values) {
        const canonical = canonicalizeExactValue(input.index.index.config, rawValue);
        if (canonical == null || fpByCanonical.has(canonical)) continue;
        fpByCanonical.set(canonical, siphash24(input.index.secret, termEncoder.encode(canonical)));
      }
    }
    offset += 1n;
  }

  const fingerprints = Array.from(fpByCanonical.values()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const runId = `${input.index.index.name}-legacy-fast-0-0`;
  const payloadRes = encodeIndexRunResult({
    meta: {
      runId,
      level: 0,
      startSegment: 0,
      endSegment: 0,
      objectKey: `legacy/${runId}`,
      filterLen: 0,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_MASK16,
    filterBytes: new Uint8Array(0),
    fingerprints,
    masks: new Array(fingerprints.length).fill(1),
  });
  if (Result.isError(payloadRes)) return Result.err({ kind: "invalid_index_build", message: payloadRes.error.message });
  return Result.ok(payloadRes.value);
}

describe("single-segment secondary L0 build performance", () => {
  test("one-segment exact batches stay below 3s on the synthetic evlog segment", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-l0-single-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const indexes = getConfiguredSecondaryIndexes(registry);
      const segment = writeEvlogSegment(root, 24_000);
      const input = {
        stream: "evlog-1",
        registry,
        startSegment: 0,
        span: 1,
        indexes: indexes.map((index, idx) => ({
          index,
          secret: new Uint8Array(16).fill(idx + 1),
        })),
        segments: [segment],
      } as const;

      const warmLegacy = buildLegacySingleSegmentSecondaryL0Result({
        stream: "evlog-1",
        registry,
        indexes: input.indexes,
        segment,
      });
      expect(Result.isOk(warmLegacy)).toBe(true);
      const warmCurrent = buildSecondaryL0RunPayloadResult(input);
      expect(Result.isOk(warmCurrent)).toBe(true);

      const legacyRes = buildLegacySingleSegmentSecondaryL0Result({
        stream: "evlog-1",
        registry,
        indexes: input.indexes,
        segment,
      });

      const currentStarted = performance.now();
      const currentRes = buildSecondaryL0RunPayloadResult(input);
      const currentElapsedMs = performance.now() - currentStarted;

      expect(Result.isOk(legacyRes)).toBe(true);
      expect(Result.isOk(currentRes)).toBe(true);
      if (Result.isOk(currentRes)) expect(currentRes.value.runs).toHaveLength(indexes.length);
      expect(currentElapsedMs).toBeLessThan(3_000);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("single-field exact jobs stay below 1s on the synthetic heavy batch", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-l0-heavy-batch-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const indexes = getConfiguredSecondaryIndexes(registry);
      const heavyIndexes = ["spanId", "path"].map((name) => indexes.find((index) => index.name === name)!);
      const segment = writeEvlogSegment(root, 24_000);
      const twoFieldInput = {
        stream: "evlog-1",
        registry,
        startSegment: 0,
        span: 1,
        indexes: heavyIndexes.map((index, idx) => ({
          index,
          secret: new Uint8Array(16).fill(idx + 1),
        })),
        segments: [segment],
      } as const;
      const singleFieldInputs = twoFieldInput.indexes.map((single, idx) => ({
        ...twoFieldInput,
        indexes: [{ ...single, secret: new Uint8Array(16).fill(idx + 11) }],
      })) as const;

      const warmTwo = buildSecondaryL0RunPayloadResult(twoFieldInput);
      expect(Result.isOk(warmTwo)).toBe(true);
      for (const singleFieldInput of singleFieldInputs) {
        const warmSingle = buildSecondaryL0RunPayloadResult(singleFieldInput);
        expect(Result.isOk(warmSingle)).toBe(true);
      }

      const singleFieldElapsedMs = singleFieldInputs.map((singleFieldInput) => {
        const started = performance.now();
        const res = buildSecondaryL0RunPayloadResult(singleFieldInput);
        const elapsedMs = performance.now() - started;
        expect(Result.isOk(res)).toBe(true);
        if (Result.isOk(res)) expect(res.value.runs).toHaveLength(1);
        return elapsedMs;
      });

      expect(Math.max(...singleFieldElapsedMs)).toBeLessThan(1_000);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("single-segment exact payloads use the compact singleton run format", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-l0-singleton-format-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const indexes = getConfiguredSecondaryIndexes(registry);
      const pathIndex = indexes.find((index) => index.name === "path");
      expect(pathIndex).toBeTruthy();
      if (!pathIndex) return;
      const segment = writeEvlogSegment(root, 24_000);
      const currentRes = buildSecondaryL0RunPayloadResult({
        stream: "evlog-1",
        registry,
        startSegment: 0,
        span: 1,
        indexes: [{ index: pathIndex, secret: new Uint8Array(16).fill(7) }],
        segments: [segment],
      });
      const legacyRes = buildLegacySingleFieldSingleSegmentSecondaryL0Result({
        stream: "evlog-1",
        registry,
        index: { index: pathIndex, secret: new Uint8Array(16).fill(7) },
        segment,
      });
      expect(Result.isOk(currentRes)).toBe(true);
      expect(Result.isOk(legacyRes)).toBe(true);
      if (Result.isError(currentRes) || Result.isError(legacyRes)) return;

      const run = currentRes.value.runs[0]!;
      expect(run.storage).toBe("bytes");
      if (run.storage !== "bytes") return;
      const decodedRes = decodeIndexRunResult(run.payload);
      expect(Result.isOk(decodedRes)).toBe(true);
      if (Result.isError(decodedRes)) return;

      expect(decodedRes.value.runType).toBe(RUN_TYPE_SINGLE_SEGMENT);
      expect(run.payload.byteLength).toBeLessThan(legacyRes.value.byteLength * 0.85);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("single-field exact raw extraction is at least 25% faster than full JSON.parse", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-l0-fastpath-"));
    try {
      const registry = buildEvlogDefaultRegistry("evlog-1");
      const segment = writeEvlogSegment(root, 48_000);
      const segBytes = readFileSync(segment.localPath);
      const payloads: Uint8Array[] = [];
      for (const recordRes of iterateBlockRecordsResult(segBytes)) {
        expect(Result.isOk(recordRes)).toBe(true);
        if (Result.isOk(recordRes)) payloads.push(recordRes.value.payload);
      }
      const accessorsRes = compileSearchFieldAccessorsResult(registry, ["requestId"], 1);
      expect(Result.isOk(accessorsRes)).toBe(true);
      if (Result.isError(accessorsRes)) return;
      const accessor = accessorsRes.value[0]!;
      const decoder = new TextDecoder();

      const consumeLegacy = () => {
        const out = new Set<string>();
        for (const payload of payloads) {
          const parsed = JSON.parse(decoder.decode(payload));
          const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(parsed, [accessor]);
          expect(Result.isOk(rawValuesRes)).toBe(true);
          if (Result.isError(rawValuesRes)) continue;
          for (const rawValue of rawValuesRes.value.get(accessor.fieldName) ?? []) {
            const canonical = canonicalizeExactValue(accessor.config, rawValue);
            if (canonical != null) out.add(canonical);
          }
        }
        return out.size;
      };

      const consumeFast = () => {
        const out = new Set<string>();
        for (const payload of payloads) {
          const rawValueRes = extractFastScalarJsonValueFromBytesResult(payload, accessor);
          expect(Result.isOk(rawValueRes)).toBe(true);
          if (Result.isError(rawValueRes) || !rawValueRes.value.exists) continue;
          const canonical = canonicalizeExactValue(accessor.config, rawValueRes.value.value);
          if (canonical != null) out.add(canonical);
        }
        return out.size;
      };

      expect(consumeLegacy()).toBeGreaterThan(0);
      expect(consumeFast()).toBeGreaterThan(0);

      const legacyStarted = performance.now();
      const legacyCount = consumeLegacy();
      const legacyElapsedMs = performance.now() - legacyStarted;

      const currentStarted = performance.now();
      const currentCount = consumeFast();
      const currentElapsedMs = performance.now() - currentStarted;

      expect(currentCount).toBe(legacyCount);
      expect(currentElapsedMs).toBeLessThan(legacyElapsedMs * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
