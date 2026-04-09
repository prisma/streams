import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildBinaryFuseResult } from "../src/index/binary_fuse";
import { buildSecondaryCompactionPayloadResult } from "../src/index/secondary_compaction_build";
import {
  secondaryCompactionFanout,
  secondaryCompactionMaxLevel,
  secondaryCompactionSourceFetchConcurrency,
} from "../src/index/secondary_indexer";
import { decodeIndexRunResult, encodeIndexRunResult, RUN_TYPE_MASK16, RUN_TYPE_POSTINGS, type IndexRun } from "../src/index/run_format";
import type { SecondaryCompactionBuildInput, SecondaryCompactionRunSource } from "../src/index/secondary_compaction_build";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../src/util/stream_paths";
import { ConcurrencyGate } from "../src/concurrency_gate";
import { SegmentDiskCache } from "../src/segment/cache";
import { MockR2Store } from "../src/objectstore/mock_r2";

function writeMaskRun(root: string, stream: string, indexName: string, runId: string, startSegment: number, endSegment: number, fingerprints: bigint[]): SecondaryCompactionRunSource {
  const masks = new Array(fingerprints.length).fill(1);
  const fuseRes = buildBinaryFuseResult(fingerprints);
  if (Result.isError(fuseRes)) throw new Error(fuseRes.error.message);
  const payloadRes = encodeIndexRunResult({
    meta: {
      runId,
      level: 0,
      startSegment,
      endSegment,
      objectKey: secondaryIndexRunObjectKey(streamHash16Hex(stream), indexName, runId),
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
  const localPath = join(root, `${runId}.bin`);
  writeFileSync(localPath, payloadRes.value);
  return { runId, startSegment, endSegment, localPath };
}

function buildLegacySecondaryCompactionPayloadResult(input: SecondaryCompactionBuildInput): Result<ReturnType<typeof decodeIndexRunResult> extends Result<infer T, any> ? { meta: T["meta"]; payload: Uint8Array } : never, { kind: "invalid_index_build"; message: string }> {
  const segments = new Map<bigint, number[]>();
  const addSegment = (fp: bigint, seg: number) => {
    const list = segments.get(fp) ?? [];
    list.push(seg);
    segments.set(fp, list);
  };

  for (const meta of input.inputs) {
    const runBytes = meta.localPath ? readFileSync(meta.localPath) : meta.bytes!;
    const runRes = decodeIndexRunResult(runBytes);
    if (Result.isError(runRes)) return Result.err({ kind: "invalid_index_build", message: runRes.error.message });
    const run = runRes.value;
    if (run.runType === RUN_TYPE_MASK16 && run.masks) {
      for (let i = 0; i < run.fingerprints.length; i += 1) {
        const fp = run.fingerprints[i]!;
        const mask = run.masks[i] ?? 0;
        for (let bit = 0; bit < 16; bit += 1) {
          if ((mask & (1 << bit)) !== 0) addSegment(fp, meta.startSegment + bit);
        }
      }
      continue;
    }
    if (run.runType === RUN_TYPE_POSTINGS && run.postings) {
      for (let i = 0; i < run.fingerprints.length; i += 1) {
        const fp = run.fingerprints[i]!;
        for (const rel of run.postings[i] ?? []) addSegment(fp, meta.startSegment + rel);
      }
      continue;
    }
    return Result.err({ kind: "invalid_index_build", message: `unknown run type ${run.runType}` });
  }

  const startSegment = input.inputs[0]!.startSegment;
  const endSegment = input.inputs[input.inputs.length - 1]!.endSegment;
  const fingerprints = Array.from(segments.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const postings: number[][] = new Array(fingerprints.length);
  for (let i = 0; i < fingerprints.length; i += 1) {
    const fp = fingerprints[i]!;
    const list = segments.get(fp) ?? [];
    list.sort((a, b) => a - b);
    const rel: number[] = [];
    let lastSeg = Number.NaN;
    for (const seg of list) {
      if (seg === lastSeg) continue;
      rel.push(seg - startSegment);
      lastSeg = seg;
    }
    postings[i] = rel;
  }

  const fuseRes = buildBinaryFuseResult(fingerprints);
  if (Result.isError(fuseRes)) return Result.err({ kind: "invalid_index_build", message: fuseRes.error.message });
  const runId = `${input.indexName}-legacy-${startSegment}-${endSegment}`;
  const payloadRes = encodeIndexRunResult({
    meta: {
      runId,
      level: input.level,
      startSegment,
      endSegment,
      objectKey: secondaryIndexRunObjectKey(streamHash16Hex(input.stream), input.indexName, runId),
      filterLen: fuseRes.value.bytes.byteLength,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_POSTINGS,
    filterBytes: fuseRes.value.bytes,
    filter: fuseRes.value.filter,
    fingerprints,
    postings,
  });
  if (Result.isError(payloadRes)) return Result.err({ kind: "invalid_index_build", message: payloadRes.error.message });
  const decodedRes = decodeIndexRunResult(payloadRes.value);
  if (Result.isError(decodedRes)) return Result.err({ kind: "invalid_index_build", message: decodedRes.error.message });
  return Result.ok({ meta: decodedRes.value.meta, payload: payloadRes.value });
}

function buildPreviousSecondaryCompactionPayloadResult(
  input: SecondaryCompactionBuildInput
): Result<
  ReturnType<typeof decodeIndexRunResult> extends Result<infer T, any> ? { meta: T["meta"]; payload: Uint8Array } : never,
  { kind: "invalid_index_build"; message: string }
> {
  const segments = new Map<bigint, number[]>();
  const addSegment = (fp: bigint, seg: number) => {
    let list = segments.get(fp);
    if (!list) {
      list = [];
      segments.set(fp, list);
    }
    list.push(seg);
  };

  for (const meta of input.inputs) {
    const runRes = decodeIndexRunResult(meta.localPath ? readFileSync(meta.localPath) : meta.bytes!);
    if (Result.isError(runRes)) return Result.err({ kind: "invalid_index_build", message: runRes.error.message });
    const run = runRes.value;
    if (run.runType === RUN_TYPE_MASK16 && run.masks) {
      for (let i = 0; i < run.fingerprints.length; i += 1) {
        const fp = run.fingerprints[i]!;
        const mask = run.masks[i] ?? 0;
        for (let bit = 0; bit < 16; bit += 1) {
          if ((mask & (1 << bit)) !== 0) addSegment(fp, meta.startSegment + bit);
        }
      }
      continue;
    }
    if (run.runType === RUN_TYPE_POSTINGS && run.postings) {
      for (let i = 0; i < run.fingerprints.length; i += 1) {
        const fp = run.fingerprints[i]!;
        for (const rel of run.postings[i] ?? []) addSegment(fp, meta.startSegment + rel);
      }
      continue;
    }
    return Result.err({ kind: "invalid_index_build", message: `unknown run type ${run.runType}` });
  }

  const startSegment = input.inputs[0]!.startSegment;
  const endSegment = input.inputs[input.inputs.length - 1]!.endSegment;
  const fingerprints = Array.from(segments.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const postings: number[][] = new Array(fingerprints.length);
  for (let i = 0; i < fingerprints.length; i += 1) {
    const fp = fingerprints[i]!;
    const list = segments.get(fp) ?? [];
    list.sort((a, b) => a - b);
    const rel: number[] = [];
    let lastSeg = Number.NaN;
    for (const seg of list) {
      if (seg === lastSeg) continue;
      rel.push(seg - startSegment);
      lastSeg = seg;
    }
    postings[i] = rel;
  }

  const runId = `${input.indexName}-previous-${startSegment}-${endSegment}`;
  const run: IndexRun = {
    meta: {
      runId,
      level: input.level,
      startSegment,
      endSegment,
      objectKey: secondaryIndexRunObjectKey(streamHash16Hex(input.stream), input.indexName, runId),
      filterLen: 0,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_POSTINGS,
    filterBytes: new Uint8Array(0),
    filter: null,
    fingerprints,
    postings,
  };
  const payloadRes = encodeIndexRunResult(run);
  if (Result.isError(payloadRes)) return Result.err({ kind: "invalid_index_build", message: payloadRes.error.message });
  const decodedRes = decodeIndexRunResult(payloadRes.value);
  if (Result.isError(decodedRes)) return Result.err({ kind: "invalid_index_build", message: decodedRes.error.message });
  return Result.ok({ meta: decodedRes.value.meta, payload: payloadRes.value });
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

describe("secondary compaction performance", () => {
  test("file-backed exact compaction source staging is at least 25% faster than the byte-buffer fallback", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-source-stage-"));
    try {
      const payload = new Uint8Array(32 * 1024 * 1024);
      for (let i = 0; i < payload.byteLength; i += 1) payload[i] = i % 251;
      const spillDir = join(root, "mock-r2");
      const objectKey = "streams/0123456789abcdef0123456789abcdef/secondary-index/evlog-1/requestId/run.irn";
      const store = new MockR2Store({ spillDir, maxInMemoryBytes: 1 });
      await store.put(objectKey, payload);

      async function stageLegacyOnce(sample: number): Promise<number> {
        const cacheDir = join(root, `legacy-cache-${sample}`);
        mkdirSync(cacheDir, { recursive: true });
        const cache = new SegmentDiskCache(cacheDir, payload.byteLength * 2, 1);
        const startedAt = performance.now();
        const bytes = await store.get(objectKey);
        if (!bytes) throw new Error("missing legacy bytes");
        if (!cache.put(objectKey, bytes)) throw new Error("legacy cache put failed");
        return performance.now() - startedAt;
      }

      async function stageFileBackedOnce(sample: number): Promise<number> {
        const cacheDir = join(root, `file-cache-${sample}`);
        const tempDir = join(root, `temps-${sample}`);
        mkdirSync(cacheDir, { recursive: true });
        mkdirSync(tempDir, { recursive: true });
        const cache = new SegmentDiskCache(cacheDir, payload.byteLength * 2, 1);
        const tempPath = join(tempDir, "run.irn");
        const startedAt = performance.now();
        const fileRes = await store.getFile!(objectKey, tempPath);
        if (!fileRes) throw new Error("missing staged file");
        if (!cache.putFromLocal(objectKey, tempPath, fileRes.size)) throw new Error("file-backed cache put failed");
        return performance.now() - startedAt;
      }

      await stageLegacyOnce(-1);
      await stageFileBackedOnce(-1);

      const legacySamples: number[] = [];
      const fileSamples: number[] = [];
      for (let i = 0; i < 4; i += 1) {
        legacySamples.push(await stageLegacyOnce(i));
        fileSamples.push(await stageFileBackedOnce(i));
      }

      expect(average(fileSamples)).toBeLessThan(average(legacySamples) * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("low-cardinality exact compaction source staging is at least 25% faster with the wider fetch concurrency", async () => {
    async function simulateSourcePrepElapsedMs(indexName: string, inputCount: number, delayMs: number): Promise<number> {
      const gate = new ConcurrencyGate(secondaryCompactionSourceFetchConcurrency(indexName, inputCount));
      const startedAt = performance.now();
      await Promise.all(
        Array.from({ length: inputCount }, async () =>
          gate.run(async () => {
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          })
        )
      );
      return performance.now() - startedAt;
    }

    async function simulateLegacySourcePrepElapsedMs(inputCount: number, delayMs: number): Promise<number> {
      const gate = new ConcurrencyGate(2);
      const startedAt = performance.now();
      await Promise.all(
        Array.from({ length: inputCount }, async () =>
          gate.run(async () => {
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          })
        )
      );
      return performance.now() - startedAt;
    }

    const inputCount = 16;
    const delayMs = 40;

    await simulateLegacySourcePrepElapsedMs(inputCount, delayMs);
    await simulateSourcePrepElapsedMs("service", inputCount, delayMs);

    const legacySamples: number[] = [];
    const widenedSamples: number[] = [];
    for (let i = 0; i < 4; i += 1) {
      legacySamples.push(await simulateLegacySourcePrepElapsedMs(inputCount, delayMs));
      widenedSamples.push(await simulateSourcePrepElapsedMs("service", inputCount, delayMs));
    }

    expect(average(widenedSamples)).toBeLessThan(average(legacySamples) * 0.75);
    expect(secondaryCompactionSourceFetchConcurrency("service", inputCount)).toBe(16);
    expect(secondaryCompactionSourceFetchConcurrency("requestId", inputCount)).toBe(2);
    expect(secondaryCompactionFanout("service", 4)).toBe(4);
    expect(secondaryCompactionFanout("requestId", 4)).toBe(2);
    expect(secondaryCompactionMaxLevel("service", 6)).toBe(6);
    expect(secondaryCompactionMaxLevel("requestId", 6)).toBe(0);
  }, 30_000);

  test("high-cardinality exact compaction fan-in 2 is at least 25% faster per job than fan-in 4", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-compact-fanout-perf-"));
    try {
      const stream = "evlog-1";
      const indexName = "spanId";
      const uniquePerRun = 220_000;
      const inputs: SecondaryCompactionRunSource[] = [];
      for (let runIndex = 0; runIndex < 4; runIndex += 1) {
        const fingerprints: bigint[] = [];
        const base = BigInt(runIndex * uniquePerRun + 1);
        for (let i = 0; i < uniquePerRun; i += 1) fingerprints.push(base + BigInt(i));
        inputs.push(writeMaskRun(root, stream, indexName, `fanout-run-${runIndex}`, runIndex, runIndex, fingerprints));
      }

      const fanout4Input: SecondaryCompactionBuildInput = {
        stream,
        indexName,
        level: 1,
        inputs,
        outputDir: join(root, "fanout4"),
      };
      const fanout2Input: SecondaryCompactionBuildInput = {
        stream,
        indexName,
        level: 1,
        inputs: inputs.slice(0, 2),
        outputDir: join(root, "fanout2"),
      };

      const warm4 = buildSecondaryCompactionPayloadResult(fanout4Input);
      const warm2 = buildSecondaryCompactionPayloadResult(fanout2Input);
      expect(Result.isOk(warm4)).toBe(true);
      expect(Result.isOk(warm2)).toBe(true);

      const fanout4Samples: number[] = [];
      const fanout2Samples: number[] = [];
      for (let sample = 0; sample < 3; sample += 1) {
        const started4 = performance.now();
        const res4 = buildSecondaryCompactionPayloadResult(fanout4Input);
        fanout4Samples.push(performance.now() - started4);
        expect(Result.isOk(res4)).toBe(true);

        const started2 = performance.now();
        const res2 = buildSecondaryCompactionPayloadResult(fanout2Input);
        fanout2Samples.push(performance.now() - started2);
        expect(Result.isOk(res2)).toBe(true);
      }

      expect(average(fanout2Samples)).toBeLessThan(average(fanout4Samples) * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("streaming exact compaction is at least 25% faster than the previous eager-decode implementation", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-compact-perf-"));
    try {
      const stream = "evlog-1";
      const indexName = "requestId";
      const uniquePerRun = 45_000;
      const inputs: SecondaryCompactionRunSource[] = [];
      for (let runIndex = 0; runIndex < 4; runIndex += 1) {
        const fingerprints: bigint[] = [];
        const base = BigInt(runIndex * uniquePerRun + 1);
        for (let i = 0; i < uniquePerRun; i += 1) fingerprints.push(base + BigInt(i));
        inputs.push(writeMaskRun(root, stream, indexName, `run-${runIndex}`, runIndex, runIndex, fingerprints));
      }
      const input: SecondaryCompactionBuildInput = {
        stream,
        indexName,
        level: 1,
        inputs,
        outputDir: join(root, "out"),
      };

      const warmPrevious = buildPreviousSecondaryCompactionPayloadResult(input);
      expect(Result.isOk(warmPrevious)).toBe(true);
      const warmCurrent = buildSecondaryCompactionPayloadResult(input);
      expect(Result.isOk(warmCurrent)).toBe(true);

      const previousStarted = performance.now();
      const previousRes = buildPreviousSecondaryCompactionPayloadResult(input);
      const previousElapsedMs = performance.now() - previousStarted;

      const currentStarted = performance.now();
      const currentRes = buildSecondaryCompactionPayloadResult(input);
      const currentElapsedMs = performance.now() - currentStarted;

      expect(Result.isOk(previousRes)).toBe(true);
      expect(Result.isOk(currentRes)).toBe(true);
      if (Result.isOk(currentRes) && Result.isOk(previousRes)) {
        const decodedCurrent = decodeIndexRunResult(
          currentRes.value.storage === "bytes" ? currentRes.value.payload : readFileSync(currentRes.value.localPath)
        );
        const decodedPrevious = decodeIndexRunResult(previousRes.value.payload);
        expect(Result.isOk(decodedCurrent)).toBe(true);
        expect(Result.isOk(decodedPrevious)).toBe(true);
        if (Result.isOk(decodedCurrent) && Result.isOk(decodedPrevious)) {
          expect(decodedCurrent.value.fingerprints).toEqual(decodedPrevious.value.fingerprints);
          expect(decodedCurrent.value.postings).toEqual(decodedPrevious.value.postings);
          expect(decodedCurrent.value.filter).toBeNull();
        }
      }
      expect(currentElapsedMs).toBeLessThan(previousElapsedMs * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);

  test("file-backed exact compaction handoff is at least 25% faster than cloning payload bytes", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-secondary-compact-file-perf-"));
    try {
      const stream = "evlog-1";
      const indexName = "requestId";
      const uniquePerRun = 400_000;
      const inputs: SecondaryCompactionRunSource[] = [];
      for (let runIndex = 0; runIndex < 4; runIndex += 1) {
        const fingerprints: bigint[] = [];
        const base = BigInt(runIndex * uniquePerRun + 1);
        for (let i = 0; i < uniquePerRun; i += 1) fingerprints.push(base + BigInt(i));
        inputs.push(writeMaskRun(root, stream, indexName, `handoff-run-${runIndex}`, runIndex, runIndex, fingerprints));
      }
      const byteInput: SecondaryCompactionBuildInput = {
        stream,
        indexName,
        level: 1,
        inputs,
      };
      const fileInput: SecondaryCompactionBuildInput = {
        ...byteInput,
        outputDir: join(root, "out"),
      };

      const warmBytes = buildSecondaryCompactionPayloadResult(byteInput);
      const warmFile = buildSecondaryCompactionPayloadResult(fileInput);
      expect(Result.isOk(warmBytes)).toBe(true);
      expect(Result.isOk(warmFile)).toBe(true);
      if (Result.isError(warmBytes) || Result.isError(warmFile)) throw new Error("failed to warm exact compaction outputs");

      const bytesRes = warmBytes.value;
      const fileRes = warmFile.value;
      const decodedBytes = decodeIndexRunResult(bytesRes.storage === "bytes" ? bytesRes.payload : readFileSync(bytesRes.localPath));
      const decodedFile = decodeIndexRunResult(fileRes.storage === "bytes" ? fileRes.payload : readFileSync(fileRes.localPath));
      expect(Result.isOk(decodedBytes)).toBe(true);
      expect(Result.isOk(decodedFile)).toBe(true);
      if (Result.isOk(decodedBytes) && Result.isOk(decodedFile)) {
        expect(decodedFile.value.fingerprints).toEqual(decodedBytes.value.fingerprints);
        expect(decodedFile.value.postings).toEqual(decodedBytes.value.postings);
      }

      const clonePasses = 8;
      const bytesStarted = performance.now();
      for (let index = 0; index < clonePasses; index += 1) {
        structuredClone({
          kind: "secondary_compaction_build",
          output: bytesRes,
        });
      }
      const bytesElapsedMs = performance.now() - bytesStarted;

      const fileStarted = performance.now();
      for (let index = 0; index < clonePasses; index += 1) {
        structuredClone({
          kind: "secondary_compaction_build",
          output: fileRes,
        });
      }
      const fileElapsedMs = performance.now() - fileStarted;

      expect(fileElapsedMs).toBeLessThan(bytesElapsedMs * 0.75);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
