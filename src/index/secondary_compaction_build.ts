import { readFileSync } from "node:fs";
import { Result } from "better-result";
import { buildBinaryFuseResult } from "./binary_fuse";
import { decodeIndexRunResult, encodeIndexRunResult, RUN_TYPE_MASK16, RUN_TYPE_POSTINGS, type IndexRun } from "./run_format";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../util/stream_paths";

export type SecondaryCompactionRunSource = {
  runId: string;
  startSegment: number;
  endSegment: number;
} & ({ localPath: string; bytes?: undefined } | { localPath?: undefined; bytes: Uint8Array });

export type SecondaryCompactionBuildInput = {
  stream: string;
  indexName: string;
  level: number;
  inputs: SecondaryCompactionRunSource[];
};

export type SecondaryCompactionBuildOutput = {
  meta: {
    runId: string;
    level: number;
    startSegment: number;
    endSegment: number;
    objectKey: string;
    filterLen: number;
    recordCount: number;
  };
  payload: Uint8Array;
};

type BuildError = {
  kind: "invalid_index_build";
  message: string;
};

function invalidIndexBuild(message: string): Result<never, BuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function readSourceBytes(source: SecondaryCompactionRunSource): Uint8Array {
  return source.localPath ? readFileSync(source.localPath) : source.bytes!;
}

export function buildSecondaryCompactionPayloadResult(
  input: SecondaryCompactionBuildInput
): Result<SecondaryCompactionBuildOutput, BuildError> {
  if (input.inputs.length === 0) return invalidIndexBuild("compact: missing inputs");
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
    const runRes = decodeIndexRunResult(readSourceBytes(meta));
    if (Result.isError(runRes)) return invalidIndexBuild(runRes.error.message);
    const run = runRes.value;
    if (run.meta.startSegment !== meta.startSegment || run.meta.endSegment !== meta.endSegment) {
      return invalidIndexBuild(`secondary compact run ${meta.runId} metadata mismatch`);
    }
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
    return invalidIndexBuild(`unknown run type ${run.runType}`);
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
  if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
  const runId = `${input.indexName}-l${input.level}-${startSegment.toString().padStart(16, "0")}-${endSegment
    .toString()
    .padStart(16, "0")}-${Date.now()}`;
  const run: IndexRun = {
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
  };
  const payloadRes = encodeIndexRunResult(run);
  if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
  return Result.ok({
    meta: run.meta,
    payload: payloadRes.value,
  });
}
