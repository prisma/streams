import { readFileSync } from "node:fs";
import { Result } from "better-result";
import { iterateBlockRecordsResult } from "../segment/format";
import { siphash24 } from "../util/siphash";
import { indexRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { buildBinaryFuseResult } from "./binary_fuse";
import { encodeIndexRunResult, RUN_TYPE_MASK16, type IndexRun } from "./run_format";

export type RoutingL0BuildInput = {
  stream: string;
  startSegment: number;
  span: number;
  secret: Uint8Array;
  segments: Array<{ segmentIndex: number; localPath: string }>;
};

export type RoutingL0BuildOutput = {
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

export function buildRoutingL0RunPayloadResult(input: RoutingL0BuildInput): Result<RoutingL0BuildOutput, BuildError> {
  const maskByFp = new Map<bigint, number>();
  for (const segment of input.segments) {
    const segBytes = readFileSync(segment.localPath);
    const bit = segment.segmentIndex - input.startSegment;
    if (bit < 0 || bit >= 16) {
      return invalidIndexBuild(`routing L0 segment ${segment.segmentIndex} is outside the 16-segment build window`);
    }
    const maskBit = 1 << bit;
    const local = new Map<bigint, number>();
    for (const recRes of iterateBlockRecordsResult(segBytes)) {
      if (Result.isError(recRes)) return invalidIndexBuild(recRes.error.message);
      if (recRes.value.routingKey.byteLength === 0) continue;
      const fp = siphash24(input.secret, recRes.value.routingKey);
      local.set(fp, (local.get(fp) ?? 0) | maskBit);
    }
    for (const [fp, mask] of local.entries()) {
      maskByFp.set(fp, (maskByFp.get(fp) ?? 0) | mask);
    }
  }

  const fingerprints = Array.from(maskByFp.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const masks = fingerprints.map((fp) => maskByFp.get(fp) ?? 0);
  const fuseRes = buildBinaryFuseResult(fingerprints);
  if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
  const { filter, bytes } = fuseRes.value;
  const endSegment = input.startSegment + input.span - 1;
  const runId = `l0-${input.startSegment.toString().padStart(16, "0")}-${endSegment.toString().padStart(16, "0")}-${Date.now()}`;
  const objectKey = indexRunObjectKey(streamHash16Hex(input.stream), runId);
  const run: IndexRun = {
    meta: {
      runId,
      level: 0,
      startSegment: input.startSegment,
      endSegment,
      objectKey,
      filterLen: bytes.byteLength,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_MASK16,
    filterBytes: bytes,
    filter,
    fingerprints,
    masks,
  };
  const payloadRes = encodeIndexRunResult(run);
  if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
  return Result.ok({
    meta: run.meta,
    payload: payloadRes.value,
  });
}
