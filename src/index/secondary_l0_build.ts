import { readFileSync } from "node:fs";
import { Result } from "better-result";
import type { SchemaRegistry } from "../schema/registry";
import { iterateBlockRecordsResult } from "../segment/format";
import { siphash24 } from "../util/siphash";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { buildBinaryFuseResult } from "./binary_fuse";
import { encodeIndexRunResult, RUN_TYPE_MASK16, type IndexRun } from "./run_format";
import { extractSecondaryIndexValuesForFieldResult, type SecondaryIndexField } from "./secondary_schema";

const PAYLOAD_DECODER = new TextDecoder();
const TERM_ENCODER = new TextEncoder();

export type SecondaryL0BuildInput = {
  stream: string;
  index: SecondaryIndexField;
  registry: SchemaRegistry;
  startSegment: number;
  span: number;
  secret: Uint8Array;
  segments: Array<{ segmentIndex: number; startOffset: bigint; localPath: string }>;
};

export type SecondaryL0BuildOutput = {
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

export function buildSecondaryL0RunPayloadResult(input: SecondaryL0BuildInput): Result<SecondaryL0BuildOutput, BuildError> {
  const maskByFp = new Map<bigint, number>();
  for (const segment of input.segments) {
    const bit = segment.segmentIndex - input.startSegment;
    if (bit < 0 || bit >= input.span) {
      return invalidIndexBuild(`exact L0 segment ${segment.segmentIndex} is outside the ${input.span}-segment build window`);
    }
    const maskBit = 1 << bit;
    const local = new Map<bigint, number>();
    const segBytes = readFileSync(segment.localPath);
    let offset = segment.startOffset;
    for (const recordRes of iterateBlockRecordsResult(segBytes)) {
      if (Result.isError(recordRes)) return invalidIndexBuild(recordRes.error.message);
      let parsed: unknown;
      try {
        parsed = JSON.parse(PAYLOAD_DECODER.decode(recordRes.value.payload));
      } catch {
        offset += 1n;
        continue;
      }
      const valuesRes = extractSecondaryIndexValuesForFieldResult(input.registry, offset, parsed, input.index);
      if (!Result.isError(valuesRes)) {
        for (const value of valuesRes.value) {
          const fp = siphash24(input.secret, TERM_ENCODER.encode(value));
          local.set(fp, (local.get(fp) ?? 0) | maskBit);
        }
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
  if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
  const endSegment = input.startSegment + input.span - 1;
  const runId = `${input.index.name}-l0-${input.startSegment.toString().padStart(16, "0")}-${endSegment
    .toString()
    .padStart(16, "0")}-${Date.now()}`;
  const run: IndexRun = {
    meta: {
      runId,
      level: 0,
      startSegment: input.startSegment,
      endSegment,
      objectKey: secondaryIndexRunObjectKey(streamHash16Hex(input.stream), input.index.name, runId),
      filterLen: fuseRes.value.bytes.byteLength,
      recordCount: fingerprints.length,
    },
    runType: RUN_TYPE_MASK16,
    filterBytes: fuseRes.value.bytes,
    filter: fuseRes.value.filter,
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
