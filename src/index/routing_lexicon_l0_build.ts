import { readFileSync } from "node:fs";
import { Result } from "better-result";
import { RestartStringTableView } from "../search/binary/restart_strings";
import { forEachRoutingKeyResult } from "../segment/format";
import { siphash24 } from "../util/siphash";
import { indexRunObjectKey, lexiconRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { buildBinaryFuseResult } from "./binary_fuse";
import { buildLexiconRunPayload, encodeLexiconRunResult, type LexiconRun } from "./lexicon_format";
import { encodeIndexRunResult, RUN_TYPE_MASK16, type IndexRun } from "./run_format";
import type { LexiconL0BuildOutput } from "./lexicon_l0_build";
import type { RoutingL0BuildOutput } from "./routing_l0_build";

const TEXT_DECODER = new TextDecoder();

export type RoutingLexiconL0BuildInput = {
  stream: string;
  sourceKind: string;
  sourceName: string;
  cacheToken?: string;
  startSegment: number;
  span: number;
  secret: Uint8Array;
  segments: Array<{ segmentIndex: number; localPath: string }>;
};

export type RoutingLexiconL0BuildOutput = {
  routing: RoutingL0BuildOutput;
  lexicon: LexiconL0BuildOutput;
};

type BuildError = {
  kind: "invalid_index_build";
  message: string;
};

function invalidIndexBuild(message: string): Result<never, BuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function compareKeys(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function buildRoutingLexiconL0RunPayloadResult(
  input: RoutingLexiconL0BuildInput
): Result<RoutingLexiconL0BuildOutput, BuildError> {
  const maskByFp = new Map<bigint, number>();
  const fpByKey = new Map<string, bigint>();
  for (const segment of input.segments) {
    const segBytes = readFileSync(segment.localPath);
    const bit = segment.segmentIndex - input.startSegment;
    if (bit < 0 || bit >= input.span) {
      return invalidIndexBuild(
        `routing+lexicon L0 segment ${segment.segmentIndex} is outside the ${input.span}-segment build window`
      );
    }
    const maskBit = 1 << bit;
    const localSeenKeys = new Set<string>();
    const localFingerprints: bigint[] = [];
    const scanRes = forEachRoutingKeyResult(segBytes, (routingKey) => {
      if (routingKey.byteLength === 0) return;
      const key = TEXT_DECODER.decode(routingKey);
      if (localSeenKeys.has(key)) return;
      localSeenKeys.add(key);
      let fp = fpByKey.get(key);
      if (fp == null) {
        fp = siphash24(input.secret, routingKey);
        fpByKey.set(key, fp);
      }
      localFingerprints.push(fp);
    });
    if (Result.isError(scanRes)) {
      return invalidIndexBuild(scanRes.error.message);
    }
    for (const fp of localFingerprints) {
      maskByFp.set(fp, (maskByFp.get(fp) ?? 0) | maskBit);
    }
  }

  const startSegment = input.startSegment;
  const endSegment = input.startSegment + input.span - 1;

  const routingFingerprints = Array.from(maskByFp.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const routingMasks = routingFingerprints.map((fp) => maskByFp.get(fp) ?? 0);
  const fuseRes = buildBinaryFuseResult(routingFingerprints);
  if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
  const routingRunId = `l0-${startSegment.toString().padStart(16, "0")}-${endSegment.toString().padStart(16, "0")}-${Date.now()}`;
  const routingObjectKey = indexRunObjectKey(streamHash16Hex(input.stream), routingRunId);
  const routingRun: IndexRun = {
    meta: {
      runId: routingRunId,
      level: 0,
      startSegment,
      endSegment,
      objectKey: routingObjectKey,
      filterLen: fuseRes.value.bytes.byteLength,
      recordCount: routingFingerprints.length,
    },
    runType: RUN_TYPE_MASK16,
    filterBytes: fuseRes.value.bytes,
    filter: fuseRes.value.filter,
    fingerprints: routingFingerprints,
    masks: routingMasks,
  };
  const routingPayloadRes = encodeIndexRunResult(routingRun);
  if (Result.isError(routingPayloadRes)) return invalidIndexBuild(routingPayloadRes.error.message);

  const lexiconTerms = Array.from(fpByKey.keys()).sort(compareKeys);
  const sourceNamePart = input.sourceName || "default";
  const lexiconRunId = `${input.sourceKind}-${sourceNamePart}-l0-${startSegment.toString().padStart(16, "0")}-${endSegment
    .toString()
    .padStart(16, "0")}-${Date.now()}`;
  const lexiconObjectKey = lexiconRunObjectKey(streamHash16Hex(input.stream), input.sourceKind, input.sourceName, lexiconRunId);
  const lexiconPayloadBytes = buildLexiconRunPayload(lexiconTerms);
  const lexiconRun: LexiconRun = {
    meta: {
      runId: lexiconRunId,
      level: 0,
      startSegment,
      endSegment,
      objectKey: lexiconObjectKey,
      recordCount: lexiconTerms.length,
    },
    payloadBytes: lexiconPayloadBytes,
    terms: new RestartStringTableView(lexiconPayloadBytes),
  };
  const lexiconPayloadRes = encodeLexiconRunResult(lexiconRun);
  if (Result.isError(lexiconPayloadRes)) return invalidIndexBuild(lexiconPayloadRes.error.message);

  return Result.ok({
    routing: {
      meta: routingRun.meta,
      payload: routingPayloadRes.value,
    },
    lexicon: {
      meta: lexiconRun.meta,
      payload: lexiconPayloadRes.value,
    },
  });
}
