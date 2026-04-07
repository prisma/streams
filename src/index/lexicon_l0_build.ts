import { readFileSync } from "node:fs";
import { Result } from "better-result";
import { iterateBlockRecordsResult } from "../segment/format";
import { streamHash16Hex, lexiconRunObjectKey } from "../util/stream_paths";
import { buildLexiconRunPayload, encodeLexiconRunResult, type LexiconRun } from "./lexicon_format";
import { RestartStringTableView } from "../search/binary/restart_strings";

const TEXT_DECODER = new TextDecoder();

export type LexiconL0BuildInput = {
  stream: string;
  sourceKind: string;
  sourceName: string;
  startSegment: number;
  span: number;
  segments: Array<{ segmentIndex: number; localPath: string }>;
};

export type LexiconL0BuildOutput = {
  meta: {
    runId: string;
    level: number;
    startSegment: number;
    endSegment: number;
    objectKey: string;
    recordCount: number;
  };
  payload: Uint8Array;
};

type BuildError = {
  kind: "invalid_lexicon_build";
  message: string;
};

function invalidLexiconBuild(message: string): Result<never, BuildError> {
  return Result.err({ kind: "invalid_lexicon_build", message });
}

function compareKeys(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function buildLexiconL0RunPayloadResult(input: LexiconL0BuildInput): Result<LexiconL0BuildOutput, BuildError> {
  const keys = new Set<string>();
  for (const segment of input.segments) {
    const bit = segment.segmentIndex - input.startSegment;
    if (bit < 0 || bit >= input.span) {
      return invalidLexiconBuild(`lexicon L0 segment ${segment.segmentIndex} is outside the ${input.span}-segment build window`);
    }
    const segBytes = readFileSync(segment.localPath);
    for (const recordRes of iterateBlockRecordsResult(segBytes)) {
      if (Result.isError(recordRes)) return invalidLexiconBuild(recordRes.error.message);
      if (recordRes.value.routingKey.byteLength === 0) continue;
      keys.add(TEXT_DECODER.decode(recordRes.value.routingKey));
    }
  }

  const startSegment = input.startSegment;
  const endSegment = input.startSegment + input.span - 1;
  const sourceNamePart = input.sourceName || "default";
  const runId = `${input.sourceKind}-${sourceNamePart}-l0-${startSegment.toString().padStart(16, "0")}-${endSegment
    .toString()
    .padStart(16, "0")}-${Date.now()}`;
  const objectKey = lexiconRunObjectKey(streamHash16Hex(input.stream), input.sourceKind, input.sourceName, runId);
  const payloadBytes = buildLexiconRunPayload(Array.from(keys).sort(compareKeys));
  const run: LexiconRun = {
    meta: {
      runId,
      level: 0,
      startSegment,
      endSegment,
      objectKey,
      recordCount: keys.size,
    },
    payloadBytes,
    terms: new RestartStringTableView(payloadBytes),
  };
  const payloadRes = encodeLexiconRunResult(run);
  if (Result.isError(payloadRes)) return invalidLexiconBuild(payloadRes.error.message);
  return Result.ok({
    meta: run.meta,
    payload: payloadRes.value,
  });
}
