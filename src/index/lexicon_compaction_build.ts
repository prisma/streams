import { readFileSync } from "node:fs";
import { Result } from "better-result";
import { decodeLexiconRunResult, encodeLexiconRunResult, buildLexiconRunPayload, type LexiconRun } from "./lexicon_format";
import { RestartStringTableView } from "../search/binary/restart_strings";
import { lexiconRunObjectKey, streamHash16Hex } from "../util/stream_paths";

export type LexiconCompactionRunSource = {
  runId: string;
  startSegment: number;
  endSegment: number;
} & ({ localPath: string; bytes?: undefined } | { localPath?: undefined; bytes: Uint8Array });

export type LexiconCompactionBuildInput = {
  stream: string;
  sourceKind: string;
  sourceName: string;
  level: number;
  inputs: LexiconCompactionRunSource[];
};

export type LexiconCompactionBuildOutput = {
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

function readSourceBytes(source: LexiconCompactionRunSource): Uint8Array {
  return source.localPath ? readFileSync(source.localPath) : source.bytes!;
}

function nextTerm(view: RestartStringTableView, ordinal: number): string | null {
  return ordinal < view.count() ? view.termAt(ordinal) : null;
}

export function buildLexiconCompactionPayloadResult(
  input: LexiconCompactionBuildInput
): Result<LexiconCompactionBuildOutput, BuildError> {
  if (input.inputs.length === 0) return invalidLexiconBuild("compact: missing inputs");
  const cursors: Array<{ view: RestartStringTableView; ordinal: number; current: string | null }> = [];
  for (const meta of input.inputs) {
    const runRes = decodeLexiconRunResult(readSourceBytes(meta));
    if (Result.isError(runRes)) return invalidLexiconBuild(runRes.error.message);
    const run = runRes.value;
    if (run.meta.startSegment !== meta.startSegment || run.meta.endSegment !== meta.endSegment) {
      return invalidLexiconBuild(`lexicon compact run ${meta.runId} metadata mismatch`);
    }
    cursors.push({ view: run.terms, ordinal: 0, current: nextTerm(run.terms, 0) });
  }

  const merged: string[] = [];
  let lastValue: string | null = null;
  for (;;) {
    let smallest: string | null = null;
    for (const cursor of cursors) {
      if (cursor.current == null) continue;
      if (smallest == null || compareKeys(cursor.current, smallest) < 0) smallest = cursor.current;
    }
    if (smallest == null) break;
    if (smallest !== lastValue) {
      merged.push(smallest);
      lastValue = smallest;
    }
    for (const cursor of cursors) {
      while (cursor.current != null && cursor.current === smallest) {
        cursor.ordinal += 1;
        cursor.current = nextTerm(cursor.view, cursor.ordinal);
      }
    }
  }

  const startSegment = input.inputs[0]!.startSegment;
  const endSegment = input.inputs[input.inputs.length - 1]!.endSegment;
  const runId = `${input.sourceKind}-${input.sourceName || "default"}-l${input.level}-${startSegment.toString().padStart(16, "0")}-${endSegment
    .toString()
    .padStart(16, "0")}-${Date.now()}`;
  const payloadBytes = buildLexiconRunPayload(merged);
  const run: LexiconRun = {
    meta: {
      runId,
      level: input.level,
      startSegment,
      endSegment,
      objectKey: lexiconRunObjectKey(streamHash16Hex(input.stream), input.sourceKind, input.sourceName, runId),
      recordCount: merged.length,
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
