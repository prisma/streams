import { Result } from "better-result";
import { RestartStringTableView, encodeRestartStringTable } from "../search/binary/restart_strings";
import { concatBytes, readU32BE, readU64BE, writeU32BE, writeU64BE } from "../util/endian";
import { dsError } from "../util/ds_error.ts";

export const LEXICON_RUN_MAGIC = "LRN1";
export const LEXICON_RUN_VERSION = 1;

export type LexiconRunMeta = {
  runId: string;
  level: number;
  startSegment: number;
  endSegment: number;
  objectKey: string;
  recordCount: number;
};

export type LexiconRun = {
  meta: LexiconRunMeta;
  payloadBytes: Uint8Array;
  terms: RestartStringTableView;
};

export type LexiconRunFormatError = {
  kind: "invalid_lexicon_run";
  message: string;
};

function invalidLexiconRun<T = never>(message: string): Result<T, LexiconRunFormatError> {
  return Result.err({ kind: "invalid_lexicon_run", message });
}

export function encodeLexiconRun(run: LexiconRun): Uint8Array {
  const res = encodeLexiconRunResult(run);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function encodeLexiconRunResult(run: LexiconRun): Result<Uint8Array, LexiconRunFormatError> {
  const payload = run.payloadBytes;
  const header = new Uint8Array(32);
  header[0] = LEXICON_RUN_MAGIC.charCodeAt(0);
  header[1] = LEXICON_RUN_MAGIC.charCodeAt(1);
  header[2] = LEXICON_RUN_MAGIC.charCodeAt(2);
  header[3] = LEXICON_RUN_MAGIC.charCodeAt(3);
  header[4] = LEXICON_RUN_VERSION;
  header[5] = run.meta.level & 0xff;
  header[6] = 0;
  header[7] = 0;
  writeU64BE(header, 8, BigInt(run.meta.startSegment));
  writeU64BE(header, 16, BigInt(run.meta.endSegment));
  writeU32BE(header, 24, run.meta.recordCount);
  writeU32BE(header, 28, payload.byteLength);
  return Result.ok(concatBytes([header, payload]));
}

export function decodeLexiconRun(data: Uint8Array): LexiconRun {
  const res = decodeLexiconRunResult(data);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function decodeLexiconRunResult(data: Uint8Array): Result<LexiconRun, LexiconRunFormatError> {
  if (data.byteLength < 32) return invalidLexiconRun("run too short");
  const magic = String.fromCharCode(data[0], data[1], data[2], data[3]);
  if (magic !== LEXICON_RUN_MAGIC) return invalidLexiconRun("invalid run magic");
  if (data[4] !== LEXICON_RUN_VERSION) return invalidLexiconRun("unsupported run version");
  const level = data[5] ?? 0;
  const startSegment = Number(readU64BE(data, 8));
  const endSegment = Number(readU64BE(data, 16));
  const recordCount = readU32BE(data, 24);
  const payloadLen = readU32BE(data, 28);
  if (32 + payloadLen > data.byteLength) return invalidLexiconRun("run payload truncated");
  const payloadBytes = data.subarray(32, 32 + payloadLen);
  const terms = new RestartStringTableView(payloadBytes);
  if (terms.count() !== recordCount) return invalidLexiconRun("record count mismatch");
  return Result.ok({
    meta: {
      runId: "",
      level,
      startSegment,
      endSegment,
      objectKey: "",
      recordCount,
    },
    payloadBytes,
    terms,
  });
}

export function buildLexiconRunPayload(keys: string[]): Uint8Array {
  return encodeRestartStringTable(keys, 16);
}
