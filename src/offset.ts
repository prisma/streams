import { decodeCrockfordBase32Fixed26Result, encodeCrockfordBase32Fixed26Result } from "./util/base32_crockford";
import { readU32BE, writeU32BE } from "./util/endian";
import { Result } from "better-result";
import { dsError } from "./util/ds_error.ts";

export type ParsedOffset =
  | { kind: "start" }
  | { kind: "seq"; epoch: number; seq: bigint; inBlock: number };

const DEFAULT_EPOCH = 0;

export function parseOffsetResult(input: string | null | undefined): Result<ParsedOffset, { kind: "invalid_offset"; message: string }> {
  if (input == null || input === "") {
    return Result.err({ kind: "invalid_offset", message: "missing offset" });
  }
  if (input === "-1") return Result.ok({ kind: "start" });

  if (input.length !== 26) {
    return Result.err({ kind: "invalid_offset", message: `invalid offset length: ${input.length}` });
  }

  const bytesRes = decodeCrockfordBase32Fixed26Result(input);
  if (Result.isError(bytesRes)) return Result.err({ kind: "invalid_offset", message: bytesRes.error.message });
  const bytes = bytesRes.value;
  const epoch = readU32BE(bytes, 0);
  const hi = readU32BE(bytes, 4);
  const lo = readU32BE(bytes, 8);
  const inBlock = readU32BE(bytes, 12);
  // Protocol offsets are shifted by +1 so we never emit reserved "-1".
  const rawSeq = (BigInt(hi) << 32n) | BigInt(lo);
  const seq = rawSeq - 1n;
  return Result.ok({ kind: "seq", epoch, seq, inBlock });
}

export function parseOffset(input: string | null | undefined): ParsedOffset {
  const res = parseOffsetResult(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function encodeOffsetResult(epoch: number, seq: bigint, inBlock = 0): Result<string, { kind: "invalid_offset"; message: string }> {
  if (seq < -1n) return Result.err({ kind: "invalid_offset", message: "invalid offset" });
  const bytes = new Uint8Array(16);
  writeU32BE(bytes, 0, epoch >>> 0);
  const rawSeq = seq + 1n;
  const hi = Number((rawSeq >> 32n) & 0xffffffffn);
  const lo = Number(rawSeq & 0xffffffffn);
  writeU32BE(bytes, 4, hi);
  writeU32BE(bytes, 8, lo);
  writeU32BE(bytes, 12, inBlock >>> 0);
  const encodedRes = encodeCrockfordBase32Fixed26Result(bytes);
  if (Result.isError(encodedRes)) return Result.err({ kind: "invalid_offset", message: encodedRes.error.message });
  return Result.ok(encodedRes.value);
}

export function encodeOffset(epoch: number, seq: bigint, inBlock = 0): string {
  const res = encodeOffsetResult(epoch, seq, inBlock);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function canonicalizeOffset(input: string): string {
  const p = parseOffset(input);
  if (p.kind === "start") return encodeOffset(DEFAULT_EPOCH, -1n, 0);
  return encodeOffset(p.epoch, p.seq, p.inBlock);
}

export function offsetToSeqOrNeg1(p: ParsedOffset): bigint {
  return p.kind === "start" ? -1n : p.seq;
}
