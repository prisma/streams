import { Result } from "better-result";
import { xxh32Result, type HashError } from "../runtime/hash";
import { dsError } from "../util/ds_error.ts";

export type TouchKeyIdError = HashError;

export function touchKeyIdFromRoutingKeyResult(key: string): Result<number, TouchKeyIdError> {
  const s = key.trim().toLowerCase();
  if (/^[0-9a-f]{16}$/.test(s)) {
    // low32 of the canonical 64-bit routing key.
    return Result.ok(Number.parseInt(s.slice(8), 16) >>> 0);
  }
  return xxh32Result(s);
}

export function touchKeyIdFromRoutingKey(key: string): number {
  const res = touchKeyIdFromRoutingKeyResult(key);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}
