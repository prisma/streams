import { Result } from "better-result";
import { dsError } from "./ds_error.ts";
/**
 * Crockford Base32 encoding/decoding.
 *
 * We use this for canonical 128-bit offsets encoded into a fixed 26-character string.
 */

const ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const DECODE_MAP: Record<string, number> = (() => {
  const m: Record<string, number> = {};
  for (let i = 0; i < ALPHABET.length; i++) {
    m[ALPHABET[i]] = i;
    m[ALPHABET[i].toLowerCase()] = i;
  }
  // Crockford aliases
  m["O"] = m["o"] = 0;
  m["I"] = m["i"] = 1;
  m["L"] = m["l"] = 1;
  return m;
})();

export type CrockfordBase32Error = {
  kind: "invalid_base32";
  message: string;
};

function invalidBase32<T = never>(message: string): Result<T, CrockfordBase32Error> {
  return Result.err({ kind: "invalid_base32", message });
}

export function encodeCrockfordBase32Fixed26Result(bytes16: Uint8Array): Result<string, CrockfordBase32Error> {
  if (bytes16.byteLength !== 16) return invalidBase32(`expected 16 bytes, got ${bytes16.byteLength}`);
  // Interpret as big-endian 128-bit unsigned integer.
  let n = 0n;
  for (const b of bytes16) n = (n << 8n) | BigInt(b);
  // Pad with 2 zero bits on the left: 128 -> 130 bits.
  n = n << 2n;

  let out = "";
  // Extract 26 groups of 5 bits from MSB to LSB.
  // Highest bit position is 129.
  for (let i = 0; i < 26; i++) {
    const shift = 5n * BigInt(25 - i);
    const idx = Number((n >> shift) & 31n);
    out += ALPHABET[idx];
  }
  return Result.ok(out);
}

export function encodeCrockfordBase32Fixed26(bytes16: Uint8Array): string {
  const res = encodeCrockfordBase32Fixed26Result(bytes16);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function decodeCrockfordBase32Fixed26Result(s: string): Result<Uint8Array, CrockfordBase32Error> {
  if (s === "-1") return invalidBase32("-1 is a sentinel offset and cannot be decoded as base32");
  if (s.length !== 26) return invalidBase32(`expected 26 chars, got ${s.length}`);
  let n = 0n;
  for (const ch of s) {
    const v = DECODE_MAP[ch];
    if (v === undefined) return invalidBase32(`invalid base32 char: ${ch}`);
    n = (n << 5n) | BigInt(v);
  }
  // Remove the 2 padding bits.
  n = n >> 2n;

  const out = new Uint8Array(16);
  for (let i = 15; i >= 0; i--) {
    out[i] = Number(n & 0xffn);
    n = n >> 8n;
  }
  return Result.ok(out);
}

export function decodeCrockfordBase32Fixed26(s: string): Uint8Array {
  const res = decodeCrockfordBase32Fixed26Result(s);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}
