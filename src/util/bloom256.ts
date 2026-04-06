import { dsError } from "./ds_error.ts";
/**
 * A tiny 256-bit bloom filter with 3 hash probes.
 *
 * This is used per DSB3 block to cheaply skip blocks during key-filtered reads.
 *
 * Correctness rule: bloom filter may have false positives but MUST NOT have false negatives.
 */

const BITS = 256n;
const MASK64 = (1n << 64n) - 1n;

function fnv1a64(data: Uint8Array): bigint {
  let h = 14695981039346656037n;
  for (const b of data) {
    h ^= BigInt(b);
    h = (h * 1099511628211n) & MASK64;
  }
  return h;
}

function mix64(x: bigint): bigint {
  // SplitMix64-esque mix.
  x = (x + 0x9e3779b97f4a7c15n) & MASK64;
  x = (x ^ (x >> 30n)) * 0xbf58476d1ce4e5b9n & MASK64;
  x = (x ^ (x >> 27n)) * 0x94d049bb133111ebn & MASK64;
  x = x ^ (x >> 31n);
  return x & MASK64;
}

export class Bloom256 {
  private bits: Uint8Array;

  constructor(bits?: Uint8Array) {
    if (bits && bits.byteLength !== 32) throw dsError("bloom must be 32 bytes");
    this.bits = bits ? new Uint8Array(bits) : new Uint8Array(32);
  }

  toBytes(): Uint8Array {
    return new Uint8Array(this.bits);
  }

  add(keyUtf8: Uint8Array): void {
    if (keyUtf8.byteLength === 0) return;
    const h1 = fnv1a64(keyUtf8);
    const h2 = mix64(h1 ^ 0xa0761d6478bd642fn);
    for (let i = 0; i < 3; i++) {
      const idx = Number((h1 + BigInt(i) * h2) % BITS); // 0..2047
      const byte = idx >> 3;
      const bit = idx & 7;
      this.bits[byte] |= 1 << bit;
    }
  }

  maybeHas(keyUtf8: Uint8Array): boolean {
    if (keyUtf8.byteLength === 0) return true;
    const h1 = fnv1a64(keyUtf8);
    const h2 = mix64(h1 ^ 0xa0761d6478bd642fn);
    for (let i = 0; i < 3; i++) {
      const idx = Number((h1 + BigInt(i) * h2) % BITS);
      const byte = idx >> 3;
      const bit = idx & 7;
      if ((this.bits[byte] & (1 << bit)) === 0) return false;
    }
    return true;
  }
}
