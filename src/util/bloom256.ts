import { dsError } from "./ds_error.ts";
/**
 * A tiny 256-bit bloom filter with 3 hash probes.
 *
 * This is used per DSB3 block to cheaply skip blocks during key-filtered reads.
 *
 * Correctness rule: bloom filter may have false positives but MUST NOT have false negatives.
 */

const BLOOM_BITS_MASK = 255;

function fnv1a32(data: Uint8Array): number {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < data.byteLength; i++) {
    h ^= data[i]!;
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h >>> 0;
}

function mix32(x: number): number {
  let h = (x ^ 0x9e3779b9) >>> 0;
  h ^= h >>> 16;
  h = Math.imul(h, 0x7feb352d) >>> 0;
  h ^= h >>> 15;
  h = Math.imul(h, 0x846ca68b) >>> 0;
  h ^= h >>> 16;
  return h >>> 0;
}

function setProbeBits(bits: Uint8Array, h1: number, h2: number): void {
  const step = (h2 | 1) >>> 0;
  for (let i = 0; i < 3; i++) {
    const idx = (h1 + Math.imul(i, step)) & BLOOM_BITS_MASK;
    const byte = idx >> 3;
    const bit = idx & 7;
    bits[byte] |= 1 << bit;
  }
}

function hasProbeBits(bits: Uint8Array, h1: number, h2: number): boolean {
  const step = (h2 | 1) >>> 0;
  for (let i = 0; i < 3; i++) {
    const idx = (h1 + Math.imul(i, step)) & BLOOM_BITS_MASK;
    const byte = idx >> 3;
    const bit = idx & 7;
    if ((bits[byte] & (1 << bit)) === 0) return false;
  }
  return true;
}

export function bloom256MaskForKey(keyUtf8: Uint8Array): Uint8Array {
  const bits = new Uint8Array(32);
  if (keyUtf8.byteLength === 0) return bits;
  const h1 = fnv1a32(keyUtf8);
  const h2 = mix32(h1);
  setProbeBits(bits, h1, h2);
  return bits;
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

  orMask(mask: Uint8Array): void {
    if (mask.byteLength !== 32) throw dsError("bloom mask must be 32 bytes");
    for (let i = 0; i < 32; i++) {
      this.bits[i] |= mask[i]!;
    }
  }

  add(keyUtf8: Uint8Array): void {
    if (keyUtf8.byteLength === 0) return;
    const h1 = fnv1a32(keyUtf8);
    const h2 = mix32(h1);
    setProbeBits(this.bits, h1, h2);
  }

  maybeHas(keyUtf8: Uint8Array): boolean {
    if (keyUtf8.byteLength === 0) return true;
    const h1 = fnv1a32(keyUtf8);
    const h2 = mix32(h1);
    return hasProbeBits(this.bits, h1, h2);
  }
}
