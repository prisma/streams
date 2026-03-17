import { Result } from "better-result";
import { readU32BE, readU64BE, writeU32BE, writeU64BE } from "../util/endian";
import { dsError } from "../util/ds_error.ts";

const MASK_64 = 0xffffffffffffffffn;
const MAX_ITERATIONS = 1024;

export type BinaryFuseFilter = {
  seed: bigint;
  segmentLength: number;
  segmentLengthMask: number;
  segmentCount: number;
  segmentCountLength: number;
  fingerprints: Uint8Array;
};

export type BinaryFuseDecodeError = {
  kind: "invalid_binary_fuse";
  message: string;
};

export type BinaryFuseBuildError = {
  kind: "invalid_binary_fuse_build";
  message: string;
};

function invalidBinaryFuse<T = never>(message: string): Result<T, BinaryFuseDecodeError> {
  return Result.err({ kind: "invalid_binary_fuse", message });
}

function invalidBinaryFuseBuild<T = never>(message: string): Result<T, BinaryFuseBuildError> {
  return Result.err({ kind: "invalid_binary_fuse_build", message });
}

export function buildBinaryFuse(keys: bigint[]): { filter: BinaryFuseFilter | null; bytes: Uint8Array } {
  const res = buildBinaryFuseResult(keys);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function buildBinaryFuseResult(
  keys: bigint[]
): Result<{ filter: BinaryFuseFilter | null; bytes: Uint8Array }, BinaryFuseBuildError> {
  if (keys.length === 0) return Result.ok({ filter: null, bytes: new Uint8Array(0) });
  const uniqueKeys = uniqueSorted(keys);
  if (uniqueKeys.length === 0) return Result.ok({ filter: null, bytes: new Uint8Array(0) });

  const filterRes = buildFilterResult(uniqueKeys);
  if (Result.isError(filterRes)) return filterRes;
  const filter = filterRes.value;
  const bytes = filter ? encodeBinaryFuse(filter) : new Uint8Array(0);
  if (filter && bytes.byteLength >= 28) {
    filter.fingerprints = bytes.subarray(28);
  }
  return Result.ok({ filter, bytes });
}

export function encodeBinaryFuse(filter: BinaryFuseFilter | null): Uint8Array {
  if (!filter) return new Uint8Array(0);
  const fpLen = filter.fingerprints.byteLength;
  const hdr = new Uint8Array(28);
  writeU64BE(hdr, 0, filter.seed);
  writeU32BE(hdr, 8, filter.segmentLength);
  writeU32BE(hdr, 12, filter.segmentLengthMask);
  writeU32BE(hdr, 16, filter.segmentCount);
  writeU32BE(hdr, 20, filter.segmentCountLength);
  writeU32BE(hdr, 24, fpLen);
  const out = new Uint8Array(28 + fpLen);
  out.set(hdr, 0);
  out.set(filter.fingerprints, 28);
  return out;
}

export function decodeBinaryFuse(data: Uint8Array): BinaryFuseFilter | null {
  const res = decodeBinaryFuseResult(data);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function decodeBinaryFuseResult(data: Uint8Array): Result<BinaryFuseFilter | null, BinaryFuseDecodeError> {
  if (data.byteLength === 0) return Result.ok(null);
  if (data.byteLength < 28) return invalidBinaryFuse("filter data too short");
  const seed = readU64BE(data, 0);
  const segmentLength = readU32BE(data, 8);
  const segmentLengthMask = readU32BE(data, 12);
  const segmentCount = readU32BE(data, 16);
  const segmentCountLength = readU32BE(data, 20);
  const fpLen = readU32BE(data, 24);
  if (data.byteLength < 28 + fpLen) return invalidBinaryFuse("filter data truncated");
  const fingerprints = data.subarray(28, 28 + fpLen);
  return Result.ok({
    seed,
    segmentLength,
    segmentLengthMask,
    segmentCount,
    segmentCountLength,
    fingerprints,
  });
}

export function binaryFuseContains(filter: BinaryFuseFilter, key: bigint): boolean {
  const hash = mixsplit(key, filter.seed);
  let f = Number(fingerprint(hash) & 0xffn);
  const [h0, h1, h2] = getHashFromHash(filter, hash);
  f ^= filter.fingerprints[h0] ^ filter.fingerprints[h1] ^ filter.fingerprints[h2];
  return f === 0;
}

function buildFilterResult(keys: bigint[]): Result<BinaryFuseFilter | null, BinaryFuseBuildError> {
  const size = keys.length;
  const params = initParams(size);
  if (!params) return Result.ok(null);

  const filter: BinaryFuseFilter = {
    seed: 0n,
    segmentLength: params.segmentLength,
    segmentLengthMask: params.segmentLengthMask,
    segmentCount: params.segmentCount,
    segmentCountLength: params.segmentCountLength,
    fingerprints: params.fingerprints,
  };

  const capacity = filter.fingerprints.length;
  const alone = new Uint32Array(capacity);
  const t2count = new Uint8Array(capacity);
  const reverseH = new Uint8Array(size);
  const t2hash = new BigUint64Array(capacity);
  const reverseOrder = new BigUint64Array(size + 1);
  reverseOrder[size] = 1n;

  const rng = { value: 1n };
  let iterations = 0;
  let expectedSize = size;

  for (;;) {
    iterations += 1;
    if (iterations > MAX_ITERATIONS) {
      return invalidBinaryFuseBuild("binary fuse: too many iterations");
    }
    t2count.fill(0);
    t2hash.fill(0n);
    reverseOrder.fill(0n, 0, expectedSize);
    reverseOrder[expectedSize] = 1n;

    filter.seed = splitmix64(rng);

    let blockBits = 1;
    while ((1 << blockBits) < filter.segmentCount) blockBits += 1;
    const startPos = new Uint32Array(1 << blockBits);
    for (let i = 0; i < startPos.length; i++) {
      startPos[i] = Number((BigInt(i) * BigInt(expectedSize)) >> BigInt(blockBits));
    }

    for (const key of keys) {
      const hash = mixsplit(key, filter.seed);
      let segmentIndex = Number(hash >> BigInt(64 - blockBits));
      const mask = (1 << blockBits) - 1;
      while (reverseOrder[startPos[segmentIndex]] !== 0n) {
        segmentIndex = (segmentIndex + 1) & mask;
      }
      reverseOrder[startPos[segmentIndex]] = hash;
      startPos[segmentIndex] += 1;
    }

    let error = 0;
    let duplicates = 0;
    const h012 = new Uint32Array(6);

    for (let i = 0; i < expectedSize; i++) {
      const hash = reverseOrder[i];
      if (hash === 0n) continue;
      const [index1, index2, index3] = getHashFromHash(filter, hash);
      t2count[index1] = (t2count[index1] + 4) & 0xff;
      t2hash[index1] ^= hash;
      t2count[index2] = (t2count[index2] + 4) & 0xff;
      t2count[index2] ^= 1;
      t2hash[index2] ^= hash;
      t2count[index3] = (t2count[index3] + 4) & 0xff;
      t2count[index3] ^= 2;
      t2hash[index3] ^= hash;

      if ((t2hash[index1] & t2hash[index2] & t2hash[index3]) === 0n) {
        if (
          (t2hash[index1] === 0n && t2count[index1] === 8) ||
          (t2hash[index2] === 0n && t2count[index2] === 8) ||
          (t2hash[index3] === 0n && t2count[index3] === 8)
        ) {
          duplicates += 1;
          t2count[index1] = (t2count[index1] - 4) & 0xff;
          t2hash[index1] ^= hash;
          t2count[index2] = (t2count[index2] - 4) & 0xff;
          t2count[index2] ^= 1;
          t2hash[index2] ^= hash;
          t2count[index3] = (t2count[index3] - 4) & 0xff;
          t2count[index3] ^= 2;
          t2hash[index3] ^= hash;
        }
      }
      if (t2count[index1] < 4 || t2count[index2] < 4 || t2count[index3] < 4) {
        error = 1;
        break;
      }
    }
    if (error === 1) continue;

    let qsize = 0;
    for (let i = 0; i < capacity; i++) {
      if ((t2count[i] >> 2) === 1) {
        alone[qsize++] = i >>> 0;
      }
    }

    let stacksize = 0;
    while (qsize > 0) {
      qsize -= 1;
      const index = alone[qsize];
      if ((t2count[index] >> 2) !== 1) continue;
      const hash = t2hash[index];
      const found = t2count[index] & 3;
      reverseH[stacksize] = found;
      reverseOrder[stacksize] = hash;
      stacksize += 1;

      const [index1, index2, index3] = getHashFromHash(filter, hash);
      h012[1] = index2;
      h012[2] = index3;
      h012[3] = index1;
      h012[4] = h012[1];

      const other1 = h012[found + 1];
      if ((t2count[other1] >> 2) === 2) alone[qsize++] = other1;
      t2count[other1] = (t2count[other1] - 4) & 0xff;
      t2count[other1] ^= mod3(found + 1);
      t2hash[other1] ^= hash;

      const other2 = h012[found + 2];
      if ((t2count[other2] >> 2) === 2) alone[qsize++] = other2;
      t2count[other2] = (t2count[other2] - 4) & 0xff;
      t2count[other2] ^= mod3(found + 2);
      t2hash[other2] ^= hash;
    }

    if (stacksize + duplicates === expectedSize) {
      expectedSize = stacksize;
      break;
    }
  }

  if (expectedSize === 0) return Result.ok(filter);

  for (let i = expectedSize - 1; i >= 0; i--) {
    const hash = reverseOrder[i];
    const xor2 = Number(fingerprint(hash) & 0xffn);
    const [index1, index2, index3] = getHashFromHash(filter, hash);
    const found = reverseH[i];
    const h0 = index1;
    const h1 = index2;
    const h2 = index3;
    const h3 = h0;
    const h4 = h1;
    const dest = found === 0 ? h0 : found === 1 ? h1 : h2;
    const v1 = found === 0 ? h1 : found === 1 ? h2 : h3;
    const v2 = found === 0 ? h2 : found === 1 ? h3 : h4;
    filter.fingerprints[dest] = xor2 ^ filter.fingerprints[v1] ^ filter.fingerprints[v2];
  }

  return Result.ok(filter);
}

function initParams(size: number): {
  segmentLength: number;
  segmentLengthMask: number;
  segmentCount: number;
  segmentCountLength: number;
  fingerprints: Uint8Array;
} | null {
  const arity = 3;
  let segmentLength = calculateSegmentLength(arity, size);
  if (segmentLength > 262144) segmentLength = 262144;
  const segmentLengthMask = segmentLength - 1;
  const sizeFactor = calculateSizeFactor(arity, size);
  let capacity = 0;
  if (size > 1) capacity = Math.round(size * sizeFactor);
  const initSegmentCount = Math.floor((capacity + segmentLength - 1) / segmentLength) - (arity - 1);
  let arrayLength = (initSegmentCount + arity - 1) * segmentLength;
  let segmentCount = Math.floor((arrayLength + segmentLength - 1) / segmentLength);
  if (segmentCount <= arity - 1) segmentCount = 1;
  else segmentCount -= arity - 1;
  arrayLength = (segmentCount + arity - 1) * segmentLength;
  const segmentCountLength = (segmentCount * segmentLength) >>> 0;
  const fingerprints = new Uint8Array(arrayLength);
  return {
    segmentLength,
    segmentLengthMask,
    segmentCount,
    segmentCountLength,
    fingerprints,
  };
}

function calculateSegmentLength(arity: number, size: number): number {
  if (size === 0) return 4;
  if (arity === 3) {
    return 1 << Math.floor(Math.log(size) / Math.log(3.33) + 2.25);
  }
  if (arity === 4) {
    return 1 << Math.floor(Math.log(size) / Math.log(2.91) - 0.5);
  }
  return 65536;
}

function calculateSizeFactor(arity: number, size: number): number {
  if (arity === 3) {
    return Math.max(1.125, 0.875 + (0.25 * Math.log(1_000_000)) / Math.log(size));
  }
  if (arity === 4) {
    return Math.max(1.075, 0.77 + (0.305 * Math.log(600_000)) / Math.log(size));
  }
  return 2.0;
}

function mod3(x: number): number {
  return x > 2 ? x - 3 : x;
}

function getHashFromHash(filter: BinaryFuseFilter, hash: bigint): [number, number, number] {
  const hi = (hash * BigInt(filter.segmentCountLength)) >> 64n;
  let h0 = Number(hi & 0xffffffffn) >>> 0;
  let h1 = (h0 + filter.segmentLength) >>> 0;
  let h2 = (h1 + filter.segmentLength) >>> 0;
  h1 = (h1 ^ Number((hash >> 18n) & BigInt(filter.segmentLengthMask))) >>> 0;
  h2 = (h2 ^ Number(hash & BigInt(filter.segmentLengthMask))) >>> 0;
  return [h0, h1, h2];
}

function uniqueSorted(keys: bigint[]): bigint[] {
  if (keys.length <= 1) return keys.slice();
  let sorted = true;
  for (let i = 1; i < keys.length; i++) {
    if (keys[i - 1] > keys[i]) {
      sorted = false;
      break;
    }
  }
  const out = sorted ? keys.slice() : keys.slice().sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  let write = 1;
  for (let i = 1; i < out.length; i++) {
    if (out[i] !== out[write - 1]) {
      out[write] = out[i];
      write += 1;
    }
  }
  return out.slice(0, write);
}

function murmur64(h: bigint): bigint {
  h ^= h >> 33n;
  h = (h * 0xff51afd7ed558ccdn) & MASK_64;
  h ^= h >> 33n;
  h = (h * 0xc4ceb9fe1a85ec53n) & MASK_64;
  h ^= h >> 33n;
  return h & MASK_64;
}

function splitmix64(seed: { value: bigint }): bigint {
  seed.value = (seed.value + 0x9e3779b97f4a7c15n) & MASK_64;
  let z = seed.value;
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK_64;
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK_64;
  return (z ^ (z >> 31n)) & MASK_64;
}

function mixsplit(key: bigint, seed: bigint): bigint {
  return murmur64((key + seed) & MASK_64);
}

function fingerprint(hash: bigint): bigint {
  return hash ^ (hash >> 32n);
}
