import { Result } from "better-result";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dsError } from "../util/ds_error.ts";
type HashInput = string | Uint8Array;

export type HashError = { kind: "hasher_not_initialized"; message: string };

type Hash32Hasher = {
  init(seed?: number): void;
  update(input: HashInput): void;
  digest(outputType?: "hex"): string | number | bigint;
};

type Hash64Hasher = {
  init(seed?: number | bigint): void;
  update(input: HashInput): void;
  digest(outputType?: "hex"): string | number | bigint;
};

type XXH3Module = {
  createXXHash3(seedLow?: number, seedHigh?: number): Promise<Hash64Hasher>;
};

type XXH64Module = {
  createXXHash64(seed?: number | bigint): Promise<Hash64Hasher>;
};

type XXH32Module = {
  createXXHash32(seed?: number): Promise<Hash32Hasher>;
};

let xxh3Hasher: Hash64Hasher | null = null;
let xxh64Hasher: Hash64Hasher | null = null;
let xxh32Hasher: Hash32Hasher | null = null;

const isBunRuntime = typeof (globalThis as any).Bun !== "undefined";
const require = createRequire(import.meta.url);

function loadVendoredModule<T>(name: string): T {
  const path = fileURLToPath(new URL(`./hash_vendor/${name}`, import.meta.url));
  return require(path) as T;
}

if (!isBunRuntime) {
  const xxh3Module = loadVendoredModule<XXH3Module>("xxhash3.umd.min.cjs");
  const xxh64Module = loadVendoredModule<XXH64Module>("xxhash64.umd.min.cjs");
  const xxh32Module = loadVendoredModule<XXH32Module>("xxhash32.umd.min.cjs");
  xxh3Hasher = (await xxh3Module.createXXHash3()) as Hash64Hasher;
  xxh64Hasher = (await xxh64Module.createXXHash64()) as Hash64Hasher;
  xxh32Hasher = (await xxh32Module.createXXHash32()) as Hash32Hasher;
}

function toBigIntDigest(value: string | number | bigint): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number") return BigInt(value >>> 0);
  const hex = value.startsWith("0x") ? value.slice(2) : value;
  if (hex.length === 0) return 0n;
  return BigInt(`0x${hex}`);
}

function toHex16(value: bigint): string {
  const masked = value & 0xffff_ffff_ffff_ffffn;
  return masked.toString(16).padStart(16, "0");
}

function bunHash64(input: HashInput, fn: (x: HashInput) => bigint): bigint {
  return fn(input);
}

function nodeHash64Result(input: HashInput, hasher: Hash64Hasher | null, label: string): Result<bigint, HashError> {
  if (!hasher) return Result.err({ kind: "hasher_not_initialized", message: `${label} hasher not initialized` });
  hasher.init();
  hasher.update(input);
  const digest = hasher.digest("hex");
  return Result.ok(toBigIntDigest(digest));
}

function nodeHash32Result(input: HashInput): Result<number, HashError> {
  if (!xxh32Hasher) return Result.err({ kind: "hasher_not_initialized", message: "xxh32 hasher not initialized" });
  xxh32Hasher.init();
  xxh32Hasher.update(input);
  const digest = xxh32Hasher.digest("hex");
  if (typeof digest === "number") return Result.ok(digest >>> 0);
  const asBigInt = toBigIntDigest(digest);
  return Result.ok(Number(asBigInt & 0xffff_ffffn) >>> 0);
}

export function xxh3BigIntResult(input: HashInput): Result<bigint, HashError> {
  if (isBunRuntime) return Result.ok(bunHash64(input, (x) => Bun.hash.xxHash3(x)));
  return nodeHash64Result(input, xxh3Hasher, "xxh3");
}

export function xxh64BigIntResult(input: HashInput): Result<bigint, HashError> {
  if (isBunRuntime) return Result.ok(bunHash64(input, (x) => Bun.hash.xxHash64(x)));
  return nodeHash64Result(input, xxh64Hasher, "xxh64");
}

export function wyhashBigIntResult(input: HashInput): Result<bigint, HashError> {
  if (isBunRuntime) return Result.ok(bunHash64(input, (x) => Bun.hash.wyhash(x)));
  // Node fallback: use xxh3 for deterministic cross-runtime behavior.
  return xxh3BigIntResult(input);
}

export function xxh3HexResult(input: HashInput): Result<string, HashError> {
  const res = xxh3BigIntResult(input);
  if (Result.isError(res)) return res;
  return Result.ok(toHex16(res.value));
}

export function xxh64HexResult(input: HashInput): Result<string, HashError> {
  const res = xxh64BigIntResult(input);
  if (Result.isError(res)) return res;
  return Result.ok(toHex16(res.value));
}

export function xxh32Result(input: HashInput): Result<number, HashError> {
  if (isBunRuntime) return Result.ok((Bun.hash.xxHash32(input) as number) >>> 0);
  return nodeHash32Result(input);
}

export function xxh3BigInt(input: HashInput): bigint {
  const res = xxh3BigIntResult(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function xxh64BigInt(input: HashInput): bigint {
  const res = xxh64BigIntResult(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function wyhashBigInt(input: HashInput): bigint {
  const res = wyhashBigIntResult(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function xxh3Hex(input: HashInput): string {
  const res = xxh3HexResult(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function xxh64Hex(input: HashInput): string {
  const res = xxh64HexResult(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function xxh32(input: HashInput): number {
  const res = xxh32Result(input);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}
