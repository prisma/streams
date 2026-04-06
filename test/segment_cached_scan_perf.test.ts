import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { closeSync, openSync, readFileSync, readSync } from "node:fs";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { readRangeFromSource } from "../src/segment/cached_segment";
import { DSB3_HEADER_BYTES, decodeBlockResult, parseBlockHeaderResult, parseFooterBytes } from "../src/segment/format";
import { Bloom256 } from "../src/util/bloom256";
import { readU32BE } from "../src/util/endian";

const FIXTURE_PATH = join(import.meta.dir, "fixtures/segments/remote-golden-stream-2/0000000000000123.bin");
const FIXTURE_SHA256 = "3f8fea5bd7fdd65e151f7a5dcf3584c4a5fb57fe1abf5c0a70072de6dd739064";
const FIXTURE_SIZE = 566114;
const HOT_KEY = new TextEncoder().encode("adaptationdev/3d-brain-js");

function fileSha256(path: string): string {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function readRangeFromFile(path: string, start: number, end: number): Uint8Array {
  const fd = openSync(path, "r");
  try {
    const out = new Uint8Array(end - start + 1);
    const bytesRead = readSync(fd, out, 0, out.byteLength, start);
    return out.subarray(0, bytesRead);
  } finally {
    closeSync(fd);
  }
}

function legacyHotKeyScan(path: string, size: number, keyBytes: Uint8Array): number {
  const tail = readRangeFromFile(path, size - 8, size - 1);
  const footerLen = readU32BE(tail, 0);
  const footerStart = size - 8 - footerLen;
  const footerBytes = readRangeFromFile(path, footerStart, footerStart + footerLen - 1);
  const footer = parseFooterBytes(footerBytes);
  if (!footer) throw new Error("missing footer");
  let decodedBlocks = 0;
  for (const entry of footer.blocks) {
    const headerBytes = readRangeFromFile(path, entry.blockOffset, entry.blockOffset + DSB3_HEADER_BYTES - 1);
    const headerRes = parseBlockHeaderResult(headerBytes);
    if (headerRes.isError) throw new Error(headerRes.error.message);
    const bloom = new Bloom256(headerRes.value.bloom);
    if (!bloom.maybeHas(keyBytes)) continue;
    const totalLen = DSB3_HEADER_BYTES + entry.compressedLen;
    const blockBytes = readRangeFromFile(path, entry.blockOffset, entry.blockOffset + totalLen - 1);
    const decodedRes = decodeBlockResult(blockBytes);
    if (decodedRes.isError) throw new Error(decodedRes.error.message);
    decodedBlocks += 1;
  }
  return decodedBlocks;
}

function mappedHotKeyScan(path: string, keyBytes: Uint8Array): number {
  const bytes = (Bun as any).mmap(path, { shared: true }) as Uint8Array;
  const source = { kind: "mapped", path, bytes } as const;
  const tail = readRangeFromSource(source, bytes.byteLength - 8, bytes.byteLength - 1);
  const footerLen = readU32BE(tail, 0);
  const limit = bytes.byteLength - 8 - footerLen;
  let off = 0;
  let decodedBlocks = 0;
  while (off < limit) {
    const headerRes = parseBlockHeaderResult(readRangeFromSource(source, off, off + DSB3_HEADER_BYTES - 1));
    if (headerRes.isError) throw new Error(headerRes.error.message);
    const header = headerRes.value;
    const bloom = new Bloom256(header.bloom);
    const totalLen = DSB3_HEADER_BYTES + header.compressedLen;
    if (bloom.maybeHas(keyBytes)) {
      const decodedRes = decodeBlockResult(readRangeFromSource(source, off, off + totalLen - 1));
      if (decodedRes.isError) throw new Error(decodedRes.error.message);
      decodedBlocks += 1;
    }
    off += totalLen;
  }
  return decodedBlocks;
}

function timeLoop(rounds: number, fn: () => number): { totalMs: number; avgMs: number; decodedBlocks: number } {
  let totalMs = 0;
  let decodedBlocks = 0;
  for (let i = 0; i < rounds; i++) {
    const startedAt = performance.now();
    decodedBlocks = fn();
    totalMs += performance.now() - startedAt;
  }
  return { totalMs, avgMs: totalMs / rounds, decodedBlocks };
}

describe("cached segment scan perf fixture", () => {
  test("uses the copied remote fixture segment", () => {
    expect(fileSha256(FIXTURE_PATH)).toBe(FIXTURE_SHA256);
    expect(readFileSync(FIXTURE_PATH).byteLength).toBe(FIXTURE_SIZE);
  });

  test("legacy syscall-heavy hot-key scan takes >100ms over 50 scans", () => {
    const result = timeLoop(50, () => legacyHotKeyScan(FIXTURE_PATH, FIXTURE_SIZE, HOT_KEY));
    expect(result.decodedBlocks).toBeGreaterThan(0);
    expect(result.totalMs).toBeGreaterThan(100);
  });

  test("mapped one-pass hot-key scan stays below 10ms average", () => {
    const warmup = timeLoop(20, () => mappedHotKeyScan(FIXTURE_PATH, HOT_KEY));
    expect(warmup.decodedBlocks).toBeGreaterThan(0);
    const result = timeLoop(200, () => mappedHotKeyScan(FIXTURE_PATH, HOT_KEY));
    expect(result.avgMs).toBeLessThan(10);
  });
});
