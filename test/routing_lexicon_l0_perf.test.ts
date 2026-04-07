import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { Result } from "better-result";
import { buildRoutingLexiconL0RunPayloadResult } from "../src/index/routing_lexicon_l0_build";
import { DSB3_HEADER_BYTES, encodeBlock, encodeFooter } from "../src/segment/format";

function writeRepeatedKeySegments(root: string, rowsPerSegment: number): Array<{ segmentIndex: number; localPath: string }> {
  const encoder = new TextEncoder();
  const keys = Array.from({ length: 100 }, (_, i) =>
    encoder.encode(`cardinality-${String(i).padStart(3, "0")}-${"x".repeat(30)}`)
  );
  const payload = encoder.encode(JSON.stringify({ randomString: "y".repeat(96), randomNumber: 42 }));
  const blocksPerSegment = 12;
  const rowsPerBlock = Math.floor(rowsPerSegment / blocksPerSegment);
  const segments: Array<{ segmentIndex: number; localPath: string }> = [];

  for (let segmentIndex = 0; segmentIndex < 16; segmentIndex++) {
    const blocks: Uint8Array[] = [];
    const footerEntries: Array<{
      blockOffset: number;
      firstOffset: bigint;
      recordCount: number;
      compressedLen: number;
      firstAppendNs: bigint;
      lastAppendNs: bigint;
    }> = [];
    let blockOffset = 0;
    let recordBase = 0;
    for (let blockIndex = 0; blockIndex < blocksPerSegment; blockIndex++) {
      const records = [];
      for (let recordIndex = 0; recordIndex < rowsPerBlock; recordIndex++) {
        const key = keys[(recordIndex + blockIndex + segmentIndex) % keys.length]!;
        records.push({
          appendNs: BigInt(recordBase + recordIndex + 1),
          routingKey: key,
          payload,
        });
      }
      const block = encodeBlock(records);
      blocks.push(block);
      footerEntries.push({
        blockOffset,
        firstOffset: BigInt(segmentIndex * rowsPerSegment + recordBase),
        recordCount: records.length,
        compressedLen: block.byteLength - DSB3_HEADER_BYTES,
        firstAppendNs: records[0]!.appendNs,
        lastAppendNs: records[records.length - 1]!.appendNs,
      });
      blockOffset += block.byteLength;
      recordBase += records.length;
    }

    const footer = encodeFooter(footerEntries);
    const bytes = new Uint8Array(blockOffset + footer.byteLength);
    let cursor = 0;
    for (const block of blocks) {
      bytes.set(block, cursor);
      cursor += block.byteLength;
    }
    bytes.set(footer, cursor);

    const localPath = join(root, `segment-${segmentIndex}.bin`);
    writeFileSync(localPath, bytes);
    segments.push({ segmentIndex, localPath });
  }

  return segments;
}

describe("routing+lexicon L0 build performance", () => {
  test("stays under 1s for repeated-key windows after warmup", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-routing-l0-perf-"));
    try {
      const segments = writeRepeatedKeySegments(root, 6_000);
      const input = {
        stream: "perf",
        sourceKind: "routing_key",
        sourceName: "",
        startSegment: 0,
        span: 16,
        secret: new Uint8Array(16).fill(7),
        segments,
      } as const;

      const warmup = buildRoutingLexiconL0RunPayloadResult(input);
      expect(Result.isOk(warmup)).toBe(true);

      const started = performance.now();
      const measured = buildRoutingLexiconL0RunPayloadResult(input);
      const elapsedMs = performance.now() - started;

      expect(Result.isOk(measured)).toBe(true);
      if (Result.isOk(measured)) {
        expect(measured.value.routing.meta.recordCount).toBe(100);
        expect(measured.value.lexicon.meta.recordCount).toBe(100);
      }
      expect(elapsedMs).toBeLessThan(1_000);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
