import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { Segmenter } from "../src/segment/segmenter";
import { Result } from "better-result";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 5000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error("timeout waiting for segmenter state");
}

describe("segmenter behavior", () => {
  test("seals when segmentTargetRows is reached", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-segmenter-rows-"));
    try {
      const cfg = {
        ...loadConfig(),
        rootDir: root,
        dbPath: `${root}/wal.sqlite`,
        port: 0,
        segmentCheckIntervalMs: 5,
        segmentMaxBytes: 1024 * 1024,
        segmentTargetRows: 3,
      };
      const db = new SqliteDurableStore(cfg.dbPath);
      db.ensureStream("rows", { contentType: "application/json" });

      const append = db.appendWalRows({
        stream: "rows",
        startOffset: 0n,
        baseAppendMs: 1n,
        rows: Array.from({ length: 5 }, (_, i) => ({
          routingKey: null,
          contentType: null,
          payload: new Uint8Array([i + 1]),
          appendMs: BigInt(i + 1),
        })),
      });
      expect(Result.isOk(append)).toBe(true);

      const segmenter = new Segmenter(cfg, db, { candidatesPerTick: 1 });
      segmenter.start();
      await waitFor(() => (db.getStream("rows")?.sealed_through ?? -1n) >= 2n);
      segmenter.stop();

      const seg = db.getSegmentByIndex("rows", 0);
      expect(seg).not.toBeNull();
      expect(seg?.end_offset).toBe(2n);
      expect(seg?.payload_bytes).toBe(3n);
      expect(db.getStream("rows")?.sealed_through).toBe(2n);
      db.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("uses recent compression to raise the logical byte seal target", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-segmenter-comp-"));
    try {
      const cfg = {
        ...loadConfig(),
        rootDir: root,
        dbPath: `${root}/wal.sqlite`,
        port: 0,
        segmentCheckIntervalMs: 5,
        segmentMaxBytes: 1024,
        segmentTargetRows: 10_000,
      };
      const db = new SqliteDurableStore(cfg.dbPath);
      db.ensureStream("compressed", { contentType: "application/octet-stream" });

      for (let i = 0; i < 8; i++) {
        db.commitSealedSegment({
          segmentId: `hist-${i}`,
          stream: "compressed",
          segmentIndex: i,
          startOffset: BigInt(i),
          endOffset: BigInt(i),
          blockCount: 1,
          lastAppendMs: BigInt(i + 1),
          payloadBytes: 10_000n,
          sizeBytes: 1_000,
          localPath: join(root, `hist-${i}.bin`),
          rowsSealed: 1n,
        });
      }

      const payload = new Uint8Array([7]);
      const append = db.appendWalRows({
        stream: "compressed",
        startOffset: 8n,
        baseAppendMs: 100n,
        rows: Array.from({ length: 6_000 }, (_, i) => ({
          routingKey: null,
          contentType: null,
          payload,
          appendMs: BigInt(100 + i),
        })),
      });
      expect(Result.isOk(append)).toBe(true);

      const segmenter = new Segmenter(cfg, db, { candidatesPerTick: 1 });
      segmenter.start();
      await waitFor(() => (db.getStream("compressed")?.sealed_through ?? -1n) >= 5_127n);
      segmenter.stop();

      const seg = db.getSegmentByIndex("compressed", 8);
      expect(seg).not.toBeNull();
      expect(seg?.end_offset).toBe(5_127n);
      expect(seg?.payload_bytes).toBe(5_120n);
      expect(db.getStream("compressed")?.sealed_through).toBe(5_127n);
      db.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("waits for the raised compression-aware byte threshold before starting a cut", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-segmenter-comp-admit-"));
    try {
      const cfg = {
        ...loadConfig(),
        rootDir: root,
        dbPath: `${root}/wal.sqlite`,
        port: 0,
        segmentCheckIntervalMs: 5,
        segmentMaxBytes: 1024,
        segmentTargetRows: 10_000,
      };
      const db = new SqliteDurableStore(cfg.dbPath);
      db.ensureStream("compressed", { contentType: "application/octet-stream" });

      for (let i = 0; i < 8; i++) {
        db.commitSealedSegment({
          segmentId: `hist-${i}`,
          stream: "compressed",
          segmentIndex: i,
          startOffset: BigInt(i),
          endOffset: BigInt(i),
          blockCount: 1,
          lastAppendMs: BigInt(i + 1),
          payloadBytes: 10_000n,
          sizeBytes: 1_000,
          localPath: join(root, `hist-${i}.bin`),
          rowsSealed: 1n,
        });
      }

      const payload = new Uint8Array([7]);
      const segmenter = new Segmenter(cfg, db, { candidatesPerTick: 1 });
      segmenter.start();

      const firstAppend = db.appendWalRows({
        stream: "compressed",
        startOffset: 8n,
        baseAppendMs: 100n,
        rows: Array.from({ length: 1_100 }, (_, i) => ({
          routingKey: null,
          contentType: null,
          payload,
          appendMs: BigInt(100 + i),
        })),
      });
      expect(Result.isOk(firstAppend)).toBe(true);

      await sleep(150);
      expect(db.getSegmentByIndex("compressed", 8)).toBeNull();
      expect(db.getStream("compressed")?.sealed_through).toBe(7n);

      const secondAppend = db.appendWalRows({
        stream: "compressed",
        startOffset: 1_108n,
        baseAppendMs: 2_000n,
        rows: Array.from({ length: 4_100 }, (_, i) => ({
          routingKey: null,
          contentType: null,
          payload,
          appendMs: BigInt(2_000 + i),
        })),
      });
      expect(Result.isOk(secondAppend)).toBe(true);

      await waitFor(() => (db.getStream("compressed")?.sealed_through ?? -1n) >= 5_127n);
      segmenter.stop();

      const seg = db.getSegmentByIndex("compressed", 8);
      expect(seg).not.toBeNull();
      expect(seg?.end_offset).toBe(5_127n);
      expect(seg?.payload_bytes).toBe(5_120n);
      expect(db.getStream("compressed")?.sealed_through).toBe(5_127n);
      db.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
