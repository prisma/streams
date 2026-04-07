import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { Segmenter } from "../src/segment/segmenter";
import { SegmenterWorkerPool } from "../src/segment/segmenter_workers";
import { Result } from "better-result";

const RUN = process.env.DS_SEGMENTER_THROUGHPUT === "1";
const COUNTS = (process.env.DS_SEGMENTER_WORKER_COUNTS ?? "1")
  .split(",")
  .map((v) => Number(v.trim()))
  .filter((v) => Number.isFinite(v) && v >= 0);
const TOTAL_BYTES = Number(process.env.DS_SEGMENTER_TOTAL_BYTES ?? 256 * 1024 * 1024);
const PAYLOAD_BYTES = Number(process.env.DS_SEGMENTER_PAYLOAD_BYTES ?? 256 * 1024);
const MIN_MBPS = Number(process.env.DS_SEGMENTER_MIN_MBPS ?? 30);
const EXPECT_SCALE = process.env.DS_SEGMENTER_EXPECT_SCALE === "1";
const STREAM_COUNT = Number(process.env.DS_SEGMENTER_STREAMS ?? 8);

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

async function waitForSealed(db: SqliteDurableStore, stream: string, targetOffset: bigint, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row = db.getStream(stream);
    if (row && row.sealed_through >= targetOffset) return;
    await sleep(10);
  }
  throw new Error("segmenter throughput timeout");
}

async function runOnce(workerCount: number): Promise<{ mbps: number; durationMs: number } > {
  const root = mkdtempSync(join(tmpdir(), "ds-segthrough-"));
  const cfg = {
    ...loadConfig(),
    rootDir: root,
    dbPath: `${root}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 5,
  };
  const db = new SqliteDurableStore(cfg.dbPath);
  db.resetSegmentInProgress();
  const streams = Array.from({ length: Math.max(1, STREAM_COUNT) }, (_, i) => `bench_stream_${i}`);
  for (const stream of streams) {
    db.ensureStream(stream, { contentType: "application/octet-stream" });
  }

  const payload = new Uint8Array(PAYLOAD_BYTES);
  payload.fill(7);
  const bytesPerStream = Math.max(PAYLOAD_BYTES, Math.ceil(TOTAL_BYTES / streams.length));
  let totalWritten = 0;
  const lastOffsets = new Map<string, bigint>();
  for (const stream of streams) {
    let offset = 0n;
    let written = 0;
    while (written < bytesPerStream) {
      const batch: Array<{ routingKey: Uint8Array | null; contentType: string | null; payload: Uint8Array; appendMs: bigint }> = [];
      const batchRows = Math.min(512, Math.ceil((bytesPerStream - written) / PAYLOAD_BYTES));
      const baseAppendMs = db.nowMs();
      for (let i = 0; i < batchRows; i++) {
        if (written >= bytesPerStream) break;
        batch.push({
          routingKey: null,
          contentType: null,
          payload,
          appendMs: baseAppendMs + BigInt(i),
        });
        written += PAYLOAD_BYTES;
        totalWritten += PAYLOAD_BYTES;
      }
      const res = db.appendWalRows({
        stream,
        startOffset: offset,
        baseAppendMs,
        rows: batch,
      });
      if (Result.isError(res)) throw new Error("append failed");
      offset = res.value.lastOffset + 1n;
    }
    lastOffsets.set(stream, offset - 1n);
  }
  const start = Date.now();
  const segmenter = workerCount > 0 ? new SegmenterWorkerPool(cfg, db, workerCount) : new Segmenter(cfg, db);
  segmenter.start();
  const deadlineMs = 180_000;
  for (const [stream, lastOffset] of lastOffsets.entries()) {
    await waitForSealed(db, stream, lastOffset, deadlineMs);
  }
  const durationMs = Date.now() - start;
  segmenter.stop();
  await sleep(50);
  db.close();
  rmSync(root, { recursive: true, force: true });
  const mbps = Number((totalWritten / 1024 / 1024) / (durationMs / 1000));
  return { mbps, durationMs };
}

const t = RUN ? test : test.skip;

describe("segmenter throughput", () => {
  t(
    "measures segmenter throughput by workers",
    async () => {
      const results: Array<{ workers: number; mbps: number }> = [];
      for (const count of COUNTS.length > 0 ? COUNTS : [1]) {
        const r = await runOnce(count);
        results.push({ workers: count, mbps: r.mbps });
        // eslint-disable-next-line no-console
        console.log(`[segmenter-throughput] workers=${count} mbps=${r.mbps.toFixed(2)} durationMs=${r.durationMs}`);
      }

      const first = results.find((r) => r.workers === 1) ?? results[0];
      if (first && MIN_MBPS > 0) {
        expect(first.mbps).toBeGreaterThanOrEqual(MIN_MBPS);
      }

      if (EXPECT_SCALE && results.length >= 2) {
        for (let i = 1; i < results.length; i++) {
          const prev = results[i - 1];
          const cur = results[i];
          expect(cur.mbps).toBeGreaterThanOrEqual(prev.mbps * 0.7);
        }
      }
    },
    180_000
  );
});
