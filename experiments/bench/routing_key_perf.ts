import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { Result } from "better-result";
import { dsError } from "../../src/util/ds_error.ts";

function envNum(name: string, def: number): number {
  const raw = process.env[name];
  if (!raw) return def;
  const v = Number(raw);
  if (!Number.isFinite(v)) throw dsError(`invalid ${name}: ${raw}`);
  return v;
}

function fmtMs(v: number): string {
  return `${v.toFixed(2)}ms`;
}

function nowMs(): number {
  return Number(performance.now());
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

function makeConfig(rootDir: string, overrides: Partial<Config>): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ...overrides,
  };
}

function pickKey(rng: () => number, keys: string[], hotKeys: string[], hotPct: number): string {
  if (hotKeys.length > 0 && rng() < hotPct) {
    const idx = Math.floor(rng() * hotKeys.length);
    return hotKeys[idx];
  }
  const idx = Math.floor(rng() * keys.length);
  return keys[idx];
}

function seededRng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return ((s >>> 0) % 1_000_000) / 1_000_000;
  };
}

async function ingest(
  app: ReturnType<typeof createApp>,
  stream: string,
  keys: string[],
  hotKeys: string[],
  opts: { events: number; payloadPool: Uint8Array[]; appendBatch: number; hotPct: number; seed: number }
): Promise<void> {
  const rng = seededRng(opts.seed);
  let remaining = opts.events;
  let idx = 0;
  while (remaining > 0) {
    const batch = opts.appendBatch > 0 ? Math.min(opts.appendBatch, remaining) : remaining;
    const rows = new Array(batch);
    for (let i = 0; i < batch; i++) {
      const key = idx < keys.length ? keys[idx++] : pickKey(rng, keys, hotKeys, opts.hotPct);
      rows[i] = {
        routingKey: new TextEncoder().encode(key),
        contentType: "application/octet-stream",
        payload: opts.payloadPool[i % opts.payloadPool.length],
      };
      remaining -= 1;
    }
    const appendRes = await app.deps.ingest.append({
      stream,
      baseAppendMs: BigInt(Date.now()),
      rows,
      contentType: "application/octet-stream",
    });
    if (Result.isError(appendRes)) throw dsError(`ingest append failed: ${appendRes.error.kind}`);
  }
}

async function waitForUploads(app: ReturnType<typeof createApp>, stream: string, targetSegments: number): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const segs = app.deps.db.listSegmentsForStream(stream);
    const pending = app.deps.db.countPendingSegments();
    const srow = app.deps.db.getStream(stream);
    const uploadedOk = srow ? srow.uploaded_through >= srow.sealed_through : false;
    if (segs.length >= targetSegments && pending === 0 && uploadedOk) return;
    await sleep(50);
  }
  throw dsError("timeout waiting for segments/uploads");
}

async function waitForIndex(app: ReturnType<typeof createApp>, stream: string, segmentCount: number): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const state = app.deps.db.getIndexState(stream);
    if (state && state.indexed_through >= segmentCount) return;
    await sleep(50);
  }
}

async function timedRead(app: ReturnType<typeof createApp>, stream: string, key: string): Promise<{ ms: number; records: number }> {
  const t0 = nowMs();
  const batch = await app.deps.reader.read({ stream, offset: "-1", key, format: "raw" });
  const ms = nowMs() - t0;
  return { ms, records: batch.records.length };
}

async function run(): Promise<void> {
  const eventsMax = envNum("DS_RK_EVENTS_MAX", 100000);
  const eventsStep = envNum("DS_RK_EVENTS_STEP", 10000);
  const payloadBytes = envNum("DS_RK_PAYLOAD_BYTES", 1024);
  const appendBatch = envNum("DS_RK_APPEND_BATCH", 500);
  const keysCount = envNum("DS_RK_KEYS", 10000);
  const hotKeysCount = envNum("DS_RK_HOT_KEYS", 64);
  const hotPct = envNum("DS_RK_HOT_PCT", 50) / 100;
  const payloadPoolSize = envNum("DS_RK_PAYLOAD_POOL", 256);
  const readEntries = envNum("DS_RK_READ_ENTRIES", 1);
  const warmReads = envNum("DS_RK_WARM_READS", 3);
  const segmentBytes = envNum("DS_RK_SEGMENT_BYTES", 4 * 1024 * 1024);
  const blockBytes = envNum("DS_RK_BLOCK_BYTES", 256 * 1024);
  const seed = envNum("DS_RK_SEED", 1);
  const getDelayMs = envNum("DS_RK_R2_GET_DELAY_MS", 0);

  const root = mkdtempSync(join(tmpdir(), "ds-routingkeyperf-"));
  const stream = "routing-perf";
  const cfg = makeConfig(root, {
    segmentMaxBytes: segmentBytes,
    blockMaxBytes: blockBytes,
    segmentCheckIntervalMs: 25,
    uploadIntervalMs: 25,
    uploadConcurrency: 4,
    readMaxRecords: readEntries,
    readMaxBytes: Math.max(readEntries * payloadBytes * 2, 1024 * 1024),
  });
  const os = new MockR2Store({ faults: { getDelayMs } });
  const app = createApp(cfg, os);

  try {
    await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": "application/octet-stream" },
      })
    );

    const keys = Array.from({ length: keysCount }, (_, i) => `key-${i}`);
    const hotKeys = keys.slice(0, Math.min(hotKeysCount, keys.length));
    const payloadPool = Array.from({ length: payloadPoolSize }, (_, i) => {
      const b = new Uint8Array(payloadBytes);
      b.fill(i % 255);
      return b;
    });

    let total = 0;
    while (total < eventsMax) {
      const step = Math.min(eventsStep, eventsMax - total);
      await ingest(app, stream, keys, hotKeys, {
        events: step,
        payloadPool,
        appendBatch,
        hotPct,
        seed: seed + total,
      });
      total += step;
      const segs = app.deps.db.listSegmentsForStream(stream).length;
      const runs = app.deps.db.listIndexRuns(stream).length;
      const state = app.deps.db.getIndexState(stream);
      const indexed = state?.indexed_through ?? 0;
      console.log(`[write] events=${total} segments=${segs} indexed=${indexed} runs=${runs}`);
    }

    const targetSegments = app.deps.db.listSegmentsForStream(stream).length;
    await waitForUploads(app, stream, targetSegments);
    app.deps.indexer?.enqueue(stream);
    await waitForIndex(app, stream, targetSegments);

    app.deps.segmenter.stop(true);
    app.deps.uploader.stop(true);
    app.deps.indexer?.stop();

    rmSync(join(root, "local", "streams"), { recursive: true, force: true });
    rmSync(join(root, "cache"), { recursive: true, force: true });

    const app2 = createApp(cfg, os);
    try {
      const hotKey = hotKeys[0] ?? keys[0];
      const coldKey = keys[Math.min(hotKeys.length + 1, keys.length - 1)];

      os.resetStats();
      const hotCold = await timedRead(app2, stream, hotKey);
      const hotWarmSamples: number[] = [];
      for (let i = 0; i < warmReads; i++) {
        hotWarmSamples.push((await timedRead(app2, stream, hotKey)).ms);
      }

      const coldCold = await timedRead(app2, stream, coldKey);
      const coldWarmSamples: number[] = [];
      for (let i = 0; i < warmReads; i++) {
        coldWarmSamples.push((await timedRead(app2, stream, coldKey)).ms);
      }

      const stats = os.stats();
      const segs = app2.deps.db.listSegmentsForStream(stream).length;
      const runs = app2.deps.db.listIndexRuns(stream).length;
      const state = app2.deps.db.getIndexState(stream);
      const indexed = state?.indexed_through ?? 0;

      const avg = (arr: number[]) => (arr.reduce((a, b) => a + b, 0) / Math.max(1, arr.length));
      console.log("");
      console.log("| events | segments | indexed | runs | hot_read_cold | hot_read_warm | cold_read_cold | cold_read_warm | r2_gets |");
      console.log("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
      console.log(
        `| ${eventsMax} | ${segs} | ${indexed} | ${runs} | ${fmtMs(hotCold.ms)} | ${fmtMs(avg(hotWarmSamples))} | ${fmtMs(
          coldCold.ms
        )} | ${fmtMs(avg(coldWarmSamples))} | ${stats.gets} |`
      );
    } finally {
      app2.close();
    }
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

run().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
