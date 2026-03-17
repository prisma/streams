import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { dsError } from "../../src/util/ds_error.ts";

function argValue(name: string, def: number): number {
  const prefix = `--${name}=`;
  for (const arg of Bun.argv.slice(2)) {
    if (arg.startsWith(prefix)) return Number(arg.slice(prefix.length));
  }
  return def;
}

function fmtMb(bytes: number): string {
  return (bytes / (1024 * 1024)).toFixed(1);
}

function fmtRss(): string {
  const rss = process.memoryUsage().rss;
  return `${fmtMb(rss)}mb`;
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

async function appendData(app: ReturnType<typeof createApp>, stream: string, totalBytes: number, payloadBytes: number): Promise<void> {
  const payload = new Uint8Array(payloadBytes);
  payload.fill(7);
  let written = 0;
  while (written < totalBytes) {
    const r = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
        method: "POST",
        headers: { "content-type": "application/octet-stream" },
        body: payload,
      })
    );
    if (r.status !== 204) throw dsError(`append failed: ${r.status} ${await r.text()}`);
    written += payloadBytes;
  }
}

async function appendSegmentedKeys(
  app: ReturnType<typeof createApp>,
  stream: string,
  segments: number,
  rowsPerSegment: number,
  payloadBytes: number,
  keyForSegment: (seg: number) => string
): Promise<void> {
  const payload = new Uint8Array(payloadBytes);
  payload.fill(7);
  for (let seg = 0; seg < segments; seg++) {
    const key = keyForSegment(seg);
    for (let i = 0; i < rowsPerSegment; i++) {
      const r = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "POST",
          headers: {
            "content-type": "application/octet-stream",
            "stream-key": key,
          },
          body: payload,
        })
      );
      if (r.status !== 204) throw dsError(`append failed: ${r.status} ${await r.text()}`);
    }
  }
}

async function waitForUploads(app: ReturnType<typeof createApp>, stream: string, targetSegments: number): Promise<void> {
  const deadline = Date.now() + 20_000;
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

async function waitForIndexRuns(app: ReturnType<typeof createApp>, stream: string, targetRuns: number): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const runs = app.deps.db.listIndexRuns(stream);
    if (runs.length >= targetRuns) return;
    await sleep(50);
  }
  throw dsError("timeout waiting for index runs");
}

async function readAll(app: ReturnType<typeof createApp>, stream: string): Promise<number> {
  let offset = "-1";
  let total = 0;
  for (;;) {
    const batch = await app.deps.reader.read({ stream, offset, key: null, format: "raw" });
    if (batch.records.length === 0) break;
    for (const rec of batch.records) total += rec.payload.byteLength;
    offset = batch.nextOffset;
  }
  return total;
}

async function readByKey(app: ReturnType<typeof createApp>, stream: string, key: string): Promise<{ bytes: number; records: number }> {
  const batch = await app.deps.reader.read({ stream, offset: "-1", key, format: "raw" });
  let total = 0;
  for (const rec of batch.records) total += rec.payload.byteLength;
  return { bytes: total, records: batch.records.length };
}

async function runScenario(label: string, opts: { cacheBytes: number; footerCacheEntries: number }): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "ds-perf-"));
  const stream = "perf";
  const segmentBytes = argValue("segment-bytes", 512 * 1024);
  const payloadBytes = argValue("payload-bytes", 64 * 1024);
  const segments = argValue("segments", 32);
  const passes = argValue("passes", 3);
  const getDelayMs = argValue("get-delay-ms", 1);

  const cfg = makeConfig(root, {
    segmentMaxBytes: segmentBytes,
    blockMaxBytes: Math.min(64 * 1024, segmentBytes),
    segmentCheckIntervalMs: 25,
    uploadIntervalMs: 25,
    uploadConcurrency: 4,
    readMaxBytes: 4 * 1024 * 1024,
    readMaxRecords: 50_000,
    segmentCacheMaxBytes: opts.cacheBytes,
    segmentFooterCacheEntries: opts.footerCacheEntries,
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

    const targetBytes = segments * segmentBytes;
    await appendData(app, stream, targetBytes, payloadBytes);
    await waitForUploads(app, stream, Math.floor(targetBytes / segmentBytes));

    app.deps.segmenter.stop(true);
    app.deps.uploader.stop(true);

    rmSync(join(root, "local", "streams"), { recursive: true, force: true });
    rmSync(join(root, "cache"), { recursive: true, force: true });

    os.resetStats();

    console.log(`\n[${label}] segments=${segments} segmentBytes=${segmentBytes} payloadBytes=${payloadBytes} passes=${passes}`);
    for (let i = 0; i < passes; i++) {
      const before = os.stats().gets;
      const t0 = nowMs();
      const bytes = await readAll(app, stream);
      const elapsed = nowMs() - t0;
      const after = os.stats().gets;
      const mb = fmtMb(bytes);
      const mbps = (bytes / (1024 * 1024)) / (elapsed / 1000);
      console.log(
        `pass ${i + 1}: ${mb}mb in ${elapsed.toFixed(1)}ms (${mbps.toFixed(1)} MB/s) get+${after - before}`
      );
    }

    const cacheStats = app.deps.reader.cacheStats();
    if (cacheStats) {
      console.log(
        `cache: entries=${cacheStats.entryCount} used=${fmtMb(cacheStats.usedBytes)}mb hits=${cacheStats.hits} misses=${cacheStats.misses} evictions=${cacheStats.evictions}`
      );
    }
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

async function runKeyScenario(label: string, opts: { indexSpan: number }): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "ds-keyperf-"));
  const stream = "keyperf";
  const segmentBytes = argValue("key-segment-bytes", 512 * 1024);
  const payloadBytes = argValue("key-payload-bytes", 8 * 1024);
  const segments = argValue("key-segments", 32);
  const rowsPerSegment = argValue("key-rows-per-segment", Math.max(1, Math.floor(segmentBytes / payloadBytes)));
  const getDelayMs = argValue("get-delay-ms", 1);
  const hotKey = `hot-${segments - 1}`;

  const cfg = makeConfig(root, {
    segmentMaxBytes: payloadBytes * rowsPerSegment,
    blockMaxBytes: Math.min(64 * 1024, segmentBytes),
    segmentTargetRows: rowsPerSegment,
    segmentCheckIntervalMs: 25,
    uploadIntervalMs: 25,
    uploadConcurrency: 4,
    readMaxBytes: 256 * 1024 * 1024,
    readMaxRecords: rowsPerSegment * 2,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    indexL0SpanSegments: opts.indexSpan,
    indexCheckIntervalMs: 50,
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

    await appendSegmentedKeys(app, stream, segments, rowsPerSegment, payloadBytes, (seg) => (seg === segments - 1 ? hotKey : `k${seg}`));
    await waitForUploads(app, stream, segments);
    if (opts.indexSpan > 0) {
      app.deps.indexer?.enqueue(stream);
      await (app.deps.indexer as any)?.tick?.();
      await waitForIndexRuns(app, stream, Math.floor(segments / opts.indexSpan));
    }

    app.deps.segmenter.stop(true);
    app.deps.uploader.stop(true);
    app.deps.indexer?.stop();

    os.resetStats();
    const before = os.stats().gets;
    const t0 = nowMs();
    const res = await readByKey(app, stream, hotKey);
    const elapsed = nowMs() - t0;
    const after = os.stats().gets;
    const mb = fmtMb(res.bytes);
    const mbps = res.bytes === 0 ? 0 : (res.bytes / (1024 * 1024)) / (elapsed / 1000);
    console.log(`\n[${label}] key=${hotKey} segments=${segments} rowsPerSegment=${rowsPerSegment} indexSpan=${opts.indexSpan}`);
    console.log(
      `read: ${mb}mb in ${elapsed.toFixed(1)}ms (${mbps.toFixed(1)} MB/s) records=${res.records} get+${after - before} rss=${fmtRss()}`
    );
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

async function main(): Promise<void> {
  await runScenario("baseline", { cacheBytes: 0, footerCacheEntries: 0 });
  const cacheBytes = argValue("cache-bytes", 256 * 1024 * 1024);
  const footerCacheEntries = argValue("footer-cache", 2048);
  await runScenario("improved", { cacheBytes, footerCacheEntries });
  await runKeyScenario("key-scan", { indexSpan: 0 });
  await runKeyScenario("key-index", { indexSpan: 16 });
}

await main();
