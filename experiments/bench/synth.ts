/**
 * Synthetic ingest benchmark.
 *
 * Usage:
 *   bun run experiments/bench/synth.ts
 *   bun run experiments/bench/synth.ts --large-payloads
 *   bun run experiments/bench/synth.ts --payload-bytes 262144
 *   bun run experiments/bench/synth.ts --memory-stress
 */

import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { StatsCollector } from "../../src/stats";
import { formatBytes } from "../../src/memory";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { dsError } from "../../src/util/ds_error.ts";

const DURATION_MS = Number(process.env.DS_BENCH_DURATION_MS ?? process.env.DS_BENCH_INTERVAL_MS ?? "60000");
const ARGS = process.argv.slice(2);
const LARGE_PAYLOAD_BYTES = 256 * 1024;
const MEMORY_STRESS = ARGS.includes("--memory-stress");

function argValue(flag: string): string | null {
  const idx = ARGS.indexOf(flag);
  if (idx === -1) return null;
  return ARGS[idx + 1] ?? null;
}

function parsePayloadBytes(): number {
  const direct = argValue("--payload-bytes");
  if (direct != null) {
    const n = Number(direct);
    if (Number.isFinite(n) && n > 0) return n;
    throw dsError(`invalid --payload-bytes: ${direct}`);
  }
  if (ARGS.includes("--large-payloads")) return LARGE_PAYLOAD_BYTES;
  return Number(process.env.DS_BENCH_PAYLOAD_BYTES ?? "1024");
}

const PAYLOAD_BYTES = parsePayloadBytes();
const CONCURRENCY = Number(process.env.DS_BENCH_CONCURRENCY ?? "32");
const REQUEST_TIMEOUT_MS = Number(process.env.DS_BENCH_REQUEST_TIMEOUT_MS ?? "5000");
const DRAIN_TIMEOUT_MS = Number(process.env.DS_BENCH_DRAIN_TIMEOUT_MS ?? "5000");
const SCENARIOS = [1, 10, 100, 1000];
const PAUSE_BACKGROUND = (process.env.DS_BENCH_PAUSE_BACKGROUND ?? "").toLowerCase() === "true" ||
  process.env.DS_BENCH_PAUSE_BACKGROUND === "1";
const YIELD_EVERY = Number(process.env.DS_BENCH_YIELD_EVERY ?? "50");
const DEBUG = (process.env.DS_BENCH_DEBUG ?? "").toLowerCase() === "true" ||
  process.env.DS_BENCH_DEBUG === "1";

const MEMORY_STRESS_LIMITS = (process.env.DS_MEMORY_STRESS_LIMITS_MB ?? "256,512,1024,2048,4096")
  .split(",")
  .map((v) => Number(v.trim()))
  .filter((v) => Number.isFinite(v) && v > 0);
const MEMORY_STRESS_STATS_MS = Number(process.env.DS_MEMORY_STRESS_STATS_MS ?? "1000");
const MEMORY_STRESS_PORT_BASE = Number(process.env.DS_MEMORY_STRESS_PORT_BASE ?? "9100");

function debug(msg: string): void {
  if (DEBUG) {
    // eslint-disable-next-line no-console
    console.log(`[bench] ${msg}`);
  }
}

function parseScenarios(): number[] {
  const raw = process.env.DS_BENCH_SCENARIOS;
  if (!raw) return SCENARIOS;
  const parsed = raw
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
  return parsed.length > 0 ? parsed : SCENARIOS;
}

type ScenarioHooks = {
  pauseBackground?: () => void;
  resumeBackground?: () => void;
  stopServer?: () => void;
};

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

function fmtMBps(bytes: number, ms: number): string {
  const mb = bytes / (1024 * 1024);
  const s = ms / 1000;
  return (mb / s).toFixed(2);
}

function parseBytesLiteral(s: string): number | null {
  const m = s.match(/([0-9.]+)(b|kb|mb|gb)/i);
  if (!m) return null;
  const value = Number(m[1]);
  if (!Number.isFinite(value)) return null;
  const unit = m[2].toLowerCase();
  const mult = unit === "b" ? 1 : unit === "kb" ? 1024 : unit === "mb" ? 1024 ** 2 : 1024 ** 3;
  return Math.floor(value * mult);
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(p * (sorted.length - 1)));
  return sorted[idx];
}

async function ensureStream(baseUrl: string, name: string): Promise<void> {
  const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(name)}`, { method: "PUT" });
  if (!r.ok && r.status !== 200 && r.status !== 201) {
    const t = await r.text();
    throw dsError(`failed to create stream ${name}: ${r.status} ${t}`);
  }
}

async function appendOnce(
  baseUrl: string,
  stream: string,
  payload: Uint8Array,
  timeoutMs: number,
  inflight?: Set<AbortController>
): Promise<Result<void, { status?: number; timeout?: boolean }>> {
  const controller = new AbortController();
  if (inflight) inflight.add(controller);
  const fetchPromise = fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "POST",
    headers: { "content-type": "application/octet-stream" },
    body: payload,
    signal: controller.signal,
  })
    .then((r) => ({ type: "response" as const, ok: r.ok, status: r.status }))
    .catch(() => ({ type: "error" as const }));
  const timeoutPromise = sleep(timeoutMs).then(() => "timeout" as const);
  try {
    const result = await Promise.race([fetchPromise, timeoutPromise]);
    if (result === "timeout") {
      controller.abort();
      return Result.err({ timeout: true });
    }
    if (result.type === "response") {
      if (result.ok) return Result.ok(undefined);
      return Result.err({ status: result.status });
    }
    return Result.err({});
  } finally {
    if (inflight) inflight.delete(controller);
  }
}

async function waitForPersistence(os: MockR2Store): Promise<number> {
  const deadline = Date.now() + 120_000;
  let persistedBytes = 0;
  while (Date.now() < deadline) {
    const keys = await os.list("streams/");
    const hasManifest = keys.some((k) => k.endsWith("/manifest.json"));
    const segmentKeys = keys.filter((k) => k.includes("/segments/") && k.endsWith(".bin"));
    if (hasManifest && segmentKeys.length > 0) {
      for (const k of segmentKeys) {
        const meta = await os.head(k);
        if (meta) persistedBytes += meta.size;
        const bytes = await os.get(k);
        if (!bytes || bytes.byteLength === 0) throw dsError("segment read failed");
      }
      return persistedBytes;
    }
    await sleep(200);
  }
  throw dsError("persistence timeout");
}

async function readLines(stream: ReadableStream<Uint8Array>, onLine: (line: string) => void): Promise<void> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let idx = buffer.indexOf("\n");
    while (idx >= 0) {
      const line = buffer.slice(0, idx).replace(/\r$/, "");
      buffer = buffer.slice(idx + 1);
      onLine(line);
      idx = buffer.indexOf("\n");
    }
  }
  if (buffer.length > 0) onLine(buffer.replace(/\r$/, ""));
}

async function runMemoryStress(): Promise<void> {
  const scenarios = parseScenarios();
  if (MEMORY_STRESS_LIMITS.length === 0) throw dsError("no memory stress limits configured");
  for (let i = 0; i < MEMORY_STRESS_LIMITS.length; i++) {
    const limit = MEMORY_STRESS_LIMITS[i];
    const port = MEMORY_STRESS_PORT_BASE + i;
    const root = mkdtempSync(join(tmpdir(), `ds-memstress-${limit}-`));
    const defaultMockMax = Math.min(limit * 1024 * 1024 * 0.25, 1024 * 1024 * 1024);
    const env = {
      ...process.env,
      DS_ROOT: root,
      DS_DB_PATH: `${root}/wal.sqlite`,
      DS_MEMORY_LIMIT_MB: String(limit),
      DS_STATS_INTERVAL_MS: String(MEMORY_STRESS_STATS_MS),
      DS_MOCK_R2_MAX_INMEM_BYTES: process.env.DS_MOCK_R2_MAX_INMEM_BYTES ?? String(Math.floor(defaultMockMax)),
      DS_MOCK_R2_SPILL_DIR: process.env.DS_MOCK_R2_SPILL_DIR ?? `${root}/mock-r2`,
      PORT: String(port),
    } as Record<string, string>;

    const proc = Bun.spawn({
      cmd: ["bun", "run", "src/server.ts", "--stats", "--object-store", "local"],
      env,
      stdout: "pipe",
      stderr: "pipe",
    });

    const rssSamples: Array<{ t: number; rss: number }> = [];
    let ready = false;
    let readyResolve: (() => void) | null = null;
    const readyPromise = new Promise<void>((res) => {
      readyResolve = res;
    });

    const onLine = (line: string) => {
      if (!ready && line.includes("prisma-streams server listening on") && line.includes(`:${port}`)) {
        ready = true;
        if (readyResolve) readyResolve();
      }
      const match = line.match(/max-rss=([0-9.]+[a-z]+)/i);
      if (match) {
        const bytes = parseBytesLiteral(match[1]);
        if (bytes != null) rssSamples.push({ t: Date.now(), rss: bytes });
      }
    };

    if (proc.stdout) void readLines(proc.stdout, onLine);
    if (proc.stderr) void readLines(proc.stderr, onLine);

    const startWait = Promise.race([readyPromise, sleep(5000).then(() => "timeout" as const)]);
    const readyResult = await startWait;
    if (readyResult === "timeout") {
      proc.kill();
      throw dsError(`memory stress server failed to start for limit ${limit}MB`);
    }

    const baseUrl = `http://localhost:${port}`;
    // eslint-disable-next-line no-console
    console.log(`\n[bench] memory stress limit=${limit}MB baseUrl=${baseUrl}`);

    for (const s of scenarios) {
      const start = Date.now();
      // eslint-disable-next-line no-console
      console.log(`[bench] scenario: ${s} active streams for ${DURATION_MS}ms...`);
      const r = await runScenario(baseUrl, null, s, DURATION_MS, null, null);
      const end = Date.now();
      const samples = rssSamples.filter((v) => v.t >= start && v.t <= end);
      const maxRss = samples.length > 0 ? Math.max(...samples.map((v) => v.rss)) : 0;
      const maxRssStr = maxRss > 0 ? formatBytes(maxRss) : "n/a";
      // eslint-disable-next-line no-console
      console.log(
        `${String(s).padStart(4)} streams | ingest ${r.ingestMbps} MB/s | persisted ${r.persistedMbps} MB/s | ` +
          `p50=${r.p50}ms p95=${r.p95}ms | appends=${r.appends} errors=${r.errors} (429=${r.errors429} 5xx=${r.errors5xx} ` +
          `timeout=${r.errorsTimeout} other=${r.errorsOther}) | max-rss=${maxRssStr}`
      );
    }

    proc.kill();
    await Promise.race([proc.exited, sleep(5000)]);
    rmSync(root, { recursive: true, force: true });
  }
}

async function runScenario(
  baseUrl: string,
  os: MockR2Store | null,
  activeStreams: number,
  durationMs: number,
  hooks?: ScenarioHooks | null,
  stats?: StatsCollector | null
) {
  const streams = Array.from({ length: activeStreams }, (_, i) => `bench_synth_${activeStreams}_${i}`);
  for (const s of streams) await ensureStream(baseUrl, s);

  const payload = new Uint8Array(PAYLOAD_BYTES);
  crypto.getRandomValues(payload);

  let bytes = 0;
  let appends = 0;
  let errors = 0;
  let errors429 = 0;
  let errors5xx = 0;
  let errorsTimeout = 0;
  let errorsOther = 0;
  if (stats) stats.snapshotAndReset();
  const latencies: number[] = [];
  let stop = false;
  let recording = true;
  const inflight = new Set<AbortController>();

  const worker = async () => {
    let iter = 0;
    while (!stop) {
      const idx = Math.floor(Math.random() * streams.length);
      const t0 = performance.now();
      const res = await appendOnce(baseUrl, streams[idx], payload, REQUEST_TIMEOUT_MS, inflight);
      const t1 = performance.now();
      if (recording) {
        if (Result.isOk(res)) {
          latencies.push(t1 - t0);
          bytes += payload.byteLength;
          appends += 1;
        } else {
          errors += 1;
          if (res.error.timeout) {
            errorsTimeout += 1;
          } else if (res.error.status === 429) {
            errors429 += 1;
          } else if (res.error.status && res.error.status >= 500) {
            errors5xx += 1;
          } else {
            errorsOther += 1;
          }
        }
      }
      iter += 1;
      if (YIELD_EVERY > 0 && iter % YIELD_EVERY === 0) await sleep(0);
    }
  };

  hooks?.pauseBackground?.();
  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  const t0 = Date.now();
  debug(`ingest start streams=${activeStreams} durationMs=${durationMs}`);
  await sleep(durationMs);
  const t1 = Date.now();
  const windowSnap = stats ? stats.snapshotAndReset() : null;
  stop = true;
  recording = false;
  hooks?.stopServer?.();
  for (const controller of inflight) controller.abort();
  debug(`stop set streams=${activeStreams}; waiting for workers`);
  const drain = Promise.race([
    Promise.allSettled(workers),
    sleep(DRAIN_TIMEOUT_MS).then(() => "timeout" as const),
  ]);
  const drained = await drain;
  if (drained === "timeout") debug(`drain timeout after ${DRAIN_TIMEOUT_MS}ms`);
  debug(`ingest done streams=${activeStreams} elapsedMs=${t1 - t0}`);
  hooks?.resumeBackground?.();

  debug(`persistence wait start streams=${activeStreams}`);
  if (os) await waitForPersistence(os);
  debug(`persistence wait done streams=${activeStreams}`);
  const uploadedBytes = windowSnap ? windowSnap.uploadedBytes : null;
  return {
    bytes,
    appends,
    errors,
    errors429,
    errors5xx,
    errorsTimeout,
    errorsOther,
    ingestMbps: fmtMBps(bytes, t1 - t0),
    persistedMbps: uploadedBytes != null ? fmtMBps(uploadedBytes, t1 - t0) : "n/a",
    p50: percentile(latencies, 0.5).toFixed(2),
    p95: percentile(latencies, 0.95).toFixed(2),
  };
}

async function main() {
  if (MEMORY_STRESS) {
    await runMemoryStress();
    debug("memory stress complete");
    return;
  }

  const benchUrl = process.env.DS_BENCH_URL;
  const external = benchUrl != null && benchUrl !== "";
  const scenarios = parseScenarios();

  if (external) {
    const baseUrl = benchUrl ?? "";
    // eslint-disable-next-line no-console
    console.log(`[bench] using external server at ${baseUrl} (persistence verification disabled)`);
    for (const s of scenarios) {
      // eslint-disable-next-line no-console
      console.log(`\n[bench] scenario: ${s} active streams for ${DURATION_MS}ms...`);
      const r = await runScenario(baseUrl, null, s, DURATION_MS, null, null);
      // eslint-disable-next-line no-console
      console.log(
        `${String(s).padStart(4)} streams | ingest ${r.ingestMbps} MB/s | persisted ${r.persistedMbps} MB/s | ` +
          `p50=${r.p50}ms p95=${r.p95}ms | appends=${r.appends} errors=${r.errors} (429=${r.errors429} 5xx=${r.errors5xx} ` +
          `timeout=${r.errorsTimeout} other=${r.errorsOther}) | max-rss=n/a`
      );
    }
    debug("all scenarios complete");
    return;
  }

  for (const s of scenarios) {
    let root: string | null = null;
    let app: ReturnType<typeof createApp> | null = null;
    let server: any = null;
    let os: MockR2Store | null = null;
    let stats: StatsCollector | null = null;
    try {
      root = mkdtempSync(join(tmpdir(), "ds-bench-"));
      const cfg: Config = {
        ...loadConfig(),
        rootDir: root,
        dbPath: `${root}/wal.sqlite`,
        port: 0,
      };
      os = new MockR2Store();
      stats = new StatsCollector();
      app = createApp(cfg, os, { stats });
      server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;
      const hooks: ScenarioHooks = {};
      if (PAUSE_BACKGROUND) {
        hooks.pauseBackground = () => {
          app!.deps.segmenter.stop();
          app!.deps.uploader.stop();
        };
        hooks.resumeBackground = () => {
          app!.deps.segmenter.start();
          app!.deps.uploader.start();
        };
      }
      hooks.stopServer = () => {
        if (server) server.stop();
      };
      // eslint-disable-next-line no-console
      console.log(`\n[bench] scenario: ${s} active streams for ${DURATION_MS}ms...`);
      const r = await runScenario(baseUrl, os, s, DURATION_MS, hooks, stats);
      const maxRssBytes = app.deps.memory?.getMaxRssBytes() ?? 0;
      const maxRss = maxRssBytes > 0 ? formatBytes(maxRssBytes) : "n/a";
      // eslint-disable-next-line no-console
      console.log(
        `${String(s).padStart(4)} streams | ingest ${r.ingestMbps} MB/s | persisted ${r.persistedMbps} MB/s | ` +
          `p50=${r.p50}ms p95=${r.p95}ms | appends=${r.appends} errors=${r.errors} (429=${r.errors429} 5xx=${r.errors5xx} ` +
          `timeout=${r.errorsTimeout} other=${r.errorsOther}) | max-rss=${maxRss}`
      );
    } finally {
      if (server) server.stop();
      if (app) app.close();
      if (root) rmSync(root, { recursive: true, force: true });
    }
  }

  debug("all scenarios complete");
}

try {
  await main();
  process.exit(0);
} catch (e) {
  // eslint-disable-next-line no-console
  console.error(e);
  process.exit(1);
}
