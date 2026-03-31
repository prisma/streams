import { loadConfig } from "./config";
import { createApp } from "./app";
import { StatsCollector, StatsReporter } from "./stats";
import { LatencyHistogramCollector, HistogramReporter } from "./hist";
import { MockR2Store } from "./objectstore/mock_r2";
import { R2ObjectStore } from "./objectstore/r2";
import { bootstrapFromR2 } from "./bootstrap";
import { initConsoleLogging } from "./util/log";

initConsoleLogging();

const args = process.argv.slice(2);
let autoTuneEnabled = false;
let autoTuneValueMb: number | null = null;
for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === "--auto-tune") {
    autoTuneEnabled = true;
    const next = args[i + 1];
    if (next && !next.startsWith("--") && /^[0-9]+$/.test(next)) {
      autoTuneValueMb = Number(next);
    }
  } else if (arg.startsWith("--auto-tune=")) {
    autoTuneEnabled = true;
    const raw = arg.split("=", 2)[1] ?? "";
    if (raw.trim() !== "") autoTuneValueMb = Number(raw);
  }
}

function formatPresetList<T>(presets: number[], selected: number, map: (preset: number) => T, fmt: (val: T) => string): string {
  return presets
    .map((preset) => {
      const value = fmt(map(preset));
      return preset === selected ? `[${value}]` : value;
    })
    .join(", ");
}

type AutoTuneConfig = {
  sqliteCacheMb: number;
  workerSqliteCacheMb: number;
  indexMemMb: number;
  ingestBatchMb: number;
  ingestQueueMb: number;
  indexBuildConcurrency: number;
  indexCompactConcurrency: number;
  segmenterWorkers: number;
  uploadConcurrency: number;
};

function memoryLimitForPreset(preset: number): number {
  return preset === 256 ? 300 : preset;
}

function applyAutoTune(overrideMb: number | null): void {
  const envMemRaw = process.env.DS_MEMORY_LIMIT_MB;
  if (overrideMb != null) {
    if (envMemRaw) {
      console.error("--auto-tune with a value cannot be used with DS_MEMORY_LIMIT_MB");
      process.exit(1);
    }
  } else if (!envMemRaw) {
    console.error("--auto-tune requires DS_MEMORY_LIMIT_MB to be set (or pass a value)");
    process.exit(1);
  }
  const memMb = overrideMb != null ? overrideMb : Number(envMemRaw);
  if (!Number.isFinite(memMb) || memMb <= 0) {
    const bad = overrideMb != null ? String(overrideMb) : String(envMemRaw);
    console.error(`invalid DS_MEMORY_LIMIT_MB: ${bad}`);
    process.exit(1);
  }
  if (process.env.DS_MEMORY_LIMIT_BYTES) {
    console.error("--auto-tune does not allow DS_MEMORY_LIMIT_BYTES; use DS_MEMORY_LIMIT_MB");
    process.exit(1);
  }

  const conflictVars = [
    "DS_SQLITE_CACHE_MB",
    "DS_SQLITE_CACHE_BYTES",
    "DS_WORKER_SQLITE_CACHE_MB",
    "DS_WORKER_SQLITE_CACHE_BYTES",
    "DS_INDEX_RUN_MEM_CACHE_BYTES",
    "DS_INGEST_MAX_BATCH_BYTES",
    "DS_INGEST_MAX_QUEUE_BYTES",
  ];
  const conflicts = conflictVars.filter((v) => process.env[v] != null);
  if (conflicts.length > 0) {
    console.error(`--auto-tune cannot be used with manual memory settings: ${conflicts.join(", ")}`);
    process.exit(1);
  }

  const presets = [256, 512, 1024, 2048, 4096, 8192];
  const preset = [...presets].reverse().find((v) => v <= memMb);
  if (!preset) {
    console.error(`DS_MEMORY_LIMIT_MB=${memMb} is below the minimum preset (256)`);
    process.exit(1);
  }

  const tuneFor = (p: number): AutoTuneConfig => ({
    sqliteCacheMb: Math.max(8, Math.floor(p / 16)),
    workerSqliteCacheMb: Math.max(8, Math.min(32, Math.floor(p / 128))),
    indexMemMb: Math.max(4, Math.floor(p / 64)),
    ingestBatchMb: Math.max(2, Math.floor(p / 128)),
    ingestQueueMb: Math.max(8, Math.floor(p / 32)),
    indexBuildConcurrency: p >= 8192 ? 8 : p >= 4096 ? 4 : p >= 1024 ? 2 : 1,
    indexCompactConcurrency: p >= 4096 ? 4 : p >= 1024 ? 2 : 1,
    segmenterWorkers: p >= 8192 ? 8 : p >= 4096 ? 4 : p >= 1024 ? 2 : 1,
    uploadConcurrency: p >= 8192 ? 16 : p >= 4096 ? 8 : p >= 1024 ? 4 : 2,
  });
  const tune = tuneFor(preset);

  const memoryLimitMb = memoryLimitForPreset(preset);
  process.env.DS_MEMORY_LIMIT_MB = String(memoryLimitMb);
  process.env.DS_SQLITE_CACHE_MB = String(tune.sqliteCacheMb);
  process.env.DS_WORKER_SQLITE_CACHE_MB = String(tune.workerSqliteCacheMb);
  process.env.DS_INDEX_RUN_MEM_CACHE_BYTES = String(tune.indexMemMb * 1024 * 1024);
  process.env.DS_INGEST_MAX_BATCH_BYTES = String(tune.ingestBatchMb * 1024 * 1024);
  process.env.DS_INGEST_MAX_QUEUE_BYTES = String(tune.ingestQueueMb * 1024 * 1024);
  process.env.DS_INDEX_BUILD_CONCURRENCY = String(tune.indexBuildConcurrency);
  process.env.DS_INDEX_COMPACT_CONCURRENCY = String(tune.indexCompactConcurrency);
  process.env.DS_SEGMENTER_WORKERS = String(tune.segmenterWorkers);
  process.env.DS_UPLOAD_CONCURRENCY = String(tune.uploadConcurrency);

  const presetLine = formatPresetList(presets, preset, (v) => v, (v) => String(v));
  console.log(`Auto-tuning for memory preset ${presetLine}`);
  console.log(
    `DS_MEMORY_LIMIT_MB presets: ${formatPresetList(presets, preset, (p) => memoryLimitForPreset(p), (v) => String(v))}`
  );
  console.log(
    `DS_SQLITE_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneFor(p).sqliteCacheMb, (v) => String(v))}`
  );
  console.log(
    `DS_WORKER_SQLITE_CACHE_MB presets: ${formatPresetList(
      presets,
      preset,
      (p) => tuneFor(p).workerSqliteCacheMb,
      (v) => String(v)
    )}`
  );
  console.log(
    `DS_INDEX_RUN_MEM_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneFor(p).indexMemMb, (v) => String(v))}`
  );
  console.log(
    `DS_INGEST_MAX_BATCH_MB presets: ${formatPresetList(presets, preset, (p) => tuneFor(p).ingestBatchMb, (v) => String(v))}`
  );
  console.log(
    `DS_INGEST_MAX_QUEUE_MB presets: ${formatPresetList(presets, preset, (p) => tuneFor(p).ingestQueueMb, (v) => String(v))}`
  );
  console.log(
    `DS_INDEX_BUILD_CONCURRENCY presets: ${formatPresetList(
      presets,
      preset,
      (p) => tuneFor(p).indexBuildConcurrency,
      (v) => String(v)
    )}`
  );
  console.log(
    `DS_INDEX_COMPACT_CONCURRENCY presets: ${formatPresetList(
      presets,
      preset,
      (p) => tuneFor(p).indexCompactConcurrency,
      (v) => String(v)
    )}`
  );
  console.log(
    `DS_SEGMENTER_WORKERS presets: ${formatPresetList(
      presets,
      preset,
      (p) => tuneFor(p).segmenterWorkers,
      (v) => String(v)
    )}`
  );
  console.log(
    `DS_UPLOAD_CONCURRENCY presets: ${formatPresetList(
      presets,
      preset,
      (p) => tuneFor(p).uploadConcurrency,
      (v) => String(v)
    )}`
  );
}

if (autoTuneEnabled) applyAutoTune(autoTuneValueMb);

const cfg = loadConfig();

const statsEnabled = args.includes("--stats");
const histEnabled = args.includes("--hist");
const bootstrapEnabled = args.includes("--bootstrap-from-r2");
const bpBudgetRaw = process.env.DS_BACKPRESSURE_BUDGET_MS;
const bpBudgetMs = bpBudgetRaw ? Number(bpBudgetRaw) : cfg.ingestFlushIntervalMs + 1;
if (bpBudgetRaw && !Number.isFinite(bpBudgetMs)) {
  // eslint-disable-next-line no-console
  console.error(`invalid DS_BACKPRESSURE_BUDGET_MS: ${bpBudgetRaw}`);
  process.exit(1);
}
const stats = statsEnabled ? new StatsCollector({ backpressureBudgetMs: bpBudgetMs }) : undefined;
const hist = histEnabled ? new LatencyHistogramCollector() : undefined;

const storeIdx = args.indexOf("--object-store");
const storeChoice = storeIdx >= 0 ? args[storeIdx + 1] : null;
if (!storeChoice || (storeChoice !== "r2" && storeChoice !== "local")) {
  // eslint-disable-next-line no-console
  console.error("missing or invalid --object-store (expected: r2 | local)");
  process.exit(1);
}

let store;
if (storeChoice === "local") {
  const memBytesRaw = process.env.DS_MOCK_R2_MAX_INMEM_BYTES;
  const memMbRaw = process.env.DS_MOCK_R2_MAX_INMEM_MB;
  const memBytes = memBytesRaw ? Number(memBytesRaw) : memMbRaw ? Number(memMbRaw) * 1024 * 1024 : null;
  if (memBytesRaw && !Number.isFinite(memBytes)) {
    // eslint-disable-next-line no-console
    console.error(`invalid DS_MOCK_R2_MAX_INMEM_BYTES: ${memBytesRaw}`);
    process.exit(1);
  }
  if (memMbRaw && !Number.isFinite(Number(memMbRaw))) {
    // eslint-disable-next-line no-console
    console.error(`invalid DS_MOCK_R2_MAX_INMEM_MB: ${memMbRaw}`);
    process.exit(1);
  }
  const spillDir = process.env.DS_MOCK_R2_SPILL_DIR;
  store = memBytes != null || spillDir ? new MockR2Store({ maxInMemoryBytes: memBytes ?? undefined, spillDir }) : new MockR2Store();
} else {
  const bucket = process.env.DURABLE_STREAMS_R2_BUCKET;
  const accountId = process.env.DURABLE_STREAMS_R2_ACCOUNT_ID;
  const accessKeyId = process.env.DURABLE_STREAMS_R2_ACCESS_KEY_ID;
  const secretAccessKey = process.env.DURABLE_STREAMS_R2_SECRET_ACCESS_KEY;
  if (!bucket || !accountId || !accessKeyId || !secretAccessKey) {
    // eslint-disable-next-line no-console
    console.error("missing R2 env vars: DURABLE_STREAMS_R2_BUCKET, DURABLE_STREAMS_R2_ACCOUNT_ID, DURABLE_STREAMS_R2_ACCESS_KEY_ID, DURABLE_STREAMS_R2_SECRET_ACCESS_KEY");
    process.exit(1);
  }
  store = new R2ObjectStore({
    bucket,
    accountId,
    accessKeyId,
    secretAccessKey,
  });
}

if (bootstrapEnabled) {
  await bootstrapFromR2(cfg, store, { clearLocal: true });
}

const app = createApp(cfg, store, { stats });
const statsIntervalMs = process.env.DS_STATS_INTERVAL_MS ? Number(process.env.DS_STATS_INTERVAL_MS) : 60_000;
if (process.env.DS_STATS_INTERVAL_MS && !Number.isFinite(statsIntervalMs)) {
  // eslint-disable-next-line no-console
  console.error(`invalid DS_STATS_INTERVAL_MS: ${process.env.DS_STATS_INTERVAL_MS}`);
  process.exit(1);
}
const statsReporter =
  statsEnabled && stats
    ? new StatsReporter(stats, app.deps.db, app.deps.uploader, app.deps.ingest, app.deps.backpressure, app.deps.memory, statsIntervalMs)
    : null;
const histReporter = histEnabled && hist ? new HistogramReporter(hist, statsIntervalMs) : null;

const fetchWithHist = hist
  ? async (req: Request): Promise<Response> => {
      const start = Date.now();
      const resp = await app.fetch(req);
      const url = req.url;
      let path: string | null = null;
      if (url.startsWith("/")) {
        path = url;
      } else {
        const schemeIdx = url.indexOf("://");
        if (schemeIdx !== -1) {
          const pathIdx = url.indexOf("/", schemeIdx + 3);
          path = pathIdx === -1 ? "/" : url.slice(pathIdx);
        }
      }
      if (path) {
        const isStream = path.startsWith("/v1/stream/") || path.startsWith("/v1/streams");
        if (isStream) {
          const ms = Date.now() - start;
          const method = req.method.toUpperCase();
          if (method === "GET" || method === "HEAD") hist.recordRead(ms);
          else if (method === "POST" || method === "PUT" || method === "DELETE") hist.recordWrite(ms);
        }
      }
      return resp;
    }
  : app.fetch;

const server = Bun.serve({
  hostname: cfg.host,
  port: cfg.port,
  // Default Bun idleTimeout is 10s, which is too low for long-poll endpoints like /touch/wait.
  // Bun expects seconds here.
  idleTimeout: (() => {
    const raw = process.env.DS_HTTP_IDLE_TIMEOUT_SECONDS;
    if (raw == null || raw.trim() === "") return 180;
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) {
      // eslint-disable-next-line no-console
      console.error(`invalid DS_HTTP_IDLE_TIMEOUT_SECONDS: ${raw}`);
      process.exit(1);
    }
    return n;
  })(),
  fetch: fetchWithHist,
});

statsReporter?.start();
histReporter?.start();

let shuttingDown = false;
const shutdown = (signal: NodeJS.Signals) => {
  if (shuttingDown) return;
  shuttingDown = true;
  // eslint-disable-next-line no-console
  console.log(`received ${signal}, shutting down prisma-streams server`);
  statsReporter?.stop();
  histReporter?.stop();
  try {
    server.stop(true);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("failed to stop HTTP server cleanly", err);
  }
  try {
    app.close();
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("failed to close application cleanly", err);
    process.exitCode = 1;
  }
};

const listenTarget = cfg.host.includes(":") ? `[${cfg.host}]:${server.port}` : `${cfg.host}:${server.port}`;

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

// eslint-disable-next-line no-console
console.log(`prisma-streams server listening on ${listenTarget}`);
