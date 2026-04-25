import { AUTO_TUNE_PRESETS, memoryLimitForPreset, tuneForPreset, type AutoTuneConfig } from "./auto_tune";

export class AutoTuneApplyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AutoTuneApplyError";
  }
}

export function parseAutoTuneArg(args: string[]): { enabled: boolean; valueMb: number | null } {
  let enabled = false;
  let valueMb: number | null = null;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--auto-tune") {
      enabled = true;
      const next = args[i + 1];
      if (next && !next.startsWith("--") && /^[0-9]+$/.test(next)) {
        valueMb = Number(next);
      }
    } else if (arg.startsWith("--auto-tune=")) {
      enabled = true;
      const raw = arg.split("=", 2)[1] ?? "";
      if (raw.trim() !== "") valueMb = Number(raw);
    }
  }
  return { enabled, valueMb };
}

function formatPresetList<T>(presets: number[], selected: number, map: (preset: number) => T, fmt: (val: T) => string): string {
  return presets
    .map((preset) => {
      const value = fmt(map(preset));
      return preset === selected ? `[${value}]` : value;
    })
    .join(", ");
}

export function applyAutoTune(
  overrideMb: number | null,
  opts: {
    env?: NodeJS.ProcessEnv;
    log?: (message: string) => void;
  } = {}
): void {
  const env = opts.env ?? process.env;
  const log = opts.log ?? console.log;
  const envMemRaw = env.DS_MEMORY_LIMIT_MB;
  if (overrideMb != null) {
    if (envMemRaw) {
      throw new AutoTuneApplyError("--auto-tune with a value cannot be used with DS_MEMORY_LIMIT_MB");
    }
  } else if (!envMemRaw) {
    throw new AutoTuneApplyError("--auto-tune requires DS_MEMORY_LIMIT_MB to be set (or pass a value)");
  }
  const memMb = overrideMb != null ? overrideMb : Number(envMemRaw);
  if (!Number.isFinite(memMb) || memMb <= 0) {
    const bad = overrideMb != null ? String(overrideMb) : String(envMemRaw);
    throw new AutoTuneApplyError(`invalid DS_MEMORY_LIMIT_MB: ${bad}`);
  }
  if (env.DS_MEMORY_LIMIT_BYTES) {
    throw new AutoTuneApplyError("--auto-tune does not allow DS_MEMORY_LIMIT_BYTES; use DS_MEMORY_LIMIT_MB");
  }

  const conflictVars = [
    "DS_SEGMENT_MAX_BYTES",
    "DS_SEGMENT_TARGET_ROWS",
    "DS_SEGMENT_CACHE_MAX_BYTES",
    "DS_INDEX_CHECK_MS",
    "DS_SQLITE_CACHE_MB",
    "DS_SQLITE_CACHE_BYTES",
    "DS_WORKER_SQLITE_CACHE_MB",
    "DS_WORKER_SQLITE_CACHE_BYTES",
    "DS_INDEX_RUN_MEM_CACHE_BYTES",
    "DS_LEXICON_INDEX_CACHE_MAX_BYTES",
    "DS_INGEST_MAX_BATCH_BYTES",
    "DS_INGEST_MAX_QUEUE_BYTES",
    "DS_INGEST_CONCURRENCY",
    "DS_READ_CONCURRENCY",
    "DS_SEARCH_CONCURRENCY",
    "DS_ASYNC_INDEX_CONCURRENCY",
    "DS_INDEX_BUILD_CONCURRENCY",
    "DS_INDEX_COMPACT_CONCURRENCY",
    "DS_SEGMENTER_WORKERS",
    "DS_UPLOAD_CONCURRENCY",
    "DS_SEARCH_COMPANION_TOC_CACHE_BYTES",
    "DS_SEARCH_COMPANION_SECTION_CACHE_BYTES",
    "DS_SEARCH_COMPANION_BATCH_SEGMENTS",
    "DS_SEARCH_COMPANION_YIELD_BLOCKS",
  ];
  const conflicts = conflictVars.filter((v) => env[v] != null);
  if (conflicts.length > 0) {
    throw new AutoTuneApplyError(`--auto-tune cannot be used with manual memory settings: ${conflicts.join(", ")}`);
  }

  const presets = [...AUTO_TUNE_PRESETS];
  const preset = [...presets].reverse().find((v) => v <= memMb);
  if (!preset) {
    throw new AutoTuneApplyError(`DS_MEMORY_LIMIT_MB=${memMb} is below the minimum preset (256)`);
  }
  const tune: AutoTuneConfig = tuneForPreset(preset);

  const memoryLimitMb = memoryLimitForPreset(preset);
  env.DS_AUTO_TUNE_REQUESTED_MB = String(memMb);
  env.DS_AUTO_TUNE_PRESET_MB = String(preset);
  env.DS_AUTO_TUNE_EFFECTIVE_MEMORY_LIMIT_MB = String(memoryLimitMb);
  env.DS_MEMORY_LIMIT_MB = String(memoryLimitMb);
  env.DS_SEGMENT_MAX_BYTES = String(tune.segmentMaxMiB * 1024 * 1024);
  env.DS_SEGMENT_TARGET_ROWS = String(tune.segmentTargetRows);
  env.DS_SEGMENT_CACHE_MAX_BYTES = String(tune.segmentCacheMb * 1024 * 1024);
  env.DS_INDEX_CHECK_MS = String(tune.indexCheckMs);
  env.DS_SQLITE_CACHE_MB = String(tune.sqliteCacheMb);
  env.DS_WORKER_SQLITE_CACHE_MB = String(tune.workerSqliteCacheMb);
  env.DS_INDEX_RUN_MEM_CACHE_BYTES = String(tune.indexMemMb * 1024 * 1024);
  env.DS_LEXICON_INDEX_CACHE_MAX_BYTES = String(tune.lexiconIndexCacheMb * 1024 * 1024);
  env.DS_SEARCH_COMPANION_TOC_CACHE_BYTES = String(tune.searchCompanionTocCacheMb * 1024 * 1024);
  env.DS_SEARCH_COMPANION_SECTION_CACHE_BYTES = String(tune.searchCompanionSectionCacheMb * 1024 * 1024);
  env.DS_INGEST_MAX_BATCH_BYTES = String(tune.ingestBatchMb * 1024 * 1024);
  env.DS_INGEST_MAX_QUEUE_BYTES = String(tune.ingestQueueMb * 1024 * 1024);
  env.DS_INGEST_CONCURRENCY = String(tune.ingestConcurrency);
  env.DS_READ_CONCURRENCY = String(tune.readConcurrency);
  env.DS_SEARCH_CONCURRENCY = String(tune.searchConcurrency);
  env.DS_ASYNC_INDEX_CONCURRENCY = String(tune.asyncIndexConcurrency);
  env.DS_INDEX_BUILD_CONCURRENCY = String(tune.indexBuildConcurrency);
  env.DS_INDEX_COMPACT_CONCURRENCY = String(tune.indexCompactConcurrency);
  env.DS_SEGMENTER_WORKERS = String(tune.segmenterWorkers);
  env.DS_UPLOAD_CONCURRENCY = String(tune.uploadConcurrency);
  env.DS_SEARCH_COMPANION_BATCH_SEGMENTS = String(tune.searchCompanionBatchSegments);
  env.DS_SEARCH_COMPANION_YIELD_BLOCKS = String(tune.searchCompanionYieldBlocks);

  const presetLine = formatPresetList(presets, preset, (v) => v, (v) => String(v));
  log(`Auto-tuning for memory preset ${presetLine}`);
  log(
    `DS_MEMORY_LIMIT_MB presets: ${formatPresetList(presets, preset, (p) => memoryLimitForPreset(p), (v) => String(v))}`
  );
  log(`DS_SEGMENT_MAX_MIB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).segmentMaxMiB, (v) => String(v))}`);
  log(`DS_SEGMENT_TARGET_ROWS presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).segmentTargetRows, (v) => String(v))}`);
  log(`DS_SEGMENT_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).segmentCacheMb, (v) => String(v))}`);
  log(`DS_INDEX_CHECK_MS presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).indexCheckMs, (v) => String(v))}`);
  log(`DS_SQLITE_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).sqliteCacheMb, (v) => String(v))}`);
  log(`DS_WORKER_SQLITE_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).workerSqliteCacheMb, (v) => String(v))}`);
  log(`DS_INDEX_RUN_MEM_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).indexMemMb, (v) => String(v))}`);
  log(`DS_LEXICON_INDEX_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).lexiconIndexCacheMb, (v) => String(v))}`);
  log(`DS_SEARCH_COMPANION_TOC_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).searchCompanionTocCacheMb, (v) => String(v))}`);
  log(`DS_SEARCH_COMPANION_SECTION_CACHE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).searchCompanionSectionCacheMb, (v) => String(v))}`);
  log(`DS_INGEST_MAX_BATCH_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).ingestBatchMb, (v) => String(v))}`);
  log(`DS_INGEST_MAX_QUEUE_MB presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).ingestQueueMb, (v) => String(v))}`);
  log(`DS_INGEST_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).ingestConcurrency, (v) => String(v))}`);
  log(`DS_READ_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).readConcurrency, (v) => String(v))}`);
  log(`DS_SEARCH_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).searchConcurrency, (v) => String(v))}`);
  log(`DS_ASYNC_INDEX_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).asyncIndexConcurrency, (v) => String(v))}`);
  log(`DS_INDEX_BUILD_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).indexBuildConcurrency, (v) => String(v))}`);
  log(`DS_INDEX_COMPACT_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).indexCompactConcurrency, (v) => String(v))}`);
  log(`DS_SEGMENTER_WORKERS presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).segmenterWorkers, (v) => String(v))}`);
  log(`DS_UPLOAD_CONCURRENCY presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).uploadConcurrency, (v) => String(v))}`);
  log(`DS_SEARCH_COMPANION_BATCH_SEGMENTS presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).searchCompanionBatchSegments, (v) => String(v))}`);
  log(`DS_SEARCH_COMPANION_YIELD_BLOCKS presets: ${formatPresetList(presets, preset, (p) => tuneForPreset(p).searchCompanionYieldBlocks, (v) => String(v))}`);
}
