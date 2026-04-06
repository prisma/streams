import type { Config } from "../config";
import { loadConfig } from "../config";
import { memoryLimitForPreset, tuneForPreset } from "../auto_tune";
import { getDbPath, getDataDir } from "./state";
import { normalizeServerName } from "./paths";

export const LOCAL_AUTO_TUNE_PRESET_MB = 1024;

export function buildLocalConfig(args: { name?: string; port?: number }): Config {
  const name = normalizeServerName(args.name);
  const dataDir = getDataDir(name);
  const dbPath = getDbPath(name);
  const base = loadConfig();
  const tune = tuneForPreset(LOCAL_AUTO_TUNE_PRESET_MB);
  const effectiveMemoryLimitMb = memoryLimitForPreset(LOCAL_AUTO_TUNE_PRESET_MB);
  const port = args.port == null ? 0 : Math.max(0, Math.floor(args.port));

  return {
    ...base,
    autoTuneRequestedMemoryMb: LOCAL_AUTO_TUNE_PRESET_MB,
    autoTunePresetMb: LOCAL_AUTO_TUNE_PRESET_MB,
    autoTuneEffectiveMemoryLimitMb: effectiveMemoryLimitMb,
    rootDir: dataDir,
    dbPath,
    memoryLimitBytes: effectiveMemoryLimitMb * 1024 * 1024,
    segmentMaxBytes: tune.segmentMaxMiB * 1024 * 1024,
    segmentTargetRows: tune.segmentTargetRows,
    sqliteCacheBytes: tune.sqliteCacheMb * 1024 * 1024,
    workerSqliteCacheBytes: tune.workerSqliteCacheMb * 1024 * 1024,
    indexRunMemoryCacheBytes: tune.indexMemMb * 1024 * 1024,
    ingestMaxBatchBytes: tune.ingestBatchMb * 1024 * 1024,
    ingestMaxQueueBytes: tune.ingestQueueMb * 1024 * 1024,
    ingestConcurrency: tune.ingestConcurrency,
    readConcurrency: tune.readConcurrency,
    searchConcurrency: tune.searchConcurrency,
    asyncIndexConcurrency: tune.asyncIndexConcurrency,
    indexBuildConcurrency: tune.indexBuildConcurrency,
    indexCompactionConcurrency: tune.indexCompactConcurrency,
    uploadConcurrency: tune.uploadConcurrency,
    searchCompanionBuildBatchSegments: tune.searchCompanionBatchSegments,
    searchCompanionYieldBlocks: tune.searchCompanionYieldBlocks,
    port,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    segmenterWorkers: 0,
    lexiconIndexCacheMaxBytes: 0,
    lexiconMappedCacheEntries: 0,
    searchCompanionFileCacheMaxBytes: 0,
    searchCompanionTocCacheBytes: 0,
    searchCompanionSectionCacheBytes: 0,
    // Local dev mode prioritizes responsiveness over throughput.
    touchCheckIntervalMs: Math.min(base.touchCheckIntervalMs, 5),
  };
}
