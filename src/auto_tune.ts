export type AutoTuneConfig = {
  segmentMaxMiB: number;
  blockMaxKiB: number;
  segmentTargetRows: number;
  sqliteCacheMb: number;
  workerSqliteCacheMb: number;
  indexMemMb: number;
  lexiconIndexCacheMb: number;
  searchCompanionTocCacheMb: number;
  searchCompanionSectionCacheMb: number;
  ingestBatchMb: number;
  ingestQueueMb: number;
  ingestConcurrency: number;
  readConcurrency: number;
  searchConcurrency: number;
  asyncIndexConcurrency: number;
  indexBuilders: number;
  segmenterWorkers: number;
  uploadConcurrency: number;
  searchCompanionBatchSegments: number;
  searchCompanionYieldBlocks: number;
};

export const AUTO_TUNE_PRESETS = [256, 512, 1024, 2048, 4096, 8192] as const;

export function memoryLimitForPreset(preset: number): number {
  return preset === 256 ? 300 : preset;
}

export function tuneForPreset(p: number): AutoTuneConfig {
  return {
    // Segment geometry is fixed across presets. Smaller hosts still scale
    // concurrency and cache budgets down, but they keep the same 16 MiB /
    // 100k-row seal thresholds so upload throughput is not dominated by many
    // tiny compressed segment objects.
    segmentMaxMiB: 16,
    // Block geometry is also fixed across presets. This keeps the segment build
    // path and read trade-offs predictable instead of introducing another
    // memory-preset-specific format knob.
    blockMaxKiB: 1024,
    segmentTargetRows: 100_000,
    sqliteCacheMb: Math.max(8, Math.floor(p / 16)),
    workerSqliteCacheMb: Math.max(8, Math.min(32, Math.floor(p / 128))),
    indexMemMb: Math.max(4, Math.floor(p / 64)),
    lexiconIndexCacheMb: p >= 8192 ? 256 : p >= 4096 ? 128 : p >= 2048 ? 64 : p >= 1024 ? 32 : p >= 512 ? 16 : 8,
    searchCompanionTocCacheMb: p >= 8192 ? 4 : p >= 4096 ? 2 : 1,
    searchCompanionSectionCacheMb: p >= 8192 ? 128 : p >= 4096 ? 64 : p >= 2048 ? 32 : p >= 1024 ? 16 : 8,
    // Keep append working sets tighter on <=2 GiB presets because the request path
    // still holds multiple copies of JSON batches while normalizing and queuing.
    ingestBatchMb: p >= 8192 ? 64 : p >= 4096 ? 16 : p >= 2048 ? 8 : p >= 1024 ? 4 : 2,
    ingestQueueMb: p >= 8192 ? 128 : p >= 4096 ? 64 : p >= 2048 ? 32 : p >= 1024 ? 16 : 8,
    ingestConcurrency: p >= 8192 ? 8 : p >= 4096 ? 4 : p >= 1024 ? 2 : 1,
    readConcurrency: p >= 8192 ? 16 : p >= 4096 ? 8 : p >= 1024 ? 4 : 2,
    searchConcurrency: p >= 8192 ? 8 : p >= 4096 ? 4 : p >= 1024 ? 2 : 1,
    asyncIndexConcurrency: p >= 8192 ? 4 : p >= 4096 ? 2 : 1,
    // Keep <=2 GiB presets single-lane for background work. These hosts do not
    // have enough headroom for append, segment cut, upload, and companion work
    // to overlap aggressively under the GH Archive "all" workload.
    indexBuilders: p >= 8192 ? 4 : p >= 4096 ? 2 : 1,
    segmenterWorkers: p >= 8192 ? 4 : p >= 2048 ? 2 : 1,
    uploadConcurrency: p >= 8192 ? 8 : p >= 2048 ? 4 : p >= 1024 ? 2 : 1,
    searchCompanionBatchSegments: p >= 8192 ? 4 : p >= 4096 ? 2 : 1,
    searchCompanionYieldBlocks: p >= 8192 ? 4 : p >= 4096 ? 2 : 1,
  };
}
