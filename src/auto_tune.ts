export type AutoTuneConfig = {
  segmentMaxMiB: number;
  segmentTargetRows: number;
  sqliteCacheMb: number;
  workerSqliteCacheMb: number;
  indexMemMb: number;
  ingestBatchMb: number;
  ingestQueueMb: number;
  indexBuildConcurrency: number;
  indexCompactConcurrency: number;
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
    // Small hosts need smaller sealed segments so bundled companion builds do
    // not materialize 16 MiB / 50k-row working sets in a single pass.
    segmentMaxMiB: p >= 4096 ? 16 : p >= 2048 ? 4 : p >= 1024 ? 2 : 1,
    segmentTargetRows: p >= 4096 ? 50_000 : p >= 2048 ? 12_500 : p >= 1024 ? 6_250 : 3_000,
    sqliteCacheMb: Math.max(8, Math.floor(p / 16)),
    workerSqliteCacheMb: Math.max(8, Math.min(32, Math.floor(p / 128))),
    indexMemMb: Math.max(4, Math.floor(p / 64)),
    // Keep append working sets tighter on <=2 GiB presets because the request path
    // still holds multiple copies of JSON batches while normalizing and queuing.
    ingestBatchMb: p >= 4096 ? Math.max(2, Math.floor(p / 128)) : Math.max(2, Math.floor(p / 256)),
    ingestQueueMb: p >= 4096 ? Math.max(8, Math.floor(p / 32)) : Math.max(8, Math.floor(p / 64)),
    // Keep <=2 GiB presets single-lane for background work. These hosts do not
    // have enough headroom for append, segment cut, upload, and companion work
    // to overlap aggressively under the GH Archive "all" workload.
    indexBuildConcurrency: p >= 8192 ? 8 : p >= 4096 ? 4 : 1,
    indexCompactConcurrency: p >= 4096 ? 4 : 1,
    segmenterWorkers: p >= 8192 ? 8 : p >= 4096 ? 4 : 1,
    uploadConcurrency: p >= 8192 ? 16 : p >= 4096 ? 8 : 2,
    searchCompanionBatchSegments: p >= 4096 ? 4 : 1,
    searchCompanionYieldBlocks: p >= 4096 ? 4 : 1,
  };
}
