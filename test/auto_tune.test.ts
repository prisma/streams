import { describe, expect, test } from "bun:test";
import { memoryLimitForPreset, tuneForPreset } from "../src/auto_tune";

describe("auto tune presets", () => {
  test("keeps ingest batch and queue budgets conservative on small presets", () => {
    expect(memoryLimitForPreset(256)).toBe(300);
    expect(tuneForPreset(256).segmentMaxMiB).toBe(16);
    expect(tuneForPreset(256).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(256).segmentTargetRows).toBe(100_000);
    expect(tuneForPreset(1024).segmentMaxMiB).toBe(16);
    expect(tuneForPreset(1024).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(1024).segmentTargetRows).toBe(100_000);
    expect(tuneForPreset(1024).ingestBatchMb).toBe(4);
    expect(tuneForPreset(1024).ingestQueueMb).toBe(16);
    expect(tuneForPreset(1024).ingestConcurrency).toBe(2);
    expect(tuneForPreset(1024).readConcurrency).toBe(4);
    expect(tuneForPreset(1024).searchConcurrency).toBe(2);
    expect(tuneForPreset(1024).asyncIndexConcurrency).toBe(1);
    expect(tuneForPreset(1024).segmenterWorkers).toBe(1);
    expect(tuneForPreset(1024).uploadConcurrency).toBe(2);
    expect(tuneForPreset(1024).indexBuilders).toBe(1);
    expect(tuneForPreset(1024).lexiconIndexCacheMb).toBe(32);
    expect(tuneForPreset(1024).searchCompanionSectionCacheMb).toBe(64);
    expect(tuneForPreset(1024).searchCompanionBatchSegments).toBe(1);
    expect(tuneForPreset(1024).searchCompanionYieldBlocks).toBe(1);
    expect(tuneForPreset(2048).segmentMaxMiB).toBe(16);
    expect(tuneForPreset(2048).segmentTargetRows).toBe(100_000);
    expect(tuneForPreset(2048).ingestBatchMb).toBe(8);
    expect(tuneForPreset(2048).ingestQueueMb).toBe(32);
    expect(tuneForPreset(2048).ingestConcurrency).toBe(2);
    expect(tuneForPreset(2048).readConcurrency).toBe(4);
    expect(tuneForPreset(2048).searchConcurrency).toBe(2);
    expect(tuneForPreset(2048).asyncIndexConcurrency).toBe(2);
    expect(tuneForPreset(2048).segmenterWorkers).toBe(2);
    expect(tuneForPreset(2048).uploadConcurrency).toBe(4);
    expect(tuneForPreset(2048).indexBuilders).toBe(2);
    expect(tuneForPreset(2048).lexiconIndexCacheMb).toBe(64);
    expect(tuneForPreset(2048).searchCompanionSectionCacheMb).toBe(512);
    expect(tuneForPreset(2048).searchCompanionBatchSegments).toBe(1);
    expect(tuneForPreset(2048).searchCompanionYieldBlocks).toBe(1);
    expect(tuneForPreset(2048).blockMaxKiB).toBe(1024);
  });

  test("preserves larger ingest presets on bigger hosts", () => {
    expect(tuneForPreset(4096).segmentMaxMiB).toBe(16);
    expect(tuneForPreset(4096).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(4096).segmentTargetRows).toBe(100_000);
    expect(tuneForPreset(4096).ingestBatchMb).toBe(16);
    expect(tuneForPreset(4096).ingestQueueMb).toBe(64);
    expect(tuneForPreset(4096).ingestConcurrency).toBe(4);
    expect(tuneForPreset(4096).readConcurrency).toBe(8);
    expect(tuneForPreset(4096).searchConcurrency).toBe(4);
    expect(tuneForPreset(4096).asyncIndexConcurrency).toBe(2);
    expect(tuneForPreset(4096).segmenterWorkers).toBe(2);
    expect(tuneForPreset(4096).uploadConcurrency).toBe(4);
    expect(tuneForPreset(4096).indexBuilders).toBe(2);
    expect(tuneForPreset(4096).lexiconIndexCacheMb).toBe(128);
    expect(tuneForPreset(4096).searchCompanionSectionCacheMb).toBe(512);
    expect(tuneForPreset(4096).searchCompanionBatchSegments).toBe(2);
    expect(tuneForPreset(4096).searchCompanionYieldBlocks).toBe(2);
  });

  test("keeps DSB3 block size fixed across presets", () => {
    expect(tuneForPreset(256).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(512).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(1024).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(2048).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(4096).blockMaxKiB).toBe(1024);
    expect(tuneForPreset(8192).blockMaxKiB).toBe(1024);
  });
});
