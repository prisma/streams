import { describe, expect, test } from "bun:test";
import { memoryLimitForPreset, tuneForPreset } from "../src/auto_tune";

describe("auto tune presets", () => {
  test("keeps ingest batch and queue budgets conservative on small presets", () => {
    expect(memoryLimitForPreset(256)).toBe(300);
    expect(tuneForPreset(1024).segmentMaxMiB).toBe(2);
    expect(tuneForPreset(1024).segmentTargetRows).toBe(6_250);
    expect(tuneForPreset(1024).ingestBatchMb).toBe(4);
    expect(tuneForPreset(1024).ingestQueueMb).toBe(16);
    expect(tuneForPreset(1024).segmenterWorkers).toBe(1);
    expect(tuneForPreset(1024).uploadConcurrency).toBe(2);
    expect(tuneForPreset(1024).indexBuildConcurrency).toBe(1);
    expect(tuneForPreset(1024).searchCompanionBatchSegments).toBe(1);
    expect(tuneForPreset(1024).searchCompanionYieldBlocks).toBe(1);
    expect(tuneForPreset(2048).segmentMaxMiB).toBe(4);
    expect(tuneForPreset(2048).segmentTargetRows).toBe(12_500);
    expect(tuneForPreset(2048).ingestBatchMb).toBe(8);
    expect(tuneForPreset(2048).ingestQueueMb).toBe(32);
    expect(tuneForPreset(2048).segmenterWorkers).toBe(1);
    expect(tuneForPreset(2048).uploadConcurrency).toBe(2);
    expect(tuneForPreset(2048).indexBuildConcurrency).toBe(1);
    expect(tuneForPreset(2048).searchCompanionBatchSegments).toBe(1);
    expect(tuneForPreset(2048).searchCompanionYieldBlocks).toBe(1);
  });

  test("preserves larger ingest presets on bigger hosts", () => {
    expect(tuneForPreset(4096).segmentMaxMiB).toBe(16);
    expect(tuneForPreset(4096).segmentTargetRows).toBe(50_000);
    expect(tuneForPreset(4096).ingestBatchMb).toBe(32);
    expect(tuneForPreset(4096).ingestQueueMb).toBe(128);
    expect(tuneForPreset(4096).segmenterWorkers).toBe(4);
    expect(tuneForPreset(4096).uploadConcurrency).toBe(8);
    expect(tuneForPreset(4096).indexBuildConcurrency).toBe(4);
    expect(tuneForPreset(4096).searchCompanionBatchSegments).toBe(4);
    expect(tuneForPreset(4096).searchCompanionYieldBlocks).toBe(4);
  });
});
