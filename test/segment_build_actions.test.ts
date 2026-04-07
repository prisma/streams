import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 10,
    uploadIntervalMs: 10,
    segmenterWorkers: 0,
    ...overrides,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForSegmentBuildAction(
  app: ReturnType<typeof createApp>,
  stream: string,
  timeoutMs = 15_000
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const actions = app.deps.db.listSegmentBuildActions(stream, 20);
    const completed = actions.find((row) => row.status === "succeeded" && row.end_time_ms != null);
    if (completed) return completed;
    await sleep(25);
  }
  throw new Error("timeout waiting for segment build action");
}

describe("segment build action observability", () => {
  test("logs sealed segment builds with rows and output size", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-segment-build-actions-"));
    const cfg = makeConfig(root, {
      segmentMaxBytes: 96,
      segmentTargetRows: 3,
    });
    const app = createApp(cfg, new MockR2Store());
    const stream = "segment-actions";
    try {
      const createRes = await app.fetch(
        new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      );
      expect([200, 201]).toContain(createRes.status);

      for (let i = 0; i < 3; i += 1) {
        const appendRes = await app.fetch(
          new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              id: i,
              bucket: `k-${i % 2}`,
              payload: "x".repeat(80),
            }),
          })
        );
        expect(appendRes.status).toBe(204);
      }

      const action = await waitForSegmentBuildAction(app, stream);
      expect(action.action_kind).toBe("segment_build");
      expect(action.input_kind).toBe("wal");
      expect(action.input_count).toBeGreaterThan(0);
      expect(action.input_size_bytes).toBeGreaterThan(0n);
      expect(action.output_count).toBe(1);
      expect(action.output_size_bytes).toBeGreaterThan(0n);
      expect(action.segment_index).not.toBeNull();
      expect(action.segment_index!).toBeGreaterThanOrEqual(0);
      expect(action.start_offset).not.toBeNull();
      expect(action.start_offset!).toBeGreaterThanOrEqual(0n);
      expect(action.end_offset).not.toBeNull();
      expect(action.end_offset!).toBeGreaterThanOrEqual(action.start_offset!);
      expect(action.begin_time_ms).toBeGreaterThan(0n);
      expect(action.end_time_ms).not.toBeNull();
      expect(action.duration_ms).not.toBeNull();
      expect(action.duration_ms).toBeGreaterThanOrEqual(0n);

      const detail = JSON.parse(action.detail_json);
      expect(detail.pending_rows_before).toBeGreaterThan(0);
      expect(detail.pending_bytes_before).toBeGreaterThan(0);
      expect(detail.block_count).toBeGreaterThan(0);
      expect(detail.stop_reason).toBeTruthy();
      expect(detail.compression_ratio).toBeGreaterThan(0);
      expect(detail.wal_fetch_ms).toBeGreaterThanOrEqual(0);
      expect(detail.wal_materialize_ms).toBeGreaterThanOrEqual(0);
      expect(detail.wal_loop_ms).toBeGreaterThanOrEqual(detail.wal_materialize_ms);
      expect(detail.block_encode_ms).toBeGreaterThanOrEqual(0);
      expect(detail.busy_retry_attempts).toBeGreaterThanOrEqual(0);
      expect(detail.busy_retry_wait_ms).toBeGreaterThanOrEqual(0);
    } finally {
      app.deps.indexer?.stop();
      await sleep(20);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
