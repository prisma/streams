import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { SegmenterControlCoordinator } from "../src/segment/segmenter_control";

describe("segmenter control coordinator", () => {
  test("routes claim and commit through the owning sqlite connection under write contention", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-segmenter-control-"));
    try {
      const cfg = {
        ...loadConfig(),
        rootDir: root,
        dbPath: `${root}/wal.sqlite`,
        port: 0,
        ingestBusyTimeoutMs: 1000,
      };
      const mainDb = new SqliteDurableStore(cfg.dbPath);
      const workerDb = new SqliteDurableStore(cfg.dbPath, { skipMigrations: true });
      workerDb.db.exec("PRAGMA busy_timeout = 0;");
      mainDb.ensureStream("s", { contentType: "application/json" });
      const coordinator = new SegmenterControlCoordinator(cfg, mainDb);

      mainDb.db.exec("BEGIN IMMEDIATE;");
      try {
        expect(() => workerDb.tryClaimSegment("s")).toThrow(/busy|locked/i);
        await expect(coordinator.tryClaimSegment("s")).resolves.toBe(true);

        const row = {
          segmentId: "seg-0",
          stream: "s",
          segmentIndex: 0,
          startOffset: 0n,
          endOffset: 0n,
          blockCount: 1,
          lastAppendMs: 1n,
          payloadBytes: 10n,
          sizeBytes: 8,
          localPath: join(root, "seg-0.bin"),
          rowsSealed: 1n,
        };

        expect(() => workerDb.commitSealedSegment(row)).toThrow(/busy|locked/i);
        await expect(coordinator.commitSealedSegment(row)).resolves.toBeUndefined();
        await expect(coordinator.releaseSegmentClaim("s")).resolves.toBeUndefined();

        mainDb.db.exec("COMMIT;");
      } catch (error) {
        try {
          mainDb.db.exec("ROLLBACK;");
        } catch {
          // ignore
        }
        throw error;
      }

      const seg = mainDb.getSegmentByIndex("s", 0);
      expect(seg).not.toBeNull();
      expect(seg?.end_offset).toBe(0n);
      expect(mainDb.getStream("s")?.sealed_through).toBe(0n);

      workerDb.close();
      mainDb.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
