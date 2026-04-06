import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteDurableStore } from "../src/db/db";
import { getSqliteAdapterRuntimeCounts, openSqliteDatabase } from "../src/sqlite/adapter";
import { readSqliteRuntimeMemoryStats } from "../src/sqlite/runtime_stats";

describe("sqlite runtime stats", () => {
  test("tracks open connections, prepared statements, and exposes runtime stats", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sqlite-runtime-"));
    const path = join(root, "test.sqlite");
    const before = getSqliteAdapterRuntimeCounts();
    const db = openSqliteDatabase(path);
    const stmt = db.query("SELECT 1 AS value;");
    try {
      const mid = getSqliteAdapterRuntimeCounts();
      expect(mid.open_connections).toBeGreaterThanOrEqual(before.open_connections + 1);
      expect(mid.prepared_statements).toBeGreaterThanOrEqual(before.prepared_statements + 1);

      const stats = readSqliteRuntimeMemoryStats(0);
      expect(stats).toEqual(
        expect.objectContaining({
          available: expect.any(Boolean),
          source: expect.any(String),
          memory_used_bytes: expect.any(Number),
          open_connections: expect.any(Number),
          prepared_statements: expect.any(Number),
        })
      );
      expect(stats.open_connections).toBeGreaterThanOrEqual(1);
      expect(stats.prepared_statements).toBeGreaterThanOrEqual(1);
      expect(stmt.get()).toEqual({ value: 1 });
    } finally {
      stmt.finalize?.();
      db.close();
      rmSync(root, { recursive: true, force: true });
    }

    const after = getSqliteAdapterRuntimeCounts();
    expect(after.open_connections).toBe(before.open_connections);
    expect(after.prepared_statements).toBe(before.prepared_statements);
  });

  test("reuses identical query text instead of growing prepared statements", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sqlite-cache-"));
    const path = join(root, "test.sqlite");
    const before = getSqliteAdapterRuntimeCounts();
    const db = openSqliteDatabase(path);
    try {
      const stmtA = db.query("SELECT 1 AS value;");
      expect(stmtA.get()).toEqual({ value: 1 });
      const afterFirst = getSqliteAdapterRuntimeCounts();
      expect(afterFirst.prepared_statements).toBeGreaterThanOrEqual(before.prepared_statements + 1);

      for (let i = 0; i < 50; i++) {
        const stmt = db.query("SELECT 1 AS value;");
        expect(stmt.get()).toEqual({ value: 1 });
      }
      const afterRepeated = getSqliteAdapterRuntimeCounts();
      expect(afterRepeated.prepared_statements).toBe(afterFirst.prepared_statements);

      const stmtB = db.query("SELECT 2 AS value;");
      expect(stmtB.get()).toEqual({ value: 2 });
      const afterDistinct = getSqliteAdapterRuntimeCounts();
      expect(afterDistinct.prepared_statements).toBe(afterFirst.prepared_statements + 1);
    } finally {
      db.close();
      rmSync(root, { recursive: true, force: true });
    }

    const after = getSqliteAdapterRuntimeCounts();
    expect(after.open_connections).toBe(before.open_connections);
    expect(after.prepared_statements).toBe(before.prepared_statements);
  });

  test("store helper loops do not accumulate prepared statements across repeated dynamic helper calls", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sqlite-store-cache-"));
    const path = join(root, "wal.sqlite");
    const before = getSqliteAdapterRuntimeCounts();
    const store = new SqliteDurableStore(path);
    try {
      store.ensureStream("cache-test");
      store.listStreamsMissingLogicalSize(10);
      store.getWalOldestOffset("cache-test");
      store.getWalOldestTimestampMs("cache-test");
      const afterWarmup = getSqliteAdapterRuntimeCounts();
      for (let i = 0; i < 100; i++) {
        store.listStreamsMissingLogicalSize(10);
        store.getWalOldestOffset("cache-test");
        store.getWalOldestTimestampMs("cache-test");
      }
      const afterLoop = getSqliteAdapterRuntimeCounts();
      expect(afterLoop.prepared_statements).toBe(afterWarmup.prepared_statements);
    } finally {
      store.close();
      rmSync(root, { recursive: true, force: true });
    }

    const after = getSqliteAdapterRuntimeCounts();
    expect(after.open_connections).toBe(before.open_connections);
    expect(after.prepared_statements).toBe(before.prepared_statements);
  });

  test("deleteWalThrough uses finalized fresh iterator statements", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sqlite-delete-iter-"));
    const path = join(root, "wal.sqlite");
    const before = getSqliteAdapterRuntimeCounts();
    const store = new SqliteDurableStore(path);
    try {
      store.ensureStream("delete-iter-test");
      store.appendWalRows({
        stream: "delete-iter-test",
        startOffset: 0n,
        baseAppendMs: 1n,
        rows: [{ routingKey: null, contentType: null, payload: new Uint8Array([1]), appendMs: 1n }],
      });

      const afterAppendWarmup = getSqliteAdapterRuntimeCounts();

      store.appendWalRows({
        stream: "delete-iter-test",
        startOffset: 1n,
        baseAppendMs: 2n,
        rows: [{ routingKey: null, contentType: null, payload: new Uint8Array([2]), appendMs: 2n }],
      });
      store.deleteWalThrough("delete-iter-test", 1n);

      const afterDelete = getSqliteAdapterRuntimeCounts();
      const delta = afterDelete.prepared_statements - afterAppendWarmup.prepared_statements;

      // deleteWalThrough may cache one UPDATE statement, but iterator DELETE statements
      // must be fresh and finalized per call rather than retained.
      expect(delta).toBeLessThanOrEqual(1);
    } finally {
      store.close();
      rmSync(root, { recursive: true, force: true });
    }

    const after = getSqliteAdapterRuntimeCounts();
    expect(after.open_connections).toBe(before.open_connections);
    expect(after.prepared_statements).toBe(before.prepared_statements);
  });
});
