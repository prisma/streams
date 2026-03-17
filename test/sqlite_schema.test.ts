import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteDurableStore } from "../src/db/db";
import { initSchema } from "../src/db/schema";
import { openSqliteDatabase } from "../src/sqlite/adapter";

describe("sqlite schema", () => {
  test("tables exist after init", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sql-"));
    try {
      const db = new SqliteDurableStore(`${root}/wal.sqlite`);
      const tables = db.db
        .query(`SELECT name FROM sqlite_master WHERE type='table';`)
        .all()
        .map((r: any) => r.name);
      expect(tables).toContain("streams");
      expect(tables).toContain("wal");
      expect(tables).toContain("segments");
      expect(tables).toContain("manifests");
      expect(tables).toContain("stream_segment_meta");
      expect(tables).toContain("index_state");
      expect(tables).toContain("index_runs");
      expect(tables).toContain("schemas");
      expect(tables).toContain("stream_interpreters");
      expect(tables).toContain("schema_version");

      const streamCols = db.db
        .query(`PRAGMA table_info(streams);`)
        .all()
        .map((r: any) => r.name);
      expect(streamCols).toContain("uploaded_segment_count");
      db.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("skipMigrations does not run schema DDL", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sql-skip-"));
    const dbPath = `${root}/wal.sqlite`;
    const db = openSqliteDatabase(dbPath);
    try {
      initSchema(db, { skipMigrations: true });
      const tables = db
        .query(`SELECT name FROM sqlite_master WHERE type='table';`)
        .all()
        .map((r: any) => String(r.name));
      expect(tables).not.toContain("schema_version");
      expect(tables).not.toContain("streams");
      expect(tables).not.toContain("wal");
    } finally {
      db.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
