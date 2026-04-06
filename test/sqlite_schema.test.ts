import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteDurableStore } from "../src/db/db";
import { initSchema, SCHEMA_VERSION } from "../src/db/schema";
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
      expect(tables).toContain("secondary_index_state");
      expect(tables).toContain("secondary_index_runs");
      expect(tables).toContain("search_companion_plans");
      expect(tables).toContain("search_segment_companions");
      expect(tables).toContain("schemas");
      expect(tables).toContain("stream_profiles");
      expect(tables).toContain("stream_touch_state");
      expect(tables).toContain("schema_version");
      expect(tables).not.toContain("search_family_state");
      expect(tables).not.toContain("search_family_segments");

      const streamCols = db.db
        .query(`PRAGMA table_info(streams);`)
        .all()
        .map((r: any) => r.name);
      expect(streamCols).toContain("uploaded_segment_count");
      expect(streamCols).toContain("profile");
      expect(streamCols).toContain("logical_size_bytes");

      const walIndexes = db.db
        .query(`PRAGMA index_list('wal');`)
        .all()
        .map((r: any) => String(r.name));
      expect(walIndexes).toContain("wal_stream_offset_uniq");
      expect(walIndexes).not.toContain("wal_stream_offset_idx");

      const segmentCols = db.db
        .query(`PRAGMA table_info(segments);`)
        .all()
        .map((r: any) => r.name);
      expect(segmentCols).toContain("payload_bytes");
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

  test("migration from v23 adds segment payload_bytes", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sql-v23-"));
    const dbPath = `${root}/wal.sqlite`;
    const db = openSqliteDatabase(dbPath);
    try {
      initSchema(db);
      db.exec(`ALTER TABLE segments DROP COLUMN payload_bytes;`);
      db.exec(`UPDATE schema_version SET version = 23;`);

      initSchema(db);

      const schemaVersion = db.query(`SELECT version FROM schema_version LIMIT 1;`).get() as any;
      expect(Number(schemaVersion.version)).toBe(SCHEMA_VERSION);

      const segmentCols = db
        .query(`PRAGMA table_info(segments);`)
        .all()
        .map((r: any) => String(r.name));
      expect(segmentCols).toContain("payload_bytes");
    } finally {
      db.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("migration from v22 drops redundant wal stream/offset index", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-sql-v22-"));
    const dbPath = `${root}/wal.sqlite`;
    const db = openSqliteDatabase(dbPath);
    try {
      initSchema(db);
      db.exec(`CREATE INDEX IF NOT EXISTS wal_stream_offset_idx ON wal(stream, offset);`);
      db.exec(`UPDATE schema_version SET version = 22;`);

      initSchema(db);

      const schemaVersion = db.query(`SELECT version FROM schema_version LIMIT 1;`).get() as any;
      expect(Number(schemaVersion.version)).toBe(SCHEMA_VERSION);

      const walIndexes = db
        .query(`PRAGMA index_list('wal');`)
        .all()
        .map((r: any) => String(r.name));
      expect(walIndexes).toContain("wal_stream_offset_uniq");
      expect(walIndexes).not.toContain("wal_stream_offset_idx");
    } finally {
      db.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
