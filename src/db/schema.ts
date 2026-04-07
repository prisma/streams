import type { SqliteDatabase } from "../sqlite/adapter.ts";
import { dsError } from "../util/ds_error.ts";

/**
 * SQLite schema + migrations.
 *
 * This rewrite uses SQLite as:
 *  - WAL (durable append log)
 *  - local metadata store (streams/segments/manifests/schemas)
 */

export const SCHEMA_VERSION = 25;

export const DEFAULT_PRAGMAS_SQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA temp_store = MEMORY;
`;

const CREATE_TABLES_V4_SQL = `
CREATE TABLE IF NOT EXISTS streams (
  stream TEXT PRIMARY KEY,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,

  content_type TEXT NOT NULL,
  profile TEXT NULL,
  stream_seq TEXT NULL,
  closed INTEGER NOT NULL DEFAULT 0,
  closed_producer_id TEXT NULL,
  closed_producer_epoch INTEGER NULL,
  closed_producer_seq INTEGER NULL,
  ttl_seconds INTEGER NULL,

  epoch INTEGER NOT NULL,
  next_offset INTEGER NOT NULL,
  sealed_through INTEGER NOT NULL,
  uploaded_through INTEGER NOT NULL,
  uploaded_segment_count INTEGER NOT NULL DEFAULT 0,

  pending_rows INTEGER NOT NULL,
  pending_bytes INTEGER NOT NULL,

  -- Logical payload bytes ever appended to this stream and still part of its
  -- visible history on this node. This is the constant-time source for
  -- management endpoints such as /_details.
  logical_size_bytes INTEGER NOT NULL DEFAULT 0,

  -- Logical size of retained rows in the wal table for this stream (payload-only bytes).
  -- This is explicitly tracked because SQLite file size is high-water and does not shrink
  -- deterministically after DELETE-based GC/retention trimming.
  wal_rows INTEGER NOT NULL DEFAULT 0,
  wal_bytes INTEGER NOT NULL DEFAULT 0,

  last_append_ms INTEGER NOT NULL,
  last_segment_cut_ms INTEGER NOT NULL,
  segment_in_progress INTEGER NOT NULL,

  expires_at_ms INTEGER NULL,
  stream_flags INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS streams_pending_bytes_idx ON streams(pending_bytes);
CREATE INDEX IF NOT EXISTS streams_last_cut_idx ON streams(last_segment_cut_ms);
CREATE INDEX IF NOT EXISTS streams_inprog_pending_idx ON streams(segment_in_progress, pending_bytes, last_segment_cut_ms);

CREATE TABLE IF NOT EXISTS wal (
  id INTEGER PRIMARY KEY,
  stream TEXT NOT NULL,
  offset INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  payload BLOB NOT NULL,
  payload_len INTEGER NOT NULL,
  routing_key BLOB NULL,
  content_type TEXT NULL,
  flags INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS wal_stream_offset_uniq ON wal(stream, offset);
CREATE INDEX IF NOT EXISTS wal_ts_idx ON wal(ts_ms);

CREATE TABLE IF NOT EXISTS segments (
  segment_id TEXT PRIMARY KEY,
  stream TEXT NOT NULL,
  segment_index INTEGER NOT NULL,
  start_offset INTEGER NOT NULL,
  end_offset INTEGER NOT NULL,
  block_count INTEGER NOT NULL,
  last_append_ms INTEGER NOT NULL,
  payload_bytes INTEGER NOT NULL DEFAULT 0,
  size_bytes INTEGER NOT NULL,
  local_path TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  uploaded_at_ms INTEGER NULL,
  r2_etag TEXT NULL
);

CREATE TABLE IF NOT EXISTS stream_segment_meta (
  stream TEXT PRIMARY KEY,
  segment_count INTEGER NOT NULL,
  segment_offsets BLOB NOT NULL,
  segment_blocks BLOB NOT NULL,
  segment_last_ts BLOB NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS segments_stream_index_uniq ON segments(stream, segment_index);
CREATE INDEX IF NOT EXISTS segments_stream_start_idx ON segments(stream, start_offset);
CREATE INDEX IF NOT EXISTS segments_pending_upload_idx ON segments(uploaded_at_ms);

CREATE TABLE IF NOT EXISTS manifests (
  stream TEXT PRIMARY KEY,
  generation INTEGER NOT NULL,
  uploaded_generation INTEGER NOT NULL,
  last_uploaded_at_ms INTEGER NULL,
  last_uploaded_etag TEXT NULL,
  last_uploaded_size_bytes INTEGER NULL
);

CREATE TABLE IF NOT EXISTS schemas (
  stream TEXT PRIMARY KEY,
  schema_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  uploaded_size_bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stream_profiles (
  stream TEXT PRIMARY KEY,
  profile_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS producer_state (
  stream TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  last_seq INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream, producer_id)
);

CREATE TABLE IF NOT EXISTS stream_touch_state (
  stream TEXT PRIMARY KEY,
  processed_through INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

-- Live dynamic template registry (per base stream).
CREATE TABLE IF NOT EXISTS live_templates (
  stream TEXT NOT NULL,
  template_id TEXT NOT NULL,
  entity TEXT NOT NULL,
  fields_json TEXT NOT NULL,
  encodings_json TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  last_seen_at_ms INTEGER NOT NULL,
  inactivity_ttl_ms INTEGER NOT NULL,
  active_from_source_offset INTEGER NOT NULL,
  retired_at_ms INTEGER NULL,
  retired_reason TEXT NULL,
  PRIMARY KEY (stream, template_id)
);

CREATE INDEX IF NOT EXISTS live_templates_stream_entity_state_last_seen_idx
  ON live_templates(stream, entity, state, last_seen_at_ms);
CREATE INDEX IF NOT EXISTS live_templates_stream_state_last_seen_idx
  ON live_templates(stream, state, last_seen_at_ms);
`;

const CREATE_INDEX_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS index_state (
  stream TEXT PRIMARY KEY,
  index_secret BLOB NOT NULL,
  indexed_through INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS index_runs (
  run_id TEXT PRIMARY KEY,
  stream TEXT NOT NULL,
  level INTEGER NOT NULL,
  start_segment INTEGER NOT NULL,
  end_segment INTEGER NOT NULL,
  object_key TEXT NOT NULL,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  filter_len INTEGER NOT NULL,
  record_count INTEGER NOT NULL,
  retired_gen INTEGER NULL,
  retired_at_ms INTEGER NULL
);

CREATE INDEX IF NOT EXISTS index_runs_stream_idx ON index_runs(stream, level, start_segment);
`;

const CREATE_SECONDARY_INDEX_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS secondary_index_state (
  stream TEXT NOT NULL,
  index_name TEXT NOT NULL,
  index_secret BLOB NOT NULL,
  config_hash TEXT NOT NULL,
  indexed_through INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream, index_name)
);

CREATE TABLE IF NOT EXISTS secondary_index_runs (
  run_id TEXT PRIMARY KEY,
  stream TEXT NOT NULL,
  index_name TEXT NOT NULL,
  level INTEGER NOT NULL,
  start_segment INTEGER NOT NULL,
  end_segment INTEGER NOT NULL,
  object_key TEXT NOT NULL,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  filter_len INTEGER NOT NULL,
  record_count INTEGER NOT NULL,
  retired_gen INTEGER NULL,
  retired_at_ms INTEGER NULL
);

CREATE INDEX IF NOT EXISTS secondary_index_runs_stream_idx
  ON secondary_index_runs(stream, index_name, level, start_segment);
`;

const CREATE_LEXICON_INDEX_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS lexicon_index_state (
  stream TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  indexed_through INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream, source_kind, source_name)
);

CREATE TABLE IF NOT EXISTS lexicon_index_runs (
  run_id TEXT PRIMARY KEY,
  stream TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  level INTEGER NOT NULL,
  start_segment INTEGER NOT NULL,
  end_segment INTEGER NOT NULL,
  object_key TEXT NOT NULL,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  record_count INTEGER NOT NULL,
  retired_gen INTEGER NULL,
  retired_at_ms INTEGER NULL
);

CREATE INDEX IF NOT EXISTS lexicon_index_runs_stream_idx
  ON lexicon_index_runs(stream, source_kind, source_name, level, start_segment);
`;

const CREATE_SEARCH_COMPANION_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS search_companion_plans (
  stream TEXT PRIMARY KEY,
  generation INTEGER NOT NULL,
  plan_hash TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS search_segment_companions (
  stream TEXT NOT NULL,
  segment_index INTEGER NOT NULL,
  object_key TEXT NOT NULL,
  plan_generation INTEGER NOT NULL,
  sections_json TEXT NOT NULL,
  section_sizes_json TEXT NOT NULL DEFAULT '{}',
  size_bytes INTEGER NOT NULL DEFAULT 0,
  primary_timestamp_min_ms INTEGER NULL,
  primary_timestamp_max_ms INTEGER NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream, segment_index)
);

CREATE INDEX IF NOT EXISTS search_segment_companions_stream_plan_idx
  ON search_segment_companions(stream, plan_generation, segment_index);
`;

const CREATE_OBJECTSTORE_REQUEST_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS objectstore_request_counts (
  stream_hash TEXT NOT NULL,
  artifact TEXT NOT NULL,
  op TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream_hash, artifact, op)
);

CREATE INDEX IF NOT EXISTS objectstore_request_counts_stream_hash_idx
  ON objectstore_request_counts(stream_hash, updated_at_ms);
`;

const CREATE_ASYNC_INDEX_ACTION_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS async_index_actions (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  stream TEXT NOT NULL,
  action_kind TEXT NOT NULL,
  target_kind TEXT NULL,
  target_name TEXT NULL,
  input_kind TEXT NOT NULL,
  input_count INTEGER NOT NULL DEFAULT 0,
  input_size_bytes INTEGER NOT NULL DEFAULT 0,
  output_count INTEGER NOT NULL DEFAULT 0,
  output_size_bytes INTEGER NOT NULL DEFAULT 0,
  start_segment INTEGER NULL,
  end_segment INTEGER NULL,
  begin_time_ms INTEGER NOT NULL,
  end_time_ms INTEGER NULL,
  duration_ms INTEGER NULL,
  status TEXT NOT NULL,
  error_message TEXT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS async_index_actions_stream_seq_idx
  ON async_index_actions(stream, seq DESC);

CREATE INDEX IF NOT EXISTS async_index_actions_kind_seq_idx
  ON async_index_actions(action_kind, seq DESC);
`;

const CREATE_TABLES_V4_SUFFIX_SQL = (suffix: string): string => `
CREATE TABLE streams_${suffix} (
  stream TEXT PRIMARY KEY,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,

  content_type TEXT NOT NULL,
  stream_seq TEXT NULL,
  closed INTEGER NOT NULL DEFAULT 0,
  closed_producer_id TEXT NULL,
  closed_producer_epoch INTEGER NULL,
  closed_producer_seq INTEGER NULL,
  ttl_seconds INTEGER NULL,

  epoch INTEGER NOT NULL,
  next_offset INTEGER NOT NULL,
  sealed_through INTEGER NOT NULL,
  uploaded_through INTEGER NOT NULL,
  uploaded_segment_count INTEGER NOT NULL DEFAULT 0,

  pending_rows INTEGER NOT NULL,
  pending_bytes INTEGER NOT NULL,
  logical_size_bytes INTEGER NOT NULL DEFAULT 0,

  last_append_ms INTEGER NOT NULL,
  last_segment_cut_ms INTEGER NOT NULL,
  segment_in_progress INTEGER NOT NULL,

  expires_at_ms INTEGER NULL,
  stream_flags INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE wal_${suffix} (
  id INTEGER PRIMARY KEY,
  stream TEXT NOT NULL,
  offset INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  payload BLOB NOT NULL,
  payload_len INTEGER NOT NULL,
  routing_key BLOB NULL,
  content_type TEXT NULL,
  flags INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE segments_${suffix} (
  segment_id TEXT PRIMARY KEY,
  stream TEXT NOT NULL,
  segment_index INTEGER NOT NULL,
  start_offset INTEGER NOT NULL,
  end_offset INTEGER NOT NULL,
  block_count INTEGER NOT NULL,
  last_append_ms INTEGER NOT NULL,
  size_bytes INTEGER NOT NULL,
  local_path TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  uploaded_at_ms INTEGER NULL,
  r2_etag TEXT NULL
);

CREATE TABLE manifests_${suffix} (
  stream TEXT PRIMARY KEY,
  generation INTEGER NOT NULL,
  uploaded_generation INTEGER NOT NULL,
  last_uploaded_at_ms INTEGER NULL,
  last_uploaded_etag TEXT NULL
);

CREATE TABLE schemas_${suffix} (
  stream TEXT PRIMARY KEY,
  schema_json TEXT NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE producer_state_${suffix} (
  stream TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  last_seq INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (stream, producer_id)
);
`;

const CREATE_INDEXES_V4_SQL = `
CREATE UNIQUE INDEX IF NOT EXISTS wal_stream_offset_uniq ON wal(stream, offset);
CREATE INDEX IF NOT EXISTS wal_ts_idx ON wal(ts_ms);

CREATE INDEX IF NOT EXISTS streams_pending_bytes_idx ON streams(pending_bytes);
CREATE INDEX IF NOT EXISTS streams_last_cut_idx ON streams(last_segment_cut_ms);
CREATE INDEX IF NOT EXISTS streams_inprog_pending_idx ON streams(segment_in_progress, pending_bytes, last_segment_cut_ms);

CREATE UNIQUE INDEX IF NOT EXISTS segments_stream_index_uniq ON segments(stream, segment_index);
CREATE INDEX IF NOT EXISTS segments_stream_start_idx ON segments(stream, start_offset);
CREATE INDEX IF NOT EXISTS segments_pending_upload_idx ON segments(uploaded_at_ms);
`;

export function initSchema(db: SqliteDatabase, opts: { skipMigrations?: boolean } = {}): void {
  db.exec(DEFAULT_PRAGMAS_SQL);

  // Some worker processes only need read/write access to existing tables and
  // should avoid concurrent schema init/migration work.
  if (opts.skipMigrations) return;

  db.exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);`);

  const readSchemaVersion = (): number | null => {
    const row = db.query("SELECT version FROM schema_version LIMIT 1;").get() as any;
    if (!row) return null;
    const raw = row.version;
    if (typeof raw === "bigint") return Number(raw);
    if (typeof raw === "number") return raw;
    return Number(raw);
  };

  const version0 = readSchemaVersion();
  if (version0 == null) {
    db.exec(CREATE_TABLES_V4_SQL);
    db.exec(CREATE_INDEX_TABLES_SQL);
    db.exec(CREATE_SECONDARY_INDEX_TABLES_SQL);
    db.exec(CREATE_LEXICON_INDEX_TABLES_SQL);
    db.exec(CREATE_SEARCH_COMPANION_TABLES_SQL);
    db.exec(CREATE_OBJECTSTORE_REQUEST_TABLES_SQL);
    db.exec(CREATE_ASYNC_INDEX_ACTION_TABLES_SQL);
    db.query("INSERT INTO schema_version(version) VALUES (?);").run(SCHEMA_VERSION);
    return;
  }

  if (version0 === SCHEMA_VERSION) return;

  let version = version0;
  while (version !== SCHEMA_VERSION) {
    if (version === 1) {
      migrateV1ToV4(db);
    } else if (version === 2) {
      migrateV2ToV4(db);
    } else if (version === 3) {
      migrateV3ToV4(db);
    } else if (version === 4) {
      migrateV4ToV5(db);
    } else if (version === 5) {
      migrateV5ToV6(db);
    } else if (version === 6) {
      migrateV6ToV7(db);
    } else if (version === 7) {
      migrateV7ToV8(db);
    } else if (version === 8) {
      migrateV8ToV9(db);
    } else if (version === 9) {
      migrateV9ToV10(db);
    } else if (version === 10) {
      migrateV10ToV11(db);
    } else if (version === 11) {
      migrateV11ToV12(db);
    } else if (version === 12) {
      migrateV12ToV13(db);
    } else if (version === 13) {
      migrateV13ToV14(db);
    } else if (version === 14) {
      migrateV14ToV15(db);
    } else if (version === 15) {
      migrateV15ToV16(db);
    } else if (version === 16) {
      migrateV16ToV17(db);
    } else if (version === 17) {
      migrateV17ToV18(db);
    } else if (version === 18) {
      migrateV18ToV19(db);
    } else if (version === 19) {
      migrateV19ToV20(db);
    } else if (version === 20) {
      migrateV20ToV21(db);
    } else if (version === 21) {
      migrateV21ToV22(db);
    } else if (version === 22) {
      migrateV22ToV23(db);
    } else if (version === 23) {
      migrateV23ToV24(db);
    } else if (version === 24) {
      migrateV24ToV25(db);
    } else {
      throw dsError(`unexpected schema version: ${version} (expected ${SCHEMA_VERSION})`);
    }
    const next = readSchemaVersion();
    if (next == null) throw dsError("schema_version row missing after migration");
    version = next;
  }
}

function migrateV1ToV4(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(CREATE_TABLES_V4_SUFFIX_SQL("v4"));

    // Streams
    db.exec(`
      INSERT INTO streams_v4(
        stream, created_at_ms, updated_at_ms,
        content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
        epoch,
        next_offset, sealed_through, uploaded_through,
        pending_rows, pending_bytes,
        last_append_ms, last_segment_cut_ms, segment_in_progress,
        expires_at_ms, stream_flags
      )
      SELECT
        stream,
        CAST(created_at_ns / 1000000 AS INTEGER),
        CAST(updated_at_ns / 1000000 AS INTEGER),
        'application/octet-stream',
        NULL,
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        epoch,
        next_seq,
        sealed_through_seq,
        uploaded_through_seq,
        pending_rows,
        pending_bytes,
        CAST(last_append_ns / 1000000 AS INTEGER),
        CAST(last_segment_cut_ns / 1000000 AS INTEGER),
        segment_in_progress,
        CASE WHEN expires_at_ns IS NULL THEN NULL ELSE CAST(expires_at_ns / 1000000 AS INTEGER) END,
        CASE WHEN deleted != 0 THEN 1 ELSE 0 END
      FROM streams;
    `);

    // WAL
    db.exec(`
      INSERT INTO wal_v4(
        stream, offset, ts_ms, payload, payload_len, routing_key, content_type, flags
      )
      SELECT
        stream,
        seq,
        CAST(append_ns / 1000000 AS INTEGER),
        payload,
        payload_len,
        CASE WHEN routing_key IS NULL THEN NULL ELSE CAST(routing_key AS BLOB) END,
        CASE WHEN is_json != 0 THEN 'application/json' ELSE NULL END,
        0
      FROM wal;
    `);

    // Segments
    db.exec(`
      INSERT INTO segments_v4(
        segment_id, stream, segment_index, start_offset, end_offset, block_count,
        last_append_ms, size_bytes, local_path, created_at_ms, uploaded_at_ms, r2_etag
      )
      SELECT
        segment_id,
        stream,
        segment_index,
        start_seq,
        end_seq,
        block_count,
        CAST(last_append_ns / 1000000 AS INTEGER),
        size_bytes,
        local_path,
        CAST(created_at_ns / 1000000 AS INTEGER),
        CASE WHEN uploaded_at_ns IS NULL THEN NULL ELSE CAST(uploaded_at_ns / 1000000 AS INTEGER) END,
        NULL
      FROM segments;
    `);

    // Manifests
    db.exec(`
      INSERT INTO manifests_v4(
        stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag
      )
      SELECT
        stream,
        generation,
        uploaded_generation,
        CASE WHEN last_uploaded_at_ns IS NULL THEN NULL ELSE CAST(last_uploaded_at_ns / 1000000 AS INTEGER) END,
        last_uploaded_etag
      FROM manifests;
    `);

    // Schemas
    db.exec(`
      INSERT INTO schemas_v4(stream, schema_json, updated_at_ms)
      SELECT stream, schema_json, CAST(updated_at_ns / 1000000 AS INTEGER)
      FROM schemas;
    `);

    db.exec(`DROP TABLE wal;`);
    db.exec(`DROP TABLE streams;`);
    db.exec(`DROP TABLE segments;`);
    db.exec(`DROP TABLE manifests;`);
    db.exec(`DROP TABLE schemas;`);

    db.exec(`ALTER TABLE streams_v4 RENAME TO streams;`);
    db.exec(`ALTER TABLE wal_v4 RENAME TO wal;`);
    db.exec(`ALTER TABLE segments_v4 RENAME TO segments;`);
    db.exec(`ALTER TABLE manifests_v4 RENAME TO manifests;`);
    db.exec(`ALTER TABLE schemas_v4 RENAME TO schemas;`);
    db.exec(`ALTER TABLE producer_state_v4 RENAME TO producer_state;`);

    db.exec(CREATE_INDEXES_V4_SQL);

    db.exec(CREATE_INDEX_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 4;`);
  });

  tx();
}

function migrateV2ToV4(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE segments ADD COLUMN block_count INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE segments ADD COLUMN last_append_ms INTEGER NOT NULL DEFAULT 0;`);

    db.exec(`ALTER TABLE streams ADD COLUMN content_type TEXT NOT NULL DEFAULT 'application/octet-stream';`);
    db.exec(`ALTER TABLE streams ADD COLUMN stream_seq TEXT NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed_producer_id TEXT NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed_producer_epoch INTEGER NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed_producer_seq INTEGER NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN ttl_seconds INTEGER NULL;`);

    db.exec(`
      CREATE TABLE IF NOT EXISTS producer_state (
        stream TEXT NOT NULL,
        producer_id TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        last_seq INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (stream, producer_id)
      );
    `);
    db.exec(CREATE_INDEX_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 4;`);
  });

  tx();
}

function migrateV3ToV4(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE streams ADD COLUMN content_type TEXT NOT NULL DEFAULT 'application/octet-stream';`);
    db.exec(`ALTER TABLE streams ADD COLUMN stream_seq TEXT NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed_producer_id TEXT NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed_producer_epoch INTEGER NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN closed_producer_seq INTEGER NULL;`);
    db.exec(`ALTER TABLE streams ADD COLUMN ttl_seconds INTEGER NULL;`);

    db.exec(`
      CREATE TABLE IF NOT EXISTS producer_state (
        stream TEXT NOT NULL,
        producer_id TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        last_seq INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (stream, producer_id)
      );
    `);
    db.exec(CREATE_INDEX_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 4;`);
  });

  tx();
}

function migrateV4ToV5(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(CREATE_INDEX_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 5;`);
  });
  tx();
}

function migrateV5ToV6(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE streams ADD COLUMN uploaded_segment_count INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`
      CREATE TABLE IF NOT EXISTS stream_segment_meta (
        stream TEXT PRIMARY KEY,
        segment_count INTEGER NOT NULL,
        segment_offsets BLOB NOT NULL,
        segment_blocks BLOB NOT NULL,
        segment_last_ts BLOB NOT NULL
      );
    `);
    db.exec(`UPDATE schema_version SET version = 6;`);
  });
  tx();
}

function migrateV6ToV7(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS stream_touch_state (
        stream TEXT PRIMARY KEY,
        processed_through INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
    `);
    db.exec(`UPDATE schema_version SET version = 7;`);
  });
  tx();
}

function migrateV7ToV8(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`UPDATE schema_version SET version = 8;`);
  });
  tx();
}

function migrateV8ToV9(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS live_templates (
        stream TEXT NOT NULL,
        template_id TEXT NOT NULL,
        entity TEXT NOT NULL,
        fields_json TEXT NOT NULL,
        encodings_json TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        last_seen_at_ms INTEGER NOT NULL,
        inactivity_ttl_ms INTEGER NOT NULL,
        active_from_source_offset INTEGER NOT NULL,
        retired_at_ms INTEGER NULL,
        retired_reason TEXT NULL,
        PRIMARY KEY (stream, template_id)
      );
    `);
    db.exec(`
      CREATE INDEX IF NOT EXISTS live_templates_stream_entity_state_last_seen_idx
        ON live_templates(stream, entity, state, last_seen_at_ms);
    `);
    db.exec(`
      CREATE INDEX IF NOT EXISTS live_templates_stream_state_last_seen_idx
        ON live_templates(stream, state, last_seen_at_ms);
    `);
    db.exec(`UPDATE schema_version SET version = 9;`);
  });
  tx();
}

function migrateV9ToV10(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE streams ADD COLUMN wal_rows INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE streams ADD COLUMN wal_bytes INTEGER NOT NULL DEFAULT 0;`);

    // Backfill current retained WAL rows/bytes per stream so metrics are correct after upgrade.
    db.exec(`DROP TABLE IF EXISTS temp.wal_stats;`);
    db.exec(`
      CREATE TEMP TABLE wal_stats AS
      SELECT stream, COUNT(*) as rows, COALESCE(SUM(payload_len), 0) as bytes
      FROM wal
      GROUP BY stream;
    `);
    db.exec(`
      UPDATE streams
      SET wal_rows = COALESCE((SELECT rows FROM wal_stats WHERE wal_stats.stream = streams.stream), 0),
          wal_bytes = COALESCE((SELECT bytes FROM wal_stats WHERE wal_stats.stream = streams.stream), 0);
    `);
    db.exec(`DROP TABLE wal_stats;`);

    db.exec(`UPDATE schema_version SET version = 10;`);
  });
  tx();
}

function migrateV10ToV11(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`DROP INDEX IF EXISTS wal_touch_stream_rk_offset_idx;`);
    db.exec(`UPDATE schema_version SET version = 11;`);
  });
  tx();
}

function migrateV11ToV12(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE streams ADD COLUMN profile TEXT NULL;`);
    db.exec(`UPDATE schema_version SET version = 12;`);
  });
  tx();
}

function migrateV12ToV13(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS stream_profiles (
        stream TEXT PRIMARY KEY,
        profile_json TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
    `);
    db.exec(`UPDATE schema_version SET version = 13;`);
  });
  tx();
}

function migrateV13ToV14(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS stream_touch_state (
        stream TEXT PRIMARY KEY,
        processed_through INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
    `);

    const hasLegacy = !!db
      .query(`SELECT name FROM sqlite_master WHERE type='table' AND name='stream_interpreters' LIMIT 1;`)
      .get();
    if (hasLegacy) {
      db.exec(`
        INSERT OR REPLACE INTO stream_touch_state(stream, processed_through, updated_at_ms)
        SELECT stream, interpreted_through, updated_at_ms
        FROM stream_interpreters;
      `);
      db.exec(`DROP TABLE stream_interpreters;`);
    }

    db.exec(`UPDATE schema_version SET version = ${SCHEMA_VERSION};`);
  });
  tx();
}

function migrateV14ToV15(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(CREATE_SECONDARY_INDEX_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 15;`);
  });
  tx();
}

function migrateV15ToV16(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`UPDATE schema_version SET version = 16;`);
  });
  tx();
}

function migrateV16ToV17(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE streams ADD COLUMN logical_size_bytes INTEGER NOT NULL DEFAULT 0;`);

    // Streams that still live entirely in the retained WAL can be backfilled
    // cheaply here. Streams with published segments are repaired asynchronously
    // at runtime from segment objects if this value is still missing.
    db.exec(`
      UPDATE streams
      SET logical_size_bytes = wal_bytes
      WHERE next_offset = wal_rows;
    `);

    db.exec(`UPDATE schema_version SET version = 17;`);
  });
  tx();
}

function migrateV17ToV18(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(CREATE_SEARCH_COMPANION_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 18;`);
  });
  tx();
}

function migrateV18ToV19(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE secondary_index_state ADD COLUMN config_hash TEXT NOT NULL DEFAULT '';`);
    db.exec(`DROP INDEX IF EXISTS search_family_segments_stream_idx;`);
    db.exec(`DROP TABLE IF EXISTS search_family_segments;`);
    db.exec(`DROP TABLE IF EXISTS search_family_state;`);
    db.exec(`UPDATE schema_version SET version = 19;`);
  });
  tx();
}

function migrateV19ToV20(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE manifests ADD COLUMN last_uploaded_size_bytes INTEGER NULL;`);
    db.exec(`ALTER TABLE schemas ADD COLUMN uploaded_size_bytes INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE index_runs ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE secondary_index_runs ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0;`);
    db.exec(`ALTER TABLE search_segment_companions ADD COLUMN section_sizes_json TEXT NOT NULL DEFAULT '{}';`);
    db.exec(`ALTER TABLE search_segment_companions ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0;`);
    db.exec(CREATE_OBJECTSTORE_REQUEST_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 20;`);
  });
  tx();
}

function migrateV20ToV21(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`ALTER TABLE search_segment_companions ADD COLUMN primary_timestamp_min_ms INTEGER NULL;`);
    db.exec(`ALTER TABLE search_segment_companions ADD COLUMN primary_timestamp_max_ms INTEGER NULL;`);
    db.exec(`UPDATE schema_version SET version = 21;`);
  });
  tx();
}

function migrateV21ToV22(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(CREATE_LEXICON_INDEX_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 22;`);
  });
  tx();
}

function migrateV22ToV23(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(`DROP INDEX IF EXISTS wal_stream_offset_idx;`);
    db.exec(`UPDATE schema_version SET version = 23;`);
  });
  tx();
}

function migrateV23ToV24(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    const hasPayloadBytes = db
      .query(`PRAGMA table_info(segments);`)
      .all()
      .some((row: any) => String(row.name) === "payload_bytes");
    if (!hasPayloadBytes) {
      db.exec(`ALTER TABLE segments ADD COLUMN payload_bytes INTEGER NOT NULL DEFAULT 0;`);
    }
    db.exec(`UPDATE schema_version SET version = 24;`);
  });
  tx();
}

function migrateV24ToV25(db: SqliteDatabase): void {
  const tx = db.transaction(() => {
    db.exec(CREATE_ASYNC_INDEX_ACTION_TABLES_SQL);
    db.exec(`UPDATE schema_version SET version = 25;`);
  });
  tx();
}
