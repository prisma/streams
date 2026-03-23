import { initSchema } from "./schema.ts";
import { openSqliteDatabase, type SqliteDatabase, type SqliteStatement } from "../sqlite/adapter.ts";
import { Result } from "better-result";

export const STREAM_FLAG_DELETED = 1 << 0;
// Internal companion touch stream. Hidden from listing and not eligible for segmentation.
export const STREAM_FLAG_TOUCH = 1 << 1;

const BASE_WAL_GC_CHUNK_OFFSETS = (() => {
  const raw = process.env.DS_BASE_WAL_GC_CHUNK_OFFSETS;
  if (raw == null || raw.trim() === "") return 100_000;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return 100_000;
  return Math.floor(n);
})();

export type StreamRow = {
  stream: string;
  created_at_ms: bigint;
  updated_at_ms: bigint;
  content_type: string;
  stream_seq: string | null;
  closed: number;
  closed_producer_id: string | null;
  closed_producer_epoch: number | null;
  closed_producer_seq: number | null;
  ttl_seconds: number | null;
  epoch: number;
  next_offset: bigint;
  sealed_through: bigint;
  uploaded_through: bigint;
  uploaded_segment_count: number;
  pending_rows: bigint;
  pending_bytes: bigint;
  wal_rows: bigint;
  wal_bytes: bigint;
  last_append_ms: bigint;
  last_segment_cut_ms: bigint;
  segment_in_progress: number;
  expires_at_ms: bigint | null;
  stream_flags: number;
};

export type SegmentRow = {
  segment_id: string;
  stream: string;
  segment_index: number;
  start_offset: bigint;
  end_offset: bigint;
  block_count: number;
  last_append_ms: bigint;
  size_bytes: number;
  local_path: string;
  created_at_ms: bigint;
  uploaded_at_ms: bigint | null;
  r2_etag: string | null;
};

export type SegmentMetaRow = {
  stream: string;
  segment_count: number;
  segment_offsets: Uint8Array;
  segment_blocks: Uint8Array;
  segment_last_ts: Uint8Array;
};

export type IndexStateRow = {
  stream: string;
  index_secret: Uint8Array;
  indexed_through: number;
  updated_at_ms: bigint;
};

export type IndexRunRow = {
  run_id: string;
  stream: string;
  level: number;
  start_segment: number;
  end_segment: number;
  object_key: string;
  filter_len: number;
  record_count: number;
  retired_gen: number | null;
  retired_at_ms: bigint | null;
};

export class SqliteDurableStore {
  public readonly db: SqliteDatabase;
  private dbstatReady: boolean | null = null;

  // Prepared statements.
  private readonly stmts: {
    getStream: SqliteStatement;
    upsertStream: SqliteStatement;
    listStreams: SqliteStatement;
    setDeleted: SqliteStatement;

    insertWal: SqliteStatement;

    updateStreamAppend: SqliteStatement;
    updateStreamAppendSeqCheck: SqliteStatement;

    candidateStreams: SqliteStatement;
    candidateStreamsNoInterval: SqliteStatement;
    listExpiredStreams: SqliteStatement;

    streamWalRange: SqliteStatement;
    streamWalRangeByKey: SqliteStatement;

    createSegment: SqliteStatement;
    listSegmentsForStream: SqliteStatement;
    getSegmentByIndex: SqliteStatement;
    findSegmentForOffset: SqliteStatement;
    nextSegmentIndex: SqliteStatement;
    markSegmentUploaded: SqliteStatement;
    pendingUploadSegments: SqliteStatement;
    countPendingSegments: SqliteStatement;
    tryClaimSegment: SqliteStatement;
    countSegmentsForStream: SqliteStatement;

    getManifest: SqliteStatement;
    upsertManifest: SqliteStatement;

    getIndexState: SqliteStatement;
    upsertIndexState: SqliteStatement;
    updateIndexedThrough: SqliteStatement;
    listIndexRuns: SqliteStatement;
    listIndexRunsAll: SqliteStatement;
    listRetiredIndexRuns: SqliteStatement;
    insertIndexRun: SqliteStatement;
    retireIndexRun: SqliteStatement;
    deleteIndexRun: SqliteStatement;
    countUploadedSegments: SqliteStatement;
    getSegmentMeta: SqliteStatement;
    ensureSegmentMeta: SqliteStatement;
    appendSegmentMeta: SqliteStatement;
    upsertSegmentMeta: SqliteStatement;
    setUploadedSegmentCount: SqliteStatement;

    advanceUploadedThrough: SqliteStatement;
    deleteWalBeforeOffset: SqliteStatement;

    getSchemaRegistry: SqliteStatement;
    upsertSchemaRegistry: SqliteStatement;
    getStreamInterpreter: SqliteStatement;
    upsertStreamInterpreter: SqliteStatement;
    deleteStreamInterpreter: SqliteStatement;
    listStreamInterpreters: SqliteStatement;
    countStreams: SqliteStatement;
    sumPendingBytes: SqliteStatement;
    sumPendingSegmentBytes: SqliteStatement;
  };

  constructor(path: string, opts: { cacheBytes?: number; skipMigrations?: boolean } = {}) {
    this.db = openSqliteDatabase(path);
    initSchema(this.db, { skipMigrations: opts.skipMigrations });
    if (opts.cacheBytes && opts.cacheBytes > 0) {
      const kb = Math.max(1, Math.floor(opts.cacheBytes / 1024));
      this.db.exec(`PRAGMA cache_size = -${kb};`);
    }

    this.stmts = {
      getStream: this.db.query(
        `SELECT stream, created_at_ms, updated_at_ms,
                content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
                epoch, next_offset, sealed_through, uploaded_through, uploaded_segment_count,
                pending_rows, pending_bytes, wal_rows, wal_bytes, last_append_ms, last_segment_cut_ms, segment_in_progress,
                expires_at_ms, stream_flags
         FROM streams WHERE stream = ? LIMIT 1;`
      ),
      upsertStream: this.db.query(
        `INSERT INTO streams(stream, created_at_ms, updated_at_ms,
                             content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
                             epoch, next_offset, sealed_through, uploaded_through, uploaded_segment_count,
                             pending_rows, pending_bytes, wal_rows, wal_bytes, last_append_ms, last_segment_cut_ms, segment_in_progress,
                             expires_at_ms, stream_flags)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET
           updated_at_ms=excluded.updated_at_ms,
           expires_at_ms=excluded.expires_at_ms,
           ttl_seconds=excluded.ttl_seconds,
           content_type=excluded.content_type,
           stream_flags=excluded.stream_flags;`
      ),
      listStreams: this.db.query(
        `SELECT stream, created_at_ms, updated_at_ms,
                content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
                epoch, next_offset, sealed_through, uploaded_through, uploaded_segment_count,
                pending_rows, pending_bytes, wal_rows, wal_bytes, last_append_ms, last_segment_cut_ms, segment_in_progress,
                expires_at_ms, stream_flags
         FROM streams
         WHERE (stream_flags & ?) = 0
           AND (expires_at_ms IS NULL OR expires_at_ms > ?)
         ORDER BY stream
         LIMIT ? OFFSET ?;`
      ),
      setDeleted: this.db.query(`UPDATE streams SET stream_flags = (stream_flags | ?), updated_at_ms=? WHERE stream=?;`),

      insertWal: this.db.query(
        `INSERT INTO wal(stream, offset, ts_ms, payload, payload_len, routing_key, content_type, flags)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?);`
      ),

      updateStreamAppend: this.db.query(
        `UPDATE streams
         SET next_offset = ?, updated_at_ms = ?, last_append_ms = ?,
             pending_rows = pending_rows + ?, pending_bytes = pending_bytes + ?,
             wal_rows = wal_rows + ?, wal_bytes = wal_bytes + ?
         WHERE stream = ? AND (stream_flags & ?) = 0;`
      ),
      updateStreamAppendSeqCheck: this.db.query(
        `UPDATE streams
         SET next_offset = ?, updated_at_ms = ?, last_append_ms = ?,
             pending_rows = pending_rows + ?, pending_bytes = pending_bytes + ?,
             wal_rows = wal_rows + ?, wal_bytes = wal_bytes + ?
         WHERE stream = ? AND (stream_flags & ?) = 0 AND next_offset = ?;`
      ),

      candidateStreams: this.db.query(
        `SELECT stream, pending_bytes, pending_rows, last_segment_cut_ms, sealed_through, next_offset, epoch
         FROM streams
         WHERE (stream_flags & ?) = 0
           AND segment_in_progress = 0
           AND (pending_bytes >= ? OR pending_rows >= ? OR (? - last_segment_cut_ms) >= ?)
         ORDER BY pending_bytes DESC
         LIMIT ?;`
      ),
      candidateStreamsNoInterval: this.db.query(
        `SELECT stream, pending_bytes, pending_rows, last_segment_cut_ms, sealed_through, next_offset, epoch
         FROM streams
         WHERE (stream_flags & ?) = 0
           AND segment_in_progress = 0
           AND (pending_bytes >= ? OR pending_rows >= ?)
         ORDER BY pending_bytes DESC
         LIMIT ?;`
      ),
      listExpiredStreams: this.db.query(
        `SELECT stream
         FROM streams
         WHERE (stream_flags & ?) = 0
           AND expires_at_ms IS NOT NULL
           AND expires_at_ms <= ?
         ORDER BY expires_at_ms ASC
         LIMIT ?;`
      ),

      streamWalRange: this.db.query(
        `SELECT offset, ts_ms, routing_key, content_type, payload
         FROM wal
         WHERE stream = ? AND offset >= ? AND offset <= ?
         ORDER BY offset ASC;`
      ),
      streamWalRangeByKey: this.db.query(
        `SELECT offset, ts_ms, routing_key, content_type, payload
         FROM wal
         WHERE stream = ? AND offset >= ? AND offset <= ? AND routing_key = ?
         ORDER BY offset ASC;`
      ),

      createSegment: this.db.query(
        `INSERT INTO segments(segment_id, stream, segment_index, start_offset, end_offset, block_count,
                              last_append_ms, size_bytes, local_path, created_at_ms, uploaded_at_ms, r2_etag)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL);`
      ),
      listSegmentsForStream: this.db.query(
        `SELECT segment_id, stream, segment_index, start_offset, end_offset, block_count, last_append_ms, size_bytes,
                local_path, created_at_ms, uploaded_at_ms, r2_etag
         FROM segments WHERE stream=? ORDER BY segment_index ASC;`
      ),
      getSegmentByIndex: this.db.query(
        `SELECT segment_id, stream, segment_index, start_offset, end_offset, block_count, last_append_ms, size_bytes,
                local_path, created_at_ms, uploaded_at_ms, r2_etag
         FROM segments WHERE stream=? AND segment_index=? LIMIT 1;`
      ),
      findSegmentForOffset: this.db.query(
        `SELECT segment_id, stream, segment_index, start_offset, end_offset, block_count, last_append_ms, size_bytes,
                local_path, created_at_ms, uploaded_at_ms, r2_etag
         FROM segments
         WHERE stream=? AND start_offset <= ? AND end_offset >= ?
         ORDER BY segment_index DESC
         LIMIT 1;`
      ),
      nextSegmentIndex: this.db.query(
        `SELECT COALESCE(MAX(segment_index)+1, 0) as next_idx FROM segments WHERE stream=?;`
      ),
      markSegmentUploaded: this.db.query(
        `UPDATE segments SET r2_etag=?, uploaded_at_ms=? WHERE segment_id=?;`
      ),
      pendingUploadSegments: this.db.query(
        `SELECT segment_id, stream, segment_index, start_offset, end_offset, block_count, last_append_ms, size_bytes,
                local_path, created_at_ms, uploaded_at_ms, r2_etag
         FROM segments WHERE uploaded_at_ms IS NULL ORDER BY created_at_ms ASC LIMIT ?;`
      ),
      countPendingSegments: this.db.query(`SELECT COUNT(*) as cnt FROM segments WHERE uploaded_at_ms IS NULL;`),
      countSegmentsForStream: this.db.query(`SELECT COUNT(*) as cnt FROM segments WHERE stream=?;`),
      tryClaimSegment: this.db.query(
        `UPDATE streams SET segment_in_progress=1, updated_at_ms=? WHERE stream=? AND segment_in_progress=0;`
      ),

      getManifest: this.db.query(`SELECT stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag FROM manifests WHERE stream=? LIMIT 1;`),
      upsertManifest: this.db.query(
        `INSERT INTO manifests(stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag)
         VALUES(?, ?, ?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET
           generation=excluded.generation,
           uploaded_generation=excluded.uploaded_generation,
           last_uploaded_at_ms=excluded.last_uploaded_at_ms,
           last_uploaded_etag=excluded.last_uploaded_etag;`
      ),

      getIndexState: this.db.query(
        `SELECT stream, index_secret, indexed_through, updated_at_ms
         FROM index_state WHERE stream=? LIMIT 1;`
      ),
      upsertIndexState: this.db.query(
        `INSERT INTO index_state(stream, index_secret, indexed_through, updated_at_ms)
         VALUES(?, ?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET
           index_secret=excluded.index_secret,
           indexed_through=excluded.indexed_through,
           updated_at_ms=excluded.updated_at_ms;`
      ),
      updateIndexedThrough: this.db.query(
        `UPDATE index_state SET indexed_through=?, updated_at_ms=? WHERE stream=?;`
      ),
      listIndexRuns: this.db.query(
        `SELECT run_id, stream, level, start_segment, end_segment, object_key, filter_len, record_count, retired_gen, retired_at_ms
         FROM index_runs WHERE stream=? AND retired_gen IS NULL
         ORDER BY start_segment ASC, level ASC;`
      ),
      listIndexRunsAll: this.db.query(
        `SELECT run_id, stream, level, start_segment, end_segment, object_key, filter_len, record_count, retired_gen, retired_at_ms
         FROM index_runs WHERE stream=?
         ORDER BY start_segment ASC, level ASC;`
      ),
      listRetiredIndexRuns: this.db.query(
        `SELECT run_id, stream, level, start_segment, end_segment, object_key, filter_len, record_count, retired_gen, retired_at_ms
         FROM index_runs WHERE stream=? AND retired_gen IS NOT NULL
         ORDER BY retired_at_ms ASC;`
      ),
      insertIndexRun: this.db.query(
        `INSERT OR IGNORE INTO index_runs(run_id, stream, level, start_segment, end_segment, object_key, filter_len, record_count, retired_gen, retired_at_ms)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL);`
      ),
      retireIndexRun: this.db.query(
        `UPDATE index_runs SET retired_gen=?, retired_at_ms=? WHERE run_id=?;`
      ),
      deleteIndexRun: this.db.query(
        `DELETE FROM index_runs WHERE run_id=?;`
      ),
      countUploadedSegments: this.db.query(
        `SELECT COALESCE(MAX(segment_index), -1) as max_idx
         FROM segments WHERE stream=? AND r2_etag IS NOT NULL;`
      ),
      getSegmentMeta: this.db.query(
        `SELECT stream, segment_count, segment_offsets, segment_blocks, segment_last_ts
         FROM stream_segment_meta WHERE stream=? LIMIT 1;`
      ),
      ensureSegmentMeta: this.db.query(
        `INSERT INTO stream_segment_meta(stream, segment_count, segment_offsets, segment_blocks, segment_last_ts)
         VALUES(?, 0, x'', x'', x'')
         ON CONFLICT(stream) DO NOTHING;`
      ),
      appendSegmentMeta: this.db.query(
        `UPDATE stream_segment_meta
         SET segment_count = segment_count + 1,
             segment_offsets = segment_offsets || ?,
             segment_blocks = segment_blocks || ?,
             segment_last_ts = segment_last_ts || ?
         WHERE stream = ?;`
      ),
      upsertSegmentMeta: this.db.query(
        `INSERT INTO stream_segment_meta(stream, segment_count, segment_offsets, segment_blocks, segment_last_ts)
         VALUES(?, ?, ?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET
           segment_count=excluded.segment_count,
           segment_offsets=excluded.segment_offsets,
           segment_blocks=excluded.segment_blocks,
           segment_last_ts=excluded.segment_last_ts;`
      ),
      setUploadedSegmentCount: this.db.query(
        `UPDATE streams SET uploaded_segment_count=?, updated_at_ms=? WHERE stream=?;`
      ),

      advanceUploadedThrough: this.db.query(
        `UPDATE streams SET uploaded_through=?, updated_at_ms=? WHERE stream=?;`
      ),
      deleteWalBeforeOffset: this.db.query(
        `DELETE FROM wal WHERE stream=? AND offset <= ?;`
      ),

      getSchemaRegistry: this.db.query(`SELECT stream, schema_json, updated_at_ms FROM schemas WHERE stream=? LIMIT 1;`),
      upsertSchemaRegistry: this.db.query(
        `INSERT INTO schemas(stream, schema_json, updated_at_ms) VALUES(?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET schema_json=excluded.schema_json, updated_at_ms=excluded.updated_at_ms;`
      ),
      getStreamInterpreter: this.db.query(
        `SELECT stream, interpreted_through, updated_at_ms
         FROM stream_interpreters WHERE stream=? LIMIT 1;`
      ),
      upsertStreamInterpreter: this.db.query(
        `INSERT INTO stream_interpreters(stream, interpreted_through, updated_at_ms)
         VALUES(?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET
           interpreted_through=excluded.interpreted_through,
           updated_at_ms=excluded.updated_at_ms;`
      ),
      deleteStreamInterpreter: this.db.query(`DELETE FROM stream_interpreters WHERE stream=?;`),
      listStreamInterpreters: this.db.query(
        `SELECT stream, interpreted_through, updated_at_ms
         FROM stream_interpreters
         ORDER BY stream ASC;`
      ),
      countStreams: this.db.query(`SELECT COUNT(*) as cnt FROM streams WHERE (stream_flags & ?) = 0;`),
      sumPendingBytes: this.db.query(`SELECT COALESCE(SUM(pending_bytes), 0) as total FROM streams;`),
      sumPendingSegmentBytes: this.db.query(`SELECT COALESCE(SUM(size_bytes), 0) as total FROM segments WHERE uploaded_at_ms IS NULL;`),
    };
  }

  private toBigInt(v: any): bigint {
    return typeof v === "bigint" ? v : BigInt(v);
  }

  private bindInt(v: bigint): number | string {
    const max = BigInt(Number.MAX_SAFE_INTEGER);
    const min = BigInt(Number.MIN_SAFE_INTEGER);
    if (v <= max && v >= min) return Number(v);
    return v.toString();
  }

  private encodeU64Le(value: bigint): Uint8Array {
    const buf = new Uint8Array(8);
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    dv.setBigUint64(0, value, true);
    return buf;
  }

  private encodeU32Le(value: number): Uint8Array {
    const buf = new Uint8Array(4);
    const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    dv.setUint32(0, value >>> 0, true);
    return buf;
  }

  private coerceStreamRow(row: any): StreamRow {
    return {
      stream: String(row.stream),
      created_at_ms: this.toBigInt(row.created_at_ms),
      updated_at_ms: this.toBigInt(row.updated_at_ms),
      content_type: String(row.content_type),
      stream_seq: row.stream_seq == null ? null : String(row.stream_seq),
      closed: Number(row.closed),
      closed_producer_id: row.closed_producer_id == null ? null : String(row.closed_producer_id),
      closed_producer_epoch: row.closed_producer_epoch == null ? null : Number(row.closed_producer_epoch),
      closed_producer_seq: row.closed_producer_seq == null ? null : Number(row.closed_producer_seq),
      ttl_seconds: row.ttl_seconds == null ? null : Number(row.ttl_seconds),
      epoch: Number(row.epoch),
      next_offset: this.toBigInt(row.next_offset),
      sealed_through: this.toBigInt(row.sealed_through),
      uploaded_through: this.toBigInt(row.uploaded_through),
      uploaded_segment_count: Number(row.uploaded_segment_count ?? 0),
      pending_rows: this.toBigInt(row.pending_rows),
      pending_bytes: this.toBigInt(row.pending_bytes),
      wal_rows: this.toBigInt(row.wal_rows ?? 0),
      wal_bytes: this.toBigInt(row.wal_bytes ?? 0),
      last_append_ms: this.toBigInt(row.last_append_ms),
      last_segment_cut_ms: this.toBigInt(row.last_segment_cut_ms),
      segment_in_progress: Number(row.segment_in_progress),
      expires_at_ms: row.expires_at_ms == null ? null : this.toBigInt(row.expires_at_ms),
      stream_flags: Number(row.stream_flags),
    };
  }

  private coerceSegmentRow(row: any): SegmentRow {
    return {
      segment_id: String(row.segment_id),
      stream: String(row.stream),
      segment_index: Number(row.segment_index),
      start_offset: this.toBigInt(row.start_offset),
      end_offset: this.toBigInt(row.end_offset),
      block_count: Number(row.block_count),
      last_append_ms: this.toBigInt(row.last_append_ms),
      size_bytes: Number(row.size_bytes),
      local_path: String(row.local_path),
      created_at_ms: this.toBigInt(row.created_at_ms),
      uploaded_at_ms: row.uploaded_at_ms == null ? null : this.toBigInt(row.uploaded_at_ms),
      r2_etag: row.r2_etag == null ? null : String(row.r2_etag),
    };
  }

  close(): void {
    this.db.close();
  }

  nowMs(): bigint {
    return BigInt(Date.now());
  }

  isDeleted(row: StreamRow): boolean {
    return (row.stream_flags & STREAM_FLAG_DELETED) !== 0;
  }

  getStream(stream: string): StreamRow | null {
    const row = this.stmts.getStream.get(stream) as any;
    return row ? this.coerceStreamRow(row) : null;
  }

  ensureStream(
    stream: string,
    opts?: {
      contentType?: string;
      expiresAtMs?: bigint | null;
      ttlSeconds?: number | null;
      closed?: boolean;
      closedProducer?: { id: string; epoch: number; seq: number } | null;
      streamFlags?: number;
    }
  ): StreamRow {
    const existing = this.getStream(stream);
    if (existing) return existing;

    const now = this.nowMs();
    const epoch = 0;
    const nextOffset = 0n;
    const contentType = opts?.contentType ?? "application/octet-stream";
    const closed = opts?.closed ? 1 : 0;
    const closedProducer = opts?.closedProducer ?? null;
    const expiresAtMs = opts?.expiresAtMs ?? null;
    const ttlSeconds = opts?.ttlSeconds ?? null;
    const streamFlags = opts?.streamFlags ?? 0;

    this.db
      .query(
        `INSERT INTO streams(
          stream, created_at_ms, updated_at_ms,
          content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
          epoch, next_offset, sealed_through, uploaded_through, uploaded_segment_count,
          pending_rows, pending_bytes, last_append_ms, last_segment_cut_ms, segment_in_progress,
          expires_at_ms, stream_flags
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
      )
      .run(
        stream,
        now,
        now,
        contentType,
        null,
        closed,
        closedProducer ? closedProducer.id : null,
        closedProducer ? closedProducer.epoch : null,
        closedProducer ? closedProducer.seq : null,
        ttlSeconds,
        epoch,
        nextOffset,
        -1n,
        -1n,
        0,
        0n,
        0n,
        now,
        now,
        0,
        expiresAtMs,
        streamFlags
      );

    this.stmts.upsertManifest.run(stream, 0, 0, null, null);
    this.ensureSegmentMeta(stream);
    return this.getStream(stream)!;
  }

  restoreStreamRow(row: StreamRow): void {
    this.stmts.upsertStream.run(
      row.stream,
      row.created_at_ms,
      row.updated_at_ms,
      row.content_type,
      row.stream_seq,
      row.closed,
      row.closed_producer_id,
      row.closed_producer_epoch,
      row.closed_producer_seq,
      row.ttl_seconds,
      row.epoch,
      row.next_offset,
      row.sealed_through,
      row.uploaded_through,
      row.uploaded_segment_count,
      row.pending_rows,
      row.pending_bytes,
      row.wal_rows,
      row.wal_bytes,
      row.last_append_ms,
      row.last_segment_cut_ms,
      row.segment_in_progress,
      row.expires_at_ms,
      row.stream_flags
    );
  }

  listStreams(limit: number, offset: number): StreamRow[] {
    const now = this.nowMs();
    const rows = this.stmts.listStreams.all(STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH, now, limit, offset) as any[];
    return rows.map((r) => this.coerceStreamRow(r));
  }

  listExpiredStreams(limit: number): string[] {
    const now = this.nowMs();
    const rows = this.stmts.listExpiredStreams.all(STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH, now, limit) as any[];
    return rows.map((r) => String(r.stream));
  }

  deleteStream(stream: string): boolean {
    const existing = this.getStream(stream);
    if (!existing) return false;
    const now = this.nowMs();
    this.stmts.setDeleted.run(STREAM_FLAG_DELETED, now, stream);
    return true;
  }

  hardDeleteStream(stream: string): boolean {
    const tx = this.db.transaction(() => {
      const existing = this.getStream(stream);
      if (!existing) return false;
      this.db.query(`DELETE FROM wal WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM segments WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM manifests WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM schemas WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM stream_interpreters WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM live_templates WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM producer_state WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM index_state WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM index_runs WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM stream_segment_meta WHERE stream=?;`).run(stream);
      this.db.query(`DELETE FROM streams WHERE stream=?;`).run(stream);
      return true;
    });
    return tx();
  }

  getSchemaRegistry(stream: string): { stream: string; registry_json: string; updated_at_ms: bigint } | null {
    const row = this.stmts.getSchemaRegistry.get(stream) as any;
    if (!row) return null;
    return { stream: String(row.stream), registry_json: String(row.schema_json), updated_at_ms: this.toBigInt(row.updated_at_ms) };
  }

  upsertSchemaRegistry(stream: string, registryJson: string): void {
    this.stmts.upsertSchemaRegistry.run(stream, registryJson, this.nowMs());
  }

  getStreamInterpreter(stream: string): { stream: string; interpreted_through: bigint; updated_at_ms: bigint } | null {
    const row = this.stmts.getStreamInterpreter.get(stream) as any;
    if (!row) return null;
    return {
      stream: String(row.stream),
      interpreted_through: this.toBigInt(row.interpreted_through),
      updated_at_ms: this.toBigInt(row.updated_at_ms),
    };
  }

  listStreamInterpreters(): Array<{ stream: string; interpreted_through: bigint; updated_at_ms: bigint }> {
    const rows = this.stmts.listStreamInterpreters.all() as any[];
    return rows.map((row) => ({
      stream: String(row.stream),
      interpreted_through: this.toBigInt(row.interpreted_through),
      updated_at_ms: this.toBigInt(row.updated_at_ms),
    }));
  }

  ensureStreamInterpreter(stream: string): void {
    const existing = this.getStreamInterpreter(stream);
    if (existing) return;
    const srow = this.getStream(stream);
    const initialThrough = srow ? srow.next_offset - 1n : -1n;
    this.stmts.upsertStreamInterpreter.run(stream, this.bindInt(initialThrough), this.nowMs());
  }

  updateStreamInterpreterThrough(stream: string, interpretedThrough: bigint): void {
    this.stmts.upsertStreamInterpreter.run(stream, this.bindInt(interpretedThrough), this.nowMs());
  }

  deleteStreamInterpreter(stream: string): void {
    this.stmts.deleteStreamInterpreter.run(stream);
  }

  addStreamFlags(stream: string, flags: number): void {
    if (!Number.isFinite(flags) || flags <= 0) return;
    this.db.query(`UPDATE streams SET stream_flags = (stream_flags | ?), updated_at_ms=? WHERE stream=?;`).run(flags, this.nowMs(), stream);
  }

  getWalOldestOffset(stream: string): bigint | null {
    const row = this.db.query(`SELECT MIN(offset) as min_off FROM wal WHERE stream=?;`).get(stream) as any;
    if (!row || row.min_off == null) return null;
    return this.toBigInt(row.min_off);
  }

  /**
   * Trim a WAL-only stream by age (in ms), leaving at least 1 record if the stream is non-empty.
   *
   * This is primarily intended for internal companion touch streams which are not segmented/uploaded.
   */
  trimWalByAge(stream: string, maxAgeMs: number): { trimmedRows: number; trimmedBytes: number; keptFromOffset: bigint | null } {
    const ageMs = Math.max(0, Math.floor(maxAgeMs));
    if (!Number.isFinite(ageMs)) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: null };

    const tx = this.db.transaction(() => {
      const lastRow = this.db.query(`SELECT offset, ts_ms FROM wal WHERE stream=? ORDER BY offset DESC LIMIT 1;`).get(stream) as any;
      if (!lastRow || lastRow.offset == null) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: null };
      const lastOffset = this.toBigInt(lastRow.offset);

      let keepFromOffset: bigint;
      if (ageMs === 0) {
        // maxAgeMs=0 means "keep only the newest row" (still leaving 1 record).
        keepFromOffset = lastOffset;
      } else {
        const cutoff = this.nowMs() - BigInt(ageMs);
        const keepRow = this.db
          .query(`SELECT offset FROM wal WHERE stream=? AND ts_ms >= ? ORDER BY offset ASC LIMIT 1;`)
          .get(stream, this.bindInt(cutoff)) as any;
        keepFromOffset = keepRow && keepRow.offset != null ? this.toBigInt(keepRow.offset) : lastOffset;
      }

      if (keepFromOffset <= 0n) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: keepFromOffset };

      const stats = this.db
        .query(
          `SELECT COALESCE(SUM(payload_len), 0) as bytes, COUNT(*) as rows
           FROM wal WHERE stream=? AND offset < ?;`
        )
        .get(stream, this.bindInt(keepFromOffset)) as any;
      const bytes = this.toBigInt(stats?.bytes ?? 0);
      const rows = this.toBigInt(stats?.rows ?? 0);
      if (rows <= 0n) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: keepFromOffset };

      this.db.query(`DELETE FROM wal WHERE stream=? AND offset < ?;`).run(stream, this.bindInt(keepFromOffset));

      // Touch streams are WAL-only: pending_* tracks WAL payload bytes/rows. Keep it consistent for stats/backpressure.
      const now = this.nowMs();
      this.db.query(
        `UPDATE streams
         SET pending_bytes = CASE WHEN pending_bytes >= ? THEN pending_bytes - ? ELSE 0 END,
             pending_rows = CASE WHEN pending_rows >= ? THEN pending_rows - ? ELSE 0 END,
             wal_bytes = CASE WHEN wal_bytes >= ? THEN wal_bytes - ? ELSE 0 END,
             wal_rows = CASE WHEN wal_rows >= ? THEN wal_rows - ? ELSE 0 END,
             updated_at_ms = ?
         WHERE stream = ?;`
      ).run(bytes, bytes, rows, rows, bytes, bytes, rows, rows, now, stream);

      const trimmedBytes = bytes <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(bytes) : Number.MAX_SAFE_INTEGER;
      const trimmedRows = rows <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(rows) : Number.MAX_SAFE_INTEGER;
      return { trimmedRows, trimmedBytes, keptFromOffset: keepFromOffset };
    });
    return tx();
  }

  countStreams(): number {
    const row = this.stmts.countStreams.get(STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH) as any;
    return row ? Number(row.cnt) : 0;
  }

  sumPendingBytes(): number {
    const row = this.stmts.sumPendingBytes.get() as any;
    const total = row?.total ?? 0;
    return Number(this.toBigInt(total));
  }

  sumPendingSegmentBytes(): number {
    const row = this.stmts.sumPendingSegmentBytes.get() as any;
    const total = row?.total ?? 0;
    return Number(this.toBigInt(total));
  }

  private ensureDbStat(): boolean {
    if (this.dbstatReady != null) return this.dbstatReady;
    try {
      this.db.exec("CREATE VIRTUAL TABLE IF NOT EXISTS temp.dbstat USING dbstat;");
      this.dbstatReady = true;
    } catch {
      this.dbstatReady = false;
    }
    return this.dbstatReady;
  }

  private estimateWalBytes(): number {
    try {
      const row = this.db.query(
        `SELECT
           COALESCE(SUM(payload_len), 0) as payload,
           COALESCE(SUM(LENGTH(routing_key)), 0) as rk,
           COALESCE(SUM(LENGTH(content_type)), 0) as ct
         FROM wal;`
      ).get() as any;
      return Number(row?.payload ?? 0) + Number(row?.rk ?? 0) + Number(row?.ct ?? 0);
    } catch {
      return 0;
    }
  }

  private estimateMetaBytes(): number {
    try {
      const streams = this.db.query(
        `SELECT
           COALESCE(SUM(LENGTH(stream)), 0) as stream,
           COALESCE(SUM(LENGTH(content_type)), 0) as content_type,
           COALESCE(SUM(LENGTH(stream_seq)), 0) as stream_seq,
           COALESCE(SUM(LENGTH(closed_producer_id)), 0) as closed_producer_id
         FROM streams;`
      ).get() as any;
      const segments = this.db.query(
        `SELECT
           COALESCE(SUM(LENGTH(segment_id)), 0) as segment_id,
           COALESCE(SUM(LENGTH(stream)), 0) as stream,
           COALESCE(SUM(LENGTH(local_path)), 0) as local_path,
           COALESCE(SUM(LENGTH(r2_etag)), 0) as r2_etag
         FROM segments;`
      ).get() as any;
      const manifests = this.db.query(
        `SELECT
           COALESCE(SUM(LENGTH(stream)), 0) as stream,
           COALESCE(SUM(LENGTH(last_uploaded_etag)), 0) as last_uploaded_etag
         FROM manifests;`
      ).get() as any;
      const schemas = this.db.query(`SELECT COALESCE(SUM(LENGTH(schema_json)), 0) as schema_json FROM schemas;`).get() as any;
      const producers = this.db.query(
        `SELECT
           COALESCE(SUM(LENGTH(stream)), 0) as stream,
           COALESCE(SUM(LENGTH(producer_id)), 0) as producer_id
         FROM producer_state;`
      ).get() as any;
      const total =
        Number(streams?.stream ?? 0) +
        Number(streams?.content_type ?? 0) +
        Number(streams?.stream_seq ?? 0) +
        Number(streams?.closed_producer_id ?? 0) +
        Number(segments?.segment_id ?? 0) +
        Number(segments?.stream ?? 0) +
        Number(segments?.local_path ?? 0) +
        Number(segments?.r2_etag ?? 0) +
        Number(manifests?.stream ?? 0) +
        Number(manifests?.last_uploaded_etag ?? 0) +
        Number(schemas?.schema_json ?? 0) +
        Number(producers?.stream ?? 0) +
        Number(producers?.producer_id ?? 0);
      return total;
    } catch {
      return 0;
    }
  }

  getWalDbSizeBytes(): number {
    if (this.ensureDbStat()) {
      try {
        const row = this.db.query(`SELECT COALESCE(SUM(pgsize), 0) as total FROM temp.dbstat WHERE name = 'wal';`).get() as any;
        return Number(row?.total ?? 0);
      } catch {
        // fall through
      }
    }
    return this.estimateWalBytes();
  }

  getMetaDbSizeBytes(): number {
    if (this.ensureDbStat()) {
      try {
        const row = this.db
          .query(`SELECT COALESCE(SUM(pgsize), 0) as total FROM temp.dbstat WHERE name != 'wal';`)
          .get() as any;
        return Number(row?.total ?? 0);
      } catch {
        // fall through
      }
    }
    return this.estimateMetaBytes();
  }

  /**
   * Append rows into WAL inside a transaction.
   *
   * Returns the last offset written.
   */
  appendWalRows(args: {
    stream: string;
    startOffset: bigint;
    expectedOffset?: bigint;
    baseAppendMs: bigint;
    rows: Array<{ routingKey: Uint8Array | null; contentType: string | null; payload: Uint8Array; appendMs: bigint }>;
  }): Result<
    { lastOffset: bigint },
    { kind: "no_rows" | "stream_missing" | "stream_expired" } | { kind: "seq_mismatch"; expectedNext: bigint }
  > {
    const { stream, startOffset, expectedOffset, rows } = args;
    if (rows.length === 0) return Result.err({ kind: "no_rows" });

    const tx = this.db.transaction(() => {
      const st = this.getStream(stream);
      if (!st || this.isDeleted(st)) return Result.err({ kind: "stream_missing" as const });
      if (st.expires_at_ms != null && this.nowMs() > st.expires_at_ms) return Result.err({ kind: "stream_expired" as const });

      if (expectedOffset !== undefined && st.next_offset !== expectedOffset) {
        return Result.err({ kind: "seq_mismatch" as const, expectedNext: st.next_offset });
      }

      let totalBytes = 0n;
      let offset = startOffset;
      for (const r of rows) {
        const payloadLen = r.payload.byteLength;
        totalBytes += BigInt(payloadLen);
        this.stmts.insertWal.run(stream, offset, r.appendMs, r.payload, payloadLen, r.routingKey, r.contentType, 0);
        offset += 1n;
      }

      const lastOffset = offset - 1n;
      const newNextOffset = lastOffset + 1n;
      const now = this.nowMs();
      const pendingRows = BigInt(rows.length);
      const lastAppend = rows[rows.length - 1].appendMs;

      this.stmts.updateStreamAppend.run(
        newNextOffset,
        now,
        lastAppend,
        pendingRows,
        totalBytes,
        pendingRows,
        totalBytes,
        stream,
        STREAM_FLAG_DELETED
      );

      return Result.ok({ lastOffset });
    });

    return tx();
  }

  /**
   * Query WAL rows within a range.
   * Uses iterate() for bounded memory.
   */
  *iterWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): Generator<any, void, void> {
    const start = this.bindInt(startOffset);
    const end = this.bindInt(endOffset);
    const stmt = routingKey
      ? this.db.query(
          `SELECT offset, ts_ms, routing_key, content_type, payload\n           FROM wal\n           WHERE stream = ? AND offset >= ? AND offset <= ? AND routing_key = ?\n           ORDER BY offset ASC;`
        )
      : this.db.query(
          `SELECT offset, ts_ms, routing_key, content_type, payload\n           FROM wal\n           WHERE stream = ? AND offset >= ? AND offset <= ?\n           ORDER BY offset ASC;`
        );
    try {
      const it = routingKey
        ? (stmt.iterate(stream, start, end, routingKey) as any)
        : (stmt.iterate(stream, start, end) as any);
      for (const row of it) {
        yield row;
      }
    } finally {
      try {
        stmt.finalize?.();
      } catch {
        // ignore
      }
    }
  }

  nextSegmentIndexForStream(stream: string): number {
    const row = this.stmts.nextSegmentIndex.get(stream) as any;
    return Number(row?.next_idx ?? 0);
  }

  createSegmentRow(row: {
    segmentId: string;
    stream: string;
    segmentIndex: number;
    startOffset: bigint;
    endOffset: bigint;
    blockCount: number;
    lastAppendMs: bigint;
    sizeBytes: number;
    localPath: string;
  }): void {
    this.stmts.createSegment.run(
      row.segmentId,
      row.stream,
      row.segmentIndex,
      row.startOffset,
      row.endOffset,
      row.blockCount,
      row.lastAppendMs,
      row.sizeBytes,
      row.localPath,
      this.nowMs()
    );
  }

  commitSealedSegment(row: {
    segmentId: string;
    stream: string;
    segmentIndex: number;
    startOffset: bigint;
    endOffset: bigint;
    blockCount: number;
    lastAppendMs: bigint;
    sizeBytes: number;
    localPath: string;
    payloadBytes: bigint;
    rowsSealed: bigint;
  }): void {
    const tx = this.db.transaction(() => {
      this.createSegmentRow(row);
      this.appendSegmentMeta(row.stream, row.endOffset + 1n, row.blockCount, row.lastAppendMs * 1_000_000n);
      this.setStreamSealedThrough(row.stream, row.endOffset, row.payloadBytes, row.rowsSealed);
    });
    tx();
  }

  listSegmentsForStream(stream: string): SegmentRow[] {
    const rows = this.stmts.listSegmentsForStream.all(stream) as any[];
    return rows.map((r) => this.coerceSegmentRow(r));
  }

  getSegmentByIndex(stream: string, segmentIndex: number): SegmentRow | null {
    const row = this.stmts.getSegmentByIndex.get(stream, segmentIndex) as any;
    return row ? this.coerceSegmentRow(row) : null;
  }

  findSegmentForOffset(stream: string, offset: bigint): SegmentRow | null {
    const bound = this.bindInt(offset);
    const row = this.stmts.findSegmentForOffset.get(stream, bound, bound) as any;
    return row ? this.coerceSegmentRow(row) : null;
  }

  pendingUploadSegments(limit: number): SegmentRow[] {
    const rows = this.stmts.pendingUploadSegments.all(limit) as any[];
    return rows.map((r) => this.coerceSegmentRow(r));
  }

  countPendingSegments(): number {
    const row = this.stmts.countPendingSegments.get() as any;
    return row ? Number(row.cnt) : 0;
  }

  countSegmentsForStream(stream: string): number {
    const row = this.stmts.countSegmentsForStream.get(stream) as any;
    return row ? Number(row.cnt) : 0;
  }

  getSegmentMeta(stream: string): SegmentMetaRow | null {
    const row = this.stmts.getSegmentMeta.get(stream) as any;
    if (!row) return null;
    const offsets = row.segment_offsets instanceof Uint8Array ? row.segment_offsets : new Uint8Array(row.segment_offsets);
    const blocks = row.segment_blocks instanceof Uint8Array ? row.segment_blocks : new Uint8Array(row.segment_blocks);
    const lastTs = row.segment_last_ts instanceof Uint8Array ? row.segment_last_ts : new Uint8Array(row.segment_last_ts);
    return {
      stream: String(row.stream),
      segment_count: Number(row.segment_count),
      segment_offsets: offsets,
      segment_blocks: blocks,
      segment_last_ts: lastTs,
    };
  }

  ensureSegmentMeta(stream: string): void {
    this.stmts.ensureSegmentMeta.run(stream);
  }

  appendSegmentMeta(stream: string, offsetPlusOne: bigint, blockCount: number, lastAppendNs: bigint): void {
    this.ensureSegmentMeta(stream);
    const offsetBytes = this.encodeU64Le(offsetPlusOne);
    const blockBytes = this.encodeU32Le(blockCount);
    const tsBytes = this.encodeU64Le(lastAppendNs);
    this.stmts.appendSegmentMeta.run(offsetBytes, blockBytes, tsBytes, stream);
  }

  upsertSegmentMeta(stream: string, count: number, offsets: Uint8Array, blocks: Uint8Array, lastTs: Uint8Array): void {
    this.stmts.upsertSegmentMeta.run(stream, count, offsets, blocks, lastTs);
  }

  rebuildSegmentMeta(stream: string): SegmentMetaRow {
    const rows = this.db
      .query(
        `SELECT end_offset, block_count, last_append_ms
         FROM segments WHERE stream=? ORDER BY segment_index ASC;`
      )
      .all(stream) as any[];
    const count = rows.length;
    const offsets = new Uint8Array(count * 8);
    const blocks = new Uint8Array(count * 4);
    const lastTs = new Uint8Array(count * 8);
    const dvOffsets = new DataView(offsets.buffer, offsets.byteOffset, offsets.byteLength);
    const dvBlocks = new DataView(blocks.buffer, blocks.byteOffset, blocks.byteLength);
    const dvLastTs = new DataView(lastTs.buffer, lastTs.byteOffset, lastTs.byteLength);
    for (let i = 0; i < rows.length; i++) {
      const endOffset = this.toBigInt(rows[i].end_offset);
      const blockCount = Number(rows[i].block_count);
      const lastAppendMs = this.toBigInt(rows[i].last_append_ms);
      dvOffsets.setBigUint64(i * 8, endOffset + 1n, true);
      dvBlocks.setUint32(i * 4, blockCount >>> 0, true);
      dvLastTs.setBigUint64(i * 8, lastAppendMs * 1_000_000n, true);
    }
    this.upsertSegmentMeta(stream, count, offsets, blocks, lastTs);
    return { stream, segment_count: count, segment_offsets: offsets, segment_blocks: blocks, segment_last_ts: lastTs };
  }

  setUploadedSegmentCount(stream: string, count: number): void {
    this.stmts.setUploadedSegmentCount.run(count, this.nowMs(), stream);
  }

  advanceUploadedSegmentCount(stream: string): number {
    const row = this.getStream(stream);
    if (!row) return 0;
    let count = row.uploaded_segment_count ?? 0;
    for (;;) {
      const seg = this.getSegmentByIndex(stream, count);
      if (!seg || !seg.r2_etag) break;
      count += 1;
    }
    if (count !== row.uploaded_segment_count) {
      this.stmts.setUploadedSegmentCount.run(count, this.nowMs(), stream);
    }
    return count;
  }

  markSegmentUploaded(segmentId: string, etag: string, uploadedAtMs: bigint): void {
    this.stmts.markSegmentUploaded.run(etag, uploadedAtMs, segmentId);
  }

  setStreamSealedThrough(stream: string, sealedThrough: bigint, bytesSealed: bigint, rowsSealed: bigint): void {
    const now = this.nowMs();
    this.db.query(
      `UPDATE streams
       SET sealed_through = ?,
           pending_bytes = CASE WHEN pending_bytes >= ? THEN pending_bytes - ? ELSE 0 END,
           pending_rows = CASE WHEN pending_rows >= ? THEN pending_rows - ? ELSE 0 END,
           last_segment_cut_ms = ?,
           updated_at_ms = ?
       WHERE stream = ?;`
    ).run(sealedThrough, bytesSealed, bytesSealed, rowsSealed, rowsSealed, now, now, stream);
  }

  setSegmentInProgress(stream: string, inProgress: number): void {
    this.db.query(`UPDATE streams SET segment_in_progress=?, updated_at_ms=? WHERE stream=?;`).run(inProgress, this.nowMs(), stream);
  }

  tryClaimSegment(stream: string): boolean {
    const res = this.stmts.tryClaimSegment.run(this.nowMs(), stream) as any;
    const changes = typeof res?.changes === "bigint" ? res.changes : BigInt(Number(res?.changes ?? 0));
    return changes > 0n;
  }

  resetSegmentInProgress(): void {
    this.db.query(`UPDATE streams SET segment_in_progress=0 WHERE segment_in_progress != 0;`).run();
  }

  advanceUploadedThrough(stream: string, uploadedThrough: bigint): void {
    this.stmts.advanceUploadedThrough.run(uploadedThrough, this.nowMs(), stream);
  }

  deleteWalThrough(stream: string, uploadedThrough: bigint): { deletedRows: number; deletedBytes: number } {
    const through = this.bindInt(uploadedThrough);
    const tx = this.db.transaction(() => {
      const stats = this.db
        .query(
          `SELECT COALESCE(SUM(payload_len), 0) as bytes, COUNT(*) as rows
           FROM wal WHERE stream=? AND offset <= ?;`
        )
        .get(stream, through) as any;
      const bytes = this.toBigInt(stats?.bytes ?? 0);
      const rows = this.toBigInt(stats?.rows ?? 0);
      if (rows <= 0n) return { deletedRows: 0, deletedBytes: 0 };

      this.stmts.deleteWalBeforeOffset.run(stream, through);

      const now = this.nowMs();
      this.db.query(
        `UPDATE streams
         SET wal_bytes = CASE WHEN wal_bytes >= ? THEN wal_bytes - ? ELSE 0 END,
             wal_rows = CASE WHEN wal_rows >= ? THEN wal_rows - ? ELSE 0 END,
             updated_at_ms = ?
         WHERE stream = ?;`
      ).run(bytes, bytes, rows, rows, now, stream);

      const deletedBytes = bytes <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(bytes) : Number.MAX_SAFE_INTEGER;
      const deletedRows = rows <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(rows) : Number.MAX_SAFE_INTEGER;
      return { deletedRows, deletedBytes };
    });
    return tx();
  }

  getManifestRow(stream: string): { stream: string; generation: number; uploaded_generation: number; last_uploaded_at_ms: bigint | null; last_uploaded_etag: string | null } {
    const row = this.stmts.getManifest.get(stream) as any;
    if (!row) {
      this.stmts.upsertManifest.run(stream, 0, 0, null, null);
      const fresh = this.stmts.getManifest.get(stream) as any;
      return {
        stream: String(fresh.stream),
        generation: Number(fresh.generation),
        uploaded_generation: Number(fresh.uploaded_generation),
        last_uploaded_at_ms: fresh.last_uploaded_at_ms == null ? null : this.toBigInt(fresh.last_uploaded_at_ms),
        last_uploaded_etag: fresh.last_uploaded_etag == null ? null : String(fresh.last_uploaded_etag),
      };
    }
    return {
      stream: String(row.stream),
      generation: Number(row.generation),
      uploaded_generation: Number(row.uploaded_generation),
      last_uploaded_at_ms: row.last_uploaded_at_ms == null ? null : this.toBigInt(row.last_uploaded_at_ms),
      last_uploaded_etag: row.last_uploaded_etag == null ? null : String(row.last_uploaded_etag),
    };
  }

  upsertManifestRow(stream: string, generation: number, uploadedGeneration: number, uploadedAtMs: bigint | null, etag: string | null): void {
    this.stmts.upsertManifest.run(stream, generation, uploadedGeneration, uploadedAtMs, etag);
  }

  getIndexState(stream: string): IndexStateRow | null {
    const row = this.stmts.getIndexState.get(stream) as any;
    if (!row) return null;
    return {
      stream: String(row.stream),
      index_secret: row.index_secret instanceof Uint8Array ? row.index_secret : new Uint8Array(row.index_secret),
      indexed_through: Number(row.indexed_through),
      updated_at_ms: this.toBigInt(row.updated_at_ms),
    };
  }

  upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): void {
    this.stmts.upsertIndexState.run(stream, indexSecret, indexedThrough, this.nowMs());
  }

  updateIndexedThrough(stream: string, indexedThrough: number): void {
    this.stmts.updateIndexedThrough.run(indexedThrough, this.nowMs(), stream);
  }

  listIndexRuns(stream: string): IndexRunRow[] {
    const rows = this.stmts.listIndexRuns.all(stream) as any[];
    return rows.map((r) => ({
      run_id: String(r.run_id),
      stream: String(r.stream),
      level: Number(r.level),
      start_segment: Number(r.start_segment),
      end_segment: Number(r.end_segment),
      object_key: String(r.object_key),
      filter_len: Number(r.filter_len),
      record_count: Number(r.record_count),
      retired_gen: r.retired_gen == null ? null : Number(r.retired_gen),
      retired_at_ms: r.retired_at_ms == null ? null : this.toBigInt(r.retired_at_ms),
    }));
  }

  listIndexRunsAll(stream: string): IndexRunRow[] {
    const rows = this.stmts.listIndexRunsAll.all(stream) as any[];
    return rows.map((r) => ({
      run_id: String(r.run_id),
      stream: String(r.stream),
      level: Number(r.level),
      start_segment: Number(r.start_segment),
      end_segment: Number(r.end_segment),
      object_key: String(r.object_key),
      filter_len: Number(r.filter_len),
      record_count: Number(r.record_count),
      retired_gen: r.retired_gen == null ? null : Number(r.retired_gen),
      retired_at_ms: r.retired_at_ms == null ? null : this.toBigInt(r.retired_at_ms),
    }));
  }

  listRetiredIndexRuns(stream: string): IndexRunRow[] {
    const rows = this.stmts.listRetiredIndexRuns.all(stream) as any[];
    return rows.map((r) => ({
      run_id: String(r.run_id),
      stream: String(r.stream),
      level: Number(r.level),
      start_segment: Number(r.start_segment),
      end_segment: Number(r.end_segment),
      object_key: String(r.object_key),
      filter_len: Number(r.filter_len),
      record_count: Number(r.record_count),
      retired_gen: r.retired_gen == null ? null : Number(r.retired_gen),
      retired_at_ms: r.retired_at_ms == null ? null : this.toBigInt(r.retired_at_ms),
    }));
  }

  insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): void {
    this.stmts.insertIndexRun.run(
      row.run_id,
      row.stream,
      row.level,
      row.start_segment,
      row.end_segment,
      row.object_key,
      row.filter_len,
      row.record_count
    );
  }

  retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void {
    if (runIds.length === 0) return;
    const tx = this.db.transaction(() => {
      for (const runId of runIds) {
        this.stmts.retireIndexRun.run(retiredGen, retiredAtMs, runId);
      }
    });
    tx();
  }

  deleteIndexRuns(runIds: string[]): void {
    if (runIds.length === 0) return;
    const tx = this.db.transaction(() => {
      for (const runId of runIds) {
        this.stmts.deleteIndexRun.run(runId);
      }
    });
    tx();
  }

  countUploadedSegments(stream: string): number {
    const row = this.stmts.countUploadedSegments.get(stream) as any;
    const maxIdx = row ? Number(row.max_idx) : -1;
    return maxIdx >= 0 ? maxIdx + 1 : 0;
  }

  commitManifest(stream: string, generation: number, etag: string, uploadedAtMs: bigint, uploadedThrough: bigint): void {
    const tx = this.db.transaction(() => {
      this.stmts.upsertManifest.run(stream, generation, generation, uploadedAtMs, etag);
      this.stmts.advanceUploadedThrough.run(uploadedThrough, this.nowMs(), stream);
      let gcThrough = uploadedThrough;
      const interp = this.stmts.getStreamInterpreter.get(stream) as any;
      if (interp) {
        const interpretedThrough = this.toBigInt(interp.interpreted_through);
        gcThrough = interpretedThrough < gcThrough ? interpretedThrough : gcThrough;
      }
      if (gcThrough < 0n) return;

      // Chunk deletes to avoid large event-loop stalls on catch-up uploads.
      // (Periodic GC in touch/manager.ts handles interpreter-gated cleanup too.)
      let deleteThrough = gcThrough;
      if (BASE_WAL_GC_CHUNK_OFFSETS > 0) {
        const oldest = this.getWalOldestOffset(stream);
        if (oldest != null) {
          const maxThrough = oldest + BigInt(BASE_WAL_GC_CHUNK_OFFSETS) - 1n;
          if (deleteThrough > maxThrough) deleteThrough = maxThrough;
        }
      }
      if (deleteThrough < 0n) return;

      const bound = this.bindInt(deleteThrough);
      const stats = this.db
        .query(
          `SELECT COALESCE(SUM(payload_len), 0) as bytes, COUNT(*) as rows
           FROM wal WHERE stream=? AND offset <= ?;`
        )
        .get(stream, bound) as any;
      const bytes = this.toBigInt(stats?.bytes ?? 0);
      const rows = this.toBigInt(stats?.rows ?? 0);
      if (rows <= 0n) return;

      this.stmts.deleteWalBeforeOffset.run(stream, bound);

      // Keep retained-WAL counters consistent for metrics/debugging.
      const now = this.nowMs();
      this.db.query(
        `UPDATE streams
         SET wal_bytes = CASE WHEN wal_bytes >= ? THEN wal_bytes - ? ELSE 0 END,
             wal_rows = CASE WHEN wal_rows >= ? THEN wal_rows - ? ELSE 0 END,
             updated_at_ms = ?
         WHERE stream = ?;`
      ).run(bytes, bytes, rows, rows, now, stream);
    });
    tx();
  }

  /** Find candidates by bytes/rows/interval. */
  candidates(
    minPendingBytes: bigint,
    minPendingRows: bigint,
    maxIntervalMs: bigint,
    limit: number
  ): Array<{ stream: string; pending_bytes: bigint; pending_rows: bigint; last_segment_cut_ms: bigint; sealed_through: bigint; next_offset: bigint; epoch: number }> {
    if (maxIntervalMs <= 0n) {
      return this.stmts.candidateStreamsNoInterval.all(STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH, minPendingBytes, minPendingRows, limit) as any;
    }
    const now = this.nowMs();
    return this.stmts.candidateStreams.all(STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH, minPendingBytes, minPendingRows, now, maxIntervalMs, limit) as any;
  }
}
