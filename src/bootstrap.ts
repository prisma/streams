import { mkdirSync, rmSync } from "node:fs";
import { dirname } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import type { Config } from "./config";
import { SqliteDurableStore } from "./db/db";
import type { ObjectStore } from "./objectstore/interface";
import { localSegmentPath, schemaObjectKey, segmentObjectKey, streamHash16Hex } from "./util/stream_paths";
import { retry } from "./util/retry";
import { dsError } from "./util/ds_error.ts";

type Manifest = Record<string, any>;

export async function bootstrapFromR2(cfg: Config, store: ObjectStore, opts: { clearLocal?: boolean } = {}): Promise<void> {
  if (opts.clearLocal !== false) {
    try {
      rmSync(cfg.dbPath, { force: true });
    } catch {
      // ignore
    }
    try {
      rmSync(`${cfg.rootDir}/local`, { recursive: true, force: true });
    } catch {
      // ignore
    }
    try {
      rmSync(`${cfg.rootDir}/cache`, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }

  mkdirSync(cfg.rootDir, { recursive: true });

  const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.sqliteCacheBytes });
  try {
    const retryOpts = {
      retries: cfg.objectStoreRetries,
      baseDelayMs: cfg.objectStoreBaseDelayMs,
      maxDelayMs: cfg.objectStoreMaxDelayMs,
      timeoutMs: cfg.objectStoreTimeoutMs,
    };
    const keys = await retry(() => store.list("streams/"), retryOpts);
    const manifestKeys = keys.filter((k) => k.endsWith("/manifest.json"));
    for (const mkey of manifestKeys) {
      const mbytes = await retry(async () => {
        const data = await store.get(mkey);
        if (!data) throw dsError(`missing manifest ${mkey}`);
        return data;
      }, retryOpts);
      const manifest = JSON.parse(new TextDecoder().decode(mbytes)) as Manifest;
      const stream = String(manifest.name ?? "");
      if (!stream) continue;

      const shash = streamHash16Hex(stream);
      const nowMs = db.nowMs();

      const createdAtMs = parseIsoMs(manifest.created_at) ?? nowMs;
      const expiresAtMs = parseIsoMs(manifest.expires_at);
      const epoch = typeof manifest.epoch === "number" ? manifest.epoch : 0;
      const nextOffsetNum = typeof manifest.next_offset === "number" ? manifest.next_offset : 0;
      const nextOffset = BigInt(nextOffsetNum);

      const contentType = typeof manifest.content_type === "string" ? manifest.content_type : "application/octet-stream";
      const profile = typeof manifest.profile === "string" && manifest.profile !== "" ? manifest.profile : "generic";
      const profileJson = manifest.profile_json && typeof manifest.profile_json === "object" ? manifest.profile_json : null;
      const streamSeq = typeof manifest.stream_seq === "string" ? manifest.stream_seq : null;
      const closed = typeof manifest.closed === "number" ? manifest.closed : 0;
      const closedProducerId = typeof manifest.closed_producer_id === "string" ? manifest.closed_producer_id : null;
      const closedProducerEpoch = typeof manifest.closed_producer_epoch === "number" ? manifest.closed_producer_epoch : null;
      const closedProducerSeq = typeof manifest.closed_producer_seq === "number" ? manifest.closed_producer_seq : null;
      const ttlSeconds = typeof manifest.ttl_seconds === "number" ? manifest.ttl_seconds : null;
      const streamFlags = typeof manifest.stream_flags === "number" ? manifest.stream_flags : 0;

      const segmentOffsetsBytes = decodeZstdBase64(manifest.segment_offsets ?? "");
      const segmentBlocksBytes = decodeZstdBase64(manifest.segment_blocks ?? "");
      const segmentLastTsBytes = decodeZstdBase64(manifest.segment_last_ts ?? "");
      const segmentOffsets = decodeU64LeArray(segmentOffsetsBytes);
      const segmentBlocks = decodeU32LeArray(segmentBlocksBytes);
      const segmentLastTs = decodeU64LeArray(segmentLastTsBytes);
      const segmentCount = typeof manifest.segment_count === "number" ? manifest.segment_count : segmentOffsets.length;

      if (segmentOffsets.length !== segmentCount || segmentBlocks.length !== segmentCount || segmentLastTs.length !== segmentCount) {
        throw dsError(`manifest array length mismatch for ${stream}`);
      }

      const lastEndOffset = segmentCount > 0 ? segmentOffsets[segmentCount - 1] - 1n : -1n;
      const uploadedPrefix = typeof manifest.uploaded_through === "number" ? manifest.uploaded_through : segmentCount;
      const uploadedThrough =
        uploadedPrefix > 0 && uploadedPrefix <= segmentOffsets.length ? segmentOffsets[uploadedPrefix - 1] - 1n : -1n;
      const lastAppendMs = segmentCount > 0 ? segmentLastTs[segmentCount - 1] / 1_000_000n : nowMs;

      db.restoreStreamRow({
        stream,
        created_at_ms: createdAtMs,
        updated_at_ms: nowMs,
        content_type: contentType,
        profile,
        stream_seq: streamSeq,
        closed,
        closed_producer_id: closedProducerId,
        closed_producer_epoch: closedProducerEpoch,
        closed_producer_seq: closedProducerSeq,
        ttl_seconds: ttlSeconds,
        epoch,
        next_offset: nextOffset,
        sealed_through: lastEndOffset,
        uploaded_through: uploadedThrough,
        uploaded_segment_count: uploadedPrefix,
        pending_rows: 0n,
        pending_bytes: 0n,
        wal_rows: 0n,
        wal_bytes: 0n,
        last_append_ms: lastAppendMs,
        last_segment_cut_ms: lastAppendMs,
        segment_in_progress: 0,
        expires_at_ms: expiresAtMs,
        stream_flags: streamFlags,
      });
      if (profileJson) {
        db.upsertStreamProfile(stream, JSON.stringify(profileJson));
      } else {
        db.deleteStreamProfile(stream);
      }

      db.upsertSegmentMeta(stream, segmentCount, segmentOffsetsBytes, segmentBlocksBytes, segmentLastTsBytes);

      const manifestHead = await retry(async () => {
        const head = await store.head(mkey);
        if (!head) throw dsError(`missing manifest head ${mkey}`);
        return head;
      }, retryOpts);
      db.upsertManifestRow(stream, Number(manifest.generation ?? 0), Number(manifest.generation ?? 0), nowMs, manifestHead?.etag ?? null);

      for (let i = 0; i < segmentCount; i++) {
        const startOffset = i === 0 ? 0n : segmentOffsets[i - 1];
        const endOffset = segmentOffsets[i] - 1n;
        const lastTsMs = segmentLastTs[i] / 1_000_000n;
        const localPath = localSegmentPath(cfg.rootDir, shash, i);
        const segmentId = `${shash}-${i}-${startOffset.toString()}-${endOffset.toString()}`;
        mkdirSync(dirname(localPath), { recursive: true });
        const objectKey = segmentObjectKey(shash, i);
        const head = await retry(async () => {
          const h = await store.head(objectKey);
          if (!h) throw dsError(`missing segment ${objectKey}`);
          return h;
        }, retryOpts);
        if (!head) throw dsError(`missing segment ${objectKey}`);
        db.createSegmentRow({
          segmentId,
          stream,
          segmentIndex: i,
          startOffset,
          endOffset,
          blockCount: segmentBlocks[i],
          lastAppendMs: lastTsMs,
          sizeBytes: head.size,
          localPath,
        });
        db.markSegmentUploaded(segmentId, head.etag, nowMs);
      }

      const indexSecretB64 = typeof manifest.index_secret === "string" ? manifest.index_secret : "";
      if (indexSecretB64) {
        const secret = new Uint8Array(Buffer.from(indexSecretB64, "base64"));
        const indexedThrough = typeof manifest.indexed_through === "number" ? manifest.indexed_through : 0;
        db.upsertIndexState(stream, secret, indexedThrough);
      }

      const activeRuns = Array.isArray(manifest.active_runs) ? manifest.active_runs : [];
      const retiredRuns = Array.isArray(manifest.retired_runs) ? manifest.retired_runs : [];
      for (const r of activeRuns) {
        db.insertIndexRun({
          run_id: String(r.run_id),
          stream,
          level: Number(r.level),
          start_segment: Number(r.start_segment),
          end_segment: Number(r.end_segment),
          object_key: String(r.object_key),
          filter_len: Number(r.filter_len ?? 0),
          record_count: Number(r.record_count ?? 0),
        });
      }
      for (const r of retiredRuns) {
        const runId = String(r.run_id);
        db.insertIndexRun({
          run_id: runId,
          stream,
          level: Number(r.level),
          start_segment: Number(r.start_segment),
          end_segment: Number(r.end_segment),
          object_key: String(r.object_key),
          filter_len: Number(r.filter_len ?? 0),
          record_count: Number(r.record_count ?? 0),
        });
        const retiredGen = typeof r.retired_gen === "number" ? r.retired_gen : Number(manifest.generation ?? 0);
        const retiredAtUnix = typeof r.retired_at_unix === "number" ? r.retired_at_unix : Math.floor(Number(nowMs) / 1000);
        db.retireIndexRuns([runId], retiredGen, BigInt(retiredAtUnix) * 1000n);
      }

      const secondaryIndexes = manifest.secondary_indexes && typeof manifest.secondary_indexes === "object" ? manifest.secondary_indexes : {};
      for (const [indexName, rawState] of Object.entries(secondaryIndexes)) {
        if (!rawState || typeof rawState !== "object") continue;
        const indexSecretB64 = typeof (rawState as any).index_secret === "string" ? (rawState as any).index_secret : "";
        if (!indexSecretB64) continue;
        const secret = new Uint8Array(Buffer.from(indexSecretB64, "base64"));
        const indexedThrough =
          typeof (rawState as any).indexed_through === "number" ? Number((rawState as any).indexed_through) : 0;
        db.upsertSecondaryIndexState(stream, indexName, secret, indexedThrough);

        const activeSecondaryRuns = Array.isArray((rawState as any).active_runs) ? (rawState as any).active_runs : [];
        const retiredSecondaryRuns = Array.isArray((rawState as any).retired_runs) ? (rawState as any).retired_runs : [];
        for (const run of activeSecondaryRuns) {
          db.insertSecondaryIndexRun({
            run_id: String(run.run_id),
            stream,
            index_name: indexName,
            level: Number(run.level),
            start_segment: Number(run.start_segment),
            end_segment: Number(run.end_segment),
            object_key: String(run.object_key),
            filter_len: Number(run.filter_len ?? 0),
            record_count: Number(run.record_count ?? 0),
          });
        }
        for (const run of retiredSecondaryRuns) {
          const runId = String(run.run_id);
          db.insertSecondaryIndexRun({
            run_id: runId,
            stream,
            index_name: indexName,
            level: Number(run.level),
            start_segment: Number(run.start_segment),
            end_segment: Number(run.end_segment),
            object_key: String(run.object_key),
            filter_len: Number(run.filter_len ?? 0),
            record_count: Number(run.record_count ?? 0),
          });
          const retiredGen = typeof run.retired_gen === "number" ? run.retired_gen : Number(manifest.generation ?? 0);
          const retiredAtUnix = typeof run.retired_at_unix === "number" ? run.retired_at_unix : Math.floor(Number(nowMs) / 1000);
          db.retireSecondaryIndexRuns([runId], retiredGen, BigInt(retiredAtUnix) * 1000n);
        }
      }

      const searchFamilies = manifest.search_families && typeof manifest.search_families === "object" ? manifest.search_families : {};
      for (const [family, rawState] of Object.entries(searchFamilies)) {
        if (!rawState || typeof rawState !== "object") continue;
        const uploadedThrough =
          typeof (rawState as any).uploaded_through === "number" ? Number((rawState as any).uploaded_through) : 0;
        db.upsertSearchFamilyState(stream, family, uploadedThrough);
        const segments = Array.isArray((rawState as any).segments) ? (rawState as any).segments : [];
        for (const segment of segments) {
          if (!segment || typeof segment !== "object") continue;
          if (typeof (segment as any).segment_index !== "number" || typeof (segment as any).object_key !== "string") continue;
          db.upsertSearchFamilySegment(stream, family, Number((segment as any).segment_index), String((segment as any).object_key));
        }
      }

      const schemaKey = schemaObjectKey(shash);
      const schemaBytes = await retry(async () => {
        const data = await store.get(schemaKey);
        if (!data) return null;
        return data;
      }, retryOpts);
      if (schemaBytes) {
        db.upsertSchemaRegistry(stream, new TextDecoder().decode(schemaBytes));
      }
    }
  } finally {
    db.close();
  }
}

function decodeZstdBase64(value: string): Uint8Array {
  if (!value) return new Uint8Array(0);
  const raw = Buffer.from(value, "base64");
  if (raw.byteLength === 0) return new Uint8Array(0);
  return new Uint8Array(zstdDecompressSync(raw));
}

function decodeU64LeArray(bytes: Uint8Array): bigint[] {
  if (bytes.byteLength === 0) return [];
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const out: bigint[] = [];
  for (let off = 0; off + 8 <= bytes.byteLength; off += 8) {
    out.push(dv.getBigUint64(off, true));
  }
  return out;
}

function decodeU32LeArray(bytes: Uint8Array): number[] {
  if (bytes.byteLength === 0) return [];
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const out: number[] = [];
  for (let off = 0; off + 4 <= bytes.byteLength; off += 4) {
    out.push(dv.getUint32(off, true));
  }
  return out;
}

function parseIsoMs(value: any): bigint | null {
  if (!value || typeof value !== "string") return null;
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) return null;
  return BigInt(ms);
}
