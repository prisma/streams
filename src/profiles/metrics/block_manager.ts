import { existsSync, readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { Result } from "better-result";
import type { Config } from "../../config";
import type { SegmentRow, SqliteDurableStore } from "../../db/db";
import type { ObjectStore } from "../../objectstore/interface";
import { SchemaRegistryStore } from "../../schema/registry";
import { iterateBlocksResult } from "../../segment/format";
import { retry } from "../../util/retry";
import { dsError } from "../../util/ds_error.ts";
import { segmentObjectKey, streamHash16Hex, searchMetricsBlockSegmentObjectKey } from "../../util/stream_paths";
import { LruCache } from "../../util/lru";
import { buildMetricsBlockRecord } from "./normalize";
import {
  decodeMetricsBlockSegmentCompanionResult,
  encodeMetricsBlockSegmentCompanion,
  type MetricsBlockSegmentCompanion,
} from "./block_format";

type MetricsBlockBuildError = { kind: "invalid_metrics_block_build"; message: string };

function invalidMetricsBlockBuild<T = never>(message: string): Result<T, MetricsBlockBuildError> {
  return Result.err({ kind: "invalid_metrics_block_build", message });
}

export class MetricsBlockManager {
  private readonly queue = new Set<string>();
  private readonly building = new Set<string>();
  private timer: any | null = null;
  private running = false;
  private readonly cache = new LruCache<string, MetricsBlockSegmentCompanion>(128);

  constructor(
    private readonly cfg: Config,
    private readonly db: SqliteDurableStore,
    private readonly os: ObjectStore,
    private readonly registry: SchemaRegistryStore,
    private readonly publishManifest?: (stream: string) => Promise<void>,
    private readonly onMetadataChanged?: (stream: string) => void
  ) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.cfg.indexCheckIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  enqueue(stream: string): void {
    this.queue.add(stream);
  }

  async getSegmentCompanion(stream: string, segmentIndex: number): Promise<MetricsBlockSegmentCompanion | null> {
    const row = this.db.getSearchFamilySegment(stream, "mblk", segmentIndex);
    if (!row) return null;
    const cached = this.cache.get(row.object_key);
    if (cached) return cached;
    const bytes = await retry(
      async () => {
        const data = await this.os.get(row.object_key);
        if (!data) throw dsError(`missing .mblk object ${row.object_key}`);
        return data;
      },
      {
        retries: this.cfg.objectStoreRetries,
        baseDelayMs: this.cfg.objectStoreBaseDelayMs,
        maxDelayMs: this.cfg.objectStoreMaxDelayMs,
        timeoutMs: this.cfg.objectStoreTimeoutMs,
      }
    );
    const decodedRes = decodeMetricsBlockSegmentCompanionResult(bytes);
    if (Result.isError(decodedRes)) throw dsError(decodedRes.error.message);
    this.cache.set(row.object_key, decodedRes.value);
    return decodedRes.value;
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const streams = Array.from(this.queue);
      this.queue.clear();
      for (const stream of streams) {
        try {
          const buildRes = await this.buildPendingSegmentsResult(stream);
          if (Result.isError(buildRes)) this.queue.add(stream);
        } catch {
          this.queue.add(stream);
        }
      }
    } finally {
      this.running = false;
    }
  }

  private async buildPendingSegmentsResult(stream: string): Promise<Result<void, MetricsBlockBuildError>> {
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return invalidMetricsBlockBuild(regRes.error.message);
      if (regRes.value.search?.profile !== "metrics") return Result.ok(undefined);

      let state = this.db.getSearchFamilyState(stream, "mblk");
      if (!state) {
        this.db.upsertSearchFamilyState(stream, "mblk", 0);
        state = this.db.getSearchFamilyState(stream, "mblk");
      }
      if (!state) return Result.ok(undefined);

      let uploadedThrough = state.uploaded_through;
      let changed = false;
      const uploadedSegments = this.db.countUploadedSegments(stream);
      while (uploadedThrough < uploadedSegments) {
        const seg = this.db.getSegmentByIndex(stream, uploadedThrough);
        if (!seg || !seg.r2_etag) break;
        const companionRes = await this.buildSegmentCompanionResult(seg);
        if (Result.isError(companionRes)) return companionRes;
        const objectId = Buffer.from(randomBytes(8)).toString("hex");
        const objectKey = searchMetricsBlockSegmentObjectKey(streamHash16Hex(stream), seg.segment_index, objectId);
        const payload = encodeMetricsBlockSegmentCompanion(companionRes.value);
        try {
          await retry(
            () => this.os.put(objectKey, payload, { contentLength: payload.byteLength }),
            {
              retries: this.cfg.objectStoreRetries,
              baseDelayMs: this.cfg.objectStoreBaseDelayMs,
              maxDelayMs: this.cfg.objectStoreMaxDelayMs,
              timeoutMs: this.cfg.objectStoreTimeoutMs,
            }
          );
        } catch (e: unknown) {
          return invalidMetricsBlockBuild(String((e as any)?.message ?? e));
        }
        this.db.upsertSearchFamilySegment(stream, "mblk", seg.segment_index, objectKey);
        uploadedThrough += 1;
        this.db.upsertSearchFamilyState(stream, "mblk", uploadedThrough);
        this.cache.set(objectKey, companionRes.value);
        changed = true;
      }

      if (changed) this.onMetadataChanged?.(stream);
      if (changed && this.publishManifest) {
        try {
          await this.publishManifest(stream);
        } catch {
          // background loop will retry
        }
      }
      return Result.ok(undefined);
    } finally {
      this.building.delete(stream);
    }
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, MetricsBlockBuildError>> {
    try {
      if (existsSync(seg.local_path)) return Result.ok(new Uint8Array(readFileSync(seg.local_path)));
      const bytes = await retry(
        async () => {
          const data = await this.os.get(segmentObjectKey(streamHash16Hex(seg.stream), seg.segment_index));
          if (!data) throw dsError(`missing segment ${seg.segment_index}`);
          return data;
        },
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
      return Result.ok(bytes);
    } catch (e: unknown) {
      return invalidMetricsBlockBuild(String((e as any)?.message ?? e));
    }
  }

  private async buildSegmentCompanionResult(seg: SegmentRow): Promise<Result<MetricsBlockSegmentCompanion, MetricsBlockBuildError>> {
    const bytesRes = await this.loadSegmentBytesResult(seg);
    if (Result.isError(bytesRes)) return bytesRes;

    const records: MetricsBlockSegmentCompanion["records"] = [];
    let minWindowStartMs: number | undefined;
    let maxWindowEndMs: number | undefined;
    let docId = 0;
    for (const blockRes of iterateBlocksResult(bytesRes.value)) {
      if (Result.isError(blockRes)) return invalidMetricsBlockBuild(blockRes.error.message);
      for (const rec of blockRes.value.decoded.records) {
        let parsed: unknown;
        try {
          parsed = JSON.parse(new TextDecoder().decode(rec.payload));
        } catch {
          docId += 1;
          continue;
        }
        const normalizedRes = buildMetricsBlockRecord(docId, parsed);
        if (!Result.isError(normalizedRes)) {
          records.push(normalizedRes.value);
          minWindowStartMs =
            minWindowStartMs == null ? normalizedRes.value.windowStartMs : Math.min(minWindowStartMs, normalizedRes.value.windowStartMs);
          maxWindowEndMs =
            maxWindowEndMs == null ? normalizedRes.value.windowEndMs : Math.max(maxWindowEndMs, normalizedRes.value.windowEndMs);
        }
        docId += 1;
      }
    }

    return Result.ok({
      version: 1,
      stream: seg.stream,
      segment_index: seg.segment_index,
      record_count: records.length,
      min_window_start_ms: minWindowStartMs,
      max_window_end_ms: maxWindowEndMs,
      records,
    });
  }
}
