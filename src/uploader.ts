import { unlinkSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { Result } from "better-result";
import type { Config } from "./config";
import type { SqliteDurableStore, SegmentRow } from "./db/db";
import type { ObjectStore } from "./objectstore/interface";
import { buildManifestResult } from "./manifest";
import { manifestObjectKey, segmentObjectKey, streamHash16Hex } from "./util/stream_paths";
import { readU64LE } from "./util/endian";
import { SegmentDiskCache } from "./segment/cache";
import { retry } from "./util/retry";
import { LruCache } from "./util/lru";
import type { StatsCollector } from "./stats";
import type { BackpressureGate } from "./backpressure";
import { dsError } from "./util/ds_error.ts";
import { RuntimeMemorySampler } from "./runtime_memory_sampler";

export type UploaderController = {
  start(): void;
  stop(hard?: boolean): void;
  countSegmentsWaiting(): number;
  getMemoryStats?: () => {
    inflight_segments: number;
    inflight_segment_bytes: number;
    manifest_inflight_streams: number;
  };
  setHooks(hooks: UploaderHooks | undefined): void;
  publishManifest(stream: string): Promise<void>;
};

export type UploaderHooks = {
  onSegmentsUploaded?: (stream: string) => void;
  onMetadataChanged?: (stream: string) => void;
};

export class Uploader {
  private readonly config: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly diskCache?: SegmentDiskCache;
  private readonly stats?: StatsCollector;
  private readonly gate?: BackpressureGate;
  private readonly memorySampler?: RuntimeMemorySampler;
  private timer: any | null = null;
  private running = false;
  private stopping = false;
  private readonly inflight = new Set<string>();
  private readonly failures = new FailureTracker(1024);
  private hooks?: UploaderHooks;
  private readonly manifestInflight = new Set<string>();
  private inflightSegmentBytes = 0;

  constructor(
    config: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    diskCache?: SegmentDiskCache,
    stats?: StatsCollector,
    gate?: BackpressureGate,
    hooks?: UploaderHooks,
    memorySampler?: RuntimeMemorySampler
  ) {
    this.config = config;
    this.db = db;
    this.os = os;
    this.diskCache = diskCache;
    this.stats = stats;
    this.gate = gate;
    this.hooks = hooks;
    this.memorySampler = memorySampler;
  }

  setHooks(hooks: UploaderHooks | undefined): void {
    this.hooks = hooks;
  }

  start(): void {
    this.stopping = false;
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.config.uploadIntervalMs);
  }

  stop(hard = false): void {
    if (hard) this.stopping = true;
    else this.stopping = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  countSegmentsWaiting(): number {
    return this.db.countPendingSegments();
  }

  getMemoryStats(): { inflight_segments: number; inflight_segment_bytes: number; manifest_inflight_streams: number } {
    return {
      inflight_segments: this.inflight.size,
      inflight_segment_bytes: this.inflightSegmentBytes,
      manifest_inflight_streams: this.manifestInflight.size,
    };
  }

  private async tick(): Promise<void> {
    if (this.stopping) return;
    if (this.running) return;
    this.running = true;
    try {
      const pending = this.db.pendingUploadHeads(1000);
      if (pending.length === 0) return;

      // Upload with bounded concurrency.
      const queue = pending.filter((s) => !this.inflight.has(s.segment_id) && !this.failures.shouldSkip(s.stream));
      if (queue.length === 0) return;
      const streams = new Set(queue.map((s) => s.stream));

      const workers: Promise<void>[] = [];
      for (let i = 0; i < this.config.uploadConcurrency; i++) {
        workers.push(this.uploadWorker(queue));
      }
      await Promise.all(workers);

      // Notify indexer / listeners.
      if (this.hooks?.onSegmentsUploaded) {
        for (const stream of streams) {
          try {
            this.hooks.onSegmentsUploaded(stream);
          } catch {
            // ignore
          }
        }
      }

      // Publish manifests for affected streams.
      for (const stream of streams) {
        if (this.failures.shouldSkip(stream)) continue;
        try {
          await this.publishManifest(stream);
        } catch (e) {
          const msg = String((e as any)?.message ?? e);
          const lower = msg.toLowerCase();
          if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
            // eslint-disable-next-line no-console
            console.error("manifest publish failed", stream, e);
          }
        }
      }
    } catch (e) {
      const msg = String((e as any)?.message ?? e);
      const lower = msg.toLowerCase();
      if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
        // eslint-disable-next-line no-console
        console.error("uploader tick error", e);
      }
    } finally {
      this.running = false;
    }
  }

  private async uploadWorker(queue: SegmentRow[]): Promise<void> {
    while (queue.length > 0) {
      if (this.stopping) return;
      const seg = queue.shift();
      if (!seg) return;
      if (this.inflight.has(seg.segment_id)) continue;
      this.inflight.add(seg.segment_id);
      this.inflightSegmentBytes += Math.max(0, seg.size_bytes);
      try {
        try {
          await this.uploadOne(seg);
          this.failures.recordSuccess(seg.stream);
        } catch (e) {
          const msg = String((e as any)?.message ?? e);
          const lower = msg.toLowerCase();
          if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
            // eslint-disable-next-line no-console
            console.error("segment upload failed", seg.segment_id, e);
          }
        }
      } finally {
        this.inflight.delete(seg.segment_id);
        this.inflightSegmentBytes = Math.max(0, this.inflightSegmentBytes - Math.max(0, seg.size_bytes));
      }
    }
  }

  private async uploadOne(seg: SegmentRow): Promise<void> {
    if (this.stopping) return;
    const shash = streamHash16Hex(seg.stream);
    const objectKey = segmentObjectKey(shash, seg.segment_index);
    const leaveUploadPhase = this.memorySampler?.enter("upload", {
      stream: seg.stream,
      segment_index: seg.segment_index,
      size_bytes: seg.size_bytes,
    });
    try {
      const res = await retry(
        async () => {
          if (this.os.putFile) {
            return this.os.putFile(objectKey, seg.local_path, seg.size_bytes);
          }
          const bytes = new Uint8Array(await readFile(seg.local_path));
          return this.os.put(objectKey, bytes, { contentLength: seg.size_bytes });
        },
        {
          retries: this.config.objectStoreRetries,
          baseDelayMs: this.config.objectStoreBaseDelayMs,
          maxDelayMs: this.config.objectStoreMaxDelayMs,
          timeoutMs: this.config.objectStoreTimeoutMs,
        }
      );
      this.db.markSegmentUploaded(seg.segment_id, res.etag, this.db.nowMs());
      this.hooks?.onMetadataChanged?.(seg.stream);
      if (this.stats) this.stats.recordUploadedBytes(seg.size_bytes);
      if (this.gate) this.gate.adjustOnUpload(seg.size_bytes);
    } catch (e) {
      this.failures.recordFailure(seg.stream);
      throw e;
    } finally {
      leaveUploadPhase?.();
    }
  }

  async publishManifest(stream: string): Promise<void> {
    if (this.stopping) return;
    if (this.manifestInflight.has(stream)) return;
    this.manifestInflight.add(stream);
    try {
      const srow = this.db.getStream(stream);
      if (!srow) return;

      const prevPrefix = srow.uploaded_segment_count ?? 0;
      let uploadedPrefix = this.db.advanceUploadedSegmentCount(stream);

      const segCount = this.db.countSegmentsForStream(stream);
      let meta = this.db.getSegmentMeta(stream);
      const needsRebuild =
        !meta ||
        meta.segment_count !== segCount ||
        meta.segment_offsets.byteLength !== segCount * 8 ||
        meta.segment_blocks.byteLength !== segCount * 4 ||
        meta.segment_last_ts.byteLength !== segCount * 8;
      if (needsRebuild) {
        meta = this.db.rebuildSegmentMeta(stream);
      }
      if (!meta) return;
      if (uploadedPrefix > meta.segment_count) {
        uploadedPrefix = meta.segment_count;
        this.db.setUploadedSegmentCount(stream, uploadedPrefix);
      }

      const uploadedThrough =
        uploadedPrefix === 0 ? -1n : readU64LE(meta.segment_offsets, (uploadedPrefix - 1) * 8) - 1n;
      const unpublishedWalBytes = this.db.getWalBytesAfterOffset(stream, uploadedThrough);
      const publishedLogicalSizeBytes =
        srow.logical_size_bytes > unpublishedWalBytes ? srow.logical_size_bytes - unpublishedWalBytes : 0n;

      const manifestRow = this.db.getManifestRow(stream);
      const generation = manifestRow.generation + 1;

      const indexState = this.db.getIndexState(stream);
      const indexRuns = this.db.listIndexRuns(stream);
      const retiredRuns = this.db.listRetiredIndexRuns(stream);
      const secondaryIndexStates = this.db.listSecondaryIndexStates(stream);
      const secondaryIndexRuns = secondaryIndexStates.flatMap((state) => this.db.listSecondaryIndexRuns(stream, state.index_name));
      const retiredSecondaryIndexRuns = secondaryIndexStates.flatMap((state) =>
        this.db.listRetiredSecondaryIndexRuns(stream, state.index_name)
      );
      const lexiconIndexStates = this.db.listLexiconIndexStates(stream);
      const lexiconIndexRuns = lexiconIndexStates.flatMap((state) =>
        this.db.listLexiconIndexRuns(stream, state.source_kind, state.source_name)
      );
      const retiredLexiconIndexRuns = lexiconIndexStates.flatMap((state) =>
        this.db.listRetiredLexiconIndexRuns(stream, state.source_kind, state.source_name)
      );
      const searchCompanionPlan = this.db.getSearchCompanionPlan(stream);
      const searchSegmentCompanions = this.db.listSearchSegmentCompanions(stream);
      let profileJson: Record<string, any> | null = null;
      const profileRow = this.db.getStreamProfile(stream);
      if (profileRow) {
        try {
          profileJson = JSON.parse(profileRow.profile_json);
        } catch {
          this.failures.recordFailure(stream);
          throw dsError(`invalid profile_json for ${stream}`);
        }
      }
      const manifestRes = buildManifestResult({
        streamName: stream,
        streamRow: srow,
        publishedLogicalSizeBytes,
        profileJson,
        segmentMeta: meta,
        uploadedPrefixCount: uploadedPrefix,
        generation,
        indexState,
        indexRuns,
        retiredRuns,
        secondaryIndexStates,
        secondaryIndexRuns,
        retiredSecondaryIndexRuns,
        lexiconIndexStates,
        lexiconIndexRuns,
        retiredLexiconIndexRuns,
        searchCompanionPlan,
        searchSegmentCompanions,
      });
      if (Result.isError(manifestRes)) {
        this.failures.recordFailure(stream);
        throw dsError(manifestRes.error.message);
      }
      const manifest = manifestRes.value;

      const shash = streamHash16Hex(stream);
      const mKey = manifestObjectKey(shash);
      const body = new TextEncoder().encode(JSON.stringify(manifest));
      let putRes;
      try {
        putRes = await retry(
          () => this.os.put(mKey, body),
          {
            retries: this.config.objectStoreRetries,
            baseDelayMs: this.config.objectStoreBaseDelayMs,
            maxDelayMs: this.config.objectStoreMaxDelayMs,
            timeoutMs: this.config.objectStoreTimeoutMs,
          }
        );
      } catch (e) {
        this.failures.recordFailure(stream);
        throw e;
      }

      // Commit point: advance uploaded_through and delete WAL prefix.
      this.db.commitManifest(stream, generation, putRes.etag, this.db.nowMs(), uploadedThrough, body.byteLength);
      this.hooks?.onMetadataChanged?.(stream);

      // Local disk cleanup: delete newly uploaded segment files.
      if (uploadedPrefix > prevPrefix) {
        for (let i = prevPrefix; i < uploadedPrefix; i++) {
          const seg = this.db.getSegmentByIndex(stream, i);
          if (!seg) continue;
          try {
            const objectKey = segmentObjectKey(shash, seg.segment_index);
            if (this.diskCache && this.diskCache.putFromLocal(objectKey, seg.local_path, seg.size_bytes)) {
              continue;
            }
            unlinkSync(seg.local_path);
          } catch {
            // ignore
          }
        }
      }
    } finally {
      this.manifestInflight.delete(stream);
    }
  }
}

class FailureTracker {
  private readonly cache: LruCache<string, { attempts: number; untilMs: number }>;

  constructor(maxEntries: number) {
    this.cache = new LruCache(maxEntries);
  }

  shouldSkip(stream: string): boolean {
    const item = this.cache.get(stream);
    if (!item) return false;
    if (Date.now() >= item.untilMs) {
      this.cache.delete(stream);
      return false;
    }
    return true;
  }

  recordFailure(stream: string): void {
    const now = Date.now();
    const item = this.cache.get(stream) ?? { attempts: 0, untilMs: now };
    item.attempts += 1;
    const backoff = Math.min(60_000, 500 * 2 ** (item.attempts - 1));
    item.untilMs = now + backoff;
    this.cache.set(stream, item);
  }

  recordSuccess(stream: string): void {
    this.cache.delete(stream);
  }
}
