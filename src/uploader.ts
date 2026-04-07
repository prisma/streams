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
import { retryAbortable } from "./util/retry";
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
  getRuntimeStats?: () => UploaderRuntimeStats;
  setHooks(hooks: UploaderHooks | undefined): void;
  publishManifest(stream: string): Promise<void>;
};

export type UploaderHooks = {
  onSegmentsUploaded?: (stream: string) => void;
  onMetadataChanged?: (stream: string) => void;
};

export type UploaderRuntimeStats = {
  last_tick_ms: number | null;
  last_select_ms: number | null;
  last_selected_segments: number;
  last_selected_streams: number;
  segment_attempts_total: number;
  segment_success_total: number;
  segment_failure_total: number;
  segment_timeout_total: number;
  segment_last_put_ms: number | null;
  segment_avg_put_ms: number | null;
  segment_max_put_ms: number | null;
  segment_last_mark_uploaded_ms: number | null;
  segment_avg_mark_uploaded_ms: number | null;
  segment_max_mark_uploaded_ms: number | null;
  segment_last_total_ms: number | null;
  segment_avg_total_ms: number | null;
  segment_max_total_ms: number | null;
  manifest_attempts_total: number;
  manifest_success_total: number;
  manifest_failure_total: number;
  manifest_timeout_total: number;
  manifest_last_build_ms: number | null;
  manifest_avg_build_ms: number | null;
  manifest_max_build_ms: number | null;
  manifest_last_put_ms: number | null;
  manifest_avg_put_ms: number | null;
  manifest_max_put_ms: number | null;
  manifest_last_commit_ms: number | null;
  manifest_avg_commit_ms: number | null;
  manifest_max_commit_ms: number | null;
  manifest_last_total_ms: number | null;
  manifest_avg_total_ms: number | null;
  manifest_max_total_ms: number | null;
};

type DurationStats = {
  count: number;
  totalMs: number;
  maxMs: number;
  lastMs: number | null;
};

function createDurationStats(): DurationStats {
  return { count: 0, totalMs: 0, maxMs: 0, lastMs: null };
}

function recordDuration(stats: DurationStats, ms: number): void {
  const safeMs = Number.isFinite(ms) ? Math.max(0, ms) : 0;
  stats.count += 1;
  stats.totalMs += safeMs;
  stats.maxMs = Math.max(stats.maxMs, safeMs);
  stats.lastMs = safeMs;
}

function averageDuration(stats: DurationStats): number | null {
  if (stats.count <= 0) return null;
  return stats.totalMs / stats.count;
}

function maxDuration(stats: DurationStats): number | null {
  if (stats.count <= 0) return null;
  return stats.maxMs;
}

function isTimeoutError(err: unknown): boolean {
  const message = String((err as any)?.message ?? err ?? "").toLowerCase();
  const code = String((err as any)?.code ?? "");
  return message.includes("timeout") || code === "ETIMEDOUT";
}

function countSegmentsThroughOffset(segmentOffsets: Uint8Array, segmentCount: number, uploadedThrough: bigint): number {
  if (uploadedThrough < 0n || segmentCount <= 0) return 0;
  const target = uploadedThrough + 1n;
  let lo = 0;
  let hi = segmentCount;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const endPlusOne = readU64LE(segmentOffsets, mid * 8);
    if (endPlusOne <= target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

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
  private lastTickMs: number | null = null;
  private lastSelectMs: number | null = null;
  private lastSelectedSegments = 0;
  private lastSelectedStreams = 0;
  private segmentAttemptsTotal = 0;
  private segmentSuccessTotal = 0;
  private segmentFailureTotal = 0;
  private segmentTimeoutTotal = 0;
  private readonly segmentPutMs = createDurationStats();
  private readonly segmentMarkUploadedMs = createDurationStats();
  private readonly segmentTotalMs = createDurationStats();
  private manifestAttemptsTotal = 0;
  private manifestSuccessTotal = 0;
  private manifestFailureTotal = 0;
  private manifestTimeoutTotal = 0;
  private readonly manifestBuildMs = createDurationStats();
  private readonly manifestPutMs = createDurationStats();
  private readonly manifestCommitMs = createDurationStats();
  private readonly manifestTotalMs = createDurationStats();

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

  getRuntimeStats(): UploaderRuntimeStats {
    return {
      last_tick_ms: this.lastTickMs,
      last_select_ms: this.lastSelectMs,
      last_selected_segments: this.lastSelectedSegments,
      last_selected_streams: this.lastSelectedStreams,
      segment_attempts_total: this.segmentAttemptsTotal,
      segment_success_total: this.segmentSuccessTotal,
      segment_failure_total: this.segmentFailureTotal,
      segment_timeout_total: this.segmentTimeoutTotal,
      segment_last_put_ms: this.segmentPutMs.lastMs,
      segment_avg_put_ms: averageDuration(this.segmentPutMs),
      segment_max_put_ms: maxDuration(this.segmentPutMs),
      segment_last_mark_uploaded_ms: this.segmentMarkUploadedMs.lastMs,
      segment_avg_mark_uploaded_ms: averageDuration(this.segmentMarkUploadedMs),
      segment_max_mark_uploaded_ms: maxDuration(this.segmentMarkUploadedMs),
      segment_last_total_ms: this.segmentTotalMs.lastMs,
      segment_avg_total_ms: averageDuration(this.segmentTotalMs),
      segment_max_total_ms: maxDuration(this.segmentTotalMs),
      manifest_attempts_total: this.manifestAttemptsTotal,
      manifest_success_total: this.manifestSuccessTotal,
      manifest_failure_total: this.manifestFailureTotal,
      manifest_timeout_total: this.manifestTimeoutTotal,
      manifest_last_build_ms: this.manifestBuildMs.lastMs,
      manifest_avg_build_ms: averageDuration(this.manifestBuildMs),
      manifest_max_build_ms: maxDuration(this.manifestBuildMs),
      manifest_last_put_ms: this.manifestPutMs.lastMs,
      manifest_avg_put_ms: averageDuration(this.manifestPutMs),
      manifest_max_put_ms: maxDuration(this.manifestPutMs),
      manifest_last_commit_ms: this.manifestCommitMs.lastMs,
      manifest_avg_commit_ms: averageDuration(this.manifestCommitMs),
      manifest_max_commit_ms: maxDuration(this.manifestCommitMs),
      manifest_last_total_ms: this.manifestTotalMs.lastMs,
      manifest_avg_total_ms: averageDuration(this.manifestTotalMs),
      manifest_max_total_ms: maxDuration(this.manifestTotalMs),
    };
  }

  private async tick(): Promise<void> {
    if (this.stopping) return;
    if (this.running) return;
    this.running = true;
    const tickStart = performance.now();
    try {
      const selectStart = performance.now();
      const pending = this.db.pendingUploadWindow(Math.max(1000, this.config.uploadConcurrency * 16), this.config.uploadConcurrency);
      this.lastSelectMs = performance.now() - selectStart;
      this.lastSelectedSegments = 0;
      this.lastSelectedStreams = 0;
      if (pending.length === 0) return;

      // Upload with bounded concurrency.
      const queue = pending.filter((s) => !this.inflight.has(s.segment_id) && !this.failures.shouldSkip(s.stream));
      this.lastSelectedSegments = queue.length;
      if (queue.length === 0) return;
      const streams = new Set(queue.map((s) => s.stream));
      this.lastSelectedStreams = streams.size;

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

      // Publish manifests for affected streams without blocking the next upload tick.
      for (const stream of streams) {
        if (this.failures.shouldSkip(stream)) continue;
        this.scheduleManifestPublish(stream);
      }
    } catch (e) {
      const msg = String((e as any)?.message ?? e);
      const lower = msg.toLowerCase();
      if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
        // eslint-disable-next-line no-console
        console.error("uploader tick error", e);
      }
    } finally {
      this.lastTickMs = performance.now() - tickStart;
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

  private scheduleManifestPublish(stream: string): void {
    void this.publishManifest(stream).catch((e) => {
      const msg = String((e as any)?.message ?? e);
      const lower = msg.toLowerCase();
      if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
        // eslint-disable-next-line no-console
        console.error("manifest publish failed", stream, e);
      }
    });
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
    const attemptStart = performance.now();
    this.segmentAttemptsTotal += 1;
    try {
      const putStart = performance.now();
      let res;
      try {
        res = await retryAbortable(
          async (signal) => {
            if (this.os.putFile) {
              return this.os.putFile(objectKey, seg.local_path, seg.size_bytes, { signal });
            }
            const bytes = new Uint8Array(await readFile(seg.local_path));
            return this.os.put(objectKey, bytes, { contentLength: seg.size_bytes, signal });
          },
          {
            retries: this.config.objectStoreRetries,
            baseDelayMs: this.config.objectStoreBaseDelayMs,
            maxDelayMs: this.config.objectStoreMaxDelayMs,
            timeoutMs: this.config.objectStoreTimeoutMs,
          }
        );
      } finally {
        recordDuration(this.segmentPutMs, performance.now() - putStart);
      }
      const markStart = performance.now();
      this.db.markSegmentUploaded(seg.segment_id, res.etag, this.db.nowMs());
      recordDuration(this.segmentMarkUploadedMs, performance.now() - markStart);
      this.segmentSuccessTotal += 1;
      this.hooks?.onMetadataChanged?.(seg.stream);
      if (this.stats) this.stats.recordUploadedBytes(seg.size_bytes);
      if (this.gate) this.gate.adjustOnUpload(seg.size_bytes);
    } catch (e) {
      this.segmentFailureTotal += 1;
      if (isTimeoutError(e)) this.segmentTimeoutTotal += 1;
      this.failures.recordFailure(seg.stream);
      throw e;
    } finally {
      recordDuration(this.segmentTotalMs, performance.now() - attemptStart);
      leaveUploadPhase?.();
    }
  }

  async publishManifest(stream: string): Promise<void> {
    if (this.stopping) return;
    if (this.manifestInflight.has(stream)) return;
    this.manifestInflight.add(stream);
    const publishStart = performance.now();
    this.manifestAttemptsTotal += 1;
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

      const previouslyPublishedPrefix = countSegmentsThroughOffset(meta.segment_offsets, meta.segment_count, srow.uploaded_through);
      const uploadedThrough =
        uploadedPrefix === 0 ? -1n : readU64LE(meta.segment_offsets, (uploadedPrefix - 1) * 8) - 1n;
      const publishedLogicalSizeBytes = this.db.getSegmentPayloadBytesRange(stream, 0, uploadedPrefix);
      const newlyPublishedLogicalSizeBytes = this.db.getSegmentPayloadBytesRange(
        stream,
        previouslyPublishedPrefix,
        uploadedPrefix
      );
      const newlyPublishedRows =
        uploadedThrough > srow.uploaded_through ? uploadedThrough - srow.uploaded_through : 0n;

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
      const buildStart = performance.now();
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
      recordDuration(this.manifestBuildMs, performance.now() - buildStart);

      const shash = streamHash16Hex(stream);
      const mKey = manifestObjectKey(shash);
      const body = new TextEncoder().encode(JSON.stringify(manifest));
      let putRes;
      const putStart = performance.now();
      try {
        putRes = await retryAbortable(
          (signal) => this.os.put(mKey, body, { signal }),
          {
            retries: this.config.objectStoreRetries,
            baseDelayMs: this.config.objectStoreBaseDelayMs,
            maxDelayMs: this.config.objectStoreMaxDelayMs,
            timeoutMs: this.config.objectStoreTimeoutMs,
          }
        );
      } finally {
        recordDuration(this.manifestPutMs, performance.now() - putStart);
      }

      // Commit point: advance uploaded_through and delete WAL prefix.
      const commitStart = performance.now();
      this.db.commitManifest(stream, generation, putRes.etag, this.db.nowMs(), uploadedThrough, body.byteLength, {
        deletedRows: newlyPublishedRows,
        deletedBytes: newlyPublishedLogicalSizeBytes,
      });
      recordDuration(this.manifestCommitMs, performance.now() - commitStart);
      this.manifestSuccessTotal += 1;
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
    } catch (e) {
      this.manifestFailureTotal += 1;
      if (isTimeoutError(e)) this.manifestTimeoutTotal += 1;
      this.failures.recordFailure(stream);
      throw e;
    } finally {
      recordDuration(this.manifestTotalMs, performance.now() - publishStart);
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
