import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { Result } from "better-result";
import type { Config } from "../config";
import type { IndexRunRow, SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SegmentDiskCache } from "../segment/cache";
import { iterateBlocksResult } from "../segment/format";
import { siphash24 } from "../util/siphash";
import { retry } from "../util/retry";
import { indexRunObjectKey, segmentObjectKey, streamHash16Hex } from "../util/stream_paths";
import { binaryFuseContains, buildBinaryFuseResult } from "./binary_fuse";
import { decodeIndexRunResult, encodeIndexRunResult, RUN_TYPE_MASK16, RUN_TYPE_POSTINGS, type IndexRun } from "./run_format";
import { IndexRunCache } from "./run_cache";
import type { Metrics } from "../metrics";
import { dsError } from "../util/ds_error.ts";
import type { AggSegmentCompanion } from "../search/agg_format";
import type { ColSegmentCompanion } from "../search/col_format";
import type { FtsSegmentCompanion } from "../search/fts_format";
import type { MetricsBlockSegmentCompanion } from "../profiles/metrics/block_format";

export type IndexCandidate = { segments: Set<number>; indexedThrough: number };
type IndexBuildError = { kind: "invalid_index_build"; message: string };

export type StreamIndexLookup = {
  start(): void;
  stop(): void;
  enqueue(stream: string): void;
  candidateSegmentsForRoutingKey(stream: string, keyBytes: Uint8Array): Promise<IndexCandidate | null>;
  candidateSegmentsForSecondaryIndex(stream: string, indexName: string, keyBytes: Uint8Array): Promise<IndexCandidate | null>;
  getAggSegmentCompanion(stream: string, segmentIndex: number): Promise<AggSegmentCompanion | null>;
  getColSegmentCompanion(stream: string, segmentIndex: number): Promise<ColSegmentCompanion | null>;
  getFtsSegmentCompanion(stream: string, segmentIndex: number): Promise<FtsSegmentCompanion | null>;
  getMetricsBlockSegmentCompanion(stream: string, segmentIndex: number): Promise<MetricsBlockSegmentCompanion | null>;
};

function invalidIndexBuild<T = never>(message: string): Result<T, IndexBuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function errorMessage(e: unknown): string {
  return String((e as any)?.message ?? e);
}

export class IndexManager {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly segmentCache?: SegmentDiskCache;
  private readonly runDiskCache?: SegmentDiskCache;
  private readonly runCache: IndexRunCache;
  private readonly span: number;
  private readonly buildConcurrency: number;
  private readonly compactionFanout: number;
  private readonly maxLevel: number;
  private readonly compactionConcurrency: number;
  private readonly retireGenWindow: number;
  private readonly retireMinMs: number;
  private readonly queue = new Set<string>();
  private readonly building = new Set<string>();
  private readonly compacting = new Set<string>();
  private readonly metrics?: Metrics;
  private lastRunCacheHits = 0;
  private lastRunCacheMisses = 0;
  private lastRunCacheEvictions = 0;
  private lastDiskHits = 0;
  private lastDiskMisses = 0;
  private lastDiskEvictions = 0;
  private lastDiskBytesAdded = 0;
  private timer: any | null = null;
  private running = false;
  private readonly publishManifest?: (stream: string) => Promise<void>;
  private readonly onMetadataChanged?: (stream: string) => void;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    segmentCache: SegmentDiskCache | undefined,
    publishManifest?: (stream: string) => Promise<void>,
    metrics?: Metrics,
    onMetadataChanged?: (stream: string) => void
  ) {
    this.cfg = cfg;
    this.db = db;
    this.os = os;
    this.segmentCache = segmentCache;
    this.publishManifest = publishManifest;
    this.span = cfg.indexL0SpanSegments;
    this.buildConcurrency = Math.max(1, cfg.indexBuildConcurrency);
    this.compactionFanout = cfg.indexCompactionFanout;
    this.maxLevel = cfg.indexMaxLevel;
    this.compactionConcurrency = Math.max(1, cfg.indexCompactionConcurrency);
    this.retireGenWindow = Math.max(0, cfg.indexRetireGenWindow);
    this.retireMinMs = Math.max(0, cfg.indexRetireMinMs);
    this.metrics = metrics;
    this.onMetadataChanged = onMetadataChanged;
    this.runCache = new IndexRunCache(cfg.indexRunMemoryCacheBytes);
    this.runDiskCache = cfg.indexRunCacheMaxBytes > 0 ? new SegmentDiskCache(`${cfg.rootDir}/cache/index`, cfg.indexRunCacheMaxBytes) : undefined;
  }

  start(): void {
    if (this.span <= 0) return;
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
    if (this.span <= 0) return;
    this.queue.add(stream);
  }

  async candidateSegmentsForRoutingKey(stream: string, keyBytes: Uint8Array): Promise<IndexCandidate | null> {
    if (this.span <= 0) return null;
    const state = this.db.getIndexState(stream);
    if (!state) return null;
    const runs = this.db.listIndexRuns(stream);
    if (runs.length === 0 && state.indexed_through === 0) return null;

    const fp = siphash24(state.index_secret, keyBytes);
    const segments = new Set<number>();
    for (const meta of runs) {
      const runRes = await this.loadRunResult(meta);
      if (Result.isError(runRes)) continue;
      const run = runRes.value;
      if (!run) continue;
      if (run.filter && !binaryFuseContains(run.filter, fp)) continue;
      if (run.runType === RUN_TYPE_MASK16 && run.masks) {
        const idx = binarySearch(run.fingerprints, fp);
        if (idx >= 0) {
          const mask = run.masks[idx];
          for (let bit = 0; bit < 16; bit++) {
            if ((mask & (1 << bit)) !== 0) segments.add(run.meta.startSegment + bit);
          }
        }
      } else if (run.postings) {
        const idx = binarySearch(run.fingerprints, fp);
        if (idx >= 0) {
          for (const seg of run.postings[idx]) segments.add(seg);
        }
      }
    }
    return { segments, indexedThrough: state.indexed_through };
  }

  async candidateSegmentsForSecondaryIndex(_stream: string, _indexName: string, _keyBytes: Uint8Array): Promise<IndexCandidate | null> {
    return null;
  }

  async getColSegmentCompanion(_stream: string, _segmentIndex: number): Promise<ColSegmentCompanion | null> {
    return null;
  }

  async getAggSegmentCompanion(_stream: string, _segmentIndex: number): Promise<AggSegmentCompanion | null> {
    return null;
  }

  async getFtsSegmentCompanion(_stream: string, _segmentIndex: number): Promise<FtsSegmentCompanion | null> {
    return null;
  }

  async getMetricsBlockSegmentCompanion(_stream: string, _segmentIndex: number): Promise<MetricsBlockSegmentCompanion | null> {
    return null;
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      if (this.metrics) {
        this.metrics.record("tieredstore.index.build.queue_len", this.queue.size, "count");
        this.metrics.record("tieredstore.index.builds_inflight", this.building.size, "count");
      }
      const streams = Array.from(this.queue);
      this.queue.clear();
      for (const stream of streams) {
        try {
          const buildRes = await this.maybeBuildRuns(stream);
          if (Result.isError(buildRes)) {
            // eslint-disable-next-line no-console
            console.error("index build failed", stream, buildRes.error.message);
            this.queue.add(stream);
            continue;
          }
          const compactRes = await this.maybeCompactRuns(stream);
          if (Result.isError(compactRes)) {
            // eslint-disable-next-line no-console
            console.error("index compaction failed", stream, compactRes.error.message);
            this.queue.add(stream);
            continue;
          }
        } catch (e) {
          const msg = String((e as any)?.message ?? e);
          const lower = msg.toLowerCase();
          if (lower.includes("database has closed") || lower.includes("closed database") || lower.includes("statement has finalized")) {
            continue;
          }
          // eslint-disable-next-line no-console
          console.error("index build failed", stream, e);
          this.queue.add(stream);
        }
      }
      this.recordCacheStats();
    } finally {
      this.running = false;
    }
  }

  private async maybeBuildRuns(stream: string): Promise<Result<void, IndexBuildError>> {
    if (this.span <= 0) return Result.ok(undefined);
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      let state = this.db.getIndexState(stream);
      if (!state) {
        const secret = randomBytes(16);
        this.db.upsertIndexState(stream, secret, 0);
        state = this.db.getIndexState(stream);
      }
      if (!state) return Result.ok(undefined);
      if (this.metrics) {
        const lag = Math.max(0, this.db.countUploadedSegments(stream) - state.indexed_through);
        this.metrics.record("tieredstore.index.lag.segments", lag, "count", undefined, stream);
      }
      let indexedThrough = state.indexed_through;
      for (;;) {
        const uploadedCount = this.db.countUploadedSegments(stream);
        if (uploadedCount < indexedThrough + this.span) return Result.ok(undefined);
        const start = indexedThrough;
        const end = start + this.span - 1;
        const segments: SegmentRow[] = [];
        let ok = true;
        for (let i = start; i <= end; i++) {
          const seg = this.db.getSegmentByIndex(stream, i);
          if (!seg || !seg.r2_etag) {
            ok = false;
            break;
          }
          segments.push(seg);
        }
        if (!ok) return Result.ok(undefined);
        const t0 = Date.now();
        const runRes = await this.buildL0RunResult(stream, start, segments, state.index_secret);
        if (Result.isError(runRes)) return runRes;
        const run = runRes.value;
        const elapsedNs = BigInt(Date.now() - t0) * 1_000_000n;
        const persistRes = await this.persistRunResult(run, stream);
        if (Result.isError(persistRes)) return persistRes;
        this.db.insertIndexRun({
          run_id: run.meta.runId,
          stream,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        if (this.metrics) {
          this.metrics.record("tieredstore.index.build.latency", Number(elapsedNs), "ns", { level: String(run.meta.level) }, stream);
          this.metrics.record("tieredstore.index.runs.built", 1, "count", { level: String(run.meta.level) }, stream);
          this.recordActiveRuns(stream);
        }
        indexedThrough = end + 1;
        this.db.updateIndexedThrough(stream, indexedThrough);
        state.indexed_through = indexedThrough;
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore manifest publish errors; will be retried by uploader/indexer
          }
        }
      }
    } finally {
      this.building.delete(stream);
    }
  }

  private async maybeCompactRuns(stream: string): Promise<Result<void, IndexBuildError>> {
    if (this.span <= 0) return Result.ok(undefined);
    if (this.compactionFanout <= 1) return Result.ok(undefined);
    if (this.compacting.has(stream)) return Result.ok(undefined);
    this.compacting.add(stream);
    try {
      for (;;) {
        const group = this.findCompactionGroup(stream);
        if (!group) {
          await this.gcRetiredRuns(stream);
          return Result.ok(undefined);
        }
        const t0 = Date.now();
        const { level, runs } = group;
        const runRes = await this.buildCompactedRunResult(stream, level + 1, runs);
        if (Result.isError(runRes)) return runRes;
        const run = runRes.value;
        const elapsedNs = BigInt(Date.now() - t0) * 1_000_000n;
        const persistRes = await this.persistRunResult(run, stream);
        if (Result.isError(persistRes)) return persistRes;
        this.db.insertIndexRun({
          run_id: run.meta.runId,
          stream,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        const state = this.db.getIndexState(stream);
        if (state && run.meta.endSegment + 1 > state.indexed_through) {
          this.db.updateIndexedThrough(stream, run.meta.endSegment + 1);
          state.indexed_through = run.meta.endSegment + 1;
        }
        const manifestRow = this.db.getManifestRow(stream);
        const retiredGen = manifestRow.generation + 1;
        const nowMs = this.db.nowMs();
        this.db.retireIndexRuns(
          runs.map((r) => r.run_id),
          retiredGen,
          nowMs
        );
        this.onMetadataChanged?.(stream);
        if (this.metrics) {
          this.metrics.record("tieredstore.index.compact.latency", Number(elapsedNs), "ns", { level: String(run.meta.level) }, stream);
          this.metrics.record("tieredstore.index.runs.compacted", 1, "count", { level: String(run.meta.level) }, stream);
          this.recordActiveRuns(stream);
        }
        for (const r of runs) {
          this.runCache.remove(r.object_key);
          this.runDiskCache?.remove(r.object_key);
        }
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore manifest publish errors; will be retried
          }
        }
        await this.gcRetiredRuns(stream);
      }
    } finally {
      this.compacting.delete(stream);
    }
  }

  private findCompactionGroup(stream: string): { level: number; runs: IndexRunRow[] } | null {
    const runs = this.db.listIndexRuns(stream);
    if (runs.length < this.compactionFanout) return null;
    const byLevel = new Map<number, IndexRunRow[]>();
    for (const r of runs) {
      const arr = byLevel.get(r.level) ?? [];
      arr.push(r);
      byLevel.set(r.level, arr);
    }
    for (let level = 0; level <= this.maxLevel; level++) {
      const levelRuns = byLevel.get(level);
      if (!levelRuns || levelRuns.length < this.compactionFanout) continue;
      const span = this.levelSpan(level);
      for (let i = 0; i + this.compactionFanout <= levelRuns.length; i++) {
        const base = levelRuns[i].start_segment;
        let ok = true;
        for (let j = 0; j < this.compactionFanout; j++) {
          const r = levelRuns[i + j];
          const expectStart = base + j * span;
          if (r.level !== level || r.start_segment !== expectStart || r.end_segment !== expectStart + span - 1) {
            ok = false;
            break;
          }
        }
        if (ok) return { level, runs: levelRuns.slice(i, i + this.compactionFanout) };
      }
    }
    return null;
  }

  private levelSpan(level: number): number {
    let span = this.span;
    for (let i = 0; i < level; i++) span *= this.compactionFanout;
    return span;
  }

  private async buildCompactedRunResult(
    stream: string,
    level: number,
    inputs: IndexRunRow[]
  ): Promise<Result<IndexRun, IndexBuildError>> {
    if (inputs.length === 0) return invalidIndexBuild("compact: missing inputs");
    const segments = new Map<bigint, Set<number>>();
    const addSegment = (fp: bigint, seg: number) => {
      let set = segments.get(fp);
      if (!set) {
        set = new Set<number>();
        segments.set(fp, set);
      }
      set.add(seg);
    };

    const pending = inputs.slice();
    const results: Array<{ meta: IndexRunRow; run: IndexRun }> = [];
    const workers = Math.min(this.compactionConcurrency, pending.length);
    let buildError: string | null = null;
    const workerTasks: Promise<void>[] = [];
    for (let w = 0; w < workers; w++) {
      workerTasks.push(
        (async () => {
          for (;;) {
            if (buildError) return;
            const meta = pending.shift();
            if (!meta) return;
            const runRes = await this.loadRunResult(meta);
            if (Result.isError(runRes)) {
              buildError = runRes.error.message;
              return;
            }
            const run = runRes.value;
            if (!run) {
              buildError = `missing run ${meta.run_id}`;
              return;
            }
            results.push({ meta, run });
          }
        })()
      );
    }
    await Promise.all(workerTasks);
    if (buildError) return invalidIndexBuild(buildError);

    for (const res of results) {
      const run = res.run;
      const meta = res.meta;
      if (run.runType === RUN_TYPE_MASK16 && run.masks) {
        for (let i = 0; i < run.fingerprints.length; i++) {
          const fp = run.fingerprints[i];
          const mask = run.masks[i];
          for (let bit = 0; bit < 16; bit++) {
            if ((mask & (1 << bit)) === 0) continue;
            addSegment(fp, meta.start_segment + bit);
          }
        }
      } else if (run.runType === RUN_TYPE_POSTINGS && run.postings) {
        for (let i = 0; i < run.fingerprints.length; i++) {
          const fp = run.fingerprints[i];
          const postings = run.postings[i];
          for (const rel of postings) addSegment(fp, meta.start_segment + rel);
        }
      } else {
        return invalidIndexBuild(`unknown run type ${run.runType}`);
      }
    }

    const startSegment = inputs[0].start_segment;
    const endSegment = inputs[inputs.length - 1].end_segment;
    const pairs = Array.from(segments.entries())
      .map(([fp, set]) => {
        const list = Array.from(set);
        list.sort((a, b) => a - b);
        const rel = list.map((seg) => seg - startSegment);
        return { fp, rel };
      })
      .sort((a, b) => (a.fp < b.fp ? -1 : a.fp > b.fp ? 1 : 0));

    const fingerprints: bigint[] = [];
    const postings: number[][] = [];
    for (const p of pairs) {
      fingerprints.push(p.fp);
      postings.push(p.rel);
    }

    const fuseRes = buildBinaryFuseResult(fingerprints);
    if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
    const { filter, bytes } = fuseRes.value;
    const shash = streamHash16Hex(stream);
    const runId = `l${level}-${startSegment.toString().padStart(16, "0")}-${endSegment.toString().padStart(16, "0")}-${Date.now()}`;
    const objectKey = indexRunObjectKey(shash, runId);
    return Result.ok({
      meta: {
        runId,
        level,
        startSegment,
        endSegment,
        objectKey,
        filterLen: bytes.byteLength,
        recordCount: fingerprints.length,
      },
      runType: RUN_TYPE_POSTINGS,
      filterBytes: bytes,
      filter,
      fingerprints,
      postings,
    });
  }

  private async gcRetiredRuns(stream: string): Promise<void> {
    const retired = this.db.listRetiredIndexRuns(stream);
    if (retired.length === 0) return;
    const manifest = this.db.getManifestRow(stream);
    const nowMs = this.db.nowMs();
    const cutoffGen = this.retireGenWindow > 0 && manifest.generation > this.retireGenWindow ? manifest.generation - this.retireGenWindow : 0;
    const toDelete: IndexRunRow[] = [];
    for (const r of retired) {
      const expiredByGen = r.retired_gen != null && r.retired_gen > 0 && r.retired_gen <= cutoffGen;
      const expiredByTTL = r.retired_at_ms != null && r.retired_at_ms + BigInt(this.retireMinMs) <= nowMs;
      if (expiredByGen || expiredByTTL) toDelete.push(r);
    }
    if (toDelete.length === 0) return;
    for (const r of toDelete) {
      try {
        await this.os.delete(r.object_key);
      } catch {
        // ignore deletion errors
      }
      this.runCache.remove(r.object_key);
      this.runDiskCache?.remove(r.object_key);
    }
    this.db.deleteIndexRuns(toDelete.map((r) => r.run_id));
  }

  private async buildL0RunResult(
    stream: string,
    startSegment: number,
    segments: SegmentRow[],
    secret: Uint8Array
  ): Promise<Result<IndexRun, IndexBuildError>> {
    const maskByFp = new Map<bigint, number>();
    const pending = segments.slice();
    const concurrency = Math.max(1, Math.min(this.buildConcurrency, pending.length));
    const results: Array<Map<bigint, number>> = [];
    let buildError: string | null = null;
    const workers: Promise<void>[] = [];
    for (let i = 0; i < concurrency; i++) {
      workers.push(
        (async () => {
          for (;;) {
            if (buildError) return;
            const seg = pending.shift();
            if (!seg) return;
            const segBytesRes = await this.loadSegmentBytesResult(seg);
            if (Result.isError(segBytesRes)) {
              buildError = segBytesRes.error.message;
              return;
            }
            const segBytes = segBytesRes.value;
            const bit = seg.segment_index - startSegment;
            const maskBit = 1 << bit;
            const local = new Map<bigint, number>();
            for (const blockRes of iterateBlocksResult(segBytes)) {
              if (Result.isError(blockRes)) {
                buildError = blockRes.error.message;
                return;
              }
              const { decoded } = blockRes.value;
              for (const rec of decoded.records) {
                if (rec.routingKey.byteLength === 0) continue;
                const fp = siphash24(secret, rec.routingKey);
                const prev = local.get(fp) ?? 0;
                local.set(fp, prev | maskBit);
              }
            }
            results.push(local);
          }
        })()
      );
    }
    await Promise.all(workers);
    if (buildError) return invalidIndexBuild(buildError);
    for (const local of results) {
      for (const [fp, mask] of local.entries()) {
        const prev = maskByFp.get(fp) ?? 0;
        maskByFp.set(fp, prev | mask);
      }
    }
    const entries = Array.from(maskByFp.entries()).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    const fingerprints = entries.map(([fp]) => fp);
    const masks = entries.map(([, mask]) => mask);
    const fuseRes = buildBinaryFuseResult(fingerprints);
    if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
    const { filter, bytes } = fuseRes.value;
    const shash = streamHash16Hex(stream);
    const endSegment = startSegment + this.span - 1;
    const runId = `l0-${startSegment.toString().padStart(16, "0")}-${endSegment.toString().padStart(16, "0")}-${Date.now()}`;
    const objectKey = indexRunObjectKey(shash, runId);
    const run: IndexRun = {
      meta: {
        runId,
        level: 0,
        startSegment,
        endSegment,
        objectKey,
        filterLen: bytes.byteLength,
        recordCount: fingerprints.length,
      },
      runType: RUN_TYPE_MASK16,
      filterBytes: bytes,
      filter,
      fingerprints,
      masks,
    };
    return Result.ok(run);
  }

  private async persistRunResult(run: IndexRun, stream?: string): Promise<Result<void, IndexBuildError>> {
    const payloadRes = encodeIndexRunResult(run);
    if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
    const payload = payloadRes.value;
    if (this.metrics) {
      this.metrics.record("tieredstore.index.bytes.written", payload.byteLength, "bytes", { level: String(run.meta.level) }, stream);
    }
    try {
      await retry(
        () => this.os.put(run.meta.objectKey, payload, { contentLength: payload.byteLength }),
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
    } catch (e: any) {
      return invalidIndexBuild(String(e?.message ?? e));
    }
    this.runDiskCache?.put(run.meta.objectKey, payload);
    this.runCache.put(run.meta.objectKey, run);
    return Result.ok(undefined);
  }

  private async loadRunResult(meta: IndexRunRow): Promise<Result<IndexRun | null, IndexBuildError>> {
    const cached = this.runCache.get(meta.object_key);
    if (cached) return Result.ok(cached);
    let bytes: Uint8Array | null = null;
    if (this.runDiskCache) {
      try {
        bytes = this.runDiskCache.get(meta.object_key);
      } catch {
        this.runDiskCache.remove(meta.object_key);
      }
    }
    if (!bytes) {
      try {
        bytes = await retry(
          async () => {
            const data = await this.os.get(meta.object_key);
            if (!data) throw dsError(`missing index run ${meta.object_key}`);
            return data;
          },
          {
            retries: this.cfg.objectStoreRetries,
            baseDelayMs: this.cfg.objectStoreBaseDelayMs,
            maxDelayMs: this.cfg.objectStoreMaxDelayMs,
            timeoutMs: this.cfg.objectStoreTimeoutMs,
          }
        );
      } catch (e: unknown) {
        return invalidIndexBuild(errorMessage(e));
      }
      if (this.metrics) {
        this.metrics.record("tieredstore.index.bytes.read", bytes.byteLength, "bytes", { level: String(meta.level) }, meta.stream);
      }
      this.runDiskCache?.put(meta.object_key, bytes);
    }
    const runRes = decodeIndexRunResult(bytes);
    if (Result.isError(runRes)) {
      this.runDiskCache?.remove(meta.object_key);
      return Result.ok(null);
    }
    const run = runRes.value;
    run.meta.runId = meta.run_id;
    run.meta.objectKey = meta.object_key;
    run.meta.level = meta.level;
    run.meta.startSegment = meta.start_segment;
    run.meta.endSegment = meta.end_segment;
    run.meta.filterLen = meta.filter_len;
    run.meta.recordCount = meta.record_count;
    this.runCache.put(meta.object_key, run);
    return Result.ok(run);
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, IndexBuildError>> {
    if (seg.local_path && seg.local_path.length > 0) {
      try {
        return Result.ok(new Uint8Array(readFileSync(seg.local_path)));
      } catch {
        // fall through
      }
    }
    const diskCache = this.segmentCache;
    const key = segmentObjectKey(streamHash16Hex(seg.stream), seg.segment_index);
    if (diskCache && diskCache.has(key)) {
      diskCache.recordHit();
      diskCache.touch(key);
      try {
        return Result.ok(new Uint8Array(readFileSync(diskCache.getPath(key))));
      } catch {
        diskCache.remove(key);
      }
    }
    if (diskCache) diskCache.recordMiss();
    try {
      const data = await retry(
        async () => {
          const objectBytes = await this.os.get(key);
          if (!objectBytes) throw dsError(`missing segment ${seg.segment_id}`);
          if (diskCache) diskCache.put(key, objectBytes);
          return objectBytes;
        },
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
      return Result.ok(data);
    } catch (e: unknown) {
      return invalidIndexBuild(errorMessage(e));
    }
  }

  private recordCacheStats(): void {
    if (!this.metrics) return;
    const mem = this.runCache.stats();
    this.metrics.record("tieredstore.index.run_cache.used_bytes", mem.usedBytes, "bytes", { cache: "mem" });
    this.metrics.record("tieredstore.index.run_cache.entries", mem.entries, "count", { cache: "mem" });
    const deltaHits = mem.hits - this.lastRunCacheHits;
    const deltaMisses = mem.misses - this.lastRunCacheMisses;
    const deltaEvict = mem.evictions - this.lastRunCacheEvictions;
    if (deltaHits > 0) this.metrics.record("tieredstore.index.run_cache.hits", deltaHits, "count", { cache: "mem" });
    if (deltaMisses > 0) this.metrics.record("tieredstore.index.run_cache.misses", deltaMisses, "count", { cache: "mem" });
    if (deltaEvict > 0) this.metrics.record("tieredstore.index.run_cache.evictions", deltaEvict, "count", { cache: "mem" });
    this.lastRunCacheHits = mem.hits;
    this.lastRunCacheMisses = mem.misses;
    this.lastRunCacheEvictions = mem.evictions;

    if (this.runDiskCache) {
      const disk = this.runDiskCache.stats();
      this.metrics.record("tieredstore.index.run_cache.used_bytes", disk.usedBytes, "bytes", { cache: "disk" });
      this.metrics.record("tieredstore.index.run_cache.entries", disk.entryCount, "count", { cache: "disk" });
      const dh = disk.hits - this.lastDiskHits;
      const dm = disk.misses - this.lastDiskMisses;
      const de = disk.evictions - this.lastDiskEvictions;
      const db = disk.bytesAdded - this.lastDiskBytesAdded;
      if (dh > 0) this.metrics.record("tieredstore.index.run_cache.hits", dh, "count", { cache: "disk" });
      if (dm > 0) this.metrics.record("tieredstore.index.run_cache.misses", dm, "count", { cache: "disk" });
      if (de > 0) this.metrics.record("tieredstore.index.run_cache.evictions", de, "count", { cache: "disk" });
      if (db > 0) this.metrics.record("tieredstore.index.run_cache.bytes_added", db, "bytes", { cache: "disk" });
      this.lastDiskHits = disk.hits;
      this.lastDiskMisses = disk.misses;
      this.lastDiskEvictions = disk.evictions;
      this.lastDiskBytesAdded = disk.bytesAdded;
    }
  }

  private recordActiveRuns(stream: string): void {
    if (!this.metrics) return;
    const runs = this.db.listIndexRuns(stream);
    this.metrics.record("tieredstore.index.active_runs", runs.length, "count", undefined, stream);
    const byLevel = new Map<number, number>();
    for (const r of runs) byLevel.set(r.level, (byLevel.get(r.level) ?? 0) + 1);
    for (const [level, count] of byLevel.entries()) {
      this.metrics.record("tieredstore.index.active_runs", count, "count", { level: String(level) }, stream);
    }
  }
}

function binarySearch(arr: bigint[], target: bigint): number {
  let lo = 0;
  let hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const v = arr[mid];
    if (v === target) return mid;
    if (v < target) lo = mid + 1;
    else hi = mid - 1;
  }
  return -1;
}

// segmentObjectKey handles stream hash + path.
