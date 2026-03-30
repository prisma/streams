import { randomBytes } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { Result } from "better-result";
import type { Config } from "../config";
import type { SecondaryIndexRunRow, SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SchemaRegistryStore } from "../schema/registry";
import { SegmentDiskCache } from "../segment/cache";
import { iterateBlocksResult } from "../segment/format";
import { retry } from "../util/retry";
import { dsError } from "../util/ds_error.ts";
import { secondaryIndexRunObjectKey, segmentObjectKey, streamHash16Hex } from "../util/stream_paths";
import { siphash24 } from "../util/siphash";
import { binaryFuseContains, buildBinaryFuseResult } from "./binary_fuse";
import { IndexRunCache } from "./run_cache";
import {
  decodeIndexRunResult,
  encodeIndexRunResult,
  RUN_TYPE_MASK16,
  RUN_TYPE_POSTINGS,
  type IndexRun,
} from "./run_format";
import {
  extractSecondaryIndexTermsResult,
  getConfiguredSecondaryIndexes,
  hashSecondaryIndexField,
  type SecondaryIndexField,
} from "./secondary_schema";

type SecondaryIndexBuildError = { kind: "invalid_index_build"; message: string };

function invalidIndexBuild<T = never>(message: string): Result<T, SecondaryIndexBuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function binarySearch(values: bigint[], needle: bigint): number {
  let lo = 0;
  let hi = values.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const cur = values[mid];
    if (cur === needle) return mid;
    if (cur < needle) lo = mid + 1;
    else hi = mid - 1;
  }
  return -1;
}

export class SecondaryIndexManager {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly registry: SchemaRegistryStore;
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
  private timer: any | null = null;
  private running = false;
  private readonly publishManifest?: (stream: string) => Promise<void>;
  private readonly onMetadataChanged?: (stream: string) => void;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    registry: SchemaRegistryStore,
    publishManifest?: (stream: string) => Promise<void>,
    onMetadataChanged?: (stream: string) => void
  ) {
    this.cfg = cfg;
    this.db = db;
    this.os = os;
    this.registry = registry;
    this.publishManifest = publishManifest;
    this.onMetadataChanged = onMetadataChanged;
    this.span = cfg.indexL0SpanSegments;
    this.buildConcurrency = Math.max(1, cfg.indexBuildConcurrency);
    this.compactionFanout = cfg.indexCompactionFanout;
    this.maxLevel = cfg.indexMaxLevel;
    this.compactionConcurrency = Math.max(1, cfg.indexCompactionConcurrency);
    this.retireGenWindow = Math.max(0, cfg.indexRetireGenWindow);
    this.retireMinMs = Math.max(0, cfg.indexRetireMinMs);
    this.runCache = new IndexRunCache(cfg.indexRunMemoryCacheBytes);
    this.runDiskCache =
      cfg.indexRunCacheMaxBytes > 0
        ? new SegmentDiskCache(`${cfg.rootDir}/cache/secondary-index`, cfg.indexRunCacheMaxBytes)
        : undefined;
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

  async candidateSegmentsForSecondaryIndex(
    stream: string,
    indexName: string,
    keyBytes: Uint8Array
  ): Promise<{ segments: Set<number>; indexedThrough: number } | null> {
    if (this.span <= 0) return null;
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return null;
    const configured = getConfiguredSecondaryIndexes(regRes.value).find((entry) => entry.name === indexName);
    if (!configured) return null;
    const state = this.db.getSecondaryIndexState(stream, indexName);
    if (!state) return null;
    if (state.config_hash !== hashSecondaryIndexField(configured)) return null;
    const runs = this.db.listSecondaryIndexRuns(stream, indexName);
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

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const streams = Array.from(this.queue);
      this.queue.clear();
      for (const stream of streams) {
        const regRes = this.registry.getRegistryResult(stream);
        if (Result.isError(regRes)) continue;
        const configured = getConfiguredSecondaryIndexes(regRes.value);
        const configuredNames = new Set(configured.map((entry) => entry.name));
        const existing = this.db.listSecondaryIndexStates(stream);
        let removedAny = false;
        for (const state of existing) {
          if (configuredNames.has(state.index_name)) continue;
          this.db.deleteSecondaryIndex(stream, state.index_name);
          removedAny = true;
        }
        if (removedAny) {
          this.onMetadataChanged?.(stream);
          if (this.publishManifest) {
            try {
              await this.publishManifest(stream);
            } catch {
              // ignore and retry on next enqueue
            }
          }
        }
        for (const index of configured) {
          try {
            const buildRes = await this.maybeBuildRuns(stream, index);
            if (Result.isError(buildRes)) {
              this.queue.add(stream);
              continue;
            }
            const compactRes = await this.maybeCompactRuns(stream, index.name);
            if (Result.isError(compactRes)) {
              this.queue.add(stream);
              continue;
            }
          } catch (e) {
            const msg = String((e as any)?.message ?? e).toLowerCase();
            if (!msg.includes("database has closed") && !msg.includes("closed database") && !msg.includes("statement has finalized")) {
              // eslint-disable-next-line no-console
              console.error("secondary index build failed", stream, index.name, e);
            }
            this.queue.add(stream);
          }
        }
      }
    } finally {
      this.running = false;
    }
  }

  private async maybeBuildRuns(stream: string, index: SecondaryIndexField): Promise<Result<void, SecondaryIndexBuildError>> {
    if (this.span <= 0) return Result.ok(undefined);
    const key = `${stream}:${index.name}`;
    if (this.building.has(key)) return Result.ok(undefined);
    this.building.add(key);
    try {
      const configHash = hashSecondaryIndexField(index);
      let state = this.db.getSecondaryIndexState(stream, index.name);
      if (!state) {
        this.db.upsertSecondaryIndexState(stream, index.name, randomBytes(16), configHash, 0);
        state = this.db.getSecondaryIndexState(stream, index.name);
      } else if (state.config_hash !== configHash) {
        this.db.deleteSecondaryIndex(stream, index.name);
        this.db.upsertSecondaryIndexState(stream, index.name, randomBytes(16), configHash, 0);
        state = this.db.getSecondaryIndexState(stream, index.name);
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore and retry later
          }
        }
      }
      if (!state) return Result.ok(undefined);

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

        const runRes = await this.buildL0RunResult(stream, index, start, segments, state.index_secret);
        if (Result.isError(runRes)) return runRes;
        const run = runRes.value;
        const persistRes = await this.persistRunResult(run);
        if (Result.isError(persistRes)) return persistRes;
        this.db.insertSecondaryIndexRun({
          run_id: run.meta.runId,
          stream,
          index_name: index.name,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        indexedThrough = end + 1;
        this.db.updateSecondaryIndexedThrough(stream, index.name, indexedThrough);
        state.indexed_through = indexedThrough;
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore and retry later
          }
        }
      }
    } finally {
      this.building.delete(key);
    }
  }

  private async maybeCompactRuns(stream: string, indexName: string): Promise<Result<void, SecondaryIndexBuildError>> {
    if (this.span <= 0) return Result.ok(undefined);
    if (this.compactionFanout <= 1) return Result.ok(undefined);
    const key = `${stream}:${indexName}`;
    if (this.compacting.has(key)) return Result.ok(undefined);
    this.compacting.add(key);
    try {
      for (;;) {
        const group = this.findCompactionGroup(stream, indexName);
        if (!group) {
          await this.gcRetiredRuns(stream, indexName);
          return Result.ok(undefined);
        }
        const { level, runs } = group;
        const runRes = await this.buildCompactedRunResult(stream, indexName, level + 1, runs);
        if (Result.isError(runRes)) return runRes;
        const run = runRes.value;
        const persistRes = await this.persistRunResult(run);
        if (Result.isError(persistRes)) return persistRes;
        this.db.insertSecondaryIndexRun({
          run_id: run.meta.runId,
          stream,
          index_name: indexName,
          level: run.meta.level,
          start_segment: run.meta.startSegment,
          end_segment: run.meta.endSegment,
          object_key: run.meta.objectKey,
          filter_len: run.meta.filterLen,
          record_count: run.meta.recordCount,
        });
        const state = this.db.getSecondaryIndexState(stream, indexName);
        if (state && run.meta.endSegment + 1 > state.indexed_through) {
          this.db.updateSecondaryIndexedThrough(stream, indexName, run.meta.endSegment + 1);
        }
        const manifestRow = this.db.getManifestRow(stream);
        this.db.retireSecondaryIndexRuns(
          runs.map((r) => r.run_id),
          manifestRow.generation + 1,
          this.db.nowMs()
        );
        this.onMetadataChanged?.(stream);
        if (this.publishManifest) {
          try {
            await this.publishManifest(stream);
          } catch {
            // ignore and retry later
          }
        }
        await this.gcRetiredRuns(stream, indexName);
      }
    } finally {
      this.compacting.delete(key);
    }
  }

  private findCompactionGroup(stream: string, indexName: string): { level: number; runs: SecondaryIndexRunRow[] } | null {
    const runs = this.db.listSecondaryIndexRuns(stream, indexName);
    if (runs.length < this.compactionFanout) return null;
    const byLevel = new Map<number, SecondaryIndexRunRow[]>();
    for (const run of runs) {
      const arr = byLevel.get(run.level) ?? [];
      arr.push(run);
      byLevel.set(run.level, arr);
    }
    for (let level = 0; level <= this.maxLevel; level++) {
      const levelRuns = byLevel.get(level);
      if (!levelRuns || levelRuns.length < this.compactionFanout) continue;
      const span = this.levelSpan(level);
      for (let i = 0; i + this.compactionFanout <= levelRuns.length; i++) {
        const base = levelRuns[i].start_segment;
        let ok = true;
        for (let j = 0; j < this.compactionFanout; j++) {
          const run = levelRuns[i + j];
          const expectStart = base + j * span;
          if (run.start_segment !== expectStart || run.end_segment !== expectStart + span - 1) {
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
    indexName: string,
    level: number,
    inputs: SecondaryIndexRunRow[]
  ): Promise<Result<IndexRun, SecondaryIndexBuildError>> {
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
    const results: Array<{ meta: SecondaryIndexRunRow; run: IndexRun }> = [];
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
            if ((mask & (1 << bit)) !== 0) addSegment(fp, meta.start_segment + bit);
          }
        }
      } else if (run.runType === RUN_TYPE_POSTINGS && run.postings) {
        for (let i = 0; i < run.fingerprints.length; i++) {
          const fp = run.fingerprints[i];
          for (const rel of run.postings[i]) addSegment(fp, meta.start_segment + rel);
        }
      } else {
        return invalidIndexBuild(`unknown run type ${run.runType}`);
      }
    }

    const startSegment = inputs[0].start_segment;
    const endSegment = inputs[inputs.length - 1].end_segment;
    const pairs = Array.from(segments.entries())
      .map(([fp, set]) => {
        const list = Array.from(set).sort((a, b) => a - b);
        return { fp, rel: list.map((seg) => seg - startSegment) };
      })
      .sort((a, b) => (a.fp < b.fp ? -1 : a.fp > b.fp ? 1 : 0));
    const fingerprints = pairs.map((pair) => pair.fp);
    const postings = pairs.map((pair) => pair.rel);
    const fuseRes = buildBinaryFuseResult(fingerprints);
    if (Result.isError(fuseRes)) return invalidIndexBuild(fuseRes.error.message);
    const shash = streamHash16Hex(stream);
    const runId = `${indexName}-l${level}-${startSegment.toString().padStart(16, "0")}-${endSegment.toString().padStart(16, "0")}-${Date.now()}`;
    return Result.ok({
      meta: {
        runId,
        level,
        startSegment,
        endSegment,
        objectKey: secondaryIndexRunObjectKey(shash, indexName, runId),
        filterLen: fuseRes.value.bytes.byteLength,
        recordCount: fingerprints.length,
      },
      runType: RUN_TYPE_POSTINGS,
      filterBytes: fuseRes.value.bytes,
      filter: fuseRes.value.filter,
      fingerprints,
      postings,
    });
  }

  private async buildL0RunResult(
    stream: string,
    index: SecondaryIndexField,
    startSegment: number,
    segments: SegmentRow[],
    secret: Uint8Array
  ): Promise<Result<IndexRun, SecondaryIndexBuildError>> {
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return invalidIndexBuild(regRes.error.message);
    const registry = regRes.value;
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
            let offset = seg.start_offset;
            for (const blockRes of iterateBlocksResult(segBytes)) {
              if (Result.isError(blockRes)) {
                buildError = blockRes.error.message;
                return;
              }
              for (const rec of blockRes.value.decoded.records) {
                let parsed: unknown;
                try {
                  parsed = JSON.parse(new TextDecoder().decode(rec.payload));
                } catch {
                  offset += 1n;
                  continue;
                }
                const termRes = extractSecondaryIndexTermsResult(registry, offset, parsed);
                if (Result.isError(termRes)) continue;
                for (const term of termRes.value) {
                  if (term.index.name !== index.name) continue;
                  const fp = siphash24(secret, term.bytes);
                  const prev = local.get(fp) ?? 0;
                  local.set(fp, prev | maskBit);
                }
                offset += 1n;
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
    const shash = streamHash16Hex(stream);
    const endSegment = startSegment + this.span - 1;
    const runId = `${index.name}-l0-${startSegment.toString().padStart(16, "0")}-${endSegment.toString().padStart(16, "0")}-${Date.now()}`;
    return Result.ok({
      meta: {
        runId,
        level: 0,
        startSegment,
        endSegment,
        objectKey: secondaryIndexRunObjectKey(shash, index.name, runId),
        filterLen: fuseRes.value.bytes.byteLength,
        recordCount: fingerprints.length,
      },
      runType: RUN_TYPE_MASK16,
      filterBytes: fuseRes.value.bytes,
      filter: fuseRes.value.filter,
      fingerprints,
      masks,
    });
  }

  private async gcRetiredRuns(stream: string, indexName: string): Promise<void> {
    const retired = this.db.listRetiredSecondaryIndexRuns(stream, indexName);
    if (retired.length === 0) return;
    const manifest = this.db.getManifestRow(stream);
    const nowMs = this.db.nowMs();
    const cutoffGen =
      this.retireGenWindow > 0 && manifest.generation > this.retireGenWindow
        ? manifest.generation - this.retireGenWindow
        : 0;
    const toDelete: SecondaryIndexRunRow[] = [];
    for (const run of retired) {
      const expiredByGen = run.retired_gen != null && run.retired_gen > 0 && run.retired_gen <= cutoffGen;
      const expiredByTTL = run.retired_at_ms != null && run.retired_at_ms + BigInt(this.retireMinMs) <= nowMs;
      if (expiredByGen || expiredByTTL) toDelete.push(run);
    }
    if (toDelete.length === 0) return;
    for (const run of toDelete) {
      try {
        await this.os.delete(run.object_key);
      } catch {
        // ignore deletion errors
      }
      this.runCache.remove(run.object_key);
      this.runDiskCache?.remove(run.object_key);
    }
    this.db.deleteSecondaryIndexRuns(toDelete.map((run) => run.run_id));
  }

  private async persistRunResult(run: IndexRun): Promise<Result<void, SecondaryIndexBuildError>> {
    const payloadRes = encodeIndexRunResult(run);
    if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
    try {
      await retry(
        () => this.os.put(run.meta.objectKey, payloadRes.value, { contentLength: payloadRes.value.byteLength }),
        {
          retries: this.cfg.objectStoreRetries,
          baseDelayMs: this.cfg.objectStoreBaseDelayMs,
          maxDelayMs: this.cfg.objectStoreMaxDelayMs,
          timeoutMs: this.cfg.objectStoreTimeoutMs,
        }
      );
    } catch (e: unknown) {
      return invalidIndexBuild(String((e as any)?.message ?? e));
    }
    this.runDiskCache?.put(run.meta.objectKey, payloadRes.value);
    this.runCache.put(run.meta.objectKey, run);
    return Result.ok(undefined);
  }

  private async loadRunResult(meta: SecondaryIndexRunRow): Promise<Result<IndexRun | null, SecondaryIndexBuildError>> {
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
            if (!data) throw dsError(`missing secondary index run ${meta.object_key}`);
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
        return invalidIndexBuild(String((e as any)?.message ?? e));
      }
      this.runDiskCache?.put(meta.object_key, bytes);
    }
    const decodeRes = decodeIndexRunResult(bytes);
    if (Result.isError(decodeRes)) return invalidIndexBuild(decodeRes.error.message);
    this.runCache.put(meta.object_key, decodeRes.value);
    return Result.ok(decodeRes.value);
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, SecondaryIndexBuildError>> {
    try {
      const data = await retry(
        async () => {
          if (existsSync(seg.local_path)) return new Uint8Array(readFileSync(seg.local_path));
          const key = segmentObjectKey(streamHash16Hex(seg.stream), seg.segment_index);
          const remote = await this.os.get(key);
          if (!remote) throw dsError(`missing segment ${key}`);
          return remote;
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
      return invalidIndexBuild(String((e as any)?.message ?? e));
    }
  }
}
