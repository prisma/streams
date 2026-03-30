import { existsSync, readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { Result } from "better-result";
import type { Config } from "../config";
import type { SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SchemaRegistryStore } from "../schema/registry";
import { iterateBlocksResult } from "../segment/format";
import { retry } from "../util/retry";
import { streamHash16Hex, searchAggSegmentObjectKey, segmentObjectKey } from "../util/stream_paths";
import { dsError } from "../util/ds_error.ts";
import { parseDurationMsResult } from "../util/duration";
import {
  decodeAggSegmentCompanionResult,
  encodeAggSegmentCompanion,
  type AggMeasureState,
  type AggSegmentCompanion,
} from "./agg_format";
import { cloneAggMeasureState, extractRollupContributionResult, mergeAggMeasureState } from "./aggregate";
import { LruCache } from "../util/lru";

type AggBuildError = { kind: "invalid_agg_build"; message: string };

function invalidAggBuild<T = never>(message: string): Result<T, AggBuildError> {
  return Result.err({ kind: "invalid_agg_build", message });
}

type GroupBuilder = {
  dimensions: Record<string, string | null>;
  measures: Record<string, AggMeasureState>;
};

export class SearchAggManager {
  private readonly queue = new Set<string>();
  private readonly building = new Set<string>();
  private timer: any | null = null;
  private running = false;
  private readonly cache = new LruCache<string, AggSegmentCompanion>(128);

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

  async getSegmentCompanion(stream: string, segmentIndex: number): Promise<AggSegmentCompanion | null> {
    const row = this.db.getSearchFamilySegment(stream, "agg", segmentIndex);
    if (!row) return null;
    const cached = this.cache.get(row.object_key);
    if (cached) return cached;
    const bytes = await retry(
      async () => {
        const data = await this.os.get(row.object_key);
        if (!data) throw dsError(`missing .agg object ${row.object_key}`);
        return data;
      },
      {
        retries: this.cfg.objectStoreRetries,
        baseDelayMs: this.cfg.objectStoreBaseDelayMs,
        maxDelayMs: this.cfg.objectStoreMaxDelayMs,
        timeoutMs: this.cfg.objectStoreTimeoutMs,
      }
    );
    const decodedRes = decodeAggSegmentCompanionResult(bytes);
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

  private async buildPendingSegmentsResult(stream: string): Promise<Result<void, AggBuildError>> {
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return invalidAggBuild(regRes.error.message);
      const rollups = regRes.value.search?.rollups;
      if (!rollups || Object.keys(rollups).length === 0) return Result.ok(undefined);

      let state = this.db.getSearchFamilyState(stream, "agg");
      if (!state) {
        this.db.upsertSearchFamilyState(stream, "agg", 0);
        state = this.db.getSearchFamilyState(stream, "agg");
      }
      if (!state) return Result.ok(undefined);

      let uploadedThrough = state.uploaded_through;
      let changed = false;
      const uploadedSegments = this.db.countUploadedSegments(stream);
      while (uploadedThrough < uploadedSegments) {
        const seg = this.db.getSegmentByIndex(stream, uploadedThrough);
        if (!seg || !seg.r2_etag) break;
        const companionRes = await this.buildSegmentCompanionResult(stream, seg);
        if (Result.isError(companionRes)) return companionRes;
        const objectId = Buffer.from(randomBytes(8)).toString("hex");
        const objectKey = searchAggSegmentObjectKey(streamHash16Hex(stream), seg.segment_index, objectId);
        const payload = encodeAggSegmentCompanion(companionRes.value);
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
          return invalidAggBuild(String((e as any)?.message ?? e));
        }
        this.db.upsertSearchFamilySegment(stream, "agg", seg.segment_index, objectKey);
        uploadedThrough += 1;
        this.db.upsertSearchFamilyState(stream, "agg", uploadedThrough);
        this.cache.set(objectKey, companionRes.value);
        changed = true;
      }
      if (changed) this.onMetadataChanged?.(stream);
      if (changed && this.publishManifest) {
        try {
          await this.publishManifest(stream);
        } catch {
          // ignore and retry later
        }
      }
      return Result.ok(undefined);
    } finally {
      this.building.delete(stream);
    }
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, AggBuildError>> {
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
      return invalidAggBuild(String((e as any)?.message ?? e));
    }
  }

  private async buildSegmentCompanionResult(stream: string, seg: SegmentRow): Promise<Result<AggSegmentCompanion, AggBuildError>> {
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return invalidAggBuild(regRes.error.message);
    const rollups = regRes.value.search?.rollups;
    if (!rollups || Object.keys(rollups).length === 0) {
      return Result.ok({ version: 1, stream, segment_index: seg.segment_index, rollups: {} });
    }

    const bytesRes = await this.loadSegmentBytesResult(seg);
    if (Result.isError(bytesRes)) return bytesRes;

    const builders = new Map<string, Map<number, Map<number, Map<string, GroupBuilder>>>>();
    let offset = seg.start_offset;
    for (const blockRes of iterateBlocksResult(bytesRes.value)) {
      if (Result.isError(blockRes)) return invalidAggBuild(blockRes.error.message);
      for (const rec of blockRes.value.decoded.records) {
        let parsed: unknown;
        try {
          parsed = JSON.parse(new TextDecoder().decode(rec.payload));
        } catch {
          offset += 1n;
          continue;
        }
        for (const [rollupName, rollup] of Object.entries(rollups)) {
          const contributionRes = extractRollupContributionResult(regRes.value, rollup, offset, parsed);
          if (Result.isError(contributionRes)) return invalidAggBuild(contributionRes.error.message);
          const contribution = contributionRes.value;
          if (!contribution) continue;
          let intervalMap = builders.get(rollupName);
          if (!intervalMap) {
            intervalMap = new Map();
            builders.set(rollupName, intervalMap);
          }
          for (const interval of rollup.intervals) {
            const intervalMsRes = parseDurationMsResult(interval);
            if (Result.isError(intervalMsRes) || intervalMsRes.value <= 0) return invalidAggBuild(`invalid rollup interval ${interval}`);
            const intervalMs = intervalMsRes.value;
            let windowMap = intervalMap.get(intervalMs);
            if (!windowMap) {
              windowMap = new Map();
              intervalMap.set(intervalMs, windowMap);
            }
            const windowStart = Math.floor(contribution.timestampMs / intervalMs) * intervalMs;
            let groups = windowMap.get(windowStart);
            if (!groups) {
              groups = new Map();
              windowMap.set(windowStart, groups);
            }
            const groupKey = (rollup.dimensions ?? [])
              .map((dimension) => `${dimension}=${contribution.dimensions[dimension] ?? ""}`)
              .join("\u0000");
            let group = groups.get(groupKey);
            if (!group) {
              group = {
                dimensions: { ...contribution.dimensions },
                measures: {},
              };
              groups.set(groupKey, group);
            }
            for (const [measureName, state] of Object.entries(contribution.measures)) {
              const existing = group.measures[measureName];
              if (!existing) {
                group.measures[measureName] = cloneAggMeasureState(state);
                continue;
              }
              group.measures[measureName] = mergeAggMeasureState(existing, state);
            }
          }
        }
        offset += 1n;
      }
    }

    const rollupOut: AggSegmentCompanion["rollups"] = {};
    for (const [rollupName, intervalMap] of builders) {
      const intervals: AggSegmentCompanion["rollups"][string]["intervals"] = {};
      for (const [intervalMs, windowMap] of intervalMap) {
        const windows = Array.from(windowMap.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([startMs, groups]) => ({
            start_ms: startMs,
            groups: Array.from(groups.values()).sort((a, b) => JSON.stringify(a.dimensions).localeCompare(JSON.stringify(b.dimensions))),
          }));
        intervals[String(intervalMs)] = {
          interval_ms: intervalMs,
          windows,
        };
      }
      rollupOut[rollupName] = { intervals };
    }

    return Result.ok({
      version: 1,
      stream,
      segment_index: seg.segment_index,
      rollups: rollupOut,
    });
  }
}
