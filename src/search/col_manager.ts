import { existsSync, readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { Result } from "better-result";
import type { Config } from "../config";
import type { SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SchemaRegistryStore } from "../schema/registry";
import { iterateBlocksResult } from "../segment/format";
import { retry } from "../util/retry";
import { streamHash16Hex, searchColSegmentObjectKey, segmentObjectKey } from "../util/stream_paths";
import { dsError } from "../util/ds_error.ts";
import { extractSearchColumnValuesResult } from "./schema";
import { encodeSortableBool, encodeSortableFloat64, encodeSortableInt64 } from "./column_encoding";
import { encodeColSegmentCompanion, decodeColSegmentCompanionResult, type ColSegmentCompanion, type ColFieldData } from "./col_format";
import { createBitset, bitsetSet } from "./bitset";
import { LruCache } from "../util/lru";

type ColBuildError = { kind: "invalid_col_build"; message: string };

function invalidColBuild<T = never>(message: string): Result<T, ColBuildError> {
  return Result.err({ kind: "invalid_col_build", message });
}

type ColumnFieldBuilder = {
  kind: ColFieldData["kind"];
  values: Array<bigint | number | boolean | null>;
  invalid: boolean;
};

export class SearchColManager {
  private readonly queue = new Set<string>();
  private readonly building = new Set<string>();
  private timer: any | null = null;
  private running = false;
  private readonly cache = new LruCache<string, ColSegmentCompanion>(128);

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

  async getSegmentCompanion(stream: string, segmentIndex: number): Promise<ColSegmentCompanion | null> {
    const row = this.db.getSearchFamilySegment(stream, "col", segmentIndex);
    if (!row) return null;
    const cached = this.cache.get(row.object_key);
    if (cached) return cached;
    const bytes = await retry(
      async () => {
        const data = await this.os.get(row.object_key);
        if (!data) throw dsError(`missing .col object ${row.object_key}`);
        return data;
      },
      {
        retries: this.cfg.objectStoreRetries,
        baseDelayMs: this.cfg.objectStoreBaseDelayMs,
        maxDelayMs: this.cfg.objectStoreMaxDelayMs,
        timeoutMs: this.cfg.objectStoreTimeoutMs,
      }
    );
    const decodedRes = decodeColSegmentCompanionResult(bytes);
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

  private async buildPendingSegmentsResult(stream: string): Promise<Result<void, ColBuildError>> {
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return invalidColBuild(regRes.error.message);
      const reg = regRes.value;
      const columnFields = Object.entries(reg.search?.fields ?? {}).filter(([, field]) => field.column === true);
      if (columnFields.length === 0) return Result.ok(undefined);

      let state = this.db.getSearchFamilyState(stream, "col");
      if (!state) {
        this.db.upsertSearchFamilyState(stream, "col", 0);
        state = this.db.getSearchFamilyState(stream, "col");
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
        const objectKey = searchColSegmentObjectKey(streamHash16Hex(stream), seg.segment_index, objectId);
        const payload = encodeColSegmentCompanion(companionRes.value);
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
          return invalidColBuild(String((e as any)?.message ?? e));
        }
        this.db.upsertSearchFamilySegment(stream, "col", seg.segment_index, objectKey);
        uploadedThrough += 1;
        this.db.upsertSearchFamilyState(stream, "col", uploadedThrough);
        this.cache.set(objectKey, companionRes.value);
        changed = true;
      }
      if (changed) this.onMetadataChanged?.(stream);
      if (changed && this.publishManifest) {
        try {
          await this.publishManifest(stream);
        } catch {
          // ignore; background loop will retry
        }
      }
      return Result.ok(undefined);
    } finally {
      this.building.delete(stream);
    }
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, ColBuildError>> {
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
      return invalidColBuild(String((e as any)?.message ?? e));
    }
  }

  private async buildSegmentCompanionResult(stream: string, seg: SegmentRow): Promise<Result<ColSegmentCompanion, ColBuildError>> {
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return invalidColBuild(regRes.error.message);
    const reg = regRes.value;
    const columnFields = Object.entries(reg.search?.fields ?? {}).filter(([, field]) => field.column === true);
    const bytesRes = await this.loadSegmentBytesResult(seg);
    if (Result.isError(bytesRes)) return bytesRes;
    const segBytes = bytesRes.value;
    const builders = new Map<string, ColumnFieldBuilder>();
    for (const [fieldName, field] of columnFields) {
      builders.set(fieldName, { kind: field.kind, values: [], invalid: false });
    }
    let docCount = 0;
    let offset = seg.start_offset;
    for (const blockRes of iterateBlocksResult(segBytes)) {
      if (Result.isError(blockRes)) return invalidColBuild(blockRes.error.message);
      for (const rec of blockRes.value.decoded.records) {
        let parsed: unknown;
        try {
          parsed = JSON.parse(new TextDecoder().decode(rec.payload));
        } catch {
          for (const builder of builders.values()) builder.values.push(null);
          offset += 1n;
          docCount += 1;
          continue;
        }
        const valuesRes = extractSearchColumnValuesResult(reg, offset, parsed);
        if (Result.isError(valuesRes)) return invalidColBuild(valuesRes.error.message);
        for (const [fieldName, builder] of builders) {
          if (builder.invalid) {
            builder.values.push(null);
            continue;
          }
          const values = valuesRes.value.get(fieldName) ?? [];
          if (values.length > 1) {
            builder.invalid = true;
            builder.values.push(null);
            continue;
          }
          builder.values.push(values.length === 1 ? values[0] : null);
        }
        offset += 1n;
        docCount += 1;
      }
    }

    const fields: Record<string, ColFieldData> = {};
    let minTimestampMs: bigint | null = null;
    let maxTimestampMs: bigint | null = null;
    const primaryTimestampField = reg.search?.primaryTimestampField;
    for (const [fieldName, builder] of builders) {
      if (builder.invalid) continue;
      const exists = createBitset(docCount);
      const encodedParts: Uint8Array[] = [];
      let minValue: bigint | number | boolean | null = null;
      let maxValue: bigint | number | boolean | null = null;
      for (let docId = 0; docId < builder.values.length; docId++) {
        const value = builder.values[docId];
        if (value == null) continue;
        bitsetSet(exists, docId);
        if (builder.kind === "integer" || builder.kind === "date") {
          const encoded = encodeSortableInt64(value as bigint);
          encodedParts.push(encoded);
        } else if (builder.kind === "float") {
          encodedParts.push(encodeSortableFloat64(value as number));
        } else if (builder.kind === "bool") {
          encodedParts.push(encodeSortableBool(value as boolean));
        }
        if (minValue == null || compareValues(value, minValue) < 0) minValue = value;
        if (maxValue == null || compareValues(value, maxValue) > 0) maxValue = value;
      }
      if (encodedParts.length === 0) continue;
      const valuesBytes = concatBytes(encodedParts);
      fields[fieldName] = {
        kind: builder.kind,
        exists_b64: Buffer.from(exists).toString("base64"),
        values_b64: Buffer.from(valuesBytes).toString("base64"),
        min_b64: minValue == null ? undefined : Buffer.from(encodeComparable(builder.kind, minValue)).toString("base64"),
        max_b64: maxValue == null ? undefined : Buffer.from(encodeComparable(builder.kind, maxValue)).toString("base64"),
      };
      if (fieldName === primaryTimestampField && builder.kind === "date") {
        minTimestampMs = minValue as bigint;
        maxTimestampMs = maxValue as bigint;
      }
    }

    return Result.ok({
      version: 1,
      stream,
      segment_index: seg.segment_index,
      doc_count: docCount,
      fields,
      primary_timestamp_field: primaryTimestampField,
      min_timestamp_ms: minTimestampMs?.toString(),
      max_timestamp_ms: maxTimestampMs?.toString(),
    });
  }
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const part of parts) total += part.byteLength;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function compareValues(left: bigint | number | boolean, right: bigint | number | boolean): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  const l = String(left);
  const r = String(right);
  return l < r ? -1 : l > r ? 1 : 0;
}

function encodeComparable(kind: ColFieldData["kind"], value: bigint | number | boolean): Uint8Array {
  if (kind === "integer" || kind === "date") return encodeSortableInt64(value as bigint);
  if (kind === "float") return encodeSortableFloat64(value as number);
  return encodeSortableBool(value as boolean);
}
