import { existsSync, readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { Result } from "better-result";
import type { Config } from "../config";
import type { SegmentRow, SqliteDurableStore } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SchemaRegistryStore } from "../schema/registry";
import { iterateBlocksResult } from "../segment/format";
import { retry } from "../util/retry";
import { streamHash16Hex, searchFtsSegmentObjectKey, segmentObjectKey } from "../util/stream_paths";
import { dsError } from "../util/ds_error.ts";
import { analyzeTextValue, extractSearchTextValuesResult } from "./schema";
import { encodeFtsSegmentCompanion, decodeFtsSegmentCompanionResult, type FtsFieldCompanion, type FtsSegmentCompanion } from "./fts_format";
import { LruCache } from "../util/lru";

type FtsBuildError = { kind: "invalid_fts_build"; message: string };

function invalidFtsBuild<T = never>(message: string): Result<T, FtsBuildError> {
  return Result.err({ kind: "invalid_fts_build", message });
}

export class SearchFtsManager {
  private readonly queue = new Set<string>();
  private readonly building = new Set<string>();
  private timer: any | null = null;
  private running = false;
  private readonly cache = new LruCache<string, FtsSegmentCompanion>(128);

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

  async getSegmentCompanion(stream: string, segmentIndex: number): Promise<FtsSegmentCompanion | null> {
    const row = this.db.getSearchFamilySegment(stream, "fts", segmentIndex);
    if (!row) return null;
    const cached = this.cache.get(row.object_key);
    if (cached) return cached;
    const bytes = await retry(
      async () => {
        const data = await this.os.get(row.object_key);
        if (!data) throw dsError(`missing .fts object ${row.object_key}`);
        return data;
      },
      {
        retries: this.cfg.objectStoreRetries,
        baseDelayMs: this.cfg.objectStoreBaseDelayMs,
        maxDelayMs: this.cfg.objectStoreMaxDelayMs,
        timeoutMs: this.cfg.objectStoreTimeoutMs,
      }
    );
    const decodedRes = decodeFtsSegmentCompanionResult(bytes);
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

  private async buildPendingSegmentsResult(stream: string): Promise<Result<void, FtsBuildError>> {
    if (this.building.has(stream)) return Result.ok(undefined);
    this.building.add(stream);
    try {
      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return invalidFtsBuild(regRes.error.message);
      const reg = regRes.value;
      const ftsFields = Object.entries(reg.search?.fields ?? {}).filter(
        ([, field]) => field.kind === "text" || field.kind === "keyword"
      );
      if (ftsFields.length === 0) return Result.ok(undefined);

      let state = this.db.getSearchFamilyState(stream, "fts");
      if (!state) {
        this.db.upsertSearchFamilyState(stream, "fts", 0);
        state = this.db.getSearchFamilyState(stream, "fts");
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
        const objectKey = searchFtsSegmentObjectKey(streamHash16Hex(stream), seg.segment_index, objectId);
        const payload = encodeFtsSegmentCompanion(companionRes.value);
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
          return invalidFtsBuild(String((e as any)?.message ?? e));
        }
        this.db.upsertSearchFamilySegment(stream, "fts", seg.segment_index, objectKey);
        uploadedThrough += 1;
        this.db.upsertSearchFamilyState(stream, "fts", uploadedThrough);
        this.cache.set(objectKey, companionRes.value);
        changed = true;
      }
      if (changed) this.onMetadataChanged?.(stream);
      if (changed && this.publishManifest) {
        try {
          await this.publishManifest(stream);
        } catch {
          // ignore
        }
      }
      return Result.ok(undefined);
    } finally {
      this.building.delete(stream);
    }
  }

  private async loadSegmentBytesResult(seg: SegmentRow): Promise<Result<Uint8Array, FtsBuildError>> {
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
      return invalidFtsBuild(String((e as any)?.message ?? e));
    }
  }

  private async buildSegmentCompanionResult(stream: string, seg: SegmentRow): Promise<Result<FtsSegmentCompanion, FtsBuildError>> {
    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return invalidFtsBuild(regRes.error.message);
    const reg = regRes.value;
    const ftsFieldEntries = Object.entries(reg.search?.fields ?? {}).filter(([, field]) => field.kind === "text" || field.kind === "keyword");
    const bytesRes = await this.loadSegmentBytesResult(seg);
    if (Result.isError(bytesRes)) return bytesRes;
    const segBytes = bytesRes.value;
    const fields = new Map<string, FtsFieldCompanion>();
    for (const [fieldName, field] of ftsFieldEntries) {
      fields.set(fieldName, {
        kind: field.kind,
        exact: field.exact === true ? true : undefined,
        prefix: field.prefix === true ? true : undefined,
        positions: field.positions === true ? true : undefined,
        exists_docs: [],
        doc_lengths: field.kind === "text" ? [] : undefined,
        terms: {},
      });
    }
    let docCount = 0;
    let offset = seg.start_offset;
    for (const blockRes of iterateBlocksResult(segBytes)) {
      if (Result.isError(blockRes)) return invalidFtsBuild(blockRes.error.message);
      for (const rec of blockRes.value.decoded.records) {
        let parsed: unknown;
        try {
          parsed = JSON.parse(new TextDecoder().decode(rec.payload));
        } catch {
          offset += 1n;
          docCount += 1;
          continue;
        }
        const valuesRes = extractSearchTextValuesResult(reg, offset, parsed);
        if (Result.isError(valuesRes)) return invalidFtsBuild(valuesRes.error.message);
        for (const [fieldName, fieldCompanion] of fields) {
          const values = valuesRes.value.get(fieldName) ?? [];
          if (values.length === 0) {
            if (fieldCompanion.doc_lengths) fieldCompanion.doc_lengths.push(0);
            continue;
          }
          fieldCompanion.exists_docs.push(docCount);
          if (fieldCompanion.kind === "keyword") {
            for (const value of values) {
              const postings = fieldCompanion.terms[value] ?? [];
              if (postings.length === 0 || postings[postings.length - 1].d !== docCount) {
                postings.push({ d: docCount });
              }
              fieldCompanion.terms[value] = postings;
            }
          } else {
            let position = 0;
            let docLength = 0;
            for (const value of values) {
              const tokens = analyzeTextValue(value, reg.search?.fields[fieldName].analyzer);
              for (const token of tokens) {
                const postings = fieldCompanion.terms[token] ?? [];
                const last = postings[postings.length - 1];
                if (!last || last.d !== docCount) {
                  postings.push({ d: docCount, p: fieldCompanion.positions ? [position] : undefined });
                } else if (fieldCompanion.positions && last.p) {
                  last.p.push(position);
                }
                fieldCompanion.terms[token] = postings;
                position += 1;
                docLength += 1;
              }
            }
            fieldCompanion.doc_lengths?.push(docLength);
          }
        }
        offset += 1n;
        docCount += 1;
      }
    }
    return Result.ok({
      version: 1,
      stream,
      segment_index: seg.segment_index,
      doc_count: docCount,
      fields: Object.fromEntries(fields.entries()),
    });
  }
}
