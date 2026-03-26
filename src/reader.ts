import { existsSync, openSync, readSync, closeSync } from "node:fs";
import type { Config } from "./config";
import type { SqliteDurableStore, SegmentRow } from "./db/db";
import type { ObjectStore } from "./objectstore/interface";
import {
  type CompiledReadFilter,
  type ReadFilterColumnClause,
  collectPositiveColumnFilterClauses,
  collectPositiveExactFilterClauses,
  evaluateReadFilterResult,
} from "./read_filter";
import { decodeJsonPayloadResult } from "./schema/read_json";
import { SchemaRegistryStore } from "./schema/registry";
import { parseOffsetResult, offsetToSeqOrNeg1, encodeOffset } from "./offset";
import { decodeBlockResult, iterateBlocksResult, parseBlockHeaderResult, parseFooterBytes, DSB3_HEADER_BYTES, type SegmentFooter } from "./segment/format";
import { SegmentDiskCache, type SegmentCacheStats } from "./segment/cache";
import { Bloom256 } from "./util/bloom256";
import { segmentObjectKey, streamHash16Hex } from "./util/stream_paths";
import { readU32BE } from "./util/endian";
import { retry, type RetryOptions } from "./util/retry";
import { LruCache } from "./util/lru";
import type { IndexCandidate, StreamIndexLookup } from "./index/indexer";
import { dsError } from "./util/ds_error.ts";
import { Result } from "better-result";
import { filterDocIdsByColumnResult } from "./search/col_runtime";
import {
  type AggregateRequest,
  extractRollupContributionResult,
  extractRollupEligibility,
  formatAggMeasureState,
  mergeAggMeasureState,
} from "./search/aggregate";
import {
  type CompiledSearchQuery,
  type SearchColumnClause,
  type SearchEvaluation,
  type SearchExactClause,
  type SearchFtsClause,
  type SearchRequest,
  type SearchSortSpec,
  collectPositiveSearchColumnClauses,
  collectPositiveSearchExactClauses,
  collectPositiveSearchFtsClauses,
  evaluateSearchQueryResult,
  extractSearchHitFieldsResult,
} from "./search/query";
import { filterDocIdsByFtsClauseResult } from "./search/fts_runtime";
import { canonicalizeColumnValue, canonicalizeExactValue } from "./search/schema";
import type { SearchRollupConfig } from "./schema/registry";
import type { AggMeasureState } from "./search/agg_format";

export type ReadFormat = "raw" | "json";

export type ReadBatch = {
  stream: string;
  format: ReadFormat;
  key: string | null;
  requestOffset: string;
  endOffset: string; // checkpoint at end of stream
  nextOffset: string; // checkpoint after this response
  endOffsetSeq: bigint;
  nextOffsetSeq: bigint;
  records: Array<{ offset: bigint; payload: Uint8Array }>; // payload bytes in wire order
  filterScannedBytes?: number;
  filterScanLimitBytes?: number;
  filterScanLimitReached?: boolean;
};

export type SearchHit = {
  offset: string;
  score: number;
  sort: unknown[];
  fields: Record<string, unknown>;
  source: unknown;
};

export type SearchResultBatch = {
  stream: string;
  snapshotEndOffset: string;
  tookMs: number;
  coverage: {
    indexedSegments: number;
    scannedSegments: number;
    scannedTailDocs: number;
    indexFamiliesUsed: string[];
  };
  total: {
    value: number;
    relation: "eq" | "gte";
  };
  hits: SearchHit[];
  nextSearchAfter: unknown[] | null;
};

export type AggregateResultBatch = {
  stream: string;
  rollup: string;
  from: string;
  to: string;
  interval: string;
  coverage: {
    usedRollups: boolean;
    indexedSegments: number;
    scannedSegments: number;
    scannedTailDocs: number;
    indexFamiliesUsed: string[];
  };
  buckets: Array<{
    start: string;
    end: string;
    groups: Array<{
      key: Record<string, string | null>;
      measures: Record<string, unknown>;
    }>;
  }>;
};

export type ReaderError =
  | { kind: "not_found"; message: string }
  | { kind: "gone"; message: string }
  | { kind: "invalid_offset"; message: string }
  | { kind: "internal"; message: string };

type FooterCacheEntry = { footer: SegmentFooter | null; footerStart: number };
const READ_FILTER_SCAN_LIMIT_BYTES = 100 * 1024 * 1024;
type SegmentCandidateInfo = { segments: Set<number> | null; indexedThrough: number };
type SearchFamilyCandidateInfo = { docIds: Set<number> | null; usedFamilies: Set<string> };
type SearchHitInternal = {
  offsetSeq: bigint;
  offset: string;
  score: number;
  sortInternal: Array<bigint | number | string | boolean | null>;
  sortResponse: unknown[];
  fields: Record<string, unknown>;
  source: unknown;
};
type AggregateGroupInternal = {
  key: Record<string, string | null>;
  measures: Record<string, AggMeasureState>;
};

function errorMessage(e: unknown): string {
  return String((e as any)?.message ?? e);
}

function utf8Bytes(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function objectKeyForSegment(seg: SegmentRow): string {
  const streamHash = streamHash16Hex(seg.stream);
  return segmentObjectKey(streamHash, seg.segment_index);
}

function readRangeFromFile(path: string, start: number, end: number): Uint8Array {
  const len = end - start + 1;
  const fd = openSync(path, "r");
  try {
    const buf = Buffer.alloc(len);
    const bytesRead = readSync(fd, buf, 0, len, start);
    if (bytesRead !== len) throw dsError("short read");
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  } finally {
    closeSync(fd);
  }
}

async function readSegmentRange(
  os: ObjectStore,
  seg: SegmentRow,
  start: number,
  end: number,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions
): Promise<Uint8Array> {
  const local = seg.local_path;
  if (existsSync(local)) return readRangeFromFile(local, start, end);

  const objectKey = objectKeyForSegment(seg);
  if (diskCache && diskCache.has(objectKey)) {
    diskCache.recordHit();
    diskCache.touch(objectKey);
    return readRangeFromFile(diskCache.getPath(objectKey), start, end);
  }
  if (diskCache) diskCache.recordMiss();

  const bytes = await retry(
    async () => {
      const res = await os.get(objectKey, { range: { start, end } });
      if (!res) throw dsError(`object store missing segment: ${objectKey}`);
      return res;
    },
    retryOpts ?? { retries: 0, baseDelayMs: 0, maxDelayMs: 0, timeoutMs: 0 }
  );
  if (diskCache && start === 0 && end === seg.size_bytes - 1) {
    diskCache.put(objectKey, bytes);
  }
  return bytes;
}

async function loadSegmentBytes(
  os: ObjectStore,
  seg: SegmentRow,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions
): Promise<Uint8Array> {
  return readSegmentRange(os, seg, 0, seg.size_bytes - 1, diskCache, retryOpts);
}

async function loadSegmentFooter(
  os: ObjectStore,
  seg: SegmentRow,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions,
  footerCache?: LruCache<string, FooterCacheEntry>
): Promise<{ footer: SegmentFooter | null; footerStart: number } | null> {
  const cacheKey = seg.segment_id;
  if (footerCache) {
    const cached = footerCache.get(cacheKey);
    if (cached) return cached;
  }
  if (seg.size_bytes < 8) return null;
  const tail = await readSegmentRange(os, seg, seg.size_bytes - 8, seg.size_bytes - 1, diskCache, retryOpts);
  const magic = String.fromCharCode(tail[4], tail[5], tail[6], tail[7]);
  if (magic !== "DSF1") return null;
  const footerLen = readU32BE(tail, 0);
  const footerStart = seg.size_bytes - 8 - footerLen;
  if (footerStart < 0) return null;
  const footerBytes = await readSegmentRange(os, seg, footerStart, footerStart + footerLen - 1, diskCache, retryOpts);
  const footer = parseFooterBytes(footerBytes);
  const result = { footer, footerStart };
  if (footerCache) footerCache.set(cacheKey, result);
  return result;
}

export class StreamReader {
  private readonly config: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly registry: SchemaRegistryStore;
  private readonly diskCache?: SegmentDiskCache;
  private readonly footerCache?: LruCache<string, FooterCacheEntry>;
  private readonly index?: StreamIndexLookup;

  constructor(
    config: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    registry: SchemaRegistryStore,
    diskCache?: SegmentDiskCache,
    index?: StreamIndexLookup
  ) {
    this.config = config;
    this.db = db;
    this.os = os;
    this.registry = registry;
    this.diskCache = diskCache;
    this.index = index;
    if (config.segmentFooterCacheEntries > 0) {
      this.footerCache = new LruCache(config.segmentFooterCacheEntries);
    }
  }

  cacheStats(): SegmentCacheStats | null {
    return this.diskCache ? this.diskCache.stats() : null;
  }

  private retryOpts(): RetryOptions {
    return {
      retries: this.config.objectStoreRetries,
      baseDelayMs: this.config.objectStoreBaseDelayMs,
      maxDelayMs: this.config.objectStoreMaxDelayMs,
      timeoutMs: this.config.objectStoreTimeoutMs,
    };
  }

  async seekOffsetByTimestampResult(stream: string, sinceMs: bigint, key: string | null): Promise<Result<string, ReaderError>> {
    const srow = this.db.getStream(stream);
    if (!srow || this.db.isDeleted(srow)) return Result.err({ kind: "not_found", message: "not_found" });
    if (srow.expires_at_ms != null && this.db.nowMs() > srow.expires_at_ms) {
      return Result.err({ kind: "gone", message: "stream expired" });
    }
    try {
      const sinceNs = sinceMs * 1_000_000n;
      const keyBytes = key ? utf8Bytes(key) : null;

      // Scan segments in order.
      const segments = this.db.listSegmentsForStream(stream);
      for (const seg of segments) {
        const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
        let curOffset = seg.start_offset;
        for (const blockRes of iterateBlocksResult(segBytes)) {
          if (Result.isError(blockRes)) return Result.err({ kind: "internal", message: blockRes.error.message });
          const { decoded } = blockRes.value;
          if (decoded.lastAppendNs < sinceNs) {
            curOffset += BigInt(decoded.recordCount);
            continue;
          }
          for (const r of decoded.records) {
            if (keyBytes && !bytesEqual(r.routingKey, keyBytes)) {
              curOffset += 1n;
              continue;
            }
            if (r.appendNs >= sinceNs) {
              const prev = curOffset - 1n;
              return Result.ok(encodeOffset(srow.epoch, prev));
            }
            curOffset += 1n;
          }
        }
      }

      // Scan WAL tail.
      const start = srow.sealed_through + 1n;
      const end = srow.next_offset - 1n;
      if (start <= end) {
        for (const rec of this.db.iterWalRange(stream, start, end, keyBytes ?? undefined)) {
          const tsNs = BigInt(rec.ts_ms) * 1_000_000n;
          if (tsNs >= sinceNs) {
            const off = BigInt(rec.offset) - 1n;
            return Result.ok(encodeOffset(srow.epoch, off));
          }
        }
      }

      const endOffsetNum = srow.next_offset - 1n;
      return Result.ok(encodeOffset(srow.epoch, endOffsetNum));
    } catch (e: unknown) {
      return Result.err({ kind: "internal", message: errorMessage(e) });
    }
  }

  async seekOffsetByTimestamp(stream: string, sinceMs: bigint, key: string | null): Promise<string> {
    const res = await this.seekOffsetByTimestampResult(stream, sinceMs, key);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  async readResult(args: {
    stream: string;
    offset: string;
    key: string | null;
    format: ReadFormat;
    filter?: CompiledReadFilter | null;
  }): Promise<Result<ReadBatch, ReaderError>> {
    const { stream, offset, key, format, filter = null } = args;
    const srow = this.db.getStream(stream);
    if (!srow || this.db.isDeleted(srow)) return Result.err({ kind: "not_found", message: "not_found" });
    if (srow.expires_at_ms != null && this.db.nowMs() > srow.expires_at_ms) {
      return Result.err({ kind: "gone", message: "stream expired" });
    }
    const epoch = srow.epoch;

    try {
      const parsed = parseOffsetResult(offset);
      if (Result.isError(parsed)) {
        return Result.err({ kind: "invalid_offset", message: parsed.error.message });
      }
      const startOffsetExclusive = offsetToSeqOrNeg1(parsed.value);
      const desiredOffset = startOffsetExclusive + 1n;

      const endOffsetNum = srow.next_offset - 1n;
      const endOffset = encodeOffset(srow.epoch, endOffsetNum);

      const results: Array<{ offset: bigint; payload: Uint8Array }> = [];
      let bytesOut = 0;
      let filterScannedBytes = 0;
      let filterScanLimitReached = false;

      // Nothing to read.
      if (desiredOffset > endOffsetNum) {
        return Result.ok({
          stream,
          format,
          key,
          requestOffset: offset,
          endOffset,
          nextOffset: encodeOffset(srow.epoch, startOffsetExclusive),
          endOffsetSeq: endOffsetNum,
          nextOffsetSeq: startOffsetExclusive,
          records: [],
          ...(filter
            ? {
                filterScannedBytes,
                filterScanLimitBytes: READ_FILTER_SCAN_LIMIT_BYTES,
                filterScanLimitReached,
              }
            : {}),
        });
      }

      let seq = desiredOffset;
      const keyBytes = key ? utf8Bytes(key) : null;
      const candidateInfo = await this.resolveCandidateSegments(stream, keyBytes, filter);
      const candidateSegments = candidateInfo.segments;
      const indexedThrough = candidateInfo.indexedThrough;
      const columnClauses = filter ? collectPositiveColumnFilterClauses(filter) : [];
      const columnIndexedThrough = filter && columnClauses.length > 0 ? this.db.getSearchFamilyState(stream, "col")?.uploaded_through ?? 0 : 0;
      const filterRegistryRes = filter ? this.registry.getRegistryResult(stream) : Result.ok(null);
      if (Result.isError(filterRegistryRes)) return Result.err({ kind: "internal", message: filterRegistryRes.error.message });
      const filterRegistry = filterRegistryRes.value;

      const evaluateRecordResult = (
        offset: bigint,
        routingKey: Uint8Array | null | undefined,
        payload: Uint8Array
      ): Result<{ matched: boolean; stop: boolean }, ReaderError> => {
        if (filter) {
          filterScannedBytes += payload.byteLength;
        }
        if (keyBytes && (!routingKey || !bytesEqual(routingKey, keyBytes))) {
          return Result.ok({
            matched: false,
            stop: !!filter && filterScannedBytes >= READ_FILTER_SCAN_LIMIT_BYTES,
          });
        }
        if (!filter) return Result.ok({ matched: true, stop: false });
        const valueRes = decodeJsonPayloadResult(this.registry, stream, offset, payload);
        if (Result.isError(valueRes)) {
          return Result.err({ kind: "internal", message: valueRes.error.message });
        }
        const matchesRes = evaluateReadFilterResult(filterRegistry!, offset, filter, valueRes.value);
        if (Result.isError(matchesRes)) return Result.err({ kind: "internal", message: matchesRes.error.message });
        return Result.ok({
          matched: matchesRes.value,
          stop: filterScannedBytes >= READ_FILTER_SCAN_LIMIT_BYTES,
        });
      };

      const scanSegmentBytes = async (
        segBytes: Uint8Array,
        seg: SegmentRow,
        allowedDocIds: Set<number> | null
      ): Promise<Result<void, ReaderError>> => {
        let curOffset = seg.start_offset;
        for (const blockRes of iterateBlocksResult(segBytes)) {
          if (Result.isError(blockRes)) return Result.err({ kind: "internal", message: blockRes.error.message });
          const { decoded } = blockRes.value;
          if (keyBytes) {
            const bloom = new Bloom256(decoded.bloom);
            if (!bloom.maybeHas(keyBytes)) {
              curOffset += BigInt(decoded.recordCount);
              continue;
            }
          }
          for (const r of decoded.records) {
            if (curOffset < seq) {
              curOffset += 1n;
              continue;
            }
            if (curOffset > endOffsetNum) break;
            const localDocId = Number(curOffset - seg.start_offset);
            if (allowedDocIds && !allowedDocIds.has(localDocId)) {
              curOffset += 1n;
              continue;
            }
            const matchRes = evaluateRecordResult(curOffset, r.routingKey, r.payload);
            if (Result.isError(matchRes)) return matchRes;
            if (matchRes.value.matched) {
              results.push({ offset: curOffset, payload: r.payload });
              bytesOut += r.payload.byteLength;
            }
            curOffset += 1n;
            if (matchRes.value.stop) {
              filterScanLimitReached = true;
              seq = curOffset;
              return Result.ok(undefined);
            }
            if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) {
              seq = curOffset;
              return Result.ok(undefined);
            }
          }
        }
        return Result.ok(undefined);
      };

      // 1) Read from sealed segments.
      while (seq <= endOffsetNum && seq <= srow.sealed_through) {
        const seg = this.db.findSegmentForOffset(stream, seq);
        if (!seg) {
          // Corruption in local metadata: sealed_through points past segments table.
          break;
        }
        if (candidateSegments && seg.segment_index < indexedThrough && !candidateSegments.has(seg.segment_index)) {
          seq = seg.end_offset + 1n;
          continue;
        }
        let allowedDocIds: Set<number> | null = null;
        if (columnClauses.length > 0 && seg.segment_index < columnIndexedThrough) {
          const docIdsRes = await this.resolveColumnCandidateDocIdsResult(stream, seg.segment_index, columnClauses);
          if (Result.isError(docIdsRes)) return Result.err({ kind: "internal", message: docIdsRes.error.message });
          if (docIdsRes.value) {
            allowedDocIds = docIdsRes.value;
            if (allowedDocIds.size === 0) {
              seq = seg.end_offset + 1n;
              continue;
            }
          }
        }
        const preferFull = !keyBytes && this.config.readMaxBytes >= seg.size_bytes;
        if (preferFull) {
          const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
          const scanRes = await scanSegmentBytes(segBytes, seg, allowedDocIds);
          if (Result.isError(scanRes)) return scanRes;
          if (filterScanLimitReached) return Result.ok(finalize());
          if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) return Result.ok(finalize());
        } else {
          const footerInfo = await loadSegmentFooter(this.os, seg, this.diskCache, this.retryOpts(), this.footerCache);
          if (!footerInfo || !footerInfo.footer) {
            const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
            const scanRes = await scanSegmentBytes(segBytes, seg, allowedDocIds);
            if (Result.isError(scanRes)) return scanRes;
            if (filterScanLimitReached) return Result.ok(finalize());
            if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) return Result.ok(finalize());
          } else {
            const footer = footerInfo.footer;
            for (const entry of footer.blocks) {
              const blockStart = entry.firstOffset;
              const blockEnd = entry.firstOffset + BigInt(entry.recordCount) - 1n;
              if (blockEnd < seq) continue;
              if (blockStart > endOffsetNum) break;

              if (keyBytes) {
                const headerBytes = await readSegmentRange(
                  this.os,
                  seg,
                  entry.blockOffset,
                  entry.blockOffset + DSB3_HEADER_BYTES - 1,
                  this.diskCache,
                  this.retryOpts()
                );
                const headerRes = parseBlockHeaderResult(headerBytes);
                if (Result.isError(headerRes)) return Result.err({ kind: "internal", message: headerRes.error.message });
                const header = headerRes.value;
                const bloom = new Bloom256(header.bloom);
                if (!bloom.maybeHas(keyBytes)) continue;
              }

              const totalLen = DSB3_HEADER_BYTES + entry.compressedLen;
              const blockBytes = await readSegmentRange(
                this.os,
                seg,
                entry.blockOffset,
                entry.blockOffset + totalLen - 1,
                this.diskCache,
                this.retryOpts()
              );
              const decodedRes = decodeBlockResult(blockBytes);
              if (Result.isError(decodedRes)) return Result.err({ kind: "internal", message: decodedRes.error.message });
              const decoded = decodedRes.value;
              let curOffset = entry.firstOffset;
              for (const r of decoded.records) {
                if (curOffset < seq) {
                  curOffset += 1n;
                  continue;
                }
                if (curOffset > endOffsetNum) break;
                const localDocId = Number(curOffset - seg.start_offset);
                if (allowedDocIds && !allowedDocIds.has(localDocId)) {
                  curOffset += 1n;
                  continue;
                }
                const matchRes = evaluateRecordResult(curOffset, r.routingKey, r.payload);
                if (Result.isError(matchRes)) return matchRes;
                if (matchRes.value.matched) {
                  results.push({ offset: curOffset, payload: r.payload });
                  bytesOut += r.payload.byteLength;
                }
                curOffset += 1n;
                if (matchRes.value.stop) {
                  filterScanLimitReached = true;
                  seq = curOffset;
                  return Result.ok(finalize());
                }
                if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) {
                  seq = curOffset;
                  return Result.ok(finalize());
                }
              }
            }
          }
        }

        // Move to next segment.
        seq = seg.end_offset + 1n;
      }

      // 2) Read remaining from WAL tail.
      if (seq <= endOffsetNum) {
        let hitLimit = false;
        for (const rec of this.db.iterWalRange(stream, seq, endOffsetNum, keyBytes ?? undefined)) {
          const s = BigInt(rec.offset);
          const payload: Uint8Array = rec.payload;
          const routingKey =
            rec.routing_key == null
              ? null
              : rec.routing_key instanceof Uint8Array
                ? rec.routing_key
                : new Uint8Array(rec.routing_key);
          const matchRes = evaluateRecordResult(s, routingKey, payload);
          if (Result.isError(matchRes)) return matchRes;
          if (matchRes.value.matched) {
            results.push({ offset: s, payload });
            bytesOut += payload.byteLength;
          }
          if (matchRes.value.stop) {
            filterScanLimitReached = true;
            hitLimit = true;
            seq = s + 1n;
            break;
          }
          if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) {
            hitLimit = true;
            // We only emitted payloads up through this offset (key-filtered reads
            // may skip offsets in SQL). Resume from the next offset.
            seq = s + 1n;
            break;
          }
        }
        if (!hitLimit) {
          // We exhausted the iterator for this [seq, endOffsetNum] range. Even if
          // it yielded zero records (common for key-filtered reads), we have
          // scanned through endOffsetNum and should advance the stream cursor to
          // avoid tight catchup loops.
          seq = endOffsetNum + 1n;
        }
      }

      return Result.ok(finalize());

      function finalize(): ReadBatch {
        // nextOffset is a stream cursor, not a "last matching record" cursor. For
        // key-filtered reads, this must still advance past non-matching offsets,
        // otherwise SSE/long-poll can spin forever when the stream advances but no
        // matching keys appear.
        const scannedThrough = seq - 1n;
        const nextOffset = encodeOffset(epoch, scannedThrough);
        return {
          stream,
          format,
          key,
          requestOffset: offset,
          endOffset,
          nextOffset,
          endOffsetSeq: endOffsetNum,
          nextOffsetSeq: scannedThrough,
          records: results,
          ...(filter
            ? {
                filterScannedBytes,
                filterScanLimitBytes: READ_FILTER_SCAN_LIMIT_BYTES,
                filterScanLimitReached,
              }
            : {}),
        };
      }
    } catch (e: unknown) {
      return Result.err({ kind: "internal", message: errorMessage(e) });
    }
  }

  async read(args: {
    stream: string;
    offset: string;
    key: string | null;
    format: ReadFormat;
    filter?: CompiledReadFilter | null;
  }): Promise<ReadBatch> {
    const res = await this.readResult(args);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  async searchResult(args: { stream: string; request: SearchRequest }): Promise<Result<SearchResultBatch, ReaderError>> {
    const startedAt = Date.now();
    const { stream, request } = args;
    const srow = this.db.getStream(stream);
    if (!srow || this.db.isDeleted(srow)) return Result.err({ kind: "not_found", message: "not_found" });
    if (srow.expires_at_ms != null && this.db.nowMs() > srow.expires_at_ms) {
      return Result.err({ kind: "gone", message: "stream expired" });
    }

    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ kind: "internal", message: regRes.error.message });
    const registry = regRes.value;
    if (!registry.search) return Result.err({ kind: "internal", message: "search is not configured for this stream" });

    try {
      const snapshotEndSeq = srow.next_offset - 1n;
      const snapshotEndOffset = encodeOffset(srow.epoch, snapshotEndSeq);
      const exactCandidateInfo = await this.resolveSearchExactCandidateSegments(stream, request.q);
      const columnClauses = collectPositiveSearchColumnClauses(request.q);
      const columnIndexedThrough = columnClauses.length > 0 ? this.db.getSearchFamilyState(stream, "col")?.uploaded_through ?? 0 : 0;
      const ftsClauses = collectPositiveSearchFtsClauses(request.q);
      const ftsIndexedThrough = ftsClauses.length > 0 ? this.db.getSearchFamilyState(stream, "fts")?.uploaded_through ?? 0 : 0;
      const deadline = request.timeoutMs == null ? null : Date.now() + request.timeoutMs;

      const hits: SearchHitInternal[] = [];
      let totalHits = 0;
      let timedOut = false;
      let indexedSegments = 0;
      let scannedSegments = 0;
      let scannedTailDocs = 0;
      const indexFamiliesUsed = new Set<string>();

      const collectSearchMatchResult = (
        offsetSeq: bigint,
        payload: Uint8Array
      ): Result<void, ReaderError> => {
        const parsedRes = decodeJsonPayloadResult(this.registry, stream, offsetSeq, payload);
        if (Result.isError(parsedRes)) return Result.err({ kind: "internal", message: parsedRes.error.message });
        const evalRes = evaluateSearchQueryResult(registry, offsetSeq, request.q, parsedRes.value);
        if (Result.isError(evalRes)) return Result.err({ kind: "internal", message: evalRes.error.message });
        if (!evalRes.value.matched) return Result.ok(undefined);
        const fieldsRes = extractSearchHitFieldsResult(registry, offsetSeq, parsedRes.value);
        if (Result.isError(fieldsRes)) return Result.err({ kind: "internal", message: fieldsRes.error.message });
        totalHits += 1;
        const sortInternal = buildSearchSortInternalValues(request.sort, fieldsRes.value, evalRes.value, offsetSeq);
        hits.push({
          offsetSeq,
          offset: encodeOffset(srow.epoch, offsetSeq),
          score: evalRes.value.score,
          sortInternal,
          sortResponse: buildSearchSortResponseValues(request.sort, sortInternal, encodeOffset(srow.epoch, offsetSeq)),
          fields: fieldsRes.value,
          source: parsedRes.value,
        });
        return Result.ok(undefined);
      };

      const scanSegmentForSearchResult = async (
        seg: SegmentRow,
        allowedDocIds: Set<number> | null
      ): Promise<Result<void, ReaderError>> => {
        const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
        let curOffset = seg.start_offset;
        for (const blockRes of iterateBlocksResult(segBytes)) {
          if (Result.isError(blockRes)) return Result.err({ kind: "internal", message: blockRes.error.message });
          for (const record of blockRes.value.decoded.records) {
            if (curOffset > snapshotEndSeq) return Result.ok(undefined);
            const localDocId = Number(curOffset - seg.start_offset);
            if (!allowedDocIds || allowedDocIds.has(localDocId)) {
              const matchRes = collectSearchMatchResult(curOffset, record.payload);
              if (Result.isError(matchRes)) return matchRes;
            }
            curOffset += 1n;
            if (deadline != null && Date.now() >= deadline) {
              timedOut = true;
              return Result.ok(undefined);
            }
          }
        }
        return Result.ok(undefined);
      };

      let seq = 0n;
      while (seq <= snapshotEndSeq && seq <= srow.sealed_through) {
        const seg = this.db.findSegmentForOffset(stream, seq);
        if (!seg) break;
        if (
          exactCandidateInfo.segments &&
          seg.segment_index < exactCandidateInfo.indexedThrough &&
          !exactCandidateInfo.segments.has(seg.segment_index)
        ) {
          seq = seg.end_offset + 1n;
          continue;
        }

        const familyCandidatesRes = await this.resolveSearchFamilyCandidatesResult(
          stream,
          seg.segment_index,
          columnClauses,
          columnIndexedThrough,
          ftsClauses,
          ftsIndexedThrough
        );
        if (Result.isError(familyCandidatesRes)) return Result.err({ kind: "internal", message: familyCandidatesRes.error.message });
        const familyCandidates = familyCandidatesRes.value;
        if (familyCandidates.docIds && familyCandidates.docIds.size === 0) {
          indexedSegments += familyCandidates.usedFamilies.size > 0 ? 1 : 0;
          for (const family of familyCandidates.usedFamilies) indexFamiliesUsed.add(family);
          seq = seg.end_offset + 1n;
          continue;
        }
        if (familyCandidates.usedFamilies.size > 0) {
          indexedSegments += 1;
          for (const family of familyCandidates.usedFamilies) indexFamiliesUsed.add(family);
        } else {
          scannedSegments += 1;
        }

        const scanRes = await scanSegmentForSearchResult(seg, familyCandidates.docIds);
        if (Result.isError(scanRes)) return scanRes;
        seq = seg.end_offset + 1n;
        if (timedOut) break;
      }

      if (!timedOut && seq <= snapshotEndSeq) {
        for (const record of this.db.iterWalRange(stream, seq, snapshotEndSeq)) {
          scannedTailDocs += 1;
          const matchRes = collectSearchMatchResult(BigInt(record.offset), record.payload);
          if (Result.isError(matchRes)) return matchRes;
          if (deadline != null && Date.now() >= deadline) {
            timedOut = true;
            break;
          }
        }
      }

      hits.sort((left, right) => compareSearchHits(left, right, request.sort));
      const filteredHits = request.searchAfter ? hits.filter((hit) => compareSearchAfter(hit, request.sort, request.searchAfter!) > 0) : hits;
      const pageHits = filteredHits.slice(0, request.size);
      const nextSearchAfter = pageHits.length === request.size ? pageHits[pageHits.length - 1].sortResponse : null;

      return Result.ok({
        stream,
        snapshotEndOffset,
        tookMs: Date.now() - startedAt,
        coverage: {
          indexedSegments,
          scannedSegments,
          scannedTailDocs,
          indexFamiliesUsed: Array.from(indexFamiliesUsed).sort(),
        },
        total: {
          value: request.trackTotalHits ? filteredHits.length : pageHits.length,
          relation: timedOut ? "gte" : "eq",
        },
        hits: pageHits.map((hit) => ({
          offset: hit.offset,
          score: hit.score,
          sort: hit.sortResponse,
          fields: hit.fields,
          source: hit.source,
        })),
        nextSearchAfter,
      });
    } catch (e: unknown) {
      return Result.err({ kind: "internal", message: errorMessage(e) });
    }
  }

  async search(args: { stream: string; request: SearchRequest }): Promise<SearchResultBatch> {
    const res = await this.searchResult(args);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  async aggregateResult(args: { stream: string; request: AggregateRequest }): Promise<Result<AggregateResultBatch, ReaderError>> {
    const { stream, request } = args;
    const srow = this.db.getStream(stream);
    if (!srow || this.db.isDeleted(srow)) return Result.err({ kind: "not_found", message: "not_found" });
    if (srow.expires_at_ms != null && this.db.nowMs() > srow.expires_at_ms) {
      return Result.err({ kind: "gone", message: "stream expired" });
    }

    const regRes = this.registry.getRegistryResult(stream);
    if (Result.isError(regRes)) return Result.err({ kind: "internal", message: regRes.error.message });
    const registry = regRes.value;
    const rollup = registry.search?.rollups?.[request.rollup];
    if (!registry.search || !rollup) {
      return Result.err({ kind: "internal", message: "rollup is not configured for this stream" });
    }

    try {
      const intervalMs = request.intervalMs;
      const intervalBig = BigInt(intervalMs);
      const fromMs = Number(request.fromMs);
      const toMs = Number(request.toMs);
      const fullStartMs = Number(((request.fromMs + intervalBig - 1n) / intervalBig) * intervalBig);
      const fullEndMs = Number((request.toMs / intervalBig) * intervalBig);
      const hasFullWindows = fullEndMs > fullStartMs;
      const dimensions = new Set(rollup.dimensions ?? []);
      const eligibility = extractRollupEligibility(request.q, dimensions);
      const aggIndexedThrough = eligibility.eligible ? Math.min(this.db.getSearchFamilyState(stream, "agg")?.uploaded_through ?? 0, this.db.countUploadedSegments(stream)) : 0;
      const selectedMeasures = new Set(request.measures ?? Object.keys(rollup.measures));

      const buckets = new Map<number, Map<string, AggregateGroupInternal>>();
      let indexedSegments = 0;
      let scannedSegments = 0;
      let scannedTailDocs = 0;
      const indexFamiliesUsed = new Set<string>();

      const mergeBucketMeasures = (bucketStartMs: number, dimensionsKey: Record<string, string | null>, measures: Record<string, AggMeasureState>): void => {
        let groups = buckets.get(bucketStartMs);
        if (!groups) {
          groups = new Map();
          buckets.set(bucketStartMs, groups);
        }
        const projectedKey: Record<string, string | null> = {};
        for (const field of request.groupBy) projectedKey[field] = dimensionsKey[field] ?? null;
        const groupKey = JSON.stringify(projectedKey);
        let group = groups.get(groupKey);
        if (!group) {
          group = { key: projectedKey, measures: {} };
          groups.set(groupKey, group);
        }
        for (const [measureName, state] of Object.entries(measures)) {
          if (!selectedMeasures.has(measureName)) continue;
          const existing = group.measures[measureName];
          if (!existing) {
            group.measures[measureName] = structuredClone(state);
            continue;
          }
          group.measures[measureName] = mergeAggMeasureState(existing, state);
        }
      };

      const matchesExactFilters = (dimensionsKey: Record<string, string | null>): boolean => {
        for (const [field, value] of Object.entries(eligibility.exactFilters)) {
          if ((dimensionsKey[field] ?? null) !== value) return false;
        }
        return true;
      };

      if (eligibility.eligible && this.index && hasFullWindows && aggIndexedThrough > 0) {
        const segments = this.db.listSegmentsForStream(stream);
        for (const seg of segments) {
          if (seg.segment_index >= aggIndexedThrough) break;
          const companion = await this.index.getAggSegmentCompanion(stream, seg.segment_index);
          const intervalCompanion = companion?.rollups?.[request.rollup]?.intervals?.[String(intervalMs)];
          if (!intervalCompanion) continue;
          let usedSegment = false;
          for (const window of intervalCompanion.windows) {
            if (window.start_ms < fullStartMs || window.start_ms >= fullEndMs) continue;
            for (const group of window.groups) {
              if (!matchesExactFilters(group.dimensions)) continue;
              mergeBucketMeasures(window.start_ms, group.dimensions, group.measures);
              usedSegment = true;
            }
          }
          if (usedSegment) {
            indexedSegments += 1;
            indexFamiliesUsed.add("agg");
          }
        }
      }

      const partialRanges: Array<{ startMs: number; endMs: number }> = [];
      if (!eligibility.eligible || aggIndexedThrough <= 0 || !hasFullWindows) {
        partialRanges.push({ startMs: fromMs, endMs: toMs });
      } else {
        if (fromMs < fullStartMs) partialRanges.push({ startMs: fromMs, endMs: fullStartMs });
        if (fullEndMs < toMs) partialRanges.push({ startMs: fullEndMs, endMs: toMs });
      }

      const scanSegmentForAggregateResult = async (
        seg: SegmentRow,
        scanRanges: Array<{ startMs: number; endMs: number }>
      ): Promise<Result<void, ReaderError>> => {
        const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
        let curOffset = seg.start_offset;
        for (const blockRes of iterateBlocksResult(segBytes)) {
          if (Result.isError(blockRes)) return Result.err({ kind: "internal", message: blockRes.error.message });
          for (const record of blockRes.value.decoded.records) {
            const parsedRes = decodeJsonPayloadResult(this.registry, stream, curOffset, record.payload);
            if (Result.isError(parsedRes)) return Result.err({ kind: "internal", message: parsedRes.error.message });
            const contributionRes = extractRollupContributionResult(registry, rollup, curOffset, parsedRes.value);
            if (Result.isError(contributionRes)) return Result.err({ kind: "internal", message: contributionRes.error.message });
            const contribution = contributionRes.value;
            if (!contribution) {
              curOffset += 1n;
              continue;
            }
            const inRange = scanRanges.some((range) => contribution.timestampMs >= range.startMs && contribution.timestampMs < range.endMs);
            if (!inRange) {
              curOffset += 1n;
              continue;
            }
            if (request.q) {
              const evalRes = evaluateSearchQueryResult(registry, curOffset, request.q, parsedRes.value);
              if (Result.isError(evalRes)) return Result.err({ kind: "internal", message: evalRes.error.message });
              if (!evalRes.value.matched) {
                curOffset += 1n;
                continue;
              }
            }
            const bucketStartMs = Math.floor(contribution.timestampMs / intervalMs) * intervalMs;
            mergeBucketMeasures(bucketStartMs, contribution.dimensions, contribution.measures);
            curOffset += 1n;
          }
        }
        scannedSegments += 1;
        return Result.ok(undefined);
      };

      const timestampField = rollup.timestampField ?? registry.search.primaryTimestampField;
      for (const seg of this.db.listSegmentsForStream(stream)) {
        const scanRanges =
          eligibility.eligible && aggIndexedThrough > 0 && hasFullWindows && seg.segment_index < aggIndexedThrough
            ? partialRanges
            : [{ startMs: fromMs, endMs: toMs }];
        if (scanRanges.length === 0) continue;
        let overlaps = false;
        for (const range of scanRanges) {
          if (await this.segmentMayOverlapTimeRange(stream, seg.segment_index, range.startMs, range.endMs, timestampField)) {
            overlaps = true;
            break;
          }
        }
        if (!overlaps) continue;
        const scanRes = await scanSegmentForAggregateResult(seg, scanRanges);
        if (Result.isError(scanRes)) return scanRes;
      }

      const tailStart = srow.sealed_through + 1n;
      const tailEnd = srow.next_offset - 1n;
      if (tailStart <= tailEnd) {
        for (const record of this.db.iterWalRange(stream, tailStart, tailEnd)) {
          scannedTailDocs += 1;
          const parsedRes = decodeJsonPayloadResult(this.registry, stream, BigInt(record.offset), record.payload);
          if (Result.isError(parsedRes)) return Result.err({ kind: "internal", message: parsedRes.error.message });
          const contributionRes = extractRollupContributionResult(registry, rollup, BigInt(record.offset), parsedRes.value);
          if (Result.isError(contributionRes)) return Result.err({ kind: "internal", message: contributionRes.error.message });
          const contribution = contributionRes.value;
          if (!contribution || contribution.timestampMs < fromMs || contribution.timestampMs >= toMs) continue;
          if (request.q) {
            const evalRes = evaluateSearchQueryResult(registry, BigInt(record.offset), request.q, parsedRes.value);
            if (Result.isError(evalRes)) return Result.err({ kind: "internal", message: evalRes.error.message });
            if (!evalRes.value.matched) continue;
          }
          const bucketStartMs = Math.floor(contribution.timestampMs / intervalMs) * intervalMs;
          mergeBucketMeasures(bucketStartMs, contribution.dimensions, contribution.measures);
        }
      }

      const bucketList = Array.from(buckets.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([startMs, groups]) => ({
          start: new Date(startMs).toISOString(),
          end: new Date(startMs + intervalMs).toISOString(),
          groups: Array.from(groups.values())
            .sort((a, b) => JSON.stringify(a.key).localeCompare(JSON.stringify(b.key)))
            .map((group) => ({
              key: group.key,
              measures: Object.fromEntries(
                Object.entries(group.measures)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([name, state]) => [name, formatAggMeasureState(state)])
              ),
            })),
        }));

      return Result.ok({
        stream,
        rollup: request.rollup,
        from: new Date(fromMs).toISOString(),
        to: new Date(toMs).toISOString(),
        interval: request.interval,
        coverage: {
          usedRollups: eligibility.eligible && aggIndexedThrough > 0 && hasFullWindows,
          indexedSegments,
          scannedSegments,
          scannedTailDocs,
          indexFamiliesUsed: Array.from(indexFamiliesUsed).sort(),
        },
        buckets: bucketList,
      });
    } catch (e: unknown) {
      return Result.err({ kind: "internal", message: errorMessage(e) });
    }
  }

  async aggregate(args: { stream: string; request: AggregateRequest }): Promise<AggregateResultBatch> {
    const res = await this.aggregateResult(args);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  private async segmentMayOverlapTimeRange(
    stream: string,
    segmentIndex: number,
    startMs: number,
    endMs: number,
    timestampField: string
  ): Promise<boolean> {
    if (!this.index) return true;
    const companion = await this.index.getColSegmentCompanion(stream, segmentIndex);
    if (!companion) return true;
    if (companion.primary_timestamp_field !== timestampField) return true;
    const minMs = companion.min_timestamp_ms == null ? null : Number(companion.min_timestamp_ms);
    const maxMs = companion.max_timestamp_ms == null ? null : Number(companion.max_timestamp_ms);
    if (!Number.isFinite(minMs) || !Number.isFinite(maxMs)) return true;
    return (maxMs as number) >= startMs && (minMs as number) < endMs;
  }

  private async resolveCandidateSegments(
    stream: string,
    keyBytes: Uint8Array | null,
    filter: CompiledReadFilter | null
  ): Promise<SegmentCandidateInfo> {
    if (!this.index) return { segments: null, indexedThrough: 0 };

    const candidates: IndexCandidate[] = [];
    if (keyBytes) {
      const keyCandidate = await this.index.candidateSegmentsForRoutingKey(stream, keyBytes);
      if (keyCandidate) candidates.push(keyCandidate);
    }
    if (filter) {
      for (const clause of collectPositiveExactFilterClauses(filter)) {
        const filterCandidate = await this.index.candidateSegmentsForSecondaryIndex(
          stream,
          clause.field,
          utf8Bytes(clause.canonicalValue)
        );
        if (filterCandidate) candidates.push(filterCandidate);
      }
    }
    if (candidates.length === 0) return { segments: null, indexedThrough: 0 };

    const indexedThrough = candidates.reduce((min, candidate) => Math.min(min, candidate.indexedThrough), Number.MAX_SAFE_INTEGER);
    if (!Number.isFinite(indexedThrough) || indexedThrough <= 0) {
      return { segments: null, indexedThrough: 0 };
    }

    let intersection: Set<number> | null = null;
    for (const candidate of candidates) {
      const covered = new Set<number>();
      for (const segmentIndex of candidate.segments) {
        if (segmentIndex < indexedThrough) covered.add(segmentIndex);
      }
      if (intersection == null) {
        intersection = covered;
        continue;
      }
      for (const segmentIndex of Array.from(intersection)) {
        if (!covered.has(segmentIndex)) intersection.delete(segmentIndex);
      }
    }
    return { segments: intersection ?? new Set<number>(), indexedThrough };
  }

  private async resolveSearchExactCandidateSegments(stream: string, query: CompiledSearchQuery): Promise<SegmentCandidateInfo> {
    if (!this.index) return { segments: null, indexedThrough: 0 };
    const clauses = collectPositiveSearchExactClauses(query);
    if (clauses.length === 0) return { segments: null, indexedThrough: 0 };

    const candidates: IndexCandidate[] = [];
    for (const clause of clauses) {
      const candidate = await this.index.candidateSegmentsForSecondaryIndex(stream, clause.field, utf8Bytes(clause.canonicalValue));
      if (candidate) candidates.push(candidate);
    }
    if (candidates.length === 0) return { segments: null, indexedThrough: 0 };

    const indexedThrough = candidates.reduce((min, candidate) => Math.min(min, candidate.indexedThrough), Number.MAX_SAFE_INTEGER);
    if (!Number.isFinite(indexedThrough) || indexedThrough <= 0) return { segments: null, indexedThrough: 0 };

    let intersection: Set<number> | null = null;
    for (const candidate of candidates) {
      const covered = new Set<number>();
      for (const segmentIndex of candidate.segments) {
        if (segmentIndex < indexedThrough) covered.add(segmentIndex);
      }
      if (intersection == null) {
        intersection = covered;
        continue;
      }
      for (const segmentIndex of Array.from(intersection)) {
        if (!covered.has(segmentIndex)) intersection.delete(segmentIndex);
      }
    }
    return { segments: intersection ?? new Set<number>(), indexedThrough };
  }

  private async resolveColumnCandidateDocIdsResult(
    stream: string,
    segmentIndex: number,
    clauses: ReadFilterColumnClause[]
  ): Promise<Result<Set<number> | null, { message: string }>> {
    if (!this.index || clauses.length === 0) return Result.ok(null);
    const companion = await this.index.getColSegmentCompanion(stream, segmentIndex);
    if (!companion) return Result.ok(null);

    let intersection: Set<number> | null = null;
    for (const clause of clauses) {
      const clauseRes = filterDocIdsByColumnResult({
        companion,
        field: clause.field,
        op: clause.op,
        value: clause.compareValue,
      });
      if (Result.isError(clauseRes)) return Result.ok(null);
      if (intersection == null) {
        intersection = clauseRes.value;
        continue;
      }
      for (const docId of Array.from(intersection)) {
        if (!clauseRes.value.has(docId)) intersection.delete(docId);
      }
      if (intersection.size === 0) break;
    }
    return Result.ok(intersection ?? new Set<number>());
  }

  private async resolveSearchColumnCandidateDocIdsResult(
    stream: string,
    segmentIndex: number,
    clauses: SearchColumnClause[]
  ): Promise<Result<Set<number> | null, { message: string }>> {
    if (!this.index || clauses.length === 0) return Result.ok(null);
    const companion = await this.index.getColSegmentCompanion(stream, segmentIndex);
    if (!companion) return Result.ok(null);

    let intersection: Set<number> | null = null;
    for (const clause of clauses) {
      const clauseRes = filterDocIdsByColumnResult({
        companion,
        field: clause.field,
        op: clause.op,
        value: clause.compareValue,
      });
      if (Result.isError(clauseRes)) return Result.ok(null);
      if (intersection == null) {
        intersection = clauseRes.value;
        continue;
      }
      for (const docId of Array.from(intersection)) {
        if (!clauseRes.value.has(docId)) intersection.delete(docId);
      }
      if (intersection.size === 0) break;
    }
    return Result.ok(intersection ?? new Set<number>());
  }

  private async resolveSearchFtsCandidateDocIdsResult(
    stream: string,
    segmentIndex: number,
    clauses: SearchFtsClause[]
  ): Promise<Result<Set<number> | null, { message: string }>> {
    if (!this.index || clauses.length === 0) return Result.ok(null);
    const companion = await this.index.getFtsSegmentCompanion(stream, segmentIndex);
    if (!companion) return Result.ok(null);

    let intersection: Set<number> | null = null;
    for (const clause of clauses) {
      const clauseRes = filterDocIdsByFtsClauseResult({ companion, clause });
      if (Result.isError(clauseRes)) return clauseRes;
      if (intersection == null) {
        intersection = clauseRes.value;
        continue;
      }
      for (const docId of Array.from(intersection)) {
        if (!clauseRes.value.has(docId)) intersection.delete(docId);
      }
      if (intersection.size === 0) break;
    }
    return Result.ok(intersection ?? new Set<number>());
  }

  private async resolveSearchFamilyCandidatesResult(
    stream: string,
    segmentIndex: number,
    columnClauses: SearchColumnClause[],
    columnIndexedThrough: number,
    ftsClauses: SearchFtsClause[],
    ftsIndexedThrough: number
  ): Promise<Result<SearchFamilyCandidateInfo, { message: string }>> {
    let intersection: Set<number> | null = null;
    const usedFamilies = new Set<string>();

    if (columnClauses.length > 0 && segmentIndex < columnIndexedThrough) {
      const columnRes = await this.resolveSearchColumnCandidateDocIdsResult(stream, segmentIndex, columnClauses);
      if (Result.isError(columnRes)) return columnRes;
      if (columnRes.value) {
        intersection = columnRes.value;
        usedFamilies.add("col");
      }
    }

    if (ftsClauses.length > 0 && segmentIndex < ftsIndexedThrough) {
      const ftsRes = await this.resolveSearchFtsCandidateDocIdsResult(stream, segmentIndex, ftsClauses);
      if (Result.isError(ftsRes)) return ftsRes;
      if (ftsRes.value) {
        if (intersection == null) intersection = ftsRes.value;
        else {
          for (const docId of Array.from(intersection)) {
            if (!ftsRes.value.has(docId)) intersection.delete(docId);
          }
        }
        usedFamilies.add("fts");
      }
    }

    return Result.ok({ docIds: intersection, usedFamilies });
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
  return true;
}

function buildSearchSortInternalValues(
  sorts: SearchSortSpec[],
  fields: Record<string, unknown>,
  evaluation: SearchEvaluation,
  offsetSeq: bigint
): Array<bigint | number | string | boolean | null> {
  return sorts.map((sort) => {
    if (sort.kind === "score") return evaluation.score;
    if (sort.kind === "offset") return offsetSeq;
    const rawValue = fields[sort.field];
    const scalar = Array.isArray(rawValue) ? rawValue[0] : rawValue;
    if (scalar == null) return null;
    if (sort.config.kind === "integer" || sort.config.kind === "float" || sort.config.kind === "date" || sort.config.kind === "bool") {
      return canonicalizeColumnValue(sort.config, scalar);
    }
    return canonicalizeExactValue(sort.config, scalar);
  });
}

function buildSearchSortResponseValues(
  sorts: SearchSortSpec[],
  sortInternal: Array<bigint | number | string | boolean | null>,
  offset: string
): unknown[] {
  return sorts.map((sort, index) => {
    const value = sortInternal[index];
    if (sort.kind === "offset") return offset;
    if (typeof value === "bigint") return Number(value);
    return value;
  });
}

function compareComparableValues(left: bigint | number | string | boolean | null, right: bigint | number | string | boolean | null): number {
  if (left == null && right == null) return 0;
  if (left == null) return 1;
  if (right == null) return -1;
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  const ls = String(left);
  const rs = String(right);
  return ls < rs ? -1 : ls > rs ? 1 : 0;
}

function compareSearchHits(left: SearchHitInternal, right: SearchHitInternal, sorts: SearchSortSpec[]): number {
  for (let i = 0; i < sorts.length; i++) {
    const cmp = compareComparableValues(left.sortInternal[i] ?? null, right.sortInternal[i] ?? null);
    if (cmp === 0) continue;
    return sorts[i].direction === "asc" ? cmp : -cmp;
  }
  return 0;
}

function normalizeSearchAfterValue(sort: SearchSortSpec, raw: unknown): bigint | number | string | boolean | null {
  if (raw == null) return null;
  if (sort.kind === "offset") {
    if (typeof raw !== "string") return null;
    const parsed = parseOffsetResult(raw);
    if (Result.isError(parsed)) return null;
    return offsetToSeqOrNeg1(parsed.value);
  }
  if (sort.kind === "score") {
    return typeof raw === "number" && Number.isFinite(raw) ? raw : null;
  }
  if (sort.config.kind === "integer" || sort.config.kind === "date") {
    if (typeof raw === "number" && Number.isFinite(raw)) return BigInt(Math.trunc(raw));
    if (typeof raw === "string" && raw.trim() !== "") {
      try {
        return BigInt(raw.trim());
      } catch {
        return null;
      }
    }
    return null;
  }
  if (sort.config.kind === "float") return typeof raw === "number" && Number.isFinite(raw) ? raw : null;
  if (sort.config.kind === "bool") return typeof raw === "boolean" ? raw : null;
  return typeof raw === "string" ? raw : null;
}

function compareSearchAfter(hit: SearchHitInternal, sorts: SearchSortSpec[], searchAfter: unknown[]): number {
  for (let i = 0; i < sorts.length; i++) {
    const after = normalizeSearchAfterValue(sorts[i], searchAfter[i]);
    const cmp = compareComparableValues(hit.sortInternal[i] ?? null, after);
    if (cmp === 0) continue;
    return sorts[i].direction === "asc" ? cmp : -cmp;
  }
  return 0;
}
