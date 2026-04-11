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
import {
  type BlockIndexEntry,
  type SegmentFormatError,
  decodeBlockResult,
  iterateBlocksResult,
  parseBlockHeaderResult,
  parseFooter,
  parseFooterBytes,
  DSB3_HEADER_BYTES,
} from "./segment/format";
import { SegmentDiskCache, type SegmentCacheStats } from "./segment/cache";
import { loadSegmentBytesCached, loadSegmentSource, readRangeFromSource, type SegmentReadSource } from "./segment/cached_segment";
import { Bloom256 } from "./util/bloom256";
import { readU32BE } from "./util/endian";
import { type RetryOptions } from "./util/retry";
import type { IndexCandidate, IndexTailMatcher, StreamIndexLookup } from "./index/indexer";
import { dsError } from "./util/ds_error.ts";
import { Result } from "better-result";
import { filterDocIdsByColumnResult } from "./search/col_runtime";
import {
  type AggregateRequest,
  cloneAggMeasureState,
  extractRollupContributionResult,
  extractRollupEligibility,
  formatAggMeasureState,
  mergeAggMeasureState,
} from "./search/aggregate";
import {
  analyzeSearchIndexedSupport,
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
  hasNonKeywordSearchFtsClauses,
  isSearchExactVisibilityEligible,
} from "./search/query";
import { filterDocIdsByFtsClausesResult, planNewestFirstFtsClausesResult } from "./search/fts_runtime";
import { canonicalizeColumnValue, canonicalizeExactValue } from "./search/schema";
import { encodeSortableBool, encodeSortableFloat64, encodeSortableInt64 } from "./search/column_encoding";
import type { SearchRollupConfig } from "./schema/registry";
import type { AggMeasureState } from "./search/agg_format";
import type { MetricsBlockSectionView } from "./profiles/metrics/block_format";
import { materializeMetricsBlockRecord } from "./profiles/metrics/normalize";
import { buildDesiredSearchCompanionPlan, hashSearchCompanionPlan } from "./search/companion_plan";
import { RuntimeMemorySampler } from "./runtime_memory_sampler";
import type { MemoryPressureMonitor } from "./memory";

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
  timedOut: boolean;
  timeoutMs: number | null;
  coverage: {
    mode: "complete" | "published";
    complete: boolean;
    streamHeadOffset: string;
    visibleThroughOffset: string;
    visibleThroughPrimaryTimestampMax: string | null;
    oldestOmittedAppendAt: string | null;
    possibleMissingEventsUpperBound: number;
    possibleMissingUploadedSegments: number;
    possibleMissingSealedRows: number;
    possibleMissingWalRows: number;
    indexedSegments: number;
    indexedSegmentTimeMs: number;
    ftsSectionGetMs: number;
    ftsDecodeMs: number;
    ftsClauseEstimateMs: number;
    scannedSegments: number;
    scannedSegmentTimeMs: number;
    scannedTailDocs: number;
    scannedTailTimeMs: number;
    exactCandidateTimeMs: number;
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
    mode: "complete" | "published";
    complete: boolean;
    streamHeadOffset: string;
    visibleThroughOffset: string;
    visibleThroughPrimaryTimestampMax: string | null;
    oldestOmittedAppendAt: string | null;
    possibleMissingEventsUpperBound: number;
    possibleMissingUploadedSegments: number;
    possibleMissingSealedRows: number;
    possibleMissingWalRows: number;
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

const READ_FILTER_SCAN_LIMIT_BYTES = 100 * 1024 * 1024;
type SegmentCandidateInfo = { segments: Set<number> | null; indexedThrough: number };
type SearchExactTailCandidateInfo = { matchers: IndexTailMatcher[]; indexedThrough: number } | null;
type SearchDocIdCandidates = {
  size: number;
  minDocId: number;
  maxDocId: number;
  sortedDocIds: Uint32Array;
};
type SearchFamilyCandidateInfo = { docIds: SearchDocIdCandidates | null; usedFamilies: Set<string> };
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
type SearchCursorFieldBound = {
  kind: "field";
  sort: Extract<SearchSortSpec, { kind: "field" }>;
  after: bigint | number | string | boolean | null;
  encoded: Uint8Array | null;
};
type PublishedCoverageState = {
  mode: "complete" | "published";
  complete: boolean;
  canSearchWalTail: boolean;
  publishedSegmentCount: number;
  visiblePublishedSegmentCount: number;
  streamHeadOffset: string;
  visibleThroughSeq: bigint;
  visibleThroughOffset: string;
  visibleThroughPrimaryTimestampMax: string | null;
  oldestOmittedAppendAt: string | null;
  possibleMissingEventsUpperBound: number;
  possibleMissingUploadedSegments: number;
  possibleMissingSealedRows: number;
  possibleMissingWalRows: number;
};

type PlannedReadSegments = {
  segments: SegmentRow[];
  sealedEndSeq: bigint;
};
type PlannedReadOrder = "asc" | "desc";
type PublishedCoverageOptions = {
  visiblePublishedSegmentCountOverride?: number | null;
  requiredCompanionFamilies?: ReadonlySet<"col" | "fts">;
  ignoreCompanionVisibility?: boolean;
  disableWalTail?: boolean;
};

function errorMessage(e: unknown): string {
  return String((e as any)?.message ?? e);
}

function utf8Bytes(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function parseCompanionSections(value: string): Set<string> {
  try {
    const parsed = JSON.parse(value);
    return new Set(Array.isArray(parsed) ? parsed.filter((entry) => typeof entry === "string") : []);
  } catch {
    return new Set();
  }
}

function companionRowHasFamilies(row: { sections_json: string }, requiredFamilies: ReadonlySet<"col" | "fts">): boolean {
  const sections = parseCompanionSections(row.sections_json);
  for (const family of requiredFamilies) {
    if (!sections.has(family)) return false;
  }
  return true;
}

async function loadSegmentBytes(
  os: ObjectStore,
  seg: SegmentRow,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions
): Promise<Uint8Array> {
  return loadSegmentBytesCached(os, seg, diskCache, retryOpts);
}

function loadSegmentDataLimitFromSource(seg: SegmentRow, source: SegmentReadSource): number {
  if (seg.size_bytes < 8) return seg.size_bytes;
  const tail = readRangeFromSource(source, seg.size_bytes - 8, seg.size_bytes - 1);
  const magic = String.fromCharCode(tail[4], tail[5], tail[6], tail[7]);
  if (magic !== "DSF1") return seg.size_bytes;
  const footerLen = readU32BE(tail, 0);
  const footerStart = seg.size_bytes - 8 - footerLen;
  return footerStart >= 0 ? footerStart : seg.size_bytes;
}

function findFirstRelevantBlockIndex(blocks: BlockIndexEntry[], seq: bigint): number {
  if (blocks.length <= 1) return 0;
  let lo = 0;
  let hi = blocks.length - 1;
  let best = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    if (blocks[mid]!.firstOffset <= seq) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return best;
}

function loadSegmentFooterBlocksFromSource(seg: SegmentRow, source: SegmentReadSource): BlockIndexEntry[] | null {
  if (seg.size_bytes < 8) return null;
  const tail = readRangeFromSource(source, seg.size_bytes - 8, seg.size_bytes - 1);
  const magic = String.fromCharCode(tail[4], tail[5], tail[6], tail[7]);
  if (magic !== "DSF1") return null;
  const footerLen = readU32BE(tail, 0);
  const footerStart = seg.size_bytes - 8 - footerLen;
  if (footerStart < 0) return null;
  const footerBytes = readRangeFromSource(source, footerStart, footerStart + footerLen - 1);
  const footer = parseFooterBytes(footerBytes);
  return footer?.blocks ?? null;
}

function loadSegmentBlockEntriesFromSourceResult(
  seg: SegmentRow,
  source: SegmentReadSource
): Result<BlockIndexEntry[], SegmentFormatError> {
  const footerBlocks = loadSegmentFooterBlocksFromSource(seg, source);
  if (footerBlocks) return Result.ok(footerBlocks);

  const limit = loadSegmentDataLimitFromSource(seg, source);
  const blocks: BlockIndexEntry[] = [];
  let blockOffset = 0;
  let blockFirstOffset = seg.start_offset;
  while (blockOffset < limit) {
    const headerBytes = readRangeFromSource(source, blockOffset, blockOffset + DSB3_HEADER_BYTES - 1);
    const headerRes = parseBlockHeaderResult(headerBytes);
    if (Result.isError(headerRes)) return headerRes;
    const header = headerRes.value;
    blocks.push({
      blockOffset,
      firstOffset: blockFirstOffset,
      recordCount: header.recordCount,
      compressedLen: header.compressedLen,
      firstAppendNs: header.firstAppendNs,
      lastAppendNs: header.lastAppendNs,
    });
    blockOffset += DSB3_HEADER_BYTES + header.compressedLen;
    blockFirstOffset += BigInt(header.recordCount);
  }
  return Result.ok(blocks);
}

function searchDocIdsLowerBound(sortedDocIds: Uint32Array, target: number): number {
  let low = 0;
  let high = sortedDocIds.length;
  while (low < high) {
    const mid = (low + high) >>> 1;
    if (sortedDocIds[mid]! < target) low = mid + 1;
    else high = mid;
  }
  return low;
}

function buildSearchDocIdCandidates(docIds: Set<number>): SearchDocIdCandidates {
  const sortedDocIds = Uint32Array.from(docIds);
  sortedDocIds.sort();
  return {
    size: sortedDocIds.length,
    minDocId: sortedDocIds.length > 0 ? sortedDocIds[0]! : 0,
    maxDocId: sortedDocIds.length > 0 ? sortedDocIds[sortedDocIds.length - 1]! : -1,
    sortedDocIds,
  };
}

function searchDocIdsHas(docIds: SearchDocIdCandidates | null, docId: number): boolean {
  if (!docIds) return true;
  if (docId < docIds.minDocId || docId > docIds.maxDocId) return false;
  const index = searchDocIdsLowerBound(docIds.sortedDocIds, docId);
  return index < docIds.size && docIds.sortedDocIds[index] === docId;
}

function searchDocIdsIntersectRange(docIds: SearchDocIdCandidates | null, start: number, end: number): boolean {
  if (!docIds) return true;
  if (end < docIds.minDocId || start > docIds.maxDocId) return false;
  const index = searchDocIdsLowerBound(docIds.sortedDocIds, start);
  return index < docIds.size && docIds.sortedDocIds[index]! <= end;
}

function intersectDocIdSets(left: Set<number> | null, right: Set<number>): Set<number> {
  if (left == null) return right;
  const target = left.size <= right.size ? left : right;
  const probe = target === left ? right : left;
  for (const docId of Array.from(target)) {
    if (!probe.has(docId)) target.delete(docId);
  }
  return target;
}

export const searchDocIdCandidatesTestHelpers = {
  build(docIds: Iterable<number>) {
    return buildSearchDocIdCandidates(new Set(docIds));
  },
  has(docIds: SearchDocIdCandidates | null, docId: number): boolean {
    return searchDocIdsHas(docIds, docId);
  },
  intersectsRange(docIds: SearchDocIdCandidates | null, start: number, end: number): boolean {
    return searchDocIdsIntersectRange(docIds, start, end);
  },
} as const;

export class StreamReader {
  private readonly config: Config;
  private readonly db: SqliteDurableStore;
  private readonly os: ObjectStore;
  private readonly registry: SchemaRegistryStore;
  private readonly diskCache?: SegmentDiskCache;
  private readonly index?: StreamIndexLookup;
  private readonly memorySampler?: RuntimeMemorySampler;
  private readonly memory?: MemoryPressureMonitor;

  constructor(
    config: Config,
    db: SqliteDurableStore,
    os: ObjectStore,
    registry: SchemaRegistryStore,
    diskCache?: SegmentDiskCache,
    index?: StreamIndexLookup,
    memorySampler?: RuntimeMemorySampler,
    memory?: MemoryPressureMonitor
  ) {
    this.config = config;
    this.db = db;
    this.os = os;
    this.registry = registry;
    this.diskCache = diskCache;
    this.index = index;
    this.memorySampler = memorySampler;
    this.memory = memory;
  }

  private planSealedReadSegments(
    stream: string,
    startSeq: bigint,
    sealedEndSeq: bigint,
    candidateSegments: Set<number> | null,
    indexedThrough: number,
    order: PlannedReadOrder = "asc"
  ): PlannedReadSegments | null {
    if (startSeq > sealedEndSeq) return { segments: [], sealedEndSeq };
    if (candidateSegments == null) return null;

    const startSeg = this.db.findSegmentForOffset(stream, startSeq);
    const endSeg = this.db.findSegmentForOffset(stream, sealedEndSeq);
    if (!startSeg || !endSeg) return null;

    const startIndex = startSeg.segment_index;
    const endIndex = endSeg.segment_index;
    const plannedIndexes: number[] = [];
    const seenIndexes = new Set<number>();
    const indexedPrefixEnd = Math.min(endIndex, indexedThrough - 1);

    if (order === "asc") {
      if (startIndex <= indexedPrefixEnd) {
        const sortedCandidateIndexes = Array.from(candidateSegments)
          .filter((segmentIndex) => segmentIndex >= startIndex && segmentIndex <= indexedPrefixEnd)
          .sort((a, b) => a - b);
        for (const segmentIndex of sortedCandidateIndexes) {
          if (seenIndexes.has(segmentIndex)) continue;
          plannedIndexes.push(segmentIndex);
          seenIndexes.add(segmentIndex);
        }
      }

      const tailStartIndex = Math.max(startIndex, indexedThrough);
      for (let segmentIndex = tailStartIndex; segmentIndex <= endIndex; segmentIndex++) {
        if (seenIndexes.has(segmentIndex)) continue;
        plannedIndexes.push(segmentIndex);
        seenIndexes.add(segmentIndex);
      }
    } else {
      for (let segmentIndex = endIndex; segmentIndex >= Math.max(startIndex, indexedThrough); segmentIndex--) {
        if (seenIndexes.has(segmentIndex)) continue;
        plannedIndexes.push(segmentIndex);
        seenIndexes.add(segmentIndex);
      }
      if (startIndex <= indexedPrefixEnd) {
        const sortedCandidateIndexes = Array.from(candidateSegments)
          .filter((segmentIndex) => segmentIndex >= startIndex && segmentIndex <= indexedPrefixEnd)
          .sort((a, b) => b - a);
        for (const segmentIndex of sortedCandidateIndexes) {
          if (seenIndexes.has(segmentIndex)) continue;
          plannedIndexes.push(segmentIndex);
          seenIndexes.add(segmentIndex);
        }
      }
    }

    const plannedSegments: SegmentRow[] = [];
    for (const segmentIndex of plannedIndexes) {
      const seg = this.db.getSegmentByIndex(stream, segmentIndex);
      if (!seg) return null;
      plannedSegments.push(seg);
    }
    return { segments: plannedSegments, sealedEndSeq };
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

  private isoTimestampFromMs(value: bigint | null): string | null {
    if (value == null) return null;
    const ms = Number(value);
    if (!Number.isFinite(ms) || ms < 0) return null;
    return new Date(ms).toISOString();
  }

  private shouldSearchWalTail(
    srow: { pending_rows: bigint; pending_bytes: bigint; last_append_ms: bigint; segment_in_progress: number },
    hasOutstandingPublishedSegments: boolean,
    hasOutstandingCompanions: boolean
  ): boolean {
    if (srow.pending_rows <= 0n) return false;
    if (hasOutstandingPublishedSegments || hasOutstandingCompanions) return false;
    if (srow.segment_in_progress !== 0) return false;
    const quietPeriodMs = Math.max(0, this.config.searchWalOverlayQuietPeriodMs);
    const quietForMs = Number(this.db.nowMs() - srow.last_append_ms);
    if (!Number.isFinite(quietForMs) || quietForMs < quietPeriodMs) return false;
    if (srow.pending_bytes > BigInt(this.config.searchWalOverlayMaxBytes)) return false;
    if (srow.pending_rows > BigInt(this.config.segmentTargetRows)) return false;
    return true;
  }

  private computeOldestOmittedAppendAt(
    stream: string,
    srow: { uploaded_through: bigint; sealed_through: bigint; pending_rows: bigint },
    visiblePublishedSegmentCount: number,
    publishedSegmentCount: number,
    shouldSearchWalTail: boolean
  ): string | null {
    if (visiblePublishedSegmentCount < publishedSegmentCount) {
      const firstOmittedSegment = this.db.getSegmentByIndex(stream, visiblePublishedSegmentCount);
      return this.isoTimestampFromMs(firstOmittedSegment?.last_append_ms ?? null);
    }
    if (srow.sealed_through > srow.uploaded_through) {
      const firstSealedOmitted = this.db.findSegmentForOffset(stream, srow.uploaded_through + 1n);
      return this.isoTimestampFromMs(firstSealedOmitted?.last_append_ms ?? null);
    }
    if (srow.pending_rows > 0n && !shouldSearchWalTail) {
      return this.isoTimestampFromMs(this.db.getWalOldestTimestampMs(stream));
    }
    return null;
  }

  private computePublishedCoverageState(
    stream: string,
    srow: {
      epoch: number;
      next_offset: bigint;
      sealed_through: bigint;
      uploaded_through: bigint;
      pending_rows: bigint;
      pending_bytes: bigint;
      last_append_ms: bigint;
      segment_in_progress: number;
    },
    registry: { search?: { fields: Record<string, unknown> } },
    options?: PublishedCoverageOptions
  ): PublishedCoverageState {
    const totalSegmentCount = this.db.countSegmentsForStream(stream);
    const publishedSegmentCount =
      srow.uploaded_through >= 0n
        ? ((this.db.findSegmentForOffset(stream, srow.uploaded_through)?.segment_index ?? -1) + 1)
        : 0;

    const desiredPlan = buildDesiredSearchCompanionPlan(registry as any);
    const requestedCompanionFamilies =
      options?.requiredCompanionFamilies == null
        ? new Set<"col" | "fts">()
        : new Set(options.requiredCompanionFamilies);
    const planHasFamilies =
      requestedCompanionFamilies.size > 0
        ? Array.from(requestedCompanionFamilies).some((family) => desiredPlan.families[family] === true)
        : Object.values(desiredPlan.families).some(Boolean);
    const requireCompanionVisibility = planHasFamilies && options?.ignoreCompanionVisibility !== true;
    let visiblePublishedSegmentCount =
      options?.visiblePublishedSegmentCountOverride == null
        ? publishedSegmentCount
        : Math.max(0, Math.min(publishedSegmentCount, options.visiblePublishedSegmentCountOverride));
    let visibleThroughPrimaryTimestampMax: string | null = null;
    if (requireCompanionVisibility && options?.visiblePublishedSegmentCountOverride == null) {
      const desiredHash = hashSearchCompanionPlan(desiredPlan);
      const companionPlanRow = this.db.getSearchCompanionPlan(stream);
      const desiredGeneration =
        companionPlanRow == null
          ? 1
          : companionPlanRow.plan_hash === desiredHash
            ? companionPlanRow.generation
            : companionPlanRow.generation + 1;
      const currentCompanions = this.db
        .listSearchSegmentCompanions(stream)
        .filter((row) => row.plan_generation === desiredGeneration);
      visiblePublishedSegmentCount = 0;
      while (visiblePublishedSegmentCount < publishedSegmentCount) {
        const row = currentCompanions.find((entry) => entry.segment_index === visiblePublishedSegmentCount) ?? null;
        if (!row) break;
        const hasRequiredFamilies =
          requestedCompanionFamilies.size > 0
            ? companionRowHasFamilies(row, requestedCompanionFamilies)
            : Object.entries(desiredPlan.families).some(([family, enabled]) => enabled && parseCompanionSections(row.sections_json).has(family));
        if (!hasRequiredFamilies) break;
        visiblePublishedSegmentCount += 1;
      }
      if (visiblePublishedSegmentCount > 0) {
        const visibleCompanionRow = currentCompanions.find((row) => row.segment_index === visiblePublishedSegmentCount - 1) ?? null;
        visibleThroughPrimaryTimestampMax = this.isoTimestampFromMs(visibleCompanionRow?.primary_timestamp_max_ms ?? null);
      }
    }
    if (options?.visiblePublishedSegmentCountOverride != null) {
      visiblePublishedSegmentCount = Math.min(visiblePublishedSegmentCount, options.visiblePublishedSegmentCountOverride);
    }

    const hasOutstandingPublishedSegments = publishedSegmentCount < totalSegmentCount;
    const hasVisibilityGapWithinPublished = visiblePublishedSegmentCount < publishedSegmentCount;
    const hasOutstandingCompanions = requireCompanionVisibility && hasVisibilityGapWithinPublished;
    const canSearchWalTail =
      options?.disableWalTail === true
        ? false
        : this.shouldSearchWalTail(srow, hasOutstandingPublishedSegments, hasOutstandingCompanions);
    const omitWalTail = srow.pending_rows > 0n && !canSearchWalTail;

    let visibleThroughSeq = srow.next_offset - 1n;
    if (hasOutstandingPublishedSegments || hasVisibilityGapWithinPublished || omitWalTail) {
      if (visiblePublishedSegmentCount > 0) {
        visibleThroughSeq = this.db.getSegmentByIndex(stream, visiblePublishedSegmentCount - 1)?.end_offset ?? -1n;
      } else {
        visibleThroughSeq = -1n;
      }
    }

    const possibleMissingUploadedSegments = Math.max(0, publishedSegmentCount - visiblePublishedSegmentCount);
    const hasOmittedPublishedSuffix = hasOutstandingPublishedSegments || hasVisibilityGapWithinPublished;
    const possibleMissingUploadedRows = hasOmittedPublishedSuffix && srow.uploaded_through > visibleThroughSeq ? Number(srow.uploaded_through - visibleThroughSeq) : 0;
    const possibleMissingSealedRows = hasOmittedPublishedSuffix && srow.sealed_through > srow.uploaded_through ? Number(srow.sealed_through - srow.uploaded_through) : 0;
    const possibleMissingWalRows = omitWalTail ? Number(srow.pending_rows) : 0;
    const possibleMissingEventsUpperBound = possibleMissingUploadedRows + possibleMissingSealedRows + possibleMissingWalRows;
    const streamHeadOffset = encodeOffset(srow.epoch, srow.next_offset - 1n);
    const oldestOmittedAppendAt = this.computeOldestOmittedAppendAt(
      stream,
      srow,
      visiblePublishedSegmentCount,
      publishedSegmentCount,
      canSearchWalTail
    );

    return {
      mode: possibleMissingEventsUpperBound === 0 ? "complete" : "published",
      complete: possibleMissingEventsUpperBound === 0,
      canSearchWalTail,
      publishedSegmentCount,
      visiblePublishedSegmentCount,
      streamHeadOffset,
      visibleThroughSeq,
      visibleThroughOffset: encodeOffset(srow.epoch, visibleThroughSeq),
      visibleThroughPrimaryTimestampMax,
      oldestOmittedAppendAt,
      possibleMissingEventsUpperBound,
      possibleMissingUploadedSegments,
      possibleMissingSealedRows,
      possibleMissingWalRows,
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
      const candidateInfo = await this.resolveCandidateSegments(stream, keyBytes, null);
      const plannedSealedSegments = this.planSealedReadSegments(
        stream,
        0n,
        srow.sealed_through,
        candidateInfo.segments,
        candidateInfo.indexedThrough,
        "asc"
      );

      for (const seg of plannedSealedSegments?.segments ?? this.db.listSegmentsForStream(stream)) {
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
        const footer = parseFooter(segBytes)?.footer;
        if (footer) {
          for (let blockIndex = findFirstRelevantBlockIndex(footer.blocks, seq); blockIndex < footer.blocks.length; blockIndex++) {
            const block = footer.blocks[blockIndex]!;
            const blockStart = block.firstOffset;
            const blockEnd = blockStart + BigInt(block.recordCount) - 1n;
            if (blockEnd < seq) continue;
            if (blockStart > endOffsetNum) break;

            if (keyBytes) {
              const headerBytes = segBytes.subarray(block.blockOffset, block.blockOffset + DSB3_HEADER_BYTES);
              const headerRes = parseBlockHeaderResult(headerBytes);
              if (Result.isError(headerRes)) return Result.err({ kind: "internal", message: headerRes.error.message });
              const bloom = new Bloom256(headerRes.value.bloom);
              if (!bloom.maybeHas(keyBytes)) continue;
            }

            const totalLen = DSB3_HEADER_BYTES + block.compressedLen;
            const blockBytes = segBytes.subarray(block.blockOffset, block.blockOffset + totalLen);
            const decodedRes = decodeBlockResult(blockBytes);
            if (Result.isError(decodedRes)) return Result.err({ kind: "internal", message: decodedRes.error.message });
            const decoded = decodedRes.value;
            let curOffset = blockStart;
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
        }

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

      const scanSegmentSource = async (
        source: SegmentReadSource,
        seg: SegmentRow,
        allowedDocIds: Set<number> | null
      ): Promise<Result<void, ReaderError>> => {
        const footerBlocks = loadSegmentFooterBlocksFromSource(seg, source);
        if (footerBlocks) {
          for (let blockIndex = findFirstRelevantBlockIndex(footerBlocks, seq); blockIndex < footerBlocks.length; blockIndex++) {
            const block = footerBlocks[blockIndex]!;
            const blockStart = block.firstOffset;
            const blockEnd = blockStart + BigInt(block.recordCount) - 1n;
            if (blockEnd < seq) continue;
            if (blockStart > endOffsetNum) break;

            const headerBytes = readRangeFromSource(source, block.blockOffset, block.blockOffset + DSB3_HEADER_BYTES - 1);
            const headerRes = parseBlockHeaderResult(headerBytes);
            if (Result.isError(headerRes)) return Result.err({ kind: "internal", message: headerRes.error.message });
            if (keyBytes) {
              const bloom = new Bloom256(headerRes.value.bloom);
              if (!bloom.maybeHas(keyBytes)) continue;
            }

            const totalLen = DSB3_HEADER_BYTES + block.compressedLen;
            const blockBytes = readRangeFromSource(source, block.blockOffset, block.blockOffset + totalLen - 1);
            const decodedRes = decodeBlockResult(blockBytes);
            if (Result.isError(decodedRes)) return Result.err({ kind: "internal", message: decodedRes.error.message });
            const decoded = decodedRes.value;
            let curOffset = blockStart;
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
        }

        const limit = loadSegmentDataLimitFromSource(seg, source);
        let blockOffset = 0;
        let blockFirstOffset = seg.start_offset;
        while (blockOffset < limit) {
          const headerBytes = readRangeFromSource(source, blockOffset, blockOffset + DSB3_HEADER_BYTES - 1);
          const headerRes = parseBlockHeaderResult(headerBytes);
          if (Result.isError(headerRes)) return Result.err({ kind: "internal", message: headerRes.error.message });
          const header = headerRes.value;
          const totalLen = DSB3_HEADER_BYTES + header.compressedLen;
          const blockStart = blockFirstOffset;
          const blockEnd = blockStart + BigInt(header.recordCount) - 1n;
          if (blockEnd < seq) {
            blockOffset += totalLen;
            blockFirstOffset = blockEnd + 1n;
            continue;
          }
          if (blockStart > endOffsetNum) break;

          if (keyBytes) {
            const bloom = new Bloom256(header.bloom);
            if (!bloom.maybeHas(keyBytes)) {
              blockOffset += totalLen;
              blockFirstOffset = blockEnd + 1n;
              continue;
            }
          }

          const blockBytes = readRangeFromSource(source, blockOffset, blockOffset + totalLen - 1);
          const decodedRes = decodeBlockResult(blockBytes);
          if (Result.isError(decodedRes)) return Result.err({ kind: "internal", message: decodedRes.error.message });
          const decoded = decodedRes.value;
          let curOffset = blockStart;
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
          blockOffset += totalLen;
          blockFirstOffset = blockEnd + 1n;
        }
        return Result.ok(undefined);
      };

      const sealedEndSeq = endOffsetNum < srow.sealed_through ? endOffsetNum : srow.sealed_through;
      const plannedSealedSegments = this.planSealedReadSegments(
        stream,
        seq,
        sealedEndSeq,
        candidateSegments,
        indexedThrough,
        "asc"
      );

      // 1) Read from sealed segments.
      if (plannedSealedSegments) {
        for (const seg of plannedSealedSegments.segments) {
          if (seg.end_offset < seq) continue;
          if (seg.start_offset > sealedEndSeq) break;
          let allowedDocIds: Set<number> | null = null;
          if (columnClauses.length > 0) {
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
            const source = await loadSegmentSource(this.os, seg, this.diskCache, this.retryOpts());
            const scanRes = await scanSegmentSource(source, seg, allowedDocIds);
            if (Result.isError(scanRes)) return scanRes;
            if (filterScanLimitReached) return Result.ok(finalize());
            if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) return Result.ok(finalize());
          }
          seq = seg.end_offset + 1n;
        }
        if (seq <= plannedSealedSegments.sealedEndSeq) {
          seq = plannedSealedSegments.sealedEndSeq + 1n;
        }
      } else {
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
        if (columnClauses.length > 0) {
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
          const source = await loadSegmentSource(this.os, seg, this.diskCache, this.retryOpts());
          const scanRes = await scanSegmentSource(source, seg, allowedDocIds);
          if (Result.isError(scanRes)) return scanRes;
          if (filterScanLimitReached) return Result.ok(finalize());
          if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) return Result.ok(finalize());
        }

          // Move to next segment.
          seq = seg.end_offset + 1n;
        }
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

  async searchResult(args: {
    stream: string;
    request: SearchRequest;
    signal?: AbortSignal | null;
  }): Promise<Result<SearchResultBatch, ReaderError>> {
    const startedAt = Date.now();
    const { stream, request, signal } = args;
    const leaveSearchPhase = this.memorySampler?.enter("search", {
      stream,
      has_query: request.q != null,
      over_limit: this.memory?.isOverLimit() === true,
    });
    const srow = this.db.getStream(stream);
    try {
      if (!srow || this.db.isDeleted(srow)) return Result.err({ kind: "not_found", message: "not_found" });
      if (srow.expires_at_ms != null && this.db.nowMs() > srow.expires_at_ms) {
        return Result.err({ kind: "gone", message: "stream expired" });
      }

      const regRes = this.registry.getRegistryResult(stream);
      if (Result.isError(regRes)) return Result.err({ kind: "internal", message: regRes.error.message });
      const registry = regRes.value;
      if (!registry.search) return Result.err({ kind: "internal", message: "search is not configured for this stream" });

      const snapshotEndSeq = srow.next_offset - 1n;
      const snapshotEndOffset = encodeOffset(srow.epoch, snapshotEndSeq);
      const deadline = request.timeoutMs == null ? null : Date.now() + request.timeoutMs;
      const leadingSort = request.sort[0] ?? null;
      const offsetSearchAfter =
        request.searchAfter && leadingSort?.kind === "offset" ? normalizeSearchAfterValue(leadingSort, request.searchAfter[0]) : null;
      const cursorFieldBound = resolveSearchCursorFieldBound(request);

      const hits: SearchHitInternal[] = [];
      let timedOut = false;
      const markTimedOutIfNeeded = (): boolean => {
        if (signal?.aborted) {
          timedOut = true;
          return true;
        }
        if (deadline == null || Date.now() < deadline) return false;
        timedOut = true;
        return true;
      };
      let indexedSegments = 0;
      let indexedSegmentTimeMs = 0;
      let ftsSectionGetMs = 0;
      let ftsDecodeMs = 0;
      let ftsClauseEstimateMs = 0;
      let scannedSegments = 0;
      let scannedSegmentTimeMs = 0;
      let scannedTailDocs = 0;
      let scannedTailTimeMs = 0;
      const indexFamiliesUsed = new Set<string>();
      const indexedSupport = analyzeSearchIndexedSupport(request.q);
      const columnClauses = collectPositiveSearchColumnClauses(request.q);
      const ftsClauses = collectPositiveSearchFtsClauses(request.q);
      const hasNonKeywordFtsClauses = hasNonKeywordSearchFtsClauses(ftsClauses);
      const exactClauses = collectPositiveSearchExactClauses(request.q, {
        excludeFtsKeywordClauses: hasNonKeywordFtsClauses,
      });
      let exactTailCandidateInfo: SearchExactTailCandidateInfo = null;
      let exactCandidateInfo: SegmentCandidateInfo = { segments: null, indexedThrough: 0 };
      let exactCandidateTimeMs = 0;
      if (!markTimedOutIfNeeded()) {
        const exactCandidateStartedAt = Date.now();
        if (this.canUseSearchExactTailMatchers(request, cursorFieldBound, ftsClauses)) {
          exactTailCandidateInfo = await this.resolveSearchExactTailCandidateInfo(stream, exactClauses);
          if (exactTailCandidateInfo) {
            exactCandidateInfo = { segments: null, indexedThrough: exactTailCandidateInfo.indexedThrough };
          } else {
            exactCandidateInfo = await this.resolveSearchExactCandidateSegments(stream, exactClauses);
          }
        } else {
          exactCandidateInfo = await this.resolveSearchExactCandidateSegments(stream, exactClauses);
        }
        exactCandidateTimeMs = Date.now() - exactCandidateStartedAt;
        markTimedOutIfNeeded();
      }
      const useExactVisibility =
        indexedSupport.indexedOnly &&
        indexedSupport.requiresExact &&
        indexedSupport.requiredCompanionFamilies.size === 0 &&
        isSearchExactVisibilityEligible(request.q) &&
        exactCandidateInfo.indexedThrough > 0;
      const coverageState = this.computePublishedCoverageState(
        stream,
        srow,
        registry,
        indexedSupport.indexedOnly
          ? {
              visiblePublishedSegmentCountOverride: useExactVisibility ? exactCandidateInfo.indexedThrough : null,
              requiredCompanionFamilies: indexedSupport.requiredCompanionFamilies,
              ignoreCompanionVisibility: indexedSupport.requiredCompanionFamilies.size === 0,
              disableWalTail: true,
            }
          : undefined
      );
      const visibleSnapshotEndSeq = coverageState.canSearchWalTail
        ? snapshotEndSeq
        : (coverageState.visibleThroughSeq < snapshotEndSeq ? coverageState.visibleThroughSeq : snapshotEndSeq);
      const visibleSealedThrough = coverageState.canSearchWalTail
        ? srow.sealed_through
        : (coverageState.visibleThroughSeq < srow.sealed_through ? coverageState.visibleThroughSeq : srow.sealed_through);

      const collectSearchMatchResult = (
        offsetSeq: bigint,
        payload: Uint8Array
      ): Result<void, ReaderError> => collectSearchMatchIntoResult(hits, offsetSeq, payload);

      const collectSearchMatchIntoResult = (
        targetHits: SearchHitInternal[],
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
        const sortInternal = buildSearchSortInternalValues(request.sort, fieldsRes.value, evalRes.value, offsetSeq);
        if (request.searchAfter && compareSearchAfterValues(sortInternal, request.sort, request.searchAfter) <= 0) {
          return Result.ok(undefined);
        }
        targetHits.push({
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
        allowedDocIds: SearchDocIdCandidates | null,
        rangeStartSeq: bigint,
        rangeEndSeq: bigint
      ): Promise<Result<void, ReaderError>> => {
        if (markTimedOutIfNeeded()) return Result.ok(undefined);
        const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
        if (markTimedOutIfNeeded()) return Result.ok(undefined);
        let curOffset = seg.start_offset;
        for (const blockRes of iterateBlocksResult(segBytes)) {
          if (Result.isError(blockRes)) return Result.err({ kind: "internal", message: blockRes.error.message });
          for (const record of blockRes.value.decoded.records) {
            if (curOffset > rangeEndSeq) return Result.ok(undefined);
            if (curOffset < rangeStartSeq) {
              curOffset += 1n;
              continue;
            }
            const localDocId = Number(curOffset - seg.start_offset);
            if (searchDocIdsHas(allowedDocIds, localDocId)) {
              const matchRes = collectSearchMatchResult(curOffset, record.payload);
              if (Result.isError(matchRes)) return matchRes;
            }
            curOffset += 1n;
            if (markTimedOutIfNeeded()) return Result.ok(undefined);
          }
        }
        return Result.ok(undefined);
      };

      const scanSegmentWithFamiliesResult = async (
        seg: SegmentRow,
        rangeStartSeq: bigint,
        rangeEndSeq: bigint
      ): Promise<Result<void, ReaderError>> => {
        const segmentStartedAt = Date.now();
        if (markTimedOutIfNeeded()) return Result.ok(undefined);
        if (
          exactCandidateInfo.segments &&
          seg.segment_index < exactCandidateInfo.indexedThrough &&
          !exactCandidateInfo.segments.has(seg.segment_index)
        ) {
          return Result.ok(undefined);
        }
        if (cursorFieldBound) {
          const overlapsCursor = await this.segmentMayOverlapSearchCursor(stream, seg.segment_index, cursorFieldBound);
          if (!overlapsCursor) {
            indexFamiliesUsed.add("col");
            indexedSegments += 1;
            indexedSegmentTimeMs += Date.now() - segmentStartedAt;
            return Result.ok(undefined);
          }
        }
        if (markTimedOutIfNeeded()) return Result.ok(undefined);

        const familyCandidatesRes = await this.resolveSearchFamilyCandidatesResult(
          stream,
          seg.segment_index,
          columnClauses,
          ftsClauses,
          indexedSupport.indexedOnly ? indexedSupport.requiredCompanionFamilies : undefined,
          undefined,
          {
            addFtsSectionGetMs: (deltaMs) => {
              ftsSectionGetMs += deltaMs;
            },
            addFtsDecodeMs: (deltaMs) => {
              ftsDecodeMs += deltaMs;
            },
            addFtsClauseEstimateMs: (deltaMs) => {
              ftsClauseEstimateMs += deltaMs;
            },
          }
        );
        if (Result.isError(familyCandidatesRes)) return Result.err({ kind: "internal", message: familyCandidatesRes.error.message });
        if (markTimedOutIfNeeded()) return Result.ok(undefined);
        const familyCandidates = familyCandidatesRes.value;
        if (familyCandidates.docIds && familyCandidates.docIds.size === 0) {
          indexedSegments += familyCandidates.usedFamilies.size > 0 ? 1 : 0;
          for (const family of familyCandidates.usedFamilies) indexFamiliesUsed.add(family);
          if (familyCandidates.usedFamilies.size > 0) indexedSegmentTimeMs += Date.now() - segmentStartedAt;
          return Result.ok(undefined);
        }
        const usedIndexedFamilies = familyCandidates.usedFamilies.size > 0;
        if (familyCandidates.usedFamilies.size > 0) {
          indexedSegments += 1;
          for (const family of familyCandidates.usedFamilies) indexFamiliesUsed.add(family);
        } else {
          scannedSegments += 1;
        }

        const scanRes = await scanSegmentForSearchResult(seg, familyCandidates.docIds, rangeStartSeq, rangeEndSeq);
        if (Result.isError(scanRes)) return scanRes;
        if (usedIndexedFamilies) indexedSegmentTimeMs += Date.now() - segmentStartedAt;
        else scannedSegmentTimeMs += Date.now() - segmentStartedAt;
        return Result.ok(undefined);
      };

      const stopIfPageComplete = (): boolean => hits.length >= request.size;

      if (leadingSort?.kind === "offset") {
        const descending = leadingSort.direction === "desc";
        const rangeStartSeq = descending ? 0n : typeof offsetSearchAfter === "bigint" ? offsetSearchAfter + 1n : 0n;
        const requestedRangeEndSeq = descending ? (typeof offsetSearchAfter === "bigint" ? offsetSearchAfter - 1n : snapshotEndSeq) : snapshotEndSeq;
        const rangeEndSeq = requestedRangeEndSeq < visibleSnapshotEndSeq ? requestedRangeEndSeq : visibleSnapshotEndSeq;

        if (rangeStartSeq <= rangeEndSeq) {
          if (descending) {
            const tailStart = srow.sealed_through + 1n;
            if (coverageState.canSearchWalTail && tailStart <= rangeEndSeq) {
              const walStart = rangeStartSeq > tailStart ? rangeStartSeq : tailStart;
              const walEnd = rangeEndSeq;
              if (walStart <= walEnd) {
              const tailStartedAt = Date.now();
              for (const record of this.db.iterWalRangeDesc(stream, walStart, walEnd)) {
                scannedTailDocs += 1;
                const matchRes = collectSearchMatchResult(BigInt(record.offset), record.payload);
                if (Result.isError(matchRes)) return matchRes;
                if (markTimedOutIfNeeded()) break;
                if (stopIfPageComplete()) break;
              }
              scannedTailTimeMs += Date.now() - tailStartedAt;
            }
          }
            if (!timedOut && !stopIfPageComplete()) {
              const sealedEnd = rangeEndSeq < visibleSealedThrough ? rangeEndSeq : visibleSealedThrough;
              if (sealedEnd >= rangeStartSeq) {
                const plannedSealedSegments = exactTailCandidateInfo
                  ? null
                  : this.planSealedReadSegments(
                      stream,
                      rangeStartSeq,
                      sealedEnd,
                      exactCandidateInfo.segments,
                      exactCandidateInfo.indexedThrough,
                      "desc"
                    );
                if (plannedSealedSegments) {
                  for (const seg of plannedSealedSegments.segments) {
                    const scanRes = await this.scanSegmentReverseForSearchResult(
                      stream,
                      seg,
                      exactCandidateInfo,
                      exactTailCandidateInfo,
                      cursorFieldBound,
                      columnClauses,
                      ftsClauses,
                      rangeStartSeq,
                      sealedEnd,
                      {
                        indexFamiliesUsed,
                        strictFamilies: indexedSupport.indexedOnly ? indexedSupport.requiredCompanionFamilies : undefined,
                        collectSearchMatchResult,
                        deadline,
                        signal,
                        isTimedOut: () => timedOut,
                        setTimedOut: (next) => {
                          timedOut = next;
                        },
                        stopIfPageComplete,
                        addIndexedSegment: () => {
                          indexedSegments += 1;
                        },
                        addScannedSegment: () => {
                          scannedSegments += 1;
                        },
                        addIndexedSegmentTimeMs: (deltaMs) => {
                          indexedSegmentTimeMs += deltaMs;
                        },
                        addFtsSectionGetMs: (deltaMs) => {
                          ftsSectionGetMs += deltaMs;
                        },
                        addFtsDecodeMs: (deltaMs) => {
                          ftsDecodeMs += deltaMs;
                        },
                        addFtsClauseEstimateMs: (deltaMs) => {
                          ftsClauseEstimateMs += deltaMs;
                        },
                        addScannedSegmentTimeMs: (deltaMs) => {
                          scannedSegmentTimeMs += deltaMs;
                        },
                        remainingHits: () => Math.max(1, request.size - hits.length),
                      }
                    );
                    if (Result.isError(scanRes)) return scanRes;
                    if (timedOut || stopIfPageComplete()) break;
                  }
                } else {
                  const startSeg = this.db.findSegmentForOffset(stream, sealedEnd);
                  let segmentIndex = startSeg?.segment_index ?? this.db.countSegmentsForStream(stream) - 1;
                  if (exactTailCandidateInfo) {
                    segmentIndex = Math.min(segmentIndex, exactTailCandidateInfo.indexedThrough - 1);
                  }
                  while (segmentIndex >= 0) {
                    const seg = this.db.getSegmentByIndex(stream, segmentIndex);
                    if (!seg) {
                      segmentIndex -= 1;
                      continue;
                    }
                    if (seg.end_offset < rangeStartSeq) break;
                    if (seg.start_offset > sealedEnd) {
                      segmentIndex -= 1;
                      continue;
                    }
                    const scanRes = await this.scanSegmentReverseForSearchResult(
                      stream,
                      seg,
                      exactCandidateInfo,
                      exactTailCandidateInfo,
                      cursorFieldBound,
                      columnClauses,
                      ftsClauses,
                      rangeStartSeq,
                      sealedEnd,
                      {
                        indexFamiliesUsed,
                        strictFamilies: indexedSupport.indexedOnly ? indexedSupport.requiredCompanionFamilies : undefined,
                        collectSearchMatchResult,
                        deadline,
                        signal,
                        isTimedOut: () => timedOut,
                        setTimedOut: (next) => {
                          timedOut = next;
                        },
                        stopIfPageComplete,
                        addIndexedSegment: () => {
                          indexedSegments += 1;
                        },
                        addScannedSegment: () => {
                          scannedSegments += 1;
                        },
                        addIndexedSegmentTimeMs: (deltaMs) => {
                          indexedSegmentTimeMs += deltaMs;
                        },
                        addFtsSectionGetMs: (deltaMs) => {
                          ftsSectionGetMs += deltaMs;
                        },
                        addFtsDecodeMs: (deltaMs) => {
                          ftsDecodeMs += deltaMs;
                        },
                        addFtsClauseEstimateMs: (deltaMs) => {
                          ftsClauseEstimateMs += deltaMs;
                        },
                        addScannedSegmentTimeMs: (deltaMs) => {
                          scannedSegmentTimeMs += deltaMs;
                        },
                        remainingHits: () => request.size - hits.length,
                      }
                    );
                    if (Result.isError(scanRes)) return scanRes;
                    if (timedOut || stopIfPageComplete()) break;
                    segmentIndex -= 1;
                  }
                }
              }
            }
          } else {
            let seq = rangeStartSeq;
            const sealedEnd = rangeEndSeq < visibleSealedThrough ? rangeEndSeq : visibleSealedThrough;
            const plannedSealedSegments = this.planSealedReadSegments(
              stream,
              rangeStartSeq,
              sealedEnd,
              exactCandidateInfo.segments,
              exactCandidateInfo.indexedThrough,
              "asc"
            );
            if (plannedSealedSegments) {
              for (const seg of plannedSealedSegments.segments) {
                const scanRes = await scanSegmentWithFamiliesResult(seg, rangeStartSeq, rangeEndSeq);
                if (Result.isError(scanRes)) return scanRes;
                seq = seg.end_offset + 1n;
                if (timedOut || stopIfPageComplete()) break;
              }
              if (seq <= plannedSealedSegments.sealedEndSeq) seq = plannedSealedSegments.sealedEndSeq + 1n;
            } else {
              while (seq <= rangeEndSeq && seq <= visibleSealedThrough) {
                const seg = this.db.findSegmentForOffset(stream, seq);
                if (!seg) break;
                const scanRes = await scanSegmentWithFamiliesResult(seg, rangeStartSeq, rangeEndSeq);
                if (Result.isError(scanRes)) return scanRes;
                seq = seg.end_offset + 1n;
                if (timedOut || stopIfPageComplete()) break;
              }
            }
            if (!timedOut && !stopIfPageComplete() && coverageState.canSearchWalTail && seq <= rangeEndSeq) {
              const tailStartedAt = Date.now();
              for (const record of this.db.iterWalRange(stream, seq, rangeEndSeq)) {
                scannedTailDocs += 1;
                const matchRes = collectSearchMatchResult(BigInt(record.offset), record.payload);
                if (Result.isError(matchRes)) return matchRes;
                if (markTimedOutIfNeeded()) break;
                if (stopIfPageComplete()) break;
              }
              scannedTailTimeMs += Date.now() - tailStartedAt;
            }
          }
        }

        const pageHits = hits.slice(0, request.size);
        const nextSearchAfter = pageHits.length === request.size ? pageHits[pageHits.length - 1].sortResponse : null;
        const exactTotalKnown = !timedOut && coverageState.complete && nextSearchAfter == null;
        return Result.ok({
          stream,
          snapshotEndOffset,
          tookMs: Date.now() - startedAt,
          timedOut,
          timeoutMs: request.timeoutMs,
          coverage: {
            mode: coverageState.mode,
            complete: coverageState.complete && !timedOut,
            streamHeadOffset: coverageState.streamHeadOffset,
            visibleThroughOffset: coverageState.visibleThroughOffset,
            visibleThroughPrimaryTimestampMax: coverageState.visibleThroughPrimaryTimestampMax,
            oldestOmittedAppendAt: coverageState.oldestOmittedAppendAt,
            possibleMissingEventsUpperBound: coverageState.possibleMissingEventsUpperBound,
            possibleMissingUploadedSegments: coverageState.possibleMissingUploadedSegments,
            possibleMissingSealedRows: coverageState.possibleMissingSealedRows,
            possibleMissingWalRows: coverageState.possibleMissingWalRows,
            indexedSegments,
            indexedSegmentTimeMs,
            ftsSectionGetMs,
            ftsDecodeMs,
            ftsClauseEstimateMs,
            scannedSegments,
            scannedSegmentTimeMs,
            scannedTailDocs,
            scannedTailTimeMs,
            exactCandidateTimeMs,
            indexFamiliesUsed: Array.from(indexFamiliesUsed).sort(),
          },
          total: {
            value: pageHits.length,
            relation: exactTotalKnown ? "eq" : "gte",
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
      }

      let seq = 0n;
      const sealedEnd = visibleSnapshotEndSeq < visibleSealedThrough ? visibleSnapshotEndSeq : visibleSealedThrough;
      const plannedSealedSegments = this.planSealedReadSegments(
        stream,
        0n,
        sealedEnd,
        exactCandidateInfo.segments,
        exactCandidateInfo.indexedThrough,
        "asc"
      );
      if (plannedSealedSegments) {
        for (const seg of plannedSealedSegments.segments) {
          const scanRes = await scanSegmentWithFamiliesResult(seg, 0n, snapshotEndSeq);
          if (Result.isError(scanRes)) return scanRes;
          seq = seg.end_offset + 1n;
          if (timedOut) break;
        }
        if (seq <= plannedSealedSegments.sealedEndSeq) seq = plannedSealedSegments.sealedEndSeq + 1n;
      } else {
        while (seq <= visibleSnapshotEndSeq && seq <= visibleSealedThrough) {
          const seg = this.db.findSegmentForOffset(stream, seq);
          if (!seg) break;
          const scanRes = await scanSegmentWithFamiliesResult(seg, 0n, snapshotEndSeq);
          if (Result.isError(scanRes)) return scanRes;
          seq = seg.end_offset + 1n;
          if (timedOut) break;
        }
      }

      if (!timedOut && coverageState.canSearchWalTail && seq <= snapshotEndSeq) {
        const tailStartedAt = Date.now();
        for (const record of this.db.iterWalRange(stream, seq, snapshotEndSeq)) {
          scannedTailDocs += 1;
          const matchRes = collectSearchMatchResult(BigInt(record.offset), record.payload);
          if (Result.isError(matchRes)) return matchRes;
          if (markTimedOutIfNeeded()) break;
        }
        scannedTailTimeMs += Date.now() - tailStartedAt;
      }

      hits.sort((left, right) => compareSearchHits(left, right, request.sort));
      const pageHits = hits.slice(0, request.size);
      const nextSearchAfter = pageHits.length === request.size ? pageHits[pageHits.length - 1].sortResponse : null;
      const exactTotalKnown = !timedOut && coverageState.complete && nextSearchAfter == null;

      return Result.ok({
        stream,
        snapshotEndOffset,
        tookMs: Date.now() - startedAt,
        timedOut,
        timeoutMs: request.timeoutMs,
        coverage: {
          mode: coverageState.mode,
          complete: coverageState.complete && !timedOut,
          streamHeadOffset: coverageState.streamHeadOffset,
          visibleThroughOffset: coverageState.visibleThroughOffset,
          visibleThroughPrimaryTimestampMax: coverageState.visibleThroughPrimaryTimestampMax,
          oldestOmittedAppendAt: coverageState.oldestOmittedAppendAt,
          possibleMissingEventsUpperBound: coverageState.possibleMissingEventsUpperBound,
          possibleMissingUploadedSegments: coverageState.possibleMissingUploadedSegments,
          possibleMissingSealedRows: coverageState.possibleMissingSealedRows,
          possibleMissingWalRows: coverageState.possibleMissingWalRows,
          indexedSegments,
          indexedSegmentTimeMs,
          ftsSectionGetMs,
          ftsDecodeMs,
          ftsClauseEstimateMs,
          scannedSegments,
          scannedSegmentTimeMs,
          scannedTailDocs,
          scannedTailTimeMs,
          exactCandidateTimeMs,
          indexFamiliesUsed: Array.from(indexFamiliesUsed).sort(),
        },
        total: {
          value: pageHits.length,
          relation: exactTotalKnown ? "eq" : "gte",
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
    } finally {
      leaveSearchPhase?.();
    }
  }

  async search(args: { stream: string; request: SearchRequest }): Promise<SearchResultBatch> {
    const res = await this.searchResult(args);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  async aggregateResult(args: { stream: string; request: AggregateRequest }): Promise<Result<AggregateResultBatch, ReaderError>> {
    const { stream, request } = args;
    const leaveAggregatePhase = this.memorySampler?.enter("aggregate", {
      stream,
      rollup: request.rollup,
      over_limit: this.memory?.isOverLimit() === true,
    });
    const srow = this.db.getStream(stream);
    try {
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

      const coverageState = this.computePublishedCoverageState(stream, srow, registry);
      const intervalMs = request.intervalMs;
      const intervalBig = BigInt(intervalMs);
      const fromMs = Number(request.fromMs);
      const toMs = Number(request.toMs);
      const fullStartMs = Number(((request.fromMs + intervalBig - 1n) / intervalBig) * intervalBig);
      const fullEndMs = Number((request.toMs / intervalBig) * intervalBig);
      const hasFullWindows = fullEndMs > fullStartMs;
      const dimensions = new Set(rollup.dimensions ?? []);
      const eligibility = extractRollupEligibility(request.q, dimensions);
      const selectedMeasures = new Set(request.measures ?? Object.keys(rollup.measures));
      const timestampField = rollup.timestampField ?? registry.search.primaryTimestampField;
      const primaryTimestampField = registry.search.primaryTimestampField;
      const usesPrimaryTimestampBounds = timestampField === primaryTimestampField;

      const buckets = new Map<number, Map<string, AggregateGroupInternal>>();
      const indexedSegmentSet = new Set<number>();
      const scannedSegmentSet = new Set<number>();
      let scannedTailDocs = 0;
      const indexFamiliesUsed = new Set<string>();
      const metricsProfile = registry.search.profile === "metrics";
      let usedRollups = false;

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
            group.measures[measureName] = cloneAggMeasureState(state);
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

      const partialRanges: Array<{ startMs: number; endMs: number }> = [];
      if (!eligibility.eligible || !hasFullWindows) {
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
        scannedSegmentSet.add(seg.segment_index);
        return Result.ok(undefined);
      };

      const segmentMayOverlapAggregateRange = async (
        seg: SegmentRow,
        startMs: number,
        endMs: number
      ): Promise<boolean> => {
        if (usesPrimaryTimestampBounds) {
          const companionRow = this.db.getSearchSegmentCompanion(stream, seg.segment_index);
          if (companionRow?.primary_timestamp_min_ms != null && companionRow.primary_timestamp_max_ms != null) {
            return companionRow.primary_timestamp_max_ms >= BigInt(startMs) && companionRow.primary_timestamp_min_ms < BigInt(endMs);
          }
        }
        return this.segmentMayOverlapTimeRange(stream, seg.segment_index, startMs, endMs, timestampField);
      };

      const scanMetricsBlockForAggregateResult = async (
        seg: SegmentRow,
        companion: MetricsBlockSectionView,
        scanRanges: Array<{ startMs: number; endMs: number }>
      ): Promise<Result<void, ReaderError>> => {
        for (const record of companion.records()) {
          const offsetSeq = seg.start_offset + BigInt(record.doc_id);
          const timestampMs = record.windowStartMs;
          const inRange = scanRanges.some((range) => timestampMs >= range.startMs && timestampMs < range.endMs);
          if (!inRange) continue;
          const materialized = materializeMetricsBlockRecord(record);
          if (request.q) {
            const evalRes = evaluateSearchQueryResult(registry, offsetSeq, request.q, materialized);
            if (Result.isError(evalRes)) return Result.err({ kind: "internal", message: evalRes.error.message });
            if (!evalRes.value.matched) continue;
          }
          const contributionRes = extractRollupContributionResult(registry, rollup, offsetSeq, materialized);
          if (Result.isError(contributionRes)) return Result.err({ kind: "internal", message: contributionRes.error.message });
          const contribution = contributionRes.value;
          if (!contribution) continue;
          const bucketStartMs = Math.floor(contribution.timestampMs / intervalMs) * intervalMs;
          mergeBucketMeasures(bucketStartMs, contribution.dimensions, contribution.measures);
        }
        indexedSegmentSet.add(seg.segment_index);
        indexFamiliesUsed.add("mblk");
        return Result.ok(undefined);
      };

      for (const seg of this.db.listSegmentsForStream(stream)) {
        if (seg.segment_index >= coverageState.visiblePublishedSegmentCount) break;
        let coveredAlignedWindows = false;
        if (eligibility.eligible && this.index && hasFullWindows) {
          const overlapsAlignedWindow = await segmentMayOverlapAggregateRange(seg, fullStartMs, fullEndMs);
          if (overlapsAlignedWindow) {
            const companion = await this.index.getAggSegmentCompanion(stream, seg.segment_index);
            const intervalCompanion = companion?.getInterval(request.rollup, intervalMs);
            if (intervalCompanion) {
              coveredAlignedWindows = true;
              indexedSegmentSet.add(seg.segment_index);
              indexFamiliesUsed.add("agg");
              usedRollups = true;
              intervalCompanion.forEachGroupInRange(fullStartMs, fullEndMs, (windowStartMs, group) => {
                if (!matchesExactFilters(group.dimensions)) return;
                mergeBucketMeasures(windowStartMs, group.dimensions, group.measures);
              });
            }
          }
        }

        const scanRanges =
          !eligibility.eligible || !hasFullWindows
            ? [{ startMs: fromMs, endMs: toMs }]
            : coveredAlignedWindows
              ? partialRanges
              : [{ startMs: fromMs, endMs: toMs }];
        if (scanRanges.length === 0) continue;
        let overlaps = false;
        for (const range of scanRanges) {
          if (await segmentMayOverlapAggregateRange(seg, range.startMs, range.endMs)) {
            overlaps = true;
            break;
          }
        }
        if (!overlaps) continue;
        let scanRes: Result<void, ReaderError>;
        if (metricsProfile && this.index) {
          const companion = await this.index.getMetricsBlockSegmentCompanion(stream, seg.segment_index);
          if (companion) {
            scanRes = await scanMetricsBlockForAggregateResult(seg, companion, scanRanges);
          } else {
            scanRes = await scanSegmentForAggregateResult(seg, scanRanges);
          }
        } else {
          scanRes = await scanSegmentForAggregateResult(seg, scanRanges);
        }
        if (Result.isError(scanRes)) return scanRes;
      }

      const tailStart = srow.sealed_through + 1n;
      const tailEnd = srow.next_offset - 1n;
      if (coverageState.canSearchWalTail && tailStart <= tailEnd) {
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
          mode: coverageState.mode,
          complete: coverageState.complete,
          streamHeadOffset: coverageState.streamHeadOffset,
          visibleThroughOffset: coverageState.visibleThroughOffset,
          visibleThroughPrimaryTimestampMax: coverageState.visibleThroughPrimaryTimestampMax,
          oldestOmittedAppendAt: coverageState.oldestOmittedAppendAt,
          possibleMissingEventsUpperBound: coverageState.possibleMissingEventsUpperBound,
          possibleMissingUploadedSegments: coverageState.possibleMissingUploadedSegments,
          possibleMissingSealedRows: coverageState.possibleMissingSealedRows,
          possibleMissingWalRows: coverageState.possibleMissingWalRows,
          usedRollups,
          indexedSegments: indexedSegmentSet.size,
          scannedSegments: scannedSegmentSet.size,
          scannedTailDocs,
          indexFamiliesUsed: Array.from(indexFamiliesUsed).sort(),
        },
        buckets: bucketList,
      });
    } catch (e: unknown) {
      return Result.err({ kind: "internal", message: errorMessage(e) });
    } finally {
      leaveAggregatePhase?.();
    }
  }

  async aggregate(args: { stream: string; request: AggregateRequest }): Promise<AggregateResultBatch> {
    const res = await this.aggregateResult(args);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }

  private async scanSegmentReverseForSearchResult(
    stream: string,
    seg: SegmentRow,
    exactCandidateInfo: SegmentCandidateInfo,
    exactTailCandidateInfo: SearchExactTailCandidateInfo,
    cursorFieldBound: SearchCursorFieldBound | null,
    columnClauses: SearchColumnClause[],
    ftsClauses: SearchFtsClause[],
    rangeStartSeq: bigint,
    rangeEndSeq: bigint,
    state: {
      indexFamiliesUsed: Set<string>;
      strictFamilies?: ReadonlySet<"col" | "fts">;
      collectSearchMatchResult: (offsetSeq: bigint, payload: Uint8Array) => Result<void, ReaderError>;
      deadline: number | null;
      signal?: AbortSignal | null;
      isTimedOut: () => boolean;
      setTimedOut: (next: boolean) => void;
      stopIfPageComplete: () => boolean;
      addIndexedSegment: () => void;
      addScannedSegment: () => void;
      addIndexedSegmentTimeMs: (deltaMs: number) => void;
      addFtsSectionGetMs: (deltaMs: number) => void;
      addFtsDecodeMs: (deltaMs: number) => void;
      addFtsClauseEstimateMs: (deltaMs: number) => void;
      addScannedSegmentTimeMs: (deltaMs: number) => void;
      remainingHits: () => number;
    }
  ): Promise<Result<void, ReaderError>> {
    const segmentStartedAt = Date.now();
    const markTimedOutIfNeeded = (): boolean => {
      if (state.signal?.aborted) {
        state.setTimedOut(true);
        return true;
      }
      if (state.deadline == null || Date.now() < state.deadline) return false;
      state.setTimedOut(true);
      return true;
    };
    if (markTimedOutIfNeeded()) return Result.ok(undefined);
    if (exactTailCandidateInfo) {
      if (seg.segment_index >= exactTailCandidateInfo.indexedThrough) return Result.ok(undefined);
      for (const matcher of exactTailCandidateInfo.matchers) {
        const matched = await matcher.matchesSegment(seg.segment_index);
        if (markTimedOutIfNeeded()) return Result.ok(undefined);
        if (!matched) return Result.ok(undefined);
      }
    }
    if (
      exactCandidateInfo.segments &&
      seg.segment_index < exactCandidateInfo.indexedThrough &&
      !exactCandidateInfo.segments.has(seg.segment_index)
    ) {
      return Result.ok(undefined);
    }
    if (cursorFieldBound) {
      const overlapsCursor = await this.segmentMayOverlapSearchCursor(stream, seg.segment_index, cursorFieldBound);
      if (!overlapsCursor) {
        state.indexFamiliesUsed.add("col");
        state.addIndexedSegment();
        state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
        return Result.ok(undefined);
      }
    }
    if (markTimedOutIfNeeded()) return Result.ok(undefined);

    const familyCandidatesRes = await this.resolveSearchFamilyCandidatesResult(
      stream,
      seg.segment_index,
      columnClauses,
      ftsClauses,
      state.strictFamilies,
      state.remainingHits(),
      {
        addFtsSectionGetMs: state.addFtsSectionGetMs,
        addFtsDecodeMs: state.addFtsDecodeMs,
        addFtsClauseEstimateMs: state.addFtsClauseEstimateMs,
      }
    );
    if (Result.isError(familyCandidatesRes)) return Result.err({ kind: "internal", message: familyCandidatesRes.error.message });
    if (markTimedOutIfNeeded()) return Result.ok(undefined);
    const familyCandidates = familyCandidatesRes.value;
    if (familyCandidates.docIds && familyCandidates.docIds.size === 0) {
      if (familyCandidates.usedFamilies.size > 0) state.addIndexedSegment();
      for (const family of familyCandidates.usedFamilies) state.indexFamiliesUsed.add(family);
      if (familyCandidates.usedFamilies.size > 0) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
      return Result.ok(undefined);
    }
    const usedIndexedFamilies = familyCandidates.usedFamilies.size > 0;
    if (familyCandidates.usedFamilies.size > 0) {
      state.addIndexedSegment();
      for (const family of familyCandidates.usedFamilies) state.indexFamiliesUsed.add(family);
    } else {
      state.addScannedSegment();
    }

    if (markTimedOutIfNeeded()) return Result.ok(undefined);
    const source = await loadSegmentSource(this.os, seg, this.diskCache, this.retryOpts());
    if (markTimedOutIfNeeded()) return Result.ok(undefined);
    const blockEntriesRes = loadSegmentBlockEntriesFromSourceResult(seg, source);
    if (Result.isError(blockEntriesRes)) return Result.err({ kind: "internal", message: blockEntriesRes.error.message });

    for (let blockIndex = blockEntriesRes.value.length - 1; blockIndex >= 0; blockIndex--) {
      const block = blockEntriesRes.value[blockIndex]!;
      const blockStartOffset = block.firstOffset;
      const blockEndOffset = blockStartOffset + BigInt(block.recordCount) - 1n;
      if (blockStartOffset > rangeEndSeq) continue;
      if (blockEndOffset < rangeStartSeq) {
        if (usedIndexedFamilies) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
        else state.addScannedSegmentTimeMs(Date.now() - segmentStartedAt);
        return Result.ok(undefined);
      }
      const localDocIdStart = Number(blockStartOffset - seg.start_offset);
      const localDocIdEnd = localDocIdStart + block.recordCount - 1;
      if (!searchDocIdsIntersectRange(familyCandidates.docIds, localDocIdStart, localDocIdEnd)) {
        if (markTimedOutIfNeeded()) {
          if (usedIndexedFamilies) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
          else state.addScannedSegmentTimeMs(Date.now() - segmentStartedAt);
          return Result.ok(undefined);
        }
        continue;
      }
      const totalLen = DSB3_HEADER_BYTES + block.compressedLen;
      const blockBytes = readRangeFromSource(source, block.blockOffset, block.blockOffset + totalLen - 1);
      const decodedRes = decodeBlockResult(blockBytes);
      if (Result.isError(decodedRes)) return Result.err({ kind: "internal", message: decodedRes.error.message });
      const decoded = decodedRes.value;
      for (let recordIndex = decoded.records.length - 1; recordIndex >= 0; recordIndex--) {
        const offsetSeq = blockStartOffset + BigInt(recordIndex);
        if (offsetSeq > rangeEndSeq) continue;
        if (offsetSeq < rangeStartSeq) {
          if (usedIndexedFamilies) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
          else state.addScannedSegmentTimeMs(Date.now() - segmentStartedAt);
          return Result.ok(undefined);
        }
        const localDocId = Number(offsetSeq - seg.start_offset);
        if (searchDocIdsHas(familyCandidates.docIds, localDocId)) {
          const matchRes = state.collectSearchMatchResult(offsetSeq, decoded.records[recordIndex]!.payload);
          if (Result.isError(matchRes)) return matchRes;
        }
        if (markTimedOutIfNeeded()) {
          if (usedIndexedFamilies) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
          else state.addScannedSegmentTimeMs(Date.now() - segmentStartedAt);
          return Result.ok(undefined);
        }
        if (state.stopIfPageComplete()) {
          if (usedIndexedFamilies) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
          else state.addScannedSegmentTimeMs(Date.now() - segmentStartedAt);
          return Result.ok(undefined);
        }
      }
    }

    if (usedIndexedFamilies) state.addIndexedSegmentTimeMs(Date.now() - segmentStartedAt);
    else state.addScannedSegmentTimeMs(Date.now() - segmentStartedAt);
    return Result.ok(undefined);
  }

  private async segmentMayOverlapSearchCursor(
    stream: string,
    segmentIndex: number,
    bound: SearchCursorFieldBound
  ): Promise<boolean> {
    if (!this.index || bound.encoded == null) return true;
    const companion = await this.index.getColSegmentCompanion(stream, segmentIndex);
    if (!companion) return true;

    if (companion.primaryTimestampField === bound.sort.field && companion.minTimestampMs() != null && companion.maxTimestampMs() != null) {
      const target = bound.after;
      if (typeof target !== "bigint") return true;
      const minMs = companion.minTimestampMs()!;
      const maxMs = companion.maxTimestampMs()!;
      return bound.sort.direction === "desc" ? minMs <= target : maxMs >= target;
    }

    const field = companion.getField(bound.sort.field);
    if (!field) return true;
    const minValue = field.minValue();
    const maxValue = field.maxValue();
    if (minValue == null || maxValue == null) return true;
    const boundValue = bound.after;
    const cmpMin = compareComparableValues(minValue, boundValue);
    const cmpMax = compareComparableValues(maxValue, boundValue);
    return bound.sort.direction === "desc" ? cmpMin <= 0 : cmpMax >= 0;
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
    if (companion && companion.primaryTimestampField === timestampField) {
      const minMs = companion.minTimestampMs() == null ? null : Number(companion.minTimestampMs());
      const maxMs = companion.maxTimestampMs() == null ? null : Number(companion.maxTimestampMs());
      if (Number.isFinite(minMs) && Number.isFinite(maxMs)) {
        return (maxMs as number) >= startMs && (minMs as number) < endMs;
      }
    }
    const metricsBlock = await this.index.getMetricsBlockSegmentCompanion(stream, segmentIndex);
    if (!metricsBlock) return true;
    const minMs = metricsBlock.minWindowStartMs;
    const maxMs = metricsBlock.maxWindowEndMs;
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

  private async resolveSearchExactCandidateSegments(stream: string, clauses: SearchExactClause[]): Promise<SegmentCandidateInfo> {
    if (!this.index) return { segments: null, indexedThrough: 0 };
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

  private canUseSearchExactTailMatchers(
    request: SearchRequest,
    cursorFieldBound: SearchCursorFieldBound | null,
    ftsClauses: readonly SearchFtsClause[]
  ): boolean {
    if (!this.index?.createSecondaryIndexTailMatcher) return false;
    if (cursorFieldBound != null) return false;
    if (collectPositiveSearchColumnClauses(request.q).length > 0) return false;
    if (hasNonKeywordSearchFtsClauses(ftsClauses)) return false;
    const leadingSort = request.sort[0] ?? null;
    return leadingSort?.kind === "offset" && leadingSort.direction === "desc";
  }

  private async resolveSearchExactTailCandidateInfo(
    stream: string,
    clauses: SearchExactClause[]
  ): Promise<SearchExactTailCandidateInfo> {
    if (!this.index?.createSecondaryIndexTailMatcher) return null;
    if (clauses.length === 0) return null;

    const matchers: IndexTailMatcher[] = [];
    for (const clause of clauses) {
      const matcher = await this.index.createSecondaryIndexTailMatcher(stream, clause.field, utf8Bytes(clause.canonicalValue));
      if (matcher) matchers.push(matcher);
    }
    if (matchers.length === 0) return null;

    const indexedThrough = matchers.reduce((min, matcher) => Math.min(min, matcher.indexedThrough), Number.MAX_SAFE_INTEGER);
    if (!Number.isFinite(indexedThrough) || indexedThrough <= 0) return null;
    return { matchers, indexedThrough };
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
      intersection = intersectDocIdSets(intersection, clauseRes.value);
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
      intersection = intersectDocIdSets(intersection, clauseRes.value);
      if (intersection.size === 0) break;
    }
    return Result.ok(intersection ?? new Set<number>());
  }

  private async resolveSearchFtsCandidateDocIdsResult(
    stream: string,
    segmentIndex: number,
    clauses: SearchFtsClause[],
    candidateDocIds?: Set<number> | null,
    maxNewestDocIds?: number,
    stats?: {
      addFtsSectionGetMs?: (deltaMs: number) => void;
      addFtsDecodeMs?: (deltaMs: number) => void;
      addFtsClauseEstimateMs?: (deltaMs: number) => void;
    }
  ): Promise<Result<{ docIds: Set<number> | null; usedFamily: boolean } | null, { message: string }>> {
    if (!this.index || clauses.length === 0) return Result.ok(null);
    const companionRes = this.index.getFtsSegmentCompanionWithStats
      ? await this.index.getFtsSegmentCompanionWithStats(stream, segmentIndex)
      : { companion: await this.index.getFtsSegmentCompanion(stream, segmentIndex), stats: { sectionGetMs: 0, decodeMs: 0 } };
    stats?.addFtsSectionGetMs?.(companionRes.stats.sectionGetMs);
    stats?.addFtsDecodeMs?.(companionRes.stats.decodeMs);
    const companion = companionRes.companion;
    if (!companion) return Result.ok(null);
    if (maxNewestDocIds != null) {
      const newestPlanRes = planNewestFirstFtsClausesResult({
        companion,
        clauses,
        candidateDocIds,
        maxNewestDocIds,
      });
      if (Result.isError(newestPlanRes)) return newestPlanRes;
      if (newestPlanRes.value?.mode === "segment_only") {
        return Result.ok({ docIds: candidateDocIds ?? null, usedFamily: true });
      }
      if (newestPlanRes.value?.mode === "doc_ids") {
        return Result.ok({ docIds: newestPlanRes.value.docIds, usedFamily: true });
      }
    }
    const clausesRes = filterDocIdsByFtsClausesResult({
      companion,
      clauses,
      candidateDocIds,
      maxNewestDocIds,
      onEstimateMs: (deltaMs) => {
        stats?.addFtsClauseEstimateMs?.(deltaMs);
      },
    });
    if (Result.isError(clausesRes)) return clausesRes;
    return Result.ok({ docIds: clausesRes.value, usedFamily: true });
  }

  private async resolveSearchFamilyCandidatesResult(
    stream: string,
    segmentIndex: number,
    columnClauses: SearchColumnClause[],
    ftsClauses: SearchFtsClause[],
    strictFamilies?: ReadonlySet<"col" | "fts">,
    newestFtsDocLimit?: number,
    stats?: {
      addFtsSectionGetMs?: (deltaMs: number) => void;
      addFtsDecodeMs?: (deltaMs: number) => void;
      addFtsClauseEstimateMs?: (deltaMs: number) => void;
    }
  ): Promise<Result<SearchFamilyCandidateInfo, { message: string }>> {
    let intersection: Set<number> | null = null;
    const usedFamilies = new Set<string>();

    if (columnClauses.length > 0) {
      const columnRes = await this.resolveSearchColumnCandidateDocIdsResult(stream, segmentIndex, columnClauses);
      if (Result.isError(columnRes)) return columnRes;
      if (columnRes.value) {
        intersection = columnRes.value;
        usedFamilies.add("col");
      } else if (strictFamilies?.has("col")) {
        intersection = new Set<number>();
        usedFamilies.add("col");
      }
    }

    if (ftsClauses.length > 0) {
      const ftsRes = await this.resolveSearchFtsCandidateDocIdsResult(
        stream,
        segmentIndex,
        ftsClauses,
        intersection,
        newestFtsDocLimit,
        stats
      );
      if (Result.isError(ftsRes)) return ftsRes;
      if (ftsRes.value?.usedFamily) {
        if (ftsRes.value.docIds) intersection = intersectDocIdSets(intersection, ftsRes.value.docIds);
        usedFamilies.add("fts");
      } else if (strictFamilies?.has("fts")) {
        if (intersection == null) intersection = new Set<number>();
        else intersection.clear();
        usedFamilies.add("fts");
      }
    }

    return Result.ok({ docIds: intersection ? buildSearchDocIdCandidates(intersection) : null, usedFamilies });
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

function compareSearchAfterValues(
  sortInternal: Array<bigint | number | string | boolean | null>,
  sorts: SearchSortSpec[],
  searchAfter: unknown[]
): number {
  for (let i = 0; i < sorts.length; i++) {
    const after = normalizeSearchAfterValue(sorts[i], searchAfter[i]);
    const cmp = compareComparableValues(sortInternal[i] ?? null, after);
    if (cmp === 0) continue;
    return sorts[i].direction === "asc" ? cmp : -cmp;
  }
  return 0;
}

function compareEncodedValues(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.byteLength, right.byteLength);
  for (let i = 0; i < length; i++) {
    if (left[i] === right[i]) continue;
    return left[i]! < right[i]! ? -1 : 1;
  }
  if (left.byteLength === right.byteLength) return 0;
  return left.byteLength < right.byteLength ? -1 : 1;
}

function encodeSearchCursorValue(sort: Extract<SearchSortSpec, { kind: "field" }>, value: bigint | number | string | boolean | null): Uint8Array | null {
  if (value == null) return null;
  if (sort.config.kind === "integer" || sort.config.kind === "date") {
    return typeof value === "bigint" ? encodeSortableInt64(value) : null;
  }
  if (sort.config.kind === "float") {
    return typeof value === "number" ? encodeSortableFloat64(value) : null;
  }
  if (sort.config.kind === "bool") {
    return typeof value === "boolean" ? encodeSortableBool(value) : null;
  }
  return null;
}

function resolveSearchCursorFieldBound(request: SearchRequest): SearchCursorFieldBound | null {
  if (!request.searchAfter || request.searchAfter.length === 0) return null;
  const leadingSort = request.sort[0];
  if (!leadingSort || leadingSort.kind !== "field") return null;
  if (
    leadingSort.config.kind !== "integer" &&
    leadingSort.config.kind !== "float" &&
    leadingSort.config.kind !== "date" &&
    leadingSort.config.kind !== "bool"
  ) {
    return null;
  }
  const after = normalizeSearchAfterValue(leadingSort, request.searchAfter[0]);
  return {
    kind: "field",
    sort: leadingSort,
    after,
    encoded: encodeSearchCursorValue(leadingSort, after),
  };
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
  return compareSearchAfterValues(hit.sortInternal, sorts, searchAfter);
}
