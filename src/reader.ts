import { existsSync, openSync, readSync, closeSync } from "node:fs";
import type { Config } from "./config";
import type { SqliteDurableStore, SegmentRow } from "./db/db";
import type { ObjectStore } from "./objectstore/interface";
import { parseOffsetResult, offsetToSeqOrNeg1, encodeOffset } from "./offset";
import { decodeBlockResult, iterateBlocksResult, parseBlockHeaderResult, parseFooterBytes, DSB3_HEADER_BYTES, type SegmentFooter } from "./segment/format";
import { SegmentDiskCache, type SegmentCacheStats } from "./segment/cache";
import { Bloom256 } from "./util/bloom256";
import { segmentObjectKey, streamHash16Hex } from "./util/stream_paths";
import { readU32BE } from "./util/endian";
import { retry, type RetryOptions } from "./util/retry";
import { LruCache } from "./util/lru";
import type { IndexManager } from "./index/indexer";
import { dsError } from "./util/ds_error.ts";
import { Result } from "better-result";

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
};

export type ReaderError =
  | { kind: "not_found"; message: string }
  | { kind: "gone"; message: string }
  | { kind: "invalid_offset"; message: string }
  | { kind: "internal"; message: string };

type FooterCacheEntry = { footer: SegmentFooter | null; footerStart: number };

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
  private readonly diskCache?: SegmentDiskCache;
  private readonly footerCache?: LruCache<string, FooterCacheEntry>;
  private readonly index?: IndexManager;

  constructor(config: Config, db: SqliteDurableStore, os: ObjectStore, diskCache?: SegmentDiskCache, index?: IndexManager) {
    this.config = config;
    this.db = db;
    this.os = os;
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

  async readResult(args: { stream: string; offset: string; key: string | null; format: ReadFormat }): Promise<Result<ReadBatch, ReaderError>> {
    const { stream, offset, key, format } = args;
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
        });
      }

      let seq = desiredOffset;
      const keyBytes = key ? utf8Bytes(key) : null;
      const indexInfo = keyBytes && this.index ? await this.index.candidateSegments(stream, keyBytes) : null;
      const candidateSegments = indexInfo?.segments ?? null;
      const indexedThrough = indexInfo?.indexedThrough ?? 0;

      const scanSegmentBytes = async (segBytes: Uint8Array, seg: SegmentRow): Promise<Result<void, ReaderError>> => {
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
            if (keyBytes && !bytesEqual(r.routingKey, keyBytes)) {
              curOffset += 1n;
              continue;
            }
            results.push({ offset: curOffset, payload: r.payload });
            bytesOut += r.payload.byteLength;
            curOffset += 1n;
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
        if (keyBytes && candidateSegments && seg.segment_index < indexedThrough && !candidateSegments.has(seg.segment_index)) {
          seq = seg.end_offset + 1n;
          continue;
        }
        const preferFull = !keyBytes && this.config.readMaxBytes >= seg.size_bytes;
        if (preferFull) {
          const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
          const scanRes = await scanSegmentBytes(segBytes, seg);
          if (Result.isError(scanRes)) return scanRes;
          if (results.length >= this.config.readMaxRecords || bytesOut >= this.config.readMaxBytes) return Result.ok(finalize());
        } else {
          const footerInfo = await loadSegmentFooter(this.os, seg, this.diskCache, this.retryOpts(), this.footerCache);
          if (!footerInfo || !footerInfo.footer) {
            const segBytes = await loadSegmentBytes(this.os, seg, this.diskCache, this.retryOpts());
            const scanRes = await scanSegmentBytes(segBytes, seg);
            if (Result.isError(scanRes)) return scanRes;
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
                if (keyBytes && !bytesEqual(r.routingKey, keyBytes)) {
                  curOffset += 1n;
                  continue;
                }
                results.push({ offset: curOffset, payload: r.payload });
                bytesOut += r.payload.byteLength;
                curOffset += 1n;
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
          results.push({ offset: s, payload });
          bytesOut += payload.byteLength;
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
        };
      }
    } catch (e: unknown) {
      return Result.err({ kind: "internal", message: errorMessage(e) });
    }
  }

  async read(args: { stream: string; offset: string; key: string | null; format: ReadFormat }): Promise<ReadBatch> {
    const res = await this.readResult(args);
    if (Result.isError(res)) throw dsError(res.error.message);
    return res.value;
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
  return true;
}
