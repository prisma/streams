import { mkdirSync, openSync, closeSync, writeSync, fsyncSync, renameSync, existsSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import type { Config } from "../config";
import type { SqliteDurableStore } from "../db/db";
import { encodeBlock, encodeFooter, type BlockIndexEntry, type SegmentRecord } from "./format";
import { readU32BE } from "../util/endian";
import { localSegmentPath, streamHash16Hex } from "../util/stream_paths";
import { LruCache } from "../util/lru";
import { yieldToEventLoop } from "../util/yield";

export type SegmenterOptions = {
  minCandidateBytes?: number; // default: segmentMaxBytes
  minCandidateRows?: number; // default: segmentTargetRows
  maxIntervalMs?: number; // default: segmentMaxIntervalMs
  candidatesPerTick?: number;
  maxRowsPerSegment?: number;
};

export type SegmenterHooks = {
  onSegmentSealed?: (stream: string, payloadBytes: number, segmentBytes: number) => void;
};

export class Segmenter {
  private readonly config: Config;
  private readonly db: SqliteDurableStore;
  private readonly opts: Required<SegmenterOptions>;
  private readonly hooks?: SegmenterHooks;
  private timer: any | null = null;
  private running = false;
  private stopping = false;
  private readonly failures = new FailureTracker(1024);

  constructor(config: Config, db: SqliteDurableStore, opts: SegmenterOptions = {}, hooks?: SegmenterHooks) {
    this.config = config;
    this.db = db;
    this.opts = {
      minCandidateBytes: opts.minCandidateBytes ?? config.segmentMaxBytes,
      minCandidateRows: opts.minCandidateRows ?? config.segmentTargetRows,
      maxIntervalMs: opts.maxIntervalMs ?? config.segmentMaxIntervalMs,
      candidatesPerTick: opts.candidatesPerTick ?? 8,
      maxRowsPerSegment: opts.maxRowsPerSegment ?? 250_000,
    };
    this.hooks = hooks;
  }

  start(): void {
    this.stopping = false;
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.config.segmentCheckIntervalMs);
  }

  stop(hard = false): void {
    if (hard) this.stopping = true;
    else this.stopping = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async tick(): Promise<void> {
    if (this.stopping) return;
    if (this.running) return;
    this.running = true;
    try {
      const candidates = this.db.candidates(
        BigInt(this.opts.minCandidateBytes),
        BigInt(this.opts.minCandidateRows),
        BigInt(this.opts.maxIntervalMs),
        this.opts.candidatesPerTick
      );
      for (const c of candidates) {
        if (this.failures.shouldSkip(c.stream)) continue;
        try {
          await this.buildOne(c.stream);
          this.failures.recordSuccess(c.stream);
        } catch (e) {
          this.failures.recordFailure(c.stream);
          const msg = String((e as any)?.message ?? e);
          const lower = msg.toLowerCase();
          if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
            // eslint-disable-next-line no-console
            console.error("segment build failed", c.stream, e);
          }
        }
      }
    } catch (e) {
      const msg = String((e as any)?.message ?? e);
      const lower = msg.toLowerCase();
      if (!this.stopping && !lower.includes("database has closed") && !lower.includes("closed database") && !lower.includes("statement has finalized")) {
        // eslint-disable-next-line no-console
        console.error("segmenter tick error", e);
      }
    } finally {
      this.running = false;
    }
  }

  private isSqliteBusy(err: any): boolean {
    const code = String(err?.code ?? "");
    const errno = Number(err?.errno ?? -1);
    return code === "SQLITE_BUSY" || code === "SQLITE_BUSY_SNAPSHOT" || errno === 5 || errno === 517;
  }

  private async runWithBusyRetry<T>(fn: () => T): Promise<T> {
    const maxBusyMs = Math.max(0, this.config.ingestBusyTimeoutMs);
    if (maxBusyMs <= 0) return fn();
    const startMs = Date.now();
    let attempt = 0;
    for (;;) {
      try {
        return fn();
      } catch (e) {
        if (!this.isSqliteBusy(e)) throw e;
        const elapsed = Date.now() - startMs;
        if (elapsed >= maxBusyMs) throw e;
        const delay = Math.min(200, 5 * 2 ** attempt);
        attempt += 1;
        await new Promise((res) => setTimeout(res, delay));
      }
    }
  }

  private cleanupTmp(tmpPath: string): void {
    try {
      if (existsSync(tmpPath)) unlinkSync(tmpPath);
    } catch {
      // ignore
    }
  }

  private async buildOne(stream: string): Promise<void> {
    if (this.stopping) return;
    const row = this.db.getStream(stream);
    if (!row || this.db.isDeleted(row)) return;
    if (row.segment_in_progress) return;

    const startOffset = row.sealed_through + 1n;
    const maxOffset = row.next_offset - 1n;
    if (startOffset > maxOffset) return;

    // Claim.
    if (!this.db.tryClaimSegment(stream)) return;

    try {
      const segmentIndex = this.db.nextSegmentIndexForStream(stream);
      const shash = streamHash16Hex(stream);
      const localPath = localSegmentPath(this.config.rootDir, shash, segmentIndex);
      const tmpPath = `${localPath}.tmp`;
      mkdirSync(dirname(localPath), { recursive: true });

      // Build blocks and stream-write to temp file.
      const fd = openSync(tmpPath, "w");
      try {
        let blockRecords: SegmentRecord[] = [];
        let blockBytesApprox = 0;
        let fileBytes = 0;
        let blockCount = 0;
        let blockFirstOffset = startOffset;
        const blockIndex: BlockIndexEntry[] = [];

        // Decide endOffset by scanning WAL rows until threshold.
        // IMPORTANT: pending_bytes tracks WAL payload bytes only (not record/block overhead).
        let payloadBytes = 0n;
        let rowsSealed = 0n;
        let endOffset = startOffset - 1n;
        let lastAppendMs = 0n;

        let lastYieldMs = Date.now();
        let recordsSinceYield = 0;
        for (const rec of this.db.iterWalRange(stream, startOffset, maxOffset)) {
          const offset = BigInt(rec.offset);
          const payload: Uint8Array = rec.payload;
          const routingKey: Uint8Array | null = rec.routing_key ?? null;
          const appendMs = BigInt(rec.ts_ms);
          lastAppendMs = appendMs;

          const keyBytes = routingKey ?? new Uint8Array(0);
          const segRec: SegmentRecord = {
            appendNs: appendMs * 1_000_000n,
            routingKey: keyBytes,
            payload,
          };
          const recSize = 8 + 4 + keyBytes.byteLength + 4 + payload.byteLength;

          if (blockRecords.length > 0 && blockBytesApprox + recSize > this.config.blockMaxBytes) {
            const blockOffset = fileBytes;
            const block = encodeBlock(blockRecords);
            const compressedLen = readU32BE(block, 8);
            blockIndex.push({
              blockOffset,
              firstOffset: blockFirstOffset,
              recordCount: blockRecords.length,
              compressedLen,
              firstAppendNs: blockRecords[0].appendNs,
              lastAppendNs: blockRecords[blockRecords.length - 1].appendNs,
            });
            writeSync(fd, block);
            fileBytes += block.byteLength;
            blockCount += 1;
            blockRecords = [];
            blockBytesApprox = 0;
            await yieldToEventLoop();
          }

          if (blockRecords.length === 0) blockFirstOffset = offset;
          blockRecords.push(segRec);
          blockBytesApprox += recSize;

          payloadBytes += BigInt(payload.byteLength);
          rowsSealed += 1n;
          endOffset = offset;

          recordsSinceYield += 1;
          if (recordsSinceYield >= 512 || Date.now() - lastYieldMs >= 10) {
            await yieldToEventLoop();
            lastYieldMs = Date.now();
            recordsSinceYield = 0;
          }

          if (payloadBytes >= BigInt(this.config.segmentMaxBytes)) break;
          if (rowsSealed >= BigInt(this.opts.maxRowsPerSegment)) break;
        }

        if (rowsSealed === 0n) return;

        if (blockRecords.length > 0) {
          const blockOffset = fileBytes;
          const block = encodeBlock(blockRecords);
          const compressedLen = readU32BE(block, 8);
          blockIndex.push({
            blockOffset,
            firstOffset: blockFirstOffset,
            recordCount: blockRecords.length,
            compressedLen,
            firstAppendNs: blockRecords[0].appendNs,
            lastAppendNs: blockRecords[blockRecords.length - 1].appendNs,
          });
          writeSync(fd, block);
          fileBytes += block.byteLength;
          blockCount += 1;
        }

        const footer = encodeFooter(blockIndex);
        writeSync(fd, footer);
        fileBytes += footer.byteLength;

        fsyncSync(fd);

        const segmentId = `${shash}-${segmentIndex}-${startOffset.toString()}-${endOffset.toString()}`;
        renameSync(tmpPath, localPath);

        if (!this.stopping) {
          try {
            await this.runWithBusyRetry(() => {
              this.db.commitSealedSegment({
                segmentId,
                stream,
                segmentIndex,
                startOffset,
                endOffset,
                blockCount,
                lastAppendMs,
                sizeBytes: fileBytes,
                localPath,
                payloadBytes,
                rowsSealed,
              });
            });
            if (this.hooks?.onSegmentSealed) this.hooks.onSegmentSealed(stream, Number(payloadBytes), fileBytes);
          } catch (e) {
            try {
              if (existsSync(localPath)) unlinkSync(localPath);
            } catch {
              // ignore
            }
            throw e;
          }
        }
      } finally {
        closeSync(fd);
        this.cleanupTmp(tmpPath);
      }
    } finally {
      // Release claim.
      if (!this.stopping) {
        try {
          this.db.setSegmentInProgress(stream, 0);
        } catch {
          // ignore
        }
      }
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
