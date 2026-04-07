import { mkdirSync, openSync, closeSync, writeSync, fsyncSync, renameSync, existsSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { isMainThread } from "node:worker_threads";
import type { Config } from "../config";
import type { SqliteDurableStore } from "../db/db";
import { encodeBlock, encodeFooter, type BlockIndexEntry, type SegmentRecord } from "./format";
import { readU32BE } from "../util/endian";
import { localSegmentPath, streamHash16Hex } from "../util/stream_paths";
import { LruCache } from "../util/lru";
import { RuntimeMemorySampler } from "../runtime_memory_sampler";
import { yieldToEventLoop } from "../util/yield";
import { beginSegmentBuildAction } from "./segment_build_actions";
import type { SegmentCommitArgs, SegmenterControl } from "./segmenter_control";

export type SegmenterOptions = {
  minCandidateBytes?: number; // default: segmentMaxBytes
  minCandidateRows?: number; // default: segmentTargetRows
  maxIntervalMs?: number; // default: segmentMaxIntervalMs
  candidatesPerTick?: number;
  maxRowsPerSegment?: number;
};

export type SegmenterHooks = {
  onSegmentSealed?: (stream: string, payloadBytes: number, segmentBytes: number) => void;
  control?: SegmenterControl;
};

export type SegmenterMemoryStats = {
  active_builds: number;
  active_streams: number;
  active_payload_bytes: number;
  active_segment_bytes_estimate: number;
  active_rows: number;
};

const SEGMENT_COMPRESSION_WINDOW = 8;
const MIN_COMPRESSED_FILL_RATIO = 0.5;
const SHOULD_COOPERATIVELY_YIELD_WITHIN_SEGMENT_BUILD = isMainThread;
type BusyRetryMetrics = {
  attempts: number;
  waitMs: number;
};

export class Segmenter {
  private readonly config: Config;
  private readonly db: SqliteDurableStore;
  private readonly opts: Required<SegmenterOptions>;
  private readonly hooks?: SegmenterHooks;
  private readonly memorySampler?: RuntimeMemorySampler;
  private timer: any | null = null;
  private running = false;
  private stopping = false;
  private readonly failures = new FailureTracker(1024);
  private activeBuildStream: string | null = null;
  private activePayloadBytes = 0;
  private activeSegmentBytesEstimate = 0;
  private activeRows = 0;

  constructor(
    config: Config,
    db: SqliteDurableStore,
    opts: SegmenterOptions = {},
    hooks?: SegmenterHooks,
    memorySampler?: RuntimeMemorySampler
  ) {
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
    this.memorySampler = memorySampler;
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

  getMemoryStats(): SegmenterMemoryStats {
    return {
      active_builds: this.activeBuildStream ? 1 : 0,
      active_streams: this.activeBuildStream ? 1 : 0,
      active_payload_bytes: this.activePayloadBytes,
      active_segment_bytes_estimate: this.activeSegmentBytesEstimate,
      active_rows: this.activeRows,
    };
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

  private async runWithBusyRetry<T>(fn: () => T, metrics?: BusyRetryMetrics): Promise<T> {
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
        if (metrics) {
          metrics.attempts += 1;
          metrics.waitMs += delay;
        }
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

  private resolvePayloadSealTargetBytes(stream: string): bigint {
    const baseTarget = BigInt(this.config.segmentMaxBytes);
    const ratio = this.db.recentSegmentCompressionRatio(stream, SEGMENT_COMPRESSION_WINDOW);
    if (ratio == null || !Number.isFinite(ratio) || ratio <= 0 || ratio >= MIN_COMPRESSED_FILL_RATIO) {
      return baseTarget;
    }
    const desiredCompressedBytes = Math.ceil(this.config.segmentMaxBytes * MIN_COMPRESSED_FILL_RATIO);
    const boosted = BigInt(Math.ceil(desiredCompressedBytes / ratio));
    return boosted > baseTarget ? boosted : baseTarget;
  }

  private shouldSealStream(row: { stream: string; pending_bytes: bigint; pending_rows: bigint; last_segment_cut_ms: bigint }): boolean {
    const payloadSealTargetBytes = this.resolvePayloadSealTargetBytes(row.stream);
    if (row.pending_bytes >= payloadSealTargetBytes) return true;
    if (row.pending_rows >= BigInt(this.opts.minCandidateRows)) return true;
    if (this.opts.maxIntervalMs > 0 && BigInt(Date.now()) - row.last_segment_cut_ms >= BigInt(this.opts.maxIntervalMs)) return true;
    return false;
  }

  private async buildOne(stream: string): Promise<void> {
    if (this.stopping) return;
    const row = this.db.getStream(stream);
    if (!row || this.db.isDeleted(row)) return;
    if (row.segment_in_progress) return;
    if (!this.shouldSealStream(row)) return;

    const startOffset = row.sealed_through + 1n;
    const maxOffset = row.next_offset - 1n;
    if (startOffset > maxOffset) return;

    // Claim.
    const claimed = this.hooks?.control ? await this.hooks.control.tryClaimSegment(stream) : this.db.tryClaimSegment(stream);
    if (!claimed) return;

    try {
      this.activeBuildStream = stream;
      this.activePayloadBytes = 0;
      this.activeSegmentBytesEstimate = 0;
      this.activeRows = 0;
      const segmentIndex = this.db.nextSegmentIndexForStream(stream);
      const shash = streamHash16Hex(stream);
      const localPath = localSegmentPath(this.config.rootDir, shash, segmentIndex);
      const tmpPath = `${localPath}.tmp`;
      const leaveCutPhase = this.memorySampler?.enter("cut", {
        stream,
        segment_index: segmentIndex,
      });
      mkdirSync(dirname(localPath), { recursive: true });
      const payloadSealTargetBytes = this.resolvePayloadSealTargetBytes(stream);
      const rowSealTarget = BigInt(this.opts.minCandidateRows);
      const action = beginSegmentBuildAction(this.db, {
        stream,
        actionKind: "segment_build",
        inputKind: "wal",
        segmentIndex,
        startOffset,
        detail: {
          pending_rows_before: Number(row.pending_rows),
          pending_bytes_before: Number(row.pending_bytes),
          seal_target_bytes: Number(payloadSealTargetBytes),
          row_seal_target: this.opts.minCandidateRows,
          max_rows_per_segment: this.opts.maxRowsPerSegment,
          block_max_bytes: this.config.blockMaxBytes,
        },
      });

      // Build blocks and stream-write to temp file.
      const fd = openSync(tmpPath, "w");
      try {
        const buildStartedAtMs = Date.now();
        let blockRecords: SegmentRecord[] = [];
        let blockBytesApprox = 0;
        let fileBytes = 0;
        let blockCount = 0;
        let blockFirstOffset = startOffset;
        const blockIndex: BlockIndexEntry[] = [];
        let blockEncodeMs = 0;
        let footerEncodeMs = 0;
        let writeMs = 0;
        let fsyncMs = 0;
        let renameMs = 0;
        let commitMs = 0;
        let walFetchMs = 0;
        let walMaterializeMs = 0;
        let walLoopMs = 0;
        const busyRetryMetrics: BusyRetryMetrics = {
          attempts: 0,
          waitMs: 0,
        };

        // Decide endOffset by scanning WAL rows until threshold.
        // IMPORTANT: pending_bytes tracks WAL payload bytes only (not record/block overhead).
        let payloadBytes = 0n;
        let rowsSealed = 0n;
        let endOffset = startOffset - 1n;
        let lastAppendMs = 0n;
        let stopReason:
          | "payload_bytes"
          | "row_target"
          | "max_rows"
          | "exhausted_pending"
          | "no_rows"
          | "error" = "exhausted_pending";

        let lastYieldMs = Date.now();
        let recordsSinceYield = 0;
        const walIterator = this.db.iterWalRange(stream, startOffset, maxOffset)[Symbol.iterator]();
        try {
          for (;;) {
            const walFetchStartedAtMs = Date.now();
            const next = walIterator.next();
            walFetchMs += Date.now() - walFetchStartedAtMs;
            if (next.done) break;
            const walLoopStartedAtMs = Date.now();
            const rec = next.value;
            const walMaterializeStartedAtMs = Date.now();
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
            walMaterializeMs += Date.now() - walMaterializeStartedAtMs;

            if (blockRecords.length > 0 && blockBytesApprox + recSize > this.config.blockMaxBytes) {
              const blockOffset = fileBytes;
              const encodeStartedAtMs = Date.now();
              const block = encodeBlock(blockRecords);
              blockEncodeMs += Date.now() - encodeStartedAtMs;
              const compressedLen = readU32BE(block, 8);
              blockIndex.push({
                blockOffset,
                firstOffset: blockFirstOffset,
                recordCount: blockRecords.length,
                compressedLen,
                firstAppendNs: blockRecords[0].appendNs,
                lastAppendNs: blockRecords[blockRecords.length - 1].appendNs,
              });
              const writeStartedAtMs = Date.now();
              writeSync(fd, block);
              writeMs += Date.now() - writeStartedAtMs;
              fileBytes += block.byteLength;
              blockCount += 1;
              blockRecords = [];
              blockBytesApprox = 0;
              if (SHOULD_COOPERATIVELY_YIELD_WITHIN_SEGMENT_BUILD) {
                await yieldToEventLoop();
              }
            }

            if (blockRecords.length === 0) blockFirstOffset = offset;
            blockRecords.push(segRec);
            blockBytesApprox += recSize;

            payloadBytes += BigInt(payload.byteLength);
            rowsSealed += 1n;
            endOffset = offset;
            this.activePayloadBytes = Number(payloadBytes);
            this.activeRows = Number(rowsSealed);
            this.activeSegmentBytesEstimate = fileBytes + blockBytesApprox;

            recordsSinceYield += 1;
            if (
              SHOULD_COOPERATIVELY_YIELD_WITHIN_SEGMENT_BUILD &&
              (recordsSinceYield >= 512 || Date.now() - lastYieldMs >= 10)
            ) {
              await yieldToEventLoop();
              lastYieldMs = Date.now();
              recordsSinceYield = 0;
            }

            walLoopMs += Date.now() - walLoopStartedAtMs;
            if (payloadBytes >= payloadSealTargetBytes) {
              stopReason = "payload_bytes";
              break;
            }
            if (rowsSealed >= rowSealTarget) {
              stopReason = "row_target";
              break;
            }
            if (rowsSealed >= BigInt(this.opts.maxRowsPerSegment)) {
              stopReason = "max_rows";
              break;
            }
          }
        } finally {
          try {
            walIterator.return?.();
          } catch {
            // ignore
          }
        }

        if (rowsSealed === 0n) {
          action.fail("segment build saw no WAL rows", {
            inputCount: 0,
            inputSizeBytes: 0n,
            outputCount: 0,
            outputSizeBytes: 0n,
            endOffset: null,
            detail: {
              stop_reason: "no_rows",
            },
          });
          return;
        }

        if (blockRecords.length > 0) {
          const blockOffset = fileBytes;
          const encodeStartedAtMs = Date.now();
          const block = encodeBlock(blockRecords);
          blockEncodeMs += Date.now() - encodeStartedAtMs;
          const compressedLen = readU32BE(block, 8);
          blockIndex.push({
            blockOffset,
            firstOffset: blockFirstOffset,
            recordCount: blockRecords.length,
            compressedLen,
            firstAppendNs: blockRecords[0].appendNs,
            lastAppendNs: blockRecords[blockRecords.length - 1].appendNs,
          });
          const writeStartedAtMs = Date.now();
          writeSync(fd, block);
          writeMs += Date.now() - writeStartedAtMs;
          fileBytes += block.byteLength;
          blockCount += 1;
        }

        const footerStartedAtMs = Date.now();
        const footer = encodeFooter(blockIndex);
        footerEncodeMs += Date.now() - footerStartedAtMs;
        const footerWriteStartedAtMs = Date.now();
        writeSync(fd, footer);
        writeMs += Date.now() - footerWriteStartedAtMs;
        fileBytes += footer.byteLength;
        this.activeSegmentBytesEstimate = fileBytes;

        const fsyncStartedAtMs = Date.now();
        fsyncSync(fd);
        fsyncMs += Date.now() - fsyncStartedAtMs;

        const segmentId = `${shash}-${segmentIndex}-${startOffset.toString()}-${endOffset.toString()}`;
        const renameStartedAtMs = Date.now();
        renameSync(tmpPath, localPath);
        renameMs += Date.now() - renameStartedAtMs;

        if (!this.stopping) {
          try {
            const commitStartedAtMs = Date.now();
            const commitArgs: SegmentCommitArgs = {
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
            };
            if (this.hooks?.control) {
              await this.hooks.control.commitSealedSegment(commitArgs);
            } else {
              await this.runWithBusyRetry(() => {
                this.db.commitSealedSegment(commitArgs);
              }, busyRetryMetrics);
            }
            commitMs += Date.now() - commitStartedAtMs;
            action.succeed({
              inputCount: Number(rowsSealed),
              inputSizeBytes: payloadBytes,
              outputCount: 1,
              outputSizeBytes: BigInt(fileBytes),
              endOffset,
              detail: {
                block_count: blockCount,
                last_append_ms: Number(lastAppendMs),
                stop_reason: stopReason,
                compression_ratio: Number(payloadBytes) > 0 ? Number(fileBytes) / Number(payloadBytes) : 0,
                wal_fetch_ms: walFetchMs,
                wal_materialize_ms: walMaterializeMs,
                wal_loop_ms: walLoopMs,
                block_encode_ms: blockEncodeMs,
                footer_encode_ms: footerEncodeMs,
                write_ms: writeMs,
                fsync_ms: fsyncMs,
                rename_ms: renameMs,
                commit_ms: commitMs,
                busy_retry_attempts: busyRetryMetrics.attempts,
                busy_retry_wait_ms: busyRetryMetrics.waitMs,
                build_before_commit_ms: Date.now() - buildStartedAtMs - commitMs,
              },
            });
            if (this.hooks?.onSegmentSealed) this.hooks.onSegmentSealed(stream, Number(payloadBytes), fileBytes);
          } catch (e) {
            try {
              if (existsSync(localPath)) unlinkSync(localPath);
            } catch {
              // ignore
            }
            action.fail(String((e as any)?.message ?? e), {
              inputCount: Number(rowsSealed),
              inputSizeBytes: payloadBytes,
              outputCount: 0,
              outputSizeBytes: BigInt(fileBytes),
              endOffset,
              detail: {
                block_count: blockCount,
                last_append_ms: Number(lastAppendMs),
                stop_reason: "error",
                wal_fetch_ms: walFetchMs,
                wal_materialize_ms: walMaterializeMs,
                wal_loop_ms: walLoopMs,
                block_encode_ms: blockEncodeMs,
                footer_encode_ms: footerEncodeMs,
                write_ms: writeMs,
                fsync_ms: fsyncMs,
                rename_ms: renameMs,
                commit_ms: commitMs,
                busy_retry_attempts: busyRetryMetrics.attempts,
                busy_retry_wait_ms: busyRetryMetrics.waitMs,
              },
            });
            throw e;
          }
        } else {
          action.fail("segment build stopped before commit", {
            inputCount: Number(rowsSealed),
            inputSizeBytes: payloadBytes,
            outputCount: 0,
            outputSizeBytes: BigInt(fileBytes),
            endOffset,
            detail: {
              block_count: blockCount,
              last_append_ms: Number(lastAppendMs),
              stop_reason: "error",
              wal_fetch_ms: walFetchMs,
              wal_materialize_ms: walMaterializeMs,
              wal_loop_ms: walLoopMs,
              block_encode_ms: blockEncodeMs,
              footer_encode_ms: footerEncodeMs,
              write_ms: writeMs,
              fsync_ms: fsyncMs,
              rename_ms: renameMs,
              commit_ms: commitMs,
              busy_retry_attempts: busyRetryMetrics.attempts,
              busy_retry_wait_ms: busyRetryMetrics.waitMs,
            },
          });
        }
      } catch (e) {
        action.fail(String((e as any)?.message ?? e), {
          inputCount: this.activeRows,
          inputSizeBytes: BigInt(this.activePayloadBytes),
          outputCount: 0,
          outputSizeBytes: BigInt(this.activeSegmentBytesEstimate),
          endOffset: null,
          detail: {
            stop_reason: "error",
          },
        });
        throw e;
      } finally {
        closeSync(fd);
        this.cleanupTmp(tmpPath);
        leaveCutPhase?.();
      }
    } finally {
      this.activeBuildStream = null;
      this.activePayloadBytes = 0;
      this.activeSegmentBytesEstimate = 0;
      this.activeRows = 0;
      // Release claim.
      if (!this.stopping) {
        try {
          if (this.hooks?.control) await this.hooks.control.releaseSegmentClaim(stream);
          else this.db.setSegmentInProgress(stream, 0);
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
