import type { Config } from "./config";
import type { SqliteDurableStore } from "./db/db";
import { STREAM_FLAG_DELETED } from "./db/db";
import type { StatsCollector } from "./stats";
import type { BackpressureGate } from "./backpressure";
import type { Metrics } from "./metrics";
import { Result } from "better-result";

export type AppendRow = {
  routingKey: Uint8Array | null;
  contentType: string | null;
  payload: Uint8Array;
};

export type ProducerInfo = {
  id: string;
  epoch: number;
  seq: number;
};

export type AppendSuccess = {
  lastOffset: bigint;
  appendedRows: number;
  closed: boolean;
  duplicate: boolean;
  producer?: { epoch: number; seq: number };
};

export type AppendError =
  | { kind: "not_found" | "gone" | "content_type_mismatch" | "overloaded" | "internal" }
  | { kind: "stream_seq"; expected: string; received: string }
  | { kind: "closed"; lastOffset: bigint }
  | { kind: "producer_stale_epoch"; producerEpoch: number }
  | { kind: "producer_gap"; expected: number; received: number }
  | { kind: "producer_epoch_seq" };

export type AppendResult = Result<AppendSuccess, AppendError>;

type AppendTask = {
  stream: string;
  baseAppendMs: bigint;
  rows: AppendRow[];
  contentType: string | null;
  streamSeq: string | null;
  producer: ProducerInfo | null;
  close: boolean;
  reservedBytes: number;
  enqueuedAtMs?: number;
  resolve: (r: AppendResult) => void;
};

export class IngestQueue {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly stats?: StatsCollector;
  private readonly gate?: BackpressureGate;
  private readonly metrics?: Metrics;
  private readonly q: AppendTask[] = [];
  private timer: any | null = null;
  private scheduled = false;
  private queuedBytes = 0;
  private lastBacklogWarnMs = 0;

  // Prepared statements local to the ingestor.
  private readonly stmts: {
    getStream: any;
    insertWal: any;
    updateStreamAppend: any;
    updateStreamCloseOnly: any;
    getProducerState: any;
    upsertProducerState: any;
  };

  constructor(cfg: Config, db: SqliteDurableStore, stats?: StatsCollector, gate?: BackpressureGate, metrics?: Metrics) {
    this.cfg = cfg;
    this.db = db;
    this.stats = stats;
    this.gate = gate;
    this.metrics = metrics;

    this.stmts = {
      getStream: this.db.db.query(
        `SELECT stream, epoch, next_offset, last_append_ms, expires_at_ms, stream_flags,
                content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq
         FROM streams WHERE stream=? LIMIT 1;`
      ),
      insertWal: this.db.db.query(
        `INSERT INTO wal(stream, offset, ts_ms, payload, payload_len, routing_key, content_type, flags)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?);`
      ),
      updateStreamAppend: this.db.db.query(
        `UPDATE streams
         SET next_offset=?, updated_at_ms=?, last_append_ms=?,
             pending_rows=pending_rows+?, pending_bytes=pending_bytes+?,
             logical_size_bytes=logical_size_bytes+?,
             wal_rows=wal_rows+?, wal_bytes=wal_bytes+?,
             stream_seq=?,
             closed=CASE WHEN ? THEN 1 ELSE closed END,
             closed_producer_id=CASE WHEN ? THEN ? ELSE closed_producer_id END,
             closed_producer_epoch=CASE WHEN ? THEN ? ELSE closed_producer_epoch END,
             closed_producer_seq=CASE WHEN ? THEN ? ELSE closed_producer_seq END
         WHERE stream=? AND (stream_flags & ?) = 0;`
      ),
      updateStreamCloseOnly: this.db.db.query(
        `UPDATE streams
         SET closed=1,
             closed_producer_id=?,
             closed_producer_epoch=?,
             closed_producer_seq=?,
             updated_at_ms=?,
             stream_seq=?
         WHERE stream=? AND (stream_flags & ?) = 0;`
      ),
      getProducerState: this.db.db.query(
        `SELECT epoch, last_seq FROM producer_state WHERE stream=? AND producer_id=? LIMIT 1;`
      ),
      upsertProducerState: this.db.db.query(
        `INSERT INTO producer_state(stream, producer_id, epoch, last_seq, updated_at_ms)
         VALUES(?, ?, ?, ?, ?)
         ON CONFLICT(stream, producer_id) DO UPDATE SET
           epoch=excluded.epoch,
           last_seq=excluded.last_seq,
           updated_at_ms=excluded.updated_at_ms;`
      ),
    };

    this.timer = setInterval(() => {
      void this.flush();
    }, this.cfg.ingestFlushIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /**
   * Enqueue an append. This returns after the batch containing it has committed.
   */
  append(args: {
    stream: string;
    baseAppendMs: bigint;
    rows: AppendRow[];
    contentType: string | null;
    streamSeq?: string | null;
    producer?: ProducerInfo | null;
    close?: boolean;
  }, opts?: { bypassBackpressure?: boolean; priority?: "high" | "normal" }): Promise<AppendResult> {
    const bytes = args.rows.reduce((acc, r) => acc + r.payload.byteLength, 0);
    if (!opts?.bypassBackpressure) {
      if (this.q.length >= this.cfg.ingestMaxQueueRequests || this.queuedBytes + bytes > this.cfg.ingestMaxQueueBytes) {
        if (this.metrics) this.metrics.record("tieredstore.backpressure.over_limit", 1, "count", { reason: "queue" });
        return Promise.resolve(Result.err({ kind: "overloaded" }));
      }
      if (this.gate && !this.gate.reserve(bytes)) {
        if (this.metrics) this.metrics.record("tieredstore.backpressure.over_limit", 1, "count", { reason: "backlog" });
        this.warnBacklog();
        return Promise.resolve(Result.err({ kind: "overloaded" }));
      }
    }
    this.queuedBytes += bytes;
    return new Promise((resolve) => {
      const task: AppendTask = {
        stream: args.stream,
        baseAppendMs: args.baseAppendMs,
        rows: args.rows,
        contentType: args.contentType ?? null,
        streamSeq: args.streamSeq ?? null,
        producer: args.producer ?? null,
        close: args.close ?? false,
        reservedBytes: opts?.bypassBackpressure ? 0 : bytes,
        enqueuedAtMs: this.stats ? Date.now() : undefined,
        resolve,
      };
      if (opts?.priority === "high") this.q.unshift(task);
      else this.q.push(task);
      // Opportunistic flush if the queue gets large.
      if (!this.scheduled && this.q.length >= this.cfg.ingestMaxBatchRequests) {
        this.scheduled = true;
        setTimeout(() => {
          this.scheduled = false;
          void this.flush();
        }, 0);
      }
    });
  }

  appendInternal(args: {
    stream: string;
    baseAppendMs: bigint;
    rows: AppendRow[];
    contentType: string | null;
  }): Promise<AppendResult> {
    return this.append(args, { bypassBackpressure: true, priority: "high" });
  }

  getQueueStats(): { requests: number; bytes: number } {
    return { requests: this.q.length, bytes: this.queuedBytes };
  }

  getMemoryStats(): { queuedPayloadBytes: number; queuedRequests: number } {
    return {
      queuedPayloadBytes: this.queuedBytes,
      queuedRequests: this.q.length,
    };
  }

  isQueueFull(): boolean {
    return this.q.length >= this.cfg.ingestMaxQueueRequests || this.queuedBytes >= this.cfg.ingestMaxQueueBytes;
  }

  private warnBacklog(): void {
    if (!this.gate) return;
    const now = Date.now();
    if (now - this.lastBacklogWarnMs < 10_000) return;
    this.lastBacklogWarnMs = now;
    const current = this.gate.getCurrentBytes();
    const max = this.gate.getMaxBytes();
    const msg =
      `[backpressure] local backlog ${formatBytes(current)} exceeds limit ${formatBytes(max)}; rejecting appends (DS_LOCAL_BACKLOG_MAX_BYTES)`;
    // eslint-disable-next-line no-console
    console.warn(msg);
  }

  async flush(): Promise<void> {
    if (this.q.length === 0) return;
    const flushStartMs = Date.now();
    let busyWaitMs = 0;

    // Drain up to limits.
    const batch: AppendTask[] = [];
    let batchBytes = 0;
    let batchReservedBytes = 0;
    let drainCount = 0;
    while (drainCount < this.q.length && batch.length < this.cfg.ingestMaxBatchRequests && batchBytes < this.cfg.ingestMaxBatchBytes) {
      const t = this.q[drainCount]!;
      batch.push(t);
      drainCount += 1;
      for (const r of t.rows) batchBytes += r.payload.byteLength;
      batchReservedBytes += t.reservedBytes;
    }
    if (drainCount > 0) {
      this.q.splice(0, drainCount);
    }
    this.queuedBytes = Math.max(0, this.queuedBytes - batchBytes);

    // Compute queue wait/backpressure stats before executing the batch.
    let bpOverMs = 0;
    if (this.stats) {
      const budgetMs = this.stats.getBackpressureBudgetMs();
      const nowMs = Date.now();
      for (const t of batch) {
        if (t.enqueuedAtMs == null) continue;
        const waitMs = Math.max(0, nowMs - t.enqueuedAtMs);
        if (waitMs > budgetMs) {
          bpOverMs += waitMs - budgetMs;
        }
      }
    }

    // Execute batch in a single SQLite transaction.
    const nowMs = this.db.nowMs();
    type StreamState = {
      epoch: number;
      nextOffset: bigint;
      lastAppendMs: bigint;
      expiresAtMs: bigint | null;
      streamFlags: number;
      contentType: string;
      streamSeq: string | null;
      closed: boolean;
      closedProducerId: string | null;
      closedProducerEpoch: number | null;
      closedProducerSeq: number | null;
    };
    type ProducerState = { epoch: number; lastSeq: number };

    let perStream = new Map<string, StreamState>();
    let perProducer = new Map<string, ProducerState | null>();

    let walBytesCommitted = 0;

    // Compute results inside the transaction, but only resolve after commit.
    let results: AppendResult[] = [];

    const resetAttempt = () => {
      perStream = new Map<string, StreamState>();
      perProducer = new Map<string, ProducerState | null>();
      walBytesCommitted = 0;
      results = new Array(batch.length);
    };

    const tx = this.db.db.transaction(() => {
      const loadStream = (stream: string): StreamState | null => {
        const cached = perStream.get(stream);
        if (cached) return cached;
        const row = this.stmts.getStream.get(stream) as any;
        if (!row || (Number(row.stream_flags) & STREAM_FLAG_DELETED) !== 0) return null;
        const st: StreamState = {
          epoch: Number(row.epoch),
          nextOffset: BigInt(row.next_offset),
          lastAppendMs: BigInt(row.last_append_ms),
          expiresAtMs: row.expires_at_ms == null ? null : BigInt(row.expires_at_ms),
          streamFlags: Number(row.stream_flags),
          contentType: String(row.content_type),
          streamSeq: row.stream_seq == null ? null : String(row.stream_seq),
          closed: Number(row.closed) !== 0,
          closedProducerId: row.closed_producer_id == null ? null : String(row.closed_producer_id),
          closedProducerEpoch: row.closed_producer_epoch == null ? null : Number(row.closed_producer_epoch),
          closedProducerSeq: row.closed_producer_seq == null ? null : Number(row.closed_producer_seq),
        };
        perStream.set(stream, st);
        return st;
      };

      const loadProducerState = (stream: string, producerId: string): ProducerState | null => {
        const key = `${stream}\u0000${producerId}`;
        if (perProducer.has(key)) return perProducer.get(key)!;
        const row = this.stmts.getProducerState.get(stream, producerId) as any;
        const state = row ? { epoch: Number(row.epoch), lastSeq: Number(row.last_seq) } : null;
        perProducer.set(key, state);
        return state;
      };

      const checkProducer = (
        task: AppendTask
      ): Result<{ duplicate: boolean; update: boolean; epoch: number; seq: number }, AppendError> => {
        const producer = task.producer!;
        const key = `${task.stream}\u0000${producer.id}`;
        const state = loadProducerState(task.stream, producer.id);
        if (!state) {
          if (producer.seq !== 0) {
            return Result.err({ kind: "producer_epoch_seq" });
          }
          const next = { epoch: producer.epoch, lastSeq: producer.seq };
          perProducer.set(key, next);
          return Result.ok({ duplicate: false, update: true, epoch: producer.epoch, seq: producer.seq });
        }
        if (producer.epoch < state.epoch) {
          return Result.err({ kind: "producer_stale_epoch", producerEpoch: state.epoch });
        }
        if (producer.epoch > state.epoch) {
          if (producer.seq !== 0) {
            return Result.err({ kind: "producer_epoch_seq" });
          }
          const next = { epoch: producer.epoch, lastSeq: producer.seq };
          perProducer.set(key, next);
          return Result.ok({ duplicate: false, update: true, epoch: producer.epoch, seq: producer.seq });
        }
        if (producer.seq <= state.lastSeq) {
          return Result.ok({ duplicate: true, update: false, epoch: state.epoch, seq: state.lastSeq });
        }
        if (producer.seq === state.lastSeq + 1) {
          const next = { epoch: state.epoch, lastSeq: producer.seq };
          perProducer.set(key, next);
          return Result.ok({ duplicate: false, update: true, epoch: state.epoch, seq: producer.seq });
        }
        return Result.err({ kind: "producer_gap", expected: state.lastSeq + 1, received: producer.seq });
      };

      const checkStreamSeq = (
        task: AppendTask,
        st: StreamState
      ): Result<{ nextSeq: string | null }, AppendError> => {
        if (task.streamSeq == null) return Result.ok({ nextSeq: st.streamSeq });
        if (st.streamSeq != null && task.streamSeq <= st.streamSeq) {
          return Result.err({
            kind: "stream_seq",
            expected: st.streamSeq,
            received: task.streamSeq,
          });
        }
        return Result.ok({ nextSeq: task.streamSeq });
      };

      for (let idx = 0; idx < batch.length; idx++) {
        const task = batch[idx];
        const st = loadStream(task.stream);
        if (!st) {
          results[idx] = Result.err({ kind: "not_found" });
          continue;
        }
        if (st.expiresAtMs != null && nowMs > st.expiresAtMs) {
          results[idx] = Result.err({ kind: "gone" });
          continue;
        }

        const tailOffset = st.nextOffset - 1n;
        const isCloseOnly = task.close && task.rows.length === 0;

        if (st.closed) {
          if (isCloseOnly) {
            results[idx] = Result.ok({
              lastOffset: tailOffset,
              appendedRows: 0,
              closed: true,
              duplicate: true,
            });
            continue;
          }
          if (
            task.producer &&
            task.close &&
            st.closedProducerId != null &&
            st.closedProducerEpoch != null &&
            st.closedProducerSeq != null &&
            st.closedProducerId === task.producer.id &&
            st.closedProducerEpoch === task.producer.epoch &&
            st.closedProducerSeq === task.producer.seq
          ) {
            results[idx] = Result.ok({
              lastOffset: tailOffset,
              appendedRows: 0,
              closed: true,
              duplicate: true,
              producer: { epoch: st.closedProducerEpoch, seq: st.closedProducerSeq },
            });
            continue;
          }
          results[idx] = Result.err({ kind: "closed", lastOffset: tailOffset });
          continue;
        }

        if (isCloseOnly) {
          let producerInfo: { epoch: number; seq: number } | undefined;
          let duplicate = false;
          if (task.producer) {
            const prodCheck = checkProducer(task);
            if (Result.isError(prodCheck)) {
              results[idx] = Result.err(prodCheck.error);
              continue;
            }
            duplicate = prodCheck.value.duplicate;
            producerInfo = { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq };
            if (prodCheck.value.update) {
              this.stmts.upsertProducerState.run(
                task.stream,
                task.producer.id,
                prodCheck.value.epoch,
                prodCheck.value.seq,
                nowMs
              );
            }
          }
          if (!duplicate) {
            const seqCheck = checkStreamSeq(task, st);
            if (Result.isError(seqCheck)) {
              results[idx] = Result.err(seqCheck.error);
              continue;
            }
            st.streamSeq = seqCheck.value.nextSeq;
            const closedProducer = task.producer ?? null;
            this.stmts.updateStreamCloseOnly.run(
              closedProducer ? closedProducer.id : null,
              closedProducer ? closedProducer.epoch : null,
              closedProducer ? closedProducer.seq : null,
              nowMs,
              st.streamSeq,
              task.stream,
              STREAM_FLAG_DELETED
            );
            st.closed = true;
            st.closedProducerId = closedProducer ? closedProducer.id : null;
            st.closedProducerEpoch = closedProducer ? closedProducer.epoch : null;
            st.closedProducerSeq = closedProducer ? closedProducer.seq : null;
          }
          results[idx] = Result.ok({
            lastOffset: tailOffset,
            appendedRows: 0,
            closed: st.closed,
            duplicate,
            producer: producerInfo,
          });
          continue;
        }

        if (!task.contentType || task.contentType !== st.contentType) {
          results[idx] = Result.err({ kind: "content_type_mismatch" });
          continue;
        }

        let producerInfo: { epoch: number; seq: number } | undefined;
        if (task.producer) {
          const prodCheck = checkProducer(task);
          if (Result.isError(prodCheck)) {
            results[idx] = Result.err(prodCheck.error);
            continue;
          }
          if (prodCheck.value.duplicate) {
            results[idx] = Result.ok({
              lastOffset: tailOffset,
              appendedRows: 0,
              closed: false,
              duplicate: true,
              producer: { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq },
            });
            continue;
          }
          producerInfo = { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq };
          if (prodCheck.value.update) {
            this.stmts.upsertProducerState.run(
              task.stream,
              task.producer.id,
              prodCheck.value.epoch,
              prodCheck.value.seq,
              nowMs
            );
          }
        }

        const seqCheck = checkStreamSeq(task, st);
        if (Result.isError(seqCheck)) {
          results[idx] = Result.err(seqCheck.error);
          continue;
        }
        st.streamSeq = seqCheck.value.nextSeq;

        // Clamp timestamps to be monotonic per stream (ms resolution).
        let appendMs = task.baseAppendMs;
        if (appendMs <= st.lastAppendMs) appendMs = st.lastAppendMs + 1n;

        let offset = st.nextOffset;
        let totalBytes = 0n;
        for (let i = 0; i < task.rows.length; i++) {
          const r = task.rows[i];
          // Use a constant per-request timestamp for all rows. Offsets already
          // provide ordering, and per-row +1ms stamping breaks age-based WAL
          // retention under high fanout (e.g. touch streams) by pushing ts_ms
          // far into the future.
          const rowAppendMs = appendMs;
          const payloadLen = r.payload.byteLength;
          totalBytes += BigInt(payloadLen);
          this.stmts.insertWal.run(task.stream, offset, rowAppendMs, r.payload, payloadLen, r.routingKey, r.contentType, 0);
          offset += 1n;
        }
        const lastOffset = offset - 1n;
        st.nextOffset = offset;
        st.lastAppendMs = appendMs;
        if (task.close) {
          st.closed = true;
          if (task.producer) {
            st.closedProducerId = task.producer.id;
            st.closedProducerEpoch = task.producer.epoch;
            st.closedProducerSeq = task.producer.seq;
          } else {
            st.closedProducerId = null;
            st.closedProducerEpoch = null;
            st.closedProducerSeq = null;
          }
        }

        const closedProducer = task.close && task.producer ? task.producer : null;
        const closeFlag = task.close ? 1 : 0;
        this.stmts.updateStreamAppend.run(
          st.nextOffset,
          nowMs,
          st.lastAppendMs,
          BigInt(task.rows.length),
          totalBytes,
          totalBytes,
          BigInt(task.rows.length),
          totalBytes,
          st.streamSeq,
          closeFlag,
          closeFlag,
          closedProducer ? closedProducer.id : null,
          closeFlag,
          closedProducer ? closedProducer.epoch : null,
          closeFlag,
          closedProducer ? closedProducer.seq : null,
          task.stream,
          STREAM_FLAG_DELETED
        );
        walBytesCommitted += Number(totalBytes);

        results[idx] = Result.ok({
          lastOffset,
          appendedRows: task.rows.length,
          closed: task.close,
          duplicate: false,
          producer: producerInfo,
        });
      }
    });

    const isSqliteBusy = (e: any): boolean => {
      const code = String(e?.code ?? "");
      const errno = Number(e?.errno ?? -1);
      return code === "SQLITE_BUSY" || code === "SQLITE_BUSY_SNAPSHOT" || errno === 5 || errno === 517;
    };

    const sleep = (ms: number): Promise<void> => new Promise((res) => setTimeout(res, ms));

    try {
      const maxBusyMs = Math.max(0, this.cfg.ingestBusyTimeoutMs);
      const startMs = Date.now();
      let attempt = 0;
      while (true) {
        resetAttempt();
        try {
          tx();
          break;
        } catch (e) {
          if (!isSqliteBusy(e)) throw e;
          if (maxBusyMs <= 0) throw e;
          const elapsed = Date.now() - startMs;
          if (elapsed >= maxBusyMs) throw e;
          const delay = Math.min(200, 5 * 2 ** attempt);
          attempt += 1;
          busyWaitMs += delay;
          await sleep(delay);
        }
      }
      if (this.gate) {
        const reservedCommitted = Math.min(batchReservedBytes, walBytesCommitted);
        this.gate.commit(walBytesCommitted, reservedCommitted);
        const extra = batchReservedBytes - walBytesCommitted;
        if (extra > 0) this.gate.release(extra);
      }
      if (this.stats && walBytesCommitted > 0) this.stats.recordWalCommitBytes(walBytesCommitted);
      if (this.stats && bpOverMs > 0) this.stats.recordBackpressureOverMs(bpOverMs);
      for (let i = 0; i < batch.length; i++) batch[i].resolve(results[i] ?? Result.err({ kind: "internal" }));
      const elapsedNs = (Date.now() - flushStartMs) * 1_000_000;
      if (this.metrics) {
        this.metrics.record("tieredstore.ingest.flush.latency", elapsedNs, "ns");
        if (busyWaitMs > 0) this.metrics.record("tieredstore.ingest.sqlite_busy.wait", busyWaitMs * 1_000_000, "ns");
      }
    } catch (e) {
      // If the whole transaction failed, all tasks are treated as internal errors.
      // eslint-disable-next-line no-console
      console.error("ingest tx failed", e);
      if (this.gate && batchReservedBytes > 0) this.gate.release(batchReservedBytes);
      for (const t of batch) t.resolve(Result.err({ kind: "internal" }));
      const elapsedNs = (Date.now() - flushStartMs) * 1_000_000;
      if (this.metrics) {
        this.metrics.record("tieredstore.ingest.flush.latency", elapsedNs, "ns");
        if (busyWaitMs > 0) this.metrics.record("tieredstore.ingest.sqlite_busy.wait", busyWaitMs * 1_000_000, "ns");
      }
    }
  }
}

function formatBytes(bytes: number): string {
  const units = ["b", "kb", "mb", "gb"];
  let value = Math.max(0, bytes);
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  const digits = idx === 0 ? 0 : 1;
  return `${value.toFixed(digits)}${units[idx]}`;
}
