import type { IngestQueue } from "../ingest";
import type { SqliteDurableStore } from "../db/db";
import { STREAM_FLAG_TOUCH } from "../db/db";
import { encodeOffset } from "../offset";
import type { TouchConfig } from "./spec";
import type { TemplateLifecycleEvent } from "./live_templates";
import type { TouchJournalIntervalStats, TouchJournalMeta } from "./touch_journal";
import { Result } from "better-result";

export type TouchKind = "table" | "template";

export type TouchEventPayload = {
  sourceOffset: string;
  entity: string;
  kind: TouchKind;
  templateId?: string;
};

type WaitOutcome = "touched" | "timeout" | "stale";
type EnsureLiveMetricsStreamError = {
  kind: "live_metrics_stream_content_type_mismatch";
  message: string;
};

type LatencyHistogram = {
  bounds: number[];
  counts: number[];
  record: (ms: number) => void;
  p50: () => number;
  p95: () => number;
  p99: () => number;
  reset: () => void;
};

function makeLatencyHistogram(): LatencyHistogram {
  const bounds = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10_000, 30_000, 120_000];
  const counts = new Array(bounds.length + 1).fill(0);
  const record = (ms: number) => {
    const x = Math.max(0, Math.floor(ms));
    let i = 0;
    while (i < bounds.length && x > bounds[i]) i++;
    counts[i] += 1;
  };
  const quantile = (q: number) => {
    const total = counts.reduce((a, b) => a + b, 0);
    if (total === 0) return 0;
    const target = Math.ceil(total * q);
    let acc = 0;
    for (let i = 0; i < counts.length; i++) {
      acc += counts[i];
      if (acc >= target) {
        return i < bounds.length ? bounds[i] : bounds[bounds.length - 1];
      }
    }
    return bounds[bounds.length - 1];
  };
  const reset = () => {
    for (let i = 0; i < counts.length; i++) counts[i] = 0;
  };
  return { bounds, counts, record, p50: () => quantile(0.5), p95: () => quantile(0.95), p99: () => quantile(0.99), reset };
}

function nowIso(ms: number): string {
  return new Date(ms).toISOString();
}

function envString(name: string): string | null {
  const v = process.env[name];
  return v && v.trim() !== "" ? v.trim() : null;
}

function getInstanceId(): string {
  return envString("DS_INSTANCE_ID") ?? envString("HOSTNAME") ?? "local";
}

function getRegion(): string {
  return envString("DS_REGION") ?? "local";
}

type StreamCounters = {
  touch: {
    coarseIntervalMs: number;
    coalesceWindowMs: number;
    mode: "idle" | "fine" | "restricted" | "coarseOnly";
    hotFineKeys: number;
    hotTemplates: number;
    hotFineKeysActive: number;
    hotFineKeysGrace: number;
    hotTemplatesActive: number;
    hotTemplatesGrace: number;
    fineWaitersActive: number;
    coarseWaitersActive: number;
    broadFineWaitersActive: number;
    touchesEmitted: number;
    uniqueKeysTouched: number;
    tableTouchesEmitted: number;
    templateTouchesEmitted: number;
    staleResponses: number;
    fineTouchesDroppedDueToBudget: number;
    fineTouchesSkippedColdTemplate: number;
    fineTouchesSkippedColdKey: number;
    fineTouchesSkippedTemplateBucket: number;
    fineTouchesSuppressedBatchesDueToLag: number;
    fineTouchesSuppressedMsDueToLag: number;
    fineTouchesSuppressedBatchesDueToBudget: number;
  };
  gc: {
    baseWalGcCalls: number;
    baseWalGcDeletedRows: number;
    baseWalGcDeletedBytes: number;
    baseWalGcMsSum: number;
    baseWalGcMsMax: number;
  };
  templates: {
    activated: number;
    retired: number;
    evicted: number;
    activationDenied: number;
  };
  wait: {
    calls: number;
    keysWatchedTotal: number;
    touched: number;
    timeout: number;
    stale: number;
    latencySumMs: number;
    latencyHist: LatencyHistogram;
  };
  interpreter: {
    eventsIn: number;
    changesOut: number;
    errors: number;
    lagSourceOffsets: number;
    scannedBatches: number;
    scannedButEmitted0Batches: number;
    noInterestFastForwardBatches: number;
    interpretedThroughDelta: number;
    touchesEmittedDelta: number;
    commitLagSamples: number;
    commitLagMsSum: number;
    commitLagHist: LatencyHistogram;
  };
};

function defaultCounters(touchCfg: TouchConfig): StreamCounters {
  return {
    touch: {
      coarseIntervalMs: touchCfg.coarseIntervalMs ?? 100,
      coalesceWindowMs: touchCfg.touchCoalesceWindowMs ?? 100,
      mode: "idle",
      hotFineKeys: 0,
      hotTemplates: 0,
      hotFineKeysActive: 0,
      hotFineKeysGrace: 0,
      hotTemplatesActive: 0,
      hotTemplatesGrace: 0,
      fineWaitersActive: 0,
      coarseWaitersActive: 0,
      broadFineWaitersActive: 0,
      touchesEmitted: 0,
      uniqueKeysTouched: 0,
      tableTouchesEmitted: 0,
      templateTouchesEmitted: 0,
      staleResponses: 0,
      fineTouchesDroppedDueToBudget: 0,
      fineTouchesSkippedColdTemplate: 0,
      fineTouchesSkippedColdKey: 0,
      fineTouchesSkippedTemplateBucket: 0,
      fineTouchesSuppressedBatchesDueToLag: 0,
      fineTouchesSuppressedMsDueToLag: 0,
      fineTouchesSuppressedBatchesDueToBudget: 0,
    },
    gc: {
      baseWalGcCalls: 0,
      baseWalGcDeletedRows: 0,
      baseWalGcDeletedBytes: 0,
      baseWalGcMsSum: 0,
      baseWalGcMsMax: 0,
    },
    templates: { activated: 0, retired: 0, evicted: 0, activationDenied: 0 },
    wait: { calls: 0, keysWatchedTotal: 0, touched: 0, timeout: 0, stale: 0, latencySumMs: 0, latencyHist: makeLatencyHistogram() },
    interpreter: {
      eventsIn: 0,
      changesOut: 0,
      errors: 0,
      lagSourceOffsets: 0,
      scannedBatches: 0,
      scannedButEmitted0Batches: 0,
      noInterestFastForwardBatches: 0,
      interpretedThroughDelta: 0,
      touchesEmittedDelta: 0,
      commitLagSamples: 0,
      commitLagMsSum: 0,
      commitLagHist: makeLatencyHistogram(),
    },
  };
}

export class LiveMetricsV2 {
  private readonly db: SqliteDurableStore;
  private readonly ingest: IngestQueue;
  private readonly metricsStream: string;
  private readonly enabled: boolean;
  private readonly intervalMs: number;
  private readonly snapshotIntervalMs: number;
  private readonly snapshotChunkSize: number;
  private readonly retentionMs: number;
  private readonly getTouchJournal?: (stream: string) => { meta: TouchJournalMeta; interval: TouchJournalIntervalStats } | null;
  private timer: any | null = null;
  private snapshotTimer: any | null = null;
  private retentionTimer: any | null = null;
  private lagTimer: any | null = null;

  private readonly instanceId = getInstanceId();
  private readonly region = getRegion();

  private readonly counters = new Map<string, StreamCounters>();

  private lagExpectedMs = 0;
  private lagMaxMs = 0;
  private lagSumMs = 0;
  private lagSamples = 0;

  constructor(
    db: SqliteDurableStore,
    ingest: IngestQueue,
    opts?: {
      enabled?: boolean;
      stream?: string;
      intervalMs?: number;
      snapshotIntervalMs?: number;
      snapshotChunkSize?: number;
      retentionMs?: number;
      getTouchJournal?: (stream: string) => { meta: TouchJournalMeta; interval: TouchJournalIntervalStats } | null;
    }
  ) {
    this.db = db;
    this.ingest = ingest;
    this.enabled = opts?.enabled !== false;
    this.metricsStream = opts?.stream ?? "live.metrics";
    this.intervalMs = opts?.intervalMs ?? 1000;
    this.snapshotIntervalMs = opts?.snapshotIntervalMs ?? 60_000;
    this.snapshotChunkSize = opts?.snapshotChunkSize ?? 200;
    this.retentionMs = opts?.retentionMs ?? 7 * 24 * 60 * 60 * 1000;
    this.getTouchJournal = opts?.getTouchJournal;
  }

  start(): void {
    if (!this.enabled) return;
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.flushTick();
    }, this.intervalMs);
    this.snapshotTimer = setInterval(() => {
      void this.emitSnapshots();
    }, this.snapshotIntervalMs);
    // Retention trims are best-effort; 60s granularity is fine.
    this.retentionTimer = setInterval(() => {
      try {
        this.db.trimWalByAge(this.metricsStream, this.retentionMs);
      } catch {
        // ignore
      }
    }, 60_000);

    // Track event-loop lag at a tighter cadence than the tick interval to
    // debug cases where timeouts/fire events are delayed under load.
    const lagIntervalMs = 100;
    this.lagExpectedMs = Date.now() + lagIntervalMs;
    this.lagMaxMs = 0;
    this.lagSumMs = 0;
    this.lagSamples = 0;
    this.lagTimer = setInterval(() => {
      const now = Date.now();
      const lag = Math.max(0, now - this.lagExpectedMs);
      this.lagMaxMs = Math.max(this.lagMaxMs, lag);
      this.lagSumMs += lag;
      this.lagSamples += 1;
      this.lagExpectedMs += lagIntervalMs;
      // If the loop was paused for a long time, avoid building up a huge debt.
      if (this.lagExpectedMs < now - 5 * lagIntervalMs) this.lagExpectedMs = now + lagIntervalMs;
    }, lagIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    if (this.retentionTimer) clearInterval(this.retentionTimer);
    if (this.lagTimer) clearInterval(this.lagTimer);
    this.timer = null;
    this.snapshotTimer = null;
    this.retentionTimer = null;
    this.lagTimer = null;
  }

  ensureStreamResult(): Result<void, EnsureLiveMetricsStreamError> {
    if (!this.enabled) return Result.ok(undefined);
    const existing = this.db.getStream(this.metricsStream);
    if (existing) {
      if (String(existing.content_type) !== "application/json") {
        return Result.err({
          kind: "live_metrics_stream_content_type_mismatch",
          message: `live metrics stream content-type mismatch: ${existing.content_type}`,
        });
      }
      if ((existing.stream_flags & STREAM_FLAG_TOUCH) === 0) this.db.addStreamFlags(this.metricsStream, STREAM_FLAG_TOUCH);
      return Result.ok(undefined);
    }
    // Treat live.metrics as WAL-only (like touch streams) so age-based retention
    // is enforceable without segment/object-store GC.
    this.db.ensureStream(this.metricsStream, { contentType: "application/json", streamFlags: STREAM_FLAG_TOUCH });
    return Result.ok(undefined);
  }

  private get(stream: string, touchCfg: TouchConfig): StreamCounters {
    const existing = this.counters.get(stream);
    if (existing) return existing;
    const c = defaultCounters(touchCfg);
    this.counters.set(stream, c);
    return c;
  }

  private ensure(stream: string): StreamCounters {
    const existing = this.counters.get(stream);
    if (existing) return existing;
    // Use defaults; actual config values will be filled in when we observe the stream.
    const c = defaultCounters({ enabled: true } as TouchConfig);
    this.counters.set(stream, c);
    return c;
  }

  recordInterpreterError(stream: string, touchCfg: TouchConfig): void {
    const c = this.get(stream, touchCfg);
    c.interpreter.errors += 1;
  }

  recordInterpreterBatch(args: {
    stream: string;
    touchCfg: TouchConfig;
    rowsRead: number;
    changes: number;
    touches: Array<{ keyId: number; kind: TouchKind }>;
    lagSourceOffsets: number;
    touchMode: "idle" | "fine" | "restricted" | "coarseOnly";
    hotFineKeys?: number;
    hotTemplates?: number;
    hotFineKeysActive?: number;
    hotFineKeysGrace?: number;
    hotTemplatesActive?: number;
    hotTemplatesGrace?: number;
    fineWaitersActive?: number;
    coarseWaitersActive?: number;
    broadFineWaitersActive?: number;
    commitLagMs?: number;
    fineTouchesDroppedDueToBudget?: number;
    fineTouchesSkippedColdTemplate?: number;
    fineTouchesSkippedColdKey?: number;
    fineTouchesSkippedTemplateBucket?: number;
    fineTouchesSuppressedDueToLag?: boolean;
    fineTouchesSuppressedDueToLagMs?: number;
    fineTouchesSuppressedDueToBudget?: boolean;
    scannedButEmitted0?: boolean;
    noInterestFastForward?: boolean;
    interpretedThroughDelta?: number;
    touchesEmittedDelta?: number;
  }): void {
    const c = this.get(args.stream, args.touchCfg);
    c.touch.coarseIntervalMs = args.touchCfg.coarseIntervalMs ?? c.touch.coarseIntervalMs;
    c.touch.coalesceWindowMs = args.touchCfg.touchCoalesceWindowMs ?? c.touch.coalesceWindowMs;
    c.touch.mode = args.touchMode;
    c.touch.hotFineKeys = Math.max(c.touch.hotFineKeys, Math.max(0, Math.floor(args.hotFineKeys ?? 0)));
    c.touch.hotTemplates = Math.max(c.touch.hotTemplates, Math.max(0, Math.floor(args.hotTemplates ?? 0)));
    c.touch.hotFineKeysActive = Math.max(c.touch.hotFineKeysActive, Math.max(0, Math.floor(args.hotFineKeysActive ?? 0)));
    c.touch.hotFineKeysGrace = Math.max(c.touch.hotFineKeysGrace, Math.max(0, Math.floor(args.hotFineKeysGrace ?? 0)));
    c.touch.hotTemplatesActive = Math.max(c.touch.hotTemplatesActive, Math.max(0, Math.floor(args.hotTemplatesActive ?? 0)));
    c.touch.hotTemplatesGrace = Math.max(c.touch.hotTemplatesGrace, Math.max(0, Math.floor(args.hotTemplatesGrace ?? 0)));
    c.touch.fineWaitersActive = Math.max(c.touch.fineWaitersActive, Math.max(0, Math.floor(args.fineWaitersActive ?? 0)));
    c.touch.coarseWaitersActive = Math.max(c.touch.coarseWaitersActive, Math.max(0, Math.floor(args.coarseWaitersActive ?? 0)));
    c.touch.broadFineWaitersActive = Math.max(c.touch.broadFineWaitersActive, Math.max(0, Math.floor(args.broadFineWaitersActive ?? 0)));
    c.interpreter.eventsIn += Math.max(0, args.rowsRead);
    c.interpreter.changesOut += Math.max(0, args.changes);
    c.interpreter.lagSourceOffsets = Math.max(c.interpreter.lagSourceOffsets, Math.max(0, args.lagSourceOffsets));
    c.interpreter.scannedBatches += 1;
    if (args.scannedButEmitted0) c.interpreter.scannedButEmitted0Batches += 1;
    if (args.noInterestFastForward) c.interpreter.noInterestFastForwardBatches += 1;
    c.interpreter.interpretedThroughDelta += Math.max(0, Math.floor(args.interpretedThroughDelta ?? 0));
    c.interpreter.touchesEmittedDelta += Math.max(0, Math.floor(args.touchesEmittedDelta ?? 0));
    if (args.commitLagMs != null && Number.isFinite(args.commitLagMs) && args.commitLagMs >= 0) {
      c.interpreter.commitLagSamples += 1;
      c.interpreter.commitLagMsSum += args.commitLagMs;
      c.interpreter.commitLagHist.record(args.commitLagMs);
    }
    c.touch.fineTouchesDroppedDueToBudget += Math.max(0, args.fineTouchesDroppedDueToBudget ?? 0);
    c.touch.fineTouchesSkippedColdTemplate += Math.max(0, args.fineTouchesSkippedColdTemplate ?? 0);
    c.touch.fineTouchesSkippedColdKey += Math.max(0, args.fineTouchesSkippedColdKey ?? 0);
    c.touch.fineTouchesSkippedTemplateBucket += Math.max(0, args.fineTouchesSkippedTemplateBucket ?? 0);
    if (args.fineTouchesSuppressedDueToLag) c.touch.fineTouchesSuppressedBatchesDueToLag += 1;
    c.touch.fineTouchesSuppressedMsDueToLag += Math.max(0, args.fineTouchesSuppressedDueToLagMs ?? 0);
    if (args.fineTouchesSuppressedDueToBudget) c.touch.fineTouchesSuppressedBatchesDueToBudget += 1;

    const unique = new Set<number>();
    let table = 0;
    let tpl = 0;
    for (const t of args.touches) {
      unique.add(t.keyId >>> 0);
      if (t.kind === "table") table++;
      else tpl++;
    }
    c.touch.touchesEmitted += args.touches.length;
    c.touch.uniqueKeysTouched += unique.size;
    c.touch.tableTouchesEmitted += table;
    c.touch.templateTouchesEmitted += tpl;
  }

  recordWait(stream: string, touchCfg: TouchConfig, keysCount: number, outcome: WaitOutcome, latencyMs: number): void {
    const c = this.get(stream, touchCfg);
    c.wait.calls += 1;
    c.wait.keysWatchedTotal += Math.max(0, keysCount);
    c.wait.latencySumMs += Math.max(0, latencyMs);
    c.wait.latencyHist.record(latencyMs);
    if (outcome === "touched") c.wait.touched += 1;
    else if (outcome === "timeout") c.wait.timeout += 1;
    else c.wait.stale += 1;
    if (outcome === "stale") c.touch.staleResponses += 1;
  }

  recordBaseWalGc(stream: string, args: { deletedRows: number; deletedBytes: number; durationMs: number }): void {
    const c = this.ensure(stream);
    c.gc.baseWalGcCalls += 1;
    c.gc.baseWalGcDeletedRows += Math.max(0, args.deletedRows);
    c.gc.baseWalGcDeletedBytes += Math.max(0, args.deletedBytes);
    c.gc.baseWalGcMsSum += Math.max(0, args.durationMs);
    c.gc.baseWalGcMsMax = Math.max(c.gc.baseWalGcMsMax, Math.max(0, args.durationMs));
  }

  async emitLifecycle(events: TemplateLifecycleEvent[]): Promise<void> {
    if (!this.enabled) return;
    if (events.length === 0) return;

    const rows = events.map((e) => ({
      routingKey: new TextEncoder().encode(`${e.stream}|${e.type}`),
      contentType: "application/json",
      payload: new TextEncoder().encode(
        JSON.stringify({
          ...e,
          liveSystemVersion: "v2",
          instanceId: this.instanceId,
          region: this.region,
        })
      ),
    }));

    for (const e of events) {
      const c = this.ensure(e.stream);
      if (e.type === "live.template_activated") c.templates.activated += 1;
      else if (e.type === "live.template_retired") c.templates.retired += 1;
      else if (e.type === "live.template_evicted") c.templates.evicted += 1;
    }

    try {
      await this.ingest.appendInternal({
        stream: this.metricsStream,
        baseAppendMs: BigInt(Date.now()),
        rows,
        contentType: "application/json",
      });
    } catch {
      // best-effort
    }
  }

  recordActivationDenied(stream: string, touchCfg: TouchConfig, n = 1): void {
    const c = this.get(stream, touchCfg);
    c.templates.activationDenied += Math.max(0, n);
  }

  private async flushTick(): Promise<void> {
    if (!this.enabled) return;
    const nowMs = Date.now();
    const clampBigInt = (v: bigint): number => {
      if (v <= 0n) return 0;
      const max = BigInt(Number.MAX_SAFE_INTEGER);
      return v > max ? Number.MAX_SAFE_INTEGER : Number(v);
    };

    const states = this.db.listStreamInterpreters();
    if (states.length === 0) return;

    const rows: Array<{ routingKey: Uint8Array | null; contentType: string; payload: Uint8Array }> = [];
    const encoder = new TextEncoder();

    const loopLagMax = this.lagMaxMs;
    const loopLagAvg = this.lagSamples > 0 ? this.lagSumMs / this.lagSamples : 0;
    this.lagMaxMs = 0;
    this.lagSumMs = 0;
    this.lagSamples = 0;

    for (const st of states) {
      const stream = st.stream;
      const regRow = this.db.getStream(stream);
      if (!regRow) continue;

      const touchCfg = ((): TouchConfig | null => {
        try {
          const row = this.db.getSchemaRegistry(stream);
          if (!row) return null;
          const raw = JSON.parse(row.registry_json);
          const cfg = raw?.interpreter?.touch;
          if (!cfg || !cfg.enabled) return null;
          return cfg as TouchConfig;
        } catch {
          return null;
        }
      })();
      if (!touchCfg) continue;

      const c = this.get(stream, touchCfg);
      const journal = this.getTouchJournal?.(stream) ?? null;
      const waitActive = journal?.meta.activeWaiters ?? 0;
      const tailSeq = regRow.next_offset > 0n ? regRow.next_offset - 1n : -1n;
      const interpretedThrough = st.interpreted_through;
      const gcThrough = interpretedThrough < regRow.uploaded_through ? interpretedThrough : regRow.uploaded_through;
      const backlog = tailSeq >= interpretedThrough ? tailSeq - interpretedThrough : 0n;
      const backlogNum = backlog > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(backlog);
      let walOldestOffset: string | null = null;
      try {
        const oldest = this.db.getWalOldestOffset(stream);
        walOldestOffset = oldest == null ? null : encodeOffset(regRow.epoch, oldest);
      } catch {
        walOldestOffset = null;
      }
      const activeTemplates = (() => {
        try {
          const row = this.db.db.query(`SELECT COUNT(*) as cnt FROM live_templates WHERE stream=? AND state='active';`).get(stream) as any;
          return Number(row?.cnt ?? 0);
        } catch {
          return 0;
        }
      })();

      const tick = {
        type: "live.tick",
        ts: nowIso(nowMs),
        stream,
        liveSystemVersion: "v2",
        instanceId: this.instanceId,
        region: this.region,
        touch: {
          coarseIntervalMs: c.touch.coarseIntervalMs,
          coalesceWindowMs: c.touch.coalesceWindowMs,
          mode: c.touch.mode,
          hotFineKeys: c.touch.hotFineKeys,
          hotTemplates: c.touch.hotTemplates,
          hotFineKeysActive: c.touch.hotFineKeysActive,
          hotFineKeysGrace: c.touch.hotFineKeysGrace,
          hotTemplatesActive: c.touch.hotTemplatesActive,
          hotTemplatesGrace: c.touch.hotTemplatesGrace,
          fineWaitersActive: c.touch.fineWaitersActive,
          coarseWaitersActive: c.touch.coarseWaitersActive,
          broadFineWaitersActive: c.touch.broadFineWaitersActive,
          touchesEmitted: c.touch.touchesEmitted,
          uniqueKeysTouched: c.touch.uniqueKeysTouched,
          tableTouchesEmitted: c.touch.tableTouchesEmitted,
          templateTouchesEmitted: c.touch.templateTouchesEmitted,
          staleResponses: c.touch.staleResponses,
          fineTouchesDroppedDueToBudget: c.touch.fineTouchesDroppedDueToBudget,
          fineTouchesSkippedColdTemplate: c.touch.fineTouchesSkippedColdTemplate,
          fineTouchesSkippedColdKey: c.touch.fineTouchesSkippedColdKey,
          fineTouchesSkippedTemplateBucket: c.touch.fineTouchesSkippedTemplateBucket,
          fineTouchesSuppressedBatchesDueToLag: c.touch.fineTouchesSuppressedBatchesDueToLag,
          fineTouchesSuppressedSecondsDueToLag: c.touch.fineTouchesSuppressedMsDueToLag / 1000,
          fineTouchesSuppressedBatchesDueToBudget: c.touch.fineTouchesSuppressedBatchesDueToBudget,
          cursor: journal?.meta.cursor ?? null,
          epoch: journal?.meta.epoch ?? null,
          generation: journal?.meta.generation ?? null,
          pendingKeys: journal?.meta.pendingKeys ?? 0,
          overflowBuckets: journal?.meta.overflowBuckets ?? 0,
        },
        templates: {
          active: activeTemplates,
          activated: c.templates.activated,
          retired: c.templates.retired,
          evicted: c.templates.evicted,
          activationDenied: c.templates.activationDenied,
        },
        wait: {
          calls: c.wait.calls,
          keysWatchedTotal: c.wait.keysWatchedTotal,
          avgKeysPerCall: c.wait.calls > 0 ? c.wait.keysWatchedTotal / c.wait.calls : 0,
          touched: c.wait.touched,
          timeout: c.wait.timeout,
          stale: c.wait.stale,
          avgLatencyMs: c.wait.calls > 0 ? c.wait.latencySumMs / c.wait.calls : 0,
          p95LatencyMs: c.wait.latencyHist.p95(),
          activeWaiters: waitActive,
          timeoutsFired: journal?.interval.timeoutsFired ?? 0,
          timeoutSweeps: journal?.interval.timeoutSweeps ?? 0,
          timeoutSweepMsSum: journal?.interval.timeoutSweepMsSum ?? 0,
          timeoutSweepMsMax: journal?.interval.timeoutSweepMsMax ?? 0,
          notifyWakeups: journal?.interval.notifyWakeups ?? 0,
          notifyFlushes: journal?.interval.notifyFlushes ?? 0,
          notifyWakeMsSum: journal?.interval.notifyWakeMsSum ?? 0,
          notifyWakeMsMax: journal?.interval.notifyWakeMsMax ?? 0,
          timeoutHeapSize: journal?.interval.heapSize ?? 0,
        },
        interpreter: {
          eventsIn: c.interpreter.eventsIn,
          changesOut: c.interpreter.changesOut,
          errors: c.interpreter.errors,
          lagSourceOffsets: c.interpreter.lagSourceOffsets,
          scannedBatches: c.interpreter.scannedBatches,
          scannedButEmitted0Batches: c.interpreter.scannedButEmitted0Batches,
          noInterestFastForwardBatches: c.interpreter.noInterestFastForwardBatches,
          interpretedThroughDelta: c.interpreter.interpretedThroughDelta,
          touchesEmittedDelta: c.interpreter.touchesEmittedDelta,
          commitLagMsAvg: c.interpreter.commitLagSamples > 0 ? c.interpreter.commitLagMsSum / c.interpreter.commitLagSamples : 0,
          commitLagMsP50: c.interpreter.commitLagHist.p50(),
          commitLagMsP95: c.interpreter.commitLagHist.p95(),
          commitLagMsP99: c.interpreter.commitLagHist.p99(),
        },
        base: {
          tailOffset: encodeOffset(regRow.epoch, tailSeq),
          nextOffset: encodeOffset(regRow.epoch, regRow.next_offset),
          sealedThrough: encodeOffset(regRow.epoch, regRow.sealed_through),
          uploadedThrough: encodeOffset(regRow.epoch, regRow.uploaded_through),
          interpretedThrough: encodeOffset(regRow.epoch, interpretedThrough),
          gcThrough: encodeOffset(regRow.epoch, gcThrough),
          walOldestOffset,
          walRetainedRows: clampBigInt(regRow.wal_rows),
          walRetainedBytes: clampBigInt(regRow.wal_bytes),
          gc: {
            calls: c.gc.baseWalGcCalls,
            deletedRows: c.gc.baseWalGcDeletedRows,
            deletedBytes: c.gc.baseWalGcDeletedBytes,
            msSum: c.gc.baseWalGcMsSum,
            msMax: c.gc.baseWalGcMsMax,
          },
          backlogSourceOffsets: backlogNum,
        },
        process: {
          eventLoopLagMsMax: loopLagMax,
          eventLoopLagMsAvg: loopLagAvg,
        },
      };

      rows.push({
        routingKey: encoder.encode(`${stream}|live.tick`),
        contentType: "application/json",
        payload: encoder.encode(JSON.stringify(tick)),
      });

      // Reset interval counters (keep config).
      c.touch.hotFineKeys = 0;
      c.touch.hotTemplates = 0;
      c.touch.hotFineKeysActive = 0;
      c.touch.hotFineKeysGrace = 0;
      c.touch.hotTemplatesActive = 0;
      c.touch.hotTemplatesGrace = 0;
      c.touch.fineWaitersActive = 0;
      c.touch.coarseWaitersActive = 0;
      c.touch.broadFineWaitersActive = 0;
      c.touch.touchesEmitted = 0;
      c.touch.uniqueKeysTouched = 0;
      c.touch.tableTouchesEmitted = 0;
      c.touch.templateTouchesEmitted = 0;
      c.touch.staleResponses = 0;
      c.touch.fineTouchesDroppedDueToBudget = 0;
      c.touch.fineTouchesSkippedColdTemplate = 0;
      c.touch.fineTouchesSkippedColdKey = 0;
      c.touch.fineTouchesSkippedTemplateBucket = 0;
      c.touch.fineTouchesSuppressedBatchesDueToLag = 0;
      c.touch.fineTouchesSuppressedMsDueToLag = 0;
      c.touch.fineTouchesSuppressedBatchesDueToBudget = 0;
      c.touch.mode = "idle";
      c.templates.activated = 0;
      c.templates.retired = 0;
      c.templates.evicted = 0;
      c.templates.activationDenied = 0;
      c.wait.calls = 0;
      c.wait.keysWatchedTotal = 0;
      c.wait.touched = 0;
      c.wait.timeout = 0;
      c.wait.stale = 0;
      c.wait.latencySumMs = 0;
      c.wait.latencyHist.reset();
      c.interpreter.eventsIn = 0;
      c.interpreter.changesOut = 0;
      c.interpreter.errors = 0;
      c.interpreter.lagSourceOffsets = 0;
      c.interpreter.scannedBatches = 0;
      c.interpreter.scannedButEmitted0Batches = 0;
      c.interpreter.noInterestFastForwardBatches = 0;
      c.interpreter.interpretedThroughDelta = 0;
      c.interpreter.touchesEmittedDelta = 0;
      c.interpreter.commitLagSamples = 0;
      c.interpreter.commitLagMsSum = 0;
      c.interpreter.commitLagHist.reset();
      c.gc.baseWalGcCalls = 0;
      c.gc.baseWalGcDeletedRows = 0;
      c.gc.baseWalGcDeletedBytes = 0;
      c.gc.baseWalGcMsSum = 0;
      c.gc.baseWalGcMsMax = 0;
    }

    if (rows.length === 0) return;
    try {
      await this.ingest.appendInternal({
        stream: this.metricsStream,
        baseAppendMs: BigInt(nowMs),
        rows,
        contentType: "application/json",
      });
    } catch {
      // best-effort
    }
  }

  private async emitSnapshots(): Promise<void> {
    if (!this.enabled) return;
    const nowMs = Date.now();
    const streams = this.db.listStreamInterpreters().map((r) => r.stream);
    if (streams.length === 0) return;

    const encoder = new TextEncoder();
    const rows: Array<{ routingKey: Uint8Array | null; contentType: string; payload: Uint8Array }> = [];

    for (const stream of streams) {
      let templates: any[] = [];
      try {
        templates = this.db.db
          .query(
            `SELECT template_id, entity, fields_json, last_seen_at_ms, state
             FROM live_templates
             WHERE stream=? AND state='active'
             ORDER BY entity ASC, template_id ASC;`
          )
          .all(stream) as any[];
      } catch {
        continue;
      }

      const snapshotId = `s-${stream}-${nowMs}`;
      const activeTemplates = templates.length;
      rows.push({
        routingKey: encoder.encode(`${stream}|live.templates_snapshot_start`),
        contentType: "application/json",
        payload: encoder.encode(
          JSON.stringify({
            type: "live.templates_snapshot_start",
            ts: nowIso(nowMs),
            stream,
            liveSystemVersion: "v2",
            instanceId: this.instanceId,
            region: this.region,
            snapshotId,
            activeTemplates,
            chunkSize: this.snapshotChunkSize,
          })
        ),
      });

      let chunkIndex = 0;
      for (let i = 0; i < templates.length; i += this.snapshotChunkSize) {
        const slice = templates.slice(i, i + this.snapshotChunkSize);
        const payloadTemplates = slice.map((t) => {
          const templateId = String(t.template_id);
          const entity = String(t.entity);
          let fields: string[] = [];
          try {
            const f = JSON.parse(String(t.fields_json));
            if (Array.isArray(f)) fields = f.map(String);
          } catch {
            // ignore
          }
          const lastSeenAgoMs = Math.max(0, nowMs - Number(t.last_seen_at_ms));
          return { templateId, entity, fields, lastSeenAgoMs, state: "active" };
        });
        rows.push({
          routingKey: encoder.encode(`${stream}|live.templates_snapshot_chunk`),
          contentType: "application/json",
          payload: encoder.encode(
            JSON.stringify({
              type: "live.templates_snapshot_chunk",
              ts: nowIso(nowMs),
              stream,
              liveSystemVersion: "v2",
              instanceId: this.instanceId,
              region: this.region,
              snapshotId,
              chunkIndex,
              templates: payloadTemplates,
            })
          ),
        });
        chunkIndex++;
      }

      rows.push({
        routingKey: encoder.encode(`${stream}|live.templates_snapshot_end`),
        contentType: "application/json",
        payload: encoder.encode(
          JSON.stringify({
            type: "live.templates_snapshot_end",
            ts: nowIso(nowMs),
            stream,
            liveSystemVersion: "v2",
            instanceId: this.instanceId,
            region: this.region,
            snapshotId,
          })
        ),
      });
    }

    if (rows.length === 0) return;
    try {
      await this.ingest.appendInternal({
        stream: this.metricsStream,
        baseAppendMs: BigInt(nowMs),
        rows,
        contentType: "application/json",
      });
    } catch {
      // best-effort
    }
  }
}
