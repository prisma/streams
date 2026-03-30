import type { Config } from "../config";
import type { SqliteDurableStore } from "../db/db";
import type { IngestQueue } from "../ingest";
import type { StreamNotifier } from "../notifier";
import type { StreamProfileStore } from "../profiles";
import { listTouchCapableProfileKinds, resolveEnabledTouchCapability, resolveTouchCapability } from "../profiles";
import { TouchProcessorWorkerPool } from "./worker_pool";
import { LruCache } from "../util/lru";
import type { BackpressureGate } from "../backpressure";
import { LiveTemplateRegistry, type TemplateDecl } from "./live_templates";
import { LiveMetricsV2 } from "./live_metrics";
import type { TouchConfig } from "./spec";
import { TouchJournal } from "./touch_journal";
import { Result } from "better-result";

const BASE_WAL_GC_INTERVAL_MS = (() => {
  const raw = process.env.DS_BASE_WAL_GC_INTERVAL_MS;
  if (raw == null || raw.trim() === "") return 1000;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    // eslint-disable-next-line no-console
    console.error(`invalid DS_BASE_WAL_GC_INTERVAL_MS: ${raw}`);
    return 1000;
  }
  return Math.floor(n);
})();

const BASE_WAL_GC_CHUNK_OFFSETS = (() => {
  const raw = process.env.DS_BASE_WAL_GC_CHUNK_OFFSETS;
  if (raw == null || raw.trim() === "") return 100_000;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    // eslint-disable-next-line no-console
    console.error(`invalid DS_BASE_WAL_GC_CHUNK_OFFSETS: ${raw}`);
    return 100_000;
  }
  return Math.floor(n);
})();

type HotFineState = {
  keyActiveCountsById: Map<number, number>;
  keyGraceExpiryMsById: Map<number, number>;
  templateActiveCountsById: Map<string, number>;
  templateGraceExpiryMsById: Map<string, number>;
  fineWaitersActive: number;
  coarseWaitersActive: number;
  broadFineWaitersActive: number;
  nextSweepAtMs: number;
  keysOverCapacity: boolean;
  templatesOverCapacity: boolean;
};

type HotFineSnapshot = {
  hotTemplateIdsForWorker: string[] | null;
  hotKeyActiveSet: ReadonlyMap<number, number> | null;
  hotKeyGraceSet: ReadonlyMap<number, number> | null;
  hotTemplateActiveCount: number;
  hotTemplateGraceCount: number;
  hotKeyActiveCount: number;
  hotKeyGraceCount: number;
  hotTemplateCount: number;
  hotKeyCount: number;
  fineWaitersActive: number;
  coarseWaitersActive: number;
  broadFineWaitersActive: number;
  templateFilteringEnabled: boolean;
  keyFilteringEnabled: boolean;
};

const HOT_INTEREST_MAX_KEYS = 64;

type TouchRecord = {
  keyId: number;
  watermark: string;
  entity: string;
  kind: "table" | "template";
  templateId?: string;
};

type RestrictedTemplateBucketState = {
  bucketId: number;
  templateKeyIds: Set<number>;
};

type StreamRuntimeTotals = {
  scanRowsTotal: number;
  scanBatchesTotal: number;
  scannedButEmitted0BatchesTotal: number;
  processedThroughDeltaTotal: number;
  touchesEmittedTotal: number;
  touchesTableTotal: number;
  touchesTemplateTotal: number;
  fineTouchesDroppedDueToBudgetTotal: number;
  fineTouchesSkippedColdTemplateTotal: number;
  fineTouchesSkippedColdKeyTotal: number;
  fineTouchesSkippedTemplateBucketTotal: number;
  waitTouchedTotal: number;
  waitTimeoutTotal: number;
  waitStaleTotal: number;
};

export class TouchProcessorManager {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly profiles: StreamProfileStore;
  private readonly pool: TouchProcessorWorkerPool;
  private timer: any | null = null;
  private running = false;
  private stopping = false;
  private readonly dirty = new Set<string>();
  private readonly failures = new FailureTracker(1024);
  private readonly lastBaseWalGc = new LruCache<string, { atMs: number; through: bigint }>(1024);
  private readonly templates: LiveTemplateRegistry;
  private readonly liveMetrics: LiveMetricsV2;
  private readonly lastTemplateGcMsByStream = new LruCache<string, number>(1024);
  private readonly journals = new Map<string, TouchJournal>();
  private readonly fineLagCoarseOnlyByStream = new Map<string, boolean>();
  private readonly touchModeByStream = new Map<string, "idle" | "fine" | "restricted" | "coarseOnly">();
  private readonly fineTokenBucketsByStream = new Map<string, { tokens: number; lastRefillMs: number }>();
  private readonly hotFineByStream = new Map<string, HotFineState>();
  private readonly lagSourceOffsetsByStream = new Map<string, number>();
  private readonly restrictedTemplateBucketStateByStream = new Map<string, RestrictedTemplateBucketState>();
  private readonly runtimeTotalsByStream = new Map<string, StreamRuntimeTotals>();
  private readonly zeroRowBacklogStreakByStream = new Map<string, number>();
  private streamScanCursor = 0;
  private restartWorkerPoolRequested = false;
  private lastWorkerPoolRestartAtMs = 0;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    ingest: IngestQueue,
    notifier: StreamNotifier,
    profiles: StreamProfileStore,
    backpressure?: BackpressureGate
  ) {
    this.cfg = cfg;
    this.db = db;
    this.profiles = profiles;
    this.pool = new TouchProcessorWorkerPool(cfg, cfg.touchWorkers);
    this.templates = new LiveTemplateRegistry(db);
    this.liveMetrics = new LiveMetricsV2(db, ingest, profiles, {
      getTouchJournal: (stream) => {
        const j = this.journals.get(stream);
        if (!j) return null;
        return { meta: j.getMeta(), interval: j.snapshotAndResetIntervalStats() };
      },
      onAppended: ({ lastOffset, stream }) => {
        notifier.notify(stream, lastOffset);
        notifier.notifyDetailsChanged(stream);
      },
    });
  }

  start(): void {
    if (this.timer) return;
    this.stopping = false;
    this.seedTouchStateFromProfiles();
    const liveMetricsRes = this.liveMetrics.ensureStreamResult();
    if (Result.isError(liveMetricsRes)) {
      // eslint-disable-next-line no-console
      console.error("touch live metrics stream validation failed", liveMetricsRes.error.message);
    } else {
      this.liveMetrics.start();
    }
    if (this.cfg.touchCheckIntervalMs > 0) {
      this.timer = setInterval(() => {
        void this.tick();
      }, this.cfg.touchCheckIntervalMs);
    }
  }

  stop(): void {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.pool.stop();
    this.liveMetrics.stop();
    for (const j of this.journals.values()) j.stop();
    this.journals.clear();
    this.fineLagCoarseOnlyByStream.clear();
    this.touchModeByStream.clear();
    this.fineTokenBucketsByStream.clear();
    this.hotFineByStream.clear();
    this.lagSourceOffsetsByStream.clear();
    this.restrictedTemplateBucketStateByStream.clear();
    this.runtimeTotalsByStream.clear();
    this.zeroRowBacklogStreakByStream.clear();
    this.restartWorkerPoolRequested = false;
    this.lastWorkerPoolRestartAtMs = 0;
  }

  notify(stream: string): void {
    this.dirty.add(stream);
  }

  async tick(): Promise<void> {
    if (this.stopping) return;
    if (this.running) return;
    if (this.cfg.touchWorkers <= 0) return;
    this.running = true;
    try {
      const nowMs = Date.now();
      const dirtyNow = new Set(this.dirty);
      this.dirty.clear();
      const states = this.db.listStreamTouchStates();
      if (states.length === 0) return;
      this.pool.start();
      const stateByStream = new Map(states.map((s) => [s.stream, s]));

      const ordered: string[] = [];
      for (const s of dirtyNow) if (stateByStream.has(s)) ordered.push(s);
      for (const s of stateByStream.keys()) if (!dirtyNow.has(s)) ordered.push(s);
      const prioritized = this.prioritizeStreamsForProcessing(ordered, nowMs);

      const maxConcurrent = Math.max(1, this.cfg.touchWorkers);
      const tasks: Promise<void>[] = [];
      if (prioritized.length > 0) {
        const total = prioritized.length;
        const start = this.streamScanCursor % total;
        for (let i = 0; i < total && tasks.length < maxConcurrent; i++) {
          if (this.stopping) break;
          const stream = prioritized[(start + i) % total]!;
          if (this.failures.shouldSkip(stream)) continue;
          const st = stateByStream.get(stream);
          if (!st) continue;
          const p = this.processOne(stream, st.processed_through).catch((e) => {
            this.failures.recordFailure(stream);
            // eslint-disable-next-line no-console
            console.error("touch processor failed", stream, e);
          });
          tasks.push(p);
        }
        this.streamScanCursor = (start + Math.max(1, tasks.length)) % total;
      }
      await Promise.all(tasks);
      if (this.restartWorkerPoolRequested) {
        this.restartWorkerPoolRequested = false;
        try {
          this.pool.restart();
          this.lastWorkerPoolRestartAtMs = Date.now();
        } catch (e) {
          // eslint-disable-next-line no-console
          console.error("touch processor worker-pool restart failed", e);
        }
      }

      // Opportunistically GC base WAL beyond the touch-processing checkpoint.
      //
      // commitManifest() already GC's on upload, but it can't retroactively GC
      // rows that were held back by touch-processing lag once the processor later
      // catches up (unless another upload happens). This loop makes GC progress
      // deterministic for "catch up after lag" scenarios.
      for (const stream of stateByStream.keys()) {
        if (this.stopping) break;
        const srow = this.db.getStream(stream);
        if (!srow || this.db.isDeleted(srow)) continue;
        const touchState = this.db.getStreamTouchState(stream);
        if (!touchState) continue;
        this.maybeGcBaseWal(stream, srow.uploaded_through, touchState.processed_through);
      }

      // Template retirement GC + last-seen flush (sliding window).
      const touchCfgByStream = new Map<string, TouchConfig>();
      let persistIntervalMin = Number.POSITIVE_INFINITY;
      for (const stream of stateByStream.keys()) {
        if (this.stopping) break;
        const profileRes = this.profiles.getProfileResult(stream);
        if (Result.isError(profileRes)) {
          // eslint-disable-next-line no-console
          console.error("touch profile read failed", stream, profileRes.error.message);
          continue;
        }
        const profile = profileRes.value;
        const enabledTouch = resolveEnabledTouchCapability(profile);
        if (!enabledTouch) continue;
        const touchCfg = enabledTouch.touchCfg;
        touchCfgByStream.set(stream, touchCfg);
        const persistInterval = touchCfg.templates?.lastSeenPersistIntervalMs ?? 5 * 60 * 1000;
        if (persistInterval < persistIntervalMin) persistIntervalMin = persistInterval;
      }

      if (touchCfgByStream.size > 0 && Number.isFinite(persistIntervalMin)) {
        this.templates.flushLastSeen(nowMs, persistIntervalMin);
      }

      for (const [stream, touchCfg] of touchCfgByStream.entries()) {
        if (this.stopping) break;
        const gcInterval = touchCfg.templates?.gcIntervalMs ?? 60_000;
        const last = this.lastTemplateGcMsByStream.get(stream) ?? 0;
        if (nowMs - last < gcInterval) continue;
        this.lastTemplateGcMsByStream.set(stream, nowMs);

        const retired = this.templates.gcRetireExpired(stream, nowMs);
        if (retired.retired.length > 0) {
          void this.liveMetrics.emitLifecycle(retired.retired);
        }
      }
    } finally {
      this.running = false;
    }
  }

  private async processOne(stream: string, processedThroughAtStart: bigint): Promise<void> {
    const srow = this.db.getStream(stream);
    if (!srow || this.db.isDeleted(srow)) {
      this.db.deleteStreamTouchState(stream);
      return;
    }

    const next = srow.next_offset;
    if (next <= 0n) return;
    const fromOffset = processedThroughAtStart + 1n;
    const toOffset = next - 1n;
    if (fromOffset > toOffset) return;

    const profileRes = this.profiles.getProfileResult(stream, srow);
    if (Result.isError(profileRes)) {
      // eslint-disable-next-line no-console
      console.error("touch profile read failed", stream, profileRes.error.message);
      this.db.deleteStreamTouchState(stream);
      return;
    }
    const profile = profileRes.value;
    const enabledTouch = resolveEnabledTouchCapability(profile);
    if (!enabledTouch) {
      this.db.deleteStreamTouchState(stream);
      return;
    }
    const touchCfg = enabledTouch.touchCfg;
    const failProcessing = (message: string): void => {
      this.failures.recordFailure(stream);
      this.liveMetrics.recordProcessorError(stream, touchCfg);
      // eslint-disable-next-line no-console
      console.error("touch processor failed", stream, message);
    };

    const nowMs = Date.now();
    const hotFine = this.getHotFineSnapshot(stream, touchCfg, nowMs);
    const fineWaitersActive = hotFine?.fineWaitersActive ?? 0;
    const coarseWaitersActive = hotFine?.coarseWaitersActive ?? 0;
    const hasAnyWaiters = fineWaitersActive + coarseWaitersActive > 0;
    const hasFineDemand =
      fineWaitersActive > 0 || (hotFine?.broadFineWaitersActive ?? 0) > 0 || (hotFine?.hotKeyCount ?? 0) > 0 || (hotFine?.hotTemplateCount ?? 0) > 0;

    // Guardrail: when lag/backlog grows too large, temporarily suppress
    // fine/template touches (coarse table touches are still emitted).
    const lagAtStart = toOffset >= processedThroughAtStart ? toOffset - processedThroughAtStart : 0n;
    const suppressFineDueToLag = this.computeSuppressFineDueToLag(stream, touchCfg, lagAtStart, hasFineDemand);
    const j = this.getOrCreateJournal(stream, touchCfg);
    j.setCoalesceMs(this.computeAdaptiveCoalesceMs(touchCfg, lagAtStart, hasAnyWaiters));

    const fineBudgetPerBatch = Math.max(0, Math.floor(touchCfg.fineTouchBudgetPerBatch ?? 2000));
    const lagReservedFineBudgetPerBatch = Math.max(0, Math.floor(touchCfg.lagReservedFineTouchBudgetPerBatch ?? 200));
    let fineBudget = !hasFineDemand ? 0 : suppressFineDueToLag ? lagReservedFineBudgetPerBatch : fineBudgetPerBatch;
    let tokenLimited = false;
    let refundFineTokens: ((used: number) => void) | null = null;
    if (fineBudget > 0) {
      const tokenGrant = this.reserveFineTokens(stream, touchCfg, fineBudget, nowMs);
      fineBudget = tokenGrant.granted;
      tokenLimited = tokenGrant.tokenLimited;
      refundFineTokens = tokenGrant.refund;
    }

    let emitFineTouches = hasFineDemand && fineBudget > 0;
    let fineGranularity: "key" | "template" = "key";
    const batchStartMs = Date.now();
    if (
      emitFineTouches &&
      hotFine &&
      hotFine.hotKeyCount === 0 &&
      hotFine.hotTemplateCount === 0 &&
      hotFine.broadFineWaitersActive === 0 &&
      hotFine.keyFilteringEnabled &&
      !hotFine.templateFilteringEnabled
    ) {
      // No observed waiters/interests for fine keys/templates: coarse-only is cheaper.
      emitFineTouches = false;
    }
    if (emitFineTouches && suppressFineDueToLag) fineGranularity = "template";
    if (fineGranularity !== "template") {
      this.restrictedTemplateBucketStateByStream.delete(stream);
    }
    const processingMode: "full" | "hotTemplatesOnly" = fineGranularity === "template" ? "hotTemplatesOnly" : "full";
    const touchMode: "idle" | "fine" | "restricted" | "coarseOnly" = !hasAnyWaiters ? "idle" : emitFineTouches ? (suppressFineDueToLag ? "restricted" : "fine") : "coarseOnly";
    this.touchModeByStream.set(stream, touchMode);

    const processRes = await this.pool.processResult({
      stream,
      fromOffset,
      toOffset,
      profile,
      maxRows: Math.max(1, this.cfg.touchMaxBatchRows),
      maxBytes: Math.max(1, this.cfg.touchMaxBatchBytes),
      emitFineTouches,
      fineTouchBudget: emitFineTouches ? fineBudget : 0,
      fineGranularity,
      processingMode,
      filterHotTemplates: !!(hotFine && hotFine.templateFilteringEnabled),
      hotTemplateIds: hotFine?.hotTemplateIdsForWorker ?? null,
    });
    if (Result.isError(processRes)) {
      failProcessing(processRes.error.message);
      return;
    }
    const res = processRes.value;
    if (res.stats.rowsRead === 0 && toOffset >= fromOffset && this.rangeLikelyHasRows(stream, fromOffset, toOffset)) {
      const nextStreak = (this.zeroRowBacklogStreakByStream.get(stream) ?? 0) + 1;
      this.zeroRowBacklogStreakByStream.set(stream, nextStreak);
      if (nextStreak >= 5) {
        const now = Date.now();
        if (now - this.lastWorkerPoolRestartAtMs >= 30_000) {
          this.restartWorkerPoolRequested = true;
          // eslint-disable-next-line no-console
          console.error(
            "touch processor produced zero-row batch despite WAL backlog; scheduling worker-pool restart",
            stream,
            `from=${fromOffset.toString()}`,
            `to=${toOffset.toString()}`
          );
        }
      }
    } else {
      this.zeroRowBacklogStreakByStream.delete(stream);
    }
    if (refundFineTokens) {
      refundFineTokens(Math.max(0, res.stats.templateTouchesEmitted ?? 0));
    }
    const batchDurationMs = Math.max(0, Date.now() - batchStartMs);

    let touches = res.touches;
    const fineDroppedDueToBudget = Math.max(0, res.stats.fineTouchesDroppedDueToBudget ?? 0);
    let fineSkippedColdKey = 0;
    let fineSkippedTemplateBucket = 0;

    if (hotFine && hotFine.keyFilteringEnabled && fineGranularity !== "template") {
      const keyActiveSet = hotFine.hotKeyActiveSet;
      const keyGraceSet = hotFine.hotKeyGraceSet;
      const keyCount = (keyActiveSet?.size ?? 0) + (keyGraceSet?.size ?? 0);
      if (keyCount === 0) {
        for (const t of touches) if (t.kind === "template") fineSkippedColdKey += 1;
        touches = touches.filter((t) => t.kind === "table");
      } else {
        const filtered: typeof touches = [];
        for (const t of touches) {
          if (t.kind !== "template") {
            filtered.push(t);
            continue;
          }
          const keyId = t.keyId >>> 0;
          if ((keyActiveSet && keyActiveSet.has(keyId)) || (keyGraceSet && keyGraceSet.has(keyId))) {
            filtered.push(t);
          } else fineSkippedColdKey += 1;
        }
        touches = filtered;
      }
    }

    if (fineGranularity === "template" && touches.length > 0) {
      const coalesced = this.coalesceRestrictedTemplateTouches(stream, touchCfg, touches);
      touches = coalesced.touches;
      fineSkippedTemplateBucket = coalesced.dropped;
    }

    if (touches.length > 0) {
      const j = this.getOrCreateJournal(stream, touchCfg);
      for (const t of touches) {
        let sourceOffsetSeq: bigint | undefined;
        try {
          sourceOffsetSeq = BigInt(t.watermark);
        } catch {
          sourceOffsetSeq = undefined;
        }
        j.touch(t.keyId >>> 0, sourceOffsetSeq);
      }
    }

    // Live Query metrics are best-effort; do not affect processing.
    try {
      const lag = toOffset >= res.processedThrough ? toOffset - res.processedThrough : 0n;
      const lagNum = lag > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(lag);
      const effectiveLag = hasFineDemand ? lagNum : 0;
      this.lagSourceOffsetsByStream.set(stream, effectiveLag);
      const maxSourceTsMs = Number(res.stats.maxSourceTsMs ?? 0);
      const commitLagMs = maxSourceTsMs > 0 ? Math.max(0, Date.now() - maxSourceTsMs) : undefined;
      this.liveMetrics.recordProcessorBatch({
        stream,
        touchCfg,
        rowsRead: res.stats.rowsRead,
        changes: res.stats.changes,
        touches: touches.map((t) => ({ keyId: t.keyId >>> 0, kind: t.kind })),
        lagSourceOffsets: effectiveLag,
        commitLagMs,
        fineTouchesDroppedDueToBudget: fineDroppedDueToBudget,
        fineTouchesSkippedColdTemplate: Math.max(0, res.stats.fineTouchesSkippedColdTemplate ?? 0),
        fineTouchesSkippedColdKey: fineSkippedColdKey,
        fineTouchesSkippedTemplateBucket: fineSkippedTemplateBucket,
        fineTouchesSuppressedDueToLag: suppressFineDueToLag,
        fineTouchesSuppressedDueToLagMs: suppressFineDueToLag ? batchDurationMs : 0,
        fineTouchesSuppressedDueToBudget: !!res.stats.fineTouchesSuppressedDueToBudget || tokenLimited,
        touchMode,
        hotFineKeys: hotFine?.hotKeyCount ?? 0,
        hotTemplates: hotFine?.hotTemplateCount ?? 0,
        hotFineKeysActive: hotFine?.hotKeyActiveCount ?? 0,
        hotFineKeysGrace: hotFine?.hotKeyGraceCount ?? 0,
        hotTemplatesActive: hotFine?.hotTemplateActiveCount ?? 0,
        hotTemplatesGrace: hotFine?.hotTemplateGraceCount ?? 0,
        fineWaitersActive,
        coarseWaitersActive,
        broadFineWaitersActive: hotFine?.broadFineWaitersActive ?? 0,
        scannedButEmitted0: res.stats.rowsRead > 0 && touches.length === 0,
        noInterestFastForward: false,
        processedThroughDelta:
          res.processedThrough >= processedThroughAtStart
            ? Number(
                (res.processedThrough - processedThroughAtStart) > BigInt(Number.MAX_SAFE_INTEGER)
                  ? BigInt(Number.MAX_SAFE_INTEGER)
                  : res.processedThrough - processedThroughAtStart
              )
            : 0,
        touchesEmittedDelta: touches.length,
      });
    } catch {
      // ignore
    }

    const processedDelta =
      res.processedThrough >= processedThroughAtStart
        ? Number(
            (res.processedThrough - processedThroughAtStart) > BigInt(Number.MAX_SAFE_INTEGER)
              ? BigInt(Number.MAX_SAFE_INTEGER)
              : res.processedThrough - processedThroughAtStart
          )
        : 0;
    const totals = this.getOrCreateRuntimeTotals(stream);
    totals.scanBatchesTotal += 1;
    totals.scanRowsTotal += Math.max(0, res.stats.rowsRead);
    if (res.stats.rowsRead > 0 && touches.length === 0) totals.scannedButEmitted0BatchesTotal += 1;
    totals.processedThroughDeltaTotal += processedDelta;
    totals.touchesEmittedTotal += touches.length;
    let tableTouches = 0;
    let templateTouches = 0;
    for (const t of touches) {
      if (t.kind === "table") tableTouches += 1;
      else templateTouches += 1;
    }
    totals.touchesTableTotal += tableTouches;
    totals.touchesTemplateTotal += templateTouches;
    totals.fineTouchesDroppedDueToBudgetTotal += fineDroppedDueToBudget;
    totals.fineTouchesSkippedColdTemplateTotal += Math.max(0, res.stats.fineTouchesSkippedColdTemplate ?? 0);
    totals.fineTouchesSkippedColdKeyTotal += fineSkippedColdKey;
    totals.fineTouchesSkippedTemplateBucketTotal += fineSkippedTemplateBucket;

    this.db.updateStreamTouchStateThrough(stream, res.processedThrough);
    if (res.processedThrough < toOffset) this.dirty.add(stream);
    this.failures.recordSuccess(stream);
  }

  private maybeGcBaseWal(stream: string, uploadedThrough: bigint, processedThrough: bigint): void {
    const gcTargetThrough = processedThrough < uploadedThrough ? processedThrough : uploadedThrough;
    if (gcTargetThrough < 0n) return;

    const now = Date.now();
    const last = this.lastBaseWalGc.get(stream) ?? { atMs: 0, through: -1n };
    // Avoid doing heavy DELETE work too frequently.
    if (now - last.atMs < BASE_WAL_GC_INTERVAL_MS) return;
    if (gcTargetThrough <= last.through) {
      this.lastBaseWalGc.set(stream, { atMs: now, through: last.through });
      return;
    }

    // Chunk deletes to avoid long event-loop stalls on "catch up after lag" runs.
    const chunk = BigInt(BASE_WAL_GC_CHUNK_OFFSETS);
    const maxThroughThisSweep = chunk > 0n ? last.through + chunk : gcTargetThrough;
    const gcThrough = gcTargetThrough > maxThroughThisSweep ? maxThroughThisSweep : gcTargetThrough;

    try {
      const start = Date.now();
      const res = this.db.deleteWalThrough(stream, gcThrough);
      const durationMs = Date.now() - start;
      if (res.deletedRows > 0 || res.deletedBytes > 0) {
        this.liveMetrics.recordBaseWalGc(stream, { deletedRows: res.deletedRows, deletedBytes: res.deletedBytes, durationMs });
      }
      this.lastBaseWalGc.set(stream, { atMs: now, through: gcThrough });
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error("base WAL gc failed", stream, e);
      this.lastBaseWalGc.set(stream, { atMs: now, through: last.through });
    }
  }

  private seedTouchStateFromProfiles(): void {
    // Bootstrap support: bootstrapFromR2 restores profile state but does not
    // populate stream_touch_state. Seeding here makes touch processing start working
    // after bootstraps and restarts without requiring a no-op config update.
    try {
      for (const kind of listTouchCapableProfileKinds()) {
        const streams = this.db.listStreamsByProfile(kind);
        for (const stream of streams) {
          const profileRes = this.profiles.getProfileResult(stream);
          if (Result.isError(profileRes)) continue;
          const touchCapability = resolveTouchCapability(profileRes.value);
          if (!touchCapability) continue;
          touchCapability.syncState({ db: this.db, stream, profile: profileRes.value });
        }
      }
    } catch {
      // ignore
    }
  }

  activateTemplates(args: {
    stream: string;
    touchCfg: TouchConfig;
    baseStreamNextOffset: bigint;
    activeFromTouchOffset: string;
    templates: TemplateDecl[];
    inactivityTtlMs: number;
  }): { activated: Array<{ templateId: string; state: "active"; activeFromTouchOffset: string }>; denied: Array<{ templateId: string; reason: string }> } {
    const nowMs = Date.now();
    const limits = {
      maxActiveTemplatesPerStream: args.touchCfg.templates?.maxActiveTemplatesPerStream ?? 2048,
      maxActiveTemplatesPerEntity: args.touchCfg.templates?.maxActiveTemplatesPerEntity ?? 256,
      activationRateLimitPerMinute: args.touchCfg.templates?.activationRateLimitPerMinute ?? 100,
    };

    const res = this.templates.activate({
      stream: args.stream,
      activeFromTouchOffset: args.activeFromTouchOffset,
      baseStreamNextOffset: args.baseStreamNextOffset,
      templates: args.templates,
      inactivityTtlMs: args.inactivityTtlMs,
      limits,
      nowMs,
    });

    const deniedRate = res.denied.filter((d) => d.reason === "rate_limited").length;
    if (deniedRate > 0) this.liveMetrics.recordActivationDenied(args.stream, args.touchCfg, deniedRate);
    if (res.lifecycle.length > 0) void this.liveMetrics.emitLifecycle(res.lifecycle);

    return { activated: res.activated, denied: res.denied };
  }

  heartbeatTemplates(args: { stream: string; touchCfg: TouchConfig; templateIdsUsed: string[] }): void {
    const nowMs = Date.now();
    this.templates.heartbeat(args.stream, args.templateIdsUsed, nowMs);
    const persistInterval = args.touchCfg.templates?.lastSeenPersistIntervalMs ?? 5 * 60 * 1000;
    this.templates.flushLastSeen(nowMs, persistInterval);
  }

  beginHotWaitInterest(args: {
    stream: string;
    touchCfg: TouchConfig;
    keyIds: number[];
    templateIdsUsed: string[];
    interestMode: "fine" | "coarse";
  }): () => void {
    const nowMs = Date.now();
    const limits = this.getHotFineLimits(args.touchCfg);
    const state = this.getOrCreateHotFineState(args.stream);
    const isFine = args.interestMode === "fine";
    if (isFine) state.fineWaitersActive += 1;
    else state.coarseWaitersActive += 1;

    const trackedKeyIds: number[] = [];
    const trackedTemplateIds: string[] = [];
    const broad = isFine && args.keyIds.length > HOT_INTEREST_MAX_KEYS;

    if (!isFine) {
      // coarse waits intentionally do not contribute fine-hot key/template sets.
    } else if (broad) {
      state.broadFineWaitersActive += 1;
    } else {
      const uniqueKeys = new Set(args.keyIds.map((raw) => Number(raw) >>> 0));
      for (const keyId of uniqueKeys) {
        if (this.acquireHotKey(state, keyId, limits.maxKeys)) trackedKeyIds.push(keyId);
      }
    }

    if (isFine) {
      const uniqueTemplates = new Set<string>();
      for (const raw of args.templateIdsUsed) {
        const templateId = String(raw).trim();
        if (!/^[0-9a-f]{16}$/.test(templateId)) continue;
        uniqueTemplates.add(templateId);
      }
      for (const templateId of uniqueTemplates) {
        if (this.acquireHotTemplate(state, templateId, limits.maxTemplates)) trackedTemplateIds.push(templateId);
      }
    }

    this.sweepHotFineState(args.stream, args.touchCfg, nowMs, true);

    let released = false;
    return () => {
      if (released) return;
      released = true;
      const st = this.hotFineByStream.get(args.stream);
      if (!st) return;
      const releaseNowMs = Date.now();
      if (isFine) st.fineWaitersActive = Math.max(0, st.fineWaitersActive - 1);
      else st.coarseWaitersActive = Math.max(0, st.coarseWaitersActive - 1);
      if (broad) st.broadFineWaitersActive = Math.max(0, st.broadFineWaitersActive - 1);
      for (const keyId of trackedKeyIds) {
        this.releaseHotKey(st, keyId, releaseNowMs, limits.keyGraceMs, limits.maxKeys);
      }
      for (const templateId of trackedTemplateIds) {
        this.releaseHotTemplate(st, templateId, releaseNowMs, limits.templateGraceMs, limits.maxTemplates);
      }
      this.sweepHotFineState(args.stream, args.touchCfg, releaseNowMs, true);
    };
  }

  getTouchRuntimeSnapshot(args: { stream: string; touchCfg: TouchConfig }): {
    lagSourceOffsets: number;
    touchMode: "idle" | "fine" | "restricted" | "coarseOnly";
    hotFineKeys: number;
    hotTemplates: number;
    hotFineKeysActive: number;
    hotFineKeysGrace: number;
    hotTemplatesActive: number;
    hotTemplatesGrace: number;
    fineWaitersActive: number;
    coarseWaitersActive: number;
    broadFineWaitersActive: number;
    hotKeyFilteringEnabled: boolean;
    hotTemplateFilteringEnabled: boolean;
    scanRowsTotal: number;
    scanBatchesTotal: number;
    scannedButEmitted0BatchesTotal: number;
    processedThroughDeltaTotal: number;
    touchesEmittedTotal: number;
    touchesTableTotal: number;
    touchesTemplateTotal: number;
    fineTouchesDroppedDueToBudgetTotal: number;
    fineTouchesSkippedColdTemplateTotal: number;
    fineTouchesSkippedColdKeyTotal: number;
    fineTouchesSkippedTemplateBucketTotal: number;
    waitTouchedTotal: number;
    waitTimeoutTotal: number;
    waitStaleTotal: number;
    journalFlushesTotal: number;
    journalNotifyWakeupsTotal: number;
    journalNotifyWakeMsTotal: number;
    journalNotifyWakeMsMax: number;
    journalTimeoutsFiredTotal: number;
    journalTimeoutSweepMsTotal: number;
  } {
    const nowMs = Date.now();
    const hot = this.getHotFineSnapshot(args.stream, args.touchCfg, nowMs);
    const totals = this.getOrCreateRuntimeTotals(args.stream);
    const journal = this.journals.get(args.stream) ?? null;
    const journalTotals = journal?.getTotalStats();
    return {
      lagSourceOffsets: this.lagSourceOffsetsByStream.get(args.stream) ?? 0,
      touchMode: this.touchModeByStream.get(args.stream) ?? (this.fineLagCoarseOnlyByStream.get(args.stream) ? "coarseOnly" : "fine"),
      hotFineKeys: hot?.hotKeyCount ?? 0,
      hotTemplates: hot?.hotTemplateCount ?? 0,
      hotFineKeysActive: hot?.hotKeyActiveCount ?? 0,
      hotFineKeysGrace: hot?.hotKeyGraceCount ?? 0,
      hotTemplatesActive: hot?.hotTemplateActiveCount ?? 0,
      hotTemplatesGrace: hot?.hotTemplateGraceCount ?? 0,
      fineWaitersActive: hot?.fineWaitersActive ?? 0,
      coarseWaitersActive: hot?.coarseWaitersActive ?? 0,
      broadFineWaitersActive: hot?.broadFineWaitersActive ?? 0,
      hotKeyFilteringEnabled: hot?.keyFilteringEnabled ?? false,
      hotTemplateFilteringEnabled: hot?.templateFilteringEnabled ?? false,
      scanRowsTotal: totals.scanRowsTotal,
      scanBatchesTotal: totals.scanBatchesTotal,
      scannedButEmitted0BatchesTotal: totals.scannedButEmitted0BatchesTotal,
      processedThroughDeltaTotal: totals.processedThroughDeltaTotal,
      touchesEmittedTotal: totals.touchesEmittedTotal,
      touchesTableTotal: totals.touchesTableTotal,
      touchesTemplateTotal: totals.touchesTemplateTotal,
      fineTouchesDroppedDueToBudgetTotal: totals.fineTouchesDroppedDueToBudgetTotal,
      fineTouchesSkippedColdTemplateTotal: totals.fineTouchesSkippedColdTemplateTotal,
      fineTouchesSkippedColdKeyTotal: totals.fineTouchesSkippedColdKeyTotal,
      fineTouchesSkippedTemplateBucketTotal: totals.fineTouchesSkippedTemplateBucketTotal,
      waitTouchedTotal: totals.waitTouchedTotal,
      waitTimeoutTotal: totals.waitTimeoutTotal,
      waitStaleTotal: totals.waitStaleTotal,
      journalFlushesTotal: journalTotals?.flushes ?? 0,
      journalNotifyWakeupsTotal: journalTotals?.notifyWakeups ?? 0,
      journalNotifyWakeMsTotal: journalTotals?.notifyWakeMsSum ?? 0,
      journalNotifyWakeMsMax: journalTotals?.notifyWakeMsMax ?? 0,
      journalTimeoutsFiredTotal: journalTotals?.timeoutsFired ?? 0,
      journalTimeoutSweepMsTotal: journalTotals?.timeoutSweepMsSum ?? 0,
    };
  }

  recordWaitMetrics(args: { stream: string; touchCfg: TouchConfig; keysCount: number; outcome: "touched" | "timeout" | "stale"; latencyMs: number }): void {
    this.liveMetrics.recordWait(args.stream, args.touchCfg, args.keysCount, args.outcome, args.latencyMs);
    const totals = this.getOrCreateRuntimeTotals(args.stream);
    if (args.outcome === "touched") totals.waitTouchedTotal += 1;
    else if (args.outcome === "timeout") totals.waitTimeoutTotal += 1;
    else totals.waitStaleTotal += 1;
  }

  resolveTemplateEntitiesForWait(args: { stream: string; templateIdsUsed: string[] }): string[] {
    const ids = Array.from(
      new Set(args.templateIdsUsed.map((x) => String(x).trim()).filter((x) => /^[0-9a-f]{16}$/.test(x)))
    );
    if (ids.length === 0) return [];
    const entities = new Set<string>();
    const chunkSize = 200;
    for (let i = 0; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      const placeholders = chunk.map(() => "?").join(",");
      const rows = this.db.db
        .query(
          `SELECT DISTINCT entity
           FROM live_templates
           WHERE stream=? AND state='active' AND template_id IN (${placeholders});`
        )
        .all(args.stream, ...chunk) as any[];
      for (const row of rows) {
        const entity = String(row?.entity ?? "").trim();
        if (entity !== "") entities.add(entity);
      }
    }
    return Array.from(entities);
  }

  getOrCreateJournal(stream: string, touchCfg: TouchConfig): TouchJournal {
    const existing = this.journals.get(stream);
    if (existing) return existing;
    const mem = touchCfg.memory ?? {};
    const j = new TouchJournal({
      bucketMs: mem.bucketMs ?? 100,
      filterPow2: mem.filterPow2 ?? 22,
      k: mem.k ?? 4,
      pendingMaxKeys: mem.pendingMaxKeys ?? 100_000,
      keyIndexMaxKeys: mem.keyIndexMaxKeys ?? 32,
    });
    this.journals.set(stream, j);
    return j;
  }

  private computeAdaptiveCoalesceMs(touchCfg: TouchConfig, lagAtStart: bigint, hasAnyWaiters: boolean): number {
    const maxCoalesceMs = Math.max(1, Math.floor(touchCfg.memory?.bucketMs ?? 100));
    if (!hasAnyWaiters) return maxCoalesceMs;

    const lagNum = lagAtStart > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(lagAtStart);
    if (lagNum <= 0) return Math.min(maxCoalesceMs, 10);
    if (lagNum <= 5_000) return Math.min(maxCoalesceMs, 50);
    return maxCoalesceMs;
  }

  getJournalIfExists(stream: string): TouchJournal | null {
    return this.journals.get(stream) ?? null;
  }

  private computeSuppressFineDueToLag(stream: string, touchCfg: TouchConfig, lagAtStart: bigint, hasFineDemand: boolean): boolean {
    if (!hasFineDemand) {
      this.fineLagCoarseOnlyByStream.set(stream, false);
      return false;
    }
    const degradeRaw = Math.max(0, Math.floor(touchCfg.lagDegradeFineTouchesAtSourceOffsets ?? 5000));
    if (degradeRaw <= 0) {
      this.fineLagCoarseOnlyByStream.set(stream, false);
      return false;
    }
    const recoverRaw = Math.max(0, Math.floor(touchCfg.lagRecoverFineTouchesAtSourceOffsets ?? 1000));
    const recover = Math.min(degradeRaw, recoverRaw);
    const lag = lagAtStart > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(lagAtStart);
    const prev = this.fineLagCoarseOnlyByStream.get(stream) ?? false;
    let next = prev;
    if (!prev && lag >= degradeRaw) next = true;
    else if (prev && lag <= recover) next = false;
    this.fineLagCoarseOnlyByStream.set(stream, next);
    return next;
  }

  private prioritizeStreamsForProcessing(ordered: string[], nowMs: number): string[] {
    if (ordered.length <= 1) return ordered;
    const hot: string[] = [];
    const cold: string[] = [];
    for (const stream of ordered) {
      let hasActiveWaiters = false;
      const profileRes = this.profiles.getProfileResult(stream);
      if (Result.isError(profileRes)) {
        hasActiveWaiters = false;
      } else {
        const enabledTouch = resolveEnabledTouchCapability(profileRes.value);
        if (enabledTouch) {
          const snap = this.getHotFineSnapshot(stream, enabledTouch.touchCfg, nowMs);
          hasActiveWaiters = snap.fineWaitersActive + snap.coarseWaitersActive > 0;
        }
      }
      if (hasActiveWaiters) hot.push(stream);
      else cold.push(stream);
    }
    if (hot.length === 0) return ordered;
    return hot.concat(cold);
  }

  private coalesceRestrictedTemplateTouches(stream: string, touchCfg: TouchConfig, touches: TouchRecord[]): { touches: TouchRecord[]; dropped: number } {
    const bucketMs = Math.max(1, Math.floor(touchCfg.memory?.bucketMs ?? 100));
    const bucketId = Math.floor(Date.now() / bucketMs);
    let state = this.restrictedTemplateBucketStateByStream.get(stream);
    if (!state || state.bucketId !== bucketId) {
      state = { bucketId, templateKeyIds: new Set<number>() };
      this.restrictedTemplateBucketStateByStream.set(stream, state);
    }

    const out: TouchRecord[] = [];
    let dropped = 0;
    for (const touch of touches) {
      if (touch.kind !== "template") {
        out.push(touch);
        continue;
      }
      const keyId = touch.keyId >>> 0;
      if (state.templateKeyIds.has(keyId)) {
        dropped += 1;
        continue;
      }
      state.templateKeyIds.add(keyId);
      out.push(touch);
    }
    return { touches: out, dropped };
  }

  private getHotFineSnapshot(stream: string, touchCfg: TouchConfig, nowMs: number): HotFineSnapshot {
    const state = this.sweepHotFineState(stream, touchCfg, nowMs, false);
    if (!state) {
      return {
        hotTemplateIdsForWorker: null,
        hotKeyActiveSet: null,
        hotKeyGraceSet: null,
        hotTemplateActiveCount: 0,
        hotTemplateGraceCount: 0,
        hotKeyActiveCount: 0,
        hotKeyGraceCount: 0,
        hotTemplateCount: 0,
        hotKeyCount: 0,
        fineWaitersActive: 0,
        coarseWaitersActive: 0,
        broadFineWaitersActive: 0,
        templateFilteringEnabled: false,
        keyFilteringEnabled: true,
      };
    }

    const hotTemplateActiveCount = state.templateActiveCountsById.size;
    const hotTemplateGraceCount = state.templateGraceExpiryMsById.size;
    const hotKeyActiveCount = state.keyActiveCountsById.size;
    const hotKeyGraceCount = state.keyGraceExpiryMsById.size;
    const hotTemplateCount = hotTemplateActiveCount + hotTemplateGraceCount;
    const hotKeyCount = hotKeyActiveCount + hotKeyGraceCount;
    const templateFilteringEnabled = !state.templatesOverCapacity && hotTemplateCount > 0;
    const keyFilteringEnabled = !state.keysOverCapacity && state.broadFineWaitersActive === 0;
    const hotTemplateIdsForWorker =
      templateFilteringEnabled ? Array.from(new Set([...state.templateActiveCountsById.keys(), ...state.templateGraceExpiryMsById.keys()])) : null;

    return {
      hotTemplateIdsForWorker,
      hotKeyActiveSet: keyFilteringEnabled ? state.keyActiveCountsById : null,
      hotKeyGraceSet: keyFilteringEnabled ? state.keyGraceExpiryMsById : null,
      hotTemplateActiveCount,
      hotTemplateGraceCount,
      hotKeyActiveCount,
      hotKeyGraceCount,
      hotTemplateCount,
      hotKeyCount,
      fineWaitersActive: state.fineWaitersActive,
      coarseWaitersActive: state.coarseWaitersActive,
      broadFineWaitersActive: state.broadFineWaitersActive,
      templateFilteringEnabled,
      keyFilteringEnabled,
    };
  }

  private getOrCreateHotFineState(stream: string): HotFineState {
    const existing = this.hotFineByStream.get(stream);
    if (existing) return existing;
    const created: HotFineState = {
      keyActiveCountsById: new Map<number, number>(),
      keyGraceExpiryMsById: new Map<number, number>(),
      templateActiveCountsById: new Map<string, number>(),
      templateGraceExpiryMsById: new Map<string, number>(),
      fineWaitersActive: 0,
      coarseWaitersActive: 0,
      broadFineWaitersActive: 0,
      nextSweepAtMs: 0,
      keysOverCapacity: false,
      templatesOverCapacity: false,
    };
    this.hotFineByStream.set(stream, created);
    return created;
  }

  private sweepHotFineState(stream: string, touchCfg: TouchConfig, nowMs: number, force: boolean): HotFineState | null {
    const state = this.hotFineByStream.get(stream);
    if (!state) return null;
    if (!force && nowMs < state.nextSweepAtMs) return state;

    const limits = this.getHotFineLimits(touchCfg);

    for (const [k, exp] of state.keyGraceExpiryMsById.entries()) {
      if (exp <= nowMs) state.keyGraceExpiryMsById.delete(k);
    }
    for (const [tpl, exp] of state.templateGraceExpiryMsById.entries()) {
      if (exp <= nowMs) state.templateGraceExpiryMsById.delete(tpl);
    }

    if (state.keyActiveCountsById.size + state.keyGraceExpiryMsById.size < limits.maxKeys) state.keysOverCapacity = false;
    if (state.templateActiveCountsById.size + state.templateGraceExpiryMsById.size < limits.maxTemplates) state.templatesOverCapacity = false;

    if (
      state.keyActiveCountsById.size === 0 &&
      state.keyGraceExpiryMsById.size === 0 &&
      state.templateActiveCountsById.size === 0 &&
      state.templateGraceExpiryMsById.size === 0 &&
      state.fineWaitersActive <= 0 &&
      state.coarseWaitersActive <= 0 &&
      state.broadFineWaitersActive <= 0
    ) {
      this.hotFineByStream.delete(stream);
      return null;
    }

    const sweepEveryMs = Math.max(250, Math.min(limits.keyGraceMs, limits.templateGraceMs, 2000));
    state.nextSweepAtMs = nowMs + sweepEveryMs;
    return state;
  }

  private getHotFineLimits(touchCfg: TouchConfig): { keyGraceMs: number; templateGraceMs: number; maxKeys: number; maxTemplates: number } {
    const mem = touchCfg.memory ?? {};
    return {
      keyGraceMs: Math.max(1, Math.floor(mem.hotKeyTtlMs ?? 10_000)),
      templateGraceMs: Math.max(1, Math.floor(mem.hotTemplateTtlMs ?? 10_000)),
      maxKeys: Math.max(1, Math.floor(mem.hotMaxKeys ?? 1_000_000)),
      maxTemplates: Math.max(1, Math.floor(mem.hotMaxTemplates ?? 4096)),
    };
  }

  private acquireHotKey(state: HotFineState, keyId: number, maxKeys: number): boolean {
    const prev = state.keyActiveCountsById.get(keyId);
    if (prev != null) {
      state.keyActiveCountsById.set(keyId, prev + 1);
      state.keyGraceExpiryMsById.delete(keyId);
      return true;
    }
    if (state.keyActiveCountsById.size + state.keyGraceExpiryMsById.size >= maxKeys) {
      state.keysOverCapacity = true;
      return false;
    }
    state.keyActiveCountsById.set(keyId, 1);
    state.keyGraceExpiryMsById.delete(keyId);
    return true;
  }

  private acquireHotTemplate(state: HotFineState, templateId: string, maxTemplates: number): boolean {
    const prev = state.templateActiveCountsById.get(templateId);
    if (prev != null) {
      state.templateActiveCountsById.set(templateId, prev + 1);
      state.templateGraceExpiryMsById.delete(templateId);
      return true;
    }
    if (state.templateActiveCountsById.size + state.templateGraceExpiryMsById.size >= maxTemplates) {
      state.templatesOverCapacity = true;
      return false;
    }
    state.templateActiveCountsById.set(templateId, 1);
    state.templateGraceExpiryMsById.delete(templateId);
    return true;
  }

  private releaseHotKey(state: HotFineState, keyId: number, nowMs: number, keyGraceMs: number, maxKeys: number): void {
    const prev = state.keyActiveCountsById.get(keyId);
    if (prev == null) return;
    if (prev > 1) {
      state.keyActiveCountsById.set(keyId, prev - 1);
      return;
    }
    state.keyActiveCountsById.delete(keyId);
    if (keyGraceMs <= 0) {
      state.keyGraceExpiryMsById.delete(keyId);
      return;
    }
    if (state.keyActiveCountsById.size + state.keyGraceExpiryMsById.size >= maxKeys) {
      state.keysOverCapacity = true;
      return;
    }
    state.keyGraceExpiryMsById.set(keyId, nowMs + keyGraceMs);
  }

  private releaseHotTemplate(state: HotFineState, templateId: string, nowMs: number, templateGraceMs: number, maxTemplates: number): void {
    const prev = state.templateActiveCountsById.get(templateId);
    if (prev == null) return;
    if (prev > 1) {
      state.templateActiveCountsById.set(templateId, prev - 1);
      return;
    }
    state.templateActiveCountsById.delete(templateId);
    if (templateGraceMs <= 0) {
      state.templateGraceExpiryMsById.delete(templateId);
      return;
    }
    if (state.templateActiveCountsById.size + state.templateGraceExpiryMsById.size >= maxTemplates) {
      state.templatesOverCapacity = true;
      return;
    }
    state.templateGraceExpiryMsById.set(templateId, nowMs + templateGraceMs);
  }

  private reserveFineTokens(
    stream: string,
    touchCfg: TouchConfig,
    wanted: number,
    nowMs: number
  ): { granted: number; tokenLimited: boolean; refund: (used: number) => void } {
    const rate = Math.max(0, Math.floor(touchCfg.fineTokensPerSecond ?? 200_000));
    const burst = Math.max(0, Math.floor(touchCfg.fineBurstTokens ?? 400_000));
    if (wanted <= 0) return { granted: 0, tokenLimited: false, refund: () => {} };
    if (rate <= 0 || burst <= 0) return { granted: 0, tokenLimited: true, refund: () => {} };

    const b = this.fineTokenBucketsByStream.get(stream) ?? { tokens: burst, lastRefillMs: nowMs };
    const elapsedMs = Math.max(0, nowMs - b.lastRefillMs);
    if (elapsedMs > 0) {
      const refill = (elapsedMs * rate) / 1000;
      b.tokens = Math.min(burst, b.tokens + refill);
      b.lastRefillMs = nowMs;
    }

    const granted = Math.max(0, Math.min(wanted, Math.floor(b.tokens)));
    b.tokens = Math.max(0, b.tokens - granted);
    this.fineTokenBucketsByStream.set(stream, b);

    return {
      granted,
      tokenLimited: granted < wanted,
      refund: (used: number) => {
        const u = Math.max(0, Math.floor(used));
        if (u >= granted) return;
        const addBack = granted - u;
        const cur = this.fineTokenBucketsByStream.get(stream);
        if (!cur) return;
        cur.tokens = Math.min(burst, cur.tokens + addBack);
        this.fineTokenBucketsByStream.set(stream, cur);
      },
    };
  }

  private getOrCreateRuntimeTotals(stream: string): StreamRuntimeTotals {
    const existing = this.runtimeTotalsByStream.get(stream);
    if (existing) return existing;
    const created: StreamRuntimeTotals = {
      scanRowsTotal: 0,
      scanBatchesTotal: 0,
      scannedButEmitted0BatchesTotal: 0,
      processedThroughDeltaTotal: 0,
      touchesEmittedTotal: 0,
      touchesTableTotal: 0,
      touchesTemplateTotal: 0,
      fineTouchesDroppedDueToBudgetTotal: 0,
      fineTouchesSkippedColdTemplateTotal: 0,
      fineTouchesSkippedColdKeyTotal: 0,
      fineTouchesSkippedTemplateBucketTotal: 0,
      waitTouchedTotal: 0,
      waitTimeoutTotal: 0,
      waitStaleTotal: 0,
    };
    this.runtimeTotalsByStream.set(stream, created);
    return created;
  }

  private rangeLikelyHasRows(stream: string, fromOffset: bigint, toOffset: bigint): boolean {
    try {
      const it = this.db.iterWalRange(stream, fromOffset, toOffset);
      const first = it.next();
      return !first.done;
    } catch {
      return false;
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
