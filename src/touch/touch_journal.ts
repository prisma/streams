type TouchHit = {
  generation: number;
  keyId: number;
  bucketMaxSourceOffsetSeq: bigint;
  flushAtMs: number;
  bucketStartMs: number;
} | null;

type Waiter = {
  afterGeneration: number;
  keys: number[];
  // For huge keysets, we avoid per-key indexing and instead scan on flush.
  broad: boolean;
  deadlineMs: number;
  heapIndex: number;
  done: boolean;
  cleanup: (hit: TouchHit) => void;
};

type IntervalStats = {
  timeoutsFired: number;
  timeoutSweeps: number;
  timeoutSweepMsSum: number;
  timeoutSweepMsMax: number;
  notifyWakeups: number;
  notifyFlushes: number;
  notifyWakeMsSum: number;
  notifyWakeMsMax: number;
  heapSize: number;
};

export type TouchJournalIntervalStats = IntervalStats;

type TotalStats = {
  timeoutsFired: number;
  timeoutSweeps: number;
  timeoutSweepMsSum: number;
  timeoutSweepMsMax: number;
  notifyWakeups: number;
  notifyFlushes: number;
  notifyWakeMsSum: number;
  notifyWakeMsMax: number;
  flushes: number;
};

function u32(x: number): number {
  return x >>> 0;
}

function mix32(x: number): number {
  // Murmur3 finalizer-ish avalanche mix.
  let y = u32(x);
  y ^= y >>> 16;
  y = Math.imul(y, 0x85ebca6b) >>> 0;
  y ^= y >>> 13;
  y = Math.imul(y, 0xc2b2ae35) >>> 0;
  y ^= y >>> 16;
  return y >>> 0;
}

function newEpochHex16(): string {
  const buf = new Uint32Array(2);
  crypto.getRandomValues(buf);
  return buf[0]!.toString(16).padStart(8, "0") + buf[1]!.toString(16).padStart(8, "0");
}

export type TouchJournalMeta = {
  mode: "memory";
  cursor: string;
  epoch: string;
  generation: number;
  bucketMs: number;
  coalesceMs: number;
  filterSize: number;
  k: number;
  pendingKeys: number;
  overflowBuckets: number;
  activeWaiters: number;
  bucketMaxSourceOffsetSeq: string;
  lastFlushAtMs: number;
  flushIntervalMsMaxLast10s: number;
  flushIntervalMsP95Last10s: number;
};

export type TouchWaitResult =
  | { stale: true; cursor: string; epoch: string; generation: number }
  | { stale: false; touched: boolean; cursor: string };

export function parseTouchCursor(raw: string): { epoch: string; generation: number } | null {
  const s = raw.trim();
  if (s === "") return null;
  const idx = s.indexOf(":");
  if (idx <= 0) return null;
  const epoch = s.slice(0, idx);
  const genRaw = s.slice(idx + 1);
  if (!/^[0-9a-f]{16}$/i.test(epoch)) return null;
  if (!/^[0-9]+$/.test(genRaw)) return null;
  const gen = Number(genRaw);
  if (!Number.isFinite(gen) || gen < 0) return null;
  return { epoch: epoch.toLowerCase(), generation: Math.floor(gen) };
}

export function formatTouchCursor(epoch: string, generation: number): string {
  return `${epoch}:${Math.max(0, Math.floor(generation))}`;
}

/**
 * Memory-only touch journal:
 * - fixed-size time-aware bloom filter: lastSet[pos] = generation
 * - bucketed flush (default 100ms) to avoid mid-bucket false negatives
 * - waiter index + single global deadline heap for reliable timeouts under load
 *
 * Safety model:
 * - No false negatives within an epoch (process lifetime), except if generation wraps (uint32).
 * - False positives are allowed (extra invalidations).
 */
export class TouchJournal {
  private readonly epoch: string;
  private generation: number;
  private readonly bucketMs: number;
  private coalesceMs: number;

  private readonly k: number;
  private readonly mask: number;
  private readonly lastSet: Uint32Array;

  private readonly pending = new Set<number>();
  private pendingBucketStartMs = 0;
  private pendingMaxSourceOffsetSeq: bigint = -1n;
  private lastFlushedSourceOffsetSeq: bigint = -1n;
  private overflow = false;
  private overflowBuckets = 0;
  private lastOverflowGeneration = 0;
  private lastFlushAtMs = 0;
  private lastBucketStartMs = 0;
  private readonly flushIntervalsLast10s: Array<{ atMs: number; intervalMs: number }> = [];

  private flushTimer: any | null = null;

  private readonly byKey = new Map<number, Set<Waiter>>();
  private readonly broad = new Set<Waiter>();
  private activeWaiters = 0;

  // Single global deadline heap + timer for waiter expiry.
  private readonly deadlineHeap: Waiter[] = [];
  private timeoutTimer: any | null = null;
  private scheduledDeadlineMs: number | null = null;

  private interval: IntervalStats = {
    timeoutsFired: 0,
    timeoutSweeps: 0,
    timeoutSweepMsSum: 0,
    timeoutSweepMsMax: 0,
    notifyWakeups: 0,
    notifyFlushes: 0,
    notifyWakeMsSum: 0,
    notifyWakeMsMax: 0,
    heapSize: 0,
  };
  private totals: TotalStats = {
    timeoutsFired: 0,
    timeoutSweeps: 0,
    timeoutSweepMsSum: 0,
    timeoutSweepMsMax: 0,
    notifyWakeups: 0,
    notifyFlushes: 0,
    notifyWakeMsSum: 0,
    notifyWakeMsMax: 0,
    flushes: 0,
  };

  // Hard bound for unique keys per bucket. If exceeded we treat the whole bucket as "overflow"
  // and wake all waiters (lossy but safe).
  private readonly pendingMaxKeys: number;
  private readonly keyIndexMaxKeys: number;

  constructor(opts: {
    bucketMs: number;
    filterPow2: number;
    k: number;
    pendingMaxKeys: number;
    keyIndexMaxKeys: number;
  }) {
    this.epoch = newEpochHex16();
    this.generation = 0;
    this.bucketMs = Math.max(1, Math.floor(opts.bucketMs));
    this.coalesceMs = this.bucketMs;
    const pow2 = Math.max(10, Math.min(30, Math.floor(opts.filterPow2)));
    const size = 1 << pow2;
    this.k = Math.max(1, Math.min(8, Math.floor(opts.k)));
    this.mask = size - 1;
    this.lastSet = new Uint32Array(size);
    this.pendingMaxKeys = Math.max(1, Math.floor(opts.pendingMaxKeys));
    this.keyIndexMaxKeys = Math.max(1, Math.floor(opts.keyIndexMaxKeys));
  }

  stop(): void {
    if (this.flushTimer) clearTimeout(this.flushTimer);
    if (this.timeoutTimer) clearTimeout(this.timeoutTimer);
    this.flushTimer = null;
    this.timeoutTimer = null;
    this.scheduledDeadlineMs = null;
    this.pending.clear();
    this.pendingBucketStartMs = 0;
    this.pendingMaxSourceOffsetSeq = -1n;
    this.lastFlushedSourceOffsetSeq = -1n;
    this.lastFlushAtMs = 0;
    this.lastBucketStartMs = 0;
    this.flushIntervalsLast10s.length = 0;
    this.byKey.clear();
    this.broad.clear();
    this.deadlineHeap.length = 0;
    this.activeWaiters = 0;
  }

  getEpoch(): string {
    return this.epoch;
  }

  getGeneration(): number {
    return this.generation >>> 0;
  }

  getCursor(): string {
    return formatTouchCursor(this.epoch, this.getGeneration());
  }

  getLastFlushedSourceOffsetSeq(): bigint {
    return this.lastFlushedSourceOffsetSeq;
  }

  getActiveWaiters(): number {
    return this.activeWaiters;
  }

  snapshotAndResetIntervalStats(): IntervalStats {
    const out = { ...this.interval, heapSize: this.deadlineHeap.length };
    this.interval = {
      timeoutsFired: 0,
      timeoutSweeps: 0,
      timeoutSweepMsSum: 0,
      timeoutSweepMsMax: 0,
      notifyWakeups: 0,
      notifyFlushes: 0,
      notifyWakeMsSum: 0,
      notifyWakeMsMax: 0,
      heapSize: 0,
    };
    return out;
  }

  getTotalStats(): TotalStats {
    return { ...this.totals };
  }

  getMeta(): TouchJournalMeta {
    const nowMs = Date.now();
    this.pruneFlushIntervals(nowMs);
    const intervals = this.flushIntervalsLast10s.map((x) => x.intervalMs);
    return {
      mode: "memory",
      cursor: this.getCursor(),
      epoch: this.epoch,
      generation: this.getGeneration(),
      bucketMs: this.bucketMs,
      coalesceMs: this.coalesceMs,
      filterSize: this.lastSet.length,
      k: this.k,
      pendingKeys: this.pending.size,
      overflowBuckets: this.overflowBuckets,
      activeWaiters: this.activeWaiters,
      bucketMaxSourceOffsetSeq: this.lastFlushedSourceOffsetSeq.toString(),
      lastFlushAtMs: this.lastFlushAtMs,
      flushIntervalMsMaxLast10s: intervals.length > 0 ? Math.max(...intervals) : 0,
      flushIntervalMsP95Last10s: percentile(intervals, 0.95),
    };
  }

  getFilterBytes(): number {
    return this.lastSet.byteLength;
  }

  touch(keyId: number, sourceOffsetSeq?: bigint): void {
    if (this.pending.size === 0 && !this.overflow && this.pendingBucketStartMs <= 0) {
      this.pendingBucketStartMs = Date.now();
    }
    if (this.pending.size >= this.pendingMaxKeys) {
      // We may drop fine touches once we overflow, but we must treat the bucket
      // as a broadcast invalidation to avoid false negatives.
      this.overflow = true;
    } else {
      this.pending.add(u32(keyId));
    }
    if (typeof sourceOffsetSeq === "bigint" && sourceOffsetSeq > this.pendingMaxSourceOffsetSeq) {
      this.pendingMaxSourceOffsetSeq = sourceOffsetSeq;
    }
    this.ensureFlushScheduled();
  }

  setCoalesceMs(ms: number): void {
    const next = Math.max(1, Math.min(this.bucketMs, Math.floor(ms)));
    this.coalesceMs = next;
  }

  /**
   * Best-effort membership query: "maybe touched since sinceGeneration".
   * False positives are allowed; false negatives within epoch are not (except overflow / generation wrap).
   */
  maybeTouchedSince(keyId: number, sinceGeneration: number): boolean {
    const since = u32(sinceGeneration);
    if (since < u32(this.lastOverflowGeneration)) return true;
    const h1 = u32(keyId);
    let h2 = mix32(h1);
    if (h2 === 0) h2 = 0x9e3779b9; // avoid zero stride
    let min = 0xffffffff;
    for (let i = 0; i < this.k; i++) {
      const pos = u32(h1 + Math.imul(i, h2)) & this.mask;
      const g = this.lastSet[pos]!;
      if (g < min) min = g;
    }
    return u32(min) > since;
  }

  maybeTouchedSinceAny(keyIds: number[], sinceGeneration: number): boolean {
    const since = u32(sinceGeneration);
    if (since < u32(this.lastOverflowGeneration)) return true;
    for (let i = 0; i < keyIds.length; i++) {
      if (this.maybeTouchedSince(keyIds[i]!, since)) return true;
    }
    return false;
  }

  /**
   * Wait for any of `keys` to be touched in a bucket generation strictly greater than `afterGeneration`.
   *
   * Returns:
   * - `{generation, keyId}` when touched
   * - `null` on timeout or abort
   */
  waitForAny(args: { keys: number[]; afterGeneration: number; timeoutMs: number; signal?: AbortSignal }): Promise<TouchHit> {
    if (args.keys.length === 0) return Promise.resolve(null);
    if (args.signal?.aborted) return Promise.resolve(null);
    const timeoutMs = Math.max(0, Math.floor(args.timeoutMs));
    if (timeoutMs <= 0) return Promise.resolve(null);

    const keys = Array.from(new Set(args.keys.map(u32)));
    const broad = keys.length > this.keyIndexMaxKeys;

    return new Promise((resolve) => {
      const waiter: Waiter = {
        afterGeneration: u32(args.afterGeneration),
        keys,
        broad,
        deadlineMs: Date.now() + timeoutMs,
        heapIndex: -1,
        done: false,
        cleanup: (hit) => {
          if (waiter.done) return;
          waiter.done = true;

          if (waiter.broad) {
            this.broad.delete(waiter);
          } else {
            for (const k of waiter.keys) {
              const s = this.byKey.get(k);
              if (!s) continue;
              s.delete(waiter);
              if (s.size === 0) this.byKey.delete(k);
            }
          }

          this.activeWaiters = Math.max(0, this.activeWaiters - 1);

          const removedRoot = this.heapRemove(waiter);
          if (args.signal) args.signal.removeEventListener("abort", onAbort);
          if (removedRoot) this.rescheduleTimeoutTimer();
          resolve(hit);
        },
      };

      if (waiter.broad) {
        this.broad.add(waiter);
      } else {
        for (const k of waiter.keys) {
          const set = this.byKey.get(k) ?? new Set<Waiter>();
          set.add(waiter);
          this.byKey.set(k, set);
        }
      }
      this.activeWaiters += 1;

      const onAbort = () => waiter.cleanup(null);
      if (args.signal) args.signal.addEventListener("abort", onAbort, { once: true });

      this.heapPush(waiter);
      this.rescheduleTimeoutTimer();
    });
  }

  private ensureFlushScheduled(): void {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => this.flushBucket(), this.coalesceMs);
  }

  private flushBucket(): void {
    this.flushTimer = null;

    const hasTouches = this.pending.size > 0 || this.overflow;
    if (!hasTouches) return;

    // Advance generation only at bucket boundaries so cursors are safe.
    this.generation = u32(this.generation + 1);
    const gen = this.getGeneration();
    const bucketMaxSourceOffsetSeq = this.pendingMaxSourceOffsetSeq;
    if (bucketMaxSourceOffsetSeq > this.lastFlushedSourceOffsetSeq) this.lastFlushedSourceOffsetSeq = bucketMaxSourceOffsetSeq;
    const flushAtMs = Date.now();
    const bucketStartMs = this.pendingBucketStartMs > 0 ? this.pendingBucketStartMs : flushAtMs;
    if (this.lastFlushAtMs > 0 && flushAtMs >= this.lastFlushAtMs) {
      this.flushIntervalsLast10s.push({ atMs: flushAtMs, intervalMs: flushAtMs - this.lastFlushAtMs });
      this.pruneFlushIntervals(flushAtMs);
    }
    this.lastFlushAtMs = flushAtMs;
    this.lastBucketStartMs = bucketStartMs;
    this.totals.flushes += 1;

    if (this.overflow) {
      this.overflowBuckets += 1;
      this.lastOverflowGeneration = gen;
    }

    // Update bloom filter for touched keys. We still update for the keys we captured even on overflow;
    // the overflow marker is what preserves correctness for dropped keys.
    for (const keyId of this.pending) {
      const h1 = u32(keyId);
      let h2 = mix32(h1);
      if (h2 === 0) h2 = 0x9e3779b9;
      for (let i = 0; i < this.k; i++) {
        const pos = u32(h1 + Math.imul(i, h2)) & this.mask;
        this.lastSet[pos] = gen;
      }
    }

    if (this.overflow) {
      // Broadcast wakeup: resolve all waiters (safe, lossy).
      const wakeStartMs = Date.now();
      let wakeups = 0;
      const all: Waiter[] = [];
      for (const s of this.byKey.values()) for (const w of s) all.push(w);
      for (const w of this.broad) all.push(w);
      for (const w of all) {
        if (w.done) continue;
        if (gen > w.afterGeneration) {
          wakeups += 1;
          w.cleanup({ generation: gen, keyId: 0, bucketMaxSourceOffsetSeq, flushAtMs, bucketStartMs });
        }
      }
      if (wakeups > 0) {
        const wakeMs = Date.now() - wakeStartMs;
        this.interval.notifyWakeups += wakeups;
        this.interval.notifyFlushes += 1;
        this.interval.notifyWakeMsSum += wakeMs;
        this.interval.notifyWakeMsMax = Math.max(this.interval.notifyWakeMsMax, wakeMs);
        this.totals.notifyWakeups += wakeups;
        this.totals.notifyFlushes += 1;
        this.totals.notifyWakeMsSum += wakeMs;
        this.totals.notifyWakeMsMax = Math.max(this.totals.notifyWakeMsMax, wakeMs);
      }
    } else {
      // Wake keyed waiters by touched key id.
      const wakeStartMs = Date.now();
      let wakeups = 0;
      for (const keyId of this.pending) {
        const set = this.byKey.get(keyId);
        if (!set || set.size === 0) continue;
        for (const w of set) {
          if (w.done) continue;
          if (gen > w.afterGeneration) {
            wakeups += 1;
            w.cleanup({ generation: gen, keyId, bucketMaxSourceOffsetSeq, flushAtMs, bucketStartMs });
          }
        }
      }

      // Wake broad waiters by scanning bloom membership for their keysets.
      if (this.broad.size > 0) {
        for (const w of this.broad) {
          if (w.done) continue;
          if (gen <= w.afterGeneration) continue;
          let hit = false;
          for (let i = 0; i < w.keys.length; i++) {
            if (this.maybeTouchedSince(w.keys[i]!, w.afterGeneration)) {
              hit = true;
              break;
            }
          }
          if (hit) {
            wakeups += 1;
            w.cleanup({ generation: gen, keyId: 0, bucketMaxSourceOffsetSeq, flushAtMs, bucketStartMs });
          }
        }
      }
      if (wakeups > 0) {
        const wakeMs = Date.now() - wakeStartMs;
        this.interval.notifyWakeups += wakeups;
        this.interval.notifyFlushes += 1;
        this.interval.notifyWakeMsSum += wakeMs;
        this.interval.notifyWakeMsMax = Math.max(this.interval.notifyWakeMsMax, wakeMs);
        this.totals.notifyWakeups += wakeups;
        this.totals.notifyFlushes += 1;
        this.totals.notifyWakeMsSum += wakeMs;
        this.totals.notifyWakeMsMax = Math.max(this.totals.notifyWakeMsMax, wakeMs);
      }
    }

    this.pending.clear();
    this.pendingBucketStartMs = 0;
    this.pendingMaxSourceOffsetSeq = -1n;
    this.overflow = false;
  }

  getLastFlushAtMs(): number {
    return this.lastFlushAtMs;
  }

  getLastBucketStartMs(): number {
    return this.lastBucketStartMs;
  }

  private pruneFlushIntervals(nowMs: number): void {
    const cutoff = nowMs - 10_000;
    while (this.flushIntervalsLast10s.length > 0 && this.flushIntervalsLast10s[0]!.atMs < cutoff) {
      this.flushIntervalsLast10s.shift();
    }
  }

  private rescheduleTimeoutTimer(): void {
    const next = this.deadlineHeap[0];
    if (!next) {
      if (this.timeoutTimer) clearTimeout(this.timeoutTimer);
      this.timeoutTimer = null;
      this.scheduledDeadlineMs = null;
      return;
    }

    if (this.timeoutTimer && this.scheduledDeadlineMs != null && this.scheduledDeadlineMs === next.deadlineMs) return;

    if (this.timeoutTimer) clearTimeout(this.timeoutTimer);
    this.scheduledDeadlineMs = next.deadlineMs;
    const delayMs = Math.max(0, next.deadlineMs - Date.now());
    this.timeoutTimer = setTimeout(() => this.expireDueWaiters(), delayMs);
  }

  private expireDueWaiters(): void {
    this.timeoutTimer = null;
    this.scheduledDeadlineMs = null;

    const start = Date.now();
    const now = start;
    let expired = 0;

    for (;;) {
      const head = this.deadlineHeap[0];
      if (!head) break;
      if (head.deadlineMs > now) break;

      const w = this.heapPopMin();
      if (!w) break;
      if (w.done) continue;

      expired += 1;
      w.cleanup(null);
    }

    if (expired > 0) {
      const sweepMs = Date.now() - start;
      this.interval.timeoutsFired += expired;
      this.interval.timeoutSweeps += 1;
      this.interval.timeoutSweepMsSum += sweepMs;
      this.interval.timeoutSweepMsMax = Math.max(this.interval.timeoutSweepMsMax, sweepMs);
      this.totals.timeoutsFired += expired;
      this.totals.timeoutSweeps += 1;
      this.totals.timeoutSweepMsSum += sweepMs;
      this.totals.timeoutSweepMsMax = Math.max(this.totals.timeoutSweepMsMax, sweepMs);
    }

    this.rescheduleTimeoutTimer();
  }

  private heapSwap(i: number, j: number): void {
    const a = this.deadlineHeap[i]!;
    const b = this.deadlineHeap[j]!;
    this.deadlineHeap[i] = b;
    this.deadlineHeap[j] = a;
    a.heapIndex = j;
    b.heapIndex = i;
  }

  private heapLess(i: number, j: number): boolean {
    const a = this.deadlineHeap[i]!;
    const b = this.deadlineHeap[j]!;
    return a.deadlineMs < b.deadlineMs;
  }

  private heapSiftUp(i: number): void {
    let idx = i;
    while (idx > 0) {
      const parent = (idx - 1) >> 1;
      if (!this.heapLess(idx, parent)) break;
      this.heapSwap(idx, parent);
      idx = parent;
    }
  }

  private heapSiftDown(i: number): void {
    let idx = i;
    for (;;) {
      const left = idx * 2 + 1;
      const right = left + 1;
      if (left >= this.deadlineHeap.length) break;
      let smallest = left;
      if (right < this.deadlineHeap.length && this.heapLess(right, left)) smallest = right;
      if (!this.heapLess(smallest, idx)) break;
      this.heapSwap(idx, smallest);
      idx = smallest;
    }
  }

  private heapPush(w: Waiter): void {
    if (w.heapIndex >= 0) return;
    w.heapIndex = this.deadlineHeap.length;
    this.deadlineHeap.push(w);
    this.heapSiftUp(w.heapIndex);
  }

  // Returns true if the root was removed.
  private heapRemove(w: Waiter): boolean {
    const idx = w.heapIndex;
    if (idx < 0) return false;

    const lastIdx = this.deadlineHeap.length - 1;
    const removedRoot = idx === 0;
    if (idx !== lastIdx) this.heapSwap(idx, lastIdx);
    this.deadlineHeap.pop();
    w.heapIndex = -1;

    if (idx < this.deadlineHeap.length) {
      this.heapSiftDown(idx);
      this.heapSiftUp(idx);
    }
    return removedRoot;
  }

  private heapPopMin(): Waiter | null {
    if (this.deadlineHeap.length === 0) return null;
    const w = this.deadlineHeap[0]!;
    const last = this.deadlineHeap.length - 1;
    if (last === 0) {
      this.deadlineHeap.pop();
      w.heapIndex = -1;
      return w;
    }
    this.heapSwap(0, last);
    this.deadlineHeap.pop();
    w.heapIndex = -1;
    this.heapSiftDown(0);
    return w;
  }
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.max(0, Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p)));
  return sorted[idx] ?? 0;
}
