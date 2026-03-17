type Hit = { seq: bigint; key: string };

type Waiter = {
  stream: string;
  afterSeq: bigint;
  keys: string[];
  deadlineMs: number;
  heapIndex: number;
  done: boolean;
  cleanup: (hit: Hit | null) => void;
};

type StreamState = {
  byKey: Map<string, Set<Waiter>>;
  activeWaiters: number;
};

export type RoutingKeyNotifierIntervalStats = {
  timeoutsFired: number;
  timeoutSweeps: number;
  timeoutSweepMsSum: number;
  timeoutSweepMsMax: number;
  heapSize: number;
};

/**
 * In-memory wait index keyed by (stream, routingKey).
 *
 * This is used to implement /touch/wait without waking *all* waiters on any
 * new touch row. Instead, only waiters watching a touched key are woken.
 *
 * Note: this is best-effort and process-local (lost on restart). Callers must
 * still do an initial DB scan for missed touches based on sinceTouchOffset.
 */
export class RoutingKeyNotifier {
  private readonly streams = new Map<string, StreamState>();

  // Single global deadline heap + timer for waiter expiry.
  private readonly deadlineHeap: Waiter[] = [];
  private timer: any | null = null;
  private scheduledDeadlineMs: number | null = null;

  // Interval stats (resettable by the metrics system).
  private interval: RoutingKeyNotifierIntervalStats = {
    timeoutsFired: 0,
    timeoutSweeps: 0,
    timeoutSweepMsSum: 0,
    timeoutSweepMsMax: 0,
    heapSize: 0,
  };

  getActiveWaiters(stream: string): number {
    return this.streams.get(stream)?.activeWaiters ?? 0;
  }

  snapshotAndResetIntervalStats(): RoutingKeyNotifierIntervalStats {
    const out = { ...this.interval, heapSize: this.deadlineHeap.length };
    this.interval = {
      timeoutsFired: 0,
      timeoutSweeps: 0,
      timeoutSweepMsSum: 0,
      timeoutSweepMsMax: 0,
      heapSize: 0,
    };
    return out;
  }

  notify(stream: string, key: string, seq: bigint): void {
    const st = this.streams.get(stream);
    if (!st) return;
    const set = st.byKey.get(key);
    if (!set || set.size === 0) return;

    // Resolve any waiters that are waiting strictly before this seq.
    for (const w of set) {
      if (w.done) continue;
      if (seq > w.afterSeq) w.cleanup({ seq, key });
    }
  }

  /**
   * Wait for any of `keys` to be notified at an offset > afterSeq.
   *
   * Returns:
   * - `{seq, key}` when touched
   * - `null` on timeout or abort
   */
  waitForAny(args: { stream: string; keys: string[]; afterSeq: bigint; timeoutMs: number; signal?: AbortSignal }): Promise<Hit | null> {
    if (args.keys.length === 0) return Promise.resolve(null);
    if (args.signal?.aborted) return Promise.resolve(null);
    const timeoutMs = Math.max(0, Math.floor(args.timeoutMs));
    if (timeoutMs <= 0) return Promise.resolve(null);
    return new Promise((resolve) => {
      const st = this.streams.get(args.stream) ?? { byKey: new Map<string, Set<Waiter>>(), activeWaiters: 0 };
      const deadlineMs = Date.now() + timeoutMs;

      const waiter: Waiter = {
        stream: args.stream,
        afterSeq: args.afterSeq,
        keys: args.keys,
        deadlineMs,
        heapIndex: -1,
        done: false,
        cleanup: (hit) => {
          if (waiter.done) return;
          waiter.done = true;

          // Remove from all key sets.
          const current = this.streams.get(args.stream);
          if (current) {
            for (const k of waiter.keys) {
              const s = current.byKey.get(k);
              if (!s) continue;
              s.delete(waiter);
              if (s.size === 0) current.byKey.delete(k);
            }
            current.activeWaiters = Math.max(0, current.activeWaiters - 1);
            if (current.byKey.size === 0 && current.activeWaiters === 0) this.streams.delete(args.stream);
          }

          const removedRoot = this.heapRemove(waiter);
          if (args.signal) args.signal.removeEventListener("abort", onAbort);
          if (removedRoot) this.rescheduleTimer();
          resolve(hit);
        },
      };

      // Register for each key.
      for (const k of args.keys) {
        const set = st.byKey.get(k) ?? new Set<Waiter>();
        set.add(waiter);
        st.byKey.set(k, set);
      }
      st.activeWaiters += 1;
      this.streams.set(args.stream, st);

      const onAbort = () => waiter.cleanup(null);
      if (args.signal) args.signal.addEventListener("abort", onAbort, { once: true });

      this.heapPush(waiter);
      this.rescheduleTimer();
    });
  }

  private rescheduleTimer(): void {
    const next = this.deadlineHeap[0];
    if (!next) {
      if (this.timer) clearTimeout(this.timer);
      this.timer = null;
      this.scheduledDeadlineMs = null;
      return;
    }

    // Avoid rescheduling unless the heap root changes (insert earlier or remove root).
    if (this.timer && this.scheduledDeadlineMs != null && this.scheduledDeadlineMs === next.deadlineMs) return;

    if (this.timer) clearTimeout(this.timer);
    this.scheduledDeadlineMs = next.deadlineMs;
    const delayMs = Math.max(0, next.deadlineMs - Date.now());
    this.timer = setTimeout(() => this.expireDueWaiters(), delayMs);
  }

  private expireDueWaiters(): void {
    this.timer = null;
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
    }

    this.rescheduleTimer();
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

  // Returns true if the root was removed (useful to decide whether to reschedule the timer).
  private heapRemove(w: Waiter): boolean {
    const idx = w.heapIndex;
    if (idx < 0) return false;

    const lastIdx = this.deadlineHeap.length - 1;
    const removedRoot = idx === 0;
    if (idx !== lastIdx) this.heapSwap(idx, lastIdx);
    this.deadlineHeap.pop();
    w.heapIndex = -1;

    if (idx < this.deadlineHeap.length) {
      // Fix heap invariant for the element swapped into idx.
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
