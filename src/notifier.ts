type Waiter = { afterSeq: bigint; resolve: () => void };
type DetailsWaiter = { afterVersion: bigint; resolve: () => void };

export type StreamNotifierMemoryStats = {
  waiterStreams: number;
  waiters: number;
  latestSeqStreams: number;
  detailsWaiterStreams: number;
  detailsWaiters: number;
  detailsVersionStreams: number;
};

export type StreamNotifierTopStreamEntry = {
  stream: string;
  waiters: number;
  details_waiters: number;
  total_waiters: number;
};

export class StreamNotifier {
  private readonly waiters = new Map<string, Set<Waiter>>();
  private readonly latestSeq = new Map<string, bigint>();
  private readonly detailsWaiters = new Map<string, Set<DetailsWaiter>>();
  private readonly detailsVersion = new Map<string, bigint>();

  notify(stream: string, newEndSeq: bigint): void {
    this.latestSeq.set(stream, newEndSeq);
    const set = this.waiters.get(stream);
    if (!set || set.size === 0) return;
    for (const w of Array.from(set)) {
      if (newEndSeq > w.afterSeq) {
        set.delete(w);
        w.resolve();
      }
    }
    if (set.size === 0) this.waiters.delete(stream);
  }

  waitFor(stream: string, afterSeq: bigint, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.resolve();
    const latest = this.latestSeq.get(stream);
    if (latest != null && latest > afterSeq) return Promise.resolve();
    return new Promise((resolve) => {
      let done = false;
      const set = this.waiters.get(stream) ?? new Set();
      const cleanup = () => {
        if (done) return;
        done = true;
        const s = this.waiters.get(stream);
        if (s) {
          s.delete(waiter);
          if (s.size === 0) this.waiters.delete(stream);
        }
        if (timeoutId) clearTimeout(timeoutId);
        if (signal) signal.removeEventListener("abort", onAbort);
        resolve();
      };
      const waiter: Waiter = { afterSeq, resolve: cleanup };
      set.add(waiter);
      this.waiters.set(stream, set);

      const onAbort = () => cleanup();
      if (signal) signal.addEventListener("abort", onAbort, { once: true });

      let timeoutId: any | null = null;
      if (timeoutMs > 0) {
        timeoutId = setTimeout(() => {
          cleanup();
        }, timeoutMs);
      }
    });
  }

  currentDetailsVersion(stream: string): bigint {
    return this.detailsVersion.get(stream) ?? 0n;
  }

  notifyDetailsChanged(stream: string): void {
    const nextVersion = (this.detailsVersion.get(stream) ?? 0n) + 1n;
    this.detailsVersion.set(stream, nextVersion);
    const set = this.detailsWaiters.get(stream);
    if (!set || set.size === 0) return;
    for (const w of Array.from(set)) {
      if (nextVersion > w.afterVersion) {
        set.delete(w);
        w.resolve();
      }
    }
    if (set.size === 0) this.detailsWaiters.delete(stream);
  }

  waitForDetailsChange(stream: string, afterVersion: bigint, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.resolve();
    const latest = this.detailsVersion.get(stream);
    if (latest != null && latest > afterVersion) return Promise.resolve();
    return new Promise((resolve) => {
      let done = false;
      const set = this.detailsWaiters.get(stream) ?? new Set();
      const cleanup = () => {
        if (done) return;
        done = true;
        const s = this.detailsWaiters.get(stream);
        if (s) {
          s.delete(waiter);
          if (s.size === 0) this.detailsWaiters.delete(stream);
        }
        if (timeoutId) clearTimeout(timeoutId);
        if (signal) signal.removeEventListener("abort", onAbort);
        resolve();
      };
      const waiter: DetailsWaiter = { afterVersion, resolve: cleanup };
      set.add(waiter);
      this.detailsWaiters.set(stream, set);

      const onAbort = () => cleanup();
      if (signal) signal.addEventListener("abort", onAbort, { once: true });

      let timeoutId: any | null = null;
      if (timeoutMs > 0) {
        timeoutId = setTimeout(() => {
          cleanup();
        }, timeoutMs);
      }
    });
  }

  notifyClose(stream: string): void {
    const set = this.waiters.get(stream);
    if (set && set.size > 0) {
      for (const w of Array.from(set)) {
        set.delete(w);
        w.resolve();
      }
      if (set.size === 0) this.waiters.delete(stream);
    }

    const detailsSet = this.detailsWaiters.get(stream);
    if (detailsSet && detailsSet.size > 0) {
      for (const w of Array.from(detailsSet)) {
        detailsSet.delete(w);
        w.resolve();
      }
      if (detailsSet.size === 0) this.detailsWaiters.delete(stream);
    }
  }

  getMemoryStats(): StreamNotifierMemoryStats {
    let waiters = 0;
    for (const set of this.waiters.values()) waiters += set.size;
    let detailsWaiters = 0;
    for (const set of this.detailsWaiters.values()) detailsWaiters += set.size;
    return {
      waiterStreams: this.waiters.size,
      waiters,
      latestSeqStreams: this.latestSeq.size,
      detailsWaiterStreams: this.detailsWaiters.size,
      detailsWaiters,
      detailsVersionStreams: this.detailsVersion.size,
    };
  }

  getTopStreams(limit = 5): StreamNotifierTopStreamEntry[] {
    const totals = new Map<string, StreamNotifierTopStreamEntry>();
    for (const [stream, waiters] of this.waiters) {
      const row = totals.get(stream) ?? { stream, waiters: 0, details_waiters: 0, total_waiters: 0 };
      row.waiters = waiters.size;
      row.total_waiters = row.waiters + row.details_waiters;
      totals.set(stream, row);
    }
    for (const [stream, detailsWaiters] of this.detailsWaiters) {
      const row = totals.get(stream) ?? { stream, waiters: 0, details_waiters: 0, total_waiters: 0 };
      row.details_waiters = detailsWaiters.size;
      row.total_waiters = row.waiters + row.details_waiters;
      totals.set(stream, row);
    }
    return Array.from(totals.values())
      .sort((a, b) => b.total_waiters - a.total_waiters || a.stream.localeCompare(b.stream))
      .slice(0, Math.max(0, Math.floor(limit)));
  }
}
