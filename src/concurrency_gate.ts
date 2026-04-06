export type GateRelease = () => void;

type Waiter = {
  resolve: (release: GateRelease) => void;
  reject: (error: unknown) => void;
  signal: AbortSignal | null;
  onAbort: (() => void) | null;
};

function abortError(): Error {
  const err = new Error("operation aborted");
  err.name = "AbortError";
  return err;
}

export class ConcurrencyGate {
  private limit: number;
  private active = 0;
  private readonly waiters: Waiter[] = [];

  constructor(limit: number) {
    this.limit = Math.max(1, Math.floor(limit));
  }

  getLimit(): number {
    return this.limit;
  }

  getActive(): number {
    return this.active;
  }

  getQueued(): number {
    return this.waiters.length;
  }

  setLimit(nextLimit: number): void {
    this.limit = Math.max(1, Math.floor(nextLimit));
    this.drain();
  }

  async acquire(signal?: AbortSignal | null): Promise<GateRelease> {
    if (signal?.aborted) throw abortError();
    if (this.active < this.limit) {
      this.active += 1;
      return this.releaseFactory();
    }
    return await new Promise<GateRelease>((resolve, reject) => {
      const waiter: Waiter = {
        resolve,
        reject,
        signal: signal ?? null,
        onAbort: null,
      };
      if (signal) {
        waiter.onAbort = () => {
          this.removeWaiter(waiter);
          reject(abortError());
        };
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      this.waiters.push(waiter);
    });
  }

  async run<T>(fn: () => Promise<T>, signal?: AbortSignal | null): Promise<T> {
    const release = await this.acquire(signal);
    try {
      return await fn();
    } finally {
      release();
    }
  }

  private releaseFactory(): GateRelease {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active = Math.max(0, this.active - 1);
      this.drain();
    };
  }

  private removeWaiter(waiter: Waiter): void {
    const idx = this.waiters.indexOf(waiter);
    if (idx >= 0) this.waiters.splice(idx, 1);
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener("abort", waiter.onAbort);
      waiter.onAbort = null;
    }
  }

  private drain(): void {
    while (this.active < this.limit && this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      if (waiter.signal?.aborted) {
        if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
        waiter.reject(abortError());
        continue;
      }
      if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
      waiter.onAbort = null;
      this.active += 1;
      waiter.resolve(this.releaseFactory());
    }
  }
}
