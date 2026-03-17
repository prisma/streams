export class MemoryGuard {
  private readonly limitBytes: number;
  private readonly resumeBytes: number;
  private readonly intervalMs: number;
  private readonly onSample?: (rssBytes: number, overLimit: boolean, limitBytes: number) => void;
  private readonly heapSnapshotPath?: string;
  private readonly heapSnapshotMinIntervalMs: number;
  private timer: any | null = null;
  private overLimit = false;
  private maxRssBytes = 0;
  private lastRssBytes = 0;
  private lastGcMs = 0;
  private lastSnapshotMs = 0;

  constructor(
    limitBytes: number,
    opts: {
      resumeFraction?: number;
      intervalMs?: number;
      onSample?: (rssBytes: number, overLimit: boolean, limitBytes: number) => void;
      heapSnapshotPath?: string;
      heapSnapshotMinIntervalMs?: number;
    } = {}
  ) {
    this.limitBytes = Math.max(0, limitBytes);
    // Resume as soon as RSS drops back below the limit by default (no hysteresis),
    // so the server doesn't "deadlock" itself under a stable high-water mark.
    const resumeFraction = Math.min(1.0, Math.max(0.5, opts.resumeFraction ?? 1.0));
    this.resumeBytes = Math.floor(this.limitBytes * resumeFraction);
    this.intervalMs = Math.max(50, opts.intervalMs ?? 1000);
    this.onSample = opts.onSample;
    this.heapSnapshotPath = opts.heapSnapshotPath;
    this.heapSnapshotMinIntervalMs = Math.max(1000, opts.heapSnapshotMinIntervalMs ?? 60_000);
  }

  start(): void {
    if (this.timer) return;
    this.sample();
    this.timer = setInterval(() => this.sample(), this.intervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private sample(): void {
    const rss = process.memoryUsage().rss;
    this.lastRssBytes = rss;
    if (rss > this.maxRssBytes) this.maxRssBytes = rss;
    if (this.onSample) {
      const overLimit = this.limitBytes > 0 && rss > this.limitBytes;
      try {
        this.onSample(rss, overLimit, this.limitBytes);
      } catch {
        // ignore
      }
    }
    if (this.limitBytes <= 0) return;
    if (this.overLimit) {
      if (rss <= this.resumeBytes) this.overLimit = false;
    } else if (rss > this.limitBytes) {
      this.overLimit = true;
    }
  }

  shouldAllow(): boolean {
    if (this.limitBytes <= 0) return true;
    return !this.overLimit;
  }

  isOverLimit(): boolean {
    return this.overLimit;
  }

  getMaxRssBytes(): number {
    return this.maxRssBytes;
  }

  snapshotMaxRssBytes(reset = true): number {
    const max = this.maxRssBytes;
    if (reset) this.maxRssBytes = this.lastRssBytes;
    return max;
  }

  getLastRssBytes(): number {
    return this.lastRssBytes;
  }

  getLimitBytes(): number {
    return this.limitBytes;
  }

  maybeGc(reason: string): void {
    const gcFn = (globalThis as any)?.Bun?.gc;
    if (typeof gcFn !== "function") return;
    const now = Date.now();
    if (now - this.lastGcMs < 10_000) return;
    this.lastGcMs = now;
    const before = process.memoryUsage().rss;
    try {
      gcFn(true);
    } catch {
      try {
        gcFn();
      } catch {
        return;
      }
    }
    const after = process.memoryUsage().rss;
    // eslint-disable-next-line no-console
    console.warn(`[gc] forced GC (${reason}) rss ${formatBytes(before)} -> ${formatBytes(after)}`);
  }

  maybeHeapSnapshot(reason: string): void {
    if (!this.heapSnapshotPath) return;
    const now = Date.now();
    if (now - this.lastSnapshotMs < this.heapSnapshotMinIntervalMs) return;
    this.lastSnapshotMs = now;
    void this.writeHeapSnapshot(reason);
  }

  private async writeHeapSnapshot(reason: string): Promise<void> {
    try {
      const v8 = await import("v8");
      if (typeof v8.writeHeapSnapshot !== "function") return;
      const fs = await import("node:fs");
      try {
        fs.unlinkSync(this.heapSnapshotPath!);
      } catch {
        // ignore
      }
      const before = process.memoryUsage().rss;
      v8.writeHeapSnapshot(this.heapSnapshotPath);
      const after = process.memoryUsage().rss;
      // eslint-disable-next-line no-console
      console.warn(`[heap] snapshot (${reason}) rss ${formatBytes(before)} -> ${formatBytes(after)} path=${this.heapSnapshotPath}`);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.warn(`[heap] snapshot failed (${reason}): ${String(err)}`);
    }
  }
}

export function formatBytes(bytes: number): string {
  const units = ["b", "kb", "mb", "gb"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  const digits = idx === 0 ? 0 : 1;
  return `${value.toFixed(digits)}${units[idx]}`;
}
