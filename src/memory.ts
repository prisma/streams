import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import os from "node:os";

const HOST_MEMORY_GUARD_FRACTION = 0.7;
const HOST_MEMORY_HEADROOM_FRACTION = 0.15;
const HOST_MEMORY_HEADROOM_MIN_BYTES = 512 * 1024 * 1024;
const HOST_MEMORY_HEADROOM_MAX_BYTES = 2 * 1024 * 1024 * 1024;
const HOST_MEMORY_HEADROOM_PROCESS_FRACTION = 0.25;
const HOST_MEMORY_HEADROOM_PROCESS_MIN_BYTES = 128 * 1024 * 1024;

export function deriveMemoryPressureLimitBytes(requestedLimitBytes: number, hostTotalBytes = os.totalmem()): number {
  const requested = Math.max(0, Math.floor(requestedLimitBytes));
  if (requested <= 0) return 0;
  if (!Number.isFinite(hostTotalBytes) || hostTotalBytes <= 0) return requested;
  const safeHostCap = Math.max(256 * 1024 * 1024, Math.floor(hostTotalBytes * HOST_MEMORY_GUARD_FRACTION));
  return Math.min(requested, safeHostCap);
}

export function deriveMemoryPressureHeadroomBytes(limitBytes: number, hostTotalBytes = os.totalmem()): number {
  const limit = Math.max(0, Math.floor(limitBytes));
  if (limit <= 0) return 0;
  if (!Number.isFinite(hostTotalBytes) || hostTotalBytes <= 0) {
    return Math.min(limit, HOST_MEMORY_HEADROOM_MIN_BYTES);
  }
  const headroomFromHost = Math.floor(hostTotalBytes * HOST_MEMORY_HEADROOM_FRACTION);
  const headroom = Math.max(HOST_MEMORY_HEADROOM_MIN_BYTES, headroomFromHost);
  return Math.min(limit, Math.min(HOST_MEMORY_HEADROOM_MAX_BYTES, headroom));
}

export function deriveHostMemoryHeadroomActivationBytes(limitBytes: number): number {
  const limit = Math.max(0, Math.floor(limitBytes));
  if (limit <= 0) return 0;
  const activation = Math.max(HOST_MEMORY_HEADROOM_PROCESS_MIN_BYTES, Math.floor(limit * HOST_MEMORY_HEADROOM_PROCESS_FRACTION));
  return Math.min(limit, activation);
}

export function isHostMemoryPressureActive(
  limitBytes: number,
  effectiveBytes: number,
  hostAvailableBytes: number,
  hostTotalBytes = os.totalmem()
): boolean {
  const headroomBytes = deriveMemoryPressureHeadroomBytes(limitBytes, hostTotalBytes);
  if (headroomBytes <= 0 || hostAvailableBytes > headroomBytes) return false;
  return effectiveBytes >= deriveHostMemoryHeadroomActivationBytes(limitBytes);
}

export class MemoryPressureMonitor {
  private readonly limitBytes: number;
  private readonly resumeBytes: number;
  private readonly hostHeadroomBytes: number;
  private readonly hostResumeHeadroomBytes: number;
  private readonly intervalMs: number;
  private readonly onSample?: (rssBytes: number, overLimit: boolean, limitBytes: number) => void;
  private readonly heapSnapshotPath?: string;
  private readonly heapSnapshotMinIntervalMs: number;
  private timer: any | null = null;
  private overLimit = false;
  private maxRssBytes = 0;
  private lastRssBytes = 0;
  private lastGcMs = 0;
  private forcedGcCount = 0;
  private forcedGcReclaimedBytesTotal = 0;
  private lastForcedGcAtMs = 0;
  private lastForcedGcBeforeBytes = 0;
  private lastForcedGcAfterBytes = 0;
  private lastForcedGcReclaimedBytes = 0;
  private lastSnapshotMs = 0;
  private heapSnapshotsWritten = 0;
  private lastDarwinPhysicalBytes = 0;
  private lastDarwinPhysicalAtMs = 0;

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
    const requestedLimitBytes = Math.max(0, Math.floor(limitBytes));
    this.limitBytes = deriveMemoryPressureLimitBytes(requestedLimitBytes);
    if (requestedLimitBytes > 0 && this.limitBytes < requestedLimitBytes) {
      // eslint-disable-next-line no-console
      console.warn(
        `[memory] clamped limit from ${formatBytes(requestedLimitBytes)} to ${formatBytes(this.limitBytes)} based on host memory`
      );
    }
    // Resume as soon as RSS drops back below the limit by default (no hysteresis),
    // so the server doesn't "deadlock" itself under a stable high-water mark.
    const resumeFraction = Math.min(1.0, Math.max(0.5, opts.resumeFraction ?? 1.0));
    this.resumeBytes = Math.floor(this.limitBytes * resumeFraction);
    this.hostHeadroomBytes = deriveMemoryPressureHeadroomBytes(this.limitBytes);
    this.hostResumeHeadroomBytes = Math.floor(this.hostHeadroomBytes * 1.25);
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
    const effectiveBytes = this.effectiveBytesForGuard(rss);
    const hostAvailableBytes = readHostAvailableMemoryBytes();
    this.lastRssBytes = rss;
    if (rss > this.maxRssBytes) this.maxRssBytes = rss;
    const hostLowMemory = isHostMemoryPressureActive(this.limitBytes, effectiveBytes, hostAvailableBytes);
    const overLimit = this.limitBytes > 0 && (effectiveBytes > this.limitBytes || hostLowMemory);
    if (this.onSample) {
      try {
        this.onSample(rss, overLimit, this.limitBytes);
      } catch {
        // ignore
      }
    }
    if (this.limitBytes <= 0) return;
    if (overLimit) {
      this.maybeGc(hostLowMemory ? "host memory headroom" : "memory sample");
      this.maybeHeapSnapshot(hostLowMemory ? "host memory headroom" : "memory sample");
    }
    if (this.overLimit) {
      if (effectiveBytes <= this.resumeBytes && hostAvailableBytes > this.hostResumeHeadroomBytes) this.overLimit = false;
    } else if (effectiveBytes > this.limitBytes) {
      this.overLimit = true;
    } else if (hostLowMemory) {
      this.overLimit = true;
    }
  }

  private effectiveBytesForGuard(rssBytes: number): number {
    if (this.limitBytes <= 0 || rssBytes <= this.limitBytes) return rssBytes;
    if (process.platform !== "darwin") return rssBytes;
    const now = Date.now();
    if (this.lastDarwinPhysicalAtMs !== 0 && now - this.lastDarwinPhysicalAtMs < 5_000) {
      return this.lastDarwinPhysicalBytes > 0 ? this.lastDarwinPhysicalBytes : this.limitBytes;
    }
    this.lastDarwinPhysicalAtMs = now;
    const physicalBytes = readDarwinTopMemBytes(process.pid);
    if (physicalBytes != null) {
      this.lastDarwinPhysicalBytes = physicalBytes;
      return physicalBytes;
    }
    if (this.lastDarwinPhysicalBytes > 0) return this.lastDarwinPhysicalBytes;
    this.lastDarwinPhysicalBytes = this.limitBytes;
    return this.lastDarwinPhysicalBytes;
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

  getGcStats(): {
    forced_gc_count: number;
    forced_gc_reclaimed_bytes_total: number;
    last_forced_gc_at_ms: number | null;
    last_forced_gc_before_bytes: number | null;
    last_forced_gc_after_bytes: number | null;
    last_forced_gc_reclaimed_bytes: number | null;
    heap_snapshots_written: number;
    last_heap_snapshot_at_ms: number | null;
  } {
    return {
      forced_gc_count: this.forcedGcCount,
      forced_gc_reclaimed_bytes_total: this.forcedGcReclaimedBytesTotal,
      last_forced_gc_at_ms: this.lastForcedGcAtMs > 0 ? this.lastForcedGcAtMs : null,
      last_forced_gc_before_bytes: this.lastForcedGcAtMs > 0 ? this.lastForcedGcBeforeBytes : null,
      last_forced_gc_after_bytes: this.lastForcedGcAtMs > 0 ? this.lastForcedGcAfterBytes : null,
      last_forced_gc_reclaimed_bytes: this.lastForcedGcAtMs > 0 ? this.lastForcedGcReclaimedBytes : null,
      heap_snapshots_written: this.heapSnapshotsWritten,
      last_heap_snapshot_at_ms: this.lastSnapshotMs > 0 ? this.lastSnapshotMs : null,
    };
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
    const reclaimed = Math.max(0, before - after);
    this.forcedGcCount += 1;
    this.forcedGcReclaimedBytesTotal += reclaimed;
    this.lastForcedGcAtMs = now;
    this.lastForcedGcBeforeBytes = before;
    this.lastForcedGcAfterBytes = after;
    this.lastForcedGcReclaimedBytes = reclaimed;
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
      this.heapSnapshotsWritten += 1;
      // eslint-disable-next-line no-console
      console.warn(`[heap] snapshot (${reason}) rss ${formatBytes(before)} -> ${formatBytes(after)} path=${this.heapSnapshotPath}`);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.warn(`[heap] snapshot failed (${reason}): ${String(err)}`);
    }
  }
}

export function parseDarwinTopMemBytes(output: string, pid: number): number | null {
  const line = output
    .split(/\r?\n/)
    .map((entry) => entry.trim())
    .find((entry) => new RegExp(`^${pid}\\s+`).test(entry));
  if (!line) return null;
  const match = line.match(new RegExp(`^${pid}\\s+([0-9]+(?:\\.[0-9]+)?)([BKMGTP])\\+?\\b`, "i"));
  if (!match) return null;
  const value = Number(match[1]);
  if (!Number.isFinite(value)) return null;
  const unit = match[2]!.toUpperCase();
  const power =
    unit === "B"
      ? 0
      : unit === "K"
        ? 1
        : unit === "M"
          ? 2
          : unit === "G"
            ? 3
            : unit === "T"
              ? 4
              : unit === "P"
                ? 5
                : -1;
  if (power < 0) return null;
  return Math.round(value * 1024 ** power);
}

export function darwinTopMemArgs(pid: number): string[] {
  return ["-l", "1", "-pid", String(pid), "-stats", "pid,mem"];
}

export function parseLinuxMemAvailableBytes(meminfo: string): number | null {
  for (const line of meminfo.split(/\r?\n/)) {
    const match = line.match(/^MemAvailable:\s+([0-9]+)\s+kB$/i);
    if (!match) continue;
    const kb = Number(match[1]);
    if (!Number.isFinite(kb) || kb < 0) return null;
    return kb * 1024;
  }
  return null;
}

function readLinuxMemAvailableBytes(): number | null {
  try {
    const meminfo = readFileSync("/proc/meminfo", "utf8");
    return parseLinuxMemAvailableBytes(meminfo);
  } catch {
    return null;
  }
}

function readHostAvailableMemoryBytes(): number {
  if (process.platform === "linux") {
    const available = readLinuxMemAvailableBytes();
    if (available != null) return available;
  }
  return os.freemem();
}

function readDarwinTopMemBytes(pid: number): number | null {
  try {
    const output = execFileSync("/usr/bin/top", darwinTopMemArgs(pid), {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      timeout: 5_000,
      maxBuffer: 256 * 1024,
    });
    return parseDarwinTopMemBytes(output, pid);
  } catch {
    return null;
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
