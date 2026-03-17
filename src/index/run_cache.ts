import type { IndexRun } from "./run_format";

export type RunCacheStats = {
  entries: number;
  usedBytes: number;
  maxBytes: number;
  hits: number;
  misses: number;
  evictions: number;
};

export class IndexRunCache {
  private readonly maxBytes: number;
  private readonly entries = new Map<string, { run: IndexRun; size: number }>();
  private usedBytes = 0;
  private hits = 0;
  private misses = 0;
  private evictions = 0;

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes;
  }

  get(key: string): IndexRun | null {
    const entry = this.entries.get(key);
    if (!entry) {
      this.misses += 1;
      return null;
    }
    this.hits += 1;
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.run;
  }

  put(key: string, run: IndexRun, sizeHintBytes?: number): void {
    if (this.maxBytes <= 0) return;
    const size = Math.max(0, sizeHintBytes ?? estimateRunBytes(run));
    if (size > this.maxBytes) return;
    const existing = this.entries.get(key);
    if (existing) {
      this.usedBytes = Math.max(0, this.usedBytes - existing.size);
      this.entries.delete(key);
    }
    while (this.usedBytes + size > this.maxBytes && this.entries.size > 0) {
      const oldestKey = this.entries.keys().next().value as string;
      const evicted = this.entries.get(oldestKey);
      if (evicted) this.usedBytes = Math.max(0, this.usedBytes - evicted.size);
      this.entries.delete(oldestKey);
      this.evictions += 1;
    }
    this.entries.set(key, { run, size });
    this.usedBytes += size;
  }

  remove(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.usedBytes = Math.max(0, this.usedBytes - entry.size);
    this.entries.delete(key);
  }

  stats(): RunCacheStats {
    return {
      entries: this.entries.size,
      usedBytes: this.usedBytes,
      maxBytes: this.maxBytes,
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
    };
  }
}

function estimateRunBytes(run: IndexRun): number {
  let bytes = run.filterBytes ? run.filterBytes.byteLength : 0;
  bytes += run.fingerprints.length * 8;
  if (run.masks) bytes += run.masks.length * 2;
  if (run.postings) {
    for (const ps of run.postings) bytes += ps.length * 4;
  }
  // JS object overhead is significant; scale to avoid undercounting cache usage.
  return bytes * 4;
}
