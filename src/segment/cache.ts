import { mkdirSync, readdirSync, statSync, unlinkSync, renameSync, existsSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { LruCache } from "../util/lru";

export type MappedSegmentFile = {
  objectKey: string;
  path: string;
  bytes: Uint8Array;
  sizeBytes: number;
};

export type SegmentCacheStats = {
  hits: number;
  misses: number;
  evictions: number;
  bytesAdded: number;
  usedBytes: number;
  maxBytes: number;
  entryCount: number;
  mappedBytes: number;
  mappedEntryCount: number;
  pinnedEntryCount: number;
};

export class SegmentDiskCache {
  private readonly rootDir: string;
  private readonly maxBytes: number;
  private readonly entries = new Map<string, { path: string; size: number }>();
  private readonly pinnedKeys = new Set<string>();
  private readonly mappedFiles: LruCache<string, MappedSegmentFile>;
  private totalBytes = 0;
  private hits = 0;
  private misses = 0;
  private evictions = 0;
  private bytesAdded = 0;

  constructor(rootDir: string, maxBytes: number, mappedEntries = 64) {
    this.rootDir = rootDir;
    this.maxBytes = maxBytes;
    this.mappedFiles = new LruCache(Math.max(1, mappedEntries));
    if (this.maxBytes > 0) {
      mkdirSync(this.rootDir, { recursive: true });
      this.loadIndex();
    }
  }

  private loadIndex(): void {
    if (!existsSync(this.rootDir)) return;
    const files: Array<{ key: string; path: string; size: number; mtimeMs: number }> = [];
    const walk = (dir: string) => {
      for (const entry of readdirSync(dir, { withFileTypes: true })) {
        const full = join(dir, entry.name);
        if (entry.isDirectory()) {
          walk(full);
        } else if (entry.isFile()) {
          const stat = statSync(full);
          const key = relative(this.rootDir, full);
          files.push({ key, path: full, size: stat.size, mtimeMs: stat.mtimeMs });
        }
      }
    };
    walk(this.rootDir);
    files.sort((a, b) => a.mtimeMs - b.mtimeMs);
    for (const f of files) {
      this.entries.set(f.key, { path: f.path, size: f.size });
      this.totalBytes += f.size;
    }
    this.evictIfNeeded(0);
  }

  getPath(objectKey: string): string {
    return join(this.rootDir, objectKey);
  }

  has(objectKey: string): boolean {
    const exists = this.entries.has(objectKey) && existsSync(this.getPath(objectKey));
    if (!exists) this.entries.delete(objectKey);
    return exists;
  }

  touch(objectKey: string): void {
    const entry = this.entries.get(objectKey);
    if (!entry) return;
    this.entries.delete(objectKey);
    this.entries.set(objectKey, entry);
  }

  recordHit(): void {
    this.hits += 1;
  }

  recordMiss(): void {
    this.misses += 1;
  }

  get(objectKey: string): Uint8Array | null {
    if (!this.has(objectKey)) {
      this.recordMiss();
      return null;
    }
    this.recordHit();
    this.touch(objectKey);
    const mapped = this.getMapped(objectKey);
    if (mapped) return mapped.bytes;
    const path = this.getPath(objectKey);
    return readFileSync(path);
  }

  getMapped(objectKey: string): MappedSegmentFile | null {
    if (!this.has(objectKey)) return null;
    const cached = this.mappedFiles.get(objectKey);
    if (cached) {
      this.pinnedKeys.add(objectKey);
      this.touch(objectKey);
      return cached;
    }

    const path = this.getPath(objectKey);
    let sizeBytes: number;
    try {
      sizeBytes = statSync(path).size;
    } catch {
      this.entries.delete(objectKey);
      return null;
    }

    let bytes: Uint8Array;
    try {
      bytes = (Bun as any).mmap(path, { shared: true }) as Uint8Array;
    } catch {
      return null;
    }
    if (bytes.byteLength !== sizeBytes) return null;

    const mapped = { objectKey, path, bytes, sizeBytes };
    this.mappedFiles.set(objectKey, mapped);
    this.pinnedKeys.add(objectKey);
    this.touch(objectKey);
    return mapped;
  }

  put(objectKey: string, bytes: Uint8Array): boolean {
    if (this.maxBytes <= 0) return false;
    const sizeBytes = bytes.byteLength;
    if (sizeBytes > this.maxBytes) return false;
    this.evictIfNeeded(sizeBytes);
    const dest = this.getPath(objectKey);
    mkdirSync(dirname(dest), { recursive: true });
    const tmp = `${dest}.tmp-${Date.now()}`;
    try {
      writeFileSync(tmp, bytes);
      renameSync(tmp, dest);
    } catch {
      try {
        unlinkSync(tmp);
      } catch {
        // ignore
      }
      return false;
    }
    const existing = this.entries.get(objectKey);
    if (existing) this.totalBytes = Math.max(0, this.totalBytes - existing.size);
    this.mappedFiles.delete(objectKey);
    this.entries.set(objectKey, { path: dest, size: sizeBytes });
    this.totalBytes += sizeBytes;
    this.bytesAdded += sizeBytes;
    return true;
  }

  putFromLocal(objectKey: string, localPath: string, sizeBytes: number): boolean {
    if (this.maxBytes <= 0) return false;
    if (sizeBytes > this.maxBytes) return false;
    this.evictIfNeeded(sizeBytes);
    const dest = this.getPath(objectKey);
    mkdirSync(dirname(dest), { recursive: true });
    try {
      renameSync(localPath, dest);
    } catch {
      return false;
    }
    const existing = this.entries.get(objectKey);
    if (existing) this.totalBytes = Math.max(0, this.totalBytes - existing.size);
    this.mappedFiles.delete(objectKey);
    this.entries.set(objectKey, { path: dest, size: sizeBytes });
    this.totalBytes += sizeBytes;
    this.bytesAdded += sizeBytes;
    return true;
  }

  remove(objectKey: string): void {
    if (this.pinnedKeys.has(objectKey)) return;
    const entry = this.entries.get(objectKey);
    if (!entry) return;
    try {
      unlinkSync(entry.path);
    } catch {
      // ignore
    }
    this.totalBytes = Math.max(0, this.totalBytes - entry.size);
    this.entries.delete(objectKey);
  }

  private evictIfNeeded(incomingBytes: number): void {
    while (this.totalBytes + incomingBytes > this.maxBytes && this.entries.size > 0) {
      const oldestKey = this.entries.keys().next().value as string;
      if (this.pinnedKeys.has(oldestKey)) {
        let removed = false;
        for (const candidateKey of this.entries.keys()) {
          if (this.pinnedKeys.has(candidateKey)) continue;
          const candidate = this.entries.get(candidateKey);
          if (!candidate) continue;
          try {
            unlinkSync(candidate.path);
          } catch {
            // ignore
          }
          this.totalBytes = Math.max(0, this.totalBytes - candidate.size);
          this.entries.delete(candidateKey);
          this.mappedFiles.delete(candidateKey);
          this.evictions += 1;
          removed = true;
          break;
        }
        if (!removed) break;
        continue;
      }
      const entry = this.entries.get(oldestKey);
      if (entry) {
        try {
          unlinkSync(entry.path);
        } catch {
          // ignore
        }
        this.totalBytes = Math.max(0, this.totalBytes - entry.size);
        this.mappedFiles.delete(oldestKey);
        this.evictions += 1;
      }
      this.entries.delete(oldestKey);
    }
  }

  stats(): SegmentCacheStats {
    let mappedBytes = 0;
    let mappedEntryCount = 0;
    for (const mapped of this.mappedFiles.values()) {
      mappedBytes += mapped.sizeBytes;
      mappedEntryCount += 1;
    }
    return {
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      bytesAdded: this.bytesAdded,
      usedBytes: this.totalBytes,
      maxBytes: this.maxBytes,
      entryCount: this.entries.size,
      mappedBytes,
      mappedEntryCount,
      pinnedEntryCount: this.pinnedKeys.size,
    };
  }

  bytesForObjectKeyPrefix(prefix: string): number {
    let total = 0;
    for (const [objectKey, entry] of this.entries.entries()) {
      if (objectKey.startsWith(prefix)) total += entry.size;
    }
    return total;
  }
}
