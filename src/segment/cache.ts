import { mkdirSync, readdirSync, statSync, unlinkSync, renameSync, existsSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";

export type SegmentCacheStats = {
  hits: number;
  misses: number;
  evictions: number;
  bytesAdded: number;
  usedBytes: number;
  maxBytes: number;
  entryCount: number;
};

export class SegmentDiskCache {
  private readonly rootDir: string;
  private readonly maxBytes: number;
  private readonly entries = new Map<string, { path: string; size: number }>();
  private totalBytes = 0;
  private hits = 0;
  private misses = 0;
  private evictions = 0;
  private bytesAdded = 0;

  constructor(rootDir: string, maxBytes: number) {
    this.rootDir = rootDir;
    this.maxBytes = maxBytes;
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
    const path = this.getPath(objectKey);
    return new Uint8Array(readFileSync(path));
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
    this.entries.set(objectKey, { path: dest, size: sizeBytes });
    this.totalBytes += sizeBytes;
    this.bytesAdded += sizeBytes;
    return true;
  }

  remove(objectKey: string): void {
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
      const entry = this.entries.get(oldestKey);
      if (entry) {
        try {
          unlinkSync(entry.path);
        } catch {
          // ignore
        }
        this.totalBytes = Math.max(0, this.totalBytes - entry.size);
        this.evictions += 1;
      }
      this.entries.delete(oldestKey);
    }
  }

  stats(): SegmentCacheStats {
    return {
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      bytesAdded: this.bytesAdded,
      usedBytes: this.totalBytes,
      maxBytes: this.maxBytes,
      entryCount: this.entries.size,
    };
  }
}
