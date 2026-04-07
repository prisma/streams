import {
  existsSync,
  mkdirSync,
  readdirSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, relative } from "node:path";
import { Result } from "better-result";
import { LruCache } from "../util/lru";

export type LexiconFileCacheError = { kind: "invalid_lexicon_cache"; message: string };

export type MappedLexiconFile = {
  objectKey: string;
  path: string;
  bytes: Uint8Array;
  sizeBytes: number;
};

type FileEntry = {
  path: string;
  size: number;
  mtimeMs: number;
};

export type LexiconFileCacheStats = {
  usedBytes: number;
  entryCount: number;
  mappedBytes: number;
  mappedEntryCount: number;
  pinnedEntryCount: number;
};

function invalidLexiconCache<T = never>(message: string): Result<T, LexiconFileCacheError> {
  return Result.err({ kind: "invalid_lexicon_cache", message });
}

export class LexiconFileCache {
  private readonly entries = new Map<string, FileEntry>();
  private readonly pinnedKeys = new Set<string>();
  private readonly mappedFiles: LruCache<string, MappedLexiconFile>;
  private totalBytes = 0;

  constructor(
    private readonly rootDir: string,
    private readonly maxBytes: number,
    mappedEntries: number
  ) {
    if (this.maxBytes > 0) mkdirSync(this.rootDir, { recursive: true });
    this.mappedFiles = new LruCache(Math.max(1, mappedEntries));
    this.loadIndex();
    this.pruneForBudget(0);
  }

  clearMapped(): void {
    this.mappedFiles.clear();
  }

  bytesForObjectKeyPrefix(prefix: string): number {
    let total = 0;
    for (const [objectKey, entry] of this.entries.entries()) {
      if (objectKey.startsWith(prefix)) total += entry.size;
    }
    return total;
  }

  storeBytesResult(objectKey: string, bytes: Uint8Array): Result<string, LexiconFileCacheError> {
    if (this.maxBytes <= 0) return Result.ok("");
    if (bytes.byteLength > this.maxBytes) {
      return invalidLexiconCache(`lexicon cache object exceeds budget: ${objectKey}`);
    }
    this.pruneForBudget(bytes.byteLength);
    const path = this.pathFor(objectKey);
    mkdirSync(dirname(path), { recursive: true });
    const tmpPath = `${path}.tmp-${process.pid}-${Date.now()}`;
    try {
      writeFileSync(tmpPath, bytes);
      renameSync(tmpPath, path);
    } catch (e: unknown) {
      try {
        unlinkSync(tmpPath);
      } catch {
        // ignore temp cleanup failures
      }
      return invalidLexiconCache(String((e as any)?.message ?? e));
    }
    const existing = this.entries.get(objectKey);
    if (existing) this.totalBytes = Math.max(0, this.totalBytes - existing.size);
    this.entries.delete(objectKey);
    this.entries.set(objectKey, { path, size: bytes.byteLength, mtimeMs: Date.now() });
    this.totalBytes += bytes.byteLength;
    return Result.ok(path);
  }

  async ensureLocalFileResult(args: {
    objectKey: string;
    expectedSize: number;
    loadBytes: () => Promise<Uint8Array>;
  }): Promise<Result<string, LexiconFileCacheError>> {
    return await this.ensureLocalFileResultImpl(args.objectKey, args.expectedSize, args.loadBytes);
  }

  async loadMappedFileResult(args: {
    objectKey: string;
    expectedSize: number;
    loadBytes: () => Promise<Uint8Array>;
  }): Promise<Result<MappedLexiconFile, LexiconFileCacheError>> {
    const cached = this.mappedFiles.get(args.objectKey);
    if (cached) {
      this.pinnedKeys.add(args.objectKey);
      this.touch(args.objectKey);
      return Result.ok(cached);
    }

    const localPathRes = await this.ensureLocalFileResultImpl(args.objectKey, args.expectedSize, args.loadBytes);
    if (Result.isError(localPathRes)) return localPathRes;

    const mappedRes = this.mapFileResult(args.objectKey, localPathRes.value, args.expectedSize);
    if (Result.isError(mappedRes)) return mappedRes;
    this.pinnedKeys.add(args.objectKey);
    this.mappedFiles.set(args.objectKey, mappedRes.value);
    this.touch(args.objectKey);
    return mappedRes;
  }

  stats(): LexiconFileCacheStats {
    let mappedBytes = 0;
    let mappedEntryCount = 0;
    for (const mapped of this.mappedFiles.values()) {
      mappedBytes += mapped.sizeBytes;
      mappedEntryCount += 1;
    }
    return {
      usedBytes: this.totalBytes,
      entryCount: this.entries.size,
      mappedBytes,
      mappedEntryCount,
      pinnedEntryCount: this.pinnedKeys.size,
    };
  }

  private async ensureLocalFileResultImpl(
    objectKey: string,
    expectedSize: number,
    loadBytes: () => Promise<Uint8Array>
  ): Promise<Result<string, LexiconFileCacheError>> {
    if (this.maxBytes <= 0) return invalidLexiconCache("lexicon cache disabled");
    const existing = this.entries.get(objectKey);
    if (existing) {
      const stat = this.safeStat(existing.path);
      if (stat && stat.size === expectedSize) {
        this.touch(objectKey);
        return Result.ok(existing.path);
      }
      this.removeEntry(objectKey);
    }
    let bytes: Uint8Array;
    try {
      bytes = await loadBytes();
    } catch (e: unknown) {
      return invalidLexiconCache(String((e as any)?.message ?? e));
    }
    if (bytes.byteLength !== expectedSize) {
      return invalidLexiconCache(`unexpected lexicon object size for ${objectKey}`);
    }
    return this.storeBytesResult(objectKey, bytes);
  }

  private mapFileResult(objectKey: string, path: string, expectedSize: number): Result<MappedLexiconFile, LexiconFileCacheError> {
    const stat = this.safeStat(path);
    if (!stat) return invalidLexiconCache(`missing cached lexicon file ${path}`);
    if (stat.size !== expectedSize) return invalidLexiconCache(`unexpected cached lexicon size for ${objectKey}`);
    let bytes: Uint8Array;
    try {
      bytes = (Bun as any).mmap(path, { shared: true }) as Uint8Array;
    } catch (e: unknown) {
      return invalidLexiconCache(String((e as any)?.message ?? e));
    }
    if (bytes.byteLength !== expectedSize) {
      return invalidLexiconCache(`unexpected mapped lexicon size for ${objectKey}`);
    }
    return Result.ok({ objectKey, path, bytes, sizeBytes: expectedSize });
  }

  private pathFor(objectKey: string): string {
    return join(this.rootDir, objectKey);
  }

  private loadIndex(): void {
    if (!existsSync(this.rootDir)) return;
    const files: Array<{ key: string; path: string; size: number; mtimeMs: number }> = [];
    const walk = (dir: string) => {
      for (const entry of readdirSync(dir, { withFileTypes: true })) {
        const full = join(dir, entry.name);
        if (entry.isDirectory()) {
          walk(full);
          continue;
        }
        if (!entry.isFile()) continue;
        const stat = statSync(full);
        files.push({
          key: relative(this.rootDir, full),
          path: full,
          size: stat.size,
          mtimeMs: stat.mtimeMs,
        });
      }
    };
    walk(this.rootDir);
    files.sort((left, right) => left.mtimeMs - right.mtimeMs);
    for (const file of files) {
      this.entries.set(file.key, { path: file.path, size: file.size, mtimeMs: file.mtimeMs });
      this.totalBytes += file.size;
    }
  }

  private pruneForBudget(incomingBytes: number): void {
    if (this.maxBytes <= 0) return;
    while (this.totalBytes + incomingBytes > this.maxBytes) {
      const next = this.entries.keys().next();
      if (next.done) break;
      const objectKey = next.value as string;
      if (this.pinnedKeys.has(objectKey)) {
        let removed = false;
        for (const candidateKey of this.entries.keys()) {
          if (this.pinnedKeys.has(candidateKey)) continue;
          this.removeEntry(candidateKey);
          removed = true;
          break;
        }
        if (!removed) break;
        continue;
      }
      this.removeEntry(objectKey);
    }
  }

  private touch(objectKey: string): void {
    const entry = this.entries.get(objectKey);
    if (!entry) return;
    this.entries.delete(objectKey);
    this.entries.set(objectKey, { ...entry });
  }

  private removeEntry(objectKey: string): void {
    if (this.pinnedKeys.has(objectKey)) return;
    const entry = this.entries.get(objectKey);
    if (!entry) return;
    try {
      unlinkSync(entry.path);
    } catch {
      // ignore remove failures
    }
    this.totalBytes = Math.max(0, this.totalBytes - entry.size);
    this.entries.delete(objectKey);
    this.mappedFiles.delete(objectKey);
  }

  private safeStat(path: string): { size: number } | null {
    try {
      return existsSync(path) ? { size: statSync(path).size } : null;
    } catch {
      return null;
    }
  }
}
