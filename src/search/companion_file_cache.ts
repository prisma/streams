import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readSync,
  readdirSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, relative } from "node:path";
import { Result } from "better-result";
import { type CompanionToc } from "./companion_format";
import { LruCache } from "../util/lru";

export type CompanionFileCacheError = { kind: "invalid_companion_cache"; message: string };

export type MappedCompanionBundle = {
  objectKey: string;
  path: string;
  bytes: Uint8Array;
  toc: CompanionToc;
  sizeBytes: number;
};

type FileEntry = {
  path: string;
  size: number;
  mtimeMs: number;
};

function invalidCompanionCache<T = never>(message: string): Result<T, CompanionFileCacheError> {
  return Result.err({ kind: "invalid_companion_cache", message });
}

export class CompanionFileCache {
  private readonly entries = new Map<string, FileEntry>();
  private readonly mappedBundles: LruCache<string, MappedCompanionBundle>;
  private totalBytes = 0;

  constructor(
    private readonly rootDir: string,
    private readonly maxBytes: number,
    private readonly maxAgeMs: number,
    mappedEntries: number
  ) {
    mkdirSync(this.rootDir, { recursive: true });
    this.mappedBundles = new LruCache(Math.max(1, mappedEntries));
    this.loadIndex();
    this.pruneForBudget(Date.now(), 0, true);
  }

  clearMapped(): void {
    this.mappedBundles.clear();
  }

  bytesForObjectKeyPrefix(prefix: string): number {
    let total = 0;
    for (const [objectKey, entry] of this.entries.entries()) {
      if (objectKey.startsWith(prefix)) total += entry.size;
    }
    return total;
  }

  remove(objectKey: string): void {
    this.removeEntry(objectKey, true);
  }

  storeBytesResult(objectKey: string, bytes: Uint8Array): Result<string, CompanionFileCacheError> {
    if (this.maxBytes <= 0) return invalidCompanionCache("search companion file cache must have a positive size budget");
    if (bytes.byteLength > this.maxBytes) {
      return invalidCompanionCache(`companion cache object exceeds budget: ${objectKey}`);
    }
    this.pruneForBudget(Date.now(), bytes.byteLength, false);
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
      return invalidCompanionCache(String((e as any)?.message ?? e));
    }
    const existing = this.entries.get(objectKey);
    if (existing) this.totalBytes = Math.max(0, this.totalBytes - existing.size);
    const nextEntry = { path, size: bytes.byteLength, mtimeMs: Date.now() };
    this.entries.delete(objectKey);
    this.entries.set(objectKey, nextEntry);
    this.totalBytes += bytes.byteLength;
    this.touch(objectKey);
    return Result.ok(path);
  }

  async loadMappedBundleResult(args: {
    objectKey: string;
    expectedSize: number;
    loadBytes: () => Promise<Uint8Array>;
    decodeToc: (bytes: Uint8Array) => Result<CompanionToc, { message: string }>;
  }): Promise<Result<MappedCompanionBundle, CompanionFileCacheError>> {
    const cached = this.mappedBundles.get(args.objectKey);
    if (cached) {
      this.touch(args.objectKey);
      return Result.ok(cached);
    }

    const localPathRes = await this.ensureLocalFileResult(args.objectKey, args.expectedSize, args.loadBytes);
    if (Result.isError(localPathRes)) return localPathRes;

    let mappedRes = this.mapBundleResult(args.objectKey, localPathRes.value, args.expectedSize, args.decodeToc);
    if (Result.isError(mappedRes)) {
      this.removeEntry(args.objectKey, true);
      const refetchedPathRes = await this.ensureLocalFileResult(args.objectKey, args.expectedSize, args.loadBytes);
      if (Result.isError(refetchedPathRes)) return refetchedPathRes;
      mappedRes = this.mapBundleResult(args.objectKey, refetchedPathRes.value, args.expectedSize, args.decodeToc);
    }
    if (Result.isError(mappedRes)) return mappedRes;

    this.mappedBundles.set(args.objectKey, mappedRes.value);
    this.touch(args.objectKey);
    return mappedRes;
  }

  private async ensureLocalFileResult(
    objectKey: string,
    expectedSize: number,
    loadBytes: () => Promise<Uint8Array>
  ): Promise<Result<string, CompanionFileCacheError>> {
    const existing = this.entries.get(objectKey);
    if (existing) {
      const stat = this.safeStat(existing.path);
      if (stat && stat.size === expectedSize) {
        this.touch(objectKey);
        return Result.ok(existing.path);
      }
      this.removeEntry(objectKey, true);
    }

    let bytes: Uint8Array;
    try {
      bytes = await loadBytes();
    } catch (e: unknown) {
      return invalidCompanionCache(String((e as any)?.message ?? e));
    }
    if (bytes.byteLength !== expectedSize) {
      return invalidCompanionCache(`unexpected companion object size for ${objectKey}`);
    }
    return this.storeBytesResult(objectKey, bytes);
  }

  private mapBundleResult(
    objectKey: string,
    path: string,
    expectedSize: number,
    decodeToc: (bytes: Uint8Array) => Result<CompanionToc, { message: string }>
  ): Result<MappedCompanionBundle, CompanionFileCacheError> {
    const stat = this.safeStat(path);
    if (!stat) return invalidCompanionCache(`missing cached companion file ${path}`);
    if (stat.size !== expectedSize) return invalidCompanionCache(`unexpected cached companion size for ${objectKey}`);
    const tocProbeLength = Math.min(expectedSize, 512);
    const tocProbe = this.readPrefix(path, tocProbeLength);
    if (Result.isError(tocProbe)) return tocProbe;
    const tocRes = decodeToc(tocProbe.value);
    if (Result.isError(tocRes)) return invalidCompanionCache(tocRes.error.message);
    let bytes: Uint8Array;
    try {
      bytes = (Bun as any).mmap(path, { shared: true }) as Uint8Array;
    } catch (e: unknown) {
      return invalidCompanionCache(String((e as any)?.message ?? e));
    }
    if (bytes.byteLength !== expectedSize) {
      return invalidCompanionCache(`unexpected mapped companion size for ${objectKey}`);
    }
    return Result.ok({
      objectKey,
      path,
      bytes,
      toc: tocRes.value,
      sizeBytes: expectedSize,
    });
  }

  private pathFor(objectKey: string): string {
    return join(this.rootDir, objectKey);
  }

  private loadIndex(): void {
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
    if (!existsSync(this.rootDir)) return;
    walk(this.rootDir);
    files.sort((left, right) => left.mtimeMs - right.mtimeMs);
    for (const file of files) {
      this.entries.set(file.key, { path: file.path, size: file.size, mtimeMs: file.mtimeMs });
      this.totalBytes += file.size;
    }
  }

  private pruneForBudget(nowMs: number, incomingBytes: number, allowMappedDeletes: boolean): void {
    if (this.maxAgeMs > 0) {
      for (const [objectKey, entry] of Array.from(this.entries.entries())) {
        if (!allowMappedDeletes && this.mappedBundles.has(objectKey)) continue;
        if (nowMs - entry.mtimeMs <= this.maxAgeMs) continue;
        this.removeEntry(objectKey, allowMappedDeletes);
      }
    }
    if (this.maxBytes <= 0) return;
    while (this.totalBytes + incomingBytes > this.maxBytes) {
      const next = this.entries.keys().next();
      if (next.done) break;
      const objectKey = next.value as string;
      if (!allowMappedDeletes && this.mappedBundles.has(objectKey)) {
        let removed = false;
        for (const candidateKey of this.entries.keys()) {
          if (this.mappedBundles.has(candidateKey)) continue;
          this.removeEntry(candidateKey, false);
          removed = true;
          break;
        }
        if (!removed) break;
        continue;
      }
      this.removeEntry(objectKey, allowMappedDeletes);
    }
  }

  private touch(objectKey: string): void {
    const entry = this.entries.get(objectKey);
    if (!entry) return;
    this.entries.delete(objectKey);
    this.entries.set(objectKey, { ...entry });
  }

  private removeEntry(objectKey: string, allowMappedDelete: boolean): void {
    if (!allowMappedDelete && this.mappedBundles.has(objectKey)) return;
    const entry = this.entries.get(objectKey);
    if (!entry) return;
    try {
      unlinkSync(entry.path);
    } catch {
      // ignore remove failures
    }
    this.totalBytes = Math.max(0, this.totalBytes - entry.size);
    this.entries.delete(objectKey);
    this.mappedBundles.delete(objectKey);
  }

  private safeStat(path: string): { size: number } | null {
    try {
      return existsSync(path) ? { size: statSync(path).size } : null;
    } catch {
      return null;
    }
  }

  private readPrefix(path: string, byteLength: number): Result<Uint8Array, CompanionFileCacheError> {
    try {
      const fd = openSync(path, "r");
      try {
        const out = new Uint8Array(byteLength);
        const bytesRead = readSync(fd, out, 0, byteLength, 0);
        return Result.ok(out.subarray(0, bytesRead));
      } finally {
        closeSync(fd);
      }
    } catch (e: unknown) {
      return invalidCompanionCache(String((e as any)?.message ?? e));
    }
  }
}
