import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync, unlinkSync, openSync, closeSync, readSync, copyFileSync, createReadStream } from "node:fs";
import { dirname, join } from "node:path";
import type { GetFileResult, GetOptions, ObjectStore, PutFileOptions, PutOptions, PutResult } from "./interface";
import { dsError } from "../util/ds_error.ts";

export type MockR2Faults = {
  putDelayMs?: number;
  getDelayMs?: number;
  headDelayMs?: number;
  listDelayMs?: number;
  failPutPrefix?: string; // fail PUT when key starts with prefix
  failPutEvery?: number; // fail every Nth PUT
  failGetEvery?: number; // fail every Nth GET
  failHeadEvery?: number;
  failListEvery?: number;
  timeoutPutEvery?: number;
  timeoutGetEvery?: number;
  timeoutHeadEvery?: number;
  timeoutListEvery?: number;
  partialGetEvery?: number; // return truncated bytes every Nth GET
};

export type MockR2Options = {
  faults?: MockR2Faults;
  maxInMemoryBytes?: number; // spill when object exceeds this
  spillDir?: string; // optional directory for large objects
};

type StoredObject = {
  etag: string;
  size: number;
  bytes?: Uint8Array;
  path?: string;
};

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((res) => setTimeout(res, ms));
}

function abortError(signal?: AbortSignal): Error {
  if (signal?.reason instanceof Error) return signal.reason;
  return dsError("aborted");
}

async function sleepWithSignal(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) throw abortError(signal);
  if (ms <= 0) return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(abortError(signal));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export class MockR2Store implements ObjectStore {
  private readonly data = new Map<string, StoredObject>();
  private readonly faults: MockR2Faults;
  private readonly maxInMemoryBytes: number;
  private readonly spillDir?: string;

  private putCount = 0;
  private getCount = 0;
  private headCount = 0;
  private listCount = 0;
  private memBytes = 0;

  constructor(opts: MockR2Options | MockR2Faults = {}) {
    if ("failPutEvery" in opts || "putDelayMs" in opts) {
      this.faults = opts as MockR2Faults;
      this.maxInMemoryBytes = Number.POSITIVE_INFINITY;
    } else {
      const o = opts as MockR2Options;
      this.faults = o.faults ?? {};
      this.maxInMemoryBytes = o.maxInMemoryBytes ?? Number.POSITIVE_INFINITY;
      this.spillDir = o.spillDir;
    }
  }

  private mkEtag(bytes: Uint8Array): string {
    return createHash("sha256").update(bytes).digest("hex");
  }

  private async hashFile(path: string): Promise<string> {
    const hash = createHash("sha256");
    await new Promise<void>((resolve, reject) => {
      const stream = createReadStream(path);
      stream.on("data", (chunk) => hash.update(chunk));
      stream.on("error", (err) => reject(err));
      stream.on("end", () => resolve());
    });
    return hash.digest("hex");
  }

  private mkPath(key: string): string {
    const hex = createHash("sha256").update(key).digest("hex");
    return this.spillDir ? join(this.spillDir, `${hex}.bin`) : hex;
  }

  private shouldSpill(len: number): boolean {
    return this.spillDir != null && len > this.maxInMemoryBytes;
  }

  private maybeFail(count: number, every?: number, msg?: string): void {
    if (every && count % every === 0) throw dsError(msg ?? "MockR2: injected failure");
  }

  private maybeTimeout(count: number, every?: number, msg?: string): void {
    if (every && count % every === 0) throw dsError(msg ?? "MockR2: injected timeout");
  }

  async put(key: string, bytes: Uint8Array, opts: PutOptions = {}): Promise<PutResult> {
    this.putCount++;
    if (this.faults.failPutPrefix && key.startsWith(this.faults.failPutPrefix)) {
      throw dsError(`MockR2: injected PUT failure for ${key}`);
    }
    this.maybeFail(this.putCount, this.faults.failPutEvery, `MockR2: injected PUT failure for ${key}`);
    this.maybeTimeout(this.putCount, this.faults.timeoutPutEvery, `MockR2: injected PUT timeout for ${key}`);
    await sleepWithSignal(this.faults.putDelayMs ?? 0, opts.signal);
    if (opts.signal?.aborted) throw abortError(opts.signal);

    const copy = new Uint8Array(bytes);
    const etag = this.mkEtag(copy);
    const size = copy.byteLength;

    const existing = this.data.get(key);
    if (existing?.bytes) this.memBytes -= existing.bytes.byteLength;
    if (existing?.path) {
      try { unlinkSync(existing.path); } catch { /* ignore */ }
    }

    if (this.shouldSpill(size)) {
      const path = this.mkPath(key);
      mkdirSync(dirname(path), { recursive: true });
      writeFileSync(path, copy);
      this.data.set(key, { etag, size, path });
    } else {
      this.memBytes += size;
      this.data.set(key, { etag, size, bytes: copy });
    }

    return { etag };
  }

  async putFile(key: string, path: string, size: number, opts: PutFileOptions = {}): Promise<PutResult> {
    this.putCount++;
    if (this.faults.failPutPrefix && key.startsWith(this.faults.failPutPrefix)) {
      throw dsError(`MockR2: injected PUT failure for ${key}`);
    }
    this.maybeFail(this.putCount, this.faults.failPutEvery, `MockR2: injected PUT failure for ${key}`);
    this.maybeTimeout(this.putCount, this.faults.timeoutPutEvery, `MockR2: injected PUT timeout for ${key}`);
    await sleepWithSignal(this.faults.putDelayMs ?? 0, opts.signal);
    if (opts.signal?.aborted) throw abortError(opts.signal);

    const etag = await this.hashFile(path);

    const existing = this.data.get(key);
    if (existing?.bytes) this.memBytes -= existing.bytes.byteLength;
    if (existing?.path) {
      try { unlinkSync(existing.path); } catch { /* ignore */ }
    }

    if (this.shouldSpill(size)) {
      const dest = this.mkPath(key);
      mkdirSync(dirname(dest), { recursive: true });
      copyFileSync(path, dest);
      this.data.set(key, { etag, size, path: dest });
    } else {
      const bytes = new Uint8Array(await Bun.file(path).arrayBuffer());
      this.memBytes += bytes.byteLength;
      this.data.set(key, { etag, size, bytes });
    }

    return { etag };
  }

  async get(key: string, opts: GetOptions = {}): Promise<Uint8Array | null> {
    this.getCount++;
    this.maybeFail(this.getCount, this.faults.failGetEvery, `MockR2: injected GET failure for ${key}`);
    this.maybeTimeout(this.getCount, this.faults.timeoutGetEvery, `MockR2: injected GET timeout for ${key}`);
    await sleep(this.faults.getDelayMs ?? 0);

    const entry = this.data.get(key);
    if (!entry) return null;

    const range = opts.range;
    const total = entry.size;
    const start = range?.start ?? 0;
    const end = range?.end ?? total - 1;
    if (start >= total) return new Uint8Array(0);
    const clampEnd = Math.min(end, total - 1);
    const length = Math.max(0, clampEnd - start + 1);

    let out: Uint8Array;
    if (entry.bytes) {
      out = entry.bytes.slice(start, start + length);
    } else if (entry.path) {
      if (length === total) {
        out = readFileSync(entry.path);
      } else {
        const fd = openSync(entry.path, "r");
        try {
          const buf = new Uint8Array(length);
          readSync(fd, buf, 0, length, start);
          out = buf;
        } finally {
          closeSync(fd);
        }
      }
    } else {
      return null;
    }

    if (this.faults.partialGetEvery && this.getCount % this.faults.partialGetEvery === 0) {
      const half = Math.max(0, Math.floor(out.byteLength / 2));
      return out.slice(0, half);
    }

    return out;
  }

  async getFile(key: string, path: string, opts: GetOptions = {}): Promise<GetFileResult | null> {
    this.getCount++;
    this.maybeFail(this.getCount, this.faults.failGetEvery, `MockR2: injected GET failure for ${key}`);
    this.maybeTimeout(this.getCount, this.faults.timeoutGetEvery, `MockR2: injected GET timeout for ${key}`);
    await sleep(this.faults.getDelayMs ?? 0);

    const entry = this.data.get(key);
    if (!entry) return null;

    const range = opts.range;
    const total = entry.size;
    const start = range?.start ?? 0;
    const end = range?.end ?? total - 1;
    if (start >= total) {
      mkdirSync(dirname(path), { recursive: true });
      writeFileSync(path, new Uint8Array(0));
      return { size: 0 };
    }
    const clampEnd = Math.min(end, total - 1);
    const length = Math.max(0, clampEnd - start + 1);

    mkdirSync(dirname(path), { recursive: true });
    if (entry.path && start === 0 && length === total) {
      copyFileSync(entry.path, path);
      return { size: total };
    }

    let out: Uint8Array;
    if (entry.bytes) {
      out = entry.bytes.slice(start, start + length);
    } else if (entry.path) {
      const fd = openSync(entry.path, "r");
      try {
        const buf = new Uint8Array(length);
        readSync(fd, buf, 0, length, start);
        out = buf;
      } finally {
        closeSync(fd);
      }
    } else {
      return null;
    }

    if (this.faults.partialGetEvery && this.getCount % this.faults.partialGetEvery === 0) {
      out = out.slice(0, Math.max(0, Math.floor(out.byteLength / 2)));
    }

    writeFileSync(path, out);
    return { size: out.byteLength };
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    this.headCount++;
    this.maybeFail(this.headCount, this.faults.failHeadEvery, `MockR2: injected HEAD failure for ${key}`);
    this.maybeTimeout(this.headCount, this.faults.timeoutHeadEvery, `MockR2: injected HEAD timeout for ${key}`);
    await sleep(this.faults.headDelayMs ?? 0);

    const v = this.data.get(key);
    if (!v) return null;
    return { etag: v.etag, size: v.size };
  }

  async delete(key: string): Promise<void> {
    const v = this.data.get(key);
    if (v?.bytes) this.memBytes -= v.bytes.byteLength;
    if (v?.path) {
      try { unlinkSync(v.path); } catch { /* ignore */ }
    }
    this.data.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    this.listCount++;
    this.maybeFail(this.listCount, this.faults.failListEvery, "MockR2: injected LIST failure");
    this.maybeTimeout(this.listCount, this.faults.timeoutListEvery, "MockR2: injected LIST timeout");
    await sleep(this.faults.listDelayMs ?? 0);

    const out: string[] = [];
    for (const k of this.data.keys()) {
      if (k.startsWith(prefix)) out.push(k);
    }
    out.sort();
    return out;
  }

  // Helpers for tests
  has(key: string): boolean {
    return this.data.has(key);
  }

  size(): number {
    return this.data.size;
  }

  memoryBytes(): number {
    return this.memBytes;
  }

  stats(): { puts: number; gets: number; heads: number; lists: number; memoryBytes: number } {
    return {
      puts: this.putCount,
      gets: this.getCount,
      heads: this.headCount,
      lists: this.listCount,
      memoryBytes: this.memBytes,
    };
  }

  resetStats(): void {
    this.putCount = 0;
    this.getCount = 0;
    this.headCount = 0;
    this.listCount = 0;
  }
}
