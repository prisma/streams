import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import type { GetOptions, ObjectStore, PutFileOptions, PutOptions, PutResult } from "../src/objectstore/interface";
import { dsError } from "../src/util/ds_error";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentMaxBytes: 1024,
    blockMaxBytes: 512,
    segmentCheckIntervalMs: 50,
    uploadIntervalMs: 50,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

class GapPreservingStore implements ObjectStore {
  readonly attemptedSegmentIndexes: number[] = [];
  private readonly inner = new MockR2Store();

  private captureSegmentIndex(key: string): number | null {
    const match = key.match(/\/segments\/([0-9]{16})\.bin$/);
    if (!match) return null;
    return Number(match[1]);
  }

  private maybeFail(key: string): void {
    const idx = this.captureSegmentIndex(key);
    if (idx == null) return;
    this.attemptedSegmentIndexes.push(idx);
    if (idx === 0) throw dsError(`forced segment upload timeout for ${key}`);
  }

  async put(key: string, data: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    this.maybeFail(key);
    return this.inner.put(key, data, opts);
  }

  async putFile(key: string, path: string, size: number, opts?: PutFileOptions): Promise<PutResult> {
    this.maybeFail(key);
    return this.inner.putFile!(key, path, size, opts);
  }

  async get(key: string, opts?: GetOptions): Promise<Uint8Array | null> {
    return this.inner.get(key, opts);
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    return this.inner.head(key);
  }

  async delete(key: string): Promise<void> {
    return this.inner.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return this.inner.list(prefix);
  }
}

class DelayedCountingStore implements ObjectStore {
  readonly attemptedSegmentIndexes: number[] = [];
  maxConcurrentSegmentPuts = 0;
  private activeSegmentPuts = 0;
  private readonly inner = new MockR2Store();

  constructor(private readonly delayMs: number) {}

  private captureSegmentIndex(key: string): number | null {
    const match = key.match(/\/segments\/([0-9]{16})\.bin$/);
    if (!match) return null;
    return Number(match[1]);
  }

  private async withDelay<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const idx = this.captureSegmentIndex(key);
    if (idx == null) return fn();
    this.attemptedSegmentIndexes.push(idx);
    this.activeSegmentPuts += 1;
    this.maxConcurrentSegmentPuts = Math.max(this.maxConcurrentSegmentPuts, this.activeSegmentPuts);
    try {
      await sleep(this.delayMs);
      return await fn();
    } finally {
      this.activeSegmentPuts = Math.max(0, this.activeSegmentPuts - 1);
    }
  }

  async put(key: string, data: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    return this.withDelay(key, () => this.inner.put(key, data, opts));
  }

  async putFile(key: string, path: string, size: number, opts?: PutFileOptions): Promise<PutResult> {
    return this.withDelay(key, () => this.inner.putFile!(key, path, size, opts));
  }

  async get(key: string, opts?: GetOptions): Promise<Uint8Array | null> {
    return this.inner.get(key, opts);
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    return this.inner.head(key);
  }

  async delete(key: string): Promise<void> {
    return this.inner.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return this.inner.list(prefix);
  }
}

describe("fault injection", () => {
  test("segment upload retries succeed under transient failures", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-fault-"));
    try {
      const cfg = makeConfig(root);
      const os = new MockR2Store({ failPutEvery: 2 });
      const app = createApp(cfg, os);
      const server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/fault_test`, { method: "PUT" });

      const payload = new Uint8Array(600);
      payload.fill(7);
      for (let i = 0; i < 6; i++) {
        const r = await fetch(`${baseUrl}/v1/stream/fault_test`, {
          method: "POST",
          headers: { "content-type": "application/octet-stream" },
          body: payload,
        });
        expect(r.status).toBe(204);
      }

      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        const keys = await os.list("streams/");
        const hasManifest = keys.some((k) => k.endsWith("/manifest.json"));
        const hasSegment = keys.some((k) => k.includes("/segments/") && k.endsWith(".bin"));
        if (hasManifest && hasSegment) break;
        await sleep(50);
      }

      const keys = await os.list("streams/");
      expect(keys.some((k) => k.endsWith("/manifest.json"))).toBe(true);
      expect(keys.some((k) => k.includes("/segments/") && k.endsWith(".bin"))).toBe(true);

      server.stop();
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("uploader does not bypass the earliest missing segment in a stream", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-upload-order-"));
    try {
      const cfg = makeConfig(root, {
        uploadConcurrency: 2,
        objectStoreRetries: 0,
      });
      const os = new GapPreservingStore();
      const app = createApp(cfg, os);
      const server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/order_test`, { method: "PUT" });

      const payload = new Uint8Array(600);
      payload.fill(9);
      for (let i = 0; i < 8; i++) {
        const r = await fetch(`${baseUrl}/v1/stream/order_test`, {
          method: "POST",
          headers: { "content-type": "application/octet-stream" },
          body: payload,
        });
        expect(r.status).toBe(204);
      }

      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        if (os.attemptedSegmentIndexes.length >= 3) break;
        await sleep(50);
      }

      expect(os.attemptedSegmentIndexes.length).toBeGreaterThan(0);
      expect(os.attemptedSegmentIndexes).toContain(0);
      expect(os.attemptedSegmentIndexes.every((idx) => idx <= 1)).toBe(true);

      server.stop();
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("uploader uses full configured concurrency for one stream", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-upload-concurrency-"));
    try {
      const cfg = makeConfig(root, {
        uploadConcurrency: 4,
        uploadIntervalMs: 250,
        objectStoreRetries: 0,
      });
      const os = new DelayedCountingStore(150);
      const app = createApp(cfg, os);
      const server = Bun.serve({ port: 0, fetch: app.fetch });
      const baseUrl = `http://localhost:${server.port}`;

      await fetch(`${baseUrl}/v1/stream/concurrency_test`, { method: "PUT" });

      const payload = new Uint8Array(600);
      payload.fill(3);
      for (let i = 0; i < 16; i++) {
        const r = await fetch(`${baseUrl}/v1/stream/concurrency_test`, {
          method: "POST",
          headers: { "content-type": "application/octet-stream" },
          body: payload,
        });
        expect(r.status).toBe(204);
      }

      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        if (os.maxConcurrentSegmentPuts >= 4) break;
        await sleep(50);
      }

      expect(os.maxConcurrentSegmentPuts).toBeGreaterThanOrEqual(4);
      expect(new Set(os.attemptedSegmentIndexes).has(0)).toBe(true);
      expect(new Set(os.attemptedSegmentIndexes).has(1)).toBe(true);
      expect(new Set(os.attemptedSegmentIndexes).has(2)).toBe(true);
      expect(new Set(os.attemptedSegmentIndexes).has(3)).toBe(true);

      server.stop();
      app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
