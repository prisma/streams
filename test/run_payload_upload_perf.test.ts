import { describe, expect, test } from "bun:test";
import { performance } from "node:perf_hooks";
import { uploadRunPayloadsBoundedResult, type RunPayloadUploadTask } from "../src/index/run_payload_upload";
import type { GetOptions, ObjectStore, PutFileNoEtagOptions, PutNoEtagOptions, PutOptions } from "../src/objectstore/interface";
import { Result } from "better-result";

class DelayedObjectStore implements ObjectStore {
  constructor(private readonly delayMs: number) {}

  async put(_key: string, _data: Uint8Array, _opts: PutOptions = {}): Promise<{ etag: string }> {
    await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    return { etag: "etag" };
  }

  async get(_key: string, _opts?: GetOptions): Promise<Uint8Array | null> {
    return null;
  }

  async head(_key: string): Promise<{ etag: string; size: number } | null> {
    return null;
  }

  async delete(_key: string): Promise<void> {}

  async list(_prefix: string): Promise<string[]> {
    return [];
  }
}

class FastPathProbeObjectStore implements ObjectStore {
  putCalls = 0;
  putNoEtagCalls = 0;
  putFileNoEtagCalls = 0;

  async put(_key: string, data: Uint8Array, _opts: PutOptions = {}): Promise<{ etag: string }> {
    this.putCalls += 1;
    return { etag: `etag-${data.byteLength}` };
  }

  async putNoEtag(_key: string, data: Uint8Array, _opts: PutNoEtagOptions = {}): Promise<number> {
    this.putNoEtagCalls += 1;
    return data.byteLength;
  }

  async putFileNoEtag(_key: string, _path: string, size: number, _opts: PutFileNoEtagOptions = {}): Promise<number> {
    this.putFileNoEtagCalls += 1;
    return size;
  }

  async get(_key: string, _opts?: GetOptions): Promise<Uint8Array | null> {
    return null;
  }

  async head(_key: string): Promise<{ etag: string; size: number } | null> {
    return null;
  }

  async delete(_key: string): Promise<void> {}

  async list(_prefix: string): Promise<string[]> {
    return [];
  }
}

async function uploadSequentially(tasks: RunPayloadUploadTask[], os: ObjectStore): Promise<number[]> {
  const out: number[] = [];
  for (const task of tasks) {
    await os.put(task.objectKey, task.payload, { contentLength: task.payload.byteLength });
    out.push(task.payload.byteLength);
  }
  return out;
}

describe("run payload upload performance", () => {
  test("uploads exact-index run payloads with at least 25% speedup using bounded parallelism", async () => {
    const tasks = Array.from({ length: 11 }, (_, index) => ({
      objectKey: `streams/test/secondary-index/run-${index}`,
      payload: new Uint8Array(64 * 1024).fill(index + 1),
    })) satisfies RunPayloadUploadTask[];
    const legacyStore = new DelayedObjectStore(30);
    const currentStore = new DelayedObjectStore(30);

    await uploadSequentially(tasks, legacyStore);
    const warmRes = await uploadRunPayloadsBoundedResult({
      tasks,
      os: currentStore,
      concurrency: 4,
      retries: 0,
      baseDelayMs: 1,
      maxDelayMs: 1,
      timeoutMs: 5_000,
    });
    expect(Result.isOk(warmRes)).toBe(true);

    const legacyStarted = performance.now();
    const legacySizes = await uploadSequentially(tasks, legacyStore);
    const legacyElapsedMs = performance.now() - legacyStarted;

    const currentStarted = performance.now();
    const currentRes = await uploadRunPayloadsBoundedResult({
      tasks,
      os: currentStore,
      concurrency: 4,
      retries: 0,
      baseDelayMs: 1,
      maxDelayMs: 1,
      timeoutMs: 5_000,
    });
    const currentElapsedMs = performance.now() - currentStarted;

    expect(Result.isOk(currentRes)).toBe(true);
    if (Result.isOk(currentRes)) expect(currentRes.value).toEqual(legacySizes);
    expect(currentElapsedMs).toBeLessThan(legacyElapsedMs * 0.75);
  }, 30_000);

  test("prefers file-backed no-etag uploads for local run artifacts", async () => {
    const smallTmpPath = `/tmp/run-upload-fast-small-${process.pid}-${Date.now()}.bin`;
    const largeTmpPath = `/tmp/run-upload-fast-large-${process.pid}-${Date.now()}.bin`;
    await Bun.write(smallTmpPath, new Uint8Array([7, 8, 9]));
    await Bun.write(largeTmpPath, new Uint8Array(3 * 1024 * 1024).fill(4));

    const os = new FastPathProbeObjectStore();
    const res = await uploadRunPayloadsBoundedResult({
      tasks: [
        { objectKey: "streams/test/secondary-index/bytes", payload: new Uint8Array([1, 2, 3]) },
        { objectKey: "streams/test/secondary-index/file-small", localPath: smallTmpPath, sizeBytes: 3 },
        { objectKey: "streams/test/secondary-index/file-large", localPath: largeTmpPath, sizeBytes: 3 * 1024 * 1024 },
      ],
      os,
      concurrency: 2,
      retries: 0,
      baseDelayMs: 1,
      maxDelayMs: 1,
      timeoutMs: 1_000,
    });

    expect(Result.isOk(res)).toBe(true);
    if (Result.isOk(res)) expect(res.value).toEqual([3, 3, 3 * 1024 * 1024]);
    expect(os.putNoEtagCalls).toBe(1);
    expect(os.putFileNoEtagCalls).toBe(2);
    expect(os.putCalls).toBe(0);
  });
});
