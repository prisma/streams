import { describe, expect, test } from "bun:test";
import { performance } from "node:perf_hooks";
import { uploadRunPayloadsBoundedResult, type RunPayloadUploadTask } from "../src/index/run_payload_upload";
import type { GetOptions, ObjectStore, PutOptions } from "../src/objectstore/interface";
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
});
