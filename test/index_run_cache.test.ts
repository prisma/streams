import { describe, expect, test } from "bun:test";
import { IndexRunCache } from "../src/index/run_cache";
import type { IndexRun } from "../src/index/run_format";

function makeRun(id: string): IndexRun {
  return {
    meta: {
      runId: id,
      level: 1,
      startSegment: 0,
      endSegment: 255,
      objectKey: id,
      filterLen: 0,
      recordCount: 1,
    },
    runType: 1,
    filterBytes: new Uint8Array(0),
    fingerprints: [1n],
    postings: [[1]],
  };
}

describe("IndexRunCache", () => {
  test("size hints can keep multiple large active runs resident", () => {
    const cache = new IndexRunCache(32);
    const a = makeRun("a");
    const b = makeRun("b");
    const c = makeRun("c");

    cache.put("a", a, 10);
    cache.put("b", b, 10);
    cache.put("c", c, 10);

    expect(cache.get("a")).toBe(a);
    expect(cache.get("b")).toBe(b);
    expect(cache.get("c")).toBe(c);
    expect(cache.stats().entries).toBe(3);
    expect(cache.stats().usedBytes).toBe(30);
  });
});
