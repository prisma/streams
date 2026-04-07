import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { BackpressureGate } from "../src/backpressure";
import { IndexSegmentLocalityManager } from "../src/index/segment_locality";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { SegmentDiskCache } from "../src/segment/cache";
import { segmentObjectKey, streamHash16Hex } from "../src/util/stream_paths";

function makeSegment(stream: string, segmentIndex: number, size: number): any {
  return {
    segment_id: `${stream}-${segmentIndex}`,
    stream,
    segment_index: segmentIndex,
    local_path: "",
    size_bytes: size,
  };
}

describe("index segment locality", () => {
  test("required routing build segments block eviction and clear after release", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-index-locality-"));
    try {
      const stream = "locality";
      const shash = streamHash16Hex(stream);
      const cache = new SegmentDiskCache(root, 10);
      const gate = new BackpressureGate(1024, 0);
      const store = new MockR2Store();
      const manager = new IndexSegmentLocalityManager(cache, store, gate);

      await store.put(segmentObjectKey(shash, 0), new Uint8Array([1, 1, 1, 1, 1]));
      await store.put(segmentObjectKey(shash, 1), new Uint8Array([2, 2, 2, 2, 2]));
      await store.put(segmentObjectKey(shash, 2), new Uint8Array([3, 3, 3, 3, 3]));

      const leaseRes = await manager.acquireRoutingWindowResult(stream, [makeSegment(stream, 0, 5), makeSegment(stream, 1, 5)]);
      expect(Result.isOk(leaseRes)).toBe(true);
      if (Result.isError(leaseRes)) return;
      expect(cache.stats().requiredEntryCount).toBe(2);

      const blockedRes = await manager.acquireRoutingWindowResult(stream, [makeSegment(stream, 2, 5)]);
      expect(Result.isError(blockedRes)).toBe(true);
      if (Result.isOk(blockedRes)) return;
      expect(blockedRes.error.kind).toBe("index_cache_overloaded");
      expect(gate.getOverloadReason()?.code).toBe("index_building_behind");

      leaseRes.value.release();
      expect(cache.stats().requiredEntryCount).toBe(0);

      const recoveredRes = await manager.acquireRoutingWindowResult(stream, [makeSegment(stream, 2, 5)]);
      expect(Result.isOk(recoveredRes)).toBe(true);
      expect(gate.getOverloadReason()).toBeNull();
      if (Result.isOk(recoveredRes)) recoveredRes.value.release();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
