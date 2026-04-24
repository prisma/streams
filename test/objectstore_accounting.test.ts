import { describe, expect, test } from "bun:test";

import { Metrics } from "../src/metrics";
import { AccountingObjectStore } from "../src/objectstore/accounting";
import type { ObjectStore, PutResult } from "../src/objectstore/interface";

const STREAM_HASH = "0123456789abcdef0123456789abcdef";

function findMetric(
  events: Array<Record<string, unknown>>,
  metric: string,
  tags: Record<string, string>
): Record<string, unknown> | undefined {
  return events.find((event) => {
    if (event.metric !== metric) return false;
    const eventTags = event.tags as Record<string, string> | undefined;
    if (!eventTags) return false;
    return Object.entries(tags).every(([key, value]) => eventTags[key] === value);
  });
}

describe("AccountingObjectStore", () => {
  test("records latency metrics and request counters for classified put/get operations", async () => {
    const requestCounts: Array<[string, string, string, number]> = [];
    const metrics = new Metrics();
    const inner: ObjectStore = {
      async put(): Promise<PutResult> {
        return { etag: "etag-put" };
      },
      async putFile(): Promise<PutResult> {
        return { etag: "etag-file" };
      },
      async get(): Promise<Uint8Array> {
        return new Uint8Array([1, 2, 3]);
      },
      async head() {
        return null;
      },
      async delete() {},
      async list() {
        return [];
      },
    };

    const store = new AccountingObjectStore(
      inner,
      {
        recordObjectStoreRequestByHash(streamHash: string, artifact: string, op: string, size: number) {
          requestCounts.push([streamHash, artifact, op, size]);
        },
      } as any,
      metrics
    );

    await store.put(`streams/${STREAM_HASH}/manifest.json`, new Uint8Array([7, 8, 9]));
    await store.get(`streams/${STREAM_HASH}/segments/0000000000000000.bin`);

    const events = metrics.flushInterval();
    expect(findMetric(events, "tieredstore.objectstore.put.latency", { artifact: "manifest", outcome: "ok" })).toEqual(
      expect.objectContaining({
        count: 1,
        metric: "tieredstore.objectstore.put.latency",
        unit: "ns",
      })
    );
    expect(findMetric(events, "tieredstore.objectstore.get.latency", { artifact: "segment", outcome: "ok" })).toEqual(
      expect.objectContaining({
        count: 1,
        metric: "tieredstore.objectstore.get.latency",
        unit: "ns",
      })
    );
    expect(requestCounts).toEqual([
      [STREAM_HASH, "manifest", "put", 3],
      [STREAM_HASH, "segment", "get", 3],
    ]);
  });

  test("records miss, error, and stream-catalog list outcomes", async () => {
    const metrics = new Metrics();
    const inner: ObjectStore = {
      async put(): Promise<PutResult> {
        return { etag: "etag-put" };
      },
      async get() {
        return null;
      },
      async head() {
        return null;
      },
      async delete() {
        throw new Error("delete failed");
      },
      async list() {
        return ["streams/example"];
      },
    };

    const store = new AccountingObjectStore(
      inner,
      {
        recordObjectStoreRequestByHash() {},
      } as any,
      metrics
    );

    await expect(store.get(`streams/${STREAM_HASH}/segments/0000000000000001.bin`)).resolves.toBeNull();
    await expect(store.list("streams/")).resolves.toEqual(["streams/example"]);
    await expect(store.delete(`streams/${STREAM_HASH}/segments/0000000000000001.bin`)).rejects.toThrow("delete failed");

    const events = metrics.flushInterval();
    expect(findMetric(events, "tieredstore.objectstore.get.latency", { artifact: "segment", outcome: "miss" })).toEqual(
      expect.objectContaining({
        count: 1,
        metric: "tieredstore.objectstore.get.latency",
      })
    );
    expect(findMetric(events, "tieredstore.objectstore.list.latency", { artifact: "stream_catalog", outcome: "ok" })).toEqual(
      expect.objectContaining({
        count: 1,
        metric: "tieredstore.objectstore.list.latency",
      })
    );
    expect(findMetric(events, "tieredstore.objectstore.delete.latency", { artifact: "segment", outcome: "error" })).toEqual(
      expect.objectContaining({
        count: 1,
        metric: "tieredstore.objectstore.delete.latency",
      })
    );
  });
});
