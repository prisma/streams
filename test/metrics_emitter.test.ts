import { describe, expect, test } from "bun:test";
import { Result } from "better-result";

import type { AppendResult, IngestQueue } from "../src/ingest";
import { Metrics } from "../src/metrics";
import { MetricsEmitter } from "../src/metrics_emitter";

function createSuccessResult(lastOffset: bigint): AppendResult {
  return Result.ok({
    appendedRows: 2,
    closed: false,
    duplicate: false,
    lastOffset,
  });
}

describe("MetricsEmitter", () => {
  test("successful flush notifies onAppended exactly once per internal append", async () => {
    const metrics = new Metrics();
    let appendCallCount = 0;
    const appended: Array<{ lastOffset: bigint; stream: string }> = [];

    const ingest = {
      appendInternal: async () => {
        appendCallCount += 1;
        return createSuccessResult(41n);
      },
      getQueueStats: () => ({
        bytes: 0,
        requests: 0,
      }),
    } as Pick<IngestQueue, "appendInternal" | "getQueueStats"> as IngestQueue;

    const emitter = new MetricsEmitter(metrics, ingest, 100, {
      onAppended: (args) => {
        appended.push(args);
      },
    });

    await (emitter as any).flush();

    expect(appendCallCount).toBe(1);
    expect(appended).toEqual([
      {
        lastOffset: 41n,
        stream: "__stream_metrics__",
      },
    ]);
  });

  test("failed flush does not notify onAppended", async () => {
    const metrics = new Metrics();
    const appended: Array<{ lastOffset: bigint; stream: string }> = [];

    const ingest = {
      appendInternal: async () =>
        Result.err({
          kind: "internal" as const,
        }),
      getQueueStats: () => ({
        bytes: 0,
        requests: 0,
      }),
    } as Pick<IngestQueue, "appendInternal" | "getQueueStats"> as IngestQueue;

    const emitter = new MetricsEmitter(metrics, ingest, 100, {
      onAppended: (args) => {
        appended.push(args);
      },
    });

    await (emitter as any).flush();

    expect(appended).toEqual([]);
  });
});
