import { describe, expect, test } from "bun:test";
import { StatsCollector, StatsReporter } from "../src/stats";
import type { SqliteDurableStore } from "../src/db/db";
import type { Uploader } from "../src/uploader";

describe("stats reporter", () => {
  test("emits a stats line and resets window counters", async () => {
    const stats = new StatsCollector();
    stats.recordIngested(1024);
    stats.recordWalCommitBytes(2048);
    stats.recordSegmentSealed(2048, 1024);
    stats.recordUploadedBytes(1024);
    stats.recordBackpressureOverMs(1500);
    stats.recordStreamTouched("alpha");
    stats.recordStreamTouched("beta");

    const db = {
      countStreams: () => 5,
      getWalDbSizeBytes: () => 8192,
      getMetaDbSizeBytes: () => 4096,
    } as unknown as SqliteDurableStore;

    const uploader = {
      countSegmentsWaiting: () => 3,
    } as unknown as Uploader;

    const reporter = new StatsReporter(stats, db, uploader, undefined, 60_000);

    const logs: string[] = [];
    const original = console.log;
    console.log = (line?: unknown) => {
      logs.push(String(line));
    };

    try {
      await (reporter as any).tick();
    } finally {
      console.log = original;
    }

    expect(logs.length).toBe(1);
    const line = logs[0];
    expect(line).toContain("ingested=1.0kb");
    expect(line).toContain("stored=3.0kb");
    expect(line).toContain("compression=2.00x");
    expect(line).toContain("uploaded=1.0kb");
    expect(line).toContain("streams-touched=2/5");
    expect(line).toContain("segments-sealed=1");
    expect(line).toContain("segments-waiting=3");
    expect(line).toContain("avg-segment-size=1.0kb");
    expect(line).toContain("wal-size=8.0kb");
    expect(line).toContain("meta-size=4.0kb");
    expect(line).toContain("backpressure=2.5%");

    logs.length = 0;
    const original2 = console.log;
    console.log = (line?: unknown) => {
      logs.push(String(line));
    };
    try {
      await (reporter as any).tick();
    } finally {
      console.log = original2;
    }

    expect(logs.length).toBe(1);
    const line2 = logs[0];
    expect(line2).toContain("ingested=0b");
    expect(line2).toContain("stored=0b");
    expect(line2).toContain("compression=n/a");
    expect(line2).toContain("uploaded=0b");
    expect(line2).toContain("streams-touched=0/5");
    expect(line2).toContain("segments-sealed=0");
    expect(line2).toContain("avg-segment-size=n/a");
    expect(line2).toContain("backpressure=0.0%");
  });
});
