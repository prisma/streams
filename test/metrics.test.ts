import { describe, expect, test } from "bun:test";
import { Metrics } from "../src/metrics";

describe("metrics histogram buckets", () => {
  test("uses positive bucket keys for values above 2^31", () => {
    const metrics = new Metrics();

    metrics.record("process.rss.bytes", 3_221_225_472, "bytes");

    const [event] = metrics.flushInterval();
    expect(event).toBeDefined();
    expect(event?.buckets).toEqual({ "2147483648": 1 });
    expect(event?.buckets).not.toHaveProperty("-2147483648");
  });
});
