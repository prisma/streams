import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { decodeMetricsBlockSegmentCompanionResult, encodeMetricsBlockSegmentCompanion, type MetricsBlockSectionInput } from "../src/profiles/metrics/block_format";
import type { MetricsBlockRecord } from "../src/profiles/metrics/normalize";

describe("mblk2 compression", () => {
  test("compresses repetitive metrics block payloads and round-trips records", () => {
    const input = buildInput(240);
    const rawJsonBytes = new TextEncoder().encode(JSON.stringify(input.records));
    const encoded = encodeMetricsBlockSegmentCompanion(input);
    const compressedPayloadLength = new DataView(encoded.buffer, encoded.byteOffset + 24, 4).getUint32(0, true);
    const compression = encoded[20] ?? 0;

    expect(compression).toBe(1);
    expect(compressedPayloadLength).toBeLessThan(rawJsonBytes.byteLength);
    expect(encoded.byteLength).toBeLessThan(rawJsonBytes.byteLength);

    const decoded = decodeMetricsBlockSegmentCompanionResult(encoded);
    if (Result.isError(decoded)) throw new Error(decoded.error.message);
    expect(decoded.value.recordCount).toBe(input.record_count);
    expect(decoded.value.minWindowStartMs).toBe(input.min_window_start_ms);
    expect(decoded.value.maxWindowEndMs).toBe(input.max_window_end_ms);
    expect(decoded.value.records()).toEqual(input.records);
  });

  test("round-trips tiny metrics block payloads", () => {
    const input = buildInput(1);
    const encoded = encodeMetricsBlockSegmentCompanion(input);
    expect([0, 1]).toContain(encoded[20] ?? 0);

    const decoded = decodeMetricsBlockSegmentCompanionResult(encoded);
    if (Result.isError(decoded)) throw new Error(decoded.error.message);
    expect(decoded.value.records()).toEqual(input.records);
  });
});

function buildInput(count: number): MetricsBlockSectionInput {
  const records: MetricsBlockRecord[] = [];
  for (let index = 0; index < count; index++) {
    records.push({
      doc_id: index,
      metric: "http.server.duration",
      unit: "ms",
      metricKind: "summary",
      temporality: "delta",
      windowStartMs: 1_711_000_000_000 + index * 60_000,
      windowEndMs: 1_711_000_060_000 + index * 60_000,
      intervalMs: 60_000,
      stream: "checkout",
      instance: "api-1",
      attributes: {
        service: "checkout",
        region: "us-east-1",
        method: "POST",
        route: "/checkout",
      },
      dimensionPairs: ["method=POST", "region=us-east-1", "route=/checkout", "service=checkout"],
      dimensionKey: "method=POST\u0000region=us-east-1\u0000route=/checkout\u0000service=checkout",
      seriesKey: `series:${index % 3}`,
      summary: {
        count: 48,
        sum: 48 * 120,
        min: 120,
        max: 120,
        histogram: { "64": 12, "128": 36 },
      },
    });
  }
  return {
    record_count: records.length,
    min_window_start_ms: records[0]?.windowStartMs,
    max_window_end_ms: records[records.length - 1]?.windowEndMs,
    records,
  };
}
