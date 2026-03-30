import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { Result } from "better-result";
import type { MetricsBlockRecord } from "./normalize";

export type MetricsBlockSegmentCompanion = {
  version: 1;
  stream: string;
  segment_index: number;
  record_count: number;
  min_window_start_ms?: number;
  max_window_end_ms?: number;
  records: MetricsBlockRecord[];
};

export type MetricsBlockFormatError = { kind: "invalid_metrics_block"; message: string };

function invalidMetricsBlock<T = never>(message: string): Result<T, MetricsBlockFormatError> {
  return Result.err({ kind: "invalid_metrics_block", message });
}

export function encodeMetricsBlockSegmentCompanion(companion: MetricsBlockSegmentCompanion): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(companion));
  return new Uint8Array(zstdCompressSync(body));
}

export function decodeMetricsBlockSegmentCompanionResult(
  bytes: Uint8Array
): Result<MetricsBlockSegmentCompanion, MetricsBlockFormatError> {
  try {
    const raw = new TextDecoder().decode(zstdDecompressSync(bytes));
    const parsed = JSON.parse(raw) as MetricsBlockSegmentCompanion;
    if (!parsed || parsed.version !== 1 || typeof parsed.stream !== "string") {
      return invalidMetricsBlock("invalid .mblk companion");
    }
    if (typeof parsed.segment_index !== "number" || !Array.isArray(parsed.records)) {
      return invalidMetricsBlock("invalid .mblk companion");
    }
    return Result.ok(parsed);
  } catch (e: unknown) {
    return invalidMetricsBlock(String((e as any)?.message ?? e));
  }
}

