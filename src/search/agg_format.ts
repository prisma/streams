import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { Result } from "better-result";

export type AggSummaryState = {
  count: number;
  sum: number;
  min: number | null;
  max: number | null;
  histogram?: Record<string, number>;
};

export type AggMeasureState =
  | { kind: "count"; value: number }
  | { kind: "summary"; summary: AggSummaryState };

export type AggWindowGroup = {
  dimensions: Record<string, string | null>;
  measures: Record<string, AggMeasureState>;
};

export type AggIntervalCompanion = {
  interval_ms: number;
  windows: Array<{
    start_ms: number;
    groups: AggWindowGroup[];
  }>;
};

export type AggRollupCompanion = {
  intervals: Record<string, AggIntervalCompanion>;
};

export type AggSegmentCompanion = {
  version: 1;
  stream: string;
  segment_index: number;
  rollups: Record<string, AggRollupCompanion>;
};

export type AggFormatError = { kind: "invalid_agg_segment"; message: string };

function invalidAgg<T = never>(message: string): Result<T, AggFormatError> {
  return Result.err({ kind: "invalid_agg_segment", message });
}

export function encodeAggSegmentCompanion(companion: AggSegmentCompanion): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(companion));
  return new Uint8Array(zstdCompressSync(body));
}

export function decodeAggSegmentCompanionResult(bytes: Uint8Array): Result<AggSegmentCompanion, AggFormatError> {
  try {
    const raw = new TextDecoder().decode(zstdDecompressSync(bytes));
    const parsed = JSON.parse(raw) as AggSegmentCompanion;
    if (!parsed || parsed.version !== 1 || typeof parsed.stream !== "string") {
      return invalidAgg("invalid .agg companion");
    }
    if (typeof parsed.segment_index !== "number" || !parsed.rollups || typeof parsed.rollups !== "object") {
      return invalidAgg("invalid .agg companion");
    }
    return Result.ok(parsed);
  } catch (e: unknown) {
    return invalidAgg(String((e as any)?.message ?? e));
  }
}
