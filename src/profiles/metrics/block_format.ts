import { Result } from "better-result";
import { BinaryCursor, BinaryWriter, readI64 } from "../../search/binary/codec";
import type { MetricsBlockRecord } from "./normalize";

export type MetricsBlockSectionInput = {
  record_count: number;
  min_window_start_ms?: number;
  max_window_end_ms?: number;
  records: MetricsBlockRecord[];
};

export type MetricsBlockFormatError = { kind: "invalid_metrics_block"; message: string };

function invalidMetricsBlock<T = never>(message: string): Result<T, MetricsBlockFormatError> {
  return Result.err({ kind: "invalid_metrics_block", message });
}

export class MetricsBlockSectionView {
  private recordsCache: MetricsBlockRecord[] | null = null;

  constructor(
    readonly recordCount: number,
    readonly minWindowStartMs: number | null,
    readonly maxWindowEndMs: number | null,
    private readonly recordsPayload: Uint8Array
  ) {}

  records(): MetricsBlockRecord[] {
    if (!this.recordsCache) {
      this.recordsCache = JSON.parse(new TextDecoder().decode(this.recordsPayload)) as MetricsBlockRecord[];
    }
    return this.recordsCache;
  }
}

export function encodeMetricsBlockSegmentCompanion(input: MetricsBlockSectionInput): Uint8Array {
  const jsonPayload = new TextEncoder().encode(JSON.stringify(input.records));
  const writer = new BinaryWriter();
  writer.writeU32(input.record_count);
  writer.writeI64(BigInt(input.min_window_start_ms ?? -1));
  writer.writeI64(BigInt(input.max_window_end_ms ?? -1));
  writer.writeU32(jsonPayload.byteLength);
  writer.writeBytes(jsonPayload);
  return writer.finish();
}

export function decodeMetricsBlockSegmentCompanionResult(bytes: Uint8Array): Result<MetricsBlockSectionView, MetricsBlockFormatError> {
  try {
    const cursor = new BinaryCursor(bytes);
    const recordCount = cursor.readU32();
    const minWindowStartMs = Number(readI64(bytes, 4));
    const maxWindowEndMs = Number(readI64(bytes, 12));
    cursor.readI64();
    cursor.readI64();
    const payloadLength = cursor.readU32();
    const payload = cursor.readBytes(payloadLength);
    return Result.ok(
      new MetricsBlockSectionView(
        recordCount,
        minWindowStartMs < 0 ? null : minWindowStartMs,
        maxWindowEndMs < 0 ? null : maxWindowEndMs,
        payload
      )
    );
  } catch (e: unknown) {
    return invalidMetricsBlock(String((e as any)?.message ?? e));
  }
}
