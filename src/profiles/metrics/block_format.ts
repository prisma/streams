import { Result } from "better-result";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, readI64 } from "../../search/binary/codec";
import { zstdCompressSync, zstdDecompressSync } from "../../util/zstd";
import type { MetricsBlockRecord } from "./normalize";

export type MetricsBlockSectionInput = {
  record_count: number;
  min_window_start_ms?: number;
  max_window_end_ms?: number;
  records: MetricsBlockRecord[];
};

export type MetricsBlockFormatError = { kind: "invalid_metrics_block"; message: string };
const METRICS_BLOCK_COMPRESSION_NONE = 0;
const METRICS_BLOCK_COMPRESSION_ZSTD = 1;

function invalidMetricsBlock<T = never>(message: string): Result<T, MetricsBlockFormatError> {
  return Result.err({ kind: "invalid_metrics_block", message });
}

export class MetricsBlockSectionView {
  private recordsCache: MetricsBlockRecord[] | null = null;
  private decodedPayload: Uint8Array | null = null;

  constructor(
    readonly recordCount: number,
    readonly minWindowStartMs: number | null,
    readonly maxWindowEndMs: number | null,
    private readonly compression: number,
    private readonly recordsPayload: Uint8Array
  ) {}

  records(): MetricsBlockRecord[] {
    if (!this.recordsCache) {
      this.recordsCache = JSON.parse(new TextDecoder().decode(this.payloadBytes())) as MetricsBlockRecord[];
    }
    return this.recordsCache;
  }

  private payloadBytes(): Uint8Array {
    if (this.decodedPayload) return this.decodedPayload;
    if (this.compression === METRICS_BLOCK_COMPRESSION_NONE) {
      this.decodedPayload = this.recordsPayload;
      return this.decodedPayload;
    }
    if (this.compression !== METRICS_BLOCK_COMPRESSION_ZSTD) {
      throw new BinaryPayloadError(`unsupported metrics block compression ${this.compression}`);
    }
    try {
      this.decodedPayload = new Uint8Array(zstdDecompressSync(this.recordsPayload));
    } catch (error: unknown) {
      throw new BinaryPayloadError(`invalid compressed metrics block payload: ${String((error as Error)?.message ?? error)}`);
    }
    return this.decodedPayload;
  }
}

export function encodeMetricsBlockSegmentCompanion(input: MetricsBlockSectionInput): Uint8Array {
  const jsonPayload = new TextEncoder().encode(JSON.stringify(input.records));
  const payload = compressPayload(jsonPayload);
  const writer = new BinaryWriter();
  writer.writeU32(input.record_count);
  writer.writeI64(BigInt(input.min_window_start_ms ?? -1));
  writer.writeI64(BigInt(input.max_window_end_ms ?? -1));
  writer.writeU8(payload.compression);
  writer.writeU8(0);
  writer.writeU16(0);
  writer.writeU32(payload.bytes.byteLength);
  writer.writeBytes(payload.bytes);
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
    const compression = cursor.readU8();
    cursor.readU8();
    cursor.readU16();
    const payloadLength = cursor.readU32();
    const payload = cursor.readBytes(payloadLength);
    return Result.ok(
      new MetricsBlockSectionView(
        recordCount,
        minWindowStartMs < 0 ? null : minWindowStartMs,
        maxWindowEndMs < 0 ? null : maxWindowEndMs,
        compression,
        payload
      )
    );
  } catch (e: unknown) {
    return invalidMetricsBlock(String((e as any)?.message ?? e));
  }
}

function compressPayload(jsonPayload: Uint8Array): { compression: number; bytes: Uint8Array } {
  if (jsonPayload.byteLength === 0) {
    return { compression: METRICS_BLOCK_COMPRESSION_NONE, bytes: jsonPayload };
  }
  const compressed = new Uint8Array(zstdCompressSync(jsonPayload));
  if (compressed.byteLength >= jsonPayload.byteLength) {
    return { compression: METRICS_BLOCK_COMPRESSION_NONE, bytes: jsonPayload };
  }
  return { compression: METRICS_BLOCK_COMPRESSION_ZSTD, bytes: compressed };
}
