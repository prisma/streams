import { Result } from "better-result";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { BinaryCursor, BinaryPayloadError, BinaryWriter, concatBytes, readF64, readI64, readU16, readU32 } from "./binary/codec";
import { RestartStringTableView, encodeRestartStringTable } from "./binary/restart_strings";
import { readUVarint, readZigZagVarint, writeUVarint, writeZigZagVarint } from "./binary/varint";
import type { SearchCompanionPlan, SearchCompanionPlanRollup } from "./companion_plan";

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

export type AggIntervalInput = {
  interval_ms: number;
  windows: Array<{
    start_ms: number;
    groups: AggWindowGroup[];
  }>;
};

export type AggRollupInput = {
  intervals: Record<string, AggIntervalInput>;
};

export type AggSectionInput = {
  rollups: Record<string, AggRollupInput>;
};

const ROLLUP_DIR_ENTRY_BYTES = 12;
const INTERVAL_DIR_ENTRY_BYTES = 12;
const DIM_DIR_ENTRY_BYTES = 20;
const MEASURE_DIR_ENTRY_BYTES = 52;
const AGG_INTERVAL_COMPRESSION_NONE = 0;
const AGG_INTERVAL_COMPRESSION_ZSTD = 1;

const MEASURE_KIND_COUNT = 1;
const MEASURE_KIND_SUMMARY = 2;

type RollupDirEntry = {
  rollupOrdinal: number;
  intervalCount: number;
  intervalsOffset: number;
  intervalsLength: number;
};

type IntervalDirEntry = {
  intervalOrdinal: number;
  compression: number;
  payloadOffset: number;
  payloadLength: number;
};

type IntervalPayload = {
  intervalOrdinal: number;
  compression: number;
  payload: Uint8Array;
};

export type AggFormatError = { kind: "invalid_agg_segment"; message: string };

function invalidAgg<T = never>(message: string): Result<T, AggFormatError> {
  return Result.err({ kind: "invalid_agg_segment", message });
}

export class AggIntervalView {
  private decoded:
    | {
        windowStarts: bigint[];
        windowGroupOffsets: Uint32Array;
        dimensions: Array<{ name: string; dict: string[]; ordinals: Uint32Array }>;
        measures: Array<{
          name: string;
          kind: AggMeasureState["kind"];
          countValues: Uint32Array;
          sumValues?: Float64Array;
          minValues?: Float64Array;
          maxValues?: Float64Array;
          histogramOffsets?: Uint32Array;
          histogramData?: Uint8Array;
        }>;
      }
    | null = null;
  private intervalBytes: Uint8Array | null = null;

  constructor(
    readonly rollupName: string,
    readonly intervalMs: number,
    private readonly rollupPlan: SearchCompanionPlanRollup,
    private readonly bytes: Uint8Array,
    private readonly compression: number,
    private readonly plan: SearchCompanionPlan
  ) {}

  forEachGroupInRange(
    startMsInclusive: number,
    endMsExclusive: number,
    visit: (windowStartMs: number, group: AggWindowGroup) => void
  ): void {
    const decoded = this.decode();
    const startIndex = lowerBoundBigint(decoded.windowStarts, BigInt(startMsInclusive));
    const endIndex = lowerBoundBigint(decoded.windowStarts, BigInt(endMsExclusive));
    for (let windowIndex = startIndex; windowIndex < endIndex; windowIndex++) {
      const windowStartMs = Number(decoded.windowStarts[windowIndex]!);
      const groupStart = decoded.windowGroupOffsets[windowIndex]!;
      const groupEnd = decoded.windowGroupOffsets[windowIndex + 1]!;
      for (let groupIndex = groupStart; groupIndex < groupEnd; groupIndex++) {
        const dimensions: Record<string, string | null> = {};
        for (const dimension of decoded.dimensions) {
          const ordinal = dimension.ordinals[groupIndex] ?? 0;
          dimensions[dimension.name] = ordinal === 0 ? null : (dimension.dict[ordinal - 1] ?? null);
        }
        const measures: Record<string, AggMeasureState> = {};
        for (const measure of decoded.measures) {
          if (measure.kind === "count") {
            measures[measure.name] = { kind: "count", value: measure.countValues[groupIndex] ?? 0 };
            continue;
          }
          measures[measure.name] = {
            kind: "summary",
            summary: {
              count: measure.countValues[groupIndex] ?? 0,
              sum: measure.sumValues?.[groupIndex] ?? 0,
              min: decodeNullableNumber(measure.minValues?.[groupIndex]),
              max: decodeNullableNumber(measure.maxValues?.[groupIndex]),
              histogram: decodeHistogram(measure.histogramOffsets, measure.histogramData, groupIndex),
            },
          };
        }
        visit(windowStartMs, { dimensions, measures });
      }
    }
  }

  private decode() {
    if (this.decoded) return this.decoded;
    const intervalBytes = this.getIntervalBytes();
    const cursor = new BinaryCursor(intervalBytes);
    const windowCount = cursor.readU32();
    const groupCount = cursor.readU32();
    const dimCount = cursor.readU16();
    const measureCount = cursor.readU16();
    const windowStartsOffset = cursor.readU32();
    const windowStartsLength = cursor.readU32();
    const windowOffsetsOffset = cursor.readU32();
    const windowOffsetsLength = cursor.readU32();
    const dimDirOffset = cursor.readU32();
    const dimDirLength = cursor.readU32();
    const measureDirOffset = cursor.readU32();
    const measureDirLength = cursor.readU32();

    const windowStartBytes = slicePayload(intervalBytes, windowStartsOffset, windowStartsLength, "invalid .agg2 window starts");
    const windowStarts: bigint[] = [];
    for (let offset = 0; offset + 8 <= windowStartBytes.byteLength; offset += 8) {
      windowStarts.push(readI64(windowStartBytes, offset));
    }
    const windowGroupOffsetsBytes = slicePayload(intervalBytes, windowOffsetsOffset, windowOffsetsLength, "invalid .agg2 window offsets");
    const windowGroupOffsets = decodeU32Array(windowGroupOffsetsBytes);

    const dimensions: Array<{ name: string; dict: string[]; ordinals: Uint32Array }> = [];
    for (let index = 0; index < dimCount; index++) {
      const entryOffset = dimDirOffset + index * DIM_DIR_ENTRY_BYTES;
      const fieldOrdinal = readU16(intervalBytes, entryOffset);
      const dictOffset = readU32(intervalBytes, entryOffset + 4);
      const dictLength = readU32(intervalBytes, entryOffset + 8);
      const ordinalsOffset = readU32(intervalBytes, entryOffset + 12);
      const ordinalsLength = readU32(intervalBytes, entryOffset + 16);
      const fieldName = this.plan.fields.find((field) => field.ordinal === fieldOrdinal)?.name;
      if (!fieldName) continue;
      const dictBytes = slicePayload(intervalBytes, dictOffset, dictLength, "invalid .agg2 dim dict");
      const ordinalBytes = slicePayload(intervalBytes, ordinalsOffset, ordinalsLength, "invalid .agg2 dim ordinals");
      dimensions.push({
        name: fieldName,
        dict: new RestartStringTableView(dictBytes).terms(),
        ordinals: decodeU32Array(ordinalBytes),
      });
    }

    const measures: Array<{
      name: string;
      kind: AggMeasureState["kind"];
      countValues: Uint32Array;
      sumValues?: Float64Array;
      minValues?: Float64Array;
      maxValues?: Float64Array;
      histogramOffsets?: Uint32Array;
      histogramData?: Uint8Array;
    }> = [];
    for (let index = 0; index < measureCount; index++) {
      const entryOffset = measureDirOffset + index * MEASURE_DIR_ENTRY_BYTES;
      const measureOrdinal = readU16(intervalBytes, entryOffset);
      const kindCode = intervalBytes[entryOffset + 2]!;
      const countOffset = readU32(intervalBytes, entryOffset + 4);
      const countLength = readU32(intervalBytes, entryOffset + 8);
      const sumOffset = readU32(intervalBytes, entryOffset + 12);
      const sumLength = readU32(intervalBytes, entryOffset + 16);
      const minOffset = readU32(intervalBytes, entryOffset + 20);
      const minLength = readU32(intervalBytes, entryOffset + 24);
      const maxOffset = readU32(intervalBytes, entryOffset + 28);
      const maxLength = readU32(intervalBytes, entryOffset + 32);
      const histOffsetsOffset = readU32(intervalBytes, entryOffset + 36);
      const histOffsetsLength = readU32(intervalBytes, entryOffset + 40);
      const histDataOffset = readU32(intervalBytes, entryOffset + 44);
      const histDataLength = readU32(intervalBytes, entryOffset + 48);
      const measurePlan = this.rollupPlan.measures.find((measure) => measure.ordinal === measureOrdinal);
      if (!measurePlan) continue;
      const countBytes = slicePayload(intervalBytes, countOffset, countLength, "invalid .agg2 measure count column");
      const measure = {
        name: measurePlan.name,
        kind: kindCode === MEASURE_KIND_SUMMARY ? ("summary" as const) : ("count" as const),
        countValues: decodeU32Array(countBytes),
      };
      if (measure.kind === "summary") {
        const sumBytes = slicePayload(intervalBytes, sumOffset, sumLength, "invalid .agg2 sum column");
        const minBytes = slicePayload(intervalBytes, minOffset, minLength, "invalid .agg2 min column");
        const maxBytes = slicePayload(intervalBytes, maxOffset, maxLength, "invalid .agg2 max column");
        const histOffsetsBytes = histOffsetsLength > 0 ? slicePayload(intervalBytes, histOffsetsOffset, histOffsetsLength, "invalid .agg2 histogram offsets") : new Uint8Array();
        measures.push({
          ...measure,
          sumValues: decodeF64Array(sumBytes),
          minValues: decodeF64Array(minBytes),
          maxValues: decodeF64Array(maxBytes),
          histogramOffsets:
            histOffsetsBytes.byteLength > 0 ? decodeU32Array(histOffsetsBytes) : undefined,
          histogramData: histDataLength > 0 ? slicePayload(intervalBytes, histDataOffset, histDataLength, "invalid .agg2 histogram data") : undefined,
        });
        continue;
      }
      measures.push(measure);
    }

    this.decoded = { windowStarts, windowGroupOffsets, dimensions, measures };
    return this.decoded;
  }

  private getIntervalBytes(): Uint8Array {
    if (this.intervalBytes) return this.intervalBytes;
    if (this.compression === AGG_INTERVAL_COMPRESSION_NONE) {
      this.intervalBytes = this.bytes;
      return this.intervalBytes;
    }
    if (this.compression !== AGG_INTERVAL_COMPRESSION_ZSTD) {
      throw new BinaryPayloadError(`unsupported .agg2 interval compression ${this.compression}`);
    }
    try {
      this.intervalBytes = new Uint8Array(zstdDecompressSync(this.bytes));
    } catch (error: unknown) {
      throw new BinaryPayloadError(`invalid .agg2 compressed interval payload: ${String((error as Error)?.message ?? error)}`);
    }
    return this.intervalBytes;
  }
}

function lowerBoundBigint(values: bigint[], target: bigint): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (values[mid]! < target) low = mid + 1;
    else high = mid;
  }
  return low;
}

export class AggSectionView {
  private readonly intervalByKey = new Map<string, AggIntervalView>();

  constructor(intervals: AggIntervalView[]) {
    for (const interval of intervals) {
      this.intervalByKey.set(`${interval.rollupName}\u0000${interval.intervalMs}`, interval);
    }
  }

  getInterval(rollupName: string, intervalMs: number): AggIntervalView | null {
    return this.intervalByKey.get(`${rollupName}\u0000${intervalMs}`) ?? null;
  }
}

export function encodeAggSegmentCompanion(input: AggSectionInput, plan: SearchCompanionPlan): Uint8Array {
  const rollupEntries = plan.rollups
    .filter((rollup) => input.rollups[rollup.name])
    .sort((a, b) => a.ordinal - b.ordinal)
    .map((rollup) => {
      const source = input.rollups[rollup.name]!;
      const intervals = rollup.intervals
        .filter((interval) => source.intervals[interval.name] || source.intervals[String(interval.ms)])
        .sort((a, b) => a.ordinal - b.ordinal)
        .map((interval) => ({
          intervalOrdinal: interval.ordinal,
          ...compressIntervalPayload(
            encodeIntervalPayload((source.intervals[interval.name] ?? source.intervals[String(interval.ms)])!, rollup, plan)
          ),
        }));
      return { rollup, intervals };
    });

  const header = new BinaryWriter();
  header.writeU16(rollupEntries.length);
  header.writeU16(0);

  const rollupDirStart = header.length;
  const rollupDirBytes = ROLLUP_DIR_ENTRY_BYTES * rollupEntries.length;
  const intervalDirStart = rollupDirStart + rollupDirBytes;
  const totalIntervalDirBytes = rollupEntries.reduce((sum, entry) => sum + INTERVAL_DIR_ENTRY_BYTES * entry.intervals.length, 0);
  let intervalDirOffset = intervalDirStart;
  let payloadOffset = intervalDirStart + totalIntervalDirBytes;
  const rollupDirs: RollupDirEntry[] = [];
  const intervalDirBlobs: Uint8Array[] = [];
  const intervalPayloadBlobs: Uint8Array[] = [];

  for (const entry of rollupEntries) {
    const intervalDir = new BinaryWriter();
    const currentIntervalDirOffset = intervalDirOffset;
    intervalDirOffset += INTERVAL_DIR_ENTRY_BYTES * entry.intervals.length;
    const intervalDirs: IntervalDirEntry[] = [];
    for (const interval of entry.intervals) {
      intervalDirs.push({
        intervalOrdinal: interval.intervalOrdinal,
        compression: interval.compression,
        payloadOffset,
        payloadLength: interval.payload.byteLength,
      });
      payloadOffset += interval.payload.byteLength;
      intervalPayloadBlobs.push(interval.payload);
    }
    for (const intervalDirEntry of intervalDirs) {
      intervalDir.writeU16(intervalDirEntry.intervalOrdinal);
      intervalDir.writeU8(intervalDirEntry.compression);
      intervalDir.writeU8(0);
      intervalDir.writeU32(intervalDirEntry.payloadOffset);
      intervalDir.writeU32(intervalDirEntry.payloadLength);
    }
    const intervalDirBytesBlob = intervalDir.finish();
    intervalDirBlobs.push(intervalDirBytesBlob);
    rollupDirs.push({
      rollupOrdinal: entry.rollup.ordinal,
      intervalCount: entry.intervals.length,
      intervalsOffset: currentIntervalDirOffset,
      intervalsLength: intervalDirBytesBlob.byteLength,
    });
  }

  const rollupDirWriter = new BinaryWriter();
  for (const entry of rollupDirs) {
    rollupDirWriter.writeU16(entry.rollupOrdinal);
    rollupDirWriter.writeU16(entry.intervalCount);
    rollupDirWriter.writeU32(entry.intervalsOffset);
    rollupDirWriter.writeU32(entry.intervalsLength);
  }

  return concatBytes([header.finish(), rollupDirWriter.finish(), ...intervalDirBlobs, ...intervalPayloadBlobs]);
}

export function decodeAggSegmentCompanionResult(bytes: Uint8Array, plan: SearchCompanionPlan): Result<AggSectionView, AggFormatError> {
  try {
    const cursor = new BinaryCursor(bytes);
    const rollupCount = cursor.readU16();
    cursor.readU16();
    const intervals: AggIntervalView[] = [];
    const rollupDirOffset = cursor.offset;
    for (let index = 0; index < rollupCount; index++) {
      const entryOffset = rollupDirOffset + index * ROLLUP_DIR_ENTRY_BYTES;
      const rollupOrdinal = readU16(bytes, entryOffset);
      const intervalCount = readU16(bytes, entryOffset + 2);
      const intervalsOffset = readU32(bytes, entryOffset + 4);
      const rollupPlan = plan.rollups.find((rollup) => rollup.ordinal === rollupOrdinal);
      if (!rollupPlan) return invalidAgg(`missing .agg2 rollup ordinal ${rollupOrdinal}`);
      for (let intervalIndex = 0; intervalIndex < intervalCount; intervalIndex++) {
        const intervalEntryOffset = intervalsOffset + intervalIndex * INTERVAL_DIR_ENTRY_BYTES;
        const intervalOrdinal = readU16(bytes, intervalEntryOffset);
        const compression = bytes[intervalEntryOffset + 2] ?? AGG_INTERVAL_COMPRESSION_NONE;
        const payloadOffset = readU32(bytes, intervalEntryOffset + 4);
        const payloadLength = readU32(bytes, intervalEntryOffset + 8);
        const intervalPlan = rollupPlan.intervals.find((interval) => interval.ordinal === intervalOrdinal);
        if (!intervalPlan) return invalidAgg(`missing .agg2 interval ordinal ${intervalOrdinal}`);
        intervals.push(
          new AggIntervalView(
            rollupPlan.name,
            intervalPlan.ms,
            rollupPlan,
            slicePayload(bytes, payloadOffset, payloadLength, "invalid .agg2 interval payload"),
            compression,
            plan
          )
        );
      }
    }
    return Result.ok(new AggSectionView(intervals));
  } catch (e: unknown) {
    return invalidAgg(String((e as any)?.message ?? e));
  }
}

function encodeIntervalPayload(input: AggIntervalInput, rollupPlan: SearchCompanionPlanRollup, plan: SearchCompanionPlan): Uint8Array {
  const windows = [...input.windows].sort((a, b) => a.start_ms - b.start_ms);
  const groups = windows.flatMap((window) => window.groups);
  const windowGroupOffsets = new Uint32Array(windows.length + 1);
  let groupOffset = 0;
  for (let index = 0; index < windows.length; index++) {
    windowGroupOffsets[index] = groupOffset;
    groupOffset += windows[index]!.groups.length;
  }
  windowGroupOffsets[windows.length] = groupOffset;

  const windowStartsWriter = new BinaryWriter();
  for (const window of windows) windowStartsWriter.writeI64(BigInt(window.start_ms));
  const windowOffsetsWriter = new BinaryWriter();
  for (const offset of windowGroupOffsets) windowOffsetsWriter.writeU32(offset);

  const dimensionPayloads = rollupPlan.dimension_ordinals.map((dimensionOrdinal) => {
    const fieldName = plan.fields.find((field) => field.ordinal === dimensionOrdinal)?.name ?? null;
    const values = groups.map((group) => (fieldName ? group.dimensions[fieldName] ?? null : null));
    const dictionary = Array.from(new Set(values.filter((value): value is string => typeof value === "string"))).sort((a, b) => a.localeCompare(b));
    const dictionaryIndex = new Map<string, number>();
    dictionary.forEach((value, index) => dictionaryIndex.set(value, index + 1));
    const ordinalsWriter = new BinaryWriter();
    for (const value of values) ordinalsWriter.writeU32(value == null ? 0 : (dictionaryIndex.get(value) ?? 0));
    return {
      fieldOrdinal: dimensionOrdinal,
      dict: encodeRestartStringTable(dictionary),
      ordinals: ordinalsWriter.finish(),
    };
  });

  const measurePayloads = rollupPlan.measures.map((measurePlan) => {
    const countWriter = new BinaryWriter();
    const sumWriter = new BinaryWriter();
    const minWriter = new BinaryWriter();
    const maxWriter = new BinaryWriter();
    const histOffsetsWriter = new BinaryWriter();
    const histDataWriter = new BinaryWriter();
    let histOffset = 0;
    for (const group of groups) {
      const state = group.measures[measurePlan.name];
      if (!state || state.kind === "count") {
        countWriter.writeU32(state?.kind === "count" ? state.value : 0);
        if (measurePlan.kind !== "count") {
          sumWriter.writeF64(0);
          minWriter.writeF64(Number.NaN);
          maxWriter.writeF64(Number.NaN);
          histOffsetsWriter.writeU32(histOffset);
        }
        continue;
      }
      countWriter.writeU32(state.summary.count);
      sumWriter.writeF64(state.summary.sum);
      minWriter.writeF64(state.summary.min ?? Number.NaN);
      maxWriter.writeF64(state.summary.max ?? Number.NaN);
      histOffsetsWriter.writeU32(histOffset);
      const histogramBlob = encodeHistogram(state.summary.histogram);
      histDataWriter.writeBytes(histogramBlob);
      histOffset += histogramBlob.byteLength;
    }
    if (measurePlan.kind === "summary") histOffsetsWriter.writeU32(histOffset);
    return {
      measureOrdinal: measurePlan.ordinal,
      kind: measurePlan.kind === "count" ? MEASURE_KIND_COUNT : MEASURE_KIND_SUMMARY,
      count: countWriter.finish(),
      sum: measurePlan.kind !== "count" ? sumWriter.finish() : new Uint8Array(),
      min: measurePlan.kind !== "count" ? minWriter.finish() : new Uint8Array(),
      max: measurePlan.kind !== "count" ? maxWriter.finish() : new Uint8Array(),
      histOffsets: measurePlan.kind !== "count" ? histOffsetsWriter.finish() : new Uint8Array(),
      histData: measurePlan.kind !== "count" ? histDataWriter.finish() : new Uint8Array(),
    };
  });

  const header = new BinaryWriter();
  header.writeU32(windows.length);
  header.writeU32(groups.length);
  header.writeU16(dimensionPayloads.length);
  header.writeU16(measurePayloads.length);
  header.writeU32(0);
  header.writeU32(0);
  header.writeU32(0);
  header.writeU32(0);
  header.writeU32(0);
  header.writeU32(0);
  header.writeU32(0);
  header.writeU32(0);

  const dimDirStart = 44;
  const dimDirLength = DIM_DIR_ENTRY_BYTES * dimensionPayloads.length;
  const measureDirStart = dimDirStart + dimDirLength;
  const measureDirLength = MEASURE_DIR_ENTRY_BYTES * measurePayloads.length;
  let payloadOffset = measureDirStart + measureDirLength;

  const windowStartsOffset = payloadOffset;
  const windowStartsBytes = windowStartsWriter.finish();
  payloadOffset += windowStartsBytes.byteLength;
  const windowOffsetsOffset = payloadOffset;
  const windowOffsetsBytes = windowOffsetsWriter.finish();
  payloadOffset += windowOffsetsBytes.byteLength;

  const dimDirWriter = new BinaryWriter();
  const dimBlobs: Uint8Array[] = [];
  for (const dim of dimensionPayloads) {
    const dictOffset = payloadOffset;
    payloadOffset += dim.dict.byteLength;
    const ordinalsOffset = payloadOffset;
    payloadOffset += dim.ordinals.byteLength;
    dimDirWriter.writeU16(dim.fieldOrdinal);
    dimDirWriter.writeU16(0);
    dimDirWriter.writeU32(dictOffset);
    dimDirWriter.writeU32(dim.dict.byteLength);
    dimDirWriter.writeU32(ordinalsOffset);
    dimDirWriter.writeU32(dim.ordinals.byteLength);
    dimBlobs.push(dim.dict, dim.ordinals);
  }

  const measureDirWriter = new BinaryWriter();
  const measureBlobs: Uint8Array[] = [];
  for (const measure of measurePayloads) {
    const countOffset = payloadOffset;
    payloadOffset += measure.count.byteLength;
    const sumOffset = payloadOffset;
    payloadOffset += measure.sum.byteLength;
    const minOffset = payloadOffset;
    payloadOffset += measure.min.byteLength;
    const maxOffset = payloadOffset;
    payloadOffset += measure.max.byteLength;
    const histOffsetsOffset = payloadOffset;
    payloadOffset += measure.histOffsets.byteLength;
    const histDataOffset = payloadOffset;
    payloadOffset += measure.histData.byteLength;
    measureDirWriter.writeU16(measure.measureOrdinal);
    measureDirWriter.writeU8(measure.kind);
    measureDirWriter.writeU8(0);
    measureDirWriter.writeU32(countOffset);
    measureDirWriter.writeU32(measure.count.byteLength);
    measureDirWriter.writeU32(sumOffset);
    measureDirWriter.writeU32(measure.sum.byteLength);
    measureDirWriter.writeU32(minOffset);
    measureDirWriter.writeU32(measure.min.byteLength);
    measureDirWriter.writeU32(maxOffset);
    measureDirWriter.writeU32(measure.max.byteLength);
    measureDirWriter.writeU32(histOffsetsOffset);
    measureDirWriter.writeU32(measure.histOffsets.byteLength);
    measureDirWriter.writeU32(histDataOffset);
    measureDirWriter.writeU32(measure.histData.byteLength);
    measureBlobs.push(measure.count, measure.sum, measure.min, measure.max, measure.histOffsets, measure.histData);
  }

  const out = new BinaryWriter();
  out.writeU32(windows.length);
  out.writeU32(groups.length);
  out.writeU16(dimensionPayloads.length);
  out.writeU16(measurePayloads.length);
  out.writeU32(windowStartsOffset);
  out.writeU32(windowStartsBytes.byteLength);
  out.writeU32(windowOffsetsOffset);
  out.writeU32(windowOffsetsBytes.byteLength);
  out.writeU32(dimDirStart);
  out.writeU32(dimDirLength);
  out.writeU32(measureDirStart);
  out.writeU32(measureDirLength);
  out.writeBytes(dimDirWriter.finish());
  out.writeBytes(measureDirWriter.finish());
  out.writeBytes(windowStartsBytes);
  out.writeBytes(windowOffsetsBytes);
  for (const blob of dimBlobs) out.writeBytes(blob);
  for (const blob of measureBlobs) out.writeBytes(blob);
  return out.finish();
}

function encodeHistogram(histogram: Record<string, number> | undefined): Uint8Array {
  const writer = new BinaryWriter();
  const entries = Object.entries(histogram ?? {}).sort((a, b) => Number(a[0]) - Number(b[0]));
  writeUVarint(writer, entries.length);
  for (const [bucket, count] of entries) {
    writeZigZagVarint(writer, Number(bucket));
    writeUVarint(writer, Math.trunc(count));
  }
  return writer.finish();
}

function compressIntervalPayload(payload: Uint8Array): { compression: number; payload: Uint8Array } {
  if (payload.byteLength === 0) {
    return { compression: AGG_INTERVAL_COMPRESSION_NONE, payload };
  }
  const compressed = new Uint8Array(zstdCompressSync(payload));
  if (compressed.byteLength >= payload.byteLength) {
    return { compression: AGG_INTERVAL_COMPRESSION_NONE, payload };
  }
  return { compression: AGG_INTERVAL_COMPRESSION_ZSTD, payload: compressed };
}

function decodeHistogram(
  offsets: Uint32Array | undefined,
  data: Uint8Array | undefined,
  groupIndex: number
): Record<string, number> | undefined {
  if (!offsets || !data || groupIndex + 1 >= offsets.length) return undefined;
  const start = offsets[groupIndex] ?? 0;
  const end = offsets[groupIndex + 1] ?? start;
  if (end <= start) return undefined;
  const cursor = new BinaryCursor(data.subarray(start, end));
  const entryCount = Number(readUVarint(cursor));
  const histogram: Record<string, number> = {};
  for (let index = 0; index < entryCount; index++) {
    histogram[String(Number(readZigZagVarint(cursor)))] = Number(readUVarint(cursor));
  }
  return Object.keys(histogram).length > 0 ? histogram : undefined;
}

function decodeNullableNumber(value: number | undefined): number | null {
  if (value == null || Number.isNaN(value)) return null;
  return value;
}

function slicePayload(bytes: Uint8Array, offset: number, length: number, message: string): Uint8Array {
  if (offset < 0 || length < 0 || offset + length > bytes.byteLength) {
    throw new BinaryPayloadError(message);
  }
  return bytes.subarray(offset, offset + length);
}

function decodeU32Array(bytes: Uint8Array): Uint32Array {
  const out = new Uint32Array(Math.floor(bytes.byteLength / 4));
  for (let index = 0; index < out.length; index++) out[index] = readU32(bytes, index * 4);
  return out;
}

function decodeF64Array(bytes: Uint8Array): Float64Array {
  const out = new Float64Array(Math.floor(bytes.byteLength / 8));
  for (let index = 0; index < out.length; index++) out[index] = readF64(bytes, index * 8);
  return out;
}
