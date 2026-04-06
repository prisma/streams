import { Result } from "better-result";
import type { AggSummaryState } from "../../search/agg_format";
import type { NormalizedMetricsRecord } from "../profile";
import { expectPlainObjectResult, isPlainObject } from "../profile";

type PrimitiveAttribute = string | number | boolean | bigint;

type HistogramMap = Record<string, number>;

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

function normalizeFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function normalizeInteger(value: unknown): number | null {
  const numeric = normalizeFiniteNumber(value);
  if (numeric == null) return null;
  return Math.trunc(numeric);
}

function normalizeHistogram(value: unknown): HistogramMap | undefined {
  if (!isPlainObject(value)) return undefined;
  const out: HistogramMap = {};
  for (const [bucket, raw] of Object.entries(value)) {
    const count = normalizeFiniteNumber(raw);
    if (count == null || count <= 0) continue;
    out[String(bucket)] = (out[String(bucket)] ?? 0) + count;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function histogramPercentile(histogram: HistogramMap | undefined, percentile: number): number | null {
  if (!histogram) return null;
  const entries = Object.entries(histogram)
    .map(([bucket, count]) => ({ bucket: Number(bucket), count }))
    .filter((entry) => Number.isFinite(entry.bucket) && Number.isFinite(entry.count) && entry.count > 0)
    .sort((a, b) => a.bucket - b.bucket);
  if (entries.length === 0) return null;
  const total = entries.reduce((sum, entry) => sum + entry.count, 0);
  if (total <= 0) return null;
  const threshold = total * percentile;
  let seen = 0;
  for (const entry of entries) {
    seen += entry.count;
    if (seen >= threshold) return entry.bucket;
  }
  return entries[entries.length - 1]?.bucket ?? null;
}

function normalizeAttributes(value: unknown): Record<string, string> {
  if (!isPlainObject(value)) return {};
  const out: Record<string, string> = {};
  for (const [key, raw] of Object.entries(value)) {
    if (typeof raw === "string") out[key] = raw;
    else if (typeof raw === "number" && Number.isFinite(raw)) out[key] = String(raw);
    else if (typeof raw === "boolean") out[key] = raw ? "true" : "false";
    else if (typeof raw === "bigint") out[key] = raw.toString();
  }
  return Object.fromEntries(Object.entries(out).sort((a, b) => a[0].localeCompare(b[0])));
}

function buildDimensionPairs(attributes: Record<string, string>): string[] {
  return Object.entries(attributes)
    .map(([key, value]) => `${key}=${value}`)
    .sort((a, b) => a.localeCompare(b));
}

function buildSeriesKey(args: {
  metricKind: string;
  temporality: string;
  metric: string;
  unit: string;
  stream: string | null;
  instance: string | null;
  dimensionKey: string | null;
}): string {
  return [
    args.metricKind,
    args.temporality,
    args.metric,
    args.unit,
    args.stream ?? "",
    args.instance ?? "",
    args.dimensionKey ?? "",
  ].join("|");
}

function normalizeSummaryResult(value: Record<string, unknown>): Result<AggSummaryState, { message: string }> {
  const summaryObject = isPlainObject(value.summary) ? value.summary : null;
  const histogram =
    normalizeHistogram(value.buckets) ??
    normalizeHistogram(summaryObject?.histogram ?? undefined);
  const count =
    normalizeFiniteNumber(value.count) ??
    normalizeFiniteNumber(summaryObject?.count);
  const sum =
    normalizeFiniteNumber(value.sum) ??
    normalizeFiniteNumber(summaryObject?.sum);
  const min =
    normalizeFiniteNumber(value.min) ??
    normalizeFiniteNumber(summaryObject?.min);
  const max =
    normalizeFiniteNumber(value.max) ??
    normalizeFiniteNumber(summaryObject?.max);

  if (count == null || count < 0) return Result.err({ message: "metrics interval requires count" });
  if (sum == null) return Result.err({ message: "metrics interval requires sum" });

  return Result.ok({
    count,
    sum,
    min: count === 0 ? null : min ?? 0,
    max: count === 0 ? null : max ?? 0,
    histogram,
  });
}

export function normalizeMetricsRecordResult(value: unknown): Result<NormalizedMetricsRecord, { message: string }> {
  const objRes = expectPlainObjectResult(value, "metrics record");
  if (Result.isError(objRes)) return objRes;
  const input = objRes.value;

  const kind = normalizeString(input.kind) ?? "interval";
  if (kind !== "interval") return Result.err({ message: "metrics record.kind must be interval" });

  const metric = normalizeString(input.metric);
  if (!metric) return Result.err({ message: "metrics record.metric must be a non-empty string" });

  const unit = normalizeString(input.unit);
  if (!unit) return Result.err({ message: "metrics record.unit must be a non-empty string" });

  const windowStart = normalizeInteger(input.windowStart);
  const windowEnd = normalizeInteger(input.windowEnd);
  if (windowStart == null || windowEnd == null) {
    return Result.err({ message: "metrics record.windowStart and windowEnd must be integers" });
  }
  if (windowEnd < windowStart) return Result.err({ message: "metrics record.windowEnd must be >= windowStart" });

  const intervalMs = normalizeInteger(input.intervalMs) ?? (windowEnd - windowStart);
  if (intervalMs < 0) return Result.err({ message: "metrics record.intervalMs must be >= 0" });

  const metricKind = normalizeString(input.metricKind) ?? "summary";
  const temporality = normalizeString(input.temporality) ?? "delta";
  const stream = normalizeString(input.stream);
  const instance = normalizeString(input.instance);
  const attributes = normalizeAttributes(input.attributes ?? input.tags);
  const dimensionPairs = buildDimensionPairs(attributes);
  const dimensionKey = dimensionPairs.length > 0 ? dimensionPairs.join("\u0000") : null;
  const seriesKey = buildSeriesKey({
    metricKind,
    temporality,
    metric,
    unit,
    stream,
    instance,
    dimensionKey,
  });

  const summaryRes = normalizeSummaryResult(input);
  if (Result.isError(summaryRes)) return summaryRes;
  const summary = summaryRes.value;
  const avg = normalizeFiniteNumber(input.avg) ?? (summary.count > 0 ? summary.sum / summary.count : 0);
  const p50 = normalizeFiniteNumber(input.p50) ?? histogramPercentile(summary.histogram, 0.5) ?? 0;
  const p95 = normalizeFiniteNumber(input.p95) ?? histogramPercentile(summary.histogram, 0.95) ?? 0;
  const p99 = normalizeFiniteNumber(input.p99) ?? histogramPercentile(summary.histogram, 0.99) ?? 0;

  const normalizedValue: Record<string, unknown> = {
    apiVersion: "durable.streams/metrics/v1",
    kind: "interval",
    metric,
    unit,
    metricKind,
    temporality,
    windowStart,
    windowEnd,
    intervalMs,
    instance,
    stream,
    tags: attributes,
    attributes,
    dimensionPairs,
    dimensionKey,
    seriesKey,
    count: summary.count,
    sum: summary.sum,
    min: summary.min,
    max: summary.max,
    avg,
    p50,
    p95,
    p99,
    buckets: summary.histogram ?? {},
    summary: {
      count: summary.count,
      sum: summary.sum,
      min: summary.min,
      max: summary.max,
      histogram: summary.histogram ?? {},
    },
  };

  return Result.ok({
    value: normalizedValue,
    routingKey: seriesKey,
    companion: {
      metric,
      unit,
      metricKind,
      temporality,
      windowStartMs: windowStart,
      windowEndMs: windowEnd,
      intervalMs,
      stream,
      instance,
      attributes,
      dimensionPairs,
      dimensionKey,
      seriesKey,
      summary,
    },
  });
}

export type MetricsBlockRecord = {
  doc_id: number;
  metric: string;
  unit: string;
  metricKind: string;
  temporality: string;
  windowStartMs: number;
  windowEndMs: number;
  intervalMs: number;
  stream: string | null;
  instance: string | null;
  attributes: Record<string, string>;
  dimensionPairs: string[];
  dimensionKey: string | null;
  seriesKey: string;
  summary: AggSummaryState;
};

export function buildMetricsBlockRecord(docId: number, value: unknown): Result<MetricsBlockRecord, { message: string }> {
  const normalizedRes = normalizeMetricsRecordResult(value);
  if (Result.isError(normalizedRes)) return normalizedRes;
  return Result.ok({
    doc_id: docId,
    ...normalizedRes.value.companion,
  });
}

export function materializeMetricsBlockRecord(record: MetricsBlockRecord): Record<string, unknown> {
  const histogram = record.summary.histogram ?? {};
  const avg = record.summary.count > 0 ? record.summary.sum / record.summary.count : 0;
  return {
    apiVersion: "durable.streams/metrics/v1",
    kind: "interval",
    metric: record.metric,
    unit: record.unit,
    metricKind: record.metricKind,
    temporality: record.temporality,
    windowStart: record.windowStartMs,
    windowEnd: record.windowEndMs,
    intervalMs: record.intervalMs,
    instance: record.instance,
    stream: record.stream,
    tags: { ...record.attributes },
    attributes: { ...record.attributes },
    dimensionPairs: [...record.dimensionPairs],
    dimensionKey: record.dimensionKey,
    seriesKey: record.seriesKey,
    count: record.summary.count,
    sum: record.summary.sum,
    min: record.summary.min,
    max: record.summary.max,
    avg,
    p50: histogramPercentile(histogram, 0.5) ?? 0,
    p95: histogramPercentile(histogram, 0.95) ?? 0,
    p99: histogramPercentile(histogram, 0.99) ?? 0,
    buckets: { ...histogram },
    summary: {
      count: record.summary.count,
      sum: record.summary.sum,
      min: record.summary.min,
      max: record.summary.max,
      histogram: { ...histogram },
    },
  };
}

export function buildInternalMetricsRecord(args: {
  metric: string;
  unit: string;
  windowStart: number;
  windowEnd: number;
  intervalMs: number;
  instance: string;
  stream?: string;
  tags?: Record<string, PrimitiveAttribute>;
  count: number;
  sum: number;
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  buckets: Record<string, number>;
}): Record<string, unknown> {
  const attributes = normalizeAttributes(args.tags);
  const dimensionPairs = buildDimensionPairs(attributes);
  const dimensionKey = dimensionPairs.length > 0 ? dimensionPairs.join("\u0000") : null;
  const seriesKey = buildSeriesKey({
    metricKind: "summary",
    temporality: "delta",
    metric: args.metric,
    unit: args.unit,
    stream: args.stream ?? null,
    instance: args.instance,
    dimensionKey,
  });
  return {
    apiVersion: "durable.streams/metrics/v1",
    kind: "interval",
    metric: args.metric,
    unit: args.unit,
    metricKind: "summary",
    temporality: "delta",
    windowStart: args.windowStart,
    windowEnd: args.windowEnd,
    intervalMs: args.intervalMs,
    instance: args.instance,
    stream: args.stream ?? null,
    tags: attributes,
    attributes,
    dimensionPairs,
    dimensionKey,
    seriesKey,
    count: args.count,
    sum: args.sum,
    min: args.count === 0 ? null : args.min,
    max: args.count === 0 ? null : args.max,
    avg: args.avg,
    p50: args.p50,
    p95: args.p95,
    p99: args.p99,
    buckets: { ...args.buckets },
    summary: {
      count: args.count,
      sum: args.sum,
      min: args.count === 0 ? null : args.min,
      max: args.count === 0 ? null : args.max,
      histogram: { ...args.buckets },
    },
  };
}
