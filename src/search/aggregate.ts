import { Result } from "better-result";
import type {
  SchemaRegistry,
  SearchConfig,
  SearchRollupConfig,
} from "../schema/registry";
import { parseTimestampMsResult } from "../util/time";
import { resolvePointerResult } from "../util/json_pointer";
import {
  canonicalizeColumnValue,
  canonicalizeExactValue,
  extractRawSearchValuesForFieldsResult,
} from "./schema";
import {
  collectPositiveSearchExactClauses,
  parseSearchQueryResult,
  type CompiledSearchQuery,
} from "./query";
import type { AggMeasureState, AggSummaryState } from "./agg_format";

export type AggregateRequest = {
  rollup: string;
  fromMs: bigint;
  toMs: bigint;
  interval: string;
  intervalMs: number;
  q: CompiledSearchQuery | null;
  groupBy: string[];
  measures: string[] | null;
};

export type RollupEligibility = {
  eligible: boolean;
  exactFilters: Record<string, string>;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function parseTimeValueResult(raw: unknown, path: string): Result<bigint, { message: string }> {
  if (typeof raw === "string") {
    const parsedRes = parseTimestampMsResult(raw);
    if (Result.isError(parsedRes)) return Result.err({ message: `${path} must be a valid timestamp` });
    return Result.ok(parsedRes.value);
  }
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Result.ok(BigInt(Math.trunc(raw)));
  }
  return Result.err({ message: `${path} must be a timestamp string or unix milliseconds` });
}

function parseIntervalMsResult(search: SearchConfig, rollupName: string, interval: string): Result<number, { message: string }> {
  const rollup = search.rollups?.[rollupName];
  if (!rollup) return Result.err({ message: `unknown rollup ${rollupName}` });
  if (!rollup.intervals.includes(interval)) return Result.err({ message: `interval ${interval} is not configured for rollup ${rollupName}` });
  const parsed = interval.trim();
  const match = /^(\d+)(ms|s|m|h|d)$/.exec(parsed);
  if (!match) return Result.err({ message: `interval ${interval} is not a supported duration` });
  const value = Number(match[1]);
  const unit = match[2];
  const multiplier = unit === "ms" ? 1 : unit === "s" ? 1000 : unit === "m" ? 60_000 : unit === "h" ? 3_600_000 : 86_400_000;
  return Result.ok(value * multiplier);
}

export function resolveRollupConfigResult(
  registry: SchemaRegistry,
  rollupName: string
): Result<SearchRollupConfig, { message: string }> {
  const search = registry.search;
  if (!search) return Result.err({ message: "search is not configured for this stream" });
  const rollup = search.rollups?.[rollupName];
  if (!rollup) return Result.err({ message: `unknown rollup ${rollupName}` });
  return Result.ok(rollup);
}

export function parseAggregateRequestBodyResult(
  registry: SchemaRegistry,
  raw: unknown
): Result<AggregateRequest, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: "aggregate request must be an object" });
  const search = registry.search;
  if (!search) return Result.err({ message: "search is not configured for this stream" });
  if (typeof raw.rollup !== "string" || raw.rollup.trim() === "") return Result.err({ message: "rollup must be a string" });
  const rollupName = raw.rollup.trim();
  const rollupRes = resolveRollupConfigResult(registry, rollupName);
  if (Result.isError(rollupRes)) return rollupRes;
  const fromRes = parseTimeValueResult(raw.from, "from");
  if (Result.isError(fromRes)) return fromRes;
  const toRes = parseTimeValueResult(raw.to, "to");
  if (Result.isError(toRes)) return toRes;
  if (toRes.value <= fromRes.value) return Result.err({ message: "to must be greater than from" });
  if (typeof raw.interval !== "string" || raw.interval.trim() === "") return Result.err({ message: "interval must be a string" });
  const interval = raw.interval.trim();
  const intervalMsRes = parseIntervalMsResult(search, rollupName, interval);
  if (Result.isError(intervalMsRes)) return intervalMsRes;

  let q: CompiledSearchQuery | null = null;
  if (raw.q !== undefined && raw.q !== null) {
    if (typeof raw.q !== "string") return Result.err({ message: "q must be a string" });
    const queryRes = parseSearchQueryResult(registry, raw.q);
    if (Result.isError(queryRes)) return queryRes;
    q = queryRes.value;
  }

  const configuredDimensions = new Set(rollupRes.value.dimensions ?? []);
  let groupBy: string[] = [];
  if (raw.group_by !== undefined) {
    if (!Array.isArray(raw.group_by)) return Result.err({ message: "group_by must be an array of strings" });
    const seen = new Set<string>();
    for (let i = 0; i < raw.group_by.length; i++) {
      if (typeof raw.group_by[i] !== "string") return Result.err({ message: `group_by[${i}] must be a string` });
      const field = raw.group_by[i].trim();
      if (!configuredDimensions.has(field)) return Result.err({ message: `group_by field ${field} is not configured on rollup ${rollupName}` });
      if (!seen.has(field)) {
        seen.add(field);
        groupBy.push(field);
      }
    }
    groupBy.sort((a, b) => a.localeCompare(b));
  }

  let measures: string[] | null = null;
  if (raw.measures !== undefined) {
    if (!Array.isArray(raw.measures) || raw.measures.length === 0) {
      return Result.err({ message: "measures must be a non-empty array of strings" });
    }
    const seen = new Set<string>();
    measures = [];
    for (let i = 0; i < raw.measures.length; i++) {
      if (typeof raw.measures[i] !== "string") return Result.err({ message: `measures[${i}] must be a string` });
      const measure = raw.measures[i].trim();
      if (!Object.prototype.hasOwnProperty.call(rollupRes.value.measures, measure)) {
        return Result.err({ message: `unknown measure ${measure} on rollup ${rollupName}` });
      }
      if (!seen.has(measure)) {
        seen.add(measure);
        measures.push(measure);
      }
    }
    measures.sort((a, b) => a.localeCompare(b));
  }

  return Result.ok({
    rollup: rollupName,
    fromMs: fromRes.value,
    toMs: toRes.value,
    interval,
    intervalMs: intervalMsRes.value,
    q,
    groupBy,
    measures,
  });
}

export function extractRollupEligibility(query: CompiledSearchQuery | null, dimensions: Set<string>): RollupEligibility {
  if (!query) return { eligible: true, exactFilters: {} };
  const clauses = collectPositiveSearchExactClauses(query);
  const exactFilters: Record<string, string> = {};
  const visit = (node: CompiledSearchQuery): boolean => {
    if (node.kind === "and") return visit(node.left) && visit(node.right);
    if (node.kind === "keyword") {
      if (node.prefix || !dimensions.has(node.field)) return false;
      exactFilters[node.field] = node.canonicalValue;
      return true;
    }
    if (node.kind === "compare") {
      if (node.op !== "eq" || !node.canonicalValue || !dimensions.has(node.field)) return false;
      exactFilters[node.field] = node.canonicalValue;
      return true;
    }
    return false;
  };
  if (!visit(query)) return { eligible: false, exactFilters: {} };
  for (const clause of clauses) {
    if (!dimensions.has(clause.field)) return { eligible: false, exactFilters: {} };
    exactFilters[clause.field] = clause.canonicalValue;
  }
  return { eligible: true, exactFilters };
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function extractHistogramObject(value: unknown): Record<string, number> | undefined {
  if (!isPlainObject(value)) return undefined;
  const out: Record<string, number> = {};
  for (const [key, raw] of Object.entries(value)) {
    const num = toFiniteNumber(raw);
    if (num == null) continue;
    out[key] = (out[key] ?? 0) + num;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

export function cloneAggMeasureState(state: AggMeasureState): AggMeasureState {
  if (state.kind === "count") return { kind: "count", value: state.value };
  return {
    kind: "summary",
    summary: {
      count: state.summary.count,
      sum: state.summary.sum,
      min: state.summary.min,
      max: state.summary.max,
      histogram: state.summary.histogram ? { ...state.summary.histogram } : undefined,
    },
  };
}

function mergeHistogram(target: Record<string, number> | undefined, next: Record<string, number> | undefined): Record<string, number> | undefined {
  if (!target && !next) return undefined;
  const out: Record<string, number> = { ...(target ?? {}) };
  for (const [bucket, value] of Object.entries(next ?? {})) {
    out[bucket] = (out[bucket] ?? 0) + value;
  }
  return out;
}

export function mergeAggMeasureState(target: AggMeasureState, next: AggMeasureState): AggMeasureState {
  if (target.kind === "count" && next.kind === "count") {
    target.value += next.value;
    return target;
  }
  if (target.kind === "summary" && next.kind === "summary") {
    target.summary.count += next.summary.count;
    target.summary.sum += next.summary.sum;
    target.summary.min =
      target.summary.min == null ? next.summary.min : next.summary.min == null ? target.summary.min : Math.min(target.summary.min, next.summary.min);
    target.summary.max =
      target.summary.max == null ? next.summary.max : next.summary.max == null ? target.summary.max : Math.max(target.summary.max, next.summary.max);
    target.summary.histogram = mergeHistogram(target.summary.histogram, next.summary.histogram);
    return target;
  }
  return target;
}

function computeHistogramPercentile(histogram: Record<string, number> | undefined, percentile: number): number | null {
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

export function formatAggMeasureState(state: AggMeasureState): unknown {
  if (state.kind === "count") {
    return { count: state.value };
  }
  const histogram = state.summary.histogram ? { ...state.summary.histogram } : undefined;
  const avg = state.summary.count > 0 ? state.summary.sum / state.summary.count : null;
  return {
    count: state.summary.count,
    sum: state.summary.sum,
    min: state.summary.min,
    max: state.summary.max,
    avg,
    p50: computeHistogramPercentile(histogram, 0.5),
    p95: computeHistogramPercentile(histogram, 0.95),
    p99: computeHistogramPercentile(histogram, 0.99),
    histogram,
  };
}

export function rollupRequiredFieldNames(registry: SchemaRegistry, rollup: SearchRollupConfig): string[] {
  const fields = new Set<string>();
  const timestampField = rollup.timestampField ?? registry.search?.primaryTimestampField;
  if (timestampField) fields.add(timestampField);
  for (const dimension of rollup.dimensions ?? []) fields.add(dimension);
  for (const measure of Object.values(rollup.measures)) {
    if (measure.kind === "summary") fields.add(measure.field);
  }
  return Array.from(fields);
}

export function extractRollupContributionResult(
  registry: SchemaRegistry,
  rollup: SearchRollupConfig,
  offset: bigint,
  value: unknown,
  precomputedRawValues?: Map<string, unknown[]>
): Result<{ timestampMs: number; dimensions: Record<string, string | null>; measures: Record<string, AggMeasureState> } | null, { message: string }> {
  if (!isPlainObject(value)) return Result.ok(null);
  const rawValuesRes = precomputedRawValues
    ? Result.ok(precomputedRawValues)
    : extractRawSearchValuesForFieldsResult(registry, offset, value, rollupRequiredFieldNames(registry, rollup));
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const rawValues = rawValuesRes.value;

  const timestampField = rollup.timestampField ?? registry.search?.primaryTimestampField;
  if (!timestampField) return Result.ok(null);
  const timestampConfig = registry.search?.fields[timestampField];
  if (!timestampConfig) return Result.ok(null);
  const timestampValues = rawValues.get(timestampField) ?? [];
  if (timestampValues.length !== 1) return Result.ok(null);
  const timestampValue = canonicalizeColumnValue(timestampConfig, timestampValues[0]);
  if (typeof timestampValue !== "bigint" && typeof timestampValue !== "number") return Result.ok(null);
  const timestampMs = typeof timestampValue === "bigint" ? Number(timestampValue) : Math.trunc(timestampValue);
  if (!Number.isFinite(timestampMs)) return Result.ok(null);

  const dimensions: Record<string, string | null> = {};
  for (const dimension of rollup.dimensions ?? []) {
    const config = registry.search?.fields[dimension];
    if (!config) return Result.ok(null);
    const values = rawValues.get(dimension) ?? [];
    if (values.length > 1) return Result.ok(null);
    if (values.length === 0) {
      dimensions[dimension] = null;
      continue;
    }
    const exactCanonical = canonicalizeExactValue(config, values[0]);
    dimensions[dimension] = exactCanonical == null ? null : exactCanonical;
  }

  const measures: Record<string, AggMeasureState> = {};
  for (const [measureName, measure] of Object.entries(rollup.measures)) {
    if (measure.kind === "count") {
      measures[measureName] = { kind: "count", value: 1 };
      continue;
    }
    if (measure.kind === "summary") {
      const config = registry.search?.fields[measure.field];
      if (!config) return Result.ok(null);
      const values = rawValues.get(measure.field) ?? [];
      if (values.length !== 1) return Result.ok(null);
      const numeric = toFiniteNumber(canonicalizeColumnValue(config, values[0]));
      if (numeric == null) return Result.ok(null);
      measures[measureName] = {
        kind: "summary",
        summary: {
          count: 1,
          sum: numeric,
          min: numeric,
          max: numeric,
          histogram: measure.histogram === "log2_v1" ? { [String(2 ** Math.floor(Math.log2(Math.max(1, Math.abs(numeric) || 1))))]: 1 } : undefined,
        },
      };
      continue;
    }
    const countResolved = resolvePointerResult(value, measure.countJsonPointer);
    if (Result.isError(countResolved) || !countResolved.value.exists) return Result.ok(null);
    const sumResolved = resolvePointerResult(value, measure.sumJsonPointer);
    if (Result.isError(sumResolved) || !sumResolved.value.exists) return Result.ok(null);
    const minResolved = resolvePointerResult(value, measure.minJsonPointer);
    if (Result.isError(minResolved) || !minResolved.value.exists) return Result.ok(null);
    const maxResolved = resolvePointerResult(value, measure.maxJsonPointer);
    if (Result.isError(maxResolved) || !maxResolved.value.exists) return Result.ok(null);
    const count = toFiniteNumber(countResolved.value.value);
    const sum = toFiniteNumber(sumResolved.value.value);
    const min = toFiniteNumber(minResolved.value.value);
    const max = toFiniteNumber(maxResolved.value.value);
    if (count == null || sum == null || min == null || max == null) return Result.ok(null);
    let histogram: Record<string, number> | undefined;
    if (measure.histogramJsonPointer) {
      const histResolved = resolvePointerResult(value, measure.histogramJsonPointer);
      if (!Result.isError(histResolved) && histResolved.value.exists) {
        histogram = extractHistogramObject(histResolved.value.value);
      }
    }
    measures[measureName] = {
      kind: "summary",
      summary: {
        count,
        sum,
        min,
        max,
        histogram,
      },
    };
  }

  return Result.ok({ timestampMs, dimensions, measures });
}
