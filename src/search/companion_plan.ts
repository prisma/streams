import { createHash } from "node:crypto";
import { Result } from "better-result";
import type {
  SchemaRegistry,
  SearchFieldBinding,
  SearchFieldConfig,
  SearchFieldKind,
  SearchRollupMeasureConfig,
} from "../schema/registry";
import { parseDurationMsResult } from "../util/duration";
import { dsError } from "../util/ds_error";

export type SearchCompanionFamily = "exact" | "col" | "fts" | "agg" | "mblk";

export type SearchCompanionPlanField = {
  ordinal: number;
  name: string;
  kind: SearchFieldKind;
  bindings: SearchFieldBinding[];
  normalizer: SearchFieldConfig["normalizer"] | null;
  analyzer: SearchFieldConfig["analyzer"] | null;
  exact: boolean;
  prefix: boolean;
  column: boolean;
  exists: boolean;
  sortable: boolean;
  aggregatable: boolean;
  contains: boolean;
  positions: boolean;
};

export type SearchCompanionPlanRollupMeasure = {
  ordinal: number;
  name: string;
  kind: SearchRollupMeasureConfig["kind"];
  field_ordinal: number | null;
  histogram: "log2_v1" | null;
};

export type SearchCompanionPlanRollupInterval = {
  ordinal: number;
  name: string;
  ms: number;
};

export type SearchCompanionPlanRollup = {
  ordinal: number;
  name: string;
  timestamp_field_ordinal: number | null;
  dimension_ordinals: number[];
  intervals: SearchCompanionPlanRollupInterval[];
  measures: SearchCompanionPlanRollupMeasure[];
};

export type SearchCompanionPlan = {
  families: Record<SearchCompanionFamily, boolean>;
  fields: SearchCompanionPlanField[];
  rollups: SearchCompanionPlanRollup[];
  summary: Record<string, unknown>;
};

export function buildDesiredSearchCompanionPlan(registry: SchemaRegistry): SearchCompanionPlan {
  const search = registry.search;
  const families: Record<SearchCompanionFamily, boolean> = {
    exact: false,
    col: false,
    fts: false,
    agg: false,
    mblk: false,
  };
  if (!search) {
    return { families, fields: [], rollups: [], summary: { search: null } };
  }

  const wantedFieldNames = new Set<string>();
  for (const [name, field] of Object.entries(search.fields)) {
    if (field.exact === true && field.kind !== "text") wantedFieldNames.add(name);
    if (field.column === true) wantedFieldNames.add(name);
    if (field.kind === "text" || (field.kind === "keyword" && field.prefix === true)) wantedFieldNames.add(name);
  }
  for (const rollup of Object.values(search.rollups ?? {})) {
    const timestampField = rollup.timestampField ?? search.primaryTimestampField;
    if (timestampField) wantedFieldNames.add(timestampField);
    for (const dimension of rollup.dimensions ?? []) wantedFieldNames.add(dimension);
    for (const measure of Object.values(rollup.measures)) {
      if (measure.kind === "summary") wantedFieldNames.add(measure.field);
    }
  }

  const orderedFieldNames = Array.from(wantedFieldNames).sort((a, b) => a.localeCompare(b));
  const fieldOrdinalByName = new Map<string, number>();
  const fields = orderedFieldNames.map((name, ordinal) => {
    const field = search.fields[name]!;
    fieldOrdinalByName.set(name, ordinal);
    return {
      ordinal,
      name,
      kind: field.kind,
      bindings: field.bindings.map((binding) => ({ version: binding.version, jsonPointer: binding.jsonPointer })),
      normalizer: field.normalizer ?? null,
      analyzer: field.analyzer ?? null,
      exact: field.exact === true,
      prefix: field.prefix === true,
      column: field.column === true,
      exists: field.exists === true,
      sortable: field.sortable === true,
      aggregatable: field.aggregatable === true,
      contains: field.contains === true,
      positions: field.positions === true,
    } satisfies SearchCompanionPlanField;
  });

  const colFields = fields.filter((field) => field.column);
  const exactFields = fields.filter((field) => field.exact && field.kind !== "text");
  const ftsFields = fields.filter((field) => field.kind === "text" || (field.kind === "keyword" && field.prefix));
  const rollups = Object.entries(search.rollups ?? {})
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, rollup], rollupOrdinal) => {
      const intervals = [...rollup.intervals]
        .sort()
        .map((intervalName, intervalOrdinal) => {
          const parsed = parseDurationMsResult(intervalName);
          if (Result.isError(parsed)) {
            throw dsError(parsed.error.message);
          }
          return {
            ordinal: intervalOrdinal,
            name: intervalName,
            ms: parsed.value,
          } satisfies SearchCompanionPlanRollupInterval;
        });
      const measures = Object.entries(rollup.measures)
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([measureName, measure], measureOrdinal) => ({
          ordinal: measureOrdinal,
          name: measureName,
          kind: measure.kind,
          field_ordinal: measure.kind === "summary" ? (fieldOrdinalByName.get(measure.field) ?? null) : null,
          histogram: measure.kind === "summary" ? measure.histogram ?? null : null,
        }));
      return {
        ordinal: rollupOrdinal,
        name,
        timestamp_field_ordinal: fieldOrdinalByName.get(rollup.timestampField ?? search.primaryTimestampField) ?? null,
        dimension_ordinals: [...(rollup.dimensions ?? [])]
          .sort((a, b) => a.localeCompare(b))
          .map((dimension) => fieldOrdinalByName.get(dimension))
          .filter((value): value is number => typeof value === "number"),
        intervals,
        measures,
      } satisfies SearchCompanionPlanRollup;
    });

  families.exact = exactFields.length > 0;
  families.col = colFields.length > 0;
  families.fts = ftsFields.length > 0;
  families.agg = rollups.length > 0;
  families.mblk = search.profile === "metrics";
  return {
    families,
    fields,
    rollups,
    summary: {
      primaryTimestampField: search.primaryTimestampField ?? null,
      primaryTimestampFieldOrdinal: fieldOrdinalByName.get(search.primaryTimestampField) ?? null,
      profile: search.profile ?? null,
      exactFields: exactFields.map((field) => ({
        ordinal: field.ordinal,
        name: field.name,
        kind: field.kind,
        bindings: field.bindings,
        normalizer: field.normalizer,
      })),
      colFields: colFields.map((field) => ({
        ordinal: field.ordinal,
        name: field.name,
        kind: field.kind,
        bindings: field.bindings,
        exists: field.exists,
        sortable: field.sortable,
      })),
      ftsFields: ftsFields.map((field) => ({
        ordinal: field.ordinal,
        name: field.name,
        kind: field.kind,
        bindings: field.bindings,
        exact: field.exact,
        prefix: field.prefix,
        positions: field.positions,
        analyzer: field.analyzer,
        normalizer: field.normalizer,
      })),
      aggRollups: rollups.map((rollup) => ({
        ordinal: rollup.ordinal,
        name: rollup.name,
        timestampFieldOrdinal: rollup.timestamp_field_ordinal,
        dimensions: rollup.dimension_ordinals,
        intervals: rollup.intervals.map((interval) => ({ ordinal: interval.ordinal, name: interval.name, ms: interval.ms })),
        measures: rollup.measures.map((measure) => ({
          ordinal: measure.ordinal,
          name: measure.name,
          kind: measure.kind,
          fieldOrdinal: measure.field_ordinal,
          histogram: measure.histogram,
        })),
      })),
    },
  };
}

export function hashSearchCompanionPlan(plan: SearchCompanionPlan): string {
  return createHash("sha256").update(JSON.stringify(plan)).digest("hex");
}

export function getPlanFieldByName(plan: SearchCompanionPlan, fieldName: string): SearchCompanionPlanField | null {
  return plan.fields.find((field) => field.name === fieldName) ?? null;
}

export function getPlanFieldByOrdinal(plan: SearchCompanionPlan, ordinal: number): SearchCompanionPlanField | null {
  return plan.fields.find((field) => field.ordinal === ordinal) ?? null;
}

export function getPlanRollupByName(plan: SearchCompanionPlan, rollupName: string): SearchCompanionPlanRollup | null {
  return plan.rollups.find((rollup) => rollup.name === rollupName) ?? null;
}

export function getPlanRollupByOrdinal(plan: SearchCompanionPlan, ordinal: number): SearchCompanionPlanRollup | null {
  return plan.rollups.find((rollup) => rollup.ordinal === ordinal) ?? null;
}
