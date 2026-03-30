import { createHash } from "node:crypto";
import type { SchemaRegistry } from "../schema/registry";

export type SearchCompanionFamily = "col" | "fts" | "agg" | "mblk";

export type SearchCompanionPlan = {
  families: Record<SearchCompanionFamily, boolean>;
  summary: Record<string, unknown>;
};

export function buildDesiredSearchCompanionPlan(registry: SchemaRegistry): SearchCompanionPlan {
  const search = registry.search;
  const families: Record<SearchCompanionFamily, boolean> = {
    col: false,
    fts: false,
    agg: false,
    mblk: false,
  };
  if (!search) {
    return { families, summary: { search: null } };
  }
  const colFields = Object.entries(search.fields)
    .filter(([, field]) => field.column === true)
    .map(([name, field]) => ({
      name,
      kind: field.kind,
      bindings: field.bindings,
      exists: field.exists === true,
      sortable: field.sortable === true,
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
  const ftsFields = Object.entries(search.fields)
    .filter(([, field]) => field.kind === "keyword" || field.kind === "text")
    .map(([name, field]) => ({
      name,
      kind: field.kind,
      bindings: field.bindings,
      exact: field.exact === true,
      prefix: field.prefix === true,
      positions: field.positions === true,
      analyzer: field.analyzer ?? null,
      normalizer: field.normalizer ?? null,
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
  const aggRollups = Object.entries(search.rollups ?? {})
    .map(([name, rollup]) => ({
      name,
      timestampField: rollup.timestampField ?? search.primaryTimestampField ?? null,
      dimensions: [...(rollup.dimensions ?? [])].sort(),
      intervals: [...rollup.intervals].sort(),
      measures: Object.keys(rollup.measures).sort(),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
  families.col = colFields.length > 0;
  families.fts = ftsFields.length > 0;
  families.agg = aggRollups.length > 0;
  families.mblk = search.profile === "metrics";
  return {
    families,
    summary: {
      primaryTimestampField: search.primaryTimestampField ?? null,
      profile: search.profile ?? null,
      colFields,
      ftsFields,
      aggRollups,
    },
  };
}

export function hashSearchCompanionPlan(plan: SearchCompanionPlan): string {
  return createHash("sha256").update(JSON.stringify(plan)).digest("hex");
}

