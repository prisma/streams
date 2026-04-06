import { Result } from "better-result";
import type { SchemaRegistry, SearchConfig, SearchFieldBinding, SearchFieldConfig } from "../schema/registry";
import { resolvePointerResult } from "../util/json_pointer";
import { schemaVersionForOffset } from "../schema/read_json";

export type SearchExactTerm = {
  field: string;
  config: SearchFieldConfig;
  canonical: string;
  bytes: Uint8Array;
};

export function resolveSearchAlias(search: SearchConfig | undefined, fieldName: string): string {
  return search?.aliases?.[fieldName] ?? fieldName;
}

export function getSearchFieldConfig(search: SearchConfig | undefined, fieldName: string): SearchFieldConfig | null {
  const resolved = resolveSearchAlias(search, fieldName);
  return search?.fields?.[resolved] ?? null;
}

export function getSearchFieldBinding(config: SearchFieldConfig, version: number): SearchFieldBinding | null {
  let selected: SearchFieldBinding | null = null;
  for (const binding of config.bindings) {
    if (binding.version <= version && (!selected || binding.version > selected.version)) {
      selected = binding;
    }
  }
  return selected;
}

export function normalizeKeywordValue(value: unknown, normalizer: SearchFieldConfig["normalizer"]): string | null {
  if (typeof value !== "string") return null;
  return normalizer === "lowercase_v1" ? value.toLowerCase() : value;
}

export function canonicalizeExactValue(config: SearchFieldConfig, value: unknown): string | null {
  switch (config.kind) {
    case "keyword":
      return normalizeKeywordValue(value, config.normalizer);
    case "integer":
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)) return String(value);
      if (typeof value === "string" && /^-?(0|[1-9][0-9]*)$/.test(value.trim())) return String(BigInt(value.trim()));
      return null;
    case "float":
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "number" && Number.isFinite(value)) return String(value);
      if (typeof value === "string" && value.trim() !== "") {
        const n = Number(value);
        if (Number.isFinite(n)) return String(n);
      }
      return null;
    case "date":
      if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
      if (typeof value === "bigint") return value.toString();
      if (typeof value === "string" && value.trim() !== "") {
        const parsed = Date.parse(value);
        if (Number.isFinite(parsed)) return String(Math.trunc(parsed));
        if (/^-?(0|[1-9][0-9]*)$/.test(value.trim())) return String(BigInt(value.trim()));
      }
      return null;
    case "bool":
      if (typeof value === "boolean") return value ? "true" : "false";
      if (typeof value === "string") {
        const lowered = value.trim().toLowerCase();
        if (lowered === "true" || lowered === "false") return lowered;
      }
      return null;
    default:
      return null;
  }
}

export function canonicalizeColumnValue(config: SearchFieldConfig, value: unknown): bigint | number | boolean | null {
  switch (config.kind) {
    case "integer": {
      const canonical = canonicalizeExactValue(config, value);
      return canonical == null ? null : BigInt(canonical);
    }
    case "date": {
      const canonical = canonicalizeExactValue(config, value);
      return canonical == null ? null : BigInt(canonical);
    }
    case "float": {
      const canonical = canonicalizeExactValue(config, value);
      if (canonical == null) return null;
      const parsed = Number(canonical);
      return Number.isFinite(parsed) ? parsed : null;
    }
    case "bool":
      return canonicalizeExactValue(config, value) === "true"
        ? true
        : canonicalizeExactValue(config, value) === "false"
          ? false
          : null;
    default:
      return null;
  }
}

export function analyzeTextValue(value: string, analyzer: SearchFieldConfig["analyzer"]): string[] {
  if (analyzer !== "unicode_word_v1") return [];
  const matches = value.toLowerCase().match(/[\p{L}\p{N}]+/gu);
  return matches ? matches.filter((token) => token.length > 0) : [];
}

function addRawValues(out: unknown[], value: unknown): void {
  if (Array.isArray(value)) {
    for (const item of value) addRawValues(out, item);
    return;
  }
  out.push(value);
}

export function extractRawSearchValuesForFieldsResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown,
  fieldNames: Iterable<string>
): Result<Map<string, unknown[]>, { message: string }> {
  if (!reg.search) return Result.ok(new Map());
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return Result.err({ message: "search fields require JSON object records" });
  }
  const version = schemaVersionForOffset(reg, offset);
  const out = new Map<string, unknown[]>();
  for (const fieldName of fieldNames) {
    const config = reg.search.fields[fieldName];
    if (!config) continue;
    const binding = getSearchFieldBinding(config, version);
    if (!binding) continue;
    const resolvedRes = resolvePointerResult(value, binding.jsonPointer);
    if (Result.isError(resolvedRes)) return Result.err({ message: resolvedRes.error.message });
    if (!resolvedRes.value.exists) continue;
    const values: unknown[] = [];
    addRawValues(values, resolvedRes.value.value);
    if (values.length > 0) out.set(fieldName, values);
  }
  return Result.ok(out);
}

export function extractRawSearchValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, unknown[]>, { message: string }> {
  return extractRawSearchValuesForFieldsResult(reg, offset, value, Object.keys(reg.search?.fields ?? {}));
}

export function extractSearchExactTermsResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<SearchExactTerm[], { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out: SearchExactTerm[] = [];
  const seen = new Set<string>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config?.exact) continue;
    for (const rawValue of values) {
      const canonical = canonicalizeExactValue(config, rawValue);
      if (canonical == null) continue;
      const dedupeKey = `${fieldName}\u0000${canonical}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      out.push({
        field: fieldName,
        config,
        canonical,
        bytes: new TextEncoder().encode(canonical),
      });
    }
  }
  return Result.ok(out);
}

export function extractSearchExactValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, string[]>, { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out = new Map<string, string[]>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config) continue;
    const exactValues: string[] = [];
    for (const rawValue of values) {
      const canonical = canonicalizeExactValue(config, rawValue);
      if (canonical != null) exactValues.push(canonical);
    }
    if (exactValues.length > 0) out.set(fieldName, exactValues);
  }
  return Result.ok(out);
}

export function extractSearchColumnValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, Array<bigint | number | boolean>>, { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out = new Map<string, Array<bigint | number | boolean>>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config?.column) continue;
    const colValues: Array<bigint | number | boolean> = [];
    for (const rawValue of values) {
      const normalized = canonicalizeColumnValue(config, rawValue);
      if (normalized != null) colValues.push(normalized);
    }
    if (colValues.length > 0) out.set(fieldName, colValues);
  }
  return Result.ok(out);
}

export function extractSearchTextValuesResult(
  reg: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, string[]>, { message: string }> {
  const rawValuesRes = extractRawSearchValuesResult(reg, offset, value);
  if (Result.isError(rawValuesRes)) return rawValuesRes;
  const out = new Map<string, string[]>();
  for (const [fieldName, values] of rawValuesRes.value) {
    const config = reg.search?.fields[fieldName];
    if (!config) continue;
    const textValues: string[] = [];
    for (const rawValue of values) {
      if (config.kind === "keyword") {
        const normalized = normalizeKeywordValue(rawValue, config.normalizer);
        if (normalized != null) textValues.push(normalized);
      } else if (config.kind === "text" && typeof rawValue === "string") {
        textValues.push(rawValue);
      }
    }
    if (textValues.length > 0) out.set(fieldName, textValues);
  }
  return Result.ok(out);
}
