import { createHash } from "node:crypto";
import { Result } from "better-result";
import type { SchemaRegistry, SearchFieldConfig } from "../schema/registry";
import { canonicalizeExactValue, extractSearchExactTermsResult, extractSearchExactValuesResult, getSearchFieldBinding } from "../search/schema";
import { schemaVersionForOffset } from "../schema/read_json";
import { resolvePointerResult } from "../util/json_pointer";

export type SecondaryIndexField = {
  name: string;
  config: SearchFieldConfig;
};

export type SecondaryIndexTerm = {
  index: SecondaryIndexField;
  canonical: string;
  bytes: Uint8Array;
};

function addRawValues(out: unknown[], value: unknown): void {
  if (Array.isArray(value)) {
    for (const item of value) addRawValues(out, item);
    return;
  }
  out.push(value);
}

export function getConfiguredSecondaryIndexes(registry: SchemaRegistry): SecondaryIndexField[] {
  const search = registry.search;
  if (!search) return [];
  return Object.entries(search.fields)
    .filter(([, config]) => config.exact === true)
    .map(([name, config]) => ({ name, config }));
}

export function hashSecondaryIndexField(index: SecondaryIndexField): string {
  return createHash("sha256")
    .update(
      JSON.stringify({
        name: index.name,
        kind: index.config.kind,
        bindings: index.config.bindings,
        normalizer: index.config.normalizer ?? null,
        analyzer: index.config.analyzer ?? null,
        exact: index.config.exact === true,
      })
    )
    .digest("hex");
}

export function canonicalizeSecondaryIndexValue(config: SearchFieldConfig, value: unknown): string | null {
  return canonicalizeExactValue(config, value);
}

export function extractSecondaryIndexTermsResult(
  registry: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<SecondaryIndexTerm[], { message: string }> {
  const termsRes = extractSearchExactTermsResult(registry, offset, value);
  if (Result.isError(termsRes)) return termsRes;
  return Result.ok(
    termsRes.value.map((term) => ({
      index: { name: term.field, config: term.config },
      canonical: term.canonical,
      bytes: term.bytes,
    }))
  );
}

export function extractSecondaryIndexValuesResult(
  registry: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<Map<string, string[]>, { message: string }> {
  return extractSearchExactValuesResult(registry, offset, value);
}

export function extractSecondaryIndexValuesForFieldResult(
  registry: SchemaRegistry,
  offset: bigint,
  value: unknown,
  index: SecondaryIndexField
): Result<string[], { message: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return Result.err({ message: "search fields require JSON object records" });
  }
  const version = schemaVersionForOffset(registry, offset);
  const binding = getSearchFieldBinding(index.config, version);
  if (!binding) return Result.ok([]);
  const resolvedRes = resolvePointerResult(value, binding.jsonPointer);
  if (Result.isError(resolvedRes)) return Result.err({ message: resolvedRes.error.message });
  if (!resolvedRes.value.exists) return Result.ok([]);

  const rawValues: unknown[] = [];
  addRawValues(rawValues, resolvedRes.value.value);
  const out: string[] = [];
  const seen = new Set<string>();
  for (const rawValue of rawValues) {
    const canonical = canonicalizeExactValue(index.config, rawValue);
    if (canonical == null || seen.has(canonical)) continue;
    seen.add(canonical);
    out.push(canonical);
  }
  return Result.ok(out);
}
