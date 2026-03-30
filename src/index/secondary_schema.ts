import { createHash } from "node:crypto";
import { Result } from "better-result";
import type { SchemaRegistry, SearchFieldConfig } from "../schema/registry";
import { canonicalizeExactValue, extractSearchExactTermsResult, extractSearchExactValuesResult } from "../search/schema";

export type SecondaryIndexField = {
  name: string;
  config: SearchFieldConfig;
};

export type SecondaryIndexTerm = {
  index: SecondaryIndexField;
  canonical: string;
  bytes: Uint8Array;
};

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
