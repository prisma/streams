import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { parseSearchQueryResult, collectPositiveSearchExactClauses, collectPositiveSearchFtsClauses } from "../src/search/query";

const REGISTRY = {
  apiVersion: "durable.streams/schema-registry/v1",
  schema: "s",
  currentVersion: 1,
  boundaries: [],
  schemas: {},
  lenses: {},
  search: {
    primaryTimestampField: "timestamp",
    aliases: {
      env: "environment",
    },
    fields: {
      timestamp: {
        kind: "date",
        bindings: [{ version: 1, jsonPointer: "/timestamp" }],
        sortable: true,
      },
      environment: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/environment" }],
        normalizer: "lowercase_v1",
        exact: true,
        prefix: true,
        exists: true,
        sortable: true,
      },
      message: {
        kind: "text",
        bindings: [{ version: 1, jsonPointer: "/message" }],
        analyzer: "unicode_word_v1",
        exists: true,
        positions: true,
      },
    },
  },
} as const;

describe("search query clause collectors", () => {
  test("can omit exact keyword clauses that are already covered by the fts family", () => {
    const queryRes = parseSearchQueryResult(REGISTRY as any, 'env:"staging" timeout');
    expect(Result.isOk(queryRes)).toBe(true);
    if (Result.isError(queryRes)) return;

    const query = queryRes.value;
    expect(collectPositiveSearchFtsClauses(query)).toEqual([
      { kind: "keyword", field: "environment", canonicalValue: "staging", prefix: false },
      {
        kind: "text",
        fields: [{ field: "message", config: REGISTRY.search.fields.message, boost: 1 }],
        tokens: ["timeout"],
        phrase: false,
        prefix: false,
      },
    ]);
    expect(collectPositiveSearchExactClauses(query)).toEqual([{ field: "environment", canonicalValue: "staging" }]);
    expect(collectPositiveSearchExactClauses(query, { excludeFtsKeywordClauses: true })).toEqual([]);
  });
});
