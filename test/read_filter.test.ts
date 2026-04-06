import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import {
  collectPositiveColumnFilterClauses,
  collectPositiveExactFilterClauses,
  evaluateReadFilterResult,
  parseReadFilterResult,
} from "../src/read_filter";
import {
  SCHEMA_REGISTRY_API_VERSION,
  type SchemaRegistry,
} from "../src/schema/registry";

const registry: SchemaRegistry = {
  apiVersion: SCHEMA_REGISTRY_API_VERSION,
  schema: "evlog",
  currentVersion: 1,
  boundaries: [{ offset: 0, version: 1 }],
  schemas: { "1": { type: "object" } },
  lenses: {},
  search: {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: {
        kind: "date",
        bindings: [{ version: 1, jsonPointer: "/eventTime" }],
        exact: true,
        column: true,
        exists: true,
        sortable: true,
      },
      service: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/service" }],
        normalizer: "lowercase_v1",
        exact: true,
        prefix: true,
        exists: true,
      },
      status: {
        kind: "integer",
        bindings: [{ version: 1, jsonPointer: "/status" }],
        exact: true,
        column: true,
        exists: true,
      },
      ok: {
        kind: "bool",
        bindings: [{ version: 1, jsonPointer: "/ok" }],
        exact: true,
        column: true,
        exists: true,
      },
      requestId: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/requestId" }],
        exact: true,
        prefix: true,
        exists: true,
      },
      why: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/why" }],
        exact: true,
        exists: true,
      },
    },
  },
};

describe("read filter", () => {
  test("parses and evaluates boolean predicates over indexed fields", () => {
    const filterRes = parseReadFilterResult(
      registry,
      '(service:api OR service:worker) status:>=500 -has:why'
    );
    expect(Result.isOk(filterRes)).toBe(true);
    if (Result.isError(filterRes)) return;

    const matchRes = evaluateReadFilterResult(registry, 0n, filterRes.value, {
      service: "api",
      status: 503,
    });
    expect(Result.isOk(matchRes)).toBe(true);
    if (Result.isError(matchRes)) return;
    expect(matchRes.value).toBe(true);

    const excludedRes = evaluateReadFilterResult(registry, 0n, filterRes.value, {
      service: "api",
      status: 503,
      why: "database down",
    });
    expect(Result.isOk(excludedRes)).toBe(true);
    if (Result.isError(excludedRes)) return;
    expect(excludedRes.value).toBe(false);
  });

  test("extracts only safe exact clauses for segment pruning", () => {
    const filterRes = parseReadFilterResult(
      registry,
      '(service:api OR service:worker) status:>=500 requestId:req_123'
    );
    expect(Result.isOk(filterRes)).toBe(true);
    if (Result.isError(filterRes)) return;

    expect(collectPositiveExactFilterClauses(filterRes.value)).toEqual([
      { field: "requestId", canonicalValue: "req_123" },
    ]);
  });

  test("preserves typed boolean equality for column pruning", () => {
    const filterRes = parseReadFilterResult(registry, "ok:false");
    expect(Result.isOk(filterRes)).toBe(true);
    if (Result.isError(filterRes)) return;

    expect(collectPositiveColumnFilterClauses(filterRes.value)).toEqual([
      { field: "ok", op: "eq", compareValue: false },
    ]);

    const matchRes = evaluateReadFilterResult(registry, 0n, filterRes.value, {
      ok: false,
    });
    expect(Result.isOk(matchRes)).toBe(true);
    if (Result.isError(matchRes)) return;
    expect(matchRes.value).toBe(true);
  });
});
