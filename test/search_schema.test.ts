import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { parseSchemaUpdateResult } from "../src/schema/registry";
import { parseSearchRequestBodyResult } from "../src/search/query";

describe("search schema config", () => {
  test("defaults non-scoring filters to offset-desc and scoring text to relevance", () => {
    const registry = {
      search: {
        primaryTimestampField: "eventTime",
        defaultFields: [{ field: "message", boost: 1 }],
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
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
            sortable: true,
          },
          message: {
            kind: "text",
            bindings: [{ version: 1, jsonPointer: "/message" }],
            analyzer: "unicode_word_v1",
            exists: true,
          },
        },
      },
    } as any;

    const filterRes = parseSearchRequestBodyResult(registry, { q: "service:checkout" });
    expect(Result.isOk(filterRes)).toBe(true);
    if (Result.isError(filterRes)) return;
    expect(filterRes.value.sort).toEqual([{ kind: "offset", direction: "desc" }]);

    const textRes = parseSearchRequestBodyResult(registry, { q: "checkout" });
    expect(Result.isOk(textRes)).toBe(true);
    if (Result.isError(textRes)) return;
    expect(textRes.value.sort.map((sort) => sort.kind)).toEqual(["score", "field", "offset"]);
  });

  test("accepts versioned search bindings, aliases, and default fields", () => {
    const res = parseSchemaUpdateResult({
      schema: {
        type: "object",
        additionalProperties: true,
      },
      search: {
        primaryTimestampField: "eventTime",
        aliases: { req: "requestId" },
        defaultFields: [{ field: "message", boost: 2 }],
        fields: {
          eventTime: {
            kind: "date",
            bindings: [
              { version: 1, jsonPointer: "/eventTime" },
              { version: 2, jsonPointer: "/ts" },
            ],
            column: true,
            exists: true,
            sortable: true,
          },
          requestId: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/requestId" }],
            normalizer: "lowercase_v1",
            exact: true,
            prefix: true,
            exists: true,
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
    });
    expect(Result.isOk(res)).toBe(true);
    if (Result.isError(res)) return;
    expect(res.value.search?.aliases).toEqual({ req: "requestId" });
    expect(res.value.search?.fields.message.positions).toBe(true);
  });

  test("accepts search rollups for count, summary, and summary_parts", () => {
    const res = parseSchemaUpdateResult({
      schema: {
        type: "object",
        additionalProperties: true,
      },
      search: {
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            column: true,
            exact: true,
            exists: true,
            sortable: true,
          },
          service: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/service" }],
            exact: true,
            prefix: true,
            exists: true,
          },
          duration: {
            kind: "float",
            bindings: [{ version: 1, jsonPointer: "/duration" }],
            column: true,
            exact: true,
            exists: true,
            sortable: true,
            aggregatable: true,
          },
        },
        rollups: {
          requests: {
            dimensions: ["service"],
            intervals: ["1m", "5m"],
            measures: {
              count: { kind: "count" },
              latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
            },
          },
          metrics: {
            dimensions: ["service"],
            intervals: ["1m"],
            measures: {
              duration: {
                kind: "summary_parts",
                countJsonPointer: "/metric/count",
                sumJsonPointer: "/metric/sum",
                minJsonPointer: "/metric/min",
                maxJsonPointer: "/metric/max",
                histogramJsonPointer: "/metric/histogram",
              },
            },
          },
        },
      },
    });
    expect(Result.isOk(res)).toBe(true);
    if (Result.isError(res)) return;
    expect(res.value.search?.rollups?.requests.measures.latency).toEqual({
      kind: "summary",
      field: "duration",
      histogram: "log2_v1",
    });
    expect(res.value.search?.rollups?.metrics.measures.duration).toEqual({
      kind: "summary_parts",
      countJsonPointer: "/metric/count",
      sumJsonPointer: "/metric/sum",
      minJsonPointer: "/metric/min",
      maxJsonPointer: "/metric/max",
      histogramJsonPointer: "/metric/histogram",
    });
  });

  test("rejects legacy indexes[]", () => {
    const res = parseSchemaUpdateResult({
      indexes: [{ name: "service", jsonPointer: "/service", kind: "keyword" }],
    });
    expect(Result.isError(res)).toBe(true);
    if (Result.isOk(res)) return;
    expect(res.error.message).toContain("indexes");
  });

  test("rejects invalid search capability combinations and kinds", () => {
    const badText = parseSchemaUpdateResult({
      search: {
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            column: true,
            exists: true,
            sortable: true,
          },
          message: {
            kind: "text",
            bindings: [{ version: 1, jsonPointer: "/message" }],
            analyzer: "unicode_word_v1",
            column: true,
          },
        },
      },
    });
    expect(Result.isError(badText)).toBe(true);

    const badKind = parseSchemaUpdateResult({
      search: {
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "timestamp",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
          },
        },
      },
    });
    expect(Result.isError(badKind)).toBe(true);
  });

  test("rejects invalid rollup definitions", () => {
    const badRollup = parseSchemaUpdateResult({
      schema: {
        type: "object",
        additionalProperties: true,
      },
      search: {
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            column: true,
            exact: true,
            exists: true,
            sortable: true,
          },
          duration: {
            kind: "float",
            bindings: [{ version: 1, jsonPointer: "/duration" }],
            column: true,
            exact: true,
            exists: true,
            sortable: true,
          },
        },
        rollups: {
          requests: {
            dimensions: ["duration"],
            intervals: [],
            measures: {
              latency: { kind: "summary", field: "duration" },
            },
          },
        },
      },
    });
    expect(Result.isError(badRollup)).toBe(true);
    if (Result.isOk(badRollup)) return;
    expect(badRollup.error.message).toContain("intervals");
  });
});
