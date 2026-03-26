import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { SEARCH_PREFIX_TERM_LIMIT, filterDocIdsByFtsClauseResult } from "../src/search/fts_runtime";
import type { FtsSegmentCompanion } from "../src/search/fts_format";

const companion: FtsSegmentCompanion = {
  version: 1,
  stream: "s",
  segment_index: 0,
  doc_count: 3,
  fields: {
    service: {
      kind: "keyword",
      exact: true,
      prefix: true,
      exists_docs: [0, 1, 2],
      terms: {
        "billing-api": [{ d: 0 }, { d: 2 }],
        "billing-worker": [{ d: 1 }],
      },
    },
    message: {
      kind: "text",
      positions: true,
      exists_docs: [0, 1, 2],
      doc_lengths: [3, 3, 4],
      terms: {
        card: [{ d: 0, p: [0] }, { d: 2, p: [0] }],
        declined: [{ d: 0, p: [1] }, { d: 2, p: [1] }],
        issuer: [{ d: 0, p: [2] }],
        retry: [{ d: 1, p: [0] }],
        later: [{ d: 1, p: [1] }],
        job: [{ d: 1, p: [2] }],
        payment: [{ d: 2, p: [2] }],
        failed: [{ d: 2, p: [3] }],
      },
    },
  },
};

describe(".fts runtime", () => {
  test("supports exact, prefix, term, and phrase candidate extraction", () => {
    const exact = filterDocIdsByFtsClauseResult({
      companion,
      clause: { kind: "keyword", field: "service", canonicalValue: "billing-api", prefix: false },
    });
    expect(Result.isOk(exact)).toBe(true);
    if (Result.isError(exact)) return;
    expect(Array.from(exact.value).sort((a, b) => a - b)).toEqual([0, 2]);

    const prefix = filterDocIdsByFtsClauseResult({
      companion,
      clause: { kind: "keyword", field: "service", canonicalValue: "billing-", prefix: true },
    });
    expect(Result.isOk(prefix)).toBe(true);
    if (Result.isError(prefix)) return;
    expect(Array.from(prefix.value).sort((a, b) => a - b)).toEqual([0, 1, 2]);

    const text = filterDocIdsByFtsClauseResult({
      companion,
      clause: {
        kind: "text",
        fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
        tokens: ["declined"],
        phrase: false,
        prefix: false,
      },
    });
    expect(Result.isOk(text)).toBe(true);
    if (Result.isError(text)) return;
    expect(Array.from(text.value).sort((a, b) => a - b)).toEqual([0, 2]);

    const phrase = filterDocIdsByFtsClauseResult({
      companion,
      clause: {
        kind: "text",
        fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1", positions: true }, boost: 1 }],
        tokens: ["card", "declined"],
        phrase: true,
        prefix: false,
      },
    });
    expect(Result.isOk(phrase)).toBe(true);
    if (Result.isError(phrase)) return;
    expect(Array.from(phrase.value).sort((a, b) => a - b)).toEqual([0, 2]);
  });

  test("returns an explicit error when prefix expansion exceeds the limit", () => {
    const manyTerms: Record<string, { d: number }[]> = {};
    for (let i = 0; i < SEARCH_PREFIX_TERM_LIMIT + 1; i++) {
      manyTerms[`req-${i}`] = [{ d: 0 }];
    }
    const overloaded: FtsSegmentCompanion = {
      ...companion,
      fields: {
        ...companion.fields,
        requestId: {
          kind: "keyword",
          exact: true,
          prefix: true,
          exists_docs: [0],
          terms: manyTerms,
        },
      },
    };
    const res = filterDocIdsByFtsClauseResult({
      companion: overloaded,
      clause: { kind: "keyword", field: "requestId", canonicalValue: "req-", prefix: true },
    });
    expect(Result.isError(res)).toBe(true);
    if (Result.isOk(res)) return;
    expect(res.error.message).toContain("prefix expansion exceeds limit");
  });
});
