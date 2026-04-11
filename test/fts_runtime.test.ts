import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { buildDesiredSearchCompanionPlan } from "../src/search/companion_plan";
import { decodeFtsSegmentCompanionResult, encodeFtsSegmentCompanion, type FtsSectionInput } from "../src/search/fts_format";
import {
  SEARCH_PREFIX_TERM_LIMIT,
  filterDocIdsByFtsClauseResult,
  filterDocIdsByFtsClausesResult,
  planNewestFirstFtsClausesResult,
} from "../src/search/fts_runtime";
const input: FtsSectionInput = {
  doc_count: 3,
  fields: {
    service: {
      kind: "keyword",
      exact: true,
      prefix: true,
      exists_docs: [0, 1, 2],
      terms: {
        "billing-api": { doc_ids: [0, 2] },
        "billing-worker": { doc_ids: [1] },
      },
    },
    message: {
      kind: "text",
      positions: true,
      exists_docs: [0, 1, 2],
      terms: {
        card: { doc_ids: [0, 2], freqs: [1, 1], positions: [0, 0] },
        declined: { doc_ids: [0, 2], freqs: [1, 1], positions: [1, 1] },
        issuer: { doc_ids: [0], freqs: [1], positions: [2] },
        retry: { doc_ids: [1], freqs: [1], positions: [0] },
        later: { doc_ids: [1], freqs: [1], positions: [1] },
        job: { doc_ids: [1], freqs: [1], positions: [2] },
        payment: { doc_ids: [2], freqs: [1], positions: [2] },
        failed: { doc_ids: [2], freqs: [1], positions: [3] },
      },
    },
  },
};

const plan = buildDesiredSearchCompanionPlan({
  apiVersion: "durable.streams/schema-registry/v1",
  schema: "s",
  currentVersion: 1,
  boundaries: [],
  schemas: {},
  lenses: {},
  search: {
    primaryTimestampField: "service",
    fields: {
      service: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/service" }],
        prefix: true,
      },
      message: {
        kind: "text",
        bindings: [{ version: 1, jsonPointer: "/message" }],
        analyzer: "unicode_word_v1",
        positions: true,
      },
    },
  },
});

const companion = (() => {
  const encoded = encodeFtsSegmentCompanion(input, plan);
  const decoded = decodeFtsSegmentCompanionResult(encoded, plan);
  if (Result.isError(decoded)) throw new Error(decoded.error.message);
  return decoded.value;
})();

const largeNewestCompanion = (() => {
  const docIds = Array.from({ length: 400 }, (_, index) => index);
  const encoded = encodeFtsSegmentCompanion(
    {
      doc_count: docIds.length,
      fields: {
        message: {
          kind: "text",
          positions: true,
          exists_docs: docIds,
          terms: {
            timeout: {
              doc_ids: docIds,
              freqs: docIds.map(() => 1),
              positions: docIds.map(() => 0),
            },
          },
        },
      },
    },
    buildDesiredSearchCompanionPlan({
      apiVersion: "durable.streams/schema-registry/v1",
      schema: "s",
      currentVersion: 1,
      boundaries: [],
      schemas: {},
      lenses: {},
      search: {
        primaryTimestampField: "message",
        fields: {
          message: {
            kind: "text",
            bindings: [{ version: 1, jsonPointer: "/message" }],
            analyzer: "unicode_word_v1",
            positions: true,
          },
        },
      },
    })
  );
  const decoded = decodeFtsSegmentCompanionResult(
    encoded,
    buildDesiredSearchCompanionPlan({
      apiVersion: "durable.streams/schema-registry/v1",
      schema: "s",
      currentVersion: 1,
      boundaries: [],
      schemas: {},
      lenses: {},
      search: {
        primaryTimestampField: "message",
        fields: {
          message: {
            kind: "text",
            bindings: [{ version: 1, jsonPointer: "/message" }],
            analyzer: "unicode_word_v1",
            positions: true,
          },
        },
      },
    })
  );
  if (Result.isError(decoded)) throw new Error(decoded.error.message);
  return decoded.value;
})();

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
    const manyTerms: Record<string, { doc_ids: number[] }> = {};
    for (let i = 0; i < SEARCH_PREFIX_TERM_LIMIT + 1; i++) {
      manyTerms[`req-${i}`] = { doc_ids: [0] };
    }
    const overloadedInput: FtsSectionInput = {
      ...input,
      fields: {
        ...input.fields,
        requestId: {
          kind: "keyword",
          exact: true,
          prefix: true,
          exists_docs: [0],
          terms: manyTerms,
        },
      },
    };
    const overloadedPlan = buildDesiredSearchCompanionPlan({
      apiVersion: "durable.streams/schema-registry/v1",
      schema: "s",
      currentVersion: 1,
      boundaries: [],
      schemas: {},
      lenses: {},
      search: {
        primaryTimestampField: "service",
        fields: {
          service: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/service" }],
            prefix: true,
          },
          message: {
            kind: "text",
            bindings: [{ version: 1, jsonPointer: "/message" }],
            analyzer: "unicode_word_v1",
            positions: true,
          },
          requestId: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/requestId" }],
            prefix: true,
          },
        },
      },
    });
    const overloadedRes = decodeFtsSegmentCompanionResult(encodeFtsSegmentCompanion(overloadedInput, overloadedPlan), overloadedPlan);
    expect(Result.isOk(overloadedRes)).toBe(true);
    if (Result.isError(overloadedRes)) return;
    const res = filterDocIdsByFtsClauseResult({
      companion: overloadedRes.value,
      clause: { kind: "keyword", field: "requestId", canonicalValue: "req-", prefix: true },
    });
    expect(Result.isError(res)).toBe(true);
    if (Result.isOk(res)) return;
    expect(res.error.message).toContain("prefix expansion exceeds limit");
  });

  test("bounds newest doc ids for simple newest-first clauses without changing clause semantics", () => {
    const newestKeyword = filterDocIdsByFtsClausesResult({
      companion,
      clauses: [{ kind: "keyword", field: "service", canonicalValue: "billing-", prefix: true }],
      maxNewestDocIds: 2,
    });
    expect(Result.isOk(newestKeyword)).toBe(true);
    if (Result.isError(newestKeyword)) return;
    expect(Array.from(newestKeyword.value).sort((a, b) => a - b)).toEqual([1, 2]);

    const newestText = filterDocIdsByFtsClausesResult({
      companion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["declined"],
          phrase: false,
          prefix: false,
        },
      ],
      maxNewestDocIds: 1,
    });
    expect(Result.isOk(newestText)).toBe(true);
    if (Result.isError(newestText)) return;
    expect(Array.from(newestText.value)).toEqual([2]);

    const newestTextWithCandidateFilter = filterDocIdsByFtsClausesResult({
      companion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["declined"],
          phrase: false,
          prefix: false,
        },
      ],
      candidateDocIds: new Set([0]),
      maxNewestDocIds: 1,
    });
    expect(Result.isOk(newestTextWithCandidateFilter)).toBe(true);
    if (Result.isError(newestTextWithCandidateFilter)) return;
    expect(Array.from(newestTextWithCandidateFilter.value)).toEqual([0]);

    const fallbackPhrase = filterDocIdsByFtsClausesResult({
      companion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1", positions: true }, boost: 1 }],
          tokens: ["card", "declined"],
          phrase: true,
          prefix: false,
        },
      ],
      maxNewestDocIds: 1,
    });
    expect(Result.isOk(fallbackPhrase)).toBe(true);
    if (Result.isError(fallbackPhrase)) return;
    expect(Array.from(fallbackPhrase.value).sort((a, b) => a - b)).toEqual([0, 2]);
  });

  test("extracts newest doc ids from large posting lists without scanning from the front", () => {
    const newestText = filterDocIdsByFtsClausesResult({
      companion: largeNewestCompanion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["timeout"],
          phrase: false,
          prefix: false,
        },
      ],
      maxNewestDocIds: 5,
    });
    expect(Result.isOk(newestText)).toBe(true);
    if (Result.isError(newestText)) return;
    expect(Array.from(newestText.value).sort((a, b) => a - b)).toEqual([395, 396, 397, 398, 399]);

    const candidateNewest = filterDocIdsByFtsClausesResult({
      companion: largeNewestCompanion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["timeout"],
          phrase: false,
          prefix: false,
        },
      ],
      candidateDocIds: new Set([17, 255, 399]),
      maxNewestDocIds: 2,
    });
    expect(Result.isOk(candidateNewest)).toBe(true);
    if (Result.isError(candidateNewest)) return;
    expect(Array.from(candidateNewest.value).sort((a, b) => a - b)).toEqual([255, 399]);
  });

  test("uses segment-only newest-first planning for common simple clauses", () => {
    const commonTermPlan = planNewestFirstFtsClausesResult({
      companion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["declined"],
          phrase: false,
          prefix: false,
        },
      ],
      maxNewestDocIds: 1,
    });
    expect(Result.isOk(commonTermPlan)).toBe(true);
    if (Result.isError(commonTermPlan)) return;
    expect(commonTermPlan.value).toEqual({ mode: "doc_ids", docIds: new Set([2]) });

    const mixedFamilyPlan = planNewestFirstFtsClausesResult({
      companion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["declined"],
          phrase: false,
          prefix: false,
        },
      ],
      candidateDocIds: new Set([0, 1]),
      maxNewestDocIds: 1,
    });
    expect(Result.isOk(mixedFamilyPlan)).toBe(true);
    if (Result.isError(mixedFamilyPlan)) return;
    expect(mixedFamilyPlan.value).toEqual({ mode: "segment_only" });

    const boundedPlan = planNewestFirstFtsClausesResult({
      companion,
      clauses: [
        {
          kind: "text",
          fields: [{ field: "message", config: { kind: "text", bindings: [], analyzer: "unicode_word_v1" }, boost: 1 }],
          tokens: ["declined"],
          phrase: false,
          prefix: false,
        },
      ],
      candidateDocIds: new Set([0]),
      maxNewestDocIds: 1,
    });
    expect(Result.isOk(boundedPlan)).toBe(true);
    if (Result.isError(boundedPlan)) return;
    expect(boundedPlan.value).toEqual({ mode: "doc_ids", docIds: new Set([0]) });
  });
});
