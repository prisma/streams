import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { buildDesiredSearchCompanionPlan } from "../src/search/companion_plan";
import {
  decodeColSegmentCompanionResult,
  encodeColSegmentCompanion,
  encodeColSegmentCompanionChunks,
  mergeEncodedColSectionChunksResult,
  type ColSectionInput,
} from "../src/search/col_format";
import { filterDocIdsByColumnResult } from "../src/search/col_runtime";

function buildCompanion() {
  const input: ColSectionInput = {
    doc_count: 4,
    fields: {
      status: {
        kind: "integer",
        doc_ids: [0, 1, 2, 3],
        values: [200n, 404n, 500n, 503n],
        min: 200n,
        max: 503n,
      },
      duration: {
        kind: "float",
        doc_ids: [0, 1, 2, 3],
        values: [1.5, 4.5, 9.75, 12.25],
        min: 1.5,
        max: 12.25,
      },
      ok: {
        kind: "bool",
        doc_ids: [0, 2],
        values: [true, false],
        min: false,
        max: true,
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
      primaryTimestampField: "status",
      fields: {
        status: { kind: "integer", bindings: [{ version: 1, jsonPointer: "/status" }], column: true },
        duration: { kind: "float", bindings: [{ version: 1, jsonPointer: "/duration" }], column: true },
        ok: { kind: "bool", bindings: [{ version: 1, jsonPointer: "/ok" }], column: true },
      },
    },
  });
  const encoded = encodeColSegmentCompanion(input, plan);
  const decoded = decodeColSegmentCompanionResult(encoded, plan);
  if (Result.isError(decoded)) throw new Error(decoded.error.message);
  return decoded.value;
}

describe(".col runtime", () => {
  test("supports has, equality, and range filters across typed fields", () => {
    const companion = buildCompanion();

    const statusEq = filterDocIdsByColumnResult({ companion, field: "status", op: "eq", value: 500n });
    expect(Result.isOk(statusEq)).toBe(true);
    if (Result.isError(statusEq)) return;
    expect(Array.from(statusEq.value).sort((a, b) => a - b)).toEqual([2]);

    const statusRange = filterDocIdsByColumnResult({ companion, field: "status", op: "gte", value: 500n });
    expect(Result.isOk(statusRange)).toBe(true);
    if (Result.isError(statusRange)) return;
    expect(Array.from(statusRange.value).sort((a, b) => a - b)).toEqual([2, 3]);

    const durationRange = filterDocIdsByColumnResult({ companion, field: "duration", op: "lt", value: 5 });
    expect(Result.isOk(durationRange)).toBe(true);
    if (Result.isError(durationRange)) return;
    expect(Array.from(durationRange.value).sort((a, b) => a - b)).toEqual([0, 1]);

    const okHas = filterDocIdsByColumnResult({ companion, field: "ok", op: "has" });
    expect(Result.isOk(okHas)).toBe(true);
    if (Result.isError(okHas)) return;
    expect(Array.from(okHas.value).sort((a, b) => a - b)).toEqual([0, 2]);
  });

  test("merges interleaved doc-sharded column payloads without changing values", () => {
    const plan = buildDesiredSearchCompanionPlan({
      apiVersion: "durable.streams/schema-registry/v1",
      schema: "s",
      currentVersion: 1,
      boundaries: [],
      schemas: {},
      lenses: {},
      search: {
        primaryTimestampField: "timestamp",
        fields: {
          timestamp: { kind: "date", bindings: [{ version: 1, jsonPointer: "/timestamp" }], column: true },
        },
      },
    });
    const left: ColSectionInput = {
      doc_count: 4,
      primary_timestamp_field: "timestamp",
      fields: {
        timestamp: {
          kind: "date",
          doc_ids: [0, 2],
          values: [1000n, 3000n],
          min: 1000n,
          max: 3000n,
        },
      },
    };
    const right: ColSectionInput = {
      doc_count: 4,
      primary_timestamp_field: "timestamp",
      fields: {
        timestamp: {
          kind: "date",
          doc_ids: [1, 3],
          values: [2000n, 4000n],
          min: 2000n,
          max: 4000n,
        },
      },
    };

    const merged = mergeEncodedColSectionChunksResult([
      encodeColSegmentCompanionChunks(left, plan).chunks.reduce((acc, chunk) => {
        const next = new Uint8Array(acc.byteLength + chunk.byteLength);
        next.set(acc, 0);
        next.set(chunk, acc.byteLength);
        return next;
      }, new Uint8Array()),
      encodeColSegmentCompanionChunks(right, plan).chunks.reduce((acc, chunk) => {
        const next = new Uint8Array(acc.byteLength + chunk.byteLength);
        next.set(acc, 0);
        next.set(chunk, acc.byteLength);
        return next;
      }, new Uint8Array()),
    ]);
    expect(Result.isOk(merged)).toBe(true);
    if (Result.isError(merged)) return;

    const encoded = merged.value.chunks.reduce((acc, chunk) => {
      const next = new Uint8Array(acc.byteLength + chunk.byteLength);
      next.set(acc, 0);
      next.set(chunk, acc.byteLength);
      return next;
    }, new Uint8Array());
    const decoded = decodeColSegmentCompanionResult(encoded, plan);
    expect(Result.isOk(decoded)).toBe(true);
    if (Result.isError(decoded)) return;

    const field = decoded.value.getField("timestamp");
    expect(field).not.toBeNull();
    if (!field) return;
    expect(field.docIds()).toEqual([0, 1, 2, 3]);
    const values: Array<[number, bigint | number | boolean]> = [];
    field.forEachValue((docId, value) => values.push([docId, value]));
    expect(values).toEqual([
      [0, 1000n],
      [1, 2000n],
      [2, 3000n],
      [3, 4000n],
    ]);
  });
});
