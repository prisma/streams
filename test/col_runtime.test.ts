import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { createBitset, bitsetSet } from "../src/search/bitset";
import { encodeSortableBool, encodeSortableFloat64, encodeSortableInt64 } from "../src/search/column_encoding";
import { filterDocIdsByColumnResult } from "../src/search/col_runtime";
import type { ColSegmentCompanion } from "../src/search/col_format";

function b64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function buildCompanion(): ColSegmentCompanion {
  const existsAll = createBitset(4);
  for (let i = 0; i < 4; i++) bitsetSet(existsAll, i);
  const existsSome = createBitset(4);
  bitsetSet(existsSome, 0);
  bitsetSet(existsSome, 2);
  return {
    version: 1,
    stream: "s",
    segment_index: 0,
    doc_count: 4,
    fields: {
      status: {
        kind: "integer",
        exists_b64: b64(existsAll),
        values_b64: b64(concat([encodeSortableInt64(200n), encodeSortableInt64(404n), encodeSortableInt64(500n), encodeSortableInt64(503n)])),
        min_b64: b64(encodeSortableInt64(200n)),
        max_b64: b64(encodeSortableInt64(503n)),
      },
      duration: {
        kind: "float",
        exists_b64: b64(existsAll),
        values_b64: b64(
          concat([encodeSortableFloat64(1.5), encodeSortableFloat64(4.5), encodeSortableFloat64(9.75), encodeSortableFloat64(12.25)])
        ),
        min_b64: b64(encodeSortableFloat64(1.5)),
        max_b64: b64(encodeSortableFloat64(12.25)),
      },
      ok: {
        kind: "bool",
        exists_b64: b64(existsSome),
        values_b64: b64(concat([encodeSortableBool(true), encodeSortableBool(false)])),
        min_b64: b64(encodeSortableBool(false)),
        max_b64: b64(encodeSortableBool(true)),
      },
    },
  };
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
});
