import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { Result } from "better-result";
import type { SearchFieldKind } from "../schema/registry";

export type ColFieldData = {
  kind: SearchFieldKind;
  exists_b64: string;
  values_b64: string;
  min_b64?: string;
  max_b64?: string;
};

export type ColSegmentCompanion = {
  version: 1;
  stream: string;
  segment_index: number;
  doc_count: number;
  fields: Record<string, ColFieldData>;
  primary_timestamp_field?: string;
  min_timestamp_ms?: string;
  max_timestamp_ms?: string;
};

export type ColFormatError = { kind: "invalid_col_segment"; message: string };

function invalidCol<T = never>(message: string): Result<T, ColFormatError> {
  return Result.err({ kind: "invalid_col_segment", message });
}

export function encodeColSegmentCompanion(companion: ColSegmentCompanion): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(companion));
  return new Uint8Array(zstdCompressSync(body));
}

export function decodeColSegmentCompanionResult(bytes: Uint8Array): Result<ColSegmentCompanion, ColFormatError> {
  try {
    const raw = new TextDecoder().decode(zstdDecompressSync(bytes));
    const parsed = JSON.parse(raw) as ColSegmentCompanion;
    if (!parsed || parsed.version !== 1 || typeof parsed.stream !== "string") {
      return invalidCol("invalid .col companion");
    }
    if (typeof parsed.segment_index !== "number" || typeof parsed.doc_count !== "number" || !parsed.fields || typeof parsed.fields !== "object") {
      return invalidCol("invalid .col companion");
    }
    return Result.ok(parsed);
  } catch (e: unknown) {
    return invalidCol(String((e as any)?.message ?? e));
  }
}
