import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { Result } from "better-result";
import type { SearchFieldKind } from "../schema/registry";

export type FtsPosting = {
  d: number;
  p?: number[];
};

export type FtsFieldCompanion = {
  kind: SearchFieldKind;
  exact?: boolean;
  prefix?: boolean;
  positions?: boolean;
  exists_docs: number[];
  doc_lengths?: number[];
  terms: Record<string, FtsPosting[]>;
};

export type FtsSegmentCompanion = {
  version: 1;
  stream: string;
  segment_index: number;
  doc_count: number;
  fields: Record<string, FtsFieldCompanion>;
};

export type FtsFormatError = { kind: "invalid_fts_segment"; message: string };

function invalidFts<T = never>(message: string): Result<T, FtsFormatError> {
  return Result.err({ kind: "invalid_fts_segment", message });
}

export function encodeFtsSegmentCompanion(companion: FtsSegmentCompanion): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(companion));
  return new Uint8Array(zstdCompressSync(body));
}

export function decodeFtsSegmentCompanionResult(bytes: Uint8Array): Result<FtsSegmentCompanion, FtsFormatError> {
  try {
    const raw = new TextDecoder().decode(zstdDecompressSync(bytes));
    const parsed = JSON.parse(raw) as FtsSegmentCompanion;
    if (!parsed || parsed.version !== 1 || typeof parsed.stream !== "string") {
      return invalidFts("invalid .fts companion");
    }
    if (typeof parsed.segment_index !== "number" || typeof parsed.doc_count !== "number" || !parsed.fields || typeof parsed.fields !== "object") {
      return invalidFts("invalid .fts companion");
    }
    return Result.ok(parsed);
  } catch (e: unknown) {
    return invalidFts(String((e as any)?.message ?? e));
  }
}
