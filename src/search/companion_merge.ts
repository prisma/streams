import { readFileSync, rmSync } from "node:fs";
import { Result } from "better-result";
import { mergeEncodedColSectionChunksResult } from "./col_format";
import {
  decodeBundledSegmentCompanionTocResult,
  encodeBundledSegmentCompanionChunksFromPayloads,
  type EncodedCompanionSectionChunkPayload,
  type CompanionSectionKind,
} from "./companion_format";
import { mergeEncodedFtsSectionChunksResult } from "./fts_format";
import { writeCompanionOutputChunksResult } from "./companion_output_file";

export type PartialCompanionOutput = {
  targetName: string;
  localPath: string;
  sizeBytes: number;
  sectionKinds: CompanionSectionKind[];
  sectionSizes?: Record<string, number>;
  primaryTimestampMinMs: bigint | null;
  primaryTimestampMaxMs: bigint | null;
};

export type CompanionMergeBuildInput = {
  stream: string;
  segmentIndex: number;
  planGeneration: number;
  outputDir: string;
  filePrefix: string;
  partials: PartialCompanionOutput[];
};

export type CompanionMergeBuildOutput = {
  storage: "file";
  localPath: string;
  sizeBytes: number;
  sectionKinds: CompanionSectionKind[];
  sectionSizes: Record<string, number>;
  primaryTimestampMinMs: bigint | null;
  primaryTimestampMaxMs: bigint | null;
};

type CompanionMergeError = { kind: "invalid_companion_merge"; message: string };

function invalidCompanionMerge<T = never>(message: string): Result<T, CompanionMergeError> {
  return Result.err({ kind: "invalid_companion_merge", message });
}

type EncodedSection = EncodedCompanionSectionChunkPayload;

function minTimestamp(left: bigint | null, right: bigint | null): bigint | null {
  if (left == null) return right;
  if (right == null) return left;
  return left < right ? left : right;
}

function maxTimestamp(left: bigint | null, right: bigint | null): bigint | null {
  if (left == null) return right;
  if (right == null) return left;
  return left > right ? left : right;
}

function loadSectionPayloadsResult(
  partials: PartialCompanionOutput[],
  kind: CompanionSectionKind
): Result<Uint8Array[], CompanionMergeError> {
  const sectionPayloads: Uint8Array[] = [];
  for (const partial of partials) {
    if (!partial.sectionKinds.includes(kind)) continue;
    const bytes = readFileSync(partial.localPath);
    const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
    if (Result.isError(tocRes)) return invalidCompanionMerge(tocRes.error.message);
    for (const section of tocRes.value.sections) {
      if (section.kind !== kind) continue;
      sectionPayloads.push(bytes.subarray(section.offset, section.offset + section.length));
    }
  }
  return Result.ok(sectionPayloads);
}

export function mergePartialCompanionFilesResult(input: {
  stream: string;
  segmentIndex: number;
  planGeneration: number;
  outputDir: string;
  filePrefix: string;
  partials: PartialCompanionOutput[];
}): Result<
  {
    localPath: string;
    sizeBytes: number;
    sectionKinds: CompanionSectionKind[];
    sectionSizes: Record<string, number>;
    primaryTimestampMinMs: bigint | null;
    primaryTimestampMaxMs: bigint | null;
  },
  CompanionMergeError
> {
  let primaryTimestampMinMs: bigint | null = null;
  let primaryTimestampMaxMs: bigint | null = null;

  try {
    for (const partial of input.partials) {
      primaryTimestampMinMs = minTimestamp(primaryTimestampMinMs, partial.primaryTimestampMinMs);
      primaryTimestampMaxMs = maxTimestamp(primaryTimestampMaxMs, partial.primaryTimestampMaxMs);
    }

    const sections: EncodedSection[] = [];
    const sectionSizes: Record<string, number> = {};
    const addChunkSection = (
      kind: CompanionSectionKind,
      chunks: Uint8Array[],
      sizeBytes: number,
      logicalLength: number,
      dirLength: number
    ): void => {
      sections.push({
        kind,
        version: 2,
        compression: 0,
        flags: 0,
        dirLength,
        logicalLength,
        sizeBytes,
        chunks,
      });
      sectionSizes[kind] = sizeBytes;
    };

    const colSectionsRes = loadSectionPayloadsResult(input.partials, "col");
    if (Result.isError(colSectionsRes)) return colSectionsRes;
    if (colSectionsRes.value.length > 0) {
      const mergedColRes = mergeEncodedColSectionChunksResult(colSectionsRes.value);
      if (Result.isError(mergedColRes)) return invalidCompanionMerge(mergedColRes.error.message);
      addChunkSection("col", mergedColRes.value.chunks, mergedColRes.value.sizeBytes, mergedColRes.value.sizeBytes, 8);
    }

    const ftsSectionsRes = loadSectionPayloadsResult(input.partials, "fts");
    if (Result.isError(ftsSectionsRes)) return ftsSectionsRes;
    if (ftsSectionsRes.value.length > 0) {
      const mergedFtsRes = mergeEncodedFtsSectionChunksResult(ftsSectionsRes.value);
      if (Result.isError(mergedFtsRes)) return invalidCompanionMerge(mergedFtsRes.error.message);
      addChunkSection("fts", mergedFtsRes.value.chunks, mergedFtsRes.value.sizeBytes, mergedFtsRes.value.sizeBytes, 8);
    }

    const chunkSet = encodeBundledSegmentCompanionChunksFromPayloads({
      stream: input.stream,
      segment_index: input.segmentIndex,
      plan_generation: input.planGeneration,
      sections,
    });
    const writeRes = writeCompanionOutputChunksResult(input.outputDir, input.filePrefix, chunkSet.chunks, chunkSet.sizeBytes);
    if (Result.isError(writeRes)) return invalidCompanionMerge(writeRes.error.message);
    return Result.ok({
      localPath: writeRes.value.localPath,
      sizeBytes: writeRes.value.sizeBytes,
      sectionKinds: sections.map((section) => section.kind),
      sectionSizes,
      primaryTimestampMinMs,
      primaryTimestampMaxMs,
    });
  } finally {
    for (const partial of input.partials) {
      try {
        rmSync(partial.localPath, { force: true });
      } catch {
        // ignore cleanup failures
      }
    }
  }
}

export function buildCompanionMergeResult(
  input: CompanionMergeBuildInput
): Result<CompanionMergeBuildOutput, CompanionMergeError> {
  const mergedRes = mergePartialCompanionFilesResult(input);
  if (Result.isError(mergedRes)) return mergedRes;
  return Result.ok({
    storage: "file",
    localPath: mergedRes.value.localPath,
    sizeBytes: mergedRes.value.sizeBytes,
    sectionKinds: mergedRes.value.sectionKinds,
    sectionSizes: mergedRes.value.sectionSizes,
    primaryTimestampMinMs: mergedRes.value.primaryTimestampMinMs,
    primaryTimestampMaxMs: mergedRes.value.primaryTimestampMaxMs,
  });
}
