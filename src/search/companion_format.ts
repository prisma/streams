import { Result } from "better-result";
import { decodeAggSegmentCompanionResult, encodeAggSegmentCompanion, type AggSectionInput, type AggSectionView } from "./agg_format";
import { decodeColSegmentCompanionResult, encodeColSegmentCompanion, encodeColSegmentCompanionChunks, type ColSectionInput, type ColSectionView } from "./col_format";
import {
  decodeFtsSegmentCompanionResult,
  encodeFtsSegmentCompanion,
  encodeFtsSegmentCompanionChunks,
  type FtsSectionInput,
  type FtsSectionView,
} from "./fts_format";
import {
  decodeMetricsBlockSegmentCompanionResult,
  encodeMetricsBlockSegmentCompanion,
  type MetricsBlockSectionInput,
  type MetricsBlockSectionView,
} from "../profiles/metrics/block_format";
import type { SearchCompanionPlan } from "./companion_plan";
import { BinaryCursor, BinaryWriter, concatBytes, readU16, readU32, readU64 } from "./binary/codec";
import { streamHash16Hex } from "../util/stream_paths";

const MAGIC = new TextEncoder().encode("PSCIX2");
const MAJOR_VERSION = 2;
const HEADER_BYTES = 58;
const SECTION_ENTRY_BYTES = 28;
export const PSCIX2_MAX_SECTION_COUNT = 4;
export const PSCIX2_MAX_TOC_BYTES = HEADER_BYTES + SECTION_ENTRY_BYTES * PSCIX2_MAX_SECTION_COUNT;

const SECTION_KIND_CODE = {
  col: 1,
  fts: 2,
  agg: 3,
  mblk: 4,
} as const;

const CODE_SECTION_KIND = {
  1: "col",
  2: "fts",
  3: "agg",
  4: "mblk",
} as const;

export type CompanionSectionKind = "col" | "fts" | "agg" | "mblk";

export type CompanionSectionInputMap = {
  col?: ColSectionInput;
  fts?: FtsSectionInput;
  agg?: AggSectionInput;
  mblk?: MetricsBlockSectionInput;
};

export type CompanionSectionMap = {
  col?: ColSectionView;
  fts?: FtsSectionView;
  agg?: AggSectionView;
  mblk?: MetricsBlockSectionView;
};

export type CompanionTocSection = {
  kind: CompanionSectionKind;
  version: number;
  compression: number;
  flags: number;
  offset: number;
  length: number;
  dir_length: number;
  logical_length: number;
};

export type CompanionToc = {
  version: 2;
  plan_generation: number;
  segment_index: number;
  stream_hash16: string;
  sections: CompanionTocSection[];
};

export type BundledSegmentCompanion = {
  toc: CompanionToc;
  sections: CompanionSectionMap;
};

export type EncodedCompanionSectionPayload = {
  kind: CompanionSectionKind;
  version: number;
  compression: number;
  flags: number;
  dirLength: number;
  logicalLength: number;
  payload: Uint8Array;
};

export type EncodedCompanionSectionChunkPayload = {
  kind: CompanionSectionKind;
  version: number;
  compression: number;
  flags: number;
  dirLength: number;
  logicalLength: number;
  sizeBytes: number;
  chunks: Uint8Array[];
};

type CompanionFormatError = { kind: "invalid_companion"; message: string };

function invalidCompanion<T = never>(message: string): Result<T, CompanionFormatError> {
  return Result.err({ kind: "invalid_companion", message });
}

function sectionKindCode(kind: CompanionSectionKind): number {
  return SECTION_KIND_CODE[kind];
}

function encodeSectionPayload(
  kind: CompanionSectionKind,
  section: CompanionSectionInputMap[CompanionSectionKind],
  plan: SearchCompanionPlan
): EncodedCompanionSectionPayload {
  if (kind === "col") {
    const payload = encodeColSegmentCompanion(section as ColSectionInput, plan);
    return { kind, version: 2, compression: 0, flags: 0, dirLength: 8, logicalLength: payload.byteLength, payload };
  }
  if (kind === "fts") {
    const payload = encodeFtsSegmentCompanion(section as FtsSectionInput, plan);
    return { kind, version: 2, compression: 0, flags: 0, dirLength: 8, logicalLength: payload.byteLength, payload };
  }
  if (kind === "agg") {
    const payload = encodeAggSegmentCompanion(section as AggSectionInput, plan);
    return { kind, version: 2, compression: 0, flags: 0, dirLength: 4, logicalLength: payload.byteLength, payload };
  }
  const payload = encodeMetricsBlockSegmentCompanion(section as MetricsBlockSectionInput);
  return { kind, version: 2, compression: 0, flags: 0, dirLength: 20, logicalLength: payload.byteLength, payload };
}

export function encodeCompanionSectionPayload(
  kind: CompanionSectionKind,
  section: CompanionSectionInputMap[CompanionSectionKind],
  plan: SearchCompanionPlan
): EncodedCompanionSectionPayload {
  return encodeSectionPayload(kind, section, plan);
}

export function encodeCompanionSectionChunkPayload(
  kind: CompanionSectionKind,
  section: CompanionSectionInputMap[CompanionSectionKind],
  plan: SearchCompanionPlan
): EncodedCompanionSectionChunkPayload {
  if (kind === "col") {
    const chunkSet = encodeColSegmentCompanionChunks(section as ColSectionInput, plan);
    return { kind, version: 2, compression: 0, flags: 0, dirLength: 8, logicalLength: chunkSet.sizeBytes, sizeBytes: chunkSet.sizeBytes, chunks: chunkSet.chunks };
  }
  if (kind === "fts") {
    const chunkSet = encodeFtsSegmentCompanionChunks(section as FtsSectionInput, plan);
    return { kind, version: 2, compression: 0, flags: 0, dirLength: 8, logicalLength: chunkSet.sizeBytes, sizeBytes: chunkSet.sizeBytes, chunks: chunkSet.chunks };
  }
  if (kind === "agg") {
    const payload = encodeAggSegmentCompanion(section as AggSectionInput, plan);
    return { kind, version: 2, compression: 0, flags: 0, dirLength: 4, logicalLength: payload.byteLength, sizeBytes: payload.byteLength, chunks: [payload] };
  }
  const payload = encodeMetricsBlockSegmentCompanion(section as MetricsBlockSectionInput);
  return { kind, version: 2, compression: 0, flags: 0, dirLength: 20, logicalLength: payload.byteLength, sizeBytes: payload.byteLength, chunks: [payload] };
}

function decodeSectionResult(
  kind: CompanionSectionKind,
  bytes: Uint8Array,
  plan: SearchCompanionPlan
): Result<CompanionSectionMap[CompanionSectionKind], CompanionFormatError> {
  if (kind === "col") {
    const decoded = decodeColSegmentCompanionResult(bytes, plan);
    if (Result.isError(decoded)) return invalidCompanion(decoded.error.message);
    return Result.ok(decoded.value as CompanionSectionMap[CompanionSectionKind]);
  }
  if (kind === "fts") {
    const decoded = decodeFtsSegmentCompanionResult(bytes, plan);
    if (Result.isError(decoded)) return invalidCompanion(decoded.error.message);
    return Result.ok(decoded.value as CompanionSectionMap[CompanionSectionKind]);
  }
  if (kind === "agg") {
    const decoded = decodeAggSegmentCompanionResult(bytes, plan);
    if (Result.isError(decoded)) return invalidCompanion(decoded.error.message);
    return Result.ok(decoded.value as CompanionSectionMap[CompanionSectionKind]);
  }
  const decoded = decodeMetricsBlockSegmentCompanionResult(bytes);
  if (Result.isError(decoded)) return invalidCompanion(decoded.error.message);
  return Result.ok(decoded.value as CompanionSectionMap[CompanionSectionKind]);
}

export function encodeBundledSegmentCompanion(companion: {
  stream: string;
  segment_index: number;
  plan_generation: number;
  plan: SearchCompanionPlan;
  sections: CompanionSectionInputMap;
}): Uint8Array {
  const sectionPayloads: EncodedCompanionSectionPayload[] = [];
  for (const kind of ["col", "fts", "agg", "mblk"] as CompanionSectionKind[]) {
    const section = companion.sections[kind];
    if (!section) continue;
    sectionPayloads.push(encodeSectionPayload(kind, section, companion.plan));
  }
  return encodeBundledSegmentCompanionFromPayloads({
    stream: companion.stream,
    segment_index: companion.segment_index,
    plan_generation: companion.plan_generation,
    sections: sectionPayloads,
  });
}

export function encodeBundledSegmentCompanionFromPayloads(companion: {
  stream: string;
  segment_index: number;
  plan_generation: number;
  sections: EncodedCompanionSectionPayload[];
}): Uint8Array {
  return concatBytes(
    encodeBundledSegmentCompanionChunksFromPayloads({
      stream: companion.stream,
      segment_index: companion.segment_index,
      plan_generation: companion.plan_generation,
      sections: companion.sections.map((section) => ({
        kind: section.kind,
        version: section.version,
        compression: section.compression,
        flags: section.flags,
        dirLength: section.dirLength,
        logicalLength: section.logicalLength,
        sizeBytes: section.payload.byteLength,
        chunks: [section.payload],
      })),
    }).chunks
  );
}

export function encodeBundledSegmentCompanionChunksFromPayloads(companion: {
  stream: string;
  segment_index: number;
  plan_generation: number;
  sections: EncodedCompanionSectionChunkPayload[];
}): { chunks: Uint8Array[]; sizeBytes: number } {
  const writer = new BinaryWriter();
  writer.writeBytes(MAGIC);
  writer.writeU16(MAJOR_VERSION);
  writer.writeU16(0);
  writer.writeU16(companion.sections.length);
  writer.writeU16(0);
  writer.writeU32(companion.plan_generation);
  writer.writeU64(BigInt(companion.segment_index));
  writer.writeBytes(hexToBytes(streamHash16Hex(companion.stream)));
  writer.writeBytes(new Uint8Array(16));

  let payloadOffset = HEADER_BYTES + SECTION_ENTRY_BYTES * companion.sections.length;
  const sectionTable = new BinaryWriter();
  for (const section of companion.sections) {
    sectionTable.writeU8(sectionKindCode(section.kind));
    sectionTable.writeU8(section.version);
    sectionTable.writeU8(section.compression);
    sectionTable.writeU8(section.flags);
    sectionTable.writeU64(BigInt(payloadOffset));
    sectionTable.writeU64(BigInt(section.sizeBytes));
    sectionTable.writeU32(section.dirLength);
    sectionTable.writeU32(section.logicalLength);
    payloadOffset += section.sizeBytes;
  }
  const chunks = [writer.finish(), sectionTable.finish(), ...companion.sections.flatMap((section) => section.chunks)];
  return { chunks, sizeBytes: payloadOffset };
}

export function decodeBundledSegmentCompanionTocResult(bytes: Uint8Array): Result<CompanionToc, CompanionFormatError> {
  if (bytes.byteLength < HEADER_BYTES) return invalidCompanion("invalid PSCIX2 header");
  for (let index = 0; index < MAGIC.byteLength; index++) {
    if (bytes[index] !== MAGIC[index]) return invalidCompanion("invalid PSCIX2 magic");
  }
  const majorVersion = readU16(bytes, MAGIC.byteLength);
  if (majorVersion !== MAJOR_VERSION) return invalidCompanion("unsupported PSCIX2 version");
  const sectionCount = readU16(bytes, MAGIC.byteLength + 4);
  if (sectionCount < 0 || sectionCount > PSCIX2_MAX_SECTION_COUNT) {
    return invalidCompanion("invalid PSCIX2 section count");
  }
  if (bytes.byteLength < HEADER_BYTES + SECTION_ENTRY_BYTES * sectionCount) {
    return invalidCompanion("truncated PSCIX2 section table");
  }
  const planGeneration = readU32(bytes, MAGIC.byteLength + 8);
  const segmentIndex = Number(readU64(bytes, MAGIC.byteLength + 12));
  const streamHash16 = bytesToHex(bytes.subarray(MAGIC.byteLength + 20, MAGIC.byteLength + 36));
  const sections: CompanionTocSection[] = [];
  let offset = HEADER_BYTES;
  for (let index = 0; index < sectionCount; index++) {
    const kindCode = bytes[offset]!;
    const kind = CODE_SECTION_KIND[kindCode as keyof typeof CODE_SECTION_KIND];
    if (!kind) return invalidCompanion("invalid PSCIX2 section kind");
    sections.push({
      kind,
      version: bytes[offset + 1]!,
      compression: bytes[offset + 2]!,
      flags: bytes[offset + 3]!,
      offset: Number(readU64(bytes, offset + 4)),
      length: Number(readU64(bytes, offset + 12)),
      dir_length: readU32(bytes, offset + 20),
      logical_length: readU32(bytes, offset + 24),
    });
    offset += SECTION_ENTRY_BYTES;
  }
  return Result.ok({
    version: 2,
    plan_generation: planGeneration,
    segment_index: segmentIndex,
    stream_hash16: streamHash16,
    sections,
  });
}

export function decodeBundledSegmentCompanionSectionResult<K extends CompanionSectionKind>(
  bytes: Uint8Array,
  kind: K,
  plan: SearchCompanionPlan
): Result<CompanionSectionMap[K] | null, CompanionFormatError> {
  const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
  if (Result.isError(tocRes)) return tocRes;
  return decodeBundledSegmentCompanionSectionFromTocResult(bytes, tocRes.value, kind, plan);
}

export function decodeBundledSegmentCompanionSectionFromTocResult<K extends CompanionSectionKind>(
  bytes: Uint8Array,
  toc: CompanionToc,
  kind: K,
  plan: SearchCompanionPlan
): Result<CompanionSectionMap[K] | null, CompanionFormatError> {
  const section = toc.sections.find((entry) => entry.kind === kind);
  if (!section) return Result.ok(null);
  const sectionEnd = section.offset + section.length;
  if (section.offset < 0 || section.length < 0 || sectionEnd > bytes.byteLength) {
    return invalidCompanion("invalid PSCIX2 section bounds");
  }
  const decodedRes = decodeSectionResult(kind, bytes.subarray(section.offset, sectionEnd), plan);
  if (Result.isError(decodedRes)) return decodedRes;
  return Result.ok(decodedRes.value as CompanionSectionMap[K]);
}

export function decodeCompanionSectionPayloadResult<K extends CompanionSectionKind>(
  kind: K,
  bytes: Uint8Array,
  plan: SearchCompanionPlan
): Result<NonNullable<CompanionSectionMap[K]>, CompanionFormatError> {
  const decodedRes = decodeSectionResult(kind, bytes, plan);
  if (Result.isError(decodedRes)) return decodedRes as Result<NonNullable<CompanionSectionMap[K]>, CompanionFormatError>;
  if (!decodedRes.value) return invalidCompanion(`missing PSCIX2 ${kind} payload`);
  return Result.ok(decodedRes.value as NonNullable<CompanionSectionMap[K]>);
}

export function decodeBundledSegmentCompanionResult(
  bytes: Uint8Array,
  plan: SearchCompanionPlan
): Result<BundledSegmentCompanion, CompanionFormatError> {
  const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
  if (Result.isError(tocRes)) return tocRes;
  const sections: CompanionSectionMap = {};
  for (const entry of tocRes.value.sections) {
    const decodedRes = decodeBundledSegmentCompanionSectionFromTocResult(bytes, tocRes.value, entry.kind, plan);
    if (Result.isError(decodedRes)) return decodedRes;
    if (decodedRes.value) (sections as Record<string, unknown>)[entry.kind] = decodedRes.value;
  }
  return Result.ok({ toc: tocRes.value, sections });
}

function hexToBytes(value: string): Uint8Array {
  const out = new Uint8Array(value.length / 2);
  for (let index = 0; index < value.length; index += 2) {
    out[index / 2] = Number.parseInt(value.slice(index, index + 2), 16);
  }
  return out;
}

function bytesToHex(value: Uint8Array): string {
  return Array.from(value)
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}
