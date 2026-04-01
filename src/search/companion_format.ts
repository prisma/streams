import { Result } from "better-result";
import {
  decodeAggSegmentCompanionResult,
  encodeAggSegmentCompanion,
  type AggSegmentCompanion,
} from "./agg_format";
import {
  decodeColSegmentCompanionResult,
  encodeColSegmentCompanion,
  type ColSegmentCompanion,
} from "./col_format";
import {
  decodeFtsSegmentCompanionResult,
  encodeFtsSegmentCompanion,
  type FtsSegmentCompanion,
} from "./fts_format";
import {
  decodeMetricsBlockSegmentCompanionResult,
  encodeMetricsBlockSegmentCompanion,
  type MetricsBlockSegmentCompanion,
} from "../profiles/metrics/block_format";

const MAGIC = new TextEncoder().encode("PSCIX1");

export type CompanionSectionKind = "col" | "fts" | "agg" | "mblk";

export type CompanionSectionMap = {
  col?: ColSegmentCompanion;
  fts?: FtsSegmentCompanion;
  agg?: AggSegmentCompanion;
  mblk?: MetricsBlockSegmentCompanion;
};

export type CompanionTocSection = {
  kind: CompanionSectionKind;
  offset: number;
  length: number;
};

export type CompanionToc = {
  version: 1;
  stream: string;
  segment_index: number;
  plan_generation: number;
  sections: CompanionTocSection[];
};

export type BundledSegmentCompanion = {
  toc: CompanionToc;
  sections: CompanionSectionMap;
};

export type EncodedCompanionSectionPayload = {
  kind: CompanionSectionKind;
  payload: Uint8Array;
};

type CompanionFormatError = { kind: "invalid_companion"; message: string };

function invalidCompanion<T = never>(message: string): Result<T, CompanionFormatError> {
  return Result.err({ kind: "invalid_companion", message });
}

function readU32(value: Uint8Array, offset: number): number {
  return new DataView(value.buffer, value.byteOffset + offset, 4).getUint32(0, false);
}

function writeU32(value: number): Uint8Array {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, value, false);
  return out;
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

function encodeSection(kind: CompanionSectionKind, section: CompanionSectionMap[CompanionSectionKind]): Uint8Array {
  if (kind === "col") return encodeColSegmentCompanion(section as ColSegmentCompanion);
  if (kind === "fts") return encodeFtsSegmentCompanion(section as FtsSegmentCompanion);
  if (kind === "agg") return encodeAggSegmentCompanion(section as AggSegmentCompanion);
  return encodeMetricsBlockSegmentCompanion(section as MetricsBlockSegmentCompanion);
}

export function encodeCompanionSectionPayload(
  kind: CompanionSectionKind,
  section: CompanionSectionMap[CompanionSectionKind]
): Uint8Array {
  return encodeSection(kind, section);
}

function decodeSectionResult(
  kind: CompanionSectionKind,
  bytes: Uint8Array
): Result<CompanionSectionMap[CompanionSectionKind], CompanionFormatError> {
  if (kind === "col") {
    const decoded = decodeColSegmentCompanionResult(bytes);
    if (Result.isError(decoded)) return invalidCompanion(decoded.error.message);
    return Result.ok(decoded.value as CompanionSectionMap[CompanionSectionKind]);
  }
  if (kind === "fts") {
    const decoded = decodeFtsSegmentCompanionResult(bytes);
    if (Result.isError(decoded)) return invalidCompanion(decoded.error.message);
    return Result.ok(decoded.value as CompanionSectionMap[CompanionSectionKind]);
  }
  if (kind === "agg") {
    const decoded = decodeAggSegmentCompanionResult(bytes);
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
  sections: CompanionSectionMap;
}): Uint8Array {
  const sectionPayloads: EncodedCompanionSectionPayload[] = [];
  for (const kind of ["col", "fts", "agg", "mblk"] as CompanionSectionKind[]) {
    const section = companion.sections[kind];
    if (!section) continue;
    sectionPayloads.push({ kind, payload: encodeCompanionSectionPayload(kind, section) });
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
  const sectionPayloads = companion.sections;
  const headerLen = MAGIC.byteLength + 4;
  let baseOffset = headerLen;
  let tocBytes = new Uint8Array();
  let sections: CompanionTocSection[] = [];
  for (;;) {
    let payloadOffset = headerLen + tocBytes.byteLength;
    sections = sectionPayloads.map(({ kind, payload }) => {
      const entry = { kind, offset: payloadOffset, length: payload.byteLength };
      payloadOffset += payload.byteLength;
      return entry;
    });
    const toc: CompanionToc = {
      version: 1,
      stream: companion.stream,
      segment_index: companion.segment_index,
      plan_generation: companion.plan_generation,
      sections,
    };
    const nextTocBytes = new TextEncoder().encode(JSON.stringify(toc));
    if (nextTocBytes.byteLength === tocBytes.byteLength && baseOffset === headerLen) {
      tocBytes = nextTocBytes;
      break;
    }
    tocBytes = nextTocBytes;
    baseOffset = headerLen;
  }

  return concat([MAGIC, writeU32(tocBytes.byteLength), tocBytes, ...sectionPayloads.map((entry) => entry.payload)]);
}

export function decodeBundledSegmentCompanionTocResult(bytes: Uint8Array): Result<CompanionToc, CompanionFormatError> {
  if (bytes.byteLength < MAGIC.byteLength + 4) return invalidCompanion("invalid .cix header");
  const magic = bytes.subarray(0, MAGIC.byteLength);
  for (let i = 0; i < MAGIC.byteLength; i++) {
    if (magic[i] !== MAGIC[i]) return invalidCompanion("invalid .cix magic");
  }
  const tocLength = readU32(bytes, MAGIC.byteLength);
  const tocStart = MAGIC.byteLength + 4;
  const tocEnd = tocStart + tocLength;
  if (tocEnd > bytes.byteLength) return invalidCompanion("invalid .cix toc length");
  let parsed: any;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes.subarray(tocStart, tocEnd)));
  } catch {
    return invalidCompanion("invalid .cix toc");
  }
  if (parsed?.version !== 1 || typeof parsed.stream !== "string" || typeof parsed.segment_index !== "number") {
    return invalidCompanion("invalid .cix toc");
  }
  if (typeof parsed.plan_generation !== "number" || !Array.isArray(parsed.sections)) {
    return invalidCompanion("invalid .cix toc");
  }
  const sections: CompanionTocSection[] = [];
  for (const section of parsed.sections) {
    if (
      !section ||
      (section.kind !== "col" && section.kind !== "fts" && section.kind !== "agg" && section.kind !== "mblk") ||
      typeof section.offset !== "number" ||
      typeof section.length !== "number"
    ) {
      return invalidCompanion("invalid .cix toc section");
    }
    sections.push({
      kind: section.kind,
      offset: section.offset,
      length: section.length,
    });
  }
  return Result.ok({
    version: 1,
    stream: parsed.stream,
    segment_index: parsed.segment_index,
    plan_generation: parsed.plan_generation,
    sections,
  });
}

export function decodeBundledSegmentCompanionSectionResult<K extends CompanionSectionKind>(
  bytes: Uint8Array,
  kind: K
): Result<CompanionSectionMap[K] | null, CompanionFormatError> {
  const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
  if (Result.isError(tocRes)) return tocRes;
  return decodeBundledSegmentCompanionSectionFromTocResult(bytes, tocRes.value, kind);
}

export function decodeBundledSegmentCompanionSectionFromTocResult<K extends CompanionSectionKind>(
  bytes: Uint8Array,
  toc: CompanionToc,
  kind: K
): Result<CompanionSectionMap[K] | null, CompanionFormatError> {
  const section = toc.sections.find((entry) => entry.kind === kind);
  if (!section) return Result.ok(null);
  const sectionEnd = section.offset + section.length;
  if (section.offset < 0 || section.length < 0 || sectionEnd > bytes.byteLength) {
    return invalidCompanion("invalid .cix section bounds");
  }
  const decodedRes = decodeSectionResult(kind, bytes.subarray(section.offset, sectionEnd));
  if (Result.isError(decodedRes)) return decodedRes;
  return Result.ok(decodedRes.value as CompanionSectionMap[K]);
}

export function decodeBundledSegmentCompanionResult(bytes: Uint8Array): Result<BundledSegmentCompanion, CompanionFormatError> {
  const tocRes = decodeBundledSegmentCompanionTocResult(bytes);
  if (Result.isError(tocRes)) return tocRes;
  const sections: CompanionSectionMap = {};
  for (const entry of tocRes.value.sections) {
    const decodedRes = decodeBundledSegmentCompanionSectionFromTocResult(bytes, tocRes.value, entry.kind);
    if (Result.isError(decodedRes)) return decodedRes;
    if (decodedRes.value) (sections as Record<string, unknown>)[entry.kind] = decodedRes.value;
  }
  return Result.ok({ toc: tocRes.value, sections });
}
