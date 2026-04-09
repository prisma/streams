import { Result } from "better-result";
import type { SchemaRegistry } from "../schema/registry";
import { iterateBlockRecordsFromFileResult, visitBlockRecordPayloadsFromFileResult } from "../segment/format";
import {
  canonicalizeExactValue,
  compileSearchFieldAccessorsResult,
  extractFastScalarJsonValueFromBytesResult,
  supportsFastScalarJsonExtraction,
  visitCompiledAccessorRawValuesResult,
  visitRawSearchValuesWithCompiledAccessorsResult,
  type CompiledSearchFieldAccessor,
} from "../search/schema";
import { schemaVersionForOffset } from "../schema/read_json";
import { siphash24 } from "../util/siphash";
import { secondaryIndexRunObjectKey, streamHash16Hex } from "../util/stream_paths";
import { encodeIndexRunResult, RUN_TYPE_MASK16 } from "./run_format";
import type { SecondaryIndexField } from "./secondary_schema";
import { writeIndexRunOutputFileResult } from "./index_run_output_file";

const PAYLOAD_DECODER = new TextDecoder();
const TERM_ENCODER = new TextEncoder();
const HIGH_CARDINALITY_SINGLE_SEGMENT_FIELDS = new Set(["requestId", "traceId", "spanId", "path"]);

function sortFingerprints(iterable: Iterable<bigint>): bigint[] {
  const values = Array.from(iterable);
  if (values.length <= 1) return values;
  const typed = new BigUint64Array(values.length);
  for (let index = 0; index < values.length; index += 1) typed[index] = values[index]!;
  typed.sort();
  return Array.from(typed);
}

export type SecondaryL0BuildInput = {
  stream: string;
  registry: SchemaRegistry;
  startSegment: number;
  span: number;
  outputDir?: string;
  indexes: Array<{
    index: SecondaryIndexField;
    secret: Uint8Array;
  }>;
  segments: Array<{ segmentIndex: number; startOffset: bigint; localPath: string }>;
};

export type SecondaryL0BuildOutput = {
  runs: Array<
    {
      indexName: string;
      meta: {
        runId: string;
        level: number;
        startSegment: number;
        endSegment: number;
        objectKey: string;
        filterLen: number;
        recordCount: number;
      };
    } & (
      | {
          storage: "bytes";
          payload: Uint8Array;
        }
      | {
          storage: "file";
          localPath: string;
          sizeBytes: number;
        }
    )
  >;
};

type BuildError = {
  kind: "invalid_index_build";
  message: string;
};

type BatchState = {
  index: SecondaryIndexField;
  secret: Uint8Array;
  maskByFp: Map<bigint, number> | null;
  fpByCanonical: Map<string, bigint> | null;
  fingerprints: Set<bigint> | null;
  fingerprintBuffer: BigUint64Array | null;
  fingerprintCount: number;
  termScratch: Uint8Array;
  lowercaseScratch: Uint8Array;
};

type JsonScalarToken =
  | { kind: "string"; start: number; end: number; hadEscape: boolean }
  | { kind: "number"; start: number; end: number }
  | { kind: "true" }
  | { kind: "false" }
  | { kind: "null" };

type SingleSegmentFastPlan = {
  asciiEntries: Array<{
    key: string;
    states: BatchState[];
  }>;
  decodedStatesByKey: Map<string, BatchState[]>;
};

function skipJsonWhitespaceBytes(bytes: Uint8Array, index: number, limit = bytes.length): number {
  let cursor = index;
  while (cursor < limit) {
    const code = bytes[cursor]!;
    if (code === 32 || code === 10 || code === 13 || code === 9) {
      cursor += 1;
      continue;
    }
    break;
  }
  return cursor;
}

function scanJsonStringEndBytes(
  bytes: Uint8Array,
  start: number,
  limit = bytes.length
): Result<{ end: number; hadEscape: boolean }, BuildError> {
  let index = start + 1;
  let hadEscape = false;
  while (index < limit) {
    const code = bytes[index]!;
    if (code === 34) return Result.ok({ end: index, hadEscape });
    if (code === 92) {
      hadEscape = true;
      index += 1;
      if (index >= limit) return invalidIndexBuild("unterminated JSON escape");
      if (bytes[index] === 117) {
        index += 4;
        if (index >= limit) return invalidIndexBuild("unterminated unicode escape");
      }
    }
    index += 1;
  }
  return invalidIndexBuild("unterminated JSON string");
}

function bytesEqualAsciiString(bytes: Uint8Array, start: number, end: number, expected: string): boolean {
  if (end - start !== expected.length) return false;
  for (let index = 0; index < expected.length; index += 1) {
    if (bytes[start + index] !== expected.charCodeAt(index)) return false;
  }
  return true;
}

function parseJsonScalarTokenBytes(
  bytes: Uint8Array,
  start: number,
  limit = bytes.length
): Result<{ token: JsonScalarToken; next: number }, BuildError> {
  const code = bytes[start];
  if (code === 34) {
    const endRes = scanJsonStringEndBytes(bytes, start, limit);
    if (Result.isError(endRes)) return endRes;
    return Result.ok({
      token: {
        kind: "string",
        start,
        end: endRes.value.end,
        hadEscape: endRes.value.hadEscape,
      },
      next: endRes.value.end + 1,
    });
  }
  if (code === 116 && bytes[start + 1] === 114 && bytes[start + 2] === 117 && bytes[start + 3] === 101) {
    return Result.ok({ token: { kind: "true" }, next: start + 4 });
  }
  if (
    code === 102 &&
    bytes[start + 1] === 97 &&
    bytes[start + 2] === 108 &&
    bytes[start + 3] === 115 &&
    bytes[start + 4] === 101
  ) {
    return Result.ok({ token: { kind: "false" }, next: start + 5 });
  }
  if (code === 110 && bytes[start + 1] === 117 && bytes[start + 2] === 108 && bytes[start + 3] === 108) {
    return Result.ok({ token: { kind: "null" }, next: start + 4 });
  }
  if (code === 45 || (code != null && code >= 48 && code <= 57)) {
    let end = start + 1;
    while (end < limit) {
      const next = bytes[end]!;
      const isNumberChar =
        (next >= 48 && next <= 57) || next === 45 || next === 43 || next === 46 || next === 69 || next === 101;
      if (!isNumberChar) break;
      end += 1;
    }
    return Result.ok({ token: { kind: "number", start, end }, next: end });
  }
  return invalidIndexBuild("unsupported JSON scalar token");
}

function skipJsonValueBytes(bytes: Uint8Array, start: number, limit = bytes.length): Result<number, BuildError> {
  const code = bytes[start];
  if (code === 34) {
    const endRes = scanJsonStringEndBytes(bytes, start, limit);
    if (Result.isError(endRes)) return endRes;
    return Result.ok(endRes.value.end + 1);
  }
  if (code === 123 || code === 91) {
    const stack = [code];
    let index = start + 1;
    while (index < limit) {
      const current = bytes[index]!;
      if (current === 34) {
        const endRes = scanJsonStringEndBytes(bytes, index, limit);
        if (Result.isError(endRes)) return endRes;
        index = endRes.value.end + 1;
        continue;
      }
      if (current === 123 || current === 91) {
        stack.push(current);
      } else if (current === 125 || current === 93) {
        const open = stack.pop();
        if ((current === 125 && open !== 123) || (current === 93 && open !== 91)) {
          return invalidIndexBuild("mismatched JSON container");
        }
        if (stack.length === 0) return Result.ok(index + 1);
      }
      index += 1;
    }
    return invalidIndexBuild("unterminated JSON container");
  }
  const scalarRes = parseJsonScalarTokenBytes(bytes, start, limit);
  if (Result.isError(scalarRes)) return scalarRes;
  return Result.ok(scalarRes.value.next);
}

function decodeJsonStringTokenBytes(bytes: Uint8Array, token: Extract<JsonScalarToken, { kind: "string" }>): Result<string, BuildError> {
  if (!token.hadEscape) return Result.ok(PAYLOAD_DECODER.decode(bytes.subarray(token.start + 1, token.end)));
  try {
    return Result.ok(JSON.parse(PAYLOAD_DECODER.decode(bytes.subarray(token.start, token.end + 1))));
  } catch {
    return invalidIndexBuild("invalid JSON string token");
  }
}

function ensureScratchCapacity(buffer: Uint8Array, minLen: number): Uint8Array {
  if (buffer.byteLength >= minLen) return buffer;
  let nextLen = buffer.byteLength > 0 ? buffer.byteLength : 64;
  while (nextLen < minLen) nextLen *= 2;
  return new Uint8Array(nextLen);
}

function shouldUseBufferedSingleSegmentFingerprints(indexName: string): boolean {
  return HIGH_CARDINALITY_SINGLE_SEGMENT_FIELDS.has(indexName);
}

function addSingleSegmentFingerprint(state: BatchState, fingerprint: bigint): void {
  if (state.fingerprints) {
    state.fingerprints.add(fingerprint);
    return;
  }
  if (!state.fingerprintBuffer) {
    state.fingerprintBuffer = new BigUint64Array(256);
  } else if (state.fingerprintCount >= state.fingerprintBuffer.length) {
    const next = new BigUint64Array(state.fingerprintBuffer.length * 2);
    next.set(state.fingerprintBuffer);
    state.fingerprintBuffer = next;
  }
  state.fingerprintBuffer[state.fingerprintCount] = fingerprint;
  state.fingerprintCount += 1;
}

function finalizeSingleSegmentFingerprints(state: BatchState): bigint[] {
  if (state.fingerprints) return sortFingerprints(state.fingerprints);
  const source = state.fingerprintBuffer?.subarray(0, state.fingerprintCount);
  if (!source || source.length === 0) return [];
  const ordered = new BigUint64Array(source);
  ordered.sort();
  const out: bigint[] = [];
  let last: bigint | null = null;
  for (const fingerprint of ordered) {
    if (last != null && fingerprint === last) continue;
    out.push(fingerprint);
    last = fingerprint;
  }
  return out;
}

function hashCanonicalString(state: BatchState, canonical: string | null): void {
  if (canonical == null) return;
  state.termScratch = ensureScratchCapacity(state.termScratch, Math.max(4, canonical.length * 4));
  const encoded = TERM_ENCODER.encodeInto(canonical, state.termScratch);
  if (encoded.read !== canonical.length) return;
  addSingleSegmentFingerprint(state, siphash24(state.secret, state.termScratch.subarray(0, encoded.written)));
}

function hashAsciiLowercaseBytes(state: BatchState, bytes: Uint8Array): void {
  state.lowercaseScratch = ensureScratchCapacity(state.lowercaseScratch, Math.max(4, bytes.byteLength));
  const lowered = state.lowercaseScratch.subarray(0, bytes.byteLength);
  for (let index = 0; index < bytes.byteLength; index += 1) {
    const code = bytes[index]!;
    lowered[index] = code >= 65 && code <= 90 ? code + 32 : code;
  }
  addSingleSegmentFingerprint(state, siphash24(state.secret, lowered));
}

function parseIso8601UtcMillisFromBytes(bytes: Uint8Array): number | null {
  if (bytes.byteLength !== 24) return null;
  const codeAt = (index: number) => bytes[index]!;
  const digitAt = (index: number): number | null => {
    const code = codeAt(index);
    return code >= 48 && code <= 57 ? code - 48 : null;
  };
  const read2 = (start: number): number | null => {
    const a = digitAt(start);
    const b = digitAt(start + 1);
    return a == null || b == null ? null : a * 10 + b;
  };
  const read3 = (start: number): number | null => {
    const a = digitAt(start);
    const b = digitAt(start + 1);
    const c = digitAt(start + 2);
    return a == null || b == null || c == null ? null : a * 100 + b * 10 + c;
  };
  const year =
    digitAt(0) == null || digitAt(1) == null || digitAt(2) == null || digitAt(3) == null
      ? null
      : digitAt(0)! * 1000 + digitAt(1)! * 100 + digitAt(2)! * 10 + digitAt(3)!;
  const month = read2(5);
  const day = read2(8);
  const hour = read2(11);
  const minute = read2(14);
  const second = read2(17);
  const millisecond = read3(20);
  if (
    year == null ||
    month == null ||
    day == null ||
    hour == null ||
    minute == null ||
    second == null ||
    millisecond == null ||
    codeAt(4) !== 45 ||
    codeAt(7) !== 45 ||
    codeAt(10) !== 84 ||
    codeAt(13) !== 58 ||
    codeAt(16) !== 58 ||
    codeAt(19) !== 46 ||
    codeAt(23) !== 90
  ) {
    return null;
  }
  return Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
}

function addSingleSegmentExactFingerprintFromToken(
  state: BatchState,
  bytes: Uint8Array,
  token: JsonScalarToken
): Result<void, BuildError> {
  switch (state.index.config.kind) {
    case "keyword": {
      if (token.kind === "null") return Result.ok(undefined);
      if (token.kind !== "string") return Result.ok(undefined);
      if (!token.hadEscape) {
        const rawBytes = bytes.subarray(token.start + 1, token.end);
        if (state.index.config.normalizer === "lowercase_v1") {
          hashAsciiLowercaseBytes(state, rawBytes);
        } else {
          addSingleSegmentFingerprint(state, siphash24(state.secret, rawBytes));
        }
        return Result.ok(undefined);
      }
      const decodedRes = decodeJsonStringTokenBytes(bytes, token);
      if (Result.isError(decodedRes)) return decodedRes;
      hashCanonicalString(state, canonicalizeExactValue(state.index.config, decodedRes.value));
      return Result.ok(undefined);
    }
    case "date": {
      if (token.kind === "null") return Result.ok(undefined);
      if (token.kind === "string" && !token.hadEscape) {
        const epochMillis = parseIso8601UtcMillisFromBytes(bytes.subarray(token.start + 1, token.end));
        if (epochMillis != null) {
          hashCanonicalString(state, String(epochMillis));
          return Result.ok(undefined);
        }
      }
      if (token.kind === "number") {
        const canonical = PAYLOAD_DECODER.decode(bytes.subarray(token.start, token.end));
        hashCanonicalString(state, canonicalizeExactValue(state.index.config, canonical));
        return Result.ok(undefined);
      }
      if (token.kind !== "string") return Result.ok(undefined);
      const decodedRes = decodeJsonStringTokenBytes(bytes, token);
      if (Result.isError(decodedRes)) return decodedRes;
      hashCanonicalString(state, canonicalizeExactValue(state.index.config, decodedRes.value));
      return Result.ok(undefined);
    }
    case "integer":
    case "float": {
      if (token.kind !== "number") return Result.ok(undefined);
      const numericText = PAYLOAD_DECODER.decode(bytes.subarray(token.start, token.end));
      hashCanonicalString(state, canonicalizeExactValue(state.index.config, numericText));
      return Result.ok(undefined);
    }
    case "bool": {
      const canonical =
        token.kind === "true" ? "true" : token.kind === "false" ? "false" : null;
      hashCanonicalString(state, canonical);
      return Result.ok(undefined);
    }
    default:
      return Result.ok(undefined);
  }
}

function findMatchingStatesForObjectKeyResult(
  bytes: Uint8Array,
  keyStart: number,
  keyEnd: number,
  hadEscape: boolean,
  plan: SingleSegmentFastPlan
): Result<BatchState[] | null, BuildError> {
  if (hadEscape) {
    const decodedRes = decodeJsonStringTokenBytes(bytes, {
      kind: "string",
      start: keyStart,
      end: keyEnd,
      hadEscape: true,
    });
    if (Result.isError(decodedRes)) return decodedRes;
    return Result.ok(plan.decodedStatesByKey.get(decodedRes.value) ?? null);
  }
  for (const entry of plan.asciiEntries) {
    if (bytesEqualAsciiString(bytes, keyStart + 1, keyEnd, entry.key)) return Result.ok(entry.states);
  }
  return Result.ok(null);
}

function visitSingleSegmentTopLevelScalarExactTokensRangeResult(
  bytes: Uint8Array,
  payloadStart: number,
  payloadEnd: number,
  plan: SingleSegmentFastPlan
): Result<void, BuildError> {
  let index = skipJsonWhitespaceBytes(bytes, payloadStart, payloadEnd);
  if (index >= payloadEnd) return invalidIndexBuild("search fields require JSON object records");
  if (bytes[index] !== 123) return invalidIndexBuild("search fields require JSON object records");
  index += 1;
  while (index < payloadEnd) {
    index = skipJsonWhitespaceBytes(bytes, index, payloadEnd);
    if (index >= payloadEnd) return invalidIndexBuild("unterminated JSON object");
    const token = bytes[index];
    if (token === 125) return Result.ok(undefined);
    if (token !== 34) return invalidIndexBuild("invalid JSON object key");
    const keyEndRes = scanJsonStringEndBytes(bytes, index, payloadEnd);
    if (Result.isError(keyEndRes)) return keyEndRes;
    const matchedStatesRes = findMatchingStatesForObjectKeyResult(bytes, index, keyEndRes.value.end, keyEndRes.value.hadEscape, plan);
    if (Result.isError(matchedStatesRes)) return matchedStatesRes;
    const matchedStates = matchedStatesRes.value;
    index = skipJsonWhitespaceBytes(bytes, keyEndRes.value.end + 1, payloadEnd);
    if (index >= payloadEnd) return invalidIndexBuild("unterminated JSON object");
    if (bytes[index] !== 58) return invalidIndexBuild("invalid JSON object separator");
    index = skipJsonWhitespaceBytes(bytes, index + 1, payloadEnd);
    if (index >= payloadEnd) return invalidIndexBuild("unterminated JSON object");
    if (!matchedStates) {
      const skipRes = skipJsonValueBytes(bytes, index, payloadEnd);
      if (Result.isError(skipRes)) return skipRes;
      index = skipRes.value;
    } else if (bytes[index] === 123 || bytes[index] === 91) {
      const skipRes = skipJsonValueBytes(bytes, index, payloadEnd);
      if (Result.isError(skipRes)) return skipRes;
      index = skipRes.value;
    } else {
      const scalarRes = parseJsonScalarTokenBytes(bytes, index, payloadEnd);
      if (Result.isError(scalarRes)) return scalarRes;
      for (const state of matchedStates) {
        const addRes = addSingleSegmentExactFingerprintFromToken(state, bytes, scalarRes.value.token);
        if (Result.isError(addRes)) return addRes;
      }
      index = scalarRes.value.next;
    }
    index = skipJsonWhitespaceBytes(bytes, index, payloadEnd);
    if (index >= payloadEnd) return invalidIndexBuild("unterminated JSON object");
    if (bytes[index] === 44) {
      index += 1;
      continue;
    }
    if (bytes[index] === 125) return Result.ok(undefined);
    return invalidIndexBuild("invalid JSON object delimiter");
  }
  return invalidIndexBuild("unterminated JSON object");
}

function buildSingleSegmentTopLevelScalarSecondaryL0RunPayloadResult(
  input: SecondaryL0BuildInput
): Result<SecondaryL0BuildOutput, BuildError> | null {
  if (input.span !== 1 || input.segments.length !== 1 || input.registry.search?.profile !== "evlog") return null;
  const segment = input.segments[0];
  if (!segment) return Result.ok({ runs: [] });
  const states = input.indexes.map((entry) => ({
    index: entry.index,
    secret: entry.secret,
    maskByFp: null,
    fpByCanonical: null,
    fingerprints: shouldUseBufferedSingleSegmentFingerprints(entry.index.name) ? null : new Set<bigint>(),
    fingerprintBuffer: shouldUseBufferedSingleSegmentFingerprints(entry.index.name) ? new BigUint64Array(256) : null,
    fingerprintCount: 0,
    termScratch: new Uint8Array(0),
    lowercaseScratch: new Uint8Array(0),
  })) satisfies BatchState[];
  const stateByName = new Map(states.map((state) => [state.index.name, state]));
  const versions = Array.from(new Set(input.registry.boundaries.map((boundary) => boundary.version)));
  const plansByVersion = new Map<number, SingleSegmentFastPlan>();
  for (const version of versions) {
    const compiledRes = buildCompiledAccessorsForVersionResult(input.registry, states, version);
    if (Result.isError(compiledRes)) return compiledRes;
    const groupedStatesByKey = new Map<string, BatchState[]>();
    for (const accessor of compiledRes.value) {
      if (!supportsFastScalarJsonExtraction(accessor) || accessor.path.length !== 1) return null;
      const state = stateByName.get(accessor.fieldName);
      if (!state) continue;
      const key = accessor.path[0]!;
      let statesForKey = groupedStatesByKey.get(key);
      if (!statesForKey) {
        statesForKey = [];
        groupedStatesByKey.set(key, statesForKey);
      }
      statesForKey.push(state);
    }
    plansByVersion.set(version, {
      asciiEntries: Array.from(groupedStatesByKey, ([key, states]) => ({ key, states })),
      decodedStatesByKey: groupedStatesByKey,
    });
  }

  let offset = segment.startOffset;
  const visitPayloadsRes = visitBlockRecordPayloadsFromFileResult(segment.localPath, (payloadBytes, payloadStart, payloadEnd) => {
    const version = schemaVersionForOffset(input.registry, offset);
    const plan = plansByVersion.get(version);
    if (!plan) return invalidIndexBuild(`missing exact fast plan for schema version ${version}`);
    const visitRes = visitSingleSegmentTopLevelScalarExactTokensRangeResult(payloadBytes, payloadStart, payloadEnd, plan);
    if (Result.isError(visitRes)) return visitRes;
    offset += 1n;
    return Result.ok(undefined);
  });
  if (Result.isError(visitPayloadsRes)) return invalidIndexBuild(visitPayloadsRes.error.message);

  const streamHash = streamHash16Hex(input.stream);
  const runs: SecondaryL0BuildOutput["runs"] = [];
  for (const state of states) {
    const fingerprints = finalizeSingleSegmentFingerprints(state);
    const runId = `${state.index.name}-l0-${input.startSegment.toString().padStart(16, "0")}-${input.startSegment
      .toString()
      .padStart(16, "0")}-${Date.now()}`;
    const meta = {
      runId,
      level: 0,
      startSegment: input.startSegment,
      endSegment: input.startSegment,
      objectKey: secondaryIndexRunObjectKey(streamHash, state.index.name, runId),
      filterLen: 0,
      recordCount: fingerprints.length,
    };
    const payloadRes = encodeIndexRunResult({
      meta,
      runType: RUN_TYPE_MASK16,
      filterBytes: new Uint8Array(0),
      fingerprints,
      masks: new Array(fingerprints.length).fill(1),
    });
    if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
    if (input.outputDir) {
      const fileRes = writeIndexRunOutputFileResult(input.outputDir, `${state.index.name}-l0-${input.startSegment}`, payloadRes.value);
      if (Result.isError(fileRes)) return invalidIndexBuild(fileRes.error.message);
      runs.push({
        indexName: state.index.name,
        meta,
        storage: "file",
        localPath: fileRes.value.localPath,
        sizeBytes: fileRes.value.sizeBytes,
      });
    } else {
      runs.push({
        indexName: state.index.name,
        meta,
        storage: "bytes",
        payload: payloadRes.value,
      });
    }
  }
  return Result.ok({ runs });
}

function buildSingleFieldSingleSegmentSecondaryL0RunPayloadResult(
  input: SecondaryL0BuildInput
): Result<SecondaryL0BuildOutput, BuildError> {
  const entry = input.indexes[0];
  if (!entry) return Result.ok({ runs: [] });
  const segment = input.segments[0];
  if (!segment) return Result.ok({ runs: [] });

  const fingerprints = new Set<bigint>();
  let fingerprintBuffer: BigUint64Array | null = shouldUseBufferedSingleSegmentFingerprints(entry.index.name)
    ? new BigUint64Array(256)
    : null;
  let fingerprintCount = 0;
  const compiledByVersion = new Map<number, CompiledSearchFieldAccessor | null>();
  let offset = segment.startOffset;

  const visitRes = visitBlockRecordPayloadsFromFileResult(segment.localPath, (payloadBytes, payloadStart, payloadEnd) => {
    const payload = payloadBytes.subarray(payloadStart, payloadEnd);
    const version = schemaVersionForOffset(input.registry, offset);
    let accessor = compiledByVersion.get(version);
    if (accessor === undefined) {
      const compiledRes = compileSearchFieldAccessorsResult(input.registry, [entry.index.name], version);
      if (Result.isError(compiledRes)) return invalidIndexBuild(compiledRes.error.message);
      accessor = compiledRes.value[0] ?? null;
      compiledByVersion.set(version, accessor);
    }
    if (accessor) {
      if (supportsFastScalarJsonExtraction(accessor)) {
        const fastValueRes = extractFastScalarJsonValueFromBytesResult(payload, accessor);
        if (Result.isOk(fastValueRes)) {
          if (fastValueRes.value.exists) {
            const canonical = canonicalizeExactValue(entry.index.config, fastValueRes.value.value);
            if (canonical != null) {
              const fingerprint = siphash24(entry.secret, TERM_ENCODER.encode(canonical));
              if (fingerprintBuffer) {
                if (fingerprintCount >= fingerprintBuffer.length) {
                  const next = new BigUint64Array(fingerprintBuffer.length * 2);
                  next.set(fingerprintBuffer);
                  fingerprintBuffer = next;
                }
                fingerprintBuffer[fingerprintCount] = fingerprint;
                fingerprintCount += 1;
              } else {
                fingerprints.add(fingerprint);
              }
            }
          }
        } else {
          let parsed: unknown;
          try {
            parsed = JSON.parse(PAYLOAD_DECODER.decode(payload));
          } catch {
            offset += 1n;
            return Result.ok(undefined);
          }
          const rawValuesRes = visitCompiledAccessorRawValuesResult(parsed, accessor, (rawValue) => {
            const canonical = canonicalizeExactValue(entry.index.config, rawValue);
            if (canonical == null) return;
            const fingerprint = siphash24(entry.secret, TERM_ENCODER.encode(canonical));
            if (fingerprintBuffer) {
              if (fingerprintCount >= fingerprintBuffer.length) {
                const next = new BigUint64Array(fingerprintBuffer.length * 2);
                next.set(fingerprintBuffer);
                fingerprintBuffer = next;
              }
              fingerprintBuffer[fingerprintCount] = fingerprint;
              fingerprintCount += 1;
            } else {
              fingerprints.add(fingerprint);
            }
          });
          if (Result.isError(rawValuesRes)) return invalidIndexBuild(rawValuesRes.error.message);
        }
      } else {
        let parsed: unknown;
        try {
          parsed = JSON.parse(PAYLOAD_DECODER.decode(payload));
        } catch {
          offset += 1n;
          return Result.ok(undefined);
        }
        const rawValuesRes = visitCompiledAccessorRawValuesResult(parsed, accessor, (rawValue) => {
          const canonical = canonicalizeExactValue(entry.index.config, rawValue);
          if (canonical == null) return;
          const fingerprint = siphash24(entry.secret, TERM_ENCODER.encode(canonical));
          if (fingerprintBuffer) {
            if (fingerprintCount >= fingerprintBuffer.length) {
              const next = new BigUint64Array(fingerprintBuffer.length * 2);
              next.set(fingerprintBuffer);
              fingerprintBuffer = next;
            }
            fingerprintBuffer[fingerprintCount] = fingerprint;
            fingerprintCount += 1;
          } else {
            fingerprints.add(fingerprint);
          }
        });
        if (Result.isError(rawValuesRes)) return invalidIndexBuild(rawValuesRes.error.message);
      }
    }
    offset += 1n;
    return Result.ok(undefined);
  });
  if (Result.isError(visitRes)) return invalidIndexBuild(visitRes.error.message);

  const orderedFingerprints = fingerprintBuffer
    ? (() => {
        const ordered = new BigUint64Array(fingerprintBuffer.subarray(0, fingerprintCount));
        ordered.sort();
        const out: bigint[] = [];
        let last: bigint | null = null;
        for (const fingerprint of ordered) {
          if (last != null && fingerprint === last) continue;
          out.push(fingerprint);
          last = fingerprint;
        }
        return out;
      })()
    : sortFingerprints(fingerprints);
  const runId = `${entry.index.name}-l0-${input.startSegment.toString().padStart(16, "0")}-${input.startSegment.toString().padStart(16, "0")}-${Date.now()}`;
  const meta = {
    runId,
    level: 0,
    startSegment: input.startSegment,
    endSegment: input.startSegment,
    objectKey: secondaryIndexRunObjectKey(streamHash16Hex(input.stream), entry.index.name, runId),
    filterLen: 0,
    recordCount: orderedFingerprints.length,
  };
  const payloadRes = encodeIndexRunResult({
    meta,
    runType: RUN_TYPE_MASK16,
    filterBytes: new Uint8Array(0),
    fingerprints: orderedFingerprints,
    masks: new Array(orderedFingerprints.length).fill(1),
  });
  if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
  if (input.outputDir) {
    const fileRes = writeIndexRunOutputFileResult(input.outputDir, `${entry.index.name}-l0-${input.startSegment}`, payloadRes.value);
    if (Result.isError(fileRes)) return invalidIndexBuild(fileRes.error.message);
    return Result.ok({
      runs: [
        {
          indexName: entry.index.name,
          meta,
          storage: "file",
          localPath: fileRes.value.localPath,
          sizeBytes: fileRes.value.sizeBytes,
        },
      ],
    });
  }
  return Result.ok({
    runs: [
      {
        indexName: entry.index.name,
        meta,
        storage: "bytes",
        payload: payloadRes.value,
      },
    ],
  });
}

function invalidIndexBuild(message: string): Result<never, BuildError> {
  return Result.err({ kind: "invalid_index_build", message });
}

function buildCompiledAccessorsForVersionResult(
  registry: SchemaRegistry,
  indexes: BatchState[],
  version: number
): Result<CompiledSearchFieldAccessor[], BuildError> {
  const compiledRes = compileSearchFieldAccessorsResult(
    registry,
    indexes.map((entry) => entry.index.name),
    version
  );
  if (Result.isError(compiledRes)) return invalidIndexBuild(compiledRes.error.message);
  return Result.ok(compiledRes.value);
}

export function buildSecondaryL0RunPayloadResult(
  input: SecondaryL0BuildInput
): Result<SecondaryL0BuildOutput, BuildError> {
  if (input.span === 1 && input.indexes.length === 1 && input.segments.length === 1) {
    return buildSingleFieldSingleSegmentSecondaryL0RunPayloadResult(input);
  }
  const singleSegmentFastRes = buildSingleSegmentTopLevelScalarSecondaryL0RunPayloadResult(input);
  if (singleSegmentFastRes) return singleSegmentFastRes;
  const singleSegmentWindow = input.span === 1;
  const states = input.indexes.map((entry) => ({
    index: entry.index,
    secret: entry.secret,
    maskByFp: singleSegmentWindow ? null : new Map<bigint, number>(),
    fpByCanonical: singleSegmentWindow ? null : new Map<string, bigint>(),
    fingerprints:
      singleSegmentWindow && !shouldUseBufferedSingleSegmentFingerprints(entry.index.name) ? new Set<bigint>() : null,
    fingerprintBuffer:
      singleSegmentWindow && shouldUseBufferedSingleSegmentFingerprints(entry.index.name) ? new BigUint64Array(256) : null,
    fingerprintCount: 0,
    termScratch: new Uint8Array(0),
    lowercaseScratch: new Uint8Array(0),
  })) satisfies BatchState[];
  const stateByName = new Map(states.map((entry) => [entry.index.name, entry]));
  const compiledByVersion = new Map<number, CompiledSearchFieldAccessor[]>();

  for (const segment of input.segments) {
    const bit = segment.segmentIndex - input.startSegment;
    if (bit < 0 || bit >= input.span) {
      return invalidIndexBuild(`exact L0 segment ${segment.segmentIndex} is outside the ${input.span}-segment build window`);
    }
    const maskBit = 1 << bit;
    const localSeen = singleSegmentWindow ? null : new Map<string, Set<string>>();
    let offset = segment.startOffset;
    for (const recordRes of iterateBlockRecordsFromFileResult(segment.localPath)) {
      if (Result.isError(recordRes)) return invalidIndexBuild(recordRes.error.message);
      let parsed: unknown;
      try {
        parsed = JSON.parse(PAYLOAD_DECODER.decode(recordRes.value.payload));
      } catch {
        offset += 1n;
        continue;
      }
      const version = schemaVersionForOffset(input.registry, offset);
      let accessors = compiledByVersion.get(version);
      if (!accessors) {
        const compiledRes = buildCompiledAccessorsForVersionResult(input.registry, states, version);
        if (Result.isError(compiledRes)) return compiledRes;
        accessors = compiledRes.value;
        compiledByVersion.set(version, accessors);
      }
      const rawValuesRes = visitRawSearchValuesWithCompiledAccessorsResult(parsed, accessors, (accessor, rawValue) => {
        const state = stateByName.get(accessor.fieldName);
        if (!state) return;
        const canonical = canonicalizeExactValue(state.index.config, rawValue);
        if (canonical == null) return;
        if (singleSegmentWindow) {
          addSingleSegmentFingerprint(state, siphash24(state.secret, TERM_ENCODER.encode(canonical)));
          return;
        }
        let seenForField = localSeen!.get(accessor.fieldName);
        if (!seenForField) {
          seenForField = new Set<string>();
          localSeen!.set(accessor.fieldName, seenForField);
        }
        if (seenForField.has(canonical)) return;
        seenForField.add(canonical);
        let fp = state.fpByCanonical!.get(canonical);
        if (fp == null) {
          fp = siphash24(state.secret, TERM_ENCODER.encode(canonical));
          state.fpByCanonical!.set(canonical, fp);
        }
        state.maskByFp!.set(fp, (state.maskByFp!.get(fp) ?? 0) | maskBit);
      });
      if (Result.isError(rawValuesRes)) return invalidIndexBuild(rawValuesRes.error.message);
      offset += 1n;
    }
  }

  const endSegment = input.startSegment + input.span - 1;
  const streamHash = streamHash16Hex(input.stream);
  const runs: SecondaryL0BuildOutput["runs"] = [];

  for (const state of states) {
    let fingerprints: bigint[];
    let masks: number[];
    if (singleSegmentWindow) {
      fingerprints = finalizeSingleSegmentFingerprints(state);
      masks = new Array(fingerprints.length).fill(1);
    } else {
      fingerprints = sortFingerprints(state.maskByFp!.keys());
      masks = fingerprints.map((fp) => state.maskByFp!.get(fp) ?? 0);
    }
    const runId = `${state.index.name}-l0-${input.startSegment.toString().padStart(16, "0")}-${endSegment
      .toString()
      .padStart(16, "0")}-${Date.now()}`;
    const meta = {
      runId,
      level: 0,
      startSegment: input.startSegment,
      endSegment,
      objectKey: secondaryIndexRunObjectKey(streamHash, state.index.name, runId),
      filterLen: 0,
      recordCount: fingerprints.length,
    };
    const payloadRes = encodeIndexRunResult({
      meta,
      runType: RUN_TYPE_MASK16,
      filterBytes: new Uint8Array(0),
      fingerprints,
      masks,
    });
    if (Result.isError(payloadRes)) return invalidIndexBuild(payloadRes.error.message);
    if (input.outputDir) {
      const fileRes = writeIndexRunOutputFileResult(
        input.outputDir,
        `${state.index.name}-l0-${input.startSegment}-${endSegment}`,
        payloadRes.value
      );
      if (Result.isError(fileRes)) return invalidIndexBuild(fileRes.error.message);
      runs.push({
        indexName: state.index.name,
        meta,
        storage: "file",
        localPath: fileRes.value.localPath,
        sizeBytes: fileRes.value.sizeBytes,
      });
    } else {
      runs.push({
        indexName: state.index.name,
        meta,
        storage: "bytes",
        payload: payloadRes.value,
      });
    }
  }

  return Result.ok({ runs });
}
