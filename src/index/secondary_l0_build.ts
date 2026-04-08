import { readFileSync } from "node:fs";
import { Result } from "better-result";
import type { SchemaRegistry } from "../schema/registry";
import { iterateBlockRecordsResult } from "../segment/format";
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
  canonicalTerms: Set<string> | null;
};

function buildSingleFieldSingleSegmentSecondaryL0RunPayloadResult(
  input: SecondaryL0BuildInput
): Result<SecondaryL0BuildOutput, BuildError> {
  const entry = input.indexes[0];
  if (!entry) return Result.ok({ runs: [] });
  const segment = input.segments[0];
  if (!segment) return Result.ok({ runs: [] });

  const canonicalTerms = new Set<string>();
  const compiledByVersion = new Map<number, CompiledSearchFieldAccessor | null>();
  const segBytes = readFileSync(segment.localPath);
  let offset = segment.startOffset;

  for (const recordRes of iterateBlockRecordsResult(segBytes)) {
    if (Result.isError(recordRes)) return invalidIndexBuild(recordRes.error.message);
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
        const fastValueRes = extractFastScalarJsonValueFromBytesResult(recordRes.value.payload, accessor);
        if (Result.isOk(fastValueRes)) {
          if (fastValueRes.value.exists) {
            const canonical = canonicalizeExactValue(entry.index.config, fastValueRes.value.value);
            if (canonical != null) canonicalTerms.add(canonical);
          }
        } else {
          let parsed: unknown;
          try {
            parsed = JSON.parse(PAYLOAD_DECODER.decode(recordRes.value.payload));
          } catch {
            offset += 1n;
            continue;
          }
          const rawValuesRes = visitCompiledAccessorRawValuesResult(parsed, accessor, (rawValue) => {
            const canonical = canonicalizeExactValue(entry.index.config, rawValue);
            if (canonical == null) return;
            canonicalTerms.add(canonical);
          });
          if (Result.isError(rawValuesRes)) return invalidIndexBuild(rawValuesRes.error.message);
        }
      } else {
        let parsed: unknown;
        try {
          parsed = JSON.parse(PAYLOAD_DECODER.decode(recordRes.value.payload));
        } catch {
          offset += 1n;
          continue;
        }
        const rawValuesRes = visitCompiledAccessorRawValuesResult(parsed, accessor, (rawValue) => {
          const canonical = canonicalizeExactValue(entry.index.config, rawValue);
          if (canonical == null) return;
          canonicalTerms.add(canonical);
        });
        if (Result.isError(rawValuesRes)) return invalidIndexBuild(rawValuesRes.error.message);
      }
    }
    offset += 1n;
  }

  const fingerprints = Array.from(canonicalTerms, (canonical) => siphash24(entry.secret, TERM_ENCODER.encode(canonical))).sort((a, b) =>
    a < b ? -1 : a > b ? 1 : 0
  );
  const runId = `${entry.index.name}-l0-${input.startSegment.toString().padStart(16, "0")}-${input.startSegment.toString().padStart(16, "0")}-${Date.now()}`;
  const meta = {
    runId,
    level: 0,
    startSegment: input.startSegment,
    endSegment: input.startSegment,
    objectKey: secondaryIndexRunObjectKey(streamHash16Hex(input.stream), entry.index.name, runId),
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
  const singleSegmentWindow = input.span === 1;
  const states = input.indexes.map((entry) => ({
    index: entry.index,
    secret: entry.secret,
    maskByFp: singleSegmentWindow ? null : new Map<bigint, number>(),
    fpByCanonical: singleSegmentWindow ? null : new Map<string, bigint>(),
    canonicalTerms: singleSegmentWindow ? new Set<string>() : null,
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
    const segBytes = readFileSync(segment.localPath);
    let offset = segment.startOffset;
    for (const recordRes of iterateBlockRecordsResult(segBytes)) {
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
          state.canonicalTerms!.add(canonical);
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
      fingerprints = Array.from(state.canonicalTerms!, (canonical) => siphash24(state.secret, TERM_ENCODER.encode(canonical))).sort((a, b) =>
        a < b ? -1 : a > b ? 1 : 0
      );
      masks = new Array(fingerprints.length).fill(1);
    } else {
      fingerprints = Array.from(state.maskByFp!.keys()).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
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
