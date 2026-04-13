import { Result } from "better-result";
import type {
  IndexRunRow,
  IndexStateRow,
  LexiconIndexRunRow,
  LexiconIndexStateRow,
  SearchCompanionPlanRow,
  SearchSegmentCompanionRow,
  SecondaryIndexRunRow,
  SecondaryIndexStateRow,
  SegmentMetaRow,
  StreamRow,
} from "./db/db";
import { encodeOffsetResult } from "./offset";
import { zstdCompressSync } from "./util/zstd";
import { dsError } from "./util/ds_error.ts";

function b64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

function compressB64(bytes: Uint8Array): string {
  return b64(new Uint8Array(zstdCompressSync(bytes)));
}

function parseSectionsJson(raw: string): string[] {
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((value): value is string => typeof value === "string") : [];
  } catch {
    return [];
  }
}

export type ManifestJson = Record<string, any>;
export type ManifestBuildError = { kind: "invalid_manifest"; message: string };

function invalidManifest<T = never>(message: string): Result<T, ManifestBuildError> {
  return Result.err({ kind: "invalid_manifest", message });
}

type BuildManifestArgs = {
  streamName: string;
  streamRow: StreamRow;
  publishedLogicalSizeBytes: bigint;
  profileJson?: Record<string, any> | null;
  segmentMeta: SegmentMetaRow;
  uploadedPrefixCount: number;
  generation: number;
  indexState?: IndexStateRow | null;
  indexRuns?: IndexRunRow[];
  retiredRuns?: IndexRunRow[];
  secondaryIndexStates?: SecondaryIndexStateRow[];
  secondaryIndexRuns?: SecondaryIndexRunRow[];
  retiredSecondaryIndexRuns?: SecondaryIndexRunRow[];
  lexiconIndexStates?: LexiconIndexStateRow[];
  lexiconIndexRuns?: LexiconIndexRunRow[];
  retiredLexiconIndexRuns?: LexiconIndexRunRow[];
  searchCompanionPlan?: SearchCompanionPlanRow | null;
  searchSegmentCompanions?: SearchSegmentCompanionRow[];
};

export function buildManifestResult(args: BuildManifestArgs): Result<ManifestJson, ManifestBuildError> {
  const {
    streamName,
    streamRow,
    publishedLogicalSizeBytes,
    profileJson,
    segmentMeta,
    uploadedPrefixCount,
    generation,
    indexState,
    indexRuns,
    retiredRuns,
  } = args;

  const createdAt = new Date(Number(streamRow.created_at_ms)).toISOString();
  const expiresAt = streamRow.expires_at_ms == null ? null : new Date(Number(streamRow.expires_at_ms)).toISOString();

  const nextOffset = streamRow.next_offset;
  const nextOffsetNum = Number(nextOffset);
  const nextOffsetEncodedRes = encodeOffsetResult(streamRow.epoch, nextOffset);
  if (Result.isError(nextOffsetEncodedRes)) return invalidManifest(nextOffsetEncodedRes.error.message);
  const nextOffsetEncoded = nextOffsetEncodedRes.value;

  const maxCount = Math.max(0, segmentMeta.segment_count);
  const prefix = Math.max(0, Math.min(uploadedPrefixCount, maxCount));
  const offBytes = segmentMeta.segment_offsets.subarray(0, prefix * 8);
  const blockBytes = segmentMeta.segment_blocks.subarray(0, prefix * 4);
  const lastTsBytes = segmentMeta.segment_last_ts.subarray(0, prefix * 8);

  const segOffsetsB64 = compressB64(offBytes);
  const segBlocksB64 = compressB64(blockBytes);
  const segLastTsB64 = compressB64(lastTsBytes);

  const activeRuns =
    indexRuns?.map((r) => ({
      run_id: r.run_id,
      level: r.level,
      start_segment: r.start_segment,
      end_segment: r.end_segment,
      object_key: r.object_key,
      size_bytes: r.size_bytes,
      filter_len: r.filter_len,
      record_count: r.record_count,
    })) ?? [];
  const retired = retiredRuns?.map((r) => ({
    run_id: r.run_id,
    level: r.level,
      start_segment: r.start_segment,
      end_segment: r.end_segment,
      object_key: r.object_key,
      size_bytes: r.size_bytes,
      filter_len: r.filter_len,
      record_count: r.record_count,
      retired_gen: r.retired_gen ?? undefined,
    retired_at_unix: r.retired_at_ms != null ? Number(r.retired_at_ms / 1000n) : undefined,
  })) ?? [];
  const indexSecret = indexState?.index_secret ? b64(indexState.index_secret) : "";
  const indexedThrough = indexState?.indexed_through ?? 0;
  const secondaryIndexes: Record<string, any> = {};
  const secondaryStates = args.secondaryIndexStates ?? [];
  const secondaryRuns = args.secondaryIndexRuns ?? [];
  const retiredSecondaryRuns = args.retiredSecondaryIndexRuns ?? [];
  for (const state of secondaryStates) {
    secondaryIndexes[state.index_name] = {
      index_secret: b64(state.index_secret),
      config_hash: state.config_hash,
      indexed_through: state.indexed_through,
      active_runs: secondaryRuns
        .filter((run) => run.index_name === state.index_name)
        .map((run) => ({
          run_id: run.run_id,
          level: run.level,
          start_segment: run.start_segment,
          end_segment: run.end_segment,
          object_key: run.object_key,
          size_bytes: run.size_bytes,
          filter_len: run.filter_len,
          record_count: run.record_count,
        })),
      retired_runs: retiredSecondaryRuns
        .filter((run) => run.index_name === state.index_name)
        .map((run) => ({
          run_id: run.run_id,
          level: run.level,
          start_segment: run.start_segment,
          end_segment: run.end_segment,
          object_key: run.object_key,
          size_bytes: run.size_bytes,
          filter_len: run.filter_len,
          record_count: run.record_count,
          retired_gen: run.retired_gen ?? undefined,
          retired_at_unix: run.retired_at_ms != null ? Number(run.retired_at_ms / 1000n) : undefined,
        })),
      };
  }
  const lexiconIndexes = (args.lexiconIndexStates ?? []).map((state) => ({
    source_kind: state.source_kind,
    source_name: state.source_name,
    indexed_through: state.indexed_through,
    active_runs: (args.lexiconIndexRuns ?? [])
      .filter((run) => run.source_kind === state.source_kind && run.source_name === state.source_name)
      .map((run) => ({
        run_id: run.run_id,
        level: run.level,
        start_segment: run.start_segment,
        end_segment: run.end_segment,
        object_key: run.object_key,
        size_bytes: run.size_bytes,
        record_count: run.record_count,
      })),
    retired_runs: (args.retiredLexiconIndexRuns ?? [])
      .filter((run) => run.source_kind === state.source_kind && run.source_name === state.source_name)
      .map((run) => ({
        run_id: run.run_id,
        level: run.level,
        start_segment: run.start_segment,
        end_segment: run.end_segment,
        object_key: run.object_key,
        size_bytes: run.size_bytes,
        record_count: run.record_count,
        retired_gen: run.retired_gen ?? undefined,
        retired_at_unix: run.retired_at_ms != null ? Number(run.retired_at_ms / 1000n) : undefined,
      })),
  }));
  const searchCompanionPlan = args.searchCompanionPlan ?? null;
  const searchCompanionSegments = (args.searchSegmentCompanions ?? [])
    .filter((segment) => segment.segment_index < prefix)
    .map((segment) => ({
      segment_index: segment.segment_index,
      object_key: segment.object_key,
      size_bytes: segment.size_bytes,
      plan_generation: segment.plan_generation,
      primary_timestamp_min_ms: segment.primary_timestamp_min_ms?.toString() ?? undefined,
      primary_timestamp_max_ms: segment.primary_timestamp_max_ms?.toString() ?? undefined,
      sections: parseSectionsJson(segment.sections_json),
      section_sizes: JSON.parse(segment.section_sizes_json || "{}"),
    }));

  return Result.ok({
    name: streamName,
    created_at: createdAt,
    expires_at: expiresAt,
    content_type: streamRow.content_type,
    profile: streamRow.profile ?? "generic",
    profile_json: profileJson ?? null,
    stream_seq: streamRow.stream_seq ?? null,
    closed: streamRow.closed,
    closed_producer_id: streamRow.closed_producer_id ?? null,
    closed_producer_epoch: streamRow.closed_producer_epoch ?? null,
    closed_producer_seq: streamRow.closed_producer_seq ?? null,
    ttl_seconds: streamRow.ttl_seconds ?? null,
    stream_flags: streamRow.stream_flags,
    generation,
    epoch: streamRow.epoch,
    next_offset: nextOffsetNum,
    next_offset_encoded: nextOffsetEncoded,
    logical_size_bytes: publishedLogicalSizeBytes.toString(),
    segment_count: prefix,
    uploaded_through: prefix,
    active_file_offset: nextOffsetNum,
    last_committed_ts: Number(streamRow.last_append_ms * 1_000_000n),
    zstd_dict: "",
    segment_offsets: segOffsetsB64,
    segment_blocks: segBlocksB64,
    segment_last_ts: segLastTsB64,
    indexed_through: indexedThrough,
    index_secret: indexSecret,
    active_runs: activeRuns,
    retired_runs: retired,
    secondary_indexes: secondaryIndexes,
    lexicon_indexes: lexiconIndexes,
    search_companions: searchCompanionPlan
      ? {
          generation: searchCompanionPlan.generation,
          plan_hash: searchCompanionPlan.plan_hash,
          plan_json: JSON.parse(searchCompanionPlan.plan_json),
          segments: searchCompanionSegments,
        }
      : null,
  });
}

export function buildManifest(args: BuildManifestArgs): ManifestJson {
  const res = buildManifestResult(args);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}
