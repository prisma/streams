import { Result } from "better-result";
import { buildRoutingL0RunPayloadResult } from "./routing_l0_build";
import { buildRoutingCompactionPayloadResult } from "./routing_compaction_build";
import { buildRoutingLexiconL0RunPayloadResult } from "./routing_lexicon_l0_build";
import { buildLexiconCompactionPayloadResult } from "./lexicon_compaction_build";
import { buildSecondaryL0RunPayloadResult } from "./secondary_l0_build";
import { buildSecondaryCompactionPayloadResult } from "./secondary_compaction_build";
import { buildEncodedBundledCompanionPayloadResult } from "../search/companion_build";
import { buildCompanionMergeResult } from "../search/companion_merge";
import { buildSearchSegmentResult } from "../search/search_segment_build";
import type { IndexBuildJobInput, IndexBuildJobOutput } from "./index_build_job";

export function runIndexBuildJobResult(
  job: IndexBuildJobInput
): Result<IndexBuildJobOutput, { message: string }> {
  switch (job.kind) {
    case "routing_l0_build": {
      const res = buildRoutingL0RunPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "routing_compaction_build": {
      const res = buildRoutingCompactionPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "routing_lexicon_l0_build": {
      const res = buildRoutingLexiconL0RunPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "lexicon_compaction_build": {
      const res = buildLexiconCompactionPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "secondary_l0_build": {
      const res = buildSecondaryL0RunPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "secondary_compaction_build": {
      const res = buildSecondaryCompactionPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "companion_build": {
      const res = buildEncodedBundledCompanionPayloadResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "companion_merge_build": {
      const res = buildCompanionMergeResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
    case "search_segment_build": {
      const res = buildSearchSegmentResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
  }
}
