import { parentPort } from "node:worker_threads";
import { Result } from "better-result";
import { buildRoutingL0RunPayloadResult } from "./routing_l0_build";
import { buildRoutingCompactionPayloadResult } from "./routing_compaction_build";
import { buildRoutingLexiconL0RunPayloadResult } from "./routing_lexicon_l0_build";
import { buildLexiconCompactionPayloadResult } from "./lexicon_compaction_build";
import { buildSecondaryL0RunPayloadResult } from "./secondary_l0_build";
import { buildSecondaryCompactionPayloadResult } from "./secondary_compaction_build";
import { buildEncodedBundledCompanionPayloadResult } from "../search/companion_build";
import { buildSearchSegmentResult } from "../search/search_segment_build";
import type { IndexBuildJobInput, IndexBuildJobOutput } from "./index_build_job";
import { dsError } from "../util/ds_error";

type Message =
  | { type: "build"; id: number; job: IndexBuildJobInput }
  | { type: "stop" };

if (!parentPort) {
  throw dsError("index build worker requires parentPort");
}

function buildResult(job: IndexBuildJobInput): Result<IndexBuildJobOutput, { message: string }> {
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
    case "search_segment_build": {
      const res = buildSearchSegmentResult(job.input);
      return Result.isError(res) ? Result.err({ message: res.error.message }) : Result.ok({ kind: job.kind, output: res.value });
    }
  }
}

parentPort.on("message", (msg: Message) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "stop") {
    process.exit(0);
    return;
  }
  const res = buildResult(msg.job);
  if (Result.isError(res)) {
    parentPort!.postMessage({ type: "error", id: msg.id, message: res.error.message });
    return;
  }
  parentPort!.postMessage({ type: "result", id: msg.id, result: res.value });
});
