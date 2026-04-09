import { copyFileSync } from "node:fs";
import { dirname, extname, join, basename } from "node:path";
import { Result } from "better-result";
import type { IndexBuildWorkerError } from "../index/index_build_job";
import type { IndexBuildJobTelemetry } from "../index/index_build_telemetry";
import { IndexBuildWorkerPool } from "../index/index_build_worker_pool";
import type { SearchSegmentBuildInput, SearchSegmentBuildOutput } from "./search_segment_build";

export type SearchSegmentSharedBuildResult = {
  output: SearchSegmentBuildOutput;
  cacheStatus: "miss" | "shared_inflight";
  jobTelemetry: IndexBuildJobTelemetry | null;
};

export class SearchSegmentBuildCoordinator {
  private readonly inflight = new Map<
    string,
    Promise<Result<{ output: SearchSegmentBuildOutput; jobTelemetry: IndexBuildJobTelemetry | null }, IndexBuildWorkerError>>
  >();

  constructor(private readonly workers: IndexBuildWorkerPool) {}

  async buildSegmentResult(
    input: SearchSegmentBuildInput
  ): Promise<Result<SearchSegmentSharedBuildResult, IndexBuildWorkerError>> {
    const key = this.cacheKey(input);
    const inflight = this.inflight.get(key);
    if (inflight) {
      const joined = await inflight;
      if (Result.isError(joined)) return joined;
      return Result.ok({
        output: this.cloneSharedOutput(joined.value.output),
        cacheStatus: "shared_inflight",
        jobTelemetry: joined.value.jobTelemetry,
      });
    }
    const promise = this.buildFreshResult(input);
    this.inflight.set(key, promise);
    try {
      const built = await promise;
      if (Result.isError(built)) return built;
      return Result.ok({
        output: built.value.output,
        cacheStatus: "miss",
        jobTelemetry: built.value.jobTelemetry,
      });
    } finally {
      this.inflight.delete(key);
    }
  }

  private async buildFreshResult(
    input: SearchSegmentBuildInput
  ): Promise<Result<{ output: SearchSegmentBuildOutput; jobTelemetry: IndexBuildJobTelemetry | null }, IndexBuildWorkerError>> {
    const res = await this.workers.buildResult({
      kind: "search_segment_build",
      input,
    });
    if (Result.isError(res)) return res;
    if (res.value.output.kind !== "search_segment_build") {
      return Result.err({ kind: "worker_pool_failure", message: "unexpected worker result kind" });
    }
    return Result.ok({
      output: res.value.output.output,
      jobTelemetry: res.value.telemetry,
    });
  }

  private cacheKey(input: SearchSegmentBuildInput): string {
    return JSON.stringify({
      stream: input.stream,
      segment_index: input.segment.segmentIndex,
      start_offset: input.segment.startOffset.toString(),
      local_path: input.segment.localPath,
      output_dir: input.outputDir ?? null,
      include_companion: input.includeCompanion !== false,
      plan_hash: input.plan ? JSON.stringify(input.plan.summary) : null,
      exact_indexes: input.exactIndexes
        .map((entry) => ({
          name: entry.index.name,
          config: entry.index.config,
          secret: Buffer.from(entry.secret).toString("hex"),
        }))
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)),
    });
  }

  private cloneSharedOutput(output: SearchSegmentBuildOutput): SearchSegmentBuildOutput {
    return {
      exactRuns: output.exactRuns.map((run) =>
        run.storage === "file"
          ? {
              ...run,
              localPath: cloneTempFile(run.localPath),
            }
          : run
      ),
      companion:
        output.companion == null
          ? null
          : output.companion.storage === "file"
            ? {
                ...output.companion,
                localPath: cloneTempFile(output.companion.localPath),
              }
            : output.companion,
    };
  }
}

function cloneTempFile(localPath: string): string {
  const ext = extname(localPath);
  const dir = dirname(localPath);
  const nonce = `${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}`;
  const base = basename(localPath, ext);
  const clonePath = join(dir, `${base}-${nonce}${ext || ".tmp"}`);
  copyFileSync(localPath, clonePath);
  return clonePath;
}
