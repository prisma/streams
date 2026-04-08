import { Result } from "better-result";
import type { IndexBuildWorkerError } from "../index/index_build_job";
import { IndexBuildWorkerPool } from "../index/index_build_worker_pool";
import type { SearchSegmentBuildInput, SearchSegmentBuildOutput } from "./search_segment_build";

export type SearchSegmentSharedBuildResult = {
  output: SearchSegmentBuildOutput;
  cacheStatus: "miss" | "shared_inflight" | "cache_hit";
};

type CacheEntry = {
  result: SearchSegmentBuildOutput;
  expiresAt: number;
};

export class SearchSegmentBuildCoordinator {
  private readonly inflight = new Map<string, Promise<Result<SearchSegmentBuildOutput, IndexBuildWorkerError>>>();
  private readonly completed = new Map<string, CacheEntry>();

  constructor(
    private readonly workers: IndexBuildWorkerPool,
    private readonly cacheTtlMs = 30_000,
    private readonly maxCompletedEntries = 8
  ) {}

  async buildSegmentResult(
    input: SearchSegmentBuildInput
  ): Promise<Result<SearchSegmentSharedBuildResult, IndexBuildWorkerError>> {
    this.pruneExpired();
    const key = this.cacheKey(input);
    const cached = this.completed.get(key);
    if (cached) {
      return Result.ok({
        output: cached.result,
        cacheStatus: "cache_hit",
      });
    }
    const inflight = this.inflight.get(key);
    if (inflight) {
      const joined = await inflight;
      if (Result.isError(joined)) return joined;
      return Result.ok({
        output: joined.value,
        cacheStatus: "shared_inflight",
      });
    }
    const promise = this.buildFreshResult(input);
    this.inflight.set(key, promise);
    try {
      const built = await promise;
      if (Result.isError(built)) return built;
      this.completed.set(key, { result: built.value, expiresAt: Date.now() + this.cacheTtlMs });
      this.pruneCompleted();
      return Result.ok({
        output: built.value,
        cacheStatus: "miss",
      });
    } finally {
      this.inflight.delete(key);
    }
  }

  private async buildFreshResult(input: SearchSegmentBuildInput): Promise<Result<SearchSegmentBuildOutput, IndexBuildWorkerError>> {
    const res = await this.workers.buildResult({
      kind: "search_segment_build",
      input,
    });
    if (Result.isError(res)) return res;
    if (res.value.kind !== "search_segment_build") {
      return Result.err({ kind: "worker_pool_failure", message: "unexpected worker result kind" });
    }
    return Result.ok(res.value.output);
  }

  private cacheKey(input: SearchSegmentBuildInput): string {
    return JSON.stringify({
      stream: input.stream,
      segment_index: input.segment.segmentIndex,
      start_offset: input.segment.startOffset.toString(),
      local_path: input.segment.localPath,
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

  private pruneExpired(): void {
    const now = Date.now();
    for (const [key, entry] of this.completed.entries()) {
      if (entry.expiresAt <= now) this.completed.delete(key);
    }
  }

  private pruneCompleted(): void {
    this.pruneExpired();
    while (this.completed.size > this.maxCompletedEntries) {
      const oldest = this.completed.keys().next();
      if (oldest.done) return;
      this.completed.delete(oldest.value);
    }
  }
}
