import { Result } from "better-result";
import type { IndexBuildWorkerError } from "./index_build_job";
import { IndexBuildWorkerPool } from "./index_build_worker_pool";
import type { RoutingLexiconL0BuildInput, RoutingLexiconL0BuildOutput } from "./routing_lexicon_l0_build";

export type RoutingLexiconSharedBuildResult = {
  output: RoutingLexiconL0BuildOutput;
  cacheStatus: "miss" | "shared_inflight" | "cache_hit";
};

type CacheEntry = {
  result: RoutingLexiconL0BuildOutput;
  expiresAt: number;
};

export class RoutingLexiconL0BuildCoordinator {
  private readonly inflight = new Map<string, Promise<Result<RoutingLexiconL0BuildOutput, IndexBuildWorkerError>>>();
  private readonly completed = new Map<string, CacheEntry>();

  constructor(
    private readonly workers: IndexBuildWorkerPool,
    private readonly cacheTtlMs = 30_000,
    private readonly maxCompletedEntries = 32
  ) {}

  async buildWindowResult(
    input: RoutingLexiconL0BuildInput
  ): Promise<Result<RoutingLexiconSharedBuildResult, IndexBuildWorkerError>> {
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
      this.completed.set(key, {
        result: built.value,
        expiresAt: Date.now() + this.cacheTtlMs,
      });
      this.pruneCompleted();
      return Result.ok({
        output: built.value,
        cacheStatus: "miss",
      });
    } finally {
      this.inflight.delete(key);
    }
  }

  private async buildFreshResult(
    input: RoutingLexiconL0BuildInput
  ): Promise<Result<RoutingLexiconL0BuildOutput, IndexBuildWorkerError>> {
    const res = await this.workers.buildResult({
      kind: "routing_lexicon_l0_build",
      input,
    });
    if (Result.isError(res)) return res;
    if (res.value.kind !== "routing_lexicon_l0_build") {
      return Result.err({ kind: "worker_pool_failure", message: "unexpected worker result kind" });
    }
    return Result.ok(res.value.output);
  }

  private cacheKey(input: RoutingLexiconL0BuildInput): string {
    return JSON.stringify({
      stream: input.stream,
      source_kind: input.sourceKind,
      source_name: input.sourceName,
      cache_token: input.cacheToken ?? "",
      start_segment: input.startSegment,
      end_segment: input.startSegment + input.span - 1,
      secret: Buffer.from(input.secret).toString("hex"),
      segments: input.segments.map((segment) => ({
        segment_index: segment.segmentIndex,
        local_path: segment.localPath,
      })),
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
