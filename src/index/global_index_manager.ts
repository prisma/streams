import type { Config } from "../config";
import type { AggSectionView } from "../search/agg_format";
import type { ColSectionView } from "../search/col_format";
import type { FtsSectionView } from "../search/fts_format";
import type { MetricsBlockSectionView } from "../profiles/metrics/block_format";
import type { RoutingKeyLexiconListResult } from "./lexicon_indexer";
import type { CompanionSectionLookupStats, StreamIndexLookup } from "./indexer";
import { IndexBuildWorkerPool } from "./index_build_worker_pool";
import { IndexManager } from "./indexer";
import { LexiconIndexManager } from "./lexicon_indexer";
import { SecondaryIndexManager } from "./secondary_indexer";
import { SearchCompanionManager } from "../search/companion_manager";
import { yieldToEventLoop } from "../util/yield";

export class GlobalIndexManager implements StreamIndexLookup {
  private readonly queue = new Set<string>();
  private timer: any | null = null;
  private active = false;
  private running = false;
  private scheduled = false;
  private roundRobinCursor = 0;

  constructor(
    private readonly cfg: Config,
    readonly routingIndex: IndexManager,
    readonly secondaryIndex: SecondaryIndexManager,
    readonly companionIndex: SearchCompanionManager,
    readonly lexiconIndex: LexiconIndexManager,
    private readonly buildWorkers: IndexBuildWorkerPool
  ) {}

  start(): void {
    this.active = true;
    this.buildWorkers.start();
    this.routingIndex.start();
    this.secondaryIndex.start();
    this.companionIndex.start();
    this.lexiconIndex.start();
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.cfg.indexCheckIntervalMs);
    this.scheduleTick();
  }

  stop(): void {
    this.active = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.scheduled = false;
    this.routingIndex.stop();
    this.secondaryIndex.stop();
    this.companionIndex.stop();
    this.lexiconIndex.stop();
    this.buildWorkers.stop();
  }

  enqueue(stream: string): void {
    this.queue.add(stream);
    this.scheduleTick();
  }

  async tick(): Promise<void> {
    if (!this.active) return;
    if (this.running) return;
    this.running = true;
    try {
      while (this.active) {
        const streams = Array.from(this.queue);
        this.queue.clear();
        for (const stream of streams) {
          this.routingIndex.enqueue(stream);
          this.secondaryIndex.enqueue(stream);
          this.companionIndex.enqueue(stream);
          this.lexiconIndex.enqueue(stream);
        }
        const workKinds: Array<() => Promise<boolean>> = [
          () => this.routingIndex.runOneBuildTask(),
          () => this.lexiconIndex.runOneBuildTask(),
          () => this.secondaryIndex.runOneBuildTask(),
          () => this.companionIndex.runOneBuildTask(),
          () => this.routingIndex.runOneCompactionTask(),
          () => this.lexiconIndex.runOneCompactionTask(),
          () => this.secondaryIndex.runOneCompactionTask(),
        ];
        const ordered = workKinds.map((_, index) => workKinds[(index + this.roundRobinCursor) % workKinds.length]!);
        this.roundRobinCursor = (this.roundRobinCursor + 1) % workKinds.length;
        const progressed = (await Promise.all(ordered.map((run) => run()))).some(Boolean);
        if (!progressed && this.queue.size === 0) break;
        await yieldToEventLoop();
      }
    } finally {
      this.running = false;
      if (this.active && this.queue.size > 0) this.scheduleTick();
    }
  }

  private scheduleTick(): void {
    if (!this.active) return;
    if (this.running || this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      void this.tick();
    });
  }

  candidateSegmentsForRoutingKey(stream: string, keyBytes: Uint8Array) {
    return this.routingIndex.candidateSegmentsForRoutingKey(stream, keyBytes);
  }

  candidateSegmentsForSecondaryIndex(stream: string, indexName: string, keyBytes: Uint8Array) {
    return this.secondaryIndex.candidateSegmentsForSecondaryIndex(stream, indexName, keyBytes);
  }

  createSecondaryIndexTailMatcher(stream: string, indexName: string, keyBytes: Uint8Array) {
    return this.secondaryIndex.createSecondaryIndexTailMatcher(stream, indexName, keyBytes);
  }

  getAggSegmentCompanion(stream: string, segmentIndex: number): Promise<AggSectionView | null> {
    return this.companionIndex.getAggSegmentCompanion(stream, segmentIndex);
  }

  getColSegmentCompanion(stream: string, segmentIndex: number): Promise<ColSectionView | null> {
    return this.companionIndex.getColSegmentCompanion(stream, segmentIndex);
  }

  getFtsSegmentCompanion(stream: string, segmentIndex: number): Promise<FtsSectionView | null> {
    return this.companionIndex.getFtsSegmentCompanion(stream, segmentIndex);
  }

  getFtsSegmentCompanionWithStats(
    stream: string,
    segmentIndex: number
  ): Promise<{ companion: FtsSectionView | null; stats: CompanionSectionLookupStats }> {
    return this.companionIndex.getFtsSegmentCompanionWithStats(stream, segmentIndex);
  }

  getMetricsBlockSegmentCompanion(stream: string, segmentIndex: number): Promise<MetricsBlockSectionView | null> {
    return this.companionIndex.getMetricsBlockSegmentCompanion(stream, segmentIndex);
  }

  listRoutingKeysResult(stream: string, after: string | null, limit: number): Promise<import("better-result").Result<RoutingKeyLexiconListResult, { kind: string; message: string }>> {
    return this.lexiconIndex.listRoutingKeysResult(stream, after, limit);
  }

  getLocalStorageUsage(stream: string) {
    return {
      routing_index_cache_bytes: this.routingIndex.getLocalCacheBytes(stream),
      exact_index_cache_bytes: this.secondaryIndex.getLocalCacheBytes(stream),
      companion_cache_bytes: this.companionIndex.getLocalCacheBytes(stream),
      lexicon_index_cache_bytes: this.lexiconIndex.getLocalCacheBytes(stream),
    };
  }
}
