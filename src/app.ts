import type { Config } from "./config";
import { createAppCore, type App } from "./app_core";
import type { ObjectStore } from "./objectstore/interface";
import { MockR2Store } from "./objectstore/mock_r2";
import { StreamReader } from "./reader";
import { SegmentDiskCache } from "./segment/cache";
import { Segmenter, type SegmenterHooks } from "./segment/segmenter";
import { SegmenterWorkerPool } from "./segment/segmenter_workers";
import { Uploader } from "./uploader";
import { retry } from "./util/retry";
import { schemaObjectKey, streamHash16Hex } from "./util/stream_paths";
import type { StatsCollector } from "./stats";
import { IndexManager, type StreamIndexLookup } from "./index/indexer";
import { SecondaryIndexManager } from "./index/secondary_indexer";
import type { SchemaRegistry } from "./schema/registry";
import { SearchCompanionManager } from "./search/companion_manager";

export type { App } from "./app_core";

export type CreateAppOptions = {
  stats?: StatsCollector;
};

class CombinedIndexController implements StreamIndexLookup {
  constructor(
    private readonly routingIndex: IndexManager,
    private readonly secondaryIndex: SecondaryIndexManager,
    private readonly companionIndex: SearchCompanionManager
  ) {}

  start(): void {
    this.routingIndex.start();
    this.secondaryIndex.start();
    this.companionIndex.start();
  }

  stop(): void {
    this.routingIndex.stop();
    this.secondaryIndex.stop();
    this.companionIndex.stop();
  }

  enqueue(stream: string): void {
    this.routingIndex.enqueue(stream);
    this.secondaryIndex.enqueue(stream);
    this.companionIndex.enqueue(stream);
  }

  candidateSegmentsForRoutingKey(stream: string, keyBytes: Uint8Array) {
    return this.routingIndex.candidateSegmentsForRoutingKey(stream, keyBytes);
  }

  candidateSegmentsForSecondaryIndex(stream: string, indexName: string, keyBytes: Uint8Array) {
    return this.secondaryIndex.candidateSegmentsForSecondaryIndex(stream, indexName, keyBytes);
  }

  getAggSegmentCompanion(stream: string, segmentIndex: number) {
    return this.companionIndex.getAggSegmentCompanion(stream, segmentIndex);
  }

  getColSegmentCompanion(stream: string, segmentIndex: number) {
    return this.companionIndex.getColSegmentCompanion(stream, segmentIndex);
  }

  getFtsSegmentCompanion(stream: string, segmentIndex: number) {
    return this.companionIndex.getFtsSegmentCompanion(stream, segmentIndex);
  }

  getMetricsBlockSegmentCompanion(stream: string, segmentIndex: number) {
    return this.companionIndex.getMetricsBlockSegmentCompanion(stream, segmentIndex);
  }
}

export function createApp(cfg: Config, os?: ObjectStore, opts: CreateAppOptions = {}): App {
  return createAppCore(cfg, {
    stats: opts.stats,
    createRuntime: ({ config, db, registry, notifier, stats, backpressure, metrics }) => {
      const store = os ?? new MockR2Store();
      const segmenterHooks: SegmenterHooks = {
        onSegmentSealed: (stream, payloadBytes, segmentBytes) => {
          if (stats) stats.recordSegmentSealed(payloadBytes, segmentBytes);
          if (backpressure) backpressure.adjustOnSeal(payloadBytes, segmentBytes);
          notifier.notifyDetailsChanged(stream);
        },
      };
      const diskCache = new SegmentDiskCache(`${config.rootDir}/cache`, config.segmentCacheMaxBytes);
      const uploader = new Uploader(config, db, store, diskCache, stats, backpressure);
      const routingIndexer = new IndexManager(
        config,
        db,
        store,
        diskCache,
        (stream) => uploader.publishManifest(stream),
        metrics,
        (stream) => notifier.notifyDetailsChanged(stream)
      );
      const secondaryIndexer = new SecondaryIndexManager(
        config,
        db,
        store,
        registry,
        (stream) => uploader.publishManifest(stream),
        (stream) => notifier.notifyDetailsChanged(stream)
      );
      const companionIndexer = new SearchCompanionManager(
        config,
        db,
        store,
        registry,
        (stream) => uploader.publishManifest(stream),
        (stream) => notifier.notifyDetailsChanged(stream)
      );
      const indexer = new CombinedIndexController(
        routingIndexer,
        secondaryIndexer,
        companionIndexer
      );
      uploader.setHooks({
        onSegmentsUploaded: (stream) => indexer.enqueue(stream),
        onMetadataChanged: (stream) => notifier.notifyDetailsChanged(stream),
      });
      const reader = new StreamReader(config, db, store, registry, diskCache, indexer);
      const segmenter =
        config.segmenterWorkers > 0
          ? new SegmenterWorkerPool(config, config.segmenterWorkers, {}, segmenterHooks)
          : new Segmenter(config, db, {}, segmenterHooks);

      return {
        store,
        reader,
        segmenter,
        uploader,
        indexer,
        uploadSchemaRegistry: async (stream: string, reg: SchemaRegistry): Promise<void> => {
          const shash = streamHash16Hex(stream);
          const key = schemaObjectKey(shash);
          const body = new TextEncoder().encode(JSON.stringify(reg));
          await retry(
            () => store.put(key, body, { contentType: "application/json", contentLength: body.byteLength }),
            {
              retries: config.objectStoreRetries,
              baseDelayMs: config.objectStoreBaseDelayMs,
              maxDelayMs: config.objectStoreMaxDelayMs,
              timeoutMs: config.objectStoreTimeoutMs,
            }
          );
        },
        start: () => {
          segmenter.start();
          uploader.start();
          indexer.start();
          setTimeout(() => {
            try {
              let offset = 0;
              const pageSize = 1000;
              for (;;) {
                const streams = db.listStreams(pageSize, offset);
                for (const row of streams) indexer.enqueue(row.stream);
                if (streams.length < pageSize) break;
                offset += streams.length;
              }
            } catch {
              // App may have been closed before the startup catch-up kickoff ran.
            }
          }, 0);
        },
      };
    },
  });
}
