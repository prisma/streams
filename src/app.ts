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
import { IndexManager } from "./index/indexer";
import type { SchemaRegistry } from "./schema/registry";

export type { App } from "./app_core";

export type CreateAppOptions = {
  stats?: StatsCollector;
};

export function createApp(cfg: Config, os?: ObjectStore, opts: CreateAppOptions = {}): App {
  return createAppCore(cfg, {
    stats: opts.stats,
    createRuntime: ({ config, db, stats, backpressure, metrics }) => {
      const store = os ?? new MockR2Store();
      const segmenterHooks: SegmenterHooks | undefined =
        stats || backpressure
          ? {
              onSegmentSealed: (payloadBytes, segmentBytes) => {
                if (stats) stats.recordSegmentSealed(payloadBytes, segmentBytes);
                if (backpressure) backpressure.adjustOnSeal(payloadBytes, segmentBytes);
              },
            }
          : undefined;
      const diskCache = new SegmentDiskCache(`${config.rootDir}/cache`, config.segmentCacheMaxBytes);
      const uploader = new Uploader(config, db, store, diskCache, stats, backpressure);
      const indexer = new IndexManager(config, db, store, diskCache, (stream) => uploader.publishManifest(stream), metrics);
      uploader.setHooks({ onSegmentsUploaded: (stream) => indexer.enqueue(stream) });
      const reader = new StreamReader(config, db, store, diskCache, indexer);
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
        },
      };
    },
  });
}
