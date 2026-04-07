import type { Config } from "./config";
import { createAppCore, type App } from "./app_core";
import type { ObjectStore } from "./objectstore/interface";
import { AccountingObjectStore } from "./objectstore/accounting";
import { MockR2Store } from "./objectstore/mock_r2";
import { StreamReader } from "./reader";
import { SegmentDiskCache } from "./segment/cache";
import { Segmenter, type SegmenterHooks } from "./segment/segmenter";
import { SegmenterWorkerPool } from "./segment/segmenter_workers";
import { Uploader } from "./uploader";
import { retryAbortable } from "./util/retry";
import { schemaObjectKey, streamHash16Hex } from "./util/stream_paths";
import type { StatsCollector } from "./stats";
import { IndexManager, type StreamIndexLookup } from "./index/indexer";
import { SecondaryIndexManager } from "./index/secondary_indexer";
import type { SchemaRegistry } from "./schema/registry";
import { SearchCompanionManager } from "./search/companion_manager";
import { LexiconIndexManager } from "./index/lexicon_indexer";
import { readSqliteRuntimeMemoryStats } from "./sqlite/runtime_stats";
import { sumRuntimeMemoryValues } from "./runtime_memory";
import { IndexSegmentLocalityManager } from "./index/segment_locality";
import { IndexBuildWorkerPool } from "./index/index_build_worker_pool";
import { GlobalIndexManager } from "./index/global_index_manager";

export type { App } from "./app_core";

export type CreateAppOptions = {
  stats?: StatsCollector;
};

export function createApp(cfg: Config, os?: ObjectStore, opts: CreateAppOptions = {}): App {
  return createAppCore(cfg, {
    stats: opts.stats,
    createRuntime: ({ config, db, ingest, registry, notifier, stats, backpressure, metrics, memorySampler, memory, asyncIndexGate, foregroundActivity }) => {
      const rawStore = os ?? new MockR2Store();
      const store = new AccountingObjectStore(rawStore, db);
      const segmenterHooks: SegmenterHooks = {
        onSegmentSealed: (stream, payloadBytes, segmentBytes) => {
          if (stats) stats.recordSegmentSealed(payloadBytes, segmentBytes);
          if (backpressure) backpressure.adjustOnSeal(payloadBytes, segmentBytes);
          notifier.notifyDetailsChanged(stream);
        },
      };
      const diskCache = new SegmentDiskCache(`${config.rootDir}/cache`, config.segmentCacheMaxBytes);
      const uploader = new Uploader(config, db, store, diskCache, stats, backpressure, undefined, memorySampler);
      const indexSegmentLocality = new IndexSegmentLocalityManager(diskCache, store, backpressure);
      const indexBuildWorkers = new IndexBuildWorkerPool(Math.max(1, config.indexBuilders));
      const routingIndexer = new IndexManager(
        config,
        db,
        store,
        diskCache,
        (stream) => uploader.publishManifest(stream),
        metrics,
        (stream) => notifier.notifyDetailsChanged(stream),
        memorySampler,
        registry,
        asyncIndexGate,
        foregroundActivity,
        indexSegmentLocality,
        indexBuildWorkers
      );
      const secondaryIndexer = new SecondaryIndexManager(
        config,
        db,
        store,
        registry,
        diskCache,
        (stream) => uploader.publishManifest(stream),
        (stream) => notifier.notifyDetailsChanged(stream),
        memorySampler,
        asyncIndexGate,
        foregroundActivity,
        indexSegmentLocality,
        indexBuildWorkers
      );
      const companionIndexer = new SearchCompanionManager(
        config,
        db,
        store,
        registry,
        diskCache,
        (stream) => uploader.publishManifest(stream),
        (stream) => notifier.notifyDetailsChanged(stream),
        metrics,
        memorySampler,
        asyncIndexGate,
        foregroundActivity,
        indexSegmentLocality,
        indexBuildWorkers
      );
      const lexiconIndexer = new LexiconIndexManager(
        config,
        db,
        store,
        diskCache,
        (stream) => uploader.publishManifest(stream),
        (stream) => notifier.notifyDetailsChanged(stream),
        metrics,
        registry,
        asyncIndexGate,
        foregroundActivity,
        indexSegmentLocality,
        indexBuildWorkers
      );
      const indexer = new GlobalIndexManager(
        config,
        routingIndexer,
        secondaryIndexer,
        companionIndexer,
        lexiconIndexer,
        indexBuildWorkers
      );
      uploader.setHooks({
        onSegmentsUploaded: (stream) => indexer.enqueue(stream),
        onMetadataChanged: (stream) => notifier.notifyDetailsChanged(stream),
      });
      const reader = new StreamReader(config, db, store, registry, diskCache, indexer, memorySampler, memory);
      const segmenter =
        config.segmenterWorkers > 0
          ? new SegmenterWorkerPool(config, config.segmenterWorkers, {}, segmenterHooks)
          : new Segmenter(config, db, {}, segmenterHooks, memorySampler);

      return {
        store,
        reader,
        segmenter,
        uploader,
        indexer,
        segmentDiskCache: diskCache,
        uploadSchemaRegistry: async (stream: string, reg: SchemaRegistry): Promise<void> => {
          const shash = streamHash16Hex(stream);
          const key = schemaObjectKey(shash);
          const body = new TextEncoder().encode(JSON.stringify(reg));
          await retryAbortable(
            (signal) => store.put(key, body, { contentType: "application/json", contentLength: body.byteLength, signal }),
            {
              retries: config.objectStoreRetries,
              baseDelayMs: config.objectStoreBaseDelayMs,
              maxDelayMs: config.objectStoreMaxDelayMs,
              timeoutMs: config.objectStoreTimeoutMs,
            }
          );
          db.setSchemaUploadedSizeBytes(stream, body.byteLength);
        },
        getLocalStorageUsage: (stream: string) => ({
          segment_cache_bytes: diskCache.bytesForObjectKeyPrefix(`streams/${streamHash16Hex(stream)}/segments/`),
          ...indexer.getLocalStorageUsage?.(stream),
        }),
        getRuntimeMemorySnapshot: () => {
          const ingestMemory = ingest.getMemoryStats();
          const segmenterMemory = segmenter.getMemoryStats?.() ?? {
            active_builds: 0,
            active_streams: 0,
            active_payload_bytes: 0,
            active_segment_bytes_estimate: 0,
            active_rows: 0,
          };
          const uploaderMemory = uploader.getMemoryStats?.() ?? {
            inflight_segments: 0,
            inflight_segment_bytes: 0,
            manifest_inflight_streams: 0,
          };
          const routingIndexMemory = routingIndexer.getMemoryStats();
          const secondaryIndexMemory = secondaryIndexer.getMemoryStats();
          const companionMemory = companionIndexer.getMemoryStats();
          const lexiconMemory = lexiconIndexer.getMemoryStats();
          const segmentDiskStats = diskCache.stats();
          const mockR2InMemoryBytes = rawStore instanceof MockR2Store ? rawStore.memoryBytes() : 0;
          const mockR2ObjectCount = rawStore instanceof MockR2Store ? rawStore.size() : 0;
          const sqliteRuntime = readSqliteRuntimeMemoryStats();
          const heapEstimates = {
            ingest_queue_payload_bytes: ingestMemory.queuedPayloadBytes,
            routing_run_cache_bytes: routingIndexMemory.runCacheBytes,
            exact_run_cache_bytes: secondaryIndexMemory.runCacheBytes,
            mock_r2_in_memory_bytes: mockR2InMemoryBytes,
          };
          const mappedFiles = {
            segment_cache_mapped_bytes: segmentDiskStats.mappedBytes,
            routing_run_disk_cache_mapped_bytes: routingIndexMemory.runDiskMappedBytes,
            exact_run_disk_cache_mapped_bytes: secondaryIndexMemory.runDiskMappedBytes,
            lexicon_index_mapped_bytes: lexiconMemory.mappedFileBytes,
            companion_bundle_mapped_bytes: companionMemory.mappedFileBytes,
          };
          const diskCaches = {
            segment_disk_cache_bytes: segmentDiskStats.usedBytes,
            segment_required_for_indexing_bytes: segmentDiskStats.requiredBytes,
            routing_run_disk_cache_bytes: routingIndexMemory.runDiskCacheBytes,
            exact_run_disk_cache_bytes: secondaryIndexMemory.runDiskCacheBytes,
            lexicon_disk_cache_bytes: lexiconMemory.fileCacheBytes,
            companion_disk_cache_bytes: companionMemory.fileCacheBytes,
          };
          const pipelineBuffers = {
            segmenter_active_payload_bytes: segmenterMemory.active_payload_bytes,
            segmenter_active_segment_bytes_estimate: segmenterMemory.active_segment_bytes_estimate,
            uploader_inflight_segment_bytes: uploaderMemory.inflight_segment_bytes,
          };
          const sqliteRuntimeBytes = {
            sqlite_memory_used_bytes: sqliteRuntime.memory_used_bytes,
            sqlite_memory_highwater_bytes: sqliteRuntime.memory_highwater_bytes,
            sqlite_pagecache_overflow_bytes: sqliteRuntime.pagecache_overflow_bytes,
            sqlite_pagecache_overflow_highwater_bytes: sqliteRuntime.pagecache_overflow_highwater_bytes,
          };
          const configuredBudgets = {
            sqlite_cache_budget_bytes: config.sqliteCacheBytes,
            worker_sqlite_cache_budget_bytes: config.workerSqliteCacheBytes,
            segment_cache_budget_bytes: config.segmentCacheMaxBytes,
            routing_run_cache_budget_bytes: config.indexRunMemoryCacheBytes,
            routing_run_disk_cache_budget_bytes: config.indexRunCacheMaxBytes,
            exact_run_cache_budget_bytes: config.indexRunMemoryCacheBytes,
            exact_run_disk_cache_budget_bytes: config.indexRunCacheMaxBytes,
            lexicon_disk_cache_budget_bytes: config.lexiconIndexCacheMaxBytes,
            companion_disk_cache_budget_bytes: config.searchCompanionFileCacheMaxBytes,
          };
          const counts = {
            ingest_queue_requests: ingestMemory.queuedRequests,
            segment_disk_cache_entries: segmentDiskStats.entryCount,
            segment_mapped_files: segmentDiskStats.mappedEntryCount,
            segment_pinned_files: segmentDiskStats.pinnedEntryCount,
            segment_required_for_indexing_files: segmentDiskStats.requiredEntryCount,
            routing_run_cache_entries: routingIndexMemory.runCacheEntries,
            routing_run_disk_cache_entries: routingIndexMemory.runDiskCacheEntries,
            routing_run_disk_cache_mapped_entries: routingIndexMemory.runDiskMappedEntries,
            routing_run_disk_cache_pinned_entries: routingIndexMemory.runDiskPinnedEntries,
            exact_run_cache_entries: secondaryIndexMemory.runCacheEntries,
            exact_run_disk_cache_entries: secondaryIndexMemory.runDiskCacheEntries,
            exact_run_disk_cache_mapped_entries: secondaryIndexMemory.runDiskMappedEntries,
            exact_run_disk_cache_pinned_entries: secondaryIndexMemory.runDiskPinnedEntries,
            lexicon_cached_files: lexiconMemory.fileCacheEntries,
            lexicon_mapped_files: lexiconMemory.mappedFileEntries,
            lexicon_pinned_files: lexiconMemory.pinnedFileEntries,
            companion_cached_files: companionMemory.fileCacheEntries,
            companion_mapped_files: companionMemory.mappedFileEntries,
            companion_pinned_files: companionMemory.pinnedFileEntries,
            mock_r2_object_count: mockR2ObjectCount,
            mock_r2_in_memory_bytes: mockR2InMemoryBytes,
            pending_upload_segments: uploader.countSegmentsWaiting(),
            uploader_inflight_segments: uploaderMemory.inflight_segments,
            uploader_manifest_inflight_streams: uploaderMemory.manifest_inflight_streams,
            segmenter_active_builds: segmenterMemory.active_builds,
            segmenter_active_streams: segmenterMemory.active_streams,
            segmenter_active_rows: segmenterMemory.active_rows,
            sqlite_pagecache_used_slots: sqliteRuntime.pagecache_used_slots,
            sqlite_pagecache_used_slots_highwater: sqliteRuntime.pagecache_used_slots_highwater,
            sqlite_malloc_count: sqliteRuntime.malloc_count,
            sqlite_malloc_count_highwater: sqliteRuntime.malloc_count_highwater,
            sqlite_open_connections: sqliteRuntime.open_connections,
            sqlite_prepared_statements: sqliteRuntime.prepared_statements,
          };
          return {
            subsystems: {
              heap_estimates: heapEstimates,
              mapped_files: mappedFiles,
              disk_caches: diskCaches,
              configured_budgets: configuredBudgets,
              pipeline_buffers: pipelineBuffers,
              sqlite_runtime: sqliteRuntimeBytes,
              counts,
            },
            totals: {
              heap_estimate_bytes: sumRuntimeMemoryValues(heapEstimates),
              mapped_file_bytes: sumRuntimeMemoryValues(mappedFiles),
              disk_cache_bytes: sumRuntimeMemoryValues(diskCaches),
              configured_budget_bytes: sumRuntimeMemoryValues(configuredBudgets),
              pipeline_buffer_bytes: sumRuntimeMemoryValues(pipelineBuffers),
              sqlite_runtime_bytes: sumRuntimeMemoryValues(sqliteRuntimeBytes),
            },
          };
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
