import type { Config } from "./config";
import { createAppCore, type App } from "./app_core";
import type { ObjectStore } from "./objectstore/interface";
import { NullObjectStore } from "./objectstore/null";
import { StreamReader } from "./reader";
import type { StreamIndexLookup } from "./index/indexer";
import type { RoutingKeyLexiconListResult } from "./index/lexicon_indexer";
import type { StatsCollector } from "./stats";
import type { UploaderController, UploaderHooks } from "./uploader";
import type { SegmenterController } from "./segment/segmenter_workers";
import { readSqliteRuntimeMemoryStats } from "./sqlite/runtime_stats";
import { Result } from "better-result";

const TEXT_DECODER = new TextDecoder();

class NoopUploader implements UploaderController {
  start(): void {}
  stop(_hard?: boolean): void {}
  countSegmentsWaiting(): number {
    return 0;
  }
  setHooks(_hooks: UploaderHooks | undefined): void {}
  async publishManifest(_stream: string): Promise<void> {}
}

const noopSegmenter: SegmenterController = {
  start(): void {},
  stop(_hard?: boolean): void {},
};

class LocalIndexLookup implements StreamIndexLookup {
  constructor(private readonly db: App["deps"]["db"]) {}

  start(): void {}

  stop(): void {}

  enqueue(_stream: string): void {}

  async candidateSegmentsForRoutingKey(_stream: string, _keyBytes: Uint8Array): Promise<null> {
    return null;
  }

  async candidateSegmentsForSecondaryIndex(_stream: string, _indexName: string, _keyBytes: Uint8Array): Promise<null> {
    return null;
  }

  async getAggSegmentCompanion(_stream: string, _segmentIndex: number): Promise<null> {
    return null;
  }

  async getColSegmentCompanion(_stream: string, _segmentIndex: number): Promise<null> {
    return null;
  }

  async getExactSegmentCompanion(_stream: string, _segmentIndex: number): Promise<null> {
    return null;
  }

  async getFtsSegmentCompanion(_stream: string, _segmentIndex: number): Promise<null> {
    return null;
  }

  async getMetricsBlockSegmentCompanion(_stream: string, _segmentIndex: number): Promise<null> {
    return null;
  }

  async listRoutingKeysResult(
    stream: string,
    after: string | null,
    limit: number
  ): Promise<Result<RoutingKeyLexiconListResult, { kind: string; message: string }>> {
    const srow = this.db.getStream(stream);
    if (!srow || this.db.isDeleted(srow)) {
      return Result.err({ kind: "invalid_lexicon_index", message: "stream not found" });
    }
    const safeLimit = Math.max(1, Math.min(limit, 500));
    const keys = new Set<string>();
    let scannedWalRows = 0;
    for (const rec of this.db.iterWalRange(stream, 0n, srow.next_offset - 1n)) {
      scannedWalRows += 1;
      const rawKey = rec.routing_key == null ? null : rec.routing_key instanceof Uint8Array ? rec.routing_key : new Uint8Array(rec.routing_key);
      if (!rawKey || rawKey.byteLength === 0) continue;
      keys.add(TEXT_DECODER.decode(rawKey));
    }
    const sorted = Array.from(keys).sort();
    const filtered = after == null ? sorted : sorted.filter((key) => key > after);
    const page = filtered.slice(0, safeLimit);
    const nextAfter = filtered.length > safeLimit ? page[page.length - 1] ?? null : null;
    return Result.ok({
      keys: page,
      nextAfter,
      tookMs: 0,
      coverage: {
        complete: true,
        indexedSegments: 0,
        scannedUploadedSegments: 0,
        scannedLocalSegments: 0,
        scannedWalRows,
        possibleMissingUploadedSegments: 0,
        possibleMissingLocalSegments: 0,
      },
        timing: {
          lexiconRunGetMs: 0,
          lexiconDecodeMs: 0,
          lexiconEnumerateMs: 0,
          lexiconMergeMs: 0,
          fallbackScanMs: 0,
        fallbackSegmentGetMs: 0,
        fallbackWalScanMs: 0,
        lexiconRunsLoaded: 0,
      },
    });
  }

  getLocalStorageUsage(_stream: string) {
    return {
      routing_index_cache_bytes: 0,
      exact_index_cache_bytes: 0,
      companion_cache_bytes: 0,
      lexicon_index_cache_bytes: 0,
    };
  }
}

export type CreateLocalAppOptions = {
  stats?: StatsCollector;
};

export function createLocalApp(cfg: Config, os?: ObjectStore, opts: CreateLocalAppOptions = {}): App {
  return createAppCore(cfg, {
    stats: opts.stats,
    createRuntime: ({ config, db, registry, memorySampler, memory }) => {
      const store = os ?? new NullObjectStore();
      const indexer = new LocalIndexLookup(db);
      const reader = new StreamReader(config, db, store, registry, undefined, indexer, memorySampler, memory);

      return {
        store,
        reader,
        segmenter: noopSegmenter,
        uploader: new NoopUploader(),
        indexer,
        uploadSchemaRegistry: async (): Promise<void> => {},
        getRuntimeMemorySnapshot: () => {
          const sqliteRuntime = readSqliteRuntimeMemoryStats();
          return {
            subsystems: {
              heap_estimates: {
                ingest_queue_payload_bytes: 0,
              },
              mapped_files: {},
              disk_caches: {},
              configured_budgets: {
                sqlite_cache_budget_bytes: config.sqliteCacheBytes,
                worker_sqlite_cache_budget_bytes: config.workerSqliteCacheBytes,
              },
              pipeline_buffers: {},
              sqlite_runtime: {
                sqlite_memory_used_bytes: sqliteRuntime.memory_used_bytes,
                sqlite_memory_highwater_bytes: sqliteRuntime.memory_highwater_bytes,
                sqlite_pagecache_overflow_bytes: sqliteRuntime.pagecache_overflow_bytes,
                sqlite_pagecache_overflow_highwater_bytes: sqliteRuntime.pagecache_overflow_highwater_bytes,
              },
              counts: {
                ingest_queue_requests: 0,
                pending_upload_segments: 0,
                sqlite_pagecache_used_slots: sqliteRuntime.pagecache_used_slots,
                sqlite_pagecache_used_slots_highwater: sqliteRuntime.pagecache_used_slots_highwater,
                sqlite_malloc_count: sqliteRuntime.malloc_count,
                sqlite_malloc_count_highwater: sqliteRuntime.malloc_count_highwater,
                sqlite_open_connections: sqliteRuntime.open_connections,
                sqlite_prepared_statements: sqliteRuntime.prepared_statements,
              },
            },
            totals: {
              heap_estimate_bytes: 0,
              mapped_file_bytes: 0,
              disk_cache_bytes: 0,
              configured_budget_bytes: config.sqliteCacheBytes + config.workerSqliteCacheBytes,
              pipeline_buffer_bytes: 0,
              sqlite_runtime_bytes: sqliteRuntime.memory_used_bytes + sqliteRuntime.pagecache_overflow_bytes,
            },
          };
        },
        start: (): void => {},
      };
    },
  });
}
