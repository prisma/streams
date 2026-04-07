import { existsSync, readFileSync } from "node:fs";
import { Result } from "better-result";
import type { BackpressureGate, OverloadReason } from "../backpressure";
import type { SegmentRow } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import { SegmentDiskCache } from "../segment/cache";
import { segmentObjectKey, streamHash16Hex } from "../util/stream_paths";

type AcquireError =
  | {
      kind: "index_cache_overloaded";
      code: "index_building_behind";
      message: string;
    }
  | {
      kind: "missing_segment";
      message: string;
    };

export type IndexingSegmentLease = {
  localSegments: Array<{ segmentIndex: number; objectKey: string; localPath: string; sizeBytes: number }>;
  release(): void;
};

export class IndexSegmentLocalityManager {
  private readonly overloadSourcePrefix = "index-locality:";

  constructor(
    private readonly segmentCache: SegmentDiskCache,
    private readonly objectStore: ObjectStore,
    private readonly backpressure?: BackpressureGate
  ) {}

  async acquireWindowResult(
    stream: string,
    segments: SegmentRow[],
    source = "indexing"
  ): Promise<Result<IndexingSegmentLease, AcquireError>> {
    const acquiredKeys: string[] = [];
    const localSegments: Array<{ segmentIndex: number; objectKey: string; localPath: string; sizeBytes: number }> = [];
    for (const segment of segments) {
      const objectKey = segmentObjectKey(streamHash16Hex(stream), segment.segment_index);
      const localPathRes = await this.ensureLocalPathResult(stream, segment, objectKey, segments, source);
      if (Result.isError(localPathRes)) {
        this.releaseKeys(acquiredKeys);
        if (localPathRes.error.kind === "index_cache_overloaded") {
          this.backpressure?.setOverloadReason(this.overloadSource(stream), {
            code: localPathRes.error.code,
            message: localPathRes.error.message,
          });
        }
        return localPathRes;
      }
      if (!this.segmentCache.markRequiredForIndexing(objectKey)) {
        this.releaseKeys(acquiredKeys);
        const error = {
          kind: "missing_segment" as const,
          message: `missing local cached segment for indexing: ${objectKey}`,
        };
        return Result.err(error);
      }
      acquiredKeys.push(objectKey);
      localSegments.push({
        segmentIndex: segment.segment_index,
        objectKey,
        localPath: localPathRes.value,
        sizeBytes: segment.size_bytes,
      });
    }
    this.backpressure?.setOverloadReason(this.overloadSource(stream), null);
    let released = false;
    return Result.ok({
      localSegments,
      release: () => {
        if (released) return;
        released = true;
        this.releaseKeys(acquiredKeys);
        this.backpressure?.setOverloadReason(this.overloadSource(stream), null);
      },
    });
  }

  clearStreamOverload(stream: string): void {
    this.backpressure?.setOverloadReason(this.overloadSource(stream), null);
  }

  async acquireRoutingWindowResult(stream: string, segments: SegmentRow[]): Promise<Result<IndexingSegmentLease, AcquireError>> {
    return await this.acquireWindowResult(stream, segments, "routing indexing");
  }

  async acquireLexiconWindowResult(stream: string, segments: SegmentRow[]): Promise<Result<IndexingSegmentLease, AcquireError>> {
    return await this.acquireWindowResult(stream, segments, "routing-key lexicon indexing");
  }

  private releaseKeys(keys: string[]): void {
    for (const objectKey of keys) this.segmentCache.releaseRequiredForIndexing(objectKey);
  }

  private overloadSource(stream: string): string {
    return `${this.overloadSourcePrefix}${stream}`;
  }

  private buildOverloadReason(stream: string, segments: SegmentRow[], source: string): AcquireError {
    const cacheStats = this.segmentCache.stats();
    const requiredWindowBytes = segments.reduce((total, segment) => total + Math.max(0, segment.size_bytes), 0);
    return {
      kind: "index_cache_overloaded",
      code: "index_building_behind",
      message:
        `${source} is too far behind to retain the next ${segments.length} required segment files locally ` +
        `(need ${(requiredWindowBytes / (1024 * 1024)).toFixed(1)} MiB, ` +
        `cache ${(cacheStats.usedBytes / (1024 * 1024)).toFixed(1)} / ${(cacheStats.maxBytes / (1024 * 1024)).toFixed(1)} MiB)`,
    };
  }

  private async ensureLocalPathResult(
    stream: string,
    segment: SegmentRow,
    objectKey: string,
    window: SegmentRow[],
    source: string
  ): Promise<Result<string, AcquireError>> {
    if (this.segmentCache.has(objectKey)) {
      return Result.ok(this.segmentCache.getPath(objectKey));
    }

    const residentBytes = this.segmentCache.get(objectKey);
    if (residentBytes && this.segmentCache.put(objectKey, residentBytes) && this.segmentCache.has(objectKey)) {
      return Result.ok(this.segmentCache.getPath(objectKey));
    }

    if (segment.local_path && segment.local_path.length > 0 && existsSync(segment.local_path)) {
      if (this.segmentCache.putFromLocal(objectKey, segment.local_path, segment.size_bytes)) {
        return Result.ok(this.segmentCache.getPath(objectKey));
      }
      const copied = new Uint8Array(readFileSync(segment.local_path));
      if (this.segmentCache.put(objectKey, copied) && this.segmentCache.has(objectKey)) {
        return Result.ok(this.segmentCache.getPath(objectKey));
      }
      return Result.err(this.buildOverloadReason(stream, window, source));
    }

    const bytes = await this.objectStore.get(objectKey);
    if (!bytes) {
      return Result.err({
        kind: "missing_segment",
        message: `missing uploaded segment object for indexing: ${objectKey}`,
      });
    }
    if (this.segmentCache.put(objectKey, bytes) && this.segmentCache.has(objectKey)) {
      return Result.ok(this.segmentCache.getPath(objectKey));
    }
    return Result.err(this.buildOverloadReason(stream, window, source));
  }
}
