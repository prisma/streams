import { existsSync, readFileSync } from "node:fs";
import type { SegmentRow } from "../db/db";
import type { ObjectStore } from "../objectstore/interface";
import type { SegmentDiskCache } from "./cache";
import { dsError } from "../util/ds_error";
import type { RetryOptions } from "../util/retry";
import { retry } from "../util/retry";
import { segmentObjectKey, streamHash16Hex } from "../util/stream_paths";

export type SegmentReadSource =
  | { kind: "mapped"; path: string; bytes: Uint8Array }
  | { kind: "bytes"; bytes: Uint8Array };

function readRangeFromBytes(bytes: Uint8Array, start: number, end: number): Uint8Array {
  const boundedStart = Math.max(0, Math.min(start, bytes.byteLength));
  const boundedEnd = Math.max(boundedStart, Math.min(end + 1, bytes.byteLength));
  return bytes.subarray(boundedStart, boundedEnd);
}

export function readRangeFromSource(source: SegmentReadSource, start: number, end: number): Uint8Array {
  return readRangeFromBytes(source.bytes, start, end);
}

export async function loadSegmentSource(
  os: ObjectStore,
  seg: SegmentRow,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions
): Promise<SegmentReadSource> {
  if (seg.local_path && seg.local_path.length > 0 && existsSync(seg.local_path)) {
    try {
      const bytes = (Bun as any).mmap(seg.local_path, { shared: true }) as Uint8Array;
      return { kind: "mapped", path: seg.local_path, bytes };
    } catch {
      return { kind: "bytes", bytes: readFileSync(seg.local_path) };
    }
  }

  const objectKey = segmentObjectKey(streamHash16Hex(seg.stream), seg.segment_index);
  const mappedCached = diskCache?.getMapped(objectKey);
  if (mappedCached && diskCache) {
    diskCache.recordHit();
    return { kind: "mapped", path: mappedCached.path, bytes: mappedCached.bytes };
  }
  if (diskCache && diskCache.has(objectKey)) {
    diskCache.recordHit();
    diskCache.touch(objectKey);
    const cachedPath = diskCache.getPath(objectKey);
    if (existsSync(cachedPath)) return { kind: "bytes", bytes: readFileSync(cachedPath) };
    diskCache.remove(objectKey);
  }

  if (diskCache) diskCache.recordMiss();

  const bytes = await retry(
    async () => {
      const res = await os.get(objectKey);
      if (!res) throw dsError(`object store missing segment: ${objectKey}`);
      return res;
    },
    retryOpts ?? { retries: 0, baseDelayMs: 0, maxDelayMs: 0, timeoutMs: 0 }
  );

  if (diskCache?.put(objectKey, bytes)) {
    const mapped = diskCache.getMapped(objectKey);
    if (mapped) return { kind: "mapped", path: mapped.path, bytes: mapped.bytes };
    return { kind: "bytes", bytes: readFileSync(diskCache.getPath(objectKey)) };
  }
  return { kind: "bytes", bytes };
}

export async function loadSegmentBytesCached(
  os: ObjectStore,
  seg: SegmentRow,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions
): Promise<Uint8Array> {
  const source = await loadSegmentSource(os, seg, diskCache, retryOpts);
  return source.bytes;
}

export async function readSegmentRangeCached(
  os: ObjectStore,
  seg: SegmentRow,
  start: number,
  end: number,
  diskCache?: SegmentDiskCache,
  retryOpts?: RetryOptions
): Promise<Uint8Array> {
  const source = await loadSegmentSource(os, seg, diskCache, retryOpts);
  return readRangeFromSource(source, start, end);
}
