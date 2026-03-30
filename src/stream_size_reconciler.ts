import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { Result } from "better-result";
import type { SqliteDurableStore, SegmentRow } from "./db/db";
import type { ObjectStore } from "./objectstore/interface";
import { iterateBlocksResult } from "./segment/format";
import { segmentObjectKey, streamHash16Hex } from "./util/stream_paths";
import { dsError } from "./util/ds_error";
import { yieldToEventLoop } from "./util/yield";

export class StreamSizeReconciler {
  private stopped = false;
  private running: Promise<void> | null = null;

  constructor(
    private readonly db: SqliteDurableStore,
    private readonly os: ObjectStore,
    private readonly onMetadataChanged?: (stream: string) => void
  ) {}

  start(): void {
    if (this.running) return;
    this.running = this.run().finally(() => {
      this.running = null;
    });
  }

  stop(): void {
    this.stopped = true;
  }

  private async run(): Promise<void> {
    while (!this.stopped) {
      const streams = this.db.listStreamsMissingLogicalSize(8);
      if (streams.length === 0) return;
      for (const stream of streams) {
        if (this.stopped) return;
        try {
          await this.reconcileStream(stream);
        } catch (e) {
          const msg = String((e as any)?.message ?? e);
          if (!this.stopped && !msg.includes("Statement has finalized")) {
            // eslint-disable-next-line no-console
            console.error("stream size reconcile failed", stream, e);
          }
        }
      }
    }
  }

  private async reconcileStream(stream: string): Promise<void> {
    for (let attempt = 0; attempt < 3 && !this.stopped; attempt++) {
      const before = this.db.getStream(stream);
      if (!before || this.db.isDeleted(before) || before.next_offset <= 0n) return;
      if (before.logical_size_bytes > 0n) return;

      const segments = this.db.listSegmentsForStream(stream);
      let total = 0n;
      for (const segment of segments) {
        if (this.stopped) return;
        total += await this.sumSegmentPayloadBytes(segment);
        await yieldToEventLoop();
      }

      const after = this.db.getStream(stream);
      if (!after || this.db.isDeleted(after)) return;
      if (after.logical_size_bytes > 0n) return;

      if (segments.length !== this.db.countSegmentsForStream(stream)) continue;

      const finalTotal = total + after.wal_bytes;
      if (finalTotal > after.logical_size_bytes) {
        this.db.setStreamLogicalSizeBytes(stream, finalTotal);
        this.onMetadataChanged?.(stream);
      }
      return;
    }
  }

  private async sumSegmentPayloadBytes(segment: SegmentRow): Promise<bigint> {
    const bytes = await this.loadSegmentBytes(segment);
    let total = 0n;
    for (const blockRes of iterateBlocksResult(bytes)) {
      if (Result.isError(blockRes)) throw dsError(blockRes.error.message);
      for (const record of blockRes.value.decoded.records) {
        total += BigInt(record.payload.byteLength);
      }
    }
    return total;
  }

  private async loadSegmentBytes(segment: SegmentRow): Promise<Uint8Array> {
    if (existsSync(segment.local_path)) {
      return new Uint8Array(await readFile(segment.local_path));
    }
    const shash = streamHash16Hex(segment.stream);
    const objectKey = segmentObjectKey(shash, segment.segment_index);
    const bytes = await this.os.get(objectKey);
    if (!bytes) throw dsError(`missing segment object ${objectKey}`);
    return bytes;
  }
}
