import { rmSync } from "node:fs";
import { Result } from "better-result";
import type { Config } from "./config";
import { ConcurrencyGate } from "./concurrency_gate";
import { SqliteDurableStore, STREAM_FLAG_DELETED } from "./db/db";
import type { Metrics } from "./metrics";
import type { ObjectStore } from "./objectstore/interface";
import type { SegmentDiskCache } from "./segment/cache";
import { retry, type RetryOptions } from "./util/retry";
import { manifestObjectKey, streamHash16Hex } from "./util/stream_paths";

export type ReapSummary = {
  stream: string;
  objectsDeleted: number;
  listPasses: number;
  skipped: boolean;
};

export type ReapError = {
  kind: "tombstone_publish_failed" | "list_failed" | "delete_failed" | "prefix_not_stable" | "stopped";
  message: string;
  key?: string;
};

// A reap that needs more passes than this is being raced by a writer; back off
// to the next sweep tick instead of spinning against it.
const MAX_LIST_PASSES = 5;

/**
 * Deletes a doomed stream's object-store data and, once the prefix is
 * verifiably empty, its local rows. A stream is doomed when its local row
 * carries STREAM_FLAG_DELETED; the row is the durable resume token, so a crash
 * at any step re-runs the reap on the next sweep tick.
 *
 * Ordering is the crash-safety contract: data objects are deleted first and
 * `manifest.json` strictly last. While the manifest exists the stream is
 * visible to restore-from-R2 as a tombstone (deleted flag or past expiry), so
 * a restore mid-reap re-arms the reap instead of resurrecting the stream.
 */
export class StreamReaper {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly store: ObjectStore;
  private readonly publishTombstone: (stream: string) => Promise<void>;
  private readonly metrics?: Metrics;
  private readonly diskCache?: SegmentDiskCache;
  private readonly localOnly: boolean;
  private readonly retryOpts: RetryOptions;
  private readonly locks = new Map<string, Promise<unknown>>();
  private stopped = false;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    store: ObjectStore,
    publishTombstone: (stream: string) => Promise<void>,
    opts: {
      metrics?: Metrics;
      diskCache?: SegmentDiskCache;
      /** Local mode has no remote objects; a reap is just the local cleanup. */
      localOnly?: boolean;
    } = {}
  ) {
    this.cfg = cfg;
    this.db = db;
    this.store = store;
    this.publishTombstone = publishTombstone;
    this.metrics = opts.metrics;
    this.diskCache = opts.diskCache;
    this.localOnly = opts.localOnly ?? false;
    this.retryOpts = {
      retries: cfg.objectStoreRetries,
      baseDelayMs: cfg.objectStoreBaseDelayMs,
      maxDelayMs: cfg.objectStoreMaxDelayMs,
      timeoutMs: cfg.objectStoreTimeoutMs,
    };
  }

  stop(): void {
    this.stopped = true;
  }

  /**
   * Serializes reaping and (in the resolution layer) hydration per stream, so
   * a reap can never interleave with a restore or recreate of the same name.
   */
  async withStreamLock<T>(stream: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.locks.get(stream) ?? Promise.resolve();
    const run = prev.catch(() => undefined).then(fn);
    const tail = run.catch(() => undefined);
    this.locks.set(stream, tail);
    try {
      return await run;
    } finally {
      // Only clear when no later caller chained behind us.
      if (this.locks.get(stream) === tail) this.locks.delete(stream);
    }
  }

  /** Idempotent; safe for non-reapable streams (returns ok skipped). */
  async reapStream(stream: string): Promise<Result<ReapSummary, ReapError>> {
    return this.withStreamLock(stream, () => this.reapLocked(stream));
  }

  /**
   * PUT-recreate entry: flags an expired-but-not-deleted row so the reap owns
   * it, then reaps. A row that turned live again under the lock is left alone.
   */
  async reapForRecreate(stream: string): Promise<Result<ReapSummary, ReapError>> {
    return this.withStreamLock(stream, async () => {
      const row = this.db.getStream(stream);
      if (!row) return Result.ok(skippedSummary(stream));
      const expired = row.expires_at_ms != null && this.db.nowMs() > row.expires_at_ms;
      if (!this.db.isDeleted(row) && !expired) return Result.ok(skippedSummary(stream));
      if (!this.db.isDeleted(row)) this.db.deleteStream(stream);
      return this.reapLocked(stream);
    });
  }

  private async reapLocked(stream: string): Promise<Result<ReapSummary, ReapError>> {
    if (this.stopped) return Result.err({ kind: "stopped" as const, message: "reaper stopped" });
    const row = this.db.getStream(stream);
    if (!row || !this.db.isDeleted(row)) return Result.ok(skippedSummary(stream));

    const startedNs = process.hrtime.bigint();
    const res = await this.reapDoomed(stream);
    if (this.metrics) {
      const elapsedNs = Number(process.hrtime.bigint() - startedNs);
      this.metrics.record("tieredstore.retention.reap.latency", elapsedNs, "ns", {
        outcome: Result.isError(res) ? "error" : "ok",
      });
      if (Result.isError(res)) {
        this.metrics.record("tieredstore.retention.reap.failures", 1, "count", { kind: res.error.kind });
      } else if (!res.value.skipped) {
        this.metrics.record("tieredstore.retention.objects_deleted", res.value.objectsDeleted, "count");
        this.metrics.record("tieredstore.retention.list_passes", res.value.listPasses, "count");
      }
    }
    return res;
  }

  private async reapDoomed(stream: string): Promise<Result<ReapSummary, ReapError>> {
    const shash = streamHash16Hex(stream);
    const prefix = `streams/${shash}/`;
    const mkey = manifestObjectKey(shash);

    if (this.localOnly) {
      this.cleanupLocal(stream, shash, []);
      return Result.ok({ stream, objectsDeleted: 0, listPasses: 0, skipped: false });
    }

    // The manifest must be terminal (deleted flag or past expiry) in R2 before
    // any data object disappears: restore-from-R2 short-circuits terminal
    // manifests, so a crash mid-reap can never brick bootstrap on a
    // head-check of an already-deleted segment.
    const terminal = await this.ensureTerminalManifest(stream, mkey);
    if (Result.isError(terminal)) return terminal;

    let listPasses = 0;
    let objectsDeleted = 0;
    const deletedKeys: string[] = [];
    for (;;) {
      if (this.stopped) return Result.err({ kind: "stopped" as const, message: "reaper stopped" });
      listPasses++;
      if (listPasses > MAX_LIST_PASSES) {
        return Result.err({
          kind: "prefix_not_stable" as const,
          message: `prefix ${prefix} still non-empty after ${MAX_LIST_PASSES} passes`,
        });
      }
      let keys: string[];
      try {
        keys = await retry(() => this.store.list(prefix), this.retryOpts);
      } catch (e) {
        return Result.err({ kind: "list_failed" as const, message: errorMessage(e) });
      }
      if (keys.length === 0) break;

      // Data objects first; the manifest goes only when it is the last key
      // left, and the follow-up list pass verifies the prefix emptied.
      const dataKeys = keys.filter((k) => k !== mkey);
      const doomedKeys = dataKeys.length > 0 ? dataKeys : keys;
      const failure = await this.deleteKeys(doomedKeys);
      if (failure) return failure;
      objectsDeleted += doomedKeys.length;
      deletedKeys.push(...doomedKeys);
    }

    this.cleanupLocal(stream, shash, deletedKeys);
    return Result.ok({ stream, objectsDeleted, listPasses, skipped: false });
  }

  /**
   * Local commit point: drops cached segment bytes (recreated streams reuse
   * the same object keys), any never-uploaded local segment files, and finally
   * every local row.
   */
  private cleanupLocal(stream: string, shash: string, deletedKeys: string[]): void {
    if (this.diskCache) {
      for (const key of deletedKeys) this.diskCache.remove(key);
    }
    try {
      rmSync(`${this.cfg.rootDir}/local/streams/${shash}`, { recursive: true, force: true });
    } catch {
      // ignore
    }
    this.db.hardDeleteStream(stream);
  }

  private async ensureTerminalManifest(stream: string, mkey: string): Promise<Result<void, ReapError>> {
    const readManifest = async (): Promise<{ terminal: boolean } | null> => {
      const bytes = await retry(() => this.store.get(mkey), this.retryOpts);
      if (!bytes) return null;
      try {
        const manifest = JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
        const flags = typeof manifest.stream_flags === "number" ? manifest.stream_flags : 0;
        const expiresAt = typeof manifest.expires_at === "string" ? Date.parse(manifest.expires_at) : Number.NaN;
        const expired = Number.isFinite(expiresAt) && BigInt(Math.trunc(expiresAt)) <= this.db.nowMs();
        return { terminal: (flags & STREAM_FLAG_DELETED) !== 0 || expired };
      } catch {
        // An unparseable manifest cannot resurrect anything on restore; treat
        // it as terminal so the reap can remove it.
        return { terminal: true };
      }
    };

    try {
      const current = await readManifest();
      if (!current || current.terminal) return Result.ok(undefined);
      await this.publishTombstone(stream);
      const republished = await readManifest();
      if (!republished || republished.terminal) return Result.ok(undefined);
      return Result.err({
        kind: "tombstone_publish_failed" as const,
        message: `manifest for ${stream} is still live after tombstone publish`,
      });
    } catch (e) {
      return Result.err({ kind: "tombstone_publish_failed" as const, message: errorMessage(e) });
    }
  }

  private async deleteKeys(keys: string[]): Promise<Result<never, ReapError> | null> {
    const gate = new ConcurrencyGate(Math.max(1, this.cfg.retentionDeleteConcurrency));
    const settled = await Promise.allSettled(
      keys.map((key) => gate.run(() => retry(() => this.store.delete(key), this.retryOpts)))
    );
    for (let i = 0; i < settled.length; i++) {
      const s = settled[i];
      if (s.status === "rejected") {
        return Result.err({ kind: "delete_failed" as const, message: errorMessage(s.reason), key: keys[i] });
      }
    }
    return null;
  }
}

function skippedSummary(stream: string): ReapSummary {
  return { stream, objectsDeleted: 0, listPasses: 0, skipped: true };
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
