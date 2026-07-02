import { Result } from "better-result";
import { manifestIsTombstone, restoreTombstoneRow } from "./bootstrap";
import { ConcurrencyGate } from "./concurrency_gate";
import type { Config } from "./config";
import type { SqliteDurableStore } from "./db/db";
import type { Metrics } from "./metrics";
import type { ObjectStore } from "./objectstore/interface";
import type { StreamReaper } from "./retention";
import { FailureTracker } from "./uploader";
import { retry, type RetryOptions } from "./util/retry";

/**
 * Drives stream retention: flags expired streams (soft delete), feeds
 * soft-deleted streams to the reaper, and periodically scans the object store
 * for doomed manifests that no longer have a local row (the local SQLite is
 * ephemeral on redeploy, so the object store is the only durable record).
 */
export class RetentionSweeper {
  private readonly cfg: Config;
  private readonly db: SqliteDurableStore;
  private readonly store: ObjectStore;
  private readonly reaper: StreamReaper;
  private readonly metrics?: Metrics;
  private readonly failures = new FailureTracker(1024);
  private readonly retryOpts: RetryOptions;
  private timer: any | null = null;
  private scanTimer: any | null = null;
  private running = false;
  private scanRunning = false;

  private readonly scanEnabled: boolean;

  constructor(
    cfg: Config,
    db: SqliteDurableStore,
    store: ObjectStore,
    reaper: StreamReaper,
    opts: { metrics?: Metrics; scanEnabled?: boolean } = {}
  ) {
    this.cfg = cfg;
    this.db = db;
    this.store = store;
    this.reaper = reaper;
    this.metrics = opts.metrics;
    this.scanEnabled = opts.scanEnabled ?? true;
    this.retryOpts = {
      retries: cfg.objectStoreRetries,
      baseDelayMs: cfg.objectStoreBaseDelayMs,
      maxDelayMs: cfg.objectStoreMaxDelayMs,
      timeoutMs: cfg.objectStoreTimeoutMs,
    };
  }

  start(): void {
    if (!this.timer && this.cfg.expirySweepIntervalMs > 0) {
      this.timer = setInterval(() => {
        void this.tick();
      }, this.cfg.expirySweepIntervalMs);
    }
    if (this.scanEnabled && !this.scanTimer && this.cfg.retentionScanIntervalMs > 0) {
      // Jitter keeps a fleet of replicas from listing the store in lockstep.
      const jitter = 0.9 + Math.random() * 0.2;
      this.scanTimer = setInterval(
        () => {
          void this.scanTick();
        },
        Math.round(this.cfg.retentionScanIntervalMs * jitter)
      );
    }
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.scanTimer) clearInterval(this.scanTimer);
    this.scanTimer = null;
    this.reaper.stop();
  }

  /** Schedules an immediate tick (e.g. right after an HTTP DELETE). */
  nudge(): void {
    void this.tick();
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const expired = this.db.listExpiredStreams(this.cfg.expirySweepBatchLimit);
      for (const stream of expired) {
        try {
          this.db.deleteStream(stream);
        } catch {
          // ignore deletion errors
        }
      }

      const reapable = this.db.listReapableStreams(this.cfg.retentionReapBatchLimit);
      for (const stream of reapable) {
        if (this.failures.shouldSkip(stream)) continue;
        const res = await this.reaper.reapStream(stream);
        if (Result.isError(res)) {
          if (res.error.kind === "stopped") return;
          this.failures.recordFailure(stream);
        } else {
          this.failures.recordSuccess(stream);
          if (!res.value.skipped) {
            this.metrics?.record("tieredstore.retention.streams_reaped", 1, "count", { trigger: "sweep" });
          }
        }
      }
    } finally {
      this.running = false;
    }
  }

  private async scanTick(): Promise<void> {
    if (this.scanRunning) return;
    this.scanRunning = true;
    const startedNs = process.hrtime.bigint();
    let manifestsChecked = 0;
    let tombstonesRestored = 0;
    try {
      const keys = await retry(() => this.store.list("streams/"), this.retryOpts);
      const manifestKeys = keys.filter((k) => k.endsWith("/manifest.json"));
      const gate = new ConcurrencyGate(Math.max(1, this.cfg.retentionDeleteConcurrency));
      await Promise.allSettled(
        manifestKeys.map((mkey) =>
          gate.run(async () => {
            manifestsChecked++;
            const bytes = await retry(() => this.store.get(mkey), this.retryOpts);
            if (!bytes) return;
            let manifest: Record<string, any>;
            try {
              manifest = JSON.parse(new TextDecoder().decode(bytes));
            } catch {
              return;
            }
            const stream = String(manifest.name ?? "");
            if (!stream) return;
            if (!manifestIsTombstone(manifest, this.db.nowMs())) return;
            // Serialize with reaps/hydrations of the same name; a local row of
            // any kind wins and already drives its own lifecycle.
            await this.reaper.withStreamLock(stream, async () => {
              if (this.db.getStream(stream)) return;
              restoreTombstoneRow(this.db, manifest, this.db.nowMs());
              tombstonesRestored++;
            });
          })
        )
      );
      if (tombstonesRestored > 0) this.nudge();
    } catch {
      // Transient list failures are retried on the next scan interval.
    } finally {
      this.scanRunning = false;
      if (this.metrics) {
        const elapsedNs = Number(process.hrtime.bigint() - startedNs);
        this.metrics.record("tieredstore.retention.scan.latency", elapsedNs, "ns");
        this.metrics.record("tieredstore.retention.scan.manifests_checked", manifestsChecked, "count");
        this.metrics.record("tieredstore.retention.scan.tombstones_restored", tombstonesRestored, "count");
      }
    }
  }
}
