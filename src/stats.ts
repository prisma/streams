import type { SqliteDurableStore } from "./db/db";
import type { UploaderController } from "./uploader";
import type { MemoryGuard } from "./memory";
import type { BackpressureGate } from "./backpressure";
import type { IngestQueue } from "./ingest";

export type StatsSnapshot = {
  ingestedBytes: number;
  walBytes: number;
  sealedPayloadBytes: number;
  sealedBytes: number;
  uploadedBytes: number;
  segmentsSealed: number;
  backpressureOverMs: number;
  activeStreams: number;
};

export class StatsCollector {
  private readonly backpressureBudgetMs: number;
  private ingestedBytes = 0;
  private walBytes = 0;
  private sealedPayloadBytes = 0;
  private sealedBytes = 0;
  private uploadedBytes = 0;
  private segmentsSealed = 0;
  private backpressureOverMs = 0;
  private readonly activeStreams = new Set<string>();

  constructor(opts?: { backpressureBudgetMs?: number }) {
    const raw = opts?.backpressureBudgetMs ?? 1;
    this.backpressureBudgetMs = Number.isFinite(raw) ? Math.max(0, raw) : 1;
  }

  recordIngested(bytes: number): void {
    this.ingestedBytes += bytes;
  }

  recordWalCommitBytes(bytes: number): void {
    this.walBytes += bytes;
  }

  recordSegmentSealed(payloadBytes: number, segmentBytes: number): void {
    this.sealedPayloadBytes += payloadBytes;
    this.sealedBytes += segmentBytes;
    this.segmentsSealed += 1;
  }

  recordUploadedBytes(bytes: number): void {
    this.uploadedBytes += bytes;
  }

  getBackpressureBudgetMs(): number {
    return this.backpressureBudgetMs;
  }

  recordBackpressureOverMs(overMs: number): void {
    if (overMs <= 0) return;
    this.backpressureOverMs += Math.max(0, overMs);
  }

  recordStreamTouched(stream: string): void {
    this.activeStreams.add(stream);
  }

  snapshotAndReset(): StatsSnapshot {
    const snapshot: StatsSnapshot = {
      ingestedBytes: this.ingestedBytes,
      walBytes: this.walBytes,
      sealedPayloadBytes: this.sealedPayloadBytes,
      sealedBytes: this.sealedBytes,
      uploadedBytes: this.uploadedBytes,
      segmentsSealed: this.segmentsSealed,
      backpressureOverMs: this.backpressureOverMs,
      activeStreams: this.activeStreams.size,
    };
    this.ingestedBytes = 0;
    this.walBytes = 0;
    this.sealedPayloadBytes = 0;
    this.sealedBytes = 0;
    this.uploadedBytes = 0;
    this.segmentsSealed = 0;
    this.backpressureOverMs = 0;
    this.activeStreams.clear();
    return snapshot;
  }
}

function formatBytes(bytes: number): string {
  const units = ["b", "kb", "mb", "gb"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  const digits = idx === 0 ? 0 : 1;
  return `${value.toFixed(digits)}${units[idx]}`;
}

export class StatsReporter {
  private timer: any | null = null;
  private sampleTimer: any | null = null;
  private running = false;
  private lastTickMs: number | null = null;
  private lastSampleMs: number | null = null;
  private rejectActiveMs = 0;
  private readonly intervalMs: number;
  private readonly stats: StatsCollector;
  private readonly db: SqliteDurableStore;
  private readonly uploader: UploaderController;
  private readonly ingest?: IngestQueue;
  private readonly backpressure?: BackpressureGate;
  private readonly memory?: MemoryGuard;

  constructor(
    stats: StatsCollector,
    db: SqliteDurableStore,
    uploader: UploaderController,
    ingest?: IngestQueue,
    backpressure?: BackpressureGate,
    memory?: MemoryGuard,
    intervalMs = 60_000
  ) {
    this.stats = stats;
    this.db = db;
    this.uploader = uploader;
    this.ingest = ingest;
    this.backpressure = backpressure;
    this.memory = memory;
    this.intervalMs = intervalMs;
  }

  start(): void {
    if (this.timer) return;
    if (!this.sampleTimer) {
      this.sampleTimer = setInterval(() => this.sample(), 250);
      this.sample();
    }
    this.timer = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.sampleTimer) clearInterval(this.sampleTimer);
    this.sampleTimer = null;
  }

  private sample(): void {
    const now = Date.now();
    const last = this.lastSampleMs;
    this.lastSampleMs = now;
    if (!last) return;
    const dt = Math.max(0, now - last);

    const rejectActive =
      (this.memory?.isOverLimit() ?? false) ||
      (this.ingest?.isQueueFull() ?? false) ||
      (this.backpressure?.isOverLimit() ?? false);
    if (rejectActive) this.rejectActiveMs += dt;
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const nowMs = Date.now();
      const windowMs = this.lastTickMs ? Math.max(1, nowMs - this.lastTickMs) : this.intervalMs;
      this.lastTickMs = nowMs;
      const snap = this.stats.snapshotAndReset();
      const storedBytes = snap.walBytes + snap.sealedBytes;
      const compression =
        snap.sealedBytes > 0 ? `${(snap.sealedPayloadBytes / snap.sealedBytes).toFixed(2)}x` : "n/a";
      const queueWaitPct = snap.backpressureOverMs > 0 ? (snap.backpressureOverMs / windowMs) * 100 : 0;
      const rejectPct = this.rejectActiveMs > 0 ? (this.rejectActiveMs / windowMs) * 100 : 0;
      const backpressurePct = Math.min(100, Math.max(queueWaitPct, rejectPct));
      this.rejectActiveMs = 0;
      const avgSegmentSize = snap.segmentsSealed > 0 ? formatBytes(snap.sealedBytes / snap.segmentsSealed) : "n/a";
      const totalStreams = this.db.countStreams();
      const segmentsWaiting = this.uploader.countSegmentsWaiting();
      const walDbBytes = this.db.getWalDbSizeBytes();
      const metaDbBytes = this.db.getMetaDbSizeBytes();
      const maxRss = this.memory ? formatBytes(this.memory.snapshotMaxRssBytes(true)) : null;
      const line =
        `ingested=${formatBytes(snap.ingestedBytes)} ` +
        `stored=${formatBytes(storedBytes)} ` +
        `compression=${compression} ` +
        `uploaded=${formatBytes(snap.uploadedBytes)} ` +
        `streams-touched=${snap.activeStreams}/${totalStreams} ` +
        `segments-sealed=${snap.segmentsSealed} ` +
        `segments-waiting=${segmentsWaiting} ` +
        `avg-segment-size=${avgSegmentSize} ` +
        `wal-size=${formatBytes(walDbBytes)} ` +
        `meta-size=${formatBytes(metaDbBytes)} ` +
        `backpressure=${backpressurePct.toFixed(1)}%` +
        (maxRss ? ` max-rss=${maxRss}` : "");
      // eslint-disable-next-line no-console
      console.log(line);
    } finally {
      this.running = false;
    }
  }
}
