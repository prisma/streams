import type { IngestQueue } from "./ingest";
import type { Metrics } from "./metrics";

export class MetricsEmitter {
  private readonly metrics: Metrics;
  private readonly ingest: IngestQueue;
  private readonly intervalMs: number;
  private timer: any | null = null;

  constructor(metrics: Metrics, ingest: IngestQueue, intervalMs: number) {
    this.metrics = metrics;
    this.ingest = ingest;
    this.intervalMs = intervalMs;
  }

  start(): void {
    if (this.intervalMs <= 0 || this.timer) return;
    this.timer = setInterval(() => {
      void this.flush();
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async flush(): Promise<void> {
    const queue = this.ingest.getQueueStats();
    this.metrics.record("tieredstore.ingest.queue.bytes", queue.bytes, "bytes");
    this.metrics.record("tieredstore.ingest.queue.requests", queue.requests, "count");
    const events = this.metrics.flushInterval();
    if (events.length === 0) return;
    const rows = events.map((e) => ({
      routingKey: e.stream ? new TextEncoder().encode(e.stream) : null,
      contentType: "application/json",
      payload: new TextEncoder().encode(JSON.stringify(e)),
    }));
    try {
      await this.ingest.appendInternal({
        stream: "__stream_metrics__",
        baseAppendMs: BigInt(Date.now()),
        rows,
        contentType: "application/json",
      });
    } catch {
      // best-effort; drop on failure
    }
  }
}
