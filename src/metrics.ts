type Tags = Record<string, string>;

type MetricEvent = {
  apiVersion: "durable.streams/metrics/v1";
  kind: "interval";
  metric: string;
  unit: "ns" | "bytes" | "count";
  windowStart: number;
  windowEnd: number;
  intervalMs: number;
  instance: string;
  stream?: string;
  tags?: Tags;
  count: number;
  sum: number;
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  buckets: Record<string, number>;
};

class Histogram {
  private readonly maxSamples: number;
  private samples: number[] = [];
  count = 0;
  sum = 0;
  min = Number.POSITIVE_INFINITY;
  max = Number.NEGATIVE_INFINITY;
  buckets: Record<string, number> = {};

  constructor(maxSamples = 1024) {
    this.maxSamples = maxSamples;
  }

  add(value: number): void {
    this.count++;
    this.sum += value;
    if (value < this.min) this.min = value;
    if (value > this.max) this.max = value;
    const bucket = Math.floor(Math.log2(Math.max(1, value)));
    const key = String(2 ** bucket);
    this.buckets[key] = (this.buckets[key] ?? 0) + 1;
    if (this.samples.length < this.maxSamples) {
      this.samples.push(value);
    } else {
      const idx = Math.floor(Math.random() * this.count);
      if (idx < this.maxSamples) this.samples[idx] = value;
    }
  }

  snapshotAndReset(): { count: number; sum: number; min: number; max: number; avg: number; p50: number; p95: number; p99: number; buckets: Record<string, number> } {
    const count = this.count;
    const sum = this.sum;
    const min = count === 0 ? 0 : this.min;
    const max = count === 0 ? 0 : this.max;
    const avg = count === 0 ? 0 : sum / count;
    const sorted = this.samples.slice().sort((a, b) => a - b);
    const p = (q: number) => (sorted.length === 0 ? 0 : sorted[Math.min(sorted.length - 1, Math.floor(q * (sorted.length - 1)))]);
    const p50 = p(0.5);
    const p95 = p(0.95);
    const p99 = p(0.99);
    const buckets = { ...this.buckets };
    this.samples = [];
    this.count = 0;
    this.sum = 0;
    this.min = Number.POSITIVE_INFINITY;
    this.max = Number.NEGATIVE_INFINITY;
    this.buckets = {};
    return { count, sum, min, max, avg, p50, p95, p99, buckets };
  }
}

type SeriesKey = string;

class MetricSeries {
  readonly metric: string;
  readonly unit: "ns" | "bytes" | "count";
  readonly stream?: string;
  readonly tags?: Tags;
  readonly hist = new Histogram();

  constructor(metric: string, unit: "ns" | "bytes" | "count", stream?: string, tags?: Tags) {
    this.metric = metric;
    this.unit = unit;
    this.stream = stream;
    this.tags = tags;
  }
}

function keyFor(metric: string, unit: string, stream?: string, tags?: Tags): SeriesKey {
  const tagStr = tags ? JSON.stringify(tags) : "";
  return `${metric}|${unit}|${stream ?? ""}|${tagStr}`;
}

function instanceId(): string {
  const host = typeof process !== "undefined" ? process.pid.toString() : "node";
  const rand = Math.random().toString(16).slice(2, 8);
  return `${host}-${rand}`;
}

export class Metrics {
  private readonly startMs = Date.now();
  private windowStartMs = Date.now();
  private readonly series = new Map<SeriesKey, MetricSeries>();
  private readonly instance = instanceId();

  record(metric: string, value: number, unit: "ns" | "bytes" | "count", tags?: Tags, stream?: string): void {
    const key = keyFor(metric, unit, stream, tags);
    let s = this.series.get(key);
    if (!s) {
      s = new MetricSeries(metric, unit, stream, tags);
      this.series.set(key, s);
    }
    s.hist.add(value);
  }

  recordAppend(bytes: number, entries: number): void {
    this.record("tieredstore.append.bytes", bytes, "bytes");
    this.record("tieredstore.append.entries", entries, "count");
  }

  recordRead(bytes: number, entries: number): void {
    this.record("tieredstore.read.bytes", bytes, "bytes");
    this.record("tieredstore.read.entries", entries, "count");
  }

  snapshot(): any {
    return {
      uptime_ms: Date.now() - this.startMs,
      series: this.series.size,
    };
  }

  flushInterval(): MetricEvent[] {
    const windowEnd = Date.now();
    const intervalMs = windowEnd - this.windowStartMs;
    const events: MetricEvent[] = [];
    for (const s of this.series.values()) {
      const snap = s.hist.snapshotAndReset();
      if (snap.count === 0) continue;
      events.push({
        apiVersion: "durable.streams/metrics/v1",
        kind: "interval",
        metric: s.metric,
        unit: s.unit,
        windowStart: this.windowStartMs,
        windowEnd,
        intervalMs,
        instance: this.instance,
        stream: s.stream,
        tags: s.tags,
        ...snap,
      });
    }
    this.windowStartMs = windowEnd;
    return events;
  }
}
