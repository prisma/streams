export type HistogramSnapshot = {
  counts: number[];
  total: number;
  max: number;
  p50: number;
  p95: number;
};

class LatencyHistogram {
  private readonly bounds: number[];
  private counts: number[];
  private spareCounts: number[];
  private total = 0;
  private max = 0;

  constructor(bounds: number[]) {
    this.bounds = bounds;
    this.counts = new Array(bounds.length + 1).fill(0);
    this.spareCounts = new Array(bounds.length + 1).fill(0);
  }

  record(ms: number): void {
    const v = Math.max(0, Math.floor(ms));
    this.total += 1;
    if (v > this.max) this.max = v;
    let idx = this.bounds.length;
    for (let i = 0; i < this.bounds.length; i++) {
      if (v <= this.bounds[i]) {
        idx = i;
        break;
      }
    }
    this.counts[idx] += 1;
  }

  private percentile(q: number, counts: number[], total: number, max: number): number {
    if (total === 0) return 0;
    const target = Math.ceil(total * q);
    let acc = 0;
    for (let i = 0; i < counts.length; i++) {
      acc += counts[i];
      if (acc >= target) {
        if (i < this.bounds.length) return this.bounds[i];
        return max;
      }
    }
    return max;
  }

  snapshotAndReset(): HistogramSnapshot {
    const counts = this.counts;
    const total = this.total;
    const max = this.max;
    const p50 = this.percentile(0.5, counts, total, max);
    const p95 = this.percentile(0.95, counts, total, max);
    this.counts = this.spareCounts;
    this.counts.fill(0);
    this.spareCounts = counts;
    this.total = 0;
    this.max = 0;
    const snap: HistogramSnapshot = {
      counts,
      total,
      max,
      p50,
      p95,
    };
    return snap;
  }
}

export class LatencyHistogramCollector {
  private readonly writes: LatencyHistogram;
  private readonly reads: LatencyHistogram;
  private readonly bounds: number[];

  constructor(bounds?: number[]) {
    this.bounds = bounds ?? [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000];
    this.writes = new LatencyHistogram(this.bounds);
    this.reads = new LatencyHistogram(this.bounds);
  }

  recordWrite(ms: number): void {
    this.writes.record(ms);
  }

  recordRead(ms: number): void {
    this.reads.record(ms);
  }

  snapshotAndReset(): { writes: HistogramSnapshot; reads: HistogramSnapshot } {
    return { writes: this.writes.snapshotAndReset(), reads: this.reads.snapshotAndReset() };
  }

  getBounds(): number[] {
    return this.bounds;
  }
}

function formatMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${ms}ms`;
}

function renderSparkline(title: string, snap: HistogramSnapshot): string {
  const total = snap.total;
  if (total === 0) return `[hist] ${title} count=0`;
  let maxCount = 1;
  for (const count of snap.counts) {
    if (count > maxCount) maxCount = count;
  }
  const symbols = " .:-=+*#%@";
  let spark = "";
  for (const count of snap.counts) {
    const level = Math.round((count / maxCount) * (symbols.length - 1));
    spark += symbols[level] ?? symbols[0];
  }
  return `[hist] ${title} count=${total} p50<=${formatMs(snap.p50)} p95<=${formatMs(snap.p95)} max=${formatMs(
    snap.max
  )} |${spark}|`;
}

export class HistogramReporter {
  private timer: any | null = null;
  private running = false;
  private readonly intervalMs: number;
  private readonly hist: LatencyHistogramCollector;
  private legendPrinted = false;
  private readonly legend: string;

  constructor(hist: LatencyHistogramCollector, intervalMs = 60_000) {
    this.hist = hist;
    this.intervalMs = intervalMs;
    const bounds = this.hist.getBounds();
    const labels = bounds.map((b) => (b >= 1000 ? `${(b / 1000).toFixed(0)}s` : `${b}ms`));
    this.legend = ` bins<=${labels.join(",")},>${labels[labels.length - 1]}`;
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const snap = this.hist.snapshotAndReset();
      if (!this.legendPrinted) {
        // eslint-disable-next-line no-console
        console.log(`[hist]${this.legend}`);
        this.legendPrinted = true;
      }
      // eslint-disable-next-line no-console
      console.log(renderSparkline("writes", snap.writes));
      // eslint-disable-next-line no-console
      console.log(renderSparkline("reads", snap.reads));
    } finally {
      this.running = false;
    }
  }
}
