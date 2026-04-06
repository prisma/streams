import { readFileSync } from "node:fs";

type SampleLine = {
  ts?: string;
  kind?: string;
  scope?: string;
  reason?: string;
  process_memory_usage?: {
    rss?: number;
    heapTotal?: number;
    heapUsed?: number;
    external?: number;
    arrayBuffers?: number;
  };
  jsc_heap_stats?: {
    heapSize?: number;
    heapCapacity?: number;
    extraMemorySize?: number;
    objectCount?: number;
  } | null;
  jsc_memory_usage?: {
    current?: number;
    peak?: number;
    currentCommit?: number;
    peakCommit?: number;
    pageFaults?: number;
  } | null;
  primary_phase?: {
    label?: string;
    duration_ms?: number;
    meta?: Record<string, unknown>;
  } | null;
};

type PeakRecord = {
  path: string;
  ts: string;
  scope: string;
  primary_phase: string | null;
  reason: string | null;
  process_rss_bytes: number;
  process_heap_used_bytes: number | null;
  jsc_heap_size_bytes: number | null;
  jsc_current_bytes: number | null;
};

function usage(): never {
  console.error("usage: bun run scripts/summarize_memory_sampler.ts <jsonl-path> [more-paths...]");
  process.exit(1);
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

const args = process.argv.slice(2);
if (args.length === 0) usage();

let firstTs: string | null = null;
let lastTs: string | null = null;
let sampleCount = 0;
let peakRss = -1;
let peakHeapUsed = -1;
let peakJscHeapSize = -1;
let peakJscCurrent = -1;
let lastRss: number | null = null;
let lastHeapUsed: number | null = null;
let lastJscHeapSize: number | null = null;
let lastJscCurrent: number | null = null;
let lastPrimaryPhase: string | null = null;
let lastReason: string | null = null;
const byPhase = new Map<string, { sample_count: number; peak_rss_bytes: number; peak_jsc_heap_size_bytes: number; peak_jsc_current_bytes: number }>();
const byScope = new Map<
  string,
  {
    sample_count: number;
    peak_rss_bytes: number;
    peak_heap_used_bytes: number;
    peak_jsc_heap_size_bytes: number;
    peak_jsc_current_bytes: number;
  }
>();
const topRssSamples: PeakRecord[] = [];

function recordTop(record: PeakRecord): void {
  topRssSamples.push(record);
  topRssSamples.sort((left, right) => right.process_rss_bytes - left.process_rss_bytes);
  if (topRssSamples.length > 10) topRssSamples.length = 10;
}

for (const path of args) {
  const text = readFileSync(path, "utf8");
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line === "") continue;
    let parsed: SampleLine;
    try {
      parsed = JSON.parse(line) as SampleLine;
    } catch {
      continue;
    }
    if (parsed.kind !== "sample") continue;
    const rss = asNumber(parsed.process_memory_usage?.rss);
    if (rss == null) continue;
    sampleCount += 1;
    if (firstTs == null && parsed.ts) firstTs = parsed.ts;
    if (parsed.ts) lastTs = parsed.ts;
    peakRss = Math.max(peakRss, rss);
    peakHeapUsed = Math.max(peakHeapUsed, asNumber(parsed.process_memory_usage?.heapUsed) ?? -1);
    peakJscHeapSize = Math.max(peakJscHeapSize, asNumber(parsed.jsc_heap_stats?.heapSize) ?? -1);
    peakJscCurrent = Math.max(peakJscCurrent, asNumber(parsed.jsc_memory_usage?.current) ?? -1);
    lastRss = rss;
    lastHeapUsed = asNumber(parsed.process_memory_usage?.heapUsed);
    lastJscHeapSize = asNumber(parsed.jsc_heap_stats?.heapSize);
    lastJscCurrent = asNumber(parsed.jsc_memory_usage?.current);
    lastPrimaryPhase = parsed.primary_phase?.label ?? null;
    lastReason = parsed.reason ?? null;

    const scope = parsed.scope ?? "unknown";
    const currentScope = byScope.get(scope) ?? {
      sample_count: 0,
      peak_rss_bytes: -1,
      peak_heap_used_bytes: -1,
      peak_jsc_heap_size_bytes: -1,
      peak_jsc_current_bytes: -1,
    };
    currentScope.sample_count += 1;
    currentScope.peak_rss_bytes = Math.max(currentScope.peak_rss_bytes, rss);
    currentScope.peak_heap_used_bytes = Math.max(
      currentScope.peak_heap_used_bytes,
      asNumber(parsed.process_memory_usage?.heapUsed) ?? -1
    );
    currentScope.peak_jsc_heap_size_bytes = Math.max(
      currentScope.peak_jsc_heap_size_bytes,
      asNumber(parsed.jsc_heap_stats?.heapSize) ?? -1
    );
    currentScope.peak_jsc_current_bytes = Math.max(
      currentScope.peak_jsc_current_bytes,
      asNumber(parsed.jsc_memory_usage?.current) ?? -1
    );
    byScope.set(scope, currentScope);

    const primaryPhase = parsed.primary_phase?.label ?? "idle";
    const currentPhase = byPhase.get(primaryPhase) ?? {
      sample_count: 0,
      peak_rss_bytes: -1,
      peak_jsc_heap_size_bytes: -1,
      peak_jsc_current_bytes: -1,
    };
    currentPhase.sample_count += 1;
    currentPhase.peak_rss_bytes = Math.max(currentPhase.peak_rss_bytes, rss);
    currentPhase.peak_jsc_heap_size_bytes = Math.max(
      currentPhase.peak_jsc_heap_size_bytes,
      asNumber(parsed.jsc_heap_stats?.heapSize) ?? -1
    );
    currentPhase.peak_jsc_current_bytes = Math.max(
      currentPhase.peak_jsc_current_bytes,
      asNumber(parsed.jsc_memory_usage?.current) ?? -1
    );
    byPhase.set(primaryPhase, currentPhase);

    recordTop({
      path,
      ts: parsed.ts ?? "",
      scope: parsed.scope ?? "unknown",
      primary_phase: parsed.primary_phase?.label ?? null,
      reason: parsed.reason ?? null,
      process_rss_bytes: rss,
      process_heap_used_bytes: asNumber(parsed.process_memory_usage?.heapUsed),
      jsc_heap_size_bytes: asNumber(parsed.jsc_heap_stats?.heapSize),
      jsc_current_bytes: asNumber(parsed.jsc_memory_usage?.current),
    });
  }
}

const phaseSummary = Array.from(byPhase.entries())
  .sort((left, right) => right[1].peak_rss_bytes - left[1].peak_rss_bytes)
  .map(([phase, summary]) => ({
    phase,
    ...summary,
  }));

const scopeSummary = Array.from(byScope.entries())
  .sort((left, right) => right[1].peak_rss_bytes - left[1].peak_rss_bytes)
  .map(([scope, summary]) => ({
    scope,
    ...summary,
  }));

process.stdout.write(
  `${JSON.stringify(
    {
      files: args,
      first_ts: firstTs,
      last_ts: lastTs,
      sample_count: sampleCount,
      peak_process_rss_bytes: peakRss >= 0 ? peakRss : null,
      peak_process_heap_used_bytes: peakHeapUsed >= 0 ? peakHeapUsed : null,
      peak_jsc_heap_size_bytes: peakJscHeapSize >= 0 ? peakJscHeapSize : null,
      peak_jsc_current_bytes: peakJscCurrent >= 0 ? peakJscCurrent : null,
      last_process_rss_bytes: lastRss,
      last_process_heap_used_bytes: lastHeapUsed,
      last_jsc_heap_size_bytes: lastJscHeapSize,
      last_jsc_current_bytes: lastJscCurrent,
      last_primary_phase: lastPrimaryPhase,
      last_reason: lastReason,
      by_scope: scopeSummary,
      by_primary_phase: phaseSummary,
      top_rss_samples: topRssSamples,
    },
    null,
    2
  )}\n`
);
