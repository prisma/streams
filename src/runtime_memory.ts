import { readFileSync } from "node:fs";

export type RuntimeMemorySubsystemGroups = {
  heap_estimates: Record<string, number>;
  mapped_files: Record<string, number>;
  disk_caches: Record<string, number>;
  configured_budgets: Record<string, number>;
  pipeline_buffers: Record<string, number>;
  sqlite_runtime: Record<string, number>;
  counts: Record<string, number>;
  [kind: string]: Record<string, number>;
};

export type RuntimeMemoryProcessUsage = {
  rss_bytes: number;
  heap_total_bytes: number;
  heap_used_bytes: number;
  external_bytes: number;
  array_buffers_bytes: number;
};

export type RuntimeMemoryProcessBreakdown = {
  source: "linux_proc_status" | "process_memory_usage_only";
  rss_anon_bytes: number | null;
  rss_file_bytes: number | null;
  rss_shmem_bytes: number | null;
  js_managed_bytes: number;
  js_external_non_array_buffers_bytes: number;
  mapped_file_bytes: number;
  sqlite_runtime_bytes: number;
  unattributed_anon_bytes: number | null;
  unattributed_rss_bytes: number;
};

export type SqliteRuntimeMemoryStats = {
  available: boolean;
  source: "sqlite3_status64" | "unavailable";
  memory_used_bytes: number;
  memory_highwater_bytes: number;
  pagecache_used_slots: number;
  pagecache_used_slots_highwater: number;
  pagecache_overflow_bytes: number;
  pagecache_overflow_highwater_bytes: number;
  malloc_count: number;
  malloc_count_highwater: number;
  open_connections: number;
  prepared_statements: number;
};

export type RuntimeGcStats = {
  forced_gc_count: number;
  forced_gc_reclaimed_bytes_total: number;
  last_forced_gc_at_ms: number | null;
  last_forced_gc_before_bytes: number | null;
  last_forced_gc_after_bytes: number | null;
  last_forced_gc_reclaimed_bytes: number | null;
  heap_snapshots_written: number;
  last_heap_snapshot_at_ms: number | null;
};

export type RuntimeMemoryTotals = {
  heap_estimate_bytes: number;
  mapped_file_bytes: number;
  disk_cache_bytes: number;
  configured_budget_bytes: number;
  pipeline_buffer_bytes: number;
  sqlite_runtime_bytes: number;
};

export type RuntimeMemorySubsystemSnapshot = {
  subsystems: RuntimeMemorySubsystemGroups;
  totals: RuntimeMemoryTotals;
};

export type RuntimeMemorySnapshot = {
  process: RuntimeMemoryProcessUsage;
  process_breakdown: RuntimeMemoryProcessBreakdown;
  sqlite: SqliteRuntimeMemoryStats;
  gc: RuntimeGcStats;
} & RuntimeMemorySubsystemSnapshot;

export type RuntimeHighWaterMark = {
  value: number;
  at: string | null;
};

export type RuntimeMemoryHighWaterSnapshot = {
  process: Record<string, RuntimeHighWaterMark>;
  process_breakdown: Record<string, RuntimeHighWaterMark>;
  sqlite: Record<string, RuntimeHighWaterMark>;
  runtime_bytes: Record<string, Record<string, RuntimeHighWaterMark>>;
  runtime_totals: Record<string, RuntimeHighWaterMark>;
};

export type LinuxStatusRssBreakdown = {
  rss_anon_bytes: number;
  rss_file_bytes: number;
  rss_shmem_bytes: number;
};

type CachedLinuxStatusBreakdown = {
  at_ms: number;
  pid: number;
  value: LinuxStatusRssBreakdown | null;
};

let cachedLinuxStatusBreakdown: CachedLinuxStatusBreakdown | null = null;

export function sumRuntimeMemoryValues(values: Record<string, number>): number {
  let total = 0;
  for (const value of Object.values(values)) total += Math.max(0, Math.floor(value));
  return total;
}

export function parseLinuxStatusRssBreakdown(status: string): LinuxStatusRssBreakdown | null {
  let anon: number | null = null;
  let file: number | null = null;
  let shmem: number | null = null;
  for (const line of status.split(/\r?\n/)) {
    let match = line.match(/^RssAnon:\s+([0-9]+)\s+kB$/i);
    if (match) {
      anon = Number(match[1]) * 1024;
      continue;
    }
    match = line.match(/^RssFile:\s+([0-9]+)\s+kB$/i);
    if (match) {
      file = Number(match[1]) * 1024;
      continue;
    }
    match = line.match(/^RssShmem:\s+([0-9]+)\s+kB$/i);
    if (match) {
      shmem = Number(match[1]) * 1024;
    }
  }
  if (anon == null || file == null || shmem == null) return null;
  return {
    rss_anon_bytes: Math.max(0, Math.floor(anon)),
    rss_file_bytes: Math.max(0, Math.floor(file)),
    rss_shmem_bytes: Math.max(0, Math.floor(shmem)),
  };
}

export function readLinuxStatusRssBreakdown(ttlMs = 1_000): LinuxStatusRssBreakdown | null {
  if (process.platform !== "linux") return null;
  const now = Date.now();
  if (
    cachedLinuxStatusBreakdown &&
    cachedLinuxStatusBreakdown.pid === process.pid &&
    now - cachedLinuxStatusBreakdown.at_ms < Math.max(0, ttlMs)
  ) {
    return cachedLinuxStatusBreakdown.value;
  }
  let value: LinuxStatusRssBreakdown | null = null;
  try {
    value = parseLinuxStatusRssBreakdown(readFileSync("/proc/self/status", "utf8"));
  } catch {
    value = null;
  }
  cachedLinuxStatusBreakdown = {
    at_ms: now,
    pid: process.pid,
    value,
  };
  return value;
}

export function buildProcessMemoryBreakdown(args: {
  process: RuntimeMemoryProcessUsage;
  mappedFileBytes: number;
  sqliteRuntimeBytes: number;
}): RuntimeMemoryProcessBreakdown {
  const rssBreakdown = readLinuxStatusRssBreakdown();
  const jsManagedBytes = Math.max(0, Math.floor(args.process.heap_used_bytes + args.process.external_bytes));
  const jsExternalNonArrayBuffersBytes = Math.max(
    0,
    Math.floor(args.process.external_bytes - args.process.array_buffers_bytes)
  );
  const mappedFileBytes = Math.max(0, Math.floor(args.mappedFileBytes));
  const sqliteRuntimeBytes = Math.max(0, Math.floor(args.sqliteRuntimeBytes));
  const unattributedRssBytes = Math.max(
    0,
    Math.floor(args.process.rss_bytes - jsManagedBytes - mappedFileBytes - sqliteRuntimeBytes)
  );
  const unattributedAnonBytes =
    rssBreakdown == null
      ? null
      : Math.max(0, Math.floor(rssBreakdown.rss_anon_bytes - jsManagedBytes - sqliteRuntimeBytes));
  return {
    source: rssBreakdown ? "linux_proc_status" : "process_memory_usage_only",
    rss_anon_bytes: rssBreakdown?.rss_anon_bytes ?? null,
    rss_file_bytes: rssBreakdown?.rss_file_bytes ?? null,
    rss_shmem_bytes: rssBreakdown?.rss_shmem_bytes ?? null,
    js_managed_bytes: jsManagedBytes,
    js_external_non_array_buffers_bytes: jsExternalNonArrayBuffersBytes,
    mapped_file_bytes: mappedFileBytes,
    sqlite_runtime_bytes: sqliteRuntimeBytes,
    unattributed_anon_bytes: unattributedAnonBytes,
    unattributed_rss_bytes: unattributedRssBytes,
  };
}
