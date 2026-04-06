import { appendFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

type JsonRecord = Record<string, unknown>;

type HealthPayload = {
  ok?: boolean;
};

type ServerMemPayload = {
  ts?: string;
  process?: {
    rss_bytes?: number;
    heap_total_bytes?: number;
    heap_used_bytes?: number;
    external_bytes?: number;
    array_buffers_bytes?: number;
  };
  process_breakdown?: {
    source?: string;
    rss_anon_bytes?: number | null;
    rss_file_bytes?: number | null;
    rss_shmem_bytes?: number | null;
    js_managed_bytes?: number;
    js_external_non_array_buffers_bytes?: number;
    mapped_file_bytes?: number;
    sqlite_runtime_bytes?: number;
    unattributed_anon_bytes?: number | null;
    unattributed_rss_bytes?: number;
  };
  sqlite?: JsonRecord;
  gc?: JsonRecord;
  high_water?: JsonRecord;
  counters?: JsonRecord;
  runtime_counts?: JsonRecord;
  runtime_bytes?: JsonRecord;
  runtime_totals?: JsonRecord;
  top_streams?: JsonRecord;
};

type StreamDetailsPayload = {
  stream?: {
    name?: string;
    next_offset?: string;
    last_append_at?: string | null;
    total_size_bytes?: string;
    pending_rows?: string;
    pending_bytes?: string;
  };
  index_status?: {
    routing_key_index?: {
      configured?: boolean;
    };
    routing_key_lexicon?: {
      configured?: boolean;
    };
    search_families?: unknown[];
  };
};

type ServerDetailsPayload = {
  runtime?: {
    memory?: {
      pressure_active?: boolean;
    };
    uploads?: {
      pending_segments?: number;
    };
    concurrency?: {
      async_index?: {
        active?: number;
        queued?: number;
      };
    };
  };
};

function usage(): never {
  console.error(
    "usage: bun run scripts/monitor_remote_stream.ts --url <base-url> --stream <name> --output <jsonl-path> [--interval-ms 300000] [--stop-at-bytes <bytes>] [--rss-threshold-bytes <bytes>] [--once]"
  );
  process.exit(1);
}

function parseArgs(argv: string[]) {
  const values = new Map<string, string>();
  const flags = new Set<string>();
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (!arg.startsWith("--")) continue;
    const next = argv[index + 1];
    if (next != null && !next.startsWith("--")) {
      values.set(arg, next);
      index += 1;
    } else {
      flags.add(arg);
    }
  }
  return { values, flags };
}

function requireString(values: Map<string, string>, flag: string): string {
  const value = values.get(flag);
  if (value == null || value === "") usage();
  return value;
}

function numberArg(values: Map<string, string>, flag: string, fallback: number): number {
  const raw = values.get(flag);
  if (raw == null) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) usage();
  return Math.floor(value);
}

async function fetchJson(path: string): Promise<{ ok: boolean; status: number | null; body: unknown; error: string | null }> {
  try {
    const response = await fetch(path);
    const text = await response.text();
    let body: unknown = text;
    try {
      body = JSON.parse(text);
    } catch {
      // keep raw text
    }
    return {
      ok: response.ok,
      status: response.status,
      body,
      error: null,
    };
  } catch (error) {
    return {
      ok: false,
      status: null,
      body: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function toBigInt(value: unknown): bigint | null {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.floor(value));
  if (typeof value === "string" && value !== "") {
    try {
      return BigInt(value);
    } catch {
      return null;
    }
  }
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

function asRecord(value: unknown): JsonRecord | null {
  if (value != null && typeof value === "object" && !Array.isArray(value)) return value as JsonRecord;
  return null;
}

async function main() {
  const { values, flags } = parseArgs(process.argv.slice(2));
  const baseUrl = requireString(values, "--url").replace(/\/+$/, "");
  const stream = requireString(values, "--stream");
  const outputPath = resolve(requireString(values, "--output"));
  const intervalMs = numberArg(values, "--interval-ms", 5 * 60 * 1000);
  const stopAtBytes = BigInt(numberArg(values, "--stop-at-bytes", 10 * 1024 * 1024 * 1024));
  const rssThresholdBytes = BigInt(numberArg(values, "--rss-threshold-bytes", 1024 * 1024 * 1024));
  const once = flags.has("--once");

  await mkdir(dirname(outputPath), { recursive: true });

  let lastTotalSizeBytes: bigint | null = null;
  let lastNextOffset: bigint | null = null;

  for (;;) {
    const ts = new Date().toISOString();
    const [healthRes, memRes, serverDetailsRes, detailsRes] = await Promise.all([
      fetchJson(`${baseUrl}/health`),
      fetchJson(`${baseUrl}/v1/server/_mem`),
      fetchJson(`${baseUrl}/v1/server/_details`),
      fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_details`),
    ]);

    const healthBody = asRecord(healthRes.body) as HealthPayload | null;
    const memBody = asRecord(memRes.body) as ServerMemPayload | null;
    const serverDetailsBody = asRecord(serverDetailsRes.body) as ServerDetailsPayload | null;
    const detailsBody = asRecord(detailsRes.body) as StreamDetailsPayload | null;
    const streamBody = detailsBody?.stream;
    const indexStatus = detailsBody?.index_status;

    const rssBytes = asNumber(memBody?.process?.rss_bytes) ?? null;
    const heapUsedBytes = asNumber(memBody?.process?.heap_used_bytes) ?? null;
    const totalSizeBytes = toBigInt(streamBody?.total_size_bytes) ?? null;
    const nextOffset = toBigInt(streamBody?.next_offset) ?? null;
    const deltaTotalSizeBytes =
      totalSizeBytes != null && lastTotalSizeBytes != null ? totalSizeBytes - lastTotalSizeBytes : null;
    const deltaNextOffset = nextOffset != null && lastNextOffset != null ? nextOffset - lastNextOffset : null;

    lastTotalSizeBytes = totalSizeBytes;
    lastNextOffset = nextOffset;

    const runtimeCounts = asRecord(memBody?.runtime_counts);
    const runtimeBytes = asRecord(memBody?.runtime_bytes);
    const runtimeTotals = asRecord(memBody?.runtime_totals);
    const processBreakdown = memBody?.process_breakdown ?? null;

    const asyncIndexActive = asNumber(serverDetailsBody?.runtime?.concurrency?.async_index?.active) ?? null;
    const asyncIndexQueued = asNumber(serverDetailsBody?.runtime?.concurrency?.async_index?.queued) ?? null;
    const uploadPendingSegments = asNumber(serverDetailsBody?.runtime?.uploads?.pending_segments) ?? null;
    const pressureActive = typeof serverDetailsBody?.runtime?.memory?.pressure_active === "boolean" ? serverDetailsBody.runtime.memory.pressure_active : null;

    const anomalies: string[] = [];
    if (!(healthBody?.ok === true && healthRes.status === 200)) anomalies.push("health_not_ok");
    if (rssBytes != null && BigInt(Math.floor(rssBytes)) >= rssThresholdBytes) anomalies.push("rss_above_threshold");
    if (pressureActive === true) anomalies.push("memory_pressure_active");
    if ((indexStatus?.routing_key_index?.configured ?? false) === true) anomalies.push("unexpected_routing_index");
    if ((indexStatus?.routing_key_lexicon?.configured ?? false) === true) anomalies.push("unexpected_routing_lexicon");
    if (Array.isArray(indexStatus?.search_families) && indexStatus.search_families.length > 0) anomalies.push("unexpected_search_families");
    if (deltaNextOffset != null && deltaNextOffset <= 0n) anomalies.push("no_progress");

    const sample = {
      ts,
      stream,
      health_ok: healthBody?.ok === true && healthRes.status === 200,
      anomalies,
      health_status: healthRes.status,
      health_error: healthRes.error,
      health_body: healthRes.body,
      details_status: detailsRes.status,
      details_error: detailsRes.error,
      server_details_status: serverDetailsRes.status,
      server_details_error: serverDetailsRes.error,
      mem_status: memRes.status,
      mem_error: memRes.error,
      rss_bytes: rssBytes,
      heap_used_bytes: heapUsedBytes,
      memory_pressure: pressureActive,
      async_index_active: asyncIndexActive,
      async_index_queued: asyncIndexQueued,
      upload_pending_segments: uploadPendingSegments,
      next_offset: streamBody?.next_offset ?? null,
      last_append_at: streamBody?.last_append_at ?? null,
      total_size_bytes: streamBody?.total_size_bytes ?? null,
      pending_rows: streamBody?.pending_rows ?? null,
      pending_bytes: streamBody?.pending_bytes ?? null,
      routing_index_configured: indexStatus?.routing_key_index?.configured ?? null,
      routing_lexicon_configured: indexStatus?.routing_key_lexicon?.configured ?? null,
      search_family_count: Array.isArray(indexStatus?.search_families) ? indexStatus.search_families.length : null,
      delta_total_size_bytes: deltaTotalSizeBytes?.toString() ?? null,
      delta_next_offset: deltaNextOffset?.toString() ?? null,
      process_breakdown: processBreakdown,
      runtime_counts: runtimeCounts,
      runtime_bytes: runtimeBytes,
      runtime_totals: runtimeTotals,
      sqlite: memBody?.sqlite ?? null,
      gc: memBody?.gc ?? null,
      high_water: memBody?.high_water ?? null,
      counters: memBody?.counters ?? null,
      top_streams: memBody?.top_streams ?? null,
      server_mem: memRes.body,
      node_details: serverDetailsRes.body,
      server_details: detailsRes.body,
    };

    await appendFile(outputPath, `${JSON.stringify(sample)}\n`, "utf8");

    if (once) return;
    if (healthBody?.ok !== true || healthRes.status !== 200) return;
    if (totalSizeBytes != null && totalSizeBytes >= stopAtBytes) return;

    await Bun.sleep(intervalMs);
  }
}

await main();
