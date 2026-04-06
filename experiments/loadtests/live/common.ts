import { dsError } from "../../../src/util/ds_error.ts";
export type ParsedArgs = {
  values: Record<string, string>;
  flags: Set<string>;
  positionals: string[];
};

export function parseArgs(argv: string[]): ParsedArgs {
  const values: Record<string, string> = {};
  const flags = new Set<string>();
  const positionals: string[] = [];

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i] ?? "";
    if (!a.startsWith("--")) {
      positionals.push(a);
      continue;
    }
    const raw = a.slice(2);
    if (raw.includes("=")) {
      const [k, v] = raw.split("=", 2);
      const key = k.trim();
      if (!key) continue;
      values[key] = v ?? "";
      continue;
    }
    const key = raw.trim();
    if (!key) continue;
    const next = argv[i + 1] ?? null;
    if (next != null && !next.startsWith("--")) {
      values[key] = next;
      i++;
      continue;
    }
    flags.add(key);
  }

  return { values, flags, positionals };
}

export function hasFlag(args: ParsedArgs, name: string): boolean {
  return args.flags.has(name);
}

export function stringArg(args: ParsedArgs, name: string, fallback: string): string {
  const v = args.values[name];
  return v === undefined ? fallback : v;
}

export function optionalStringArg(args: ParsedArgs, name: string): string | null {
  const v = args.values[name];
  return v === undefined ? null : v;
}

export function intArg(args: ParsedArgs, name: string, fallback: number): number {
  const raw = args.values[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n)) throw dsError(`invalid --${name}: ${raw}`);
  return n;
}

export function floatArg(args: ParsedArgs, name: string, fallback: number): number {
  const raw = args.values[name];
  if (raw === undefined) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw dsError(`invalid --${name}: ${raw}`);
  return n;
}

export function csvArg(args: ParsedArgs, name: string, fallback: string[]): string[] {
  const raw = args.values[name];
  if (raw === undefined) return fallback;
  const out = raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  return out;
}

export function csvIntArg(args: ParsedArgs, name: string, fallback: number[]): number[] {
  const raw = args.values[name];
  if (raw === undefined) return fallback;
  const out = raw
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && Number.isInteger(n) && n >= 0);
  if (out.length === 0) throw dsError(`invalid --${name}: ${raw}`);
  return out;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

export async function fetchJson(url: string, init: RequestInit): Promise<any> {
  const deadline = Date.now() + 30_000;
  let attempt = 0;
  const abortSignal = init.signal;
  for (;;) {
    let r: Response;
    try {
      r = await fetch(url, init);
    } catch (e) {
      if (abortSignal?.aborted || (e && typeof e === "object" && (e as any).name === "AbortError")) throw e;
      if (Date.now() > deadline) throw e;
      const backoff = Math.min(1000, 50 * 2 ** attempt);
      attempt++;
      await sleep(backoff);
      continue;
    }
    const text = await r.text().catch(() => "");
    if (r.status === 429 && Date.now() <= deadline) {
      // Overloaded; retry with backoff.
      const backoff = Math.min(1000, 50 * 2 ** attempt);
      attempt++;
      await sleep(backoff);
      continue;
    }
    if (!r.ok) throw dsError(`HTTP ${r.status} ${url}: ${text}`);
    if (text === "") return null;
    return JSON.parse(text);
  }
}

export async function deleteStream(baseUrl: string, stream: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  let attempt = 0;
  for (;;) {
    let r: Response;
    try {
      r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, { method: "DELETE" });
    } catch (e) {
      if (Date.now() > deadline) throw e;
      const backoff = Math.min(1000, 50 * 2 ** attempt);
      attempt++;
      await sleep(backoff);
      continue;
    }
    if (r.status === 404) return;
    if (r.status === 429 && Date.now() <= deadline) {
      const backoff = Math.min(1000, 50 * 2 ** attempt);
      attempt++;
      await sleep(backoff);
      continue;
    }
    if (!r.ok && r.status !== 204) {
      const t = await r.text().catch(() => "");
      throw dsError(`failed to delete stream ${stream}: ${r.status} ${t}`);
    }
    return;
  }
}

export async function ensureStream(baseUrl: string, stream: string, contentType = "application/json"): Promise<void> {
  const deadline = Date.now() + 120_000;
  let attempt = 0;
  for (;;) {
    let r: Response;
    try {
      r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
        method: "PUT",
        headers: { "content-type": contentType },
      });
    } catch (e) {
      if (Date.now() > deadline) throw e;
      const backoff = Math.min(1000, 50 * 2 ** attempt);
      attempt++;
      await sleep(backoff);
      continue;
    }
    if (r.status === 429 && Date.now() <= deadline) {
      const backoff = Math.min(1000, 50 * 2 ** attempt);
      attempt++;
      await sleep(backoff);
      continue;
    }
    if (!r.ok && r.status !== 200 && r.status !== 201) {
      const t = await r.text().catch(() => "");
      throw dsError(`failed to create stream ${stream}: ${r.status} ${t}`);
    }
    return;
  }
}

export async function ensureSchemaAndProfile(baseUrl: string, stream: string, profile: any, schema?: any): Promise<void> {
  const reg = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, { method: "GET" });
  const currentVersion = typeof reg?.currentVersion === "number" ? reg.currentVersion : 0;
  if (currentVersion === 0) {
    await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ schema: schema ?? { type: "object", additionalProperties: true } }),
    });
  }
  await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile,
    }),
  });
}

export async function waitForTouchReady(baseUrl: string, stream: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
    if (r.status === 404) {
      if (Date.now() > deadline) throw dsError("touch/meta not ready (timeout)");
      await sleep(200);
      continue;
    }
    if (!r.ok) {
      const t = await r.text();
      throw dsError(`touch/meta failed: ${r.status} ${t}`);
    }
    return;
  }
}

export type TouchMetaMemory = {
  mode: "memory";
  cursor: string;
  epoch: string;
  generation: number;
  bucketMs: number;
  coalesceMs?: number;
  filterSize: number;
  k: number;
  pendingKeys: number;
  overflowBuckets: number;
  activeWaiters: number;
  bucketMaxSourceOffsetSeq?: string;
  lastFlushAtMs?: number;
  flushIntervalMsMaxLast10s?: number;
  flushIntervalMsP95Last10s?: number;
  coarseIntervalMs: number;
  touchCoalesceWindowMs: number;
  activeTemplates: number;
  lagSourceOffsets?: number;
  touchMode?: "idle" | "fine" | "restricted" | "coarseOnly";
  walScannedThrough?: string | null;
  hotFineKeys?: number;
  hotTemplates?: number;
  hotFineKeysActive?: number;
  hotFineKeysGrace?: number;
  hotTemplatesActive?: number;
  hotTemplatesGrace?: number;
  fineWaitersActive?: number;
  coarseWaitersActive?: number;
  broadFineWaitersActive?: number;
  hotKeyFilteringEnabled?: boolean;
  hotTemplateFilteringEnabled?: boolean;
  scanRowsTotal?: number;
  scanBatchesTotal?: number;
  scannedButEmitted0BatchesTotal?: number;
  processedThroughDeltaTotal?: number;
  touchesEmittedTotal?: number;
  touchesTableTotal?: number;
  touchesTemplateTotal?: number;
  fineTouchesDroppedDueToBudgetTotal?: number;
  fineTouchesSkippedColdTemplateTotal?: number;
  fineTouchesSkippedColdKeyTotal?: number;
  fineTouchesSkippedTemplateBucketTotal?: number;
  waitTouchedTotal?: number;
  waitTimeoutTotal?: number;
  waitStaleTotal?: number;
  journalFlushesTotal?: number;
  journalNotifyWakeupsTotal?: number;
  journalNotifyWakeMsTotal?: number;
  journalNotifyWakeMsMax?: number;
  journalTimeoutsFiredTotal?: number;
  journalTimeoutSweepMsTotal?: number;
};

export type TouchMeta = TouchMetaMemory;

export function isMemoryTouchMeta(meta: TouchMeta): meta is TouchMetaMemory {
  return (meta as any)?.mode === "memory";
}

export async function getTouchMeta(baseUrl: string, stream: string): Promise<TouchMeta> {
  const meta = await fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
  return meta as TouchMeta;
}

export type TemplateDecl = { entity: string; fields: Array<{ name: string; encoding: "string" | "int64" | "bool" | "datetime" | "bytes" }> };

export async function activateTemplatesChunked(args: {
  baseUrl: string;
  stream: string;
  templates: TemplateDecl[];
  inactivityTtlMs: number;
  chunkSize?: number;
  signal?: AbortSignal;
}): Promise<{ activated: any[]; denied: any[] }> {
  const chunkSize = Math.max(1, Math.min(256, args.chunkSize ?? 256));
  const activated: any[] = [];
  const denied: any[] = [];
  for (let i = 0; i < args.templates.length; i += chunkSize) {
    const slice = args.templates.slice(i, i + chunkSize);
    const res = await fetchJson(`${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}/touch/templates/activate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ templates: slice, inactivityTtlMs: args.inactivityTtlMs }),
      signal: args.signal,
    });
    if (Array.isArray(res?.activated)) activated.push(...res.activated);
    if (Array.isArray(res?.denied)) denied.push(...res.denied);
  }
  return { activated, denied };
}

export type JsonStreamTail = {
  stop: () => void;
  task: Promise<void>;
};

export function startTailJsonStream(opts: {
  baseUrl: string;
  stream: string;
  offset: string;
  timeoutMs: number;
  onEvents: (events: any[], headers: Headers) => void | Promise<void>;
  onError?: (err: unknown) => void;
}): JsonStreamTail {
  const controller = new AbortController();
  const safeOnError = (err: unknown) => {
    try {
      opts.onError?.(err);
    } catch {
      // ignore
    }
  };

  // Never reject: load tests should keep going even if the metrics tailer flakes.
  const task = (async () => {
    let offset = opts.offset;
    const encodedStream = encodeURIComponent(opts.stream);
    while (!controller.signal.aborted) {
      let r: Response | null = null;
      try {
        const url = `${opts.baseUrl}/v1/stream/${encodedStream}?offset=${encodeURIComponent(offset)}&format=json&live=long-poll&timeout_ms=${Math.max(0, Math.floor(opts.timeoutMs))}`;
        r = await fetch(url, { method: "GET", signal: controller.signal });
      } catch (e) {
        if (controller.signal.aborted) break;
        safeOnError(e);
        await sleep(200);
        continue;
      }
      if (!r.ok && r.status !== 204) {
        const text = await r.text().catch(() => "");
        safeOnError(dsError(`tail failed: HTTP ${r.status}: ${text}`));
        await sleep(200);
        continue;
      }

      const next = r.headers.get("stream-next-offset");
      if (next) offset = next;

      if (r.status === 204) continue;
      let json: any;
      try {
        json = await r.json();
      } catch (e) {
        safeOnError(e);
        continue;
      }
      const events = Array.isArray(json) ? json : [];
      try {
        await opts.onEvents(events, r.headers);
      } catch (e) {
        safeOnError(e);
      }
    }
  })().catch((e) => {
    if (controller.signal.aborted) return;
    safeOnError(e);
  });

  return {
    stop: () => controller.abort(),
    task,
  };
}
