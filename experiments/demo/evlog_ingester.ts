import { hasFlag, parseIntArg, parseStringArg, sleep } from "./common";
import { dsError } from "../../src/util/ds_error.ts";

export const EVLOG_DEFAULT_BASE_URL = "http://127.0.0.1:8787";
export const EVLOG_DEFAULT_STREAM = "evlog-1";
export const EVLOG_DEFAULT_BATCH_SIZE = 1_000;
export const EVLOG_DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
export const EVLOG_DEFAULT_RETRY_DELAY_MS = 1_000;
export const EVLOG_DEFAULT_REPORT_EVERY_MS = 5_000;

const SERVICES = [
  "auth",
  "billing",
  "catalog",
  "checkout",
  "fulfillment",
  "gateway",
  "identity",
  "inventory",
  "mailer",
  "mobile-api",
  "notifications",
  "orders",
  "payments",
  "pricing",
  "search",
  "shipping",
] as const;
const ENVIRONMENTS = ["prod", "staging", "dev"] as const;
const REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"] as const;
const METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"] as const;
const PATH_PREFIXES = [
  "/api/cart",
  "/api/checkout",
  "/api/customers",
  "/api/inventory",
  "/api/invoices",
  "/api/orders",
  "/api/payments",
  "/api/search",
  "/api/session",
  "/api/shipments",
] as const;
const MESSAGE_TEMPLATES = [
  "request completed successfully",
  "request failed downstream dependency",
  "request timed out waiting for upstream",
  "request hit validation guardrail",
  "request triggered retry budget",
  "request saturated queue depth",
  "request missed cache warm path",
  "request exceeded policy threshold",
] as const;
const WHY_TEMPLATES = [
  "issuer declined the authorization attempt",
  "inventory reservation expired before commit",
  "upstream dependency returned an invalid payload",
  "circuit breaker opened after repeated timeouts",
  "tenant quota exceeded configured limit",
  "retry budget was exhausted for the request",
  "request payload failed schema validation",
  "database replica lag exceeded read SLA",
] as const;
const FIX_TEMPLATES = [
  "retry on a different payment instrument",
  "replay after refreshing the cached resource",
  "route the request to a healthy region",
  "defer the job until quota resets",
  "fall back to the slower but safer code path",
  "rebuild the request from the latest state",
  "inspect the downstream release candidate",
  "increase the queue worker pool temporarily",
] as const;
const ERROR_MESSAGES = [
  "issuer-declined",
  "inventory-stale-read",
  "lease-timeout",
  "payload-schema-mismatch",
  "quota-exhausted",
  "read-replica-lagged",
  "search-backend-timeout",
  "shipping-rate-unavailable",
] as const;

const EVLOG_PROFILE_ENVELOPE = {
  apiVersion: "durable.streams/profile/v1",
  profile: { kind: "evlog" },
} as const;

export type EvlogEvent = {
  timestamp: string;
  service: string;
  environment: string;
  version: string;
  region: string;
  requestId: string;
  traceId: string;
  spanId: string;
  method: string;
  path: string;
  status: number;
  duration: number;
  message: string;
  why?: string;
  fix?: string;
  link?: string;
  sampling: {
    kept: boolean;
    rate: number;
  };
  context: {
    error?: { message: string };
    buildId: string;
    host: string;
    retryCount: number;
    tenant: string;
    userId: string;
  };
};

export type EvlogIngestSummary = {
  stream: string;
  startId: number;
  nextId: number;
  appendedEvents: number;
  appendedBytes: number;
  batches: number;
};

export type EvlogIngestRunOptions = {
  fetchImpl?: typeof fetch;
};

type ProfileResource = {
  profile?: {
    kind?: string;
  };
};

type StreamDetails = {
  stream?: {
    next_offset?: string;
  };
};

function mulberry32Step(state: number): [number, number] {
  const next = (state + 0x6d2b79f5) >>> 0;
  let t = Math.imul(next ^ (next >>> 15), next | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return [((t ^ (t >>> 14)) >>> 0) >>> 0, next];
}

function seedForId(id: number, salt: number): number {
  return (Math.imul(id >>> 0, 2654435761) ^ salt) >>> 0;
}

function randomBase36String(seed: number, len: number): string {
  let out = "";
  let state = seed >>> 0;
  while (out.length < len) {
    const [value, nextState] = mulberry32Step(state);
    state = nextState;
    out += value.toString(36).padStart(7, "0");
  }
  return out.slice(0, len);
}

function pick<T>(values: readonly T[], state: number): T {
  return values[state % values.length]!;
}

function buildPath(service: string, state: number): string {
  const prefix = pick(PATH_PREFIXES, state);
  const suffix = randomBase36String(seedForId(state + 1, 909), 12);
  return `${prefix}/${service}/${suffix}`;
}

function buildStatus(state: number): number {
  const roll = state % 100;
  if (roll < 55) return 200;
  if (roll < 65) return 201;
  if (roll < 73) return 204;
  if (roll < 80) return 400;
  if (roll < 85) return 404;
  if (roll < 89) return 409;
  if (roll < 93) return 429;
  if (roll < 97) return 500;
  return 503;
}

function buildDurationMs(state: number): number {
  return Number(((state % 9_000) / 3.7 + 5).toFixed(3));
}

export function buildEvlogEvent(id: number, timestamp: string, salt = 1): EvlogEvent {
  let state = seedForId(id, salt);
  const next = () => {
    const [value, nextState] = mulberry32Step(state);
    state = nextState;
    return value;
  };

  const requestGroup = Math.floor(id / 4);
  const service = pick(SERVICES, next());
  const environment = pick(ENVIRONMENTS, next());
  const region = pick(REGIONS, next());
  const method = pick(METHODS, next());
  const status = buildStatus(next());
  const duration = buildDurationMs(next());
  const message = `${pick(MESSAGE_TEMPLATES, next())} for ${service}`;
  const why = status >= 400 ? pick(WHY_TEMPLATES, next()) : undefined;
  const fix = status >= 400 ? pick(FIX_TEMPLATES, next()) : undefined;
  const errorMessage = status >= 400 ? pick(ERROR_MESSAGES, next()) : undefined;

  return {
    timestamp,
    service,
    environment,
    version: `2026.${String((id % 12) + 1).padStart(2, "0")}.${String((id % 28) + 1).padStart(2, "0")}`,
    region,
    requestId: `req_${id.toString(36)}_${randomBase36String(next(), 8)}`,
    traceId: `trace_${requestGroup.toString(36)}_${randomBase36String(seedForId(requestGroup, 404), 10)}`,
    spanId: `span_${id.toString(36)}_${randomBase36String(next(), 8)}`,
    method,
    path: buildPath(service, next()),
    status,
    duration,
    message,
    why,
    fix,
    link: status >= 400 ? `https://runbooks.internal/${service}/${status}` : undefined,
    sampling: {
      kept: (next() & 1) === 0,
      rate: Number((((next() % 900) + 100) / 1000).toFixed(3)),
    },
    context: {
      ...(errorMessage ? { error: { message: errorMessage } } : {}),
      buildId: `build_${randomBase36String(next(), 10)}`,
      host: `host-${randomBase36String(next(), 6)}`,
      retryCount: next() % 5,
      tenant: `tenant_${(next() % 200).toString().padStart(3, "0")}`,
      userId: `user_${randomBase36String(next(), 10)}`,
    },
  };
}

async function fetchJson(fetchImpl: typeof fetch, url: string, init: RequestInit = {}): Promise<any> {
  const response = await fetchImpl(url, init);
  const text = await response.text();
  if (!response.ok) {
    throw dsError(`HTTP ${response.status} ${url}: ${text}`);
  }
  if (text === "") return null;
  return JSON.parse(text);
}

function parseRetryAfterMs(value: string | null): number {
  if (value == null || value.trim() === "") return EVLOG_DEFAULT_RETRY_DELAY_MS;
  const trimmed = value.trim();
  const seconds = Number(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.max(0, Math.ceil(seconds * 1000));
  const whenMs = Date.parse(trimmed);
  if (Number.isFinite(whenMs)) return Math.max(0, whenMs - Date.now());
  return EVLOG_DEFAULT_RETRY_DELAY_MS;
}

async function fetchWithTimeout(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const signal = AbortSignal.timeout(timeoutMs);
  return fetchImpl(url, { ...init, signal });
}

async function deleteStreamIfExists(fetchImpl: typeof fetch, baseUrl: string, stream: string): Promise<void> {
  const response = await fetchImpl(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, { method: "DELETE" });
  if (response.status === 404) return;
  if (!response.ok) {
    throw dsError(`failed to delete stream ${stream}: HTTP ${response.status} ${await response.text()}`);
  }
}

async function ensureEvlogProfile(fetchImpl: typeof fetch, baseUrl: string, stream: string): Promise<void> {
  const url = `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_profile`;
  const current = (await fetchJson(fetchImpl, url, { method: "GET" })) as ProfileResource | null;
  if (current?.profile?.kind === "evlog") return;
  await fetchJson(fetchImpl, url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(EVLOG_PROFILE_ENVELOPE),
  });
}

async function ensureEvlogStream(
  fetchImpl: typeof fetch,
  baseUrl: string,
  stream: string,
  opts: { reset: boolean }
): Promise<void> {
  if (opts.reset) {
    await deleteStreamIfExists(fetchImpl, baseUrl, stream);
  }
  const createResponse = await fetchImpl(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: "",
  });
  if (!createResponse.ok) {
    throw dsError(`failed to create stream ${stream}: HTTP ${createResponse.status} ${await createResponse.text()}`);
  }
  await ensureEvlogProfile(fetchImpl, baseUrl, stream);
}

async function fetchNextOffset(fetchImpl: typeof fetch, baseUrl: string, stream: string): Promise<number> {
  const details = (await fetchJson(
    fetchImpl,
    `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_details`,
    { method: "GET" }
  )) as StreamDetails;
  const nextOffsetRaw = details.stream?.next_offset ?? "0";
  const nextOffset = BigInt(nextOffsetRaw);
  const asNumber = Number(nextOffset);
  if (!Number.isSafeInteger(asNumber) || asNumber < 0) {
    throw dsError(`next_offset is not a safe integer for evlog replay ids: ${nextOffsetRaw}`);
  }
  return asNumber;
}

async function appendBatch(
  fetchImpl: typeof fetch,
  baseUrl: string,
  stream: string,
  batchBody: string,
  requestTimeoutMs: number,
  retryDelayMs: number
): Promise<void> {
  const url = `${baseUrl}/v1/stream/${encodeURIComponent(stream)}`;
  for (;;) {
    let response: Response;
    try {
      response = await fetchWithTimeout(
        fetchImpl,
        url,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: batchBody,
        },
        requestTimeoutMs
      );
    } catch (error) {
      process.stderr.write(
        `[evlog-ingester] append error on ${stream}; retrying in ${retryDelayMs}ms: ${
          error instanceof Error ? error.message : String(error)
        }\n`
      );
      if (retryDelayMs > 0) await sleep(retryDelayMs);
      continue;
    }
    if (response.ok) {
      await response.arrayBuffer();
      return;
    }
    if (response.status === 408 || response.status === 429 || response.status === 503) {
      const waitMs = response.status === 408 ? retryDelayMs : parseRetryAfterMs(response.headers.get("retry-after"));
      await response.arrayBuffer();
      process.stderr.write(`[evlog-ingester] append HTTP ${response.status} on ${stream}; retrying in ${waitMs}ms\n`);
      if (waitMs > 0) await sleep(waitMs);
      continue;
    }
    throw dsError(`append failed for ${stream}: HTTP ${response.status} ${await response.text()}`);
  }
}

function usage(): never {
  console.error(
    "usage: bun run experiments/demo/evlog_ingester.ts --url <base-url> --stream <name> [--batch-size 1000] [--delay-ms 0] [--report-every-ms 5000] [--max-batches N] [--resume-stream] [--reset]"
  );
  process.exit(1);
}

export async function runEvlogIngester(
  args: string[] = process.argv.slice(2),
  opts: EvlogIngestRunOptions = {}
): Promise<EvlogIngestSummary> {
  if (hasFlag(args, "--help")) usage();
  const baseUrl = parseStringArg(args, "--url", EVLOG_DEFAULT_BASE_URL).replace(/\/+$/, "");
  const stream = parseStringArg(args, "--stream", EVLOG_DEFAULT_STREAM);
  const batchSize = parseIntArg(args, "--batch-size", EVLOG_DEFAULT_BATCH_SIZE);
  const delayMs = parseIntArg(args, "--delay-ms", 0);
  const maxBatches = parseIntArg(args, "--max-batches", 0);
  const reportEveryMs = parseIntArg(args, "--report-every-ms", EVLOG_DEFAULT_REPORT_EVERY_MS);
  const requestTimeoutMs = parseIntArg(args, "--request-timeout-ms", EVLOG_DEFAULT_REQUEST_TIMEOUT_MS);
  const retryDelayMs = parseIntArg(args, "--retry-delay-ms", EVLOG_DEFAULT_RETRY_DELAY_MS);
  const reset = hasFlag(args, "--reset");
  const resumeStream = hasFlag(args, "--resume-stream");
  if (batchSize <= 0) throw dsError("--batch-size must be positive");

  const fetchImpl = opts.fetchImpl ?? fetch;
  await ensureEvlogStream(fetchImpl, baseUrl, stream, { reset });

  let nextId = resumeStream ? await fetchNextOffset(fetchImpl, baseUrl, stream) : 0;
  const startId = nextId;
  let appendedEvents = 0;
  let appendedBytes = 0;
  let batches = 0;
  let lastReportAt = Date.now();

  for (;;) {
    const batchStartMs = Date.now();
    const batch: EvlogEvent[] = [];
    for (let index = 0; index < batchSize; index += 1) {
      batch.push(buildEvlogEvent(nextId + index, new Date(batchStartMs + index).toISOString()));
    }
    const body = JSON.stringify(batch);
    await appendBatch(fetchImpl, baseUrl, stream, body, requestTimeoutMs, retryDelayMs);
    nextId += batch.length;
    appendedEvents += batch.length;
    appendedBytes += Buffer.byteLength(body);
    batches += 1;

    const now = Date.now();
    if (now - lastReportAt >= reportEveryMs) {
      const mib = (appendedBytes / 1024 / 1024).toFixed(2);
      process.stderr.write(
        `[evlog-ingester] stream=${stream} nextId=${nextId} appendedEvents=${appendedEvents} appendedBytesMiB=${mib} batches=${batches}\n`
      );
      lastReportAt = now;
    }

    if (maxBatches > 0 && batches >= maxBatches) {
      return {
        stream,
        startId,
        nextId,
        appendedEvents,
        appendedBytes,
        batches,
      };
    }
    if (delayMs > 0) await sleep(delayMs);
  }
}

if (import.meta.main) {
  runEvlogIngester().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
