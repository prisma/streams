import { hasFlag, parseIntArg, parseStringArg, sleep } from "./common";
import { dsError } from "../../src/util/ds_error.ts";

export const SIMPLE_DEFAULT_BASE_URL = "http://127.0.0.1:8787";
export const SIMPLE_DEFAULT_STREAM = "simple-1";
export const SIMPLE_DEFAULT_BATCH_SIZE = 5_000;
export const SIMPLE_DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
export const SIMPLE_DEFAULT_RETRY_DELAY_MS = 1_000;
export const SIMPLE_DEFAULT_REPORT_EVERY_MS = 5_000;
const RANDOM_STRING_LENGTH = 100;
const CARDINALITY_VALUE_LENGTH = 50;
const CARDINALITY100_SIZE = 100;
const CARDINALITY1000_SIZE = 1_000;
const SIMPLE_SCHEMA_UPDATE = {
  routingKey: {
    jsonPointer: "/cardinality100",
    required: true,
  },
};

export type SimpleEvent = {
  randomString: string;
  randomNumber: number;
  id: number;
  cardinality100: string;
  cardinality1000: string;
  time: string;
};

export type SimpleIngestSummary = {
  stream: string;
  startId: number;
  nextId: number;
  appendedEvents: number;
  appendedBytes: number;
  batches: number;
};

export type SimpleIngestRunOptions = {
  fetchImpl?: typeof fetch;
};

type SimpleStreamSchema = {
  currentVersion?: number;
  routingKey?: { jsonPointer?: string; required?: boolean };
};

type SimpleStreamDetails = {
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

export function buildSimpleCardinalityValues(count: number, salt: number): string[] {
  const values: string[] = [];
  for (let index = 0; index < count; index += 1) {
    values.push(randomBase36String(seedForId(index + 1, salt), CARDINALITY_VALUE_LENGTH));
  }
  return values;
}

export function buildSimpleRoutingOnlyUpdate(): Record<string, unknown> {
  return SIMPLE_SCHEMA_UPDATE;
}

export function buildSimpleEvent(
  id: number,
  nowIso: string,
  cardinality100Values: string[],
  cardinality1000Values: string[],
  salt = 1
): SimpleEvent {
  let state = seedForId(id, salt);
  const next = () => {
    const [value, nextState] = mulberry32Step(state);
    state = nextState;
    return value;
  };
  const cardinality100 = cardinality100Values[next() % cardinality100Values.length]!;
  const cardinality1000 = cardinality1000Values[next() % cardinality1000Values.length]!;
  return {
    randomString: randomBase36String(next(), RANDOM_STRING_LENGTH),
    randomNumber: next(),
    id,
    cardinality100,
    cardinality1000,
    time: nowIso,
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
  if (value == null || value.trim() === "") return SIMPLE_DEFAULT_RETRY_DELAY_MS;
  const trimmed = value.trim();
  const seconds = Number(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.max(0, Math.ceil(seconds * 1000));
  const whenMs = Date.parse(trimmed);
  if (Number.isFinite(whenMs)) return Math.max(0, whenMs - Date.now());
  return SIMPLE_DEFAULT_RETRY_DELAY_MS;
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

async function ensureRoutingOnlySchema(fetchImpl: typeof fetch, baseUrl: string, stream: string): Promise<void> {
  const schemaUrl = `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_schema`;
  const current = (await fetchJson(fetchImpl, schemaUrl, { method: "GET" })) as SimpleStreamSchema | null;
  if (
    current?.routingKey?.jsonPointer === SIMPLE_SCHEMA_UPDATE.routingKey.jsonPointer &&
    current?.routingKey?.required === SIMPLE_SCHEMA_UPDATE.routingKey.required
  ) {
    return;
  }
  await fetchJson(fetchImpl, schemaUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(SIMPLE_SCHEMA_UPDATE),
  });
}

async function ensureSimpleStream(
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
  await fetchJson(fetchImpl, `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: { kind: "generic" },
    }),
  });
  await ensureRoutingOnlySchema(fetchImpl, baseUrl, stream);
}

async function fetchNextOffset(fetchImpl: typeof fetch, baseUrl: string, stream: string): Promise<number> {
  const details = (await fetchJson(
    fetchImpl,
    `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/_details`,
    { method: "GET" }
  )) as SimpleStreamDetails;
  const nextOffsetRaw = details.stream?.next_offset ?? "0";
  const nextOffset = BigInt(nextOffsetRaw);
  const asNumber = Number(nextOffset);
  if (!Number.isSafeInteger(asNumber) || asNumber < 0) {
    throw dsError(`next_offset is not a safe integer for synthetic id generation: ${nextOffsetRaw}`);
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
        `[simple-ingester] append error on ${stream}; retrying in ${retryDelayMs}ms: ${
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
      process.stderr.write(
        `[simple-ingester] append HTTP ${response.status} on ${stream}; retrying in ${waitMs}ms\n`
      );
      if (waitMs > 0) await sleep(waitMs);
      continue;
    }
    throw dsError(`append failed for ${stream}: HTTP ${response.status} ${await response.text()}`);
  }
}

function usage(): never {
  console.error(
    "usage: bun run experiments/demo/simple_ingester.ts --url <base-url> --stream <name> [--batch-size 5000] [--delay-ms 0] [--report-every-ms 5000] [--max-batches N] [--resume-stream] [--reset]"
  );
  process.exit(1);
}

export async function runSimpleIngester(
  args: string[] = process.argv.slice(2),
  opts: SimpleIngestRunOptions = {}
): Promise<SimpleIngestSummary> {
  if (hasFlag(args, "--help")) usage();
  const baseUrl = parseStringArg(args, "--url", SIMPLE_DEFAULT_BASE_URL).replace(/\/+$/, "");
  const stream = parseStringArg(args, "--stream", SIMPLE_DEFAULT_STREAM);
  const batchSize = parseIntArg(args, "--batch-size", SIMPLE_DEFAULT_BATCH_SIZE);
  const delayMs = parseIntArg(args, "--delay-ms", 0);
  const maxBatches = parseIntArg(args, "--max-batches", 0);
  const reportEveryMs = parseIntArg(args, "--report-every-ms", SIMPLE_DEFAULT_REPORT_EVERY_MS);
  const requestTimeoutMs = parseIntArg(args, "--request-timeout-ms", SIMPLE_DEFAULT_REQUEST_TIMEOUT_MS);
  const retryDelayMs = parseIntArg(args, "--retry-delay-ms", SIMPLE_DEFAULT_RETRY_DELAY_MS);
  const reset = hasFlag(args, "--reset");
  const resumeStream = hasFlag(args, "--resume-stream");
  if (batchSize <= 0) {
    throw dsError("--batch-size must be positive");
  }

  const fetchImpl = opts.fetchImpl ?? fetch;
  const cardinality100Values = buildSimpleCardinalityValues(CARDINALITY100_SIZE, 101);
  const cardinality1000Values = buildSimpleCardinalityValues(CARDINALITY1000_SIZE, 1001);

  await ensureSimpleStream(fetchImpl, baseUrl, stream, { reset });

  let nextId = resumeStream ? await fetchNextOffset(fetchImpl, baseUrl, stream) : 0;
  const startId = nextId;
  let appendedEvents = 0;
  let appendedBytes = 0;
  let batches = 0;
  let lastReportAt = Date.now();

  for (;;) {
    const nowIso = new Date().toISOString();
    const batch: SimpleEvent[] = [];
    for (let index = 0; index < batchSize; index += 1) {
      batch.push(buildSimpleEvent(nextId + index, nowIso, cardinality100Values, cardinality1000Values));
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
        `[simple-ingester] stream=${stream} nextId=${nextId} appendedEvents=${appendedEvents} appendedBytesMiB=${mib} batches=${batches}\n`
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
    if (delayMs > 0) {
      await sleep(delayMs);
    }
  }
}

if (import.meta.main) {
  runSimpleIngester().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
