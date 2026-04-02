import { dsError } from "../../src/util/ds_error.ts";

const DEFAULT_BASE_URL = "http://127.0.0.1:8787";
const DEFAULT_STREAM_PREFIX = "gharchive-demo";
const DEFAULT_BATCH_MAX_BYTES = 8 * 1024 * 1024;
const DEFAULT_BATCH_MAX_RECORDS = 1_000;
const DEFAULT_ALL_RANGE_BATCH_MAX_BYTES = 2 * 1024 * 1024;
const DEFAULT_ALL_RANGE_BATCH_MAX_RECORDS = 250;
const GH_ARCHIVE_REQUEST_TIMEOUT_MS = 20_000;
const GH_ARCHIVE_TIMEOUT_RETRY_DELAY_MS = 1_000;
const GH_ARCHIVE_FETCH_TIMEOUT_CODE = "gharchive_fetch_timeout";
const GH_ARCHIVE_START_MS = Date.UTC(2011, 1, 12, 0, 0, 0, 0);
const GH_ARCHIVE_HOUR_MISSING_CODE = "gharchive_hour_missing";

export type GhArchiveRangeName = "day" | "week" | "month" | "year" | "all";

const GH_ARCHIVE_EXACT_FIELDS = [
  "eventType",
  "ghArchiveId",
  "actorLogin",
  "repoName",
  "repoOwner",
  "orgLogin",
  "action",
  "refType",
  "public",
  "isBot",
] as const;

const GH_ARCHIVE_COLUMN_FIELDS = ["eventTime", "public", "isBot", "commitCount", "payloadBytes", "payloadKb"] as const;

const GH_ARCHIVE_FTS_FIELDS = ["eventType", "repoOwner", "action", "refType", "title", "message", "body"] as const;

const GH_ARCHIVE_ROLLUP_NAMES = ["events"] as const;

export const GH_ARCHIVE_ONLY_INDEX_SELECTORS = [
  ...GH_ARCHIVE_EXACT_FIELDS.map((name) => `exact:${name}` as const),
  ...GH_ARCHIVE_COLUMN_FIELDS.map((name) => `col:${name}` as const),
  ...GH_ARCHIVE_FTS_FIELDS.map((name) => `fts:${name}` as const),
  ...GH_ARCHIVE_ROLLUP_NAMES.map((name) => `agg:${name}` as const),
] as const;

export type GhArchiveOnlyIndexSelector = (typeof GH_ARCHIVE_ONLY_INDEX_SELECTORS)[number];

export type GhArchiveDemoEvent = {
  action: string | null;
  actorLogin: string | null;
  archiveHour: string;
  body: string | null;
  commitCount: number;
  eventTime: string;
  eventType: string;
  ghArchiveId: string;
  isBot: boolean;
  message: string | null;
  orgLogin: string | null;
  payloadBytes: number;
  payloadKb: number;
  public: boolean;
  refType: string | null;
  repoName: string | null;
  repoOwner: string | null;
  title: string | null;
};

export type GhArchiveDemoSummary = {
  avgIngestMiBPerSec: number;
  appendAckMiBPerSec: number;
  appendBackoffCount: number;
  appendBackoffWaitMs: number;
  downloadMiBPerSec: number;
  downloadedHours: number;
  elapsedMs: number;
  endHour: string;
  hours: number;
  missingHours: number;
  normalizeMiBPerSec: number;
  normalizedBytes: number;
  normalizedRows: number;
  range: GhArchiveRangeName;
  rawSourceBytes: number;
  startHour: string;
  streamCount: number;
  streams: string[];
};

export type GhArchiveDemoRunOptions = {
  fetchImpl?: typeof fetch;
  now?: Date;
  requestTimeoutMs?: number;
  timeoutRetryDelayMs?: number;
};

type GhArchiveHttpClient = {
  fetchImpl: typeof fetch;
  requestTimeoutMs: number;
  timeoutRetryDelayMs: number;
};

export function resolveGhArchiveBatchDefaults(range: GhArchiveRangeName): { batchMaxBytes: number; batchMaxRecords: number } {
  if (range === "all") {
    return {
      batchMaxBytes: DEFAULT_ALL_RANGE_BATCH_MAX_BYTES,
      batchMaxRecords: DEFAULT_ALL_RANGE_BATCH_MAX_RECORDS,
    };
  }
  return {
    batchMaxBytes: DEFAULT_BATCH_MAX_BYTES,
    batchMaxRecords: DEFAULT_BATCH_MAX_RECORDS,
  };
}

type GhArchiveRawEvent = {
  actor?: { login?: string | null } | null;
  created_at?: string | null;
  id?: string | number | null;
  org?: { login?: string | null } | null;
  payload?: Record<string, unknown> | null;
  public?: boolean | null;
  repo?: { name?: string | null } | null;
  type?: string | null;
};

type GhArchiveSchemaBuildOptions = {
  noIndex?: boolean;
  onlyIndex?: GhArchiveOnlyIndexSelector | null;
  onlyIndexes?: GhArchiveOnlyIndexSelector[];
};

type GhArchiveDemoStreamTarget = {
  stream: string;
  selector: GhArchiveOnlyIndexSelector | null;
  schema: GhArchiveSchemaBuildOptions;
};

const GH_ARCHIVE_SEARCH_ALIASES = {
  actor: "actorLogin",
  id: "ghArchiveId",
  org: "orgLogin",
  owner: "repoOwner",
  repo: "repoName",
  type: "eventType",
} as const;

const GH_ARCHIVE_DEFAULT_FIELDS = [
  { field: "title", boost: 1.5 },
  { field: "message", boost: 1.25 },
  { field: "body", boost: 0.9 },
] as const;

const GH_ARCHIVE_SEARCH_FIELDS = {
  eventTime: {
    kind: "date",
    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
    column: true,
    exists: true,
    sortable: true,
  },
  eventType: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/eventType" }],
    normalizer: "lowercase_v1",
    exact: true,
    prefix: true,
    exists: true,
    sortable: true,
  },
  ghArchiveId: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/ghArchiveId" }],
    exact: true,
    exists: true,
  },
  actorLogin: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/actorLogin" }],
    normalizer: "lowercase_v1",
    exact: true,
    exists: true,
    sortable: true,
  },
  repoName: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/repoName" }],
    normalizer: "lowercase_v1",
    exact: true,
    exists: true,
    sortable: true,
  },
  repoOwner: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/repoOwner" }],
    normalizer: "lowercase_v1",
    exact: true,
    prefix: true,
    exists: true,
    sortable: true,
  },
  orgLogin: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/orgLogin" }],
    normalizer: "lowercase_v1",
    exact: true,
    exists: true,
    sortable: true,
  },
  action: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/action" }],
    normalizer: "lowercase_v1",
    exact: true,
    prefix: true,
    exists: true,
    sortable: true,
  },
  refType: {
    kind: "keyword",
    bindings: [{ version: 1, jsonPointer: "/refType" }],
    normalizer: "lowercase_v1",
    exact: true,
    prefix: true,
    exists: true,
    sortable: true,
  },
  public: {
    kind: "bool",
    bindings: [{ version: 1, jsonPointer: "/public" }],
    exact: true,
    column: true,
    exists: true,
    sortable: true,
  },
  isBot: {
    kind: "bool",
    bindings: [{ version: 1, jsonPointer: "/isBot" }],
    exact: true,
    column: true,
    exists: true,
    sortable: true,
  },
  commitCount: {
    kind: "integer",
    bindings: [{ version: 1, jsonPointer: "/commitCount" }],
    column: true,
    exists: true,
    sortable: true,
    aggregatable: true,
  },
  payloadBytes: {
    kind: "integer",
    bindings: [{ version: 1, jsonPointer: "/payloadBytes" }],
    column: true,
    exists: true,
    sortable: true,
    aggregatable: true,
  },
  payloadKb: {
    kind: "float",
    bindings: [{ version: 1, jsonPointer: "/payloadKb" }],
    column: true,
    exists: true,
    sortable: true,
    aggregatable: true,
  },
  title: {
    kind: "text",
    bindings: [{ version: 1, jsonPointer: "/title" }],
    analyzer: "unicode_word_v1",
    exists: true,
    positions: true,
  },
  message: {
    kind: "text",
    bindings: [{ version: 1, jsonPointer: "/message" }],
    analyzer: "unicode_word_v1",
    exists: true,
    positions: true,
  },
  body: {
    kind: "text",
    bindings: [{ version: 1, jsonPointer: "/body" }],
    analyzer: "unicode_word_v1",
    exists: true,
    positions: true,
  },
} as const satisfies Record<string, Record<string, unknown>>;

const GH_ARCHIVE_ROLLUPS = {
  events: {
    timestampField: "eventTime",
    dimensions: ["eventType", "repoOwner", "public", "isBot"],
    intervals: ["1m", "5m", "15m", "1h", "6h", "1d", "7d"],
    measures: {
      events: { kind: "count" },
      payloadBytes: {
        kind: "summary",
        field: "payloadBytes",
        histogram: "log2_v1",
      },
      commitCount: {
        kind: "summary",
        field: "commitCount",
        histogram: "log2_v1",
      },
    },
  },
} as const satisfies Record<string, Record<string, unknown>>;

function cloneJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function buildMinimalTimestampSearchField(): Record<string, unknown> {
  return {
    kind: "date",
    bindings: [{ version: 1, jsonPointer: "/eventTime" }],
    exists: true,
  };
}

function buildSearchField(fieldName: keyof typeof GH_ARCHIVE_SEARCH_FIELDS): Record<string, unknown> {
  return cloneJson(GH_ARCHIVE_SEARCH_FIELDS[fieldName]);
}

function omitSearchFieldKeys(field: Record<string, unknown>, keys: string[]): Record<string, unknown> {
  for (const key of keys) delete field[key];
  return field;
}

function buildFullSearchConfig(): Record<string, unknown> {
  return {
    primaryTimestampField: "eventTime",
    aliases: cloneJson(GH_ARCHIVE_SEARCH_ALIASES),
    defaultFields: cloneJson(GH_ARCHIVE_DEFAULT_FIELDS),
    fields: cloneJson(GH_ARCHIVE_SEARCH_FIELDS),
    rollups: cloneJson(GH_ARCHIVE_ROLLUPS),
  };
}

function buildIsolatedExactSearchConfig(fieldName: (typeof GH_ARCHIVE_EXACT_FIELDS)[number]): Record<string, unknown> {
  const field = omitSearchFieldKeys(buildSearchField(fieldName), ["prefix", "column", "aggregatable"]);
  return {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: buildMinimalTimestampSearchField(),
      [fieldName]: field,
    },
  };
}

function buildIsolatedColumnSearchConfig(fieldName: (typeof GH_ARCHIVE_COLUMN_FIELDS)[number]): Record<string, unknown> {
  const field = fieldName === "eventTime" ? buildSearchField("eventTime") : omitSearchFieldKeys(buildSearchField(fieldName), ["exact", "prefix"]);
  return {
    primaryTimestampField: "eventTime",
    fields:
      fieldName === "eventTime"
        ? {
            eventTime: field,
          }
        : {
            eventTime: buildMinimalTimestampSearchField(),
            [fieldName]: field,
          },
  };
}

function buildIsolatedFtsSearchConfig(fieldName: (typeof GH_ARCHIVE_FTS_FIELDS)[number]): Record<string, unknown> {
  const field = omitSearchFieldKeys(buildSearchField(fieldName), ["exact", "column", "aggregatable"]);
  return {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: buildMinimalTimestampSearchField(),
      [fieldName]: field,
    },
  };
}

function buildIsolatedAggSearchConfig(rollupName: (typeof GH_ARCHIVE_ROLLUP_NAMES)[number]): Record<string, unknown> {
  if (rollupName !== "events") throw dsError(`unsupported GH Archive rollup selector: ${rollupName}`);
  return {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: buildMinimalTimestampSearchField(),
      payloadBytes: {
        kind: "integer",
        bindings: [{ version: 1, jsonPointer: "/payloadBytes" }],
        exists: true,
        aggregatable: true,
      },
      commitCount: {
        kind: "integer",
        bindings: [{ version: 1, jsonPointer: "/commitCount" }],
        exists: true,
        aggregatable: true,
      },
    },
    rollups: {
      events: {
        timestampField: "eventTime",
        intervals: cloneJson(GH_ARCHIVE_ROLLUPS.events.intervals),
        measures: {
          events: { kind: "count" },
          payloadBytes: {
            kind: "summary",
            field: "payloadBytes",
            histogram: "log2_v1",
          },
          commitCount: {
            kind: "summary",
            field: "commitCount",
            histogram: "log2_v1",
          },
        },
      },
    },
  };
}

function buildIsolatedSearchConfig(selector: GhArchiveOnlyIndexSelector): Record<string, unknown> {
  const [kind, name] = selector.split(":", 2);
  if (kind === "exact" && GH_ARCHIVE_EXACT_FIELDS.includes(name as (typeof GH_ARCHIVE_EXACT_FIELDS)[number])) {
    return buildIsolatedExactSearchConfig(name as (typeof GH_ARCHIVE_EXACT_FIELDS)[number]);
  }
  if (kind === "col" && GH_ARCHIVE_COLUMN_FIELDS.includes(name as (typeof GH_ARCHIVE_COLUMN_FIELDS)[number])) {
    return buildIsolatedColumnSearchConfig(name as (typeof GH_ARCHIVE_COLUMN_FIELDS)[number]);
  }
  if (kind === "fts" && GH_ARCHIVE_FTS_FIELDS.includes(name as (typeof GH_ARCHIVE_FTS_FIELDS)[number])) {
    return buildIsolatedFtsSearchConfig(name as (typeof GH_ARCHIVE_FTS_FIELDS)[number]);
  }
  if (kind === "agg" && GH_ARCHIVE_ROLLUP_NAMES.includes(name as (typeof GH_ARCHIVE_ROLLUP_NAMES)[number])) {
    return buildIsolatedAggSearchConfig(name as (typeof GH_ARCHIVE_ROLLUP_NAMES)[number]);
  }
  throw dsError(`invalid --onlyindex selector: ${selector}`);
}

function mergeSearchFieldConfigs(
  left: Record<string, unknown> | undefined,
  right: Record<string, unknown>
): Record<string, unknown> {
  if (!left) return cloneJson(right);
  const merged = { ...cloneJson(left), ...cloneJson(right) } as Record<string, unknown>;
  if (left.kind !== undefined && right.kind !== undefined && left.kind !== right.kind) {
    throw dsError(`cannot combine incompatible search field kinds: ${String(left.kind)} vs ${String(right.kind)}`);
  }
  if (left.bindings !== undefined && right.bindings !== undefined) {
    const leftBindings = JSON.stringify(left.bindings);
    const rightBindings = JSON.stringify(right.bindings);
    if (leftBindings !== rightBindings) {
      throw dsError(`cannot combine incompatible search field bindings for kind ${String(merged.kind ?? "unknown")}`);
    }
  }
  return merged;
}

function combineSearchConfigs(selectors: GhArchiveOnlyIndexSelector[]): Record<string, unknown> {
  const combinedFields: Record<string, Record<string, unknown>> = {};
  const combinedRollups: Record<string, unknown> = {};
  for (const selector of selectors) {
    const partial = buildIsolatedSearchConfig(selector);
    const partialFields = (partial.fields ?? {}) as Record<string, Record<string, unknown>>;
    for (const [fieldName, fieldConfig] of Object.entries(partialFields)) {
      combinedFields[fieldName] = mergeSearchFieldConfigs(combinedFields[fieldName], fieldConfig);
    }
    const partialRollups = (partial.rollups ?? {}) as Record<string, unknown>;
    for (const [rollupName, rollupConfig] of Object.entries(partialRollups)) {
      combinedRollups[rollupName] = cloneJson(rollupConfig);
    }
  }
  const combined: Record<string, unknown> = {
    primaryTimestampField: "eventTime",
    fields: combinedFields,
  };
  if (Object.keys(combinedRollups).length > 0) combined.rollups = combinedRollups;
  return combined;
}

function normalizeOnlyIndexSelectors(opts: GhArchiveSchemaBuildOptions): GhArchiveOnlyIndexSelector[] {
  const selectors: GhArchiveOnlyIndexSelector[] = [];
  if (opts.onlyIndex) selectors.push(opts.onlyIndex);
  if (Array.isArray(opts.onlyIndexes)) selectors.push(...opts.onlyIndexes);
  const seen = new Set<GhArchiveOnlyIndexSelector>();
  const normalized: GhArchiveOnlyIndexSelector[] = [];
  for (const selector of selectors) {
    if (seen.has(selector)) continue;
    seen.add(selector);
    normalized.push(selector);
  }
  return normalized;
}

function argValue(args: string[], flag: string): string | null {
  const idx = args.indexOf(flag);
  if (idx === -1) return null;
  return args[idx + 1] ?? null;
}

function argValues(args: string[], flag: string): string[] {
  const values: string[] = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i] !== flag) continue;
    const value = args[i + 1];
    if (value != null) values.push(value);
  }
  return values;
}

function parseStringArg(args: string[], flag: string, fallback: string): string {
  const raw = argValue(args, flag);
  return raw == null ? fallback : raw;
}

function parseIntArg(args: string[], flag: string, fallback: number): number {
  const raw = argValue(args, flag);
  if (raw == null) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0 || !Number.isInteger(value)) {
    throw dsError(`invalid ${flag}: ${raw}`);
  }
  return value;
}

function hasFlag(args: string[], flag: string): boolean {
  return args.includes(flag);
}

function parseRangeArg(args: string[]): GhArchiveRangeName {
  const positional = args.find((arg) => !arg.startsWith("--"));
  const raw = (positional ?? argValue(args, "--range") ?? "day").trim().toLowerCase();
  if (raw === "day" || raw === "week" || raw === "month" || raw === "year" || raw === "all") return raw;
  throw dsError(`invalid range: ${raw}`);
}

function parseOnlyIndexArgs(args: string[]): GhArchiveOnlyIndexSelector[] {
  const rawValues = argValues(args, "--onlyindex");
  if (rawValues.length === 0) return [];
  const selectors: GhArchiveOnlyIndexSelector[] = [];
  const seen = new Set<GhArchiveOnlyIndexSelector>();
  for (const rawValue of rawValues) {
    for (const token of rawValue.split(",")) {
      const selectorRaw = token.trim();
      if (selectorRaw === "") continue;
      if (!(GH_ARCHIVE_ONLY_INDEX_SELECTORS as readonly string[]).includes(selectorRaw)) {
        throw dsError(`invalid --onlyindex: ${selectorRaw}; supported selectors: ${GH_ARCHIVE_ONLY_INDEX_SELECTORS.join(", ")}`);
      }
      const selector = selectorRaw as GhArchiveOnlyIndexSelector;
      if (seen.has(selector)) continue;
      seen.add(selector);
      selectors.push(selector);
    }
  }
  return selectors;
}

function floorUtcHour(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), 0, 0, 0));
}

function addHours(date: Date, hours: number): Date {
  return new Date(date.getTime() + hours * 60 * 60 * 1000);
}

function formatArchiveHour(date: Date): string {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  const h = String(date.getUTCHours()).padStart(2, "0");
  return `${y}-${m}-${d}-${h}`;
}

export function buildGhArchiveStreamName(prefix: string, range: GhArchiveRangeName): string {
  return `${prefix}-${range}`;
}

export function buildGhArchiveIndexedStreamName(
  prefix: string,
  range: GhArchiveRangeName,
  selector: GhArchiveOnlyIndexSelector
): string {
  return `${buildGhArchiveStreamName(prefix, range)}-${selector.replace(":", "-")}`;
}

export function resolveGhArchiveStreamTargets(
  prefix: string,
  range: GhArchiveRangeName,
  opts: { noIndex?: boolean; onlyIndexes?: GhArchiveOnlyIndexSelector[] } = {}
): GhArchiveDemoStreamTarget[] {
  if (opts.noIndex) {
    return [
      {
        stream: buildGhArchiveStreamName(prefix, range),
        selector: null,
        schema: { noIndex: true },
      },
    ];
  }
  const selectors =
    Array.isArray(opts.onlyIndexes) && opts.onlyIndexes.length > 0
      ? opts.onlyIndexes
      : [...GH_ARCHIVE_ONLY_INDEX_SELECTORS];
  return selectors.map((selector) => ({
    stream: buildGhArchiveIndexedStreamName(prefix, range, selector),
    selector,
    schema: { onlyIndex: selector },
  }));
}

function streamUrl(baseUrl: string, stream: string): string {
  return `${baseUrl}/v1/stream/${encodeURIComponent(stream)}`;
}

function hasErrorCode(error: unknown, code: string): boolean {
  return (
    typeof error === "object" &&
    error != null &&
    "code" in error &&
    (error as { code?: unknown }).code === code
  );
}

function isAbortLikeError(error: unknown): boolean {
  return typeof error === "object" && error != null && "name" in error && (error as { name?: unknown }).name === "AbortError";
}

async function fetchWithTimeout(
  client: GhArchiveHttpClient,
  input: RequestInfo | URL,
  init: RequestInit = {}
): Promise<Response> {
  const controller = new AbortController();
  const parentSignal = init.signal;
  const abortFromParent = () => controller.abort();
  if (parentSignal) {
    if (parentSignal.aborted) abortFromParent();
    else parentSignal.addEventListener("abort", abortFromParent, { once: true });
  }
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, client.requestTimeoutMs);
  try {
    return await client.fetchImpl(input, { ...init, signal: controller.signal });
  } catch (error) {
    if (timedOut || (!parentSignal?.aborted && controller.signal.aborted && isAbortLikeError(error))) {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      throw dsError(`request timed out after ${client.requestTimeoutMs}ms: ${url}`, {
        code: GH_ARCHIVE_FETCH_TIMEOUT_CODE,
        cause: error,
      });
    }
    throw error;
  } finally {
    clearTimeout(timer);
    parentSignal?.removeEventListener("abort", abortFromParent);
  }
}

async function withTimeoutRetries<T>(client: GhArchiveHttpClient, description: string, run: () => Promise<T>): Promise<T> {
  for (;;) {
    try {
      return await run();
    } catch (error) {
      if (!hasErrorCode(error, GH_ARCHIVE_FETCH_TIMEOUT_CODE)) throw error;
      process.stderr.write(
        `[gharchive-demo] timeout during ${description}; retrying in ${client.timeoutRetryDelayMs}ms\n`
      );
      if (client.timeoutRetryDelayMs > 0) await sleep(client.timeoutRetryDelayMs);
    }
  }
}

async function fetchWithServerBackoff(
  client: GhArchiveHttpClient,
  description: string,
  run: () => Promise<Response>
): Promise<Response> {
  for (;;) {
    const response = await withTimeoutRetries(client, description, run);
    if (response.status !== 429 && response.status !== 503) return response;
    const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
    process.stderr.write(`[gharchive-demo] backoff during ${description}; retrying in ${retryAfterMs}ms\n`);
    if (retryAfterMs > 0) await sleep(retryAfterMs);
  }
}

export function resolveGhArchiveRangeHours(
  range: GhArchiveRangeName,
  now: Date = new Date()
): { start: Date; endExclusive: Date; hours: number } {
  const endExclusive = floorUtcHour(now);
  const rangeHours =
    range === "day"
      ? 24
      : range === "week"
        ? 24 * 7
        : range === "month"
          ? 24 * 30
          : range === "year"
            ? 24 * 365
            : Math.max(0, Math.floor((endExclusive.getTime() - GH_ARCHIVE_START_MS) / (60 * 60 * 1000)));
  const start =
    range === "all" ? new Date(GH_ARCHIVE_START_MS) : addHours(endExclusive, -rangeHours);
  return { start, endExclusive, hours: rangeHours };
}

export function* iterateGhArchiveHours(start: Date, endExclusive: Date): Generator<Date> {
  for (let cursor = start.getTime(); cursor < endExclusive.getTime(); cursor += 60 * 60 * 1000) {
    yield new Date(cursor);
  }
}

export function buildGhArchiveArchiveUrl(hour: Date): string {
  return `https://data.gharchive.org/${formatArchiveHour(hour)}.json.gz`;
}

function getNestedRecord(value: unknown, key: string): Record<string, unknown> | null {
  if (!value || typeof value !== "object") return null;
  const next = (value as Record<string, unknown>)[key];
  return next && typeof next === "object" ? (next as Record<string, unknown>) : null;
}

function getNestedString(value: unknown, ...path: string[]): string | null {
  let cursor: unknown = value;
  for (const part of path) {
    if (!cursor || typeof cursor !== "object") return null;
    cursor = (cursor as Record<string, unknown>)[part];
  }
  return typeof cursor === "string" && cursor.length > 0 ? cursor : null;
}

function firstString(values: Array<string | null | undefined>): string | null {
  for (const value of values) {
    if (typeof value === "string" && value.trim() !== "") return value.trim();
  }
  return null;
}

function truncateText(value: string | null, maxChars: number): string | null {
  if (value == null) return null;
  const trimmed = value.trim();
  if (trimmed.length <= maxChars) return trimmed;
  return trimmed.slice(0, Math.max(0, maxChars - 1)).trimEnd();
}

function extractCommitMessages(payload: Record<string, unknown> | null): string[] {
  const commits = Array.isArray(payload?.commits) ? payload?.commits : [];
  const out: string[] = [];
  for (const commit of commits) {
    if (!commit || typeof commit !== "object") continue;
    const message = (commit as Record<string, unknown>).message;
    if (typeof message === "string" && message.trim() !== "") out.push(message.trim());
  }
  return out;
}

function extractCommitCount(payload: Record<string, unknown> | null): number {
  const commits = Array.isArray(payload?.commits) ? payload?.commits.length : 0;
  const size = typeof payload?.size === "number" && Number.isFinite(payload.size) ? Math.max(0, Math.floor(payload.size)) : 0;
  return Math.max(commits, size);
}

function extractTitle(payload: Record<string, unknown> | null): string | null {
  return firstString([
    getNestedString(payload, "issue", "title"),
    getNestedString(payload, "pull_request", "title"),
    getNestedString(payload, "release", "name"),
    getNestedString(payload, "release", "tag_name"),
    getNestedString(payload, "comment", "body"),
    getNestedString(payload, "review", "body"),
    getNestedString(payload, "review_comment", "body"),
  ]);
}

function extractBody(payload: Record<string, unknown> | null, commitMessages: string[]): string | null {
  return truncateText(
    firstString([
      getNestedString(payload, "issue", "body"),
      getNestedString(payload, "pull_request", "body"),
      getNestedString(payload, "comment", "body"),
      getNestedString(payload, "review", "body"),
      getNestedString(payload, "review_comment", "body"),
      getNestedString(payload, "release", "body"),
      commitMessages.join("\n"),
    ]),
    1_024
  );
}

export function normalizeGhArchiveEvent(
  raw: GhArchiveRawEvent,
  archiveHour: Date,
  rawPayloadBytes: number
): GhArchiveDemoEvent | null {
  const eventType = typeof raw.type === "string" && raw.type !== "" ? raw.type : null;
  const eventTime = typeof raw.created_at === "string" && raw.created_at !== "" ? raw.created_at : null;
  const ghArchiveId = raw.id == null ? null : String(raw.id);
  if (!eventType || !eventTime || !ghArchiveId) return null;

  const payload = raw.payload && typeof raw.payload === "object" ? (raw.payload as Record<string, unknown>) : null;
  const actorLogin = typeof raw.actor?.login === "string" ? raw.actor.login : null;
  const repoName = typeof raw.repo?.name === "string" ? raw.repo.name : null;
  const repoOwner = repoName && repoName.includes("/") ? repoName.split("/")[0] : null;
  const orgLogin = typeof raw.org?.login === "string" ? raw.org.login : null;
  const action = typeof payload?.action === "string" ? payload.action : null;
  const refType = typeof payload?.ref_type === "string" ? payload.ref_type : null;
  const commitMessages = extractCommitMessages(payload);
  const commitCount = extractCommitCount(payload);
  const title = truncateText(extractTitle(payload), 512);
  const body = extractBody(payload, commitMessages);
  const publicValue = raw.public === true;
  const isBot = actorLogin != null && /\[bot\]$|-bot$/i.test(actorLogin);
  const message = truncateText(
    [
      eventType,
      action,
      repoName,
      actorLogin,
      title,
      commitMessages.slice(0, 5).join("\n"),
    ]
      .filter((value): value is string => typeof value === "string" && value.trim() !== "")
      .join("\n"),
    1_024
  );

  return {
    action,
    actorLogin,
    archiveHour: formatArchiveHour(archiveHour),
    body,
    commitCount,
    eventTime,
    eventType,
    ghArchiveId,
    isBot,
    message,
    orgLogin,
    payloadBytes: rawPayloadBytes,
    payloadKb: Number((rawPayloadBytes / 1024).toFixed(3)),
    public: publicValue,
    refType,
    repoName,
    repoOwner,
    title,
  };
}

export function buildGhArchiveSchemaUpdate(opts: GhArchiveSchemaBuildOptions = {}): Record<string, unknown> {
  const update: Record<string, unknown> = {
    schema: {
      type: "object",
      additionalProperties: false,
      required: [
        "ghArchiveId",
        "eventTime",
        "eventType",
        "public",
        "isBot",
        "commitCount",
        "payloadBytes",
        "payloadKb",
        "archiveHour",
      ],
      properties: {
        ghArchiveId: { type: "string" },
        eventTime: { type: "string", format: "date-time" },
        eventType: { type: "string" },
        public: { type: "boolean" },
        isBot: { type: "boolean" },
        actorLogin: { type: ["string", "null"] },
        repoName: { type: ["string", "null"] },
        repoOwner: { type: ["string", "null"] },
        orgLogin: { type: ["string", "null"] },
        action: { type: ["string", "null"] },
        refType: { type: ["string", "null"] },
        title: { type: ["string", "null"] },
        message: { type: ["string", "null"] },
        body: { type: ["string", "null"] },
        archiveHour: { type: "string" },
        commitCount: { type: "integer", minimum: 0 },
        payloadBytes: { type: "integer", minimum: 0 },
        payloadKb: { type: "number", minimum: 0 },
      },
    },
    routingKey: {
      jsonPointer: "/repoName",
      required: false,
    },
  };
  if (opts.noIndex) return update;
  const onlyIndexSelectors = normalizeOnlyIndexSelectors(opts);
  update.search =
    onlyIndexSelectors.length === 0
      ? buildFullSearchConfig()
      : onlyIndexSelectors.length === 1
        ? buildIsolatedSearchConfig(onlyIndexSelectors[0]!)
        : combineSearchConfigs(onlyIndexSelectors);
  return update;
}

async function fetchJson(client: GhArchiveHttpClient, url: string, init: RequestInit = {}): Promise<any> {
  const response = await fetchWithServerBackoff(client, `request ${url}`, () => fetchWithTimeout(client, url, init));
  const text = await response.text();
  if (!response.ok) throw dsError(`HTTP ${response.status} ${url}: ${text}`);
  if (text === "") return null;
  return JSON.parse(text);
}

async function deleteStreamIfExists(client: GhArchiveHttpClient, baseUrl: string, stream: string): Promise<void> {
  const response = await fetchWithServerBackoff(client, `delete stream ${stream}`, () =>
    fetchWithTimeout(client, streamUrl(baseUrl, stream), { method: "DELETE" })
  );
  if (response.status === 404) return;
  if (!response.ok) {
    throw dsError(`failed to delete stream ${stream}: HTTP ${response.status} ${await response.text()}`);
  }
}

async function createConfiguredStream(
  client: GhArchiveHttpClient,
  baseUrl: string,
  stream: string,
  opts: GhArchiveSchemaBuildOptions = {}
): Promise<void> {
  const createResponse = await fetchWithServerBackoff(client, `create stream ${stream}`, () =>
    fetchWithTimeout(client, streamUrl(baseUrl, stream), {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: "",
    })
  );
  if (!createResponse.ok) {
    throw dsError(`failed to create stream ${stream}: HTTP ${createResponse.status} ${await createResponse.text()}`);
  }

  await fetchJson(client, `${streamUrl(baseUrl, stream)}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: { kind: "generic" },
    }),
  });

  await fetchJson(client, `${streamUrl(baseUrl, stream)}/_schema`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(buildGhArchiveSchemaUpdate(opts)),
  });
}

function parseRetryAfterMs(value: string | null): number {
  if (value == null || value.trim() === "") return 1_000;
  const trimmed = value.trim();
  const seconds = Number(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.max(0, Math.ceil(seconds * 1000));
  const whenMs = Date.parse(trimmed);
  if (Number.isFinite(whenMs)) return Math.max(0, whenMs - Date.now());
  return 1_000;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function appendBatchToStream(
  client: GhArchiveHttpClient,
  baseUrl: string,
  streamName: string,
  batchBody: string
): Promise<{ ackMs: number; backoffCount: number; backoffWaitMs: number }> {
  let ackMs = 0;
  let backoffCount = 0;
  let backoffWaitMs = 0;
  for (;;) {
    let response: Response;
    const appendStartedAt = Date.now();
    try {
      response = await fetchWithTimeout(client, streamUrl(baseUrl, streamName), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: batchBody,
      });
    } catch (error) {
      if (hasErrorCode(error, GH_ARCHIVE_FETCH_TIMEOUT_CODE)) {
        backoffCount += 1;
        backoffWaitMs += client.timeoutRetryDelayMs;
        process.stderr.write(
          `[gharchive-demo] append timeout on ${streamName}; retrying in ${client.timeoutRetryDelayMs}ms\n`
        );
        if (client.timeoutRetryDelayMs > 0) await sleep(client.timeoutRetryDelayMs);
        continue;
      }
      throw dsError(`append failed for ${streamName}`, { cause: error });
    }
    ackMs += Date.now() - appendStartedAt;
    if (response.ok) {
      await response.arrayBuffer();
      return { ackMs, backoffCount, backoffWaitMs };
    }
    if (response.status === 429 || response.status === 503) {
      const waitMs = parseRetryAfterMs(response.headers.get("retry-after"));
      backoffCount += 1;
      backoffWaitMs += waitMs;
      await response.arrayBuffer();
      await sleep(waitMs);
      continue;
    }
    throw dsError(`append failed for ${streamName}: HTTP ${response.status} ${await response.text()}`);
  }
}

function rateMiBPerSec(bytes: number, elapsedMs: number): number {
  if (elapsedMs <= 0 || bytes <= 0) return 0;
  return Number((((bytes / 1024 / 1024) * 1000) / elapsedMs).toFixed(3));
}

async function* iterateGhArchiveLines(
  client: GhArchiveHttpClient,
  url: string,
  opts: { onDownloadWaitMs?: (elapsedMs: number) => void } = {}
): AsyncGenerator<string> {
  const fetchStartedAt = Date.now();
  const response = await withTimeoutRetries(client, `download ${url}`, () => fetchWithTimeout(client, url));
  opts.onDownloadWaitMs?.(Date.now() - fetchStartedAt);
  if (response.status === 404) {
    throw dsError(`gh archive hour unavailable: ${url}`, { code: GH_ARCHIVE_HOUR_MISSING_CODE });
  }
  if (!response.ok || !response.body) {
    throw dsError(`failed to download ${url}: HTTP ${response.status}`);
  }
  const decoded = response.body.pipeThrough(new DecompressionStream("gzip")).pipeThrough(new TextDecoderStream());
  const reader = decoded.getReader();
  let buffer = "";
  for (;;) {
    const readStartedAt = Date.now();
    const { value, done } = await reader.read();
    opts.onDownloadWaitMs?.(Date.now() - readStartedAt);
    if (done) break;
    buffer += value;
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline === -1) break;
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.trim() !== "") yield line;
    }
  }
  if (buffer.trim() !== "") yield buffer;
}

export async function runGhArchiveDemo(
  args: string[] = process.argv.slice(2),
  opts: GhArchiveDemoRunOptions = {}
): Promise<GhArchiveDemoSummary> {
  const range = parseRangeArg(args);
  const baseUrl = parseStringArg(args, "--url", DEFAULT_BASE_URL);
  const streamPrefix = parseStringArg(args, "--stream-prefix", DEFAULT_STREAM_PREFIX);
  const batchDefaults = resolveGhArchiveBatchDefaults(range);
  const batchMaxBytes = parseIntArg(args, "--batch-max-bytes", batchDefaults.batchMaxBytes);
  const batchMaxRecords = parseIntArg(args, "--batch-max-records", batchDefaults.batchMaxRecords);
  const noIndex = hasFlag(args, "--noindex");
  const onlyIndexes = parseOnlyIndexArgs(args);
  if (noIndex && onlyIndexes.length > 0) {
    throw dsError("--noindex and --onlyindex cannot be combined");
  }
  const streamTargets = resolveGhArchiveStreamTargets(streamPrefix, range, { noIndex, onlyIndexes });
  const client: GhArchiveHttpClient = {
    fetchImpl: opts.fetchImpl ?? fetch,
    requestTimeoutMs: opts.requestTimeoutMs ?? GH_ARCHIVE_REQUEST_TIMEOUT_MS,
    timeoutRetryDelayMs: opts.timeoutRetryDelayMs ?? GH_ARCHIVE_TIMEOUT_RETRY_DELAY_MS,
  };
  const startedAt = Date.now();

  const { start, endExclusive, hours } = resolveGhArchiveRangeHours(range, opts.now);

  for (const target of streamTargets) {
    await deleteStreamIfExists(client, baseUrl, target.stream);
    await createConfiguredStream(client, baseUrl, target.stream, target.schema);
  }

  const encoder = new TextEncoder();
  let normalizedRows = 0;
  let rawSourceBytes = 0;
  let normalizedBytes = 0;
  let downloadedHours = 0;
  let missingHours = 0;
  let downloadTimeMs = 0;
  let normalizeTimeMs = 0;
  let appendAckTimeMs = 0;
  let appendBackoffCount = 0;
  let appendBackoffWaitMs = 0;
  let batchRows: string[] = [];
  let batchBytes = 2;

  const flush = async () => {
    if (batchRows.length === 0) return;
    const batchBody = `[${batchRows.join(",")}]`;
    for (const target of streamTargets) {
      const appendRes = await appendBatchToStream(client, baseUrl, target.stream, batchBody);
      appendAckTimeMs += appendRes.ackMs;
      appendBackoffCount += appendRes.backoffCount;
      appendBackoffWaitMs += appendRes.backoffWaitMs;
    }
    batchRows = [];
    batchBytes = 2;
  };

  for (const hour of iterateGhArchiveHours(start, endExclusive)) {
    const archiveUrl = buildGhArchiveArchiveUrl(hour);
    try {
      for await (const line of iterateGhArchiveLines(client, archiveUrl, {
        onDownloadWaitMs: (elapsedMs) => {
          downloadTimeMs += elapsedMs;
        },
      })) {
        rawSourceBytes += encoder.encode(line).byteLength;
        const normalizeStartedAt = Date.now();
        let parsed: GhArchiveRawEvent;
        try {
          parsed = JSON.parse(line) as GhArchiveRawEvent;
        } catch {
          normalizeTimeMs += Date.now() - normalizeStartedAt;
          continue;
        }
        const normalized = normalizeGhArchiveEvent(parsed, hour, encoder.encode(line).byteLength);
        if (!normalized) {
          normalizeTimeMs += Date.now() - normalizeStartedAt;
          continue;
        }
        const serialized = JSON.stringify(normalized);
        const serializedBytes = encoder.encode(serialized).byteLength;
        if (
          batchRows.length > 0 &&
          (batchRows.length >= batchMaxRecords || batchBytes + serializedBytes + 1 > batchMaxBytes)
        ) {
          await flush();
        }
        batchRows.push(serialized);
        batchBytes += serializedBytes + 1;
        normalizedRows += 1;
        normalizedBytes += serializedBytes;
        normalizeTimeMs += Date.now() - normalizeStartedAt;
      }
      downloadedHours += 1;
    } catch (error) {
      if (hasErrorCode(error, GH_ARCHIVE_HOUR_MISSING_CODE)) {
        missingHours += 1;
        process.stderr.write(`[gharchive-demo] missing hour ${formatArchiveHour(hour)} (${archiveUrl})\n`);
        continue;
      }
      throw error;
    }
  }

  await flush();
  if (downloadedHours === 0) {
    throw dsError(`no GH Archive hours were available for ${range} (${formatArchiveHour(start)} -> ${formatArchiveHour(addHours(endExclusive, -1))})`);
  }
  const elapsedMs = Date.now() - startedAt;
  const avgIngestMiBPerSec = rateMiBPerSec(normalizedBytes, elapsedMs);
  const downloadMiBPerSec = rateMiBPerSec(rawSourceBytes, downloadTimeMs);
  const normalizeMiBPerSec = rateMiBPerSec(normalizedBytes, normalizeTimeMs);
  const appendAckMiBPerSec = rateMiBPerSec(normalizedBytes, appendAckTimeMs);

  return {
    avgIngestMiBPerSec,
    appendAckMiBPerSec,
    appendBackoffCount,
    appendBackoffWaitMs,
    downloadMiBPerSec,
    downloadedHours,
    elapsedMs,
    endHour: formatArchiveHour(addHours(endExclusive, -1)),
    hours,
    missingHours,
    normalizeMiBPerSec,
    normalizedBytes,
    normalizedRows,
    range,
    rawSourceBytes,
    startHour: formatArchiveHour(start),
    streamCount: streamTargets.length,
    streams: streamTargets.map((target) => target.stream),
  };
}

async function main(): Promise<void> {
  const summary = await runGhArchiveDemo();
  const lines = [
    `GH Archive demo complete`,
    `range: ${summary.range} (${summary.startHour} -> ${summary.endHour}, requested=${summary.hours}, downloaded=${summary.downloadedHours}, missing=${summary.missingHours})`,
    `stream_count: ${summary.streamCount}`,
    `streams: ${summary.streams.join(",")}`,
    `rows: ${summary.normalizedRows}`,
    `raw_source_bytes: ${summary.rawSourceBytes}`,
    `normalized_bytes: ${summary.normalizedBytes}`,
    `avg_ingest_mib_per_s: ${summary.avgIngestMiBPerSec}`,
    `download_mib_per_s: ${summary.downloadMiBPerSec}`,
    `normalize_mib_per_s: ${summary.normalizeMiBPerSec}`,
    `append_ack_mib_per_s: ${summary.appendAckMiBPerSec}`,
    `append_backoff_count: ${summary.appendBackoffCount}`,
    `append_backoff_wait_ms: ${summary.appendBackoffWaitMs}`,
    `elapsed_ms: ${summary.elapsedMs}`,
  ];
  process.stdout.write(`${lines.join("\n")}\n`);
}

if (import.meta.main) {
  main().catch((error) => {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exitCode = 1;
  });
}
