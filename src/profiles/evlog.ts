import { Result } from "better-result";
import type {
  CachedStreamProfile,
  PreparedJsonRecord,
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
  StreamProfileSpec,
  UnifiedTimelineItem,
} from "./profile";
import {
  cloneStreamProfileSpec,
  expectPlainObjectResult,
  isPlainObject,
  normalizeProfileContentType,
  parseStoredProfileJsonResult,
  rejectUnknownKeysResult,
} from "./profile";
import { buildEvlogDefaultRegistry } from "./evlog/schema";

export type EvlogStreamProfile = {
  kind: "evlog";
  redactKeys?: string[];
  correlation?: {
    requestIdFields?: string[];
    traceContextFields?: string[];
    parseTraceparent?: boolean;
  };
  observability?: {
    request?: {
      tracesStream: string;
    };
  };
};

const DEFAULT_REDACT_KEYS = ["password", "token", "secret", "authorization", "cookie", "apikey"] as const;
const REDACTED_VALUE = "[REDACTED]";
const EVLOG_RESERVED_FIELDS = new Set([
  "timestamp",
  "level",
  "service",
  "environment",
  "version",
  "region",
  "requestId",
  "traceId",
  "spanId",
  "method",
  "path",
  "status",
  "duration",
  "message",
  "why",
  "fix",
  "link",
  "sampling",
  "redaction",
  "context",
]);

type RedactionResult = {
  value: unknown;
  paths: string[];
};

function cloneEvlogProfile(profile: EvlogStreamProfile): EvlogStreamProfile {
  return cloneStreamProfileSpec(profile) as EvlogStreamProfile;
}

function cloneEvlogCache(cache: CachedStreamProfile | null): CachedStreamProfile | null {
  if (!cache || cache.profile.kind !== "evlog") return null;
  return {
    profile: cloneEvlogProfile(cache.profile as EvlogStreamProfile),
    updatedAtMs: cache.updatedAtMs,
  };
}

function isEvlogProfile(profile: StreamProfileSpec | null | undefined): profile is EvlogStreamProfile {
  return !!profile && profile.kind === "evlog";
}

function parseRedactKeysResult(raw: unknown, path: string): Result<string[] | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  if (!Array.isArray(raw)) return Result.err({ message: `${path} must be an array of strings` });
  if (raw.length > 64) return Result.err({ message: `${path} too large (max 64)` });

  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of raw) {
    if (typeof item !== "string") return Result.err({ message: `${path} must be an array of strings` });
    const value = item.trim().toLowerCase();
    if (value === "") return Result.err({ message: `${path} must not contain empty strings` });
    if (seen.has(value)) continue;
    seen.add(value);
    normalized.push(value);
  }
  return Result.ok(normalized);
}

function parseStringListResult(raw: unknown, path: string, maxItems: number): Result<string[] | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  if (!Array.isArray(raw)) return Result.err({ message: `${path} must be an array of strings` });
  if (raw.length > maxItems) return Result.err({ message: `${path} too large (max ${maxItems})` });
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of raw) {
    if (typeof item !== "string") return Result.err({ message: `${path} must be an array of strings` });
    const value = item.trim();
    if (value === "") return Result.err({ message: `${path} must not contain empty strings` });
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return Result.ok(out);
}

function parseEvlogCorrelationResult(raw: unknown, path: string): Result<EvlogStreamProfile["correlation"] | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["requestIdFields", "traceContextFields", "parseTraceparent"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  const requestIdFieldsRes = parseStringListResult(objRes.value.requestIdFields, `${path}.requestIdFields`, 64);
  if (Result.isError(requestIdFieldsRes)) return requestIdFieldsRes;
  const traceContextFieldsRes = parseStringListResult(objRes.value.traceContextFields, `${path}.traceContextFields`, 64);
  if (Result.isError(traceContextFieldsRes)) return traceContextFieldsRes;
  if (objRes.value.parseTraceparent !== undefined && typeof objRes.value.parseTraceparent !== "boolean") {
    return Result.err({ message: `${path}.parseTraceparent must be boolean` });
  }
  const correlation: NonNullable<EvlogStreamProfile["correlation"]> = {};
  if (requestIdFieldsRes.value) correlation.requestIdFields = requestIdFieldsRes.value;
  if (traceContextFieldsRes.value) correlation.traceContextFields = traceContextFieldsRes.value;
  if (objRes.value.parseTraceparent !== undefined) correlation.parseTraceparent = objRes.value.parseTraceparent;
  return Result.ok(Object.keys(correlation).length > 0 ? correlation : undefined);
}

function parseStreamNameResult(raw: unknown, path: string): Result<string | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  if (typeof raw !== "string") return Result.err({ message: `${path} must be a string` });
  const value = raw.trim();
  if (value === "") return Result.err({ message: `${path} must not be empty` });
  return Result.ok(value);
}

function parseEvlogObservabilityResult(raw: unknown, path: string): Result<EvlogStreamProfile["observability"] | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["request"], path);
  if (Result.isError(keyCheck)) return keyCheck;

  if (objRes.value.request === undefined) return Result.ok(undefined);
  const requestRes = expectPlainObjectResult(objRes.value.request, `${path}.request`);
  if (Result.isError(requestRes)) return requestRes;
  const requestKeyCheck = rejectUnknownKeysResult(requestRes.value, ["tracesStream"], `${path}.request`);
  if (Result.isError(requestKeyCheck)) return requestKeyCheck;
  const tracesStreamRes = parseStreamNameResult(requestRes.value.tracesStream, `${path}.request.tracesStream`);
  if (Result.isError(tracesStreamRes)) return tracesStreamRes;
  if (!tracesStreamRes.value) return Result.ok(undefined);

  return Result.ok({
    request: {
      tracesStream: tracesStreamRes.value,
    },
  });
}

function validateEvlogProfileResult(raw: unknown, path: string): Result<EvlogStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== "evlog") {
    return Result.err({ message: `${path}.kind must be evlog` });
  }
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind", "redactKeys", "correlation", "observability"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  const redactKeysRes = parseRedactKeysResult(objRes.value.redactKeys, `${path}.redactKeys`);
  if (Result.isError(redactKeysRes)) return redactKeysRes;
  const correlationRes = parseEvlogCorrelationResult(objRes.value.correlation, `${path}.correlation`);
  if (Result.isError(correlationRes)) return correlationRes;
  const observabilityRes = parseEvlogObservabilityResult(objRes.value.observability, `${path}.observability`);
  if (Result.isError(observabilityRes)) return observabilityRes;
  const profile: EvlogStreamProfile = { kind: "evlog" };
  if (redactKeysRes.value) profile.redactKeys = redactKeysRes.value;
  if (correlationRes.value) profile.correlation = correlationRes.value;
  if (observabilityRes.value) profile.observability = observabilityRes.value;
  return Result.ok(profile);
}

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

function normalizeTraceField(input: Record<string, unknown>, field: "traceId" | "spanId"): string | null {
  const direct = normalizeString(input[field]);
  if (direct) return direct;
  const traceContext = isPlainObject(input.traceContext) ? input.traceContext : null;
  return traceContext ? normalizeString(traceContext[field]) : null;
}

function readDottedString(input: Record<string, unknown>, path: string): string | null {
  let cur: unknown = input;
  for (const part of path.split(".")) {
    if (!isPlainObject(cur)) return null;
    cur = cur[part];
  }
  return normalizeString(cur);
}

function normalizeRequestId(input: Record<string, unknown>, profile: EvlogStreamProfile): string | null {
  const fields = profile.correlation?.requestIdFields ?? ["requestId", "context.requestId"];
  for (const field of fields) {
    const value = readDottedString(input, field);
    if (value) return value;
  }
  return null;
}

function normalizeConfiguredTraceField(input: Record<string, unknown>, profile: EvlogStreamProfile, field: "traceId" | "spanId"): string | null {
  const fields = profile.correlation?.traceContextFields;
  if (!fields) return normalizeTraceField(input, field);
  for (const path of fields) {
    if (path !== field && !path.endsWith(`.${field}`)) continue;
    const value = readDottedString(input, path);
    if (value) return value;
  }
  return normalizeTraceField(input, field);
}

function parseTraceparent(input: Record<string, unknown>): { traceId: string; spanId: string } | null {
  for (const path of ["traceparent", "traceContext.traceparent", "context.traceparent", "headers.traceparent"]) {
    const value = readDottedString(input, path);
    if (!value) continue;
    const match = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(?:-.+)?$/i.exec(value);
    if (!match) continue;
    const traceId = match[2].toLowerCase();
    const spanId = match[3].toLowerCase();
    if (/^0+$/.test(traceId) || /^0+$/.test(spanId)) continue;
    return { traceId, spanId };
  }
  return null;
}

function normalizeOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function normalizeOptionalInteger(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n) && Number.isInteger(n)) return n;
  }
  return null;
}

function deriveLevel(input: Record<string, unknown>, status: number | null): string {
  const direct = normalizeString(input.level)?.toLowerCase();
  if (direct === "debug" || direct === "info" || direct === "warn" || direct === "error") {
    return direct;
  }
  if (normalizeString(input.why) || normalizeString(input.fix) || normalizeString(input.link)) return "error";
  if (status != null && status >= 500) return "error";
  if (status != null && status >= 400) return "warn";
  return "info";
}

function redactValue(value: unknown, redactKeys: Set<string>, path = ""): RedactionResult {
  if (Array.isArray(value)) {
    const items = value.map((item, index) => redactValue(item, redactKeys, path === "" ? String(index) : `${path}.${index}`));
    return {
      value: items.map((item) => item.value),
      paths: items.flatMap((item) => item.paths),
    };
  }
  if (!isPlainObject(value)) return { value: structuredClone(value), paths: [] };

  const out: Record<string, unknown> = {};
  const paths: string[] = [];
  for (const [key, raw] of Object.entries(value)) {
    const keyPath = path === "" ? key : `${path}.${key}`;
    if (redactKeys.has(key.toLowerCase())) {
      out[key] = REDACTED_VALUE;
      paths.push(keyPath);
      continue;
    }
    const nested = redactValue(raw, redactKeys, keyPath);
    out[key] = nested.value;
    paths.push(...nested.paths);
  }
  return { value: out, paths };
}

function buildContext(input: Record<string, unknown>): Record<string, unknown> {
  const context: Record<string, unknown> = isPlainObject(input.context) ? structuredClone(input.context) : {};
  for (const [key, value] of Object.entries(input)) {
    if (EVLOG_RESERVED_FIELDS.has(key)) continue;
    context[key] = structuredClone(value);
  }
  if (!isPlainObject(input.context) && Object.prototype.hasOwnProperty.call(input, "context")) {
    context.context = structuredClone(input.context);
  }
  return context;
}

function normalizeEvlogRecordResult(profile: EvlogStreamProfile, value: unknown): Result<PreparedJsonRecord, { message: string }> {
  const objRes = expectPlainObjectResult(value, "evlog record");
  if (Result.isError(objRes)) return objRes;
  const input = objRes.value;

  const status = normalizeOptionalInteger(input.status);
  const duration = normalizeOptionalNumber(input.duration);
  const timestamp = normalizeString(input.timestamp) ?? new Date().toISOString();
  const requestId = normalizeRequestId(input, profile);
  const traceparent = profile.correlation?.parseTraceparent === false ? null : parseTraceparent(input);
  const traceId = normalizeConfiguredTraceField(input, profile, "traceId") ?? traceparent?.traceId ?? null;
  const spanId = normalizeConfiguredTraceField(input, profile, "spanId") ?? traceparent?.spanId ?? null;
  const contextRes = redactValue(buildContext(input), new Set([...DEFAULT_REDACT_KEYS, ...(profile.redactKeys ?? [])]));

  const normalized = {
    timestamp,
    level: deriveLevel(input, status),
    service: normalizeString(input.service),
    environment: normalizeString(input.environment),
    version: normalizeString(input.version),
    region: normalizeString(input.region),
    requestId,
    traceId,
    spanId,
    method: normalizeString(input.method),
    path: normalizeString(input.path),
    status,
    duration,
    message: normalizeString(input.message),
    why: normalizeString(input.why),
    fix: normalizeString(input.fix),
    link: normalizeString(input.link),
    sampling: Object.prototype.hasOwnProperty.call(input, "sampling") ? structuredClone(input.sampling) : null,
    redaction: { keys: contextRes.paths },
    context: contextRes.value as Record<string, unknown>,
  };

  return Result.ok({
    value: normalized,
    routingKey: requestId ?? traceId ?? null,
  });
}

function evlogSeverity(record: Record<string, unknown>): "debug" | "info" | "warn" | "error" {
  const level = normalizeString(record.level)?.toLowerCase();
  if (level === "debug" || level === "info" || level === "warn" || level === "error") return level;
  const status = normalizeOptionalInteger(record.status);
  if (status != null && status >= 500) return "error";
  if (status != null && status >= 400) return "warn";
  return "info";
}

function evlogTimelineItems(args: { stream: string; offset?: string; record: unknown }): UnifiedTimelineItem[] {
  if (!isPlainObject(args.record)) return [];
  const record = args.record;
  const timestamp = normalizeString(record.timestamp);
  if (!timestamp) return [];
  const message = normalizeString(record.message);
  const method = normalizeString(record.method);
  const path = normalizeString(record.path);
  const title = message ?? ([method, path].filter(Boolean).join(" ") || "evlog event");
  return [
    {
      kind: "evlog.event",
      time: timestamp,
      duration: normalizeOptionalNumber(record.duration),
      service: normalizeString(record.service),
      title,
      severity: evlogSeverity(record),
      ids: {
        requestId: normalizeString(record.requestId),
        traceId: normalizeString(record.traceId),
        spanId: normalizeString(record.spanId),
      },
      source: {
        stream: args.stream,
        offset: args.offset,
        profile: "evlog",
      },
      data: record,
    },
  ];
}

export const EVLOG_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: "evlog",
  usesStoredProfileRow: true,

  defaultProfile(): EvlogStreamProfile {
    return { kind: "evlog" };
  },

  validateResult(raw, path) {
    return validateEvlogProfileResult(raw, path);
  },

  readProfileResult({ row, cached }): Result<StreamProfileReadResult, { message: string }> {
    if (!row) return Result.ok({ profile: { kind: "evlog" }, cache: null });
    const cachedCopy = cloneEvlogCache(cached);
    if (cachedCopy && cachedCopy.updatedAtMs === row.updated_at_ms) {
      return Result.ok({
        profile: cloneEvlogProfile(cachedCopy.profile as EvlogStreamProfile),
        cache: cachedCopy,
      });
    }
    const parsedRes = parseStoredProfileJsonResult(row.profile_json);
    if (Result.isError(parsedRes)) return parsedRes;
    const profileRes = validateEvlogProfileResult(parsedRes.value, "profile");
    if (Result.isError(profileRes)) return profileRes;
    const profile = cloneEvlogProfile(profileRes.value);
    return Result.ok({
      profile: cloneEvlogProfile(profile),
      cache: { profile, updatedAtMs: row.updated_at_ms },
    });
  },

  persistProfileResult({ db, registry, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string; code?: string }> {
    if (!isEvlogProfile(profile)) {
      return Result.err({ kind: "bad_request", message: "invalid evlog profile" });
    }
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "evlog profile requires application/json stream content-type",
      });
    }
    if (streamRow.profile !== "evlog" && streamRow.next_offset > 0n) {
      return Result.err({
        kind: "bad_request",
        message: "evlog profile must be installed before appending data",
      });
    }

    const persistedProfile = cloneEvlogProfile(profile);
    const registryRes = registry.replaceRegistryResult(stream, buildEvlogDefaultRegistry(stream));
    if (Result.isError(registryRes)) {
      return Result.err({ kind: "bad_request", message: registryRes.error.message });
    }
    db.updateStreamProfile(stream, persistedProfile.kind);
    db.upsertStreamProfile(stream, JSON.stringify(persistedProfile));
    db.deleteStreamTouchState(stream);
    const row = db.getStreamProfile(stream);
    return Result.ok({
      profile: cloneEvlogProfile(persistedProfile),
      cache: {
        profile: persistedProfile,
        updatedAtMs: row?.updated_at_ms ?? db.nowMs(),
      },
      schemaRegistry: registryRes.value,
    });
  },

  jsonIngest: {
    prepareRecordResult({ profile, value }) {
      if (!isEvlogProfile(profile)) return Result.err({ message: "invalid evlog profile" });
      return normalizeEvlogRecordResult(profile, value);
    },
  },

  correlation: {
    toTimelineItems(args) {
      return evlogTimelineItems(args);
    },
  },
};
