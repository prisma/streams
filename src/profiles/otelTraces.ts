import { Result } from "better-result";
import type {
  CachedStreamProfile,
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
  StreamProfileSpec,
  UnifiedTimelineItem,
} from "./profile";
import {
  cloneStreamProfileSpec,
  expectPlainObjectResult,
  normalizeProfileContentType,
  parseStoredProfileJsonResult,
  rejectUnknownKeysResult,
  isPlainObject,
} from "./profile";
import { buildOtelTracesDefaultRegistry } from "./otelTraces/schema";
import {
  DEFAULT_ATTRIBUTE_LIMITS,
  DEFAULT_OTEL_TRACE_REDACT_KEYS,
  DEFAULT_REQUEST_ID_ATTRIBUTES,
  DEFAULT_STORE_CONFIG,
  normalizeOtelTraceRecordResult,
  type DbStatementMode,
  type OtelTraceAttributeLimits,
  type OtelTraceStoreConfig,
  type OtelTracesStreamProfile,
} from "./otelTraces/normalize";
import { decodeOtlpTraceExportRequestResult } from "./otelTraces/otlp";

export type { OtelTracesStreamProfile };

function cloneOtelTracesProfile(profile: OtelTracesStreamProfile): OtelTracesStreamProfile {
  return cloneStreamProfileSpec(profile) as OtelTracesStreamProfile;
}

function cloneOtelTracesCache(cache: CachedStreamProfile | null): CachedStreamProfile | null {
  if (!cache || cache.profile.kind !== "otel-traces") return null;
  return {
    profile: cloneOtelTracesProfile(cache.profile as OtelTracesStreamProfile),
    updatedAtMs: cache.updatedAtMs,
  };
}

function isOtelTracesProfile(profile: StreamProfileSpec | null | undefined): profile is OtelTracesStreamProfile {
  return !!profile && profile.kind === "otel-traces";
}

function parseStringArrayResult(raw: unknown, path: string, maxItems: number): Result<string[] | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  if (!Array.isArray(raw)) return Result.err({ message: `${path} must be an array of strings` });
  if (raw.length > maxItems) return Result.err({ message: `${path} too large (max ${maxItems})` });
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of raw) {
    if (typeof item !== "string") return Result.err({ message: `${path} must be an array of strings` });
    const value = item.trim();
    if (value === "") return Result.err({ message: `${path} must not contain empty strings` });
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(path.endsWith("redactKeys") ? key : value);
  }
  return Result.ok(out);
}

function parsePositiveIntResult(raw: unknown, path: string, fallback: number): Result<number, { message: string }> {
  if (raw === undefined) return Result.ok(fallback);
  if (typeof raw !== "number" || !Number.isFinite(raw) || !Number.isInteger(raw) || raw <= 0) {
    return Result.err({ message: `${path} must be a positive integer` });
  }
  return Result.ok(raw);
}

function parseAttributeLimitsResult(raw: unknown, path: string): Result<Partial<OtelTraceAttributeLimits> | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  const keyCheck = rejectUnknownKeysResult(
    objRes.value,
    ["maxAttributeValueBytes", "maxAttributesPerSpan", "maxEventsPerSpan", "maxLinksPerSpan", "maxStatementBytes"],
    path
  );
  if (Result.isError(keyCheck)) return keyCheck;
  const out: Partial<OtelTraceAttributeLimits> = {};
  for (const key of Object.keys(DEFAULT_ATTRIBUTE_LIMITS) as Array<keyof OtelTraceAttributeLimits>) {
    const valueRes = parsePositiveIntResult(objRes.value[key], `${path}.${key}`, DEFAULT_ATTRIBUTE_LIMITS[key]);
    if (Result.isError(valueRes)) return valueRes;
    if (objRes.value[key] !== undefined) out[key] = valueRes.value;
  }
  return Result.ok(Object.keys(out).length > 0 ? out : undefined);
}

function parseStoreResult(raw: unknown, path: string): Result<Partial<OtelTraceStoreConfig> | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["rawResourceAttributes", "rawSpanAttributes", "rawEvents", "rawLinks"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  const out: Partial<OtelTraceStoreConfig> = {};
  for (const key of Object.keys(DEFAULT_STORE_CONFIG) as Array<keyof OtelTraceStoreConfig>) {
    const value = objRes.value[key];
    if (value === undefined) continue;
    if (typeof value !== "boolean") return Result.err({ message: `${path}.${key} must be boolean` });
    out[key] = value;
  }
  return Result.ok(Object.keys(out).length > 0 ? out : undefined);
}

function parseDbStatementModeResult(raw: unknown, path: string): Result<DbStatementMode | undefined, { message: string }> {
  if (raw === undefined) return Result.ok(undefined);
  if (raw === "drop" || raw === "raw") return Result.ok(raw);
  return Result.err({ message: `${path} must be drop or raw` });
}

function validateOtelTracesProfileResult(raw: unknown, path: string): Result<OtelTracesStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== "otel-traces") return Result.err({ message: `${path}.kind must be otel-traces` });
  const keyCheck = rejectUnknownKeysResult(
    objRes.value,
    ["kind", "redactKeys", "requestIdAttributes", "attributeLimits", "store", "dbStatementMode"],
    path
  );
  if (Result.isError(keyCheck)) return keyCheck;
  const redactKeysRes = parseStringArrayResult(objRes.value.redactKeys, `${path}.redactKeys`, 64);
  if (Result.isError(redactKeysRes)) return redactKeysRes;
  const requestIdAttributesRes = parseStringArrayResult(objRes.value.requestIdAttributes, `${path}.requestIdAttributes`, 64);
  if (Result.isError(requestIdAttributesRes)) return requestIdAttributesRes;
  const limitsRes = parseAttributeLimitsResult(objRes.value.attributeLimits, `${path}.attributeLimits`);
  if (Result.isError(limitsRes)) return limitsRes;
  const storeRes = parseStoreResult(objRes.value.store, `${path}.store`);
  if (Result.isError(storeRes)) return storeRes;
  const dbStatementModeRes = parseDbStatementModeResult(objRes.value.dbStatementMode, `${path}.dbStatementMode`);
  if (Result.isError(dbStatementModeRes)) return dbStatementModeRes;
  const profile: OtelTracesStreamProfile = { kind: "otel-traces" };
  if (redactKeysRes.value) profile.redactKeys = redactKeysRes.value;
  if (requestIdAttributesRes.value) profile.requestIdAttributes = requestIdAttributesRes.value;
  if (limitsRes.value) profile.attributeLimits = limitsRes.value;
  if (storeRes.value) profile.store = storeRes.value;
  if (dbStatementModeRes.value) profile.dbStatementMode = dbStatementModeRes.value;
  return Result.ok(profile);
}

function getString(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function getNumber(record: Record<string, unknown>, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function severityForSpan(record: Record<string, unknown>): "debug" | "info" | "warn" | "error" {
  const status = isPlainObject(record.status) ? getString(record.status, "code") : null;
  const error = isPlainObject(record.error) && record.error.isError === true;
  return status === "error" || error ? "error" : "info";
}

function buildOtelTimelineItems(args: { stream: string; offset?: string; record: unknown }): UnifiedTimelineItem[] {
  if (!isPlainObject(args.record)) return [];
  const record = args.record;
  const traceId = getString(record, "traceId");
  const spanId = getString(record, "spanId");
  const parentSpanId = getString(record, "parentSpanId");
  const requestId = getString(record, "requestId");
  const service = getString(record, "service");
  const title = getString(record, "name") ?? spanId ?? "span";
  const timestamp = getString(record, "timestamp");
  const endTimestamp = getString(record, "endTimestamp");
  const duration = getNumber(record, "duration");
  const severity = severityForSpan(record);
  const source = { stream: args.stream, offset: args.offset, profile: "otel-traces" };
  const ids = { requestId, traceId, spanId, parentSpanId };
  const out: UnifiedTimelineItem[] = [];
  if (timestamp) {
    out.push({
      kind: "otel.span.start",
      time: timestamp,
      duration,
      service,
      title,
      severity,
      ids,
      source,
      data: record,
    });
  }
  if (Array.isArray(record.events)) {
    for (const event of record.events) {
      if (!isPlainObject(event)) continue;
      const eventTime = getString(event, "timestamp");
      const eventName = getString(event, "name") ?? "span event";
      if (!eventTime) continue;
      out.push({
        kind: eventName === "exception" ? "otel.exception" : "otel.span.event",
        time: eventTime,
        service,
        title: eventName,
        severity: eventName === "exception" ? "error" : severity,
        ids,
        source,
        data: event,
      });
    }
  }
  if (endTimestamp) {
    out.push({
      kind: "otel.span.end",
      time: endTimestamp,
      duration,
      service,
      title,
      severity,
      ids,
      source,
      data: record,
    });
  }
  return out;
}

export const OTEL_TRACES_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: "otel-traces",
  usesStoredProfileRow: true,

  defaultProfile(): OtelTracesStreamProfile {
    return { kind: "otel-traces" };
  },

  validateResult(raw, path) {
    return validateOtelTracesProfileResult(raw, path);
  },

  readProfileResult({ row, cached }): Result<StreamProfileReadResult, { message: string }> {
    if (!row) return Result.ok({ profile: { kind: "otel-traces" }, cache: null });
    const cachedCopy = cloneOtelTracesCache(cached);
    if (cachedCopy && cachedCopy.updatedAtMs === row.updated_at_ms) {
      return Result.ok({
        profile: cloneOtelTracesProfile(cachedCopy.profile as OtelTracesStreamProfile),
        cache: cachedCopy,
      });
    }
    const parsedRes = parseStoredProfileJsonResult(row.profile_json);
    if (Result.isError(parsedRes)) return parsedRes;
    const profileRes = validateOtelTracesProfileResult(parsedRes.value, "profile");
    if (Result.isError(profileRes)) return profileRes;
    const profile = cloneOtelTracesProfile(profileRes.value);
    return Result.ok({
      profile: cloneOtelTracesProfile(profile),
      cache: { profile, updatedAtMs: row.updated_at_ms },
    });
  },

  persistProfileResult({ db, registry, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string; code?: string }> {
    if (!isOtelTracesProfile(profile)) return Result.err({ kind: "bad_request", message: "invalid otel-traces profile" });
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "otel-traces profile requires application/json stream content-type",
      });
    }
    if (streamRow.profile !== "otel-traces" && streamRow.next_offset > 0n) {
      return Result.err({
        kind: "bad_request",
        message: "otel-traces profile must be installed before appending data",
      });
    }

    const persistedProfile = cloneOtelTracesProfile(profile);
    const registryRes = registry.replaceRegistryResult(stream, buildOtelTracesDefaultRegistry(stream));
    if (Result.isError(registryRes)) {
      return Result.err({ kind: "bad_request", message: registryRes.error.message });
    }
    db.updateStreamProfile(stream, persistedProfile.kind);
    db.upsertStreamProfile(stream, JSON.stringify(persistedProfile));
    db.deleteStreamTouchState(stream);
    const row = db.getStreamProfile(stream);
    return Result.ok({
      profile: cloneOtelTracesProfile(persistedProfile),
      cache: {
        profile: persistedProfile,
        updatedAtMs: row?.updated_at_ms ?? db.nowMs(),
      },
      schemaRegistry: registryRes.value,
    });
  },

  jsonIngest: {
    prepareRecordResult({ profile, value }) {
      if (!isOtelTracesProfile(profile)) return Result.err({ message: "invalid otel-traces profile" });
      return normalizeOtelTraceRecordResult(profile, value);
    },
  },

  otlpTraces: {
    decodeExportRequestResult({ profile, stream, contentType, contentEncoding, body, maxDecodedBytes }) {
      if (!isOtelTracesProfile(profile)) return Result.err({ status: 400, message: "invalid otel-traces profile" });
      return decodeOtlpTraceExportRequestResult({ stream, profile, contentType, contentEncoding, body, maxDecodedBytes });
    },
  },

  correlation: {
    toTimelineItems(args) {
      return buildOtelTimelineItems(args);
    },
  },
};
