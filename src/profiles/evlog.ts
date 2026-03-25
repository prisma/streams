import { Result } from "better-result";
import type {
  CachedStreamProfile,
  PreparedJsonRecord,
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
  StreamProfileSpec,
} from "./profile";
import {
  cloneStreamProfileSpec,
  expectPlainObjectResult,
  isPlainObject,
  normalizeProfileContentType,
  parseStoredProfileJsonResult,
  rejectUnknownKeysResult,
} from "./profile";

export type EvlogStreamProfile = {
  kind: "evlog";
  redactKeys?: string[];
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

function validateEvlogProfileResult(raw: unknown, path: string): Result<EvlogStreamProfile, { message: string }> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  if (objRes.value.kind !== "evlog") {
    return Result.err({ message: `${path}.kind must be evlog` });
  }
  const keyCheck = rejectUnknownKeysResult(objRes.value, ["kind", "redactKeys"], path);
  if (Result.isError(keyCheck)) return keyCheck;
  const redactKeysRes = parseRedactKeysResult(objRes.value.redactKeys, `${path}.redactKeys`);
  if (Result.isError(redactKeysRes)) return redactKeysRes;
  return Result.ok(redactKeysRes.value ? { kind: "evlog", redactKeys: redactKeysRes.value } : { kind: "evlog" });
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

function normalizeOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
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

  const status = normalizeOptionalNumber(input.status);
  const duration = normalizeOptionalNumber(input.duration);
  const timestamp = normalizeString(input.timestamp) ?? new Date().toISOString();
  const requestId = normalizeString(input.requestId);
  const traceId = normalizeTraceField(input, "traceId");
  const spanId = normalizeTraceField(input, "spanId");
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

  persistProfileResult({ db, stream, streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string; code?: string }> {
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
    db.updateStreamProfile(stream, persistedProfile.kind);
    db.upsertStreamProfile(stream, JSON.stringify(persistedProfile));
    const row = db.getStreamProfile(stream);
    return Result.ok({
      profile: cloneEvlogProfile(persistedProfile),
      cache: {
        profile: persistedProfile,
        updatedAtMs: row?.updated_at_ms ?? db.nowMs(),
      },
    });
  },

  jsonIngest: {
    prepareRecordResult({ profile, value }) {
      if (!isEvlogProfile(profile)) return Result.err({ message: "invalid evlog profile" });
      return normalizeEvlogRecordResult(profile, value);
    },
  },
};
