import { Result } from "better-result";
import type { SqliteDurableStore, StreamRow } from "../db/db";
import type { TouchProcessorManager } from "../touch/manager";
import type { CanonicalChange } from "../touch/canonical_change";
import type { TouchConfig } from "../touch/spec";

export const STREAM_PROFILE_API_VERSION = "durable.streams/profile/v1" as const;
export const DEFAULT_STREAM_PROFILE = "generic" as const;

export type StreamProfileKind = string;

export type StreamProfileSpec = {
  kind: StreamProfileKind;
  [key: string]: unknown;
};

export type StreamProfileResource = {
  apiVersion: typeof STREAM_PROFILE_API_VERSION;
  profile: StreamProfileSpec;
};

export type StreamProfileMutationError = {
  kind: "bad_request";
  message: string;
  code?: string;
};

export type StreamProfileReadError = {
  kind: "invalid_profile";
  message: string;
  code?: string;
};

export type StreamProfileValidationError = {
  message: string;
};

export type StoredProfileRow = {
  stream: string;
  profile_json: string;
  updated_at_ms: bigint;
};

export type CachedStreamProfile = {
  profile: StreamProfileSpec;
  updatedAtMs: bigint;
};

export type StreamProfileReadResult = {
  profile: StreamProfileSpec;
  cache: CachedStreamProfile | null;
};

export type StreamProfilePersistResult = {
  profile: StreamProfileSpec;
  cache: CachedStreamProfile | null;
};

export type PersistProfileArgs = {
  db: SqliteDurableStore;
  stream: string;
  streamRow: StreamRow;
  profile: StreamProfileSpec;
};

export type PreparedJsonRecord = {
  value: unknown;
  routingKey: string | null;
};

export type StreamTouchRoute =
  | { kind: "meta" }
  | { kind: "wait" }
  | { kind: "templates_activate" };

export type StreamProfileTouchResponder = {
  json(status: number, body: any, headers?: HeadersInit): Response;
  badRequest(message: string): Response;
  internalError(message?: string): Response;
  notFound(message?: string): Response;
};

export type StreamTouchRouteArgs = {
  route: StreamTouchRoute;
  req: Request;
  stream: string;
  streamRow: StreamRow;
  profile: StreamProfileSpec;
  db: SqliteDurableStore;
  touchManager: TouchProcessorManager;
  respond: StreamProfileTouchResponder;
};

export interface StreamTouchCapability {
  getTouchConfig(profile: StreamProfileSpec): TouchConfig | null;
  syncState(args: { db: SqliteDurableStore; stream: string; profile: StreamProfileSpec }): void;
  deriveCanonicalChanges(record: unknown, profile: StreamProfileSpec): CanonicalChange[];
  handleRoute?(args: StreamTouchRouteArgs): Promise<Response>;
}

export interface StreamProfileJsonIngestCapability {
  prepareRecordResult(args: { stream: string; profile: StreamProfileSpec; value: unknown }): Result<PreparedJsonRecord, StreamProfileValidationError>;
}

export interface StreamProfileDefinition {
  kind: StreamProfileKind;
  usesStoredProfileRow: boolean;
  defaultProfile(): StreamProfileSpec;
  validateResult(raw: unknown, path: string): Result<StreamProfileSpec, StreamProfileValidationError>;
  readProfileResult(args: { row: StoredProfileRow | null; cached: CachedStreamProfile | null }): Result<StreamProfileReadResult, StreamProfileValidationError>;
  persistProfileResult(args: PersistProfileArgs): Result<StreamProfilePersistResult, StreamProfileMutationError>;
  touch?: StreamTouchCapability;
  jsonIngest?: StreamProfileJsonIngestCapability;
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

export function expectPlainObjectResult(
  value: unknown,
  path: string
): Result<Record<string, unknown>, StreamProfileValidationError> {
  if (!isPlainObject(value)) return Result.err({ message: `${path} must be an object` });
  return Result.ok(value);
}

export function rejectUnknownKeysResult(
  obj: Record<string, unknown>,
  allowed: readonly string[],
  path: string
): Result<void, StreamProfileValidationError> {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(obj)) {
    if (!allowedSet.has(key)) return Result.err({ message: `${path}.${key} is not supported` });
  }
  return Result.ok(undefined);
}

export function normalizeProfileContentType(value: string | null): string | null {
  if (!value) return null;
  const base = value.split(";")[0]?.trim().toLowerCase();
  return base ? base : null;
}

export function parseStoredProfileJsonResult(raw: string): Result<unknown, StreamProfileValidationError> {
  try {
    return Result.ok(JSON.parse(raw));
  } catch (e: any) {
    return Result.err({ message: String(e?.message ?? e) });
  }
}

export function readProfileKindResult(
  raw: unknown,
  path = "profile"
): Result<StreamProfileKind, StreamProfileValidationError> {
  const objRes = expectPlainObjectResult(raw, path);
  if (Result.isError(objRes)) return objRes;
  const kind = typeof objRes.value.kind === "string" ? objRes.value.kind.trim() : "";
  if (kind !== "") return Result.ok(kind);
  return Result.err({ message: `${path}.kind must be a non-empty string` });
}

export function parseProfileUpdateEnvelopeResult(body: unknown): Result<unknown, StreamProfileValidationError> {
  const bodyRes = expectPlainObjectResult(body, "profile update");
  if (Result.isError(bodyRes)) {
    return Result.err({ message: "profile update must be a JSON object" });
  }
  const keyCheck = rejectUnknownKeysResult(bodyRes.value, ["apiVersion", "profile"], "profileUpdate");
  if (Result.isError(keyCheck)) return keyCheck;
  if (bodyRes.value.apiVersion !== undefined && bodyRes.value.apiVersion !== STREAM_PROFILE_API_VERSION) {
    return Result.err({ message: "invalid profile apiVersion" });
  }
  if (!Object.prototype.hasOwnProperty.call(bodyRes.value, "profile")) {
    return Result.err({ message: "missing profile" });
  }
  return Result.ok(bodyRes.value.profile);
}

export function cloneStreamProfileSpec(profile: StreamProfileSpec): StreamProfileSpec {
  return structuredClone(profile);
}

export function buildStreamProfileResource(profile: StreamProfileSpec): StreamProfileResource {
  return {
    apiVersion: STREAM_PROFILE_API_VERSION,
    profile: cloneStreamProfileSpec(profile),
  };
}
